//! Pushing this repository's refs to a git server somewhere else, and
//! fetching that server's refs into it.
//!
//! The one place Enroute is a git *client*: everything else here terminates
//! somebody's connection, and this dials out. A push sends a pack built by
//! the same code that answers a fetch; a fetch lands its pack through the
//! same checks as a push. Nothing about a remote is kept, because a
//! remote is a relationship between an application's repository and somebody
//! else's, and this layer holds neither — so a caller brings the URL, the
//! credentials and the refs with the call.

mod advertise;
mod commands;
mod endpoint;
mod fetch;
mod report;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt as _;
use reqwest::RequestBuilder;
use reqwest::header::{ACCEPT, CONTENT_TYPE};

use crate::git::oid::{ObjectFormat, Oid};
use crate::protocol::AGENT;
use crate::protocol::pktline::{self, Pkt, Reader};
use crate::query::{self, PushPack};
use crate::repo::view::Snapshot;

use self::advertise::Advertisement;
use self::commands::{Command, Step, plan};
use self::endpoint::Endpoint;
pub use self::endpoint::Reach;
pub(crate) use self::fetch::{FetchRequest, Fetched};

/// The largest advertisement this reads, and the largest report.
///
/// Neither is a repository's worth of data: one is bounded by how many refs
/// the remote has, the other by how many this push sent.
const MAX_ADVERTISEMENT: usize = 64 << 20;
const MAX_REPORT: usize = 8 << 20;

/// How long the remote has to answer the questions that carry no pack.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a push may take, end to end.
///
/// A backstop rather than a tuning knob: a caller names the remote, and one
/// that takes a pack and never answers must not hold a task here for ever.
const PUSH_TIMEOUT: Duration = Duration::from_hours(1);

/// How long a remote may say nothing at all while an answer is being read.
const STALL_TIMEOUT: Duration = Duration::from_mins(1);

/// How many pack chunks wait for the remote before the writer stops.
const PACK_CHANNEL_CAPACITY: usize = 16;

/// Where a push goes, and what proves it may land there.
#[derive(Debug, Clone, Default)]
pub struct Remote {
    /// The URL a git client would be given, e.g.
    /// `https://github.com/acme/widgets.git`.
    pub url: String,
    /// Sent with every request rather than after a challenge: a git host
    /// answers an unauthenticated push with a 401 and nothing else.
    pub credentials: Option<Credentials>,
}

/// What a remote takes as proof.
#[derive(Clone)]
pub enum Credentials {
    /// A username and a password — GitHub reads a token as the password and
    /// ignores the username.
    Basic {
        /// Sent as the basic-auth username.
        username: String,
        /// The password, or the token standing in for one.
        password: String,
    },
    /// `Authorization: Bearer`, for a host that reads one.
    Bearer(String),
}

/// Debug by hand, so a credential cannot reach a log by being inside
/// something that was printed.
impl std::fmt::Debug for Credentials {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(match *self {
            Self::Basic { .. } => "Basic(redacted)",
            Self::Bearer(_) => "Bearer(redacted)",
        })
    }
}

/// One ref to send: what it is called here, and what it is called there.
#[derive(Debug, Clone, Default)]
pub struct RefSpec {
    /// The ref to send, fully qualified, or empty to delete `destination`.
    pub source: String,
    /// The ref to move on the remote, fully qualified, or empty for
    /// `source` under the same name.
    pub destination: String,
    /// Send even when what the remote holds is not an ancestor of what is
    /// sent.
    pub force: bool,
}

/// A push, as a whole.
#[derive(Debug, Clone, Default)]
pub struct PushRequest {
    /// Where it goes.
    pub remote: Remote,
    /// What to send, refused when empty rather than meaning every ref.
    pub refs: Vec<RefSpec>,
    /// Land every ref or none, using git's `atomic` capability.
    pub atomic: bool,
}

/// What one ref did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefOutcome {
    /// The ref on the remote this is about.
    pub destination: String,
    /// What the remote held before the push, all-zero for a ref it did not
    /// have.
    pub old_id: Oid,
    /// What it was asked to hold, all-zero for a delete.
    pub new_id: Oid,
    /// What came of it.
    pub status: PushStatus,
}

/// What came of one ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushStatus {
    /// The remote moved the ref, or created it.
    Updated,
    /// The remote deleted it.
    Deleted,
    /// The remote already held what was sent, so nothing was sent for it.
    UpToDate,
    /// Refused, in the remote's own words — or this side's, when the
    /// fast-forward check refused it before it was sent.
    Rejected(String),
}

/// Everything that can stop a push before it has an outcome to report.
///
/// A ref the remote refuses is not here: that is a [`RefOutcome`], because a
/// push can land some refs and be refused others.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The request cannot be carried out as it is stated.
    #[error("{0}")]
    Request(String),
    /// The remote cannot be reached, or is not one this deployment may dial.
    #[error("{0}")]
    Remote(String),
    /// The remote would not take the credentials.
    #[error("the remote refused the credentials")]
    Unauthorized,
    /// The remote does not offer something the request needs.
    #[error("the remote does not offer {0}")]
    Unsupported(String),
    /// The remote answered with something this side cannot read as git.
    #[error("{0}")]
    Protocol(String),
    /// The remote would not store the pack, so no ref moved.
    #[error("the remote stored nothing: {0}")]
    PackRefused(String),
    /// The push reached the remote and its answer did not come back, so what
    /// landed is the remote's to say and nobody asked it.
    #[error("the push was sent and the remote's answer was not readable: {0}")]
    ReportUnread(String),
    /// The connection itself.
    #[error("reaching the remote: {0}")]
    Transport(#[from] reqwest::Error),
    /// Reading what this repository holds.
    #[error(transparent)]
    Storage(#[from] anyhow::Error),
}

/// The pkt-lines in what the remote answered.
///
/// Framing this side could not read is the remote's answer being wrong,
/// never this side's request.
fn packets(body: &[u8]) -> impl Iterator<Item = Result<Pkt<'_>, Error>> {
    Reader::new(body).map(|packet| packet.map_err(|error| Error::Protocol(format!("{error:#}"))))
}

/// A line's payload as text, lossy: a garbled report reads better than none.
fn text(line: &[u8]) -> String {
    String::from_utf8_lossy(pktline::chomp(line)).into_owned()
}

/// Enroute as a git client.
///
/// Holds the HTTP client and what it may dial, so a deployment's answer to
/// both is decided once rather than per push.
#[derive(Debug)]
pub struct Client {
    http: reqwest::Client,
    reach: Reach,
}

impl Client {
    /// A client that may dial whatever `reach` allows.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built, which is the TLS
    /// backend failing to start.
    pub fn new(reach: Reach) -> Result<Self, Error> {
        let mut http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            // A redirect is refused rather than followed: it is a URL the
            // caller did not name, and their credentials are on the request.
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(AGENT);
        if !reach.private {
            http = http.dns_resolver(Arc::new(endpoint::PublicOnly));
        }
        let http = http.build()?;
        Ok(Self { http, reach })
    }

    /// Push `request.refs` from `snap` to `request.remote`, and report what
    /// each did.
    ///
    /// One snapshot, so that the refs, the fast-forward checks and the pack
    /// all agree.
    ///
    /// # Errors
    ///
    /// Returns an error if the remote cannot be reached or dialed, does not
    /// answer git, or refuses the pack — nothing landed then.
    #[tracing::instrument(
        name = "enroute::remote::push",
        skip(self, snap, request),
        // The URL is recorded once it has been through `Endpoint`, which is
        // what refuses credentials inside it: a field here is read on the
        // way in, and would carry a rejected URL's token onto the span.
        fields(refs = request.refs.len(), remote = tracing::field::Empty)
    )]
    pub async fn push(
        &self,
        snap: &Arc<Snapshot>,
        request: &PushRequest,
    ) -> Result<Vec<RefOutcome>, Error> {
        if request.refs.is_empty() {
            return Err(Error::Request("a push names no refs".into()));
        }
        let credentials = request.remote.credentials.as_ref();
        let endpoint = Endpoint::parse(&request.remote.url, self.reach)?;
        tracing::Span::current().record("remote", tracing::field::display(endpoint.url()));
        let format = snap.format();
        let advertisement = self.discover(&endpoint, credentials, format).await?;

        if request.atomic && !advertisement.offers("atomic") {
            return Err(Error::Unsupported("atomic".into()));
        }
        if !advertisement.offers("report-status") {
            return Err(Error::Unsupported("report-status".into()));
        }

        let steps = plan(format, &request.refs, &snap.meta.refs, &advertisement)?;
        let steps = fast_forward_only(snap, steps).await?;
        // The remote applies what it is sent, all or nothing. A ref refused
        // on this side is never in that set, so an atomic push holding one
        // has to stop here — or the remote lands the rest atomically and the
        // push was not all-or-nothing after all.
        let steps = if request.atomic {
            hold_back(steps)
        } else {
            steps
        };

        let commands: Vec<&Command> = steps
            .iter()
            .filter_map(|step| match *step {
                Step::Send(ref command) => Some(command),
                Step::Done(_) => None,
            })
            .collect();
        if commands.is_empty() {
            return Ok(settled(steps));
        }

        let report = self
            .send(
                snap,
                &endpoint,
                credentials,
                &commands,
                request,
                &advertisement,
            )
            .await?;
        Ok(reported(steps, &report))
    }

    /// Read what the remote holds.
    async fn discover(
        &self,
        endpoint: &Endpoint,
        credentials: Option<&Credentials>,
        format: ObjectFormat,
    ) -> Result<Advertisement, Error> {
        let response = authorize(self.http.get(endpoint.advertisement()?), credentials)
            .header(ACCEPT, "application/x-git-receive-pack-advertisement")
            .timeout(DISCOVERY_TIMEOUT)
            .send()
            .await?;

        refuse_redirect(&response)?;
        check_status(&response)?;
        let body = read_capped(response, MAX_ADVERTISEMENT).await?;
        advertise::parse(format, &body)
    }

    /// Send the commands and the pack they need, and read the report.
    async fn send(
        &self,
        snap: &Arc<Snapshot>,
        endpoint: &Endpoint,
        credentials: Option<&Credentials>,
        commands: &[&Command],
        request: &PushRequest,
        advertisement: &Advertisement,
    ) -> Result<report::Report, Error> {
        // As git: the format is named when the remote names one (it is the
        // repository's, or the advertisement was refused).
        let format = advertisement
            .capabilities
            .iter()
            .any(|c| c.starts_with("object-format="))
            .then(|| snap.format());
        let head = request_head(commands, &capabilities(request.atomic, format))?;
        let wants: Vec<Oid> = commands
            .iter()
            .filter(|command| !command.deletes())
            .map(|command| command.new)
            .collect();

        // A push of nothing but deletions carries no pack at all, which is
        // what git itself sends.
        let body = if wants.is_empty() {
            reqwest::Body::from(head)
        } else {
            // The haves are everything the remote advertised. What it does
            // not hold, it did not advertise; what this repository does not
            // hold is dropped by the walk.
            let pack = query::pack_for_push(snap, &wants, &advertisement.tips)
                .await
                .map_err(|error| Error::Storage(anyhow::anyhow!("{error}")))?;
            let pack = stream(pack);
            let head = futures::stream::once(async move { Ok(Bytes::from(head)) });
            reqwest::Body::wrap_stream(head.chain(pack))
        };

        let response = authorize(self.http.post(endpoint.receive_pack()?), credentials)
            .header(CONTENT_TYPE, "application/x-git-receive-pack-request")
            .header(ACCEPT, "application/x-git-receive-pack-result")
            .timeout(PUSH_TIMEOUT)
            .body(body)
            .send()
            .await?;
        check_status(&response)?;

        // Past here the remote has the push and has already decided what to
        // do with it, so a failure to read its answer is not a failure to
        // land: the caller is told the difference.
        let body = read_capped(response, MAX_REPORT)
            .await
            .map_err(|error| Error::ReportUnread(error.to_string()))?;
        let report =
            report::parse(&body).map_err(|error| Error::ReportUnread(error.to_string()))?;
        match report.unpack_error {
            Some(why) => Err(Error::PackRefused(why)),
            None => Ok(report),
        }
    }
}

/// A pack's bytes, as they go on the wire.
pub(crate) type PackStream = futures::stream::BoxStream<'static, Result<Bytes, std::io::Error>>;

/// `pack`, written on its own task while the remote reads it.
fn stream(pack: PushPack) -> PackStream {
    let (tx, rx) = tokio::sync::mpsc::channel(PACK_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let sink = crate::util::channel_sink(tx.clone(), Bytes::copy_from_slice);
        let Err(error) = pack.write(sink).await else {
            return;
        };
        tracing::error!(error = format!("{error:#}"), "writing a pack for a push");
        // An error in the body stops the request, so the remote gets no
        // complete pack.
        if tx.send(Err(std::io::Error::other(error))).await.is_err() {
            tracing::debug!("the push stopped before the pack failed");
        }
    });
    Box::pin(crate::util::received(rx))
}

/// Refuse, here, every command that would discard what the remote holds.
///
/// Git's wire protocol carries no force bit, so the client is what refuses a
/// push that loses history — and here Enroute is the client.
async fn fast_forward_only(snap: &Arc<Snapshot>, steps: Vec<Step>) -> Result<Vec<Step>, Error> {
    let mut checked = Vec::with_capacity(steps.len());
    for step in steps {
        let Step::Send(command) = step else {
            checked.push(step);
            continue;
        };
        let unguarded = command.force || command.old.is_zero() || command.deletes();
        if unguarded || query::is_ancestor(snap, command.old, command.new).await? {
            checked.push(Step::Send(command));
            continue;
        }
        checked.push(Step::Done(RefOutcome {
            destination: command.refname,
            old_id: command.old,
            new_id: command.new,
            status: PushStatus::Rejected(format!(
                "not a fast-forward: the remote holds {}, which this repository \
                 cannot show is an ancestor of {}",
                command.old, command.new
            )),
        }));
    }
    Ok(checked)
}

/// Refuse the whole push when any ref was refused here, for a caller that
/// asked for all or nothing.
///
/// Unchanged when nothing was refused: a ref the remote already holds is not
/// a refusal, and needs nothing sent for it either way.
fn hold_back(steps: Vec<Step>) -> Vec<Step> {
    let refused = steps.iter().find_map(|step| match *step {
        Step::Done(RefOutcome {
            ref destination,
            status: PushStatus::Rejected(_),
            ..
        }) => Some(destination.clone()),
        _ => None,
    });
    let Some(refused) = refused else { return steps };

    steps
        .into_iter()
        .map(|step| match step {
            Step::Send(command) => Step::Done(RefOutcome {
                destination: command.refname,
                old_id: command.old,
                new_id: command.new,
                status: PushStatus::Rejected(format!(
                    "not sent: this push lands every ref or none, and {refused} was refused"
                )),
            }),
            done @ Step::Done(_) => done,
        })
        .collect()
}

/// The outcomes of a push that sent nothing.
fn settled(steps: Vec<Step>) -> Vec<RefOutcome> {
    steps
        .into_iter()
        .filter_map(|step| match step {
            Step::Done(outcome) => Some(outcome),
            Step::Send(_) => None,
        })
        .collect()
}

/// Every step's outcome, with what the remote said about the ones it was
/// sent.
fn reported(steps: Vec<Step>, report: &report::Report) -> Vec<RefOutcome> {
    steps
        .into_iter()
        .map(|step| match step {
            Step::Done(outcome) => outcome,
            Step::Send(command) => {
                let status = match report.refs.get(&command.refname) {
                    Some(None) if command.deletes() => PushStatus::Deleted,
                    Some(None) => PushStatus::Updated,
                    Some(Some(why)) => PushStatus::Rejected(why.clone()),
                    // The remote stored the pack and then said nothing about
                    // this ref. Reported rather than assumed either way.
                    None => PushStatus::Rejected(
                        "the remote did not say what happened to this ref".into(),
                    ),
                };
                RefOutcome {
                    destination: command.refname,
                    old_id: command.old,
                    new_id: command.new,
                    status,
                }
            }
        })
        .collect()
}

/// What this push asks the remote for, naming `format` if given.
fn capabilities(atomic: bool, format: Option<ObjectFormat>) -> String {
    let mut capabilities = String::from("report-status");
    if atomic {
        capabilities.push_str(" atomic");
    }
    if let Some(format) = format {
        capabilities.push_str(" object-format=");
        capabilities.push_str(format.name());
    }
    capabilities.push_str(" agent=");
    capabilities.push_str(AGENT);
    capabilities
}

/// The commands, as the pkt-lines that open a `receive-pack` request.
fn request_head(commands: &[&Command], capabilities: &str) -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    for (index, command) in commands.iter().enumerate() {
        let mut line = format!(
            "{} {} {}",
            command.old.to_hex(),
            command.new.to_hex(),
            command.refname
        )
        .into_bytes();
        // The capability list rides on the first command and nowhere else,
        // behind a NUL, exactly as git writes it.
        if index == 0 {
            line.push(0);
            line.extend_from_slice(capabilities.as_bytes());
        }
        line.push(b'\n');
        pktline::try_data(&mut body, &line)
            .map_err(|error| Error::Protocol(format!("encoding a command: {error:#}")))?;
    }
    pktline::flush(&mut body);
    Ok(body)
}

/// Add the push's credentials to `request`. reqwest marks the header
/// sensitive, which keeps it out of anything that prints the request.
fn authorize(request: RequestBuilder, credentials: Option<&Credentials>) -> RequestBuilder {
    match credentials {
        None => request,
        Some(Credentials::Basic { username, password }) => {
            request.basic_auth(username, Some(password))
        }
        Some(Credentials::Bearer(token)) => request.bearer_auth(token),
    }
}

/// Refuse a remote that answers a discovery with a redirect.
///
/// The credentials are on the request and they are proof for the host the
/// caller named, so a hop is a URL to hand them to somebody else.
fn refuse_redirect(response: &reqwest::Response) -> Result<(), Error> {
    if !response.status().is_redirection() {
        return Ok(());
    }
    let target = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("somewhere else");
    Err(Error::Remote(format!(
        "the remote redirects to {target}; Enroute does not follow that, so \
         name that URL instead"
    )))
}

/// Refuse an answer that is not the one asked for.
fn check_status(response: &reqwest::Response) -> Result<(), Error> {
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    match status.as_u16() {
        401 | 403 => Err(Error::Unauthorized),
        404 => Err(Error::Remote(
            "the remote has no repository there, or will not admit to one".into(),
        )),
        code => Err(Error::Remote(format!("the remote answered {code}"))),
    }
}

/// Read a response body, up to `limit`.
///
/// The far end is somebody else's server, so what it sends is bounded here
/// rather than trusted to be small.
async fn read_capped(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, Error> {
    let mut body: Vec<u8> = Vec::new();
    // Bounded per read rather than over the whole answer: a large
    // advertisement is allowed to take its time, and a remote that has gone
    // quiet is not.
    while let Some(chunk) = next_chunk(&mut response).await? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(Error::Protocol(format!(
                "the remote answered with more than {limit} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The next piece of a response body, failing if the remote goes quiet for
/// `STALL_TIMEOUT`.
async fn next_chunk(response: &mut reqwest::Response) -> Result<Option<Bytes>, Error> {
    Ok(tokio::time::timeout(STALL_TIMEOUT, response.chunk())
        .await
        .map_err(|_elapsed| Error::Remote("the remote stopped answering".into()))??)
}

#[cfg(test)]
mod tests {
    use reqwest::header::AUTHORIZATION;

    use super::*;

    fn oid(byte: char) -> Oid {
        Oid::from_hex(&String::from(byte).repeat(40)).expect("hex")
    }

    fn command(refname: &str, old: Oid, new: Oid) -> Command {
        Command {
            refname: refname.to_owned(),
            old,
            new,
            force: false,
        }
    }

    #[test]
    fn the_capability_list_rides_on_the_first_command_only() {
        let first = command("refs/heads/main", Oid::ZERO, oid('a'));
        let second = command("refs/heads/wip", oid('b'), oid('c'));
        let head = request_head(&[&first, &second], "report-status").expect("encoded");
        let head = String::from_utf8(head).expect("ascii");

        assert!(head.contains("refs/heads/main\u{0}report-status\n"));
        assert!(head.contains("refs/heads/wip\n"));
        assert!(!head.contains("refs/heads/wip\u{0}"));
        assert!(head.ends_with("0000"));
    }

    #[test]
    fn atomic_is_asked_for_only_when_it_was_requested() {
        assert!(capabilities(true, None).contains("atomic"));
        assert!(!capabilities(false, None).contains("atomic"));
        assert!(capabilities(false, None).starts_with("report-status"));
    }

    #[test]
    fn credentials_are_sent_as_a_header_and_never_printed() {
        let request = authorize(
            reqwest::Client::new().get("https://example.com"),
            Some(&Credentials::Basic {
                username: "x-access-token".into(),
                password: "hunter2".into(),
            }),
        )
        .build()
        .expect("a request");
        let sent = request.headers().get(AUTHORIZATION).expect("set");

        assert!(sent.is_sensitive());
        assert_eq!(
            sent.to_str().expect("ascii"),
            "Basic eC1hY2Nlc3MtdG9rZW46aHVudGVyMg=="
        );
        assert!(!format!("{:?}", Credentials::Bearer("hunter2".into())).contains("hunter2"));
    }

    #[test]
    fn an_atomic_push_holding_a_refused_ref_sends_nothing() {
        let refused = Step::Done(RefOutcome {
            destination: "refs/heads/main".into(),
            old_id: oid('a'),
            new_id: oid('b'),
            status: PushStatus::Rejected("not a fast-forward".into()),
        });
        let steps = vec![
            refused,
            Step::Send(command("refs/heads/dev", Oid::ZERO, oid('c'))),
        ];

        let held = hold_back(steps);

        assert!(
            held.iter().all(|step| matches!(step, Step::Done(_))),
            "a command survived an atomic push that was already refused"
        );
        let Some(Step::Done(outcome)) = held.get(1) else {
            panic!("an outcome")
        };
        assert!(
            matches!(outcome.status, PushStatus::Rejected(ref why) if why.contains("refs/heads/main"))
        );
    }

    /// A ref the remote already holds is not a refusal, so it does not take
    /// the rest of an atomic push down with it.
    #[test]
    fn an_atomic_push_still_sends_what_is_only_up_to_date() {
        let steps = vec![
            Step::Done(RefOutcome {
                destination: "refs/heads/main".into(),
                old_id: oid('a'),
                new_id: oid('a'),
                status: PushStatus::UpToDate,
            }),
            Step::Send(command("refs/heads/dev", Oid::ZERO, oid('c'))),
        ];

        let held = hold_back(steps);

        assert!(matches!(held.get(1), Some(Step::Send(_))));
    }

    #[test]
    fn a_ref_the_remote_did_not_report_on_is_not_read_as_landed() {
        let steps = vec![Step::Send(command("refs/heads/main", Oid::ZERO, oid('a')))];
        let outcomes = reported(steps, &report::Report::default());

        assert!(matches!(
            outcomes.first().map(|outcome| &outcome.status),
            Some(&PushStatus::Rejected(_))
        ));
    }
}

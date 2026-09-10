use std::future::Future;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use futures::channel::mpsc;
use gix_hash::ObjectId;
use gix_packetline::Channel;
use tokio::io::BufReader;
use tokio::time::Instant;

use enroute_git_cost::Meter;
use enroute_git_ingest::{
    Actor, IngestProgress, IngestRequest, IngestWorker, ProgressSink, ReceiveHooks,
    RefUpdateOutcome, noop_progress,
};
use enroute_git_retrieve::{RefUpdate, RepoMetadata, Storage};

use crate::Error;
use crate::pack::{
    MAX_SIDEBAND, SidebandTx, send_close, send_data_frames, send_frame_copy, send_keepalive,
    send_sideband_error,
};
use crate::pktline::{Body, read_lines, trim_lf};

// ── entry point ──────────────────────────────────────────────────────────────

/// The response to a `git-receive-pack` request: a complete pkt-line body,
/// or a streamed one carrying progress frames via `side-band-64k`.
#[derive(Debug)]
pub enum ReceivePackResponse {
    /// A complete pkt-line report-status body (or empty, for the
    /// flush-only connectivity probe).
    Body(Vec<u8>),
    /// A streamed, sideband-framed report-status response.
    Streamed(mpsc::Receiver<Result<Bytes, std::io::Error>>),
}

const RECEIVE_PACK_CHANNEL_CAPACITY: usize = 4;

/// How often the streamed path polls for a progress update.
///
/// Paced for a visibly-moving meter, not the keepalive floor — coarser reads
/// as a stalled push. Stays far under a 60s proxy idle-connection timeout.
const KEEPALIVE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Handle a `git-receive-pack` request.
///
/// `body` is one of three shapes: a flush-only probe, ref-update pkt-lines
/// alone (a delete-only push), or those followed by a packfile.
///
/// # Errors
///
/// Returns an error if the request is malformed, the pack fails to parse or
/// checksum, or the object store fails.
// An unpriced operation (a flush-only probe, a push rejected before it
// reaches a worker) is absent from the span rather than present as a zero.
#[tracing::instrument(
    name = "enroute_git_proto::receive_pack::receive_pack",
    skip(state, repo, body, actor, worker, hooks),
    fields(
        repo_id = %repo.id,
        actor = %actor.id,
        cost_primary_get_class = tracing::field::Empty,
        cost_primary_put_class = tracing::field::Empty,
        cost_primary_deletes = tracing::field::Empty,
        cost_primary_bytes_read = tracing::field::Empty,
        cost_primary_bytes_written = tracing::field::Empty,
        cost_handoff_get_class = tracing::field::Empty,
        cost_handoff_put_class = tracing::field::Empty,
        cost_handoff_deletes = tracing::field::Empty,
        cost_handoff_bytes_read = tracing::field::Empty,
        cost_handoff_bytes_written = tracing::field::Empty,
        cost_lambda_invocations = tracing::field::Empty,
        cost_lambda_mb_millis = tracing::field::Empty,
    )
)]
pub async fn receive_pack<R>(
    state: Storage,
    repo: RepoMetadata,
    actor: Actor,
    worker: Arc<dyn IngestWorker>,
    hooks: Arc<dyn ReceiveHooks>,
    body: R,
) -> Result<ReceivePackResponse, Error>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut pkt_buf = BufReader::new(body);
    let (updates, side_band_64k) = parse_ref_updates(&read_lines(&mut pkt_buf).await?)?;

    // `git push` over HTTP sends a flush-only probe (no updates, no pack) to
    // check the connection before streaming the real, non-replayable pack
    // body. Real git answers it with an empty 200, not an error — match that,
    // or every push aborts before the real request is sent.
    if updates.is_empty() {
        return Ok(ReceivePackResponse::Body(Vec::new()));
    }

    // Only the refs this push touches, not the whole repo — `finalize`'s NFF
    // pre-screen only looks up `old_id` by refname for `updates`. Fetching
    // everything here used to dominate `pg_stat_statements` total_time.
    let refnames: Vec<&str> = updates.iter().map(|u| u.refname.as_str()).collect();
    let existing = state.rows.repo(repo.id).refs_matching(&refnames).await?;

    let request = IngestRequest {
        repo,
        existing,
        updates,
    };

    // Where the push's cost is reported. Captured rather than re-read at the
    // point of recording: on the streamed path that happens in a spawned task,
    // long after this function has returned.
    let cost = tracing::Span::current();
    let meter = Meter::new();

    let receiving = Receiving {
        state,
        worker,
        hooks,
        actor,
    };

    if !side_band_64k {
        // No sideband means no channel to report on, hence no progress sink —
        // and nowhere to print what `post-receive` said, which is why its
        // messages are dropped here rather than smuggled into report-status.
        // Git does the same: no sideband, no `remote:` lines.
        let (_messages, report) = run_ingest(
            &receiving,
            request,
            Box::new(pkt_buf),
            &noop_progress,
            &cost,
            &meter,
        )
        .await?;
        return Ok(ReceivePackResponse::Body(report));
    }

    let (tx, rx) = mpsc::channel(RECEIVE_PACK_CHANNEL_CAPACITY);
    crate::spawn_instrumented(run_ingest_streamed(
        receiving,
        request,
        Box::new(pkt_buf),
        tx,
        cost,
        meter,
    ));
    Ok(ReceivePackResponse::Streamed(rx))
}

/// Everything a push is received against: where its objects go, where the
/// work runs, who decides what lands, and who is pushing.
///
/// Grouped because they travel together to a spawned task on the streamed
/// path, avoiding four separate lifetimes.
struct Receiving {
    state: Storage,
    worker: Arc<dyn IngestWorker>,
    hooks: Arc<dyn ReceiveHooks>,
    actor: Actor,
}

/// Hands one push to the worker, decides what lands, and turns the outcomes
/// into a report-status body.
///
/// Two calls, answering different things: the worker stores objects, maybe in
/// another process; what lands is a `pre-receive` question for the application.
async fn run_ingest(
    receiving: &Receiving,
    request: IngestRequest,
    pack: enroute_git_ingest::PackReader,
    on_ingest_progress: ProgressSink<'_>,
    cost: &tracing::Span,
    meter: &Arc<Meter>,
) -> Result<(Vec<String>, Vec<u8>), Error> {
    // No length hint: the body is still arriving, and its `Content-Length`
    // (where there is one) covers the commands as well as the pack.
    let pack = enroute_git_ingest::IncomingPack {
        reader: pack,
        len_hint: None,
    };
    // Kept back before the request goes to the worker, which consumes it —
    // deciding what lands needs the same updates the objects were stored for.
    let repo = request.repo.clone();
    let updates = request.updates.clone();

    let ingested = receiving
        .worker
        .ingest(request, pack, on_ingest_progress, meter)
        .await;
    // Before the `?`, deliberately: a push that died late still spent
    // everything it had spent by then, and those are the expensive ones. A
    // remote worker has already folded in what it reported.
    meter.units().record_on(cost);

    let applied = enroute_git_ingest::apply_ref_updates(
        &receiving.state,
        &repo,
        &receiving.actor,
        &updates,
        ingested?,
        receiving.hooks.as_ref(),
        on_ingest_progress,
    )
    .await?;
    Ok((applied.messages, build_report_status(&applied.outcomes)?))
}

/// Build the `unpack ok`/`ok <ref>`/`ng <ref> <reason>` + flush report-status
/// body.
fn build_report_status(ref_results: &[RefUpdateOutcome]) -> Result<Vec<u8>, Error> {
    let mut out = Body::new();
    out.line(b"unpack ok\n")?;
    for outcome in ref_results {
        let line = match &outcome.result {
            Ok(()) => format!("ok {}\n", outcome.refname),
            Err(rejection) => format!("ng {} {rejection}\n", outcome.refname),
        };
        out.line(line.as_bytes())?;
    }
    out.flush();
    Ok(out.into_bytes())
}

// ── streamed (side-band-64k) path ────────────────────────────────────────────
// Negotiated responses multiplex progress, the report-status body, and
// errors over one sideband stream instead of holding the connection silent.

/// The latest stage a push has reported, or `None` while the pack arrives.
///
/// Latest-value-wins: the poll below reads it every tick, and a report the
/// next one overwrites was never on screen.
type Status = std::sync::Mutex<Option<IngestProgress>>;

/// How long a stage may sit at one value before its line starts carrying the
/// seconds it has been there.
const STALL_AFTER: Duration = Duration::from_secs(3);

/// What a stage is called on the pushing client's terminal, and what it
/// counts where it counts anything.
///
/// The one place a stage gets a display-facing shape, so two of them cannot
/// disagree about what a push is doing.
fn stage(progress: IngestProgress) -> (&'static str, Option<(u64, u64)>) {
    use IngestProgress as P;
    match progress {
        P::Dispatching => ("Contacting worker", None),
        P::ResolvingObjects { done, total } => ("Resolving objects", Some((done, total))),
        P::PreparingPacks { done, total } => ("Preparing packs", Some((done, total))),
        P::CompressingObjects { done, total } => ("Compressing objects", Some((done, total))),
        P::CheckingConnectivity => ("Checking connectivity", None),
        P::UpdatingRepository { done, total } => ("Updating repository", Some((done, total))),
        P::RecordingCommits => ("Recording commits", None),
        P::UpdatingReferences => ("Updating references", None),
    }
}

/// The text a stage shows the pushing user.
///
/// `complete` runs the counter out to its total, since the poll that notices
/// a stage finish may have missed its last few increments.
fn render_progress(progress: IngestProgress, complete: bool) -> String {
    let (label, counted) = stage(progress);
    // A zero total means the stage has not found its denominator yet, so it
    // renders as the bare name and avoids a divide by zero.
    match counted {
        None | Some((_, 0)) => format!("{label}..."),
        Some((done, total)) => {
            let done = if complete { total } else { done };
            format!("{label}: {}% ({done}/{total})", done * 100 / total)
        }
    }
}

/// Whether two reports share a terminal line, which is what makes an update
/// overdraw the last one rather than open a line of its own.
fn same_line(a: IngestProgress, b: IngestProgress) -> bool {
    std::mem::discriminant(&a) == std::mem::discriminant(&b)
}

/// Races `ingest` against a keepalive poll, sending whatever `status`
/// currently holds as a `Channel::Progress` frame on every tick.
///
/// Each stage gets one terminal line: updates overdraw it, a new stage
/// closes it and starts below. A stalled value grows a seconds counter.
async fn race_keepalive<F, T>(
    ingest: F,
    status: &Status,
    poll_interval: Duration,
    tx: &mut SidebandTx,
) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    tokio::pin!(ingest);

    let mut ticker = tokio::time::interval(poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval` fires immediately; consume that tick so the first real poll
    // is a full interval away.
    ticker.tick().await;

    // The last report drawn, if any. Its stage is the identity of the line
    // currently on screen, so a stage change is what retires that line.
    let mut drawn: Option<IngestProgress> = None;
    // When that value was first seen, which is what the stall counter counts
    // from. `tokio`'s clock, not `std`'s, so a paused-time test advances it.
    let mut since = Instant::now();
    loop {
        tokio::select! {
            result = &mut ingest => {
                // Close the line so git's "To <url>" summary doesn't land on
                // top of a `\r`-parked one. Send failures are dropped: the
                // ingest result outranks them.
                if let Some(prev) = drawn {
                    let message = format!("{}\n", render_progress(prev, true));
                    drop(send_frame_copy(Channel::Progress, message.as_bytes(), tx).await);
                }
                return result;
            }
            _ = ticker.tick() => {
                let Some(progress) = *status.lock().unwrap_or_else(PoisonError::into_inner) else {
                    // Still receiving the pack: hold the connection open
                    // without touching the terminal, leaving the client's live
                    // "Writing objects" meter alone.
                    send_keepalive(tx).await?;
                    continue;
                };

                if drawn != Some(progress) {
                    since = Instant::now();
                }

                // A new stage closes the previous line with `\n` so it stays
                // on screen and this one starts fresh below it.
                if let Some(prev) = drawn
                    && !same_line(prev, progress)
                {
                    let message = format!("{}\n", render_progress(prev, true));
                    send_frame_copy(Channel::Progress, message.as_bytes(), tx).await?;
                }

                // Trailing `\r`, where git's own meter puts it: draws the
                // whole line and parks the cursor to be overdrawn in place.
                let text = render_progress(progress, false);
                let stalled = since.elapsed();
                let message = if stalled >= STALL_AFTER {
                    format!("{text} ({}s)\r", stalled.as_secs())
                } else {
                    format!("{text}\r")
                };
                send_frame_copy(Channel::Progress, message.as_bytes(), tx).await?;
                drawn = Some(progress);
            }
        }
    }
}

/// Negotiated-path producer: runs [`run_ingest`] while [`race_keepalive`]
/// polls for progress.
///
/// Sends the report-status body, or an in-band `Channel::Error`, down `tx`.
async fn run_ingest_streamed(
    receiving: Receiving,
    request: IngestRequest,
    pack: enroute_git_ingest::PackReader,
    mut tx: SidebandTx,
    cost: tracing::Span,
    meter: Arc<Meter>,
) {
    let status = Status::new(None);
    let status = &status;
    let on_ingest_progress = move |progress: IngestProgress| {
        *status.lock().unwrap_or_else(PoisonError::into_inner) = Some(progress);
    };

    let ingest = run_ingest(
        &receiving,
        request,
        pack,
        &on_ingest_progress,
        &cost,
        &meter,
    );

    match race_keepalive(ingest, status, KEEPALIVE_POLL_INTERVAL, &mut tx).await {
        Ok((messages, report)) => {
            // Before the report body, so a client prints them above its
            // `ok <ref>` lines — where a `post-receive` hook's output goes.
            if send_messages(&messages, &mut tx).await.is_ok()
                && send_data_frames(&report, &mut tx).await.is_ok()
            {
                drop(send_close(&mut tx).await);
            }
        }
        Err(e) => send_sideband_error(&e.to_string(), &mut tx).await,
    }
}

/// Print what `post-receive` said, one progress frame per line.
///
/// An application hands over lines and this owns the framing, so a message it
/// wrote as several lines is sent as several frames rather than one long one.
async fn send_messages(messages: &[String], tx: &mut SidebandTx) -> Result<(), Error> {
    for line in messages.iter().flat_map(|message| message.lines()) {
        for chunk in format!("{line}\n").as_bytes().chunks(MAX_SIDEBAND) {
            send_frame_copy(Channel::Progress, chunk, tx).await?;
        }
    }
    Ok(())
}

// ── wire parsing ─────────────────────────────────────────────────────────────

/// Parse ref-update pkt-lines into [`RefUpdate`]s, plus whether the client
/// requested `side-band-64k` in the first line's capability list.
fn parse_ref_updates(lines: &[Vec<u8>]) -> Result<(Vec<RefUpdate>, bool), Error> {
    // A shallow-clone push prefixes the command list with one `shallow <oid>`
    // line per boundary commit. Skipped outright, before capability-stripping
    // "the first line," since shallow lines shift which line that is.
    let first_command = lines
        .iter()
        .position(|line| !trim_lf(line).starts_with(b"shallow "))
        .unwrap_or(lines.len());
    let mut updates: Vec<RefUpdate> = Vec::new();
    let mut side_band_64k = false;
    for (i, line) in lines.iter().enumerate().skip(first_command) {
        let data = trim_lf(line);
        let data = if i == first_command {
            let mut split = data.splitn(2, |b| *b == 0);
            let cmd = split.next().unwrap_or(data);
            if let Some(caps) = split.next() {
                side_band_64k = caps
                    .split(|b| *b == b' ')
                    .any(|token| token == b"side-band-64k");
            }
            cmd
        } else {
            data
        };
        let parts: Vec<&[u8]> = data.splitn(3, |b| *b == b' ').collect();
        if let (Some(old), Some(new), Some(refname)) = (parts.first(), parts.get(1), parts.get(2)) {
            let old_id = ObjectId::from_hex(old)
                .map_err(|e| Error::BadRequest(format!("invalid old-id: {e}")))?;
            let new_id = ObjectId::from_hex(new)
                .map_err(|e| Error::BadRequest(format!("invalid new-id: {e}")))?;
            updates.push(RefUpdate {
                old_id,
                new_id,
                refname: String::from_utf8_lossy(refname).into_owned(),
            });
        }
    }
    Ok((updates, side_band_64k))
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;
    use std::sync::{Arc, PoisonError};
    use std::time::Duration;

    use futures::StreamExt as _;
    use futures::channel::mpsc;

    use super::{
        Body, Error, IngestProgress, ReceivePackResponse, Status, parse_ref_updates,
        race_keepalive, receive_pack, render_progress, same_line,
    };
    use enroute_git_retrieve::RepoMetadata;
    use enroute_git_test_support::{
        debug_pktlines, extract_sideband_channel, linear_commit, make_pack, make_state,
    };

    /// A worker staging into memory of its own — these tests exercise
    /// `receive_pack`, not where the staged bytes land.
    fn local_worker(
        state: &enroute_git_retrieve::Storage,
    ) -> Arc<dyn enroute_git_ingest::IngestWorker> {
        enroute_git_ingest::LocalIngestWorker::shared(
            state.clone(),
            Arc::new(object_store::memory::InMemory::new()),
        )
    }

    /// Every stage a push can report, so a case over all of them says so.
    const STAGES: [IngestProgress; 8] = [
        IngestProgress::Dispatching,
        IngestProgress::ResolvingObjects { done: 1, total: 2 },
        IngestProgress::PreparingPacks { done: 1, total: 2 },
        IngestProgress::CompressingObjects { done: 1, total: 2 },
        IngestProgress::CheckingConnectivity,
        IngestProgress::UpdatingRepository { done: 1, total: 2 },
        IngestProgress::RecordingCommits,
        IngestProgress::UpdatingReferences,
    ];

    /// Resolution is the first stage that speaks, and it is the longest, so
    /// it carries a proportion rather than a bare label.
    #[test]
    fn resolving_objects_renders_a_proportion() {
        let stage = IngestProgress::ResolvingObjects { done: 3, total: 4 };
        assert_eq!(
            render_progress(stage, false),
            "Resolving objects: 75% (3/4)"
        );
        assert_eq!(
            render_progress(stage, true),
            "Resolving objects: 100% (4/4)"
        );
    }

    /// A zero total means the stage hasn't found its denominator yet, so it
    /// renders as the bare stage name and avoids a divide-by-zero.
    #[test]
    fn a_counted_stage_without_a_total_renders_as_a_bare_name() {
        let stage = IngestProgress::ResolvingObjects { done: 0, total: 0 };
        assert_eq!(render_progress(stage, false), "Resolving objects...");
    }

    /// Every stage owns a line no other stage uses, since two sharing one
    /// would overdraw each other — the regression this split prevents.
    #[test]
    fn every_ingest_stage_renders_under_a_name_of_its_own() {
        let mut taken: Vec<String> = Vec::new();
        for stage in STAGES {
            let text = render_progress(stage, false);
            assert!(
                !taken.contains(&text),
                "{stage:?} renders as {text:?}, which another stage already draws: {taken:?}"
            );
            assert!(
                STAGES
                    .iter()
                    .filter(|other| same_line(**other, stage))
                    .count()
                    == 1,
                "{stage:?} shares a line with another stage"
            );
            taken.push(text);
        }
    }

    // ── parse_ref_updates ────────────────────────────────────────────────────

    #[test]
    fn parse_ref_updates_single_create() {
        let line = b"0000000000000000000000000000000000000000 \
                     deadbeefdeadbeefdeadbeefdeadbeefdeadbeef refs/heads/main\n"
            .to_vec();
        let (updates, side_band_64k) = parse_ref_updates(&[line]).unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].refname, "refs/heads/main");
        assert_eq!(
            updates[0].new_id.to_hex().to_string(),
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        assert!(!side_band_64k);
    }

    #[test]
    fn parse_ref_updates_strips_capabilities_from_first_line() {
        let line = b"0000000000000000000000000000000000000000 \
                     deadbeefdeadbeefdeadbeefdeadbeefdeadbeef refs/heads/main\0report-status\n"
            .to_vec();
        let (updates, side_band_64k) = parse_ref_updates(&[line]).unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].refname, "refs/heads/main");
        assert!(!side_band_64k);
    }

    #[test]
    fn parse_ref_updates_detects_side_band_64k() {
        let line = b"0000000000000000000000000000000000000000 \
                     deadbeefdeadbeefdeadbeefdeadbeefdeadbeef refs/heads/main\0report-status side-band-64k\n"
            .to_vec();
        let (_, side_band_64k) = parse_ref_updates(&[line]).unwrap();
        assert!(side_band_64k);
    }

    #[test]
    fn parse_ref_updates_multiple_lines() {
        let lines = vec![
            b"0000000000000000000000000000000000000000 \
              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa refs/heads/main\0report-status\n"
                .to_vec(),
            b"0000000000000000000000000000000000000000 \
              bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb refs/heads/dev\n"
                .to_vec(),
        ];
        let (updates, _) = parse_ref_updates(&lines).unwrap();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].refname, "refs/heads/main");
        assert_eq!(updates[1].refname, "refs/heads/dev");
    }

    /// Regression: capability-stripping once stayed pinned to `lines[0]`,
    /// leaking into the refname parse once shallow lines shifted it to 1.
    #[test]
    fn parse_ref_updates_skips_leading_shallow_lines() {
        let lines = vec![
            b"shallow aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n".to_vec(),
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
              bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb refs/heads/main\0report-status\n"
                .to_vec(),
        ];
        let (updates, _) = parse_ref_updates(&lines).unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].refname, "refs/heads/main");
    }

    /// Multiple `shallow` lines (one per shallow-boundary commit) are all
    /// skipped, not just a single leading one.
    #[test]
    fn parse_ref_updates_skips_multiple_leading_shallow_lines() {
        let lines = vec![
            b"shallow aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n".to_vec(),
            b"shallow bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n".to_vec(),
            b"cccccccccccccccccccccccccccccccccccccccc \
              dddddddddddddddddddddddddddddddddddddddd refs/heads/main\0report-status\n"
                .to_vec(),
        ];
        let (updates, _) = parse_ref_updates(&lines).unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].refname, "refs/heads/main");
    }

    #[test]
    fn parse_ref_updates_invalid_sha_returns_error() {
        let line = b"not-a-sha deadbeefdeadbeefdeadbeefdeadbeefdeadbeef refs/heads/main\n".to_vec();
        parse_ref_updates(&[line]).unwrap_err();
    }

    #[test]
    fn parse_ref_updates_malformed_line_silently_skipped() {
        let line = b"no-spaces-at-all\n".to_vec();
        let (updates, _) = parse_ref_updates(&[line]).unwrap();
        assert!(updates.is_empty());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    fn receive_pack_body(updates: &[(&str, &str, &str)], pack: &[u8]) -> Vec<u8> {
        receive_pack_body_with_caps(updates, "report-status", pack)
    }

    fn receive_pack_body_with_caps(
        updates: &[(&str, &str, &str)],
        caps: &str,
        pack: &[u8],
    ) -> Vec<u8> {
        let mut head = Body::new();
        for (i, (old_id, new_id, refname)) in updates.iter().enumerate() {
            let line = if i == 0 {
                format!("{old_id} {new_id} {refname}\0{caps}\n")
            } else {
                format!("{old_id} {new_id} {refname}\n")
            };
            head.line(line.as_bytes()).unwrap();
        }
        head.flush();
        let mut body = head.into_bytes();
        body.extend_from_slice(pack);
        body
    }

    async fn do_receive_pack(
        state: enroute_git_retrieve::Storage,
        repo: &RepoMetadata,
        body: Vec<u8>,
    ) -> String {
        let worker = local_worker(&state);
        let response = receive_pack(
            state,
            repo.clone(),
            super::Actor::new("alice"),
            worker,
            Arc::new(enroute_git_ingest::NoHooks),
            Cursor::new(body),
        )
        .await
        .unwrap();
        let ReceivePackResponse::Body(out) = response else {
            panic!("expected a non-streamed response: no side-band-64k was negotiated");
        };
        String::from_utf8_lossy(&out).into_owned()
    }

    #[tokio::test]
    async fn receive_pack_rejects_bogus_object_count() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let zeros = "0000000000000000000000000000000000000000";
        let missing = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

        // A pack header claiming far more objects than the (empty) body actually
        // contains: this must be rejected before any preallocation happens.
        let mut pack = Vec::new();
        pack.extend_from_slice(b"PACK");
        pack.extend_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&u32::MAX.to_be_bytes());
        let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
        h.update(&pack);
        pack.extend_from_slice(h.try_finalize().unwrap().as_slice());

        let body = receive_pack_body(&[(zeros, missing, "refs/heads/main")], &pack);
        let worker = local_worker(&state);
        let err = receive_pack(
            state,
            repo,
            super::Actor::new("alice"),
            worker,
            Arc::new(enroute_git_ingest::NoHooks),
            Cursor::new(body),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("max"), "{err}");
    }

    #[tokio::test]
    async fn receive_pack_accepts_fully_connected_push() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let zeros = "0000000000000000000000000000000000000000";

        let commit = linear_commit(b"hello\n", None, 0, "init");
        let raw = do_receive_pack(
            state,
            &repo,
            receive_pack_body(
                &[(zeros, &commit.commit_sha, "refs/heads/main")],
                &make_pack(&commit.pack_entries()),
            ),
        )
        .await;
        insta::assert_snapshot!(debug_pktlines(raw.as_bytes()), @"
        unpack ok
        ok refs/heads/main
        [flush]
        ");
    }

    /// The push path's half of the cost accounting, needing the same guard
    /// as `upload_pack` against its field list drifting from this one.
    #[tokio::test]
    async fn a_push_records_every_cost_field_on_its_span() {
        // Held across the awaits below so the span `receive_pack` opens is
        // created against this subscriber rather than a global one.
        let (recorded, _guard) = enroute_git_test_support::capture_recorded_fields();

        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let zeros = "0000000000000000000000000000000000000000";
        let commit = linear_commit(b"hello\n", None, 0, "init");

        let raw = do_receive_pack(
            state,
            &repo,
            receive_pack_body(
                &[(zeros, &commit.commit_sha, "refs/heads/main")],
                &make_pack(&commit.pack_entries()),
            ),
        )
        .await;
        assert!(raw.contains("ok refs/heads/main"), "{raw}");

        recorded.assert_all_recorded(&enroute_git_cost::FIELD_NAMES);

        // A push that landed wrote its objects to the primary store, so a zero
        // would mean the meter never reached the code that spends.
        assert!(
            recorded
                .get(enroute_git_cost::PRIMARY_PUT_CLASS)
                .unwrap_or(0)
                > 0,
            "a push that landed wrote nothing",
        );
        assert!(
            recorded
                .get(enroute_git_cost::PRIMARY_BYTES_WRITTEN)
                .unwrap_or(0)
                > 0,
            "a push that landed stored no bytes",
        );
        // `LocalIngestWorker` runs the push here rather than in a Lambda, and
        // its session scratch is deliberately unmetered — see
        // `enroute_git_cost::StoreRole::Handoff`.
        assert_eq!(recorded.get(enroute_git_cost::LAMBDA_INVOCATIONS), Some(0));
        assert_eq!(recorded.get(enroute_git_cost::HANDOFF_PUT_CLASS), Some(0));
    }

    #[tokio::test]
    async fn receive_pack_accepts_a_delete_only_push_with_no_pack() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let zeros = "0000000000000000000000000000000000000000";

        let commit = linear_commit(b"hello\n", None, 0, "init");
        do_receive_pack(
            state.clone(),
            &repo,
            receive_pack_body(
                &[(zeros, &commit.commit_sha, "refs/heads/doomed")],
                &make_pack(&commit.pack_entries()),
            ),
        )
        .await;

        let raw = do_receive_pack(
            state.clone(),
            &repo,
            receive_pack_body(&[(&commit.commit_sha, zeros, "refs/heads/doomed")], &[]),
        )
        .await;
        insta::assert_snapshot!(debug_pktlines(raw.as_bytes()), @"
        unpack ok
        ok refs/heads/doomed
        [flush]
        ");

        let refs = state.rows.repo(repo.id).refs_for(&repo).await.unwrap();
        assert!(!refs.contains_key("refs/heads/doomed"));
    }

    #[tokio::test]
    async fn receive_pack_rejects_funny_head_refname_without_blocking_other_refs() {
        // Matches real git's behavior: a funny-refname rejection is per-ref,
        // not a whole-request failure — other refs in the same push still
        // land normally.
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let zeros = "0000000000000000000000000000000000000000";

        let commit = linear_commit(b"hello\n", None, 0, "init");
        let raw = do_receive_pack(
            state.clone(),
            &repo,
            receive_pack_body(
                &[
                    (zeros, commit.commit_sha.as_str(), "HEAD"),
                    (zeros, commit.commit_sha.as_str(), "refs/heads/main"),
                ],
                &make_pack(&commit.pack_entries()),
            ),
        )
        .await;
        insta::assert_snapshot!(debug_pktlines(raw.as_bytes()), @"
        unpack ok
        ng HEAD funny refname
        ok refs/heads/main
        [flush]
        ");

        let refs = state.rows.repo(repo.id).refs_for(&repo).await.unwrap();
        assert_eq!(
            refs.get("HEAD").map(String::as_str),
            Some("ref: refs/heads/main")
        );
        assert_eq!(
            refs.get("refs/heads/main").map(String::as_str),
            Some(commit.commit_sha.as_str())
        );
    }

    // ── streamed (side-band-64k) path ───────────────────────────────────────

    /// A status slot already holding `stage`, shared with the polling task.
    fn reporting(stage: Option<IngestProgress>) -> Arc<Status> {
        Arc::new(Status::new(stage))
    }

    /// What the ingest side does when it reaches a stage.
    fn report(status: &Status, stage: IngestProgress) {
        *status.lock().unwrap_or_else(PoisonError::into_inner) = Some(stage);
    }

    /// A trivial `ingest` future for [`race_keepalive`] tests: resolves to
    /// `result` after `duration`, with no real pack/store/DB involved.
    async fn sleep_then_ok(duration: Duration, result: Vec<u8>) -> Result<Vec<u8>, Error> {
        tokio::time::sleep(duration).await;
        Ok(result)
    }

    /// The text of a sideband frame, asserting it went out on the progress
    /// channel — no test wants to read one without checking that.
    fn progress_payload(frame: &bytes::Bytes) -> String {
        assert_eq!(frame[4], 2, "expected a Channel::Progress frame");
        String::from_utf8(frame[5..].to_vec()).unwrap()
    }

    /// Each stage owns one terminal line: updates end in `\r` to overdraw
    /// it, and a new stage first closes the old one with `\n`.
    ///
    /// The closing line renders complete, since a poll can miss the last
    /// increments and a line frozen at "50%" would read as a stall.
    #[tokio::test(start_paused = true)]
    async fn race_keepalive_gives_each_stage_its_own_line() {
        let status = reporting(Some(IngestProgress::UpdatingRepository {
            done: 1,
            total: 4,
        }));
        let status_for_task = Arc::clone(&status);
        let (mut tx, mut rx) = mpsc::channel(8);

        let handle = tokio::spawn(async move {
            let ingest = sleep_then_ok(Duration::from_secs(45), b"done".to_vec());
            race_keepalive(ingest, &status_for_task, Duration::from_secs(10), &mut tx).await
        });

        let payload = |frame: bytes::Bytes| progress_payload(&frame);

        tokio::time::advance(Duration::from_secs(11)).await;
        assert_eq!(
            payload(rx.next().await.unwrap().unwrap()),
            "Updating repository: 25% (1/4)\r"
        );

        // Same stage, live cell update: redrawn in place, no new line.
        report(
            &status,
            IngestProgress::UpdatingRepository { done: 3, total: 4 },
        );
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(
            payload(rx.next().await.unwrap().unwrap()),
            "Updating repository: 75% (3/4)\r"
        );

        // New stage: the old line is closed off at 100% — never the 75% the
        // last poll happened to catch — and the new one starts below it.
        report(&status, IngestProgress::UpdatingReferences);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(
            payload(rx.next().await.unwrap().unwrap()),
            "Updating repository: 100% (4/4)\n"
        );
        assert_eq!(
            payload(rx.next().await.unwrap().unwrap()),
            "Updating references...\r"
        );

        // Once `ingest` resolves, its result wins over any further polling.
        tokio::time::advance(Duration::from_secs(20)).await;
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, b"done");
    }

    /// A stage that reports nothing new — the commit-graph write, a slow
    /// store read — leaves a line indistinguishable from a dead connection.
    ///
    /// Past [`super::STALL_AFTER`] it grows a seconds counter so something
    /// on screen keeps moving; a stage that does advance never shows one.
    #[tokio::test(start_paused = true)]
    async fn race_keepalive_counts_seconds_on_a_line_that_stops_moving() {
        let status = reporting(Some(IngestProgress::RecordingCommits));
        let status_for_task = Arc::clone(&status);
        let (mut tx, mut rx) = mpsc::channel(8);

        let handle = tokio::spawn(async move {
            let ingest = sleep_then_ok(Duration::from_secs(45), b"done".to_vec());
            race_keepalive(ingest, &status_for_task, Duration::from_secs(4), &mut tx).await
        });

        let payload = |frame: bytes::Bytes| progress_payload(&frame);

        // The first draw of a value is never stalled, however long the push
        // has already been running.
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(
            payload(rx.next().await.unwrap().unwrap()),
            "Recording commits...\r"
        );

        // Same value, now past the threshold, and counting.
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(
            payload(rx.next().await.unwrap().unwrap()),
            "Recording commits... (4s)\r"
        );
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(
            payload(rx.next().await.unwrap().unwrap()),
            "Recording commits... (8s)\r"
        );

        // A new value restarts the count, so a stage that moves stays clean.
        report(&status, IngestProgress::UpdatingReferences);
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(
            payload(rx.next().await.unwrap().unwrap()),
            "Recording commits...\n"
        );
        assert_eq!(
            payload(rx.next().await.unwrap().unwrap()),
            "Updating references...\r"
        );

        tokio::time::advance(Duration::from_secs(40)).await;
        assert_eq!(handle.await.unwrap().unwrap(), b"done");
    }

    /// The client's sideband demuxer forks before `pack_objects`, so
    /// anything sent mid-upload would blink against its own meter.
    ///
    /// So until a stage is reported, ticks must emit only git's no-op
    /// keepalive (an empty `Data` frame), never a `Progress` one.
    #[tokio::test(start_paused = true)]
    async fn race_keepalive_stays_off_the_terminal_while_the_pack_arrives() {
        let status = reporting(None);
        let status_for_task = Arc::clone(&status);
        let (mut tx, mut rx) = mpsc::channel(4);

        let handle = tokio::spawn(async move {
            let ingest = sleep_then_ok(Duration::from_secs(45), b"done".to_vec());
            race_keepalive(ingest, &status_for_task, Duration::from_secs(10), &mut tx).await
        });

        tokio::time::advance(Duration::from_secs(11)).await;
        let frame = rx.next().await.unwrap().unwrap();
        assert_eq!(
            &frame[..],
            b"0005\x01",
            "pre-stage ticks must be git's empty-Data keepalive, which draws nothing"
        );

        // The meter starts as soon as the pipeline reports its first stage.
        report(&status, IngestProgress::CheckingConnectivity);
        tokio::time::advance(Duration::from_secs(10)).await;
        let frame = rx.next().await.unwrap().unwrap();
        assert_eq!(frame[4], 2, "expected a Channel::Progress frame");

        tokio::time::advance(Duration::from_secs(34)).await;
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, b"done");
    }

    /// Regression test for where the `\r` sits: git's sideband demuxer
    /// flushes a progress line only on `\r`/`\n`.
    ///
    /// Text-then-`\r` draws a complete line; `\r`-then-text flushes a bare
    /// "remote: " and strands the text until the following frame.
    #[tokio::test(start_paused = true)]
    async fn race_keepalive_progress_frames_are_carriage_return_terminated() {
        let status = reporting(Some(IngestProgress::CheckingConnectivity));
        let status_for_task = Arc::clone(&status);
        let (mut tx, mut rx) = mpsc::channel(4);

        let handle = tokio::spawn(async move {
            let ingest = sleep_then_ok(Duration::from_secs(45), b"done".to_vec());
            race_keepalive(ingest, &status_for_task, Duration::from_secs(10), &mut tx).await
        });

        tokio::time::advance(Duration::from_secs(11)).await;
        let frame = rx.next().await.unwrap().unwrap();
        let payload = String::from_utf8(frame[5..].to_vec()).unwrap();
        assert_eq!(
            payload, "Checking connectivity...\r",
            "the whole line must precede the \\r, or git draws only \"remote: \""
        );

        // Ingest resolving terminates the line with `\n` so git's "To <url>"
        // summary doesn't overwrite the last update.
        tokio::time::advance(Duration::from_secs(34)).await;
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, b"done");

        let mut last = None;
        while let Some(frame) = rx.next().await {
            last = Some(frame.unwrap());
        }
        let last = last.expect("expected a final progress frame");
        assert!(
            String::from_utf8_lossy(&last[5..]).ends_with("...\n"),
            "final progress frame must end the line with \\n"
        );
    }

    #[tokio::test]
    async fn receive_pack_streams_sideband_framed_report_status_when_negotiated() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let zeros = "0000000000000000000000000000000000000000";

        let commit = linear_commit(b"hello\n", None, 0, "init");
        let body = receive_pack_body_with_caps(
            &[(zeros, &commit.commit_sha, "refs/heads/main")],
            "report-status side-band-64k",
            &make_pack(&commit.pack_entries()),
        );

        let worker = local_worker(&state);
        let response = receive_pack(
            state,
            repo,
            super::Actor::new("alice"),
            worker,
            Arc::new(enroute_git_ingest::NoHooks),
            Cursor::new(body),
        )
        .await
        .unwrap();
        let ReceivePackResponse::Streamed(rx) = response else {
            panic!("expected a streamed response: side-band-64k was negotiated");
        };

        let frames: Vec<bytes::Bytes> = rx.map(Result::unwrap).collect().await;
        let all: Vec<u8> = frames.concat();
        let data = extract_sideband_channel(&all, 1 /* Channel::Data */);
        insta::assert_snapshot!(debug_pktlines(&data), @"
        unpack ok
        ok refs/heads/main
        [flush]
        ");
    }
}

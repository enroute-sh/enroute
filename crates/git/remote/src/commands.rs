//! What each refspec asks the remote to do, worked out before anything is
//! sent.
//!
//! A spec becomes a command or an outcome: a ref the remote already holds at
//! the value asked for is answered here, and never reaches the wire.

use std::collections::{HashMap, HashSet};

use gix_hash::ObjectId;

use crate::advertise::Advertisement;
use crate::{Error, PushStatus, RefOutcome, RefSpec};

/// The all-zero id, which is how git says "no value" on both sides of a
/// command.
pub(crate) fn null() -> ObjectId {
    ObjectId::null(gix_hash::Kind::Sha1)
}

/// One `<old> <new> <ref>` for the remote to apply.
#[derive(Debug, Clone)]
pub(crate) struct Command {
    pub(crate) refname: String,
    pub(crate) old: ObjectId,
    pub(crate) new: ObjectId,
    /// Whether the caller allowed this to discard what the remote holds.
    pub(crate) force: bool,
}

impl Command {
    pub(crate) fn deletes(&self) -> bool {
        self.new == null()
    }
}

/// What one refspec turned into.
#[derive(Debug)]
pub(crate) enum Step {
    /// Still to be sent, once the fast-forward check has run.
    Send(Command),
    /// Answered without asking the remote.
    Done(RefOutcome),
}

/// Turn every refspec into a command or an outcome, in the caller's order.
///
/// # Errors
///
/// A refspec that names no ref, names one twice, or names something git
/// would not accept as a refname.
pub(crate) fn plan(
    specs: &[RefSpec],
    local: &HashMap<String, ObjectId>,
    advertisement: &Advertisement,
) -> Result<Vec<Step>, Error> {
    let mut destinations: HashSet<&str> = HashSet::new();
    let mut steps = Vec::with_capacity(specs.len());

    for spec in specs {
        let destination = destination(spec)?;
        if !destinations.insert(destination) {
            return Err(Error::Request(format!(
                "{destination} is named by more than one refspec"
            )));
        }
        steps.push(step(spec, destination, local, advertisement));
    }
    Ok(steps)
}

/// The ref this spec moves on the remote.
fn destination(spec: &RefSpec) -> Result<&str, Error> {
    let destination = if spec.destination.is_empty() {
        spec.source.as_str()
    } else {
        spec.destination.as_str()
    };
    if destination.is_empty() {
        return Err(Error::Request("a refspec names no ref".into()));
    }
    // Fully qualified, because this side has no branch namespace to guess
    // with: whether `main` means a branch or a tag is the remote's to say,
    // and a guess would silently push onto the wrong one.
    if !destination.starts_with("refs/")
        || destination
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte == 0)
    {
        return Err(Error::Request(format!(
            "{destination} is not a fully qualified refname"
        )));
    }
    Ok(destination)
}

fn step(
    spec: &RefSpec,
    destination: &str,
    local: &HashMap<String, ObjectId>,
    advertisement: &Advertisement,
) -> Step {
    let old = advertisement
        .refs
        .get(destination)
        .copied()
        .unwrap_or_else(null);

    let new = if spec.source.is_empty() {
        null()
    } else {
        match local.get(&spec.source) {
            Some(oid) => *oid,
            None => {
                return Step::Done(RefOutcome {
                    destination: destination.to_owned(),
                    old_id: old,
                    new_id: null(),
                    status: PushStatus::Rejected(format!(
                        "{} is not a ref this repository holds",
                        spec.source
                    )),
                });
            }
        }
    };

    if old == new {
        return Step::Done(RefOutcome {
            destination: destination.to_owned(),
            old_id: old,
            new_id: new,
            status: PushStatus::UpToDate,
        });
    }

    Step::Send(Command {
        refname: destination.to_owned(),
        old,
        new,
        force: spec.force,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(byte: char) -> ObjectId {
        ObjectId::from_hex(String::from(byte).repeat(40).as_bytes()).expect("hex")
    }

    fn spec(source: &str, destination: &str) -> RefSpec {
        RefSpec {
            source: source.to_owned(),
            destination: destination.to_owned(),
            force: false,
        }
    }

    fn advertisement(refs: &[(&str, ObjectId)]) -> Advertisement {
        Advertisement {
            refs: refs
                .iter()
                .map(|(name, oid)| ((*name).to_owned(), *oid))
                .collect(),
            ..Advertisement::default()
        }
    }

    fn local(refs: &[(&str, ObjectId)]) -> HashMap<String, ObjectId> {
        refs.iter()
            .map(|(name, oid)| ((*name).to_owned(), *oid))
            .collect()
    }

    #[test]
    fn a_ref_the_remote_already_holds_is_never_sent() {
        let here = local(&[("refs/heads/main", oid('a'))]);
        let there = advertisement(&[("refs/heads/main", oid('a'))]);

        let steps = plan(&[spec("refs/heads/main", "")], &here, &there).expect("planned");
        assert!(matches!(
            steps.first(),
            Some(Step::Done(RefOutcome {
                status: PushStatus::UpToDate,
                ..
            }))
        ));
    }

    #[test]
    fn a_ref_this_repository_does_not_hold_is_refused_here() {
        let steps = plan(
            &[spec("refs/heads/gone", "")],
            &local(&[]),
            &advertisement(&[]),
        )
        .expect("planned");
        assert!(matches!(
            steps.first(),
            Some(Step::Done(RefOutcome {
                status: PushStatus::Rejected(_),
                ..
            }))
        ));
    }

    #[test]
    fn an_empty_source_deletes_the_destination() {
        let there = advertisement(&[("refs/heads/wip", oid('b'))]);
        let steps = plan(&[spec("", "refs/heads/wip")], &local(&[]), &there).expect("planned");

        let Some(Step::Send(command)) = steps.first() else {
            panic!("a command")
        };
        assert!(command.deletes());
        assert_eq!(command.old, oid('b'));
    }

    #[test]
    fn deleting_a_ref_the_remote_does_not_have_is_already_done() {
        let steps = plan(
            &[spec("", "refs/heads/wip")],
            &local(&[]),
            &advertisement(&[]),
        )
        .expect("planned");
        assert!(matches!(
            steps.first(),
            Some(Step::Done(RefOutcome {
                status: PushStatus::UpToDate,
                ..
            }))
        ));
    }

    #[test]
    fn a_source_is_pushed_under_the_destination_name() {
        let here = local(&[("refs/heads/main", oid('a'))]);
        let steps = plan(
            &[spec("refs/heads/main", "refs/heads/trunk")],
            &here,
            &advertisement(&[]),
        )
        .expect("planned");

        let Some(Step::Send(command)) = steps.first() else {
            panic!("a command")
        };
        assert_eq!(command.refname, "refs/heads/trunk");
        assert_eq!(command.old, null());
        assert_eq!(command.new, oid('a'));
    }

    #[test]
    fn refuses_a_refspec_that_is_not_fully_qualified() {
        plan(&[spec("main", "")], &local(&[]), &advertisement(&[])).unwrap_err();
        plan(&[spec("", "")], &local(&[]), &advertisement(&[])).unwrap_err();
    }

    #[test]
    fn refuses_two_refspecs_that_move_one_ref() {
        let here = local(&[("refs/heads/main", oid('a')), ("refs/heads/wip", oid('b'))]);
        let specs = [
            spec("refs/heads/main", "refs/heads/trunk"),
            spec("refs/heads/wip", "refs/heads/trunk"),
        ];
        plan(&specs, &here, &advertisement(&[])).unwrap_err();
    }
}

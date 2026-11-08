//! What each refspec asks the remote to do, worked out before anything is
//! sent.
//!
//! A spec becomes a command or an outcome: a ref the remote already holds at
//! the value asked for is answered here, and never reaches the wire.

use std::collections::HashSet;

use crate::git::oid::{ObjectFormat, Oid};
use crate::git::refname;
use crate::meta::Map;

use super::advertise::Advertisement;
use super::{Error, PushStatus, RefOutcome, RefSpec};

/// One `<old> <new> <ref>` for the remote to apply.
///
/// Git spells "no value" on either side as the all-zero id.
#[derive(Debug, Clone)]
pub(super) struct Command {
    pub(super) refname: String,
    pub(super) old: Oid,
    pub(super) new: Oid,
    /// Whether the caller allowed this to discard what the remote holds.
    pub(super) force: bool,
}

impl Command {
    pub(super) fn deletes(&self) -> bool {
        self.new.is_zero()
    }
}

/// What one refspec turned into.
#[derive(Debug)]
pub(super) enum Step {
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
pub(super) fn plan(
    format: ObjectFormat,
    specs: &[RefSpec],
    local: &Map<Oid>,
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
        steps.push(step(format, spec, destination, local, advertisement));
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
    // `Delete` checks the format alone: which names the remote takes is the
    // remote's to say.
    if !refname::valid(destination, refname::Mode::Delete) {
        return Err(Error::Request(format!(
            "{destination} is not a fully qualified refname"
        )));
    }
    Ok(destination)
}

/// The step of `spec`, whose ids are of `format`.
fn step(
    format: ObjectFormat,
    spec: &RefSpec,
    destination: &str,
    local: &Map<Oid>,
    advertisement: &Advertisement,
) -> Step {
    let old = advertisement
        .refs
        .get(destination)
        .copied()
        .unwrap_or(format.zero());

    let new = if spec.source.is_empty() {
        format.zero()
    } else {
        match local.get(&spec.source) {
            Some(oid) => *oid,
            None => {
                return Step::Done(RefOutcome {
                    destination: destination.to_owned(),
                    old_id: old,
                    new_id: format.zero(),
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

    fn oid(byte: char) -> Oid {
        Oid::from_hex(&String::from(byte).repeat(40)).expect("hex")
    }

    fn spec(source: &str, destination: &str) -> RefSpec {
        RefSpec {
            source: source.to_owned(),
            destination: destination.to_owned(),
            force: false,
        }
    }

    fn advertisement(refs: &[(&str, Oid)]) -> Advertisement {
        Advertisement {
            refs: refs
                .iter()
                .map(|(name, oid)| ((*name).to_owned(), *oid))
                .collect(),
            ..Advertisement::default()
        }
    }

    fn local(refs: &[(&str, Oid)]) -> Map<Oid> {
        refs.iter()
            .map(|(name, oid)| ((*name).to_owned(), *oid))
            .collect()
    }

    #[test]
    fn a_ref_the_remote_already_holds_is_never_sent() {
        let here = local(&[("refs/heads/main", oid('a'))]);
        let there = advertisement(&[("refs/heads/main", oid('a'))]);

        let steps = plan(
            ObjectFormat::Sha1,
            &[spec("refs/heads/main", "")],
            &here,
            &there,
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
    fn a_ref_this_repository_does_not_hold_is_refused_here() {
        let steps = plan(
            ObjectFormat::Sha1,
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
        let steps = plan(
            ObjectFormat::Sha1,
            &[spec("", "refs/heads/wip")],
            &local(&[]),
            &there,
        )
        .expect("planned");

        let Some(Step::Send(command)) = steps.first() else {
            panic!("a command")
        };
        assert!(command.deletes());
        assert_eq!(command.old, oid('b'));
    }

    #[test]
    fn deleting_a_ref_the_remote_does_not_have_is_already_done() {
        let steps = plan(
            ObjectFormat::Sha1,
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
            ObjectFormat::Sha1,
            &[spec("refs/heads/main", "refs/heads/trunk")],
            &here,
            &advertisement(&[]),
        )
        .expect("planned");

        let Some(Step::Send(command)) = steps.first() else {
            panic!("a command")
        };
        assert_eq!(command.refname, "refs/heads/trunk");
        assert_eq!(command.old, Oid::ZERO);
        assert_eq!(command.new, oid('a'));
    }

    #[test]
    fn refuses_a_refspec_that_is_not_fully_qualified() {
        plan(
            ObjectFormat::Sha1,
            &[spec("main", "")],
            &local(&[]),
            &advertisement(&[]),
        )
        .unwrap_err();
        plan(
            ObjectFormat::Sha1,
            &[spec("", "")],
            &local(&[]),
            &advertisement(&[]),
        )
        .unwrap_err();
        plan(
            ObjectFormat::Sha1,
            &[spec("", "refs/heads/a..b")],
            &local(&[]),
            &advertisement(&[]),
        )
        .unwrap_err();
    }

    #[test]
    fn refuses_two_refspecs_that_move_one_ref() {
        let here = local(&[("refs/heads/main", oid('a')), ("refs/heads/wip", oid('b'))]);
        let specs = [
            spec("refs/heads/main", "refs/heads/trunk"),
            spec("refs/heads/wip", "refs/heads/trunk"),
        ];
        plan(ObjectFormat::Sha1, &specs, &here, &advertisement(&[])).unwrap_err();
    }
}

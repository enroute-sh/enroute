//! Between the wire form and the in-process types.
//!
//! What is here is what does not mirror one for one: an arm that is renamed,
//! dropped or built out of more than the other side holds. The rest is
//! declared once by `wire`'s `mirror!`, which generates the same pair.
//! `to_wire` matches exhaustively on the `git` types — do not add a
//! catch-all arm, since that is what stops this module compiling when a
//! variant is added. `from_wire` cannot be exhaustive, the sender possibly
//! being a version ahead, so it degrades per `Unknown` instead.

#[cfg(feature = "server")]
use std::time::Duration;

#[cfg(feature = "server")]
use enroute_git_core::Error;
#[cfg(feature = "client")]
use enroute_git_cost::LambdaUnits;
use enroute_git_cost::Units;
use enroute_git_ingest::{IngestRequest, Ingested, PushRejection};
#[cfg(feature = "server")]
use enroute_git_retrieve::{RefUpdate, RepoMetadata};

use crate::wire;

// ── git → wire ─────────────────────────────────────────────────────────────

/// Exhaustive over [`PushRejection`] by design — see the module docs.
#[must_use]
#[cfg(feature = "server")]
pub(crate) fn rejection_to_wire(rejection: PushRejection) -> wire::Rejection {
    match rejection {
        PushRejection::MissingObjects => wire::Rejection::MissingObjects,
        PushRejection::FunnyRefname => wire::Rejection::FunnyRefname,
        PushRejection::NonCommitObject => wire::Rejection::NonCommitObject,
        PushRejection::Policy(reason) => wire::Rejection::Policy { reason },
        PushRejection::RefStore(kind) => wire::Rejection::RefStore {
            kind: wire::RefStoreRejection::from_git(kind),
        },
        PushRejection::Internal => wire::Rejection::Internal,
    }
}

/// What the worker established, as it reports it.
#[must_use]
#[cfg(feature = "server")]
pub(crate) fn ingested_to_wire(ingested: Ingested) -> wire::Ingested {
    wire::Ingested {
        rejected: ingested
            .rejected
            .into_iter()
            .map(|(refname, rejection)| wire::Refused {
                refname,
                rejection: rejection_to_wire(rejection),
            })
            .collect(),
        screened: ingested.screened,
    }
}

/// What the worker spent, for the [`wire::Frame::Done`] it reports it in.
///
/// `units.lambda` is deliberately dropped: an invocation is counted by
/// whoever made it, not by the worker reporting it.
#[must_use]
#[cfg(feature = "server")]
pub(crate) fn cost_to_wire(units: Units, memory_mb: u64, elapsed: Duration) -> wire::Cost {
    // `lambda` deliberately dropped, not ignored with `..`: destructuring is
    // what makes a new field on `Units` a compile error here.
    let Units {
        primary,
        handoff,
        lambda: _,
    } = units;
    wire::Cost {
        primary: wire::StoreCost::from_git(primary),
        handoff: wire::StoreCost::from_git(handoff),
        memory_mb,
        duration_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
    }
}

/// Build the call for a push whose pack has already been delivered as `pack`.
///
/// By value: the caller is done with it, and a push with many refs would
/// otherwise be copied wholesale just to be dropped.
#[must_use]
#[cfg(feature = "client")]
pub(crate) fn request_to_wire(request: IngestRequest, pack: wire::Pack) -> wire::Request {
    wire::Request {
        repo: wire::Repo {
            id: request.repo.id.as_i64(),
            storage_key: request.repo.storage_key.to_string(),
            default_branch: request.repo.default_branch,
        },
        existing: request.existing,
        updates: request
            .updates
            .into_iter()
            .map(|u| wire::RefUpdate {
                refname: u.refname,
                old_id: u.old_id.to_hex().to_string(),
                new_id: u.new_id.to_hex().to_string(),
            })
            .collect(),
        pack,
    }
}

// ── wire → git ─────────────────────────────────────────────────────────────

/// What a pushing client sees for a rejection this build can't classify.
///
/// `PushRejection` has no "refused, reason unknown" variant, so `Policy`
/// stands in — its `Display` is the bare reason, imprecise but correct.
#[must_use]
#[cfg(feature = "client")]
pub(crate) fn opaque_rejection(detail: &str) -> PushRejection {
    PushRejection::Policy(format!("rejected by ingest worker ({detail})"))
}

/// The nearest in-process rejection, degrading unnameable ones per
/// [`opaque_rejection`].
#[must_use]
#[cfg(feature = "client")]
pub(crate) fn rejection_from_wire(rejection: wire::Rejection) -> PushRejection {
    match rejection {
        wire::Rejection::MissingObjects => PushRejection::MissingObjects,
        wire::Rejection::FunnyRefname => PushRejection::FunnyRefname,
        wire::Rejection::NonCommitObject => PushRejection::NonCommitObject,
        wire::Rejection::Policy { reason } => PushRejection::Policy(reason),
        wire::Rejection::RefStore { kind } => match kind.to_git() {
            Some(kind) => PushRejection::RefStore(kind),
            None => opaque_rejection("unrecognised ref-store refusal"),
        },
        wire::Rejection::Internal => PushRejection::Internal,
        wire::Rejection::Unknown => opaque_rejection("unrecognised reason"),
    }
}

/// What the worker established, as the front door needs it to decide what
/// lands.
#[must_use]
#[cfg(feature = "client")]
pub(crate) fn ingested_from_wire(ingested: wire::Ingested) -> Ingested {
    Ingested {
        rejected: ingested
            .rejected
            .into_iter()
            .map(|refused| (refused.refname, rejection_from_wire(refused.rejection)))
            .collect(),
        screened: ingested.screened,
    }
}

/// What the worker reported spending, as units the caller can add to its own.
///
/// The invocation is counted here, on the side that made the call, so a
/// retried or duplicated frame cannot inflate the count.
#[must_use]
#[cfg(feature = "client")]
pub(crate) fn cost_from_wire(cost: wire::Cost) -> Units {
    Units {
        primary: cost.primary.to_git(),
        handoff: cost.handoff.to_git(),
        lambda: LambdaUnits {
            invocations: 1,
            mb_millis: cost.memory_mb.saturating_mul(cost.duration_ms),
        },
    }
}

/// Rebuild the in-process request, catching malformed input: hex that isn't an
/// object id, or a storage key that isn't a UUID.
///
/// # Errors
/// Returns an error if any object id or the storage key fails to parse.
#[cfg(feature = "server")]
pub(crate) fn request_from_wire(request: wire::Request) -> Result<IngestRequest, Error> {
    let storage_key: uuid::Uuid = request
        .repo
        .storage_key
        .parse()
        .map_err(|e| Error::Invalid(format!("storage_key: {e}")))?;

    let updates = request
        .updates
        .into_iter()
        .map(|u| {
            Ok(RefUpdate {
                old_id: parse_oid(&u.old_id)?,
                new_id: parse_oid(&u.new_id)?,
                refname: u.refname,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(IngestRequest {
        repo: RepoMetadata {
            id: enroute_git_core::RepoId::new(request.repo.id),
            storage_key: storage_key.into(),
            default_branch: request.repo.default_branch,
        },
        existing: request.existing,
        updates,
    })
}

#[cfg(feature = "server")]
fn parse_oid(hex: &str) -> Result<gix_hash::ObjectId, Error> {
    gix_hash::ObjectId::from_hex(hex.as_bytes())
        .map_err(|e| Error::Invalid(format!("object id {hex}: {e}")))
}

#[cfg(all(test, feature = "client", feature = "server"))]
mod tests {
    use enroute_git_ingest::IngestProgress;
    use enroute_git_retrieve::RefUpdateRejection;

    use super::*;

    /// The wording must survive the round trip — it reaches the client's
    /// terminal.
    #[test]
    fn rejections_round_trip_with_their_wording() {
        let cases = [
            PushRejection::MissingObjects,
            PushRejection::FunnyRefname,
            PushRejection::NonCommitObject,
            PushRejection::Policy("no force push".to_string()),
            PushRejection::RefStore(RefUpdateRejection::NonFastForward),
            PushRejection::RefStore(RefUpdateRejection::UnknownCommit),
            PushRejection::RefStore(RefUpdateRejection::AlreadyExists),
            PushRejection::RefStore(RefUpdateRejection::InvalidRefname),
            PushRejection::Internal,
        ];
        for original in cases {
            let want = original.to_string();
            let back = rejection_from_wire(rejection_to_wire(original.clone()));
            assert_eq!(back, original, "variant changed for {want}");
            assert_eq!(back.to_string(), want, "wording changed");
        }
    }

    #[test]
    fn progress_round_trips() {
        let cases = [
            IngestProgress::Dispatching,
            IngestProgress::ResolvingObjects { done: 3, total: 9 },
            IngestProgress::PreparingPacks { done: 2, total: 7 },
            IngestProgress::CompressingObjects { done: 4, total: 5 },
            IngestProgress::CheckingConnectivity,
            IngestProgress::UpdatingRepository { done: 1, total: 2 },
            IngestProgress::RecordingCommits,
            IngestProgress::UpdatingReferences,
        ];
        for original in cases {
            assert_eq!(wire::Progress::from_git(original).to_git(), Some(original));
        }
    }

    /// A worker one version ahead must not fail the push: an unknown stage
    /// is skipped, an unknown rejection still rejects.
    #[test]
    fn unknown_variants_degrade_rather_than_fail() {
        assert_eq!(wire::Progress::Unknown.to_git(), None);

        let rejected = rejection_from_wire(wire::Rejection::Unknown);
        assert!(
            rejected.to_string().contains("ingest worker"),
            "should name its source: {rejected}"
        );

        let refstore = rejection_from_wire(wire::Rejection::RefStore {
            kind: wire::RefStoreRejection::Unknown,
        });
        assert!(refstore.to_string().contains("ref-store"), "{refstore}");
    }

    /// A refusal keeps its refname and reason, and a screened ref stays
    /// screened rather than becoming one or the other.
    #[test]
    fn ingested_round_trips() {
        let original = Ingested {
            rejected: [
                ("refs/heads/main".to_string(), PushRejection::MissingObjects),
                (
                    "refs/heads/wip".to_string(),
                    PushRejection::Policy("needs a review".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
            screened: vec!["refs/heads/old".to_string()],
        };

        let back = ingested_from_wire(ingested_to_wire(original.clone()));

        assert_eq!(back.screened, original.screened);
        assert_eq!(back.rejected.len(), original.rejected.len());
        for (refname, rejection) in &original.rejected {
            assert_eq!(
                back.rejected.get(refname).map(ToString::to_string),
                Some(rejection.to_string()),
                "{refname}"
            );
        }
    }
}

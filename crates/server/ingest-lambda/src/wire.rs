//! The protocol between the front door and the ingest worker.
//!
//! Deliberately duplicates `enroute-git-ingest`'s and `enroute-git-metadata`'s
//! types: were the protocol those types, a change to `PushRejection` or
//! `IngestProgress` would silently be a protocol change. Owning the wire form
//! instead makes [`super::convert`] stop compiling on such a change. Every
//! enum a worker *sends* has an `Unknown` fallback, and structs tolerate
//! unknown fields, for when the two ends are already different versions.
//! [`Push`], [`Pack`] and [`Call`]'s two stores are the exceptions — a worker
//! that cannot tell where the push is, or which bucket it is for, cannot run
//! it — so the two ends must move together, which only a pinned qualifier
//! makes possible. The stores carry no default deliberately: one would let an
//! older worker ignore them and fall back to its own environment, which is the
//! disagreement they exist to prevent, made invisible.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use enroute_config::StoreUri;
use enroute_git_cost::StoreUnits;
use enroute_git_ingest::IngestProgress;
use enroute_git_retrieve::RefUpdateRejection;

/// Declares a wire type that mirrors a `git` one, with the conversions.
///
/// The list is written once, so the two forms cannot drift, and `from_git`
/// stays exhaustive — which is what stops a new variant compiling silently.
macro_rules! mirror {
    (
        $(#[$outer:meta])*
        enum $name:ident = $git:ident {
            $(
                $(#[$variant_doc:meta])*
                $variant:ident $({ $($(#[$field_doc:meta])* $field:ident: $ty:ty),* $(,)? })?,
            )*
        }
    ) => {
        $(#[$outer])*
        pub enum $name {
            $(
                $(#[$variant_doc])*
                $variant $({ $($(#[$field_doc])* $field: $ty),* })?,
            )*
            /// One this build has no name for.
            ///
            /// Read as nothing rather than refused, so a worker one version
            /// ahead loses only the wording.
            #[serde(other)]
            Unknown,
        }

        impl $name {
            /// The wire form of `value`.
            #[must_use]
            #[cfg(feature = "server")]
            pub(crate) fn from_git(value: $git) -> Self {
                match value {
                    $($git::$variant $({ $($field),* })? => Self::$variant $({ $($field),* })?,)*
                }
            }

            /// `None` for one this build cannot name.
            #[must_use]
            #[cfg(feature = "client")]
            pub(crate) fn to_git(self) -> Option<$git> {
                match self {
                    $(Self::$variant $({ $($field),* })? => Some($git::$variant $({ $($field),* })?),)*
                    Self::Unknown => None,
                }
            }
        }
    };
    (
        $(#[$outer:meta])*
        struct $name:ident = $git:ident {
            $($(#[$field_doc:meta])* $field:ident: $ty:ty,)*
        }
    ) => {
        $(#[$outer])*
        pub struct $name {
            $($(#[$field_doc])* pub $field: $ty,)*
        }

        impl $name {
            /// The wire form of `value`.
            #[must_use]
            #[cfg(feature = "server")]
            pub(crate) fn from_git(value: $git) -> Self {
                let $git { $($field),* } = value;
                Self { $($field),* }
            }

            /// The in-process form, which holds exactly the same counters.
            #[must_use]
            #[cfg(feature = "client")]
            pub(crate) fn to_git(self) -> $git {
                let Self { $($field),* } = self;
                $git { $($field),* }
            }
        }
    };
}

/// One invocation: a push, and what the worker needs before it can read one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Call {
    /// W3C `traceparent`, so a push is one trace rather than two.
    ///
    /// Out here rather than on [`Request`]: the worker opens its span before
    /// it can fetch a [`Push::Staged`] one.
    #[serde(default)]
    pub traceparent: Option<String>,
    /// Where the repository's permanent objects go.
    ///
    /// Sent rather than configured at both ends, because the two must be the
    /// same bucket and nothing was checking that they were.
    pub objects: StoreUri,
    /// The bucket a staged push and a staged pack are read back from.
    pub staging: StoreUri,
    /// The push, or where to read it.
    pub push: Push,
}

/// How the push reaches the worker.
///
/// A push scales with the refs it touches while the payload quota does not,
/// so staging keeps a too-large push sendable rather than unrunnable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Push {
    /// In the call itself.
    Inline(Request),
    /// Encoded into the staging bucket, for a push the call cannot hold.
    Staged(Staged),
}

/// Where something too large for the call was left instead.
///
/// One type for the pack and the push alike: whatever it holds, it is a key
/// and a length, and both ends read it back the same way.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Staged {
    /// Key within the bucket [`Call::staging`] names.
    pub key: String,
    /// Length in bytes, so a short read is caught as itself rather than as a
    /// truncated pack or malformed JSON.
    pub len: u64,
}

/// One push, as handed to a worker that isn't in this process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    /// The repository, as the front door resolved it.
    ///
    /// Passed rather than re-resolved: `MetadataStore` has no lookup by id.
    pub repo: Repo,
    /// Current values of the refs this push touches.
    ///
    /// Passed so both ends pre-screen against the same values; the ref
    /// store re-validates under its own transaction regardless.
    pub existing: BTreeMap<String, String>,
    /// The requested updates, in the client's order — which is the order
    /// outcomes come back in.
    pub updates: Vec<RefUpdate>,
    /// The pack, or where to find it.
    pub pack: Pack,
}

/// A repository, flattened to primitives so the protocol doesn't inherit
/// `RepoMetadata`'s representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Repo {
    /// `repositories.id`.
    pub id: i64,
    /// The random key object storage is laid out under, as a UUID string.
    pub storage_key: String,
    /// What `HEAD` resolves to, e.g. `refs/heads/main`.
    pub default_branch: String,
}

/// One requested ref update.
///
/// Object ids cross as hex rather than through a `gix-hash` serde feature,
/// so the protocol doesn't inherit that type's representation either.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefUpdate {
    /// The ref being updated, e.g. `refs/heads/main`.
    pub refname: String,
    /// The value `refname` must currently have, or all-zeroes for "no current
    /// value required".
    pub old_id: String,
    /// The value to set it to, or all-zeroes to delete.
    pub new_id: String,
}

/// How the pack reaches the worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pack {
    /// In the staging bucket, streamed there and streamed back.
    Staged(Staged),
    /// In the call itself, sparing the bucket a write and a read.
    ///
    /// Bounded well below the payload quota by the front door — see
    /// `client::INLINE_MAX`.
    Inline {
        /// Base64 because the payload is JSON, which has no byte string.
        ///
        /// Its own length is the pack's, so there is no `len` to disagree with.
        #[serde(with = "base64_bytes")]
        bytes: Bytes,
    },
}

/// Bytes as a base64 string.
///
/// `serde_bytes` would be the obvious alternative, but it encodes to a JSON
/// array of numbers — several bytes per byte.
mod base64_bytes {
    use std::fmt;

    use base64::Engine as _;
    use base64::display::Base64Display;
    use base64::engine::general_purpose::STANDARD;
    use bytes::Bytes;
    use serde::{Deserializer, Serializer, de};

    /// Into the serializer's own buffer: `collect_str` streams a `Display`
    /// through, escaping as it goes, rather than copying a finished string.
    pub(super) fn serialize<S: Serializer>(
        bytes: &Bytes,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&Base64Display::new(bytes, &STANDARD))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Bytes, D::Error> {
        deserializer.deserialize_str(Base64Visitor)
    }

    /// Decodes where the parser found it: base64 has no character JSON
    /// escapes, so `serde_json` hands over a slice of the payload itself.
    struct Base64Visitor;

    impl de::Visitor<'_> for Base64Visitor {
        type Value = Bytes;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a base64 string")
        }

        fn visit_str<E: de::Error>(self, text: &str) -> Result<Bytes, E> {
            STANDARD.decode(text).map(Bytes::from).map_err(E::custom)
        }
    }
}

#[cfg(test)]
mod pack_tests {
    use super::*;

    /// An inline pack is arbitrary binary, and every byte of it has to survive
    /// a trip through JSON.
    #[test]
    fn an_inline_pack_round_trips_every_byte() {
        let bytes: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let pack = Pack::Inline {
            bytes: Bytes::from(bytes.clone()),
        };

        let json = serde_json::to_string(&pack).expect("encoding");
        let Pack::Inline { bytes: back } = serde_json::from_str(&json).expect("decoding") else {
            panic!("variant changed across the wire");
        };
        assert_eq!(back.as_ref(), bytes.as_slice());
    }

    /// A JSON array of numbers would cost several bytes per byte, which is the
    /// difference between fitting Lambda's payload quota and not.
    #[test]
    fn an_inline_pack_encodes_as_one_base64_string() {
        let json = serde_json::to_value(Pack::Inline {
            bytes: Bytes::from_static(&[0xff, 0x00, 0xfe]),
        })
        .expect("encoding");

        assert_eq!(json["inline"]["bytes"], "/wD+");
    }
}

/// One line of a worker's response, newline-delimited.
///
/// A stream, not a single reply, because progress has to reach the pushing
/// client while the work is still running.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Frame {
    /// A stage transition, or a counter within one.
    Progress(Progress),
    /// The objects are stored and the commit graph is written.
    ///
    /// Not a verdict on the push: no ref has moved, and whether one may is
    /// decided by the front door, the side that can reach the application.
    Done {
        /// Updates this ingestion already refused, and those it skipped
        /// without deciding.
        ///
        /// Everything else is still live.
        ingested: Ingested,
        /// What the invocation consumed, or `None` from a worker built
        /// before this existed.
        ///
        /// Optional rather than required so a front door and a worker at
        /// different versions still agree about the *push*.
        #[serde(default)]
        cost: Option<Cost>,
    },
    /// The push failed outright, as distinct from individual refs being
    /// rejected — which is what `Done` carries.
    Failed {
        /// Rendered for the pushing client; nothing structured is worth
        /// preserving, since this ends the push either way.
        message: String,
        /// What the invocation consumed before it failed.
        ///
        /// Carried here as well as on `Done` because a push that dies late
        /// has already spent everything it was going to.
        #[serde(default)]
        cost: Option<Cost>,
    },
    /// A frame this build doesn't know.
    ///
    /// Skipped: a worker one version ahead must not fail an otherwise fine
    /// push.
    #[serde(other)]
    Unknown,
}

/// What one invocation consumed, as only the worker can account for it.
///
/// Mirrors `enroute_git_cost::Units` minus the invocation itself, which is
/// the caller's to count — the one part that happened on its side of the wire.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Cost {
    /// Requests against the permanent object store.
    pub primary: StoreCost,
    /// Requests against the handoff bucket: reading what the front door left
    /// there, and nothing else.
    pub handoff: StoreCost,
    /// The function's configured memory in MB, which is what Lambda bills
    /// duration against.
    ///
    /// Reported rather than assumed: a memory change is a console or
    /// Terraform edit no front-door build would see.
    pub memory_mb: u64,
    /// How long the invocation had been running when it reported this.
    ///
    /// Measured from the handler being entered, so a cold start's boot is
    /// included, as Lambda bills it.
    pub duration_ms: u64,
}

mirror! {
    /// One store's billable requests and bytes.
    #[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
    struct StoreCost = StoreUnits {
        /// GET and HEAD requests.
        get_class: u64,
        /// PUT, COPY and LIST requests.
        put_class: u64,
        /// DELETE requests.
        deletes: u64,
        /// Bytes in response bodies.
        bytes_read: u64,
        /// Bytes in request bodies.
        bytes_written: u64,
    }
}

mirror! {
    /// A stage of the push.
    ///
    /// A `total` of zero means the stage has started without yet knowing its
    /// denominator, not that there is nothing to do — render the name alone.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    #[serde(tag = "stage", rename_all = "snake_case")]
    enum Progress = IngestProgress {
        /// Delivering the push to the worker and starting it.
        ///
        /// Sent by the front door, not by a worker.
        Dispatching,
        /// Materializing pack entries into objects.
        ResolvingObjects {
            /// Entries resolved so far.
            done: u64,
            /// Entries in the pack.
            total: u64,
        },
        /// Diffing each pushed commit against its first parent.
        PreparingPacks {
            /// Commits whose pack contents are settled.
            done: u64,
            /// Commits in the push.
            total: u64,
        },
        /// Encoding and staging the versions the plan calls for.
        CompressingObjects {
            /// Encodings staged so far.
            done: u64,
            /// Encodings the plan calls for.
            total: u64,
        },
        /// Walking pushed refs' ancestry for reachability.
        CheckingConnectivity,
        /// Promoting staged objects to the primary store.
        UpdatingRepository {
            /// Commit packs uploaded so far.
            done: u64,
            /// Connected commits to promote.
            total: u64,
        },
        /// Writing the commit graph, then running the repository's
        /// `pre-receive` hook.
        RecordingCommits,
        /// Applying ref updates.
        UpdatingReferences,
    }
}

/// What ingesting a push established about its updates, mirroring
/// `enroute_git_ingest::Ingested`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Ingested {
    /// Updates already refused, and why.
    #[serde(default)]
    pub rejected: Vec<Refused>,
    /// Refnames kept out of storage as non-fast-forward, still to be
    /// re-checked when the refs are applied — not refusals.
    #[serde(default)]
    pub screened: Vec<String>,
}

/// One update this ingestion refused.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refused {
    /// The ref this is about.
    pub refname: String,
    /// Why it will not land.
    pub rejection: Rejection,
}

/// Why a ref update didn't land, mirroring `enroute_git_ingest::PushRejection`.
#[derive(Debug, Clone, Serialize, Deserialize)]
// Tagged "rejection" rather than the more natural "reason", which would collide
// with `Policy`'s own field of that name.
#[serde(tag = "rejection", rename_all = "snake_case")]
pub enum Rejection {
    /// Something reachable from the new tip is missing.
    MissingObjects,
    /// Fails git's funny-refname check.
    FunnyRefname,
    /// A branch update pointing at a known non-commit.
    NonCommitObject,
    /// Refused by the repository's `pre-receive` hook.
    Policy {
        /// The denial reason, shown to the pushing client.
        reason: String,
    },
    /// The ref store's own compare-and-set refused it.
    RefStore {
        /// Which refusal.
        kind: RefStoreRejection,
    },
    /// No result was recorded — an invariant violation, not a real reason.
    Internal,
    /// A reason this build has no name for.
    ///
    /// Still rejects the ref — the worker is authoritative on that; only
    /// the wording is lost.
    #[serde(other)]
    Unknown,
}

mirror! {
    /// The ref store's refusals.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum RefStoreRejection = RefUpdateRejection {
        /// `old_id` didn't match the ref's current value.
        NonFastForward,
        /// `new_id` isn't a recorded commit — branches only.
        UnknownCommit,
        /// An unguarded create found the ref already existed.
        AlreadyExists,
        /// Not a refname git itself would accept.
        InvalidRefname,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One call, as the front door writes it.
    fn call() -> serde_json::Value {
        serde_json::json!({
            "objects": "s3://objects/prefix",
            "staging": "s3://handoff?s3_express=true",
            "push": { "staged": { "key": "packs/one", "len": 12 } },
        })
    }

    /// The whole point of sending them.
    ///
    /// A worker too old to know these fields must fail to read the call, not
    /// fall back to a bucket the front door does not serve.
    #[test]
    fn a_call_without_its_stores_is_refused() {
        for missing in ["objects", "staging"] {
            let mut without = call();
            drop(
                without
                    .as_object_mut()
                    .expect("an object")
                    .remove(missing)
                    .expect("the field was there"),
            );
            let error = serde_json::from_value::<Call>(without)
                .expect_err("a call missing a store it needs")
                .to_string();
            assert!(error.contains(missing), "{error}");
        }
    }

    /// The options are the difference between one bucket and another, so they
    /// have to survive the trip.
    #[test]
    fn the_stores_a_call_names_arrive_as_they_were_sent() {
        let call: Call = serde_json::from_value(call()).expect("a call");
        let there_and_back: Call =
            serde_json::from_str(&serde_json::to_string(&call).expect("encodes")).expect("decodes");

        assert_eq!(there_and_back.objects.to_uri(), "s3://objects/prefix");
        assert_eq!(
            there_and_back.staging.to_uri(),
            "s3://handoff?s3_express=true"
        );
    }

    /// A traceparent is not a store: a front door that sends none still has
    /// its push run, which is what `default` is for and these are not.
    #[test]
    fn a_call_without_a_traceparent_is_read() {
        let call: Call = serde_json::from_value(call()).expect("a call");
        assert!(call.traceparent.is_none());
    }

    /// The shapes both ends already speak, pinned against a declaration that
    /// is generated rather than written out.
    #[test]
    fn the_mirrored_types_keep_their_json_shape() {
        assert_eq!(
            serde_json::to_value(Progress::ResolvingObjects { done: 3, total: 9 })
                .expect("encoding"),
            serde_json::json!({ "stage": "resolving_objects", "done": 3, "total": 9 })
        );
        assert_eq!(
            serde_json::to_value(Progress::RecordingCommits).expect("encoding"),
            serde_json::json!({ "stage": "recording_commits" })
        );
        assert_eq!(
            serde_json::to_value(RefStoreRejection::NonFastForward).expect("encoding"),
            serde_json::json!("non_fast_forward")
        );
        assert_eq!(
            serde_json::to_value(StoreCost {
                get_class: 1,
                put_class: 2,
                deletes: 3,
                bytes_read: 4,
                bytes_written: 5,
            })
            .expect("encoding"),
            serde_json::json!({
                "get_class": 1,
                "put_class": 2,
                "deletes": 3,
                "bytes_read": 4,
                "bytes_written": 5,
            })
        );
    }

    /// A worker one version ahead must not fail the frame it sends: what this
    /// build cannot name is read as unknown.
    #[test]
    fn a_variant_this_build_cannot_name_reads_as_unknown() {
        let progress: Progress =
            serde_json::from_value(serde_json::json!({ "stage": "polishing" })).expect("decoding");
        assert!(matches!(progress, Progress::Unknown));

        let refusal: RefStoreRejection =
            serde_json::from_value(serde_json::json!("sideways")).expect("decoding");
        assert!(matches!(refusal, RefStoreRejection::Unknown));
    }
}

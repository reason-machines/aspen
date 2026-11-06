//! Errors, and how each one reaches a Git client.
//!
//! Every error a request can end in maps, through [`ToWire::to_wire`], to
//! one [`WireError`]: where it appears ([`Surface`]), the text the client
//! sees, whether retrying the same request can succeed, and whether it is an
//! alert (an error metric that pages) rather than only a log line. The
//! protocol layer renders a `WireError`; it never invents its own text.

use crate::id::{ObjectId, PackId};

/// Where a client sees an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    /// An `ng <ref> <message>` line in a push's report-status, repeated on
    /// side-band 2 even under `quiet` (S2).
    PushRef,
    /// The `unpack <message>` status of a push: the whole pack is refused,
    /// and every ref gets `ng <ref> unpacker error`.
    PushUnpack,
    /// An `ERR <message>` pkt-line on a fetch or ls-refs; clients show it
    /// verbatim (S5).
    FetchErr,
    /// A plain HTTP status before any Git exchange starts.
    Http(u16),
}

/// An error as the client sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    /// Where it appears.
    pub surface: Surface,
    /// The text. Never contains secrets, blob keys or internal addresses.
    pub message: String,
    /// Whether the same request, retried unchanged, can succeed.
    pub retryable: bool,
    /// Whether the event pages an operator: stored data broke an invariant
    /// that a retry cannot heal.
    pub alert: bool,
}

/// Maps an error to what the client sees.
pub trait ToWire {
    /// The wire form of this error.
    fn to_wire(&self) -> WireError;
}

/// Why one ref of a push is rejected (`ng` line). The rules that produce
/// each reason are in [`crate::refs::CommitCheck`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectReason {
    /// The ref is outside the repository's writable namespaces.
    NotWritable,
    /// The principal may not write this ref.
    PermissionDenied,
    /// A `-o lease=<ref>:<oid>` does not match the ref's current value.
    LeaseMismatch,
    /// Delete of a ref that does not exist, naming an old value it
    /// cannot have. Without an old value such a delete is a no-op.
    /// Stock `git receive-pack` differs for an old value naming an object
    /// it does not have: it accepts that delete as a no-op with a warning.
    /// Only a raw-protocol client sends one; stock `git push` never does.
    NoSuchRef,
    /// Trunk can never be deleted.
    TrunkDelete,
    /// The client's old value is not the ref's current value.
    StaleOld,
    /// A delete with a zero old value and no `-o force`.
    DeleteNeedsForce,
    /// A force was requested where the repository's `allow_force` forbids it.
    ForceNotAllowed,
    /// A zero old value for a ref that was advertised to the client.
    RefExists,
    /// Replacing an existing tag without `-o force`.
    TagExists,
    /// A non-fast-forward without an explicit force.
    NonFastForward,
    /// Ancestry could not be decided yet (history still being indexed).
    AncestryUndecided,
    /// The compare-and-swap lost after the bounded recompute.
    CasLost,
    /// Trunk was rewound between the push's lookup and its commit.
    TrunkRewritten,
    /// Another command of an `--atomic` push was rejected.
    AtomicPeerRejected,
    /// The pack was refused, so no ref was updated.
    UnpackFailed,
    /// A branch (`refs/heads/*`) would point at an object that is not a
    /// commit, which stock `git receive-pack` refuses too.
    NotACommit,
    /// Mirror mode, proxy push-through (node C4): the upstream repository
    /// refused the update. The status line carries the upstream's reason.
    UpstreamRejected,
    /// Mirror mode, proxy push-through: the upstream repository could not
    /// be reached or did not answer in time.
    UpstreamUnavailable,
    /// Mirror mode, proxy push-through: the upstream's value of the ref is
    /// not the one Aspen holds (the mirror is behind, or the ref was
    /// changed at the upstream directly), so the update was not forwarded.
    UpstreamAhead,
    /// The repository's push rules (a pre-receive hook the server was
    /// configured with) refused the update. The hook's own reason replaces
    /// this text on the `ng` line when it gives one.
    PushRule,
}

impl RejectReason {
    /// Every reason, for exhaustive tests.
    pub const ALL: [RejectReason; 21] = [
        Self::NotWritable,
        Self::PermissionDenied,
        Self::LeaseMismatch,
        Self::NoSuchRef,
        Self::TrunkDelete,
        Self::StaleOld,
        Self::DeleteNeedsForce,
        Self::ForceNotAllowed,
        Self::RefExists,
        Self::TagExists,
        Self::NonFastForward,
        Self::AncestryUndecided,
        Self::CasLost,
        Self::TrunkRewritten,
        Self::AtomicPeerRejected,
        Self::UnpackFailed,
        Self::NotACommit,
        Self::UpstreamRejected,
        Self::UpstreamUnavailable,
        Self::UpstreamAhead,
        Self::PushRule,
    ];

    /// The `ng` reason text.
    ///
    /// The texts for a lost race ([`Self::StaleOld`], [`Self::CasLost`],
    /// [`Self::NonFastForward`], [`Self::RefExists`]) contain one of the
    /// phrases the differential oracle recognises as a race loss
    /// (`aspen_testkit::RACE_LOSS_REASONS`).
    pub const fn message(self) -> &'static str {
        match self {
            Self::NotWritable => {
                "refusing to update a ref outside this repository's writable namespaces"
            }
            Self::PermissionDenied => "permission denied",
            Self::LeaseMismatch => "stale info: the lease does not match the ref's current value",
            Self::NoSuchRef => "no such ref",
            Self::TrunkDelete => "refusing to delete the trunk branch",
            Self::StaleOld => "incorrect old value: the ref moved (fetch first)",
            Self::DeleteNeedsForce => {
                "deleting a ref this client has not seen needs -o force, -o lease=<ref>:<oid>, or the branch-scoped URL <repo>.git/-/<ref>"
            }
            Self::ForceNotAllowed => {
                "force is not allowed for this ref by the repository's allow_force policy"
            }
            Self::RefExists => "reference already exists",
            Self::TagExists => "tag already exists (push with -o force to replace it)",
            Self::NonFastForward => "non-fast-forward (fetch and rebase, or push with -o force)",
            Self::AncestryUndecided => {
                "cannot decide ancestry yet; history is still being indexed, retry"
            }
            Self::CasLost => "failed to update ref: it moved during the push, retry",
            Self::TrunkRewritten => "trunk was rewritten during your push; retry",
            Self::AtomicPeerRejected => "atomic push failed",
            Self::UnpackFailed => "unpacker error",
            Self::NotACommit => "refusing to point a branch at an object that is not a commit",
            Self::UpstreamRejected => "rejected by the upstream repository this mirror forwards to",
            Self::UpstreamUnavailable => "the upstream repository could not be reached; retry",
            Self::UpstreamAhead => {
                "the upstream repository has moved past this mirror; retry once the mirror has synced"
            }
            Self::PushRule => "refused by this repository's push rules",
        }
    }

    /// Whether retrying the same push unchanged can succeed.
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::AncestryUndecided
                | Self::CasLost
                | Self::TrunkRewritten
                | Self::UpstreamUnavailable
                | Self::UpstreamAhead
        )
    }
}

impl ToWire for RejectReason {
    fn to_wire(&self) -> WireError {
        WireError {
            surface: Surface::PushRef,
            message: self.message().to_owned(),
            retryable: self.retryable(),
            alert: false,
        }
    }
}

/// A failure of a metadata store, blob store or object lookup.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// A row or blob that should exist does not.
    #[error("not found: {what}")]
    NotFound {
        /// What was looked up.
        what: String,
    },
    /// A uniqueness rule was violated (for example a repository name taken,
    /// or a pack checksum registered with different sizes).
    #[error("conflict: {what}")]
    Conflict {
        /// What conflicted.
        what: String,
    },
    /// The backend is temporarily unreachable or overloaded.
    #[error("unavailable: {reason}")]
    Unavailable {
        /// Detail for logs.
        reason: String,
    },
    /// A local file could not be read or written.
    #[error("local I/O: {reason}")]
    LocalIo {
        /// Detail for logs.
        reason: String,
    },
    /// Stored data breaks an invariant (for example an `.idx` that does not
    /// parse, or a `packs` row whose blob is missing).
    #[error("corrupt: {what}")]
    Corrupt {
        /// What is corrupt.
        what: String,
    },
}

/// How every [`StoreError::pack_swept`] reason starts.
const PACK_SWEPT: &str = "pack garbage-collected after its grace period: ";

/// How every [`StoreError::lease_lost`] reason starts.
const LEASE_LOST: &str = "lease no longer held: ";

impl StoreError {
    /// Whether the store error means stored data is wrong, which pages.
    pub fn is_corruption(&self) -> bool {
        matches!(self, Self::Corrupt { .. } | Self::NotFound { .. })
    }

    /// A writer named a pack it registered, but garbage collection deleted
    /// the row first: the writer took longer than the grace period
    /// (`aspen_compact::CompactConfig::gc_grace`) from registering to
    /// committing. [`StoreError::Unavailable`], so the writer retries and
    /// registers the pack anew; [`StoreError::is_pack_swept`] tells it
    /// apart, so callers can log and count it (issue #203).
    pub fn pack_swept(detail: impl std::fmt::Display) -> Self {
        Self::Unavailable {
            reason: format!("{PACK_SWEPT}{detail}"),
        }
    }

    /// Whether this is a [`StoreError::pack_swept`] refusal.
    pub fn is_pack_swept(&self) -> bool {
        matches!(self, Self::Unavailable { reason } if reason.starts_with(PACK_SWEPT))
    }

    /// Work done under a [`crate::meta::Lease`] was refused because the
    /// lease expired or another owner took it: nothing was written.
    /// [`StoreError::Conflict`], so it is not retried as is; the caller
    /// stops and acquires the lease again later.
    /// [`StoreError::is_lease_lost`] tells it apart.
    pub fn lease_lost(detail: impl std::fmt::Display) -> Self {
        Self::Conflict {
            what: format!("{LEASE_LOST}{detail}"),
        }
    }

    /// Whether this is a [`StoreError::lease_lost`] refusal.
    pub fn is_lease_lost(&self) -> bool {
        matches!(self, Self::Conflict { what } if what.starts_with(LEASE_LOST))
    }

    fn wire(&self, surface: Surface) -> WireError {
        WireError {
            surface,
            message: "internal storage error; retry".to_owned(),
            retryable: true,
            alert: self.is_corruption(),
        }
    }
}

/// Why a pushed pack is refused (`unpack <message>`). No `packs` row is
/// written for a refused pack.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnpackError {
    /// The pack header's object count is above the limit; refused before
    /// the body is read (S4).
    #[error("pack has {count} objects, above the limit of {limit}")]
    TooManyObjects {
        /// Count from the header.
        count: u32,
        /// The limit.
        limit: u32,
    },
    /// The pack is larger than the limit.
    #[error("pack is larger than the limit of {limit} bytes")]
    PackTooLarge {
        /// The limit.
        limit: u64,
    },
    /// The pack does not parse: bad header, CRC32, zlib stream, trailer,
    /// delta, or a SHA-1 collision attack.
    #[error("malformed pack: {reason}")]
    Malformed {
        /// What is wrong.
        reason: String,
    },
    /// A REF_DELTA base is neither in the pack nor in the push's
    /// dependencies.
    #[error("missing delta base {id}")]
    MissingBase {
        /// The base.
        id: ObjectId,
    },
    /// The client's pack holds the same object twice (S4). Stock
    /// `receive-pack` accepts this without `--strict`; Aspen does not,
    /// because stored packs never repeat an object.
    #[error("object {id} appears twice in the pack")]
    DuplicateObject {
        /// The object.
        id: ObjectId,
    },
    /// The connectivity check found an object the pack references that is
    /// in neither the pack nor its dependencies.
    #[error("missing object {id} referenced by the pack")]
    MissingObject {
        /// The object.
        id: ObjectId,
    },
    /// Completing the pack needs more memory for delta bases at once than
    /// one push may hold (`aspen_pack::locate::LocateOptions::max_prefetch_bytes`).
    /// Nothing is wrong with stored data, and the same push fails the same
    /// way, so it is not retryable and does not page.
    #[error(
        "the pack's delta bases need more than {budget} bytes of memory at once; push fewer large changes at a time"
    )]
    BasesOverBudget {
        /// The per-push budget in bytes.
        budget: u64,
    },
    /// The push builds on commits its refs do not depend on (with
    /// `push.negotiate`, Git leaves out every commit the server
    /// acknowledged, whichever ref it is on), and what its writes would copy
    /// from those commits' versions into their own packs is more than one
    /// push may bring (its byte limit, the caller's quota included). Without
    /// `push.negotiate` the client sends those objects itself, compressed,
    /// under the same limit.
    #[error(
        "this push builds on commits of other refs whose objects take more than {limit} bytes to copy; push it with `git -c push.negotiate=false push`"
    )]
    LeftOutTooLarge {
        /// The push's byte limit.
        limit: u64,
    },
    /// A store failed during ingest.
    #[error("storage: {0}")]
    Store(StoreError),
}

impl ToWire for UnpackError {
    fn to_wire(&self) -> WireError {
        match self {
            Self::Store(e) => e.wire(Surface::PushUnpack),
            other => WireError {
                surface: Surface::PushUnpack,
                message: other.to_string(),
                retryable: false,
                alert: false,
            },
        }
    }
}

/// A protocol feature Aspen does not serve, answered with a clear `ERR`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsupported {
    /// Partial-clone `filter` (never advertised, S5).
    Filter,
    /// A shallow request to a repository with `shallow = refuse`.
    Shallow,
    /// A want that is not a commit or tag Aspen can resolve, such as the
    /// blob wants of a lazy fetch from a partial clone (S5).
    NonCommitWant,
}

/// Why a fetch or ls-refs fails (`ERR <message>`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FetchError {
    /// A feature Aspen does not serve.
    #[error("{message}")]
    Unsupported {
        /// Which feature.
        feature: Unsupported,
        /// The text the client sees.
        message: String,
    },
    /// A commit on a history walk is neither indexed nor readable from the
    /// ref version's packs. Never answered from a partial walk.
    #[error("commit {id} cannot be resolved yet; retry")]
    CommitUnresolved {
        /// The commit.
        id: ObjectId,
    },
    /// A stored pack breaks the base-first invariant: a delta whose base is
    /// neither earlier in its segment nor already emitted. Retryable, and an
    /// alert, because only rewriting the pack fixes it.
    #[error("stored pack {pack} has a forward delta base at offset {offset}; retry")]
    StoredPackNotBaseFirst {
        /// The pack.
        pack: PackId,
        /// Offset of the delta in that pack.
        offset: u64,
    },
    /// A store failed.
    #[error("storage: {0}")]
    Store(StoreError),
}

impl FetchError {
    /// The `ERR` for a `filter` line.
    pub fn filter() -> Self {
        Self::Unsupported {
            feature: Unsupported::Filter,
            message:
                "partial clone (filter) is not supported by this server; fetch without --filter"
                    .to_owned(),
        }
    }

    /// The `ERR` for a shallow request when the repository refuses them.
    pub fn shallow_refused() -> Self {
        Self::Unsupported {
            feature: Unsupported::Shallow,
            message: "shallow fetches are disabled for this repository; fetch without --depth"
                .to_owned(),
        }
    }

    /// The `ERR` for a want Aspen can't resolve to a commit or tag.
    pub fn non_commit_want(id: ObjectId) -> Self {
        Self::Unsupported {
            feature: Unsupported::NonCommitWant,
            message: format!(
                "want {id} is not a commit or tag this server can serve (lazy fetches from partial clones are not supported)"
            ),
        }
    }
}

impl ToWire for FetchError {
    fn to_wire(&self) -> WireError {
        match self {
            Self::Store(e) => e.wire(Surface::FetchErr),
            other => WireError {
                surface: Surface::FetchErr,
                message: other.to_string(),
                retryable: matches!(
                    other,
                    Self::CommitUnresolved { .. } | Self::StoredPackNotBaseFirst { .. }
                ),
                alert: matches!(other, Self::StoredPackNotBaseFirst { .. }),
            },
        }
    }
}

/// Why authentication or authorization fails.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// No credential, or a credential that is invalid or expired.
    #[error("authentication required")]
    Unauthenticated,
    /// The principal is known but may not do this.
    #[error("permission denied")]
    Forbidden,
    /// The repository does not exist, or the principal may not know that it
    /// does. The two are indistinguishable on purpose.
    #[error("repository not found")]
    NotFound,
    /// The identity provider or ACL store is temporarily unreachable.
    #[error("authorization service unavailable")]
    Unavailable,
}

impl ToWire for AuthError {
    fn to_wire(&self) -> WireError {
        let status = match self {
            Self::Unauthenticated => 401,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::Unavailable => 503,
        };
        WireError {
            surface: Surface::Http(status),
            message: self.to_string(),
            retryable: matches!(self, Self::Unavailable),
            alert: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The metric and log for a writer past the grace period match on
    /// `is_pack_swept`, which reads the same constant the constructor
    /// writes: rewording one rewords both.
    #[test]
    fn pack_swept_is_recognised_and_nothing_else_is() {
        let e = StoreError::pack_swept("pack 7 of push 3");
        assert!(e.is_pack_swept());
        assert!(matches!(e, StoreError::Unavailable { .. }), "retryable");
        assert!(!e.is_corruption());
        for other in [
            StoreError::Unavailable {
                reason: "connection reset".to_owned(),
            },
            StoreError::Unavailable {
                reason: format!("wrapped: {PACK_SWEPT}"),
            },
            StoreError::Corrupt {
                what: PACK_SWEPT.to_owned(),
            },
        ] {
            assert!(!other.is_pack_swept(), "{other:?}");
        }
    }

    #[test]
    fn every_reject_reason_has_text_and_goes_on_the_ng_line() {
        for r in RejectReason::ALL {
            let w = r.to_wire();
            assert_eq!(w.surface, Surface::PushRef);
            assert!(!w.message.is_empty());
            assert!(!w.alert);
            // One line, as pkt-line `ng` reasons must be.
            assert!(!w.message.contains('\n'));
        }
    }

    #[test]
    fn only_server_side_transients_are_retryable() {
        let retryable: Vec<_> = RejectReason::ALL
            .into_iter()
            .filter(|r| r.retryable())
            .collect();
        assert_eq!(
            retryable,
            vec![
                RejectReason::AncestryUndecided,
                RejectReason::CasLost,
                RejectReason::TrunkRewritten,
                RejectReason::UpstreamUnavailable,
                RejectReason::UpstreamAhead,
            ]
        );
        for r in retryable {
            assert!(r.message().contains("retry"), "{r:?}");
        }
    }

    #[test]
    fn race_losses_use_phrases_the_oracle_recognises() {
        for r in [
            RejectReason::StaleOld,
            RejectReason::CasLost,
            RejectReason::NonFastForward,
            RejectReason::RefExists,
        ] {
            assert!(
                aspen_testkit::RACE_LOSS_REASONS
                    .iter()
                    .any(|p| r.message().contains(p)),
                "{r:?}: {}",
                r.message()
            );
        }
    }

    #[test]
    fn unpack_errors_refuse_the_pack() {
        let id = ObjectId::from_bytes([0xab; 20]);
        let w = UnpackError::DuplicateObject { id }.to_wire();
        assert_eq!(w.surface, Surface::PushUnpack);
        assert!(w.message.contains(&id.to_hex()), "names the object");
        assert!(!w.retryable && !w.alert);
        let w = UnpackError::MissingBase { id }.to_wire();
        assert!(w.message.contains("missing delta base"));
        // A push over the prefetch budget fails the same way on every
        // retry, and nothing stored is wrong (A2 decisions row, B1).
        let w = UnpackError::BasesOverBudget { budget: 1 << 30 }.to_wire();
        assert_eq!(w.surface, Surface::PushUnpack);
        assert!(w.message.contains("1073741824 bytes"), "{}", w.message);
        assert!(!w.retryable && !w.alert);
        let w = UnpackError::Store(StoreError::Unavailable {
            reason: "s3 503 at bucket/secret-key".into(),
        })
        .to_wire();
        assert!(w.retryable && !w.alert);
        assert!(
            !w.message.contains("secret-key"),
            "no internals on the wire"
        );
    }

    #[test]
    fn forward_base_in_a_stored_pack_is_retryable_and_pages() {
        let w = FetchError::StoredPackNotBaseFirst {
            pack: PackId::new(4).unwrap(),
            offset: 1234,
        }
        .to_wire();
        assert_eq!(w.surface, Surface::FetchErr);
        assert!(w.retryable && w.alert);
    }

    #[test]
    fn unresolved_commit_is_retryable_but_does_not_page() {
        let w = FetchError::CommitUnresolved {
            id: ObjectId::from_bytes([1; 20]),
        }
        .to_wire();
        assert!(w.retryable && !w.alert);
    }

    #[test]
    fn unsupported_features_are_final_and_explain_themselves() {
        for e in [
            FetchError::filter(),
            FetchError::shallow_refused(),
            FetchError::non_commit_want(ObjectId::from_bytes([2; 20])),
        ] {
            let w = e.to_wire();
            assert_eq!(w.surface, Surface::FetchErr);
            assert!(!w.retryable && !w.alert);
            assert!(w.message.contains("not supported") || w.message.contains("disabled"));
        }
    }

    #[test]
    fn corrupt_storage_pages_and_transient_storage_does_not() {
        let corrupt = FetchError::Store(StoreError::Corrupt { what: "idx".into() }).to_wire();
        assert!(corrupt.alert && corrupt.retryable);
        let missing = FetchError::Store(StoreError::NotFound {
            what: "pack".into(),
        })
        .to_wire();
        assert!(missing.alert);
        let down = FetchError::Store(StoreError::Unavailable { reason: "x".into() }).to_wire();
        assert!(!down.alert && down.retryable);
    }

    #[test]
    fn auth_errors_are_http_statuses() {
        assert_eq!(
            AuthError::Unauthenticated.to_wire().surface,
            Surface::Http(401)
        );
        assert_eq!(AuthError::Forbidden.to_wire().surface, Surface::Http(403));
        assert_eq!(AuthError::NotFound.to_wire().surface, Surface::Http(404));
        let w = AuthError::Unavailable.to_wire();
        assert_eq!(w.surface, Surface::Http(503));
        assert!(w.retryable);
    }
}

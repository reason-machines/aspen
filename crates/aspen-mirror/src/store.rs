//! What mirror mode stores, and the contract for the store that keeps it.
//! `aspen-server` implements [`MirrorStore`] on the metadata database
//! (`aspen-meta`, migration 0003).

use std::future::Future;

use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, RepoId, UnixMicros};
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::{Repo, RepoSettings};

use crate::credential::CredentialName;
use crate::upstream::UpstreamUrl;

/// How pushes to a mirror reach its upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MirrorMode {
    /// Synchronous push-through: a push is forwarded to the upstream before
    /// Aspen accepts it, and the client gets the upstream's verdict. The
    /// upstream stays the source of truth; Aspen follows it.
    Proxy,
    /// Background push-through: Aspen accepts a push as the primary, and a
    /// `mirror_push` job pushes it to the upstream afterwards. Changes made
    /// at the upstream directly flow back while Aspen has not moved the
    /// same ref; when both moved, the ref is in conflict.
    Async,
}

impl MirrorMode {
    /// Both modes, in the order of the schema's `CHECK` list.
    pub const ALL: [MirrorMode; 2] = [Self::Proxy, Self::Async];

    /// The value stored in `mirrors.mode`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Proxy => "proxy",
            Self::Async => "async",
        }
    }
}

impl std::str::FromStr for MirrorMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|m| m.as_str() == s)
            .ok_or_else(|| format!("unknown mirror mode {s:?} (proxy or async)"))
    }
}

impl std::fmt::Display for MirrorMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A mirrored repository's settings and last sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorRecord {
    /// The repository.
    pub repo: RepoId,
    /// Its upstream.
    pub upstream: UpstreamUrl,
    /// How pushes reach the upstream.
    pub mode: MirrorMode,
    /// The configured credential it uses; `None` for anonymous access.
    pub credential: Option<CredentialName>,
    /// When the mirror was set up.
    pub created_at: UnixMicros,
    /// When the last sync pass ended.
    pub last_sync_at: Option<UnixMicros>,
    /// The last pass's error, if it failed.
    pub last_error: Option<String>,
}

/// Both sides moved a ref since they last agreed, to different values.
/// `None` is a deletion on that side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Conflict {
    /// When it was found.
    pub at: UnixMicros,
    /// Aspen's value then.
    pub aspen: Option<ObjectId>,
    /// The upstream's value then.
    pub upstream: Option<ObjectId>,
}

/// The ledger state of one ref.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LedgerEntry {
    /// The value Aspen and the upstream last agreed on; `None` for "both
    /// absent" (a ref with no ledger row).
    pub synced: Option<ObjectId>,
    /// The open conflict, if any.
    pub conflict: Option<Conflict>,
}

/// The store mirror mode keeps its settings and ledger in. Every method is
/// one short transaction.
pub trait MirrorStore: Send + Sync {
    /// Creates repository `name` with `settings` and makes it a mirror with
    /// the record `record` builds from the new repository's ID, in one
    /// transaction: both or neither. A taken name is
    /// [`StoreError::Conflict`].
    fn create_mirror(
        &self,
        name: &RepoName,
        settings: &RepoSettings,
        record: impl FnOnce(RepoId) -> MirrorRecord + Send,
    ) -> impl Future<Output = Result<Repo, StoreError>> + Send;

    /// A repository's mirror settings, if it is a mirror.
    fn mirror(
        &self,
        repo: RepoId,
    ) -> impl Future<Output = Result<Option<MirrorRecord>, StoreError>> + Send;

    /// Every mirror.
    fn mirrors(&self) -> impl Future<Output = Result<Vec<MirrorRecord>, StoreError>> + Send;

    /// Stops mirroring a repository (its refs stay); whether it was one.
    fn delete_mirror(&self, repo: RepoId) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Records the end of a sync pass, with its error if it failed.
    fn record_sync(
        &self,
        repo: RepoId,
        at: UnixMicros,
        error: Option<&str>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The whole ledger of a repository, in ref-name order.
    fn ledger(
        &self,
        repo: RepoId,
    ) -> impl Future<Output = Result<Vec<(RefName, LedgerEntry)>, StoreError>> + Send;

    /// One ref's ledger state (empty without a row).
    fn ledger_entry(
        &self,
        repo: RepoId,
        name: &RefName,
    ) -> impl Future<Output = Result<LedgerEntry, StoreError>> + Send;

    /// Sets one ref's ledger state if its `synced` value is still
    /// `expected`; returns whether it did. An empty `next` removes the row.
    fn set_ledger(
        &self,
        repo: RepoId,
        name: &RefName,
        expected: Option<ObjectId>,
        next: LedgerEntry,
        at: UnixMicros,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// How many refs, over all mirrors, are in conflict.
    fn conflict_count(&self) -> impl Future<Output = Result<u64, StoreError>> + Send;
}

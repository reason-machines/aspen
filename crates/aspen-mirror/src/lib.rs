//! Mirror mode (node C4 of `docs/PLAN.md`): Aspen in front of GitHub,
//! GitLab or any smart-HTTP Git server.
//!
//! Fetches are served from Aspen's own packs. Pushes reach the upstream in
//! one of two [`MirrorMode`]s:
//!
//! - **proxy**: synchronously. Aspen checks the push as usual, forwards the
//!   accepted updates to the upstream ([`Mirror::forward`]) and commits only
//!   what the upstream accepted; the client sees the upstream's verdict.
//!   The upstream stays the source of truth.
//! - **async**: in the background. Aspen is the primary: it accepts the
//!   push, and a `mirror_push` job pushes the ref to the upstream
//!   ([`Mirror::run_one_job`]).
//!
//! In both modes a sync pass ([`Mirror::sync`]) runs every
//! `sync_interval` per mirror and imports what changed at the upstream
//! directly. Per ref, a ledger records the value both sides last agreed on;
//! a ref both sides changed since is a **conflict** in async mode, recorded
//! and reported (`aspen_alert_mirror_conflicts`, `aspen mirror status`) and
//! never overwritten until an operator resolves it ([`Mirror::resolve`]).
//! [`reconcile`] holds the rule.
//!
//! `docs/MIRROR.md` is the operator's guide.
//!
//! # Layering
//!
//! This crate talks to upstreams itself ([`upstream`], [`wire`]) and uses
//! only `aspen-core` and `aspen-proto` among Aspen's crates (`cargo xtask
//! check-deps`). What it needs from Aspen's own storage goes through
//! [`MirrorStore`] (settings and ledger), `aspen_core::meta::MetaStore`
//! (refs and jobs) and [`LocalRepo`] (writing refs through the push path,
//! and building packs for the upstream), which `aspen-server` implements.

#![deny(missing_docs)]

pub mod credential;
pub mod metrics;
pub mod reconcile;
pub mod service;
pub mod settings;
pub mod store;
pub mod upstream;
pub mod wire;

use std::future::Future;
use std::path::Path;

use aspen_core::error::StoreError;
use aspen_core::id::ObjectId;
use aspen_core::name::RefName;
use aspen_core::repo::Repo;

pub use credential::{CredentialName, Credentials};
pub use metrics::{Forwarded, Metrics};
pub use reconcile::Side;
pub use service::{JobWorker, Mirror, MirrorConfig, MirrorError, RefOutcome, SyncReport};
pub use store::{Conflict, LedgerEntry, MirrorMode, MirrorRecord, MirrorStore};
pub use upstream::UpstreamUrl;

/// One ref update: `old` to `new`, `None` meaning absent (a create from
/// `None`, a delete to `None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefUpdate {
    /// The ref.
    pub name: RefName,
    /// The value it must have now.
    pub old: Option<ObjectId>,
    /// The value it gets.
    pub new: Option<ObjectId>,
}

/// What Aspen's push path made of an update from the upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// The ref now has the new value.
    Updated,
    /// The push path refused it; its reason (for example the ref moved
    /// since it was read, which the next pass sees).
    Rejected(String),
}

/// Aspen's own repository operations that mirror mode needs, implemented
/// by the composition root over the push path and the fetch planner.
pub trait LocalRepo: Send + Sync {
    /// Applies `update` to `repo` through the push path, as a push from
    /// the upstream: `pack` (a pack file, needed unless the update is a
    /// delete) holds what the new value reaches beyond Aspen's trunk and
    /// the ref's current value. The update's `old` is a compare-and-swap
    /// expectation. The repository's force policy does not apply: the
    /// upstream already accepted the change. It is never forwarded back
    /// to the upstream.
    fn apply(
        &self,
        repo: &Repo,
        update: &RefUpdate,
        pack: Option<&Path>,
    ) -> impl Future<Output = Result<Applied, StoreError>> + Send;

    /// Writes to `out` a self-contained pack with every object `want`
    /// reaches that the `haves` do not, for a push to the upstream. Haves
    /// Aspen cannot use are ignored, which only makes the pack larger.
    fn write_pack(
        &self,
        repo: &Repo,
        want: ObjectId,
        haves: &[ObjectId],
        out: &Path,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

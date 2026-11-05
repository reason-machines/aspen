//! Push hooks: repository rules that run inside receive-pack, the way a
//! `pre-receive` and a `post-receive` hook run inside `git receive-pack`.
//!
//! A [`PushHook`] is given to the service in
//! [`super::ReceiveConfig::hook`]. For each ref update of a push it is
//! asked, in [`PushHook::check`], whether the update may land:
//!
//! - after the commit check accepted the update and after its storage was
//!   planned, so a hook never sees an update the server would refuse
//!   anyway (a non-fast-forward without force, a branch at a tree);
//! - before `--atomic` is applied, so a refusal fails an atomic push as a
//!   whole;
//! - before the compare-and-swap, against the ref's **stored** tip, which
//!   the server read itself. The client's old value is never used: an
//!   update of a branch the client was not shown arrives with a zero old
//!   value (S2). When the compare-and-swap loses and the verdict is
//!   recomputed against the ref's new version, the hook is asked again.
//!
//! Once a group of updates has committed (the whole push under `--atomic`,
//! otherwise each ref on its own), [`PushHook::committed`] is told about
//! it, exactly once per committed update.
//!
//! [`HookObjects`] gives the hook what it needs to inspect a push without
//! a repository on disk: the exact commit range an update adds
//! ([`HookObjects::range`], the set `git rev-list <new> ^<exclude>...`
//! lists, computed with B1's ancestry walk, which is exact under clock
//! skew), and any object the push brought or the server holds
//! ([`HookObjects::read`]).
//!
//! A refusal shows on the update's `ng` line, and its detail lines on
//! side-band 2 (`remote: aspen: ...`), even under `quiet`.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use aspen_core::error::StoreError;
use aspen_core::id::ObjectId;
use aspen_core::ingest::ObjectKind;
use aspen_core::name::RefName;
use aspen_core::repo::Repo;

/// A boxed `Send` future, so [`PushHook`] can be a trait object.
pub type HookFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An object's kind and contents.
pub type Object = (ObjectKind, Vec<u8>);

/// Who is pushing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Caller {
    /// The principal, as the access layer identified it (`anonymous`, or
    /// the subject), or a label an in-process caller chose.
    pub principal: String,
    /// The key the access layer rate-limits this caller by: the principal
    /// when signed in, the client address (an IPv6 address by its /64)
    /// when anonymous. Empty when there is no access layer.
    pub key: String,
}

/// One ref update a hook is asked about.
#[derive(Debug, Clone, Copy)]
pub struct HookUpdate<'a> {
    /// The repository.
    pub repo: &'a Repo,
    /// Who is pushing.
    pub caller: &'a Caller,
    /// The ref.
    pub name: &'a RefName,
    /// The ref's tip as stored, read by the server for this verdict;
    /// `None` when the ref does not exist (never created, or deleted).
    pub stored: Option<ObjectId>,
    /// The new tip; `None` for a delete.
    pub new: Option<ObjectId>,
    /// The trunk tip this push's lookup and connectivity check searched;
    /// `None` when trunk does not exist.
    pub trunk: Option<ObjectId>,
}

/// A hook's answer for one update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookVerdict {
    /// The update may land.
    Accept,
    /// The update is refused.
    Reject {
        /// One line for the update's `ng` line.
        reason: String,
        /// More lines for side-band 2, such as each problem found.
        details: Vec<String>,
    },
}

/// One commit of a [`CommitRange`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeCommit {
    /// The commit.
    pub id: ObjectId,
    /// Its parents, in commit order.
    pub parents: Vec<ObjectId>,
}

/// The commits an update adds, as [`HookObjects::range`] found them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRange {
    /// Every commit reachable from the new tip and from none of the
    /// excluded tips, parents before children (`git rev-list
    /// --topo-order --reverse`).
    Commits(Vec<RangeCommit>),
    /// More commits than the limit the hook passed.
    TooLong,
    /// A commit could not be resolved (history still being indexed, or an
    /// object that is not a commit where a commit is needed), or a walk
    /// ran past its budget. Retrying later may decide it.
    Undecided,
}

/// Objects a hook may read while it checks an update.
pub trait HookObjects: Send {
    /// The commits reachable from `new` and from none of `exclude`, at
    /// most `max_commits` of them. Each commit is tested exactly against
    /// each excluded tip with the push's ancestry walk; nothing below an
    /// excluded commit is visited.
    fn range<'a>(
        &'a mut self,
        new: ObjectId,
        exclude: &'a [ObjectId],
        max_commits: usize,
    ) -> HookFuture<'a, Result<CommitRange, StoreError>>;

    /// One object, from the pushed pack, the packs the push searched, or
    /// the closure of the updated ref's stored version; `None` if none of
    /// them has it.
    fn read(&mut self, id: ObjectId) -> HookFuture<'_, Result<Option<Object>, StoreError>>;
}

/// One update that committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedUpdate {
    /// The ref.
    pub name: RefName,
    /// Its tip before the push, as the hook saw it (`stored`).
    pub old: Option<ObjectId>,
    /// Its tip now; `None` for a delete.
    pub new: Option<ObjectId>,
}

/// Repository rules run inside receive-pack (module docs).
pub trait PushHook: Send + Sync + fmt::Debug {
    /// Whether `update` may land. An error fails the push as a storage
    /// error (`unpack` line, retryable), as when the server cannot read
    /// what it needs to decide; nothing is stored and [`Self::committed`]
    /// is not called for a push whose checks run before any commit.
    ///
    /// Without `--atomic` each ref commits on its own, and a lost
    /// compare-and-swap asks the hook again about that ref after earlier
    /// refs may have committed. An error then still answers the whole push
    /// with the `unpack` error, though those earlier refs stay committed
    /// (and were reported to [`Self::committed`]), as with any storage
    /// error at that point. The client's next fetch shows what landed.
    fn check<'a>(
        &'a self,
        update: HookUpdate<'a>,
        objects: &'a mut dyn HookObjects,
    ) -> HookFuture<'a, Result<HookVerdict, StoreError>>;

    /// These updates of `repo` by `caller` committed, in one transaction.
    fn committed<'a>(
        &'a self,
        repo: &'a Repo,
        caller: &'a Caller,
        updates: &'a [CommittedUpdate],
    ) -> HookFuture<'a, ()>;
}

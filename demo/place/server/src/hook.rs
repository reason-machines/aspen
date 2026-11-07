//! The pre-receive check of `docs/TILES.md`, as an Aspen push hook, and
//! the post-receive step that puts each accepted push on the canvas.
//!
//! For a Git client's update of a branch the hook:
//!
//! 1. checks the ref (`main` and the server's own namespaces are
//!    protected, only branches are accepted), and refuses a delete or a
//!    non-fast-forward of a `paint/` branch ([`rules::forward_only`]),
//!    judged against the stored tip;
//! 2. takes the commits `git rev-list <new> ^<trunk tip> ^<stored tip>`
//!    lists, where both excluded tips are the ones the server read
//!    ([`HookUpdate::trunk`], [`HookUpdate::stored`]), never the client's
//!    old value (A9 review obligation 4). When that range, walked at most
//!    [`PROBE_COMMITS`] deep, runs into a root commit or past that depth,
//!    the push may be built on a `main` from before a history reset
//!    ([`crate::place::Place::reset_history`]): the commits of any earlier
//!    `main` it reaches are excluded too ([`MainHistory`]), so a clone made
//!    before a reset still pushes, without walking the old history first;
//! 3. diffs each commit against its first parent and runs
//!    [`rules::validate_push`] over them;
//! 4. takes a token from the server-wide push ceiling
//!    ([`Shared::take_push`], shared with web strokes), then charges the
//!    push to the caller's per-branch quota, giving the token back if the
//!    quota refuses (an update asked about again after a lost
//!    compare-and-swap takes another, as it is charged to its quota
//!    again). Either refusal is the update's `ng` reason, which Git
//!    prints as `! [remote rejected] ... (<reason>)`. The pack was read
//!    by then, but nothing of a refused update is stored: receive-pack
//!    uploads a pack only when an update that names it is about to
//!    commit.
//!
//! The tiles it accepted are kept until the update commits; then
//! [`PushHook::committed`] records the push, which gives it its sequence
//! number and sends the `push` event. Updates made by the server itself
//! (web strokes, agents, the merger bot) carry [`INTERNAL_KEY`] and were
//! checked before they were written.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, RefVersionId, RepoId};
use aspen_core::ingest::{ObjectKind, SortedIds};
use aspen_core::meta::{CommitLookup, MetaStore};
use aspen_core::repo::Repo;
use aspen_server::receive::hook::{
    Caller, CommitRange, CommittedUpdate, HookFuture, HookObjects, HookUpdate, HookVerdict,
    PushHook,
};

use crate::git::{MODE_TREE, TreeEntry, parse_commit, parse_tree};
use crate::limits::{PushCeiling, Windows};
use crate::rules::{
    self, ChangeKind, FileChange, PushedCommit, Pusher, Rejection, Rules, TILE_MODE, Tile,
};
use crate::state::{Actor, State, now_ms};

/// The rate-limit key of updates the server makes itself. The access
/// layer gives every HTTP caller a key starting `subject:` or `ip:`, so no
/// client can present this one.
pub const INTERNAL_KEY: &str = "place:internal";

/// What the hook found for an update, kept until it commits.
#[derive(Debug, Clone)]
pub struct Accepted {
    /// Who painted.
    pub actor: Actor,
    /// The tiles the push paints.
    pub tiles: Vec<Tile>,
    /// Every tile of the new tip, when the server built it.
    pub full: Option<Arc<Vec<u8>>>,
    /// When the check accepted it, in ms. An update that never commits (a
    /// lost race, a refused atomic peer) is dropped after [`ACCEPTED_TTL_MS`].
    pub at_ms: u64,
}

/// How long an accepted update waits for its commit before it is dropped.
pub const ACCEPTED_TTL_MS: u64 = 10 * 60 * 1000;

/// Most accepted updates kept waiting for their commit at once.
pub const MAX_ACCEPTED: usize = 1024;

/// Who checked an update waiting in [`Waiting`], which is also who made
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Writer {
    /// A Git client's push, checked by the hook.
    Git,
    /// The server's own write (caller key [`INTERNAL_KEY`]), checked
    /// before it was written.
    Server,
}

/// Accepted updates waiting to commit, by (ref, new tip), kept apart by
/// [`Writer`].
///
/// The hook is not told about an update that passed its check but never
/// committed (a refused `--atomic` peer, a compare-and-swap lost for good,
/// a push that failed after the check), so a Git client's entries are
/// dropped here: after [`ACCEPTED_TTL_MS`], and, whenever [`MAX_ACCEPTED`]
/// are waiting, the oldest first. A Git update evicted before it commits
/// is still stored in Git; only its tiles are missing from the canvas
/// until the branch is pushed again. Pushes that commit within a second of
/// their check are evicted only by more than [`MAX_ACCEPTED`] checks in
/// that second.
///
/// The server's own entries are never evicted, so no number of Git checks
/// can take a stroke's entry before it commits (its writer would then
/// find no sequence number). Their writer removes them however its push
/// ends, and the API's write guard and concurrency bound how many there
/// are.
#[derive(Debug, Default)]
pub struct Waiting {
    git: HashMap<(String, ObjectId), Accepted>,
    server: HashMap<(String, ObjectId), Accepted>,
}

impl Waiting {
    /// Keeps `a`, written by `writer`, until `(name, new)` commits (type
    /// docs for the bound).
    pub fn insert(&mut self, writer: Writer, name: &str, new: ObjectId, a: Accepted) {
        let key = (name.to_owned(), new);
        if writer == Writer::Server {
            self.server.insert(key, a);
            return;
        }
        if !self.git.contains_key(&key) && self.git.len() >= MAX_ACCEPTED {
            let cutoff = a.at_ms.saturating_sub(ACCEPTED_TTL_MS);
            self.git.retain(|_, v| v.at_ms > cutoff);
            if self.git.len() >= MAX_ACCEPTED {
                let oldest = self
                    .git
                    .iter()
                    .min_by_key(|(_, v)| v.at_ms)
                    .map(|(k, _)| k.clone());
                if let Some(oldest) = oldest {
                    self.git.remove(&oldest);
                }
            }
        }
        self.git.insert(key, a);
    }

    fn map(&mut self, writer: Writer) -> &mut HashMap<(String, ObjectId), Accepted> {
        match writer {
            Writer::Git => &mut self.git,
            Writer::Server => &mut self.server,
        }
    }

    /// Takes `writer`'s entry for `(name, new)`.
    pub fn take(&mut self, writer: Writer, name: &str, new: ObjectId) -> Option<Accepted> {
        self.map(writer).remove(&(name.to_owned(), new))
    }

    /// Whether `writer`'s `(name, new)` is waiting.
    pub fn contains(&self, writer: Writer, name: &str, new: ObjectId) -> bool {
        let map = match writer {
            Writer::Git => &self.git,
            Writer::Server => &self.server,
        };
        map.contains_key(&(name.to_owned(), new))
    }

    /// How many updates are waiting.
    pub fn len(&self) -> usize {
        self.git.len() + self.server.len()
    }

    /// Whether none is.
    pub fn is_empty(&self) -> bool {
        self.git.is_empty() && self.server.is_empty()
    }
}

impl Shared {
    /// Keeps `a` until `(name, new)` commits ([`Waiting`]).
    pub fn accept(&self, writer: Writer, name: &str, new: ObjectId, a: Accepted) {
        self.accepted
            .lock()
            .expect("not poisoned")
            .insert(writer, name, new, a);
    }
}

/// Limits of the check itself.
#[derive(Debug, Clone, Copy)]
pub struct CheckLimits {
    /// Most commits one update may add.
    pub max_commits: usize,
    /// Most tree entries one update's diffs may visit.
    pub max_tree_entries: usize,
    /// Pushes per caller and branch per rolling minute.
    pub pushes_per_minute: usize,
}

/// State the hook shares with the rest of the service.
#[derive(Debug)]
pub struct Shared {
    /// The canvas repository.
    pub repo: RepoId,
    /// Canvas size, palette, tile limit.
    pub rules: Arc<Rules>,
    /// The canvas.
    pub state: Mutex<State>,
    /// Check limits.
    pub limits: CheckLimits,
    /// The blob of each palette color's tile file, by palette index.
    pub palette_blobs: Vec<ObjectId>,
    /// Accepted updates waiting to commit.
    pub accepted: Mutex<Waiting>,
    /// Sequence numbers of the server's own committed pushes (caller key
    /// [`INTERNAL_KEY`] only), for the writer that is waiting on them,
    /// which removes its entry however its push ends.
    pub committed: Mutex<HashMap<(String, ObjectId), u64>>,
    /// Git pushes per caller and branch.
    pub git_quota: Mutex<Windows>,
    /// Which commits are on `main`'s history, past or present.
    pub main_history: Arc<dyn MainHistory>,
    /// Every push the server takes from outside, web strokes and Git
    /// updates together ([`Shared::take_push`]).
    pub pushes: Mutex<PushCeiling>,
}

/// What the commit index says about one commit, for [`MainHistory`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Indexed {
    /// The commit is reachable from a version of `main`: one of the server's
    /// own commits, from this history or one before a reset.
    Main,
    /// Indexed under another ref (a branch), with these parents.
    Other(Vec<ObjectId>),
    /// Not indexed (a commit of the push being checked, or one not known).
    NotIndexed,
}

/// Tells the hook which commits are on `main`'s history. Only the server
/// writes `main` (the merger bot, the README refresh and history resets),
/// so a commit any version of `main` reaches holds tiles the server
/// checked: a push built on it is checked from there, like one built on
/// the current `main`.
pub trait MainHistory: Send + Sync + std::fmt::Debug {
    /// One answer per ID, in order. `versions` is what earlier calls of
    /// the same walk learned about ref versions; it lives as long as the
    /// walk, so each version is looked up at most once per walk.
    fn classify<'a>(
        &'a self,
        ids: &'a [ObjectId],
        versions: &'a mut VersionCache,
    ) -> HookFuture<'a, Result<Vec<Indexed>, StoreError>>;
}

/// Whether each ref version a walk met is one of `main`'s, kept for the
/// whole walk ([`MainHistory::classify`]).
#[derive(Debug, Default)]
pub struct VersionCache(HashMap<RefVersionId, bool>);

/// [`MainHistory`] from Aspen's commit index: a commit's row names a
/// version whose tip reaches it, and a commit is on `main`'s history
/// exactly when that version is one of `main`'s (`main` takes no client
/// pushes, so a version of it reaches only server commits; ref versions are
/// never deleted, so this holds for every history before a reset too).
#[derive(Debug)]
pub struct IndexedMain<M> {
    /// The metadata store.
    pub meta: M,
    /// The canvas repository.
    pub repo: RepoId,
}

impl<M: MetaStore + std::fmt::Debug + 'static> MainHistory for IndexedMain<M> {
    fn classify<'a>(
        &'a self,
        ids: &'a [ObjectId],
        versions: &'a mut VersionCache,
    ) -> HookFuture<'a, Result<Vec<Indexed>, StoreError>> {
        Box::pin(async move {
            let sorted = SortedIds::from_unsorted(ids.to_vec());
            let rows = self.meta.lookup_commits(self.repo, &sorted).await?;
            let by_id: HashMap<ObjectId, CommitLookup> =
                sorted.as_slice().iter().copied().zip(rows).collect();
            let on_main = &mut versions.0;
            let mut out = Vec::with_capacity(ids.len());
            for id in ids {
                let Some(CommitLookup::Indexed(row)) = by_id.get(id) else {
                    out.push(Indexed::NotIndexed);
                    continue;
                };
                let main = match on_main.get(&row.ref_version) {
                    Some(m) => *m,
                    None => {
                        let m = self
                            .meta
                            .ref_version(row.ref_version)
                            .await?
                            .is_some_and(|v| {
                                v.repo == self.repo && v.name.as_str() == rules::TRUNK_REF
                            });
                        on_main.insert(row.ref_version, m);
                        m
                    }
                };
                out.push(if main {
                    Indexed::Main
                } else {
                    Indexed::Other(row.parents.clone())
                });
            }
            Ok(out)
        })
    }
}

/// The server-wide push ceiling refused a push; retry after this long.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtLimit {
    /// The ceiling's sustained rate, for the message.
    pub per_second: f64,
    /// When to retry, in whole seconds (at least 1).
    pub retry_after_secs: u64,
}

impl AtLimit {
    /// One line for a Git `ng` reason or an API error.
    pub fn message(&self) -> String {
        format!(
            "place is at its limit: about {} pushes a second from everyone together. \
             This push was not taken; retry in {} s",
            self.per_second, self.retry_after_secs
        )
    }
}

impl Shared {
    /// Takes one push from the server-wide ceiling. Web strokes and Git
    /// updates from outside take one each; the server's own writers (the
    /// merger bot, ambient and swarm agents) take none, so merges never
    /// wait on it.
    ///
    /// A refusal leaves a log line on standard error, the first at once and
    /// then at most one every [`crate::limits::REFUSAL_REPORT_EVERY`],
    /// counting the refusals since the line before and in all.
    pub fn take_push(&self) -> Result<(), AtLimit> {
        let mut ceiling = self.pushes.lock().expect("not poisoned");
        let refused = match ceiling.take(Instant::now()) {
            Ok(()) => return Ok(()),
            Err(r) => r,
        };
        let (per_second, burst) = (ceiling.per_second(), ceiling.burst());
        drop(ceiling);
        if let Some((since, total)) = refused.report {
            eprintln!(
                "place: push ceiling ({per_second}/s, burst {burst}) refused {since} pushes since its last report, {total} in all"
            );
        }
        Err(AtLimit {
            per_second,
            retry_after_secs: refused.wait.as_secs_f64().ceil().max(1.0) as u64,
        })
    }

    /// Gives back a push taken with [`Shared::take_push`] that a later
    /// check refused, so it does not count.
    pub fn give_back_push(&self) {
        self.pushes
            .lock()
            .expect("not poisoned")
            .give_back(Instant::now());
    }
}

impl Shared {
    fn palette_color(&self, blob: &ObjectId) -> Option<u8> {
        self.palette_blobs
            .iter()
            .position(|b| b == blob)
            .map(|i| i as u8)
    }
}

/// The hook (module docs).
#[derive(Debug, Clone)]
pub struct PlaceHook(pub Arc<Shared>);

impl PushHook for PlaceHook {
    fn check<'a>(
        &'a self,
        update: HookUpdate<'a>,
        objects: &'a mut dyn HookObjects,
    ) -> HookFuture<'a, Result<HookVerdict, StoreError>> {
        Box::pin(async move {
            if update.repo.id != self.0.repo || update.caller.key == INTERNAL_KEY {
                return Ok(HookVerdict::Accept);
            }
            check_git_update(&self.0, update, objects).await
        })
    }

    fn committed<'a>(
        &'a self,
        repo: &'a Repo,
        caller: &'a Caller,
        updates: &'a [CommittedUpdate],
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            if repo.id != self.0.repo {
                return;
            }
            // Only the server's own writers wait for a sequence number; a
            // Git client's push is answered by receive-pack itself.
            let waited_on = caller.key == INTERNAL_KEY;
            let now = now_ms();
            for u in updates {
                let Some(short) = u.name.as_str().strip_prefix("refs/heads/") else {
                    continue;
                };
                if u.name.as_str() == rules::TRUNK_REF {
                    // The merger bot records its own merges.
                    continue;
                }
                let Some(new) = u.new else {
                    self.0
                        .state
                        .lock()
                        .expect("not poisoned")
                        .branch_deleted(short, now);
                    continue;
                };
                let writer = if waited_on {
                    Writer::Server
                } else {
                    Writer::Git
                };
                let accepted = self.0.accepted.lock().expect("not poisoned").take(
                    writer,
                    u.name.as_str(),
                    new,
                );
                let Some(a) = accepted else {
                    // The hook was not asked (never for this repository),
                    // or the entry was evicted (`Waiting`): show the branch
                    // without tiles.
                    continue;
                };
                let seq = self
                    .0
                    .state
                    .lock()
                    .expect("not poisoned")
                    .record_push(short, a.actor, new, a.tiles, a.full, now);
                if waited_on {
                    self.0
                        .committed
                        .lock()
                        .expect("not poisoned")
                        .insert((u.name.as_str().to_owned(), new), seq);
                }
            }
        })
    }
}

/// One problem goes on the `ng` line; several are listed one per line on
/// side-band 2, with a count on the `ng` line.
fn refuse(rejections: Vec<Rejection>) -> HookVerdict {
    match &rejections[..] {
        [one] => refuse_one(one.message.clone()),
        _ => HookVerdict::Reject {
            reason: format!(
                "{} problems with this push; each is listed above",
                rejections.len()
            ),
            details: rejections.into_iter().map(|r| r.message).collect(),
        },
    }
}

fn refuse_one(reason: String) -> HookVerdict {
    HookVerdict::Reject {
        reason,
        details: Vec::new(),
    }
}

async fn check_git_update(
    shared: &Shared,
    update: HookUpdate<'_>,
    objects: &mut dyn HookObjects,
) -> Result<HookVerdict, StoreError> {
    let name = update.name.as_str();
    if let Some(r) = rules::validate_ref(name, Pusher::Git) {
        return Ok(refuse(vec![r]));
    }
    let forward_only = rules::forward_only(name);
    let Some(new) = update.new else {
        if forward_only {
            return Ok(refuse_one(format!(
                "{name}: {prefix} branches cannot be deleted, because anyone can push to any of them; leave it (it expires once its tiles merge) and push to a new branch, e.g. git push origin HEAD:refs/heads/{prefix}<you>-2",
                prefix = rules::FORWARD_ONLY_PREFIX
            )));
        }
        // A delete of any other unreserved branch is Aspen's ref-update
        // policy, not a tile rule. No commit rules apply.
        return Ok(HookVerdict::Accept);
    };
    if forward_only
        && let Some(stored) = update.stored
        && stored != new
    {
        // Fast-forward exactly when the stored tip is an ancestor of the
        // new one: the walk from `stored` that excludes `new` then lists
        // nothing, and with a limit of 0 any commit it would list is
        // `TooLong`. The stored tip is the server's, never the client's
        // old value, so `-o force` or a stale lease cannot get past it.
        match objects.range(stored, &[new], 0).await? {
            CommitRange::Commits(_) => {}
            CommitRange::TooLong => {
                return Ok(refuse_one(format!(
                    "{name}: {prefix} branches only move forward, so a force-push cannot rewrite one (it may be someone else's); push your history to a new branch instead, e.g. git push origin HEAD:refs/heads/{prefix}<you>-2",
                    prefix = rules::FORWARD_ONLY_PREFIX
                )));
            }
            CommitRange::Undecided => {
                return Ok(refuse_one(
                    "the server could not read this push's history yet; retry".to_owned(),
                ));
            }
        }
    }
    let exclude: Vec<ObjectId> = update.trunk.into_iter().chain(update.stored).collect();
    let limits = shared.limits;
    let range = commit_range(
        &*shared.main_history,
        objects,
        new,
        exclude,
        limits.max_commits,
    )
    .await?;
    let range = match range {
        CommitRange::Commits(c) => c,
        CommitRange::TooLong => {
            return Ok(refuse_one(format!(
                "this push adds more than {} commits; push fewer at a time",
                limits.max_commits
            )));
        }
        CommitRange::Undecided => {
            return Ok(refuse_one(
                "the server could not read this push's history yet; retry".to_owned(),
            ));
        }
    };
    let mut budget = limits.max_tree_entries;
    let mut commits = Vec::with_capacity(range.len());
    for c in &range {
        let mut pushed = PushedCommit {
            id: c.id.to_string(),
            parent_count: c.parents.len(),
            changes: Vec::new(),
        };
        if c.parents.len() <= 1 {
            let tree = commit_tree(objects, c.id).await?;
            let parent_tree = match c.parents.first() {
                Some(p) => Some(commit_tree(objects, *p).await?),
                None => None,
            };
            match diff(shared, objects, parent_tree, Some(tree), &mut budget).await? {
                Some(changes) => pushed.changes = changes,
                None => {
                    return Ok(refuse_one(format!(
                        "this push changes more than {} tree entries; push fewer files at a time",
                        limits.max_tree_entries
                    )));
                }
            }
        }
        commits.push(pushed);
    }
    let tiles = match rules::validate_push(&commits, &shared.rules) {
        Ok(t) => t,
        Err(r) => return Ok(refuse(r)),
    };
    if let Err(at_limit) = shared.take_push() {
        return Ok(refuse_one(at_limit.message()));
    }
    let quota_key = format!("{}\n{name}", update.caller.key);
    let admitted = shared
        .git_quota
        .lock()
        .expect("not poisoned")
        .admit(&quota_key, now_ms());
    if let Err(retry_after_ms) = admitted {
        shared.give_back_push();
        return Ok(refuse_one(format!(
            "rate limited: {} pushes a minute per address and branch; retry in {} s",
            limits.pushes_per_minute,
            retry_after_ms.div_ceil(1000).max(1)
        )));
    }
    let short = name.strip_prefix("refs/heads/").unwrap_or(name);
    shared.accept(
        Writer::Git,
        name,
        new,
        Accepted {
            actor: Actor::git(short),
            tiles,
            full: None,
            at_ms: now_ms(),
        },
    );
    Ok(HookVerdict::Accept)
}

/// How deep the hook walks a push's range before it also looks for an
/// earlier `main`'s commits. A range this short that reaches no root
/// commit is an ordinary push, checked with one walk as before; a push
/// built on a `main` from before a history reset reads at most this many
/// commits of the old history before the hook stops at it.
pub const PROBE_COMMITS: usize = 64;

/// Most IDs [`main_boundary`] asks [`MainHistory::classify`] about at once.
const CLASSIFY_BATCH: usize = 512;

/// The commits `new` adds over `exclude`, at most `max_commits` of them
/// ([`HookObjects::range`]), stopping at the commits of any version of
/// `main` too, past or present ([`main_boundary`]). The range is first
/// walked [`PROBE_COMMITS`] deep; only a push that reaches a root commit
/// or goes deeper pays for the boundary walk and a second range walk.
async fn commit_range(
    history: &dyn MainHistory,
    objects: &mut dyn HookObjects,
    new: ObjectId,
    mut exclude: Vec<ObjectId>,
    max_commits: usize,
) -> Result<CommitRange, StoreError> {
    let probe = PROBE_COMMITS.min(max_commits);
    let first = objects.range(new, &exclude, probe).await?;
    let reaches_a_root = match &first {
        CommitRange::Commits(c) => c.iter().any(|c| c.parents.is_empty()),
        CommitRange::TooLong => true,
        CommitRange::Undecided => false,
    };
    if !reaches_a_root {
        return Ok(first);
    }
    // Built on a `main` from before a history reset, on no `main` at all,
    // or simply long: stop at the earlier `main`'s commits too, before
    // walking any further.
    let boundary = main_boundary(history, objects, new, max_commits).await?;
    if boundary.is_empty() && (matches!(first, CommitRange::Commits(_)) || probe == max_commits) {
        // No `main` to stop at, and the first walk already saw the whole
        // range (an unrelated history) or went as deep as allowed.
        return Ok(first);
    }
    exclude.extend(boundary);
    objects.range(new, &exclude, max_commits).await
}

/// The commits of `main`'s history (any epoch, [`MainHistory`]) that the
/// history of `new` reaches first: walking down from `new` breadth first,
/// a commit on `main` is kept and not walked past.
///
/// The walk spends at most `budget` units in all, one per ID it asks the
/// index about and one per object it reads, whatever the shape of the
/// history (a wide octopus generation, a deep chain off `main`, IDs that
/// cannot be read). Once the budget is spent it gives up and returns what
/// it found so far; every ID it returns is on `main`, so a partial answer
/// only leaves the range longer, which then reports it as too long.
/// Commits the index knows are classified from it in batches; the push's
/// own commits are read from its pack.
async fn main_boundary(
    history: &dyn MainHistory,
    objects: &mut dyn HookObjects,
    new: ObjectId,
    budget: usize,
) -> Result<Vec<ObjectId>, StoreError> {
    let mut boundary = Vec::new();
    let mut seen: HashSet<ObjectId> = [new].into();
    let mut queue: VecDeque<ObjectId> = [new].into();
    let mut versions = VersionCache::default();
    let mut spent = 0usize;
    while !queue.is_empty() && spent < budget {
        let n = queue.len().min(budget - spent).min(CLASSIFY_BATCH);
        let batch: Vec<ObjectId> = queue.drain(..n).collect();
        spent += batch.len();
        let answers = history.classify(&batch, &mut versions).await?;
        for (id, answer) in batch.iter().zip(answers) {
            let parents = match answer {
                Indexed::Main => {
                    boundary.push(*id);
                    continue;
                }
                Indexed::Other(parents) => parents,
                Indexed::NotIndexed => {
                    if spent >= budget {
                        return Ok(boundary);
                    }
                    spent += 1;
                    match objects.read(*id).await? {
                        Some((ObjectKind::Commit, data)) => {
                            parse_commit(&data)
                                .map_err(|what| StoreError::Corrupt { what })?
                                .1
                        }
                        // Not readable or not a commit: the range decides.
                        _ => continue,
                    }
                }
            };
            // Every queued ID costs at least one unit, so IDs past what the
            // budget can still pay for would never be looked at: do not keep
            // them (an octopus commit may name any number of parents).
            for p in parents {
                if spent + queue.len() >= budget {
                    break;
                }
                if seen.insert(p) {
                    queue.push_back(p);
                }
            }
        }
    }
    Ok(boundary)
}

async fn read(
    objects: &mut dyn HookObjects,
    id: ObjectId,
    want: ObjectKind,
) -> Result<Vec<u8>, StoreError> {
    match objects.read(id).await? {
        Some((kind, data)) if kind == want => Ok(data),
        Some((kind, _)) => Err(StoreError::Corrupt {
            what: format!("{id} is a {kind:?}, expected a {want:?}"),
        }),
        None => Err(StoreError::Unavailable {
            reason: format!("object {id} of a pushed commit is not readable"),
        }),
    }
}

async fn commit_tree(objects: &mut dyn HookObjects, id: ObjectId) -> Result<ObjectId, StoreError> {
    let data = read(objects, id, ObjectKind::Commit).await?;
    let (tree, _) = parse_commit(&data).map_err(|what| StoreError::Corrupt { what })?;
    Ok(tree)
}

async fn tree_entries(
    objects: &mut dyn HookObjects,
    id: Option<ObjectId>,
) -> Result<Vec<TreeEntry>, StoreError> {
    match id {
        None => Ok(Vec::new()),
        Some(id) => {
            let data = read(objects, id, ObjectKind::Tree).await?;
            parse_tree(&data).map_err(|what| StoreError::Corrupt { what })
        }
    }
}

/// Every file `new` changes against `old`, with a tile's contents loaded
/// when the rules need them. `None` when the diff visits more than
/// `budget` entries. A path whose kind changes (a file replaced by a
/// directory) is a delete plus additions, as `git diff` shows it.
async fn diff(
    shared: &Shared,
    objects: &mut dyn HookObjects,
    old: Option<ObjectId>,
    new: Option<ObjectId>,
    budget: &mut usize,
) -> Result<Option<Vec<FileChange>>, StoreError> {
    let mut out = Vec::new();
    let mut work = vec![(old, new, String::new())];
    while let Some((old, new, prefix)) = work.pop() {
        if old == new {
            continue;
        }
        let a = tree_entries(objects, old).await?;
        let b = tree_entries(objects, new).await?;
        let Some(left) = budget.checked_sub(a.len() + b.len()) else {
            return Ok(None);
        };
        *budget = left;
        let mut by_name: std::collections::BTreeMap<
            &[u8],
            (Option<&TreeEntry>, Option<&TreeEntry>),
        > = Default::default();
        for e in &a {
            by_name.entry(&e.name).or_default().0 = Some(e);
        }
        for e in &b {
            by_name.entry(&e.name).or_default().1 = Some(e);
        }
        for (name, (o, n)) in by_name {
            let path = format!("{prefix}{}", String::from_utf8_lossy(name));
            let dir = |e: &TreeEntry| e.mode == MODE_TREE;
            match (o, n) {
                (Some(o), Some(n)) if o.id == n.id && o.mode == n.mode => {}
                (Some(o), Some(n)) if dir(o) && dir(n) => {
                    work.push((Some(o.id), Some(n.id), format!("{path}/")));
                }
                (o, n) => {
                    if let Some(o) = o {
                        if dir(o) {
                            work.push((Some(o.id), None, format!("{path}/")));
                        } else if !n.is_some_and(|n| !dir(n)) {
                            out.push(FileChange {
                                path: path.clone(),
                                kind: ChangeKind::Delete,
                                mode: None,
                                content: None,
                            });
                        }
                    }
                    if let Some(n) = n {
                        if dir(n) {
                            work.push((None, Some(n.id), format!("{path}/")));
                        } else {
                            let kind = if o.is_some_and(|o| !dir(o)) {
                                ChangeKind::Modify
                            } else {
                                ChangeKind::Add
                            };
                            let content = tile_content(shared, objects, &path, n).await?;
                            out.push(FileChange {
                                path,
                                kind,
                                mode: Some(n.mode),
                                content,
                            });
                        }
                    }
                }
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(Some(out))
}

/// A file's contents, read only when the rules will look at them: a valid
/// tile path with mode 100644. A palette color's blob is known by its ID.
async fn tile_content(
    shared: &Shared,
    objects: &mut dyn HookObjects,
    path: &str,
    e: &TreeEntry,
) -> Result<Option<Vec<u8>>, StoreError> {
    if e.mode != TILE_MODE || rules::parse_tile_path(path, &shared.rules).is_err() {
        return Ok(None);
    }
    if let Some(i) = shared.palette_color(&e.id) {
        return Ok(Some(rules::tile_bytes(
            &shared.rules.palette[usize::from(i)].color,
        )));
    }
    match objects.read(e.id).await? {
        Some((ObjectKind::Blob, data)) => Ok(Some(data)),
        _ => Ok(Some(Vec::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(ms: u64) -> Accepted {
        Accepted {
            actor: Actor::git("paint/t"),
            tiles: Vec::new(),
            full: None,
            at_ms: ms,
        }
    }

    fn oid(n: usize) -> ObjectId {
        crate::git::object_id(ObjectKind::Blob, n.to_string().as_bytes())
    }

    #[test]
    fn updates_that_never_commit_are_bounded_and_the_oldest_go_first() {
        let mut w = Waiting::default();
        // All within the TTL, so only the cap can drop them.
        for n in 0..MAX_ACCEPTED + 10 {
            w.insert(
                Writer::Git,
                "refs/heads/paint/t",
                oid(n),
                at(1_000 + n as u64),
            );
            assert!(w.len() <= MAX_ACCEPTED, "{} waiting", w.len());
        }
        assert_eq!(w.len(), MAX_ACCEPTED);
        for n in 0..10 {
            assert!(
                !w.contains(Writer::Git, "refs/heads/paint/t", oid(n)),
                "oldest {n} kept"
            );
        }
        for n in 10..MAX_ACCEPTED + 10 {
            assert!(
                w.contains(Writer::Git, "refs/heads/paint/t", oid(n)),
                "newer {n} dropped"
            );
        }
        // Re-accepting a waiting update (a verdict recomputed after a lost
        // compare-and-swap) replaces it and evicts nothing.
        w.insert(Writer::Git, "refs/heads/paint/t", oid(10), at(5_000));
        assert_eq!(w.len(), MAX_ACCEPTED);
        assert!(w.contains(Writer::Git, "refs/heads/paint/t", oid(11)));
    }

    #[test]
    fn expired_updates_are_dropped_before_live_ones() {
        let mut w = Waiting::default();
        w.insert(Writer::Git, "refs/heads/a", oid(0), at(0));
        for n in 1..MAX_ACCEPTED {
            w.insert(
                Writer::Git,
                "refs/heads/b",
                oid(n),
                at(ACCEPTED_TTL_MS + n as u64),
            );
        }
        let now = 2 * ACCEPTED_TTL_MS;
        w.insert(Writer::Git, "refs/heads/c", oid(0), at(now));
        assert_eq!(w.len(), MAX_ACCEPTED);
        assert!(
            !w.contains(Writer::Git, "refs/heads/a", oid(0)),
            "expired entry kept"
        );
        assert!(w.take(Writer::Git, "refs/heads/c", oid(0)).is_some());
        assert!(
            w.take(Writer::Git, "refs/heads/c", oid(0)).is_none(),
            "taken once"
        );
    }

    #[test]
    fn the_servers_own_updates_are_never_evicted_by_git_checks() {
        let mut w = Waiting::default();
        w.insert(Writer::Server, "refs/heads/paint/s", oid(0), at(0));
        // Far more Git checks than the cap, all newer, and past the
        // server entry's TTL: neither the cap nor the TTL may take it.
        for n in 0..2 * MAX_ACCEPTED {
            w.insert(
                Writer::Git,
                "refs/heads/paint/t",
                oid(n),
                at(2 * ACCEPTED_TTL_MS + n as u64),
            );
        }
        assert_eq!(w.len(), MAX_ACCEPTED + 1);
        assert!(w.contains(Writer::Server, "refs/heads/paint/s", oid(0)));
        // A Git client's entry for the same update is not the server's.
        assert!(w.take(Writer::Git, "refs/heads/paint/s", oid(0)).is_none());
        assert!(
            w.take(Writer::Server, "refs/heads/paint/s", oid(0))
                .is_some()
        );
    }
}

/// What the walks of a push's history cost: every ID asked of the commit
/// index and every object read is counted, against an in-memory history.
#[cfg(test)]
mod walk_tests {
    use super::*;
    use aspen_server::receive::hook::RangeCommit;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn oid(n: usize) -> ObjectId {
        crate::git::object_id(ObjectKind::Blob, format!("c{n}").as_bytes())
    }

    /// The commit index: what it says about each commit, and how many IDs
    /// it was asked about.
    #[derive(Debug, Default)]
    struct Index {
        rows: HashMap<ObjectId, Indexed>,
        asked: AtomicUsize,
        largest_batch: AtomicUsize,
    }

    impl MainHistory for Index {
        fn classify<'a>(
            &'a self,
            ids: &'a [ObjectId],
            _: &'a mut VersionCache,
        ) -> HookFuture<'a, Result<Vec<Indexed>, StoreError>> {
            self.asked.fetch_add(ids.len(), Ordering::Relaxed);
            self.largest_batch.fetch_max(ids.len(), Ordering::Relaxed);
            let out = ids
                .iter()
                .map(|id| self.rows.get(id).cloned().unwrap_or(Indexed::NotIndexed))
                .collect();
            Box::pin(async move { Ok(out) })
        }
    }

    /// The objects a push can read: commits with their parents. `range`
    /// walks down from the new tip like receive-pack's, reading each commit
    /// it visits and stopping at excluded tips and their ancestors.
    #[derive(Debug, Default)]
    struct Objects {
        parents: HashMap<ObjectId, Vec<ObjectId>>,
        reads: usize,
    }

    impl Objects {
        fn add(&mut self, id: ObjectId, parents: Vec<ObjectId>) {
            self.parents.insert(id, parents);
        }
    }

    impl HookObjects for Objects {
        fn range<'a>(
            &'a mut self,
            new: ObjectId,
            exclude: &'a [ObjectId],
            max_commits: usize,
        ) -> HookFuture<'a, Result<CommitRange, StoreError>> {
            // Ancestors of the excluded tips (not charged: the real walk
            // tests ancestry against them without visiting below them).
            let mut hidden: HashSet<ObjectId> = HashSet::new();
            let mut stack: Vec<ObjectId> = exclude.to_vec();
            while let Some(id) = stack.pop() {
                if hidden.insert(id) {
                    stack.extend(self.parents.get(&id).into_iter().flatten().copied());
                }
            }
            let mut out = Vec::new();
            let mut seen: HashSet<ObjectId> = [new].into();
            let mut queue: VecDeque<ObjectId> = [new].into();
            let result = loop {
                let Some(id) = queue.pop_front() else {
                    out.reverse();
                    break CommitRange::Commits(out);
                };
                if hidden.contains(&id) {
                    continue;
                }
                self.reads += 1;
                let Some(parents) = self.parents.get(&id).cloned() else {
                    break CommitRange::Undecided;
                };
                out.push(RangeCommit {
                    id,
                    parents: parents.clone(),
                });
                if out.len() > max_commits {
                    break CommitRange::TooLong;
                }
                queue.extend(parents.into_iter().filter(|p| seen.insert(*p)));
            };
            Box::pin(async move { Ok(result) })
        }

        fn read(
            &mut self,
            id: ObjectId,
        ) -> HookFuture<'_, Result<Option<aspen_server::receive::hook::Object>, StoreError>>
        {
            self.reads += 1;
            let out = self.parents.get(&id).map(|parents| {
                let data = crate::git::write_commit(oid(0), parents, "T <t@example.com>", 0, "c");
                (ObjectKind::Commit, data)
            });
            Box::pin(async move { Ok(out) })
        }
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    /// One octopus generation of 5,000 parents, none of them indexed or
    /// readable: the walk asks about and reads at most its budget in all,
    /// never the whole generation, in batches the index can take.
    #[test]
    fn the_boundary_walk_stays_within_its_budget_on_a_wide_generation() {
        let budget = 1000;
        let index = Index::default();
        let mut objects = Objects::default();
        let new = oid(1);
        objects.add(new, (10..5010).map(oid).collect());
        let boundary = block_on(main_boundary(&index, &mut objects, new, budget)).unwrap();
        assert!(boundary.is_empty());
        let spent = index.asked.load(Ordering::Relaxed) + objects.reads;
        assert!(spent <= budget, "spent {spent} of {budget}");
        assert!(spent >= budget - 1, "gave up early: {spent}");
        assert!(index.largest_batch.load(Ordering::Relaxed) <= CLASSIFY_BATCH);
    }

    /// A chain of 10,000 indexed commits off `main` with `main` below it:
    /// the walk gives up within its budget, and finds `main` when the
    /// chain is short enough. Each version is looked up once per walk.
    #[test]
    fn the_boundary_walk_gives_up_on_a_deep_chain_off_main() {
        for (chain, budget, found) in [(10_000usize, 1000usize, false), (100, 1000, true)] {
            let mut index = Index::default();
            let mut objects = Objects::default();
            let main = oid(0);
            index.rows.insert(main, Indexed::Main);
            for n in 1..=chain {
                index.rows.insert(oid(n), Indexed::Other(vec![oid(n - 1)]));
            }
            let new = oid(chain + 1);
            objects.add(new, vec![oid(chain)]);
            let boundary = block_on(main_boundary(&index, &mut objects, new, budget)).unwrap();
            assert_eq!(boundary == vec![main], found, "chain {chain}");
            let spent = index.asked.load(Ordering::Relaxed) + objects.reads;
            assert!(spent <= budget, "chain {chain}: spent {spent} of {budget}");
        }
    }

    /// A clone from before a history reset pushes three commits on the old
    /// `main`: the hook reads and asks about the same few dozen commits
    /// whether the old history is 200 or 5,000 commits long, never walking
    /// it down to its root. An ordinary push on today's `main` asks the
    /// index nothing.
    #[test]
    fn a_push_on_an_earlier_main_costs_the_same_whatever_its_length() {
        let max_commits = 1000;
        let mut costs = Vec::new();
        for old in [200usize, 5000] {
            let mut index = Index::default();
            let mut objects = Objects::default();
            // The old history: a chain from its root, all on `main`.
            objects.add(oid(0), Vec::new());
            index.rows.insert(oid(0), Indexed::Main);
            for n in 1..old {
                objects.add(oid(n), vec![oid(n - 1)]);
                index.rows.insert(oid(n), Indexed::Main);
            }
            // Today's `main`: a root of its own.
            let today = oid(1_000_000);
            objects.add(today, Vec::new());
            index.rows.insert(today, Indexed::Main);
            // The push: three commits on the old tip.
            let pushed = [oid(2_000_001), oid(2_000_002), oid(2_000_003)];
            objects.add(pushed[0], vec![oid(old - 1)]);
            objects.add(pushed[1], vec![pushed[0]]);
            objects.add(pushed[2], vec![pushed[1]]);
            let range = block_on(commit_range(
                &index,
                &mut objects,
                pushed[2],
                vec![today],
                max_commits,
            ))
            .unwrap();
            let CommitRange::Commits(commits) = range else {
                panic!("old {old}: {range:?}");
            };
            let ids: Vec<ObjectId> = commits.iter().map(|c| c.id).collect();
            assert_eq!(ids, pushed, "old {old}");
            let cost = index.asked.load(Ordering::Relaxed) + objects.reads;
            assert!(cost <= PROBE_COMMITS + 16, "old {old}: cost {cost}");
            costs.push(cost);

            // An ordinary push: two commits on today's main.
            let mut objects2 = Objects::default();
            objects2.add(today, Vec::new());
            objects2.add(oid(3_000_001), vec![today]);
            objects2.add(oid(3_000_002), vec![oid(3_000_001)]);
            let before = index.asked.load(Ordering::Relaxed);
            let range = block_on(commit_range(
                &index,
                &mut objects2,
                oid(3_000_002),
                vec![today],
                max_commits,
            ))
            .unwrap();
            assert!(matches!(range, CommitRange::Commits(ref c) if c.len() == 2));
            assert_eq!(index.asked.load(Ordering::Relaxed), before, "no lookups");
            assert_eq!(objects2.reads, 2);
        }
        assert_eq!(costs[0], costs[1], "cost grew with the old history");
    }
}

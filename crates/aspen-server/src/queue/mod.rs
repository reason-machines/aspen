//! The merge queue: clients enqueue changes by pushing them to their own
//! ref under `refs/queue/<trunk branch>/`, and the server lands them on
//! trunk in batches, each batch one compare-and-swap (docs/PLAN.md,
//! decisions log, 2026-11-06).
//!
//! Without it, every client that lands on trunk fetches trunk, rebases and
//! pushes, and loses the race to every other client in flight: trunk
//! advances once per fetch-rebase-push cycle however many clients there
//! are. The queue folds every pending change into one trunk update.
//!
//! **Enqueue.** A client pushes its commit through the plain URL to a ref
//! of its own, `refs/queue/main/<anything>` for trunk `refs/heads/main`.
//! Receive-pack treats it as an ordinary write (the namespace is writable
//! while the queue is on, [`with_queue_writable`]); the commit wakes this
//! worker.
//!
//! **A batch.** The worker reads trunk's tip T and the pending entries,
//! oldest first, at most [`QueueConfig::batch`]. For an entry with commit
//! C it finds the base M, the first commit on C's first-parent line that
//! is an ancestor of T, and compares the paths C changed since M with the
//! paths trunk changed since M and the paths earlier entries of the batch
//! changed. Changes are recorded per file (a directory that appears or
//! goes away is its files appearing or going away), so a path conflicts
//! with another when they are equal (unless both made the same change) or
//! one is a directory above the other (a file replaced by a directory). A
//! conflict-free entry becomes one new trunk commit: its tree is the
//! batch's tree with C's changes applied, its parent the batch's previous
//! commit, its author and message C's, its committer the queue. A change
//! of several commits lands squashed into one. An entry that changes
//! nothing lands as nothing (its outcome names the commit it is already
//! in). A conflicting entry is rejected, never dropped.
//!
//! **Finding bases fast.** Each worker keeps, per repository, the newest
//! commits of trunk's first-parent chain (`TrunkChain`), extended by the
//! batches it lands and caught up by a short walk when trunk moved by
//! other pushes. An entry's base is the first commit of its own
//! first-parent line in that set, found by reading only the entry's own
//! commits; the ancestry walk down trunk is the fallback.
//!
//! **Reading a change once.** Each queued change's own pack (its commits,
//! new trees and blobs) is read whole, in one read per change, all at
//! once; trees the worker read or wrote stay cached between batches, and
//! so do the root trees of the trunk commits it wrote, so a batch based on
//! the worker's previous one reads no trunk object at all.
//!
//! **Building a batch fast.** Everything an entry needs that does not
//! depend on the rest of the batch (its commit, base, own changes and the
//! blobs it brings) is prepared for all entries at once, up to 16 at a
//! time, and each level of a tree comparison is one read of every tree it
//! needs; trunk's changes since a base are computed once per base, and the
//! trunk trees the batch will edit are read a level at a time before the
//! entries are placed. Placing entries is then a short pass in queue
//! order that reads nothing. Every read goes to the object store (about
//! 20 ms on S3), so a batch costs about as many round trips as its trees
//! are deep, not several per entry.
//!
//! **Landing.** One atomic in-process push ([`crate::receive::ReceivePack::push`],
//! the same path a client's push takes) updates trunk from T (a
//! compare-and-swap), deletes every processed queue ref against the
//! commit the worker read (a compare-and-swap too), and creates each
//! entry's outcome: `refs/queue-done/main/<anything>` at the trunk commit
//! that landed it, or `refs/queue-rejected/main/<anything>` at a report
//! commit whose message names the original commit and the conflicting
//! paths (`git fetch origin refs/queue-rejected/main/x && git log -1
//! FETCH_HEAD`).
//! Outcome refs are deleted once they are older than
//! [`QueueConfig::outcome_ttl`] (an hour by default), so they do not pile
//! up: every outcome is a ref, and a full ref listing lists them all.
//! Atomic means a batch lands with its queue refs moved or not at all, so
//! an entry lands exactly once. If trunk or a queue ref moved meanwhile
//! (a direct push to trunk, another instance's worker, a client
//! re-pushing its entry), the push is refused as a whole and the batch is
//! rebuilt from what is stored: correctness comes from the
//! compare-and-swaps, so any number of instances may run workers.
//!
//! The report commit's parent is trunk's tip and its tree trunk's tree,
//! not the rejected commit: the in-process push depends on trunk only, so
//! it may name only objects trunk's closure or its own pack holds, and the
//! rejected commit is in neither (its queue ref is deleted in the same
//! push). The client still has its commit.
//!
//! Two workers that build a batch from the same trunk tip and entries in
//! the same second write identical commits, so the second one's push is
//! an exact replay that receive-pack accepts as a no-op (issue #132); both
//! count the batch in their metrics, but it lands once.

mod objects;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, RepoId, UnixMicros};
use aspen_core::ingest::{ObjectKind, SearchPacks};
use aspen_core::meta::{MetaStore, RefListQuery};
use aspen_core::name::{RefName, RefPattern};
use aspen_core::refs::{Ancestry, RefVersion};
use aspen_core::repo::{ForcePolicy, Repo};

use crate::receive::{AnyRef, ReceivePack, ServiceError};
use objects::{Command, Entry, PackBuilder, Tree};

/// Every queue ref starts with this; the next component is trunk's branch
/// name.
pub const QUEUE_PREFIX: &str = "refs/queue/";
/// Outcome refs of landed entries.
pub const DONE_PREFIX: &str = "refs/queue-done/";
/// Outcome refs of rejected entries.
pub const REJECTED_PREFIX: &str = "refs/queue-rejected/";

/// How far back an entry's first-parent line is searched for a commit
/// trunk holds before the entry is rejected as unrelated.
const MAX_BASE_DEPTH: usize = 1000;
/// Atomic pushes tried per batch before the worker leaves it for its next
/// pass (each refusal rebuilds the batch from what is stored).
const MAX_ATTEMPTS: usize = 5;

/// The queue's settings.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Most entries landed by one trunk update.
    pub batch: usize,
    /// Wait between passes when nothing woke the worker.
    pub idle_poll: Duration,
    /// Time between scans of every repository (entries a crashed instance
    /// was woken for are found by the next scan of any instance).
    pub scan_every: Duration,
    /// How long an outcome ref (`refs/queue-done/...`,
    /// `refs/queue-rejected/...`) is kept after it was written; `None`
    /// keeps them for ever. Every outcome is a ref, so without this a busy
    /// repository gains thousands an hour, and everything that lists refs
    /// (a full `ls-refs`, a clone, a mirror) grows without bound.
    pub outcome_ttl: Option<Duration>,
    /// Time between expiry passes over one repository's outcome refs.
    pub expire_every: Duration,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            batch: 64,
            idle_poll: Duration::from_millis(500),
            scan_every: Duration::from_secs(30),
            outcome_ttl: Some(Duration::from_secs(3600)),
            expire_every: Duration::from_secs(60),
        }
    }
}

/// `repo` with its trunk's queue namespace writable, as a client's push
/// sees it while the queue is on.
pub fn with_queue_writable(repo: &Repo) -> Repo {
    let Some(short) = branch(repo) else {
        return repo.clone();
    };
    let p = RefPattern::parse(&format!("{QUEUE_PREFIX}{short}/*")).expect("valid pattern");
    Repo {
        settings: repo.settings.clone().with_writable([p]),
        ..repo.clone()
    }
}

/// Trunk's branch name (`main` for `refs/heads/main`).
fn branch(repo: &Repo) -> Option<&str> {
    repo.settings.trunk().as_str().strip_prefix("refs/heads/")
}

/// Repositories woken by queue pushes since the worker last looked.
#[derive(Debug, Default)]
pub(crate) struct QueueSignal {
    dirty: Mutex<BTreeSet<RepoId>>,
    notify: tokio::sync::Notify,
}

impl QueueSignal {
    pub(crate) fn mark(&self, repo: RepoId) {
        self.dirty.lock().expect("not poisoned").insert(repo);
        self.notify.notify_one();
    }

    fn take(&self) -> BTreeSet<RepoId> {
        std::mem::take(&mut *self.dirty.lock().expect("not poisoned"))
    }
}

/// The queue's counters (`/-/metrics`, docs/OBSERVABILITY.md).
#[derive(Debug, Default)]
pub(crate) struct QueueMetrics {
    merged: AtomicU64,
    rejected: AtomicU64,
    batches: AtomicU64,
    refused: AtomicU64,
    wait_micros: AtomicU64,
    walked: AtomicU64,
    expired: AtomicU64,
    /// Microseconds spent in each [`Stage`] of building and landing
    /// batches.
    stages: [AtomicU64; Stage::ALL.len()],
}

/// The parts of one batch attempt, timed for `/-/metrics`.
#[derive(Debug, Clone, Copy)]
enum Stage {
    /// Listing pending entries, reading trunk.
    List,
    /// Trunk's closure, chain and tree.
    Trunk,
    /// Preparing the entries (their commits, bases, changes and blobs).
    Prepare,
    /// Reading the trunk trees the batch edits.
    TrunkTrees,
    /// Placing the entries: conflicts, new trees and commits.
    Place,
    /// Looking up outcome refs a client reuses.
    Outcomes,
    /// Building the batch's pack.
    Pack,
    /// The atomic in-process push.
    Push,
}

impl Stage {
    const ALL: [Stage; 8] = [
        Stage::List,
        Stage::Trunk,
        Stage::Prepare,
        Stage::TrunkTrees,
        Stage::Place,
        Stage::Outcomes,
        Stage::Pack,
        Stage::Push,
    ];

    fn label(self) -> &'static str {
        match self {
            Stage::List => "list",
            Stage::Trunk => "trunk",
            Stage::Prepare => "prepare",
            Stage::TrunkTrees => "trunk_trees",
            Stage::Place => "place",
            Stage::Outcomes => "outcomes",
            Stage::Pack => "pack",
            Stage::Push => "push",
        }
    }
}

/// Times the stages of one attempt into [`QueueMetrics`].
struct Clock<'a> {
    metrics: &'a QueueMetrics,
    at: std::time::Instant,
}

impl<'a> Clock<'a> {
    fn new(metrics: &'a QueueMetrics) -> Self {
        Self {
            metrics,
            at: std::time::Instant::now(),
        }
    }

    /// Charges the time since the last lap to `stage`.
    fn lap(&mut self, stage: Stage) {
        let now = std::time::Instant::now();
        let us = (now - self.at).as_micros() as u64;
        self.metrics.stages[stage as usize].fetch_add(us, Ordering::Relaxed);
        self.at = now;
    }
}

impl QueueMetrics {
    /// Counts the commits an ancestry walk the queue asked for expanded.
    pub(crate) fn record_walk(&self, commits: u64) {
        self.walked.fetch_add(commits, Ordering::Relaxed);
    }

    pub(crate) fn render(&self, out: &mut String) {
        use crate::metrics::expo;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        expo::counter(
            out,
            "aspen_queue_merged_total",
            "Merge-queue entries landed on trunk.",
            get(&self.merged),
        );
        expo::counter(
            out,
            "aspen_queue_rejected_total",
            "Merge-queue entries rejected (a conflict, or no history in common with trunk).",
            get(&self.rejected),
        );
        expo::counter(
            out,
            "aspen_queue_batches_total",
            "Merge-queue batches landed (one atomic trunk update each); entries per batch is (merged + rejected) / batches.",
            get(&self.batches),
        );
        expo::counter(
            out,
            "aspen_queue_refused_total",
            "Batch pushes refused because trunk or a queue ref moved meanwhile (the batch is rebuilt).",
            get(&self.refused),
        );
        expo::counter(
            out,
            "aspen_queue_wait_seconds_total",
            "Seconds from enqueue (the queue ref's commit) to the outcome landing, summed over entries.",
            get(&self.wait_micros) / 1_000_000,
        );
        expo::counter(
            out,
            "aspen_queue_ancestry_commits_total",
            "Commits the merge queue's ancestry walks expanded (base searches that trunk's recent first-parent chain did not answer), each a commit-index or pack read.",
            get(&self.walked),
        );
        expo::counter(
            out,
            "aspen_queue_outcomes_expired_total",
            "Merge-queue outcome refs deleted after ASPEN_MERGE_QUEUE_OUTCOME_TTL_SECS.",
            get(&self.expired),
        );
        expo::family(
            out,
            "aspen_queue_stage_seconds_total",
            "Time the merge queue spent in each stage of building and landing batches (list, trunk, prepare, trunk_trees, place, outcomes, pack, push), summed over attempts.",
            expo::Kind::Counter,
            Stage::ALL.iter().map(|s| {
                (
                    vec![("stage", s.label())],
                    get(&self.stages[*s as usize]) as f64 / 1e6,
                )
            }),
        );
    }
}

/// What a pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueReport {
    /// Batches landed.
    pub batches: u64,
    /// Entries landed on trunk.
    pub merged: u64,
    /// Entries rejected.
    pub rejected: u64,
    /// Batch pushes refused because trunk or a queue ref moved.
    pub refused: u64,
    /// Commits ancestry walks expanded during the pass (on this instance).
    pub ancestry_commits: u64,
    /// Outcome refs deleted because they outlived the TTL.
    pub expired: u64,
}

impl std::ops::AddAssign for QueueReport {
    fn add_assign(&mut self, o: Self) {
        self.batches += o.batches;
        self.merged += o.merged;
        self.rejected += o.rejected;
        self.refused += o.refused;
        self.ancestry_commits += o.ancestry_commits;
        self.expired += o.expired;
    }
}

/// The merge-queue worker of one instance.
#[derive(Debug)]
pub struct MergeQueue<M, B> {
    svc: Arc<ReceivePack<M, B>>,
    config: QueueConfig,
    /// What the worker keeps about each repository between batches.
    state: Mutex<HashMap<RepoId, RepoState>>,
    /// When each repository's outcome refs were last expired.
    expired_at: Mutex<HashMap<RepoId, std::time::Instant>>,
}

/// Outcome refs one expiry push deletes.
const EXPIRE_PER_PUSH: usize = 500;

/// Commits of trunk's first-parent chain kept per repository.
const CHAIN_LEN: usize = 4096;
/// Most trunk commits walked to catch up with a trunk that moved by pushes
/// the worker did not make; past that the chain starts again from them.
const CHAIN_REFRESH: usize = 256;
/// Commits of an entry's own first-parent line searched for one on the
/// chain before falling back to ancestry walks.
const LINE_DEPTH: usize = 64;

/// The newest [`CHAIN_LEN`] commits of trunk's first-parent chain ending at
/// `tip`. An entry's base is the first commit of its own first-parent line
/// in this set, found by reading the entry's own commits: no walk of trunk
/// per entry. Without it every base search asked "is this an ancestor of
/// trunk?" with a fresh walk down trunk, one commit-index round trip per
/// trunk commit, so clients whose bases lagged further behind trunk made
/// batches slower, which made them lag further (bench/throughput: 128
/// clients, 12 s per batch).
#[derive(Debug, Clone, Default)]
struct TrunkChain {
    tip: Option<ObjectId>,
    set: HashSet<ObjectId>,
    /// Oldest first.
    order: std::collections::VecDeque<ObjectId>,
    /// The root trees of the chain's commits the worker wrote itself, so
    /// neither a batch's start nor an entry based on one reads the commit.
    trees: HashMap<ObjectId, ObjectId>,
}

/// What a worker keeps in memory about one repository between batches.
#[derive(Debug, Default)]
struct RepoState {
    chain: TrunkChain,
    /// Trees read or written, by ID (immutable, so never stale), shared by
    /// every batch; cleared when it holds more than [`TREES_KEPT`].
    trees: Arc<Mutex<HashMap<ObjectId, Tree>>>,
}

/// Trees a worker keeps per repository between batches.
const TREES_KEPT: usize = 100_000;

/// The largest queued change's pack read whole in one read.
const WHOLE_PACK_BYTES: u64 = 8 << 20;

impl TrunkChain {
    /// Adds `id` as the chain's new tip, dropping the oldest commits past
    /// [`CHAIN_LEN`].
    fn push(&mut self, id: ObjectId) {
        if self.set.insert(id) {
            self.order.push_back(id);
        }
        while self.order.len() > CHAIN_LEN {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
                self.trees.remove(&old);
            }
        }
        self.tip = Some(id);
    }

    /// Keeps only `id` and the commits before it.
    fn truncate_to(&mut self, id: ObjectId) {
        while let Some(&last) = self.order.back() {
            if last == id {
                break;
            }
            self.order.pop_back();
            self.set.remove(&last);
            self.trees.remove(&last);
        }
        self.tip = Some(id);
    }
}

/// The repository as the worker pushes to it: trunk plus the queue's
/// namespaces writable, and any ref but trunk forceable (outcome refs a
/// client reuses are replaced; trunk is only ever fast-forwarded).
fn worker_repo(repo: &Repo, short: &str) -> Repo {
    let extra = [QUEUE_PREFIX, DONE_PREFIX, REJECTED_PREFIX]
        .into_iter()
        .map(|p| RefPattern::parse(&format!("{p}{short}/*")).expect("valid pattern"));
    Repo {
        settings: repo
            .settings
            .clone()
            .with_writable(extra)
            .with_allow_force(ForcePolicy::Branches),
        ..repo.clone()
    }
}

/// Changed files and their new entries (`None`: removed).
type Changes = BTreeMap<Vec<u8>, Option<Entry>>;

/// One edit of a tree: a path's components and the new entry there
/// (`None`: removed).
type Edit<'s> = (Vec<&'s [u8]>, Option<Entry>);

/// An entry's fate within a batch.
enum Fate {
    /// Lands as this trunk commit (or is already in it).
    Landed(ObjectId),
    /// Rejected with this report.
    Rejected(String),
    /// Left for a later pass (an ancestry question was undecided).
    Later,
}

impl<M, B> MergeQueue<M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// The worker over `svc`, whose configuration must have the queue on.
    pub fn new(svc: Arc<ReceivePack<M, B>>, config: QueueConfig) -> Self {
        Self {
            svc,
            config,
            state: Mutex::new(HashMap::new()),
            expired_at: Mutex::new(HashMap::new()),
        }
    }

    /// Deletes `repo`'s outcome refs older than the TTL, each with a
    /// compare-and-swap delete against the commit listed (a client that
    /// reused the name meanwhile keeps its new outcome), in pushes of up to
    /// [`EXPIRE_PER_PUSH`] refs. Not atomic: one ref that moved does not
    /// keep the others.
    async fn expire(&self, repo: &Repo) -> Result<u64, StoreError> {
        let (Some(ttl), Some(short)) = (self.config.outcome_ttl, branch(repo)) else {
            return Ok(0);
        };
        let cutoff = UnixMicros::now().0 - i64::try_from(ttl.as_micros()).unwrap_or(i64::MAX);
        let meta = self.svc.meta();
        let mut old = Vec::new();
        let mut after: Option<RefName> = None;
        loop {
            let page = meta
                .list_refs(
                    repo.id,
                    &RefListQuery {
                        prefixes: vec![
                            format!("{DONE_PREFIX}{short}/"),
                            format!("{REJECTED_PREFIX}{short}/"),
                        ],
                        after: after.clone(),
                        limit: 1000,
                    },
                )
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(last.name.clone());
            old.extend(
                page.into_iter()
                    .filter(|v| v.created_at.0 < cutoff)
                    .filter_map(|v| {
                        Some(Command {
                            name: v.name.as_str().to_owned(),
                            old: v.value.tip()?,
                            new: ObjectId::ZERO,
                        })
                    }),
            );
        }
        let target = worker_repo(repo, short);
        let mut deleted = 0;
        for chunk in old.chunks(EXPIRE_PER_PUSH) {
            let body = objects::request(chunk, None, false);
            let out = self
                .svc
                .push(&target, None, &AnyRef, None, std::io::Cursor::new(body))
                .await
                .map_err(|ServiceError::Store(e)| e)?;
            deleted += objects::count_ok(&out.response);
        }
        self.svc
            .queue_metrics
            .expired
            .fetch_add(deleted, Ordering::Relaxed);
        Ok(deleted)
    }

    /// [`Self::expire`] at most every [`QueueConfig::expire_every`] per
    /// repository, unless `now` is set.
    async fn maybe_expire(&self, repo: &Repo, now: bool) -> Result<u64, StoreError> {
        if !now {
            let mut at = self.expired_at.lock().expect("not poisoned");
            if at
                .get(&repo.id)
                .is_some_and(|t| t.elapsed() < self.config.expire_every)
            {
                return Ok(0);
            }
            at.insert(repo.id, std::time::Instant::now());
        }
        self.expire(repo).await
    }

    /// Trunk's first-parent chain ending at `t`, caught up from the chain
    /// last seen: by the worker's own batches (recorded when they land) or
    /// by walking back from `t` to a commit the chain holds.
    async fn trunk_chain(
        &self,
        reader: &Reader<'_, M, B>,
        repo: RepoId,
        t: ObjectId,
    ) -> Result<(Arc<HashSet<ObjectId>>, Arc<HashMap<ObjectId, ObjectId>>), StoreError> {
        let mut chain = self
            .state
            .lock()
            .expect("not poisoned")
            .get(&repo)
            .map(|r| r.chain.clone())
            .unwrap_or_default();
        if chain.tip != Some(t) {
            let mut walked = Vec::new();
            let mut met = None;
            let mut at = Some(t);
            while let Some(x) = at {
                if chain.set.contains(&x) {
                    met = Some(x);
                    break;
                }
                walked.push(x);
                if walked.len() >= CHAIN_REFRESH {
                    break;
                }
                at = reader.first_parent(x).await?;
            }
            match met {
                // Trunk moved on from a commit the chain holds (forward, or
                // forced back to an older one first).
                Some(x) => chain.truncate_to(x),
                None => chain = TrunkChain::default(),
            }
            for id in walked.into_iter().rev() {
                chain.push(id);
            }
        }
        let set = Arc::new(chain.set.clone());
        let trees = Arc::new(chain.trees.clone());
        self.state
            .lock()
            .expect("not poisoned")
            .entry(repo)
            .or_default()
            .chain = chain;
        Ok((set, trees))
    }

    /// Records a landed batch: trunk moved from `from` through `written`.
    fn extend_chain(&self, repo: RepoId, from: ObjectId, written: &[(ObjectId, ObjectId)]) {
        let mut state = self.state.lock().expect("not poisoned");
        if let Some(r) = state.get_mut(&repo)
            && r.chain.tip == Some(from)
        {
            for (id, tree) in written {
                r.chain.push(*id);
                r.chain.trees.insert(*id, *tree);
            }
        }
    }

    /// The repository's tree cache, emptied first if it grew past
    /// [`TREES_KEPT`].
    fn tree_cache(&self, repo: RepoId) -> Arc<Mutex<HashMap<ObjectId, Tree>>> {
        let mut state = self.state.lock().expect("not poisoned");
        let trees = state.entry(repo).or_default().trees.clone();
        {
            let mut t = trees.lock().expect("not poisoned");
            if t.len() > TREES_KEPT {
                t.clear();
            }
        }
        trees
    }

    /// Runs passes until `shutdown`: woken repositories at once, every
    /// repository every [`QueueConfig::scan_every`].
    pub async fn run(
        &self,
        shutdown: impl std::future::Future<Output = ()>,
        mut on_error: impl FnMut(&StoreError),
    ) {
        tokio::pin!(shutdown);
        let mut last_scan: Option<std::time::Instant> = None;
        loop {
            let mut repos = self.svc.queue_signal.take();
            if last_scan.is_none_or(|t| t.elapsed() >= self.config.scan_every) {
                match self.all_repos().await {
                    Ok(all) => {
                        repos.extend(all);
                        last_scan = Some(std::time::Instant::now());
                    }
                    Err(e) => on_error(&e),
                }
            }
            for repo in repos {
                if let Err(e) = self.drain(repo, false).await {
                    on_error(&e);
                }
            }
            tokio::select! {
                _ = &mut shutdown => return,
                _ = self.svc.queue_signal.notify.notified() => {}
                _ = tokio::time::sleep(self.config.idle_poll) => {}
            }
        }
    }

    /// One pass over every repository: lands what is pending until nothing
    /// is, or a batch makes no progress.
    pub async fn run_once(&self) -> Result<QueueReport, StoreError> {
        let walked = || self.svc.queue_metrics.walked.load(Ordering::Relaxed);
        let before = walked();
        let mut report = QueueReport::default();
        for repo in self.all_repos().await? {
            report += self.drain(repo, true).await?;
        }
        report.ancestry_commits = walked() - before;
        Ok(report)
    }

    async fn all_repos(&self) -> Result<Vec<RepoId>, StoreError> {
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let page = self.svc.meta().list_repos(after, 1000).await?;
            let Some(last) = page.last() else { break };
            after = Some(last.id);
            out.extend(page.iter().map(|r| r.id));
        }
        Ok(out)
    }

    /// Lands `repo`'s pending entries, batch after batch, then expires
    /// its old outcome refs (every time when `expire_now`, otherwise at
    /// most every [`QueueConfig::expire_every`]).
    async fn drain(&self, repo: RepoId, expire_now: bool) -> Result<QueueReport, StoreError> {
        let mut report = QueueReport::default();
        let Some(repo) = self.svc.meta().repo_by_id(repo).await? else {
            return Ok(report);
        };
        loop {
            let r = self.batch(&repo).await?;
            let progress = r.merged + r.rejected > 0;
            report += r;
            if !progress {
                break;
            }
        }
        report.expired += self.maybe_expire(&repo, expire_now).await?;
        Ok(report)
    }

    /// Builds and lands one batch, rebuilding it after a refused push.
    async fn batch(&self, repo: &Repo) -> Result<QueueReport, StoreError> {
        let mut report = QueueReport::default();
        let Some(short) = branch(repo) else {
            return Ok(report);
        };
        for _ in 0..MAX_ATTEMPTS {
            match self.attempt(repo, short).await? {
                Attempt::Nothing => return Ok(report),
                Attempt::Landed { merged, rejected } => {
                    report.batches += 1;
                    report.merged += merged;
                    report.rejected += rejected;
                    let m = &self.svc.queue_metrics;
                    m.batches.fetch_add(1, Ordering::Relaxed);
                    m.merged.fetch_add(merged, Ordering::Relaxed);
                    m.rejected.fetch_add(rejected, Ordering::Relaxed);
                    return Ok(report);
                }
                Attempt::Refused(why) => {
                    tracing::debug!(repo = %repo.name, "merge queue: batch refused, rebuilding: {why}");
                    report.refused += 1;
                    self.svc
                        .queue_metrics
                        .refused
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(report)
    }

    async fn attempt(&self, repo: &Repo, short: &str) -> Result<Attempt, StoreError> {
        let meta = self.svc.meta();
        let mut clock = Clock::new(&self.svc.queue_metrics);
        let prefix = format!("{QUEUE_PREFIX}{short}/");
        let mut entries: Vec<RefVersion> = Vec::new();
        let mut after: Option<RefName> = None;
        // Every pending entry is listed, so the oldest ones are taken first
        // whatever their names.
        loop {
            let page = meta
                .list_refs(
                    repo.id,
                    &RefListQuery {
                        prefixes: vec![prefix.clone()],
                        after: after.clone(),
                        limit: 1000,
                    },
                )
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(last.name.clone());
            entries.extend(page.into_iter().filter(|v| v.value.tip().is_some()));
        }
        if entries.is_empty() {
            return Ok(Attempt::Nothing);
        }
        entries.sort_by_key(|v| (v.created_at.0, v.id.get()));
        entries.truncate(self.config.batch.max(1));
        let trunk_name = repo.settings.trunk().clone();
        let Some(trunk) = meta
            .current_version(repo.id, &trunk_name)
            .await?
            .filter(|v| v.value.tip().is_some())
        else {
            // Nothing to land on: entries wait until trunk exists.
            return Ok(Attempt::Nothing);
        };
        let t = trunk.value.tip().expect("live");
        clock.lap(Stage::List);
        let mut reader = Reader {
            svc: &self.svc,
            repo: repo.id,
            trunk_packs: self.svc.closure_of(repo.id, &[trunk.id]).await?,
            trees: self.tree_cache(repo.id),
            chain: Arc::default(),
            chain_trees: Arc::default(),
            local: Mutex::new(HashMap::new()),
        };
        (reader.chain, reader.chain_trees) = self.trunk_chain(&reader, repo.id, t).await?;
        let t_tree = match reader.chain_trees.get(&t) {
            Some(tree) => *tree,
            None => reader.commit_tree(&reader.trunk_packs, t).await?,
        };
        clock.lap(Stage::Trunk);
        // Every entry is prepared at once (its commit, base, own changes and
        // the objects it brings), then the batch is assembled in order.
        let prepared = reader.prepare_all(&trunk, t_tree, &entries).await?;
        clock.lap(Stage::Prepare);
        reader.prepare_trunk(t_tree, &prepared).await?;
        clock.lap(Stage::TrunkTrees);
        let mut b = Batch {
            reader: &reader,
            trunk_name: &trunk_name,
            pack: PackBuilder::default(),
            tip: t,
            root: Some(t_tree),
            done: Changes::new(),
            written: Vec::new(),
            now: UnixMicros::now().0 / 1_000_000,
        };
        let mut fates = Vec::with_capacity(entries.len());
        for p in prepared {
            fates.push(b.entry(p).await?);
        }
        clock.lap(Stage::Place);
        if fates.iter().all(|f| matches!(f, Fate::Later)) {
            return Ok(Attempt::Nothing);
        }
        // Outcome refs a client reuses are cleared first (on their own: a
        // refused clear only means the batch push below is refused too and
        // rebuilt).
        let mut commands = Vec::new();
        if b.tip != t {
            commands.push(Command {
                name: trunk_name.as_str().to_owned(),
                old: t,
                new: b.tip,
            });
        }
        let (mut merged, mut rejected) = (0, 0);
        let mut clears = Vec::new();
        for (e, fate) in entries.iter().zip(&fates) {
            let suffix = &e.name.as_str()[prefix.len()..];
            let (outcome, new) = match fate {
                Fate::Later => continue,
                Fate::Landed(at) => {
                    merged += 1;
                    (format!("{DONE_PREFIX}{short}/{suffix}"), *at)
                }
                Fate::Rejected(report) => {
                    rejected += 1;
                    let text = format!(
                        "Rejected by the merge queue: {}\n\nCommit: {}\n\n{report}\n",
                        e.name,
                        e.value.tip().expect("live")
                    );
                    let note = objects::write_note_commit(t_tree, t, b.now, &text);
                    (
                        format!("{REJECTED_PREFIX}{short}/{suffix}"),
                        b.pack.add(ObjectKind::Commit, note),
                    )
                }
            };
            commands.push(Command {
                name: e.name.as_str().to_owned(),
                old: e.value.tip().expect("live"),
                new: ObjectId::ZERO,
            });
            let name = RefName::new(outcome.clone()).map_err(|err| StoreError::Corrupt {
                what: format!("outcome ref {outcome}: {err}"),
            })?;
            if let Some(cur) = meta
                .current_version(repo.id, &name)
                .await?
                .and_then(|v| v.value.tip())
            {
                clears.push(Command {
                    name: outcome.clone(),
                    old: cur,
                    new: ObjectId::ZERO,
                });
            }
            commands.push(Command {
                name: outcome,
                old: ObjectId::ZERO,
                new,
            });
        }
        let target = worker_repo(repo, short);
        clock.lap(Stage::Outcomes);
        if !clears.is_empty() {
            let body = objects::atomic_request(&clears, None);
            if let Err(why) = self.push(&target, body).await? {
                return Ok(Attempt::Refused(why));
            }
        }
        // Compressing and hashing the batch's objects is CPU work: off the
        // async workers that serve every request.
        let pack = if b.pack.is_empty() {
            None
        } else {
            let pb = std::mem::take(&mut b.pack);
            Some(
                tokio::task::spawn_blocking(move || pb.finish())
                    .await
                    .map_err(|e| StoreError::Unavailable {
                        reason: format!("merge queue: building the batch's pack: {e}"),
                    })?,
            )
        };
        let body = objects::atomic_request(&commands, pack);
        let written = std::mem::take(&mut b.written);
        clock.lap(Stage::Pack);
        let pushed = self.push(&target, body).await?;
        clock.lap(Stage::Push);
        match pushed {
            Ok(()) => {
                self.extend_chain(repo.id, t, &written);
                let now = UnixMicros::now().0;
                let waited: i64 = entries
                    .iter()
                    .zip(&fates)
                    .filter(|(_, f)| !matches!(f, Fate::Later))
                    .map(|(e, _)| (now - e.created_at.0).max(0))
                    .sum();
                self.svc
                    .queue_metrics
                    .wait_micros
                    .fetch_add(waited as u64, Ordering::Relaxed);
                Ok(Attempt::Landed { merged, rejected })
            }
            Err(why) => Ok(Attempt::Refused(why)),
        }
    }

    /// Pushes `body` in process; `Ok(Err(reason))` when it was refused.
    async fn push(&self, repo: &Repo, body: Vec<u8>) -> Result<Result<(), String>, StoreError> {
        let out = self
            .svc
            .push(repo, None, &AnyRef, None, std::io::Cursor::new(body))
            .await
            .map_err(|ServiceError::Store(e)| e)?;
        Ok(objects::parse_report(&out.response))
    }
}

enum Attempt {
    Nothing,
    Landed { merged: u64, rejected: u64 },
    Refused(String),
}

/// How many entries of a batch are prepared at once.
const PREPARE_AT_ONCE: usize = 256;

/// What one entry needs before it can be placed in the batch, found
/// independently of the other entries (and so for all of them at once).
enum Prepared {
    /// Left for a later pass (an ancestry question was undecided).
    Later,
    /// Rejected whatever the rest of the batch does.
    Rejected(String),
    /// Trunk already holds the change, or it changes nothing.
    Nothing,
    /// A change to place.
    Change(Box<Change>),
}

/// One entry's change since its base.
struct Change {
    commit: objects::Commit,
    /// The base and its tree.
    m: ObjectId,
    m_tree: ObjectId,
    /// Files changed since the base.
    mine: Changes,
    /// The blobs those files point at.
    blobs: HashMap<ObjectId, (ObjectKind, Vec<u8>)>,
    /// Trunk's changes since the base.
    theirs: Arc<Changes>,
}

/// Object reads for one batch, shared by its entries' preparation. Each
/// read is one prefetch of many objects at once: reads go to the blob
/// store, so a batch costs a number of round trips that depends on the
/// depth of the trees it changes, not on how many entries it holds (it
/// used to be several round trips per entry, one entry after another).
struct Reader<'a, M, B> {
    svc: &'a ReceivePack<M, B>,
    repo: RepoId,
    trunk_packs: SearchPacks,
    /// Trees read or written so far (the worker's, kept between batches).
    trees: Arc<Mutex<HashMap<ObjectId, Tree>>>,
    /// Trunk's recent first-parent chain ([`TrunkChain`]), and the root
    /// trees of its commits the worker wrote.
    chain: Arc<HashSet<ObjectId>>,
    chain_trees: Arc<HashMap<ObjectId, ObjectId>>,
    /// Every object of the batch's entries' own packs, each pack read whole
    /// in one read ([`Reader::load_entry_packs`]).
    local: Mutex<HashMap<ObjectId, (ObjectKind, Vec<u8>)>>,
}

impl<M, B> Reader<'_, M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// The objects `ids` from `packs` (or trunk's), in one read each;
    /// missing is corruption: every object read is reachable from a
    /// stored version.
    async fn read_all(
        &self,
        packs: &SearchPacks,
        mut ids: Vec<ObjectId>,
    ) -> Result<HashMap<ObjectId, (ObjectKind, Vec<u8>)>, StoreError> {
        ids.sort_unstable();
        ids.dedup();
        let mut out = HashMap::with_capacity(ids.len());
        {
            let local = self.local.lock().expect("not poisoned");
            ids.retain(|id| match local.get(id) {
                Some(o) => {
                    out.insert(*id, o.clone());
                    false
                }
                None => true,
            });
        }
        if ids.is_empty() {
            return Ok(out);
        }
        out.extend(self.svc.read_many(packs, ids.clone()).await?);
        let missing: Vec<ObjectId> = ids.into_iter().filter(|i| !out.contains_key(i)).collect();
        if !missing.is_empty() && packs != &self.trunk_packs {
            out.extend(
                self.svc
                    .read_many(&self.trunk_packs, missing.clone())
                    .await?,
            );
        }
        if let Some(id) = missing.iter().find(|i| !out.contains_key(i)) {
            return Err(corrupt(*id)(
                "not in the closure it was reached from".into(),
            ));
        }
        Ok(out)
    }

    async fn read(
        &self,
        packs: &SearchPacks,
        id: ObjectId,
    ) -> Result<(ObjectKind, Vec<u8>), StoreError> {
        let mut got = self.read_all(packs, vec![id]).await?;
        Ok(got.remove(&id).expect("read_all returns every id or fails"))
    }

    /// The first parent of trunk commit `id`, from the commit index (one
    /// metadata read) or else trunk's packs.
    async fn first_parent(&self, id: ObjectId) -> Result<Option<ObjectId>, StoreError> {
        use aspen_core::ingest::SortedIds;
        use aspen_core::meta::CommitLookup;
        let rows = self
            .svc
            .meta()
            .lookup_commits(self.repo, &SortedIds::from_unsorted(vec![id]))
            .await?;
        if let Some(CommitLookup::Indexed(row)) = rows.first() {
            return Ok(row.parents.first().copied());
        }
        match self.commit(&self.trunk_packs, id).await? {
            Some(c) => Ok(c.parents.first().copied()),
            None => Err(corrupt(id)("a trunk commit that is not a commit".into())),
        }
    }

    async fn commit(
        &self,
        packs: &SearchPacks,
        id: ObjectId,
    ) -> Result<Option<objects::Commit>, StoreError> {
        match self.read(packs, id).await? {
            (ObjectKind::Commit, data) => {
                Ok(Some(objects::parse_commit(&data).map_err(corrupt(id))?))
            }
            _ => Ok(None),
        }
    }

    /// The tree of commit `id`.
    async fn commit_tree(&self, packs: &SearchPacks, id: ObjectId) -> Result<ObjectId, StoreError> {
        match self.commit(packs, id).await? {
            Some(c) => Ok(c.tree),
            None => Err(corrupt(id)("not a commit".into())),
        }
    }

    fn cached(&self, id: &ObjectId) -> Option<Tree> {
        self.trees.lock().expect("not poisoned").get(id).cloned()
    }

    fn remember(&self, id: ObjectId, t: Tree) {
        self.trees.lock().expect("not poisoned").insert(id, t);
    }

    /// Reads every tree of `ids` not cached yet, in one read.
    async fn load_trees(&self, packs: &SearchPacks, ids: &[ObjectId]) -> Result<(), StoreError> {
        let want: Vec<ObjectId> = {
            let trees = self.trees.lock().expect("not poisoned");
            ids.iter()
                .filter(|i| !trees.contains_key(*i))
                .copied()
                .collect()
        };
        if want.is_empty() {
            return Ok(());
        }
        for (id, (kind, data)) in self.read_all(packs, want).await? {
            if kind != ObjectKind::Tree {
                return Err(corrupt(id)("not a tree".into()));
            }
            self.remember(id, objects::parse_tree(&data).map_err(corrupt(id))?);
        }
        Ok(())
    }

    async fn tree(&self, packs: &SearchPacks, id: ObjectId) -> Result<Tree, StoreError> {
        if let Some(t) = self.cached(&id) {
            return Ok(t);
        }
        self.load_trees(packs, &[id]).await?;
        Ok(self.cached(&id).expect("loaded"))
    }

    /// The changes from tree `a` to tree `b`, per file, reading the trees
    /// of each level of the comparison in one read.
    async fn diff(
        &self,
        packs: &SearchPacks,
        a: Option<ObjectId>,
        b: Option<ObjectId>,
    ) -> Result<Changes, StoreError> {
        /// What is left to compare: a pair of trees, or every file below
        /// a directory that appears (`new`) or goes away.
        enum Step {
            Pair(Vec<u8>, Option<ObjectId>, Option<ObjectId>),
            Leaves {
                at: Vec<u8>,
                id: ObjectId,
                new: bool,
            },
        }
        let mut out = Changes::new();
        let mut level = vec![Step::Pair(Vec::new(), a, b)];
        while !level.is_empty() {
            let ids: Vec<ObjectId> = level
                .iter()
                .flat_map(|s| match s {
                    Step::Pair(_, a, b) if a != b => vec![*a, *b],
                    Step::Pair(..) => vec![],
                    Step::Leaves { id, .. } => vec![Some(*id)],
                })
                .flatten()
                .collect();
            self.load_trees(packs, &ids).await?;
            let tree = |id: Option<ObjectId>| match id {
                Some(id) => self.cached(&id).expect("loaded"),
                None => Tree::new(),
            };
            let mut next = Vec::new();
            for step in level {
                match step {
                    Step::Pair(_, a, b) if a == b => {}
                    Step::Pair(prefix, a, b) => {
                        let (ta, tb) = (tree(a), tree(b));
                        let names: BTreeSet<&Vec<u8>> = ta.keys().chain(tb.keys()).collect();
                        for name in names {
                            let (ea, eb) = (ta.get(name), tb.get(name));
                            if ea == eb {
                                continue;
                            }
                            let path = join(&prefix, name);
                            match (ea, eb) {
                                (Some(x), Some(y)) if x.is_tree() && y.is_tree() => {
                                    next.push(Step::Pair(path, Some(x.id), Some(y.id)));
                                }
                                _ => {
                                    // Changes are recorded per file: a
                                    // directory that appears or goes away is
                                    // its files appearing or going away, so
                                    // two changes adding different files to
                                    // one new directory do not conflict.
                                    match ea {
                                        Some(x) if x.is_tree() => next.push(Step::Leaves {
                                            at: path.clone(),
                                            id: x.id,
                                            new: false,
                                        }),
                                        Some(_) if eb.is_none_or(Entry::is_tree) => {
                                            out.insert(path.clone(), None);
                                        }
                                        _ => {}
                                    }
                                    match eb {
                                        Some(y) if y.is_tree() => next.push(Step::Leaves {
                                            at: path,
                                            id: y.id,
                                            new: true,
                                        }),
                                        Some(y) => {
                                            out.insert(path, Some(*y));
                                        }
                                        None => {}
                                    }
                                }
                            }
                        }
                    }
                    Step::Leaves { at, id, new } => {
                        for (name, e) in tree(Some(id)) {
                            let path = join(&at, &name);
                            if e.is_tree() {
                                next.push(Step::Leaves {
                                    at: path,
                                    id: e.id,
                                    new,
                                });
                            } else if new {
                                out.insert(path, Some(e));
                            } else {
                                // A file that went away, unless the new
                                // side put something at its path (recorded
                                // by that side).
                                out.entry(path).or_insert(None);
                            }
                        }
                    }
                }
            }
            level = next;
        }
        Ok(out)
    }

    /// Reads each entry's own pack whole, all at once (one read each), and
    /// keeps every object in [`Reader::local`]: a queued change's pack
    /// holds its commits, new trees and blobs, so preparing it then reads
    /// nothing else from the blob store except trunk trees not yet cached.
    /// A pack over [`WHOLE_PACK_BYTES`], or one that does not read or
    /// parse, is skipped: its objects are read one at a time as before.
    async fn load_entry_packs(&self, entries: &[RefVersion]) -> Result<(), StoreError> {
        use aspen_core::blob::ByteRange;
        use futures_util::stream::{self, StreamExt};
        let mut ids: Vec<_> = entries.iter().filter_map(|e| e.value.pack()).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.is_empty() {
            return Ok(());
        }
        let rows = self.svc.meta().packs(&ids).await?;
        let blobs = self.svc.blobs();
        let mut reads = Vec::new();
        for row in rows.iter().filter(|r| r.pack.size <= WHOLE_PACK_BYTES) {
            let Some(range) = ByteRange::new(0, row.pack.size) else {
                continue;
            };
            let key = row.keys().pack;
            reads.push(async move { blobs.read_exact_range(&key, range).await });
        }
        let packs: Vec<Vec<u8>> = stream::iter(reads)
            .buffer_unordered(PREPARE_AT_ONCE)
            .filter_map(|r| async move { r.ok() })
            .collect()
            .await;
        let max = self.svc.max_object_bytes();
        let parsed = tokio::task::spawn_blocking(move || {
            packs
                .iter()
                .filter_map(|p| objects::parse_pack(p, max).ok())
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|e| StoreError::Unavailable {
            reason: format!("merge queue: reading queued packs: {e}"),
        })?;
        let mut local = self.local.lock().expect("not poisoned");
        let mut trees = self.trees.lock().expect("not poisoned");
        for objs in parsed {
            for (id, (kind, data)) in objs {
                if kind == ObjectKind::Tree
                    && let Ok(t) = objects::parse_tree(&data)
                {
                    trees.insert(id, t);
                }
                local.insert(id, (kind, data));
            }
        }
        Ok(())
    }

    /// Prepares every entry, [`PREPARE_AT_ONCE`] at a time, in the
    /// entries' order. Trunk's changes since a base are computed once per
    /// base and shared.
    async fn prepare_all(
        &self,
        trunk: &RefVersion,
        trunk_tree: ObjectId,
        entries: &[RefVersion],
    ) -> Result<Vec<Prepared>, StoreError> {
        use futures_util::stream::{self, StreamExt, TryStreamExt};
        self.load_entry_packs(entries).await?;
        let mut jobs = Vec::with_capacity(entries.len());
        for e in entries {
            jobs.push(self.prepare(trunk, e));
        }
        let mut prepared: Vec<Prepared> = stream::iter(jobs)
            .buffered(PREPARE_AT_ONCE)
            .try_collect()
            .await?;
        // Trunk's changes since each base, once per base, all at once.
        let mut bases: Vec<(ObjectId, ObjectId)> = prepared
            .iter()
            .filter_map(|p| match p {
                Prepared::Change(c) => Some((c.m, c.m_tree)),
                _ => None,
            })
            .collect();
        bases.sort_unstable();
        bases.dedup();
        let mut diffs = Vec::with_capacity(bases.len());
        for &(_, m_tree) in &bases {
            diffs.push(self.diff(&self.trunk_packs, Some(m_tree), Some(trunk_tree)));
        }
        let diffs: Vec<Changes> = stream::iter(diffs)
            .buffered(PREPARE_AT_ONCE)
            .try_collect()
            .await?;
        let since: HashMap<ObjectId, Arc<Changes>> = bases
            .iter()
            .map(|(m, _)| *m)
            .zip(diffs.into_iter().map(Arc::new))
            .collect();
        for p in &mut prepared {
            if let Prepared::Change(c) = p {
                c.theirs = since[&c.m].clone();
            }
        }
        Ok(prepared)
    }

    /// One entry's commit, base, changes and the blobs it brings.
    async fn prepare(&self, trunk: &RefVersion, e: &RefVersion) -> Result<Prepared, StoreError> {
        let c = e.value.tip().expect("live");
        let packs = self.svc.closure_of(self.repo, &[e.id]).await?;
        let Some(commit) = self.commit(&packs, c).await? else {
            return Ok(Prepared::Rejected(format!("{c} is not a commit")));
        };
        let t = trunk.value.tip().expect("live");
        let versions = [trunk.id, e.id];
        // The base: the first commit on C's first-parent line that trunk
        // holds (C itself if trunk already has it). Usually it is on
        // trunk's recent first-parent chain, found reading only C's line.
        // A commit trunk holds only through a merge's side is not on the
        // chain; taking an older base then makes the entry's changes
        // include ones trunk already has, which compare equal (no
        // conflict) unless trunk changed those paths again (a conflict,
        // reported): never a wrong tree.
        let mut base = None;
        let mut at = (c, commit.clone());
        let mut known_tree = None;
        for _ in 0..LINE_DEPTH {
            if self.chain.contains(&at.0) {
                base = Some(at.0);
                break;
            }
            let Some(&p) = at.1.parents.first() else {
                break;
            };
            // A parent the worker landed: its tree is known, no read.
            if self.chain.contains(&p)
                && let Some(tree) = self.chain_trees.get(&p)
            {
                base = Some(p);
                known_tree = Some(*tree);
                break;
            }
            match self.commit(&packs, p).await? {
                Some(pc) => at = (p, pc),
                None => return Err(corrupt(p)("a parent that is not a commit".into())),
            }
        }
        if base.is_none() {
            at = (c, commit.clone());
        }
        for _ in 0..MAX_BASE_DEPTH {
            if base.is_some() {
                break;
            }
            match self.svc.is_ancestor(self.repo, at.0, t, &versions).await? {
                Ancestry::Descendant => {
                    base = Some(at.0);
                    break;
                }
                Ancestry::Undecided => return Ok(Prepared::Later),
                Ancestry::NotDescendant => {}
            }
            let Some(&p) = at.1.parents.first() else {
                break;
            };
            match self.commit(&packs, p).await? {
                Some(pc) => at = (p, pc),
                None => return Err(corrupt(p)("a parent that is not a commit".into())),
            }
        }
        let Some(m) = base else {
            return Ok(Prepared::Rejected(format!(
                "{c} has no commit in common with {} within {MAX_BASE_DEPTH} first parents",
                trunk.name
            )));
        };
        if m == c {
            return Ok(Prepared::Nothing);
        }
        // The loop stopped at the base: `at` is its commit, unless the
        // base's tree was known without reading it.
        let m_tree = known_tree.unwrap_or(at.1.tree);
        let mine = self.diff(&packs, Some(m_tree), Some(commit.tree)).await?;
        if mine.is_empty() {
            return Ok(Prepared::Nothing);
        }
        let ids: Vec<ObjectId> = mine
            .values()
            .flatten()
            .filter(|e| e.mode != objects::MODE_GITLINK && !e.is_tree())
            .map(|e| e.id)
            .collect();
        let blobs = if ids.is_empty() {
            HashMap::new()
        } else {
            self.read_all(&packs, ids).await?
        };
        Ok(Prepared::Change(Box::new(Change {
            commit,
            m,
            m_tree,
            mine,
            blobs,
            theirs: Arc::default(),
        })))
    }

    /// Reads, a level at a time, the trees of trunk's tree `root` that the
    /// prepared changes will edit, so placing them reads nothing.
    async fn prepare_trunk(&self, root: ObjectId, prepared: &[Prepared]) -> Result<(), StoreError> {
        let mut dirs: BTreeSet<Vec<u8>> = BTreeSet::new();
        for p in prepared {
            if let Prepared::Change(c) = p {
                for path in c.mine.keys() {
                    for (i, &byte) in path.iter().enumerate() {
                        if byte == b'/' {
                            dirs.insert(path[..i].to_vec());
                        }
                    }
                }
            }
        }
        let mut level = vec![(Vec::new(), root)];
        while !level.is_empty() {
            let ids: Vec<ObjectId> = level.iter().map(|(_, id)| *id).collect();
            self.load_trees(&self.trunk_packs, &ids).await?;
            let mut next = Vec::new();
            for (at, id) in level {
                for (name, e) in self.cached(&id).expect("loaded") {
                    let path = if at.is_empty() {
                        name.clone()
                    } else {
                        join(&at, &name)
                    };
                    if e.is_tree() && dirs.contains(&path) {
                        next.push((path, e.id));
                    }
                }
            }
            level = next;
        }
        Ok(())
    }
}

fn join(prefix: &[u8], name: &[u8]) -> Vec<u8> {
    let mut path = prefix.to_vec();
    if !path.is_empty() {
        path.push(b'/');
    }
    path.extend_from_slice(name);
    path
}

/// One batch being assembled, entry after entry, from prepared entries.
struct Batch<'a, 'r, M, B> {
    reader: &'r Reader<'a, M, B>,
    trunk_name: &'r RefName,
    /// Objects the batch's trunk update brings.
    pack: PackBuilder,
    /// The batch's trunk tip and root tree so far.
    tip: ObjectId,
    root: Option<ObjectId>,
    /// Paths changed by entries landed earlier in the batch.
    done: Changes,
    /// The trunk commits the batch wrote, in order, with their trees.
    written: Vec<(ObjectId, ObjectId)>,
    now: i64,
}

impl<M, B> Batch<'_, '_, M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    async fn entry(&mut self, p: Prepared) -> Result<Fate, StoreError> {
        let c = match p {
            Prepared::Later => return Ok(Fate::Later),
            Prepared::Rejected(r) => return Ok(Fate::Rejected(r)),
            Prepared::Nothing => return Ok(Fate::Landed(self.tip)),
            Prepared::Change(c) => c,
        };
        let Change {
            commit,
            m,
            mine,
            blobs,
            theirs,
            ..
        } = *c;
        let mut conflicts = conflicts(&mine, &theirs);
        conflicts.extend(conflicts_with(&mine, &self.done));
        if !conflicts.is_empty() {
            conflicts.sort();
            conflicts.dedup();
            let list: Vec<String> = conflicts
                .iter()
                .map(|p| String::from_utf8_lossy(p).into_owned())
                .collect();
            return Ok(Fate::Rejected(format!(
                "Conflicting paths (changed on {} since {m}, or by an entry landed before it):\n{}",
                self.trunk_name,
                list.join("\n")
            )));
        }
        // The objects the change brings, then the new trees and commit.
        for (id, (kind, data)) in blobs {
            if !self.pack.has(&id) {
                self.pack.add(kind, data);
            }
        }
        let root = self.root.expect("set");
        let edits: Vec<Edit<'_>> = mine
            .iter()
            .map(|(p, e)| (p.split(|&b| b == b'/').collect(), *e))
            .collect();
        let new_root = match self.apply(Some(root), &edits).await? {
            Some(id) => id,
            None => self.write_tree(Tree::new()),
        };
        let data = objects::write_commit(&commit, new_root, &[self.tip], self.now);
        self.tip = self.pack.add(ObjectKind::Commit, data);
        self.written.push((self.tip, new_root));
        self.root = Some(new_root);
        self.done.extend(mine);
        Ok(Fate::Landed(self.tip))
    }

    fn write_tree(&mut self, t: Tree) -> ObjectId {
        let id = self.pack.add(ObjectKind::Tree, objects::write_tree(&t));
        self.reader.remember(id, t);
        id
    }

    async fn apply_owned<'s>(
        &'s mut self,
        id: Option<ObjectId>,
        edits: Vec<Edit<'s>>,
    ) -> Result<Option<ObjectId>, StoreError> {
        self.apply(id, &edits).await
    }

    /// Applies `edits` (path components and new entries) below tree `id`
    /// (trunk's, already read by [`Reader::prepare_trunk`], or one the
    /// batch wrote); `None` when the result is empty.
    fn apply<'s>(
        &'s mut self,
        id: Option<ObjectId>,
        edits: &'s [Edit<'s>],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<ObjectId>, StoreError>> + Send + 's>,
    >
    where
        M: 's,
        B: 's,
    {
        Box::pin(async move {
            let mut t = match id {
                Some(id) => {
                    let packs = self.reader.trunk_packs.clone();
                    self.reader.tree(&packs, id).await?
                }
                None => Tree::new(),
            };
            let mut by_name: BTreeMap<&[u8], Vec<Edit<'_>>> = BTreeMap::new();
            for (path, e) in edits {
                by_name
                    .entry(path[0])
                    .or_default()
                    .push((path[1..].to_vec(), *e));
            }
            for (name, sub) in by_name {
                let exact = sub
                    .iter()
                    .find(|(rest, _)| rest.is_empty())
                    .map(|(_, e)| *e);
                let deeper: Vec<_> = sub
                    .into_iter()
                    .filter(|(rest, _)| !rest.is_empty())
                    .collect();
                match exact {
                    // A file (or the whole entry) replaced: what was below
                    // the name is gone with it.
                    Some(Some(e)) => {
                        t.insert(name.to_vec(), e);
                        continue;
                    }
                    Some(None) => {
                        t.remove(name);
                    }
                    None => {}
                }
                if deeper.is_empty() {
                    continue;
                }
                let below = t.get(name).filter(|e| e.is_tree()).map(|e| e.id);
                match self.apply_owned(below, deeper).await? {
                    Some(id) => {
                        t.insert(
                            name.to_vec(),
                            Entry {
                                mode: objects::MODE_TREE,
                                id,
                            },
                        );
                    }
                    None => {
                        t.remove(name);
                    }
                }
            }
            if t.is_empty() {
                return Ok(None);
            }
            Ok(Some(self.write_tree(t)))
        })
    }
}

/// Paths of `mine` that conflict with `theirs`: the same path changed
/// differently, or one path a directory above the other.
fn conflicts(mine: &Changes, theirs: &Changes) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for (p, e) in mine {
        if let Some(o) = theirs.get(p) {
            if o != e {
                out.push(p.clone());
            }
            continue;
        }
        // A directory above `p` changed on the other side.
        let above = p
            .iter()
            .enumerate()
            .filter(|&(_, &b)| b == b'/')
            .any(|(i, _)| theirs.contains_key(&p[..i]));
        // Something below `p` changed on the other side.
        let mut lo = p.clone();
        lo.push(b'/');
        let mut hi = p.clone();
        hi.push(b'/' + 1);
        let below = theirs.range(lo..hi).next().is_some();
        if above || below {
            out.push(p.clone());
        }
    }
    out
}

/// [`conflicts`] against earlier entries of the batch, which conflict even
/// when they made the same change (each entry must land as its own
/// change).
fn conflicts_with(mine: &Changes, done: &Changes) -> Vec<Vec<u8>> {
    let mut out = conflicts(mine, done);
    out.extend(mine.keys().filter(|p| done.contains_key(*p)).cloned());
    out
}

fn corrupt(id: ObjectId) -> impl Fn(String) -> StoreError {
    move |e| StoreError::Corrupt {
        what: format!("merge queue: object {id}: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(n: u8) -> Option<Entry> {
        Some(Entry {
            mode: 0o100644,
            id: ObjectId::from_bytes([n; 20]),
        })
    }

    fn changes(items: &[(&str, Option<Entry>)]) -> Changes {
        items
            .iter()
            .map(|(p, e)| (p.as_bytes().to_vec(), *e))
            .collect()
    }

    #[test]
    fn paths_conflict_when_equal_or_nested() {
        let theirs = changes(&[("a/b.txt", e(1)), ("dir", None), ("same", e(3))]);
        let mine = changes(&[
            ("a/c.txt", e(2)),
            ("a/b.txt", e(9)),
            ("dir/x", e(2)),
            ("a", None),
            ("same", e(3)),
            ("dirx", e(4)),
        ]);
        let got: Vec<String> = conflicts(&mine, &theirs)
            .into_iter()
            .map(|p| String::from_utf8(p).unwrap())
            .collect();
        assert_eq!(got, vec!["a", "a/b.txt", "dir/x"]);
    }

    #[test]
    fn earlier_entries_conflict_even_with_the_same_change() {
        let done = changes(&[("x", e(1))]);
        assert_eq!(
            conflicts_with(&changes(&[("x", e(1))]), &done),
            vec![b"x".to_vec()]
        );
        assert!(conflicts_with(&changes(&[("y", e(1))]), &done).is_empty());
    }
}

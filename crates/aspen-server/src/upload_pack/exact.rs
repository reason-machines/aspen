//! Exact cached shallow packs (node C8).
//!
//! Without this module a shallow fetch is served the full closure of its
//! wants plus an exact `shallow-info` (S5): correct, but every
//! `git fetch --depth=1` of a large repository costs a full clone, and a CI
//! fleet sends a thousand of them for each push. Here the server builds,
//! once per ref version and depth, the pack such a fetch needs (the
//! commits fewer than `depth` parents below the tip, and every tree and
//! blob their trees reach), stores it as an ordinary pack, records it in
//! `shallow_packs`, and serves it to every later shallow fetch of that tip.
//!
//! # Which requests use it
//!
//! A shallow request uses exact packs when all of these hold; otherwise it
//! gets the full closure, exactly as before:
//!
//! - the server has a [`ShallowCacheConfig`];
//! - it asks for a plain depth (`deepen <n>`, `1 <= n <=`
//!   [`ShallowCacheConfig::max_depth`]), without `deepen-relative`,
//!   `deepen-since` or `deepen-not`;
//! - it has at most [`ShallowCacheConfig::max_wants`] wants, and each want
//!   is the tip of the version its `commit_index` row (or tag ref) names.
//!
//! Each want then gets its own version's pack, built if missing; a want
//! whose region is its whole closure (a young repository, or a depth past
//! the root) gets its closure, which is then exact, and nothing is stored
//! for it. The
//! client's own shallow lines and haves do not matter: the pack for a want
//! holds everything a client with nothing needs for that boundary, which
//! is a superset of what any client needs (a client that already has some
//! of it receives those objects again, as with the full closure). Several
//! wants get the union of their packs, stitched with the usual keep-first
//! deduplication. That is exact too: a commit is fewer than `n` parents
//! below a set of wants exactly when it is fewer than `n` parents below one
//! of them, so the region of the set is the union of the wants' regions,
//! and `shallow-info`, which is computed for the whole request as before,
//! draws the boundary of that union.
//!
//! # Building
//!
//! A missing pack is built in a task of its own: the walk that writes
//! `shallow-info` gives the region, and
//! `aspen_compact::Compactor::build_shallow_pack` writes, verifies,
//! registers, uploads and records the pack. Its sources are the version's
//! layered answer (below) when it has one, else the packs of the version's
//! full-closure plan. The request that started the build, and every
//! request for the same version and depth that arrives meanwhile, waits
//! for it; a fetch storm after a push builds the pack once and serves it to
//! all. At most [`ShallowCacheConfig::max_builds`] builds run at once per
//! process. A build that fails or finds the closure over
//! [`ShallowCacheConfig::max_source_bytes`] is not retried for
//! [`ShallowCacheConfig::retry_after`]; one that finds the closure already
//! exact is remembered for a day. Several servers may build the same pack
//! at once; the bytes are the same and the first recording wins.
//!
//! # Layered answers
//!
//! A build copies and verifies a whole snapshot, so on a large repository
//! it takes longer than a fetch should wait (about 20 s for a 300,000-file
//! trunk), and a busy trunk has a new tip on every landing. A request that
//! has waited [`ShallowCacheConfig::layered_wait`] for a build, or that
//! finds no build slot free or its version's last build failed, is
//! therefore served the version's **layered answer** when it has one
//! (`super::layered`; single-want requests only): the exact pack recorded
//! for an earlier version of the same ref at the same depth, without that
//! version's commits, plus a small patch pack of everything else the region
//! needs. That is the region plus the earlier snapshot's replaced trees
//! and blobs, never the full history;
//! the build goes on for later fetches. A request without a layered answer
//! waits up to [`ShallowCacheConfig::build_wait`] in all, and is served the
//! full closure when the build does not end in time, no slot is free or the
//! last build failed (a cold start: no version of the ref within reach has
//! a recorded pack).
//!
//! # Bounds (issue #108)
//!
//! Each repository's recorded packs stay within
//! [`ShallowCacheConfig::max_repo_bytes`]: recording a new pack evicts the
//! repository's least recently used ones until it fits, and a pack larger
//! than the cap on its own is not stored (the fetch is served the full
//! closure, and the version and depth are not tried again for
//! [`ShallowCacheConfig::retry_after`]). A fetch served a recorded pack
//! updates the recording's last use, at most once per
//! [`USE_NOTE_EVERY`] per recording and process.
//!
//! Garbage collection evicts recordings of versions that are no longer
//! current (`aspen_compact::CompactConfig::shallow_ttl`) and recordings no
//! fetch has used for `aspen_compact::CompactConfig::shallow_unused`. Any
//! eviction keeps the pack's blobs a whole grace period, so a fetch still
//! streaming it is not cut off.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aspen_compact::{CompactConfig, CompactError, Compactor, ShallowBuild, ShallowSpec};
use aspen_core::blob::BlobStore;
use aspen_core::error::{FetchError, StoreError, ToWire};
use aspen_core::fetch::{Deepen, FetchRequest, PlanMode};
use aspen_core::id::{ObjectId, PackId, RefVersionId, RepoId};
use aspen_core::meta::{MetaStore, ShallowDepth};
use aspen_core::name::RefName;
use aspen_core::pack::PackRecord;
use aspen_core::refs::RefVersion;
use aspen_core::repo::Repo;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use tokio::sync::{OnceCell, Semaphore, watch};

use super::Failure;
use super::fetch::{ShallowRows, plan_quietly};
use super::graph::LoadedGraph;
use super::layered::{self, Layered};
use super::resolve::Want;
use super::shallow;
use crate::Server;
use crate::metrics::{ShallowBuildResult, ShallowFallback, ShallowServed as Served};

/// How the server builds and serves exact shallow packs.
#[derive(Debug, Clone)]
pub struct ShallowCacheConfig {
    /// Local disk for a build's source packs and the new pack: a real disk,
    /// sized for [`ShallowCacheConfig::max_source_bytes`] plus the largest
    /// shallow pack, per concurrent build.
    pub spool_dir: PathBuf,
    /// The deepest `deepen <n>` served from exact packs. Every depth asked
    /// for stores its own pack, so this bounds what one version can cost.
    pub max_depth: u32,
    /// The most wants a shallow request may have and still use exact packs.
    pub max_wants: usize,
    /// How long a request waits for a build of a pack it needs before it is
    /// served the full closure instead.
    pub build_wait: Duration,
    /// Builds that may run at once in this process.
    pub max_builds: usize,
    /// After a build of a version and depth ended without recording a pack,
    /// how long requests for it are served the full closure without trying
    /// again.
    pub retry_after: Duration,
    /// The largest full closure (bytes of its packs) a build downloads.
    pub max_source_bytes: u64,
    /// The most objects one exact pack may hold.
    pub max_objects: usize,
    /// The most a new pack may inflate to in all, checked by its
    /// verification.
    pub max_inflated_bytes: MaxInflatedBytes,
    /// Longest delta chain an exact pack may hold (as for compaction packs).
    pub max_delta_depth: u32,
    /// How long a request waits for a build before it is served the
    /// version's layered answer, when it has one (module docs). At most
    /// [`Self::build_wait`].
    pub layered_wait: Duration,
    /// The most bytes of objects a layered answer reads to check that it is
    /// complete (per tree level), and the most its patch pack may hold;
    /// over either there is no layered answer.
    pub max_layered_bytes: u64,
    /// The most bytes (packs, `.idx` files and entry tables) one
    /// repository's recorded exact packs may take together. Recording a new
    /// pack evicts the repository's least recently used ones to stay
    /// within it; a pack larger than this on its own is not stored. Such a
    /// version and depth is built again, and dropped again, at most once
    /// per [`Self::retry_after`] per process.
    pub max_repo_bytes: u64,
}

impl ShallowCacheConfig {
    /// The defaults: depths up to 64, at most 8 wants, wait 20 s for a
    /// build (500 ms when a layered answer is ready, which may read and
    /// patch up to 256 MiB), 2 builds at once, 10 minutes before a failed build is tried
    /// again, closures up to 4 GiB, 10 million objects, delta depth 50,
    /// 8 GiB of recorded packs per repository.
    pub fn new(spool_dir: impl Into<PathBuf>, max_inflated_bytes: MaxInflatedBytes) -> Self {
        Self {
            spool_dir: spool_dir.into(),
            max_depth: 64,
            max_wants: 8,
            build_wait: Duration::from_secs(20),
            layered_wait: Duration::from_millis(500),
            max_layered_bytes: 256 << 20,
            max_builds: 2,
            retry_after: Duration::from_secs(600),
            max_source_bytes: 4 << 30,
            max_objects: 10_000_000,
            max_inflated_bytes,
            max_delta_depth: 50,
            max_repo_bytes: DEFAULT_MAX_REPO_BYTES,
        }
    }
}

/// Default [`ShallowCacheConfig::max_repo_bytes`]: 8 GiB.
pub const DEFAULT_MAX_REPO_BYTES: u64 = 8 << 30;

/// How often one process updates a recording's last use while fetches keep
/// being served it. Garbage collection's unused window is at least ten of
/// these (`aspen_compact::MIN_SHALLOW_UNUSED`).
pub const USE_NOTE_EVERY: Duration = Duration::from_secs(60);

/// The builds of one process.
pub(crate) struct ShallowCache<M, B> {
    config: ShallowCacheConfig,
    compactor: Compactor<M, B>,
    permits: Arc<Semaphore>,
    builds: Mutex<HashMap<Key, Build>>,
    /// When this process last updated each recording's last use.
    noted: Mutex<HashMap<Key, Instant>>,
    /// Layered answers, computed once per version and depth at a time.
    layered: Mutex<HashMap<Key, LayeredCell>>,
    /// The newest version of each ref and depth this process saw recorded:
    /// where a layered answer looks for its base first.
    hints: Mutex<HashMap<HintKey, (u64, RefVersionId)>>,
    /// The commits and tags of recorded packs used as a layered answer's
    /// base (immutable, so remembered).
    base_commits: Mutex<HashMap<PackId, Arc<Vec<ObjectId>>>>,
}

/// A layered answer being computed or computed (when the cell was made).
type LayeredCell = (Instant, Arc<OnceCell<Option<Arc<Layered>>>>);

/// A repository, ref and depth.
type HintKey = (RepoId, RefName, ShallowDepth);

/// How long a computed layered answer is reused. Well under the garbage
/// collection grace period, which its patch pack's registration starts.
const LAYERED_TTL: Duration = Duration::from_secs(60);

/// How long a computation that found no layered answer is remembered.
const NO_LAYERED_TTL: Duration = Duration::from_secs(5);

/// Entries in [`ShallowCache::layered`] and [`ShallowCache::hints`] past
/// which all are dropped, to bound memory.
const LAYERED_PRUNE_AT: usize = 1024;

impl<M, B> std::fmt::Debug for ShallowCache<M, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShallowCache")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// A version and depth. Version IDs are unique across repositories.
type Key = (RefVersionId, ShallowDepth);

/// What a build delivers to the requests waiting for it.
#[derive(Debug, Clone)]
enum Delivered {
    /// The recorded pack.
    Pack(PackRecord),
    /// The region reaches the whole closure, so the closure is exact.
    Closure,
    /// Nothing usable (failed, too large, not a commit).
    Nothing,
}

enum Build {
    /// Running; resolves to `Some(..)` when it ends.
    Running(watch::Receiver<Option<Delivered>>),
    /// Ended without recording a pack at this time.
    Settled(Instant),
    /// The closure is already exact (found at this time). That cannot
    /// change for a version and depth; it is forgotten after a day only to
    /// bound memory.
    ClosureIsExact(Instant),
}

/// Entries in [`ShallowCache::noted`] past which the stale ones are
/// dropped, to bound memory.
const NOTED_PRUNE_AT: usize = 4096;

/// How long [`Build::ClosureIsExact`] is remembered.
const CLOSURE_MEMORY: Duration = Duration::from_secs(24 * 3600);

/// What one want contributes to an exact answer.
enum Share {
    /// Its version's exact pack; `true` when this request waited for the
    /// build.
    Pack(PackRecord, bool),
    /// Its full closure, which is exact.
    Closure,
    /// Its layered answer.
    Layered(Arc<Layered>),
}

impl<M, B> ShallowCache<M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    pub(crate) fn new(
        meta: M,
        blobs: B,
        config: ShallowCacheConfig,
        max_object_bytes: MaxObjectBytes,
    ) -> Self {
        let mut compact = CompactConfig::new(
            &config.spool_dir,
            max_object_bytes,
            config.max_inflated_bytes,
        );
        compact.max_delta_depth = config.max_delta_depth;
        Self {
            permits: Arc::new(Semaphore::new(config.max_builds)),
            compactor: Compactor::new(meta, blobs, compact),
            config,
            builds: Mutex::new(HashMap::new()),
            noted: Mutex::new(HashMap::new()),
            layered: Mutex::new(HashMap::new()),
            hints: Mutex::new(HashMap::new()),
            base_commits: Mutex::new(HashMap::new()),
        }
    }

    /// The remembered commits and tags of `pack`, a layered answer's base.
    pub(crate) fn base_commits(&self, pack: PackId) -> Option<Arc<Vec<ObjectId>>> {
        self.base_commits
            .lock()
            .expect("base commits lock")
            .get(&pack)
            .cloned()
    }

    /// Remembers the commits and tags of `pack`.
    pub(crate) fn note_base_commits(&self, pack: PackId, ids: Arc<Vec<ObjectId>>) {
        let mut known = self.base_commits.lock().expect("base commits lock");
        if known.len() >= LAYERED_PRUNE_AT {
            known.clear();
        }
        known.insert(pack, ids);
    }

    pub(crate) fn config(&self) -> &ShallowCacheConfig {
        &self.config
    }

    /// The newest recorded version this process knows of for `name` at
    /// `depth`: its `seq` and ID.
    pub(crate) fn hint(
        &self,
        repo: RepoId,
        name: &RefName,
        depth: ShallowDepth,
    ) -> Option<(u64, RefVersionId)> {
        self.hints
            .lock()
            .expect("hints lock")
            .get(&(repo, name.clone(), depth))
            .copied()
    }

    /// Notes that `version` has a recorded pack at `depth`.
    pub(crate) fn note_recorded(&self, repo: RepoId, version: &RefVersion, depth: ShallowDepth) {
        let mut hints = self.hints.lock().expect("hints lock");
        if hints.len() >= LAYERED_PRUNE_AT {
            hints.clear();
        }
        let seq = version.seq.get();
        let slot = hints
            .entry((repo, version.name.clone(), depth))
            .or_insert((seq, version.id));
        if slot.0 <= seq {
            *slot = (seq, version.id);
        }
    }

    /// Whether a fetch served the recording of `key` now should update its
    /// last use: not when this process did within [`USE_NOTE_EVERY`].
    fn note_due(&self, key: Key) -> bool {
        let mut noted = self.noted.lock().expect("noted lock");
        if noted
            .get(&key)
            .is_some_and(|at| at.elapsed() < USE_NOTE_EVERY)
        {
            return false;
        }
        if noted.len() >= NOTED_PRUNE_AT {
            noted.retain(|_, at| at.elapsed() < USE_NOTE_EVERY);
        }
        noted.insert(key, Instant::now());
        true
    }

    /// The depth the request can be served exact packs for, if any (see
    /// the module docs; wants are checked by the caller).
    fn depth(&self, request: &FetchRequest, wants: usize) -> Option<ShallowDepth> {
        let Some(Deepen::Depth(n)) = &request.deepen else {
            return None;
        };
        if request.deepen_relative || wants == 0 || wants > self.config.max_wants {
            return None;
        }
        (n.get() <= self.config.max_depth)
            .then(|| ShallowDepth::new(n.get()))
            .flatten()
    }
}

/// The exact shallow packs of `request`, one per want (deduplicated), or
/// `None` when the request is to be served the full closure (see the module
/// docs). Builds missing packs and waits for them.
pub(crate) async fn packs<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    request: &FetchRequest,
    wants: &[Want],
) -> Result<Option<ShallowRows>, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let Some(cache) = server.shallow_cache() else {
        return Ok(None);
    };
    let Some(depth) = cache.depth(request, wants.len()) else {
        return Ok(None);
    };
    let started = Instant::now();
    let rows = choose(server, cache, repo, wants, depth).await;
    server.metrics().shallow_wait(started.elapsed());
    rows
}

/// [`packs`] for a request that can use exact packs at `depth`.
async fn choose<M, B>(
    server: &Server<M, B>,
    cache: &ShallowCache<M, B>,
    repo: &Repo,
    wants: &[Want],
    depth: ShallowDepth,
) -> Result<Option<ShallowRows>, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let metrics = server.metrics();
    let mut versions = Vec::with_capacity(wants.len());
    for want in wants {
        let version = server
            .meta()
            .ref_version(want.version)
            .await
            .map_err(FetchError::Store)?
            .ok_or_else(|| {
                FetchError::Store(StoreError::Corrupt {
                    what: format!(
                        "ref version {} named for want {} does not exist",
                        want.version, want.id
                    ),
                })
            })?;
        if version.value.tip() != Some(want.id) {
            metrics.shallow_fallback(ShallowFallback::NotATip);
            return Ok(None);
        }
        versions.push(version);
    }
    let mut rows: Vec<PackRecord> = Vec::with_capacity(wants.len());
    let mut leave_out = Vec::new();
    let mut how = Served::Recorded;
    // A layered answer leaves its base's commits out of the pack; with
    // several wants another want's pack could hold the same commit, so
    // only a single want is served one.
    let layered_ok = wants.len() == 1;
    for (want, version) in wants.iter().zip(versions) {
        let share = match pack_for(server, cache, repo, want, version, depth, layered_ok).await? {
            Ok(share) => share,
            Err(why) => {
                metrics.shallow_fallback(why);
                return Ok(None);
            }
        };
        let add = match share {
            Share::Pack(row, built) => {
                if built {
                    how = Served::Built;
                }
                vec![row]
            }
            Share::Closure => {
                if how == Served::Recorded {
                    how = Served::Closure;
                }
                closure(server, repo, want).await?
            }
            Share::Layered(l) => {
                how = Served::Layered;
                leave_out.extend_from_slice(&l.leave_out);
                l.rows.clone()
            }
        };
        for row in add {
            if !rows.iter().any(|r| r.id == row.id) {
                rows.push(row);
            }
        }
    }
    metrics.shallow_served(how);
    Ok(Some(ShallowRows { rows, leave_out }))
}

/// The packs of one want's full closure, in plan order.
async fn closure<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    want: &Want,
) -> Result<Vec<PackRecord>, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let (rows, _) = plan_quietly(
        server,
        repo,
        PlanMode::FullClosure,
        std::slice::from_ref(want),
        Vec::new(),
        LoadedGraph::default(),
    )
    .await?;
    Ok(rows)
}

/// The layered answer of `want` (the tip of `version`) at `depth`, computed
/// once per version and depth for every request and build that asks while
/// it is fresh (module docs). `None` when there is none; a failure to
/// compute one is logged and is `None` too, since the request can still be
/// served without it.
async fn layered_answer<M, B>(
    server: &Server<M, B>,
    cache: &ShallowCache<M, B>,
    repo: &Repo,
    want: &Want,
    version: &RefVersion,
    depth: ShallowDepth,
) -> Option<Arc<Layered>>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let key = (version.id, depth);
    let cell = {
        let mut cells = cache.layered.lock().expect("layered lock");
        let fresh = |(at, cell): &LayeredCell| match cell.get() {
            None => true,
            Some(Some(_)) => at.elapsed() < LAYERED_TTL,
            Some(None) => at.elapsed() < NO_LAYERED_TTL,
        };
        match cells.get(&key) {
            Some(entry) if fresh(entry) => Arc::clone(&entry.1),
            _ => {
                if cells.len() >= LAYERED_PRUNE_AT {
                    cells.retain(|_, e| fresh(e));
                }
                let cell = Arc::new(OnceCell::new());
                cells.insert(key, (Instant::now(), Arc::clone(&cell)));
                cell
            }
        }
    };
    cell.get_or_init(|| async {
        let started = Instant::now();
        match layered::compute(server, cache, repo, want, version, depth).await {
            Ok(Some(l)) => {
                tracing::info!(
                    repo = repo.name.as_str(),
                    version = version.id.get(),
                    depth = depth.get(),
                    base = l.base.get(),
                    increments = l.increments,
                    patched = l.patched,
                    ms = started.elapsed().as_millis() as u64,
                    "layered shallow answer ready"
                );
                server.metrics().shallow_layered_patched(l.patched as u64);
                Some(Arc::new(l))
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(
                    repo = repo.name.as_str(),
                    version = version.id.get(),
                    depth = depth.get(),
                    error = %e.to_wire().message,
                    "layered shallow answer failed"
                );
                None
            }
        }
    })
    .await
    .clone()
}

/// What one want's version at `depth` contributes; or why the request
/// falls back.
async fn pack_for<M, B>(
    server: &Server<M, B>,
    cache: &ShallowCache<M, B>,
    repo: &Repo,
    want: &Want,
    version: RefVersion,
    depth: ShallowDepth,
    layered_ok: bool,
) -> Result<Result<Share, ShallowFallback>, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    if let Some(row) = server
        .meta()
        .shallow_pack(repo.id, version.id, depth)
        .await
        .map_err(FetchError::Store)?
    {
        cache.note_recorded(repo.id, &version, depth);
        if cache.note_due((version.id, depth)) {
            // Off the fetch's path: a failure only leaves the last use a
            // little older, which the unused window allows for.
            let (meta, repo, version) = (server.meta().clone(), repo.id, version.id);
            tokio::spawn(async move {
                if let Err(e) = meta.mark_shallow_pack_used(repo, version, depth).await {
                    tracing::warn!(error = %e, "could not record an exact shallow pack's use");
                }
            });
        }
        return Ok(Ok(Share::Pack(row, false)));
    }
    let key = (version.id, depth);
    let layered = |why: ShallowFallback| {
        let version = version.clone();
        async move {
            if !layered_ok {
                return Ok::<_, Failure>(Err(why));
            }
            Ok(
                match layered_answer(server, cache, repo, want, &version, depth).await {
                    Some(l) => Ok(Share::Layered(l)),
                    None => Err(why),
                },
            )
        }
    };
    // Either a build to wait for, or why there is none.
    let next: Result<watch::Receiver<Option<Delivered>>, ShallowFallback> = {
        let mut builds = cache.builds.lock().expect("builds lock");
        match builds.get(&key) {
            Some(Build::Running(rx)) => Ok(rx.clone()),
            Some(Build::ClosureIsExact(at)) if at.elapsed() < CLOSURE_MEMORY => {
                return Ok(Ok(Share::Closure));
            }
            Some(Build::Settled(at)) if at.elapsed() < cache.config.retry_after => {
                Err(ShallowFallback::RecentlyFailed)
            }
            _ => 'start: {
                let Ok(permit) = Arc::clone(&cache.permits).try_acquire_owned() else {
                    break 'start Err(ShallowFallback::NoBuildSlot);
                };
                let retry_after = cache.config.retry_after;
                builds.retain(|_, b| match b {
                    Build::Running(_) => true,
                    Build::Settled(at) => at.elapsed() < retry_after,
                    Build::ClosureIsExact(at) => at.elapsed() < CLOSURE_MEMORY,
                });
                let (tx, rx) = watch::channel(None);
                builds.insert(key, Build::Running(rx.clone()));
                let (server, repo, want, version) =
                    (server.clone(), repo.clone(), want.clone(), version.clone());
                tokio::spawn(async move {
                    let task = {
                        let server = server.clone();
                        tokio::spawn(
                            async move { build(&server, &repo, &want, &version, depth).await },
                        )
                    };
                    // A panicking build is a failed build, never a key that
                    // stays "running" for good.
                    let delivered = task.await.unwrap_or_else(|e| {
                        tracing::error!(error = %e, "exact shallow pack build panicked");
                        server.metrics().shallow_build(ShallowBuildResult::Failed);
                        Delivered::Nothing
                    });
                    drop(permit);
                    if let Some(cache) = server.shallow_cache() {
                        let mut builds = cache.builds.lock().expect("builds lock");
                        match delivered {
                            Delivered::Pack(_) => builds.remove(&key),
                            Delivered::Closure => {
                                builds.insert(key, Build::ClosureIsExact(Instant::now()))
                            }
                            Delivered::Nothing => {
                                builds.insert(key, Build::Settled(Instant::now()))
                            }
                        };
                    }
                    let _ = tx.send(Some(delivered));
                });
                Ok(rx)
            }
        }
    };
    let mut done = match next {
        Ok(rx) => rx,
        Err(why) => return layered(why).await,
    };
    let delivered = |result: &Option<Delivered>| match result.clone() {
        Some(Delivered::Pack(row)) => Ok(Share::Pack(row, true)),
        Some(Delivered::Closure) => Ok(Share::Closure),
        _ => Err(ShallowFallback::BuildRecordedNothing),
    };
    // First the short wait, then the layered answer if there is one: a
    // build that ends quickly gives the exact pack, a slow one keeps
    // nobody waiting.
    let first = if layered_ok {
        cache.config.layered_wait.min(cache.config.build_wait)
    } else {
        cache.config.build_wait
    };
    match tokio::time::timeout(first, done.wait_for(Option::is_some)).await {
        Ok(Ok(result)) => return Ok(delivered(&result)),
        // The build task is gone without a result: treat as failed.
        Ok(Err(_)) => return Ok(Err(ShallowFallback::BuildRecordedNothing)),
        Err(_) => {}
    }
    if layered_ok && let Some(l) = layered_answer(server, cache, repo, want, &version, depth).await
    {
        return Ok(Ok(Share::Layered(l)));
    }
    let rest = cache.config.build_wait.saturating_sub(first);
    let waited = tokio::time::timeout(rest, done.wait_for(Option::is_some)).await;
    match waited {
        Ok(Ok(result)) => Ok(delivered(&result)),
        // The build task is gone without a result: treat as failed.
        Ok(Err(_)) => Ok(Err(ShallowFallback::BuildRecordedNothing)),
        Err(_) => Ok(Err(ShallowFallback::WaitTimedOut)),
    }
}

/// Builds and records the pack of `want` (the tip of `version`) at
/// `depth`; what waiting requests get (logged and counted).
async fn build<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    want: &Want,
    version: &RefVersion,
    depth: ShallowDepth,
) -> Delivered
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let started = Instant::now();
    let outcome = try_build(server, repo, want, version, depth).await;
    let took = started.elapsed();
    let metrics = server.metrics();
    let span = (
        repo.name.as_str(),
        version.id.get(),
        depth.get(),
        took.as_millis() as u64,
    );
    match outcome {
        Ok(Some(ShallowBuild::Recorded { pack: row, evicted })) => {
            if let Some(cache) = server.shallow_cache() {
                cache.note_recorded(repo.id, version, depth);
            }
            metrics.shallow_build(ShallowBuildResult::Recorded);
            metrics.shallow_build_time(took);
            metrics.shallow_limit_evictions(evicted);
            tracing::info!(
                repo = span.0,
                version = span.1,
                depth = span.2,
                build_ms = span.3,
                pack = row.id.get(),
                size = row.pack.size,
                objects = row.pack.object_count,
                evicted,
                "exact shallow pack recorded"
            );
            Delivered::Pack(row)
        }
        Ok(Some(ShallowBuild::NotNeeded)) => {
            metrics.shallow_build(ShallowBuildResult::NotNeeded);
            tracing::debug!(
                repo = span.0,
                version = span.1,
                depth = span.2,
                "exact shallow pack not needed: the full closure is exact"
            );
            Delivered::Closure
        }
        Ok(Some(ShallowBuild::TooLarge { bytes })) => {
            metrics.shallow_build(ShallowBuildResult::TooLarge);
            tracing::info!(
                repo = span.0,
                version = span.1,
                depth = span.2,
                closure_bytes = bytes,
                "exact shallow pack not built: the closure is over max_source_bytes"
            );
            Delivered::Nothing
        }
        Ok(Some(ShallowBuild::OverLimit { bytes, limit })) => {
            metrics.shallow_build(ShallowBuildResult::OverLimit);
            tracing::info!(
                repo = span.0,
                version = span.1,
                depth = span.2,
                pack_bytes = bytes,
                max_repo_bytes = limit,
                "exact shallow pack not stored: it alone is over max_repo_bytes"
            );
            Delivered::Nothing
        }
        Ok(None) => {
            metrics.shallow_build(ShallowBuildResult::NotACommit);
            Delivered::Nothing
        }
        Err(BuildError::Fetch(failure)) => {
            metrics.shallow_build(ShallowBuildResult::Failed);
            if let Failure::Fetch(e) = &failure
                && e.to_wire().alert
            {
                metrics.corruption_alert();
            }
            tracing::warn!(
                repo = span.0, version = span.1, depth = span.2,
                error = %failure.to_wire().message, "exact shallow pack build failed"
            );
            Delivered::Nothing
        }
        Err(BuildError::Compact(CompactError::Store(e))) if e.is_pack_swept() => {
            metrics.shallow_build(ShallowBuildResult::GraceExceeded);
            tracing::warn!(
                repo = span.0, version = span.1, depth = span.2, error = %e,
                "exact shallow pack build took longer than gc_grace: its pack was garbage-collected before it was recorded"
            );
            Delivered::Nothing
        }
        Err(BuildError::Compact(e)) => {
            metrics.shallow_build(ShallowBuildResult::Failed);
            if e.is_corruption() {
                metrics.corruption_alert();
                tracing::error!(
                    repo = span.0, version = span.1, depth = span.2, error = %e,
                    "exact shallow pack build found corrupt data"
                );
            } else {
                tracing::warn!(
                    repo = span.0, version = span.1, depth = span.2, error = %e,
                    "exact shallow pack build failed"
                );
            }
            Delivered::Nothing
        }
    }
}

enum BuildError {
    Fetch(Failure),
    Compact(CompactError),
}

impl From<Failure> for BuildError {
    fn from(e: Failure) -> Self {
        Self::Fetch(e)
    }
}

impl From<FetchError> for BuildError {
    fn from(e: FetchError) -> Self {
        Self::Fetch(e.into())
    }
}

/// `None` when the want peels to a tree or blob, which has no region.
async fn try_build<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    want: &Want,
    version: &RefVersion,
    depth: ShallowDepth,
) -> Result<Option<ShallowBuild>, BuildError>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let cache = server
        .shallow_cache()
        .expect("builds run only on a server with a shallow cache");
    let spec = |region, sources, closure| ShallowSpec {
        repo: repo.id,
        version: version.id,
        depth,
        want: want.id,
        region,
        sources,
        closure,
        max_source_bytes: cache.config.max_source_bytes,
        max_objects: cache.config.max_objects,
        max_repo_bytes: cache.config.max_repo_bytes,
    };
    // First from the version's layered answer: the patch pack and the last
    // recorded pack of this ref at this depth. One small download besides
    // the base, and no
    // full-closure plan (which loads every version of the closure: seconds
    // on a ref with thousands of versions). When there is none (a cold
    // start), or the build ends any other way, build from the full closure.
    if let Some(l) = layered_answer(server, cache, repo, want, version, depth).await {
        let sources = l.rows.iter().map(|r| r.id).collect();
        match cache
            .compactor
            .build_shallow_pack(&spec(l.region.clone(), sources, false))
            .await
        {
            Ok(built @ ShallowBuild::Recorded { .. }) => {
                server.metrics().shallow_build_incremental();
                return Ok(Some(built));
            }
            Ok(other) => tracing::debug!(
                version = version.id.get(),
                depth = depth.get(),
                outcome = ?other,
                "incremental exact shallow pack not recorded; building from the full closure"
            ),
            Err(e) => tracing::warn!(
                version = version.id.get(),
                depth = depth.get(),
                error = %e,
                "incremental exact shallow pack failed; building from the full closure"
            ),
        }
    }
    let (rows, _) = plan_quietly(
        server,
        repo,
        PlanMode::FullClosure,
        std::slice::from_ref(want),
        Vec::new(),
        LoadedGraph::default(),
    )
    .await?;
    let sources: Vec<_> = rows.iter().map(|r| r.id).collect();
    let Some(region) = region(server, repo, want, depth, sources.clone()).await? else {
        return Ok(None);
    };
    cache
        .compactor
        .build_shallow_pack(&spec(region, sources, true))
        .await
        .map(Some)
        .map_err(BuildError::Compact)
}

/// The region of `want` at `depth`, reading unindexed commits from
/// `sources`: the same walk, from the same commit, as the `shallow-info` of
/// a `deepen <depth>` request for this want, so the pack ends exactly at
/// the boundary that request is told. `None` when the want peels to a tree
/// or blob.
pub(crate) async fn region<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    want: &Want,
    depth: ShallowDepth,
    sources: Vec<aspen_core::id::PackId>,
) -> Result<Option<Vec<ObjectId>>, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let mut objects = shallow::Objects::new(
        server.meta(),
        server.objects(),
        repo.id,
        sources,
        server.config().max_shallow_commits,
    );
    let Some(head) = objects.peel(want.id).await? else {
        return Ok(None);
    };
    let (border, inner) = shallow::by_depth(&mut objects, vec![head], depth.get()).await?;
    let mut region: Vec<ObjectId> = border
        .into_iter()
        .chain(inner)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    region.sort_unstable();
    Ok(Some(region))
}

//! The receive-pack service (node B1, `docs/PLAN.md` push path).
//!
//! ```text
//! GET  info/refs?service=git-receive-pack   advertise()  trunk (+ scoped ref), .have tips
//! POST git-receive-pack                     push()
//!   1. parse commands and options           aspen-proto RequestParser
//!   2. pass 1 to a spool on local disk      aspen-pack ingest::receive
//!   3. prefetch bases (shared budget)       aspen-pack StoredPacks::prefetch_bases
//!   4. complete, .idx, entry table          aspen-pack ingest::complete
//!   5. object graph, connectivity           aspen-pack graph, ObjectLocator
//!   6. commit check per command             aspen-core CommitCheck + exact ancestry walk
//!   7. one pack per write                   aspen-pack subset (forced-trunk rule too)
//!   8. register, upload, commit with CAS    MetaStore::register_pack, BlobStore, MetaStore::commit_push
//!   9. report                               aspen-proto ReportStatus
//! ```
//!
//! The service holds no state between requests except caches of immutable
//! data (`.idx` files, ref versions) and the prefetch gate that bounds the
//! memory all concurrent pushes hold for delta bases.
//!
//! **Mirror mode** (node C4) hooks in through [`PushThrough`]: for a
//! repository in proxy mode, the accepted updates are forwarded to the
//! upstream between step 7 and step 8, and only what the upstream accepted
//! is committed; in async mode every write enqueues a `mirror_push` job in
//! its own transaction. Updates coming *from* the upstream enter through
//! [`ReceivePack::import`], which the hook never sees.

mod ancestry;
mod closure;
mod gate;
mod group;
pub mod hook;
pub(crate) mod http;
mod push;
mod source;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::meta::{CoverLayout, MetaStore, have_tip_seqs};
use aspen_core::name::RefName;
use aspen_core::pack::PushLimits;
use aspen_core::refs::RefValue;
use aspen_core::repo::{ForcePolicy, Repo};
use aspen_mirror::{Applied, Forwarded, MirrorMode, RefUpdate};
use aspen_pack::locate::{LocateOptions, StoredPacks};
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_proto::caps::Agent;
use aspen_proto::receive::{
    AdvertisedRef, ReceiveAdvertisement, ReceiveLimits, write_advertisement,
};
use aspen_proto::service::{ProtocolVersion, ReceiveVersion, Service, write_info_refs_header};

pub use group::{GroupCommit, GroupCommitStats};
pub use push::{PushOutcome, Timings};

/// Limits and tuning of the receive-pack service. Every memory and size
/// limit is explicit: there is no `Default`, because the object and total
/// limits must come from the operator (A2 decisions rows).
#[derive(Debug, Clone)]
pub struct ReceiveConfig {
    /// Where pushes are spooled and completed: a real disk (never tmpfs),
    /// sized for about twice the largest accepted push plus its `.idx`
    /// (S4). Each push works in its own temporary directory under it. The
    /// delta-chain maps of sampled packs (`aspen_pack::chains`, 16 bytes
    /// per object of each sampled pack in the `.idx` cache) are kept in
    /// `chains/` under it.
    pub spool_dir: PathBuf,
    /// Object count and pack size limits, checked before and while the
    /// pack body is read.
    pub push_limits: PushLimits,
    /// Limits on the pkt-line part of the request.
    pub request_limits: ReceiveLimits,
    /// The largest object or delta result any step inflates.
    pub max_object_bytes: MaxObjectBytes,
    /// The most one pushed pack, with its completion bases, may inflate to.
    pub max_inflated_bytes: MaxInflatedBytes,
    /// `.idx` cache, read parallelism and the **per-push** prefetch budget
    /// (`max_prefetch_bytes`). A push whose bases need more is refused with
    /// a non-retryable `unpack` error.
    pub locate: LocateOptions,
    /// Memory all concurrent pushes may hold for prefetched bases together.
    /// Each push reserves `prefetch_floor_bytes` of it, and grows to
    /// `locate.max_prefetch_bytes` only when its reads need more, so this
    /// admits `this / prefetch_floor_bytes` small pushes at once and
    /// `this / locate.max_prefetch_bytes` (at least 1) large ones; the rest
    /// wait (`gate.rs`).
    pub prefetch_memory_bytes: u64,
    /// What each push reserves of `prefetch_memory_bytes` before its first
    /// read of stored packs (default 64 MiB).
    pub prefetch_floor_bytes: u64,
    /// How long a push waits for a prefetch slot before it is answered with
    /// a retryable error.
    pub prefetch_wait: Duration,
    /// Threads for completion and scans; `None` uses every core.
    pub thread_limit: Option<usize>,
    /// Most commits one ancestry question may expand before it answers
    /// "undecided" (a retryable `ng`).
    pub max_ancestry_commits: usize,
    /// Commit index rows written in the push's own transaction, per write;
    /// a write with more commits gets its tip's row plus an
    /// `index_commits` job for the rest.
    pub sync_commit_rows: usize,
    /// Trunk versions, newest first, tried as the dependency of a forced
    /// trunk update before it is stored with its full closure.
    pub forced_trunk_candidates: usize,
    /// The `agent=` value advertised.
    pub agent: Agent,
    /// Where compaction covers go. A write whose new version ends a
    /// level-1 cover ([`CoverLayout::ends_a_cover`]) enqueues a `compact`
    /// job for it, in the push's own transaction. The compactor
    /// (`aspen-compact`) must use the same layout.
    pub cover_layout: CoverLayout,
    /// Repository rules run on every update ([`hook`]); `None` runs none.
    pub hook: Option<Arc<dyn hook::PushHook>>,
    /// The merge queue ([`crate::queue`]): clients may push to
    /// `refs/queue/<trunk branch>/...` (added to every repository's
    /// writable namespaces), and every committed queue push wakes the
    /// queue worker. Off: those refs are refused like any ref outside the
    /// writable namespaces, as before.
    pub merge_queue: bool,
    /// Group commit ([`GroupCommit`]): while `enabled`, concurrent pushes
    /// register their packs in one transaction and commit their ref
    /// updates in another, each push still durable before it is answered
    /// and its compare-and-swap its own. Off by default: two transactions
    /// per push.
    pub group_commit: GroupCommit,
    /// Bytes of trees kept in memory for the walk that diffs each pushed
    /// commit against its parent (`aspen_pack::changed`): the trees trunk's
    /// pushes bring and the ones the walk reads from stored packs, least
    /// recently used evicted first. The next push to trunk then diffs
    /// against trunk's newest trees without reading any, and needs no
    /// lookup for anything it did not change. 0 keeps none (each walk reads
    /// the old trees it needs from stored packs). Default 64 MiB.
    pub trunk_tree_cache_bytes: u64,
}

impl ReceiveConfig {
    /// A configuration with the given spool directory and object limits:
    /// 10 M objects and 8 GiB per push, the default request limits, the
    /// default locator options (so a 1 GiB-or-more per-push prefetch
    /// budget) and 4 GiB of prefetch memory shared by all pushes, of which
    /// each push first reserves 64 MiB.
    pub fn new(
        spool_dir: impl Into<PathBuf>,
        max_object_bytes: MaxObjectBytes,
        max_inflated_bytes: MaxInflatedBytes,
    ) -> Self {
        Self {
            spool_dir: spool_dir.into(),
            push_limits: PushLimits {
                max_objects: 10_000_000,
                max_pack_bytes: 8 << 30,
            },
            request_limits: ReceiveLimits::default(),
            max_object_bytes,
            max_inflated_bytes,
            locate: LocateOptions::new(max_object_bytes),
            prefetch_memory_bytes: 4 << 30,
            prefetch_floor_bytes: 64 << 20,
            prefetch_wait: Duration::from_secs(60),
            thread_limit: None,
            max_ancestry_commits: 1_000_000,
            sync_commit_rows: 10_000,
            forced_trunk_candidates: 64,
            agent: Agent::aspen(),
            cover_layout: CoverLayout::DEFAULT,
            hook: None,
            merge_queue: false,
            group_commit: GroupCommit::default(),
            trunk_tree_cache_bytes: 64 << 20,
        }
    }
}

/// Why a request could not be answered with a Git response at all.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// A store failed before the Git exchange started (HTTP 503).
    #[error("storage: {0}")]
    Store(#[from] StoreError),
}

/// Which refs the caller may update. The access layer has already checked
/// that the caller may push to the repository; receive-pack asks this for
/// every command and refuses the others with
/// [`aspen_core::error::RejectReason::PermissionDenied`] on their `ng` line.
pub trait RefAccess: Sync {
    /// Whether the caller may update `name`.
    fn may_update(&self, name: &RefName) -> bool;

    /// Who the caller is, for [`hook::PushHook`]s. The default is an
    /// unnamed caller with no rate-limit key.
    fn caller(&self) -> hook::Caller {
        hook::Caller::default()
    }
}

impl RefAccess for crate::access::Authorized {
    fn may_update(&self, name: &RefName) -> bool {
        self.authorize_ref(name).is_ok()
    }

    fn caller(&self) -> hook::Caller {
        let principal = match self.identity.principal() {
            aspen_core::auth::Principal::Anonymous => "anonymous".to_owned(),
            aspen_core::auth::Principal::Subject { id } => id.clone(),
        };
        hook::Caller {
            principal,
            key: self.caller_key.clone(),
        }
    }
}

/// A boxed `Send` future, for the object-safe [`PushThrough`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Mirror mode's hook into the push path (node C4). The composition root
/// sets it with [`ReceivePack::set_push_through`]; `aspen-server`'s
/// [`crate::mirror`] module implements it over `aspen_mirror::Mirror`.
pub trait PushThrough: Send + Sync + std::fmt::Debug {
    /// How pushes to `repo` reach its upstream; `None` if it is not a
    /// mirror. Asked once per client push.
    fn mode<'a>(&'a self, repo: &'a Repo) -> BoxFuture<'a, Result<Option<MirrorMode>, StoreError>>;

    /// Proxy mode: forwards the accepted `updates` (old values are Aspen's
    /// current ones) with the push's completed pack, before Aspen commits
    /// them. One answer per update, in order.
    fn forward<'a>(
        &'a self,
        repo: &'a Repo,
        updates: &'a [RefUpdate],
        pack: Option<&'a Path>,
        atomic: bool,
    ) -> BoxFuture<'a, Vec<Forwarded>>;

    /// After a client push to a mirror committed something: wakes the
    /// worker for the `mirror_push` jobs (async mode), or asks for a sync
    /// when an update the upstream accepted did not commit locally
    /// (`lagging`, proxy mode).
    fn committed(&self, repo: &Repo, mode: MirrorMode, lagging: bool);
}

/// Where a push comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    /// A Git client.
    Client,
    /// Mirror mode, importing the upstream's value of a ref.
    Upstream,
}

/// Every ref may be updated: for callers with no access layer (local
/// development, tests).
#[derive(Debug, Clone, Copy, Default)]
pub struct AnyRef;

impl RefAccess for AnyRef {
    fn may_update(&self, _: &RefName) -> bool {
        true
    }
}

/// The receive-pack service over a metadata store `M` and a blob store `B`.
#[derive(Debug)]
pub struct ReceivePack<M, B> {
    meta: M,
    blobs: B,
    packs: StoredPacks<M, B>,
    config: ReceiveConfig,
    gate: gate::PrefetchGate,
    closures: closure::Closures,
    /// Trunk's newest trees ([`ReceiveConfig::trunk_tree_cache_bytes`]).
    trees: aspen_pack::changed::TreeCache,
    totals: std::sync::Mutex<Timings>,
    metrics: crate::metrics::PushMetrics,
    push_through: OnceLock<Arc<dyn PushThrough>>,
    group: Option<group::Group<M>>,
    /// Repositories with merge-queue pushes the worker has not seen yet.
    pub(crate) queue_signal: crate::queue::QueueSignal,
    pub(crate) queue_metrics: crate::queue::QueueMetrics,
}

impl<M, B> ReceivePack<M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// The service. `spool_dir` is created if missing.
    pub fn new(meta: M, blobs: B, config: ReceiveConfig) -> std::io::Result<Arc<Self>> {
        std::fs::create_dir_all(&config.spool_dir)?;
        // Sampled packs keep their delta-chain maps on the spool disk, so a
        // deep chain in a large pack costs one round of reads (item 11).
        let packs = StoredPacks::new(meta.clone(), blobs.clone(), config.locate)
            .with_chain_dir(config.spool_dir.join("chains"));
        let group = config
            .group_commit
            .enabled
            .then(|| group::Group::new(meta.clone(), config.group_commit));
        Ok(Arc::new(Self {
            group,
            gate: gate::PrefetchGate::new(
                config.prefetch_memory_bytes,
                config.prefetch_floor_bytes,
                config.locate.max_prefetch_bytes,
                config.prefetch_wait,
            ),
            meta,
            blobs,
            packs,
            closures: closure::Closures::new(config.cover_layout),
            trees: aspen_pack::changed::TreeCache::new(config.trunk_tree_cache_bytes),
            config,
            totals: std::sync::Mutex::new(Timings::default()),
            metrics: crate::metrics::PushMetrics::default(),
            push_through: OnceLock::new(),
            queue_signal: crate::queue::QueueSignal::default(),
            queue_metrics: crate::queue::QueueMetrics::default(),
        }))
    }

    /// Sets mirror mode's hook (once; a second call is refused with the
    /// hook given back).
    pub fn set_push_through(&self, hook: Arc<dyn PushThrough>) -> Result<(), Arc<dyn PushThrough>> {
        self.push_through.set(hook)
    }

    pub(crate) fn push_through(&self) -> Option<&Arc<dyn PushThrough>> {
        self.push_through.get()
    }

    /// Most bytes a compressed push body may send, however little they
    /// inflate to: what an uncompressed body could carry (a pack at the
    /// pack limit, and twice the request limit for the commands with their
    /// pkt-line framing), plus 1/1024 of the pack for gzip's overhead on
    /// data that does not compress (5 bytes per 64 KiB block). The
    /// per-push cap, when set, binds sooner.
    pub(crate) fn max_sent_bytes(&self) -> u64 {
        let pack = self.config.push_limits.max_pack_bytes;
        let request =
            u64::try_from(self.config.request_limits.max_request_bytes).unwrap_or(u64::MAX);
        pack.saturating_add(pack / 1024)
            .saturating_add(request.saturating_mul(2))
    }

    /// Applies one ref update from a mirror's upstream through the push
    /// path, as a push to the branch scope `update.name` with the update's
    /// old value (a compare-and-swap) and `-o force`: the upstream already
    /// accepted the change, so the repository's force policy does not
    /// apply, and a forced trunk update stays exact under the forced-trunk
    /// rule. `pack` must hold what the new value reaches beyond Aspen's
    /// trunk and the ref's current value; it is needed unless the update
    /// is a delete. The update is never forwarded back to the upstream.
    pub async fn import(
        self: &Arc<Self>,
        r: &Repo,
        update: &RefUpdate,
        pack: Option<&Path>,
    ) -> Result<Applied, ServiceError> {
        let repo = Repo {
            settings: r.settings.clone().with_allow_force(ForcePolicy::All),
            ..r.clone()
        };
        let mut head = Vec::new();
        let zero = aspen_core::id::ObjectId::ZERO;
        let mut line = format!(
            "{} {} {}",
            update.old.unwrap_or(zero),
            update.new.unwrap_or(zero),
            update.name
        )
        .into_bytes();
        line.extend_from_slice(b"\0report-status push-options\n");
        aspen_proto::pkt::write_data(&mut head, &line).map_err(|e| StoreError::LocalIo {
            reason: e.to_string(),
        })?;
        aspen_proto::pkt::write_flush(&mut head);
        aspen_proto::pkt::write_text(&mut head, "force").expect("short line");
        aspen_proto::pkt::write_flush(&mut head);
        let head = std::io::Cursor::new(head);
        let body: Box<dyn std::io::Read + Send> = match (update.new, pack) {
            (None, _) => Box::new(head),
            (Some(_), Some(p)) => {
                let file = std::fs::File::open(p).map_err(|e| StoreError::LocalIo {
                    reason: format!("{}: {e}", p.display()),
                })?;
                Box::new(std::io::Read::chain(head, file))
            }
            (Some(_), None) => {
                return Err(StoreError::LocalIo {
                    reason: "an import of a new value needs a pack".into(),
                }
                .into());
            }
        };
        let outcome = self
            .push_from(
                &repo,
                Some(&update.name),
                &AnyRef,
                None,
                body,
                Origin::Upstream,
            )
            .await?;
        Ok(
            match aspen_mirror::wire::parse_push_report(&outcome.response, false) {
                Ok(report) => match report.status(&update.name) {
                    Ok(()) => Applied::Updated,
                    Err(why) => Applied::Rejected(why),
                },
                Err(e) => Applied::Rejected(e.to_string()),
            },
        )
    }

    /// The configuration.
    pub fn config(&self) -> &ReceiveConfig {
        &self.config
    }

    /// The most pushes admitted into prefetch and completion at once.
    pub fn prefetch_slots(&self) -> usize {
        self.gate.slots()
    }

    /// The most pushes that have been in prefetch and completion at once
    /// since the service started (a gauge for metrics and tests).
    pub fn prefetch_peak(&self) -> usize {
        self.gate.peak()
    }

    /// Objects read from stored packs that the base-object cache answered,
    /// and those read from the blob store, since the service started.
    pub fn base_cache_counts(&self) -> (u64, u64) {
        self.packs.object_cache_counts()
    }

    /// Reads of stored packs that needed more than the per-push floor, so
    /// their slot grew to the full per-push budget.
    pub fn prefetch_escalations(&self) -> u64 {
        self.gate.escalations()
    }

    /// Prefetch slots granted since the service started (each read of
    /// stored packs takes one).
    pub fn prefetch_slots_acquired(&self) -> u64 {
        self.gate.acquired()
    }

    /// Stage times summed over every push answered so far (for metrics).
    pub fn timings(&self) -> Timings {
        *self.totals.lock().expect("not poisoned")
    }

    /// What the stored-pack locator has read and looked up so far, over
    /// every push and import.
    pub fn locate_stats(&self) -> aspen_pack::locate::LocateStats {
        self.packs.stats()
    }

    fn record(&self, t: &Timings) {
        *self.totals.lock().expect("not poisoned") += t;
    }

    /// Group commit's counters; `None` when it is off.
    pub fn group_commit_stats(&self) -> Option<GroupCommitStats> {
        self.group.as_ref().map(group::Group::stats)
    }

    /// Registers a push's pack: in a group-commit batch when group commit
    /// is on, in a transaction of its own otherwise.
    pub(crate) async fn register_pack(
        &self,
        pack: &aspen_core::pack::NewPack,
    ) -> Result<aspen_core::pack::PackRecord, StoreError> {
        match &self.group {
            Some(g) => g.register_pack(pack.clone()).await,
            None => self.meta.register_pack(pack).await,
        }
    }

    /// Commits a push's ref updates: in a group-commit batch when group
    /// commit is on, in a transaction of its own otherwise. Called only
    /// after every pack the writes name is uploaded.
    pub(crate) async fn commit_push(
        &self,
        push: aspen_core::meta::PushCommit,
    ) -> Result<aspen_core::meta::CommitOutcome, StoreError> {
        match &self.group {
            Some(g) => g.commit_push(push).await,
            None => self.meta.commit_push(&push).await,
        }
    }

    /// Push counters (node C5).
    pub fn metrics(&self) -> &crate::metrics::PushMetrics {
        &self.metrics
    }

    /// Writes the push families of `/-/metrics`.
    pub(crate) fn render_metrics(&self, out: &mut String) {
        let gate = crate::metrics::push::GateStats {
            slots: self.gate.slots(),
            in_use: self.gate.in_use(),
            peak: self.gate.peak(),
            acquired: self.gate.acquired(),
            timeouts: self.gate.timeouts(),
            escalations: self.gate.escalations(),
        };
        self.metrics.render(out, &self.timings(), gate);
        let (hits, misses) = self.packs.object_cache_counts();
        crate::metrics::expo::counter(
            out,
            "aspen_push_base_cache_hits_total",
            "Delta bases and other objects read from stored packs that the base-object cache answered.",
            hits,
        );
        crate::metrics::expo::counter(
            out,
            "aspen_push_base_cache_misses_total",
            "Delta bases and other objects read from stored packs that came from the blob store.",
            misses,
        );
        let l = self.packs.stats();
        for (name, help, v) in [
            (
                "aspen_push_idx_loads_total",
                "Pack indexes (.idx) read from the blob store into the locator's cache, whole or as a sample.",
                l.whole_idx_loads + l.sampled_idx_loads,
            ),
            (
                "aspen_push_idx_sampled_loads_total",
                "Of those, indexes kept as a sample: over ASPEN_IDX_WHOLE_MAX_BYTES, or not fitting ASPEN_IDX_CACHE_BYTES beside the other indexes of their search.",
                l.sampled_idx_loads,
            ),
            (
                "aspen_push_idx_load_waits_total",
                "Index cache misses that waited for another request's read of the same .idx instead of reading it again.",
                l.idx_load_waits,
            ),
            (
                "aspen_push_idx_read_bytes_total",
                "Bytes of .idx read from the blob store: whole loads, samples, and lookups in samples.",
                l.idx_bytes_read,
            ),
        ] {
            crate::metrics::expo::counter(out, name, help, v);
        }
        if self.config.merge_queue {
            self.queue_metrics.render(out);
        }
        if let Some(g) = self.group_commit_stats() {
            use crate::metrics::expo;
            for (name, help, v) in [
                (
                    "aspen_push_group_commit_batches_total",
                    "Group commit: commit transactions run, each for one or more pushes.",
                    g.commit_batches,
                ),
                (
                    "aspen_push_group_commit_pushes_total",
                    "Group commit: push commits those transactions took.",
                    g.commits,
                ),
                (
                    "aspen_push_group_register_batches_total",
                    "Group commit: pack registration transactions run.",
                    g.register_batches,
                ),
                (
                    "aspen_push_group_registrations_total",
                    "Group commit: pack registrations those transactions took.",
                    g.registrations,
                ),
                (
                    "aspen_push_group_failed_batches_total",
                    "Group commit: batches that failed as a whole; every push in them was refused.",
                    g.failed_batches,
                ),
            ] {
                expo::counter(out, name, help, v);
            }
        }
    }

    /// The metadata store.
    pub(crate) fn meta(&self) -> &M {
        &self.meta
    }

    /// The blob store (the merge queue reads queued packs whole).
    pub(crate) fn blobs(&self) -> &B {
        &self.blobs
    }

    /// The largest object any step inflates.
    pub(crate) fn max_object_bytes(&self) -> u64 {
        self.config.max_object_bytes.get()
    }

    /// Whether `a` is an ancestor of (or equal to) `b`, answered exactly
    /// from `commit_index` and the packs of `versions`' closures (which
    /// must reach both commits), the way a push's commit check answers it.
    pub async fn is_ancestor(
        &self,
        repo: aspen_core::id::RepoId,
        a: aspen_core::id::ObjectId,
        b: aspen_core::id::ObjectId,
        versions: &[aspen_core::id::RefVersionId],
    ) -> Result<aspen_core::refs::Ancestry, StoreError> {
        let source = source::PushCommits {
            meta: &self.meta,
            repo,
            packs: &self.packs,
            gate: &self.gate,
            closures: &self.closures,
            fallback: Vec::new(),
            lazy_roots: versions.to_vec(),
            lazy: tokio::sync::OnceCell::new(),
        };
        let mut walker = ancestry::Walker::new(self.config.max_ancestry_commits);
        let answer = walker.is_ancestor(&source, a, b).await;
        self.queue_metrics.record_walk(walker.expanded());
        answer
    }

    /// The packs of `versions`' closures, in probe order.
    pub(crate) async fn closure_of(
        &self,
        repo: aspen_core::id::RepoId,
        versions: &[aspen_core::id::RefVersionId],
    ) -> Result<aspen_core::ingest::SearchPacks, StoreError> {
        closure::closure_packs(&self.meta, &self.closures, repo, versions).await
    }

    /// The objects of `ids` that `packs` hold, read under one prefetch
    /// slot.
    pub(crate) async fn read_many(
        &self,
        packs: &aspen_core::ingest::SearchPacks,
        ids: Vec<aspen_core::id::ObjectId>,
    ) -> Result<
        std::collections::HashMap<
            aspen_core::id::ObjectId,
            (aspen_core::ingest::ObjectKind, Vec<u8>),
        >,
        StoreError,
    > {
        use aspen_core::ingest::{BaseLookup, CandidateBases, SortedIds};
        let mut out = std::collections::HashMap::new();
        if ids.is_empty() {
            return Ok(out);
        }
        let wanted = SortedIds::from_unsorted(ids);
        let slot = self.gate.acquire().await?;
        let fetched = match slot
            .prefetch(&self.packs, packs, &CandidateBases(wanted.clone()))
            .await
        {
            Ok((f, _)) => f.bases,
            Err(aspen_pack::locate::PrefetchError::OverBudget { budget }) => {
                return Err(StoreError::Unavailable {
                    reason: format!("objects need more than {budget} bytes of bases"),
                });
            }
            Err(aspen_pack::locate::PrefetchError::Store(e)) => return Err(e),
        };
        let mut buf = Vec::new();
        for id in wanted.as_slice() {
            if let Some(kind) = fetched.find_base(id, &mut buf)? {
                out.insert(*id, (kind, std::mem::take(&mut buf)));
            }
        }
        Ok(out)
    }

    /// One object from the closure of `version` (its pack and every pack
    /// its dependencies reach), read under a prefetch slot like any other
    /// read of stored packs; `None` if the closure lacks it. For
    /// applications built on the server (a hook that keeps its own state
    /// reads the trees it needs this way).
    pub async fn read_object(
        &self,
        repo: aspen_core::id::RepoId,
        version: aspen_core::id::RefVersionId,
        id: aspen_core::id::ObjectId,
    ) -> Result<Option<(aspen_core::ingest::ObjectKind, Vec<u8>)>, StoreError> {
        let packs = closure::closure_packs(&self.meta, &self.closures, repo, &[version]).await?;
        self.read_from(&packs, id).await
    }

    /// One object from `packs`, under a prefetch slot.
    pub(crate) async fn read_from(
        &self,
        packs: &aspen_core::ingest::SearchPacks,
        id: aspen_core::id::ObjectId,
    ) -> Result<Option<(aspen_core::ingest::ObjectKind, Vec<u8>)>, StoreError> {
        use aspen_core::ingest::{BaseLookup, CandidateBases, SortedIds};
        let one = CandidateBases(SortedIds::from_unsorted(vec![id]));
        let slot = self.gate.acquire().await?;
        let fetched = match slot.prefetch(&self.packs, packs, &one).await {
            Ok((f, _)) => f.bases,
            Err(aspen_pack::locate::PrefetchError::OverBudget { budget }) => {
                return Err(StoreError::Unavailable {
                    reason: format!("object {id} needs more than {budget} bytes of bases"),
                });
            }
            Err(aspen_pack::locate::PrefetchError::Store(e)) => return Err(e),
        };
        let mut buf = Vec::new();
        let found = fetched.find_base(&id, &mut buf)?.map(|k| (k, buf));
        drop(fetched);
        drop(slot);
        Ok(found)
    }

    /// The body of `GET info/refs?service=git-receive-pack`: the service
    /// header, then [`ReceivePack::advertise_refs`].
    pub async fn advertise(
        &self,
        r: &Repo,
        scope: Option<&RefName>,
        requested: ProtocolVersion,
    ) -> Result<Vec<u8>, ServiceError> {
        let mut out = Vec::new();
        write_info_refs_header(
            &mut out,
            Service::ReceivePack,
            requested.min(ProtocolVersion::V1),
        );
        out.extend_from_slice(&self.advertise_refs(r, scope, requested).await?);
        Ok(out)
    }

    /// The ref advertisement a push starts with, as a connection (SSH)
    /// sends it: a `version 1` line if the client asked for v1, then trunk
    /// (and the branch-scoped ref, if any) with earlier trunk tips as
    /// `.have` lines (S2): every one of the newest `8 × have_tips`, then
    /// tips spaced a quarter apart down to the oldest
    /// ([`have_tip_seqs`]), so a client whose base is any trunk version
    /// sends little more than its own commits, without `push.negotiate`.
    ///
    /// A `.have` tip tells the client it may leave out everything that tip
    /// reaches, so only tips inside the closure a push searches are shown:
    /// trunk versions the current trunk version reaches through its
    /// dependencies (`Closures::held_seqs`). Tips a forced trunk
    /// update skipped are outside it and never advertised. The walk is the
    /// one a push's search set needs anyway, and is cached: it reads the
    /// versions pushed since the cached closure (one, between two pushes on
    /// one instance), then one batch of the advertised versions' rows that
    /// this instance does not remember (none, between two of its pushes).
    pub async fn advertise_refs(
        &self,
        r: &Repo,
        scope: Option<&RefName>,
        requested: ProtocolVersion,
    ) -> Result<Vec<u8>, ServiceError> {
        let (refs, counted) = self
            .meta
            .count_statements(
                aspen_core::blob::BlobCaller::PushAdvertise.scope(self.advertised(r, scope)),
            )
            .await;
        let (refs, haves) = refs?;
        if let Some(n) = counted {
            self.record(&Timings {
                advert_db_queries: n.queries,
                ..Timings::default()
            });
        }
        let version = ReceiveVersion::answering(requested);
        let advert = ReceiveAdvertisement {
            agent: self.config.agent.clone(),
            no_thin: r.settings.no_thin(),
        };
        let mut out = Vec::new();
        write_advertisement(&mut out, version, &advert, &refs, &haves).map_err(|e| {
            StoreError::Corrupt {
                what: e.to_string(),
            }
        })?;
        Ok(out)
    }

    /// The refs and `.have` tips of [`ReceivePack::advertise_refs`]: trunk
    /// and the versions pushed since this instance last asked in one
    /// statement, the `.have` versions from the closure cache (one batch
    /// for any it lacks), and the scoped ref in another.
    async fn advertised(
        &self,
        r: &Repo,
        scope: Option<&RefName>,
    ) -> Result<(Vec<AdvertisedRef>, Vec<aspen_core::id::ObjectId>), ServiceError> {
        let trunk = r.settings.trunk().clone();
        let mut refs = Vec::new();
        let mut haves = Vec::new();
        let n = r.settings.have_tips();
        // The dense window of `have_tip_seqs`, read with the current
        // version, so that versions other instances pushed come in the same
        // statement.
        let window = 8 * u64::from(n);
        if let Some(recent) = self
            .closures
            .recent(&self.meta, r.id, &trunk, window)
            .await?
            && let Some(tip) = recent[0].value.tip()
        {
            let v = &recent[0];
            refs.push(AdvertisedRef {
                name: trunk.clone(),
                tip,
            });
            if n > 0 {
                let held = self
                    .closures
                    .held_seqs(&self.meta, r.id, v.id, &trunk)
                    .await?;
                let want = have_tip_seqs(v.seq, n, &held);
                for old in self
                    .closures
                    .versions_at(&self.meta, r.id, &trunk, &want)
                    .await?
                {
                    if let RefValue::Live { tip: t, .. } = old.value
                        && t != tip
                        && !haves.contains(&t)
                    {
                        haves.push(t);
                    }
                }
            }
        }
        if let Some(scope) = scope.filter(|s| *s != &trunk)
            && let Some(v) = self.meta.current_version(r.id, scope).await?
            && let Some(tip) = v.value.tip()
        {
            refs.push(AdvertisedRef {
                name: scope.clone(),
                tip,
            });
        }
        Ok((refs, haves))
    }
}

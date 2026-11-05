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

pub use push::{PushOutcome, Timings};

/// Limits and tuning of the receive-pack service. Every memory and size
/// limit is explicit: there is no `Default`, because the object and total
/// limits must come from the operator (A2 decisions rows).
#[derive(Debug, Clone)]
pub struct ReceiveConfig {
    /// Where pushes are spooled and completed: a real disk (never tmpfs),
    /// sized for about twice the largest accepted push plus its `.idx`
    /// (S4). Each push works in its own temporary directory under it.
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
    /// The service admits at most `max(1, this / locate.max_prefetch_bytes)`
    /// pushes into prefetch and completion at once; the rest wait.
    pub prefetch_memory_bytes: u64,
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
}

impl ReceiveConfig {
    /// A configuration with the given spool directory and object limits:
    /// 10 M objects and 8 GiB per push, the default request limits, the
    /// default locator options (so a 1 GiB-or-more per-push prefetch
    /// budget) and 4 GiB of prefetch memory shared by all pushes.
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
            prefetch_wait: Duration::from_secs(60),
            thread_limit: None,
            max_ancestry_commits: 1_000_000,
            sync_commit_rows: 10_000,
            forced_trunk_candidates: 64,
            agent: Agent::aspen(),
            cover_layout: CoverLayout::DEFAULT,
            hook: None,
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
    totals: std::sync::Mutex<Timings>,
    metrics: crate::metrics::PushMetrics,
    push_through: OnceLock<Arc<dyn PushThrough>>,
}

impl<M, B> ReceivePack<M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// The service. `spool_dir` is created if missing.
    pub fn new(meta: M, blobs: B, config: ReceiveConfig) -> std::io::Result<Arc<Self>> {
        std::fs::create_dir_all(&config.spool_dir)?;
        let packs = StoredPacks::new(meta.clone(), blobs.clone(), config.locate);
        let slots = (config.prefetch_memory_bytes / config.locate.max_prefetch_bytes.max(1)).max(1);
        Ok(Arc::new(Self {
            gate: gate::PrefetchGate::new(slots as usize, config.prefetch_wait),
            meta,
            blobs,
            packs,
            closures: closure::Closures::new(config.cover_layout),
            config,
            totals: std::sync::Mutex::new(Timings::default()),
            metrics: crate::metrics::PushMetrics::default(),
            push_through: OnceLock::new(),
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

    /// Prefetch slots granted since the service started (each read of
    /// stored packs takes one).
    pub fn prefetch_slots_acquired(&self) -> u64 {
        self.gate.acquired()
    }

    /// Stage times summed over every push answered so far (for metrics).
    pub fn timings(&self) -> Timings {
        *self.totals.lock().expect("not poisoned")
    }

    fn record(&self, t: &Timings) {
        *self.totals.lock().expect("not poisoned") += t;
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
        };
        self.metrics.render(out, &self.timings(), gate);
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
    /// (and the branch-scoped ref, if any) with up to `have_tips` earlier
    /// trunk tips as `.have` lines (S2).
    ///
    /// A `.have` tip tells the client it may leave out everything that tip
    /// reaches, so only tips inside the closure a push searches are shown:
    /// the earlier versions reached from the current trunk version through
    /// trunk dependencies that are each the version's own parent. The walk
    /// stops at the first version whose trunk dependency is not its parent
    /// (a forced trunk update, or a first push after a delete), so tips a
    /// forced update skipped are never advertised.
    pub async fn advertise_refs(
        &self,
        r: &Repo,
        scope: Option<&RefName>,
        requested: ProtocolVersion,
    ) -> Result<Vec<u8>, ServiceError> {
        let trunk = r.settings.trunk().clone();
        let mut refs = Vec::new();
        let mut haves = Vec::new();
        if let Some(v) = self.meta.current_version(r.id, &trunk).await?
            && let Some(tip) = v.value.tip()
        {
            refs.push(AdvertisedRef {
                name: trunk.clone(),
                tip,
            });
            let want = have_tip_seqs(v.seq, r.settings.have_tips());
            let chain = self
                .trunk_chain(r, &trunk, &v, want.last().copied())
                .await?;
            for old in chain.iter().filter(|o| want.contains(&o.seq)) {
                if let RefValue::Live { tip: t, .. } = old.value
                    && t != tip
                    && !haves.contains(&t)
                {
                    haves.push(t);
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

    /// The earlier trunk versions down to `oldest`, newest first, for as
    /// long as each newer one's trunk dependency is its parent. Read in
    /// batches that double, so a short chain costs few rows.
    async fn trunk_chain(
        &self,
        r: &Repo,
        trunk: &RefName,
        current: &aspen_core::refs::RefVersion,
        oldest: Option<aspen_core::id::Seq>,
    ) -> Result<Vec<aspen_core::refs::RefVersion>, ServiceError> {
        let mut out = Vec::new();
        let Some(oldest) = oldest else {
            return Ok(out);
        };
        let mut newer = current.clone();
        let mut batch = 1u64;
        let mut back = 1u64;
        loop {
            let seqs: Vec<_> = (back..back + batch)
                .filter_map(|b| current.seq.back(b))
                .filter(|s| *s >= oldest)
                .collect();
            if seqs.is_empty() {
                return Ok(out);
            }
            let mut got = self.meta.versions_by_seq(r.id, trunk, &seqs).await?;
            got.sort_by_key(|v| std::cmp::Reverse(v.seq));
            for old in got {
                if newer.parent != Some(old.id) || newer.deps.trunk != Some(old.id) {
                    return Ok(out);
                }
                out.push(old.clone());
                newer = old;
            }
            if newer.seq == oldest {
                return Ok(out);
            }
            back += batch;
            batch *= 2;
        }
    }
}

//! One push, from the request bytes to the report (see the module docs of
//! [`super`] for the stages).

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aspen_core::blob::{BlobCaller, BlobStore};
use aspen_core::error::{RejectReason, StoreError, ToWire, UnpackError};
use aspen_core::id::{ObjectId, PackId, RefVersionId, Seq, UnixMicros};
use aspen_core::ingest::{BaseLookup, CandidateBases, ObjectKind, SearchPacks, SortedIds};
use aspen_core::meta::{
    CommitLookup, CommitOutcome, MetaStore, NewCommit, PushCommit, RefWrite, VersionJob,
    VersionJobKind,
};
use aspen_core::name::RefName;
use aspen_core::pack::{EntryKind, EntryTable, NewPack, PushLimits};

use aspen_core::refs::{
    Ancestry, AncestryAnswer, CAS_RECOMPUTE_LIMIT, CheckInput, Closure, CommitCheck, Deps,
    PushOptions, RefCommand, RefVersion, Verdict, VerdictKind, apply_atomic, replay_parent,
};
use aspen_core::repo::Repo;
use aspen_pack::graph::{PackGraph, SharedSet};
use aspen_pack::ingest::{self, CompleteOptions, IngestedPack, Received};
use aspen_pack::locate::PrefetchError;
use aspen_pack::scan::{ScanOptions, references};
use aspen_pack::subset::{ExtraObject, ExtraSpill, SubsetSource, write_subset};
use aspen_proto::pkt::PktReader;
use aspen_proto::receive::{
    ReceiveAdvertisement, ReceiveRequest, RefStatus, ReportStatus, RequestParser, write_fatal,
};

use tracing::Instrument as _;

use super::ancestry::{CommitNode, Lookup, Walker};
use super::closure::closure_packs;
use super::hook::{
    Caller, CommitRange, CommittedUpdate, HookFuture, HookObjects, HookUpdate, HookVerdict,
    PushHook, RangeCommit,
};
use super::source::PushCommits;
use super::{Origin, PushThrough, ReceivePack, RefAccess, ServiceError};
use crate::metrics::{PushResult, RefOutcome};
use aspen_mirror::{Forwarded, MirrorMode, RefUpdate};

/// What a push produced: the response body, and where its time went.
#[derive(Debug, Clone, Default)]
pub struct PushOutcome {
    /// The `git-receive-pack` result body.
    pub response: Vec<u8>,
    /// Time per stage.
    pub timings: Timings,
    /// How the request ended, for metrics.
    pub(crate) result: PushResult,
    /// Pack bytes the client sent.
    pub(crate) received_bytes: u64,
}

/// Wall-clock time of each push stage, for metrics and the B1
/// measurements. Stages that did not run are zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Timings {
    /// Pushes these figures add up (1 for a single push).
    pub pushes: u64,
    /// Computing the search packs: the packs of the closures of trunk and
    /// the scoped ref (issue #149).
    pub search: Duration,
    /// Parsing the request and pass 1 (spool and validate).
    pub receive: Duration,
    /// Waiting for a prefetch slot.
    pub gate_wait: Duration,
    /// Locating and reading the candidate bases from the blob store.
    pub prefetch: Duration,
    /// Completion, `.idx` and entry table.
    pub complete: Duration,
    /// Object graph, connectivity and the per-write split.
    pub graph: Duration,
    /// Commit check (ancestry walks included).
    pub check: Duration,
    /// The push hook's checks, with the range walks they asked for (up to
    /// one ancestry walk per range commit and excluded tip).
    pub hook: Duration,
    /// Registering packs and uploading them.
    pub upload: Duration,
    /// The commit transactions.
    pub commit: Duration,
    /// Commits the ancestry walks expanded (each a `commit_index` read or a
    /// read from a pack unless an earlier walk of the push cached it).
    pub ancestry_commits: u64,
    /// Object IDs looked up in the dependencies' packs to find which of the
    /// pushed pack's objects they hold and to check connectivity. With the
    /// walk of the changed paths (`aspen_pack::changed`) this is about the
    /// objects the push changed, not every entry of every tree it touched.
    pub located: u64,
    /// Tree entries the walk of the changed paths compared.
    pub changed_entries: u64,
    /// Metadata statements the push sent from its own task (the store's
    /// count, `MetaStore::count_statements`): reads and writes, then
    /// `BEGIN`, `COMMIT`, `ROLLBACK` and savepoints. The access layer's
    /// repository and token lookups come before and are not counted, and
    /// neither is a group-commit batch, which runs on a task of its own.
    pub db_queries: u64,
    /// See [`Timings::db_queries`].
    pub db_transaction_control: u64,
    /// Statements the receive-pack advertisement (the `info/refs` request
    /// before a push) sent, counted the same way.
    pub advert_db_queries: u64,
}

impl std::ops::AddAssign<&Timings> for Timings {
    fn add_assign(&mut self, t: &Timings) {
        self.pushes += t.pushes;
        self.search += t.search;
        self.receive += t.receive;
        self.gate_wait += t.gate_wait;
        self.prefetch += t.prefetch;
        self.complete += t.complete;
        self.graph += t.graph;
        self.check += t.check;
        self.hook += t.hook;
        self.upload += t.upload;
        self.commit += t.commit;
        self.ancestry_commits += t.ancestry_commits;
        self.located += t.located;
        self.changed_entries += t.changed_entries;
        self.db_queries += t.db_queries;
        self.db_transaction_control += t.db_transaction_control;
        self.advert_db_queries += t.advert_db_queries;
    }
}

/// The pkt-line request and pass 1, run on a blocking thread.
enum Parsed {
    /// Nothing at all, or a lone flush (git's auth probe of a large push).
    Empty,
    /// The request does not parse: answer with a fatal error.
    Fatal(String),
    /// The request, and pass 1 of its pack when it has one.
    Request(Box<(ReceiveRequest, Result<Option<Received>, UnpackError>)>),
}

fn parse_and_receive<R: Read>(
    body: R,
    advert: ReceiveAdvertisement,
    request_limits: aspen_proto::receive::ReceiveLimits,
    push_limits: PushLimits,
    spool: &Path,
) -> Parsed {
    let mut reader = PktReader::new(body);
    let mut parser = RequestParser::new(advert, request_limits);
    let mut any = false;
    let req = loop {
        let p = match reader.read_packet() {
            Ok(Some(p)) => p,
            Ok(None) if !any => return Parsed::Empty,
            Ok(None) => return Parsed::Fatal("protocol error: request ended early".into()),
            Err(e) => return Parsed::Fatal(format!("protocol error: {e}")),
        };
        any = true;
        match parser.packet(p) {
            Ok(Some(req)) => break req,
            Ok(None) => {}
            Err(e) => return Parsed::Fatal(e.to_string()),
        }
    };
    if req.command_count() == 0 {
        return Parsed::Empty;
    }
    let pack = if req.expects_pack() {
        let mut rest = reader.into_remainder();
        let r = ingest::receive(&mut rest, spool, &push_limits);
        if r.is_err() {
            // Read what is left (up to the size limit) so the client gets
            // its report instead of a reset connection.
            let _ = std::io::copy(
                &mut (&mut rest).take(push_limits.max_pack_bytes),
                &mut std::io::sink(),
            );
        }
        r.map(Some)
    } else {
        Ok(None)
    };
    Parsed::Request(Box::new((req, pack)))
}

fn local_io(e: impl std::fmt::Display) -> StoreError {
    StoreError::LocalIo {
        reason: e.to_string(),
    }
}

/// The thin bases among `candidates` that `packs` hold, read under `slot`.
async fn prefetch<M, B>(
    slot: &super::gate::Slot,
    stored: &aspen_pack::locate::StoredPacks<M, B>,
    packs: &SearchPacks,
    candidates: &CandidateBases,
) -> Result<aspen_core::ingest::Prefetched<aspen_core::ingest::InMemoryBases>, UnpackError>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    match slot.prefetch(stored, packs, candidates).await {
        Ok((p, _stats)) => Ok(p),
        Err(PrefetchError::OverBudget { budget }) => Err(UnpackError::BasesOverBudget { budget }),
        Err(PrefetchError::Store(e)) => Err(UnpackError::Store(e)),
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T, StoreError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| local_io(format!("worker thread: {e}")))
}

impl<M, B> ReceivePack<M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// Runs one push to `r`: `body` is the POST body of
    /// `git-receive-pack`, read on a blocking thread. `access` says which
    /// refs the caller may update, and `quota` tightens the configured push
    /// limits for this caller (the access layer's object and byte caps).
    /// Returns the response body.
    ///
    /// Each push is recorded in [`ReceivePack::metrics`] and logged at
    /// `info` inside a `push` span.
    pub async fn push<R: Read + Send + 'static>(
        self: &Arc<Self>,
        r: &Repo,
        scope: Option<&RefName>,
        access: &dyn RefAccess,
        quota: Option<crate::access::PushLimits>,
        body: R,
    ) -> Result<PushOutcome, ServiceError> {
        self.push_from(r, scope, access, quota, body, Origin::Client)
            .await
    }

    /// [`Self::push`] for a push from `origin`: a client, or mirror mode's
    /// import of an upstream's value ([`ReceivePack::import`]). Both are
    /// measured and logged alike, with the origin in the span.
    pub(crate) async fn push_from<R: Read + Send + 'static>(
        self: &Arc<Self>,
        r: &Repo,
        scope: Option<&RefName>,
        access: &dyn RefAccess,
        quota: Option<crate::access::PushLimits>,
        body: R,
        origin: Origin,
    ) -> Result<PushOutcome, ServiceError> {
        let started = Instant::now();
        // With the merge queue on, clients may also write the queue's
        // namespace (never its outcome refs, which only the queue writes).
        let queued;
        let r = if self.config.merge_queue && origin == Origin::Client {
            queued = crate::queue::with_queue_writable(r);
            &queued
        } else {
            r
        };
        let span = tracing::info_span!(
            "push",
            repo = %r.name,
            scope = scope.map(|s| s.as_str()),
            origin = ?origin,
        );
        // Blob requests are counted per push stage (the scopes inside);
        // what no stage claims is `push_other`.
        let result = BlobCaller::PushOther
            .scope(self.push_inner(r, scope, access, quota, body, origin))
            .instrument(span.clone())
            .await;
        let took = started.elapsed();
        let _enter = span.enter();
        match &result {
            Ok(o) => {
                self.metrics.finished(o.result, took, o.received_bytes);
                if o.result != PushResult::Probe {
                    let t = &o.timings;
                    tracing::info!(
                        result = ?o.result,
                        received_bytes = o.received_bytes,
                        ms = took.as_millis() as u64,
                        search_ms = t.search.as_millis() as u64,
                        receive_ms = t.receive.as_millis() as u64,
                        prefetch_ms = (t.gate_wait + t.prefetch).as_millis() as u64,
                        complete_ms = t.complete.as_millis() as u64,
                        graph_ms = t.graph.as_millis() as u64,
                        check_ms = t.check.as_millis() as u64,
                        upload_ms = t.upload.as_millis() as u64,
                        commit_ms = t.commit.as_millis() as u64,
                        "push answered"
                    );
                }
            }
            Err(e) => {
                self.metrics.finished(PushResult::StoreError, took, 0);
                tracing::warn!(error = %e, ms = took.as_millis() as u64, "push failed with a storage error");
            }
        }
        result
    }

    async fn push_inner<R: Read + Send + 'static>(
        self: &Arc<Self>,
        r: &Repo,
        scope: Option<&RefName>,
        access: &dyn RefAccess,
        quota: Option<crate::access::PushLimits>,
        body: R,
        origin: Origin,
    ) -> Result<PushOutcome, ServiceError> {
        let work = tempfile::Builder::new()
            .prefix("push-")
            .tempdir_in(&self.config.spool_dir)
            .map_err(local_io)?;
        let spool = work.path().join("received.pack");
        let advert = ReceiveAdvertisement {
            agent: self.config.agent.clone(),
            no_thin: r.settings.no_thin(),
        };
        let request_limits = self.config.request_limits;
        let mut push_limits = self.config.push_limits;
        if let Some(q) = quota {
            if let Some(n) = q.max_objects {
                push_limits.max_objects = push_limits
                    .max_objects
                    .min(u32::try_from(n).unwrap_or(u32::MAX));
            }
            if let Some(n) = q.max_pack_bytes {
                push_limits.max_pack_bytes = push_limits.max_pack_bytes.min(n);
            }
        }
        let mut timings = Timings {
            pushes: 1,
            ..Timings::default()
        };
        let started = Instant::now();
        let parsed =
            blocking(move || parse_and_receive(body, advert, request_limits, push_limits, &spool))
                .await?;
        timings.receive = started.elapsed();
        let (req, pack) = match parsed {
            Parsed::Empty => {
                return Ok(PushOutcome {
                    response: Vec::new(),
                    timings,
                    result: PushResult::Probe,
                    received_bytes: 0,
                });
            }
            Parsed::Fatal(msg) => {
                let mut response = Vec::new();
                write_fatal(&mut response, None, &msg);
                return Ok(PushOutcome {
                    response,
                    timings,
                    result: PushResult::Fatal,
                    received_bytes: 0,
                });
            }
            Parsed::Request(r) => *r,
        };
        let mut report = ReportStatus::new(&req);
        let mut result = PushResult::Reported;
        let mut received_bytes = 0;
        let unpack_failed = |report: &mut ReportStatus, e: &UnpackError| {
            tracing::info!(error = %e, "pack refused");
            report
                .unpack_failed(&e.to_wire())
                .expect("unpack errors render on the unpack line");
            for _ in &req.commands {
                self.metrics
                    .ref_result(RefOutcome::Rejected(RejectReason::UnpackFailed));
            }
        };
        match pack {
            Err(e) => {
                result = PushResult::UnpackFailed;
                unpack_failed(&mut report, &e);
            }
            Ok(received) => {
                received_bytes = received.as_ref().map_or(0, |r| r.received_bytes);
                let mut push = Push {
                    svc: self,
                    access,
                    caller: access.caller(),
                    stored: HashMap::new(),
                    hook_refusals: HashMap::new(),
                    repo: r,
                    scope,
                    req: &req,
                    work: work.path().to_owned(),
                    timings: &mut timings,
                    walker: Walker::new(self.config.max_ancestry_commits),
                    trunk_dep: None,
                    scoped_dep: None,
                    search: SearchPacks::default(),
                    wide: None,
                    wide_roots: Vec::new(),
                    filled: HashMap::new(),
                    copied_bytes: 0,
                    copy_limit: push_limits.max_pack_bytes,
                    pack: None,
                    entered_trees: Vec::new(),
                    held: Vec::new(),
                    plans: HashMap::new(),
                    planned: Vec::new(),
                    by_tip: HashMap::new(),
                    seqs: HashMap::new(),
                    noop_rows: HashMap::new(),
                    trunk_pack: None,
                    own_of: Vec::new(),
                    own_budget: 0,
                    own_probe_left: 0,
                    scratch: Vec::new(),
                    received_bytes_left: received.as_ref().map_or(0, |r| r.received_bytes),
                    next_dir: 0,
                    origin,
                    mirror: None,
                    forwarded: HashSet::new(),
                    upstream_detail: HashMap::new(),
                };
                let (ran, counted) = self
                    .meta
                    .count_statements(push.run(received, &mut report))
                    .await;
                drop(push);
                if let Some(n) = counted {
                    timings.db_queries = n.queries;
                    timings.db_transaction_control = n.transaction_control;
                }
                match ran {
                    Ok(()) => {}
                    Err(e) => {
                        result = PushResult::UnpackFailed;
                        unpack_failed(&mut report, &e);
                    }
                }
            }
        }
        let mut response = Vec::new();
        report
            .write(&mut response, &req.caps)
            .map_err(|e| local_io(format!("push report: {e}")))?;
        drop(work);
        self.record(&timings);
        Ok(PushOutcome {
            response,
            timings,
            result,
            received_bytes,
        })
    }
}

/// The completed pack of a push and its object graph.
struct PushedPack {
    ingested: IngestedPack,
    table: EntryTable,
    graph: PackGraph,
    /// Graph position of each entry of `table`.
    pos_of_entry: Vec<usize>,
    /// Entry of each graph position.
    entry_of_pos: Vec<usize>,
}

/// Index of a [`PlannedPack`] in [`Push::planned`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlannedId(usize);

/// No `Own` planned pack holds this position ([`Push::own_of`]).
const NO_OWNER: u32 = u32::MAX;

/// A pack one or more writes of the push will name, uploaded only when the
/// first write that names it is about to commit (decisions log, "where a
/// write's objects are stored").
struct PlannedPack {
    contents: Contents,
    stored: Option<PackId>,
}

/// What a planned pack holds. Only trunk's set and the shared set are kept
/// as one flag per object; at most one of each is open at a time, so a
/// push with any number of writes holds O(objects) of them.
enum Contents {
    /// What a non-forced trunk write's tip reaches, one flag per position.
    Trunk(Vec<bool>),
    /// What one non-trunk write's tip reaches, as positions. All `Own`
    /// packs of a push together hold at most the pushed pack's object
    /// count minus trunk's (`Push::own_budget`).
    Own(Vec<u32>),
    /// What several non-trunk writes' tips reach together. Frozen once
    /// stored; later writes start a new one.
    Shared(SharedSet),
    /// A write's entries (flags in entry-table order, empty when the push
    /// sent no pack) plus objects its tip reaches that only other stored
    /// packs hold, read from them and spilled to local disk
    /// ([`Push::fill`]): a forced trunk write's skipped history, or what a
    /// write reaches through a boundary outside its dependencies.
    Filled {
        keep: Vec<bool>,
        spill: Option<ExtraSpill>,
    },
    /// Uploaded; the contents are no longer needed.
    Done,
}

/// Where a write's objects are stored, and whether that pack holds only
/// what the write's tip reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Placement {
    pack: PlannedId,
    fit: Fit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fit {
    /// The pack is exactly what the tip reaches (outside the deps).
    Own,
    /// The pack is another write's and holds more than the tip reaches:
    /// the write's version is `Wider`.
    Contained,
    /// A shared pack: exact only if this tip turns out to reach the whole
    /// set ([`SharedSet::spanning_tip`]), decided when it is stored.
    Shared { tip_at: usize },
}

/// How one ref write is stored: computed once per new tip, reused when a
/// lost compare-and-swap makes the verdicts be recomputed.
#[derive(Debug, Clone)]
enum Plan {
    Store {
        pack: Option<Placement>,
        deps: Deps,
        /// The closure if the write's pack holds only what its tip reaches;
        /// `Wider` otherwise.
        closure: Closure,
        commits: Vec<NewCommit>,
        jobs: Vec<VersionJob>,
    },
    /// The write is refused whatever the ref's current version.
    Reject(RejectReason),
}

struct Push<'a, M, B> {
    svc: &'a ReceivePack<M, B>,
    access: &'a dyn RefAccess,
    /// Who is pushing, for the push hook.
    caller: Caller,
    /// Each command's ref tip as read for its current verdict (`None`: no
    /// such ref), which the push hook is given as the stored tip.
    stored: HashMap<RefName, Option<ObjectId>>,
    /// The push hook's refusals: the `ng` reason and the detail lines.
    hook_refusals: HashMap<RefName, (String, Vec<String>)>,
    repo: &'a Repo,
    scope: Option<&'a RefName>,
    req: &'a ReceiveRequest,
    work: PathBuf,
    timings: &'a mut Timings,
    walker: Walker,
    /// The live trunk version the lookup and connectivity check search.
    trunk_dep: Option<RefVersion>,
    /// The live scoped version, for a push through a branch-scoped URL, or
    /// the current version of the one branch a plain-URL push updates
    /// (`plain_url_branch_dep`, issue #203).
    scoped_dep: Option<RefVersion>,
    /// Closure packs of `trunk_dep` and `scoped_dep`: what the push's
    /// writes depend on, so what counts as already stored (`held`).
    search: SearchPacks,
    /// `search` plus the closures of the versions that hold the pushed
    /// pack's boundary commits, once the boundary turned out to lie outside
    /// the dependencies ([`Push::widen`]). Every read searches it (thin
    /// bases, the connectivity check, objects a write copies); `held` never
    /// does, so nothing found only here is left out of a write's pack.
    wide: Option<SearchPacks>,
    /// The versions `wide` searches besides the dependencies.
    wide_roots: Vec<RefVersionId>,
    /// Placement of each tip planned while `wide` is set ([`Push::fill`]).
    filled: HashMap<ObjectId, Option<Placement>>,
    /// Bytes the push's writes copied from storage while `wide` is set,
    /// and the most they may: the push's own byte limit
    /// ([`UnpackError::LeftOutTooLarge`]).
    copied_bytes: u64,
    copy_limit: u64,
    pack: Option<PushedPack>,
    /// Pushed trees the walk of the changed paths entered, by graph
    /// position: trunk's newest trees once a trunk write commits.
    entered_trees: Vec<usize>,
    /// Per graph position: the object is in `search`.
    held: Vec<bool>,
    /// Plans by (ref, new tip).
    plans: HashMap<(RefName, ObjectId), Plan>,
    planned: Vec<PlannedPack>,
    /// The placement of each non-forced tip planned so far: writes at one
    /// tip store the same objects.
    by_tip: HashMap<ObjectId, Placement>,
    /// `seq` of every current version a verdict was computed against, so a
    /// write knows its new version's `seq` (for the `compact` job).
    seqs: HashMap<RefVersionId, Seq>,
    /// Commit rows (and whether they are all the tip's) that `place` handed
    /// to a no-op's tip, kept for the first write at that tip.
    noop_rows: HashMap<ObjectId, (Vec<NewCommit>, bool)>,
    /// The non-forced trunk write's pack.
    trunk_pack: Option<PlannedId>,
    /// Per graph position: the first `Own` pack holding it, or `NO_OWNER`.
    own_of: Vec<u32>,
    /// Objects `Own` packs may still hold.
    own_budget: usize,
    /// Objects that walks which did not fit in `own_budget` may still
    /// visit; at 0, no more own packs are tried and every later write that
    /// is not contained in a planned pack goes to the shared one. Starts at
    /// the pushed pack's object count, so planning stays linear in it.
    own_probe_left: usize,
    /// Scratch flags for `PackGraph::reach_within`.
    scratch: Vec<bool>,
    /// `received_bytes` for the next stored pack: the client's byte count
    /// goes to the first one, 0 to the rest (decisions log).
    received_bytes_left: u64,
    /// Numbers the per-write output directories.
    next_dir: usize,
    /// A client's push, or an import from a mirror's upstream.
    origin: Origin,
    /// Mirror mode's hook and the repository's mode, for a client push to
    /// a mirror.
    mirror: Option<(Arc<dyn PushThrough>, MirrorMode)>,
    /// Refs the upstream accepted (proxy mode).
    forwarded: HashSet<RefName>,
    /// The upstream's reasons for refusals (proxy mode).
    upstream_detail: HashMap<RefName, String>,
}

impl<M, B> Push<'_, M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    fn config(&self) -> &super::ReceiveConfig {
        &self.svc.config
    }

    fn trunk(&self) -> &RefName {
        self.repo.settings.trunk()
    }

    /// The packs every read of a stored object searches: the dependencies',
    /// widened by [`Push::widen`] when the pack's boundary is outside them.
    fn reads(&self) -> &SearchPacks {
        self.wide.as_ref().unwrap_or(&self.search)
    }

    fn commits_source(
        &self,
        lazy_roots: Vec<aspen_core::id::RefVersionId>,
    ) -> PushCommits<'_, M, B> {
        PushCommits {
            meta: &self.svc.meta,
            repo: self.repo.id,
            packs: &self.svc.packs,
            gate: &self.svc.gate,
            closures: &self.svc.closures,
            fallback: vec![self.reads().clone()],
            lazy_roots,
            lazy: tokio::sync::OnceCell::new(),
        }
    }

    async fn run(
        &mut self,
        received: Option<Received>,
        report: &mut ReportStatus,
    ) -> Result<(), UnpackError> {
        let options = match PushOptions::parse(self.req.push_options.iter().map(String::as_str)) {
            Ok(o) => o,
            Err(e) => {
                // The pack was read; nothing is written.
                for c in &self.req.commands {
                    set(report, &c.name, RefStatus::Rejected(e.to_string()));
                    self.svc.metrics.ref_result(RefOutcome::BadPushOptions);
                }
                return Ok(());
            }
        };
        // `-o land`: every branch this push writes is also queued for the
        // merge queue, as if pushed to `refs/queue/<trunk>/<branch>`.
        let land = self.req.push_options.iter().any(|o| o == "land");
        if land && !self.config().merge_queue {
            for c in &self.req.commands {
                set(
                    report,
                    &c.name,
                    RefStatus::Rejected(
                        "push option `land` needs the merge queue (ASPEN_MERGE_QUEUE=on)".into(),
                    ),
                );
                self.svc.metrics.ref_result(RefOutcome::BadPushOptions);
            }
            return Ok(());
        }
        let store = |e: StoreError| UnpackError::Store(e);
        if self.origin == Origin::Client
            && let Some(hook) = self.svc.push_through()
            && let Some(mode) = hook.mode(self.repo).await.map_err(store)?
        {
            self.mirror = Some((Arc::clone(hook), mode));
        }
        let meta = &self.svc.meta;
        let trunk = self.trunk().clone();
        // Every ref this push reads, in one statement: trunk, the ref it
        // builds on (the scoped ref, or the branch of a plain-URL push),
        // and each command's ref for the commit check below.
        let dep_ref = match self.scope.filter(|s| **s != trunk) {
            Some(scope) => Some(scope.clone()),
            None if self.scope.is_none() && self.origin == Origin::Client => {
                plain_url_branch_dep(&self.req.commands, &trunk, &options).cloned()
            }
            None => None,
        };
        let mut names = vec![trunk.clone()];
        names.extend(dep_ref.iter().cloned());
        for c in &self.req.commands {
            if !names.contains(&c.name) {
                names.push(c.name.clone());
            }
        }
        let read = meta
            .current_versions(self.repo.id, &names)
            .await
            .map_err(store)?;
        // The tips' generations, read with the refs: a fast-forward's new
        // commits get theirs from them without a statement of their own.
        for c in read.iter().flatten() {
            if let (Some(tip), Some(g)) = (c.version.value.tip(), c.tip_generation) {
                self.walker.remember_generation(tip, g);
            }
        }
        let read: Vec<Option<RefVersion>> =
            read.into_iter().map(|c| c.map(|c| c.version)).collect();
        let current_of = |name: &RefName| -> Option<RefVersion> {
            names
                .iter()
                .position(|n| n == name)
                .and_then(|i| read[i].clone())
        };
        self.trunk_dep = current_of(&trunk).filter(|v| v.value.tip().is_some());
        self.scoped_dep = dep_ref
            .as_ref()
            .and_then(current_of)
            .filter(|v| v.value.tip().is_some());
        self.svc.closures.remember(read.iter().flatten());
        let roots: Vec<_> = self
            .trunk_dep
            .iter()
            .chain(&self.scoped_dep)
            .map(|v| v.id)
            .collect();
        let started = Instant::now();
        self.search = BlobCaller::PushSearch
            .scope(closure_packs(
                meta,
                &self.svc.closures,
                self.repo.id,
                &roots,
            ))
            .await
            .map_err(store)?;
        self.timings.search += started.elapsed();

        if let Some(received) = received.filter(|r| r.object_count > 0) {
            BlobCaller::PushGraph.scope(self.ingest(received)).await?;
        }
        // A tip the client did not send must be stored already. Outside the
        // dependencies, it is a commit the negotiation acknowledged (a new
        // branch at, or a tag of, another branch's commit).
        if self.may_widen() {
            let outside: Vec<ObjectId> = self
                .tips()
                .into_iter()
                .filter(|t| {
                    self.pack
                        .as_ref()
                        .is_none_or(|p| p.graph.position(t).is_none())
                })
                .collect();
            BlobCaller::PushSearch.scope(self.widen(outside)).await?;
        }

        // The commit check, against each ref's current version.
        let started = Instant::now();
        let mut verdicts = Vec::with_capacity(self.req.commands.len());
        let mut currents = Vec::with_capacity(self.req.commands.len());
        // Against the versions read before the search: a verdict on a
        // version that has moved since loses its compare-and-swap and is
        // computed again against the new one (step 4), so reading again
        // here would only cost a statement per push (decisions log,
        // throughput item 6b).
        for c in &self.req.commands {
            let current = current_of(&c.name);
            currents.push(current.as_ref().and_then(|v| v.value.tip()));
            self.stored
                .insert(c.name.clone(), current.as_ref().and_then(|v| v.value.tip()));
            verdicts.push(
                BlobCaller::PushCheck
                    .scope(self.verdict(c, current.as_ref(), &options))
                    .await?,
            );
        }
        self.timings.check += started.elapsed();
        let atomic = self.req.caps.atomic;
        // Plans are made here; nothing is uploaded until a write is about
        // to commit, so a refused write (and every write of a refused
        // atomic push) stores nothing.
        let mut verdicts = self.finish_verdicts(verdicts, atomic).await?;
        // Proxy mode: the upstream decides before anything commits here.
        if let Some((hook, MirrorMode::Proxy)) = self.mirror.clone() {
            self.forward(hook.as_ref(), &mut verdicts, &currents, atomic)
                .await;
        }

        // Commit: all refs at once under `atomic`, each on its own
        // otherwise, recomputing verdicts after a lost compare-and-swap.
        let groups: Vec<Vec<usize>> = if atomic {
            vec![(0..verdicts.len()).collect()]
        } else {
            (0..verdicts.len()).map(|i| vec![i]).collect()
        };
        for group in groups {
            let mut attempt = 0;
            loop {
                if group.iter().any(|&i| !verdicts[i].kind.is_accepted()) {
                    break;
                }
                let mut writes = Vec::new();
                for &i in &group {
                    if let Some(w) = self.write_for(&verdicts[i]).await? {
                        writes.push(w);
                    }
                }
                if writes.is_empty() {
                    break;
                }
                let closures: Vec<(bool, Closure)> = writes
                    .iter()
                    .filter_map(|w| Some((w.name == trunk, w.value.closure()?)))
                    .collect();
                let queued = if land {
                    self.land_writes(&mut writes, &trunk).await.map_err(store)?
                } else {
                    false
                };
                let started = Instant::now();
                let outcome = self
                    .svc
                    .commit_push(PushCommit {
                        repo: self.repo.id,
                        writes,
                    })
                    .await;
                self.timings.commit += started.elapsed();
                if let Err(e) = &outcome
                    && e.is_pack_swept()
                {
                    // Registering to committing took longer than the
                    // grace period: the client retries the push.
                    self.svc.metrics.grace_exceeded();
                    tracing::warn!(
                        error = %e,
                        "push refused: its pack was garbage-collected before the push committed (slower than gc_grace)"
                    );
                }
                match outcome.map_err(store)? {
                    CommitOutcome::Committed(created) => {
                        self.svc.closures.committed(&created);
                        self.seed_base_cache().await;
                        if closures.iter().any(|(is_trunk, _)| *is_trunk) {
                            self.remember_trunk_trees();
                        }
                        for (is_trunk, c) in closures {
                            if !is_trunk {
                                self.svc.metrics.branch_committed(c);
                                continue;
                            }
                            if c == Closure::Wider {
                                tracing::warn!(
                                    "committed a wider trunk version: every later incremental fetch of this repository sends a full clone"
                                );
                            }
                            self.svc.metrics.trunk_committed(c);
                        }
                        if queued {
                            self.svc.queue_signal.mark(self.repo.id);
                        }
                        self.notify_committed(&group, &verdicts).await;
                        break;
                    }
                    CommitOutcome::CasLost { .. } if attempt >= CAS_RECOMPUTE_LIMIT => {
                        for &i in &group {
                            verdicts[i].kind = VerdictKind::Reject(RejectReason::CasLost);
                        }
                        break;
                    }
                    CommitOutcome::CasLost { .. } => {
                        attempt += 1;
                        let mut again = Vec::with_capacity(group.len());
                        for &i in &group {
                            let c = &self.req.commands[i];
                            let current = meta
                                .current_version(self.repo.id, &c.name)
                                .await
                                .map_err(store)?;
                            self.stored.insert(
                                c.name.clone(),
                                current.as_ref().and_then(|v| v.value.tip()),
                            );
                            again.push(
                                BlobCaller::PushCheck
                                    .scope(self.verdict(c, current.as_ref(), &options))
                                    .await?,
                            );
                        }
                        let again = self.finish_verdicts(again, atomic).await?;
                        for (&i, v) in group.iter().zip(again) {
                            verdicts[i] = v;
                        }
                    }
                }
            }
        }
        let mut lagging = false;
        for v in &verdicts {
            let (status, outcome) = match &v.kind {
                // The upstream, the source of truth in proxy mode, took the
                // update; a sync brings Aspen to it.
                VerdictKind::Reject(_) if self.forwarded.contains(&v.name) => {
                    lagging = true;
                    (RefStatus::Ok, RefOutcome::Ok)
                }
                VerdictKind::Reject(RejectReason::PushRule) => {
                    let status = match self.hook_refusals.get(&v.name) {
                        Some((reason, details)) => {
                            for line in details {
                                report.note(line);
                            }
                            RefStatus::Rejected(reason.clone())
                        }
                        None => RefStatus::from(RejectReason::PushRule),
                    };
                    (status, RefOutcome::Rejected(RejectReason::PushRule))
                }
                VerdictKind::Reject(r) => match self.upstream_detail.get(&v.name) {
                    Some(why) => (
                        RefStatus::Rejected(format!("{}: {why}", r.message())),
                        RefOutcome::Rejected(*r),
                    ),
                    None => (RefStatus::from(*r), RefOutcome::Rejected(*r)),
                },
                // A delete of a ref that does not exist (issue #133):
                // warn as stock `git receive-pack` does.
                VerdictKind::Noop if v.new.is_zero() => {
                    report.warn("deleting a non-existent ref");
                    (RefStatus::Ok, RefOutcome::Ok)
                }
                _ => (RefStatus::Ok, RefOutcome::Ok),
            };
            self.svc.metrics.ref_result(outcome);
            set(report, &v.name, status);
        }
        if let Some((hook, mode)) = &self.mirror
            && (lagging || verdicts.iter().any(|v| v.kind.writes()))
        {
            hook.committed(self.repo, *mode, lagging);
        }
        Ok(())
    }

    /// Keeps the trees a committed trunk write brought, and its commits'
    /// root trees, for the next push's walk of the changed paths: trunk's
    /// newest trees ([`super::ReceiveConfig::trunk_tree_cache_bytes`]).
    fn remember_trunk_trees(&mut self) {
        let Some(p) = &self.pack else {
            return;
        };
        let g = &p.graph;
        let cache = &self.svc.trees;
        for c in g.commits() {
            let at = g.position(&c.id).expect("a commit of the pack");
            if let Some(tree) = g
                .internal_refs(at)
                .iter()
                .map(|&r| g.id(r as usize))
                .chain(g.external_refs(at).iter().copied())
                .find(|r| !c.parents.contains(r))
            {
                cache.insert_root(self.repo.id, c.id, tree);
            }
        }
        for &at in &std::mem::take(&mut self.entered_trees) {
            if let Some(data) = g.tree_data(at) {
                cache.insert_tree(self.repo.id, g.id(at), data.clone());
            }
        }
    }

    /// Proxy mode: forwards every accepted write to the upstream and keeps
    /// only what it accepted. An update whose upstream value is not Aspen's
    /// is not sent (`UpstreamAhead`); under `--atomic` the hook answers all
    /// updates alike.
    async fn forward(
        &mut self,
        hook: &dyn PushThrough,
        verdicts: &mut [Verdict],
        currents: &[Option<ObjectId>],
        atomic: bool,
    ) {
        let at: Vec<usize> = (0..verdicts.len())
            .filter(|&i| verdicts[i].kind.writes())
            .collect();
        if at.is_empty() {
            return;
        }
        let updates: Vec<RefUpdate> = at
            .iter()
            .map(|&i| {
                let v = &verdicts[i];
                let deletes = matches!(
                    v.kind,
                    VerdictKind::DeleteCas | VerdictKind::DeleteUnconditional
                );
                RefUpdate {
                    name: v.name.clone(),
                    old: currents[i],
                    new: (!deletes).then_some(v.new),
                }
            })
            .collect();
        let pack = self.pack.as_ref().map(|p| p.ingested.pack_path.clone());
        let answers = hook
            .forward(self.repo, &updates, pack.as_deref(), atomic)
            .await;
        for (&i, answer) in at.iter().zip(answers) {
            let name = verdicts[i].name.clone();
            let reject = |verdicts: &mut [Verdict], r| verdicts[i].kind = VerdictKind::Reject(r);
            match answer {
                Forwarded::Accepted => {
                    self.forwarded.insert(name);
                }
                Forwarded::Rejected(why) => {
                    reject(verdicts, RejectReason::UpstreamRejected);
                    self.upstream_detail.insert(name, why);
                }
                Forwarded::Ahead => reject(verdicts, RejectReason::UpstreamAhead),
                Forwarded::Unavailable(why) => {
                    reject(verdicts, RejectReason::UpstreamUnavailable);
                    self.upstream_detail.insert(name, why);
                }
                // No longer a mirror: Aspen's own verdict stands, and the
                // update is not one the upstream took.
                Forwarded::NotMirror => {}
            }
        }
    }

    /// The `mirror_push` job a write enqueues in async mode.
    fn mirror_job(&self) -> Option<VersionJob> {
        matches!(self.mirror, Some((_, MirrorMode::Async))).then(|| VersionJob {
            kind: VersionJobKind::MirrorPush,
            run_after: UnixMicros::now(),
        })
    }

    /// Tells the push hook which updates of `group` committed.
    /// Puts the trees this push stored into the base-object cache. The
    /// next push to the same ref (an agent's next commit, the next trunk
    /// landing, a pull request branched from the new trunk) sends its trees
    /// as deltas against these, and resolving them from the stored pack
    /// costs a blob-store round trip per delta in their chains: about
    /// 40-80 ms of each landing in the throughput benchmark. Best effort:
    /// a read that fails only means the next push reads the store.
    async fn seed_base_cache(&self) {
        const MAX_TREES: usize = 32;
        let Some(p) = &self.pack else { return };
        let trees: Vec<ObjectId> = (0..p.graph.len())
            .filter(|&at| p.graph.kind(at) == ObjectKind::Tree)
            .map(|at| p.graph.id(at))
            .collect();
        if trees.is_empty() || trees.len() > MAX_TREES {
            return;
        }
        let (pack, idx) = (p.ingested.pack_path.clone(), p.ingested.idx_path.clone());
        let max = self.config().max_object_bytes;
        let read = blocking(move || {
            trees
                .into_iter()
                .filter_map(|id| {
                    aspen_pack::scan::read_object(&pack, &idx, &id, max)
                        .ok()
                        .flatten()
                        .map(|(kind, data)| (id, kind, data))
                })
                .collect::<Vec<_>>()
        })
        .await;
        if let Ok(objects) = read {
            for (id, kind, data) in objects {
                self.svc.packs.remember(id, kind, data);
            }
        }
    }

    /// For `-o land`: adds, beside each branch write (not trunk, not a
    /// delete), a write of its queue ref `refs/queue/<trunk>/<branch>` at
    /// the same version value and dependencies, so the queue entry lands
    /// in the same transaction as the branch (its compare-and-swap is on
    /// the queue ref's current version). Returns whether any was added.
    async fn land_writes(
        &self,
        writes: &mut Vec<RefWrite>,
        trunk: &RefName,
    ) -> Result<bool, StoreError> {
        let Some(short) = trunk.as_str().strip_prefix("refs/heads/") else {
            return Ok(false);
        };
        let mut extra = Vec::new();
        for w in writes.iter() {
            let Some(branch) = w.name.as_str().strip_prefix("refs/heads/") else {
                continue;
            };
            if w.name == *trunk || w.value.tip().is_none() {
                continue;
            }
            let name = format!("{}{short}/{branch}", crate::queue::QUEUE_PREFIX);
            let name = RefName::new(name.clone()).map_err(|e| StoreError::Corrupt {
                what: format!("queue ref {name}: {e}"),
            })?;
            let current = self.svc.meta.current_version(self.repo.id, &name).await?;
            extra.push(RefWrite {
                name,
                expected: current.map(|v| v.id),
                value: w.value,
                deps: w.deps,
                commits: Vec::new(),
                jobs: Vec::new(),
            });
        }
        let any = !extra.is_empty();
        writes.extend(extra);
        Ok(any)
    }

    async fn notify_committed(&self, group: &[usize], verdicts: &[Verdict]) {
        if self.config().merge_queue
            && group.iter().any(|&i| {
                let v = &verdicts[i];
                v.kind.writes()
                    && !is_delete(&v.kind)
                    && v.name.as_str().starts_with(crate::queue::QUEUE_PREFIX)
            })
        {
            self.svc.queue_signal.mark(self.repo.id);
        }
        let Some(hook) = self.config().hook.clone() else {
            return;
        };
        let updates: Vec<CommittedUpdate> = group
            .iter()
            .map(|&i| &verdicts[i])
            .filter(|v| v.kind.writes())
            .map(|v| CommittedUpdate {
                name: v.name.clone(),
                old: self.stored.get(&v.name).copied().flatten(),
                new: (!is_delete(&v.kind)).then_some(v.new),
            })
            .collect();
        if !updates.is_empty() {
            hook.committed(self.repo, &self.caller, &updates).await;
        }
    }

    /// Asks the push hook about every update that would write, against
    /// the stored tip its verdict was computed with. A refusal turns the
    /// verdict into [`RejectReason::PushRule`] and keeps the hook's text
    /// for the report; an acceptance clears an earlier refusal (a verdict
    /// recomputed after a lost compare-and-swap is asked again).
    async fn run_hook(&mut self, verdicts: &mut [Verdict]) -> Result<(), UnpackError> {
        let Some(hook) = self.config().hook.clone() else {
            return Ok(());
        };
        let caller = self.caller.clone();
        let trunk = self.trunk_dep.as_ref().and_then(|d| d.value.tip());
        let started = Instant::now();
        let result = self.ask_hook(&*hook, &caller, trunk, verdicts).await;
        self.timings.hook += started.elapsed();
        result
    }

    /// [`Self::run_hook`]'s loop, timed as a whole.
    async fn ask_hook(
        &mut self,
        hook: &dyn PushHook,
        caller: &Caller,
        trunk: Option<ObjectId>,
        verdicts: &mut [Verdict],
    ) -> Result<(), UnpackError> {
        let repo = self.repo;
        for v in verdicts.iter_mut().filter(|v| v.kind.writes()) {
            let name = v.name.clone();
            let update = HookUpdate {
                repo,
                caller,
                name: &name,
                stored: self.stored.get(&name).copied().flatten(),
                new: (!is_delete(&v.kind)).then_some(v.new),
                trunk,
            };
            let mut objects = PushObjects {
                push: &mut *self,
                extra: v.against.into_iter().collect(),
            };
            match hook
                .check(update, &mut objects)
                .await
                .map_err(UnpackError::Store)?
            {
                HookVerdict::Accept => {
                    self.hook_refusals.remove(&name);
                }
                HookVerdict::Reject { reason, details } => {
                    v.kind = VerdictKind::Reject(RejectReason::PushRule);
                    self.hook_refusals.insert(name, (reason, details));
                }
            }
        }
        Ok(())
    }

    /// The commits reachable from `new` and from none of `exclude`, for
    /// the push hook ([`HookObjects::range`]). Each candidate is tested
    /// with the exact ancestry walk against every excluded tip, from the
    /// new tip down; a candidate below an excluded tip is not expanded.
    async fn hook_range(
        &mut self,
        new: ObjectId,
        exclude: &[ObjectId],
        max_commits: usize,
        extra: &[aspen_core::id::RefVersionId],
    ) -> Result<CommitRange, StoreError> {
        let unpack = |e: UnpackError| match e {
            UnpackError::Store(e) => e,
            other => StoreError::Unavailable {
                reason: other.to_string(),
            },
        };
        let mut parents_of: HashMap<ObjectId, Vec<ObjectId>> = HashMap::new();
        let mut below: HashSet<ObjectId> = HashSet::new();
        let mut stack = vec![new];
        while let Some(c) = stack.pop() {
            if parents_of.contains_key(&c) || below.contains(&c) {
                continue;
            }
            let mut is_below = false;
            for &x in exclude {
                match self.ancestry(c, x, extra).await.map_err(unpack)? {
                    Ancestry::Descendant => {
                        is_below = true;
                        break;
                    }
                    Ancestry::NotDescendant => {}
                    Ancestry::Undecided => return Ok(CommitRange::Undecided),
                }
            }
            if is_below {
                below.insert(c);
                continue;
            }
            if parents_of.len() >= max_commits {
                return Ok(CommitRange::TooLong);
            }
            // The pushed pack's commits are in its graph (not indexed yet);
            // any other commit comes from the index or the packs.
            let parents = match self.pack.as_ref().and_then(|p| p.graph.commit(&c)) {
                Some(info) => info.parents.clone(),
                None => match self.commit_node_with(c, extra).await.map_err(unpack)? {
                    Some(node) => node.parents,
                    None => return Ok(CommitRange::Undecided),
                },
            };
            stack.extend(parents.iter().rev().copied());
            parents_of.insert(c, parents);
        }
        // Parents before children: a post-order walk from the new tip.
        let mut out = Vec::with_capacity(parents_of.len());
        let mut done: HashSet<ObjectId> = HashSet::new();
        let mut work = vec![(new, false)];
        while let Some((c, expanded)) = work.pop() {
            let Some(parents) = parents_of.get(&c) else {
                continue;
            };
            if done.contains(&c) {
                continue;
            }
            if expanded {
                done.insert(c);
                out.push(RangeCommit {
                    id: c,
                    parents: parents.clone(),
                });
                continue;
            }
            work.push((c, true));
            work.extend(
                parents
                    .iter()
                    .rev()
                    .filter(|p| !done.contains(*p))
                    .map(|p| (*p, false)),
            );
        }
        Ok(CommitRange::Commits(out))
    }

    /// One object for the push hook: from the pushed pack, the search
    /// packs, then the closures of `extra`.
    async fn hook_read(
        &mut self,
        id: ObjectId,
        extra: &[aspen_core::id::RefVersionId],
    ) -> Result<Option<(ObjectKind, Vec<u8>)>, StoreError> {
        if let Some(p) = &self.pack
            && p.graph.position(&id).is_some()
        {
            let (pack, idx) = (p.ingested.pack_path.clone(), p.ingested.idx_path.clone());
            let max = self.config().max_object_bytes;
            return blocking(move || aspen_pack::scan::read_object(&pack, &idx, &id, max))
                .await?
                .map_err(|e| local_io(format!("pushed pack: {e}")));
        }
        let search = self.reads().clone();
        if let Some(found) = self.svc.read_from(&search, id).await? {
            return Ok(Some(found));
        }
        if extra.is_empty() {
            return Ok(None);
        }
        let packs = closure_packs(&self.svc.meta, &self.svc.closures, self.repo.id, extra).await?;
        self.svc.read_from(&packs, id).await
    }

    /// Applies the rules that need more than one ref's verdict or the
    /// write's plan: a write refused by its plan (a branch at a non-commit,
    /// an undecided forced-trunk dependency), then `--atomic`, including an
    /// invalid ref name under `--atomic`. Trunk is planned first, so that
    /// other writes contained in its pack can share it.
    ///
    /// A no-op's tip is placed too ([`Push::place_noop`]), although a no-op
    /// writes nothing: under `--atomic` a lost compare-and-swap recomputes
    /// every verdict, and a no-op whose ref moved meanwhile can come back as
    /// a write. Its placement must then already exist, made before any pack
    /// was uploaded, or it could open a second shared pack after the first
    /// one was stored and break the 2N storage bound (issue #49).
    async fn finish_verdicts(
        &mut self,
        mut verdicts: Vec<Verdict>,
        atomic: bool,
    ) -> Result<Vec<Verdict>, UnpackError> {
        let mut order: Vec<usize> = (0..verdicts.len()).collect();
        let trunk = self.trunk().clone();
        order.sort_by_key(|&i| verdicts[i].name != trunk);
        for &i in &order {
            let v = &verdicts[i];
            if v.kind.writes()
                && !matches!(
                    v.kind,
                    VerdictKind::DeleteCas | VerdictKind::DeleteUnconditional
                )
            {
                let (name, new) = (v.name.clone(), v.new);
                if let Plan::Reject(r) = BlobCaller::PushGraph.scope(self.plan(&name, new)).await? {
                    verdicts[i].kind = VerdictKind::Reject(r);
                }
            }
        }
        // No-ops last, so they never take own-pack budget from a write.
        for &i in &order {
            if matches!(verdicts[i].kind, VerdictKind::Noop) {
                let (name, new) = (verdicts[i].name.clone(), verdicts[i].new);
                self.place_noop(&name, new);
            }
        }
        self.run_hook(&mut verdicts).await?;
        if atomic && !self.req.invalid.is_empty() {
            for v in &mut verdicts {
                v.kind = VerdictKind::Reject(RejectReason::AtomicPeerRejected);
            }
        }
        Ok(if atomic {
            apply_atomic(verdicts)
        } else {
            verdicts
        })
    }

    /// The write for an accepted verdict, uploading its pack first if no
    /// earlier write did.
    async fn write_for(&mut self, v: &Verdict) -> Result<Option<RefWrite>, UnpackError> {
        if !v.kind.writes() {
            return Ok(None);
        }
        if matches!(
            v.kind,
            VerdictKind::DeleteCas | VerdictKind::DeleteUnconditional
        ) {
            return Ok(
                RefWrite::from_verdict(v, None, Deps::default(), Closure::Exact).map(|mut w| {
                    w.jobs.extend(self.mirror_job());
                    w
                }),
            );
        }
        let Some(Plan::Store {
            pack,
            deps,
            closure,
            commits,
            jobs,
        }) = self.plans.get(&(v.name.clone(), v.new)).cloned()
        else {
            return Err(UnpackError::Store(local_io(format!(
                "no plan for {} at {}",
                v.name, v.new
            ))));
        };
        let (pack_id, closure) = match pack {
            None => (None, closure),
            Some(pl) => {
                let id = BlobCaller::PushUpload
                    .scope(self.ensure_stored(pl.pack))
                    .await?;
                let closure = if self.pack_is_exact(pl) {
                    closure
                } else {
                    Closure::Wider
                };
                (Some(id), closure)
            }
        };
        // The new version's seq: one past the version the verdict was
        // computed against (always recorded by `verdict`), or 1.
        let seq = match v.against {
            None => Some(Seq::FIRST),
            Some(id) => self.seqs.get(&id).and_then(|s| s.next()),
        };
        let layout = self.config().cover_layout;
        Ok(
            RefWrite::from_verdict(v, pack_id, deps, closure).map(|mut w| {
                w.commits = commits;
                w.jobs = jobs;
                w.jobs.extend(self.mirror_job());
                if seq.is_some_and(|s| layout.ends_a_cover(s)) {
                    w.jobs.push(VersionJob {
                        kind: VersionJobKind::Compact,
                        run_after: UnixMicros::now(),
                    });
                }
                w
            }),
        )
    }

    /// Whether the placed pack holds only what the write's tip reaches.
    /// For a shared pack, valid once it is stored (frozen).
    fn pack_is_exact(&self, pl: Placement) -> bool {
        match pl.fit {
            Fit::Own => true,
            Fit::Contained => false,
            Fit::Shared { tip_at } => match &self.planned[pl.pack.0].contents {
                Contents::Shared(s) => s.spanning_tip() == Some(tip_at),
                _ => false,
            },
        }
    }

    async fn verdict(
        &mut self,
        command: &RefCommand,
        current: Option<&RefVersion>,
        options: &PushOptions,
    ) -> Result<Verdict, UnpackError> {
        if let Some(c) = current {
            self.seqs.insert(c.id, c.seq);
        }
        if !self.access.may_update(&command.name) {
            return Ok(Verdict {
                name: command.name.clone(),
                against: current.map(|v| v.id),
                new: command.new,
                kind: VerdictKind::Reject(RejectReason::PermissionDenied),
            });
        }
        // The version before the current one, read only when the command
        // may be an exact replay of the current version's own update
        // (issue #132): the ref already has the asked-for value, but the
        // command names another old value.
        let previous = match replay_parent(command, current, options) {
            Some(id) => self
                .svc
                .meta
                .ref_version(id)
                .await
                .map_err(UnpackError::Store)?,
            None => None,
        };
        let check = CommitCheck::start(CheckInput {
            settings: &self.repo.settings,
            command,
            current,
            options,
            scoped: self.scope,
            previous: previous.as_ref(),
        });
        match check {
            CommitCheck::Decided(v) => Ok(v),
            CommitCheck::NeedsAncestry(n) => {
                let (a, d) = (n.ancestor, n.descendant);
                let extra = current.map(|v| v.id).into_iter().collect::<Vec<_>>();
                let answer = self.ancestry(a, d, &extra).await?;
                n.resolve(AncestryAnswer::new(a, d, answer))
                    .map_err(|e| UnpackError::Store(local_io(e)))
            }
        }
    }

    /// Is `a` an ancestor of `d`? Unindexed commits are read from the
    /// push's search packs, then (only if still unknown) from the closures
    /// of `extra` versions.
    async fn ancestry(
        &mut self,
        a: ObjectId,
        d: ObjectId,
        extra: &[aspen_core::id::RefVersionId],
    ) -> Result<Ancestry, UnpackError> {
        let searched: Vec<_> = self
            .trunk_dep
            .iter()
            .chain(&self.scoped_dep)
            .map(|v| v.id)
            .collect();
        let lazy_roots = extra
            .iter()
            .copied()
            .filter(|v| !searched.contains(v))
            .collect();
        let mut walker = std::mem::replace(&mut self.walker, Walker::new(0));
        let source = self.commits_source(lazy_roots);
        let answer = walker.is_ancestor(&source, a, d).await;
        drop(source);
        self.timings.ancestry_commits = walker.expanded();
        self.walker = walker;
        answer.map_err(UnpackError::Store)
    }

    /// Whether this push may search beyond its dependencies
    /// ([`Push::widen`]): a client's. A mirror import applies what the
    /// upstream sent, which is always built against what Aspen advertised.
    fn may_widen(&self) -> bool {
        self.origin == Origin::Client
    }

    /// The new value of every command that is not a delete.
    fn tips(&self) -> Vec<ObjectId> {
        self.req
            .commands
            .iter()
            .filter(|c| !c.new.is_zero())
            .map(|c| c.new)
            .collect()
    }

    /// The received pack's boundary commits (`aspen_pack::boundary`) plus
    /// the commands' tips. A pack the scan cannot read gives only the tips:
    /// completion reports what is wrong with it.
    async fn boundary_of(&self, received: &Received) -> Result<Vec<ObjectId>, UnpackError> {
        let spool = received.spool.clone();
        let max = self.config().max_object_bytes;
        let scanned = blocking(move || aspen_pack::boundary::boundary(&spool, max))
            .await
            .map_err(UnpackError::Store)?;
        let mut ids = match scanned {
            Ok(b) => b.as_slice().to_vec(),
            Err(e) => {
                tracing::info!(error = %e, "boundary scan of the received pack failed");
                Vec::new()
            }
        };
        ids.extend(self.tips());
        Ok(ids)
    }

    /// **Boundary outside the dependencies.** With `push.negotiate`, Git
    /// leaves out of its pack every commit Aspen acknowledged in the
    /// negotiation fetch, and those can belong to any ref: the client never
    /// says which ref it will push (`upload_pack::fetch::negotiation_acks`).
    /// When some of `ids` (boundary commits and tips) are in neither the
    /// pushed pack nor what the push searches, each one's `commit_index` row
    /// names a version whose closure holds it and everything it reaches;
    /// those versions' closures are added to the packs every read searches
    /// (`wide`). Returns whether anything was added.
    ///
    /// The writes' dependencies do not change. Instead each write is planned
    /// with [`Push::fill`], which copies into its own pack every object its
    /// tip reaches that its dependencies lack, reading those from the wider
    /// search: the bytes the client would have sent without
    /// `push.negotiate`, read from storage instead. So every invariant of a
    /// write stays as it was (connected, exact when its dependencies are,
    /// trunk never depending on a branch), at the cost of server reads on
    /// these pushes only. A plain fast-forward of the pushed branch never
    /// gets here: its acknowledged commits are in the branch's own version,
    /// which the push already searches (`plain_url_branch_dep`).
    async fn widen(&mut self, ids: Vec<ObjectId>) -> Result<bool, UnpackError> {
        let store = UnpackError::Store;
        let ids = SortedIds::from_unsorted(ids);
        if ids.is_empty() {
            return Ok(false);
        }
        let svc = self.svc;
        let found = aspen_core::ingest::ObjectLocator::locate(&svc.packs, self.reads(), &ids)
            .await
            .map_err(store)?;
        let outside = SortedIds::from_unsorted(
            ids.as_slice()
                .iter()
                .zip(&found)
                .filter(|(_, f)| f.is_none())
                .map(|(id, _)| *id)
                .collect(),
        );
        if outside.is_empty() {
            return Ok(false);
        }
        let started = Instant::now();
        let rows = svc
            .meta
            .lookup_commits(self.repo.id, &outside)
            .await
            .map_err(store)?;
        let mut roots: Vec<RefVersionId> = self
            .trunk_dep
            .iter()
            .chain(&self.scoped_dep)
            .map(|v| v.id)
            .chain(self.wide_roots.iter().copied())
            .collect();
        let mut added = 0;
        for row in rows {
            if let CommitLookup::Indexed(row) = row
                && !roots.contains(&row.ref_version)
            {
                roots.push(row.ref_version);
                self.wide_roots.push(row.ref_version);
                added += 1;
            }
        }
        if added == 0 {
            self.timings.search += started.elapsed();
            return Ok(false);
        }
        self.wide = Some(
            closure_packs(&svc.meta, &svc.closures, self.repo.id, &roots)
                .await
                .map_err(store)?,
        );
        self.timings.search += started.elapsed();
        svc.metrics.push_widened();
        tracing::info!(
            outside = outside.len(),
            versions = added,
            "push builds on commits outside its dependencies; their versions are searched too"
        );
        Ok(true)
    }

    /// Prefetch, completion, the object graph and the whole-pack
    /// connectivity check, widening the search ([`Push::widen`]) when the
    /// pack's boundary is outside the dependencies.
    async fn ingest(&mut self, received: Received) -> Result<(), UnpackError> {
        let store = UnpackError::Store;
        let svc = self.svc;
        let started = Instant::now();
        let mut slot = svc.gate.acquire().await.map_err(store)?;
        self.timings.gate_wait = started.elapsed();
        let started = Instant::now();
        let mut prefetched = BlobCaller::PushPrefetch
            .scope(prefetch(
                &slot,
                &svc.packs,
                self.reads(),
                &received.candidates,
            ))
            .await?;
        self.timings.prefetch = started.elapsed();
        // A REF_DELTA base the dependencies lack is either in the pack (a
        // libgit2 push) or a thin base of a commit the client left out
        // because the negotiation acknowledged it although no dependency
        // holds it (`push.negotiate`). The boundary scan tells them apart.
        if !prefetched.not_in_deps.is_empty() && self.may_widen() {
            drop(prefetched);
            drop(slot);
            let boundary = self.boundary_of(&received).await?;
            let widened = BlobCaller::PushSearch.scope(self.widen(boundary)).await?;
            let started = Instant::now();
            slot = svc.gate.acquire().await.map_err(store)?;
            self.timings.gate_wait += started.elapsed();
            let started = Instant::now();
            prefetched = BlobCaller::PushPrefetch
                .scope(prefetch(
                    &slot,
                    &svc.packs,
                    self.reads(),
                    &received.candidates,
                ))
                .await?;
            if widened {
                tracing::debug!(
                    not_in_deps = prefetched.not_in_deps.len(),
                    "thin bases read again after widening"
                );
            }
            self.timings.prefetch += started.elapsed();
        }
        let started = Instant::now();
        let options = CompleteOptions {
            thread_limit: self.config().thread_limit,
            max_object_bytes: self.config().max_object_bytes,
            max_inflated_bytes: self.config().max_inflated_bytes,
        };
        let out_dir = self.work.join("completed");
        std::fs::create_dir_all(&out_dir).map_err(|e| store(local_io(e)))?;
        let received = Arc::new(received);
        let r = received.clone();
        // The slot moves with the bases into the blocking task, so it is
        // held until completion ends even if this request is dropped.
        let ingested = blocking(move || {
            let out = ingest::complete(&r, &prefetched.bases, &out_dir, &options);
            drop(prefetched);
            drop(slot);
            out
        })
        .await
        .map_err(store)??;
        self.timings.complete = started.elapsed();

        let started = Instant::now();
        let scan = ScanOptions {
            commits: true,
            missing: true,
            thread_limit: self.config().thread_limit,
            max_object_bytes: self.config().max_object_bytes,
            max_inflated_bytes: self.config().max_inflated_bytes,
            verify_checksums: false,
        };
        let (pack_path, idx_path, size) = (
            ingested.pack_path.clone(),
            ingested.idx_path.clone(),
            ingested.size,
        );
        let (graph, table) = blocking(move || {
            let graph =
                PackGraph::scan_keeping_trees(&pack_path, &idx_path, scan, MAX_DIFFED_TREE_BYTES)
                    .map_err(UnpackError::from)?;
            let layout =
                |e: aspen_pack::LayoutError| store(local_io(format!("completed pack: {e}")));
            let idx_bytes = std::fs::read(&idx_path).map_err(|e| store(local_io(e)))?;
            let idx = aspen_pack::idx::IdxV2::parse(&idx_bytes, Some(size))
                .map_err(|e| layout(e.into()))?;
            let file = std::fs::File::open(&pack_path).map_err(|e| store(local_io(e)))?;
            let table = aspen_pack::entries::build(file, size, &idx).map_err(layout)?;
            Ok::<_, UnpackError>((graph, table))
        })
        .await
        .map_err(store)??;
        // Which of the pack's objects the dependencies already hold
        // (completion bases, and anything the client sent again), and what
        // the pack references and lacks, which must be in the dependencies
        // (connectivity). The walk of the changed paths answers both for
        // whatever the push did not change without a lookup; only the rest
        // is looked up.
        let known: Vec<ObjectId> = self
            .trunk_dep
            .iter()
            .chain(&self.scoped_dep)
            .filter_map(|v| v.value.tip())
            .collect();
        let mut source = DepTrees {
            svc,
            repo: self.repo.id,
            search: &self.search,
        };
        let changed = aspen_pack::changed::changed_paths(
            aspen_pack::changed::ChangedInput {
                graph: &graph,
                known: &known,
            },
            &mut source,
        )
        .await
        .map_err(store)?;
        let missing = match changed {
            Some(ch) => {
                self.timings.changed_entries += ch.stats.entries;
                let mut held = ch.held;
                // Completion bases came from the packs reads search, which
                // are the dependencies' unless the search was widened
                // (`Push::widen`); then a base may be another ref's object,
                // and only the lookup below may call it held.
                if self.wide.is_none() {
                    for id in ingested.completion_bases.as_slice() {
                        if let Some(at) = graph.position(id) {
                            held[at] = true;
                        }
                    }
                }
                // One lookup answers both questions: which of the rest the
                // dependencies hold, and whether what the pack references
                // and lacks is there. The references of objects that turn
                // out held are looked up for nothing, which costs IDs, not
                // a second round trip (item 11).
                let rest: Vec<usize> = (0..graph.len()).filter(|&at| !held[at]).collect();
                let mut ids: Vec<ObjectId> = rest.iter().map(|&at| graph.id(at)).collect();
                ids.extend_from_slice(&ch.check);
                for &at in &rest {
                    if !ch.visited[at] {
                        ids.extend_from_slice(graph.external_refs(at));
                    }
                }
                let ids = SortedIds::from_unsorted(ids);
                self.timings.located += ids.len() as u64;
                let found =
                    aspen_core::ingest::ObjectLocator::locate(&svc.packs, &self.search, &ids)
                        .await
                        .map_err(store)?;
                let mut present = std::collections::HashSet::with_capacity(ids.len());
                for (id, f) in ids.as_slice().iter().zip(found) {
                    if f.is_some() {
                        present.insert(*id);
                        if let Some(at) = graph.position(id) {
                            held[at] = true;
                        }
                    }
                }
                let mut check = ch.check;
                for (at, (&visited, &held)) in ch.visited.iter().zip(&held).enumerate() {
                    if !visited && !held {
                        check.extend_from_slice(graph.external_refs(at));
                    }
                }
                self.held = held;
                self.entered_trees = ch.trees;
                // What the lookup found in the packs the connectivity check
                // searches needs no second lookup.
                if self.wide.is_none() {
                    check.retain(|id| !present.contains(id));
                }
                SortedIds::from_unsorted(check)
            }
            None => {
                let all = SortedIds::from_unsorted(graph.ids().to_vec());
                self.timings.located += all.len() as u64;
                let found =
                    aspen_core::ingest::ObjectLocator::locate(&svc.packs, &self.search, &all)
                        .await
                        .map_err(store)?;
                self.held = found.iter().map(Option::is_some).collect();
                graph.missing()
            }
        };
        self.timings.located += missing.len() as u64;
        // A miss may be a commit the negotiation acknowledged that no
        // dependency holds (`push.negotiate`); what is missing, with the
        // tips, is then the boundary to widen the search by.
        if let Err(e) =
            aspen_pack::scan::check_connectivity(&svc.packs, self.reads(), &missing).await
        {
            let mut boundary = missing.as_slice().to_vec();
            boundary.extend(self.tips());
            if !(matches!(e, UnpackError::MissingObject { .. })
                && self.may_widen()
                && self.widen(boundary).await?)
            {
                return Err(e);
            }
            aspen_pack::scan::check_connectivity(&svc.packs, self.reads(), &missing).await?;
        }
        let pos_of_entry = table
            .entries()
            .iter()
            .map(|e| {
                graph
                    .position(&e.id)
                    .ok_or_else(|| store(local_io("entry table and .idx disagree")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut entry_of_pos = vec![0; pos_of_entry.len()];
        for (e, &at) in pos_of_entry.iter().enumerate() {
            entry_of_pos[at] = e;
        }
        for c in graph.commits() {
            self.walker.seed(
                c.id,
                CommitNode {
                    parents: c.parents.clone(),
                    time: c.committer_time,
                    generation: None,
                },
            );
        }
        let n = graph.len();
        self.own_of = vec![NO_OWNER; n];
        self.own_budget = n;
        self.own_probe_left = n;
        self.scratch = vec![false; n];
        self.pack = Some(PushedPack {
            ingested,
            table,
            graph,
            pos_of_entry,
            entry_of_pos,
        });
        self.timings.graph += started.elapsed();
        Ok(())
    }

    /// The kind of `id`, from the pushed pack or the search packs; `None`
    /// if neither has it.
    async fn kind_of(&self, id: ObjectId) -> Result<Option<ObjectKind>, UnpackError> {
        if let Some(p) = &self.pack
            && let Some(at) = p.graph.position(&id)
        {
            return Ok(Some(p.graph.kind(at)));
        }
        Ok(self.read_object(id).await?.map(|(k, _)| k))
    }

    /// The plan for writing `tip` to `name`, computed once.
    async fn plan(&mut self, name: &RefName, tip: ObjectId) -> Result<Plan, UnpackError> {
        let key = (name.clone(), tip);
        if let Some(p) = self.plans.get(&key) {
            return Ok(p.clone());
        }
        let started = Instant::now();
        let plan = self.make_plan(name, tip).await?;
        self.timings.graph += started.elapsed();
        self.plans.insert(key, plan.clone());
        Ok(plan)
    }

    async fn make_plan(&mut self, name: &RefName, tip: ObjectId) -> Result<Plan, UnpackError> {
        let store = UnpackError::Store;
        // The tip must exist (in the pack or the dependencies), and a branch
        // must point at a commit.
        let kind = self
            .kind_of(tip)
            .await?
            .ok_or(UnpackError::MissingObject { id: tip })?;
        if name.is_branch() && kind != ObjectKind::Commit {
            return Ok(Plan::Reject(RejectReason::NotACommit));
        }
        let trunk = self.trunk().clone();
        let is_trunk = name == &trunk;
        // A trunk write whose trunk dependency is not an ancestor of the
        // new tip is a forced trunk update: it gets its own dependency and
        // the skipped versions' objects (decisions log, forced trunk rule).
        let forced_trunk = match (&self.trunk_dep, is_trunk) {
            (Some(dep), true) => {
                let dep_tip = dep.value.tip().expect("live");
                match self.ancestry(dep_tip, tip, &[]).await? {
                    Ancestry::Descendant => false,
                    Ancestry::NotDescendant => true,
                    Ancestry::Undecided => {
                        return Ok(Plan::Reject(RejectReason::AncestryUndecided));
                    }
                }
            }
            _ => false,
        };
        let (deps, dep_versions, pack, mut commits, all_rows) = if forced_trunk {
            match self.forced_trunk(tip).await? {
                None => return Ok(Plan::Reject(RejectReason::AncestryUndecided)),
                Some((a, pack, commits)) => (
                    Deps {
                        trunk: a.as_ref().map(|v| v.id),
                        scoped_old: None,
                    },
                    a.into_iter().collect::<Vec<_>>(),
                    pack,
                    commits,
                    true,
                ),
            }
        } else {
            let deps = Deps {
                trunk: self.trunk_dep.as_ref().map(|v| v.id),
                scoped_old: self.scoped_dep.as_ref().map(|v| v.id),
            };
            let dep_versions: Vec<RefVersion> = self
                .trunk_dep
                .iter()
                .chain(&self.scoped_dep)
                .cloned()
                .collect();
            let (pack, commits, all) = match self.noop_rows.remove(&tip) {
                // A no-op placed this tip first (`place_noop`): this write
                // takes its placement and the commit rows that came with it.
                Some((rows, all)) => (self.by_tip.get(&tip).copied(), rows, all),
                None if self.wide.is_some() => self.fill_write(is_trunk, tip).await?,
                None => self.place(is_trunk, tip),
            };
            (deps, dep_versions, pack, commits, all)
        };

        // Closure, if the write's pack holds only what its tip reaches
        // (the placement decides that, `write_for` applies it): exact when
        // every dependency is exact and its tip is an ancestor of the new
        // tip. Reachability from a tag is reachability from what it peels
        // to.
        let peeled = self.peel(tip, kind).await?;
        // The branch's own dependency is answered first: when its version
        // is exact and depends on this same trunk version, its tip already
        // reaches trunk's (an exact closure needs every dependency's tip to
        // be an ancestor), so a new tip that reaches the branch's reaches
        // trunk's too. Without this, every push to a branch walked the
        // branch's whole history since trunk again, one commit lookup per
        // commit, and a long-lived branch got slower with every push
        // (bench/throughput).
        let mut order: Vec<usize> = (0..dep_versions.len()).collect();
        order.sort_by_key(|&i| dep_versions[i].name == trunk);
        let mut answered: Vec<Option<Ancestry>> = vec![None; dep_versions.len()];
        for &i in &order {
            let d = &dep_versions[i];
            let dt = d.value.tip().expect("live dependency");
            let proven = d.name == trunk
                && dep_versions.iter().zip(&answered).any(|(s, a)| {
                    s.name != trunk
                        && *a == Some(Ancestry::Descendant)
                        && s.value.closure() == Some(Closure::Exact)
                        && s.deps.trunk == Some(d.id)
                });
            answered[i] = Some(match peeled {
                _ if proven => Ancestry::Descendant,
                Some(c) => self.ancestry(dt, c, &[d.id]).await?,
                // A tree or blob reaches no commit.
                None => Ancestry::NotDescendant,
            });
        }
        let mut answers = Vec::with_capacity(dep_versions.len());
        for (d, a) in dep_versions.iter().zip(answered) {
            let dt = d.value.tip().expect("live dependency");
            let a = a.expect("every dependency answered");
            answers.push((d.value, Some(AncestryAnswer::new(dt, tip, a))));
        }
        let closure = Closure::of_new_version(tip, true, answers.iter().map(|(v, a)| (v, *a)))
            .map_err(|e| store(local_io(e)))?;
        if pack.is_none() && deps.trunk.is_none() && deps.scoped_old.is_none() {
            // Nothing stored and nothing depended on: the tip's objects are
            // nowhere. Only an orphan forced trunk update could get here,
            // and it always stores a pack.
            return Err(UnpackError::MissingObject { id: tip });
        }

        // Commit index rows: the commits this write's placement made it the
        // first to store, up to the configured number; the tip's row
        // always. A write that stores more, or shares commits another write
        // carries the rows of, also gets an `index_commits` job.
        let mut jobs = Vec::new();
        let index_job = VersionJob {
            kind: VersionJobKind::IndexCommits,
            run_after: UnixMicros::now(),
        };
        if commits.len() > self.config().sync_commit_rows {
            commits.retain(|c| c.commit == tip);
            jobs.push(index_job);
        } else if !all_rows {
            jobs.push(index_job);
        }
        if kind == ObjectKind::Commit && !commits.iter().any(|c| c.commit == tip) {
            match self.pack.as_ref().and_then(|p| p.graph.commit(&tip)) {
                Some(c) => commits.push(c.to_new_commit()),
                None => {
                    // A tip the push did not bring (a new branch at an
                    // existing commit) still needs its row if the index
                    // lacks one.
                    let ids = SortedIds::from_unsorted(vec![tip]);
                    let row = self
                        .svc
                        .meta
                        .lookup_commits(self.repo.id, &ids)
                        .await
                        .map_err(store)?;
                    if matches!(row.first(), Some(CommitLookup::NotIndexed)) {
                        match self.commit_node(tip).await? {
                            Some(node) => commits.push(NewCommit {
                                commit: tip,
                                parents: node.parents,
                                committer_time: node.time,
                                generation: node.generation,
                                root_tree: None,
                            }),
                            None => return Ok(Plan::Reject(RejectReason::AncestryUndecided)),
                        }
                    }
                }
            }
        }
        // Generation numbers for the rows, from the pushed pack, the tips of
        // the refs this push read and the rows its ancestry questions
        // already read (`Walker::known_generation`): never a statement of
        // their own, so a branch push costs what it did before generations
        // (throughput item 6b). A row left without one (a commit on a parent
        // that is neither a tip it read nor a row it read) gets it from the
        // `index_commits` job's backfill.
        let mut walker = std::mem::replace(&mut self.walker, Walker::new(0));
        for c in &mut commits {
            if c.generation.is_none() {
                c.generation = walker.known_generation(c.commit);
            }
        }
        self.walker = walker;
        if commits.iter().any(|c| c.generation.is_none())
            && !jobs.iter().any(|j| j.kind == VersionJobKind::IndexCommits)
        {
            jobs.push(index_job);
        }
        Ok(Plan::Store {
            pack,
            deps,
            closure,
            commits,
            jobs,
        })
    }

    /// Places a no-op's tip before any pack is stored, as if it were a
    /// write (issue #49), so that if a recomputed verdict turns the no-op
    /// into a write, its objects already have a place. Only a tip in the
    /// pushed pack that the dependencies do not hold has one (`place`
    /// returns nothing otherwise, and a no-op's tip need not be in either).
    /// Nothing is stored for it unless a write names the pack; the commit
    /// rows the placement hands out are kept for that write.
    fn place_noop(&mut self, name: &RefName, tip: ObjectId) {
        if self.wide.is_some() {
            // Writes get their own packs then (`fill_write`), made when
            // they are planned; no shared pack can be opened late.
            return;
        }
        if self.by_tip.contains_key(&tip) {
            return;
        }
        let is_trunk = name == self.trunk();
        let (placement, rows, all) = self.place(is_trunk, tip);
        if placement.is_some() {
            self.noop_rows.insert(tip, (rows, all));
        }
    }

    /// Decides where a non-forced write's objects go (decisions log,
    /// "where a write's objects are stored"), in order:
    ///
    /// 1. nothing, when the tip is outside the pushed pack or held by the
    ///    dependencies;
    /// 2. the placement of an earlier write at the same tip;
    /// 3. trunk: its own pack, always;
    /// 4. a planned pack that already holds the tip (trunk's, an own pack,
    ///    a stored shared pack): every planned pack is closed under
    ///    references, so it holds everything the tip reaches. `Wider`;
    /// 5. its own pack, if it fits in what is left of the own-pack budget
    ///    (the pushed pack's object count minus trunk's), so all own packs
    ///    together never hold more objects than the pushed pack. Walks
    ///    that do not fit stop early, and stop being tried once they have
    ///    visited as many objects as the pushed pack holds;
    /// 6. the open shared pack.
    ///
    /// Returns the placement, the commit rows the write carries (commits
    /// it is the first to place) and whether those are all the commits its
    /// tip reaches in the pushed pack.
    fn place(
        &mut self,
        is_trunk: bool,
        tip: ObjectId,
    ) -> (Option<Placement>, Vec<NewCommit>, bool) {
        let Some(p) = &self.pack else {
            return (None, Vec::new(), true);
        };
        let Some(at) = p.graph.position(&tip).filter(|&at| !self.held[at]) else {
            return (None, Vec::new(), true);
        };
        if let Some(pl) = self.by_tip.get(&tip) {
            return (Some(*pl), Vec::new(), false);
        }
        let g = &p.graph;
        let rows = |positions: &mut dyn Iterator<Item = usize>| -> Vec<NewCommit> {
            positions
                .filter(|&a| g.kind(a) == ObjectKind::Commit)
                .filter_map(|a| g.commit(&g.id(a)).map(|c| c.to_new_commit()))
                .collect()
        };
        let (pl, commits, all) = 'placed: {
            if is_trunk && self.trunk_pack.is_none() {
                let reach = g.reach(&tip, &self.held);
                let commits = rows(
                    &mut reach
                        .members
                        .iter()
                        .enumerate()
                        .filter(|m| *m.1)
                        .map(|m| m.0),
                );
                self.own_budget = self.own_budget.saturating_sub(reach.count);
                let id = PlannedId(self.planned.len());
                self.planned.push(PlannedPack {
                    contents: Contents::Trunk(reach.members),
                    stored: None,
                });
                self.trunk_pack = Some(id);
                break 'placed (
                    Placement {
                        pack: id,
                        fit: Fit::Own,
                    },
                    commits,
                    true,
                );
            }
            let containing = self
                .trunk_pack
                .filter(|t| matches!(&self.planned[t.0].contents, Contents::Trunk(m) if m[at]))
                .or_else(|| {
                    (self.own_of[at] != NO_OWNER).then(|| PlannedId(self.own_of[at] as usize))
                })
                .or_else(|| {
                    self.planned
                        .iter()
                        .position(|pp| {
                            pp.stored.is_some()
                                && matches!(&pp.contents, Contents::Shared(s) if s.contains(at))
                        })
                        .map(PlannedId)
                });
            if let Some(id) = containing {
                break 'placed (
                    Placement {
                        pack: id,
                        fit: Fit::Contained,
                    },
                    Vec::new(),
                    false,
                );
            }
            if self.own_probe_left > 0 {
                match g.reach_within(at, &self.held, self.own_budget, &mut self.scratch) {
                    Some(positions) => {
                        let id = PlannedId(self.planned.len());
                        for &q in &positions {
                            if self.own_of[q as usize] == NO_OWNER {
                                self.own_of[q as usize] = id.0 as u32;
                            }
                        }
                        self.own_budget -= positions.len();
                        let commits = rows(&mut positions.iter().map(|&q| q as usize));
                        self.planned.push(PlannedPack {
                            contents: Contents::Own(positions),
                            stored: None,
                        });
                        break 'placed (
                            Placement {
                                pack: id,
                                fit: Fit::Own,
                            },
                            commits,
                            true,
                        );
                    }
                    None => {
                        self.own_probe_left =
                            self.own_probe_left.saturating_sub(self.own_budget + 1);
                    }
                }
            }
            let open = self
                .planned
                .iter()
                .position(|pp| pp.stored.is_none() && matches!(pp.contents, Contents::Shared(_)));
            let id = match open {
                Some(i) => PlannedId(i),
                None => {
                    // Every write is planned before any pack is stored (a
                    // recomputed verdict reuses its plan), so the shared
                    // pack is never frozen while placement goes on.
                    debug_assert!(
                        !self
                            .planned
                            .iter()
                            .any(|pp| matches!(pp.contents, Contents::Shared(_))),
                        "a second shared pack in one push"
                    );
                    self.planned.push(PlannedPack {
                        contents: Contents::Shared(SharedSet::new(g.len())),
                        stored: None,
                    });
                    PlannedId(self.planned.len() - 1)
                }
            };
            let Contents::Shared(set) = &mut self.planned[id.0].contents else {
                unreachable!("an open shared pack")
            };
            let added = set.add(g, at, &self.held);
            let commits = rows(&mut added.added.iter().map(|&q| q as usize));
            (
                Placement {
                    pack: id,
                    fit: Fit::Shared { tip_at: at },
                },
                commits,
                !added.met_existing,
            )
        };
        self.by_tip.insert(tip, pl);
        (Some(pl), commits, all)
    }

    /// The commit `id` (of type `kind`) peels to: itself for a commit, the
    /// end of its target chain for a tag, `None` when that is not a commit.
    async fn peel(
        &self,
        mut id: ObjectId,
        mut kind: ObjectKind,
    ) -> Result<Option<ObjectId>, UnpackError> {
        for _ in 0..64 {
            match kind {
                ObjectKind::Commit => return Ok(Some(id)),
                ObjectKind::Tree | ObjectKind::Blob => return Ok(None),
                ObjectKind::Tag => {}
            }
            let data = match self
                .pack
                .as_ref()
                .and_then(|p| p.graph.position(&id).map(|at| (p, at)))
            {
                Some((p, at)) => {
                    // A tag references exactly its target.
                    let g = &p.graph;
                    let target = g
                        .internal_refs(at)
                        .first()
                        .map(|&r| g.id(r as usize))
                        .or_else(|| g.external_refs(at).first().copied());
                    match target {
                        Some(t) => {
                            id = t;
                            kind = self
                                .kind_of(t)
                                .await?
                                .ok_or(UnpackError::MissingObject { id: t })?;
                            continue;
                        }
                        None => return Ok(None),
                    }
                }
                None => self.read_object(id).await?,
            };
            let Some((_, data)) = data else {
                return Err(UnpackError::MissingObject { id });
            };
            let mut target = None;
            references(ObjectKind::Tag, &data, |t| target = Some(t)).map_err(|e| {
                UnpackError::Store(StoreError::Corrupt {
                    what: format!("stored tag {id}: {e}"),
                })
            })?;
            let Some(t) = target else { return Ok(None) };
            id = t;
            kind = self
                .kind_of(t)
                .await?
                .ok_or(UnpackError::MissingObject { id: t })?;
        }
        Ok(None)
    }

    /// One object from the search packs, read under a prefetch slot.
    async fn read_object(
        &self,
        id: ObjectId,
    ) -> Result<Option<(ObjectKind, Vec<u8>)>, UnpackError> {
        let one = CandidateBases(SortedIds::from_unsorted(vec![id]));
        let slot = self.svc.gate.acquire().await.map_err(UnpackError::Store)?;
        let fetched = match slot.prefetch(&self.svc.packs, self.reads(), &one).await {
            Ok((f, _)) => f.bases,
            Err(PrefetchError::OverBudget { budget }) => {
                return Err(UnpackError::BasesOverBudget { budget });
            }
            Err(PrefetchError::Store(e)) => return Err(UnpackError::Store(e)),
        };
        let mut buf = Vec::new();
        let found = fetched
            .find_base(&id, &mut buf)
            .map_err(UnpackError::Store)?
            .map(|k| (k, buf));
        drop(fetched);
        drop(slot);
        Ok(found)
    }

    /// The parents and time of `id`, through the walker's sources.
    async fn commit_node(&mut self, id: ObjectId) -> Result<Option<CommitNode>, UnpackError> {
        self.commit_node_with(id, &[]).await
    }

    /// [`Self::commit_node`], also reading the closures of `extra` for a
    /// commit neither the index nor the search packs have.
    async fn commit_node_with(
        &mut self,
        id: ObjectId,
        extra: &[aspen_core::id::RefVersionId],
    ) -> Result<Option<CommitNode>, UnpackError> {
        let searched: Vec<_> = self
            .trunk_dep
            .iter()
            .chain(&self.scoped_dep)
            .map(|v| v.id)
            .collect();
        let lazy = extra
            .iter()
            .copied()
            .filter(|v| !searched.contains(v))
            .collect();
        let source = self.commits_source(lazy);
        let l = super::ancestry::CommitSource::lookup(&source, &SortedIds::from_unsorted(vec![id]))
            .await
            .map_err(UnpackError::Store)?;
        Ok(match l.into_iter().next() {
            Some(Lookup::Commit(n)) => Some(n),
            _ => None,
        })
    }

    /// The forced-trunk rule: the newest of the last few trunk versions
    /// whose tip is an ancestor of `tip` becomes the dependency (none for
    /// orphan history, or when none of them is), and every object `tip`
    /// reaches outside that dependency's closure goes into the write's
    /// pack: the pack's own objects by membership, the skipped versions'
    /// objects as extra full objects. `None` when ancestry is undecided.
    ///
    /// The skipped objects are read under one prefetch slot, held from the
    /// first read until the last object is spilled to local disk, so the
    /// memory held at once is one prefetch call's, as for any other read.
    #[allow(clippy::type_complexity)]
    async fn forced_trunk(
        &mut self,
        tip: ObjectId,
    ) -> Result<Option<(Option<RefVersion>, Option<Placement>, Vec<NewCommit>)>, UnpackError> {
        let store = UnpackError::Store;
        let meta = &self.svc.meta;
        let trunk = self.trunk().clone();
        let cur = self
            .trunk_dep
            .clone()
            .expect("a forced trunk update has a trunk");
        let k = self.config().forced_trunk_candidates as u64;
        let seqs: Vec<_> = (0..k).filter_map(|back| cur.seq.back(back)).collect();
        let mut candidates = meta
            .versions_by_seq(self.repo.id, &trunk, &seqs)
            .await
            .map_err(store)?;
        candidates.sort_by_key(|v| std::cmp::Reverse(v.seq));
        let mut chosen = None;
        // Ancestry walks take their own slots: no slot is held here yet.
        for v in candidates {
            let Some(vt) = v.value.tip() else { continue };
            if v.value.closure() != Some(Closure::Exact) {
                continue;
            }
            match self.ancestry(vt, tip, &[v.id]).await? {
                Ancestry::Descendant => {
                    chosen = Some(v);
                    break;
                }
                Ancestry::NotDescendant => {}
                Ancestry::Undecided => return Ok(None),
            }
        }
        let meta = &self.svc.meta;
        let dep_packs = match &chosen {
            Some(v) => closure_packs(meta, &self.svc.closures, self.repo.id, &[v.id])
                .await
                .map_err(store)?,
            None => SearchPacks::default(),
        };
        let held: Vec<bool> = match &self.pack {
            Some(p) => {
                let all = SortedIds::from_unsorted(p.graph.ids().to_vec());
                aspen_core::ingest::ObjectLocator::locate(&self.svc.packs, &dep_packs, &all)
                    .await
                    .map_err(store)?
                    .iter()
                    .map(Option::is_some)
                    .collect()
            }
            None => Vec::new(),
        };
        let (pack, commits) = self.fill(tip, &dep_packs, &held, true).await?;
        Ok(Some((chosen, pack, commits)))
    }

    /// Plans a write while the search is widened ([`Push::widen`]): with
    /// its usual dependencies (`search`), and a pack that holds everything
    /// its tip reaches that they lack, copied from the wider search where
    /// the pushed pack lacks it ([`Push::fill`]). Writes at one tip share
    /// one pack; the first one carries the commit rows.
    async fn fill_write(
        &mut self,
        is_trunk: bool,
        tip: ObjectId,
    ) -> Result<(Option<Placement>, Vec<NewCommit>, bool), UnpackError> {
        if let Some(pl) = self.filled.get(&tip) {
            return Ok((*pl, Vec::new(), pl.is_none()));
        }
        let deps = self.search.clone();
        let held = std::mem::take(&mut self.held);
        let filled = self.fill(tip, &deps, &held, is_trunk).await;
        self.held = held;
        let (pl, commits) = filled?;
        self.filled.insert(tip, pl);
        Ok((pl, commits, true))
    }

    /// The pack of a write whose dependencies' closure is `dep_packs`: every
    /// object `tip` reaches that `dep_packs` lack, as entries of the pushed
    /// pack (those `held` does not mark: `held[i]` says `dep_packs` hold
    /// the pushed pack's object `i`) plus full objects read from the packs
    /// every read searches ([`Push::reads`]) and spilled to local disk.
    /// `None` when the dependencies hold everything. `keep_held_bases` keeps
    /// a kept delta's held base as an entry, as trunk's pack does
    /// ([`keep_flags`]); otherwise such a delta is stored whole.
    ///
    /// The objects outside the pushed pack are read under one prefetch
    /// slot, held from the first read until the last object is spilled, so
    /// the memory held at once is one prefetch call's, as for any other
    /// read.
    async fn fill(
        &mut self,
        tip: ObjectId,
        dep_packs: &SearchPacks,
        held: &[bool],
        keep_held_bases: bool,
    ) -> Result<(Option<Placement>, Vec<NewCommit>), UnpackError> {
        let store = UnpackError::Store;
        let n = self.pack.as_ref().map_or(0, |p| p.graph.len());
        let mut members = vec![false; n];
        let mut spill: Option<ExtraSpill> = None;
        let budget = self.config().locate.max_prefetch_bytes;
        let mut checked = std::collections::HashSet::new();
        let mut queue = vec![tip];
        let mut slot = None;
        while !queue.is_empty() {
            let mut outside = Vec::new();
            for id in std::mem::take(&mut queue) {
                match self.pack.as_ref().and_then(|p| p.graph.position(&id)) {
                    Some(at) => {
                        if held[at] || members[at] {
                            continue;
                        }
                        members[at] = true;
                        let g = &self.pack.as_ref().expect("in the pack").graph;
                        queue.extend(g.internal_refs(at).iter().map(|&r| g.id(r as usize)));
                        queue.extend_from_slice(g.external_refs(at));
                    }
                    None => {
                        if checked.insert(id) {
                            outside.push(id);
                        }
                    }
                }
            }
            if outside.is_empty() {
                continue;
            }
            let outside = SortedIds::from_unsorted(outside);
            let in_dep =
                aspen_core::ingest::ObjectLocator::locate(&self.svc.packs, dep_packs, &outside)
                    .await
                    .map_err(store)?;
            let to_read: Vec<ObjectId> = outside
                .as_slice()
                .iter()
                .zip(&in_dep)
                .filter(|(_, f)| f.is_none())
                .map(|(id, _)| *id)
                .collect();
            if to_read.is_empty() {
                continue;
            }
            if slot.is_none() {
                slot = Some(self.svc.gate.acquire().await.map_err(store)?);
            }
            let held_slot = slot.as_ref().expect("acquired");
            let read = match held_slot
                .prefetch(
                    &self.svc.packs,
                    self.reads(),
                    &CandidateBases(SortedIds::from_unsorted(to_read.clone())),
                )
                .await
            {
                Ok((f, _)) => f,
                Err(PrefetchError::OverBudget { budget }) => {
                    return Err(UnpackError::BasesOverBudget { budget });
                }
                Err(PrefetchError::Store(e)) => return Err(store(e)),
            };
            if let Some(missing) = read.not_in_deps.as_slice().first() {
                return Err(UnpackError::MissingObject { id: *missing });
            }
            if spill.is_none() {
                self.next_dir += 1;
                let path = self.work.join(format!("fill-{}.extras", self.next_dir));
                spill = Some(ExtraSpill::create(path).map_err(|e| store(local_io(e)))?);
            }
            let sp = spill.as_mut().expect("created");
            let mut buf = Vec::new();
            for id in to_read {
                let kind = read
                    .bases
                    .find_base(&id, &mut buf)
                    .map_err(store)?
                    .ok_or(UnpackError::MissingObject { id })?;
                references(kind, &buf, |r| queue.push(r)).map_err(|e| {
                    store(StoreError::Corrupt {
                        what: format!("stored object {id}: {e}"),
                    })
                })?;
                sp.push(id, kind, &buf).map_err(|e| store(local_io(e)))?;
                if self.wide.is_some() {
                    // A small push naming a commit of another ref must not
                    // make the server copy more than the client could
                    // have sent.
                    self.copied_bytes += buf.len() as u64;
                    if self.copied_bytes > self.copy_limit {
                        return Err(UnpackError::LeftOutTooLarge {
                            limit: self.copy_limit,
                        });
                    }
                }
                if sp.bytes() > budget {
                    return Err(UnpackError::BasesOverBudget { budget });
                }
            }
        }
        if let Some(sp) = spill.as_mut() {
            sp.finish().map_err(|e| store(local_io(e)))?;
        }
        drop(slot);
        let commits: Vec<NewCommit> = match &self.pack {
            Some(p) => p
                .graph
                .commits()
                .iter()
                .filter(|c| p.graph.position(&c.id).is_some_and(|at| members[at]))
                .map(|c| c.to_new_commit())
                .collect(),
            None => Vec::new(),
        };
        let keep = match &self.pack {
            Some(p) if members.iter().any(|m| *m) => {
                keep_flags(p, &members, keep_held_bases.then_some(held))
            }
            Some(p) => vec![false; p.table.entries().len()],
            None => Vec::new(),
        };
        // Charge what the pack keeps of the pushed pack against the
        // own-pack budget, as a non-forced trunk's reach is, so the push's
        // packs still hold at most twice the pushed pack's objects (plus the
        // spilled objects, which the client did not send).
        self.own_budget = self
            .own_budget
            .saturating_sub(keep.iter().filter(|k| **k).count());
        let pack = if keep.iter().any(|k| *k) || spill.is_some() {
            let id = PlannedId(self.planned.len());
            self.planned.push(PlannedPack {
                contents: Contents::Filled { keep, spill },
                stored: None,
            });
            Some(Placement {
                pack: id,
                fit: Fit::Own,
            })
        } else {
            None
        };
        Ok((pack, commits))
    }

    /// Registers a planned pack and uploads it to the row's keys, once; later writes that name
    /// it reuse the row. A shared pack is frozen by this: later writes go
    /// to a new one.
    async fn ensure_stored(&mut self, pid: PlannedId) -> Result<PackId, UnpackError> {
        let store = UnpackError::Store;
        if let Some(id) = self.planned[pid.0].stored {
            return Ok(id);
        }
        let p = self.pack.as_ref();
        // Only trunk's pack keeps held delta bases: own and shared packs
        // store such deltas whole, so they never hold an object outside
        // what their tips reach and stay within the storage bound
        // (decisions log, "where a write's objects are stored").
        let flags_of = |members: &[bool], held: Option<&[bool]>| {
            keep_flags(p.expect("planned from the pack"), members, held)
        };
        let (keep, spill) = match &mut self.planned[pid.0].contents {
            Contents::Trunk(members) => (flags_of(members, Some(&self.held)), None),
            Contents::Shared(set) => (flags_of(&set.members(), None), None),
            c @ Contents::Own(_) => {
                let Contents::Own(positions) = std::mem::replace(c, Contents::Done) else {
                    unreachable!()
                };
                let mut members = vec![false; self.held.len()];
                for q in positions {
                    members[q as usize] = true;
                }
                (flags_of(&members, None), None)
            }
            c @ Contents::Filled { .. } => {
                let Contents::Filled { keep, spill } = std::mem::replace(c, Contents::Done) else {
                    unreachable!()
                };
                (keep, spill)
            }
            Contents::Done => {
                return Err(store(local_io(
                    "a planned pack was emptied before it was stored",
                )));
            }
        };
        let whole = spill.is_none() && !keep.is_empty() && keep.iter().all(|k| *k);
        let received_bytes = std::mem::take(&mut self.received_bytes_left);
        let (paths, new_pack): ((PathBuf, PathBuf, PathBuf), NewPack) = if whole {
            // Every object is this write's: store the completed pack as is.
            let ing = &p.expect("keep flags come from a pack").ingested;
            let mut np = ing.new_pack(self.repo.id);
            np.origin = aspen_core::pack::PackOrigin::Push { received_bytes };
            (
                (
                    ing.pack_path.clone(),
                    ing.idx_path.clone(),
                    ing.entries_path.clone(),
                ),
                np,
            )
        } else {
            self.next_dir += 1;
            let out = self.work.join(format!("write-{}", self.next_dir));
            let source = p.filter(|_| !keep.is_empty()).map(|p| {
                (
                    p.ingested.pack_path.clone(),
                    p.ingested.idx_path.clone(),
                    p.table.clone(),
                )
            });
            let max = self.config().max_object_bytes;
            let sub = blocking(move || {
                let source = source.as_ref().map(|(p, i, t)| SubsetSource {
                    pack: p,
                    idx: i,
                    table: t,
                    keep: &keep,
                });
                let none: &[ExtraObject] = &[];
                match &spill {
                    Some(s) => write_subset(source, s, &out, max),
                    None => write_subset(source, none, &out, max),
                }
            })
            .await
            .map_err(store)?
            .map_err(|e| store(local_io(format!("writing a write's pack: {e}"))))?;
            let np = sub.new_pack(self.repo.id, received_bytes);
            ((sub.pack_path, sub.idx_path, sub.entries_path), np)
        };
        let started = Instant::now();
        // Register, then upload, then (in the write's commit) reference.
        // The row's generation fixes the keys, and a checksum garbage
        // collection deleted before gets a new generation, so no pending
        // blob deletion can remove what is uploaded here (issue #48). An
        // existing row is uploaded to as well: its writer may have died
        // before uploading, and the bytes are the same.
        let record = self.svc.register_pack(&new_pack).await.map_err(store)?;
        let keys = record.keys();
        let blobs = &self.svc.blobs;
        let (a, b, c) = futures_util::future::join3(
            blobs.upload_file(&keys.pack, &paths.0),
            blobs.upload_file(&keys.idx, &paths.1),
            blobs.upload_file(&keys.entries, &paths.2),
        )
        .await;
        a.map_err(store)?;
        b.map_err(store)?;
        c.map_err(store)?;
        let id = record.id;
        // Later pushes and fetches of this ref search this pack: hand the
        // locator its `.idx` and chain map now rather than have them read
        // back (a round trip per push, #319 and item 11). Best effort.
        if self.svc.packs.keeps_whole(record.pack.idx_size) {
            let (idx, entries) = (paths.1.clone(), paths.2.clone());
            if let Ok((Ok(idx), Ok(table))) =
                blocking(move || (std::fs::read(idx), std::fs::read(entries))).await
            {
                self.svc.packs.remember_pack(record, idx, &table);
            }
        }
        self.timings.upload += started.elapsed();
        self.planned[pid.0].stored = Some(id);
        Ok(id)
    }
}

/// Which entries of the pushed pack (in entry-table order) a pack stores:
/// its members, plus, when `held` is given (trunk's pack), every delta
/// base the dependencies already hold, transitively. A kept delta whose
/// base is not kept is stored undeltified by the subset writer.
fn keep_flags(p: &PushedPack, members: &[bool], held: Option<&[bool]>) -> Vec<bool> {
    let entries = p.table.entries();
    let mut keep: Vec<bool> = p.pos_of_entry.iter().map(|&at| members[at]).collect();
    let Some(held) = held else {
        return keep;
    };
    for i in (0..entries.len()).rev() {
        if !keep[i] {
            continue;
        }
        let base = match entries[i].kind {
            EntryKind::OfsDelta { base_offset } => entries
                .binary_search_by_key(&base_offset, |e| e.offset)
                .ok(),
            EntryKind::RefDelta { base } => p.graph.position(&base).map(|at| p.entry_of_pos[at]),
            _ => None,
        };
        if let Some(b) = base
            && !keep[b]
            && held[p.pos_of_entry[b]]
        {
            keep[b] = true;
        }
    }
    keep
}

/// Most bytes of pushed trees kept in memory for the walk of the changed
/// paths. A push whose trees add up to more (an import of a large tree)
/// looks up every object it references instead, as before the walk.
const MAX_DIFFED_TREE_BYTES: u64 = 16 << 20;

/// Most bytes one read of old trees (or commits, for their root trees) for
/// that walk may hold; over it, those trees count as unreadable and the
/// walk looks up their entries instead.
const OLD_TREE_READ_BYTES: u64 = 16 << 20;

/// The dependencies' trees, for the walk of the changed paths: from trunk's
/// newest trees in memory, else read from the push's search packs (and
/// then kept in memory).
struct DepTrees<'a, M, B> {
    svc: &'a ReceivePack<M, B>,
    repo: aspen_core::id::RepoId,
    search: &'a SearchPacks,
}

impl<M, B> DepTrees<'_, M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// Reads objects of `kind` from the search packs; `None` for one they
    /// lack, of another kind, or over the read budget.
    async fn read(
        &self,
        ids: &[ObjectId],
        kind: ObjectKind,
    ) -> Result<Vec<Option<Vec<u8>>>, StoreError> {
        let wanted = aspen_core::ingest::CandidateBases(SortedIds::from_unsorted(ids.to_vec()));
        let bases = match self
            .svc
            .packs
            .prefetch_bases_within(self.search, &wanted, OLD_TREE_READ_BYTES)
            .await
        {
            Ok((p, _)) => p.bases,
            Err(PrefetchError::OverBudget { .. }) => return Ok(vec![None; ids.len()]),
            Err(PrefetchError::Store(e)) => return Err(e),
        };
        let mut buf = Vec::new();
        ids.iter()
            .map(|id| {
                Ok(match bases.find_base(id, &mut buf)? {
                    Some(k) if k == kind => Some(buf.clone()),
                    _ => None,
                })
            })
            .collect()
    }
}

impl<M, B> aspen_pack::changed::OldTrees for DepTrees<'_, M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    async fn trees(&mut self, ids: &[ObjectId]) -> Result<Vec<Option<Arc<[u8]>>>, StoreError> {
        let cache = &self.svc.trees;
        let mut out: Vec<Option<Arc<[u8]>>> =
            ids.iter().map(|id| cache.tree(self.repo, id)).collect();
        let misses: Vec<ObjectId> = ids
            .iter()
            .zip(&out)
            .filter(|(_, o)| o.is_none())
            .map(|(id, _)| *id)
            .collect();
        if misses.is_empty() {
            return Ok(out);
        }
        let read = self.read(&misses, ObjectKind::Tree).await?;
        let read: HashMap<ObjectId, Arc<[u8]>> = misses
            .into_iter()
            .zip(read)
            .filter_map(|(id, d)| Some((id, Arc::<[u8]>::from(d?))))
            .collect();
        for (id, o) in ids.iter().zip(out.iter_mut()) {
            if o.is_none()
                && let Some(d) = read.get(id)
            {
                cache.insert_tree(self.repo, *id, d.clone());
                *o = Some(d.clone());
            }
        }
        Ok(out)
    }

    async fn commit_trees(
        &mut self,
        ids: &[ObjectId],
    ) -> Result<Vec<Option<ObjectId>>, StoreError> {
        let cache = &self.svc.trees;
        let mut out: Vec<Option<ObjectId>> =
            ids.iter().map(|id| cache.root(self.repo, id)).collect();
        let misses: Vec<ObjectId> = ids
            .iter()
            .zip(&out)
            .filter(|(_, o)| o.is_none())
            .map(|(id, _)| *id)
            .collect();
        if misses.is_empty() {
            return Ok(out);
        }
        let read = self.read(&misses, ObjectKind::Commit).await?;
        let read: HashMap<ObjectId, ObjectId> = misses
            .into_iter()
            .zip(read)
            .filter_map(|(id, d)| Some((id, aspen_pack::scan::commit_tree(&d?).ok()?)))
            .collect();
        for (id, o) in ids.iter().zip(out.iter_mut()) {
            if o.is_none()
                && let Some(t) = read.get(id)
            {
                cache.insert_root(self.repo, *id, *t);
                *o = Some(*t);
            }
        }
        Ok(out)
    }
}

fn set(report: &mut ReportStatus, name: &RefName, status: RefStatus) {
    report
        .set(name, status)
        .expect("every valid command is in the report");
}

/// The branch whose current version a push through the plain repository
/// URL also depends on (issue #203): the one ref it updates, when that is a
/// branch other than trunk and the update is not a delete.
///
/// The plain URL advertises only trunk (S2), so the client builds its pack
/// against trunk and resends every object of the branch it does not share
/// with trunk. Searching only trunk's closure, as before, stored all of them
/// again on every push, so a branch's storage grew with the square of its
/// length. With the branch's current version in the search, its objects
/// count as held and are not stored, and the write records that version as
/// its `scoped_old` dependency, as a push through the branch's scoped URL
/// does, so the new version stays connected.
///
/// The dependency is the version searched, not the parent: after a lost
/// compare-and-swap the verdict is recomputed against the newer current
/// version but the plan, and so the deps, stay. The new version then does
/// not depend on its parent, and the compactor records no cover chain
/// across it (`aspen_core::meta::VerifiedRange::verify` and `join` chain
/// only a version whose parent is among its deps). If a forced update came
/// in between, the searched version's tip is not an ancestor of the new
/// tip and the new version is `Wider`: more bytes on later incremental
/// fetches of that branch, never a missing object (the pack drops only
/// objects the searched version holds; a delta whose base it dropped is
/// stored whole, `aspen_pack::subset::write_subset`).
///
/// A write that depends on a `Wider` version is `Wider` itself, so once a
/// branch version is `Wider`, later plain-URL fast-forwards of it stay
/// `Wider` until a forced update (which depends on trunk only) or a push
/// through the branch's scoped URL that does not depend on it. This adds
/// little: a plain-URL push of a branch whose tip does not contain trunk's
/// current tip is `Wider` already through its trunk dependency, before and
/// after issue #203; the inheritance only matters for a branch kept
/// up to date with trunk. Using only exact versions here was tried and
/// undoes the fix: nearly every branch version is `Wider` through trunk,
/// so nearly every push would store the branch's whole history again
/// (9.23x instead of 3.56x in `tests/storage.rs`). Wider versions are
/// counted (`aspen_push_branch_versions_total{closure="wider"}`).
///
/// Not for a forced or leased update (`-o force`, `-o lease=`): it may
/// rewrite the branch, and a dependency whose tip is not an ancestor of the
/// new tip would make the new version `Wider`, so such an update keeps
/// storing everything it brings, exact, as before. Any other plain-URL
/// update that is accepted is a fast-forward of the version its verdict
/// was computed against, normally the one searched here.
///
/// Mirror imports (`Origin::Upstream`) are not given it: they apply the
/// upstream's value, which may be a forced update without `-o force`.
///
/// A push of several refs keeps trunk as its only search root: a write
/// depends on at most one version besides trunk, and everything the search
/// holds must be in each write's dependencies.
fn plain_url_branch_dep<'c>(
    commands: &'c [RefCommand],
    trunk: &RefName,
    options: &PushOptions,
) -> Option<&'c RefName> {
    match commands {
        [c] if c.name != *trunk
            && c.name.is_branch()
            && !c.new.is_zero()
            && !options.force_requested(&c.name)
            && options.lease(&c.name).is_none() =>
        {
            Some(&c.name)
        }
        _ => None,
    }
}

fn is_delete(kind: &VerdictKind) -> bool {
    matches!(
        kind,
        VerdictKind::DeleteCas | VerdictKind::DeleteUnconditional
    )
}

/// [`HookObjects`] over one push, for one update: `extra` is the updated
/// ref's stored version, whose closure is read last.
struct PushObjects<'p, 'a, M, B> {
    push: &'p mut Push<'a, M, B>,
    extra: Vec<aspen_core::id::RefVersionId>,
}

impl<M, B> HookObjects for PushObjects<'_, '_, M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    fn range<'a>(
        &'a mut self,
        new: ObjectId,
        exclude: &'a [ObjectId],
        max_commits: usize,
    ) -> HookFuture<'a, Result<CommitRange, StoreError>> {
        Box::pin(async move {
            let extra = self.extra.clone();
            self.push
                .hook_range(new, exclude, max_commits, &extra)
                .await
        })
    }

    fn read(
        &mut self,
        id: ObjectId,
    ) -> HookFuture<'_, Result<Option<super::hook::Object>, StoreError>> {
        Box::pin(async move {
            let extra = self.extra.clone();
            self.push.hook_read(id, &extra).await
        })
    }
}

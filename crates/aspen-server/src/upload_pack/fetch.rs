//! `fetch` (fetch path steps 2 to 5).
//!
//! One POST is one negotiation round:
//!
//! 1. Refuse what Aspen never serves (`filter`; shallow when the
//!    repository refuses it) and pick the plan mode: a shallow request
//!    (`shallow` or `deepen*` lines) plans the full closure of the wants and
//!    ignores the haves (A5 review).
//! 2. Resolve wants and haves (`super::resolve`).
//! 3. Without `done`, answer with `acknowledgments`: `ACK` for each have
//!    that stands for a version, and `ready` once one does (the plan cannot
//!    get smaller from more haves of a branch the client already shares).
//!    With no usable have yet, the response ends there and the client sends
//!    its next round. A shallow request is `ready` at once: its haves are
//!    never used.
//! 4. Plan with the cover planner, loading versions as it walks
//!    (`super::graph`); record `PlanStats` in [`crate::Metrics`]. A
//!    shallow request whose wants have exact cached shallow packs is served
//!    from those instead, and is not planned (`super::exact`, node C8).
//! 5. For a shallow request, compute `shallow-info` (`super::shallow`).
//! 6. Send packfile URIs for the packs that may go as URIs, and stitch the
//!    rest into the inline pack (`super::deliver`).
//!
//! `include-tag` is accepted and ignored in v1: tags are sent when wanted
//! (a clone wants them), and Git backfills annotated tags it sees in
//! `ls-refs` with a second fetch. `no-progress` is honoured; `thin-pack`
//! changes nothing, because stored packs are self-contained.

use aspen_core::blob::BlobStore;
use aspen_core::fetch::{FetchRequest, HaveVersion, PlanInput, PlanMode, ShallowInfo};
use aspen_core::id::ObjectId;
use aspen_core::meta::MetaStore;
use aspen_core::pack::PackRecord;
use aspen_core::repo::Repo;
use aspen_proto::v2::{Acknowledgments, FetchArgs, FetchResponse};
use axum::body::Body;

use super::Failure;
use super::deliver::{self, Split};
use super::exact;
use super::graph::{self, LoadedGraph};
use super::resolve;
use super::shallow;
use crate::Server;
use crate::access::Authorized;
use crate::admission::Slot;
use crate::metrics::Delivery;

/// What a round produced.
enum Round {
    /// Acknowledgments without `ready`: the client sends another round.
    Acks(Acknowledgments),
    /// Everything up to the pack, ready to stream.
    Pack {
        head: Vec<u8>,
        response: FetchResponse,
        plan: aspen_pack::stitch::StitchPlan,
        stitched: Vec<PackRecord>,
        delivery: Delivery,
        slot: Option<Slot>,
    },
}

/// The whole response to one `fetch` command by `caller`.
pub(crate) async fn respond<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    caller: &Authorized,
    args: FetchArgs,
) -> Body
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let mut response = FetchResponse::new(&args);
    let mut out = Vec::new();
    match round(server, repo, caller, &args, response).await {
        Ok(Round::Pack {
            head,
            response,
            plan,
            stitched,
            delivery,
            slot,
        }) => {
            return deliver::stream_body(server, response, head, plan, stitched, delivery, slot);
        }
        Ok(Round::Acks(acks)) => {
            response = FetchResponse::new(&args);
            response
                .acknowledgments(&mut out, &acks)
                .expect("first section of a round without done");
        }
        Err(failure) => {
            deliver::report_early(server, &failure).await;
            response = FetchResponse::new(&args);
            response
                .error(&mut out, &failure.to_wire())
                .expect("a fetch error before any section");
        }
    }
    Body::from(out)
}

async fn round<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    caller: &Authorized,
    args: &FetchArgs,
    mut response: FetchResponse,
) -> Result<Round, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let request = &args.request;
    let mode = request.plan_mode(repo.settings.shallow())?;
    if args.wait_for_done && request.wants.is_empty() && !request.done {
        return Ok(Round::Acks(
            negotiation_acks(server.meta(), repo, &request.haves).await?,
        ));
    }
    if request.wants.is_empty() {
        return Err(Failure::refused(
            "protocol error: a fetch must have at least one want",
        ));
    }
    let meta = server.meta();
    let wants = resolve::wants(meta, repo.id, &request.wants, server.config().max_tag_scan).await?;
    let (haves, graph, dropped) = resolve_haves(meta, repo, mode, &request.haves).await?;
    let mut head = Vec::new();
    if !request.done {
        let mut common: Vec<_> = haves.iter().map(|(id, _)| *id).collect();
        common.sort_unstable();
        let ready = !args.wait_for_done && (mode == PlanMode::FullClosure || !common.is_empty());
        let acks = Acknowledgments { common, ready };
        if !ready {
            return Ok(Round::Acks(acks));
        }
        response
            .acknowledgments(&mut head, &acks)
            .expect("first section of a round without done");
    }
    let (rows, leave_out) = if request.is_shallow() {
        let (rows, info) = shallow_plan(server, repo, request, &wants).await?;
        response
            .shallow_info(&mut head, &info)
            .expect("shallow-info follows acknowledgments");
        (rows.rows, rows.leave_out)
    } else {
        let rows = plan_rows(server, repo, mode, &wants, haves, graph, dropped).await?;
        (rows, Vec::new())
    };
    // A pack sent as a URI goes whole, so a plan that leaves objects out
    // is stitched.
    let Split { uris, stitched } = if leave_out.is_empty() {
        deliver::split(server, request, rows).await
    } else {
        Split {
            uris: Vec::new(),
            stitched: rows,
        }
    };
    let slot = deliver::stitch_slot(caller, &stitched).await?;
    let plan = deliver::stitch_plan(server.blobs(), &stitched, &leave_out).await?;
    response
        .packfile_uris(&mut head, &uris)
        .expect("URIs are only made in schemes the client listed");
    response
        .start_packfile(&mut head)
        .expect("packfile follows the earlier sections");
    Ok(Round::Pack {
        head,
        response,
        plan,
        delivery: Delivery {
            by_uri: uris.len() as u64,
            stitched: stitched.len() as u64,
        },
        stitched,
        slot,
    })
}

/// The answer to a negotiation-only round (`wait-for-done`, haves and no
/// wants): `ACK` for every have with a `commit_index` row, whatever version
/// the row names, and never `ready`.
///
/// This is what `git push` runs first when `push.negotiate` is set: it
/// leaves every acknowledged commit, and everything that commit reaches,
/// out of the pack it sends. Without it a push through the plain repository
/// URL, which advertises only trunk (S2), resends the pushed branch's whole
/// history since trunk every time.
///
/// An acknowledged commit need not be in the dependencies the push will
/// search: the client does not say which ref it will push, so a commit of
/// another branch (a branch created from it, a merge of it into trunk) can
/// be acknowledged. Receive-pack finds such a pack's boundary commits
/// through the same `commit_index` rows and copies what the write needs
/// from their versions (`crate::receive`, "boundary outside the
/// dependencies"). A row is never removed, and every version a row names
/// stays connected and stored, so an acknowledged commit can always be
/// found again at push time. A commit with no row yet (a non-tip commit of
/// a push the indexing job has not reached) is not acknowledged; Git then
/// sends its parents as haves in the next round, at worst resending it.
pub(crate) async fn negotiation_acks<M: MetaStore>(
    meta: &M,
    repo: &Repo,
    haves: &[ObjectId],
) -> Result<Acknowledgments, Failure> {
    let sorted = aspen_core::ingest::SortedIds::from_unsorted(haves.to_vec());
    let rows = meta
        .lookup_commits(repo.id, &sorted)
        .await
        .map_err(aspen_core::error::FetchError::Store)?;
    let common = sorted
        .as_slice()
        .iter()
        .zip(rows)
        .filter(|(_, row)| matches!(row, aspen_core::meta::CommitLookup::Indexed(_)))
        .map(|(id, _)| *id)
        .collect();
    Ok(Acknowledgments {
        common,
        ready: false,
    })
}

/// The haves that stand for a version (none for a shallow request), a
/// graph holding the versions read to decide, and how many were dropped.
pub(crate) async fn resolve_haves<M: MetaStore>(
    meta: &M,
    repo: &Repo,
    mode: PlanMode,
    ids: &[ObjectId],
) -> Result<(Vec<(ObjectId, HaveVersion)>, LoadedGraph, usize), Failure> {
    let mut graph = LoadedGraph::default();
    let (haves, have_versions, dropped) = match mode {
        PlanMode::Incremental => resolve::haves(meta, repo.id, ids).await?,
        PlanMode::FullClosure => (Vec::new(), Vec::new(), 0),
    };
    for v in have_versions {
        graph.insert(v);
    }
    Ok((haves, graph, dropped))
}

/// Plans the wants against the haves (fetch path step 2) and returns the
/// planned packs' rows in plan order; records `PlanStats`.
pub(crate) async fn plan_rows<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    mode: PlanMode,
    wants: &[resolve::Want],
    haves: Vec<(ObjectId, HaveVersion)>,
    graph: LoadedGraph,
    dropped: usize,
) -> Result<Vec<PackRecord>, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let haves_used = haves.len();
    let started = std::time::Instant::now();
    let (rows, stats, loaded, heat) = plan_counted(server, repo, mode, wants, haves, graph).await?;
    let took = started.elapsed();
    server
        .metrics()
        .plan(&stats, loaded, haves_used, dropped, took);
    server.note_compaction_heat(heat);
    tracing::debug!(
        visited = stats.visited,
        chain_skips = stats.chain_skips,
        loaded_versions = loaded,
        needed = stats.needed,
        packs = stats.packs,
        haves_used,
        haves_dropped = dropped,
        plan_ms = took.as_millis() as u64,
        "fetch planned"
    );
    Ok(rows)
}

/// [`plan_rows`] without recording anything: for plans that serve no fetch
/// themselves (the sources of an exact shallow pack, `super::exact`).
pub(crate) async fn plan_quietly<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    mode: PlanMode,
    wants: &[resolve::Want],
    haves: Vec<(ObjectId, HaveVersion)>,
    graph: LoadedGraph,
) -> Result<(Vec<PackRecord>, aspen_planner::PlanStats), Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let (rows, stats, _, _) = plan_counted(server, repo, mode, wants, haves, graph).await?;
    Ok((rows, stats))
}

/// Plans like [`plan_quietly`] and also returns how many ref versions the
/// planner's graph held when the plan was made (every one was one metadata
/// read: the wants', the haves' and those loaded on demand) and the plan's
/// compaction heat ([`super::heat::heat_of`]).
async fn plan_counted<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    mode: PlanMode,
    wants: &[resolve::Want],
    haves: Vec<(ObjectId, HaveVersion)>,
    mut graph: LoadedGraph,
) -> Result<
    (
        Vec<PackRecord>,
        aspen_planner::PlanStats,
        usize,
        Vec<aspen_core::meta::CompactionHeat>,
    ),
    Failure,
>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let want_versions: Vec<_> = wants.iter().map(|w| w.version).collect();
    let input = PlanInput::new(mode, want_versions, haves.iter().map(|(_, h)| *h).collect());
    let (plan, stats) = graph::plan(
        server.meta(),
        repo.id,
        &mut graph,
        &input,
        server.config().max_plan_versions,
    )
    .await?;
    let rows = plan
        .packs
        .iter()
        .map(|id| {
            graph
                .pack_row(*id)
                .cloned()
                .expect("the planner only returns packs the graph holds")
        })
        .collect::<Vec<_>>();
    let heat = super::heat::heat_of(repo.id, &graph, &rows);
    Ok((rows, stats, graph.version_count(), heat))
}

/// The packs of a shallow answer, and objects every copy of which is left
/// out of the stitched pack (a layered answer's earlier commits; empty
/// otherwise).
#[derive(Debug, Default)]
pub(crate) struct ShallowRows {
    pub(crate) rows: Vec<PackRecord>,
    pub(crate) leave_out: Vec<ObjectId>,
}

/// The packs and the `shallow-info` of a shallow request (fetch path step
/// 5): the exact cached shallow packs (or a layered answer) when the
/// request can use them (`super::exact`, node C8), otherwise the full
/// closure of the wants (the over-fetch fallback, S5). `shallow-info` is
/// computed the same way in every case.
pub(crate) async fn shallow_plan<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    request: &FetchRequest,
    wants: &[resolve::Want],
) -> Result<(ShallowRows, ShallowInfo), Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let rows = match exact::packs(server, repo, request, wants).await? {
        Some(rows) => rows,
        None => ShallowRows {
            leave_out: Vec::new(),
            rows: {
                let (haves, graph, dropped) =
                    resolve_haves(server.meta(), repo, PlanMode::FullClosure, &[]).await?;
                plan_rows(
                    server,
                    repo,
                    PlanMode::FullClosure,
                    wants,
                    haves,
                    graph,
                    dropped,
                )
                .await?
            },
        },
    };
    let info = shallow_info(server, repo, request, wants, &rows.rows).await?;
    Ok((rows, info))
}

/// `shallow-info` for a shallow request whose planned packs are `rows`
/// (fetch path step 5).
pub(crate) async fn shallow_info<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    request: &FetchRequest,
    wants: &[resolve::Want],
    rows: &[PackRecord],
) -> Result<ShallowInfo, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let mut objects = shallow::Objects::new(
        server.meta(),
        server.objects(),
        repo.id,
        rows.iter().map(|r| r.id).collect(),
        server.config().max_shallow_commits,
    );
    let want_ids: Vec<_> = wants.iter().map(|w| w.id).collect();
    shallow::compute(&mut objects, repo, request, &want_ids).await
}

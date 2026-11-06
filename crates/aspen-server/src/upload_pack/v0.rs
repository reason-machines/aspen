//! `git-upload-pack` in protocol v0 and v1, for clients without v2 fetch
//! (libgit2, go-git, `git -c protocol.version=0`). The wire format is
//! [`aspen_proto::upload_v0`] (node A1b); this module holds the policy.
//!
//! - **Advertisement.** v0 lists refs up front, so listing every ref would
//!   undo fetch-path step 1. Aspen shows `HEAD` (a symref to trunk), trunk,
//!   and the ref of a branch-scoped URL, the same narrow set a push sees;
//!   other refs need v2 or a branch-scoped URL (decisions log, A1). A
//!   scoped annotated tag gets its peeled line when its tag object is in
//!   the pack its version stored.
//! - **Header.** What Aspen refuses (`filter`, shallow when the repository
//!   refuses it, wants it cannot resolve) is an `ERR` at the header, before
//!   any acknowledgement. A deepen request gets its shallow section there:
//!   a shallow request plans the full closure of the wants whatever the
//!   haves are (A5 review), so its plan, and so its boundary, are known
//!   before negotiation.
//! - **Negotiation.** Smart HTTP is stateless: each `POST` is one round
//!   ([`Transport::Stateless`]). Over SSH one connection carries every
//!   round ([`Transport::Stateful`], [`serve_stateful`]): each round is
//!   answered as soon as it ends, a deepen request's shallow section is
//!   sent before the first round, and once the pack is due the rest of the
//!   input is left unread (A1b's C3 obligation: with `no-done`, git
//!   pipelines rounds after the one answered `ready`). For an incremental
//!   fetch, a have is
//!   acknowledged as common only when it stands for a version
//!   (`HaveVersion::exact_tip`), as in v2, and the round is `ready` once
//!   one does. A shallow request ignores haves, so any have
//!   `commit_index` knows is acknowledged, which lets the client reach
//!   `ready` (and stop resending its header) as soon as possible.
//! - **Pack.** The acknowledged commons are planned with like v2's haves.
//!   v0 has no `packfile-uris`, so every planned pack is stitched. An
//!   error after the final acknowledgement goes on band 3; a client without
//!   side-band cannot be shown one, so its connection is dropped.

use aspen_core::blob::BlobStore;
use aspen_core::error::FetchError;
use aspen_core::fetch::{FetchRequest, PlanMode, ShallowInfo};
use aspen_core::id::{ObjectId, PackId};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{CommitLookup, MetaStore};
use aspen_core::repo::Repo;
use aspen_proto::pkt;
use aspen_proto::service::V0Version;
use aspen_proto::upload_v0::{
    Answered, Header, Negotiating, PackResponse, RequestParser, Transport, V0Advertisement, V0Head,
    V0Limits, V0Ref, write_advertisement,
};
use axum::body::Body;
use tokio::io::{AsyncRead, AsyncWrite};

use super::deliver::{self, PackWriter};
use super::fetch::{ShallowRows, plan_rows, resolve_haves, shallow_plan};
use super::{Failure, resolve, shallow};
use crate::Server;
use crate::access::{Authorized, GitRoute};
use crate::conn::{self, PktInput};
use crate::metrics::Delivery;

/// Appends the v0 advertisement for `repo` (after the smart-HTTP header).
pub(crate) async fn advertise<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    route: &GitRoute,
    version: V0Version,
    out: &mut Vec<u8>,
) -> Result<(), FetchError>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let meta = server.meta();
    let trunk = repo.settings.trunk();
    let trunk_tip = meta
        .current_version(repo.id, trunk)
        .await
        .map_err(FetchError::Store)?
        .and_then(|v| v.value.tip());
    let mut refs = Vec::new();
    let head = trunk_tip.map(|tip| {
        refs.push(V0Ref {
            name: trunk.clone(),
            tip,
            peeled: None,
        });
        V0Head {
            tip,
            target: trunk.clone(),
        }
    });
    if let Some(scoped) = route.scope.as_ref().filter(|s| *s != trunk)
        && let Some(v) = meta
            .current_version(repo.id, scoped)
            .await
            .map_err(FetchError::Store)?
        && let Some(tip) = v.value.tip()
    {
        let peeled = if scoped.as_str().starts_with("refs/tags/") {
            peel(server, repo, tip, v.value.pack()).await
        } else {
            None
        };
        refs.push(V0Ref {
            name: scoped.clone(),
            tip,
            peeled,
        });
    }
    let advert = V0Advertisement::new(server.config().agent.clone());
    write_advertisement(out, version, &advert, head.as_ref(), &refs)
        .expect("tips are live, refs distinct and HEAD's tip is trunk's");
    Ok(())
}

/// What an annotated tag at `tip` peels to, if its object is in `pack`.
/// `None` for a lightweight tag, or when the tag object is elsewhere (the
/// peeled line only lets a client auto-follow tags; a clone works without).
async fn peel<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    tip: ObjectId,
    pack: Option<PackId>,
) -> Option<ObjectId>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let mut objects = shallow::Objects::new(
        server.meta(),
        server.objects(),
        repo.id,
        pack.into_iter().collect(),
        shallow::PEEL_COMMITS,
    );
    objects.peel(tip).await.ok().flatten().filter(|c| *c != tip)
}

/// What is known about a request before negotiation.
struct Prepared {
    mode: PlanMode,
    wants: Vec<resolve::Want>,
    /// For a shallow request: its planned packs, computed at the header.
    shallow_rows: Option<ShallowRows>,
    shallow: Option<ShallowInfo>,
}

/// The whole response to one v0 `POST git-upload-pack` (`body` is the
/// decoded request).
pub(crate) async fn serve<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    caller: &Authorized,
    body: &[u8],
) -> Body
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let mut out = Vec::new();
    let mut buf = body;
    let err = |message: String| {
        let mut out = Vec::new();
        pkt::write_err(&mut out, &message);
        Body::from(out)
    };
    let limits = V0Limits::from(server.config().v2_limits);
    let mut parser = RequestParser::new(Transport::Stateless, limits);
    let req = loop {
        let (packet, used) = match pkt::decode(buf) {
            Ok(Some(p)) => p,
            Ok(None) => {
                return err("protocol error: the request ended before its first flush".into());
            }
            Err(e) => return err(format!("protocol error: {e}")),
        };
        buf = &buf[used..];
        match parser.packet(packet) {
            Ok(None) => {}
            Ok(Some(Header::Fetch(req))) => break req,
            Ok(Some(Header::End)) => return Body::empty(),
            Err(e) => return err(e.to_string()),
        }
    };
    let prepared = match prepare(server, repo, &req.args().request).await {
        Ok(p) => p,
        Err(failure) => {
            deliver::report_early(server, &failure).await;
            req.error(&mut out, &failure.to_wire())
                .expect("fetch errors are FetchErr");
            return Body::from(out);
        }
    };
    let mut neg = req
        .start(&mut out, prepared.shallow.as_ref())
        .expect("a shallow section exactly when the request deepens");
    let (args, response) = loop {
        let (packet, used) = match pkt::decode(buf) {
            Ok(Some(p)) => p,
            Ok(None) => {
                return match neg.end_of_input() {
                    // A header-only deepen request: the shallow section is
                    // the whole answer.
                    Ok(()) => Body::from(out),
                    Err(e) => {
                        pkt::write_err(&mut out, &e.to_string());
                        Body::from(out)
                    }
                };
            }
            Err(e) => {
                pkt::write_err(&mut out, &format!("protocol error: {e}"));
                return Body::from(out);
            }
        };
        buf = &buf[used..];
        neg = match neg.packet(packet) {
            Ok(Negotiating::More(n)) => n,
            Ok(Negotiating::Round(round)) => {
                let common = match shared(server, repo, prepared.mode, round.haves()).await {
                    Ok(c) => c,
                    Err(failure) => {
                        deliver::report_early(server, &failure).await;
                        round
                            .error(&mut out, &failure.to_wire())
                            .expect("fetch errors are FetchErr");
                        return Body::from(out);
                    }
                };
                let ready = !common.is_empty() || !round.commons().is_empty();
                match round
                    .answer(&mut out, &common, ready)
                    .expect("commons are haves of the round; ready only with one")
                {
                    Answered::Pack { args, response } => break (args, response),
                    Answered::ResponseEnd => return Body::from(out),
                    Answered::More(_) => unreachable!("stateless"),
                }
            }
            Err(e) => {
                pkt::write_err(&mut out, &e.to_string());
                return Body::from(out);
            }
        };
    };
    pack(server, repo, caller, prepared, &args.request, response, out).await
}

/// One v0 or v1 fetch over a connection (SSH), after the advertisement:
/// the header, then every negotiation round on the same stream, each
/// answered as soon as it ends, then the pack. Policy is the stateless
/// path's ([`serve`]): the same refusals at the header, the same haves
/// shared, the same `ready` rule and the same stitched pack.
///
/// Once negotiation ends with the pack ([`Answered::Pack`]) nothing more is
/// read: the rest of the input is handed to a task that discards it, so
/// rounds git pipelined after the one answered `ready` are neither parsed
/// nor allowed to stall the connection, and the pack is written at once.
/// A client that hangs up before the pack gets nothing more; an error is
/// written as `ERR` before the pack and on band 3 inside it, as over HTTP.
pub(crate) async fn serve_stateful<M, B, R, W>(
    server: &Server<M, B>,
    repo: &Repo,
    caller: &Authorized,
    mut input: PktInput<R>,
    out: &mut W,
) -> std::io::Result<()>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    let mut buf = Vec::new();
    let limits = V0Limits::from(server.config().v2_limits);
    let mut parser = RequestParser::new(Transport::Stateful, limits);
    let req = loop {
        // A client that hangs up before its header has nothing to fetch.
        let Some(frame) = input.next().await? else {
            return Ok(());
        };
        match parser.packet(conn::decode(frame)?) {
            Ok(None) => {}
            Ok(Some(Header::Fetch(req))) => break req,
            Ok(Some(Header::End)) => return Ok(()),
            Err(e) => {
                pkt::write_err(&mut buf, &e.to_string());
                return conn::send(out, &buf).await;
            }
        }
    };
    let prepared = match prepare(server, repo, &req.args().request).await {
        Ok(p) => p,
        Err(failure) => {
            deliver::report_early(server, &failure).await;
            req.error(&mut buf, &failure.to_wire())
                .expect("fetch errors are FetchErr");
            return conn::send(out, &buf).await;
        }
    };
    let mut neg = req
        .start(&mut buf, prepared.shallow.as_ref())
        .expect("a shallow section exactly when the request deepens");
    // A deepen client reads its shallow section before it sends haves.
    conn::send(out, &buf).await?;
    buf.clear();
    let (args, response) = loop {
        // A client that hangs up mid-negotiation is gone: nothing to write.
        let Some(frame) = input.next().await? else {
            return Ok(());
        };
        neg = match neg.packet(conn::decode(frame)?) {
            Ok(Negotiating::More(n)) => n,
            Ok(Negotiating::Round(round)) => {
                let common = match shared(server, repo, prepared.mode, round.haves()).await {
                    Ok(c) => c,
                    Err(failure) => {
                        deliver::report_early(server, &failure).await;
                        round
                            .error(&mut buf, &failure.to_wire())
                            .expect("fetch errors are FetchErr");
                        return conn::send(out, &buf).await;
                    }
                };
                let ready = !common.is_empty() || !round.commons().is_empty();
                match round
                    .answer(&mut buf, &common, ready)
                    .expect("commons are haves of the round; ready only with one")
                {
                    Answered::More(n) => {
                        conn::send(out, &buf).await?;
                        buf.clear();
                        n
                    }
                    Answered::Pack { args, response } => break (args, response),
                    Answered::ResponseEnd => unreachable!("stateful"),
                }
            }
            Err(e) => {
                pkt::write_err(&mut buf, &e.to_string());
                return conn::send(out, &buf).await;
            }
        };
    };
    // Whatever the client sends from here on (pipelined rounds) is already
    // answered by `ready`: leave it unread, but keep the stream moving.
    tokio::spawn(input.drain());
    let body = pack(server, repo, caller, prepared, &args.request, response, buf).await;
    conn::send_body(out, body).await
}

/// Refusals, want resolution and, for a shallow request, the plan and its
/// boundary: everything decided at the header.
async fn prepare<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    request: &FetchRequest,
) -> Result<Prepared, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let mode = request.plan_mode(repo.settings.shallow())?;
    let wants = resolve::wants(
        server.meta(),
        repo.id,
        &request.wants,
        server.config().max_tag_scan,
    )
    .await?;
    let (shallow_rows, shallow) = match mode {
        PlanMode::Incremental => (None, None),
        PlanMode::FullClosure => {
            // `shallow` lines without `deepen*` get no shallow section in
            // v0 (A1b: `start` refuses an empty one), unlike v2.
            match request.deepen {
                Some(_) => {
                    let (rows, info) = shallow_plan(server, repo, request, &wants).await?;
                    (Some(rows), Some(info))
                }
                None => {
                    let (haves, graph, dropped) =
                        resolve_haves(server.meta(), repo, mode, &[]).await?;
                    let rows = plan_rows(server, repo, mode, &wants, haves, graph, dropped).await?;
                    (
                        Some(ShallowRows {
                            rows,
                            leave_out: Vec::new(),
                        }),
                        None,
                    )
                }
            }
        }
    };
    Ok(Prepared {
        mode,
        wants,
        shallow_rows,
        shallow,
    })
}

/// The haves of a round the server shares (see the module docs).
async fn shared<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    mode: PlanMode,
    haves: &[ObjectId],
) -> Result<Vec<ObjectId>, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    if haves.is_empty() {
        return Ok(Vec::new());
    }
    match mode {
        PlanMode::Incremental => {
            let (used, _, _) = resolve::haves(server.meta(), repo.id, haves).await?;
            Ok(used.into_iter().map(|(id, _)| id).collect())
        }
        PlanMode::FullClosure => {
            let sorted = SortedIds::from_unsorted(haves.to_vec());
            let rows = server
                .meta()
                .lookup_commits(repo.id, &sorted)
                .await
                .map_err(FetchError::Store)?;
            Ok(sorted
                .as_slice()
                .iter()
                .zip(rows)
                .filter(|(_, r)| matches!(r, CommitLookup::Indexed(_)))
                .map(|(id, _)| *id)
                .collect())
        }
    }
}

/// Plans (unless the header already did), stitches and streams the pack
/// after `head` (the acknowledgements).
async fn pack<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    caller: &Authorized,
    prepared: Prepared,
    request: &FetchRequest,
    mut response: PackResponse,
    mut head: Vec<u8>,
) -> Body
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let planned = async {
        let ShallowRows { rows, leave_out } = match prepared.shallow_rows {
            Some(rows) => rows,
            None => ShallowRows {
                leave_out: Vec::new(),
                rows: {
                    let (haves, graph, dropped) =
                        resolve_haves(server.meta(), repo, prepared.mode, &request.haves).await?;
                    plan_rows(
                        server,
                        repo,
                        prepared.mode,
                        &prepared.wants,
                        haves,
                        graph,
                        dropped,
                    )
                    .await?
                },
            },
        };
        let slot = deliver::stitch_slot(caller, &rows).await?;
        let plan = deliver::stitch_plan(server.blobs(), &rows, &leave_out).await?;
        Ok::<_, Failure>((rows, plan, slot))
    };
    match planned.await {
        Ok((rows, plan, slot)) => {
            let delivery = Delivery {
                by_uri: 0,
                stitched: rows.len() as u64,
            };
            deliver::stream_body(server, response, head, plan, rows, delivery, slot)
        }
        Err(failure) => {
            deliver::report_early(server, &failure).await;
            let wire = failure.to_wire();
            if response.fail(&mut head, &wire) {
                Body::from(head)
            } else {
                deliver::aborted_body(head, &wire.message)
            }
        }
    }
}

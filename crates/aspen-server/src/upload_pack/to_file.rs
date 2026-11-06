//! A pack for a mirror's upstream (node C4): the objects some wants reach
//! that some haves do not, planned and stitched exactly as a fetch is
//! (`super::fetch`), written to a local file instead of a response.
//!
//! The planned versions' haves go through `HaveVersion::exact_tip` as in a
//! fetch, so a have Aspen cannot use is dropped and the pack is larger,
//! never short. The result is a stitched pack: self-contained, each object
//! once, every base first (A2), so the upstream indexes it on its own.
//! Fetch counters are not touched.

use std::path::Path;

use aspen_core::blob::{BlobStore, ByteRange};
use aspen_core::error::{FetchError, StoreError};
use aspen_core::fetch::{PlanInput, PlanMode};
use aspen_core::id::ObjectId;
use aspen_core::meta::MetaStore;
use aspen_core::pack::EntryTable;
use aspen_core::repo::Repo;
use aspen_pack::stitch::Stitcher;
use tokio::io::AsyncWriteExt as _;

use super::{Failure, deliver, fetch, graph, resolve};
use crate::Server;

fn store(f: Failure) -> StoreError {
    match f {
        Failure::Fetch(FetchError::Store(e)) => e,
        other => StoreError::Unavailable {
            reason: other.to_wire().message,
        },
    }
}

/// Writes the pack of `wants` minus `haves` to `out`; returns its size.
pub(crate) async fn write_pack<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    wants: &[ObjectId],
    haves: &[ObjectId],
    out: &Path,
) -> Result<u64, StoreError>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let meta = server.meta();
    let config = server.config();
    let wants = resolve::wants(meta, repo.id, wants, config.max_tag_scan)
        .await
        .map_err(store)?;
    let (haves, mut graph, _) = fetch::resolve_haves(meta, repo, PlanMode::Incremental, haves)
        .await
        .map_err(store)?;
    let input = PlanInput::new(
        PlanMode::Incremental,
        wants.iter().map(|w| w.version).collect(),
        haves.iter().map(|(_, h)| *h).collect(),
    );
    let (plan, _) = graph::plan(meta, repo.id, &mut graph, &input, config.max_plan_versions)
        .await
        .map_err(store)?;
    let rows: Vec<_> = plan
        .packs
        .iter()
        .map(|id| {
            graph
                .pack_row(*id)
                .cloned()
                .expect("the planner only returns packs the graph holds")
        })
        .collect();
    let fetch_err = |e: FetchError| match e {
        FetchError::Store(e) => e,
        other => StoreError::Unavailable {
            reason: other.to_string(),
        },
    };
    let stitch_plan = deliver::stitch_plan(server.blobs(), &rows, &[])
        .await
        .map_err(fetch_err)?;
    let io = |e: std::io::Error| StoreError::LocalIo {
        reason: format!("{}: {e}", out.display()),
    };
    let mut file = tokio::fs::File::create(out).await.map_err(io)?;
    let blobs = server.blobs();
    let chunk = config.stitch_read_bytes.max(1);
    let mut buf = Vec::new();
    let mut size = 0u64;
    let mut stitcher = Stitcher::new(stitch_plan, &mut buf);
    for row in &rows {
        let keys = row.keys();
        let range =
            ByteRange::new(0, row.pack.entries_size).ok_or_else(|| StoreError::Corrupt {
                what: format!("pack {}: empty entry table", row.id),
            })?;
        let table = blobs.read_exact_range(&keys.entries, range).await?;
        let table = EntryTable::decode_for(&table, row)?;
        stitcher.begin_segment(table).map_err(fetch_err)?;
        let mut at = 0;
        while at < row.pack.size {
            let end = (at + chunk).min(row.pack.size);
            let bytes = blobs
                .read_exact_range(&keys.pack, ByteRange::new(at, end).expect("at < end"))
                .await?;
            stitcher.feed(&bytes, &mut buf).map_err(fetch_err)?;
            at = end;
            size += buf.len() as u64;
            file.write_all(&buf).await.map_err(io)?;
            buf.clear();
        }
        stitcher.end_segment().map_err(fetch_err)?;
    }
    stitcher.finish(&mut buf).map_err(fetch_err)?;
    size += buf.len() as u64;
    file.write_all(&buf).await.map_err(io)?;
    file.sync_all().await.map_err(io)?;
    Ok(size)
}

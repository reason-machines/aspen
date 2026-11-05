//! Delivering a plan (fetch path steps 3 and 4): packfile URIs for the
//! clients that opted in, and the stitched pack for everything else.
//!
//! **URIs** (S3). With express routes configured (C2, [`crate::express`]),
//! a pack first tries an express or CDN URI. Otherwise it goes out as a URI
//! only when the client sent `packfile-uris` with the URL's scheme, the
//! store presigned a URL,
//! and the URL is safe for the client: an Aspen-served URL (empty `206`
//! past the end) for every client, a direct-to-bucket URL (`416` past the
//! end) only for `git/2.56` and later. At most
//! [`crate::ServerConfig::max_uris_per_response`] URIs go in one response;
//! the advertised hash is the pack's trailing SHA-1 (`packs.checksum`).
//! Every pack not sent as a URI is stitched into the inline pack, which is
//! therefore empty (header and trailer only) when every pack is a URI.
//! Stored packs are self-contained, so each URI pack and the inline pack
//! index on their own.
//!
//! **Stitching** (S1, A2). The segments' `.idx` files are read and merged
//! before the `packfile` section starts, so a corrupt or missing index is
//! still an `ERR`. Each segment's entry table and pack are then streamed
//! through `aspen_pack::stitch::Stitcher` in fixed-size reads, and its
//! output goes to the client on side-band 1 as it is produced. An error
//! after that point is a band-3 message.
//!
//! Blob reads are round trips (tens of milliseconds on S3), and a plan can
//! name hundreds of segments when compaction lags behind a burst of small
//! pushes, and a large cover is many reads of
//! [`crate::ServerConfig::stitch_read_bytes`] each. Both passes therefore
//! keep up to [`READ_AHEAD`] reads in flight, still consuming them in plan
//! order: the `.idx` files, then every segment's entry table (read with
//! its first range) and pack ranges, across segment boundaries. Read one after another, a plan of 1,600 small packs
//! spent over a minute before its first byte. A stored pack that breaks the
//! base-first invariant also enqueues a `rewrite_pack` job and counts as an
//! alert, because only rewriting the pack fixes it.

use aspen_blob::RECOMMENDED_MAX_PRESIGN_TTL;
use aspen_core::blob::{BlobStore, ByteRange, PastEndRange};
use aspen_core::error::{FetchError, StoreError, ToWire, WireError};
use aspen_core::fetch::{FetchRequest, PackfileUri, allows_direct_bucket_uris};
use aspen_core::id::{ObjectId, UnixMicros};
use aspen_core::meta::{JobKind, MetaStore, NewJob};
use aspen_core::pack::{EntryTable, PackRecord};
use aspen_pack::idx::IdxV2;
use aspen_pack::stitch::{SegmentIds, StitchPlan, Stitcher};
use aspen_proto::upload_v0::PackResponse;
use aspen_proto::v2::FetchResponse;
use axum::body::Body;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use tokio::sync::mpsc;

use super::Failure;
use crate::Server;
use crate::access::Authorized;
use crate::admission::Slot;
use crate::metrics::Delivery;

/// Frames buffered between the stitching task and the connection.
const CHANNEL_FRAMES: usize = 8;

/// Blob reads a stitch keeps in flight at once: with the default
/// [`crate::ServerConfig::stitch_read_bytes`], at most 4 MiB per stitch.
pub(crate) const READ_AHEAD: usize = 16;

/// The planned packs, split into URIs and packs to stitch, in plan order.
#[derive(Debug, Default)]
pub(crate) struct Split {
    pub(crate) uris: Vec<PackfileUri>,
    pub(crate) stitched: Vec<PackRecord>,
}

/// Decides, pack by pack, whether it goes out as a URI.
pub(crate) async fn split<M, B>(
    server: &Server<M, B>,
    request: &FetchRequest,
    packs: Vec<PackRecord>,
) -> Split
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let config = server.config();
    let mut out = Split::default();
    if !config.packfile_uris || request.packfile_uri_protocols.is_empty() {
        out.stitched = packs;
        return out;
    }
    let ttl = config.presign_ttl.min(RECOMMENDED_MAX_PRESIGN_TTL);
    let direct_ok = allows_direct_bucket_uris(request.agent.as_deref());
    let mut presign_works = true;
    for p in packs {
        if out.uris.len() >= config.max_uris_per_response {
            out.stitched.push(p);
            continue;
        }
        if let Some(uri) = crate::express::uri(server, &p, request, direct_ok) {
            out.uris.push(uri);
            continue;
        }
        if !presign_works {
            out.stitched.push(p);
            continue;
        }
        let url = match server.blobs().presign_get(&p.keys().pack, ttl).await {
            Ok(url) => url,
            Err(_) => {
                // The store does not presign (no signer, or a transient
                // failure): stitch this pack and every later one.
                presign_works = false;
                out.stitched.push(p);
                continue;
            }
        };
        let scheme_ok = url
            .url
            .split_once("://")
            .is_some_and(|(s, _)| request.packfile_uri_protocols.iter().any(|p| p == s));
        let past_end_ok = match url.past_end {
            PastEndRange::Empty206 => true,
            PastEndRange::Status416 => direct_ok,
        };
        if scheme_ok && past_end_ok {
            out.uris.push(PackfileUri {
                hash: p.pack.checksum,
                uri: url.url,
            });
        } else {
            out.stitched.push(p);
        }
    }
    out
}

/// Reads every segment's `.idx` and merges them: the stitched pack's object
/// count and the repeated objects.
pub(crate) async fn stitch_plan<B: BlobStore>(
    blobs: &B,
    rows: &[PackRecord],
) -> Result<StitchPlan, FetchError> {
    // Futures collected first (they start only when polled): a stream
    // mapping `&PackRecord` with a closure is not `Send` for every lifetime.
    let reads: Vec<_> = rows.iter().map(|row| segment_ids(blobs, row)).collect();
    let ids: Vec<Vec<(ObjectId, u64)>> = stream::iter(reads)
        .buffered(READ_AHEAD)
        .try_collect()
        .await?;
    StitchPlan::new(
        rows.iter()
            .zip(ids)
            .map(|(row, ids)| SegmentIds {
                pack: row.id,
                checksum: row.pack.checksum,
                ids: ids.into_iter(),
            })
            .collect(),
    )
    .map_err(FetchError::Store)
}

/// One segment's object IDs and offsets, from its `.idx`.
async fn segment_ids<B: BlobStore>(
    blobs: &B,
    row: &PackRecord,
) -> Result<Vec<(ObjectId, u64)>, FetchError> {
    let range = ByteRange::new(0, row.pack.idx_size).ok_or_else(|| corrupt(row, "empty .idx"))?;
    let bytes = blobs
        .read_exact_range(&row.keys().idx, range)
        .await
        .map_err(FetchError::Store)?;
    let idx = IdxV2::parse(&bytes, Some(row.pack.size))
        .map_err(|e| corrupt(row, &format!(".idx: {e}")))?;
    if idx.pack_checksum() != row.pack.checksum || idx.len() != row.pack.object_count as usize {
        return Err(corrupt(
            row,
            ".idx does not describe the pack its row names",
        ));
    }
    Ok(idx.ids_with_offsets().collect())
}

/// One read of a stitch: a range of a segment's pack, with the segment's
/// entry table when it is the first range.
struct Piece {
    table: Option<EntryTable>,
    bytes: Vec<u8>,
    last: bool,
}

/// Reads `row`'s pack from `at` to `end`, and its entry table too when
/// `at` is 0.
async fn read_piece<B: BlobStore>(
    blobs: &B,
    row: &PackRecord,
    at: u64,
    end: u64,
) -> Result<Piece, FetchError> {
    let keys = row.keys();
    let range = ByteRange::new(at, end).ok_or_else(|| corrupt(row, "empty pack"))?;
    let bytes = blobs.read_exact_range(&keys.pack, range);
    let last = end == row.pack.size;
    if at > 0 {
        let bytes = bytes.await.map_err(FetchError::Store)?;
        return Ok(Piece {
            table: None,
            bytes,
            last,
        });
    }
    let table_range = ByteRange::new(0, row.pack.entries_size)
        .ok_or_else(|| corrupt(row, "empty entry table"))?;
    let (table, bytes) =
        futures_util::future::try_join(blobs.read_exact_range(&keys.entries, table_range), bytes)
            .await
            .map_err(FetchError::Store)?;
    let table = EntryTable::decode_for(&table, row).map_err(FetchError::Store)?;
    Ok(Piece {
        table: Some(table),
        bytes,
        last,
    })
}

fn corrupt(row: &PackRecord, what: &str) -> FetchError {
    FetchError::Store(StoreError::Corrupt {
        what: format!("pack {} ({}): {what}", row.id, row.pack.checksum),
    })
}

/// The pack part of a fetch response: v2's `packfile` section
/// ([`FetchResponse`]) or v0's pack ([`PackResponse`]). Each call is valid
/// once the pack has started; a misuse is a bug in this module.
pub(crate) trait PackWriter: Send + 'static {
    /// Appends pack bytes.
    fn data(&mut self, out: &mut Vec<u8>, bytes: &[u8]);
    /// Appends a progress line (dropped if the client asked for none).
    fn progress(&mut self, out: &mut Vec<u8>, message: &str);
    /// Ends the response.
    fn finish(&mut self, out: &mut Vec<u8>);
    /// Ends the response with an error; `false` when the client cannot be
    /// shown one (v0 without side-band), so the connection must be dropped.
    fn fail(&mut self, out: &mut Vec<u8>, e: &WireError) -> bool;
}

impl PackWriter for FetchResponse {
    fn data(&mut self, out: &mut Vec<u8>, bytes: &[u8]) {
        self.pack_data(out, bytes)
            .expect("inside the packfile section");
    }
    fn progress(&mut self, out: &mut Vec<u8>, message: &str) {
        FetchResponse::progress(self, out, message).expect("inside the packfile section");
    }
    fn finish(&mut self, out: &mut Vec<u8>) {
        FetchResponse::finish(self, out).expect("inside the packfile section");
    }
    fn fail(&mut self, out: &mut Vec<u8>, e: &WireError) -> bool {
        self.error(out, e)
            .expect("a fetch error inside the packfile section");
        true
    }
}

impl PackWriter for PackResponse {
    fn data(&mut self, out: &mut Vec<u8>, bytes: &[u8]) {
        self.pack_data(out, bytes).expect("before the pack ended");
    }
    fn progress(&mut self, out: &mut Vec<u8>, message: &str) {
        PackResponse::progress(self, out, message).expect("before the pack ended");
    }
    fn finish(&mut self, out: &mut Vec<u8>) {
        PackResponse::finish(self, out).expect("before the pack ended");
    }
    fn fail(&mut self, out: &mut Vec<u8>, e: &WireError) -> bool {
        self.error(out, e)
            .expect("a fetch error before the pack ended")
    }
}

/// The stitch slot for an inline pack of `rows`, taken before its stitch
/// plan holds every segment's object IDs ([`crate::admission`]): `None`
/// when there is nothing to stitch. A caller or instance at its limit is
/// a retryable `ERR`.
pub(crate) async fn stitch_slot(
    caller: &Authorized,
    rows: &[PackRecord],
) -> Result<Option<Slot>, Failure> {
    if rows.is_empty() {
        return Ok(None);
    }
    caller
        .stitch()
        .await
        .map(Some)
        .map_err(|busy| Failure::Refused {
            message: busy.to_string(),
            retryable: true,
        })
}

/// What the response stream carries: bytes, or an error that aborts the
/// connection (for a client that cannot be shown one).
type Chunk = Result<Bytes, std::io::Error>;

/// A body that sends `head` and then aborts the connection.
pub(crate) fn aborted_body(head: Vec<u8>, why: &str) -> Body {
    let items: Vec<Chunk> = vec![
        Ok(Bytes::from(head)),
        Err(std::io::Error::other(why.to_owned())),
    ];
    Body::from_stream(stream::iter(items))
}

/// The response body: `head` (every section up to and including the
/// `packfile` line in v2; the acknowledgements in v0), then the stitched
/// pack, then the end of the response, or an error if stitching fails.
/// The fetch counts as delivered only once the last frame is handed to the
/// connection; a client that hung up before that is not counted. `slot`
/// (the stitch slot) is held until the stream ends, however it ends: the
/// task stops as soon as the connection drops the body.
pub(crate) fn stream_body<M, B, W>(
    server: &Server<M, B>,
    response: W,
    head: Vec<u8>,
    plan: StitchPlan,
    rows: Vec<PackRecord>,
    delivery: Delivery,
    slot: Option<Slot>,
) -> Body
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    W: PackWriter,
{
    let (tx, rx) = mpsc::channel::<Chunk>(CHANNEL_FRAMES);
    let server = server.clone();
    tokio::spawn(async move {
        let _slot = slot;
        let mut response = response;
        if tx.send(Ok(Bytes::from(head))).await.is_err() {
            return;
        }
        let mut frame = Vec::new();
        match stitch(&server, &mut response, plan, &rows, &tx).await {
            Ok(size) => {
                response.finish(&mut frame);
                if tx.send(Ok(Bytes::from(frame))).await.is_ok() {
                    server.metrics().delivered(delivery);
                    server.metrics().stitched_bytes(size);
                }
            }
            Err(e) => {
                report(&server, &e).await;
                let item = if response.fail(&mut frame, &e.to_wire()) {
                    Ok(Bytes::from(frame))
                } else {
                    Err(std::io::Error::other(e.to_string()))
                };
                let _ = tx.send(item).await;
            }
        }
    });
    Body::from_stream(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|b| (b, rx))
    }))
}

async fn stitch<M, B, W>(
    server: &Server<M, B>,
    response: &mut W,
    plan: StitchPlan,
    rows: &[PackRecord],
    tx: &mpsc::Sender<Chunk>,
) -> Result<u64, FetchError>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    W: PackWriter,
{
    let blobs = server.blobs();
    let chunk = server.config().stitch_read_bytes.max(1);
    let mut out = Vec::new();
    let mut sent = 0u64;
    let mut stitcher = Stitcher::new(plan, &mut out);
    let send = |out: &mut Vec<u8>, response: &mut W| {
        let mut frame = Vec::with_capacity(out.len() + 16);
        response.data(&mut frame, out);
        out.clear();
        Ok(Bytes::from(frame))
    };
    if !rows.is_empty() {
        let objects = stitcher_objects(rows);
        let mut frame = Vec::new();
        response.progress(
            &mut frame,
            &format!(
                "Aspen: stitching {} stored packs ({objects} objects)",
                rows.len()
            ),
        );
        if tx.send(Ok(Bytes::from(frame))).await.is_err() {
            return Err(gone());
        }
    }
    // Every read of the stitch in plan order, started up to READ_AHEAD
    // ahead (futures run only when polled).
    let reads: Vec<_> = rows
        .iter()
        .flat_map(|row| {
            let size = row.pack.size;
            (0..size.div_ceil(chunk)).map(move |i| (row, i * chunk, ((i + 1) * chunk).min(size)))
        })
        .map(|(row, at, end)| read_piece(blobs, row, at, end))
        .collect();
    let mut pieces = stream::iter(reads).buffered(READ_AHEAD);
    while let Some(piece) = pieces.next().await {
        let piece = piece?;
        if let Some(table) = piece.table {
            stitcher.begin_segment(table)?;
        }
        stitcher.feed(&piece.bytes, &mut out)?;
        if piece.last {
            stitcher.end_segment()?;
        }
        if !out.is_empty() {
            sent += out.len() as u64;
            if tx.send(send(&mut out, response)).await.is_err() {
                return Err(gone());
            }
        }
    }
    stitcher.finish(&mut out)?;
    sent += out.len() as u64;
    if tx.send(send(&mut out, response)).await.is_err() {
        return Err(gone());
    }
    Ok(sent)
}

fn stitcher_objects(rows: &[PackRecord]) -> u64 {
    rows.iter().map(|r| u64::from(r.pack.object_count)).sum()
}

/// A client that hung up mid-stream: nothing more can reach it, and it is
/// not a server failure. Reported as a transient store error so the error
/// path stays uniform; [`report`] recognises it and counts nothing.
fn gone() -> FetchError {
    FetchError::Store(StoreError::Unavailable {
        reason: GONE.to_owned(),
    })
}

const GONE: &str = "client disconnected";

/// Counts an error, and for a forward base in a stored pack, enqueues the
/// rewrite (idempotent per pack, so a page that repeats on every fetch
/// leaves one job).
async fn report<M, B>(server: &Server<M, B>, e: &FetchError)
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    if matches!(e, FetchError::Store(StoreError::Unavailable { reason }) if reason == GONE) {
        return;
    }
    server.metrics().fetch_error();
    match e {
        FetchError::StoredPackNotBaseFirst { pack, .. } => {
            server.metrics().forward_base_alert();
            tracing::error!(
                pack = %pack,
                error = %e,
                "a stored pack is not base-first; the fetch failed and a rewrite_pack job is enqueued"
            );
            if let Ok(rows) = server.meta().packs(&[*pack]).await
                && let Some(row) = rows.first()
            {
                let enqueued = server
                    .meta()
                    .enqueue_jobs(&[NewJob {
                        repo: row.pack.repo,
                        kind: JobKind::RewritePack { pack: *pack },
                        run_after: UnixMicros::now(),
                    }])
                    .await;
                if let Err(e) = enqueued {
                    // The alert already pages; this says the repair is not
                    // queued yet (the next fetch that hits the pack tries
                    // again).
                    server.metrics().job_enqueue_failure();
                    tracing::error!(pack = %pack, error = %e, "cannot enqueue rewrite_pack");
                }
            }
        }
        FetchError::Store(s) if s.is_corruption() => {
            server.metrics().corruption_alert();
            tracing::error!(error = %s, "stored data does not match its row");
        }
        _ => tracing::info!(error = %e, "fetch failed"),
    }
}

/// Counts a failure that ended the response before the pack.
pub(crate) async fn report_early<M, B>(server: &Server<M, B>, failure: &Failure)
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    match failure {
        Failure::Fetch(e) => report(server, e).await,
        Failure::Refused { .. } => server.metrics().fetch_error(),
    }
}

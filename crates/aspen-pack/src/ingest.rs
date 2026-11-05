//! Push ingest (`docs/PLAN.md`, push path step 2), as two calls with the
//! caller's async base prefetch between them:
//!
//! ```text
//! receive(input) ──► Received { candidates } ──► caller: BasePrefetcher::prefetch
//!                                                        │
//!        IngestedPack ◄── complete(received, bases) ◄────┘
//! ```
//!
//! [`receive`] is pass 1: it streams the client's pack to a spool file while
//! gix parses it (inflating every entry, checking the trailer with SHA-1
//! collision detection), refuses a header count above the limit before
//! reading the body, and collects the base IDs of every REF_DELTA as
//! candidates.
//!
//! [`complete`] runs the completion sequence of
//! [`aspen_core::ingest::CompletionRun`]: gix re-reads the spool with a base
//! lookup that answers only from the prefetched bases, inserts each base it
//! gets just before its first delta and writes the `.idx`; a scan of the
//! sorted `.idx` decides whether to accept the run, rerun it once refusing
//! the repeated IDs, or refuse the push because the client sent an object
//! twice. A REF_DELTA that still points forward is then repaired by moving
//! it (and anything based on it) to just after its base. The result is a
//! self-contained, base-first pack with no object twice, its `.idx` and its
//! entry table, ready to upload.
//!
//! The connectivity check and commit rows come from [`crate::scan`]; the
//! upload and the `packs` row are the caller's.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use aspen_core::error::{StoreError, UnpackError};
use aspen_core::id::{ObjectId, PackChecksum, RepoId};
use aspen_core::ingest::{
    AfterRun, BaseLookup, CandidateBases, Completed, CompletionRun, ObjectKind, Refusing,
    RunReport, SortedIds,
};
use aspen_core::pack::{
    EntryKind, EntryTable, EntryTableError, NewPack, PACK_HEADER_LEN, PACK_TRAILER_LEN, PackEntry,
    PackOrigin, PushLimits, entry_table_len, find_repeated,
};

use crate::header::{self, MAX_HEADER_LEN};
use crate::idx::{self, IdxEntry, IdxV2};
use crate::{
    LayoutError, MAX_DELTA_DEPTH, MaxInflatedBytes, MaxObjectBytes, Sha1, entries, from_gix,
};

/// A pack received by pass 1, spooled to local disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    /// The spool file: exactly the bytes the client sent.
    pub spool: PathBuf,
    /// Object count from the pack header.
    pub object_count: u32,
    /// Bytes the client sent (`packs.received_bytes`).
    pub received_bytes: u64,
    /// Base IDs of every REF_DELTA entry; prefetch these.
    pub candidates: CandidateBases,
    /// The client's trailer.
    pub client_checksum: PackChecksum,
    /// Offsets of every delta entry, and of the largest other entry: the
    /// entries whose declared sizes [`complete`] checks against the object
    /// limit before gix decodes anything (8 bytes per delta).
    checked_offsets: Vec<u64>,
    /// Sum of the full (non-delta) entries' declared sizes, for the total
    /// limit; the deltas' result sizes are read by [`complete`].
    full_bytes: u64,
}

/// Pass 1: streams `input` to `spool` (created or truncated; put it on a
/// real disk, never tmpfs, S4) while parsing and validating it.
///
/// The header's object count is checked against `limits` before any of the
/// body is read, and the pack may not exceed `limits.max_pack_bytes`.
/// Reading stops at the pack's trailer; bytes after it are never requested,
/// so a connection that stays open (SSH) cannot stall the receive.
pub fn receive(
    input: &mut dyn Read,
    spool: &Path,
    limits: &PushLimits,
) -> Result<Received, UnpackError> {
    let mut head = [0u8; PACK_HEADER_LEN as usize];
    input
        .read_exact(&mut head)
        .map_err(|e| malformed(format!("cannot read the pack header: {e}")))?;
    let (_version, object_count) = gix_pack::data::header::decode(&head)
        .map_err(|e| malformed(format!("bad pack header: {}", gix_chain(&e))))?;
    limits.check_header(object_count)?;
    let file = File::create(spool).map_err(local_io)?;
    let over_limit = Cell::new(false);
    let tee = Tee {
        inner: io::Cursor::new(head).chain(input),
        out: BufWriter::with_capacity(1 << 20, file),
        written: 0,
        limit: limits.max_pack_bytes,
        over_limit: &over_limit,
    };
    let mut reader = BufReader::with_capacity(64 * 1024, tee);
    let mut candidates = Vec::new();
    let mut deltas = Vec::new();
    let mut largest: Option<(u64, u64)> = None;
    let mut full_bytes = 0u64;
    let mut end = PACK_HEADER_LEN;
    let mut trailer = None;
    if object_count == 0 {
        // The reader replays the header first, then the trailer follows.
        let mut both = [0u8; (PACK_HEADER_LEN + PACK_TRAILER_LEN) as usize];
        reader
            .read_exact(&mut both)
            .map_err(|e| malformed(format!("cannot read the trailer: {e}")))?;
        let t: [u8; 20] = both[PACK_HEADER_LEN as usize..]
            .try_into()
            .expect("20 bytes");
        if crate::sha1(&head).as_bytes() != &t {
            return Err(malformed("trailer checksum does not match"));
        }
        trailer = Some(t);
        end += PACK_TRAILER_LEN;
    } else {
        let iter = gix_pack::data::input::BytesToEntriesIter::new_from_header(
            &mut reader,
            gix_pack::data::input::Mode::Verify,
            gix_pack::data::input::EntryDataMode::Crc32,
            gix_hash::Kind::Sha1,
        )
        .map_err(|e| pass1_error(&e, &over_limit, limits))?;
        for entry in iter {
            let entry = entry.map_err(|e| pass1_error(&e, &over_limit, limits))?;
            match entry.header {
                gix_pack::data::entry::Header::RefDelta { base_id } => {
                    candidates.push(from_gix(&base_id));
                    deltas.push(entry.pack_offset);
                }
                gix_pack::data::entry::Header::OfsDelta { .. } => deltas.push(entry.pack_offset),
                _ => {
                    full_bytes = full_bytes.saturating_add(entry.decompressed_size);
                    if largest.is_none_or(|(size, _)| entry.decompressed_size > size) {
                        largest = Some((entry.decompressed_size, entry.pack_offset));
                    }
                }
            }
            end = entry.pack_offset + u64::from(entry.header_size) + entry.compressed_size;
            if let Some(t) = entry.trailer {
                trailer = Some(t.as_slice().try_into().expect("SHA-1"));
            }
        }
        end += PACK_TRAILER_LEN;
    }
    let trailer = trailer.ok_or_else(|| malformed("pack ended without a trailer"))?;
    // Whatever the buffered reader pulled in beyond the trailer arrived in
    // the same read as the pack's last bytes. Git clients send nothing after
    // the pack, so that is a malformed request, not data to ignore.
    let extra = reader.buffer().len() as u64;
    let mut tee = reader.into_inner();
    if tee.written - extra != end {
        return Err(malformed("pack length does not match its entries"));
    }
    if extra > 0 {
        return Err(malformed("unexpected bytes after the pack trailer"));
    }
    tee.out.flush().map_err(local_io)?;
    Ok(Received {
        spool: spool.to_owned(),
        object_count,
        received_bytes: end,
        candidates: CandidateBases(SortedIds::from_unsorted(candidates)),
        client_checksum: PackChecksum::from_bytes(trailer),
        checked_offsets: merge_offset(deltas, largest.map(|(_, at)| at)),
        full_bytes,
    })
}

/// `sorted` with `extra` inserted in order.
fn merge_offset(mut sorted: Vec<u64>, extra: Option<u64>) -> Vec<u64> {
    if let Some(at) = extra {
        let i = sorted.partition_point(|&o| o < at);
        sorted.insert(i, at);
    }
    sorted
}

/// Limits and tuning for [`complete`]. There is no `Default`: both limits
/// must come from the server's configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteOptions {
    /// Threads for delta resolution; `None` uses every core. Peak memory
    /// is at most `max_inflated_bytes` of resolved objects plus about
    /// threads × 3 × `max_object_bytes` of working buffers (base, delta,
    /// result) plus the delta tree (S4: about 140 bytes per object).
    pub thread_limit: Option<usize>,
    /// The largest object or delta result completion inflates. An entry
    /// that claims more is refused as [`UnpackError::Malformed`] before
    /// anything that size is allocated.
    pub max_object_bytes: MaxObjectBytes,
    /// The most the pack, with the thin bases completion inserts, may
    /// inflate to in total ([`MaxInflatedBytes`]). A pack that declares
    /// more is refused as [`UnpackError::Malformed`] before gix resolves
    /// anything.
    pub max_inflated_bytes: MaxInflatedBytes,
}

impl CompleteOptions {
    /// Options with these limits and every core.
    pub fn new(max_object_bytes: MaxObjectBytes, max_inflated_bytes: MaxInflatedBytes) -> Self {
        Self {
            thread_limit: None,
            max_object_bytes,
            max_inflated_bytes,
        }
    }
}

/// A completed, indexed pack on local disk, ready to upload.
///
/// It is self-contained, base-first and holds no object twice; its `.idx`
/// and entry table describe it exactly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestedPack {
    /// `<dir>/<checksum>.pack`.
    pub pack_path: PathBuf,
    /// `<dir>/<checksum>.idx`.
    pub idx_path: PathBuf,
    /// `<dir>/<checksum>.entries`.
    pub entries_path: PathBuf,
    /// Trailing SHA-1 of the stored bytes.
    pub checksum: PackChecksum,
    /// Stored size.
    pub size: u64,
    /// `.idx` size.
    pub idx_size: u64,
    /// Objects stored: the client's plus the inserted bases.
    pub object_count: u32,
    /// REF_DELTA entries left in the stored pack.
    pub ref_delta_count: u32,
    /// Bytes the client sent.
    pub received_bytes: u64,
    /// How the completion sequence ended.
    pub completion: Completed,
    /// Whether the forward-base repair rewrote the pack.
    pub repaired: bool,
    /// Longest delta chain in the stored pack (0: no deltas), at most
    /// [`MAX_DELTA_DEPTH`]. A stitched pack's depth can approach the sum of
    /// its segments' depths, so the fetch planner sees this per pack.
    pub max_delta_depth: u32,
    /// The objects completion inserted from the dependencies (the final
    /// run's answered lookups), which the client did not send. The
    /// pack-reachability walk behind `refs::Closure::of_new_version` must
    /// exclude them (decisions log, N1 review of #18): they are often
    /// objects the new tip does not reach.
    pub completion_bases: SortedIds,
}

impl IngestedPack {
    /// The `packs` row to register once the three blobs are durable.
    pub fn new_pack(&self, repo: RepoId) -> NewPack {
        NewPack {
            repo,
            checksum: self.checksum,
            size: self.size,
            idx_size: self.idx_size,
            entries_size: entry_table_len(self.object_count),
            object_count: self.object_count,
            ref_delta_count: self.ref_delta_count,
            origin: PackOrigin::Push {
                received_bytes: self.received_bytes,
            },
        }
    }
}

/// Completes, indexes and lays out a received pack (see the module docs).
///
/// `bases` is the prefetched lookup: it must answer from local data only.
/// Output files go to `out_dir`, named by the final checksum; scratch files
/// go to a subdirectory of it that is removed before this returns, whether
/// it succeeds or fails.
///
/// A pack with no objects is refused with [`UnpackError::Malformed`]: a push
/// that brings no objects stores no pack, and the caller should not call
/// this for it. So is a pack with an object or delta result over
/// `options.max_object_bytes`, one that inflates (with its inserted bases)
/// to more than `options.max_inflated_bytes`, or a delta chain deeper than
/// [`MAX_DELTA_DEPTH`].
pub fn complete<L: BaseLookup>(
    received: &Received,
    bases: &L,
    out_dir: &Path,
    options: &CompleteOptions,
) -> Result<IngestedPack, UnpackError> {
    if received.object_count == 0 {
        return Err(malformed("an empty pack stores nothing"));
    }
    // Unique per call: two pushes of the same bytes may share `out_dir`.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let scratch = out_dir.join(format!(
        "ingest-{}-{}-{}.tmp",
        received.client_checksum.to_hex(),
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    fs::create_dir(&scratch).map_err(local_io)?;
    let result = complete_in(received, bases, out_dir, &scratch, options);
    let _ = fs::remove_dir_all(&scratch);
    result
}

fn complete_in<L: BaseLookup>(
    received: &Received,
    bases: &L,
    out_dir: &Path,
    scratch: &Path,
    options: &CompleteOptions,
) -> Result<IngestedPack, UnpackError> {
    // gix's own limit also covers its delta tree, so it grows with the entry
    // count: check every declared size against the object limit first, and
    // their sum against the total limit (gix's resolver can hold every
    // object on a path of the delta tree at once).
    let declared_error = |e| match e {
        crate::declared::DeclaredError::Io(e) => local_io(e),
        e => malformed(e.to_string()),
    };
    let totals = crate::declared::check_at(
        File::open(&received.spool).map_err(local_io)?,
        received.checked_offsets.iter().copied(),
        options.max_object_bytes,
    )
    .map_err(declared_error)?;
    let own_bytes = received.full_bytes.saturating_add(totals.delta_results);
    crate::declared::check_total(own_bytes, options.max_inflated_bytes, ",")
        .map_err(declared_error)?;
    let mut run = CompletionRun::first();
    let (pack, idx_bytes, completed, inserted) = loop {
        let run_dir = scratch.join(format!("run-{}", run.attempt()));
        fs::create_dir_all(&run_dir).map_err(local_io)?;
        let lookup = Refusing::new(bases, run.refused());
        let (pack, idx_bytes, unanswered, inserted) = complete_once(
            &received.spool,
            received.object_count,
            &lookup,
            &run_dir,
            options,
            options.max_inflated_bytes.get() - own_bytes,
        )?;
        let idx = IdxV2::parse_allowing_repeats(&idx_bytes, None).map_err(internal)?;
        let ids: Vec<ObjectId> = idx.ids().collect();
        // The parse checked that the IDs never decrease; this scan finds the
        // ones that repeat.
        let repeated = find_repeated(&ids).map_err(|e| internal(e.to_string()))?;
        match run.finish(RunReport {
            unanswered,
            repeated,
        })? {
            AfterRun::Rerun(next) => run = next,
            AfterRun::Completed(done) => break (pack, idx_bytes, done, inserted),
        }
    };
    let idx = IdxV2::parse(&idx_bytes, None).map_err(internal)?;
    let size = fs::metadata(&pack).map_err(local_io)?.len();
    let table = entries::build(File::open(&pack).map_err(local_io)?, size, &idx)
        .map_err(layout_internal)?;
    // Only a REF_DELTA can point forward, and one remains only if the final
    // run left a lookup unanswered or refused an ID, which is exactly when
    // `Completed::forward_base_check` is set.
    if !completed.forward_base_check && table.ref_delta_count() > 0 {
        return Err(internal(
            "a REF_DELTA remains although the final run answered every lookup",
        ));
    }
    let needs_repair = if completed.forward_base_check {
        match table.check_layout() {
            Ok(()) => false,
            Err(EntryTableError::ForwardBase { .. }) => true,
            Err(e) => return Err(internal(format!("completed pack: {e}"))),
        }
    } else {
        false
    };
    let (pack, idx_bytes, table) = if needs_repair {
        let repaired = scratch.join("repaired.pack");
        let (idx_bytes, table) = repair(&pack, &table, &repaired).map_err(layout_internal)?;
        (repaired, idx_bytes, table)
    } else {
        (pack, idx_bytes, table)
    };
    // gix asks the lookup once per base it inserts, so the answered lookups
    // are exactly the objects added to the client's pack.
    let completion_bases = SortedIds::from_unsorted(inserted);
    // Git never writes a chain deeper than this, and a deeper one makes
    // every later read and stitch of the pack walk it (decisions log, A2).
    let max_delta_depth = crate::verify::max_delta_depth(&table);
    if max_delta_depth > MAX_DELTA_DEPTH {
        return Err(malformed(format!(
            "a delta chain of depth {max_delta_depth} is deeper than Git's maximum of {MAX_DELTA_DEPTH}"
        )));
    }
    if table.entries().len() != received.object_count as usize + completion_bases.len() {
        return Err(internal(format!(
            "{} stored objects from {} received and {} inserted bases",
            table.entries().len(),
            received.object_count,
            completion_bases.len()
        )));
    }
    let checksum = *table.checksum();
    let base = out_dir.join(checksum.to_hex());
    let paths = |ext| base.with_extension(ext);
    let (pack_path, idx_path, entries_path) = (paths("pack"), paths("idx"), paths("entries"));
    fs::rename(&pack, &pack_path).map_err(local_io)?;
    fs::write(&idx_path, &idx_bytes).map_err(local_io)?;
    fs::write(&entries_path, table.encode()).map_err(local_io)?;
    Ok(IngestedPack {
        pack_path,
        idx_path,
        entries_path,
        checksum,
        size: table.pack_size(),
        idx_size: idx_bytes.len() as u64,
        object_count: table.entries().len() as u32,
        ref_delta_count: table.ref_delta_count(),
        received_bytes: received.received_bytes,
        completion: completed,
        repaired: needs_repair,
        max_delta_depth,
        completion_bases,
    })
}

/// One gix completion run over the spool. Returns the pack, its `.idx`
/// bytes, how many lookups went unanswered and the IDs that were answered.
/// The bases inserted may total at most `base_budget` bytes.
fn complete_once<L: BaseLookup>(
    spool: &Path,
    object_count: u32,
    lookup: &L,
    run_dir: &Path,
    options: &CompleteOptions,
    base_budget: u64,
) -> Result<(PathBuf, Vec<u8>, u64, Vec<ObjectId>), UnpackError> {
    let find = GixLookup {
        inner: lookup,
        unanswered: Cell::new(0),
        answered: RefCell::new(Vec::new()),
        error: RefCell::new(None),
        budget: Cell::new(base_budget),
        over_budget: Cell::new(None),
        limit: options.max_inflated_bytes.get(),
        max_object: options.max_object_bytes.get(),
        over_object_limit: Cell::new(None),
    };
    let reader = BufReader::with_capacity(64 * 1024, File::open(spool).map_err(local_io)?);
    let pack = run_dir.join("completed.pack");
    let out = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&pack)
        .map_err(local_io)?;
    let mut idx_bytes = Vec::new();
    let outcome = write_completed(
        reader,
        out,
        &pack,
        &find,
        &mut idx_bytes,
        object_count,
        options,
    );
    if let Some(total) = find.over_budget.get() {
        // gix inserts every base while it copies the entries, before it
        // resolves any delta, so nothing was resolved yet.
        return Err(malformed(
            crate::declared::DeclaredError::OverTotal {
                total,
                with: ", counting the delta bases completion inserts,",
                limit: options.max_inflated_bytes.get(),
            }
            .to_string(),
        ));
    }
    if let Some((id, len)) = find.over_object_limit.take() {
        return Err(malformed(format!(
            "delta base {id} is {len} bytes, over the {}-byte object limit",
            options.max_object_bytes.get()
        )));
    }
    if let Some(e) = find.error.borrow_mut().take() {
        return Err(UnpackError::Store(e));
    }
    let outcome = outcome.map_err(|e| completion_error(&e))?;
    if outcome.num_objects == 0 {
        return Err(internal("gix wrote no pack"));
    }
    let answered = find.answered.take();
    Ok((pack, idx_bytes, find.unanswered.get(), answered))
}

/// What `gix_pack::Bundle::write_to_directory` does with a thin-pack lookup,
/// with aspen-pack's [`ThinCompletion`](crate::thin::ThinCompletion) in
/// place of gix's own completion iterator (issue #151): gix parses the
/// spool, the iterator inserts the bases, gix writes the completed pack to
/// `out` (the file at `pack`) and its `.idx` v2 to `idx`, resolving every
/// delta. The options are gix's defaults, so the bytes are what gix wrote
/// before.
fn write_completed<F: gix_object::Find>(
    reader: BufReader<File>,
    out: File,
    pack: &Path,
    lookup: F,
    idx: &mut Vec<u8>,
    object_count: u32,
    options: &CompleteOptions,
) -> gix_error::Result<gix_pack::index::write::Outcome> {
    let defaults = gix_pack::bundle::write::Options::default();
    let parsed = gix_pack::data::input::BytesToEntriesIter::new_from_header(
        reader,
        defaults.iteration_mode,
        gix_pack::data::input::EntryDataMode::KeepAndCrc32,
        gix_hash::Kind::Sha1,
    )?;
    let version = parsed.version();
    let completed = crate::thin::ThinCompletion::new(parsed, lookup, defaults.compression);
    let mut entries = gix_pack::data::input::EntriesToBytesIter::new(
        completed,
        ReadableBufWriter(BufWriter::with_capacity(64 * 1024, out)),
        version,
        gix_hash::Kind::Sha1,
    );
    let interrupt = AtomicBool::new(false);
    gix_pack::index::write_data_iter_to_stream(
        defaults.index_version,
        // Called once every entry is written and the trailer flushed.
        || {
            let data =
                gix_pack::data::File::at(pack, gix_hash::Kind::Sha1).map_err(io::Error::other)?;
            Ok((entry_slice, data))
        },
        &mut entries,
        options.thread_limit,
        &mut gix_utils::progress::Discard,
        idx,
        &interrupt,
        gix_hash::Kind::Sha1,
        // Completion can at most double the entries (one base per
        // REF_DELTA), which is how gix sizes its delta tree.
        Some(
            options
                .max_object_bytes
                .gix_alloc_limit(u64::from(object_count) * 2),
        ),
        version,
    )
}

fn entry_slice(range: gix_pack::data::EntryRange, data: &gix_pack::data::File) -> Option<&[u8]> {
    data.entry_slice(range)
}

/// A buffered pack file that gix's pack writer can also read back (it
/// re-reads the whole file to hash it once the header count is rewritten).
struct ReadableBufWriter(BufWriter<File>);

impl Read for ReadableBufWriter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.flush()?;
        self.0.get_mut().read(buf)
    }
}

impl Write for ReadableBufWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Seek for ReadableBufWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}

/// Adapts a [`BaseLookup`] to the `gix_object::Find` gix calls, counting
/// the lookups it does not answer and refusing bases over the object limit
/// or past the budget left by the total limit.
struct GixLookup<'a, L> {
    inner: &'a L,
    unanswered: Cell<u64>,
    answered: RefCell<Vec<ObjectId>>,
    error: RefCell<Option<StoreError>>,
    /// Bytes of inserted bases still allowed.
    budget: Cell<u64>,
    /// The total reached by the base that went over the budget.
    over_budget: Cell<Option<u64>>,
    /// The total limit, to report that total.
    limit: u64,
    /// The object limit each inserted base must be within.
    max_object: u64,
    /// The base that was over the object limit, and its length.
    over_object_limit: Cell<Option<(ObjectId, u64)>>,
}

impl<L: BaseLookup> gix_object::Find for &GixLookup<'_, L> {
    fn try_find<'a>(
        &self,
        id: &gix_hash::oid,
        buffer: &'a mut Vec<u8>,
    ) -> gix_error::Result<Option<gix_object::Data<'a>>> {
        let core_id = from_gix(id);
        match self.inner.find_base(&core_id, buffer) {
            Ok(Some(kind)) => {
                let len = buffer.len() as u64;
                if len > self.max_object {
                    self.over_object_limit.set(Some((core_id, len)));
                    return Err(gix_error::message!("a delta base is over the object limit")
                        .validation_error());
                }
                if len > self.budget.get() {
                    let total = (self.limit - self.budget.get()).saturating_add(len);
                    self.over_budget.set(Some(total));
                    return Err(gix_error::message!(
                        "inserted delta bases exceed the total inflated limit"
                    )
                    .validation_error());
                }
                self.budget.set(self.budget.get() - len);
                self.answered.borrow_mut().push(core_id);
                Ok(Some(gix_object::Data::new(
                    buffer,
                    gix_kind(kind),
                    gix_hash::Kind::Sha1,
                )))
            }
            Ok(None) => {
                self.unanswered.set(self.unanswered.get() + 1);
                Ok(None)
            }
            Err(e) => {
                let err = gix_error::Error::from_error(e.clone());
                *self.error.borrow_mut() = Some(e);
                Err(err)
            }
        }
    }
}

pub(crate) fn gix_kind(kind: ObjectKind) -> gix_object::Kind {
    match kind {
        ObjectKind::Commit => gix_object::Kind::Commit,
        ObjectKind::Tree => gix_object::Kind::Tree,
        ObjectKind::Blob => gix_object::Kind::Blob,
        ObjectKind::Tag => gix_object::Kind::Tag,
    }
}

pub(crate) fn core_kind(kind: gix_object::Kind) -> ObjectKind {
    match kind {
        gix_object::Kind::Commit => ObjectKind::Commit,
        gix_object::Kind::Tree => ObjectKind::Tree,
        gix_object::Kind::Blob => ObjectKind::Blob,
        gix_object::Kind::Tag => ObjectKind::Tag,
    }
}

/// Every message in a gix error chain, outermost first.
pub(crate) fn gix_chain(e: &gix_error::Error) -> String {
    let parts: Vec<String> = e.iter_errors().map(|e| e.to_string()).collect();
    if parts.is_empty() {
        e.to_string()
    } else {
        parts.join(": ")
    }
}

/// The text gix 0.77 uses when a REF_DELTA base is in neither the lookup
/// nor the pack (`cache::delta::traverse`). Pinned by a test, so a gix
/// upgrade that changes it fails loudly instead of degrading the message.
const GIX_MISSING_BASE: &str = "The ref-delta base object ";

pub(crate) fn completion_error(e: &gix_error::Error) -> UnpackError {
    let chain = gix_chain(e);
    if let Some(at) = chain.find(GIX_MISSING_BASE)
        && let Some(hex) = chain.get(at + GIX_MISSING_BASE.len()..at + GIX_MISSING_BASE.len() + 40)
        && let Ok(id) = ObjectId::parse_hex(hex)
    {
        return UnpackError::MissingBase { id };
    }
    if e.classify().is_resource_exhausted() {
        return malformed(format!(
            "an object exceeds this server's allocation limit ({chain})"
        ));
    }
    malformed(chain)
}

pub(crate) fn pass1_error(
    e: &gix_error::Error,
    over_limit: &Cell<bool>,
    limits: &PushLimits,
) -> UnpackError {
    if over_limit.get() {
        UnpackError::PackTooLarge {
            limit: limits.max_pack_bytes,
        }
    } else {
        malformed(gix_chain(e))
    }
}

fn malformed(reason: impl Into<String>) -> UnpackError {
    UnpackError::Malformed {
        reason: reason.into(),
    }
}

fn local_io(e: io::Error) -> UnpackError {
    UnpackError::Store(StoreError::LocalIo {
        reason: e.to_string(),
    })
}

/// A broken invariant of our own output (not the client's): a bug, reported
/// as a local storage failure so the push is retryable and nothing is
/// stored.
fn internal(e: impl std::fmt::Display) -> UnpackError {
    UnpackError::Store(StoreError::LocalIo {
        reason: format!("ingest invariant: {e}"),
    })
}

fn layout_internal(e: LayoutError) -> UnpackError {
    match e {
        LayoutError::Io(e) => local_io(e),
        other => internal(other),
    }
}

/// Copies everything read to the spool, and refuses to read past `limit`.
struct Tee<'a, R> {
    inner: R,
    out: BufWriter<File>,
    written: u64,
    limit: u64,
    over_limit: &'a Cell<bool>,
}

impl<R: Read> Read for Tee<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let room = self.limit.saturating_sub(self.written);
        if room == 0 {
            // Allow one byte past the limit so a pack of exactly `limit`
            // bytes is read to its end, but a larger one is caught.
            let mut probe = [0u8; 1];
            let n = self.inner.read(&mut probe)?;
            if n > 0 {
                self.over_limit.set(true);
                return Err(io::Error::other("pack exceeds the size limit"));
            }
            return Ok(0);
        }
        let want = buf.len().min(usize::try_from(room).unwrap_or(usize::MAX));
        let n = self.inner.read(&mut buf[..want])?;
        self.out.write_all(&buf[..n])?;
        self.written += n as u64;
        Ok(n)
    }
}

/// Rewrites `pack` base-first: entries keep their order, except that each
/// entry whose base comes later (and, transitively, any entry based on it)
/// moves to just after its base. Compressed data is copied unchanged;
/// OFS_DELTA distances and every CRC32 are recomputed, the pack is
/// re-hashed, and the `.idx` is rebuilt from the known IDs without
/// resolving any delta. Returns the new `.idx` bytes and entry table.
///
/// `table` must describe a self-contained pack (every REF_DELTA base is in
/// it), which gix guarantees for a completed pack.
pub(crate) fn repair(
    pack: &Path,
    table: &EntryTable,
    out: &Path,
) -> Result<(Vec<u8>, EntryTable), LayoutError> {
    let entries = table.entries();
    let n = entries.len();
    let by_id: HashMap<ObjectId, usize> =
        entries.iter().enumerate().map(|(i, e)| (e.id, i)).collect();
    let base_of = |i: usize| -> Result<Option<usize>, LayoutError> {
        match entries[i].kind {
            EntryKind::OfsDelta { base_offset } => entries
                .binary_search_by_key(&base_offset, |e| e.offset)
                .map(Some)
                .map_err(|_| LayoutError::mismatch("OFS_DELTA base is not an entry")),
            EntryKind::RefDelta { base } => by_id.get(&base).copied().map(Some).ok_or_else(|| {
                LayoutError::mismatch(format!("REF_DELTA base {base} not in the pack"))
            }),
            _ => Ok(None),
        }
    };
    // New order: emit an entry once its base is emitted; an entry waiting on
    // a later base follows it, depth first, in original order.
    let mut order = Vec::with_capacity(n);
    let mut emitted = vec![false; n];
    let mut waiting: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..n {
        match base_of(i)? {
            Some(b) if !emitted[b] => waiting.entry(b).or_default().push(i),
            _ => {
                let mut stack = vec![i];
                while let Some(j) = stack.pop() {
                    order.push(j);
                    emitted[j] = true;
                    if let Some(children) = waiting.remove(&j) {
                        stack.extend(children.into_iter().rev());
                    }
                }
            }
        }
    }
    if order.len() != n {
        return Err(LayoutError::mismatch("delta bases form a cycle"));
    }

    let mut src = File::open(pack)?;
    let mut dst = HashingWriter::new(BufWriter::with_capacity(1 << 20, File::create(out)?));
    let mut head = Vec::with_capacity(PACK_HEADER_LEN as usize);
    head.extend_from_slice(b"PACK");
    head.extend_from_slice(&2u32.to_be_bytes());
    head.extend_from_slice(&(n as u32).to_be_bytes());
    dst.write_all(&head)?;
    let mut new_offset = vec![0u64; n];
    let mut idx_entries = Vec::with_capacity(n);
    let mut new_entries = Vec::with_capacity(n);
    let mut copy_buf = vec![0u8; 1 << 16];
    for &i in &order {
        let e = &entries[i];
        let len = table.entry_len(i);
        let mut raw = [0u8; MAX_HEADER_LEN];
        let hl = usize::from(e.header_len);
        src.seek(SeekFrom::Start(e.offset))?;
        src.read_exact(&mut raw[..hl])?;
        let h = header::decode(&raw[..hl]).map_err(|source| LayoutError::Header {
            offset: e.offset,
            source,
        })?;
        let at = dst.written;
        new_offset[i] = at;
        let mut new_header = Vec::with_capacity(MAX_HEADER_LEN);
        let kind = match e.kind {
            EntryKind::OfsDelta { .. } => {
                let b = base_of(i)?.expect("a delta has a base");
                let base_at = new_offset[b];
                new_header.extend_from_slice(&raw[..usize::from(h.size_len)]);
                header::encode_distance(at - base_at, &mut new_header);
                EntryKind::OfsDelta {
                    base_offset: base_at,
                }
            }
            other => {
                new_header.extend_from_slice(&raw[..hl]);
                other
            }
        };
        let mut crc = crc32fast::Hasher::new();
        crc.update(&new_header);
        dst.write_all(&new_header)?;
        let mut left = len - hl as u64;
        while left > 0 {
            let k = left.min(copy_buf.len() as u64) as usize;
            src.read_exact(&mut copy_buf[..k])?;
            crc.update(&copy_buf[..k]);
            dst.write_all(&copy_buf[..k])?;
            left -= k as u64;
        }
        idx_entries.push(IdxEntry {
            id: e.id,
            offset: at,
            crc32: crc.finalize(),
        });
        new_entries.push(PackEntry {
            offset: at,
            header_len: new_header.len() as u8,
            id: e.id,
            kind,
        });
    }
    let (mut w, sum, size) = dst.finish()?;
    w.write_all(&sum)?;
    w.flush()?;
    let checksum = PackChecksum::from_bytes(sum);
    let idx_bytes = idx::write(idx_entries, &checksum);
    let table = EntryTable::new(checksum, size + PACK_TRAILER_LEN, new_entries)?;
    table.check_layout()?;
    Ok((idx_bytes, table))
}

/// Hashes and counts every byte written.
pub(crate) struct HashingWriter<W> {
    inner: W,
    hash: Sha1,
    pub(crate) written: u64,
}

impl<W: Write> HashingWriter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner,
            hash: Sha1::new(),
            written: 0,
        }
    }

    pub(crate) fn finish(self) -> io::Result<(W, [u8; 20], u64)> {
        let sum = self
            .hash
            .finish()
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok((self.inner, sum, self.written))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hash.update(&buf[..n]);
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
pub(crate) fn object_id_of(kind: ObjectKind, data: &[u8]) -> ObjectId {
    let k = match kind {
        ObjectKind::Commit => "commit",
        ObjectKind::Tree => "tree",
        ObjectKind::Blob => "blob",
        ObjectKind::Tag => "tag",
    };
    let mut h = Sha1::new();
    h.update(format!("{k} {}\0", data.len()).as_bytes());
    h.update(data);
    ObjectId::from_bytes(h.finish().unwrap())
}

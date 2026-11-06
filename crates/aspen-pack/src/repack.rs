//! The repack writer: one pack built from several stored packs on local
//! disk. [`write_repack`] writes the pack a compacted cover points at (node
//! C1); [`write_repack_subset`] writes a pack holding only some of the
//! sources' objects, the exact shallow pack of node C8 (its object set
//! comes from [`crate::shallow::shallow_objects`]).
//!
//! The sources are stored packs (self-contained, base-first, no object
//! twice; [`aspen_core::pack`]) in the caller's order, oldest first. The
//! output keeps the same three invariants by construction:
//!
//! - **No object twice, nothing lost**: the output holds exactly the union
//!   of the sources' objects ([`write_repack`]), or exactly the objects the
//!   caller keeps ([`write_repack_subset`]; each must be in some source).
//!   Each object's first copy in source order is kept and every later copy
//!   is dropped, the stitcher's keep-first rule ([`crate::stitch`]).
//!   Entries of objects the caller does not keep are left out.
//! - **Base-first and self-contained**: a kept delta's base is earlier in
//!   its own source. Either that copy was emitted, or it was dropped because
//!   an earlier source's copy was emitted first; the delta is based on
//!   whichever copy was emitted. Compressed data is copied unchanged; only
//!   an OFS_DELTA's distance is rewritten, against its base's new offset. A
//!   REF_DELTA stays a REF_DELTA. In a subset, a kept delta whose base is
//!   not kept has no copy to point at, so it is stored undeltified: its
//!   full object is resolved from its own source and recompressed, as for
//!   the depth cap below.
//! - **Bounded delta depth**: each entry's depth is its base's plus one
//!   (full objects are 0). Keep-first adds depths across sources (a later
//!   source's chain can hang off the end of an earlier source's), and client
//!   packs may be up to [`MAX_DELTA_DEPTH`](crate::MAX_DELTA_DEPTH) deep, so
//!   an entry whose depth would exceed [`RepackOptions::max_delta_depth`] is
//!   stored undeltified instead: its full object is resolved from its own
//!   source pack and recompressed, and its dependents count from 0 again.
//!   The output's depth is at most the cap whatever the inputs (decisions
//!   log, A2: compaction writes packs at depth 50 or less). Trees (every
//!   object that is not a blob) have their own, lower cap,
//!   [`RepackOptions::max_tree_delta_depth`]: a push needs the trees on the
//!   paths it changes as delta bases, and keep-first chains each version of
//!   a hot tree (the root, the directories above busy files) onto the one
//!   before, so without it every compaction pack held its hot trees at the
//!   full depth and each push read 50 levels of them (monorepo benchmark,
//!   item 11). An entry's kind is its chain's: a delta's is its base's.
//!
//! The same sources in the same order (and, for a subset, the same kept
//! set) always give a byte-identical pack, so a retried compaction produces
//! the same checksum and the same blob keys. [`write_repack`] is the subset
//! writer with nothing left out, and writes the same bytes as
//! [`write_repack_subset`] keeping every object.
//!
//! Each source's entry table is checked against its `.idx` before anything
//! is written: entry tables carry no checksum of their own, and the writer
//! decides which entries to drop by the IDs in the table. Each source's
//! bytes are hashed as they are read and the trailer checked, and each
//! entry header is checked against its table entry, so a corrupt or
//! mismatched source is a [`LayoutError`], never a wrong pack.
//!
//! Memory: O(total objects). The writer holds one record per distinct
//! object (its first source, output offset and depth), plus the output's
//! `.idx` entries and entry table, about 150 bytes per object in all, and
//! one source's `.idx` at a time while checking. Entry data is streamed in
//! 64 KiB chunks; no pack is ever held in memory. An undeltified entry holds
//! its full object (at most [`MaxObjectBytes`]) while it is recompressed,
//! and the delta bases resolved for undeltified entries are cached up to
//! 96 MiB (Git's default `core.deltaBaseCacheLimit`), so undeltifying many
//! entries of one chain decodes the chain once. A subset's kept set is the
//! caller's; the writer adds a map entry per kept object, as above.
//!
//! The `.idx` and the entry table are written from what was emitted, CRC32s
//! included, with no second pass over the output.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use aspen_core::id::{ObjectId, PackChecksum, RepoId, SHA1_LEN};
use aspen_core::pack::{
    CoverLevel, EntryKind, EntryTable, NewPack, PACK_HEADER_LEN, PACK_TRAILER_LEN, PackEntry,
    PackOrigin, entry_table_len,
};

use crate::base_cache::{BaseCache, DEFAULT_BASE_CACHE_BYTES};
use crate::header::{self, MAX_HEADER_LEN, RawBase};
use crate::idx::{self, IdxEntry, IdxV2};
use crate::ingest::{HashingWriter, core_kind, gix_chain};
use crate::subset::{entry_kind, write_full};
use crate::{LayoutError, MaxObjectBytes, Sha1, to_gix};

/// One stored pack to repack, on local disk.
#[derive(Debug, Clone, Copy)]
pub struct RepackSource<'a> {
    /// The pack file: self-contained, base-first, no object twice.
    pub pack: &'a Path,
    /// Its `.idx`.
    pub idx: &'a Path,
    /// Its entry table, decoded for its `packs` row
    /// ([`EntryTable::decode_for`]).
    pub table: &'a EntryTable,
}

/// How [`write_repack`] and [`write_repack_subset`] write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepackOptions {
    /// The longest delta chain the output may hold; an entry that would be
    /// deeper is stored undeltified. Compaction uses 50, Git's default
    /// `pack.depth` (decisions log, A2).
    pub max_delta_depth: u32,
    /// The longest delta chain a tree, commit or tag may sit at (anything
    /// but a blob); at most `max_delta_depth` applies when this is larger.
    /// Compaction uses 10 (decisions log, item 11).
    pub max_tree_delta_depth: u32,
    /// Bounds each object resolved to undeltify an entry.
    pub max_object_bytes: MaxObjectBytes,
}

/// A pack written by [`write_repack`] or [`write_repack_subset`], on local
/// disk, ready to upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepackedPack {
    /// `<dir>/<checksum>.pack`.
    pub pack_path: PathBuf,
    /// `<dir>/<checksum>.idx`.
    pub idx_path: PathBuf,
    /// `<dir>/<checksum>.entries`.
    pub entries_path: PathBuf,
    /// Trailing SHA-1.
    pub checksum: PackChecksum,
    /// Size in bytes.
    pub size: u64,
    /// `.idx` size.
    pub idx_size: u64,
    /// Objects: the size of the union of the sources' objects, or of the
    /// kept set for [`write_repack_subset`].
    pub object_count: u32,
    /// REF_DELTA entries.
    pub ref_delta_count: u32,
    /// Longest delta chain (0: no deltas); at most
    /// [`RepackOptions::max_delta_depth`].
    pub max_delta_depth: u32,
    /// Longest delta chain of an object that is not a blob; at most
    /// [`RepackOptions::max_tree_delta_depth`].
    pub max_tree_delta_depth: u32,
    /// Kept deltas stored undeltified: because they would have exceeded the
    /// depth cap, or because their base was left out
    /// ([`RepackedPack::base_left_out`] of them).
    pub undeltified: u32,
    /// Of [`RepackedPack::undeltified`], the deltas stored whole because
    /// their base is not in the kept set (always 0 for [`write_repack`]).
    pub base_left_out: u32,
    /// Entries of kept objects dropped because an earlier source held the
    /// same object.
    pub dropped: u64,
    /// Entries left out because their object is not in the kept set (always
    /// 0 for [`write_repack`]). Every source entry is emitted, dropped or
    /// left out: `object_count + dropped + left_out` is the sources' total
    /// entry count.
    pub left_out: u64,
}

impl RepackedPack {
    /// The `packs` row for this pack, a compaction pack at `level`.
    pub fn new_pack(&self, repo: RepoId, level: CoverLevel) -> NewPack {
        NewPack {
            repo,
            checksum: self.checksum,
            size: self.size,
            idx_size: self.idx_size,
            entries_size: entry_table_len(self.object_count),
            object_count: self.object_count,
            ref_delta_count: self.ref_delta_count,
            origin: PackOrigin::Compaction { level },
        }
    }
}

/// Where an object's kept copy is.
#[derive(Debug, Clone, Copy)]
struct Slot {
    /// Index of the first source that holds it: that copy is kept.
    owner: u32,
    /// Output offset, delta depth and object kind of the kept copy, once
    /// emitted.
    emitted: Option<(u64, u32, Kind)>,
}

/// Writes one pack holding the union of `sources`' objects, first copy
/// wins, to `out_dir`, named by its checksum.
///
/// Fails with [`LayoutError`] if there is no object to write, if a
/// source's entry table, `.idx` and bytes disagree, or if a source breaks
/// a stored-pack invariant (a base outside it or after its delta).
pub fn write_repack(
    sources: &[RepackSource<'_>],
    out_dir: &Path,
    options: &RepackOptions,
) -> Result<RepackedPack, LayoutError> {
    repack(sources, None, out_dir, options)
}

/// Writes one pack holding exactly the objects in `keep`, each from the
/// first source in order that holds it, to `out_dir`, named by its
/// checksum. Entries of objects not in `keep` are left out; a kept delta
/// whose base is left out is stored undeltified, so the pack stays
/// self-contained and base-first. The depth cap applies as in
/// [`write_repack`].
///
/// Fails with [`LayoutError::Mismatch`] naming an ID of `keep` that no
/// source holds (the smallest such ID), and otherwise as [`write_repack`].
pub fn write_repack_subset(
    sources: &[RepackSource<'_>],
    keep: &HashSet<ObjectId>,
    out_dir: &Path,
    options: &RepackOptions,
) -> Result<RepackedPack, LayoutError> {
    repack(sources, Some(keep), out_dir, options)
}

/// [`write_repack`] (`keep` is `None`) and [`write_repack_subset`].
fn repack(
    sources: &[RepackSource<'_>],
    keep: Option<&HashSet<ObjectId>>,
    out_dir: &Path,
    options: &RepackOptions,
) -> Result<RepackedPack, LayoutError> {
    for (i, s) in sources.iter().enumerate() {
        check_source(s).map_err(|e| match e {
            LayoutError::Mismatch(what) => {
                LayoutError::mismatch(format!("repack source {i}: {what}"))
            }
            e => e,
        })?;
    }

    // Keep-first planning: the first source that lists a kept ID owns it.
    // An ID with no slot is left out.
    let mut planned: u64 = 0;
    let mut left_out: u64 = 0;
    let mut slots: HashMap<ObjectId, Slot> = HashMap::new();
    for (i, s) in sources.iter().enumerate() {
        let owner = u32::try_from(i).map_err(|_| LayoutError::mismatch("too many sources"))?;
        for e in s.table.entries() {
            if keep.is_some_and(|k| !k.contains(&e.id)) {
                left_out += 1;
                continue;
            }
            planned += 1;
            slots.entry(e.id).or_insert(Slot {
                owner,
                emitted: None,
            });
        }
    }
    if let Some(keep) = keep
        && slots.len() != keep.len()
    {
        let absent = keep
            .iter()
            .filter(|id| !slots.contains_key(id))
            .min()
            .expect("every slot is a kept ID, so some kept ID has none");
        return Err(LayoutError::mismatch(format!(
            "{absent} is to be kept but no repack source holds it"
        )));
    }
    if slots.is_empty() {
        return Err(LayoutError::mismatch("a repack needs an object"));
    }
    let count =
        u32::try_from(slots.len()).map_err(|_| LayoutError::mismatch("too many objects"))?;
    let dropped = planned - u64::from(count);

    fs::create_dir_all(out_dir)?;
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = out_dir.join(format!(
        "repack-{}-{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let result = (|| {
        let mut dst = HashingWriter::new(BufWriter::with_capacity(1 << 20, File::create(&tmp)?));
        let mut head = Vec::with_capacity(PACK_HEADER_LEN as usize);
        head.extend_from_slice(b"PACK");
        head.extend_from_slice(&2u32.to_be_bytes());
        head.extend_from_slice(&count.to_be_bytes());
        dst.write_all(&head)?;
        let mut out = Output {
            idx_entries: Vec::with_capacity(count as usize),
            entries: Vec::with_capacity(count as usize),
            max_depth: 0,
            max_tree_depth: 0,
            undeltified: 0,
            base_left_out: 0,
        };
        let mut copy_buf = vec![0u8; 1 << 16];
        let mut bases = BaseCache::new(DEFAULT_BASE_CACHE_BYTES);
        for (i, s) in sources.iter().enumerate() {
            copy_source(
                i as u32,
                s,
                options,
                &mut slots,
                &mut dst,
                &mut out,
                &mut copy_buf,
                &mut bases,
            )
            .map_err(|e| match e {
                LayoutError::Mismatch(what) => {
                    LayoutError::mismatch(format!("repack source {i}: {what}"))
                }
                e => e,
            })?;
        }
        if out.entries.len() != count as usize {
            return Err(LayoutError::mismatch(format!(
                "emitted {} objects, planned {count}",
                out.entries.len()
            )));
        }
        let (mut w, sum, size) = dst.finish()?;
        w.write_all(&sum)?;
        w.flush()?;
        drop(w);
        let checksum = PackChecksum::from_bytes(sum);
        let idx_bytes = idx::write(out.idx_entries, &checksum);
        let table = EntryTable::new(checksum, size + PACK_TRAILER_LEN, out.entries)?;
        table.check_layout()?;
        let base = out_dir.join(checksum.to_hex());
        let paths = |ext| base.with_extension(ext);
        let (pack_path, idx_path, entries_path) = (paths("pack"), paths("idx"), paths("entries"));
        fs::rename(&tmp, &pack_path)?;
        fs::write(&idx_path, &idx_bytes)?;
        fs::write(&entries_path, table.encode())?;
        Ok(RepackedPack {
            pack_path,
            idx_path,
            entries_path,
            checksum,
            size: table.pack_size(),
            idx_size: idx_bytes.len() as u64,
            object_count: count,
            ref_delta_count: table.ref_delta_count(),
            max_delta_depth: out.max_depth,
            max_tree_delta_depth: out.max_tree_depth,
            undeltified: out.undeltified,
            base_left_out: out.base_left_out,
            dropped,
            left_out,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

/// What has been emitted so far.
struct Output {
    idx_entries: Vec<IdxEntry>,
    entries: Vec<PackEntry>,
    max_depth: u32,
    max_tree_depth: u32,
    undeltified: u32,
    base_left_out: u32,
}

/// What an entry resolves to, as far as the depth caps care.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Blob,
    /// A tree, commit or tag.
    Tree,
}

impl Kind {
    fn of(kind: EntryKind) -> Option<Self> {
        match kind {
            EntryKind::Blob => Some(Self::Blob),
            EntryKind::Tree | EntryKind::Commit | EntryKind::Tag => Some(Self::Tree),
            EntryKind::OfsDelta { .. } | EntryKind::RefDelta { .. } => None,
        }
    }
}

/// Checks that a source's file, `.idx` and entry table describe the same
/// pack: same size, same checksum, and the same `(id, offset)` pairs (the
/// counts are equal and table IDs are unique, so every table entry found at
/// its offset in the `.idx` makes the two the same set). Also checks that
/// the table is self-contained and base-first.
fn check_source(s: &RepackSource<'_>) -> Result<(), LayoutError> {
    let size = fs::metadata(s.pack)?.len();
    if size != s.table.pack_size() {
        return Err(LayoutError::mismatch(format!(
            "pack file is {size} bytes, its entry table says {}",
            s.table.pack_size()
        )));
    }
    let idx_bytes = fs::read(s.idx)?;
    let idx = IdxV2::parse(&idx_bytes, Some(size))?;
    if idx.pack_checksum() != *s.table.checksum() {
        return Err(LayoutError::mismatch(
            "the entry table and the .idx name different packs",
        ));
    }
    let entries = s.table.entries();
    if idx.len() != entries.len() {
        return Err(LayoutError::mismatch(format!(
            "the entry table lists {} entries, the .idx {}",
            entries.len(),
            idx.len()
        )));
    }
    for e in entries {
        match idx.lookup(&e.id) {
            Some(p) if idx.offset(p) == e.offset => {}
            _ => {
                return Err(LayoutError::mismatch(format!(
                    "the entry table puts {} at offset {}, unlike the .idx",
                    e.id, e.offset
                )));
            }
        }
    }
    s.table.check_layout()?;
    Ok(())
}

/// Reads a source pack front to back, hashing every byte before the
/// trailer.
struct SourceReader {
    inner: BufReader<File>,
    hash: Sha1,
    pos: u64,
}

impl SourceReader {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), LayoutError> {
        self.inner.read_exact(buf)?;
        self.hash.update(buf);
        self.pos += buf.len() as u64;
        Ok(())
    }

    fn skip(&mut self, mut n: u64, buf: &mut [u8]) -> Result<(), LayoutError> {
        while n > 0 {
            let k = n.min(buf.len() as u64) as usize;
            self.read_exact(&mut buf[..k])?;
            n -= k as u64;
        }
        Ok(())
    }
}

/// Emits source `index`'s owned entries; checks its header, entry headers
/// and trailer as it reads. Entries with no slot are left out.
#[allow(clippy::too_many_arguments)]
fn copy_source<W: Write>(
    index: u32,
    s: &RepackSource<'_>,
    options: &RepackOptions,
    slots: &mut HashMap<ObjectId, Slot>,
    dst: &mut HashingWriter<W>,
    out: &mut Output,
    copy_buf: &mut [u8],
    bases: &mut BaseCache,
) -> Result<(), LayoutError> {
    let table = s.table;
    let entries = table.entries();
    let mut src = SourceReader {
        inner: BufReader::with_capacity(1 << 20, File::open(s.pack)?),
        hash: Sha1::new(),
        pos: 0,
    };
    let mut head = [0u8; PACK_HEADER_LEN as usize];
    src.read_exact(&mut head)?;
    if &head[..4] != b"PACK"
        || u32::from_be_bytes(head[4..8].try_into().expect("4 bytes")) != 2
        || u32::from_be_bytes(head[8..12].try_into().expect("4 bytes")) as usize != entries.len()
    {
        return Err(LayoutError::mismatch(
            "pack header is not version 2 with the table's count",
        ));
    }
    // Opened lazily: only an undeltified entry needs delta resolution.
    let mut bundle: Option<gix_pack::Bundle> = None;
    for (i, e) in entries.iter().enumerate() {
        debug_assert_eq!(src.pos, e.offset, "entries tile the pack");
        let len = table.entry_len(i);
        let hl = usize::from(e.header_len);
        let mut raw = [0u8; MAX_HEADER_LEN];
        src.read_exact(&mut raw[..hl])?;
        let h = header::decode(&raw[..hl]).map_err(|source| LayoutError::Header {
            offset: e.offset,
            source,
        })?;
        if !header_matches(&h, e) {
            return Err(LayoutError::mismatch(format!(
                "entry at offset {} does not match its entry table record",
                e.offset
            )));
        }
        match slots.get(&e.id) {
            Some(slot) if slot.owner == index => {}
            // Left out, or an earlier source's copy was kept.
            _ => {
                src.skip(len - hl as u64, copy_buf)?;
                continue;
            }
        }
        let base_id = match e.kind {
            EntryKind::OfsDelta { base_offset } => Some(
                entries[entries
                    .binary_search_by_key(&base_offset, |b| b.offset)
                    .map_err(|_| LayoutError::mismatch("OFS_DELTA base is not an entry"))?]
                .id,
            ),
            EntryKind::RefDelta { base } => Some(base),
            _ => None,
        };
        // A base with no slot was left out; a base with a slot was planned
        // (kept), and base-first sources emit it before its delta.
        let (base, base_left_out) = match base_id {
            None => (None, false),
            Some(b) => match slots.get(&b) {
                None => (None, true),
                Some(slot) => (
                    Some(slot.emitted.ok_or_else(|| {
                        LayoutError::mismatch(format!(
                            "the base of {} is not emitted before it",
                            e.id
                        ))
                    })?),
                    false,
                ),
            },
        };
        let at = dst.written;
        let depth = base.map_or(0, |(_, d, _)| d + 1);
        // A delta is what its base is; a full entry says what it is.
        let kind = base.map(|(_, _, k)| k).or(Kind::of(e.kind));
        let cap = match kind {
            Some(Kind::Tree) => options.max_tree_delta_depth.min(options.max_delta_depth),
            _ => options.max_delta_depth,
        };
        if base_left_out || depth > cap {
            // No base in the output, or too deep: store the object whole,
            // which restarts the chain.
            src.skip(len - hl as u64, copy_buf)?;
            if bundle.is_none() {
                bundle = Some(open_bundle(s, options.max_object_bytes)?);
            }
            let b = bundle.as_ref().expect("opened");
            let mut buf = Vec::new();
            let mut inflate = gix_zlib::Inflate::default();
            let (data, _) = b
                .find(
                    &to_gix(&e.id),
                    &mut buf,
                    &mut inflate,
                    &mut bases.for_source(index),
                )
                .map_err(|err| LayoutError::mismatch(gix_chain(&err)))?
                .ok_or_else(|| LayoutError::mismatch(format!("{} is not in its own pack", e.id)))?;
            let kind = core_kind(data.kind);
            let (head_len, crc) = write_full(dst, kind, data.data)?;
            let full = entry_kind(kind);
            out.undeltified += 1;
            out.base_left_out += u32::from(base_left_out);
            let entry = PackEntry {
                offset: at,
                header_len: head_len,
                id: e.id,
                kind: full,
            };
            emit(
                slots,
                out,
                entry,
                0,
                Kind::of(full).expect("a full entry"),
                crc,
            );
            continue;
        }
        let mut new_header = Vec::with_capacity(MAX_HEADER_LEN);
        let kind = kind.ok_or_else(|| {
            LayoutError::mismatch(format!("the base of {} is not emitted before it", e.id))
        })?;
        let new_kind = match (e.kind, base) {
            (EntryKind::OfsDelta { .. }, Some((base_at, _, _))) => {
                new_header.extend_from_slice(&raw[..usize::from(h.size_len)]);
                header::encode_distance(at - base_at, &mut new_header);
                EntryKind::OfsDelta {
                    base_offset: base_at,
                }
            }
            (other, _) => {
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
        let entry = PackEntry {
            offset: at,
            header_len: new_header.len() as u8,
            id: e.id,
            kind: new_kind,
        };
        emit(slots, out, entry, depth, kind, crc.finalize());
    }
    let data_end = table.pack_size() - PACK_TRAILER_LEN;
    if src.pos != data_end {
        return Err(LayoutError::mismatch("entries do not tile the pack"));
    }
    let mut trailer = [0u8; SHA1_LEN];
    src.inner.read_exact(&mut trailer)?;
    let sum = src
        .hash
        .finish()
        .map_err(|e| LayoutError::mismatch(e.to_string()))?;
    if sum != trailer || &trailer != table.checksum().as_bytes() {
        return Err(LayoutError::mismatch(
            "the pack's bytes do not hash to its checksum",
        ));
    }
    Ok(())
}

/// Whether a decoded entry header says what the table record says.
fn header_matches(h: &header::RawHeader, e: &PackEntry) -> bool {
    h.len == e.header_len
        && match (e.kind, h.base) {
            (EntryKind::Commit, RawBase::None) => h.type_code == 1,
            (EntryKind::Tree, RawBase::None) => h.type_code == 2,
            (EntryKind::Blob, RawBase::None) => h.type_code == 3,
            (EntryKind::Tag, RawBase::None) => h.type_code == 4,
            (EntryKind::OfsDelta { base_offset }, RawBase::Distance(d)) => {
                e.offset.checked_sub(d) == Some(base_offset)
            }
            (EntryKind::RefDelta { base }, RawBase::Id(id)) => base == id,
            _ => false,
        }
}

/// Records an emitted entry of `kind` at `depth` with its CRC32.
fn emit(
    slots: &mut HashMap<ObjectId, Slot>,
    out: &mut Output,
    entry: PackEntry,
    depth: u32,
    kind: Kind,
    crc32: u32,
) {
    if let Some(slot) = slots.get_mut(&entry.id) {
        slot.emitted = Some((entry.offset, depth, kind));
    }
    out.max_depth = out.max_depth.max(depth);
    if kind == Kind::Tree {
        out.max_tree_depth = out.max_tree_depth.max(depth);
    }
    out.idx_entries.push(IdxEntry {
        id: entry.id,
        offset: entry.offset,
        crc32,
    });
    out.entries.push(entry);
}

/// A gix bundle over a source's pack and `.idx`, which need not share a
/// file stem.
pub(crate) fn open_bundle(
    s: &RepackSource<'_>,
    max_object_bytes: MaxObjectBytes,
) -> Result<gix_pack::Bundle, LayoutError> {
    let gix = |e: gix_error::Error| LayoutError::mismatch(gix_chain(&e));
    let pack = gix_pack::data::File::at(s.pack, gix_hash::Kind::Sha1)
        .map_err(gix)?
        .with_alloc_limit_bytes(Some(max_object_bytes.gix_alloc_limit(0)));
    let index = gix_pack::index::File::at(s.idx, gix_hash::Kind::Sha1).map_err(gix)?;
    Ok(gix_pack::Bundle { pack, index })
}

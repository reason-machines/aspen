//! Writing a pack that holds a subset of a completed pack's entries, plus
//! extra full objects: the per-write packs of a multi-ref push and of a
//! forced trunk update (node B1, decisions log).
//!
//! The output keeps the stored-pack invariants (self-contained, base-first,
//! no object twice) by construction:
//!
//! - kept entries keep their order, which is base-first in the input;
//! - a kept delta whose base entry is also kept is copied byte for byte
//!   (an OFS_DELTA gets the distance to its base's new offset; a REF_DELTA
//!   is unchanged);
//! - a kept delta whose base is **not** kept is stored undeltified: its full
//!   object is resolved from the input pack and recompressed. The foreign
//!   base is never copied in, so a write's pack holds only what the caller
//!   chose (decisions log, N1 re-review: "never with the foreign base copied
//!   in");
//! - extra objects (full objects from the dependencies, for the forced-trunk
//!   rule) follow the kept entries, undeltified. They come from memory
//!   (`[ExtraObject]`) or from an [`ExtraSpill`] on local disk, which the
//!   push path uses so that a forced trunk update holds no more than one
//!   prefetch's worth of objects in memory at a time.
//!
//! The `.idx` and the entry table are written from what was emitted, with
//! no second pass over the output, and the layout is checked before return.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use aspen_core::id::{ObjectId, PackChecksum, RepoId};
use aspen_core::ingest::ObjectKind;
use aspen_core::pack::{
    EntryKind, EntryTable, NewPack, PACK_HEADER_LEN, PACK_TRAILER_LEN, PackEntry, PackOrigin,
    entry_table_len, find_repeated,
};

use crate::header::{self, MAX_HEADER_LEN};
use crate::idx::{self, IdxEntry};
use crate::ingest::{HashingWriter, gix_chain};
use crate::{LayoutError, MaxObjectBytes, to_gix};

/// A full object to add to a subset pack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtraObject {
    /// Its ID (the caller read it from a stored pack by this ID).
    pub id: ObjectId,
    /// Its type.
    pub kind: ObjectKind,
    /// Its full, undeltified content.
    pub data: Vec<u8>,
}

/// What [`ExtraObjects::each`] calls with each object: its ID, type and
/// full content.
pub type ExtraVisitor<'a> = dyn FnMut(ObjectId, ObjectKind, &[u8]) -> Result<(), LayoutError> + 'a;

/// Where [`write_subset`] reads its extra objects from, one at a time.
pub trait ExtraObjects {
    /// How many there are.
    fn count(&self) -> usize;
    /// Calls `f` with each object (ID, type, full content) in order.
    fn each(&self, f: &mut ExtraVisitor<'_>) -> Result<(), LayoutError>;
}

impl ExtraObjects for [ExtraObject] {
    fn count(&self) -> usize {
        self.len()
    }

    fn each(&self, f: &mut ExtraVisitor<'_>) -> Result<(), LayoutError> {
        self.iter().try_for_each(|x| f(x.id, x.kind, &x.data))
    }
}

/// Extra objects kept in a file on local disk instead of in memory: only
/// their IDs, types and sizes stay in memory (37 bytes each).
#[derive(Debug)]
pub struct ExtraSpill {
    path: PathBuf,
    file: BufWriter<File>,
    index: Vec<(ObjectId, ObjectKind, u64)>,
    bytes: u64,
}

impl ExtraSpill {
    /// An empty spill in a new file at `path`.
    pub fn create(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let file = BufWriter::with_capacity(1 << 20, File::create(&path)?);
        Ok(Self {
            path,
            file,
            index: Vec::new(),
            bytes: 0,
        })
    }

    /// Appends one object.
    pub fn push(&mut self, id: ObjectId, kind: ObjectKind, data: &[u8]) -> std::io::Result<()> {
        self.file.write_all(data)?;
        self.index.push((id, kind, data.len() as u64));
        self.bytes += data.len() as u64;
        Ok(())
    }

    /// Objects appended.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Whether nothing was appended.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Total content bytes appended.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Flushes the file, so [`ExtraObjects::each`] can read it back.
    pub fn finish(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl ExtraObjects for ExtraSpill {
    fn count(&self) -> usize {
        self.index.len()
    }

    fn each(&self, f: &mut ExtraVisitor<'_>) -> Result<(), LayoutError> {
        if self.index.is_empty() {
            return Ok(());
        }
        let mut file = std::io::BufReader::with_capacity(1 << 20, File::open(&self.path)?);
        let mut buf = Vec::new();
        for &(id, kind, len) in &self.index {
            let len = usize::try_from(len)
                .map_err(|_| LayoutError::mismatch("spilled object too large"))?;
            buf.resize(len, 0);
            file.read_exact(&mut buf)?;
            f(id, kind, &buf)?;
        }
        Ok(())
    }
}

/// A pack written by [`write_subset`], on local disk, ready to upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubsetPack {
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
    /// Objects.
    pub object_count: u32,
    /// REF_DELTA entries.
    pub ref_delta_count: u32,
    /// Kept deltas that were stored undeltified because their base was not
    /// kept.
    pub undeltified: u32,
}

impl SubsetPack {
    /// The `packs` row for this pack, with `received_bytes` as given (the
    /// first write's pack of a push records what the client sent, the
    /// others 0; decisions log).
    pub fn new_pack(&self, repo: RepoId, received_bytes: u64) -> NewPack {
        NewPack {
            repo,
            checksum: self.checksum,
            size: self.size,
            idx_size: self.idx_size,
            entries_size: entry_table_len(self.object_count),
            object_count: self.object_count,
            ref_delta_count: self.ref_delta_count,
            origin: PackOrigin::Push { received_bytes },
        }
    }
}

/// The completed pack a subset is taken from.
#[derive(Debug, Clone, Copy)]
pub struct SubsetSource<'a> {
    /// The pack file.
    pub pack: &'a Path,
    /// Its `.idx`.
    pub idx: &'a Path,
    /// Its entry table.
    pub table: &'a EntryTable,
    /// One flag per entry of `table`, in the table's (offset) order: the
    /// entries to keep.
    pub keep: &'a [bool],
}

/// Writes the kept entries of `source` (none when there is no source),
/// followed by `extras`, to `out_dir`, named by the new checksum.
///
/// `max_object_bytes` bounds each object resolved to undeltify an entry.
/// Fails with [`LayoutError::Mismatch`] if nothing would be written, or if
/// an extra repeats a kept object or another extra.
pub fn write_subset<X: ExtraObjects + ?Sized>(
    source: Option<SubsetSource<'_>>,
    extras: &X,
    out_dir: &Path,
    max_object_bytes: MaxObjectBytes,
) -> Result<SubsetPack, LayoutError> {
    let empty_table;
    let (pack, idx, table, keep): (Option<&Path>, Option<&Path>, &EntryTable, &[bool]) =
        match source {
            Some(s) => (Some(s.pack), Some(s.idx), s.table, s.keep),
            None => {
                empty_table = EntryTable::new(
                    PackChecksum::from_bytes([0; 20]),
                    PACK_HEADER_LEN + PACK_TRAILER_LEN,
                    Vec::new(),
                )?;
                (None, None, &empty_table, &[])
            }
        };
    let entries = table.entries();
    if keep.len() != entries.len() {
        return Err(LayoutError::mismatch(format!(
            "{} keep flags for {} entries",
            keep.len(),
            entries.len()
        )));
    }
    let count = keep.iter().filter(|k| **k).count() + extras.count();
    if count == 0 {
        return Err(LayoutError::mismatch("a subset pack needs an object"));
    }
    let count = u32::try_from(count).map_err(|_| LayoutError::mismatch("too many objects"))?;
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

    // Opened lazily: only an undeltified entry needs delta resolution.
    let mut bundle: Option<gix_pack::Bundle> = None;
    let mut resolve = |id: &ObjectId| -> Result<(ObjectKind, Vec<u8>), LayoutError> {
        if bundle.is_none() {
            let idx = idx.ok_or_else(|| LayoutError::mismatch("no source pack"))?;
            let mut b = gix_pack::Bundle::at(idx, gix_hash::Kind::Sha1)
                .map_err(|e| LayoutError::mismatch(gix_chain(&e)))?;
            b.pack = b
                .pack
                .with_alloc_limit_bytes(Some(max_object_bytes.gix_alloc_limit(0)));
            bundle = Some(b);
        }
        let b = bundle.as_ref().expect("opened");
        let mut buf = Vec::new();
        let mut inflate = gix_zlib::Inflate::default();
        let (data, _) = b
            .find(
                &to_gix(id),
                &mut buf,
                &mut inflate,
                &mut gix_pack::cache::Never,
            )
            .map_err(|e| LayoutError::mismatch(gix_chain(&e)))?
            .ok_or_else(|| LayoutError::mismatch(format!("{id} is not in its own pack")))?;
        Ok((crate::ingest::core_kind(data.kind), data.data.to_vec()))
    };

    fs::create_dir_all(out_dir)?;
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = out_dir.join(format!(
        "subset-{}-{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let result = (|| {
        let mut src = match pack {
            Some(p) => Some(File::open(p)?),
            None => None,
        };
        let mut dst = HashingWriter::new(BufWriter::with_capacity(1 << 20, File::create(&tmp)?));
        let mut head = Vec::with_capacity(PACK_HEADER_LEN as usize);
        head.extend_from_slice(b"PACK");
        head.extend_from_slice(&2u32.to_be_bytes());
        head.extend_from_slice(&count.to_be_bytes());
        dst.write_all(&head)?;
        let mut new_offset: Vec<Option<u64>> = vec![None; entries.len()];
        let mut idx_entries = Vec::with_capacity(count as usize);
        let mut new_entries = Vec::with_capacity(count as usize);
        let mut undeltified = 0u32;
        let mut copy_buf = vec![0u8; 1 << 16];
        for i in (0..entries.len()).filter(|&i| keep[i]) {
            let e = &entries[i];
            let at = dst.written;
            new_offset[i] = Some(at);
            let base = base_of(i)?;
            let kept_base = base.and_then(|b| new_offset[b]);
            if base.is_some() && kept_base.is_none() {
                // The base is not in this subset: store the object whole.
                let (kind, data) = resolve(&e.id)?;
                let (head_len, crc) = write_full(&mut dst, kind, &data)?;
                undeltified += 1;
                idx_entries.push(IdxEntry {
                    id: e.id,
                    offset: at,
                    crc32: crc,
                });
                new_entries.push(PackEntry {
                    offset: at,
                    header_len: head_len,
                    id: e.id,
                    kind: entry_kind(kind),
                });
                continue;
            }
            let len = table.entry_len(i);
            let hl = usize::from(e.header_len);
            let mut raw = [0u8; MAX_HEADER_LEN];
            let src = src.as_mut().expect("entries come from a source pack");
            src.seek(SeekFrom::Start(e.offset))?;
            src.read_exact(&mut raw[..hl])?;
            let h = header::decode(&raw[..hl]).map_err(|source| LayoutError::Header {
                offset: e.offset,
                source,
            })?;
            let mut new_header = Vec::with_capacity(MAX_HEADER_LEN);
            let kind = match e.kind {
                EntryKind::OfsDelta { .. } => {
                    let base_at = kept_base.expect("a kept base");
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
        extras.each(&mut |id, kind, data| {
            let at = dst.written;
            let (head_len, crc) = write_full(&mut dst, kind, data)?;
            idx_entries.push(IdxEntry {
                id,
                offset: at,
                crc32: crc,
            });
            new_entries.push(PackEntry {
                offset: at,
                header_len: head_len,
                id,
                kind: entry_kind(kind),
            });
            Ok(())
        })?;
        let mut ids: Vec<ObjectId> = idx_entries.iter().map(|e| e.id).collect();
        ids.sort_unstable();
        if let Some(id) = find_repeated(&ids)
            .map_err(|e| LayoutError::mismatch(e.to_string()))?
            .first()
        {
            return Err(LayoutError::mismatch(format!(
                "object {id} would be stored twice"
            )));
        }
        let (mut w, sum, size) = dst.finish()?;
        w.write_all(&sum)?;
        w.flush()?;
        drop(w);
        let checksum = PackChecksum::from_bytes(sum);
        let idx_bytes = idx::write(idx_entries, &checksum);
        let table = EntryTable::new(checksum, size + PACK_TRAILER_LEN, new_entries)?;
        table.check_layout()?;
        let base = out_dir.join(checksum.to_hex());
        let paths = |ext| base.with_extension(ext);
        let (pack_path, idx_path, entries_path) = (paths("pack"), paths("idx"), paths("entries"));
        fs::rename(&tmp, &pack_path)?;
        fs::write(&idx_path, &idx_bytes)?;
        fs::write(&entries_path, table.encode())?;
        Ok(SubsetPack {
            pack_path,
            idx_path,
            entries_path,
            checksum,
            size: table.pack_size(),
            idx_size: idx_bytes.len() as u64,
            object_count: count,
            ref_delta_count: table.ref_delta_count(),
            undeltified,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

pub(crate) fn entry_kind(kind: ObjectKind) -> EntryKind {
    match kind {
        ObjectKind::Commit => EntryKind::Commit,
        ObjectKind::Tree => EntryKind::Tree,
        ObjectKind::Blob => EntryKind::Blob,
        ObjectKind::Tag => EntryKind::Tag,
    }
}

/// Writes one undeltified entry; returns its header length and CRC32.
pub(crate) fn write_full<W: Write>(
    dst: &mut W,
    kind: ObjectKind,
    data: &[u8],
) -> Result<(u8, u32), LayoutError> {
    let mut entry = Vec::with_capacity(MAX_HEADER_LEN + data.len() / 2 + 64);
    let type_code = match kind {
        ObjectKind::Commit => 1,
        ObjectKind::Tree => 2,
        ObjectKind::Blob => 3,
        ObjectKind::Tag => 4,
    };
    header::encode_type_and_size(type_code, data.len() as u64, &mut entry);
    let head_len = entry.len() as u8;
    let mut z = gix_zlib::stream::deflate::Write::new(&mut entry, gix_zlib::Compression::DEFAULT);
    z.write_all(data)?;
    z.flush()?;
    drop(z);
    let mut crc = crc32fast::Hasher::new();
    crc.update(&entry);
    dst.write_all(&entry)?;
    Ok((head_len, crc.finalize()))
}

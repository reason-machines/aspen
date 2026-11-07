//! A version 2 `.idx` too large to keep in memory, kept as a sample: its
//! fanout table and one object ID of every [`SAMPLE_EVERY`]. A lookup finds
//! the one stretch of [`SAMPLE_EVERY`] sorted IDs that can hold the object
//! in memory, then reads that stretch, and the object's offset, from the
//! blob store: two or three small range reads, however many objects the
//! pack holds.
//!
//! The whole file is still read once, when the sample is built, and checked
//! as strictly as [`crate::idx::IdxV2::parse`] checks a kept one (fanout,
//! ID order, offsets inside the pack, the large-offset table, and the
//! trailing checksum), so a lookup can trust what it reads. The checks run
//! as the sections stream in, so building one never holds the file.
//!
//! **Local copy** (node 12b). Given a directory on local disk
//! ([`SampleBuilder::copy_to`]), the builder also writes the sections a
//! lookup reads (IDs, offsets and large offsets, 24 bytes per object plus 8
//! per large offset) to an unnamed file there as they stream in, and a
//! lookup reads its window from that file ([`SampledIdx::lookup_local`]),
//! normally from the page cache: no blob store request, however many
//! objects a push looks up or how large the pack is. The copy is kept only
//! if the whole `.idx` checked, so it holds exactly the checked bytes; it
//! is as trusted as the spool file a push completes from. It is deleted
//! when the sample is dropped. Without one (no directory, or the file could
//! not be written), a lookup reads its window from the blob store: two or
//! three range reads, as before.
//!
//! The locator ([`crate::locate::StoredPacks`]) samples an `.idx` larger
//! than [`crate::locate::LocateOptions::max_whole_idx_bytes`]. Without
//! this, a repository whose `.idx` files do not fit the `.idx` cache read
//! them whole on every push: work that grows with the repository, not with
//! the push.

use std::fs::File;
use std::io::{BufWriter, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::Path;
use std::sync::Mutex;

use aspen_core::blob::ByteRange;
use aspen_core::id::{ObjectId, SHA1_LEN};
use aspen_core::pack::{PACK_HEADER_LEN, PACK_TRAILER_LEN, PackRecord};

use crate::Sha1;
use crate::idx::IdxError;

/// One ID of every this many is kept in memory; a lookup reads at most this
/// many IDs (20 bytes each) from the blob store.
pub const SAMPLE_EVERY: u64 = 256;

const MAGIC: [u8; 4] = [0xff, 0x74, 0x4f, 0x63];
const FANOUT_AT: u64 = 8;
const IDS_AT: u64 = FANOUT_AT + 256 * 4;
const LARGE_BIT: u32 = 1 << 31;

fn bad(what: impl Into<String>) -> IdxError {
    IdxError(what.into())
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

/// The sections of an `.idx`, in file order, as byte ranges the builder is
/// fed one after another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    /// IDs `[from, to)`.
    Ids(u64, u64),
    /// CRC32s `[from, to)`.
    Crcs(u64, u64),
    /// Offsets `[from, to)`.
    Offsets(u64, u64),
    /// Large offsets `[from, to)`.
    Large(u64, u64),
    /// The pack checksum and the index checksum.
    Trailer,
}

/// A sampled `.idx`, checked whole once.
#[derive(Debug)]
pub struct SampledIdx {
    pub(crate) record: PackRecord,
    count: u64,
    large_count: u64,
    fanout: Vec<u32>,
    /// The ID at every position `k * SAMPLE_EVERY`.
    samples: Vec<ObjectId>,
    /// The lookup sections on local disk (module docs), if kept.
    local: Option<LocalSections>,
}

/// The sections of an `.idx` a lookup reads, on local disk: its IDs, then
/// its offsets and large offsets (the `.idx` without its header, CRCs and
/// trailer).
#[derive(Debug)]
struct LocalSections {
    file: Mutex<File>,
}

/// Builds a [`SampledIdx`] from an `.idx` fed in file order.
#[derive(Debug)]
pub struct SampleBuilder {
    record: PackRecord,
    hasher: Sha1,
    count: u64,
    large_count: u64,
    fanout: Vec<u32>,
    samples: Vec<ObjectId>,
    prev: Option<[u8; SHA1_LEN]>,
    per_byte: Vec<u32>,
    next_large: u64,
    /// Where the next section must start.
    expect: Section,
    /// The local copy being written ([`SampleBuilder::copy_to`]); dropped,
    /// and the sample kept without one, if a write fails.
    local: Option<BufWriter<File>>,
}

impl SampleBuilder {
    /// Starts from the header and fanout table (the first 1,032 bytes) of
    /// the `.idx` of `record`, whose size the row states.
    pub fn new(record: PackRecord, head: &[u8]) -> Result<Self, IdxError> {
        if head.len() as u64 != IDS_AT {
            return Err(bad("short header"));
        }
        if head[..4] != MAGIC || u32_at(head, 4) != 2 {
            return Err(bad("not a version 2 index"));
        }
        let mut fanout = Vec::with_capacity(256);
        let mut prev = 0;
        for i in 0..256 {
            let f = u32_at(head, (FANOUT_AT as usize) + 4 * i);
            if f < prev {
                return Err(bad("fanout table is not monotone"));
            }
            prev = f;
            fanout.push(f);
        }
        let count = u64::from(prev);
        if count != u64::from(record.pack.object_count) {
            return Err(bad("object count does not match the pack row"));
        }
        let min = IDS_AT + 28 * count + 2 * SHA1_LEN as u64;
        let size = record.pack.idx_size;
        if size < min || !(size - min).is_multiple_of(8) {
            return Err(bad("length does not fit the object count"));
        }
        let mut hasher = Sha1::new();
        hasher.update(head);
        Ok(Self {
            record,
            hasher,
            count,
            large_count: (size - min) / 8,
            fanout,
            samples: Vec::with_capacity((count / SAMPLE_EVERY + 1) as usize),
            prev: None,
            per_byte: vec![0; 256],
            next_large: 0,
            expect: if count == 0 {
                Section::Large(0, 0)
            } else {
                Section::Ids(0, 0)
            },
            local: None,
        })
    }

    /// Also copies the sections a lookup reads to an unnamed file in `dir`
    /// (created if missing), so lookups in the finished sample read local
    /// disk instead of the blob store (module docs). If the file cannot be
    /// made or written, the sample is built without a copy.
    #[must_use]
    pub fn copy_to(mut self, dir: &Path) -> Self {
        self.local = std::fs::create_dir_all(dir)
            .and_then(|()| tempfile::tempfile_in(dir))
            .ok()
            .map(|f| BufWriter::with_capacity(1 << 16, f));
        self
    }

    /// Appends `bytes` to the local copy, dropping the copy if that fails.
    fn copy(&mut self, bytes: &[u8]) {
        if let Some(w) = &mut self.local
            && w.write_all(bytes).is_err()
        {
            self.local = None;
        }
    }

    /// The sections to read, in order, each at most `per_read` entries,
    /// with the byte range each covers.
    pub fn sections(&self, per_read: u64) -> Vec<(Section, ByteRange)> {
        let n = self.count;
        let per_read = per_read.max(1);
        let mut out = Vec::new();
        let mut split = |make: fn(u64, u64) -> Section, total: u64, at: u64, width: u64| {
            let mut from = 0;
            while from < total {
                let to = (from + per_read).min(total);
                let r = ByteRange::new(at + width * from, at + width * to).expect("non-empty");
                out.push((make(from, to), r));
                from = to;
            }
        };
        split(Section::Ids, n, IDS_AT, SHA1_LEN as u64);
        split(Section::Crcs, n, IDS_AT + 20 * n, 4);
        split(Section::Offsets, n, IDS_AT + 24 * n, 4);
        split(Section::Large, self.large_count, IDS_AT + 28 * n, 8);
        let size = self.record.pack.idx_size;
        out.push((
            Section::Trailer,
            ByteRange::new(size - 2 * SHA1_LEN as u64, size).expect("40 bytes"),
        ));
        out
    }

    /// Feeds one section, as [`SampleBuilder::sections`] listed it, in
    /// order.
    pub fn feed(&mut self, section: Section, bytes: &[u8]) -> Result<(), IdxError> {
        let in_order = match (self.expect, section) {
            (Section::Ids(_, e), Section::Ids(f, _))
            | (Section::Crcs(_, e), Section::Crcs(f, _))
            | (Section::Offsets(_, e), Section::Offsets(f, _))
            | (Section::Large(_, e), Section::Large(f, _)) => f == e,
            (Section::Ids(_, e), Section::Crcs(0, _))
            | (Section::Crcs(_, e), Section::Offsets(0, _)) => e == self.count,
            (Section::Offsets(_, e), Section::Large(0, _)) => e == self.count,
            (Section::Offsets(_, e), Section::Trailer) => e == self.count && self.large_count == 0,
            (Section::Large(_, e), Section::Trailer) => e == self.large_count,
            _ => false,
        };
        if !in_order {
            return Err(bad(format!("section {section:?} out of order")));
        }
        let width = match section {
            Section::Ids(..) => SHA1_LEN as u64,
            Section::Crcs(..) | Section::Offsets(..) => 4,
            Section::Large(..) => 8,
            Section::Trailer => 2 * SHA1_LEN as u64,
        };
        let entries = match section {
            Section::Ids(f, t) | Section::Crcs(f, t) | Section::Offsets(f, t) => t - f,
            Section::Large(f, t) => t - f,
            Section::Trailer => 1,
        };
        if bytes.len() as u64 != width * entries {
            return Err(bad("short read of a section"));
        }
        let data_end = self.record.pack.size.saturating_sub(PACK_TRAILER_LEN);
        let in_pack = |off: u64| off >= PACK_HEADER_LEN && off < data_end;
        match section {
            Section::Ids(from, _) => {
                for (k, id) in bytes.as_chunks::<SHA1_LEN>().0.iter().enumerate() {
                    let i = from + k as u64;
                    let id = *id;
                    if self.prev.is_some_and(|p| p >= id) {
                        return Err(bad(format!("ids are not strictly increasing at {i}")));
                    }
                    self.prev = Some(id);
                    self.per_byte[usize::from(id[0])] += 1;
                    if i.is_multiple_of(SAMPLE_EVERY) {
                        self.samples.push(ObjectId::from_bytes(id));
                    }
                }
            }
            Section::Crcs(..) => {}
            Section::Offsets(..) => {
                for raw in bytes.as_chunks::<4>().0 {
                    let raw = u32::from_be_bytes(*raw);
                    if raw & LARGE_BIT != 0 {
                        if u64::from(raw & !LARGE_BIT) != self.next_large
                            || self.next_large >= self.large_count
                        {
                            return Err(bad("large offset table is not used in order"));
                        }
                        self.next_large += 1;
                    } else if !in_pack(u64::from(raw)) {
                        return Err(bad(format!("offset {raw} is outside the pack's entries")));
                    }
                }
            }
            Section::Large(..) => {
                for raw in bytes.as_chunks::<8>().0 {
                    let off = u64::from_be_bytes(*raw);
                    if off < u64::from(LARGE_BIT) {
                        return Err(bad("small offset stored in the large offset table"));
                    }
                    if !in_pack(off) {
                        return Err(bad(format!("offset {off} is outside the pack's entries")));
                    }
                }
            }
            Section::Trailer => {
                if self.next_large != self.large_count {
                    return Err(bad("unused large offsets"));
                }
                let mut want = 0u32;
                for (i, n) in self.per_byte.iter().enumerate() {
                    want += n;
                    if self.fanout[i] != want {
                        return Err(bad("fanout table does not match the ids"));
                    }
                }
                if bytes[..SHA1_LEN] != self.record.pack.checksum.as_bytes()[..] {
                    return Err(bad(".idx does not describe this pack"));
                }
                self.hasher.update(&bytes[..SHA1_LEN]);
                let digest = std::mem::replace(&mut self.hasher, Sha1::new())
                    .finish()
                    .unwrap_or([0; SHA1_LEN]);
                if digest[..] != bytes[SHA1_LEN..] {
                    return Err(bad("index checksum does not match"));
                }
                self.expect = Section::Trailer;
                return Ok(());
            }
        }
        if !matches!(section, Section::Crcs(..)) {
            self.copy(bytes);
        }
        self.hasher.update(bytes);
        self.expect = section;
        Ok(())
    }

    /// The sample, once the trailer has been fed.
    pub fn finish(self) -> Result<SampledIdx, IdxError> {
        if self.expect != Section::Trailer {
            return Err(bad("the index ended early"));
        }
        // The trailer's checksum covered every byte copied.
        let local = self
            .local
            .and_then(|w| w.into_inner().ok())
            .map(|file| LocalSections {
                file: Mutex::new(file),
            });
        Ok(SampledIdx {
            record: self.record,
            count: self.count,
            large_count: self.large_count,
            fanout: self.fanout,
            samples: self.samples,
            local,
        })
    }
}

impl SampledIdx {
    /// The range of an `.idx` holding its header and fanout table, the
    /// first read of [`SampleBuilder::new`].
    pub fn head_range() -> ByteRange {
        ByteRange::new(0, IDS_AT).expect("non-empty")
    }

    /// Bytes this sample holds in memory.
    pub fn cost(&self) -> u64 {
        (self.samples.len() * SHA1_LEN + self.fanout.len() * 4) as u64 + 64
    }

    /// Whether the lookup sections are on local disk (module docs).
    pub fn is_local(&self) -> bool {
        self.local.is_some()
    }

    /// The offset of `id` (`Some(None)` when the pack lacks it), answered
    /// from the local copy with no blob store read: [`Self::window`],
    /// [`Self::find`] and the offset, as a lookup over the blob store does.
    /// `None` when there is no local copy or a local read fails; the caller
    /// then reads the blob store.
    pub fn lookup_local(&self, id: &ObjectId) -> Option<Option<u64>> {
        self.local.as_ref()?;
        let Some((from, _, range)) = self.window(id) else {
            return Some(None);
        };
        let Some(i) = Self::find(from, &self.read_local(range)?, id) else {
            return Some(None);
        };
        let raw = self.read_local(self.offset_range(i))?;
        Some(Some(match self.small_offset(&raw) {
            Ok(offset) => offset,
            Err(large) => Self::large_offset(&self.read_local(large)?),
        }))
    }

    /// The bytes of `range` of the `.idx`, from its local copy: `None`
    /// when there is none, the range is not inside the IDs or the offsets
    /// and large offsets, or the local read fails (the caller then reads
    /// the blob store).
    fn read_local(&self, range: ByteRange) -> Option<Vec<u8>> {
        let local = self.local.as_ref()?;
        let n = self.count;
        let ids = IDS_AT..IDS_AT + 20 * n;
        let offsets = IDS_AT + 24 * n..IDS_AT + 28 * n + 8 * self.large_count;
        // Where the range starts in the copy, which leaves out the 4 bytes
        // of CRC per object between the IDs and the offsets.
        let at = if ids.start <= range.start() && range.end() <= ids.end {
            range.start() - IDS_AT
        } else if offsets.start <= range.start() && range.end() <= offsets.end {
            range.start() - IDS_AT - 4 * n
        } else {
            return None;
        };
        let mut bytes = vec![0; usize::try_from(range.len()).ok()?];
        let mut file = local.file.lock().expect("not poisoned");
        file.seek(SeekFrom::Start(at)).ok()?;
        file.read_exact(&mut bytes).ok()?;
        Some(bytes)
    }

    /// The positions `[from, to)` that can hold `id`, and the byte range of
    /// their IDs in the `.idx`; `None` when no position can.
    pub fn window(&self, id: &ObjectId) -> Option<(u64, u64, ByteRange)> {
        let first = usize::from(id.as_bytes()[0]);
        let lo = if first == 0 {
            0
        } else {
            u64::from(self.fanout[first - 1])
        };
        let hi = u64::from(self.fanout[first]);
        let k = self.samples.partition_point(|s| s <= id);
        if lo >= hi || k == 0 {
            return None;
        }
        let page = (k as u64 - 1) * SAMPLE_EVERY;
        let from = page.max(lo);
        let to = (page + SAMPLE_EVERY).min(hi).min(self.count);
        (from < to).then(|| {
            let r = ByteRange::new(IDS_AT + 20 * from, IDS_AT + 20 * to).expect("non-empty");
            (from, to, r)
        })
    }

    /// The position of `id` among the IDs `[from, ...)` read for
    /// [`SampledIdx::window`].
    pub fn find(from: u64, ids: &[u8], id: &ObjectId) -> Option<u64> {
        let n = ids.len() / SHA1_LEN;
        let (mut lo, mut hi) = (0, n);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match ids[SHA1_LEN * mid..SHA1_LEN * (mid + 1)].cmp(id.as_bytes().as_slice()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(from + mid as u64),
            }
        }
        None
    }

    /// The byte range of the 4-byte offset of position `i`.
    pub fn offset_range(&self, i: u64) -> ByteRange {
        let at = IDS_AT + 24 * self.count + 4 * i;
        ByteRange::new(at, at + 4).expect("non-empty")
    }

    /// The byte range of the 4-byte offsets of positions `[from, to)`: the
    /// offsets of a whole [`SampledIdx::window`], read beside its IDs so a
    /// lookup takes one round trip, not two.
    pub fn offsets_range(&self, from: u64, to: u64) -> ByteRange {
        let at = IDS_AT + 24 * self.count;
        ByteRange::new(at + 4 * from, at + 4 * to).expect("non-empty")
    }

    /// The offset a 4-byte offset entry gives, or the byte range of the
    /// large offset it points at.
    pub fn small_offset(&self, raw: &[u8]) -> Result<u64, ByteRange> {
        let raw = u32_at(raw, 0);
        if raw & LARGE_BIT == 0 {
            return Ok(u64::from(raw));
        }
        let j = u64::from(raw & !LARGE_BIT).min(self.large_count.saturating_sub(1));
        let at = IDS_AT + 28 * self.count + 8 * j;
        Err(ByteRange::new(at, at + 8).expect("non-empty"))
    }

    /// A large offset read from the range [`SampledIdx::small_offset`] gave.
    pub fn large_offset(raw: &[u8]) -> u64 {
        u64::from_be_bytes(raw[..8].try_into().expect("8 bytes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::idx::{IdxEntry, IdxV2, write};
    use aspen_core::id::{PackChecksum, PackId, RepoId, UnixMicros};
    use aspen_core::pack::{NewPack, PackGeneration, PackOrigin};

    fn record(idx: &[u8], count: u32, pack_size: u64, checksum: PackChecksum) -> PackRecord {
        PackRecord {
            id: PackId::new(1).unwrap(),
            generation: PackGeneration::FIRST,
            created_at: UnixMicros(0),
            pack: NewPack {
                repo: RepoId::new(1).unwrap(),
                checksum,
                size: pack_size,
                idx_size: idx.len() as u64,
                entries_size: 44 + 52 * u64::from(count),
                object_count: count,
                ref_delta_count: 0,
                origin: PackOrigin::Push { received_bytes: 0 },
            },
        }
    }

    fn sample(idx: &[u8], rec: PackRecord, per_read: u64) -> Result<SampledIdx, IdxError> {
        let mut b = SampleBuilder::new(rec, &idx[..IDS_AT as usize])?;
        for (s, r) in b.sections(per_read) {
            b.feed(s, &idx[r.start() as usize..r.end() as usize])?;
        }
        b.finish()
    }

    fn entries(n: u64, big: bool) -> Vec<IdxEntry> {
        (0..n)
            .map(|i| {
                let id = crate::sha1(&i.to_be_bytes());
                let offset = if big && i % 3 == 0 {
                    (3u64 << 31) + i
                } else {
                    PACK_HEADER_LEN + i * 10
                };
                IdxEntry {
                    id,
                    crc32: i as u32,
                    offset,
                }
            })
            .collect()
    }

    /// Invariant: a sampled lookup finds exactly what a whole parse finds,
    /// at every position and for IDs that are absent, with small and large
    /// offsets, whatever the read size.
    #[test]
    fn lookups_agree_with_the_whole_index() {
        for (n, big) in [(0u64, false), (1, false), (255, false), (2_000, true)] {
            let checksum = PackChecksum::from_bytes([7; 20]);
            let idx = write(entries(n, big), &checksum);
            let pack_size = 1 << 40;
            let whole = IdxV2::parse(&idx, Some(pack_size)).unwrap();
            for per_read in [1, 7, 4096] {
                let s =
                    sample(&idx, record(&idx, n as u32, pack_size, checksum), per_read).unwrap();
                let read = |r: ByteRange| &idx[r.start() as usize..r.end() as usize];
                for i in 0..n as usize {
                    let id = whole.id(i);
                    let (from, _, r) = s.window(&id).expect("a window holds it");
                    let at = SampledIdx::find(from, read(r), &id).expect("found");
                    assert_eq!(at, i as u64);
                    let off = match s.small_offset(read(s.offset_range(at))) {
                        Ok(o) => o,
                        Err(r) => SampledIdx::large_offset(read(r)),
                    };
                    assert_eq!(off, whole.offset(i));
                }
                for k in 0..200u64 {
                    let id = crate::sha1(&(1_000_000 + k).to_be_bytes());
                    let found = s
                        .window(&id)
                        .and_then(|(from, _, r)| SampledIdx::find(from, read(r), &id));
                    assert_eq!(found.map(|f| f as usize), whole.lookup(&id));
                }
            }
        }
    }

    /// Invariant (12b): a sample with a local copy answers every lookup
    /// from it exactly as a whole parse does (present IDs with small and
    /// large offsets, absent IDs), and the copy goes when the sample does.
    #[test]
    fn local_lookups_agree_with_the_whole_index() {
        let dir = tempfile::tempdir().unwrap();
        for (n, big) in [(0u64, false), (1, false), (255, false), (2_000, true)] {
            let checksum = PackChecksum::from_bytes([7; 20]);
            let idx = write(entries(n, big), &checksum);
            let pack_size = 1 << 40;
            let whole = IdxV2::parse(&idx, Some(pack_size)).unwrap();
            let rec = record(&idx, n as u32, pack_size, checksum);
            for per_read in [1, 7, 4096] {
                let mut b = SampleBuilder::new(rec.clone(), &idx[..IDS_AT as usize])
                    .unwrap()
                    .copy_to(dir.path());
                for (s, r) in b.sections(per_read) {
                    b.feed(s, &idx[r.start() as usize..r.end() as usize])
                        .unwrap();
                }
                let s = b.finish().unwrap();
                assert!(s.is_local());
                for i in 0..n as usize {
                    let id = whole.id(i);
                    assert_eq!(s.lookup_local(&id), Some(Some(whole.offset(i))));
                }
                for k in 0..200u64 {
                    let id = crate::sha1(&(1_000_000 + k).to_be_bytes());
                    let want = whole.lookup(&id).map(|i| whole.offset(i));
                    assert_eq!(s.lookup_local(&id), Some(want));
                }
                // Without a copy, nothing is answered locally.
                let plain = sample(&idx, rec.clone(), per_read).unwrap();
                assert!(!plain.is_local());
                if n > 0 {
                    assert_eq!(plain.lookup_local(&whole.id(0)), None);
                }
            }
        }
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "local copies are unnamed files, gone with their samples"
        );
    }

    /// Invariant: building a sample refuses everything a whole parse
    /// refuses: a flipped byte anywhere, a row that names another pack or
    /// another count.
    #[test]
    fn a_damaged_index_is_refused() {
        let checksum = PackChecksum::from_bytes([7; 20]);
        let n = 600;
        let idx = write(entries(n, true), &checksum);
        let pack_size = 1 << 40;
        let rec = record(&idx, n as u32, pack_size, checksum);
        assert!(sample(&idx, rec.clone(), 64).is_ok());
        for at in (0..idx.len()).step_by(37) {
            let mut bad = idx.clone();
            bad[at] ^= 0x40;
            assert!(
                sample(&bad, rec.clone(), 64).is_err(),
                "a flipped byte at {at} was accepted"
            );
        }
        let mut other = rec.clone();
        other.pack.checksum = PackChecksum::from_bytes([8; 20]);
        assert!(sample(&idx, other, 64).is_err());
        let mut fewer = rec.clone();
        fewer.pack.object_count -= 1;
        assert!(sample(&idx, fewer, 64).is_err());
        let mut small = rec;
        small.pack.size = 1 << 20;
        assert!(sample(&idx, small, 64).is_err(), "offsets past the pack");
    }
}

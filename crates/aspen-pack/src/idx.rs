//! Git pack index files, version 2: parsing (with full validation) and
//! writing.
//!
//! ```text
//! magic ff 74 4f 63 | version 2
//! fanout       256 × u32   objects whose first byte is <= i
//! ids          n × [20]    sorted ascending
//! crc32        n × u32     of each entry's bytes in the pack
//! offsets      n × u32     high bit set: index into the large-offset table
//! large        m × u64     offsets of 2 GiB and more
//! pack checksum [20]
//! idx checksum  [20]       SHA-1 of everything before it
//! ```
//!
//! The `.idx` is how Aspen finds objects in stored packs (base lookup,
//! connectivity, the stitcher's duplicate merge), so the parser checks every
//! structural rule: fanout monotone and consistent with the IDs, IDs strictly
//! increasing, offsets inside the pack, large offsets used exactly when
//! needed, and the trailing checksum.

use aspen_core::id::{ObjectId, PackChecksum, SHA1_LEN};
use aspen_core::pack::{IDX_V2_FIXED_LEN, PACK_HEADER_LEN, PACK_TRAILER_LEN};

use crate::sha1;

const MAGIC: [u8; 4] = [0xff, 0x74, 0x4f, 0x63];
const VERSION: u32 = 2;
const FANOUT_AT: usize = 8;
const IDS_AT: usize = FANOUT_AT + 256 * 4;
const LARGE_BIT: u32 = 1 << 31;

/// Why bytes are not a valid version 2 `.idx`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid .idx: {0}")]
pub struct IdxError(pub String);

fn bad(what: impl Into<String>) -> IdxError {
    IdxError(what.into())
}

/// A parsed, validated version 2 `.idx`, borrowing its bytes.
#[derive(Debug, Clone, Copy)]
pub struct IdxV2<'a> {
    bytes: &'a [u8],
    count: usize,
    large_at: usize,
}

impl<'a> IdxV2<'a> {
    /// Parses and validates `bytes`. `pack_size`, when known (from the
    /// `packs` row or the pack file), bounds every offset.
    pub fn parse(bytes: &'a [u8], pack_size: Option<u64>) -> Result<Self, IdxError> {
        Self::parse_with(bytes, pack_size, false)
    }

    /// Like [`IdxV2::parse`], but accepts an ID listed more than once (IDs
    /// must still never decrease). gix writes such an `.idx` for a pack that
    /// holds an object twice; ingest parses it this way only to find the
    /// repeats ([`aspen_core::pack::find_repeated`]).
    pub fn parse_allowing_repeats(
        bytes: &'a [u8],
        pack_size: Option<u64>,
    ) -> Result<Self, IdxError> {
        Self::parse_with(bytes, pack_size, true)
    }

    fn parse_with(
        bytes: &'a [u8],
        pack_size: Option<u64>,
        allow_repeats: bool,
    ) -> Result<Self, IdxError> {
        if (bytes.len() as u64) < IDX_V2_FIXED_LEN {
            return Err(bad("shorter than the fixed part"));
        }
        if bytes[..4] != MAGIC || u32_at(bytes, 4) != VERSION {
            return Err(bad("not a version 2 index"));
        }
        let mut prev = 0;
        for i in 0..256 {
            let f = u32_at(bytes, FANOUT_AT + 4 * i);
            if f < prev {
                return Err(bad("fanout table is not monotone"));
            }
            prev = f;
        }
        let count = prev as usize;
        let crc_at = IDS_AT + SHA1_LEN * count;
        let offsets_at = crc_at + 4 * count;
        let large_at = offsets_at + 4 * count;
        let min = large_at + 2 * SHA1_LEN;
        if bytes.len() < min || !(bytes.len() - min).is_multiple_of(8) {
            return Err(bad("length does not fit the object count"));
        }
        let large_count = (bytes.len() - min) / 8;
        let body_end = bytes.len() - SHA1_LEN;
        if sha1(&bytes[..body_end]).as_bytes() != &bytes[body_end..] {
            return Err(bad("index checksum does not match"));
        }
        let idx = Self {
            bytes,
            count,
            large_at,
        };
        // IDs strictly increasing and consistent with the fanout.
        let mut per_byte = [0u32; 256];
        for i in 0..count {
            let id = idx.id_bytes(i);
            let ordered = i == 0 || {
                let prev = idx.id_bytes(i - 1);
                prev < id || (allow_repeats && prev == id)
            };
            if !ordered {
                return Err(bad(format!("ids are not strictly increasing at {i}")));
            }
            per_byte[usize::from(id[0])] += 1;
        }
        let mut want = 0u32;
        for (i, n) in per_byte.iter().enumerate() {
            want += n;
            if u32_at(bytes, FANOUT_AT + 4 * i) != want {
                return Err(bad("fanout table does not match the ids"));
            }
        }
        // Offsets: large ones only through the large table, each large entry
        // used once, in order, and only for offsets that need it (as Git
        // writes them).
        let data_end = pack_size.map(|s| s.saturating_sub(PACK_TRAILER_LEN));
        let mut next_large = 0usize;
        for i in 0..count {
            let raw = u32_at(bytes, offsets_at + 4 * i);
            let off = if raw & LARGE_BIT != 0 {
                let j = (raw & !LARGE_BIT) as usize;
                if j != next_large || j >= large_count {
                    return Err(bad("large offset table is not used in order"));
                }
                next_large += 1;
                let off = u64_at(bytes, large_at + 8 * j);
                if off < u64::from(LARGE_BIT) {
                    return Err(bad("small offset stored in the large offset table"));
                }
                off
            } else {
                u64::from(raw)
            };
            if off < PACK_HEADER_LEN || data_end.is_some_and(|end| off >= end) {
                return Err(bad(format!("offset {off} is outside the pack's entries")));
            }
        }
        if next_large != large_count {
            return Err(bad("unused large offsets"));
        }
        Ok(idx)
    }

    /// A view of bytes that [`IdxV2::parse`] already validated (kept by a
    /// cache): reads the object count from the fanout and nothing else.
    pub(crate) fn parse_trusted(bytes: &'a [u8]) -> Self {
        let count = u32_at(bytes, FANOUT_AT + 4 * 255) as usize;
        Self {
            bytes,
            count,
            large_at: IDS_AT + (SHA1_LEN + 8) * count,
        }
    }

    /// Number of objects.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the index lists no object.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn id_bytes(&self, i: usize) -> &'a [u8] {
        &self.bytes[IDS_AT + SHA1_LEN * i..IDS_AT + SHA1_LEN * (i + 1)]
    }

    /// The `i`-th ID in sorted order.
    pub fn id(&self, i: usize) -> ObjectId {
        ObjectId::from_slice(self.id_bytes(i)).expect("20 bytes")
    }

    /// CRC32 of the `i`-th object's entry.
    pub fn crc32(&self, i: usize) -> u32 {
        u32_at(self.bytes, IDS_AT + SHA1_LEN * self.count + 4 * i)
    }

    /// Pack offset of the `i`-th object's entry.
    pub fn offset(&self, i: usize) -> u64 {
        let raw = u32_at(self.bytes, IDS_AT + (SHA1_LEN + 4) * self.count + 4 * i);
        if raw & LARGE_BIT != 0 {
            u64_at(self.bytes, self.large_at + 8 * (raw & !LARGE_BIT) as usize)
        } else {
            u64::from(raw)
        }
    }

    /// The sorted position of `id`, by fanout and binary search (any one of
    /// its positions if it repeats).
    pub fn lookup(&self, id: &ObjectId) -> Option<usize> {
        let first = id.as_bytes()[0] as usize;
        let lo = if first == 0 {
            0
        } else {
            u32_at(self.bytes, FANOUT_AT + 4 * (first - 1)) as usize
        };
        let hi = u32_at(self.bytes, FANOUT_AT + 4 * first) as usize;
        let (mut lo, mut hi) = (lo, hi);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.id_bytes(mid).cmp(id.as_bytes().as_slice()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Every ID, ascending.
    pub fn ids(&self) -> impl ExactSizeIterator<Item = ObjectId> + 'a {
        let idx = *self;
        (0..self.count).map(move |i| idx.id(i))
    }

    /// `(id, offset)` for every object, ascending by ID: the input
    /// [`crate::stitch::StitchPlan::new`] takes.
    pub fn ids_with_offsets(&self) -> impl ExactSizeIterator<Item = (ObjectId, u64)> + 'a {
        let idx = *self;
        (0..self.count).map(move |i| (idx.id(i), idx.offset(i)))
    }

    /// `(offset, id, crc32)` for every object, in pack (offset) order.
    pub fn by_offset(&self) -> Vec<(u64, ObjectId, u32)> {
        let mut v: Vec<_> = (0..self.count)
            .map(|i| (self.offset(i), self.id(i), self.crc32(i)))
            .collect();
        v.sort_unstable_by_key(|e| e.0);
        v
    }

    /// The checksum of the pack this index describes.
    pub fn pack_checksum(&self) -> PackChecksum {
        let at = self.bytes.len() - 2 * SHA1_LEN;
        PackChecksum::from_slice(&self.bytes[at..at + SHA1_LEN]).expect("20 bytes")
    }
}

/// One object for [`write()`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdxEntry {
    /// The object.
    pub id: ObjectId,
    /// Offset of its entry in the pack.
    pub offset: u64,
    /// CRC32 of its entry's bytes (header and compressed data).
    pub crc32: u32,
}

/// Writes a version 2 `.idx`, byte-identical to what `git index-pack`
/// writes for the same pack. `entries` may be in any order; IDs must not
/// repeat (the caller's pack is already checked for that).
pub fn write(mut entries: Vec<IdxEntry>, pack: &PackChecksum) -> Vec<u8> {
    entries.sort_unstable_by_key(|e| e.id);
    debug_assert!(entries.windows(2).all(|w| w[0].id < w[1].id));
    let n = entries.len();
    let mut out = Vec::with_capacity(IDX_V2_FIXED_LEN as usize + 28 * n);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION.to_be_bytes());
    let mut fanout = [0u32; 256];
    for e in &entries {
        fanout[usize::from(e.id.as_bytes()[0])] += 1;
    }
    let mut acc = 0u32;
    for f in fanout {
        acc += f;
        out.extend_from_slice(&acc.to_be_bytes());
    }
    for e in &entries {
        out.extend_from_slice(e.id.as_bytes());
    }
    for e in &entries {
        out.extend_from_slice(&e.crc32.to_be_bytes());
    }
    let mut large = Vec::new();
    for e in &entries {
        if e.offset < u64::from(LARGE_BIT) {
            out.extend_from_slice(&(e.offset as u32).to_be_bytes());
        } else {
            out.extend_from_slice(&(LARGE_BIT | large.len() as u32).to_be_bytes());
            large.push(e.offset);
        }
    }
    for off in large {
        out.extend_from_slice(&off.to_be_bytes());
    }
    out.extend_from_slice(pack.as_bytes());
    let sum = sha1(&out);
    out.extend_from_slice(sum.as_bytes());
    out
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(first: u8, rest: u8) -> ObjectId {
        let mut b = [rest; 20];
        b[0] = first;
        ObjectId::from_bytes(b)
    }

    fn sample() -> Vec<u8> {
        write(
            vec![
                IdxEntry {
                    id: oid(0xaa, 1),
                    offset: 12,
                    crc32: 7,
                },
                IdxEntry {
                    id: oid(0x01, 2),
                    offset: 40,
                    crc32: 8,
                },
                IdxEntry {
                    id: oid(0xaa, 0),
                    offset: 3 << 31,
                    crc32: 9,
                },
            ],
            &PackChecksum::from_bytes([5; 20]),
        )
    }

    #[test]
    fn written_indexes_parse_and_answer_lookups() {
        let bytes = sample();
        let idx = IdxV2::parse(&bytes, None).unwrap();
        assert_eq!(idx.len(), 3);
        assert_eq!(
            idx.ids().collect::<Vec<_>>(),
            vec![oid(0x01, 2), oid(0xaa, 0), oid(0xaa, 1)]
        );
        let i = idx.lookup(&oid(0xaa, 0)).unwrap();
        assert_eq!((idx.offset(i), idx.crc32(i)), (3 << 31, 9));
        assert_eq!(idx.lookup(&oid(0xaa, 2)), None);
        assert_eq!(idx.lookup(&oid(0x00, 0)), None);
        assert_eq!(idx.pack_checksum(), PackChecksum::from_bytes([5; 20]));
        assert_eq!(idx.by_offset()[0].0, 12);
        assert!(aspen_core::pack::idx_v2_size_fits(3, bytes.len() as u64));
    }

    #[test]
    fn corrupt_indexes_are_rejected() {
        let good = sample();
        // Any flipped byte breaks the checksum.
        for at in [0, 9, IDS_AT, good.len() - 1] {
            let mut b = good.clone();
            b[at] ^= 1;
            assert!(IdxV2::parse(&b, None).is_err(), "byte {at}");
        }
        assert!(IdxV2::parse(&good[..good.len() - 8], None).is_err());
        // Offsets past the pack's entries.
        assert!(IdxV2::parse(&good, Some(100)).is_err());
        // A structurally wrong index with a valid checksum: ids out of order.
        let mut swapped = good[..good.len() - 20].to_vec();
        let (a, b) = (IDS_AT + 20, IDS_AT + 40);
        let tmp = swapped[a..a + 20].to_vec();
        swapped.copy_within(b..b + 20, a);
        swapped[b..b + 20].copy_from_slice(&tmp);
        let sum = sha1(&swapped);
        swapped.extend_from_slice(sum.as_bytes());
        let err = IdxV2::parse(&swapped, None).unwrap_err();
        assert!(err.0.contains("strictly increasing"), "{err}");
    }

    #[test]
    fn empty_index_is_valid() {
        let bytes = write(vec![], &PackChecksum::from_bytes([0; 20]));
        assert_eq!(bytes.len() as u64, IDX_V2_FIXED_LEN);
        assert!(IdxV2::parse(&bytes, Some(32)).unwrap().is_empty());
    }
}

//! Stored packs (`packs` table), their blob keys, and the per-entry table
//! written beside each pack.
//!
//! Every stored pack satisfies three invariants, which ingest and compaction
//! establish and every reader may rely on without checking a flag:
//!
//! - **Self-contained**: every delta's base is in the same pack (thin pushes
//!   are completed at ingest, S3).
//! - **Base-first**: every delta's base entry comes before the delta (S1).
//! - **No object twice**: each object ID appears at most once (S4).
//!
//! With all three, the stitcher deduplicates any sequence of stored packs in
//! one streaming pass (`docs/PLAN.md`, fetch path step 4).

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use crate::blob::BlobKey;
use crate::error::{StoreError, UnpackError};
use crate::id::{DB_MAX, ObjectId, PackChecksum, PackId, RepoId, SHA1_LEN, UnixMicros};

/// Size of a pack header: `PACK`, version, object count.
pub const PACK_HEADER_LEN: u64 = 12;

/// Size of a pack's trailing SHA-1.
pub const PACK_TRAILER_LEN: u64 = SHA1_LEN as u64;

/// The longest any packfile URL Aspen hands out stays valid: a presigned
/// bucket URL (`aspen_blob::RECOMMENDED_MAX_PRESIGN_TTL`, which the fetch
/// path clamps every presign to) and a pack express URL, `ttl + window`
/// (`aspen_express::url::MAX_URL_LIFETIME`, which refuses a longer
/// policy). Both are defined as this constant. Garbage collection relies
/// on it: `aspen_compact::CompactConfig::validate` refuses a `gc_grace`
/// shorter than twice this, so every URL handed out for a pack before it
/// became an orphan has expired before the pack is deleted (issue #203).
pub const MAX_PACK_URL_LIFETIME: Duration = Duration::from_secs(60 * 60);

/// Highest compaction level. L0 holds push packs; L1 to L4 are covers of
/// geometrically growing span.
pub const MAX_LEVEL: u8 = 4;

/// A compaction level, 0 to [`MAX_LEVEL`] (`packs.level`). A pack's level
/// follows from its origin ([`NewPack::level`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PackLevel(u8);

impl PackLevel {
    /// Level 0: packs as pushed.
    pub const L0: PackLevel = PackLevel(0);

    /// A level; `None` above [`MAX_LEVEL`].
    pub const fn new(level: u8) -> Option<Self> {
        if level <= MAX_LEVEL {
            Some(Self(level))
        } else {
            None
        }
    }

    /// The number.
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// A compaction output level, 1 to [`MAX_LEVEL`] (`covers.level`, and the
/// level of every compaction pack).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CoverLevel(u8);

impl CoverLevel {
    /// A level; `None` outside `1..=MAX_LEVEL`.
    pub const fn new(level: u8) -> Option<Self> {
        if level >= 1 && level <= MAX_LEVEL {
            Some(Self(level))
        } else {
            None
        }
    }

    /// The number.
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl From<CoverLevel> for PackLevel {
    fn from(level: CoverLevel) -> Self {
        PackLevel(level.0)
    }
}

/// Where a stored pack came from, which also fixes its level: push packs
/// are exactly the L0 packs (schema v1 `packs_push_iff_l0`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PackOrigin {
    /// A push, completed at ingest. Always level 0.
    Push {
        /// Bytes the client sent. Compared with the stored size, it shows how
        /// much thin-pack completion grew the pack (S4).
        received_bytes: u64,
    },
    /// Written by the repack writer, at level 1 to 4: a cover's pack at the
    /// cover's level, or an exact shallow pack (node C8,
    /// [`crate::meta::MetaStore::set_shallow_pack`]), which is registered at
    /// level 1 and which no cover names. Schema v1 allows only these two
    /// origins (`packs_origin_values`), and SQLite cannot change a `CHECK`
    /// constraint without rebuilding `packs` and every table that references
    /// it, so a shallow pack has no origin of its own; the `shallow_packs`
    /// table says what it is.
    Compaction {
        /// The cover level the pack was written for.
        level: CoverLevel,
    },
}

impl PackOrigin {
    /// The value stored in `packs.origin`.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Push { .. } => "push",
            Self::Compaction { .. } => "compaction",
        }
    }

    /// The pack's level.
    pub const fn level(&self) -> PackLevel {
        match self {
            Self::Push { .. } => PackLevel::L0,
            Self::Compaction { level } => PackLevel(level.0),
        }
    }
}

/// Which registration of a checksum a `packs` row is. The first row for a
/// checksum is generation 0; when garbage collection has deleted a row
/// (leaving a tombstone with its generation), the next registration of the
/// same checksum gets the next generation and therefore new blob keys
/// ([`PackKeys`]). A blob deletion that garbage collection started for an
/// old generation can then never remove the blobs of a newer one, however
/// late it runs (issue #48).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PackGeneration(u32);

impl PackGeneration {
    /// The first registration of a checksum.
    pub const FIRST: Self = Self(0);

    /// Generation `n`.
    pub const fn new(n: u32) -> Self {
        Self(n)
    }

    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The generation after this one; `None` past `u32::MAX`.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(n) => Some(Self(n)),
            None => None,
        }
    }
}

impl fmt::Display for PackGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The blob keys of one stored pack, all derived from its repository,
/// checksum and generation:
///
/// ```text
/// repos/<repo id>/packs/<checksum>.pack        generation 0
/// repos/<repo id>/packs/<checksum>.idx
/// repos/<repo id>/packs/<checksum>.entries
/// repos/<repo id>/packs/<checksum>.g<n>.pack   generation n >= 1
/// repos/<repo id>/packs/<checksum>.g<n>.idx
/// repos/<repo id>/packs/<checksum>.g<n>.entries
/// ```
///
/// Because the checksum is the SHA-1 of the pack's bytes, a key can never
/// serve different bytes (S3). The generation ([`PackGeneration`]) gives a
/// checksum registered again after garbage collection keys that no earlier
/// deletion names. The repository prefix keeps tenants apart in the bucket
/// and lets garbage collection work per repository. Only a registered row
/// ([`PackRecord::keys`]) knows its generation, so a writer uploads after
/// [`crate::meta::MetaStore::register_pack`] returns, never before.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PackKeys {
    /// The pack file.
    pub pack: BlobKey,
    /// Its Git `.idx` (version 2).
    pub idx: BlobKey,
    /// Its entry table ([`EntryTable`]).
    pub entries: BlobKey,
}

impl PackKeys {
    /// The keys for a pack.
    pub fn new(repo: RepoId, checksum: &PackChecksum, generation: PackGeneration) -> Self {
        let base = match generation.get() {
            0 => format!("repos/{repo}/packs/{checksum}"),
            n => format!("repos/{repo}/packs/{checksum}.g{n}"),
        };
        let key =
            |ext: &str| BlobKey::new(format!("{base}.{ext}")).expect("derived keys are valid");
        Self {
            pack: key("pack"),
            idx: key("idx"),
            entries: key("entries"),
        }
    }
}

/// Size of the fixed part of a version 2 `.idx`: 8-byte header, 256-entry
/// fanout table of 4 bytes each, and the pack and index checksums.
pub const IDX_V2_FIXED_LEN: u64 = 8 + 256 * 4 + 2 * SHA1_LEN as u64;
/// Bytes per object in a version 2 `.idx`: ID, CRC32 and 4-byte offset.
pub const IDX_V2_PER_OBJECT: u64 = SHA1_LEN as u64 + 4 + 4;
/// Bytes per large (8-byte) offset in a version 2 `.idx`; at most one per
/// object, only for entries past 2 GiB.
pub const IDX_V2_LARGE_OFFSET: u64 = 8;

/// Whether `idx_size` is a possible size for a version 2 `.idx` of
/// `object_count` objects (schema v1 `packs_idx_size_v2`).
pub const fn idx_v2_size_fits(object_count: u32, idx_size: u64) -> bool {
    let n = object_count as u64;
    let min = IDX_V2_FIXED_LEN + IDX_V2_PER_OBJECT * n;
    idx_size >= min
        && idx_size <= min + IDX_V2_LARGE_OFFSET * n
        && (idx_size - min).is_multiple_of(IDX_V2_LARGE_OFFSET)
}

/// The size of the entry table of a pack with `object_count` objects
/// (schema v1 `packs_entries_size_exact`).
pub const fn entry_table_len(object_count: u32) -> u64 {
    ENTRY_TABLE_HEADER_LEN as u64 + ENTRY_RECORD_LEN as u64 * object_count as u64
}

/// Why a pack row is invalid. Each variant matches a named constraint of
/// schema v1's `packs` table.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PackRowError {
    /// Packs with no objects are never stored; such a push records no pack.
    #[error("a stored pack has at least one object")]
    Empty,
    /// More REF_DELTA entries than objects.
    #[error("ref_delta_count {ref_delta} exceeds object_count {objects}")]
    RefDeltaCount {
        /// REF_DELTA count.
        ref_delta: u32,
        /// Object count.
        objects: u32,
    },
    /// The pack is smaller than a header plus a trailer.
    #[error("pack size {0} is too small")]
    TooSmall(u64),
    /// A byte count does not fit a `BIGINT` column.
    #[error("{field} {value} exceeds i64::MAX")]
    TooLarge {
        /// The field.
        field: &'static str,
        /// Its value.
        value: u64,
    },
    /// The `.idx` size is impossible for a version 2 index of this many
    /// objects.
    #[error("idx_size {idx_size} is not a version 2 .idx size for {objects} objects")]
    IdxSize {
        /// `.idx` size.
        idx_size: u64,
        /// Object count.
        objects: u32,
    },
    /// The entry table size is not [`entry_table_len`] of the object count.
    #[error("entries_size {entries_size} should be {expected}")]
    EntriesSize {
        /// Entry table size.
        entries_size: u64,
        /// The size for the object count.
        expected: u64,
    },
}

/// A pack to register, after it and its `.idx` and entry table are durable
/// in the blob store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPack {
    /// Repository.
    pub repo: RepoId,
    /// Trailing SHA-1 of the stored bytes; the `packfile-uris` hash.
    pub checksum: PackChecksum,
    /// Stored pack size in bytes.
    pub size: u64,
    /// `.idx` size in bytes (about 6% of the pack, S4).
    pub idx_size: u64,
    /// Entry table size in bytes; always [`entry_table_len`] of
    /// `object_count`.
    pub entries_size: u64,
    /// Objects in the pack.
    pub object_count: u32,
    /// REF_DELTA entries in the pack. `no-ref-delta` may be advertised only
    /// when every planned pack has zero (S3).
    pub ref_delta_count: u32,
    /// Push or compaction, which fixes the level.
    pub origin: PackOrigin,
}

impl NewPack {
    /// Bytes the pack takes in the blob store: the pack, its `.idx` and its
    /// entry table.
    pub fn stored_bytes(&self) -> u64 {
        self.size
            .saturating_add(self.idx_size)
            .saturating_add(self.entries_size)
    }

    /// Checks every row-level rule schema v1 enforces on `packs`, so a row
    /// that passes is never refused by the database for its values.
    pub fn validate(&self) -> Result<(), PackRowError> {
        let bounded = |field, value: u64| {
            if value > DB_MAX {
                Err(PackRowError::TooLarge { field, value })
            } else {
                Ok(())
            }
        };
        bounded("size", self.size)?;
        bounded("idx_size", self.idx_size)?;
        bounded("entries_size", self.entries_size)?;
        if let PackOrigin::Push { received_bytes } = self.origin {
            bounded("received_bytes", received_bytes)?;
        }
        if self.object_count == 0 {
            return Err(PackRowError::Empty);
        }
        if self.ref_delta_count > self.object_count {
            return Err(PackRowError::RefDeltaCount {
                ref_delta: self.ref_delta_count,
                objects: self.object_count,
            });
        }
        if self.size < PACK_HEADER_LEN + PACK_TRAILER_LEN {
            return Err(PackRowError::TooSmall(self.size));
        }
        if !idx_v2_size_fits(self.object_count, self.idx_size) {
            return Err(PackRowError::IdxSize {
                idx_size: self.idx_size,
                objects: self.object_count,
            });
        }
        let expected = entry_table_len(self.object_count);
        if self.entries_size != expected {
            return Err(PackRowError::EntriesSize {
                entries_size: self.entries_size,
                expected,
            });
        }
        Ok(())
    }

    /// The pack's level: 0 for push packs, the cover level for compaction.
    pub fn level(&self) -> PackLevel {
        self.origin.level()
    }
}

/// A `packs` row. The row's key columns are derived from `pack` and
/// `generation` ([`PackRecord::keys`]); a store that reads a row whose
/// stored keys differ reports [`StoreError::Corrupt`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackRecord {
    /// Row ID. IDs are never reused, so an ID also fixes the generation.
    pub id: PackId,
    /// Everything registered.
    pub pack: NewPack,
    /// Which registration of the checksum this row is.
    pub generation: PackGeneration,
    /// When the row was written.
    pub created_at: UnixMicros,
}

impl PackRecord {
    /// The blob keys of the pack, its `.idx` and its entry table.
    pub fn keys(&self) -> PackKeys {
        PackKeys::new(self.pack.repo, &self.pack.checksum, self.generation)
    }
}

/// Limits checked on a push before its body is read (S4): the object count
/// is in the pack's first 12 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushLimits {
    /// Most objects per pushed pack. Size gix's heap at about 140 bytes per
    /// object (S4).
    pub max_objects: u32,
    /// Most bytes per pushed pack. The spool disk must hold about twice this.
    pub max_pack_bytes: u64,
}

impl PushLimits {
    /// Checks a pack header's object count.
    pub fn check_header(&self, object_count: u32) -> Result<(), UnpackError> {
        if object_count > self.max_objects {
            Err(UnpackError::TooManyObjects {
                count: object_count,
                limit: self.max_objects,
            })
        } else {
            Ok(())
        }
    }
}

/// Magic bytes at the start of an entry table.
pub const ENTRY_TABLE_MAGIC: [u8; 8] = *b"ASPENENT";
/// Entry table format version.
pub const ENTRY_TABLE_VERSION: u32 = 1;
/// Bytes before the first record: magic (8), version (4), entry count (4),
/// pack size (8), pack checksum (20).
pub const ENTRY_TABLE_HEADER_LEN: usize = 44;
/// Bytes per record: offset (8), type (1), header length (1), zero (2),
/// object ID (20), base (20).
pub const ENTRY_RECORD_LEN: usize = 52;

/// The kind of one pack entry, with its delta base.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
    /// A full commit (pack type 1).
    Commit,
    /// A full tree (type 2).
    Tree,
    /// A full blob (type 3).
    Blob,
    /// A full annotated tag (type 4).
    Tag,
    /// A delta against the entry at an earlier offset of the same pack
    /// (type 6).
    OfsDelta {
        /// Absolute offset of the base entry.
        base_offset: u64,
    },
    /// A delta against an object named by ID (type 7).
    RefDelta {
        /// The base object.
        base: ObjectId,
    },
}

impl EntryKind {
    /// The shortest and longest possible entry header for this kind: the
    /// type-and-size varint (1 to 10 bytes for a 64-bit size), plus an
    /// OFS_DELTA distance varint (1 to 10 bytes) or a REF_DELTA base ID
    /// (20 bytes).
    pub const fn header_len_range(&self) -> (u8, u8) {
        match self {
            Self::Commit | Self::Tree | Self::Blob | Self::Tag => (1, 10),
            Self::OfsDelta { .. } => (2, 20),
            Self::RefDelta { .. } => (1 + SHA1_LEN as u8, 10 + SHA1_LEN as u8),
        }
    }

    /// The pack type code.
    pub const fn type_code(&self) -> u8 {
        match self {
            Self::Commit => 1,
            Self::Tree => 2,
            Self::Blob => 3,
            Self::Tag => 4,
            Self::OfsDelta { .. } => 6,
            Self::RefDelta { .. } => 7,
        }
    }
}

/// One entry of a stored pack, as the stitcher needs it: where it is, how
/// long its header is, which object it is, and what it is based on. The
/// entry's compressed data is copied unchanged; only an OFS_DELTA header may
/// be rewritten.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PackEntry {
    /// Offset of the entry's header in the pack.
    pub offset: u64,
    /// Length of the header: type-and-size varint plus the OFS_DELTA
    /// distance or REF_DELTA base ID.
    pub header_len: u8,
    /// The object the entry resolves to.
    pub id: ObjectId,
    /// Kind and base.
    pub kind: EntryKind,
}

/// Why an entry table is invalid, or why a pack breaks a layout invariant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EntryTableError {
    /// Wrong magic, version or length.
    #[error("malformed entry table: {0}")]
    Malformed(&'static str),
    /// A record is out of order or out of bounds.
    #[error("entry at offset {offset}: {rule}")]
    BadEntry {
        /// Offset of the entry.
        offset: u64,
        /// What is wrong.
        rule: &'static str,
    },
    /// The same object appears twice.
    #[error("object {0} appears twice")]
    Duplicate(ObjectId),
    /// A REF_DELTA's base is not in the pack (the pack is thin).
    #[error("entry at offset {offset} is based on {base}, which is not in the pack")]
    ExternalBase {
        /// Offset of the delta.
        offset: u64,
        /// The missing base.
        base: ObjectId,
    },
    /// A REF_DELTA's base comes later in the pack.
    #[error("entry at offset {offset} comes before its base")]
    ForwardBase {
        /// Offset of the delta.
        offset: u64,
    },
}

/// The per-entry table of one stored pack, written at ingest (and by the
/// compactor) to `<checksum>.entries` next to the pack and `.idx`.
///
/// The stitcher reads it with one sequential read per segment instead of
/// range-reading entry headers out of the pack. Records are sorted by
/// offset. Encoding, all integers big-endian:
///
/// ```text
/// header  magic "ASPENENT" | version u32 = 1 | count u32 | pack size u64 | pack checksum [20]
/// record  offset u64 | type u8 | header len u8 | 0u16 | object id [20] | base [20]
/// ```
///
/// `base` is the base offset as a u64 followed by 12 zero bytes for an
/// OFS_DELTA, the base ID for a REF_DELTA, and zeros otherwise. An entry's
/// length in the pack is the next record's offset minus its own (the last
/// entry ends where the trailer starts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryTable {
    checksum: PackChecksum,
    pack_size: u64,
    entries: Vec<PackEntry>,
}

impl EntryTable {
    /// Builds a table, checking that offsets are increasing and in bounds,
    /// header lengths are possible for each kind and fit before the next
    /// entry, OFS_DELTA bases name earlier entries, no object ID repeats,
    /// and an empty table describes a pack with no data between header and
    /// trailer.
    pub fn new(
        checksum: PackChecksum,
        pack_size: u64,
        entries: Vec<PackEntry>,
    ) -> Result<Self, EntryTableError> {
        if entries.len() > u32::MAX as usize {
            return Err(EntryTableError::Malformed("more than u32::MAX entries"));
        }
        let data_end = pack_size
            .checked_sub(PACK_TRAILER_LEN)
            .filter(|&e| e >= PACK_HEADER_LEN)
            .ok_or(EntryTableError::Malformed(
                "pack size smaller than header and trailer",
            ))?;
        if entries.is_empty() && data_end != PACK_HEADER_LEN {
            return Err(EntryTableError::Malformed(
                "no entries, but the pack has data between header and trailer",
            ));
        }
        let mut expected = PACK_HEADER_LEN;
        for (i, e) in entries.iter().enumerate() {
            let bad = |rule| EntryTableError::BadEntry {
                offset: e.offset,
                rule,
            };
            if i == 0 && e.offset != PACK_HEADER_LEN {
                return Err(bad("first entry must start right after the pack header"));
            }
            if e.offset < expected || entries.get(i + 1).is_some_and(|n| n.offset <= e.offset) {
                return Err(bad("entries must be sorted by offset and must not overlap"));
            }
            if e.offset >= data_end {
                return Err(bad("entry starts inside the trailer"));
            }
            let (min_header, max_header) = e.kind.header_len_range();
            if e.header_len < min_header || e.header_len > max_header {
                return Err(bad("header length is impossible for the entry type"));
            }
            let next = entries.get(i + 1).map_or(data_end, |n| n.offset);
            match e.offset.checked_add(u64::from(e.header_len)) {
                Some(data) if data < next => {}
                _ => return Err(bad("header does not fit before the next entry")),
            }
            if let EntryKind::OfsDelta { base_offset } = e.kind {
                if base_offset >= e.offset {
                    return Err(bad("OFS_DELTA base must be earlier in the pack"));
                }
                if entries[..i]
                    .binary_search_by_key(&base_offset, |b| b.offset)
                    .is_err()
                {
                    return Err(bad("OFS_DELTA base is not the start of an entry"));
                }
            }
            expected = e.offset + 1;
        }
        let mut ids: Vec<ObjectId> = entries.iter().map(|e| e.id).collect();
        ids.sort_unstable();
        if let Some(dup) = find_repeated(&ids).expect("just sorted").first() {
            return Err(EntryTableError::Duplicate(*dup));
        }
        Ok(Self {
            checksum,
            pack_size,
            entries,
        })
    }

    /// The pack's checksum, which binds the table to one pack.
    pub fn checksum(&self) -> &PackChecksum {
        &self.checksum
    }

    /// The pack's size.
    pub fn pack_size(&self) -> u64 {
        self.pack_size
    }

    /// The entries, in offset order.
    pub fn entries(&self) -> &[PackEntry] {
        &self.entries
    }

    /// Length of entry `i` in the pack, header included.
    pub fn entry_len(&self, i: usize) -> u64 {
        let next = self
            .entries
            .get(i + 1)
            .map_or(self.pack_size - PACK_TRAILER_LEN, |n| n.offset);
        next - self.entries[i].offset
    }

    /// Checks the self-contained and base-first invariants. OFS_DELTA bases
    /// are always earlier by construction; this checks REF_DELTA bases.
    pub fn check_layout(&self) -> Result<(), EntryTableError> {
        let offsets: HashMap<ObjectId, u64> =
            self.entries.iter().map(|e| (e.id, e.offset)).collect();
        for e in &self.entries {
            if let EntryKind::RefDelta { base } = e.kind {
                match offsets.get(&base) {
                    None => {
                        return Err(EntryTableError::ExternalBase {
                            offset: e.offset,
                            base,
                        });
                    }
                    // A REF_DELTA based on itself is a forward base too: its
                    // base is not earlier.
                    Some(&b) if b >= e.offset => {
                        return Err(EntryTableError::ForwardBase { offset: e.offset });
                    }
                    Some(_) => {}
                }
            }
        }
        Ok(())
    }

    /// Number of REF_DELTA entries, for `packs.ref_delta_count`.
    pub fn ref_delta_count(&self) -> u32 {
        self.entries
            .iter()
            .filter(|e| matches!(e.kind, EntryKind::RefDelta { .. }))
            .count() as u32
    }

    /// Encodes the table.
    pub fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(ENTRY_TABLE_HEADER_LEN + ENTRY_RECORD_LEN * self.entries.len());
        out.extend_from_slice(&ENTRY_TABLE_MAGIC);
        out.extend_from_slice(&ENTRY_TABLE_VERSION.to_be_bytes());
        out.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.pack_size.to_be_bytes());
        out.extend_from_slice(self.checksum.as_bytes());
        for e in &self.entries {
            out.extend_from_slice(&e.offset.to_be_bytes());
            out.push(e.kind.type_code());
            out.push(e.header_len);
            out.extend_from_slice(&[0, 0]);
            out.extend_from_slice(e.id.as_bytes());
            let mut base = [0u8; SHA1_LEN];
            match e.kind {
                EntryKind::OfsDelta { base_offset } => {
                    base[..8].copy_from_slice(&base_offset.to_be_bytes());
                }
                EntryKind::RefDelta { base: id } => base = *id.as_bytes(),
                _ => {}
            }
            out.extend_from_slice(&base);
        }
        out
    }

    /// Decodes the stored entry table of `pack` and checks that it describes
    /// that pack: same checksum, pack size, object count and REF_DELTA
    /// count. Every failure is [`StoreError::Corrupt`], because the table
    /// was validated when it was written.
    pub fn decode_for(bytes: &[u8], pack: &PackRecord) -> Result<Self, StoreError> {
        let key = pack.keys().entries;
        let corrupt = |what: String| StoreError::Corrupt {
            what: format!("entry table {key}: {what}"),
        };
        let table = Self::decode(bytes).map_err(|e| corrupt(e.to_string()))?;
        let p = &pack.pack;
        if table.checksum != p.checksum {
            return Err(corrupt(format!(
                "describes pack {}, not {}",
                table.checksum, p.checksum
            )));
        }
        if table.pack_size != p.size {
            return Err(corrupt(format!(
                "pack size {} but the row says {}",
                table.pack_size, p.size
            )));
        }
        if table.entries.len() != p.object_count as usize {
            return Err(corrupt(format!(
                "{} entries but the row says {} objects",
                table.entries.len(),
                p.object_count
            )));
        }
        if table.ref_delta_count() != p.ref_delta_count {
            return Err(corrupt(format!(
                "{} REF_DELTA entries but the row says {}",
                table.ref_delta_count(),
                p.ref_delta_count
            )));
        }
        Ok(table)
    }

    /// Decodes and validates a table, without tying it to a pack.
    fn decode(bytes: &[u8]) -> Result<Self, EntryTableError> {
        use EntryTableError::Malformed;
        if bytes.len() < ENTRY_TABLE_HEADER_LEN {
            return Err(Malformed("shorter than the header"));
        }
        if bytes[..8] != ENTRY_TABLE_MAGIC {
            return Err(Malformed("bad magic"));
        }
        let u32_at = |at: usize| u32::from_be_bytes(bytes[at..at + 4].try_into().expect("4 bytes"));
        let u64_at = |at: usize| u64::from_be_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
        if u32_at(8) != ENTRY_TABLE_VERSION {
            return Err(Malformed("unsupported version"));
        }
        let count = u32_at(12) as usize;
        let pack_size = u64_at(16);
        let checksum = PackChecksum::from_slice(&bytes[24..44]).expect("20 bytes");
        let body = &bytes[ENTRY_TABLE_HEADER_LEN..];
        if body.len() != count * ENTRY_RECORD_LEN {
            return Err(Malformed("length does not match the entry count"));
        }
        let mut entries = Vec::with_capacity(count);
        for rec in body.as_chunks::<ENTRY_RECORD_LEN>().0 {
            let offset = u64::from_be_bytes(rec[..8].try_into().expect("8 bytes"));
            let bad = |rule| EntryTableError::BadEntry { offset, rule };
            if rec[10..12] != [0, 0] {
                return Err(bad("reserved bytes are not zero"));
            }
            let id = ObjectId::from_slice(&rec[12..32]).expect("20 bytes");
            let base = &rec[32..52];
            let full = |kind| {
                if base.iter().all(|&b| b == 0) {
                    Ok(kind)
                } else {
                    Err(bad("base must be zero for a full object"))
                }
            };
            let kind = match rec[8] {
                1 => full(EntryKind::Commit)?,
                2 => full(EntryKind::Tree)?,
                3 => full(EntryKind::Blob)?,
                4 => full(EntryKind::Tag)?,
                6 => {
                    if base[8..].iter().any(|&b| b != 0) {
                        return Err(bad("OFS_DELTA base padding is not zero"));
                    }
                    EntryKind::OfsDelta {
                        base_offset: u64::from_be_bytes(base[..8].try_into().expect("8 bytes")),
                    }
                }
                7 => EntryKind::RefDelta {
                    base: ObjectId::from_slice(base).expect("20 bytes"),
                },
                _ => return Err(bad("unknown pack entry type")),
            };
            entries.push(PackEntry {
                offset,
                header_len: rec[9],
                id,
                kind,
            });
        }
        Self::new(checksum, pack_size, entries)
    }
}

/// An ID list that should be sorted is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("IDs are not sorted at position {position}")]
pub struct NotSorted {
    /// Index of the first ID smaller than the one before it.
    pub position: usize,
}

/// The IDs that appear more than once in a sorted sequence (such as the ID
/// list of an `.idx`), each reported once, in order.
///
/// The duplicate scan of S4 relies on the input being sorted (an `.idx`
/// lists IDs in order), so the order is checked here, in the same pass, and
/// unsorted input is an error rather than a silently incomplete answer.
pub fn find_repeated(sorted: &[ObjectId]) -> Result<Vec<ObjectId>, NotSorted> {
    let mut out: Vec<ObjectId> = Vec::new();
    for (i, w) in sorted.windows(2).enumerate() {
        if w[1] < w[0] {
            return Err(NotSorted { position: i + 1 });
        }
        if w[0] == w[1] && out.last() != Some(&w[0]) {
            out.push(w[0]);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }
    fn sum() -> PackChecksum {
        PackChecksum::from_bytes([0xcc; 20])
    }
    /// An entry with the shortest header its kind allows.
    fn e(offset: u64, id: u8, kind: EntryKind) -> PackEntry {
        PackEntry {
            offset,
            header_len: kind.header_len_range().0,
            id: oid(id),
            kind,
        }
    }

    fn sample() -> EntryTable {
        EntryTable::new(
            sum(),
            100,
            vec![
                e(12, 1, EntryKind::Blob),
                e(20, 2, EntryKind::OfsDelta { base_offset: 12 }),
                e(30, 3, EntryKind::RefDelta { base: oid(2) }),
                e(60, 4, EntryKind::Commit),
            ],
        )
        .unwrap()
    }

    fn new_pack() -> NewPack {
        NewPack {
            repo: RepoId::new(1).unwrap(),
            checksum: sum(),
            size: 100,
            idx_size: 1184,
            entries_size: 252,
            object_count: 4,
            ref_delta_count: 1,
            origin: PackOrigin::Push { received_bytes: 80 },
        }
    }

    fn record() -> PackRecord {
        PackRecord {
            id: PackId::new(3).unwrap(),
            pack: new_pack(),
            generation: PackGeneration::FIRST,
            created_at: UnixMicros(0),
        }
    }

    #[test]
    fn entry_table_round_trips() {
        let t = sample();
        let bytes = t.encode();
        assert_eq!(bytes.len() as u64, entry_table_len(4));
        assert_eq!(EntryTable::decode_for(&bytes, &record()).unwrap(), t);
        assert_eq!(t.entry_len(0), 8);
        assert_eq!(t.entry_len(3), 100 - 20 - 60);
        assert_eq!(t.ref_delta_count(), 1);
        t.check_layout().unwrap();
    }

    #[test]
    fn empty_table_round_trips_only_for_an_empty_data_region() {
        let t = EntryTable::new(sum(), 32, vec![]).unwrap();
        assert_eq!(EntryTable::decode(&t.encode()).unwrap(), t);
        assert_eq!(
            EntryTable::new(sum(), 100, vec![]),
            Err(EntryTableError::Malformed(
                "no entries, but the pack has data between header and trailer"
            ))
        );
    }

    #[test]
    fn entry_table_rejects_bad_layouts() {
        let with_header = |mut p: PackEntry, len| {
            p.header_len = len;
            p
        };
        let cases: Vec<(Vec<PackEntry>, &str)> = vec![
            (vec![e(13, 1, EntryKind::Blob)], "first entry"),
            (
                vec![e(12, 1, EntryKind::Blob), e(12, 2, EntryKind::Blob)],
                "sorted",
            ),
            (
                vec![e(12, 1, EntryKind::Blob), e(13, 2, EntryKind::Blob)],
                "header does not fit",
            ),
            (
                vec![e(12, 1, EntryKind::OfsDelta { base_offset: 12 })],
                "earlier",
            ),
            (
                vec![
                    e(12, 1, EntryKind::Blob),
                    e(20, 2, EntryKind::OfsDelta { base_offset: 13 }),
                ],
                "start of an entry",
            ),
            (
                vec![e(12, 1, EntryKind::Blob), e(80, 2, EntryKind::Blob)],
                "trailer",
            ),
            (
                vec![with_header(e(12, 1, EntryKind::Blob), 0)],
                "impossible for the entry type",
            ),
            (
                vec![with_header(e(12, 1, EntryKind::Blob), 11)],
                "impossible for the entry type",
            ),
            (
                vec![with_header(
                    e(12, 1, EntryKind::OfsDelta { base_offset: 0 }),
                    1,
                )],
                "impossible for the entry type",
            ),
            (
                vec![with_header(
                    e(12, 1, EntryKind::RefDelta { base: oid(2) }),
                    20,
                )],
                "impossible for the entry type",
            ),
        ];
        for (entries, expect) in cases {
            let err = EntryTable::new(sum(), 100, entries).unwrap_err();
            assert!(err.to_string().contains(expect), "{err} vs {expect}");
        }
        assert_eq!(
            EntryTable::new(
                sum(),
                100,
                vec![e(12, 1, EntryKind::Blob), e(20, 1, EntryKind::Tree)]
            ),
            Err(EntryTableError::Duplicate(oid(1)))
        );
    }

    #[test]
    fn a_corrupt_pack_size_does_not_overflow_the_header_check() {
        // A header length that runs past u64::MAX must be an error, not an
        // arithmetic overflow.
        let mut last = e(u64::MAX - 30, 2, EntryKind::RefDelta { base: oid(1) });
        last.header_len = 30;
        let entries = vec![e(12, 1, EntryKind::Blob), last];
        let err = EntryTable::new(sum(), u64::MAX, entries).unwrap_err();
        assert!(err.to_string().contains("header does not fit"), "{err}");
    }

    #[test]
    fn entry_table_decode_rejects_corruption() {
        let good = sample().encode();
        let decode = |bytes: &[u8]| EntryTable::decode_for(bytes, &record());
        let mut magic = good.clone();
        magic[0] = b'X';
        assert!(decode(&magic).is_err());
        let mut version = good.clone();
        version[11] = 2;
        assert!(decode(&version).is_err());
        assert!(decode(&good[..good.len() - 1]).is_err());
        let mut kind = good.clone();
        kind[ENTRY_TABLE_HEADER_LEN + 8] = 5;
        assert!(decode(&kind).is_err());
        let mut reserved = good.clone();
        reserved[ENTRY_TABLE_HEADER_LEN + 10] = 1;
        assert!(decode(&reserved).is_err());
        let mut full_base = good;
        full_base[ENTRY_TABLE_HEADER_LEN + 40] = 1;
        let err = decode(&full_base).unwrap_err();
        assert!(err.is_corruption(), "{err}");
    }

    #[test]
    fn entry_table_must_describe_the_pack_it_is_read_for() {
        let bytes = sample().encode();
        let mismatch = |f: fn(&mut NewPack)| {
            let mut r = record();
            f(&mut r.pack);
            match EntryTable::decode_for(&bytes, &r) {
                Err(StoreError::Corrupt { what }) => what,
                other => panic!("expected Corrupt, got {other:?}"),
            }
        };
        assert!(
            mismatch(|p| p.checksum = PackChecksum::from_bytes([1; 20])).contains("describes pack")
        );
        assert!(mismatch(|p| p.size = 101).contains("pack size"));
        assert!(mismatch(|p| p.object_count = 5).contains("entries"));
        assert!(mismatch(|p| p.ref_delta_count = 0).contains("REF_DELTA"));
        let what = mismatch(|p| p.size = 101);
        assert!(what.contains("repos/1/packs/"), "{what}");
    }

    #[test]
    fn layout_check_finds_thin_forward_and_self_bases() {
        let thin = EntryTable::new(
            sum(),
            100,
            vec![e(12, 1, EntryKind::RefDelta { base: oid(9) })],
        )
        .unwrap();
        assert_eq!(
            thin.check_layout(),
            Err(EntryTableError::ExternalBase {
                offset: 12,
                base: oid(9)
            })
        );
        let forward = EntryTable::new(
            sum(),
            100,
            vec![
                e(12, 1, EntryKind::RefDelta { base: oid(2) }),
                e(40, 2, EntryKind::Blob),
            ],
        )
        .unwrap();
        assert_eq!(
            forward.check_layout(),
            Err(EntryTableError::ForwardBase { offset: 12 })
        );
        let own_base = EntryTable::new(
            sum(),
            100,
            vec![e(12, 1, EntryKind::RefDelta { base: oid(1) })],
        )
        .unwrap();
        assert_eq!(
            own_base.check_layout(),
            Err(EntryTableError::ForwardBase { offset: 12 })
        );
    }

    #[test]
    fn repeated_ids_are_reported_once_each() {
        let ids = [oid(1), oid(2), oid(2), oid(2), oid(3), oid(4), oid(4)];
        assert_eq!(find_repeated(&ids), Ok(vec![oid(2), oid(4)]));
        assert_eq!(find_repeated(&[oid(1), oid(2)]), Ok(vec![]));
        assert_eq!(find_repeated(&[]), Ok(vec![]));
    }

    #[test]
    fn unsorted_input_is_an_error_not_a_partial_answer() {
        // Sorted, this has a repeat of 2; unsorted, a window scan misses it.
        assert_eq!(
            find_repeated(&[oid(2), oid(1), oid(2)]),
            Err(NotSorted { position: 1 })
        );
        assert_eq!(
            find_repeated(&[oid(1), oid(3), oid(3), oid(2)]),
            Err(NotSorted { position: 3 })
        );
    }

    #[test]
    fn pack_keys_are_derived_from_repo_and_checksum() {
        let repo = RepoId::new(7).unwrap();
        let k = PackKeys::new(repo, &sum(), PackGeneration::FIRST);
        let hex = "cc".repeat(20);
        assert_eq!(k.pack.as_str(), format!("repos/7/packs/{hex}.pack"));
        assert_eq!(k.idx.as_str(), format!("repos/7/packs/{hex}.idx"));
        assert_eq!(k.entries.as_str(), format!("repos/7/packs/{hex}.entries"));
        let r = record();
        assert_eq!(
            r.keys(),
            PackKeys::new(r.pack.repo, &sum(), PackGeneration::FIRST)
        );
        // A later generation never shares a key with an earlier one.
        let g2 = PackKeys::new(repo, &sum(), PackGeneration::new(2));
        assert_eq!(g2.pack.as_str(), format!("repos/7/packs/{hex}.g2.pack"));
        assert_eq!(g2.idx.as_str(), format!("repos/7/packs/{hex}.g2.idx"));
        assert_eq!(
            g2.entries.as_str(),
            format!("repos/7/packs/{hex}.g2.entries")
        );
        let g1 = PackKeys::new(repo, &sum(), PackGeneration::new(1));
        for (a, b) in [(&k, &g1), (&g1, &g2), (&k, &g2)] {
            for x in [&a.pack, &a.idx, &a.entries] {
                assert!(![&b.pack, &b.idx, &b.entries].contains(&x), "{x}");
            }
        }
        assert_eq!(PackGeneration::new(u32::MAX).next(), None);
    }

    #[test]
    fn pack_rows_validate() {
        new_pack().validate().unwrap();
        let invalid = |f: fn(&mut NewPack)| {
            let mut p = new_pack();
            f(&mut p);
            p.validate().unwrap_err()
        };
        assert_eq!(invalid(|p| p.object_count = 0), PackRowError::Empty);
        assert!(matches!(
            invalid(|p| p.ref_delta_count = 5),
            PackRowError::RefDeltaCount { .. }
        ));
        assert_eq!(invalid(|p| p.size = 31), PackRowError::TooSmall(31));
        assert_eq!(
            invalid(|p| p.size = DB_MAX + 1),
            PackRowError::TooLarge {
                field: "size",
                value: DB_MAX + 1
            }
        );
        assert!(matches!(
            invalid(|p| p.origin = PackOrigin::Push {
                received_bytes: u64::MAX
            }),
            PackRowError::TooLarge {
                field: "received_bytes",
                ..
            }
        ));
        assert!(matches!(
            invalid(|p| p.idx_size = u64::MAX),
            PackRowError::TooLarge { .. }
        ));
        assert!(matches!(
            invalid(|p| p.entries_size = u64::MAX),
            PackRowError::TooLarge { .. }
        ));
        // The old fixture value: too small for 4 objects.
        assert!(matches!(
            invalid(|p| p.idx_size = 1100),
            PackRowError::IdxSize { .. }
        ));
        assert!(matches!(
            invalid(|p| p.idx_size = 0),
            PackRowError::IdxSize { .. }
        ));
        assert!(matches!(
            invalid(|p| p.idx_size = 1185),
            PackRowError::IdxSize { .. }
        ));
        assert!(matches!(
            invalid(|p| p.idx_size = 1184 + 5 * 8),
            PackRowError::IdxSize { .. }
        ));
        assert!(matches!(
            invalid(|p| p.entries_size = 0),
            PackRowError::EntriesSize { expected: 252, .. }
        ));
        assert!(matches!(
            invalid(|p| p.entries_size = 253),
            PackRowError::EntriesSize { .. }
        ));
        // Up to one large offset per object.
        let mut big = new_pack();
        big.idx_size = 1184 + 4 * 8;
        big.validate().unwrap();
    }

    #[test]
    fn origin_fixes_the_level() {
        assert_eq!(new_pack().level(), PackLevel::L0);
        let mut p = new_pack();
        p.origin = PackOrigin::Compaction {
            level: CoverLevel::new(3).unwrap(),
        };
        p.validate().unwrap();
        assert_eq!(p.level(), PackLevel::new(3).unwrap());
        assert_eq!(p.origin.as_str(), "compaction");
        // A compaction pack cannot be built at L0, nor a cover at L0 or L5.
        assert!(CoverLevel::new(0).is_none());
        assert!(CoverLevel::new(5).is_none());
        assert_eq!(PackLevel::from(CoverLevel::new(4).unwrap()).get(), 4);
    }

    #[test]
    fn idx_sizes_follow_the_version_2_layout() {
        assert_eq!(IDX_V2_FIXED_LEN, 1072);
        assert!(idx_v2_size_fits(4, 1184));
        assert!(idx_v2_size_fits(4, 1216));
        assert!(!idx_v2_size_fits(4, 1224));
        assert!(!idx_v2_size_fits(4, 1183));
        assert!(idx_v2_size_fits(u32::MAX, 1072 + 28 * u64::from(u32::MAX)));
    }

    #[test]
    fn levels_stop_at_four() {
        assert_eq!(PackLevel::new(4).unwrap().get(), 4);
        assert!(PackLevel::new(5).is_none());
    }

    #[test]
    fn push_limits_check_the_header_count() {
        let l = PushLimits {
            max_objects: 10,
            max_pack_bytes: 1 << 30,
        };
        assert!(l.check_header(10).is_ok());
        assert_eq!(
            l.check_header(11),
            Err(UnpackError::TooManyObjects {
                count: 11,
                limit: 10
            })
        );
    }
}

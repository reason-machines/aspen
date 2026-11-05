//! The stitcher (`docs/PLAN.md`, fetch path step 4; spike S1): one pack
//! built from the selected stored packs ("segments"), in plan order, for
//! clients that do not use packfile URIs.
//!
//! - every entry whose object ID was already emitted is dropped (the first
//!   copy wins);
//! - every other entry's compressed data is copied unchanged; only an
//!   OFS_DELTA's base distance is rewritten, against wherever its base's
//!   copy was emitted;
//! - the header's object count is known from the plan before the first byte,
//!   and the trailing SHA-1 is computed over the output.
//!
//! Stored packs are self-contained and base-first, so one streaming pass
//! suffices: every kept entry's base is earlier in its own segment, and that
//! base was either emitted or dropped because an earlier copy was emitted.
//! The stitcher never reorders or buffers entries. A delta whose base is not
//! earlier in its segment means a stored pack broke the base-first
//! invariant: the fetch fails with
//! [`FetchError::StoredPackNotBaseFirst`] (retryable, and it pages).
//!
//! It is sans-IO: the caller reads each segment's blob in any chunk sizes
//! and feeds the bytes in; output bytes are appended to a buffer the caller
//! drains to the client. A segment that ends early, runs long, or whose
//! bytes disagree with its entry table is [`StoreError::Corrupt`], never a
//! truncated pack.
//!
//! Memory: the duplicate set (found by a k-way merge of the segments'
//! sorted `.idx` ID lists, [`StitchPlan::new`]) holds each repeated ID with
//! its emitted offset; the current segment adds its entry table and an
//! old-to-new offset map. O(duplicates + entries in the largest segment).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use aspen_core::error::{FetchError, StoreError};
use aspen_core::id::{ObjectId, PackChecksum, PackId};
use aspen_core::pack::{EntryKind, EntryTable, PACK_HEADER_LEN, PACK_TRAILER_LEN};

use crate::Sha1;
use crate::header::{self, MAX_HEADER_LEN, RawBase};

/// One segment as the plan sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedSegment {
    pack: PackId,
    checksum: PackChecksum,
    entries: u32,
    /// SHA-1 over the `.idx`'s `(id, offset)` pairs in ID order, so
    /// [`Stitcher::begin_segment`] can check the entry table against it.
    layout: [u8; 20],
}

/// Hashes one `(id, offset)` pair into a segment's layout digest.
fn hash_pair(h: &mut Sha1, id: &ObjectId, offset: u64) {
    h.update(id.as_bytes());
    h.update(&offset.to_be_bytes());
}

/// Where a repeated object's first copy is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Duplicate {
    /// Index of the first segment that holds it: that copy is kept.
    owner: u32,
    /// Output offset and delta depth of the kept copy, once emitted.
    emitted: Option<(u64, u32)>,
}

/// The output of planning: the object count of the stitched pack and the
/// repeated objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StitchPlan {
    segments: Vec<PlannedSegment>,
    object_count: u32,
    duplicates: HashMap<ObjectId, Duplicate>,
}

/// One segment's input to [`StitchPlan::new`].
#[derive(Debug)]
pub struct SegmentIds<I> {
    /// The stored pack.
    pub pack: PackId,
    /// Its checksum (`packs.checksum`).
    pub checksum: PackChecksum,
    /// Its objects as `(id, offset)` in ascending ID order, such as
    /// [`crate::idx::IdxV2::ids_with_offsets`]. The offsets let the
    /// stitcher check each entry table against the `.idx`: entry tables
    /// have no checksum of their own, and one whose IDs were swapped would
    /// otherwise make the stitcher drop the wrong entry.
    pub ids: I,
}

impl StitchPlan {
    /// Plans a stitch of `segments`, in send order, by a k-way merge of
    /// their sorted ID lists: O(segments) memory for the merge plus one
    /// entry per repeated ID.
    ///
    /// An ID list that is not strictly increasing is [`StoreError::Corrupt`]
    /// (an `.idx` lists IDs in order, and a stored pack never repeats one).
    pub fn new<I: Iterator<Item = (ObjectId, u64)>>(
        segments: Vec<SegmentIds<I>>,
    ) -> Result<Self, StoreError> {
        let mut planned = Vec::with_capacity(segments.len());
        let mut iters = Vec::with_capacity(segments.len());
        let mut hashes = Vec::with_capacity(segments.len());
        for s in segments {
            planned.push(PlannedSegment {
                pack: s.pack,
                checksum: s.checksum,
                entries: 0,
                layout: [0; 20],
            });
            iters.push(s.ids);
            hashes.push(Sha1::new());
        }
        let mut heap = BinaryHeap::new();
        for (i, it) in iters.iter_mut().enumerate() {
            if let Some((id, offset)) = it.next() {
                hash_pair(&mut hashes[i], &id, offset);
                heap.push(Reverse((id, i)));
            }
        }
        let mut total: u64 = 0;
        let mut dropped: u64 = 0;
        let mut duplicates = HashMap::new();
        let mut current: Option<(ObjectId, usize)> = None;
        while let Some(Reverse((id, i))) = heap.pop() {
            total += 1;
            planned[i].entries += 1;
            match current {
                // Equal IDs pop in segment order, so the first is the owner.
                Some((cur, owner)) if cur == id => {
                    dropped += 1;
                    duplicates.entry(id).or_insert(Duplicate {
                        owner: owner as u32,
                        emitted: None,
                    });
                }
                _ => current = Some((id, i)),
            }
            if let Some((next, offset)) = iters[i].next() {
                hash_pair(&mut hashes[i], &next, offset);
                if next <= id {
                    return Err(StoreError::Corrupt {
                        what: format!(
                            "pack {}: .idx IDs are not strictly increasing at {next}",
                            planned[i].pack
                        ),
                    });
                }
                heap.push(Reverse((next, i)));
            }
        }
        let object_count = u32::try_from(total - dropped).map_err(|_| StoreError::Corrupt {
            what: "a stitched pack cannot hold more than u32::MAX objects".into(),
        })?;
        for (seg, h) in planned.iter_mut().zip(hashes) {
            seg.layout = h.finish().map_err(|e| StoreError::Corrupt {
                what: format!("pack {}: {e}", seg.pack),
            })?;
        }
        Ok(Self {
            segments: planned,
            object_count,
            duplicates,
        })
    }

    /// Objects in the stitched pack: the header count.
    pub fn object_count(&self) -> u32 {
        self.object_count
    }

    /// Number of repeated objects whose later copies are dropped.
    pub fn duplicate_count(&self) -> usize {
        self.duplicates.len()
    }
}

/// Totals of a finished stitch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StitchOutcome {
    /// The stitched pack's trailing SHA-1.
    pub checksum: PackChecksum,
    /// Bytes emitted, header and trailer included.
    pub size: u64,
    /// Entries emitted.
    pub object_count: u32,
    /// Entries dropped as later copies.
    pub dropped: u64,
    /// Entries whose OFS_DELTA header was rewritten.
    pub rewritten_headers: u64,
    /// Longest delta chain in the output (0: no deltas). Keep-first can hang
    /// a later segment's delta off an earlier segment's copy of its base,
    /// so this can approach the sum of the segments' own depths.
    pub max_delta_depth: u32,
}

/// Where the current segment's byte stream is.
#[derive(Debug)]
enum Phase {
    /// Reading the 12-byte pack header.
    Head,
    /// Reading entry `index`'s header bytes.
    EntryHeader,
    /// Copying (or skipping) the rest of the current entry.
    EntryData { keep: bool },
    /// Reading the 20-byte trailer.
    Trailer,
    /// Every byte of the segment was read.
    Done,
}

#[derive(Debug)]
struct Segment {
    index: u32,
    table: EntryTable,
    /// Output offset and depth of each emitted entry, by entry index.
    emitted: Vec<Option<(u64, u32)>>,
    /// Entry index by ID, only built when the segment has REF_DELTAs.
    by_id: HashMap<ObjectId, usize>,
    pos: u64,
    entry: usize,
    phase: Phase,
    buf: Vec<u8>,
    hash: Sha1,
}

/// A stitch in progress. Create with [`Stitcher::new`], then for each
/// planned segment in order: [`Stitcher::begin_segment`], any number of
/// [`Stitcher::feed`] calls with the segment's bytes, and
/// [`Stitcher::end_segment`]. [`Stitcher::finish`] appends the trailer.
///
/// After an error the output is unusable: abort the response.
#[derive(Debug)]
pub struct Stitcher {
    plan: StitchPlan,
    next_segment: u32,
    segment: Option<Segment>,
    out_pos: u64,
    out_hash: Sha1,
    emitted: u64,
    dropped: u64,
    rewritten: u64,
    max_depth: u32,
}

impl Stitcher {
    /// Starts a stitch; appends the pack header (with the planned object
    /// count) to `out`.
    pub fn new(plan: StitchPlan, out: &mut Vec<u8>) -> Self {
        let mut head = Vec::with_capacity(PACK_HEADER_LEN as usize);
        head.extend_from_slice(b"PACK");
        head.extend_from_slice(&2u32.to_be_bytes());
        head.extend_from_slice(&plan.object_count.to_be_bytes());
        let mut s = Self {
            plan,
            next_segment: 0,
            segment: None,
            out_pos: 0,
            out_hash: Sha1::new(),
            emitted: 0,
            dropped: 0,
            rewritten: 0,
            max_depth: 0,
        };
        s.emit(&head, out);
        s
    }

    /// Starts the next planned segment with its entry table, which must be
    /// the table of the planned pack (decode it with
    /// [`EntryTable::decode_for`] against the pack's row).
    pub fn begin_segment(&mut self, table: EntryTable) -> Result<(), FetchError> {
        if self.segment.is_some() {
            return Err(corrupt("begin_segment before end_segment"));
        }
        let index = self.next_segment;
        let planned = self
            .plan
            .segments
            .get(index as usize)
            .ok_or_else(|| corrupt("more segments than planned"))?;
        let mismatch = || {
            corrupt(format!(
                "pack {}: entry table does not match the planned .idx",
                planned.pack
            ))
        };
        if table.checksum() != &planned.checksum
            || table.entries().len() != planned.entries as usize
        {
            return Err(mismatch());
        }
        // Same (id, offset) pairs as the .idx: the entry table decides
        // which entries are dropped, so it must name each entry as the
        // .idx (checksummed) does. Costs a sort of the segment's entries,
        // small next to hashing its bytes.
        let mut pairs: Vec<(ObjectId, u64)> =
            table.entries().iter().map(|e| (e.id, e.offset)).collect();
        pairs.sort_unstable();
        let mut h = Sha1::new();
        for (id, offset) in &pairs {
            hash_pair(&mut h, id, *offset);
        }
        drop(pairs);
        if h.finish().map_err(|e| corrupt(e.to_string()))? != planned.layout {
            return Err(mismatch());
        }
        let by_id = if table.ref_delta_count() > 0 {
            table
                .entries()
                .iter()
                .enumerate()
                .map(|(i, e)| (e.id, i))
                .collect()
        } else {
            HashMap::new()
        };
        let n = table.entries().len();
        self.segment = Some(Segment {
            index,
            table,
            emitted: vec![None; n],
            by_id,
            pos: 0,
            entry: 0,
            phase: Phase::Head,
            buf: Vec::with_capacity(MAX_HEADER_LEN),
            hash: Sha1::new(),
        });
        self.next_segment += 1;
        Ok(())
    }

    /// Feeds the next bytes of the current segment's pack blob; appends any
    /// output to `out`.
    pub fn feed(&mut self, mut bytes: &[u8], out: &mut Vec<u8>) -> Result<(), FetchError> {
        let mut seg = self
            .segment
            .take()
            .ok_or_else(|| corrupt("feed without a segment"))?;
        let result = (|| {
            while !bytes.is_empty() {
                self.step(&mut seg, &mut bytes, out)?;
            }
            Ok(())
        })();
        self.segment = Some(seg);
        result
    }

    /// Ends the current segment. Fewer bytes than the pack's size (a short
    /// read from the blob store) is [`StoreError::Corrupt`].
    pub fn end_segment(&mut self) -> Result<(), FetchError> {
        let seg = self
            .segment
            .take()
            .ok_or_else(|| corrupt("end_segment without a segment"))?;
        if !matches!(seg.phase, Phase::Done) {
            return Err(corrupt(format!(
                "pack {}: read {} of {} bytes",
                self.plan.segments[seg.index as usize].pack,
                seg.pos,
                seg.table.pack_size()
            )));
        }
        Ok(())
    }

    /// Appends the trailer; checks that every planned segment was stitched
    /// and that the planned count was emitted.
    pub fn finish(mut self, out: &mut Vec<u8>) -> Result<StitchOutcome, FetchError> {
        if self.segment.is_some() || self.next_segment as usize != self.plan.segments.len() {
            return Err(corrupt("finish before every planned segment was stitched"));
        }
        if self.emitted != u64::from(self.plan.object_count) {
            return Err(corrupt(format!(
                "emitted {} objects, planned {}",
                self.emitted, self.plan.object_count
            )));
        }
        let sum = std::mem::replace(&mut self.out_hash, Sha1::new())
            .finish()
            .map_err(|e| corrupt(e.to_string()))?;
        out.extend_from_slice(&sum);
        Ok(StitchOutcome {
            checksum: PackChecksum::from_bytes(sum),
            size: self.out_pos + PACK_TRAILER_LEN,
            object_count: self.plan.object_count,
            dropped: self.dropped,
            rewritten_headers: self.rewritten,
            max_delta_depth: self.max_depth,
        })
    }

    fn emit(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        self.out_hash.update(bytes);
        self.out_pos += bytes.len() as u64;
        out.extend_from_slice(bytes);
    }

    /// Consumes some of `bytes` for the current phase.
    fn step(
        &mut self,
        seg: &mut Segment,
        bytes: &mut &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), FetchError> {
        let pack_id = self.plan.segments[seg.index as usize].pack;
        let size = seg.table.pack_size();
        let data_end = size - PACK_TRAILER_LEN;
        let take = |seg: &mut Segment, bytes: &mut &[u8], upto: u64| -> Vec<u8> {
            let n = (upto - seg.pos).min(bytes.len() as u64) as usize;
            let (now, rest) = bytes.split_at(n);
            *bytes = rest;
            seg.pos += n as u64;
            now.to_vec()
        };
        match seg.phase {
            Phase::Head => {
                let got = take(seg, bytes, PACK_HEADER_LEN);
                seg.hash.update(&got);
                seg.buf.extend_from_slice(&got);
                if seg.pos == PACK_HEADER_LEN {
                    let h = std::mem::take(&mut seg.buf);
                    let count = u32::from_be_bytes(h[8..12].try_into().expect("4"));
                    if &h[..4] != b"PACK"
                        || u32::from_be_bytes(h[4..8].try_into().expect("4")) != 2
                        || count as usize != seg.table.entries().len()
                    {
                        return Err(corrupt(format!(
                            "pack {pack_id}: header does not match its entry table"
                        )));
                    }
                    seg.phase = if seg.table.entries().is_empty() {
                        Phase::Trailer
                    } else {
                        Phase::EntryHeader
                    };
                }
            }
            Phase::EntryHeader => {
                let e = seg.table.entries()[seg.entry];
                let end = e.offset + u64::from(e.header_len);
                let got = take(seg, bytes, end);
                seg.hash.update(&got);
                seg.buf.extend_from_slice(&got);
                if seg.pos == end {
                    let keep = self.entry_header(seg, pack_id, out)?;
                    seg.buf.clear();
                    seg.phase = Phase::EntryData { keep };
                }
            }
            Phase::EntryData { keep } => {
                let end = seg
                    .table
                    .entries()
                    .get(seg.entry + 1)
                    .map_or(data_end, |n| n.offset);
                let n = (end - seg.pos).min(bytes.len() as u64) as usize;
                let (now, rest) = bytes.split_at(n);
                seg.hash.update(now);
                if keep {
                    self.emit(now, out);
                }
                *bytes = rest;
                seg.pos += n as u64;
                if seg.pos == end {
                    seg.entry += 1;
                    seg.phase = if seg.entry == seg.table.entries().len() {
                        Phase::Trailer
                    } else {
                        Phase::EntryHeader
                    };
                }
            }
            Phase::Trailer => {
                let got = take(seg, bytes, size);
                seg.buf.extend_from_slice(&got);
                if seg.pos == size {
                    let sum = std::mem::replace(&mut seg.hash, Sha1::new())
                        .finish()
                        .map_err(|e| corrupt(e.to_string()))?;
                    if seg.buf != sum || seg.buf != seg.table.checksum().as_bytes() {
                        return Err(corrupt(format!(
                            "pack {pack_id}: bytes do not match the pack checksum"
                        )));
                    }
                    seg.phase = Phase::Done;
                }
            }
            Phase::Done => {
                return Err(corrupt(format!(
                    "pack {pack_id}: more bytes than its size {size}"
                )));
            }
        }
        Ok(())
    }

    /// Decides the fate of the entry whose header is in `seg.buf`, and
    /// emits its (possibly rewritten) header if it is kept.
    fn entry_header(
        &mut self,
        seg: &mut Segment,
        pack_id: PackId,
        out: &mut Vec<u8>,
    ) -> Result<bool, FetchError> {
        let i = seg.entry;
        let e = seg.table.entries()[i];
        let mismatch = || {
            corrupt(format!(
                "pack {pack_id}: entry at {} does not match its entry table",
                e.offset
            ))
        };
        let raw = header::decode(&seg.buf).map_err(|_| mismatch())?;
        let consistent = raw.len == e.header_len
            && raw.type_code == e.kind.type_code()
            && match (raw.base, e.kind) {
                (RawBase::Distance(d), EntryKind::OfsDelta { base_offset }) => {
                    e.offset.checked_sub(d) == Some(base_offset)
                }
                (RawBase::Id(id), EntryKind::RefDelta { base }) => id == base,
                (RawBase::None, _) => true,
                _ => false,
            };
        if !consistent {
            return Err(mismatch());
        }
        let duplicate = self.plan.duplicates.get(&e.id).copied();
        if let Some(d) = duplicate
            && d.owner != seg.index
        {
            if d.owner > seg.index || d.emitted.is_none() {
                return Err(corrupt(format!(
                    "object {} was planned in another segment but not emitted there",
                    e.id
                )));
            }
            self.dropped += 1;
            return Ok(false);
        }
        // The base's emitted position and depth: earlier in this segment
        // (kept or dropped as a repeat) or nowhere (a broken invariant).
        let base_index = match e.kind {
            EntryKind::OfsDelta { base_offset } => Some(
                seg.table
                    .entries()
                    .binary_search_by_key(&base_offset, |b| b.offset)
                    .map_err(|_| mismatch())?,
            ),
            EntryKind::RefDelta { base } => match seg.by_id.get(&base) {
                Some(&b) if b < i => Some(b),
                Some(_) => {
                    return Err(FetchError::StoredPackNotBaseFirst {
                        pack: pack_id,
                        offset: e.offset,
                    });
                }
                None => {
                    return Err(corrupt(format!(
                        "pack {pack_id} is thin: entry at {} is based on {base}, which it lacks",
                        e.offset
                    )));
                }
            },
            _ => None,
        };
        let base_at = match base_index {
            None => None,
            Some(b) => Some(match seg.emitted[b] {
                Some(at) => at,
                None => {
                    let id = seg.table.entries()[b].id;
                    self.plan
                        .duplicates
                        .get(&id)
                        .and_then(|d| d.emitted)
                        .ok_or_else(|| {
                            corrupt(format!("base {id} of a kept entry was never emitted"))
                        })?
                }
            }),
        };
        let at = self.out_pos;
        let depth = base_at.map_or(0, |(_, d)| d + 1);
        if matches!(e.kind, EntryKind::OfsDelta { .. }) {
            let (base_pos, _) = base_at.expect("a delta has a base");
            let old_distance = e.offset
                - match e.kind {
                    EntryKind::OfsDelta { base_offset } => base_offset,
                    _ => unreachable!(),
                };
            let distance = at - base_pos;
            if distance == old_distance {
                let h = std::mem::take(&mut seg.buf);
                self.emit(&h, out);
                seg.buf = h;
            } else {
                let mut h = Vec::with_capacity(MAX_HEADER_LEN);
                h.extend_from_slice(&seg.buf[..usize::from(raw.size_len)]);
                header::encode_distance(distance, &mut h);
                self.emit(&h, out);
                self.rewritten += 1;
            }
        } else {
            let h = std::mem::take(&mut seg.buf);
            self.emit(&h, out);
            seg.buf = h;
        }
        seg.emitted[i] = Some((at, depth));
        if let Some(d) = self.plan.duplicates.get_mut(&e.id) {
            d.emitted = Some((at, depth));
        }
        self.emitted += 1;
        self.max_depth = self.max_depth.max(depth);
        Ok(true)
    }
}

fn corrupt(what: impl Into<String>) -> FetchError {
    FetchError::Store(StoreError::Corrupt { what: what.into() })
}

/// Stitches whole in-memory segments: a convenience for tests, tools and
/// small packs. `segments` pairs each planned pack's row ID with its bytes
/// and entry table.
pub fn stitch_all(
    segments: Vec<(PackId, Vec<u8>, EntryTable)>,
) -> Result<(Vec<u8>, StitchOutcome), FetchError> {
    let mut sorted: Vec<Vec<(ObjectId, u64)>> = segments
        .iter()
        .map(|(_, _, t)| {
            let mut ids: Vec<(ObjectId, u64)> =
                t.entries().iter().map(|e| (e.id, e.offset)).collect();
            ids.sort_unstable();
            ids
        })
        .collect();
    let plan = StitchPlan::new(
        segments
            .iter()
            .zip(sorted.iter_mut())
            .map(|((pack, _, t), ids)| SegmentIds {
                pack: *pack,
                checksum: *t.checksum(),
                ids: std::mem::take(ids).into_iter(),
            })
            .collect(),
    )
    .map_err(FetchError::Store)?;
    let mut out = Vec::new();
    let mut s = Stitcher::new(plan, &mut out);
    for (_, bytes, table) in segments {
        s.begin_segment(table)?;
        s.feed(&bytes, &mut out)?;
        s.end_segment()?;
    }
    let outcome = s.finish(&mut out)?;
    Ok((out, outcome))
}

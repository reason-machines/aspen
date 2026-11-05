//! Thin-pack completion over gix's pack entries.
//!
//! [`ThinCompletion`] sits between gix's parser of the spooled pack
//! (`BytesToEntriesIter`) and its pack writer (`EntriesToBytesIter`). For
//! each REF_DELTA whose base the lookup returns, it inserts the base as a
//! full object just before that delta and turns the delta, and every later
//! REF_DELTA on the same base, into an OFS_DELTA. Every entry after an
//! insertion or a header change moves, so every OFS_DELTA distance is
//! recomputed from where its base now is. A REF_DELTA the lookup does not
//! answer stays a REF_DELTA, for gix's indexer to resolve inside the pack.
//!
//! This replaces gix-pack 0.77's `LookupRefDeltaObjectsIter`, which places
//! bases the same way but recomputes OFS_DELTA distances only while the
//! *net* size change so far is non-zero. An inserted base adds its bytes
//! and each REF_DELTA turned OFS_DELTA saves 17 to 19 header bytes; when
//! the two cancel exactly, later OFS_DELTAs keep stale distances and point
//! at the wrong entry or into the middle of one, so valid thin pushes were
//! refused (issue #151; decisions log). Here the distance is always derived
//! from an exact map of old offsets to new ones, whatever the net change.
//! For every pack where gix's arithmetic was right, the output is the same
//! bytes: bases go in the same places with the same compression.

use std::collections::HashMap;

use gix_hash::ObjectId;
use gix_pack::data::entry::Header;
use gix_pack::data::input::Entry;

/// The completion iterator (see the module docs).
pub(crate) struct ThinCompletion<I, F> {
    inner: I,
    lookup: F,
    compression: gix_zlib::Compression,
    /// The delta that goes right after the base just returned.
    pending: Option<Entry>,
    /// Offset shift (new offset minus old offset) of the entries read so
    /// far, as `(old offset, shift)` at each entry whose shift differs from
    /// the entry before it, in increasing old offset. An entry's shift is
    /// that of the last breakpoint at or before its old offset (0 before
    /// the first). A breakpoint is added only when an entry is placed
    /// after the shift changed, which an inserted base does and so does
    /// any entry whose header changes length (a REF_DELTA turned
    /// OFS_DELTA, or an OFS_DELTA whose new distance encodes in a
    /// different number of bytes). So it holds at most one breakpoint per
    /// received entry: usually about one per inserted base, but in the
    /// worst case it grows with the entry count.
    breakpoints: Vec<(u64, i64)>,
    /// The shift of the next entry read from `inner`.
    shift: i64,
    /// New offset of each base inserted so far.
    inserted: HashMap<ObjectId, u64>,
    buf: Vec<u8>,
}

impl<I, F> ThinCompletion<I, F> {
    pub(crate) fn new(inner: I, lookup: F, compression: gix_zlib::Compression) -> Self {
        ThinCompletion {
            inner,
            lookup,
            compression,
            pending: None,
            breakpoints: Vec::new(),
            shift: 0,
            inserted: HashMap::new(),
            buf: Vec::new(),
        }
    }

    /// The shift of the entry that starts at `old` in the received pack.
    /// For an offset inside an entry it is that entry's shift, so a
    /// distance into the middle of an entry stays in the middle of it and
    /// gix's indexer refuses it as it would without completion.
    fn shift_at(&self, old: u64) -> i64 {
        let after = self.breakpoints.partition_point(|&(at, _)| at <= old);
        after.checked_sub(1).map_or(0, |i| self.breakpoints[i].1)
    }

    /// Places `entry`, read at `old` with every bytes before it shifted by
    /// `self.shift`, at its new offset.
    fn place(&mut self, entry: &mut Entry, old: u64) {
        if self.breakpoints.last().map_or(0, |&(_, s)| s) != self.shift {
            self.breakpoints.push((old, self.shift));
        }
        entry.pack_offset = shifted(old, self.shift);
    }

    /// Makes `entry` (already placed) an OFS_DELTA at `distance` from its
    /// base, and shifts the entries after it by the change in its header.
    fn point_back(&mut self, entry: &mut Entry, distance: u64) {
        entry.header = Header::OfsDelta {
            base_distance: distance,
        };
        let before = i64::from(entry.header_size);
        entry.header_size = entry.header.size(entry.decompressed_size) as u16;
        entry.crc32 = Some(entry.compute_crc32());
        self.shift += i64::from(entry.header_size) - before;
    }
}

fn shifted(old: u64, shift: i64) -> u64 {
    old.checked_add_signed(shift)
        .expect("an entry never moves before the pack header")
}

fn corrupt(message: String) -> gix_error::Error {
    gix_error::message!("{message}").corrupted_error()
}

impl<I, F> Iterator for ThinCompletion<I, F>
where
    I: Iterator<Item = gix_error::Result<Entry>>,
    F: gix_object::Find,
{
    type Item = gix_error::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(delta) = self.pending.take() {
            return Some(Ok(delta));
        }
        let mut entry = match self.inner.next()? {
            Ok(entry) => entry,
            Err(e) => return Some(Err(e)),
        };
        let old = entry.pack_offset;
        match entry.header {
            Header::RefDelta { base_id } => {
                if let Some(&base) = self.inserted.get(&base_id) {
                    self.place(&mut entry, old);
                    let distance = entry.pack_offset - base;
                    self.point_back(&mut entry, distance);
                    return Some(Ok(entry));
                }
                let found = match self.lookup.try_find(&base_id, &mut self.buf) {
                    Ok(found) => found,
                    Err(e) => return Some(Err(e)),
                };
                let Some(obj) = found else {
                    self.place(&mut entry, old);
                    return Some(Ok(entry));
                };
                let at = shifted(old, self.shift);
                let mut base = match Entry::from_data_obj(&obj, at, self.compression) {
                    Ok(base) => base,
                    Err(e) => return Some(Err(e)),
                };
                base.pack_offset = at;
                self.inserted.insert(base_id, at);
                let len = base.bytes_in_pack();
                self.shift += i64::try_from(len).expect("an entry is shorter than 2^63 bytes");
                self.place(&mut entry, old);
                self.point_back(&mut entry, len);
                self.pending = Some(entry);
                Some(Ok(base))
            }
            Header::OfsDelta { base_distance } => {
                let Some(base_old) = Header::verified_base_pack_offset(old, base_distance) else {
                    return Some(Err(corrupt(format!(
                        "The OFS_DELTA base distance {base_distance} is invalid for pack offset {old}"
                    ))));
                };
                let base = shifted(base_old, self.shift_at(base_old));
                self.place(&mut entry, old);
                let Some(distance) = entry.pack_offset.checked_sub(base).filter(|d| *d > 0) else {
                    return Some(Err(corrupt(format!(
                        "The OFS_DELTA at pack offset {old} does not point back"
                    ))));
                };
                if distance != base_distance {
                    self.point_back(&mut entry, distance);
                }
                Some(Ok(entry))
            }
            _ => {
                self.place(&mut entry, old);
                Some(Ok(entry))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // At most one inserted base per entry; gix sizes its delta tree
        // from the upper bound.
        let (min, max) = self.inner.size_hint();
        (min, max.map(|max| max.saturating_mul(2)))
    }
}

//! Building a pack's entry table ([`aspen_core::pack::EntryTable`]) with a
//! header walk: entry boundaries come from the `.idx` offsets, and only each
//! entry's header is read. Nothing is inflated.

use std::io::{BufReader, Read, Seek, SeekFrom};

use aspen_core::pack::{EntryKind, EntryTable, PACK_HEADER_LEN, PACK_TRAILER_LEN, PackEntry};

use crate::LayoutError;
use crate::header::{self, MAX_HEADER_LEN, RawBase};
use crate::idx::IdxV2;

/// Reads the header of every entry of `pack` (whose size is `pack_size`) at
/// the offsets `idx` lists, and builds the entry table. The pack's trailer
/// must equal the checksum the `.idx` names.
///
/// The table's own validation ([`EntryTable::new`]) then checks that
/// entries tile the pack, that OFS_DELTA bases are earlier entries and that
/// no ID repeats. Whether REF_DELTA bases are in the pack and earlier is
/// [`EntryTable::check_layout`], which the caller runs.
pub fn build<R: Read + Seek>(
    pack: R,
    pack_size: u64,
    idx: &IdxV2<'_>,
) -> Result<EntryTable, LayoutError> {
    let data_end = pack_size
        .checked_sub(PACK_TRAILER_LEN)
        .filter(|&e| e >= PACK_HEADER_LEN)
        .ok_or_else(|| LayoutError::mismatch("pack is smaller than a header and trailer"))?;
    let mut r = BufReader::with_capacity(1 << 20, pack);
    let mut head = [0u8; PACK_HEADER_LEN as usize];
    r.seek(SeekFrom::Start(0))?;
    r.read_exact(&mut head)?;
    if &head[..4] != b"PACK" || u32::from_be_bytes(head[4..8].try_into().expect("4")) != 2 {
        return Err(LayoutError::mismatch("not a version 2 pack"));
    }
    let count = u32::from_be_bytes(head[8..12].try_into().expect("4"));
    if count as usize != idx.len() {
        return Err(LayoutError::mismatch(format!(
            "pack header counts {count} objects, the .idx {}",
            idx.len()
        )));
    }
    let by_offset = idx.by_offset();
    let mut entries = Vec::with_capacity(by_offset.len());
    let mut pos = PACK_HEADER_LEN;
    let mut buf = [0u8; MAX_HEADER_LEN];
    for (i, &(offset, id, _crc)) in by_offset.iter().enumerate() {
        let next = by_offset.get(i + 1).map_or(data_end, |n| n.0);
        if offset < pos || next <= offset {
            return Err(LayoutError::mismatch(format!(
                "entry offsets overlap at {offset}"
            )));
        }
        r.seek_relative((offset - pos) as i64)?;
        let n = (next - offset).min(MAX_HEADER_LEN as u64) as usize;
        r.read_exact(&mut buf[..n])?;
        pos = offset + n as u64;
        let raw =
            header::decode(&buf[..n]).map_err(|source| LayoutError::Header { offset, source })?;
        let kind = match (raw.type_code, raw.base) {
            (1, _) => EntryKind::Commit,
            (2, _) => EntryKind::Tree,
            (3, _) => EntryKind::Blob,
            (4, _) => EntryKind::Tag,
            (6, RawBase::Distance(d)) => EntryKind::OfsDelta {
                base_offset: offset
                    .checked_sub(d)
                    .filter(|&b| b >= PACK_HEADER_LEN)
                    .ok_or_else(|| {
                        LayoutError::mismatch(format!(
                            "OFS_DELTA at {offset} points before the pack"
                        ))
                    })?,
            },
            (7, RawBase::Id(base)) => EntryKind::RefDelta { base },
            _ => unreachable!("header::decode pairs type codes with bases"),
        };
        entries.push(PackEntry {
            offset,
            header_len: raw.len,
            id,
            kind,
        });
    }
    r.seek_relative((data_end - pos) as i64)?;
    let mut trailer = [0u8; PACK_TRAILER_LEN as usize];
    r.read_exact(&mut trailer)?;
    let checksum = idx.pack_checksum();
    if &trailer != checksum.as_bytes() {
        return Err(LayoutError::mismatch(
            "the pack's trailer is not the checksum its .idx names",
        ));
    }
    Ok(EntryTable::new(checksum, pack_size, entries)?)
}

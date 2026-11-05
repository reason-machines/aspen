//! Checks every size a pack declares against the object limit
//! ([`MaxObjectBytes`]) before gix sees the pack, and sums them for the
//! total limit ([`crate::MaxInflatedBytes`]).
//!
//! A pack entry's header states its inflated size, and a delta's data
//! starts with two more sizes: its base's and its result's. gix sizes its
//! buffers from these claims, under one `alloc_limit_bytes` that must also
//! cover its delta tree (an allocation per entry), so gix's own limit grows
//! with the entry count and cannot enforce the object limit by itself. This
//! module does: it reads each entry's header and, for a delta, inflates only
//! the first [`DELTA_PREFIX`] bytes of its data, with fixed-size buffers.
//!
//! Two walks share the entry check:
//!
//! - [`check_at`] visits entries at known offsets (from pass 1 of ingest, or
//!   from a stored pack's `.idx`) and never inflates more than a delta's
//!   prefix;
//! - [`check_stream`] walks a pack from its first byte, inflating every entry
//!   to find where the next one starts (for [`crate::verify::verify_pack`],
//!   which has no `.idx` to trust). It stops an entry's stream one byte past
//!   its declared size, so a zlib bomb costs no more than its claim.
//!
//! Both return [`Totals`], which the caller checks with [`check_total`].

use std::io::{self, BufRead, BufReader, Read, Seek};

use aspen_core::pack::PACK_HEADER_LEN;

use crate::header::{self, MAX_HEADER_LEN, RawHeader};
use crate::{MaxInflatedBytes, MaxObjectBytes};

/// Bytes of a delta's inflated data that hold its two size varints (at most
/// 10 bytes each for 64-bit sizes).
pub(crate) const DELTA_PREFIX: usize = 20;

/// The fewest bytes of data a delta may have: Git's `DELTA_SIZE_MIN`. Git's
/// `patch_delta` refuses a shorter delta (`git index-pack` fails with
/// "failed to apply delta"), while gix applies it, so ingest refuses it
/// here to store only packs Git can index (issue #162).
pub(crate) const MIN_DELTA_DATA: u64 = 4;

/// Why a pack fails the size check.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DeclaredError {
    /// A declared size is over the object limit.
    #[error(
        "pack entry at offset {offset} declares a {what} of {size} bytes, over the {limit}-byte object limit"
    )]
    OverLimit {
        offset: u64,
        what: &'static str,
        size: u64,
        limit: u64,
    },
    /// The pack's entries inflate to more than the total limit.
    #[error(
        "the pack inflates to at least {total} bytes{with} over the {limit}-byte limit for one pack"
    )]
    OverTotal {
        total: u64,
        with: &'static str,
        limit: u64,
    },
    /// A delta's data is shorter than Git accepts ([`MIN_DELTA_DATA`]).
    #[error(
        "pack entry at offset {offset} is a delta of {size} bytes, shorter than the {MIN_DELTA_DATA} bytes Git requires (git index-pack refuses it: failed to apply delta)"
    )]
    ShortDelta { offset: u64, size: u64 },
    /// An entry inflates past the size its header declares.
    #[error("pack entry at offset {offset} inflates past its declared size of {size} bytes")]
    Overrun { offset: u64, size: u64 },
    /// The entry's header or the start of its data does not parse.
    #[error("pack entry at offset {offset}: {reason}")]
    Malformed { offset: u64, reason: String },
    /// The pack cannot be read.
    #[error("cannot read the pack: {0}")]
    Io(#[from] io::Error),
}

/// What the visited entries inflate to, as declared (saturating sums).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Totals {
    /// Declared sizes of the full (non-delta) entries.
    pub full: u64,
    /// Declared result sizes of the deltas.
    pub delta_results: u64,
}

impl Totals {
    fn add(&mut self, entry: Inflates) {
        match entry {
            Inflates::Full(n) => self.full = self.full.saturating_add(n),
            Inflates::Delta(n) => self.delta_results = self.delta_results.saturating_add(n),
        }
    }

    /// Both sums.
    pub(crate) fn all(self) -> u64 {
        self.full.saturating_add(self.delta_results)
    }
}

/// What one entry inflates to: a full object's size or a delta's result.
#[derive(Debug, Clone, Copy)]
enum Inflates {
    Full(u64),
    Delta(u64),
}

/// Refuses `total` bytes over `limit`; `with` names anything counted beyond
/// the pack's own entries (it follows "inflates to N bytes").
pub(crate) fn check_total(
    total: u64,
    limit: MaxInflatedBytes,
    with: &'static str,
) -> Result<(), DeclaredError> {
    if total > limit.get() {
        return Err(DeclaredError::OverTotal {
            total,
            with,
            limit: limit.get(),
        });
    }
    Ok(())
}

/// Checks the entries starting at `offsets` (ascending) in `pack`.
pub(crate) fn check_at<R: Read + Seek>(
    pack: R,
    offsets: impl IntoIterator<Item = u64>,
    limit: MaxObjectBytes,
) -> Result<Totals, DeclaredError> {
    let mut r = BufReader::with_capacity(64 * 1024, pack);
    r.seek(io::SeekFrom::Start(0))?;
    let mut pos = 0u64;
    let mut inflate = gix_zlib::Decompress::new();
    let mut totals = Totals::default();
    for offset in offsets {
        if offset < pos {
            return Err(malformed(offset, "entry offsets are not ascending"));
        }
        r.seek_relative(i64::try_from(offset - pos).map_err(|_| malformed(offset, "offset"))?)?;
        let (raw, read, inflates) = check_entry(&mut r, offset, limit, &mut inflate, false)?;
        totals.add(inflates);
        pos = offset + u64::from(raw.len) + read;
    }
    Ok(totals)
}

/// Walks the whole pack read by `pack` from its first byte: the 12-byte
/// header, then as many entries as it declares. Stops early, without error,
/// at the end of the input; whatever is wrong with a pack apart from its
/// declared sizes is for the caller's full parse to report.
pub(crate) fn check_stream<R: Read>(
    pack: R,
    limit: MaxObjectBytes,
) -> Result<Totals, DeclaredError> {
    let mut r = BufReader::with_capacity(64 * 1024, pack);
    let mut head = [0u8; PACK_HEADER_LEN as usize];
    let mut totals = Totals::default();
    if r.read_exact(&mut head).is_err() {
        return Ok(totals);
    }
    let count = u32::from_be_bytes(head[8..12].try_into().expect("4 bytes"));
    let mut offset = PACK_HEADER_LEN;
    let mut inflate = gix_zlib::Decompress::new();
    for _ in 0..count {
        if r.fill_buf()?.is_empty() {
            break;
        }
        let (raw, read, inflates) = check_entry(&mut r, offset, limit, &mut inflate, true)?;
        totals.add(inflates);
        offset += u64::from(raw.len) + read;
    }
    Ok(totals)
}

/// Reads one entry at the reader's position (`offset` in the pack): its
/// header, then, for a delta, its size varints. With `to_end`, inflates the
/// rest of the entry's stream too, so the reader ends at the next entry,
/// but no further than one byte past its declared size (an [`Overrun`]).
/// Returns the header, the compressed bytes consumed and what the entry
/// inflates to.
///
/// [`Overrun`]: DeclaredError::Overrun
fn check_entry<R: BufRead>(
    r: &mut R,
    offset: u64,
    limit: MaxObjectBytes,
    inflate: &mut gix_zlib::Decompress,
    to_end: bool,
) -> Result<(RawHeader, u64, Inflates), DeclaredError> {
    let raw = read_header(r, offset)?;
    let over = |what, size: u64| {
        (size > limit.get()).then_some(DeclaredError::OverLimit {
            offset,
            what,
            size,
            limit: limit.get(),
        })
    };
    if let Some(e) = over("size", raw.size) {
        return Err(e);
    }
    inflate.reset();
    let is_delta = matches!(raw.type_code, 6 | 7);
    let mut inflates = Inflates::Full(raw.size);
    if is_delta {
        // A delta's header size is the length of its data: two size varints
        // and the instructions.
        if raw.size < MIN_DELTA_DATA {
            return Err(DeclaredError::ShortDelta {
                offset,
                size: raw.size,
            });
        }
        let want = usize::try_from(raw.size).map_or(DELTA_PREFIX, |s| s.min(DELTA_PREFIX));
        let mut prefix = [0u8; DELTA_PREFIX];
        let got = inflate_into(r, inflate, &mut prefix[..want], offset)?;
        let mut at = 0;
        let mut size = || {
            delta_varint(&prefix[..got], &mut at)
                .ok_or_else(|| malformed(offset, "delta sizes do not parse"))
        };
        let base = size()?;
        let result = size()?;
        if let Some(e) = over("delta base", base).or_else(|| over("delta result", result)) {
            return Err(e);
        }
        inflates = Inflates::Delta(result);
    }
    if to_end {
        let mut sink = [0u8; 8 * 1024];
        loop {
            // One byte past the declared size detects an overrun without
            // inflating the rest of it.
            let left = (raw.size + 1).saturating_sub(inflate.total_out());
            if left == 0 {
                return Err(DeclaredError::Overrun {
                    offset,
                    size: raw.size,
                });
            }
            let n = usize::try_from(left).map_or(sink.len(), |l| l.min(sink.len()));
            if inflate_into(r, inflate, &mut sink[..n], offset)? == 0 {
                break;
            }
        }
    }
    Ok((raw, inflate.total_in(), inflates))
}

/// Reads an entry header byte by byte (at most [`MAX_HEADER_LEN`] bytes, so
/// nothing after it is consumed) and decodes it.
fn read_header<R: BufRead>(r: &mut R, offset: u64) -> Result<RawHeader, DeclaredError> {
    let mut buf = [0u8; MAX_HEADER_LEN];
    let mut n = 0;
    let mut next = |r: &mut R, n: &mut usize| -> Result<u8, DeclaredError> {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)
            .map_err(|_| malformed(offset, "truncated entry header"))?;
        if *n == MAX_HEADER_LEN {
            return Err(malformed(offset, "entry header is too long"));
        }
        buf[*n] = b[0];
        *n += 1;
        Ok(b[0])
    };
    let first = next(r, &mut n)?;
    let mut c = first;
    while c & 0x80 != 0 {
        c = next(r, &mut n)?;
    }
    match (first >> 4) & 7 {
        6 => loop {
            if next(r, &mut n)? & 0x80 == 0 {
                break;
            }
        },
        7 => {
            for _ in 0..aspen_core::id::SHA1_LEN {
                next(r, &mut n)?;
            }
        }
        _ => {}
    }
    header::decode(&buf[..n]).map_err(|e| malformed(offset, e.to_string()))
}

/// Inflates from `r` into `dst` until it is full or the stream ends.
fn inflate_into<R: BufRead>(
    r: &mut R,
    inflate: &mut gix_zlib::Decompress,
    dst: &mut [u8],
    offset: u64,
) -> Result<usize, DeclaredError> {
    if dst.is_empty() {
        return Ok(0);
    }
    gix_zlib::stream::inflate::read(r, inflate, dst).map_err(|e| {
        if e.kind() == io::ErrorKind::InvalidInput {
            malformed(offset, e.to_string())
        } else {
            DeclaredError::Io(e)
        }
    })
}

/// Git's delta size varint: 7 bits per byte, least significant first.
fn delta_varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let b = *bytes.get(*at)?;
        *at += 1;
        if shift > 63 || (shift == 63 && b & 0x7e != 0) {
            return None;
        }
        value |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
    }
}

fn malformed(offset: u64, reason: impl Into<String>) -> DeclaredError {
    DeclaredError::Malformed {
        offset,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::delta_varint;

    #[test]
    fn delta_varints_parse_and_overflow_is_refused() {
        let mut at = 0;
        assert_eq!(delta_varint(&[0x80, 0x01, 0x05], &mut at), Some(128));
        assert_eq!(delta_varint(&[0x80, 0x01, 0x05], &mut at), Some(5));
        let mut max = vec![0xff; 9];
        max.push(0x01);
        assert_eq!(delta_varint(&max, &mut 0), Some(u64::MAX));
        let mut over = vec![0xff; 9];
        over.push(0x02);
        assert_eq!(delta_varint(&over, &mut 0), None);
        assert_eq!(delta_varint(&[0x80], &mut 0), None);
    }
}

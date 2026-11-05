//! Pack entry headers: the type-and-size varint, followed by an OFS_DELTA
//! base distance or a REF_DELTA base ID.
//!
//! Everything here is pure byte manipulation. The stitcher and the
//! forward-base repair rewrite only these headers; compressed entry data is
//! always copied unchanged.

use aspen_core::id::{ObjectId, SHA1_LEN};

/// Longest possible entry header: a 10-byte type-and-size varint (64-bit
/// size) plus a 20-byte REF_DELTA base ID.
pub const MAX_HEADER_LEN: usize = 10 + SHA1_LEN;

/// The base part of an entry header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawBase {
    /// A full object (type 1 to 4).
    None,
    /// OFS_DELTA: the base starts `distance` bytes before this entry.
    Distance(u64),
    /// REF_DELTA: the base object's ID.
    Id(ObjectId),
}

/// A decoded entry header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawHeader {
    /// Pack type code (1, 2, 3, 4, 6 or 7).
    pub type_code: u8,
    /// Inflated size of the entry's data (for a delta, of the delta itself).
    pub size: u64,
    /// Bytes taken by the type-and-size varint.
    pub size_len: u8,
    /// The base, for deltas.
    pub base: RawBase,
    /// Bytes taken by the whole header.
    pub len: u8,
}

/// Why bytes are not a valid entry header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HeaderError {
    /// The bytes end inside the header.
    #[error("truncated entry header")]
    Truncated,
    /// Type 0 or 5, which Git never writes.
    #[error("invalid pack entry type {0}")]
    BadType(u8),
    /// A varint longer than its 64-bit value allows.
    #[error("entry header varint overflows")]
    Overflow,
    /// An OFS_DELTA distance of zero (a delta based on itself).
    #[error("OFS_DELTA distance is zero")]
    ZeroDistance,
}

/// Decodes the header at the start of `bytes`.
pub fn decode(bytes: &[u8]) -> Result<RawHeader, HeaderError> {
    let mut pos = 0;
    let mut c = byte(bytes, &mut pos)?;
    let type_code = (c >> 4) & 7;
    if !matches!(type_code, 1..=4 | 6 | 7) {
        return Err(HeaderError::BadType(type_code));
    }
    let mut size = u64::from(c & 15);
    let mut shift = 4u32;
    while c & 0x80 != 0 {
        c = byte(bytes, &mut pos)?;
        if shift > 60 || (shift == 60 && c & 0x7f > 0x0f) {
            return Err(HeaderError::Overflow);
        }
        size |= u64::from(c & 0x7f) << shift;
        shift += 7;
    }
    let size_len = pos as u8;
    let base = match type_code {
        6 => {
            let mut c = byte(bytes, &mut pos)?;
            let mut distance = u64::from(c & 0x7f);
            while c & 0x80 != 0 {
                c = byte(bytes, &mut pos)?;
                distance = distance
                    .checked_add(1)
                    .and_then(|d| d.checked_mul(128))
                    .ok_or(HeaderError::Overflow)?
                    | u64::from(c & 0x7f);
            }
            if distance == 0 {
                return Err(HeaderError::ZeroDistance);
            }
            RawBase::Distance(distance)
        }
        7 => {
            let id = bytes
                .get(pos..pos + SHA1_LEN)
                .ok_or(HeaderError::Truncated)?;
            pos += SHA1_LEN;
            RawBase::Id(ObjectId::from_slice(id).expect("20 bytes"))
        }
        _ => RawBase::None,
    };
    Ok(RawHeader {
        type_code,
        size,
        size_len,
        base,
        len: pos as u8,
    })
}

fn byte(bytes: &[u8], pos: &mut usize) -> Result<u8, HeaderError> {
    let b = *bytes.get(*pos).ok_or(HeaderError::Truncated)?;
    *pos += 1;
    Ok(b)
}

/// Appends the type-and-size varint.
pub fn encode_type_and_size(type_code: u8, size: u64, out: &mut Vec<u8>) {
    let mut c = (type_code << 4) | (size & 15) as u8;
    let mut size = size >> 4;
    while size != 0 {
        out.push(c | 0x80);
        c = (size & 0x7f) as u8;
        size >>= 7;
    }
    out.push(c);
}

/// Appends Git's OFS_DELTA distance varint (big-endian groups of 7 bits,
/// each continuation group biased by one).
pub fn encode_distance(distance: u64, out: &mut Vec<u8>) {
    debug_assert!(distance > 0);
    let mut buf = [0u8; 10];
    let mut i = buf.len() - 1;
    let mut d = distance;
    buf[i] = (d & 0x7f) as u8;
    d >>= 7;
    while d != 0 {
        d -= 1;
        i -= 1;
        buf[i] = 0x80 | (d & 0x7f) as u8;
        d >>= 7;
    }
    out.extend_from_slice(&buf[i..]);
}

/// Length of [`encode_distance`]'s output for `distance`.
pub fn distance_len(distance: u64) -> usize {
    let mut n = 1;
    let mut d = distance >> 7;
    while d != 0 {
        d -= 1;
        n += 1;
        d >>= 7;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances_round_trip_at_every_varint_boundary() {
        // Git's bias makes 127 one byte, 128 two bytes, 16511 two bytes and
        // 16512 three bytes.
        for (d, len) in [
            (1, 1),
            (127, 1),
            (128, 2),
            (16511, 2),
            (16512, 3),
            (u64::MAX >> 1, 9),
        ] {
            let mut h = Vec::new();
            encode_type_and_size(6, 5, &mut h);
            encode_distance(d, &mut h);
            assert_eq!(distance_len(d), len, "{d}");
            let raw = decode(&h).unwrap();
            assert_eq!(raw.base, RawBase::Distance(d));
            assert_eq!(usize::from(raw.len), 1 + len);
        }
    }

    #[test]
    fn sizes_round_trip() {
        for size in [0, 15, 16, 2047, 2048, 1 << 40, u64::MAX] {
            let mut h = Vec::new();
            encode_type_and_size(3, size, &mut h);
            let raw = decode(&h).unwrap();
            assert_eq!((raw.type_code, raw.size), (3, size));
            assert_eq!(raw.size_len, raw.len);
        }
    }

    #[test]
    fn ref_delta_headers_carry_the_base_id() {
        let mut h = Vec::new();
        encode_type_and_size(7, 300, &mut h);
        h.extend_from_slice(&[9; 20]);
        let raw = decode(&h).unwrap();
        assert_eq!(raw.base, RawBase::Id(ObjectId::from_bytes([9; 20])));
        assert_eq!(usize::from(raw.len), h.len());
        assert_eq!(decode(&h[..h.len() - 1]), Err(HeaderError::Truncated));
    }

    #[test]
    fn malformed_headers_are_errors_not_panics() {
        assert_eq!(decode(&[]), Err(HeaderError::Truncated));
        assert_eq!(decode(&[0x50]), Err(HeaderError::BadType(5)));
        assert_eq!(decode(&[0x00]), Err(HeaderError::BadType(0)));
        assert_eq!(decode(&[0x90]), Err(HeaderError::Truncated));
        assert_eq!(decode(&[0x60, 0x00]), Err(HeaderError::ZeroDistance));
        assert_eq!(decode(&[0xb0; 12]), Err(HeaderError::Overflow));
        let mut long = vec![0x60];
        long.extend_from_slice(&[0xff; 11]);
        long.push(0);
        assert_eq!(decode(&long), Err(HeaderError::Overflow));
    }
}

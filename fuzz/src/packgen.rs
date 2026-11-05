//! A raw pack writer for the pack targets: any entry layout, including
//! the ones stock Git never writes (REF_DELTA cycles, forward bases,
//! repeated objects, thin bases, lying sizes, deep chains), plus object
//! contents shaped like commits, trees and tags.

use std::io::Write;

use aspen_core::id::ObjectId;
use aspen_core::ingest::ObjectKind;

use crate::input::Input;

/// SHA-1 of `bytes`.
pub fn sha1(bytes: &[u8]) -> [u8; 20] {
    let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
    h.update(bytes);
    let id = h
        .try_finalize()
        .expect("no collision attack in generated data");
    id.as_bytes().try_into().expect("SHA-1")
}

/// Git's name for an object kind.
pub fn kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Commit => "commit",
        ObjectKind::Tree => "tree",
        ObjectKind::Blob => "blob",
        ObjectKind::Tag => "tag",
    }
}

/// The pack type code of an object kind.
pub fn type_code(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Commit => 1,
        ObjectKind::Tree => 2,
        ObjectKind::Blob => 3,
        ObjectKind::Tag => 4,
    }
}

/// The object ID Git gives `data` of `kind`.
pub fn object_id(kind: ObjectKind, data: &[u8]) -> ObjectId {
    let mut buf = format!("{} {}\0", kind_name(kind), data.len()).into_bytes();
    buf.extend_from_slice(data);
    ObjectId::from_bytes(sha1(&buf))
}

/// zlib-deflates `data`.
pub fn deflate(data: &[u8]) -> Vec<u8> {
    let mut w =
        gix_zlib::stream::deflate::Write::new(Vec::new(), gix_zlib::Compression::BEST_SPEED);
    w.write_all(data).expect("in-memory write");
    w.flush().expect("in-memory flush");
    w.into_inner()
}

/// Git's delta-header varint.
pub fn varint(mut n: u64, out: &mut Vec<u8>) {
    loop {
        let b = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// One copy instruction (`offset`, `len` of at most 0xff_ffff).
fn copy_op(offset: u64, len: u64, out: &mut Vec<u8>) {
    let mut op = 0x80u8;
    let mut args = Vec::new();
    for i in 0..4 {
        let b = ((offset >> (8 * i)) & 0xff) as u8;
        if b != 0 {
            op |= 1 << i;
            args.push(b);
        }
    }
    for i in 0..3 {
        let b = ((len >> (8 * i)) & 0xff) as u8;
        if b != 0 {
            op |= 0x10 << i;
            args.push(b);
        }
    }
    out.push(op);
    out.extend(args);
}

/// The fewest bytes of data a delta may have: Git's `DELTA_SIZE_MIN`.
/// `git index-pack` refuses a shorter delta ("failed to apply delta"),
/// and so does ingest (issue #162). Git never writes one.
pub const MIN_DELTA_DATA: u64 = 4;

/// A valid delta from `base` to `target`: copy the common prefix and
/// suffix, insert the middle.
pub fn delta(base: &[u8], target: &[u8]) -> Vec<u8> {
    let mut d = Vec::new();
    varint(base.len() as u64, &mut d);
    varint(target.len() as u64, &mut d);
    let prefix = base.iter().zip(target).take_while(|(a, b)| a == b).count();
    let suffix = base[prefix..]
        .iter()
        .rev()
        .zip(target[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let mut at = 0u64;
    let mut left = prefix as u64;
    while left > 0 {
        let n = left.min(0xff_ffff);
        copy_op(at, n, &mut d);
        at += n;
        left -= n;
    }
    for chunk in target[prefix..target.len() - suffix].chunks(127) {
        d.push(chunk.len() as u8);
        d.extend_from_slice(chunk);
    }
    let mut at = (base.len() - suffix) as u64;
    let mut left = suffix as u64;
    while left > 0 {
        let n = left.min(0xff_ffff);
        copy_op(at, n, &mut d);
        at += n;
        left -= n;
    }
    d
}

/// Pack entry header: type and size.
pub fn entry_header(type_code: u8, size: u64, out: &mut Vec<u8>) {
    let mut b = (type_code << 4) | (size & 0x0f) as u8;
    let mut rest = size >> 4;
    while rest != 0 {
        out.push(b | 0x80);
        b = (rest & 0x7f) as u8;
        rest >>= 7;
    }
    out.push(b);
}

/// OFS_DELTA distance encoding.
pub fn ofs_distance(mut distance: u64, out: &mut Vec<u8>) {
    let mut bytes = vec![(distance & 0x7f) as u8];
    distance >>= 7;
    while distance != 0 {
        distance -= 1;
        bytes.push(0x80 | (distance & 0x7f) as u8);
        distance >>= 7;
    }
    bytes.reverse();
    out.extend(bytes);
}

/// One entry exactly as it goes on the wire.
#[derive(Debug, Clone)]
pub struct RawEntry {
    /// 1-4 for full objects, 6 OFS_DELTA, 7 REF_DELTA (others are invalid).
    pub type_code: u8,
    /// The size in the header (inflated size of `body`'s plain bytes).
    pub declared: u64,
    /// OFS_DELTA: index of the base entry (the distance is computed).
    pub ofs_base: Option<usize>,
    /// OFS_DELTA: a raw distance instead (may point anywhere).
    pub ofs_distance: Option<u64>,
    /// REF_DELTA: the base ID.
    pub ref_base: Option<ObjectId>,
    /// The deflated body.
    pub body: Vec<u8>,
}

/// A whole pack: header with `count` (or the entry count), entries, and a
/// correct trailer.
pub fn write_pack(entries: &[RawEntry], count: Option<u32>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    let n = count.unwrap_or(entries.len() as u32);
    out.extend_from_slice(&n.to_be_bytes());
    let mut offsets = Vec::with_capacity(entries.len());
    for e in entries {
        let at = out.len() as u64;
        offsets.push(at);
        entry_header(e.type_code, e.declared, &mut out);
        if let Some(b) = e.ofs_base {
            ofs_distance(at - offsets[b], &mut out);
        } else if let Some(d) = e.ofs_distance {
            ofs_distance(d, &mut out);
        }
        if let Some(id) = e.ref_base {
            out.extend_from_slice(id.as_bytes());
        }
        out.extend_from_slice(&e.body);
    }
    let sum = sha1(&out);
    out.extend_from_slice(&sum);
    out
}

/// Replaces the trailer of `pack` with the SHA-1 of everything before it.
pub fn fix_trailer(pack: &mut [u8]) {
    if pack.len() < 20 {
        return;
    }
    let n = pack.len() - 20;
    let sum = sha1(&pack[..n]);
    pack[n..].copy_from_slice(&sum);
}

/// 40 hex digits of an ID-like value from the input (sometimes malformed).
fn hex_id(input: &mut Input<'_>) -> String {
    let id = ObjectId::from_bytes(sha1(&[input.byte()]));
    match input.below(16) {
        0 => id.to_hex()[..input.below(40)].to_owned(),
        1 => id.to_hex().to_uppercase(),
        2 => format!("{}z", &id.to_hex()[..39]),
        _ => id.to_hex(),
    }
}

/// Object contents shaped like `kind`, with fuzzer-chosen deviations: the
/// parsers behind commit extraction and the push's reachability graph read
/// what clients send.
pub fn content(kind: ObjectKind, input: &mut Input<'_>) -> Vec<u8> {
    let noise = |input: &mut Input<'_>, v: &mut Vec<u8>| {
        if input.one_in(4) {
            let at = input.below(v.len() + 1);
            let junk = input.bytes(8).to_vec();
            v.splice(at..at, junk);
        }
    };
    let mut v = Vec::new();
    match kind {
        ObjectKind::Blob => v.extend_from_slice(input.bytes(96)),
        ObjectKind::Commit => {
            v.extend_from_slice(format!("tree {}\n", hex_id(input)).as_bytes());
            for _ in 0..input.below(4) {
                v.extend_from_slice(format!("parent {}\n", hex_id(input)).as_bytes());
            }
            let time = match input.below(8) {
                0 => "-1".to_owned(),
                1 => "99999999999999999999999".to_owned(),
                2 => String::new(),
                _ => input.u32().to_string(),
            };
            v.extend_from_slice(b"author A <a@x> 1 +0000\n");
            if !input.one_in(8) {
                v.extend_from_slice(format!("committer C <c@x>> {time} +0000\n").as_bytes());
            }
            v.extend_from_slice(b"\n");
            v.extend_from_slice(input.bytes(16));
        }
        ObjectKind::Tree => {
            for _ in 0..input.below(5) {
                let mode: &[u8] = match input.below(6) {
                    0 => b"40000",
                    1 => b"160000",
                    2 => b"120000",
                    3 => b"100755",
                    4 => b"0644",
                    _ => b"100644",
                };
                v.extend_from_slice(mode);
                v.push(b' ');
                let name = input.bytes(6);
                v.extend_from_slice(if name.is_empty() { b"f" } else { name });
                v.push(0);
                let id = sha1(&[input.byte()]);
                let len = if input.one_in(16) {
                    input.below(20)
                } else {
                    20
                };
                v.extend_from_slice(&id[..len]);
            }
        }
        ObjectKind::Tag => {
            v.extend_from_slice(format!("object {}\n", hex_id(input)).as_bytes());
            let t = ["commit", "tree", "blob", "tag", "nope"][input.below(5)];
            v.extend_from_slice(format!("type {t}\ntag v{}\n", input.byte()).as_bytes());
            v.extend_from_slice(b"tagger T <t@x> 1 +0000\n\nmsg\n");
        }
    }
    noise(input, &mut v);
    v
}

/// An object kind, mostly blobs.
pub fn kind(input: &mut Input<'_>) -> ObjectKind {
    match input.below(8) {
        0 => ObjectKind::Commit,
        1 => ObjectKind::Tree,
        2 => ObjectKind::Tag,
        _ => ObjectKind::Blob,
    }
}

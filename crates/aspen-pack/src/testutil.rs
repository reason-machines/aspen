//! Test helpers: a raw pack writer for layouts stock Git never produces
//! (all-REF_DELTA, forward bases, repeated objects, thin packs), and thin
//! wrappers around the `git` binary.

use std::io::Write;
use std::path::Path;
use std::process::Command;

use aspen_core::id::{ObjectId, PackId};
use aspen_core::ingest::ObjectKind;
use aspen_core::pack::EntryTable;

use crate::header;
use crate::{MaxInflatedBytes, MaxObjectBytes};

/// The object limit tests run with: 64 MiB.
pub(crate) const MAX_OBJECT: MaxObjectBytes = match MaxObjectBytes::new(64 << 20) {
    Some(m) => m,
    None => panic!(),
};
/// The total inflated limit tests run with: 1 GiB.
pub(crate) const MAX_INFLATED: MaxInflatedBytes = match MaxInflatedBytes::new(1 << 30) {
    Some(m) => m,
    None => panic!(),
};
use crate::ingest::object_id_of;

/// A Git object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Obj {
    pub kind: ObjectKind,
    pub data: Vec<u8>,
}

impl Obj {
    pub(crate) fn blob(data: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: ObjectKind::Blob,
            data: data.into(),
        }
    }

    pub(crate) fn id(&self) -> ObjectId {
        object_id_of(self.kind, &self.data)
    }
}

/// What a REF_DELTA is based on.
#[derive(Debug, Clone)]
pub(crate) enum Base {
    /// Another entry of the same pack, by index (may be later: a forward
    /// base).
    Entry(usize),
    /// An object that is not in the pack (a thin base).
    External(Obj),
}

/// One entry to write.
#[derive(Debug, Clone)]
pub(crate) enum Spec {
    Full(Obj),
    Ref(Base, Obj),
    /// OFS_DELTA against an earlier entry.
    Ofs(usize, Obj),
}

impl Spec {
    fn obj(&self) -> &Obj {
        match self {
            Spec::Full(o) | Spec::Ref(_, o) | Spec::Ofs(_, o) => o,
        }
    }
}

pub(crate) fn deflate(data: &[u8]) -> Vec<u8> {
    let mut w =
        gix_zlib::stream::deflate::Write::new(Vec::new(), gix_zlib::Compression::BEST_SPEED);
    w.write_all(data).unwrap();
    w.flush().unwrap();
    w.into_inner()
}

fn varint(mut n: usize, out: &mut Vec<u8>) {
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

/// A valid Git delta from `base` to `target`: copy their common prefix,
/// then insert the rest.
pub(crate) fn delta(base: &[u8], target: &[u8]) -> Vec<u8> {
    let mut d = Vec::new();
    varint(base.len(), &mut d);
    varint(target.len(), &mut d);
    let prefix = base
        .iter()
        .zip(target)
        .take_while(|(a, b)| a == b)
        .count()
        .min(0xff_ffff);
    if prefix > 0 {
        // Copy from offset 0 (no offset bytes), size in up to 3 bytes.
        let mut op = 0x80u8;
        let mut args = Vec::new();
        for i in 0..3 {
            let b = ((prefix >> (8 * i)) & 0xff) as u8;
            if b != 0 {
                op |= 0x10 << i;
                args.push(b);
            }
        }
        d.push(op);
        d.extend(args);
    }
    for chunk in target[prefix..].chunks(127) {
        d.push(chunk.len() as u8);
        d.extend_from_slice(chunk);
    }
    d
}

fn type_code(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Commit => 1,
        ObjectKind::Tree => 2,
        ObjectKind::Blob => 3,
        ObjectKind::Tag => 4,
    }
}

/// Writes a version 2 pack with exactly these entries, in this order.
pub(crate) fn build_pack(specs: &[Spec]) -> Vec<u8> {
    build_pack_with_offsets(specs).0
}

/// [`build_pack`], plus the offset of each entry in the pack.
pub(crate) fn build_pack_with_offsets(specs: &[Spec]) -> (Vec<u8>, Vec<u64>) {
    let mut out = Vec::new();
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    out.extend_from_slice(&(specs.len() as u32).to_be_bytes());
    let mut offsets = Vec::new();
    for spec in specs {
        let at = out.len() as u64;
        offsets.push(at);
        let (code, body, base) = match spec {
            Spec::Full(o) => (type_code(o.kind), o.data.clone(), None),
            Spec::Ofs(b, o) => {
                let base = specs[*b].obj();
                let mut dist = Vec::new();
                header::encode_distance(at - offsets[*b], &mut dist);
                (6, delta(&base.data, &o.data), Some(dist))
            }
            Spec::Ref(b, o) => {
                let base = match b {
                    Base::Entry(j) => specs[*j].obj().clone(),
                    Base::External(x) => x.clone(),
                };
                assert_eq!(base.kind, o.kind, "a delta keeps its base's type");
                (
                    7,
                    delta(&base.data, &o.data),
                    Some(base.id().as_bytes().to_vec()),
                )
            }
        };
        header::encode_type_and_size(code, body.len() as u64, &mut out);
        out.extend(base.unwrap_or_default());
        out.extend(deflate(&body));
    }
    let sum = crate::sha1(&out);
    out.extend_from_slice(sum.as_bytes());
    (out, offsets)
}

/// Runs `git` with `args` in `dir`; panics with its stderr on failure.
pub(crate) fn git(dir: &Path, args: &[&str]) -> String {
    let out = git_raw(dir, args, None);
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

pub(crate) fn git_raw(dir: &Path, args: &[&str], stdin: Option<&[u8]>) -> std::process::Output {
    let mut cmd = Command::new("git");
    cmd.current_dir(dir)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "A2")
        .env("GIT_AUTHOR_EMAIL", "a2@example.invalid")
        .env("GIT_COMMITTER_NAME", "A2")
        .env("GIT_COMMITTER_EMAIL", "a2@example.invalid")
        .env("GIT_AUTHOR_DATE", "2026-11-03T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-11-03T00:00:00Z");
    if let Some(input) = stdin {
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        child.stdin.take().unwrap().write_all(input).unwrap();
        child.wait_with_output().unwrap()
    } else {
        cmd.output().unwrap()
    }
}

/// `git index-pack` on a copy of `pack` (without `--fix-thin`); returns the
/// sorted object IDs it lists, or its stderr.
pub(crate) fn git_index_pack(pack: &[u8], dir: &Path, strict: bool) -> Result<Vec<String>, String> {
    let p = dir.join(format!("check-{}.pack", crate::sha1(pack)));
    std::fs::write(&p, pack).unwrap();
    let mut args = vec!["index-pack"];
    if strict {
        args.push("--strict");
    }
    let ps = p.to_str().unwrap().to_owned();
    args.push(&ps);
    let out = git_raw(dir, &args, None);
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).into_owned());
    }
    let idx = p.with_extension("idx");
    let listed = git_raw(dir, &["show-index"], Some(&std::fs::read(&idx).unwrap()));
    let mut ids: Vec<String> = String::from_utf8(listed.stdout)
        .unwrap()
        .lines()
        .map(|l| l.split_whitespace().nth(1).unwrap().to_owned())
        .collect();
    ids.sort();
    Ok(ids)
}

/// Writes `pack` to `dir`, verifies it with [`crate::verify::verify_pack`],
/// and returns its entry table.
pub(crate) fn table_of(pack: &[u8], dir: &Path) -> EntryTable {
    let p = dir.join(format!("t-{}.pack", crate::sha1(pack)));
    std::fs::write(&p, pack).unwrap();
    crate::verify::verify_pack(
        &p,
        dir,
        crate::testutil::MAX_OBJECT,
        crate::testutil::MAX_INFLATED,
    )
    .unwrap_or_else(|e| panic!("{e}"))
    .table
}

pub(crate) fn pack_id(n: i64) -> PackId {
    PackId::new(n).unwrap()
}

/// A scratch directory under the target dir's tmp (a real disk).
pub(crate) fn scratch() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// A Git delta copy instruction for `len` bytes at `offset` of the base.
fn copy_op(offset: usize, mut len: usize, out: &mut Vec<u8>) {
    let mut offset = offset;
    while len > 0 {
        let n = len.min(0xff_ffff);
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
            let b = ((n >> (8 * i)) & 0xff) as u8;
            if b != 0 {
                op |= 0x10 << i;
                args.push(b);
            }
        }
        out.push(op);
        out.extend(args);
        offset += n;
        len -= n;
    }
}

/// A pack that makes a delta resolver hold `levels` full objects of `size`
/// bytes at once although it is about `size / 1000 + 100 × levels` bytes on
/// the wire. Entry 0 is a blob of `size` zeros; level `i` (1-based) is that
/// blob with its first `i` bytes set to 1, an OFS_DELTA of a few bytes on
/// level `i - 1`. Each level `i - 1` has two children, written in this
/// order: a tiny leaf, then level `i`. A resolver working LIFO resolves
/// level `i` first while the leaf still holds level `i - 1`, so every level
/// on the path stays alive until the end.
pub(crate) fn ladder_pack(levels: usize, size: usize) -> Vec<u8> {
    assert!(levels < size);
    let mut out = Vec::new();
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    out.extend_from_slice(&(1 + 2 * levels as u32).to_be_bytes());
    let mut prev = out.len() as u64;
    header::encode_type_and_size(3, size as u64, &mut out);
    out.extend(deflate(&vec![0u8; size]));
    let ofs = |out: &mut Vec<u8>, base: u64, delta: &[u8]| -> u64 {
        let at = out.len() as u64;
        header::encode_type_and_size(6, delta.len() as u64, out);
        header::encode_distance(at - base, out);
        out.extend(deflate(delta));
        at
    };
    for i in 1..=levels {
        let leaf = format!("leaf {i}");
        let mut d = Vec::new();
        varint(size, &mut d);
        varint(leaf.len(), &mut d);
        d.push(leaf.len() as u8);
        d.extend_from_slice(leaf.as_bytes());
        ofs(&mut out, prev, &d);
        let mut d = Vec::new();
        varint(size, &mut d);
        varint(size, &mut d);
        if i > 1 {
            copy_op(0, i - 1, &mut d);
        }
        d.extend_from_slice(&[1, 1]);
        copy_op(i, size - i, &mut d);
        prev = ofs(&mut out, prev, &d);
    }
    let sum = crate::sha1(&out);
    out.extend_from_slice(sum.as_bytes());
    out
}

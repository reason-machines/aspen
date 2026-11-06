//! The Git objects the merge queue reads and writes itself: trees,
//! commits, annotated tags, a pack of them, and the atomic
//! `git-receive-pack` request that hands the pack to Aspen in process.
//!
//! Everything the queue writes goes through Aspen's own push path
//! ([`crate::receive::ReceivePack::push`]), so it is checked, completed,
//! stored and committed exactly like a `git push`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;

use aspen_core::id::ObjectId;
use aspen_core::ingest::ObjectKind;
use sha1::{Digest, Sha1};

/// Tree entry mode of a directory.
pub(crate) const MODE_TREE: u32 = 0o40000;
/// Tree entry mode of a submodule (gitlink): it names a commit of another
/// repository, which no pack here holds.
pub(crate) const MODE_GITLINK: u32 = 0o160000;

/// The ID Git gives an object.
pub(crate) fn object_id(kind: ObjectKind, data: &[u8]) -> ObjectId {
    let mut h = Sha1::new();
    h.update(format!("{} {}\0", kind_name(kind), data.len()).as_bytes());
    h.update(data);
    ObjectId::from_bytes(h.finalize().into())
}

pub(crate) fn kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Commit => "commit",
        ObjectKind::Tree => "tree",
        ObjectKind::Blob => "blob",
        ObjectKind::Tag => "tag",
    }
}

/// One entry of a tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    /// Mode (`0o100644`, `0o40000`, ...).
    pub(crate) mode: u32,
    /// The object.
    pub(crate) id: ObjectId,
}

impl Entry {
    /// Whether the entry is a directory.
    pub(crate) fn is_tree(&self) -> bool {
        self.mode == MODE_TREE
    }
}

/// A tree's entries by name.
pub(crate) type Tree = BTreeMap<Vec<u8>, Entry>;

/// Parses a tree object.
pub(crate) fn parse_tree(data: &[u8]) -> Result<Tree, String> {
    let mut out = Tree::new();
    let mut rest = data;
    while !rest.is_empty() {
        let sp = rest
            .iter()
            .position(|&b| b == b' ')
            .ok_or("tree entry without a mode")?;
        let mode = std::str::from_utf8(&rest[..sp])
            .ok()
            .and_then(|m| u32::from_str_radix(m, 8).ok())
            .ok_or("tree entry with a bad mode")?;
        rest = &rest[sp + 1..];
        let nul = rest
            .iter()
            .position(|&b| b == 0)
            .ok_or("tree entry without a name")?;
        let name = rest[..nul].to_vec();
        rest = &rest[nul + 1..];
        if rest.len() < 20 {
            return Err("tree entry cut short".into());
        }
        let mut id = [0u8; 20];
        id.copy_from_slice(&rest[..20]);
        rest = &rest[20..];
        out.insert(
            name,
            Entry {
                mode,
                id: ObjectId::from_bytes(id),
            },
        );
    }
    Ok(out)
}

/// Writes a tree object, ordering the entries the way Git does (a
/// directory sorts as if its name ended in `/`).
pub(crate) fn write_tree(tree: &Tree) -> Vec<u8> {
    let mut entries: Vec<(&Vec<u8>, &Entry)> = tree.iter().collect();
    let key = |(name, e): &(&Vec<u8>, &Entry)| {
        let mut k = (*name).clone();
        if e.is_tree() {
            k.push(b'/');
        }
        k
    };
    entries.sort_by_key(key);
    let mut out = Vec::new();
    for (name, e) in entries {
        write!(out, "{:o} ", e.mode).expect("Vec write");
        out.extend_from_slice(name);
        out.push(0);
        out.extend_from_slice(e.id.as_bytes());
    }
    out
}

/// What the queue keeps of a commit it rewrites.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Commit {
    pub(crate) tree: ObjectId,
    pub(crate) parents: Vec<ObjectId>,
    /// The `author` header's value, kept byte for byte.
    pub(crate) author: Vec<u8>,
    /// The `encoding` header's value, if any.
    pub(crate) encoding: Option<Vec<u8>>,
    /// Everything after the blank line that ends the headers.
    pub(crate) message: Vec<u8>,
}

/// Parses a commit object. Signatures (`gpgsig`) and merge tags are
/// dropped: a rewritten commit has another ID, so they no longer verify.
pub(crate) fn parse_commit(data: &[u8]) -> Result<Commit, String> {
    let (head, message) = match data.windows(2).position(|w| w == b"\n\n") {
        Some(at) => (&data[..at], data[at + 2..].to_vec()),
        None => (data, Vec::new()),
    };
    let mut tree = None;
    let mut parents = Vec::new();
    let mut author = None;
    let mut encoding = None;
    let hex = |s: &[u8]| {
        std::str::from_utf8(s)
            .ok()
            .and_then(|s| s.parse::<ObjectId>().ok())
            .ok_or_else(|| "commit with a bad object ID".to_owned())
    };
    for line in head.split(|&b| b == b'\n') {
        if let Some(t) = line.strip_prefix(b"tree ") {
            tree = Some(hex(t)?);
        } else if let Some(p) = line.strip_prefix(b"parent ") {
            parents.push(hex(p)?);
        } else if let Some(a) = line.strip_prefix(b"author ") {
            author = Some(a.to_vec());
        } else if let Some(e) = line.strip_prefix(b"encoding ") {
            encoding = Some(e.to_vec());
        }
    }
    Ok(Commit {
        tree: tree.ok_or("commit without a tree")?,
        parents,
        author: author.ok_or("commit without an author")?,
        encoding,
        message,
    })
}

/// The identity the queue commits and tags as.
pub(crate) const QUEUE_IDENT: &str = "Aspen merge queue <merge-queue@aspen.invalid>";

/// Writes `commit` with a new tree and parents, committed by the queue at
/// `unix_seconds`.
pub(crate) fn write_commit(
    commit: &Commit,
    tree: ObjectId,
    parents: &[ObjectId],
    unix_seconds: i64,
) -> Vec<u8> {
    let mut out = format!("tree {tree}\n").into_bytes();
    for p in parents {
        out.extend_from_slice(format!("parent {p}\n").as_bytes());
    }
    out.extend_from_slice(b"author ");
    out.extend_from_slice(&commit.author);
    out.push(b'\n');
    out.extend_from_slice(format!("committer {QUEUE_IDENT} {unix_seconds} +0000\n").as_bytes());
    if let Some(e) = &commit.encoding {
        out.extend_from_slice(b"encoding ");
        out.extend_from_slice(e);
        out.push(b'\n');
    }
    out.push(b'\n');
    out.extend_from_slice(&commit.message);
    out
}

/// Writes a commit by the queue holding `message`, with `tree` and the
/// one parent `parent` (a rejection report).
pub(crate) fn write_note_commit(
    tree: ObjectId,
    parent: ObjectId,
    unix_seconds: i64,
    message: &str,
) -> Vec<u8> {
    let mut out = format!(
        "tree {tree}\nparent {parent}\nauthor {QUEUE_IDENT} {unix_seconds} +0000\ncommitter {QUEUE_IDENT} {unix_seconds} +0000\n\n"
    );
    out.push_str(message.trim_end_matches('\n'));
    out.push('\n');
    out.into_bytes()
}

/// Every object of a whole pack file (`bytes`, trailer included), each
/// resolved to its full content. Deltas must be based on objects of the
/// same pack, as every stored pack's are (stored packs are
/// self-contained). The merge queue reads a queued change's whole pack in
/// one read this way, instead of one read per object it needs.
pub(crate) fn parse_pack(
    bytes: &[u8],
    max_object: u64,
) -> Result<HashMap<ObjectId, (ObjectKind, Vec<u8>)>, String> {
    use aspen_pack::header::{RawBase, decode};
    if bytes.len() < 12 + 20 || &bytes[..4] != b"PACK" {
        return Err("not a pack".into());
    }
    let count = u32::from_be_bytes(bytes[8..12].try_into().expect("4 bytes")) as usize;
    let end = bytes.len() - 20;
    let mut at = 12usize;
    // Resolved objects by offset, for OFS_DELTA bases; ids for REF_DELTA.
    let mut by_offset: HashMap<u64, (ObjectKind, ObjectId)> = HashMap::with_capacity(count);
    let mut out: HashMap<ObjectId, (ObjectKind, Vec<u8>)> = HashMap::with_capacity(count);
    for _ in 0..count {
        if at >= end {
            return Err("the pack ends before its last entry".into());
        }
        let h = decode(&bytes[at..end]).map_err(|e| e.to_string())?;
        if h.size > max_object {
            return Err(format!("an entry of {} bytes is over the limit", h.size));
        }
        let data_at = at + usize::from(h.len);
        let mut z = flate2::Decompress::new(true);
        let mut data = Vec::with_capacity(h.size as usize);
        z.decompress_vec(
            &bytes[data_at..end],
            &mut data,
            flate2::FlushDecompress::Finish,
        )
        .map_err(|e| format!("inflating the entry at {at}: {e}"))?;
        if data.len() as u64 != h.size {
            return Err(format!("the entry at {at} inflates to the wrong size"));
        }
        let (kind, data) = match h.base {
            RawBase::None => (kind_of(h.type_code)?, data),
            RawBase::Distance(d) => {
                let base_at = (at as u64)
                    .checked_sub(d)
                    .ok_or_else(|| format!("OFS_DELTA at {at} points before the pack"))?;
                let (kind, id) = by_offset
                    .get(&base_at)
                    .copied()
                    .ok_or_else(|| format!("OFS_DELTA at {at}: no entry at {base_at}"))?;
                let base = &out[&id].1;
                (
                    kind,
                    aspen_pack::locate::apply_delta(base, &data, max_object)?,
                )
            }
            RawBase::Id(base) => {
                let (kind, b) = out.get(&base).ok_or_else(|| {
                    format!("REF_DELTA at {at}: {base} is not earlier in the pack")
                })?;
                (
                    *kind,
                    aspen_pack::locate::apply_delta(b, &data, max_object)?,
                )
            }
        };
        let id = object_id(kind, &data);
        by_offset.insert(at as u64, (kind, id));
        out.insert(id, (kind, data));
        at = data_at + z.total_in() as usize;
    }
    Ok(out)
}

fn kind_of(type_code: u8) -> Result<ObjectKind, String> {
    Ok(match type_code {
        1 => ObjectKind::Commit,
        2 => ObjectKind::Tree,
        3 => ObjectKind::Blob,
        4 => ObjectKind::Tag,
        t => return Err(format!("entry type {t} is not an object")),
    })
}

/// A pack of whole (undeltified) objects, each written once, in the order
/// added.
#[derive(Debug, Default)]
pub(crate) struct PackBuilder {
    objects: Vec<(ObjectKind, Vec<u8>)>,
    ids: HashSet<ObjectId>,
}

impl PackBuilder {
    /// Adds an object (once) and returns its ID.
    pub(crate) fn add(&mut self, kind: ObjectKind, data: Vec<u8>) -> ObjectId {
        let id = object_id(kind, &data);
        if self.ids.insert(id) {
            self.objects.push((kind, data));
        }
        id
    }

    /// Whether the pack holds `id`.
    pub(crate) fn has(&self, id: &ObjectId) -> bool {
        self.ids.contains(id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// The pack file's bytes.
    pub(crate) fn finish(self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"PACK");
        out.extend_from_slice(&2u32.to_be_bytes());
        out.extend_from_slice(&(self.objects.len() as u32).to_be_bytes());
        for (kind, data) in self.objects {
            let ty: u8 = match kind {
                ObjectKind::Commit => 1,
                ObjectKind::Tree => 2,
                ObjectKind::Blob => 3,
                ObjectKind::Tag => 4,
            };
            let mut size = data.len() as u64;
            let mut byte = (ty << 4) | (size & 0x0f) as u8;
            size >>= 4;
            while size > 0 {
                out.push(byte | 0x80);
                byte = (size & 0x7f) as u8;
                size >>= 7;
            }
            out.push(byte);
            let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
            z.write_all(&data).expect("Vec write");
            out.extend_from_slice(&z.finish().expect("Vec write"));
        }
        let trailer: [u8; 20] = Sha1::digest(&out).into();
        out.extend_from_slice(&trailer);
        out
    }
}

fn pkt(out: &mut Vec<u8>, line: &str) {
    out.extend_from_slice(format!("{:04x}", line.len() + 4).as_bytes());
    out.extend_from_slice(line.as_bytes());
}

/// One command of an in-process push: `old` to `new` (zero for a create
/// or a delete).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Command {
    pub(crate) name: String,
    pub(crate) old: ObjectId,
    pub(crate) new: ObjectId,
}

/// The body of an `--atomic` `POST git-receive-pack` for `commands`, with
/// `pack` (a push of deletes only has none). Asks for `report-status`
/// without side-band, so [`parse_report`] reads the answer.
pub(crate) fn atomic_request(commands: &[Command], pack: Option<Vec<u8>>) -> Vec<u8> {
    request(commands, pack, true)
}

/// A receive-pack request for `commands`, atomic or not.
pub(crate) fn request(commands: &[Command], pack: Option<Vec<u8>>, atomic: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let atomic = if atomic { " atomic" } else { "" };
    for (i, c) in commands.iter().enumerate() {
        if i == 0 {
            pkt(
                &mut out,
                &format!(
                    "{} {} {}\0report-status{atomic} object-format=sha1 agent=aspen-merge-queue\n",
                    c.old, c.new, c.name
                ),
            );
        } else {
            pkt(&mut out, &format!("{} {} {}\n", c.old, c.new, c.name));
        }
    }
    out.extend_from_slice(b"0000");
    if let Some(p) = pack {
        out.extend_from_slice(&p);
    }
    out
}

/// What receive-pack answered: `Ok(())` when every command is `ok`,
/// otherwise the first refusal (`<ref> <reason>`, or the unpack error).
/// How many commands receive-pack's report answered `ok`.
pub(crate) fn count_ok(body: &[u8]) -> u64 {
    let mut rest = body;
    let mut n = 0;
    while rest.len() >= 4 {
        let Some(len) = std::str::from_utf8(&rest[..4])
            .ok()
            .and_then(|h| usize::from_str_radix(h, 16).ok())
        else {
            break;
        };
        if len == 0 {
            rest = &rest[4..];
            continue;
        }
        if len < 4 || len > rest.len() {
            break;
        }
        if rest[4..len].starts_with(b"ok ") {
            n += 1;
        }
        rest = &rest[len..];
    }
    n
}

pub(crate) fn parse_report(body: &[u8]) -> Result<(), String> {
    let mut rest = body;
    let mut seen_ok = false;
    while rest.len() >= 4 {
        let Some(len) = std::str::from_utf8(&rest[..4])
            .ok()
            .and_then(|h| usize::from_str_radix(h, 16).ok())
        else {
            break;
        };
        if len == 0 {
            rest = &rest[4..];
            continue;
        }
        if len < 4 || len > rest.len() {
            break;
        }
        let line = String::from_utf8_lossy(&rest[4..len]);
        let line = line.trim_end_matches('\n');
        rest = &rest[len..];
        if let Some(e) = line.strip_prefix("ERR ") {
            return Err(e.to_owned());
        } else if let Some(u) = line.strip_prefix("unpack ") {
            if u != "ok" {
                return Err(format!("unpack failed: {u}"));
            }
        } else if line.starts_with("ok ") {
            seen_ok = true;
        } else if let Some(ng) = line.strip_prefix("ng ") {
            return Err(ng.to_owned());
        }
    }
    if seen_ok {
        Ok(())
    } else {
        Err("no report from receive-pack".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_built_pack_parses_back_to_its_objects() {
        let mut pb = PackBuilder::default();
        let blob = pb.add(ObjectKind::Blob, b"hello\n".to_vec());
        let tree = pb.add(
            ObjectKind::Tree,
            write_tree(&Tree::from([(
                b"a.txt".to_vec(),
                Entry {
                    mode: 0o100644,
                    id: blob,
                },
            )])),
        );
        let objs = parse_pack(&pb.finish(), 1 << 20).unwrap();
        assert_eq!(objs.len(), 2);
        assert_eq!(objs[&blob], (ObjectKind::Blob, b"hello\n".to_vec()));
        assert_eq!(objs[&tree].0, ObjectKind::Tree);
    }

    #[test]
    fn object_ids_match_git() {
        assert_eq!(
            object_id(ObjectKind::Tree, b"").to_string(),
            "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
        );
    }

    #[test]
    fn trees_round_trip_in_git_order() {
        let id = object_id(ObjectKind::Blob, b"x");
        let mut t = Tree::new();
        t.insert(b"a.txt".to_vec(), Entry { mode: 0o100644, id });
        t.insert(
            b"a".to_vec(),
            Entry {
                mode: MODE_TREE,
                id,
            },
        );
        t.insert(b"a-b".to_vec(), Entry { mode: 0o100644, id });
        let bytes = write_tree(&t);
        // "a-b" < "a.txt" < "a/" in Git's order.
        let names: Vec<&[u8]> = bytes
            .split(|&b| b == 0)
            .filter_map(|s| s.iter().position(|&b| b == b' ').map(|p| &s[p + 1..]))
            .collect();
        assert_eq!(names, vec![&b"a-b"[..], b"a.txt", b"a"]);
        assert_eq!(parse_tree(&bytes).unwrap(), t);
    }

    #[test]
    fn commits_keep_author_and_message_and_drop_signatures() {
        let tree = object_id(ObjectKind::Tree, b"");
        let raw = format!(
            "tree {tree}\nparent {tree}\nauthor A <a@x> 1 +0100\ncommitter C <c@x> 2 +0000\ngpgsig -----BEGIN\n -----END\n\nsubject\n\nbody\n"
        );
        let c = parse_commit(raw.as_bytes()).unwrap();
        assert_eq!(c.author, b"A <a@x> 1 +0100");
        assert_eq!(c.message, b"subject\n\nbody\n");
        let out = String::from_utf8(write_commit(&c, tree, &[tree], 5)).unwrap();
        assert!(out.contains("author A <a@x> 1 +0100\n"));
        assert!(out.contains(&format!("committer {QUEUE_IDENT} 5 +0000\n")));
        assert!(!out.contains("gpgsig"));
        assert!(out.ends_with("\n\nsubject\n\nbody\n"));
    }
}

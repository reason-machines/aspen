//! The few Git objects `place` writes and reads itself: tile blobs, the
//! `tiles/` and root trees, commits, a pack of them, and the
//! `git-receive-pack` request that hands that pack to Aspen in process.
//!
//! Everything the server writes goes through Aspen's own push path
//! ([`aspen_server::receive::ReceivePack::push`]), so it is checked,
//! completed, stored and committed exactly like a `git push`.

use std::io::Write;

use aspen_core::error::RejectReason;
use aspen_core::id::ObjectId;
use aspen_core::ingest::ObjectKind;
use sha1::{Digest, Sha1};

/// Tree entry mode of a directory.
pub const MODE_TREE: u32 = 0o40000;
/// Tree entry mode of a regular file.
pub const MODE_FILE: u32 = 0o100644;

/// The ID Git gives an object.
pub fn object_id(kind: ObjectKind, data: &[u8]) -> ObjectId {
    let mut h = Sha1::new();
    h.update(format!("{} {}\0", kind_name(kind), data.len()).as_bytes());
    h.update(data);
    ObjectId::from_bytes(h.finalize().into())
}

fn kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Commit => "commit",
        ObjectKind::Tree => "tree",
        ObjectKind::Blob => "blob",
        ObjectKind::Tag => "tag",
    }
}

/// One entry of a tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    /// Mode (`0o100644`, `0o40000`, ...).
    pub mode: u32,
    /// File name, any bytes but `/` and NUL.
    pub name: Vec<u8>,
    /// The object.
    pub id: ObjectId,
}

impl TreeEntry {
    /// Whether the entry is a directory.
    pub fn is_tree(&self) -> bool {
        self.mode == MODE_TREE
    }
}

/// Parses a tree object.
pub fn parse_tree(data: &[u8]) -> Result<Vec<TreeEntry>, String> {
    let mut out = Vec::new();
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
        out.push(TreeEntry {
            mode,
            name,
            id: ObjectId::from_bytes(id),
        });
    }
    Ok(out)
}

/// Writes a tree object, sorting the entries the way Git does (a
/// directory sorts as if its name ended in `/`).
pub fn write_tree(mut entries: Vec<TreeEntry>) -> Vec<u8> {
    let key = |e: &TreeEntry| {
        let mut k = e.name.clone();
        if e.is_tree() {
            k.push(b'/');
        }
        k
    };
    entries.sort_by_key(key);
    let mut out = Vec::new();
    for e in entries {
        write!(out, "{:o} ", e.mode).expect("Vec write");
        out.extend_from_slice(&e.name);
        out.push(0);
        out.extend_from_slice(e.id.as_bytes());
    }
    out
}

/// The tree and parents of a commit object.
pub fn parse_commit(data: &[u8]) -> Result<(ObjectId, Vec<ObjectId>), String> {
    let mut tree = None;
    let mut parents = Vec::new();
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            break;
        }
        let hex = |s: &[u8]| {
            std::str::from_utf8(s)
                .ok()
                .and_then(|s| s.parse::<ObjectId>().ok())
                .ok_or_else(|| "commit with a bad object ID".to_owned())
        };
        if let Some(t) = line.strip_prefix(b"tree ") {
            tree = Some(hex(t)?);
        } else if let Some(p) = line.strip_prefix(b"parent ") {
            parents.push(hex(p)?);
        }
    }
    Ok((tree.ok_or("commit without a tree")?, parents))
}

/// Writes a commit object by `who` (`Name <email>`) at `unix_seconds`.
pub fn write_commit(
    tree: ObjectId,
    parents: &[ObjectId],
    who: &str,
    unix_seconds: i64,
    message: &str,
) -> Vec<u8> {
    write_commit_by(tree, parents, who, who, unix_seconds, message)
}

/// [`write_commit`] with an author other than the committer (a persona's
/// stroke: the persona is the author, the server the committer).
pub fn write_commit_by(
    tree: ObjectId,
    parents: &[ObjectId],
    author: &str,
    who: &str,
    unix_seconds: i64,
    message: &str,
) -> Vec<u8> {
    let mut out = format!("tree {tree}\n");
    for p in parents {
        out.push_str(&format!("parent {p}\n"));
    }
    out.push_str(&format!("author {author} {unix_seconds} +0000\n"));
    out.push_str(&format!("committer {who} {unix_seconds} +0000\n\n"));
    // One message, any text, ending in one newline; no NUL bytes.
    let message = message.replace('\0', " ");
    out.push_str(message.trim_end_matches('\n'));
    out.push('\n');
    out.into_bytes()
}

/// A pack of whole (undeltified) objects. Objects are written once each,
/// in the order added.
#[derive(Debug, Default)]
pub struct PackBuilder {
    objects: Vec<(ObjectKind, Vec<u8>)>,
    ids: std::collections::HashSet<ObjectId>,
}

impl PackBuilder {
    /// Adds an object (once) and returns its ID.
    pub fn add(&mut self, kind: ObjectKind, data: Vec<u8>) -> ObjectId {
        let id = object_id(kind, &data);
        if self.ids.insert(id) {
            self.objects.push((kind, data));
        }
        id
    }

    /// The pack file's bytes.
    pub fn finish(self) -> Vec<u8> {
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

/// The body of `POST git-receive-pack` for one ref update: `old` to `new`
/// (zero for a create or a delete), with `pack` (none for a delete).
/// Asks for `report-status` without side-band, so [`parse_report`] reads
/// the answer.
pub fn receive_request(name: &str, old: ObjectId, new: ObjectId, pack: Option<Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::new();
    pkt(
        &mut out,
        &format!("{old} {new} {name}\0report-status object-format=sha1 agent=aspen-place\n"),
    );
    out.extend_from_slice(b"0000");
    if let Some(p) = pack {
        out.extend_from_slice(&p);
    }
    out
}

/// Like [`receive_request`], with the `force` push option: the forced
/// update of `name` from `old` to `new` with `pack`.
pub fn forced_receive_request(name: &str, old: ObjectId, new: ObjectId, pack: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::new();
    pkt(
        &mut out,
        &format!(
            "{old} {new} {name}\0report-status push-options object-format=sha1 agent=aspen-place\n"
        ),
    );
    out.extend_from_slice(b"0000");
    pkt(&mut out, "force\n");
    out.extend_from_slice(b"0000");
    out.extend_from_slice(&pack);
    out
}

/// What receive-pack answered for the one ref of a [`receive_request`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Report {
    /// `ok <ref>`.
    Ok,
    /// `ng <ref> <reason>`, or an unpack failure.
    Rejected(String),
}

impl Report {
    /// The receive-pack reason of a rejection, when its text is exactly one
    /// of [`RejectReason::message`]; `None` for an acceptance, a push
    /// hook's own text, or an unpack failure.
    pub fn reason(&self) -> Option<RejectReason> {
        match self {
            Self::Ok => None,
            Self::Rejected(m) => RejectReason::ALL.into_iter().find(|r| r.message() == m),
        }
    }
}

/// Reads a `report-status` answer (no side-band) for a single-ref push.
pub fn parse_report(body: &[u8]) -> Report {
    let mut rest = body;
    let mut unpack: Option<String> = None;
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
            return Report::Rejected(e.to_owned());
        } else if let Some(u) = line.strip_prefix("unpack ") {
            if u != "ok" {
                unpack = Some(format!("unpack failed: {u}"));
            }
        } else if line.starts_with("ok ") {
            return match unpack {
                Some(u) => Report::Rejected(u),
                None => Report::Ok,
            };
        } else if let Some(ng) = line.strip_prefix("ng ") {
            let reason = ng.split_once(' ').map_or(ng, |(_, r)| r);
            return Report::Rejected(unpack.unwrap_or_else(|| reason.to_owned()));
        }
    }
    Report::Rejected(unpack.unwrap_or_else(|| "no report from receive-pack".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_ids_match_git() {
        // `git hash-object --stdin <<< ''` and the empty tree.
        assert_eq!(
            object_id(ObjectKind::Blob, b"\n").to_string(),
            "8b137891791fe96927ad78e64b0aad7bded08bdc"
        );
        assert_eq!(
            object_id(ObjectKind::Tree, b"").to_string(),
            "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
        );
    }

    #[test]
    fn trees_sort_directories_as_if_they_ended_in_a_slash() {
        let id = object_id(ObjectKind::Blob, b"x");
        let e = |name: &str, mode| TreeEntry {
            mode,
            name: name.into(),
            id,
        };
        let t = write_tree(vec![
            e("a.b", MODE_FILE),
            e("a", MODE_TREE),
            e("a-", MODE_FILE),
        ]);
        let names: Vec<_> = parse_tree(&t)
            .unwrap()
            .into_iter()
            .map(|e| String::from_utf8(e.name).unwrap())
            .collect();
        // "a-" (0x2d) < "a.b" (0x2e) < "a/" (0x2f).
        assert_eq!(names, ["a-", "a.b", "a"]);
    }

    #[test]
    fn reports_are_read_per_ref() {
        let mut ok = Vec::new();
        pkt(&mut ok, "unpack ok\n");
        pkt(&mut ok, "ok refs/heads/x\n");
        ok.extend_from_slice(b"0000");
        assert_eq!(parse_report(&ok), Report::Ok);
        let mut ng = Vec::new();
        pkt(&mut ng, "unpack ok\n");
        pkt(&mut ng, "ng refs/heads/x non-fast-forward\n");
        assert_eq!(
            parse_report(&ng),
            Report::Rejected("non-fast-forward".into())
        );
        let mut unpack = Vec::new();
        pkt(&mut unpack, "unpack index-pack failed\n");
        pkt(&mut unpack, "ng refs/heads/x unpacker error\n");
        assert_eq!(
            parse_report(&unpack),
            Report::Rejected("unpack failed: index-pack failed".into())
        );
        assert_eq!(parse_report(&unpack).reason(), None);
    }

    #[test]
    fn every_receive_pack_reason_reads_back_typed_and_hook_text_does_not() {
        for r in RejectReason::ALL {
            let mut ng = Vec::new();
            pkt(&mut ng, "unpack ok\n");
            pkt(&mut ng, &format!("ng refs/heads/x {}\n", r.message()));
            assert_eq!(parse_report(&ng).reason(), Some(r), "{r:?}");
        }
        assert_eq!(Report::Ok.reason(), None);
        assert_eq!(
            Report::Rejected("x: no such ref here".into()).reason(),
            None
        );
    }
}

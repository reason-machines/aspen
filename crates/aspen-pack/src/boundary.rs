//! The commits a received pack is built on: its **boundary**.
//!
//! A client leaves out of its pack everything it believes the server has.
//! Through the plain repository URL that used to be only trunk's history,
//! which every push searches. With `push.negotiate`, `git push` first runs a
//! negotiation-only fetch, and every commit Aspen acknowledges there is left
//! out too, with everything it reaches, even when it belongs to another
//! branch than the one pushed. Receive-pack then has to find where those
//! objects are stored before it can complete the pack's thin deltas, and it
//! cannot complete the pack first to find out: completion needs the bases.
//!
//! [`boundary`] reads the spooled pack once more, as the client sent it,
//! and returns the commits its commits name as parents, and its tags name
//! as targets, that are not in the pack. Those are the commits the client
//! left out at the edge of what it sent; everything else it left out is
//! reachable from them (or from what the server advertised). Receive-pack
//! looks them up in `commit_index`, whose rows name a version that holds
//! each one.
//!
//! Only commits and tags are resolved. A commit or tag stored as a delta is
//! resolved when its base chain stays inside the pack (an OFS_DELTA chain,
//! or a REF_DELTA against a commit or tag resolved earlier in the pack),
//! which covers every pack stock Git writes: Git's thin bases are trees and
//! blobs of the boundary commits, never commits. Anything else is skipped;
//! a skipped commit can only hide part of the boundary, and receive-pack
//! then fails the push on the missing object exactly as it would without
//! this scan.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use aspen_core::id::ObjectId;
use aspen_core::ingest::{ObjectKind, SortedIds};

use crate::ingest::gix_chain;
use crate::locate::apply_delta;
use crate::scan::references;
use crate::{MaxObjectBytes, from_gix};

/// The commits and tags (by ID) that the pack's commits name as parents and
/// its tags name as targets, minus the commits and tags the pack holds.
///
/// Reads the pack at `spool` once, inflating every entry (gix checks each
/// zlib stream) and keeping only commits and tags, each at most
/// `max_object_bytes`. Fails on a pack gix cannot read or a commit or tag
/// that does not parse; receive-pack has already read the same pack once,
/// so that is an I/O error or a pack it is about to refuse anyway.
pub fn boundary(spool: &Path, max_object_bytes: MaxObjectBytes) -> Result<SortedIds, String> {
    let mut file = File::open(spool).map_err(|e| format!("opening the received pack: {e}"))?;
    let mut head = [0u8; 12];
    file.read_exact(&mut head)
        .map_err(|e| format!("reading the received pack: {e}"))?;
    if head[8..] == [0; 4] {
        // No entries: gix wants at least one.
        return Ok(SortedIds::default());
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|e| format!("reading the received pack: {e}"))?;
    let reader = BufReader::with_capacity(64 * 1024, file);
    let iter = gix_pack::data::input::BytesToEntriesIter::new_from_header(
        reader,
        gix_pack::data::input::Mode::AsIs,
        gix_pack::data::input::EntryDataMode::Keep,
        gix_hash::Kind::Sha1,
    )
    .map_err(|e| format!("reading the received pack: {}", gix_chain(&e)))?;
    let max = max_object_bytes.get();
    // Commits and tags resolved so far, by pack offset and by ID.
    let mut by_offset: HashMap<u64, (ObjectKind, ObjectId, Vec<u8>)> = HashMap::new();
    let mut by_id: HashMap<ObjectId, u64> = HashMap::new();
    let mut referenced: Vec<ObjectId> = Vec::new();
    for entry in iter {
        let entry = entry.map_err(|e| format!("reading the received pack: {}", gix_chain(&e)))?;
        let at = entry.pack_offset;
        let resolved = match entry.header {
            gix_pack::data::entry::Header::Commit => {
                Some((ObjectKind::Commit, inflate(&entry, max)?))
            }
            gix_pack::data::entry::Header::Tag => Some((ObjectKind::Tag, inflate(&entry, max)?)),
            gix_pack::data::entry::Header::OfsDelta { base_distance } => {
                match at
                    .checked_sub(base_distance)
                    .and_then(|b| by_offset.get(&b))
                {
                    Some((kind, _, base)) => {
                        Some((*kind, apply_delta(base, &inflate(&entry, max)?, max)?))
                    }
                    None => None,
                }
            }
            gix_pack::data::entry::Header::RefDelta { base_id } => {
                match by_id
                    .get(&from_gix(&base_id))
                    .and_then(|b| by_offset.get(b))
                {
                    Some((kind, _, base)) => {
                        Some((*kind, apply_delta(base, &inflate(&entry, max)?, max)?))
                    }
                    None => None,
                }
            }
            gix_pack::data::entry::Header::Tree | gix_pack::data::entry::Header::Blob => None,
        };
        let Some((kind, data)) = resolved else {
            continue;
        };
        let id = object_id(kind, &data);
        match kind {
            ObjectKind::Commit => {
                let (parents, _) = crate::scan::parse_commit(&data)
                    .map_err(|e| format!("commit {id} in the received pack: {e}"))?;
                referenced.extend(parents);
            }
            ObjectKind::Tag => references(ObjectKind::Tag, &data, |t| referenced.push(t))
                .map_err(|e| format!("tag {id} in the received pack: {e}"))?,
            ObjectKind::Tree | ObjectKind::Blob => unreachable!("only commits and tags are kept"),
        }
        by_id.insert(id, at);
        by_offset.insert(at, (kind, id, data));
    }
    referenced.retain(|r| !by_id.contains_key(r));
    Ok(SortedIds::from_unsorted(referenced))
}

/// The entry's inflated data (gix kept its compressed bytes).
fn inflate(entry: &gix_pack::data::input::Entry, max: u64) -> Result<Vec<u8>, String> {
    if entry.decompressed_size > max {
        return Err(format!(
            "entry at {} inflates to {} bytes, over the object limit",
            entry.pack_offset, entry.decompressed_size
        ));
    }
    let compressed = entry
        .compressed
        .as_deref()
        .ok_or("gix did not keep the entry's bytes")?;
    let mut out = vec![0; entry.decompressed_size as usize];
    let mut inflate = gix_zlib::Decompress::new();
    let n = gix_zlib::stream::inflate::read(&mut &compressed[..], &mut inflate, &mut out)
        .map_err(|e| format!("entry at {}: {e}", entry.pack_offset))?;
    if n != out.len() {
        return Err(format!(
            "entry at {} inflates to {n} bytes, not the {} its header says",
            entry.pack_offset,
            out.len()
        ));
    }
    Ok(out)
}

fn object_id(kind: ObjectKind, data: &[u8]) -> ObjectId {
    let name = match kind {
        ObjectKind::Commit => "commit",
        ObjectKind::Tree => "tree",
        ObjectKind::Blob => "blob",
        ObjectKind::Tag => "tag",
    };
    let mut h = crate::Sha1::new();
    h.update(format!("{name} {}\0", data.len()).as_bytes());
    h.update(data);
    // A detected collision attack yields the all-zero ID, which no
    // `commit_index` row has, so such a commit only hides boundary.
    ObjectId::from_bytes(h.finish().unwrap_or([0; aspen_core::id::SHA1_LEN]))
}

#[cfg(test)]
mod tests {
    use super::boundary;
    use crate::testutil::{Base, MAX_OBJECT, Obj, Spec, build_pack, scratch};
    use aspen_core::id::ObjectId;
    use aspen_core::ingest::ObjectKind;

    fn commit(tree: ObjectId, parents: &[ObjectId], msg: &str) -> Obj {
        let mut s = format!("tree {tree}\n");
        for p in parents {
            s.push_str(&format!("parent {p}\n"));
        }
        s.push_str("author A <a@example.com> 1 +0000\ncommitter A <a@example.com> 1 +0000\n\n");
        s.push_str(msg);
        s.push('\n');
        Obj {
            kind: ObjectKind::Commit,
            data: s.into_bytes(),
        }
    }

    fn tag(target: ObjectId) -> Obj {
        Obj {
            kind: ObjectKind::Tag,
            data: format!(
                "object {target}\ntype commit\ntag v1\ntagger A <a@example.com> 1 +0000\n\nv1\n"
            )
            .into_bytes(),
        }
    }

    fn run(specs: &[Spec]) -> Vec<ObjectId> {
        let dir = scratch();
        let path = dir.path().join("p.pack");
        std::fs::write(&path, build_pack(specs)).unwrap();
        boundary(&path, MAX_OBJECT).unwrap().as_slice().to_vec()
    }

    fn sorted(mut v: Vec<ObjectId>) -> Vec<ObjectId> {
        v.sort();
        v
    }

    /// Invariant: the boundary is exactly the parents and tag targets the
    /// pack does not hold, whether its commits are whole, OFS_DELTAs or
    /// REF_DELTAs against an earlier commit; trees, blobs and thin bases
    /// play no part.
    #[test]
    fn boundary_is_the_parents_and_targets_the_pack_lacks() {
        let tree = Obj::blob("not really a tree").id();
        let outside_a = commit(tree, &[], "a").id();
        let outside_b = commit(tree, &[], "b").id();
        let c1 = commit(tree, &[outside_a], "c1 with a message long enough to delta");
        let c2 = commit(
            tree,
            &[c1.id()],
            "c2 with a message long enough to delta, too",
        );
        let c3 = commit(
            tree,
            &[c2.id(), outside_b],
            "c3 merges a commit the pack does not hold",
        );
        let external_tag_target = commit(tree, &[], "tagged elsewhere").id();
        let thin_base = Obj::blob("a thin base the server holds");
        let thin = Obj::blob("a thin base the server holds, changed");
        let got = run(&[
            Spec::Full(c1.clone()),
            Spec::Ofs(0, c2.clone()),
            Spec::Ref(Base::Entry(1), c3.clone()),
            Spec::Ref(Base::External(thin_base), thin),
            Spec::Full(tag(c3.id())),
            Spec::Full(tag(external_tag_target)),
        ]);
        assert_eq!(got, sorted(vec![outside_a, outside_b, external_tag_target]));
    }

    /// Invariant: a pack with no commits (a tag-only or blob-only push, or
    /// an empty pack) has an empty boundary, and so does a pack whose
    /// history is all inside it (a first push).
    #[test]
    fn closed_and_commitless_packs_have_no_boundary() {
        let tree = Obj::blob("t").id();
        let root = commit(tree, &[], "root");
        let child = commit(tree, &[root.id()], "child");
        assert!(run(&[Spec::Full(root), Spec::Full(child)]).is_empty());
        assert!(run(&[Spec::Full(Obj::blob("x"))]).is_empty());
        assert!(run(&[]).is_empty());
    }

    /// A delta whose base is outside the pack is skipped, not an error:
    /// only stock Git's packs are promised a full boundary, and Git never
    /// deltas a commit against one the pack lacks.
    #[test]
    fn a_commit_delta_against_an_outside_base_is_skipped() {
        let tree = Obj::blob("t").id();
        let base = commit(tree, &[], "outside base");
        let hidden_parent = commit(tree, &[], "parent").id();
        let c = commit(tree, &[hidden_parent], "outside base");
        assert!(run(&[Spec::Ref(Base::External(base), c)]).is_empty());
    }
}

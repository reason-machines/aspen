//! Commit extraction and the connectivity check's inputs.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use aspen_core::error::{StoreError, UnpackError};
use aspen_core::id::{ObjectId, PackId};
use aspen_core::ingest::{ObjectKind, ObjectLocation, ObjectLocator, SearchPacks, SortedIds};

use crate::scan::{
    ScanError, ScanOptions, check_connectivity, parse_commit, read_object, references, scan,
};
use crate::testutil::{git, scratch};

fn oid(n: u8) -> ObjectId {
    ObjectId::from_bytes([n; 20])
}

#[test]
fn commit_references_are_tree_then_parents() {
    let c = format!(
        "tree {}\nparent {}\nparent {}\nauthor A <a@x> 1 +0000\ncommitter C <c@x> 1700000000 +0200\n\nmsg\n",
        oid(1),
        oid(2),
        oid(3)
    );
    let mut got = Vec::new();
    references(ObjectKind::Commit, c.as_bytes(), |id| got.push(id)).unwrap();
    assert_eq!(got, vec![oid(1), oid(2), oid(3)]);
    assert_eq!(
        parse_commit(c.as_bytes()).unwrap(),
        (vec![oid(2), oid(3)], 1_700_000_000)
    );
    assert!(references(ObjectKind::Commit, b"not a commit", |_| {}).is_err());
}

#[test]
fn an_unparseable_committer_time_is_zero_as_in_git() {
    let c = format!(
        "tree {}\nauthor A <a@x> 1 +0000\ncommitter C <c@x> garbage\n\nm\n",
        oid(1)
    );
    assert_eq!(parse_commit(c.as_bytes()).unwrap(), (vec![], 0));
    // A committer line not directly after the author line is not read.
    let c = format!(
        "tree {}\ncommitter C <c@x> 5 +0000\nauthor A <a@x> 1 +0000\n\nm\n",
        oid(1)
    );
    assert_eq!(parse_commit(c.as_bytes()).unwrap(), (vec![], 0));
    // Leading spaces are skipped and an overflowing time saturates.
    let c = format!(
        "tree {}\nauthor A <a@x> 1 +0000\ncommitter C <c@x>   99999999999999999999999 +0000\n\nm\n",
        oid(1)
    );
    assert_eq!(parse_commit(c.as_bytes()).unwrap(), (vec![], i64::MAX));
    assert!(parse_commit(b"parent x\n").is_err());
}

#[test]
fn tree_references_skip_gitlinks() {
    // A gitlink (mode 160000) names a commit in another repository; it is
    // never in this repository's closure, so it must not be checked.
    let mut t = Vec::new();
    for (mode, name, id) in [
        ("100644", "a", oid(1)),
        ("160000", "sub", oid(2)),
        ("40000", "d", oid(3)),
    ] {
        t.extend_from_slice(format!("{mode} {name}\0").as_bytes());
        t.extend_from_slice(id.as_bytes());
    }
    let mut got = Vec::new();
    references(ObjectKind::Tree, &t, |id| got.push(id)).unwrap();
    assert_eq!(got, vec![oid(1), oid(3)]);
}

#[test]
fn tag_references_its_target_and_blobs_reference_nothing() {
    let tag = format!(
        "object {}\ntype commit\ntag v1\ntagger T <t@x> 1 +0000\n\nv1\n",
        oid(7)
    );
    let mut got = Vec::new();
    references(ObjectKind::Tag, tag.as_bytes(), |id| got.push(id)).unwrap();
    assert_eq!(got, vec![oid(7)]);
    references(ObjectKind::Blob, b"tree 0000", |_| {
        panic!("blobs have no references")
    })
    .unwrap();
}

#[test]
fn scan_matches_git_log_and_a_closed_pack_misses_nothing() {
    let dir = scratch();
    let src = crate::tests::ingest::history(dir.path(), 6);
    git(&src, &["tag", "-a", "-m", "v1", "v1"]);
    git(&src, &["repack", "-a", "-d", "-q"]);
    let pack_dir = src.join(".git/objects/pack");
    let pack = std::fs::read_dir(&pack_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "pack"))
        .unwrap();
    let idx = pack.with_extension("idx");
    let out = scan(
        &pack,
        &idx,
        ScanOptions {
            commits: true,
            missing: true,
            thread_limit: Some(2),
            max_object_bytes: crate::testutil::MAX_OBJECT,
            max_inflated_bytes: crate::testutil::MAX_INFLATED,
            verify_checksums: false,
        },
    )
    .unwrap();
    assert!(out.missing.is_empty(), "a full repack is closed");
    let log = git(&src, &["log", "--format=%H %ct %P"]);
    let mut want: Vec<(ObjectId, i64, Vec<ObjectId>)> = log
        .lines()
        .map(|l| {
            let mut f = l.split_whitespace();
            let id = ObjectId::parse_hex(f.next().unwrap()).unwrap();
            let t = f.next().unwrap().parse().unwrap();
            (id, t, f.map(|p| ObjectId::parse_hex(p).unwrap()).collect())
        })
        .collect();
    want.sort_by_key(|w| w.0);
    let got: Vec<_> = out
        .commits
        .iter()
        .map(|c| (c.id, c.committer_time, c.parents.clone()))
        .collect();
    assert_eq!(got, want);
    // The tip's row needs one .idx lookup.
    let head = ObjectId::parse_hex(git(&src, &["rev-parse", "HEAD"]).trim()).unwrap();
    let (kind, data) = read_object(&pack, &idx, &head, crate::testutil::MAX_OBJECT)
        .unwrap()
        .unwrap();
    assert_eq!(kind, ObjectKind::Commit);
    let row = out.commits.iter().find(|c| c.id == head).unwrap();
    assert_eq!(
        parse_commit(&data).unwrap(),
        (row.parents.clone(), row.committer_time)
    );
    assert_eq!(row.to_new_commit().commit, head);
    assert!(
        read_object(&pack, &idx, &oid(9), crate::testutil::MAX_OBJECT)
            .unwrap()
            .is_none()
    );
}

/// A locator that knows a fixed set of IDs.
struct Known(Vec<ObjectId>);

impl ObjectLocator for Known {
    async fn locate(
        &self,
        _packs: &SearchPacks,
        ids: &SortedIds,
    ) -> Result<Vec<Option<ObjectLocation>>, StoreError> {
        Ok(ids
            .as_slice()
            .iter()
            .map(|id| {
                self.0.contains(id).then_some(ObjectLocation {
                    pack: PackId::new(1).unwrap(),
                    offset: 12,
                })
            })
            .collect())
    }
}

pub(crate) fn ready<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("test futures are ready at once"),
    }
}

#[test]
fn connectivity_fails_on_the_first_object_deps_lack() {
    let packs = SearchPacks::new([PackId::new(1).unwrap()]);
    let missing = SortedIds::from_unsorted(vec![oid(3), oid(1), oid(2)]);
    let all = Known(vec![oid(1), oid(2), oid(3)]);
    ready(check_connectivity(&all, &packs, &missing)).unwrap();
    let some = Known(vec![oid(1), oid(3)]);
    assert_eq!(
        ready(check_connectivity(&some, &packs, &missing)),
        Err(UnpackError::MissingObject { id: oid(2) })
    );
    ready(check_connectivity(
        &Known(vec![]),
        &packs,
        &SortedIds::default(),
    ))
    .unwrap();
}

#[test]
fn uppercase_hex_parents_are_parents_as_in_git() {
    // Invariant: every parent Git's parse_commit_buffer reads is a
    // reference, so the connectivity check sees it. get_oid_hex accepts
    // either case; a parser that stopped at an uppercase parent line would
    // let a push name a missing parent and be accepted.
    let dir = scratch();
    git(dir.path(), &["init", "-q"]);
    let empty_tree = git(
        dir.path(),
        &["hash-object", "-t", "tree", "-w", "/dev/null"],
    );
    let tree = ObjectId::parse_hex(empty_tree.trim()).unwrap();
    let c = format!(
        "tree {}\nparent {}\nparent {}\nauthor A <a@x> 1 +0000\ncommitter C <c@x> 7 +0000\n\nm\n",
        tree.to_hex().to_uppercase(),
        oid(2),
        oid(0xab).to_hex().to_uppercase(),
    );
    let mut got = Vec::new();
    references(ObjectKind::Commit, c.as_bytes(), |id| got.push(id)).unwrap();
    assert_eq!(got, vec![tree, oid(2), oid(0xab)]);
    assert_eq!(
        parse_commit(c.as_bytes()).unwrap(),
        (vec![oid(2), oid(0xab)], 7)
    );
    // Git itself reads the same parents from the same bytes.
    let out = crate::testutil::git_raw(
        dir.path(),
        &[
            "hash-object",
            "-t",
            "commit",
            "--literally",
            "-w",
            "--stdin",
        ],
        Some(c.as_bytes()),
    );
    assert!(out.status.success());
    let id = String::from_utf8(out.stdout).unwrap();
    let line = git(
        dir.path(),
        &["rev-list", "--no-walk", "--parents", id.trim()],
    );
    assert_eq!(
        line.trim(),
        format!("{} {} {}", id.trim(), oid(2), oid(0xab))
    );
}

#[test]
fn a_malformed_parent_line_is_an_error_as_in_git() {
    // Invariant: a `parent ` line that is not a full ID followed by a
    // newline makes the commit unparseable (Git: "bad parents"), so its
    // references are never silently dropped.
    let tree = format!("tree {}\n", oid(1));
    for bad in [
        format!("{tree}parent {}\n", &oid(2).to_hex()[..39]),
        format!(
            "{tree}parent {}x\nauthor A <a@x> 1 +0000\n",
            &oid(2).to_hex()[..39]
        ),
        format!("{tree}parent {} \nauthor A <a@x> 1 +0000\n", oid(2)),
        format!(
            "{tree}parent {}\nparent zz\nauthor A <a@x> 1 +0000\n",
            oid(2)
        ),
    ] {
        assert!(
            references(ObjectKind::Commit, bad.as_bytes(), |_| {}).is_err(),
            "{bad:?}"
        );
    }
    // A tree line with a bad ID is an error too.
    let bad_tree = format!("tree {}g\n\n", &oid(1).to_hex()[..39]);
    assert!(parse_commit(bad_tree.as_bytes()).is_err());
}

#[test]
fn the_committer_time_follows_the_last_angle_bracket_as_in_git() {
    // Git walks back from the end of the committer line to its last `>`;
    // a `-` is read by strtoumax, wrapping to a huge unsigned value.
    let c = format!(
        "tree {}\nauthor A <a@x> 1 +0000\ncommitter C <c@x> <d>  42 +0000\n\nm\n",
        oid(1)
    );
    assert_eq!(parse_commit(c.as_bytes()).unwrap(), (vec![], 42));
    let c = format!(
        "tree {}\nauthor A <a@x> 1 +0000\ncommitter C <c@x> -5 +0000\n\nm\n",
        oid(1)
    );
    assert_eq!(parse_commit(c.as_bytes()).unwrap(), (vec![], i64::MAX));
    // No newline after the committer line: Git gives 0.
    let c = format!(
        "tree {}\nauthor A <a@x> 1 +0000\ncommitter C <c@x> 9",
        oid(1)
    );
    assert_eq!(parse_commit(c.as_bytes()).unwrap(), (vec![], 0));
}

#[test]
fn scans_and_reads_refuse_objects_over_the_limit() {
    // Invariant: scan and read_object pass the object limit to gix, so a
    // stored or received pack cannot make them allocate past it.
    let dir = scratch();
    let src = crate::tests::ingest::history(dir.path(), 2);
    git(&src, &["repack", "-a", "-d", "-q"]);
    let pack = std::fs::read_dir(src.join(".git/objects/pack"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "pack"))
        .unwrap();
    let idx = pack.with_extension("idx");
    let tiny = crate::MaxObjectBytes::new(8).unwrap();
    let head = ObjectId::parse_hex(git(&src, &["rev-parse", "HEAD"]).trim()).unwrap();
    assert!(read_object(&pack, &idx, &head, tiny).is_err());
    let options = ScanOptions {
        commits: true,
        missing: true,
        thread_limit: Some(1),
        max_object_bytes: tiny,
        max_inflated_bytes: crate::testutil::MAX_INFLATED,
        verify_checksums: false,
    };
    assert!(matches!(
        scan(&pack, &idx, options),
        Err(ScanError::Pack(_))
    ));
}

/// `pack` written to `dir` and indexed by `git index-pack`: its paths.
fn indexed(pack: &[u8], dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    crate::testutil::git_index_pack(pack, dir, false).unwrap();
    let p = dir.join(format!("check-{}.pack", crate::sha1(pack)));
    let idx = p.with_extension("idx");
    (p, idx)
}

fn scan_options(limit: crate::MaxObjectBytes, verify_checksums: bool) -> ScanOptions {
    ScanOptions {
        commits: true,
        missing: true,
        thread_limit: Some(1),
        max_object_bytes: limit,
        max_inflated_bytes: crate::testutil::MAX_INFLATED,
        verify_checksums,
    }
}

#[test]
fn scan_refuses_an_entry_over_the_limit_in_a_large_pack() {
    // Invariant: scan checks each entry's declared size against the object
    // limit itself; gix's own limit also covers its delta tree and so grows
    // with the object count (3,000 entries: 768 KB under a 64-byte limit).
    use crate::testutil::{Obj, Spec, build_pack};
    let dir = scratch();
    let mut specs: Vec<Spec> = (0..3000)
        .map(|i| Spec::Full(Obj::blob(format!("b{i}"))))
        .collect();
    specs.push(Spec::Full(Obj::blob(vec![b'x'; 200])));
    let (pack, idx) = indexed(&build_pack(&specs), dir.path());
    let tiny = crate::MaxObjectBytes::new(64).unwrap();
    match scan(&pack, &idx, scan_options(tiny, false)) {
        Err(ScanError::Pack(m)) => assert!(m.contains("object limit"), "{m}"),
        other => panic!("{other:?}"),
    }
    let enough = crate::MaxObjectBytes::new(200).unwrap();
    scan(&pack, &idx, scan_options(enough, false)).unwrap();
}

#[test]
fn scan_refuses_a_pack_inflating_past_the_total_limit() {
    // Invariant: scan checks the sum of the declared sizes against the total
    // limit before gix's resolver (which can hold a whole path of the delta
    // tree at once) runs; see the ingest test of the same pack.
    let dir = scratch();
    let (pack, idx) = indexed(&crate::testutil::ladder_pack(16, 64 << 10), dir.path());
    let mut options = scan_options(crate::MaxObjectBytes::new(64 << 10).unwrap(), false);
    options.max_inflated_bytes = crate::MaxInflatedBytes::new(512 << 10).unwrap();
    match scan(&pack, &idx, options) {
        Err(ScanError::Pack(m)) => assert!(m.contains("limit for one pack"), "{m}"),
        other => panic!("{other:?}"),
    }
    options.max_inflated_bytes = crate::MaxInflatedBytes::new(2 << 20).unwrap();
    scan(&pack, &idx, options).unwrap();
}

#[test]
fn scan_with_checksums_refuses_a_flipped_byte() {
    // Invariant: a scan of a pack read back from storage can verify it, so
    // a blob that no longer matches its row yields no commit rows. Each
    // flip (the pack trailer, the `.idx` checksum, an entry's CRC32) leaves
    // every object decodable, so only that checksum check can catch it.
    use crate::testutil::{Obj, Spec, build_pack};
    let dir = scratch();
    let specs: Vec<Spec> = (0..10)
        .map(|i| Spec::Full(Obj::blob(format!("b{i}"))))
        .collect();
    let (pack, idx) = indexed(&build_pack(&specs), dir.path());
    let limit = crate::testutil::MAX_OBJECT;
    scan(&pack, &idx, scan_options(limit, true)).unwrap();
    let mut bytes = std::fs::read(&pack).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&pack, &bytes).unwrap();
    scan(&pack, &idx, scan_options(limit, false)).unwrap();
    assert!(matches!(
        scan(&pack, &idx, scan_options(limit, true)),
        Err(ScanError::Pack(_))
    ));
    bytes[last] ^= 1;
    std::fs::write(&pack, &bytes).unwrap();
    let good_idx = std::fs::read(&idx).unwrap();
    // `git index-pack` writes the `.idx` read-only.
    std::fs::remove_file(&idx).unwrap();
    let n = specs.len();
    // The `.idx`'s own checksum (its last 20 bytes).
    let mut flipped = good_idx.clone();
    let end = flipped.len() - 1;
    flipped[end] ^= 1;
    std::fs::write(&idx, &flipped).unwrap();
    scan(&pack, &idx, scan_options(limit, false)).unwrap();
    assert!(
        matches!(
            scan(&pack, &idx, scan_options(limit, true)),
            Err(ScanError::Pack(_))
        ),
        "the .idx checksum is verified"
    );
    // An entry's CRC32 in the `.idx` (the table after the fan-out and the
    // IDs), with the `.idx` checksum recomputed so only the CRC32 is wrong.
    let mut flipped = good_idx.clone();
    flipped[8 + 1024 + 20 * n] ^= 1;
    let body = flipped.len() - 20;
    let sum = crate::sha1(&flipped[..body]);
    flipped[body..].copy_from_slice(sum.as_bytes());
    std::fs::write(&idx, &flipped).unwrap();
    scan(&pack, &idx, scan_options(limit, false)).unwrap();
    assert!(
        matches!(
            scan(&pack, &idx, scan_options(limit, true)),
            Err(ScanError::Pack(_))
        ),
        "every entry's CRC32 is verified"
    );
}

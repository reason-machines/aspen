//! Deltas with less data than Git accepts (issue #162).
//!
//! Git's `patch_delta` refuses a delta whose data (base size, result size
//! and instructions together) is shorter than `DELTA_SIZE_MIN`, 4 bytes, so
//! `git index-pack` fails such a pack with "failed to apply delta". gix
//! applies it. Ingest must refuse exactly the packs stock Git refuses here:
//! each case runs through ingest and through `git index-pack --fix-thin`
//! against the same objects, and both must refuse it or both must store the
//! same objects. Which way a case goes is computed from the pack's own
//! entries, not assumed: refused exactly when a delta is under 4 bytes.

use aspen_core::error::UnpackError;
use aspen_core::ingest::{InMemoryBases, ObjectKind};

use crate::testutil::{Base, Obj, Spec, build_pack, git_index_pack, scratch};
use crate::verify::{VerifyError, verify_pack};

use super::ingest::{ingest, stored_ids};
use super::thin::{git_fix_thin, parse_bases};

/// Git's `DELTA_SIZE_MIN`, restated so the test does not take ingest's word
/// for it.
const GIT_MIN: u64 = 4;

/// The issue's reproducer: the blob `abc`, then an OFS_DELTA on it whose
/// data is `03 00` (base size 3, result size 0, no instructions).
const ISSUE_HEX: &str = "5041434b000000020000000233789c4b4c4a0600024d0127620c789c6366000000080004fc6b1135dc9455723e3d38473a3efa8518be6517";

struct Case {
    name: &'static str,
    pack: Vec<u8>,
    /// What the base lookup (and git's repository) holds.
    objects: Vec<(ObjectKind, Vec<u8>)>,
    /// Whether the pack has no thin base, so `verify_pack` and a plain
    /// `git index-pack` apply.
    self_contained: bool,
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn blob(data: &[u8]) -> (ObjectKind, Vec<u8>) {
    (ObjectKind::Blob, data.to_vec())
}

fn cases() -> Vec<Case> {
    let abc = Obj::blob("abc");
    let ofs = |base: &[u8], target: &[u8]| {
        build_pack(&[Spec::Full(Obj::blob(base)), Spec::Ofs(0, Obj::blob(target))])
    };
    let thin = |base: &Obj, target: &[u8]| {
        build_pack(&[Spec::Ref(Base::External(base.clone()), Obj::blob(target))])
    };
    let big = vec![b'b'; 20_000];
    vec![
        Case {
            name: "issue reproducer (OFS_DELTA abc to empty)",
            pack: hex(ISSUE_HEX),
            objects: vec![],
            self_contained: true,
        },
        Case {
            name: "OFS_DELTA from a 200-byte base to the empty blob",
            pack: ofs(&[b'a'; 200], b""),
            objects: vec![],
            self_contained: true,
        },
        Case {
            name: "thin REF_DELTA abc to the empty blob",
            pack: thin(&abc, b""),
            objects: vec![blob(b"abc")],
            self_contained: false,
        },
        Case {
            name: "D3 sweep input 10679 (thin REF_DELTA to the empty blob)",
            pack: include_bytes!("data/issue-162/sweep-10679.pack").to_vec(),
            objects: parse_bases(include_str!("data/issue-162/sweep-10679.bases")),
            self_contained: false,
        },
        Case {
            name: "OFS_DELTA from a 20,000-byte base to the empty blob",
            pack: ofs(&big, b""),
            objects: vec![],
            self_contained: true,
        },
        Case {
            name: "OFS_DELTA abc to x",
            pack: ofs(b"abc", b"x"),
            objects: vec![],
            self_contained: true,
        },
        Case {
            name: "thin REF_DELTA abc to x",
            pack: thin(&abc, b"x"),
            objects: vec![blob(b"abc")],
            self_contained: false,
        },
    ]
}

/// The shortest delta in `pack` as `(offset, data length)`, read with gix's
/// own entry parser (no lookup, so thin packs parse too).
fn shortest_delta(pack: &[u8]) -> Option<(u64, u64)> {
    let iter = gix_pack::data::input::BytesToEntriesIter::new_from_header(
        std::io::BufReader::new(pack),
        gix_pack::data::input::Mode::Verify,
        gix_pack::data::input::EntryDataMode::Ignore,
        gix_hash::Kind::Sha1,
    )
    .unwrap();
    iter.map(Result::unwrap)
        .filter(|e| e.header.is_delta())
        .map(|e| (e.pack_offset, e.decompressed_size))
        .min_by_key(|&(_, size)| size)
}

fn lookup(objects: &[(ObjectKind, Vec<u8>)]) -> InMemoryBases {
    let mut bases = InMemoryBases::default();
    for (kind, data) in objects {
        let obj = Obj {
            kind: *kind,
            data: data.clone(),
        };
        bases.insert(obj.id(), obj.kind, obj.data);
    }
    bases
}

/// The reason ingest and `verify_pack` give for the delta at `offset`.
fn short_reason(offset: u64, size: u64) -> String {
    format!(
        "pack entry at offset {offset} is a delta of {size} bytes, shorter than the \
         {GIT_MIN} bytes Git requires"
    )
}

#[test]
fn ingest_refuses_exactly_the_short_deltas_git_refuses() {
    // Every case runs, so a failure lists all the cases it breaks.
    let mut wrong = Vec::new();
    let mut refused = 0;
    for case in cases() {
        let dir = scratch();
        let (offset, size) = shortest_delta(&case.pack).expect("every case has a delta");
        let git = git_fix_thin(&case.pack, &case.objects, dir.path());
        let short = size < GIT_MIN;
        match (&git, short) {
            (Err(e), true) if e.contains("failed to apply delta") => {}
            (Ok(_), false) => {}
            _ => wrong.push(format!(
                "{}: the case does not test what it means to: shortest delta {size} bytes, \
                 git {git:?}",
                case.name
            )),
        }
        match (ingest(&case.pack, &lookup(&case.objects), dir.path()), &git) {
            (Err(UnpackError::Malformed { reason }), Err(_)) => {
                refused += 1;
                if !reason.contains(&short_reason(offset, size)) {
                    wrong.push(format!(
                        "{}: refused for another reason: {reason}",
                        case.name
                    ));
                }
            }
            (Ok(stored), Ok(expected)) => {
                let stored = stored_ids(&stored, dir.path());
                if &stored != expected {
                    wrong.push(format!(
                        "{}: stored objects differ from git index-pack --fix-thin's",
                        case.name
                    ));
                }
            }
            (Ok(stored), Err(e)) => wrong.push(format!(
                "{}: ingest stored {} objects, git refuses it: {}",
                case.name,
                stored.object_count,
                e.trim()
            )),
            (Err(e), _) => wrong.push(format!("{}: ingest refuses it: {e:?}", case.name)),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    // Both directions are exercised: 4 short cases, 3 at or over the minimum.
    assert_eq!(refused, 4);
}

#[test]
fn verify_pack_agrees_with_git_on_short_deltas() {
    // verify_pack checks stored and compacted packs with the same entry
    // check, so it refuses what git index-pack refuses and accepts the rest.
    for case in cases().into_iter().filter(|c| c.self_contained) {
        let dir = scratch();
        let (offset, size) = shortest_delta(&case.pack).unwrap();
        let path = dir.path().join("in.pack");
        std::fs::write(&path, &case.pack).unwrap();
        let verified = verify_pack(
            &path,
            dir.path(),
            crate::testutil::MAX_OBJECT,
            crate::testutil::MAX_INFLATED,
        );
        match (verified, git_index_pack(&case.pack, dir.path(), false)) {
            (Err(VerifyError::Index(reason)), Err(git)) => {
                assert!(
                    git.contains("failed to apply delta"),
                    "{}: {git}",
                    case.name
                );
                assert!(
                    reason.contains(&short_reason(offset, size)),
                    "{}: {reason}",
                    case.name
                );
            }
            (Ok(v), Ok(ids)) => assert_eq!(v.table.entries().len(), ids.len(), "{}", case.name),
            (v, git) => panic!(
                "{}: verify_pack and git disagree: {:?} against {git:?}",
                case.name,
                v.map(|v| v.table.entries().len())
            ),
        }
    }
}

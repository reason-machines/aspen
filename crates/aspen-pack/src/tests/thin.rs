//! Thin-pack completion when inserted bytes and saved header bytes cancel
//! (issue #151).
//!
//! Each fixture in `data/issue-151/` is a pushed pack plus the objects the
//! base lookup holds (`<kind> <hex>` per line), found by D3's `pack_ingest`
//! fuzz target. In each one a base the lookup returns is inserted before a
//! REF_DELTA, the REF_DELTAs turned OFS_DELTAs save exactly as many header
//! bytes as the base adds, and a later OFS_DELTA then needs a new distance.
//! gix-pack 0.77's completion iterator kept the old distance whenever the
//! net change was zero, so these pushes were refused as malformed ("never
//! added to the tree" or "delta base size does not match"). Stock
//! `git index-pack --fix-thin` accepts all six against the same objects.

use std::collections::BTreeSet;

use aspen_core::id::ObjectId;
use aspen_core::ingest::{InMemoryBases, ObjectKind};

use crate::testutil::{Obj, git, git_raw, scratch};

use super::ingest::{ingest, stored_ids};

const FIXTURES: [(&str, &[u8], &str); 6] = [
    (
        "issue-input-1",
        include_bytes!("data/issue-151/issue-input-1.pack"),
        include_str!("data/issue-151/issue-input-1.bases"),
    ),
    (
        "issue-input-2",
        include_bytes!("data/issue-151/issue-input-2.pack"),
        include_str!("data/issue-151/issue-input-2.bases"),
    ),
    (
        "sweep-4502",
        include_bytes!("data/issue-151/sweep-4502.pack"),
        include_str!("data/issue-151/sweep-4502.bases"),
    ),
    (
        "sweep-9258",
        include_bytes!("data/issue-151/sweep-9258.pack"),
        include_str!("data/issue-151/sweep-9258.bases"),
    ),
    (
        "sweep-11844",
        include_bytes!("data/issue-151/sweep-11844.pack"),
        include_str!("data/issue-151/sweep-11844.bases"),
    ),
    (
        "sweep-12720",
        include_bytes!("data/issue-151/sweep-12720.pack"),
        include_str!("data/issue-151/sweep-12720.bases"),
    ),
];

pub(super) fn parse_bases(text: &str) -> Vec<(ObjectKind, Vec<u8>)> {
    text.lines()
        .filter(|l| !l.is_empty())
        .map(|line| {
            let (kind, hex) = line.split_once(' ').unwrap_or((line, ""));
            let kind = match kind {
                "commit" => ObjectKind::Commit,
                "tree" => ObjectKind::Tree,
                "blob" => ObjectKind::Blob,
                "tag" => ObjectKind::Tag,
                other => panic!("unknown kind {other}"),
            };
            let data = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            (kind, data)
        })
        .collect()
}

fn kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Commit => "commit",
        ObjectKind::Tree => "tree",
        ObjectKind::Blob => "blob",
        ObjectKind::Tag => "tag",
    }
}

/// The objects stock `git index-pack --fix-thin` stores for `pack` in a
/// repository holding exactly `objects`, or git's stderr when it refuses
/// the pack.
pub(super) fn git_fix_thin(
    pack: &[u8],
    objects: &[(ObjectKind, Vec<u8>)],
    dir: &std::path::Path,
) -> Result<BTreeSet<ObjectId>, String> {
    let repo = dir.join("git.git");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-q", "--bare"]);
    for (kind, data) in objects {
        let out = git_raw(
            &repo,
            &[
                "hash-object",
                "-w",
                "--literally",
                "--stdin",
                "-t",
                kind_name(*kind),
            ],
            Some(data),
        );
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let out = git_raw(&repo, &["index-pack", "--stdin", "--fix-thin"], Some(pack));
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).into_owned());
    }
    // `pack\t<checksum>`: list that pack's objects.
    let checksum = String::from_utf8(out.stdout).unwrap();
    let checksum = checksum.trim().trim_start_matches("pack").trim();
    let idx = std::fs::read(repo.join(format!("objects/pack/pack-{checksum}.idx"))).unwrap();
    let listed = git_raw(&repo, &["show-index"], Some(&idx));
    Ok(String::from_utf8(listed.stdout)
        .unwrap()
        .lines()
        .map(|l| ObjectId::parse_hex(l.split_whitespace().nth(1).unwrap()).unwrap())
        .collect())
}

#[test]
fn a_net_zero_completion_shift_still_moves_later_ofs_deltas() {
    // Every fixture runs, so a failure lists all the inputs it breaks.
    let mut refused = Vec::new();
    for (name, pack, bases_text) in FIXTURES {
        let dir = scratch();
        let objects = parse_bases(bases_text);
        let mut bases = InMemoryBases::default();
        for (kind, data) in &objects {
            let obj = Obj {
                kind: *kind,
                data: data.clone(),
            };
            bases.insert(obj.id(), obj.kind, obj.data);
        }
        let expected = git_fix_thin(pack, &objects, dir.path())
            .unwrap_or_else(|e| panic!("stock git refuses {name}: {e}"));
        match ingest(pack, &bases, dir.path()) {
            Ok(stored) => assert_eq!(
                stored_ids(&stored, dir.path()),
                expected,
                "{name}: stored objects differ from git index-pack --fix-thin's"
            ),
            Err(e) => refused.push(format!("{name}: {e}")),
        }
    }
    assert!(
        refused.is_empty(),
        "packs stock git completes are refused:\n{}",
        refused.join("\n")
    );
}

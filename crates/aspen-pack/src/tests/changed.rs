//! The walk of the paths a push changed (`crate::changed`), on real
//! histories: what it does not send to the connectivity check must exist in
//! the dependencies, what it calls held must be held, and an ordinary
//! one-file change needs no lookup at all.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use aspen_core::error::StoreError;
use aspen_core::id::ObjectId;

use crate::changed::{ChangedInput, OldTrees, changed_paths};
use crate::graph::PackGraph;
use crate::scan::ScanOptions;
use crate::testutil::{MAX_INFLATED, MAX_OBJECT, git, git_raw, scratch};

use super::scan::ready;

/// The dependencies' objects, read with `git cat-file` from `repo`, and
/// counted.
struct Git<'a> {
    repo: &'a Path,
    base: &'a BTreeSet<ObjectId>,
    reads: usize,
}

impl Git<'_> {
    fn read(&mut self, id: &ObjectId) -> Option<Vec<u8>> {
        // Only what the dependencies hold may be read.
        assert!(self.base.contains(id), "read {id}, which the base lacks");
        self.reads += 1;
        let kind = git(self.repo, &["cat-file", "-t", &id.to_hex()]);
        Some(git_raw(self.repo, &["cat-file", kind.trim(), &id.to_hex()], None).stdout)
    }
}

impl OldTrees for Git<'_> {
    async fn trees(&mut self, ids: &[ObjectId]) -> Result<Vec<Option<Arc<[u8]>>>, StoreError> {
        Ok(ids.iter().map(|id| self.read(id).map(Arc::from)).collect())
    }

    async fn commit_trees(
        &mut self,
        ids: &[ObjectId],
    ) -> Result<Vec<Option<ObjectId>>, StoreError> {
        Ok(ids
            .iter()
            .map(|id| crate::scan::commit_tree(&self.read(id)?).ok())
            .collect())
    }
}

fn objects(repo: &Path, revs: &str) -> BTreeSet<ObjectId> {
    let mut args = vec!["rev-list", "--objects"];
    args.extend(revs.split(' '));
    git(repo, &args)
        .lines()
        .map(|l| ObjectId::parse_hex(&l[..40]).unwrap())
        .collect()
}

fn rev(repo: &Path, r: &str) -> ObjectId {
    ObjectId::parse_hex(git(repo, &["rev-parse", r]).trim()).unwrap()
}

fn write(repo: &Path, path: &str, body: &str) {
    let p = repo.join(path);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, body).unwrap();
}

fn commit(repo: &Path, msg: &str) {
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "--allow-empty", "-m", msg]);
}

/// The pack a stock `git push` of `tip` sends to a server at `base`,
/// completed and indexed (`git index-pack --fix-thin`), and its graph.
fn pushed(repo: &Path, dir: &Path, tip: &str, base: Option<&str>) -> PackGraph {
    let revs = match base {
        Some(b) => format!("{tip}\n^{b}\n"),
        None => format!("{tip}\n"),
    };
    let out = git_raw(
        repo,
        &[
            "pack-objects",
            "-q",
            "--revs",
            "--stdout",
            "--thin",
            "--delta-base-offset",
        ],
        Some(revs.as_bytes()),
    );
    assert!(out.status.success());
    let pack = dir.join("p.pack");
    let idx = dir.join("p.idx");
    let fixed = git_raw(
        repo,
        &[
            "index-pack",
            "--stdin",
            "--fix-thin",
            pack.to_str().unwrap(),
        ],
        Some(&out.stdout),
    );
    assert!(
        fixed.status.success(),
        "{}",
        String::from_utf8_lossy(&fixed.stderr)
    );
    let options = ScanOptions {
        commits: true,
        missing: true,
        thread_limit: Some(1),
        max_object_bytes: MAX_OBJECT,
        max_inflated_bytes: MAX_INFLATED,
        verify_checksums: true,
    };
    PackGraph::scan_keeping_trees(&pack, &idx, options, 1 << 20).unwrap()
}

/// Runs the walk for a push of `tip` onto `base` and checks it: every
/// reference of an object it visited is in the pack, in its check list, or
/// in the base (so leaving it out of the connectivity check is sound);
/// everything it calls held is in the base. Returns the check list's
/// length and the number of old objects read.
fn check(repo: &Path, tip: &str, base: Option<&str>) -> (usize, usize) {
    let dir = scratch();
    let g = pushed(repo, dir.path(), tip, base);
    let held_by_deps = match base {
        Some(b) => objects(repo, b),
        None => BTreeSet::new(),
    };
    let known: Vec<ObjectId> = base.map(|b| rev(repo, b)).into_iter().collect();
    let mut source = Git {
        repo,
        base: &held_by_deps,
        reads: 0,
    };
    let ch = ready(changed_paths(
        ChangedInput {
            graph: &g,
            known: &known,
        },
        &mut source,
    ))
    .unwrap()
    .expect("trees kept");
    let check: BTreeSet<ObjectId> = ch.check.iter().copied().collect();
    for at in 0..g.len() {
        if ch.held[at] {
            assert!(held_by_deps.contains(&g.id(at)), "held {} is not", g.id(at));
        }
        if !ch.visited[at] {
            continue;
        }
        for r in g.external_refs(at) {
            assert!(
                check.contains(r) || held_by_deps.contains(r),
                "{} references {r}, which is neither checked nor in the base",
                g.id(at)
            );
        }
    }
    // Every commit the push brings was walked.
    let want = objects(
        repo,
        &match base {
            Some(b) => format!("{tip} ^{b}"),
            None => tip.to_owned(),
        },
    );
    let reached: HashMap<ObjectId, bool> =
        (0..g.len()).map(|at| (g.id(at), ch.visited[at])).collect();
    for id in &want {
        assert!(reached.contains_key(id), "the pack lacks {id}");
    }
    for c in g.commits() {
        assert!(reached[&c.id]);
    }
    (check.len(), source.reads)
}

#[test]
fn a_one_file_change_needs_no_lookup() {
    // Invariant: a commit that changes one file deep in a tree checks
    // nothing against the dependencies: every entry it did not change is
    // the parent's, and the parent is the dependency's tip. It reads only
    // the old trees on the changed path.
    let dir = scratch();
    let repo = dir.path().join("r");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    for a in 0..4 {
        for b in 0..4 {
            for f in 0..6 {
                write(
                    &repo,
                    &format!("a{a}/b{b}/c/f{f}.txt"),
                    &format!("{a} {b} {f}\n"),
                );
            }
        }
    }
    commit(&repo, "base");
    write(&repo, "a2/b1/c/f3.txt", "changed\n");
    commit(&repo, "change");
    let (checked, reads) = check(&repo, "HEAD", Some("HEAD~1"));
    assert_eq!(checked, 0, "nothing to look up");
    // At most the base commit and the old root, a2, a2/b1 and a2/b1/c
    // (fewer when the pack carries an old tree as a completion base).
    assert!(reads <= 5, "{reads} old objects read");
}

#[test]
fn renames_reverts_merges_and_new_roots_are_sound() {
    // Invariant: whatever a push does, what the walk leaves out of the
    // connectivity check exists in the base. Moves and copies of existing
    // objects, a revert to an older version, a merge of a side branch,
    // several commits at once, a deleted directory, a commit whose parent
    // is not the base, and a first push (no base) all keep it.
    let dir = scratch();
    let repo = dir.path().join("r");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    for i in 0..5 {
        write(&repo, &format!("d{i}/x/f.txt"), &format!("v1 {i}\n"));
        write(&repo, &format!("d{i}/y.txt"), &format!("y {i}\n"));
    }
    commit(&repo, "c1");
    write(&repo, "d0/x/f.txt", "v2\n");
    commit(&repo, "c2");
    git(&repo, &["tag", "base"]);
    // Copy and move existing trees and blobs.
    git(&repo, &["mv", "d1", "moved"]);
    std::fs::create_dir_all(repo.join("copy")).unwrap();
    std::fs::copy(repo.join("d2/y.txt"), repo.join("copy/y.txt")).unwrap();
    commit(&repo, "moves");
    // Revert d0/x/f.txt to its c1 version, which the base's history holds
    // but its tip does not.
    write(&repo, "d0/x/f.txt", "v1 0\n");
    commit(&repo, "revert");
    // A side branch merged back.
    git(&repo, &["checkout", "-q", "-b", "side", "base"]);
    write(&repo, "d3/side.txt", "side\n");
    commit(&repo, "side");
    git(&repo, &["checkout", "-q", "main"]);
    git(&repo, &["merge", "-q", "--no-edit", "side"]);
    std::fs::remove_dir_all(repo.join("d4")).unwrap();
    commit(&repo, "delete d4");
    let (checked, _) = check(&repo, "HEAD", Some("base"));
    assert!(
        checked > 0,
        "the moved, copied and reverted objects are checked"
    );
    // The same push against an older base: the commits' parent is not
    // the base's tip.
    check(&repo, "HEAD", Some("base~1"));
    // A first push.
    let (checked, reads) = check(&repo, "HEAD", None);
    assert_eq!((checked, reads), (0, 0), "a first push holds everything");
}

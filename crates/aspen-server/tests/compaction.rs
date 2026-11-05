//! Compaction (node C1) end to end: stock `git` pushes through Aspen's
//! receive-pack, the compactor (`aspen-compact`) runs the `compact` jobs
//! those pushes enqueued, and stock `git` clones and fetches the result.
//!
//! The server uses a cover fanout of 4 so that 64 pushes build all of
//! levels 1 to 3 (16, 4 and 1 covers).

mod receive_support;

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::time::Duration;

use aspen_blob::LocalBlobStore;
use aspen_compact::{CompactConfig, Compactor, JobOutcome, Worker, WorkerOptions};
use aspen_core::id::{ObjectId, PackId, Seq};
use aspen_core::meta::{Cover, CoverLayout, MetaStore};
use aspen_core::name::RefName;
use aspen_core::repo::ForcePolicy;
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};

use receive_support::*;

const FANOUT: u64 = 4;

async fn push(w: &Work, url: &str, args: &[&str]) -> (bool, String) {
    let mut a = vec!["push", "--porcelain", url];
    a.extend_from_slice(args);
    let a: Vec<String> = a.iter().map(|s| s.to_string()).collect();
    let cmd = w.command();
    blocking(move || {
        let mut cmd = cmd;
        let out = cmd.args(&a).output().unwrap();
        let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&out.stderr));
        (out.status.success(), text)
    })
    .await
}

async fn git(w: &Work, args: &[&str]) -> String {
    let a: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let (home, path) = (w.home.clone(), w.path.clone());
    blocking(move || {
        let a: Vec<&str> = a.iter().map(String::as_str).collect();
        git_ok(&home, &path, &a)
    })
    .await
}

fn compactor(s: &Server) -> Compactor<SqlStore, LocalBlobStore> {
    let mut config = CompactConfig::new(
        s.dir.path().join("compact"),
        MaxObjectBytes::new(64 * MIB).unwrap(),
        MaxInflatedBytes::new(1024 * MIB).unwrap(),
    );
    config.layout = CoverLayout::new(FANOUT).unwrap();
    std::fs::create_dir_all(&config.spool_dir).unwrap();
    Compactor::new(s.meta.clone(), s.blobs.clone(), config)
}

/// Runs every due `compact` job; returns how many were done.
async fn drain(s: &Server) -> usize {
    let mut worker = Worker::new(compactor(s), WorkerOptions::new("test-worker"));
    let mut done = 0;
    while let Some(outcome) = worker.run_one().await.unwrap() {
        match outcome {
            JobOutcome::Done { .. } => done += 1,
            JobOutcome::Failed { error, .. } => panic!("a compact job failed: {error}"),
        }
    }
    done
}

/// One counter or sum from `/-/metrics`.
async fn metric(s: &Server, name: &str) -> u64 {
    let addr = s.base.trim_start_matches("http://").to_owned();
    let name = name.to_owned();
    blocking(move || {
        let mut conn = std::net::TcpStream::connect(&addr).unwrap();
        write!(
            conn,
            "GET /-/metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut body = String::new();
        conn.read_to_string(&mut body).unwrap();
        body.lines()
            .find_map(|l| l.strip_prefix(&format!("{name} ")))
            .unwrap_or_else(|| panic!("no metric {name}"))
            .trim()
            .parse()
            .unwrap()
    })
    .await
}

fn main_ref() -> RefName {
    RefName::new("refs/heads/main").unwrap()
}

/// Versions of a cover's range, by seq.
async fn range_packs(s: &Server, cover: &Cover) -> Vec<PackId> {
    let seqs: Vec<Seq> = (cover.range.first().get()..=cover.range.last().get())
        .map(|n| Seq::new(n).unwrap())
        .collect();
    let versions = s
        .meta
        .versions_by_seq(s.repo.id, &cover.name, &seqs)
        .await
        .unwrap();
    assert_eq!(versions.len(), seqs.len());
    let mut packs: Vec<PackId> = versions.iter().filter_map(|v| v.value.pack()).collect();
    packs.sort_unstable();
    packs.dedup();
    packs
}

/// A fresh clone of `main` only, current on trunk.
async fn trunk_client(s: &Server, name: &str) -> Work {
    let w = Work {
        path: s.dir.path().join(name),
        home: s.home.clone(),
    };
    let parent = Work {
        path: s.dir.path().to_owned(),
        home: s.home.clone(),
    };
    git(
        &parent,
        &[
            "clone",
            "-q",
            "--single-branch",
            "-b",
            "main",
            &s.url(),
            name,
        ],
    )
    .await;
    w
}

/// What planning one stale-branch fetch cost, read from `/-/metrics`.
#[derive(Debug)]
struct PlanCost {
    /// `PlanStats.visited`.
    visited: u64,
    /// `PlanStats.chain_skips`.
    chain_skips: u64,
    /// Ref versions read from the store to plan it.
    loaded: u64,
}

/// What planning one `git fetch origin feature` by a client current on
/// trunk cost.
async fn stale_branch_fetch(s: &Server, client: &str) -> PlanCost {
    const VISITED: &str = "aspen_fetch_plan_visited_versions_sum";
    const SKIPS: &str = "aspen_fetch_plan_chain_skips_total";
    const LOADED: &str = "aspen_fetch_plan_loaded_versions_sum";
    let c = trunk_client(s, client).await;
    let before = (
        metric(s, VISITED).await,
        metric(s, SKIPS).await,
        metric(s, LOADED).await,
    );
    git(
        &c,
        &[
            "fetch",
            "-q",
            "origin",
            "refs/heads/feature:refs/heads/feature",
        ],
    )
    .await;
    git(&c, &["fsck", "--strict", "--no-dangling"]).await;
    PlanCost {
        visited: metric(s, VISITED).await - before.0,
        chain_skips: metric(s, SKIPS).await - before.1,
        loaded: metric(s, LOADED).await - before.2,
    }
}

/// Invariants, end to end with stock git:
/// - every trunk push whose `seq` ends a level-1 cover enqueued one
///   `compact` job, and the compactor builds every aligned cover at every
///   level, each with a chain (ordinary trunk pushes depend on their
///   parent) and a pack;
/// - each cover's pack is self-contained, base-first, without repeats, and
///   holds exactly the objects of its range's versions' packs;
/// - a clone afterwards is sent the one level-3 pack instead of 64 push
///   packs, and passes `fsck --strict`;
/// - the stale-branch fetch (a client current on trunk fetching a branch
///   created 59 trunk pushes ago) visits a handful of versions instead of
///   about 60: the have-side skip through the chains;
/// - running the compactor again builds nothing (idempotent).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_end_to_end_with_stock_git() {
    let s = Server::start_with(
        |c| c.cover_layout = CoverLayout::new(FANOUT).unwrap(),
        ForcePolicy::Branches,
    )
    .await;
    let w = s.clone_dir("w");
    for i in 1..=64u32 {
        w.commit(
            &format!("f{}.txt", i % 5),
            &format!("version {i}\n{}\n", "x".repeat(i as usize)),
            &format!("commit {i}"),
        );
        let (ok, out) = push(&w, &s.url(), &["main"]).await;
        assert!(ok, "push {i}: {out}");
        if i == 5 {
            w.git(&["checkout", "-q", "-b", "feature"]);
            w.commit("feature.txt", "feature\n", "feature");
            let (ok, out) = push(&w, &s.url(), &["feature"]).await;
            assert!(ok, "{out}");
            w.git(&["checkout", "-q", "main"]);
        }
    }
    assert_eq!(s.live("refs/heads/main").await.seq.get(), 64);

    // Before compaction: the walk steps down trunk from the tip to the
    // branch's dependency.
    let unskipped = stale_branch_fetch(&s, "stale-before").await;
    assert_eq!(unskipped.chain_skips, 0, "no covers yet: {unskipped:?}");
    assert!(
        unskipped.loaded >= unskipped.visited,
        "every visited version was read: {unskipped:?}"
    );
    let linear = unskipped.visited;
    assert!(
        linear >= 55,
        "the linear walk visits about 60 versions: {linear}"
    );

    // 64 / 4 jobs, one per level-1 boundary.
    assert_eq!(drain(&s).await, 16);
    let covers = s.meta.covers(s.repo.id, &main_ref()).await.unwrap();
    let per_level = |l: u8| covers.iter().filter(|c| c.level.get() == l).count();
    assert_eq!((per_level(1), per_level(2), per_level(3)), (16, 4, 1));
    for c in &covers {
        assert!(c.chain.is_some(), "{c:?} is a dependency chain");
        let pack = c.pack.unwrap_or_else(|| panic!("{c:?} has a pack"));
        let row = &s.pack_rows(&[pack]).await[0];
        assert_eq!(row.pack.level().get(), c.level.get());
        s.verify_stored(row);
        let mut union = BTreeSet::<ObjectId>::new();
        for r in s.pack_rows(&range_packs(&s, c).await).await {
            union.extend(s.pack_objects(&r));
        }
        assert_eq!(
            s.pack_objects(row),
            union,
            "L{} {}..={} holds exactly its versions' objects",
            c.level.get(),
            c.range.first(),
            c.range.last()
        );
    }

    // A clone is sent trunk's level-3 pack and the branch's own pack.
    let before = metric(&s, "aspen_fetch_packs_planned_total").await;
    let parent = Work {
        path: s.dir.path().to_owned(),
        home: s.home.clone(),
    };
    git(&parent, &["clone", "-q", &s.url(), "full"]).await;
    let full = Work {
        path: s.dir.path().join("full"),
        home: s.home.clone(),
    };
    git(&full, &["fsck", "--strict", "--no-dangling"]).await;
    assert_eq!(full.rev("origin/main"), w.rev("main"));
    assert_eq!(full.rev("origin/feature"), w.rev("feature"));
    assert_eq!(
        metric(&s, "aspen_fetch_packs_planned_total").await - before,
        2
    );

    // After compaction the stale-branch fetch skips trunk through the
    // chains.
    let after = stale_branch_fetch(&s, "stale-after").await;
    assert!(
        after.chain_skips > 0,
        "the walk skipped through chains: {after:?}"
    );
    assert!(
        after.loaded < unskipped.loaded,
        "skipping also reads fewer versions: {after:?} against {unskipped:?}"
    );
    let skipped = after.visited;
    // git's first negotiation round sends 16 haves, its 16 newest trunk
    // commits; each is the exact tip of a trunk version, so each is a held
    // version the walk expands once. Beyond those, the walk visits the
    // branch and its dependency, plus at most two versions per chain jump.
    assert!(
        skipped <= 16 + 2 + 2 * 3,
        "the skip walk visits the haves and a few versions, not {skipped}"
    );
    assert!(skipped * 3 < linear, "{skipped} visits against {linear}");

    // Idempotent: nothing more to build.
    let report = compactor(&s)
        .compact_through(s.repo.id, &main_ref(), Seq::new(64).unwrap())
        .await
        .unwrap();
    assert_eq!(report.covers_created, 0, "{report:?}");
    assert_eq!(report.packs_written, 0, "{report:?}");
}

/// Invariants for plain-URL branch pushes (issue #203):
/// - a range of fast-forwards gets a cover with a chain, since each such
///   push also depends on the branch's version it searched, and an
///   incremental fetch with a have inside that range is complete;
/// - a range in which a version does not depend on its parent (a forced
///   update, `-o force`, depends on trunk only) gets a cover and a pack but
///   no chain, and a clone through it is complete: the planner never
///   treats an unchained range as held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_url_fast_forwards_chain_and_a_forced_update_does_not() {
    let s = Server::start_with(
        |c| c.cover_layout = CoverLayout::new(FANOUT).unwrap(),
        ForcePolicy::Branches,
    )
    .await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "base\n", "base");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "topic"]);
    let parent = Work {
        path: s.dir.path().to_owned(),
        home: s.home.clone(),
    };
    for i in 1..=8u32 {
        if i == 3 {
            // A client current on topic version 2, inside the chained
            // range 1 to 4.
            git(&parent, &["clone", "-q", "-b", "topic", &s.url(), "behind"]).await;
        }
        if i == 6 {
            // A forced update: a commit off trunk that does not descend
            // from the branch's tip.
            w.git(&["reset", "-q", "--hard", "main"]);
        }
        w.commit("t.txt", &format!("topic {i}\n"), &format!("topic {i}"));
        let force = i == 6;
        let mut args = vec![];
        if force {
            args.extend(["-o", "force", "--force"]);
        }
        args.push("topic");
        let (ok, out) = push(&w, &s.url(), &args).await;
        assert!(ok, "push {i}: {out}");
        if i == 4 {
            // Versions 1 to 4 are compacted into a chained cover. The
            // client behind fetches with its have (version 2's tip) inside
            // that range, and gets a complete result.
            assert_eq!(drain(&s).await, 1);
            assert!(
                s.meta
                    .covers(s.repo.id, &RefName::new("refs/heads/topic").unwrap())
                    .await
                    .unwrap()
                    .iter()
                    .any(|c| c.range.first() == Seq::FIRST && c.chain.is_some()),
                "versions 1 to 4 carry a chain"
            );
            let behind = Work {
                path: s.dir.path().join("behind"),
                home: s.home.clone(),
            };
            git(&behind, &["fetch", "-q", "origin", "topic"]).await;
            git(&behind, &["fsck", "--strict", "--no-dangling"]).await;
            assert_eq!(
                git(&behind, &["rev-parse", "FETCH_HEAD"]).await.trim(),
                w.rev("topic")
            );
        }
    }
    assert_eq!(drain(&s).await, 1);
    let topic = RefName::new("refs/heads/topic").unwrap();
    let covers = s.meta.covers(s.repo.id, &topic).await.unwrap();
    // Two level-1 covers; level 2 spans 16 versions.
    assert_eq!(covers.len(), 2, "{covers:?}");
    assert!(covers.iter().all(|c| c.pack.is_some()), "{covers:?}");
    let first = |n: u64| {
        covers
            .iter()
            .find(|c| c.range.first() == Seq::new(n).unwrap())
            .unwrap_or_else(|| panic!("no cover from {n}: {covers:?}"))
    };
    // Versions 2 to 4 fast-forward their parent; version 6 is forced.
    assert!(first(1).chain.is_some(), "{covers:?}");
    assert!(first(5).chain.is_none(), "{covers:?}");
    // A clone of the branch is complete.
    git(&parent, &["clone", "-q", "-b", "topic", &s.url(), "c"]).await;
    let c = Work {
        path: s.dir.path().join("c"),
        home: s.home.clone(),
    };
    git(&c, &["fsck", "--strict", "--no-dangling"]).await;
    assert_eq!(c.rev("HEAD"), w.rev("topic"));
}

/// Invariant (issue #48): garbage collection deletes a registered pack
/// that nothing names, row and blobs, once it has gone unregistered for the
/// grace period, and never a pack a version or cover names; every ref
/// still clones and passes `fsck --strict` afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn garbage_collection_deletes_only_orphans() {
    let s = Server::start_with(
        |c| c.cover_layout = CoverLayout::new(FANOUT).unwrap(),
        ForcePolicy::Branches,
    )
    .await;
    let w = s.clone_dir("w");
    for i in 1..=4u32 {
        w.commit("a.txt", &format!("{i}\n"), &format!("c{i}"));
        assert!(push(&w, &s.url(), &["main"]).await.0);
    }
    assert_eq!(drain(&s).await, 1);

    // An orphan: a pack registered and uploaded for a write that never
    // committed, as a lost compare-and-swap leaves it. Built from a copy of
    // the first push's pack with one more object, so its bytes differ.
    let side = s.clone_dir("side");
    side.commit("orphan.txt", "never pushed\n", "orphan");
    let orphan = store_orphan(&s, &side).await;
    let orphan_keys = orphan.keys();
    assert!(s.blobs.path_of(&orphan_keys.pack).exists());

    let mut c = compactor(&s);
    let mut config = c.config().clone();
    config.gc_grace = Duration::ZERO;
    c = Compactor::new(s.meta.clone(), s.blobs.clone(), config);
    let report = c.collect_garbage().await.unwrap();
    assert_eq!(report.packs_deleted, vec![orphan.id], "{report:?}");
    assert!(report.blobs_left.is_empty(), "{report:?}");
    for key in [&orphan_keys.pack, &orphan_keys.idx, &orphan_keys.entries] {
        assert!(!s.blobs.path_of(key).exists(), "{key} is deleted");
    }
    assert!(s.meta.packs(&[orphan.id]).await.is_err(), "the row is gone");
    // Nothing else is an orphan.
    assert!(c.collect_garbage().await.unwrap().packs_deleted.is_empty());

    let parent = Work {
        path: s.dir.path().to_owned(),
        home: s.home.clone(),
    };
    git(&parent, &["clone", "-q", &s.url(), "after"]).await;
    let after = Work {
        path: s.dir.path().join("after"),
        home: s.home.clone(),
    };
    git(&after, &["fsck", "--strict", "--no-dangling"]).await;
    assert_eq!(after.rev("HEAD"), w.rev("main"));
}

/// Registers and uploads a pack of `side`'s objects that no version names.
async fn store_orphan(s: &Server, side: &Work) -> aspen_core::pack::PackRecord {
    use aspen_pack::ingest::{CompleteOptions, complete, receive};
    let tip = side.rev("HEAD");
    let pack = {
        let mut child = side
            .command()
            .args(["pack-objects", "--stdout", "--revs", "-q"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(format!("{tip}\n").as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        out.stdout
    };
    let work = tempfile::tempdir().unwrap();
    let limits = aspen_core::pack::PushLimits {
        max_objects: 1_000,
        max_pack_bytes: 64 * MIB,
    };
    let received = receive(&mut pack.as_slice(), &work.path().join("spool"), &limits).unwrap();
    let options = CompleteOptions::new(
        MaxObjectBytes::new(64 * MIB).unwrap(),
        MaxInflatedBytes::new(1024 * MIB).unwrap(),
    );
    let done = complete(
        &received,
        &aspen_core::ingest::InMemoryBases::default(),
        work.path(),
        &options,
    )
    .unwrap();
    let new = done.new_pack(s.repo.id);
    let record = s.meta.register_pack(&new).await.unwrap();
    let keys = record.keys();
    use aspen_core::blob::BlobStore;
    s.blobs
        .upload_file(&keys.pack, &done.pack_path)
        .await
        .unwrap();
    s.blobs
        .upload_file(&keys.idx, &done.idx_path)
        .await
        .unwrap();
    s.blobs
        .upload_file(&keys.entries, &done.entries_path)
        .await
        .unwrap();
    record
}

/// Invariant: a write with more commits than the push indexes itself gets
/// its tip's row and an `index_commits` job; the worker then writes a row,
/// naming that version, for every commit the tip reaches in its pack, read
/// from the stored pack, with the parents and times Git gives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_index_commits_job_writes_the_rest_of_a_large_push() {
    use aspen_core::ingest::SortedIds;
    use aspen_core::meta::CommitLookup;
    let s = Server::start_with(
        |c| {
            c.sync_commit_rows = 2;
            c.cover_layout = CoverLayout::new(FANOUT).unwrap();
        },
        ForcePolicy::Branches,
    )
    .await;
    let w = s.clone_dir("w");
    let mut commits = Vec::new();
    for i in 0..10u32 {
        commits.push(w.commit("a.txt", &format!("{i}\n"), &format!("c{i}")));
    }
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let v = s.live("refs/heads/main").await;
    let ids = SortedIds::from_unsorted(commits.iter().map(|c| oid(c)).collect());
    let indexed = |rows: &[CommitLookup]| {
        rows.iter()
            .filter(|r| matches!(r, CommitLookup::Indexed(_)))
            .count()
    };
    let before = s.meta.lookup_commits(s.repo.id, &ids).await.unwrap();
    assert_eq!(indexed(&before), 1, "the push wrote only the tip's row");

    assert_eq!(drain(&s).await, 1, "one index_commits job, no compact job");
    let after = s.meta.lookup_commits(s.repo.id, &ids).await.unwrap();
    assert_eq!(indexed(&after), 10);
    for (id, row) in ids.as_slice().iter().zip(&after) {
        let CommitLookup::Indexed(row) = row else {
            unreachable!()
        };
        assert_eq!(row.ref_version, v.id);
        let hex = id.to_hex();
        let parents: Vec<ObjectId> = w
            .git(&["rev-list", "--parents", "-n", "1", &hex])
            .split_whitespace()
            .skip(1)
            .map(oid)
            .collect();
        assert_eq!(row.parents, parents, "{hex}");
        let time: i64 = w
            .git(&["show", "-s", "--format=%ct", &hex])
            .trim()
            .parse()
            .unwrap();
        assert_eq!(row.committer_time, time, "{hex}");
    }
}

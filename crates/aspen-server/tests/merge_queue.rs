//! The merge queue ([`aspen_server::queue`]) end to end: stock `git`
//! enqueues changes by pushing to `refs/queue/main/...`, the worker runs one
//! pass at a time ([`MergeQueue::run_once`], no timers), and stock `git`
//! checks what landed on `main` and the outcome refs.

mod receive_support;

use aspen_server::queue::{MergeQueue, QueueConfig};
use receive_support::*;

async fn start() -> Server {
    Server::start_with(|c| c.merge_queue = true, Default::default()).await
}

fn queue(s: &Server) -> MergeQueue<aspen_meta::SqlStore, aspen_blob::LocalBlobStore> {
    MergeQueue::new(s.svc.clone(), QueueConfig::default())
}

async fn push(w: &Work, url: &str, args: &[&str]) -> (bool, String) {
    let mut cmd = w.command();
    cmd.arg("push").arg("--porcelain").arg(url).args(args);
    blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await
}

/// `main` with `base.txt` and `shared.txt`, and a working clone of it.
async fn seeded(s: &Server, name: &str) -> Work {
    let w = s.clone_dir(name);
    w.commit("base.txt", "base\n", "base");
    w.commit("shared.txt", "one\n", "shared");
    let (ok, out) = push(&w, &s.url(), &["main"]).await;
    assert!(ok, "{out}");
    w
}

/// A client that fetched `main` and works on top of it.
async fn client(s: &Server, name: &str) -> Work {
    let w = s.clone_dir(name);
    w.git(&["fetch", "-q", &s.url(), "main"]);
    w.git(&["reset", "-q", "--hard", "FETCH_HEAD"]);
    w
}

/// Every ref the server lists under `prefix`, as (name, object).
fn ls_remote(w: &Work, url: &str, prefix: &str) -> Vec<(String, String)> {
    w.git(&["ls-remote", url, &format!("{prefix}*")])
        .lines()
        .filter_map(|l| l.split_once('\t'))
        .map(|(oid, name)| (name.to_owned(), oid.to_owned()))
        .collect()
}

/// A clone of everything, checked with `git fsck --strict`.
async fn checked_clone(s: &Server, name: &str) -> Work {
    let parent = Work {
        path: s.dir.path().to_owned(),
        home: s.home.clone(),
    };
    let (url, n) = (s.url(), name.to_owned());
    let p = parent.path.clone();
    let h = parent.home.clone();
    blocking(move || git_ok(&h, &p, &["clone", "-q", &url, &n])).await;
    let w = Work {
        path: s.dir.path().join(name),
        home: s.home.clone(),
    };
    w.git(&["fsck", "--strict", "--no-dangling"]);
    w
}

/// Invariant: non-conflicting entries from several clients land in one
/// batch, one trunk commit each with its author's message and file, on
/// top of each other; every queue ref moves to its done ref at the commit
/// that landed it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_conflicting_entries_land_in_one_batch() {
    let s = start().await;
    let seed = seeded(&s, "seed").await;
    let base = seed.rev("main");
    for i in 0..4 {
        let w = client(&s, &format!("c{i}")).await;
        w.commit(
            &format!("dir{i}/file.txt"),
            &format!("client {i}\n"),
            &format!("change {i}"),
        );
        let (ok, out) = push(&w, &s.url(), &[&format!("HEAD:refs/queue/main/c{i}")]).await;
        assert!(ok, "{out}");
    }
    let report = queue(&s).run_once().await.unwrap();
    assert_eq!((report.batches, report.merged, report.rejected), (1, 4, 0));

    let full = checked_clone(&s, "full").await;
    let log = full.git(&["log", "--format=%s", &format!("{base}..origin/main")]);
    let mut subjects: Vec<&str> = log.lines().collect();
    subjects.sort();
    assert_eq!(subjects, ["change 0", "change 1", "change 2", "change 3"]);
    assert_eq!(
        full.git(&["rev-list", "--count", &format!("{base}..origin/main")])
            .trim(),
        "4",
        "one trunk commit per entry, in a line"
    );
    for i in 0..4 {
        assert_eq!(
            full.git(&["show", &format!("origin/main:dir{i}/file.txt")]),
            format!("client {i}\n")
        );
    }
    assert_eq!(full.git(&["show", "origin/main:base.txt"]), "base\n");
    assert!(ls_remote(&full, &s.url(), "refs/queue/").is_empty());
    let done = ls_remote(&full, &s.url(), "refs/queue-done/main/");
    assert_eq!(done.len(), 4);
    for (name, oid) in done {
        let (ok, _) = full.try_git(&["merge-base", "--is-ancestor", &oid, "origin/main"]);
        assert!(ok, "{name} at {oid} is on main");
    }
    // Nothing left: another pass lands nothing.
    assert_eq!(queue(&s).run_once().await.unwrap().merged, 0);
}

/// Invariant: changes adding different files to the same new directory
/// both land (conflicts are per file, not per directory).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_files_in_one_new_directory_do_not_conflict() {
    let s = start().await;
    let _seed = seeded(&s, "seed").await;
    for i in 0..3 {
        let w = client(&s, &format!("c{i}")).await;
        w.commit(
            &format!("agents/out/{i}.txt"),
            &format!("{i}\n"),
            &format!("agent {i}"),
        );
        assert!(
            push(&w, &s.url(), &[&format!("HEAD:refs/queue/main/c{i}")])
                .await
                .0
        );
    }
    let report = queue(&s).run_once().await.unwrap();
    assert_eq!((report.merged, report.rejected), (3, 0), "{report:?}");
    let full = checked_clone(&s, "full").await;
    for i in 0..3 {
        assert_eq!(
            full.git(&["show", &format!("origin/main:agents/out/{i}.txt")]),
            format!("{i}\n")
        );
    }
}

/// Invariant: an entry that changes a path trunk changed since its base
/// is rejected with a report naming the path, and the other entries land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflicting_entry_is_rejected_and_the_rest_land() {
    let s = start().await;
    let seed = seeded(&s, "seed").await;
    let stale = client(&s, "stale").await;
    // Trunk changes shared.txt after `stale` fetched it.
    seed.commit("shared.txt", "two\n", "trunk edits shared");
    assert!(push(&seed, &s.url(), &["main"]).await.0);
    stale.commit("shared.txt", "mine\n", "stale edits shared");
    assert!(
        push(&stale, &s.url(), &["HEAD:refs/queue/main/stale"])
            .await
            .0
    );
    let ok = client(&s, "ok").await;
    ok.commit("other.txt", "fine\n", "independent");
    assert!(push(&ok, &s.url(), &["HEAD:refs/queue/main/ok"]).await.0);

    let report = queue(&s).run_once().await.unwrap();
    assert_eq!((report.merged, report.rejected), (1, 1));
    let full = checked_clone(&s, "full").await;
    assert_eq!(full.git(&["show", "origin/main:shared.txt"]), "two\n");
    assert_eq!(full.git(&["show", "origin/main:other.txt"]), "fine\n");
    let rejected = ls_remote(&full, &s.url(), "refs/queue-rejected/main/");
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].0, "refs/queue-rejected/main/stale");
    full.git(&["fetch", "-q", &s.url(), "refs/queue-rejected/main/stale"]);
    let report = full.git(&["log", "-1", "--format=%B", "FETCH_HEAD"]);
    assert!(report.contains("shared.txt"), "{report}");
    assert!(
        report.contains(&stale.rev("HEAD")),
        "names the commit: {report}"
    );
    assert!(ls_remote(&full, &s.url(), "refs/queue/").is_empty());
}

/// A delete against a modify of the same file is a conflict too, and so
/// is a change below a directory trunk deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_against_modify_conflicts() {
    let s = start().await;
    let seed = s.clone_dir("seed");
    seed.commit("keep/a.txt", "a\n", "a");
    seed.commit("gone/b.txt", "b\n", "b");
    assert!(push(&seed, &s.url(), &["main"]).await.0);
    let modifier = client(&s, "modifier").await;
    let nested = client(&s, "nested").await;
    // Trunk deletes keep/a.txt and the whole gone/ directory.
    seed.git(&["rm", "-q", "keep/a.txt", "-r", "gone"]);
    seed.git(&["commit", "-q", "-m", "delete"]);
    assert!(push(&seed, &s.url(), &["main"]).await.0);
    modifier.commit("keep/a.txt", "changed\n", "modify a");
    assert!(
        push(&modifier, &s.url(), &["HEAD:refs/queue/main/modify"])
            .await
            .0
    );
    nested.commit("gone/b.txt", "changed\n", "modify b");
    assert!(
        push(&nested, &s.url(), &["HEAD:refs/queue/main/nested"])
            .await
            .0
    );
    let report = queue(&s).run_once().await.unwrap();
    assert_eq!((report.merged, report.rejected), (0, 2));
    let full = checked_clone(&s, "full").await;
    assert_eq!(
        ls_remote(&full, &s.url(), "refs/queue-rejected/main/").len(),
        2
    );
    assert_eq!(
        full.rev("origin/main"),
        seed.rev("HEAD"),
        "trunk did not move"
    );
}

/// Invariant: two entries in one batch that touch the same path cannot
/// both land as separate changes: the later one is rejected. A multi-commit
/// entry lands squashed into one trunk commit, and an entry that deletes a
/// file and adds a directory lands both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_batch_overlap_rejects_the_later_entry_and_squashes() {
    let s = start().await;
    let _seed = seeded(&s, "seed").await;
    let first = client(&s, "first").await;
    first.commit("x.txt", "first\n", "first x");
    assert!(
        push(&first, &s.url(), &["HEAD:refs/queue/main/1-first"])
            .await
            .0
    );
    let second = client(&s, "second").await;
    second.commit("x.txt", "second\n", "second x");
    assert!(
        push(&second, &s.url(), &["HEAD:refs/queue/main/2-second"])
            .await
            .0
    );
    let multi = client(&s, "multi").await;
    multi.git(&["rm", "-q", "base.txt"]);
    multi.git(&["commit", "-q", "-m", "drop base"]);
    multi.commit("new/deep/file.txt", "deep\n", "add new dir");
    assert!(
        push(&multi, &s.url(), &["HEAD:refs/queue/main/3-multi"])
            .await
            .0
    );
    let report = queue(&s).run_once().await.unwrap();
    assert_eq!((report.merged, report.rejected), (2, 1));
    let full = checked_clone(&s, "full").await;
    assert_eq!(full.git(&["show", "origin/main:x.txt"]), "first\n");
    assert_eq!(
        full.git(&["show", "origin/main:new/deep/file.txt"]),
        "deep\n"
    );
    let (has_base, _) = full.try_git(&["cat-file", "-e", "origin/main:base.txt"]);
    assert!(!has_base, "base.txt was deleted");
    assert_eq!(
        full.git(&["log", "-1", "--format=%s", "origin/main"])
            .trim(),
        "add new dir",
        "the two-commit entry landed as one commit with its tip's message"
    );
    let rejected = ls_remote(&full, &s.url(), "refs/queue-rejected/main/");
    assert_eq!(rejected.len(), 1);
    assert!(rejected[0].0.ends_with("2-second"));
}

/// Invariant: when trunk moves between the worker reading it and its
/// batch landing (a direct push, or another instance's worker), the batch
/// is refused as a whole and rebuilt; every entry lands exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trunk_push_racing_the_worker_lands_every_entry_once() {
    let s = start().await;
    let seed = seeded(&s, "seed").await;
    for i in 0..3 {
        let w = client(&s, &format!("c{i}")).await;
        w.commit(
            &format!("f{i}.txt"),
            &format!("{i}\n"),
            &format!("entry {i}"),
        );
        assert!(
            push(&w, &s.url(), &[&format!("HEAD:refs/queue/main/c{i}")])
                .await
                .0
        );
    }
    // Two workers race each other, and a direct trunk push races both.
    seed.commit("direct.txt", "direct\n", "direct push");
    let url = s.url();
    let direct = push(&seed, &url, &["main"]);
    let (a, b) = (queue(&s), queue(&s));
    let (ra, rb, (ok, out)) = tokio::join!(a.run_once(), b.run_once(), direct);
    let (ra, rb) = (ra.unwrap(), rb.unwrap());
    // The direct push may lose to a batch (non-fast-forward): retry it the
    // way a client would.
    if !ok {
        assert!(
            out.contains("fetch first") || out.contains("non-fast-forward"),
            "{out}"
        );
        seed.git(&["pull", "-q", "--rebase", &s.url(), "main"]);
        assert!(push(&seed, &s.url(), &["HEAD:main"]).await.0);
    }
    let rest = queue(&s).run_once().await.unwrap();
    // Each worker counts what its push landed; a worker that rebuilt the
    // same batch in the same second wrote identical commits, which land
    // once (its push is a no-op replay). What landed is checked below.
    assert!(
        ra.merged + rb.merged + rest.merged >= 3,
        "{ra:?} {rb:?} {rest:?}"
    );
    let full = checked_clone(&s, "full").await;
    let log = full.git(&["log", "--format=%s", "origin/main"]);
    for i in 0..3 {
        assert_eq!(
            log.lines().filter(|l| *l == format!("entry {i}")).count(),
            1,
            "entry {i} landed once: {log}"
        );
    }
    assert!(log.lines().any(|l| l == "direct push"), "{log}");
    assert_eq!(ls_remote(&full, &s.url(), "refs/queue-done/main/").len(), 3);
}

/// With the queue off, `refs/queue/...` is outside the writable
/// namespaces and the worker has nothing to do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_the_queue_off_queue_refs_are_refused() {
    let s = Server::start().await;
    let seed = seeded(&s, "seed").await;
    seed.commit("q.txt", "q\n", "queued");
    let (ok, out) = push(&seed, &s.url(), &["HEAD:refs/queue/main/x"]).await;
    assert!(!ok, "{out}");
    assert!(out.contains("refs/queue/main/x"), "{out}");
    let report = queue(&s).run_once().await.unwrap();
    assert_eq!(report.merged + report.rejected, 0);
}

/// Pushes trunk from a working clone the first time the queue worker's
/// trunk update is checked: after the worker read trunk and built its
/// batch, before the batch commits. That is the race with a direct push,
/// made deterministic.
#[derive(Debug, Default)]
struct PushTrunkFirst {
    fired: std::sync::atomic::AtomicBool,
    clone: std::sync::OnceLock<(std::path::PathBuf, std::path::PathBuf, String)>,
}

impl aspen_server::receive::hook::PushHook for PushTrunkFirst {
    fn check<'a>(
        &'a self,
        update: aspen_server::receive::hook::HookUpdate<'a>,
        _: &'a mut dyn aspen_server::receive::hook::HookObjects,
    ) -> aspen_server::receive::hook::HookFuture<
        'a,
        Result<aspen_server::receive::hook::HookVerdict, aspen_core::error::StoreError>,
    > {
        Box::pin(async move {
            let worker = update.caller.principal.is_empty();
            if worker
                && update.name.as_str() == "refs/heads/main"
                && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                let (home, path, url) = self.clone.get().expect("set").clone();
                blocking(move || git_ok(&home, &path, &["push", "-q", &url, "main"])).await;
            }
            Ok(aspen_server::receive::hook::HookVerdict::Accept)
        })
    }

    fn committed<'a>(
        &'a self,
        _: &'a aspen_core::repo::Repo,
        _: &'a aspen_server::receive::hook::Caller,
        _: &'a [aspen_server::receive::hook::CommittedUpdate],
    ) -> aspen_server::receive::hook::HookFuture<'a, ()> {
        Box::pin(async {})
    }
}

/// Invariant: a trunk push that commits after the worker read trunk and
/// before its batch commits makes the batch's compare-and-swap fail; the
/// atomic push is refused as a whole (no queue ref moves), the batch is
/// rebuilt on the new trunk, and every entry lands exactly once, on top of
/// the direct push.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_compare_and_swap_rebuilds_the_batch() {
    let hook = std::sync::Arc::new(PushTrunkFirst::default());
    let h = hook.clone();
    let s = Server::start_with(
        move |c| {
            c.merge_queue = true;
            c.hook = Some(h);
        },
        Default::default(),
    )
    .await;
    let seed = seeded(&s, "seed").await;
    seed.commit("direct.txt", "direct\n", "direct push");
    hook.clone
        .set((seed.home.clone(), seed.path.clone(), s.url()))
        .unwrap();
    for i in 0..3 {
        let w = client(&s, &format!("c{i}")).await;
        w.commit(
            &format!("f{i}.txt"),
            &format!("{i}\n"),
            &format!("entry {i}"),
        );
        assert!(
            push(&w, &s.url(), &[&format!("HEAD:refs/queue/main/c{i}")])
                .await
                .0
        );
    }
    let report = queue(&s).run_once().await.unwrap();
    assert_eq!(report.refused, 1, "{report:?}");
    assert_eq!((report.batches, report.merged), (1, 3), "{report:?}");
    let full = checked_clone(&s, "full").await;
    let log = full.git(&["log", "--format=%s", "origin/main"]);
    let subjects: Vec<&str> = log.lines().take(4).collect();
    assert_eq!(
        subjects[3], "direct push",
        "the batch landed on the direct push: {log}"
    );
    for i in 0..3 {
        assert_eq!(
            log.lines().filter(|l| *l == format!("entry {i}")).count(),
            1,
            "{log}"
        );
    }
    assert_eq!(ls_remote(&full, &s.url(), "refs/queue-done/main/").len(), 3);
    assert!(ls_remote(&full, &s.url(), "refs/queue/").is_empty());
}

/// Invariant: outcome refs older than the TTL are deleted, and fresher
/// ones (and the trunk commits they named) stay. Without expiry every
/// outcome stayed a ref for ever, and full ref listings grew with every
/// merged change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcomes_older_than_the_ttl_are_deleted() {
    let s = start().await;
    seeded(&s, "seed").await;
    const TTL: std::time::Duration = std::time::Duration::from_secs(2);
    // Outcomes are written by a queue that keeps them, and expired by one
    // with a TTL, so a slow machine cannot expire them early.
    let keep = MergeQueue::new(
        s.svc.clone(),
        QueueConfig {
            outcome_ttl: None,
            ..QueueConfig::default()
        },
    );
    let expiring = MergeQueue::new(
        s.svc.clone(),
        QueueConfig {
            outcome_ttl: Some(TTL),
            ..QueueConfig::default()
        },
    );
    let enqueue = |name: &'static str, file: &'static str| {
        let s = &s;
        async move {
            let w = client(s, name).await;
            w.commit(file, &format!("{name}\n"), name);
            let (ok, out) = push(&w, &s.url(), &[&format!("HEAD:refs/queue/main/{name}")]).await;
            assert!(ok, "{out}");
            w
        }
    };
    // `old` lands, and so does a conflicting `bad` (rejected: base.txt).
    enqueue("old", "old.txt").await;
    let report = keep.run_once().await.unwrap();
    assert_eq!((report.merged, report.expired), (1, 0), "{report:?}");
    let bad = client(&s, "bad").await;
    bad.git(&["reset", "-q", "--hard", "HEAD~2"]);
    bad.commit("base.txt", "conflict\n", "bad");
    let (ok, out) = push(&bad, &s.url(), &["HEAD:refs/queue/main/bad"]).await;
    assert!(ok, "{out}");
    let main = s.clone_dir("main");
    let other = client(&s, "other").await;
    other.commit("base.txt", "trunk moved\n", "trunk");
    assert!(push(&other, &s.url(), &["HEAD:main"]).await.0);
    let report = keep.run_once().await.unwrap();
    assert_eq!(report.rejected, 1, "{report:?}");
    assert_eq!(ls_remote(&main, &s.url(), "refs/queue-done/main/").len(), 1);
    assert_eq!(
        ls_remote(&main, &s.url(), "refs/queue-rejected/main/").len(),
        1
    );
    // Past the TTL, `fresh` lands: the two old outcomes go, its own stays.
    tokio::time::sleep(TTL + std::time::Duration::from_millis(100)).await;
    enqueue("fresh", "fresh.txt").await;
    let report = expiring.run_once().await.unwrap();
    assert_eq!((report.merged, report.expired), (1, 2), "{report:?}");
    let done = ls_remote(&main, &s.url(), "refs/queue-done/main/");
    assert_eq!(
        done.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        ["refs/queue-done/main/fresh"]
    );
    assert!(ls_remote(&main, &s.url(), "refs/queue-rejected/main/").is_empty());
    let full = checked_clone(&s, "full").await;
    assert_eq!(
        full.git(&["show", "origin/main:old.txt"]),
        "old\n",
        "the change stays"
    );
    assert_eq!(full.git(&["show", "origin/main:fresh.txt"]), "fresh\n");

    // With no TTL, nothing expires.
    tokio::time::sleep(TTL + std::time::Duration::from_millis(100)).await;
    assert_eq!(keep.run_once().await.unwrap().expired, 0);
    assert_eq!(ls_remote(&main, &s.url(), "refs/queue-done/main/").len(), 1);
}

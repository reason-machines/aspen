//! The receive-pack service end to end: stock `git push` over smart HTTP
//! (and hand-built requests for shapes `git push` never sends), checked
//! against what a fetch relies on (`receive_support::Server::check_ref`).

mod receive_support;

use aspen_core::ingest::ObjectKind;
use aspen_core::refs::Closure;
use aspen_core::repo::ForcePolicy;
use receive_support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_push_stores_one_exact_self_contained_pack() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    w.commit("b/c.txt", "two\n", "two");
    let url = s.url();
    let (ok, out) = blocking({
        let cmd = w.command();
        move || {
            let mut cmd = cmd;
            let o = cmd.args(["push", &url, "main"]).output().unwrap();
            (
                o.status.success(),
                String::from_utf8_lossy(&o.stderr).into_owned(),
            )
        }
    })
    .await;
    assert!(ok, "{out}");
    let v = s.check_ref("refs/heads/main", &w).await;
    assert_eq!(v.value.closure(), Some(Closure::Exact));
    assert_eq!(v.deps.trunk, None, "the first push depends on nothing");
    assert!(v.value.pack().is_some());
    let row = &s.pack_rows(&[v.value.pack().unwrap()]).await[0];
    assert_eq!(row.pack.object_count as usize, w.objects("main").len());
}

async fn push(w: &Work, url: &str, args: &[&str]) -> (bool, String) {
    let mut cmd = w.command();
    cmd.arg("push").arg(url).args(args);
    blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_thin_push_is_completed_and_depends_on_the_trunk_it_searched() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    // A file large enough that git sends the next version as a delta.
    let big: String = (0..2000).map(|i| format!("line {i}\n")).collect();
    w.commit("big.txt", &big, "one");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let first = s.live("refs/heads/main").await;
    w.commit("big.txt", &format!("{big}more\n"), "two");
    let (ok, out) = push(&w, &s.url(), &["main"]).await;
    assert!(ok, "{out}");
    let v = s.check_ref("refs/heads/main", &w).await;
    assert_eq!(v.deps.trunk, Some(first.id));
    assert_eq!(v.value.closure(), Some(Closure::Exact));
    let row = &s.pack_rows(&[v.value.pack().unwrap()]).await[0];
    let aspen_core::pack::PackOrigin::Push { received_bytes } = row.pack.origin else {
        panic!("a push pack")
    };
    // The client sent a thin pack (a delta against big.txt's first version);
    // the stored pack carries that base, so it is larger.
    assert!(
        received_bytes < row.pack.size,
        "received {received_bytes}, stored {}",
        row.pack.size
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_ref_push_stores_one_pack_per_write() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.commit("a.txt", "two\n", "two");
    w.git(&["tag", "-a", "v1", "-m", "release one"]);
    w.git(&["checkout", "-q", "-b", "feature"]);
    w.commit("f.txt", "feature\n", "feature");
    w.git(&["checkout", "-q", "main"]);
    let (ok, out) = push(&w, &s.url(), &["--follow-tags", "main", "feature"]).await;
    assert!(ok, "{out}");
    let main = s.check_ref("refs/heads/main", &w).await;
    let tag = s.check_ref("refs/tags/v1", &w).await;
    let feature = s.check_ref("refs/heads/feature", &w).await;
    for v in [&main, &tag, &feature] {
        assert_eq!(v.value.closure(), Some(Closure::Exact), "{}", v.name);
    }
    // main's own pack holds neither the tag object nor feature's objects,
    // so a have of main's tip can never subtract them.
    let main_pack = s.pack_objects(&s.pack_rows(&[main.value.pack().unwrap()]).await[0]);
    let tag_obj = oid(&w.rev("v1"));
    assert!(!main_pack.contains(&tag_obj));
    assert!(!main_pack.contains(&oid(&w.rev("feature"))));
    assert_ne!(main.value.pack(), tag.value.pack());
    assert_ne!(main.value.pack(), feature.value.pack());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_refs_at_one_new_commit_share_one_pack() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.commit("a.txt", "two\n", "two");
    let (ok, out) = push(&w, &s.url(), &["HEAD:refs/heads/x", "HEAD:refs/heads/y"]).await;
    assert!(ok, "{out}");
    let x = s.check_ref("refs/heads/x", &w).await;
    let y = s.check_ref("refs/heads/y", &w).await;
    assert!(x.value.pack().is_some());
    assert_eq!(x.value.pack(), y.value.pack());
    // Same tip, same objects: the shared pack is exactly what each reaches.
    assert_eq!(x.value.closure(), Some(Closure::Exact));
    assert_eq!(y.value.closure(), Some(Closure::Exact));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_at_an_existing_commit_stores_no_pack() {
    // Only tips get commit rows (sync_commit_rows = 0), so c1 has none.
    let s = Server::start_with(|c| c.sync_commit_rows = 0, ForcePolicy::Branches).await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "c1");
    w.commit("a.txt", "two\n", "c2");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let c1 = aspen_core::ingest::SortedIds::from_unsorted(vec![oid(&w.rev("HEAD~1"))]);
    let lookup = |s: &Server| {
        let (meta, repo, c1) = (s.meta.clone(), s.repo.id, c1.clone());
        async move {
            aspen_core::meta::MetaStore::lookup_commits(&meta, repo, &c1)
                .await
                .unwrap()
        }
    };
    assert!(matches!(
        lookup(&s).await[0],
        aspen_core::meta::CommitLookup::NotIndexed
    ));
    let (ok, out) = push(&w, &s.url(), &["HEAD~1:refs/heads/copy"]).await;
    assert!(ok, "{out}");
    w.git(&["branch", "copy", "HEAD~1"]);
    let v = s.check_ref("refs/heads/copy", &w).await;
    assert_eq!(v.value.pack(), None);
    assert_eq!(
        v.value.closure(),
        Some(Closure::Wider),
        "trunk (c2) is not c1's ancestor"
    );
    // Its tip got a commit row even though the push brought no commit.
    let row = lookup(&s).await;
    let aspen_core::meta::CommitLookup::Indexed(row) = &row[0] else {
        panic!("c1 is indexed now")
    };
    assert_eq!(row.ref_version, v.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_fast_forwards_need_an_explicit_force_and_the_policy() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "topic"]);
    w.commit("t.txt", "t1\n", "t1");
    assert!(push(&w, &s.url(), &["topic"]).await.0);
    w.git(&["reset", "-q", "--hard", "main"]);
    w.commit("t.txt", "t2\n", "t2 rewritten");
    // `--force` is invisible for an unadvertised branch (S2).
    let (ok, out) = push(&w, &s.url(), &["--force", "topic"]).await;
    assert!(!ok);
    assert!(out.contains("non-fast-forward"), "{out}");
    let (ok, out) = push(&w, &s.url(), &["-o", "force", "topic"]).await;
    assert!(ok, "{out}");
    let v = s.check_ref("refs/heads/topic", &w).await;
    assert_eq!(v.value.tip().unwrap(), oid(&w.rev("topic")));
    // Trunk may not be forced under the default policy.
    w.git(&["checkout", "-q", "main"]);
    w.git(&["commit", "-q", "--amend", "-m", "rewritten"]);
    let (ok, out) = push(&w, &s.url(), &["--force", "main"]).await;
    assert!(!ok);
    assert!(out.contains("allow_force"), "{out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forced_trunk_update_stays_exact() {
    let s = Server::start_with(|_| {}, ForcePolicy::All).await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "t1");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let t1 = s.live("refs/heads/main").await;
    w.commit("b.txt", "only in t2\n", "t2");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.commit("c.txt", "t3\n", "t3");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    // Rewind to t1 and commit b.txt's content again: the client reuses the
    // blob trunk already has (in t2's pack), which t1 does not reach.
    w.git(&["reset", "-q", "--hard", "HEAD~2"]);
    w.commit("b2.txt", "only in t2\n", "t1'");
    let (ok, out) = push(&w, &s.url(), &["--force", "main"]).await;
    assert!(ok, "{out}");
    let v = s.check_ref("refs/heads/main", &w).await;
    assert_eq!(
        v.value.closure(),
        Some(Closure::Exact),
        "the forced trunk version is exact"
    );
    assert_eq!(
        v.deps.trunk,
        Some(t1.id),
        "depends on the newest ancestor trunk version"
    );
    // A later ordinary trunk push stays exact too.
    w.commit("d.txt", "after\n", "t4");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let later = s.check_ref("refs/heads/main", &w).await;
    assert_eq!(later.value.closure(), Some(Closure::Exact));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_orphan_forced_trunk_update_stores_its_full_closure() {
    let s = Server::start_with(|_| {}, ForcePolicy::All).await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "t1");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.git(&["checkout", "-q", "--orphan", "fresh"]);
    w.commit("a.txt", "one\n", "orphan root reusing a blob");
    let (ok, out) = push(&w, &s.url(), &["--force", "fresh:main"]).await;
    assert!(ok, "{out}");
    w.git(&["branch", "-q", "-f", "main", "fresh"]);
    let v = s.check_ref("refs/heads/main", &w).await;
    assert_eq!(v.deps.trunk, None);
    assert_eq!(v.value.closure(), Some(Closure::Exact));
    // Its own pack holds everything it reaches, including the reused blob.
    let own = s.pack_objects(&s.pack_rows(&[v.value.pack().unwrap()]).await[0]);
    assert_eq!(own, w.objects("main"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_pushes_to_one_branch_have_one_winner() {
    let s = Server::start().await;
    let base = s.clone_dir("base");
    base.commit("a.txt", "base\n", "base");
    assert!(push(&base, &s.url(), &["main"]).await.0);
    let mut tasks = Vec::new();
    for i in 0..4 {
        let w = s.clone_dir(&format!("w{i}"));
        w.git(&["fetch", "-q", base.path.to_str().unwrap(), "main"]);
        w.git(&["reset", "-q", "--hard", "FETCH_HEAD"]);
        w.commit("a.txt", &format!("racer {i}\n"), &format!("racer {i}"));
        let url = s.url();
        tasks.push(tokio::spawn(async move {
            let r = push(&w, &url, &["main:refs/heads/race"]).await;
            (r, w)
        }));
    }
    let mut winners = Vec::new();
    for t in tasks {
        let ((ok, out), w) = t.await.unwrap();
        if ok {
            winners.push(w);
        } else {
            assert!(
                aspen_testkit::RACE_LOSS_REASONS
                    .iter()
                    .any(|p| out.contains(p)),
                "a loser's reason names the race: {out}"
            );
        }
    }
    assert_eq!(winners.len(), 1, "exactly one racer creates the branch");
    let v = s.check_ref("refs/heads/race", &winners[0]).await;
    assert_eq!(v.value.tip().unwrap(), oid(&winners[0].rev("HEAD")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_need_a_known_old_value_or_force() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    assert!(
        push(
            &w,
            &s.url(),
            &["main", "main:refs/heads/gone", "main:refs/heads/gone2"]
        )
        .await
        .0
    );
    let (ok, out) = push(&w, &s.url(), &["--delete", "refs/heads/gone"]).await;
    assert!(!ok);
    assert!(out.contains("-o force"), "{out}");
    let (ok, out) = push(
        &w,
        &s.url(),
        &["-o", "force", "--delete", "refs/heads/gone"],
    )
    .await;
    assert!(ok, "{out}");
    assert_eq!(
        s.current("refs/heads/gone").await.unwrap().value.tip(),
        None
    );
    // Through the branch-scoped URL the client knows the old value.
    let (ok, out) = push(&w, &s.scoped_url("gone2"), &["--delete", "gone2"]).await;
    assert!(ok, "{out}");
    assert_eq!(
        s.current("refs/heads/gone2").await.unwrap().value.tip(),
        None
    );
    let (ok, out) = push(&w, &s.url(), &["-o", "force", "--delete", "main"]).await;
    assert!(!ok);
    assert!(out.contains("trunk"), "{out}");
}

/// Issue #133: a delete of a ref that does not exist succeeds as a no-op
/// with a warning, as stock `git receive-pack` does ("deleting a
/// non-existent ref"), whether the ref never existed or was deleted
/// already, through the plain or the branch-scoped URL, even under
/// `--quiet`. Nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_of_a_missing_ref_is_a_noop_with_a_warning() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    assert!(
        push(&w, &s.url(), &["main", "main:refs/heads/gone"])
            .await
            .0
    );
    let (ok, out) = push(
        &w,
        &s.url(),
        &["-o", "force", "--delete", "refs/heads/gone"],
    )
    .await;
    assert!(ok, "{out}");
    let deleted = s.current("refs/heads/gone").await.unwrap();
    assert_eq!(deleted.value.tip(), None);

    // Stock git prints exactly this line, with no `aspen:` prefix.
    let warning = "remote: warning: deleting a non-existent ref";
    for (url, args) in [
        (s.url(), vec!["--delete", "refs/heads/gone"]),
        (s.url(), vec!["--quiet", "--delete", "refs/heads/gone"]),
        (
            s.scoped_url("gone"),
            vec!["--porcelain", ":refs/heads/gone"],
        ),
        (s.url(), vec!["--delete", "refs/heads/never"]),
        (
            s.scoped_url("never"),
            vec!["--porcelain", ":refs/heads/never"],
        ),
    ] {
        let (ok, out) = push(&w, &url, &args).await;
        assert!(ok, "{args:?}: {out}");
        assert!(out.contains(warning), "{args:?}: {out}");
    }
    assert_eq!(
        s.current("refs/heads/gone").await,
        Some(deleted),
        "no new version for a deleted ref"
    );
    assert_eq!(
        s.current("refs/heads/never").await,
        None,
        "no row for a ref that never existed"
    );
}

/// Issue #133 in a multi-ref push: the missing-ref delete is an `ok` no-op
/// with the warning next to an update, atomic or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_of_a_missing_ref_next_to_an_update_is_ok_with_a_warning() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    assert!(
        push(&w, &s.url(), &["main", "main:refs/heads/keep"])
            .await
            .0
    );
    let warning = "remote: warning: deleting a non-existent ref";
    for (i, atomic) in [true, false].into_iter().enumerate() {
        w.commit("a.txt", &format!("two {i}\n"), "two");
        let mut args = vec!["--porcelain", "main:refs/heads/keep", ":refs/heads/never"];
        if atomic {
            args.insert(0, "--atomic");
        }
        let (ok, out) = push(&w, &s.url(), &args).await;
        assert!(ok, "{args:?}: {out}");
        assert!(out.contains(warning), "{args:?}: {out}");
        assert!(
            out.contains("\t:refs/heads/never\t[deleted]"),
            "{args:?}: {out}"
        );
        let keep = s.check_ref("refs/heads/keep", &w).await;
        assert_eq!(keep.value.tip().unwrap(), oid(&w.rev("main")), "{args:?}");
        assert_eq!(s.current("refs/heads/never").await, None, "{args:?}");
    }
}

/// Issue #133 under `--atomic`: when another command of the push is
/// rejected, the missing-ref delete is reported failed like every other
/// command, nothing is written, and the warning, which only an accepted
/// delete prints, is not shown. Without `--atomic` the same push accepts
/// the delete with its warning and rejects only the non-fast-forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_atomic_push_reports_a_missing_ref_delete_failed() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    assert!(
        push(&w, &s.url(), &["main", "main:refs/heads/keep"])
            .await
            .0
    );
    let keep = s.current("refs/heads/keep").await.unwrap();
    w.git(&["checkout", "-q", "--orphan", "other"]);
    w.commit("z.txt", "z\n", "unrelated");
    let warning = "deleting a non-existent ref";

    let packs_before = s.stored_pack_files();
    let args = [
        "--atomic",
        "--porcelain",
        ":refs/heads/never",
        "other:refs/heads/keep",
        "other:refs/heads/new",
    ];
    let (ok, out) = push(&w, &s.url(), &args).await;
    assert!(!ok, "{out}");
    assert!(!out.contains(warning), "{out}");
    assert!(
        out.contains("!\t:refs/heads/never\t[remote rejected] (atomic push failed)"),
        "{out}"
    );
    assert!(
        out.contains(":refs/heads/keep\t[remote rejected] (non-fast-forward"),
        "{out}"
    );
    assert!(
        !out.contains("\t:refs/heads/never\t[deleted]"),
        "the delete is not reported ok: {out}"
    );
    assert_eq!(s.current("refs/heads/keep").await, Some(keep.clone()));
    assert_eq!(s.current("refs/heads/never").await, None);
    assert_eq!(s.current("refs/heads/new").await, None);
    assert_eq!(
        s.stored_pack_files(),
        packs_before,
        "a refused atomic push uploads nothing"
    );

    let (ok, out) = push(&w, &s.url(), &args[1..]).await;
    assert!(!ok, "{out}");
    assert!(out.contains(warning), "{out}");
    assert!(out.contains("\t:refs/heads/never\t[deleted]"), "{out}");
    assert!(
        out.contains(":refs/heads/keep\t[remote rejected] (non-fast-forward"),
        "{out}"
    );
    assert_eq!(s.current("refs/heads/keep").await, Some(keep));
    assert_eq!(s.current("refs/heads/never").await, None);
    let new = s.check_ref("refs/heads/new", &w).await;
    assert_eq!(new.value.tip().unwrap(), oid(&w.rev("other")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_atomic_push_is_all_or_nothing() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    assert!(
        push(&w, &s.url(), &["main", "main:refs/heads/keep"])
            .await
            .0
    );
    w.git(&["checkout", "-q", "--orphan", "other"]);
    w.commit("z.txt", "z\n", "unrelated");
    // `keep` is a non-fast-forward, so the new branch is refused too, and
    // nothing is stored for either write.
    let packs_before = s.stored_pack_files();
    let (ok, out) = push(
        &w,
        &s.url(),
        &["--atomic", "other:refs/heads/keep", "other:refs/heads/new"],
    )
    .await;
    assert!(!ok, "{out}");
    assert!(out.contains("atomic"), "{out}");
    assert!(s.current("refs/heads/new").await.is_none());
    assert_eq!(
        s.stored_pack_files(),
        packs_before,
        "a refused atomic push uploads nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_at_a_non_commit_is_refused() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let tree = w.rev("HEAD^{tree}");
    let (ok, out) = push(&w, &s.url(), &[&format!("{tree}:refs/heads/tree")]).await;
    assert!(!ok);
    assert!(out.contains("not a commit"), "{out}");
    assert!(s.current("refs/heads/tree").await.is_none());
    // A tag may point at anything.
    let (ok, out) = push(&w, &s.url(), &[&format!("{tree}:refs/tags/tree")]).await;
    assert!(ok, "{out}");
    let v = s.live("refs/tags/tree").await;
    assert_eq!(v.value.tip().unwrap(), oid(&tree));
    let _ = ObjectKind::Tree;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_advertisement_shows_trunk_and_log_spaced_haves() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    let mut tips = Vec::new();
    for i in 0..6 {
        tips.push(w.commit("a.txt", &format!("{i}\n"), &format!("c{i}")));
        assert!(push(&w, &s.url(), &["main"]).await.0);
    }
    w.git(&["branch", "side"]);
    assert!(push(&w, &s.url(), &["side"]).await.0);
    let body = s
        .svc
        .advertise(&s.repo, None, aspen_proto::service::ProtocolVersion::V0)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains(&format!("{} refs/heads/main", tips[5])));
    // 1, 2 and 4 versions back: seqs 5, 4 and 2.
    for i in [4, 3, 1] {
        assert!(text.contains(&format!("{} .have", tips[i])), "{text}");
    }
    assert!(!text.contains("refs/heads/side"));
    let scoped = s
        .svc
        .advertise(
            &s.repo,
            Some(&aspen_core::name::RefName::new("refs/heads/side").unwrap()),
            aspen_proto::service::ProtocolVersion::V0,
        )
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&scoped).contains("refs/heads/side"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_created_at_an_old_trunk_commit_is_wider() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "1\n", "c1");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.commit("a.txt", "2\n", "c2");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    // Off c1: its trunk dependency (c2) is not an ancestor of its tip, so
    // its closure holds c2's objects it does not reach.
    w.git(&["checkout", "-q", "-b", "old", "HEAD~1"]);
    w.commit("o.txt", "old\n", "on c1");
    assert!(push(&w, &s.url(), &["old"]).await.0);
    let v = s.check_ref("refs/heads/old", &w).await;
    assert_eq!(v.value.closure(), Some(Closure::Wider));
    // Off c2: exact.
    w.git(&["checkout", "-q", "-b", "new", "main"]);
    w.commit("n.txt", "new\n", "on c2");
    assert!(push(&w, &s.url(), &["new"]).await.0);
    let v = s.check_ref("refs/heads/new", &w).await;
    assert_eq!(v.value.closure(), Some(Closure::Exact));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scoped_push_depends_on_the_scoped_branch() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "1\n", "c1");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "topic"]);
    let big: String = (0..3000).map(|i| format!("topic line {i}\n")).collect();
    w.commit("t.txt", &big, "t1");
    assert!(push(&w, &s.url(), &["topic"]).await.0);
    let t1 = s.live("refs/heads/topic").await;
    // Through the scoped URL the client sees topic, so it sends t.txt's new
    // version as a delta against a blob that only topic's pack holds.
    w.commit("t.txt", &format!("{big}more\n"), "t2");
    let (ok, out) = push(&w, &s.scoped_url("topic"), &["topic"]).await;
    assert!(ok, "{out}");
    let v = s.check_ref("refs/heads/topic", &w).await;
    assert_eq!(v.deps.scoped_old, Some(t1.id));
    assert_eq!(v.value.closure(), Some(Closure::Exact));
    let row = &s.pack_rows(&[v.value.pack().unwrap()]).await[0];
    let aspen_core::pack::PackOrigin::Push { received_bytes } = row.pack.origin else {
        panic!()
    };
    assert!(received_bytes < 4000, "a thin pack: {received_bytes} bytes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refs_outside_the_callers_grant_are_refused_one_by_one() {
    // B3's ref-limited write grants: each command is checked on its own.
    let s = Server::start_full(
        |_| {},
        ForcePolicy::Branches,
        serde_json::json!({
            "anonymous": ["write:demo:refs/heads/main", "write:demo:refs/heads/agent/*"]
        }),
    )
    .await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    let (ok, out) = push(&w, &s.url(), &["main", "main:refs/heads/agent/x"]).await;
    assert!(ok, "{out}");
    let (ok, out) = push(&w, &s.url(), &["main:refs/heads/other"]).await;
    assert!(!ok);
    assert!(out.contains("permission denied"), "{out}");
    assert!(s.current("refs/heads/other").await.is_none());
    w.commit("a.txt", "two\n", "two");
    let (ok, out) = push(&w, &s.url(), &["--atomic", "main", "main:refs/heads/other"]).await;
    assert!(!ok, "{out}");
    assert_ne!(
        s.live("refs/heads/main").await.value.tip().unwrap(),
        oid(&w.rev("main")),
        "the atomic push moved nothing"
    );
}

/// Invariant (decisions log, "where a write's objects are stored"): a
/// non-trunk write whose objects are all in trunk's pack of the same push
/// shares that pack and is `Wider`; trunk stays `Exact` and nothing is
/// stored twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tag_inside_trunks_pack_shares_it_and_is_wider() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "c1");
    w.commit("a.txt", "two\n", "c2");
    w.git(&["tag", "lw", "HEAD~1"]);
    let (ok, out) = push(&w, &s.url(), &["main", "lw"]).await;
    assert!(ok, "{out}");
    let main = s.check_ref("refs/heads/main", &w).await;
    let lw = s.check_ref("refs/tags/lw", &w).await;
    assert_eq!(main.value.closure(), Some(Closure::Exact));
    assert_eq!(
        lw.value.pack(),
        main.value.pack(),
        "the tag shares trunk's pack"
    );
    assert_eq!(lw.value.closure(), Some(Closure::Wider));
    assert_eq!(s.stored_pack_files(), 1);
}

/// Invariant: a first push of trunk and many annotated tags stores every
/// object at most twice in total (one pack per write would store each
/// tag's whole history again), every ref stays connected, and trunk stays
/// `Exact`. The measured version with 200 tags is in `tests/measure.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_push_of_many_tags_stores_each_object_at_most_twice() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    for i in 0..20 {
        w.commit(&format!("f{i}.txt"), &format!("{i}\n"), &format!("c{i}"));
        for j in 0..2 {
            w.git(&[
                "tag",
                "-a",
                &format!("v{i}.{j}"),
                "-m",
                &format!("release {i}.{j}"),
            ]);
        }
    }
    let (ok, out) = push(&w, &s.url(), &["main", "--tags"]).await;
    assert!(ok, "{out}");
    let sent = w.objects("--all").len() as u64;
    let mut packs = Vec::new();
    for i in 0..20 {
        for j in 0..2 {
            let v = s.live(&format!("refs/tags/v{i}.{j}")).await;
            packs.extend(v.value.pack());
        }
    }
    let main = s.check_ref("refs/heads/main", &w).await;
    assert_eq!(main.value.closure(), Some(Closure::Exact));
    packs.extend(main.value.pack());
    packs.sort_unstable();
    packs.dedup();
    let stored: u64 = s
        .pack_rows(&packs)
        .await
        .iter()
        .map(|r| u64::from(r.pack.object_count))
        .sum();
    assert!(
        stored <= 2 * sent,
        "stored {stored} objects for {sent} sent, in {} packs",
        packs.len()
    );
    for t in ["refs/tags/v0.0", "refs/tags/v10.1", "refs/tags/v19.1"] {
        s.check_ref(t, &w).await;
    }
}

/// Invariant: after a forced trunk update, the push advertisement shows no
/// `.have` tip outside the closure a push searches, so a client that still
/// holds the skipped tip sends what only the skipped history had (here a
/// blob), instead of omitting it and failing for good with a missing
/// object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_forced_trunk_update_skipped_tips_are_not_advertised() {
    let s = Server::start_with(|_| {}, ForcePolicy::All).await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "t1");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let t2 = w.commit("b.txt", "only in t2\n", "t2");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.git(&["branch", "old"]);
    w.git(&["reset", "-q", "--hard", "HEAD~1"]);
    w.commit("c.txt", "c\n", "t1'");
    let (ok, out) = push(&w, &s.url(), &["--force", "main"]).await;
    assert!(ok, "{out}");
    let body = s
        .svc
        .advertise(&s.repo, None, aspen_proto::service::ProtocolVersion::V0)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(!text.contains(&t2), "skipped tip advertised: {text}");
    // Reuse the blob only the skipped history holds.
    w.commit("b2.txt", "only in t2\n", "t2'");
    let (ok, out) = push(&w, &s.url(), &["main"]).await;
    assert!(ok, "{out}");
    s.check_ref("refs/heads/main", &w).await;
}

/// Sums the objects of the distinct packs the live versions of `refs`
/// name.
async fn stored_objects(s: &Server, refs: &[String]) -> (u64, usize) {
    let mut packs = Vec::new();
    for r in refs {
        packs.extend(s.live(r).await.value.pack());
    }
    packs.sort_unstable();
    packs.dedup();
    let stored = s
        .pack_rows(&packs)
        .await
        .iter()
        .map(|r| u64::from(r.pack.object_count))
        .sum();
    (stored, packs.len())
}

/// Every object a push of `revs` sends from `w` when the client holds
/// nothing the server advertised (no thin bases are possible).
fn sent_objects(w: &Work, revs: &[String]) -> u64 {
    let mut args = vec!["rev-list", "--objects"];
    args.extend(revs.iter().map(String::as_str));
    w.git(&args).lines().count() as u64
}

/// A server that advertises no `.have` tips, with trunk at `t0..t39`
/// (each revision of `f.txt` shortening one more line, so git deltifies
/// each against the one before: a chain as deep as the history) and then
/// one more commit from another clone. The returned clone `w` stays at
/// `t39`, so it holds nothing the server advertises: a push from it
/// resends trunk's whole history, and its deltas end in that held chain.
async fn behind_trunk() -> (Server, Work, Vec<String>) {
    let s = Server::start_repo(
        |_| {},
        ForcePolicy::Branches,
        serde_json::json!({ "anonymous": ["write:demo"] }),
        |b| b.have_tips(0),
    )
    .await;
    let w = s.clone_dir("w");
    let mut lines: Vec<String> = (0..400)
        .map(|i| format!("line {i} {}\n", "x".repeat(40)))
        .collect();
    for r in 0..40 {
        lines[r] = format!("line {r}\n");
        w.commit("f.txt", &lines.concat(), &format!("t{r}"));
    }
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let o = s.clone_dir("o");
    o.git(&["pull", "-q", w.path.to_str().unwrap(), "main"]);
    o.commit("g.txt", "g\n", "t40");
    assert!(push(&o, &s.url(), &["main"]).await.0);
    // One packing thread, so git's delta choices do not vary between runs.
    w.git(&["config", "pack.threads", "1"]);
    (s, w, lines)
}

/// Commits `count` branches `b<i>` on `base`, each shortening its own line
/// of `lines`, and pushes them all. Returns the branch names.
async fn push_branches(
    s: &Server,
    w: &Work,
    lines: &[String],
    base: &str,
    count: usize,
) -> Vec<String> {
    let mut branches = Vec::new();
    for b in 0..count {
        let name = format!("b{b}");
        w.git(&["checkout", "-q", "-b", &name, base]);
        let mut mine = lines.to_vec();
        mine[300 + b] = format!("line {} in {name}\n", 300 + b);
        w.commit("f.txt", &mine.concat(), &name);
        branches.push(name);
    }
    let refspecs: Vec<&str> = branches.iter().map(String::as_str).collect();
    let (ok, out) = push(w, &s.url(), &refspecs).await;
    assert!(ok, "{out}");
    branches
}

/// Checks that each branch is connected and depends on trunk, and that no
/// pack a branch names stores an object trunk's closure holds: own and
/// shared packs never copy a held delta base. Returns the objects stored
/// and the number of packs.
async fn check_branches(s: &Server, w: &Work, branches: &[String]) -> (u64, usize) {
    let trunk = s.live("refs/heads/main").await;
    let held = s.closure_objects(&trunk).await;
    let refs: Vec<String> = branches.iter().map(|b| format!("refs/heads/{b}")).collect();
    for r in &refs {
        let v = s.check_ref(r, w).await;
        assert_eq!(v.deps.trunk, Some(trunk.id), "{r}");
        let row = &s.pack_rows(&[v.value.pack().unwrap()]).await[0];
        let copied = s.pack_objects(row).intersection(&held).count();
        assert_eq!(copied, 0, "{r}'s pack stores {copied} objects trunk holds");
    }
    stored_objects(s, &refs).await
}

/// Invariant: a multi-ref push that resends history the dependencies
/// already hold, deltified in deep chains, still stores at most twice the
/// completed pack's objects, and no branch's pack holds a held object.
/// Before own packs stopped copying held delta bases, each of the 24 own
/// packs carried the held chain under its blob (about 600 objects stored
/// for 192).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branches_over_resent_history_store_at_most_twice_the_pack() {
    let (s, w, lines) = behind_trunk().await;
    let branches = push_branches(&s, &w, &lines, "main", 24).await;
    let sent = sent_objects(&w, &branches);
    let (stored, packs) = check_branches(&s, &w, &branches).await;
    assert!(
        stored <= 2 * sent,
        "stored {stored} objects for {sent} in the completed pack, in {packs} packs"
    );
}

/// Invariant: the shared pack, too, holds no object the dependencies hold.
/// The branches share 40 commits on top of the resent history, so only
/// two own packs fit the budget and the other six branches share one pack,
/// whose first blob is a delta into the held chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_pack_over_resent_history_copies_no_held_base() {
    let (s, w, mut lines) = behind_trunk().await;
    for r in 40..80 {
        lines[r] = format!("line {r}\n");
        w.commit("f.txt", &lines.concat(), &format!("s{r}"));
    }
    let branches = push_branches(&s, &w, &lines, "main", 8).await;
    let sent = sent_objects(&w, &branches);
    let (stored, packs) = check_branches(&s, &w, &branches).await;
    assert!(packs < branches.len(), "some branches share a pack");
    assert!(
        stored <= 2 * sent,
        "stored {stored} objects for {sent} in the completed pack, in {packs} packs"
    );
}

/// Invariant: a forced trunk update's pack is charged against the own-pack
/// budget, so a forced trunk with a branch and an annotated tag on its new
/// history still stores at most twice the completed pack's objects
/// (without the charge: the forced pack, the branch's own pack and the
/// tag's shared pack would each hold about the whole history).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forced_trunk_with_refs_on_its_history_stores_at_most_twice_the_pack() {
    let s = Server::start_with(|_| {}, ForcePolicy::All).await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "t1");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.git(&["checkout", "-q", "--orphan", "new"]);
    for i in 0..20 {
        w.commit(&format!("n{i}.txt"), &format!("{i}\n"), &format!("n{i}"));
    }
    w.git(&["branch", "x", "HEAD~1"]);
    w.git(&["tag", "-a", "v", "-m", "release"]);
    let (ok, out) = push(&w, &s.url(), &["--force", "new:main", "x", "v"]).await;
    assert!(ok, "{out}");
    let revs = ["new", "x", "v"].map(String::from);
    let sent = sent_objects(&w, &revs);
    let refs = ["refs/heads/main", "refs/heads/x", "refs/tags/v"].map(String::from);
    let (stored, packs) = stored_objects(&s, &refs).await;
    assert!(
        stored <= 2 * sent,
        "stored {stored} objects for {sent} in the completed pack, in {packs} packs"
    );
    let main = s.check_ref("refs/heads/main", &w).await;
    assert_eq!(main.value.closure(), Some(Closure::Exact));
    s.check_ref("refs/heads/x", &w).await;
    s.check_ref("refs/tags/v", &w).await;
}

/// Invariant: a write in the shared pack is `Exact` when its tip reaches
/// every other tip there. `a-side` (c1, c2 and a side commit) takes most
/// of the own-pack budget, so neither `b-tip` (c4) nor `c-mid` (c3) fits
/// an own pack; both go to the shared pack, which is exactly what c4
/// reaches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_spanning_tip_of_the_shared_pack_is_exact() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    for i in 1..=2 {
        w.commit(&format!("c{i}.txt"), &format!("{i}\n"), &format!("c{i}"));
    }
    w.git(&["checkout", "-q", "-b", "a-side"]);
    w.commit("side.txt", "side\n", "side");
    w.git(&["checkout", "-q", "main"]);
    w.commit("c3.txt", "3\n", "c3");
    w.git(&["branch", "c-mid"]);
    w.commit("c4.txt", "4\n", "c4");
    w.git(&["branch", "b-tip"]);
    let (ok, out) = push(&w, &s.url(), &["a-side", "b-tip", "c-mid"]).await;
    assert!(ok, "{out}");
    let a = s.check_ref("refs/heads/a-side", &w).await;
    let b = s.check_ref("refs/heads/b-tip", &w).await;
    let c = s.check_ref("refs/heads/c-mid", &w).await;
    assert_eq!(a.value.closure(), Some(Closure::Exact));
    assert_eq!(b.value.pack(), c.value.pack(), "b-tip and c-mid share");
    assert_ne!(a.value.pack(), b.value.pack());
    assert_eq!(b.value.closure(), Some(Closure::Exact), "the spanning tip");
    assert_eq!(c.value.closure(), Some(Closure::Wider));
    let shared = s.pack_objects(&s.pack_rows(&[b.value.pack().unwrap()]).await[0]);
    assert_eq!(shared, w.objects("b-tip"), "exactly what c4 reaches");
}

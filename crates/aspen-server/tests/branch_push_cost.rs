//! What one push to a long-lived branch costs must not grow with the
//! branch's history (bench/throughput: 128 writers each pushing to their own
//! branch slowed from about 400 to under 100 pushes/s in five minutes, as
//! every push walked its branch back to trunk, one commit lookup per
//! commit, to prove trunk's tip reaches the new tip).

mod receive_support;

use aspen_core::refs::Closure;
use receive_support::*;

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

/// Invariant: a fast-forward of an exact branch version that depends on the
/// current trunk version stays exact without walking the branch back to
/// trunk, through the plain URL and the branch-scoped URL alike; once
/// trunk moves, the walk runs again and the answer is still right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pushes_to_a_long_branch_do_not_walk_its_history() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("base.txt", "base\n", "base");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "agent"]);
    for (url, branch) in [
        (s.url(), "refs/heads/agent"),
        (s.scoped_url("agent2"), "refs/heads/agent2"),
    ] {
        w.git(&[
            "checkout",
            "-q",
            "-B",
            &branch["refs/heads/".len()..],
            "main",
        ]);
        let mut per_push = Vec::new();
        for i in 0..40 {
            w.commit("agent.txt", &format!("agent {i}\n"), &format!("agent {i}"));
            let before = s.svc.timings().ancestry_commits;
            let (ok, out) = push(&w, &url, &[&format!("HEAD:{branch}")]).await;
            assert!(ok, "{branch} push {i}: {out}");
            per_push.push(s.svc.timings().ancestry_commits - before);
            let v = s.live(branch).await;
            assert_eq!(v.value.closure(), Some(Closure::Exact), "{branch} push {i}");
        }
        // Walking back to trunk on every push costs about one commit per
        // commit of the branch (about 40 by now); without that, a push
        // walks a few commits (more when equal commit times make the
        // fast-forward check explore).
        let late: u64 = per_push[30..].iter().sum::<u64>() / 10;
        assert!(
            late <= 8,
            "{branch}: the 31st to 40th pushes walked {late} commits on average \
             (it grows with the branch when every push walks back to trunk): {per_push:?}"
        );
    }
    // Trunk moves: the branch's version depends on the old trunk version,
    // so the next push walks, and the branch is not based on the new trunk
    // tip, so its version is wider.
    w.git(&["checkout", "-q", "main"]);
    w.commit("trunk.txt", "trunk moved\n", "trunk");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.git(&["checkout", "-q", "agent"]);
    w.commit("agent.txt", "after trunk moved\n", "agent after");
    let (ok, out) = push(&w, &s.url(), &["HEAD:refs/heads/agent"]).await;
    assert!(ok, "{out}");
    let v = s.live("refs/heads/agent").await;
    assert_eq!(
        v.value.closure(),
        Some(Closure::Wider),
        "trunk's new tip is not an ancestor of the branch"
    );
}

/// The next push to a ref sends its trees as deltas against the trees the
/// previous push stored, and those are seeded into the base-object cache
/// when that push commits: the second push resolves its bases without
/// reading the blob store (bench/throughput: each trunk landing and each
/// branch push spent 40-80 ms reading them back).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_next_push_finds_the_trees_the_last_one_stored() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    for i in 0..300 {
        std::fs::write(w.path.join(format!("f{i:03}.txt")), format!("file {i}\n")).unwrap();
    }
    w.commit("seed.txt", "seed\n", "many files");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.commit("f007.txt", "changed\n", "one change");
    let (hits, misses) = s.svc.base_cache_counts();
    let (ok, out) = push(&w, &s.url(), &["main"]).await;
    assert!(ok, "{out}");
    let (hits2, misses2) = s.svc.base_cache_counts();
    assert!(
        hits2 > hits,
        "the root tree came from the cache: {hits}->{hits2}"
    );
    assert_eq!(misses2, misses, "nothing was read back from the blob store");
}

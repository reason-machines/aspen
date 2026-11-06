//! What one branch push costs the metadata database, in statements
//! (throughput item 6b). Behind three servers, branch pushes stopped at
//! about 445/s with PostgreSQL at 99.5% CPU and the servers at 25%: about
//! 30 statements per push, among them one read per ref version of the
//! closure the push searches (about 15), separate reads of each ref before
//! and after the search, and two transactions of four to five statements
//! each to register the pack and commit the version. Now a branch push to
//! PostgreSQL sends at most 5, counted by the store (`SqlStore::statements`)
//! and reported per push (`Timings::db_queries`). The access layer's
//! repository and token lookups, once per request before item 6a, are
//! cached for a few seconds (`ASPEN_REPO_CACHE_TTL_MS`), and are counted
//! here too.

mod receive_support;

use std::time::Duration;

use aspen_server::receive::{GroupCommit, Timings};
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

/// Everything one push sent, as receive-pack counts it: the advertisement's
/// queries, then the push's queries and its transaction control.
fn sent(before: &Timings, after: &Timings) -> (u64, u64, u64) {
    (
        after.advert_db_queries - before.advert_db_queries,
        after.db_queries - before.db_queries,
        after.db_transaction_control - before.db_transaction_control,
    )
}

/// Invariant: a push to an agent's own branch through the plain URL, once
/// the branch exists, sends PostgreSQL at most 5 statements, `BEGIN` and
/// `COMMIT` included: the advertisement reads trunk and its recent
/// versions in one, and the push reads trunk and the branch in one,
/// registers its pack in one and commits its version, commit row and jobs
/// in one. Nothing else the server sends to the store goes uncounted by
/// receive-pack but the access layer's repository lookup of each of the
/// push's two requests, which its cache (item 6a) answers in the steady
/// state: counting those lookups too, the steady-state pushes average at
/// most 5. (The access layer's repository rows expire after 5 s, so a push
/// that comes just after an expiry may read its repository again.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_push_sends_at_most_5_statements() {
    let Some(s) = Server::start_postgres(|_| {}).await else {
        return;
    };
    let w = s.clone_dir("w");
    w.commit("base.txt", "base\n", "base");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "agent"]);
    let mut per_push = Vec::new();
    let mut store_per_push = Vec::new();
    for i in 0..20 {
        w.commit("agent.txt", &format!("agent {i}\n"), &format!("agent {i}"));
        let before = s.svc.timings();
        let store_before = s.meta.statements();
        let (ok, out) = push(&w, &s.url(), &["HEAD:refs/heads/agent"]).await;
        assert!(ok, "push {i}: {out}");
        let after = s.svc.timings();
        let (advert, queries, control) = sent(&before, &after);
        let store = (s.meta.statements() - store_before).total();
        assert_eq!(after.pushes - before.pushes, 1);
        // Only the repository lookups are outside receive-pack's count: one
        // per request, or none once they are cached.
        let counted = advert + queries + control;
        assert!(
            (counted..=counted + 2).contains(&store),
            "push {i}: the store sent {store} statements, receive-pack counted {counted}"
        );
        per_push.push((advert, queries, control));
        store_per_push.push(store);
    }
    eprintln!("per push (advertisement, queries, transaction control): {per_push:?}");
    eprintln!("per push, everything the store sent: {store_per_push:?}");
    // The first push creates the branch and reads what this server has not
    // seen yet; every later one is the steady state.
    for (i, &(advert, queries, control)) in per_push.iter().enumerate().skip(1) {
        assert!(
            advert + queries + control <= 5,
            "push {i} sent {advert} + {queries} + {control} statements \
             (advertisement, push, transaction control): {per_push:?}"
        );
    }
    // Everything the store sent, repository lookups included.
    let steady = &store_per_push[1..];
    let total: u64 = steady.iter().sum();
    assert!(
        total <= 5 * steady.len() as u64,
        "{total} statements for {} pushes, repository lookups included: {store_per_push:?}",
        steady.len()
    );
    let v = s.live("refs/heads/agent").await;
    assert_eq!(v.seq.get(), 20);
}

/// With group commit on, concurrent pushes share one transaction for their
/// registrations and one for their commits, and inside it each push is one
/// statement of each kind, with no savepoint around it: 8 concurrent
/// branch pushes send at most 5 queries each on average (repository lookups
/// included; the cache answers them after each clone's first push), and `BEGIN` and
/// `COMMIT` once per batch (the batches run on a task of their own, so the
/// store's total is what is counted here).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_committed_branch_pushes_share_their_transactions() {
    const N: usize = 8;
    let Some(s) = Server::start_postgres(|c| {
        c.group_commit = GroupCommit {
            enabled: true,
            window: Duration::from_millis(50),
            ..GroupCommit::default()
        };
    })
    .await
    else {
        return;
    };
    let w = s.clone_dir("w");
    w.commit("base.txt", "base\n", "base");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let mut clones = Vec::new();
    for i in 0..N {
        let c = s.clone_dir(&format!("c{i}"));
        c.git(&["pull", "-q", &s.url(), "main"]);
        c.git(&["checkout", "-q", "-b", &format!("agent{i}")]);
        c.commit("a.txt", &format!("{i} first\n"), "first");
        let (ok, out) = push(&c, &s.url(), &[&format!("HEAD:refs/heads/agent{i}")]).await;
        assert!(ok, "{out}");
        c.commit("a.txt", &format!("{i} second\n"), "second");
        clones.push(std::sync::Arc::new(c));
    }
    let before = s.meta.statements();
    let stats = s.svc.group_commit_stats().unwrap();
    let mut tasks = Vec::new();
    for (i, c) in clones.iter().enumerate() {
        let (c, url) = (c.clone(), s.url());
        tasks.push(tokio::spawn(async move {
            push(&c, &url, &[&format!("HEAD:refs/heads/agent{i}")]).await
        }));
    }
    for t in tasks {
        let (ok, out) = t.await.unwrap();
        assert!(ok, "{out}");
    }
    let sent = s.meta.statements() - before;
    let after = s.svc.group_commit_stats().unwrap();
    let batches = (after.commit_batches - stats.commit_batches)
        + (after.register_batches - stats.register_batches);
    eprintln!(
        "{N} pushes: {} queries, {} transaction control, {batches} batches",
        sent.queries, sent.transaction_control
    );
    // `BEGIN` and `COMMIT` for each batch and nothing else: no savepoint.
    assert_eq!(sent.transaction_control, 2 * batches, "{sent:?}");
    assert!(batches < 2 * N as u64, "pushes shared batches: {batches}");
    let queries = sent.queries as f64 / N as f64;
    assert!(queries <= 5.0, "{queries} queries per push: {sent:?}");
}

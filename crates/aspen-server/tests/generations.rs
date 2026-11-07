//! Generation numbers in `commit_index` (decisions log, "12c: generation
//! numbers"), end to end through stock `git push`:
//!
//! - every row a push writes carries the commit's generation, equal to the
//!   one computed from Git's own history;
//! - the `index_commits` job's backfill gives rows that lost theirs (as
//!   rows written before migration 0011 have none) exactly the same values;
//! - a branch push one commit at a time writes its generation from the
//!   tips the push read, and enqueues no backfill; one onto a tip with no
//!   generation (as before migration 0011) enqueues one;
//! - a backfill that may hold only three rows at a time fills a long
//!   history whose parents mostly lie in other rounds;
//! - a row whose ancestor has no row waits on it: the backfill examines it
//!   once, later backfills do not read it again, and the job that gives the
//!   missing commit its row fills the rows above it;
//! - ancestry answers equal `git merge-base --is-ancestor` with every row,
//!   half the rows and no row carrying a generation.

mod receive_support;

use std::collections::HashMap;

use aspen_compact::{CompactConfig, Compactor};
use aspen_core::id::ObjectId;
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{CommitLookup, MetaStore};
use aspen_core::refs::Ancestry;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use receive_support::*;
use sqlx::AssertSqlSafe;

async fn push(w: &Work, url: &str, args: &[&str]) {
    let mut cmd = w.command();
    cmd.arg("push").arg("--porcelain").arg(url).args(args);
    let (ok, out) = blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await;
    assert!(ok, "push {args:?}: {out}");
}

/// A history with merges and two roots: `main` and `side` diverge and
/// merge back twice, and `orphan` starts from nothing and is merged in.
fn build_history(w: &Work) {
    w.commit("a.txt", "a0\n", "a0");
    for i in 1..6 {
        w.commit("a.txt", &format!("a{i}\n"), &format!("a{i}"));
    }
    w.git(&["checkout", "-q", "-b", "side", "HEAD~3"]);
    for i in 0..7 {
        w.commit("s.txt", &format!("s{i}\n"), &format!("s{i}"));
    }
    w.git(&["checkout", "-q", "main"]);
    w.git(&["merge", "-q", "--no-edit", "side"]);
    w.commit("a.txt", "a6\n", "a6");
    w.git(&["checkout", "-q", "side"]);
    w.commit("s.txt", "s7\n", "s7");
    w.git(&["checkout", "-q", "--orphan", "orphan"]);
    w.git(&["rm", "-rfq", "."]);
    for i in 0..4 {
        w.commit("o.txt", &format!("o{i}\n"), &format!("o{i}"));
    }
    w.git(&["checkout", "-q", "main"]);
    w.git(&[
        "merge",
        "-q",
        "--no-edit",
        "--allow-unrelated-histories",
        "orphan",
    ]);
    w.git(&["merge", "-q", "--no-edit", "side"]);
    w.commit("a.txt", "a7\n", "a7");
}

/// Every commit of the work tree's refs and its generation, computed from
/// `git rev-list --parents` (parents before children with `--reverse
/// --topo-order`).
fn git_generations(w: &Work) -> HashMap<ObjectId, u64> {
    let out = w.git(&[
        "rev-list",
        "--all",
        "--reverse",
        "--topo-order",
        "--parents",
    ]);
    let mut g: HashMap<ObjectId, u64> = HashMap::new();
    for line in out.lines() {
        let ids: Vec<ObjectId> = line.split_whitespace().map(oid).collect();
        let gen_ = 1 + ids[1..].iter().map(|p| g[p]).max().unwrap_or(0);
        g.insert(ids[0], gen_);
    }
    g
}

async fn stored_generations(s: &Server, ids: &[ObjectId]) -> HashMap<ObjectId, Option<u64>> {
    let sorted = SortedIds::from_unsorted(ids.to_vec());
    let got = s.meta.lookup_commits(s.repo.id, &sorted).await.unwrap();
    sorted
        .as_slice()
        .iter()
        .zip(got)
        .map(|(id, l)| match l {
            CommitLookup::Indexed(r) => (*id, r.generation),
            CommitLookup::NotIndexed => panic!("{id} has no commit_index row"),
        })
        .collect()
}

fn compactor(s: &Server) -> Compactor<aspen_meta::SqlStore, aspen_blob::LocalBlobStore> {
    compactor_holding(s, aspen_compact::DEFAULT_BACKFILL_HELD)
}

/// A compactor whose backfill holds at most `held` rows at a time.
fn compactor_holding(
    s: &Server,
    held: usize,
) -> Compactor<aspen_meta::SqlStore, aspen_blob::LocalBlobStore> {
    let spool = s.dir.path().join("compact");
    std::fs::create_dir_all(&spool).unwrap();
    let mut config = CompactConfig::new(
        spool,
        MaxObjectBytes::new(64 * MIB).unwrap(),
        MaxInflatedBytes::new(1024 * MIB).unwrap(),
    );
    config.backfill_held = held;
    Compactor::new(s.meta.clone(), s.blobs.clone(), config)
}

/// `n` commits on `main`, one after another, by `git fast-import`.
fn linear_history(w: &Work, n: usize) {
    use std::io::Write as _;
    let mut s = Vec::new();
    for i in 0..n {
        write!(
            s,
            "commit refs/heads/main\ncommitter A <a@a> {} +0000\ndata 2\nc\n",
            1_700_000_000 + i
        )
        .unwrap();
        let body = format!("{i}\n");
        write!(
            s,
            "M 100644 inline f{}.txt\ndata {}\n{body}\n",
            i % 20,
            body.len()
        )
        .unwrap();
    }
    let mut child = w
        .command()
        .args(["fast-import", "--quiet", "--done"])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(&s).unwrap();
    stdin.write_all(b"done\n").unwrap();
    drop(stdin);
    assert!(child.wait().unwrap().success(), "fast-import failed");
}

/// Indexes every ref's version, so every pushed commit has a row whatever
/// the push wrote synchronously.
async fn index_all(s: &Server, refs: &[&str]) {
    for r in refs {
        let v = s.live(r).await.id;
        compactor(s).index_version(v).await.unwrap();
    }
}

/// The number of `index_commits` jobs in the queue.
async fn index_commits_jobs(s: &Server) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE kind = 'index_commits'")
        .fetch_one(s.meta.pool())
        .await
        .unwrap()
}

async fn clear_generations(s: &Server, filter: &str) {
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE commit_index SET generation = NULL WHERE {filter}"
    )))
    .execute(s.meta.pool())
    .await
    .unwrap();
}

/// Invariant: a push writes every row's generation equal to Git's, and the
/// backfill restores exactly those values for rows without one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pushed_rows_carry_gits_generations_and_the_backfill_restores_them() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    build_history(&w);
    // Push in pieces, so later pushes take their parents' generations from
    // stored rows: an early main, then side, then everything.
    let early = w.rev("main~4");
    push(&w, &s.url(), &[&format!("{early}:refs/heads/main")]).await;
    push(&w, &s.url(), &["side"]).await;
    push(&w, &s.url(), &["main", "orphan"]).await;
    let want = git_generations(&w);
    let ids: Vec<ObjectId> = want.keys().copied().collect();
    let got = stored_generations(&s, &ids).await;
    for (id, g) in &want {
        assert_eq!(got[id], Some(*g), "{id}: the push wrote Git's generation");
    }

    // As before migration 0011: no generations at all. The backfill fills
    // every row, from the rows alone.
    clear_generations(&s, "1 = 1").await;
    let filled = compactor(&s).backfill_generations(s.repo.id).await.unwrap();
    assert_eq!(filled, want.len());
    let got = stored_generations(&s, &ids).await;
    for (id, g) in &want {
        assert_eq!(got[id], Some(*g), "{id}: backfilled");
    }
    // Nothing left; a second run changes nothing.
    assert_eq!(
        compactor(&s).backfill_generations(s.repo.id).await.unwrap(),
        0
    );
}

/// Invariant: a branch push one commit at a time (a fast-forward of the
/// branch, a new branch off trunk, a forced update back onto trunk) writes
/// its commit's generation itself and enqueues no `index_commits` job: the
/// parent is a tip the push read, and that read returns its generation.
/// The push before this computed generations only from rows its ancestry
/// questions read, which a fast-forward does not, and enqueued a backfill
/// job on nearly every push. A last fast-forward onto a tip whose row has
/// no generation (as rows written before migration 0011) must enqueue one,
/// so the count of zero is not an artifact of how jobs are counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fast_forward_writes_its_generation_without_a_backfill_job() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "base\n", "base");
    push(&w, &s.url(), &["main"]).await;
    w.git(&["checkout", "-q", "-b", "topic"]);
    for i in 1..=6u32 {
        if i == 4 {
            w.git(&["reset", "-q", "--hard", "main"]);
        }
        w.commit("t.txt", &format!("topic {i}\n"), &format!("topic {i}"));
        if i == 4 {
            push(&w, &s.url(), &["-o", "force", "--force", "topic"]).await;
        } else {
            push(&w, &s.url(), &["topic"]).await;
        }
        let tip = oid(&w.rev("topic"));
        let want = git_generations(&w)[&tip];
        assert_eq!(
            stored_generations(&s, &[tip]).await[&tip],
            Some(want),
            "push {i} wrote its tip's generation"
        );
    }
    assert_eq!(
        index_commits_jobs(&s).await,
        0,
        "no push left a row for the backfill"
    );

    // As before migration 0011: no row has a generation, so neither the
    // tip the push reads nor any row its ancestry questions read gives the
    // new commit one, and the push leaves it to the backfill.
    let old_tip = oid(&w.rev("topic"));
    clear_generations(&s, "1 = 1").await;
    assert_eq!(stored_generations(&s, &[old_tip]).await[&old_tip], None);
    w.commit("t.txt", "topic 7\n", "topic 7");
    push(&w, &s.url(), &["topic"]).await;
    let tip = oid(&w.rev("topic"));
    assert_eq!(stored_generations(&s, &[tip]).await[&tip], None);
    assert!(
        index_commits_jobs(&s).await > 0,
        "a push onto a tip without a generation enqueues a backfill"
    );
}

/// Invariant: the backfill fills every row whatever it may hold at once.
/// Holding three rows, a 300-commit history is examined in 100 rounds in
/// commit ID order (every row names the one version), so nearly every
/// parent lies in another round: the row waits on it and is filled when
/// that round fills it. The backfill before rows could wait repeated its
/// rounds at most 16 times and left most of these rows without one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backfill_holding_three_rows_at_a_time_fills_a_long_history() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    linear_history(&w, 300);
    push(&w, &s.url(), &["main"]).await;
    index_all(&s, &["refs/heads/main"]).await;
    let want = git_generations(&w);
    assert_eq!(want.len(), 300);
    let ids: Vec<ObjectId> = want.keys().copied().collect();

    clear_generations(&s, "1 = 1").await;
    let c = compactor_holding(&s, 3);
    assert_eq!(c.backfill_generations(s.repo.id).await.unwrap(), 300);
    let got = stored_generations(&s, &ids).await;
    for (id, g) in &want {
        assert_eq!(got[id], Some(*g), "{id}");
    }
    assert_eq!(c.backfill_generations(s.repo.id).await.unwrap(), 0);
}

/// Invariant: a row whose ancestor has no row is examined once. The
/// backfill records what it waits on; a later backfill reads none of those
/// rows (its one statement is the empty page of rows to examine), however
/// many there are; and the job that writes the missing row fills every row
/// above it, including those its own pass does not reach.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_above_a_commit_without_a_row_wait_once_and_fill_when_it_gets_one() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    build_history(&w);
    push(&w, &s.url(), &["main", "side", "orphan"]).await;
    let refs = ["refs/heads/main", "refs/heads/side", "refs/heads/orphan"];
    index_all(&s, &refs).await;
    let want = git_generations(&w);
    let ids: Vec<ObjectId> = want.keys().copied().collect();

    // a1's row is gone (as for a commit no job indexed), and no row has a
    // generation (as before migration 0011).
    let a1 = w
        .git(&["log", "--all", "--format=%H %s"])
        .lines()
        .find_map(|l| l.strip_suffix(" a1").map(oid))
        .expect("commit a1");
    sqlx::query("DELETE FROM commit_index WHERE commit_oid = $1")
        .bind(a1.as_bytes().to_vec())
        .execute(s.meta.pool())
        .await
        .unwrap();
    clear_generations(&s, "1 = 1").await;
    let above: Vec<ObjectId> = ids
        .iter()
        .copied()
        .filter(|c| {
            *c != a1
                && w.try_git(&[
                    "merge-base",
                    "--is-ancestor",
                    &a1.to_string(),
                    &c.to_string(),
                ])
                .0
        })
        .collect();
    assert!(above.len() > 10, "most of the history is above a1");

    let filled = compactor(&s).backfill_generations(s.repo.id).await.unwrap();
    assert_eq!(filled, want.len() - 1 - above.len());
    let rest: Vec<ObjectId> = ids.iter().copied().filter(|c| *c != a1).collect();
    let got = stored_generations(&s, &rest).await;
    for c in &rest {
        let expect = (!above.contains(c)).then(|| want[c]);
        assert_eq!(got[c], expect, "{c}");
    }

    // Again: nothing to examine, nothing read but the empty page.
    let before = s.meta.statements();
    assert_eq!(
        compactor(&s).backfill_generations(s.repo.id).await.unwrap(),
        0
    );
    let after = s.meta.statements();
    assert_eq!(after.queries - before.queries, 1, "one empty page");

    // The job for main's version writes a1's row with its generation;
    // every row above it gets Git's, side's tip too, which main does not
    // reach and that job's own pass does not cover.
    index_all(&s, &["refs/heads/main"]).await;
    let got = stored_generations(&s, &ids).await;
    for (id, g) in &want {
        assert_eq!(got[id], Some(*g), "{id}");
    }
}

/// Invariant: ancestry answers equal `git merge-base --is-ancestor` for
/// every pair of commits, with all, half or none of the rows carrying a
/// generation (a row without one sends the walk to the paint walk).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ancestry_matches_git_with_all_half_or_no_generations() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    build_history(&w);
    push(&w, &s.url(), &["main", "side", "orphan"]).await;
    let versions: Vec<_> = {
        let mut v = Vec::new();
        for r in ["refs/heads/main", "refs/heads/side", "refs/heads/orphan"] {
            v.push(s.live(r).await.id);
        }
        v
    };
    let commits: Vec<ObjectId> = git_generations(&w).into_keys().collect();
    let hex: HashMap<ObjectId, String> = commits.iter().map(|c| (*c, c.to_string())).collect();
    // Git's answers, once.
    let mut git: HashMap<(ObjectId, ObjectId), bool> = HashMap::new();
    for a in &commits {
        for b in &commits {
            let (ok, _) = w.try_git(&["merge-base", "--is-ancestor", &hex[a], &hex[b]]);
            git.insert((*a, *b), ok);
        }
    }
    for (what, filter) in [
        ("all", None),
        // About half the rows: those whose ID starts below 0x80.
        ("half", Some("substr(commit_oid, 1, 1) < x'80'")),
        ("none", Some("1 = 1")),
    ] {
        if let Some(f) = filter {
            clear_generations(&s, f).await;
        }
        for a in &commits {
            for b in &commits {
                let got = s
                    .svc
                    .is_ancestor(s.repo.id, *a, *b, &versions)
                    .await
                    .unwrap();
                let want = if git[&(*a, *b)] {
                    Ancestry::Descendant
                } else {
                    Ancestry::NotDescendant
                };
                assert_eq!(got, want, "{what}: is {a} an ancestor of {b}?");
            }
        }
    }
}

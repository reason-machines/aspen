//! Pushes through the plain repository URL, with and without
//! `git config push.negotiate true`, end to end with stock `git`.
//!
//! The plain URL advertises only trunk (S2), so a client pushing a branch
//! builds its pack against trunk and resends the branch's whole history
//! since trunk on every push: the bytes sent, and the bytes the server
//! receives, inflates and hashes, grow with the branch. With
//! `push.negotiate`, `git push` first runs a negotiation-only fetch
//! (`git fetch --negotiate-only`: protocol v2, `wait-for-done`, haves and no
//! wants) and leaves out every commit the server acknowledges. Aspen used to
//! fail that fetch (`server does not support wait-for-done`), and Git then
//! pushed the whole branch anyway.
//!
//! The tests check that:
//!
//! - with `push.negotiate`, repeated pushes to one long branch send, store
//!   and take the same whatever the branch's length, and the negotiation
//!   succeeds;
//! - without it, the resent history is received again but not stored again;
//! - every push shape whose acknowledged commits lie outside the pushed
//!   ref's dependencies (a branch created from another branch, an amended
//!   forced push, a merge into trunk, tags, a branch at an existing commit,
//!   two branches at once) still lands, connected, with trunk exact;
//! - the negotiation round itself acknowledges exactly the commits the
//!   server has indexed, and never says `ready`.

mod receive_support;

use std::time::{Duration, Instant};

use aspen_core::pack::PackOrigin;
use aspen_core::refs::{Closure, RefVersion};
use receive_support::*;

/// Pushes per run: enough for a resent history to dwarf one commit.
const PUSHES: usize = 40;

/// What one push cost the server, from the version it wrote.
#[derive(Debug, Clone, Copy)]
struct Cost {
    /// Bytes the client sent (`packs.received_bytes`).
    received: u64,
    /// Bytes stored for the version's pack.
    stored: u64,
    /// Wall time of the `git push`.
    took: Duration,
}

async fn push(w: &Work, url: &str, negotiate: bool, args: &[&str]) -> (bool, String) {
    let mut cmd = w.command();
    cmd.args(["-c", &format!("push.negotiate={negotiate}")])
        .arg("push")
        .arg("--porcelain")
        .arg(url)
        .args(args);
    blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await
}

/// Git's words when the negotiation fetch fails; the push then goes on
/// without it.
fn negotiation_failed(out: &str) -> bool {
    out.contains("push negotiation failed") || out.contains("wait-for-done")
}

async fn cost_of(s: &Server, v: &RefVersion, took: Duration) -> Cost {
    let id = v.value.pack().expect("every push here brings a new commit");
    let row = s.pack_rows(&[id]).await.remove(0);
    let PackOrigin::Push { received_bytes } = row.pack.origin else {
        panic!("a push's pack has a push origin")
    };
    Cost {
        received: received_bytes,
        stored: row.pack.size,
        took,
    }
}

/// One commit of the same shape on every push: a changed line in a shared
/// file and a new version of a small file, so a push's own objects (and
/// the trees completion stores whole as thin bases) cost the same whatever
/// the branch's length; only the history behind it grows.
fn commit(w: &Work, i: usize) {
    let shared: String = (0..100)
        .map(|l| {
            if l == i % 100 {
                format!("line {l}: push {i:04}\n")
            } else {
                format!("line {l}: the quick brown fox jumps over the lazy dog\n")
            }
        })
        .collect();
    std::fs::write(w.path.join("shared.txt"), shared).unwrap();
    w.commit(
        "own/latest.txt",
        &format!("push {i:04} of the agent's branch\n"),
        &format!("push {i:04}"),
    );
}

/// `PUSHES` pushes of one more commit each to `refs/heads/agent` through the
/// plain URL; returns each push's cost.
async fn long_branch(s: &Server, negotiate: bool) -> Vec<Cost> {
    let w = s.clone_dir(if negotiate { "negotiate" } else { "plain" });
    w.commit("base.txt", &"base line\n".repeat(200), "base");
    assert!(push(&w, &s.url(), negotiate, &["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "agent"]);
    let mut costs = Vec::new();
    for i in 0..PUSHES {
        commit(&w, i);
        let started = Instant::now();
        let (ok, out) = push(&w, &s.url(), negotiate, &["HEAD:refs/heads/agent"]).await;
        let took = started.elapsed();
        assert!(ok, "push {i}: {out}");
        if negotiate {
            assert!(!negotiation_failed(&out), "push {i}: {out}");
        }
        let v = s.live("refs/heads/agent").await;
        // Trunk never moves, so every version stays exact.
        assert_eq!(v.value.closure(), Some(Closure::Exact), "push {i}");
        costs.push(cost_of(s, &v, took).await);
    }
    s.check_ref("refs/heads/agent", &w).await;
    eprintln!(
        "push.negotiate={negotiate}: received bytes per push {:?}",
        costs.iter().map(|c| c.received).collect::<Vec<_>>()
    );
    eprintln!(
        "push.negotiate={negotiate}: stored bytes per push {:?}",
        costs.iter().map(|c| c.stored).collect::<Vec<_>>()
    );
    eprintln!(
        "push.negotiate={negotiate}: push ms {:?}",
        costs.iter().map(|c| c.took.as_millis()).collect::<Vec<_>>()
    );
    costs
}

fn median(mut d: Vec<Duration>) -> Duration {
    d.sort();
    d[d.len() / 2]
}

/// Invariant: with `push.negotiate`, the negotiation succeeds and the n-th
/// push to a long branch costs what the first did: the same bytes sent and
/// stored (within a few bytes of compression noise), and no slower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negotiated_pushes_to_a_long_branch_stay_flat() {
    let s = Server::start().await;
    let costs = long_branch(&s, true).await;
    let (early, late) = (&costs[..10], &costs[PUSHES - 10..]);
    let max = |c: &[Cost], f: fn(&Cost) -> u64| c.iter().map(f).max().unwrap();
    let min = |c: &[Cost], f: fn(&Cost) -> u64| c.iter().map(f).min().unwrap();
    // A push of one commit like the others: its own objects only. Without
    // negotiation the 40th push sends about 40 commits.
    assert!(
        max(late, |c| c.received) <= max(early, |c| c.received) + 64,
        "received bytes grew: {:?}",
        costs.iter().map(|c| c.received).collect::<Vec<_>>()
    );
    assert!(
        max(late, |c| c.stored) <= max(early, |c| c.stored) + 64,
        "stored bytes grew: {:?}",
        costs.iter().map(|c| c.stored).collect::<Vec<_>>()
    );
    assert!(
        min(late, |c| c.received) + 64 >= min(early, |c| c.received),
        "the pushes are not alike"
    );
    // Latency: loose, since CI machines are noisy; what grew before was the
    // pack, which the byte checks pin exactly.
    let (e, l) = (
        median(early.iter().map(|c| c.took).collect()),
        median(late.iter().map(|c| c.took).collect()),
    );
    assert!(
        l <= e * 3 + Duration::from_millis(250),
        "push latency grew from a median of {e:?} to {l:?}"
    );
    // The branch's own version holds what the negotiation acknowledged, so
    // no push had to look beyond its dependencies.
    assert_eq!(s.svc.metrics().widened_total(), 0);
}

/// Without `push.negotiate` the client still resends the branch's history
/// (Git's choice, given only trunk); the server receives it but stores only
/// the new commit's objects, so stored bytes stay flat while received bytes
/// grow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resent_history_is_not_stored_again() {
    let s = Server::start().await;
    let costs = long_branch(&s, false).await;
    let first = costs[0];
    let last = costs[PUSHES - 1];
    assert!(
        last.received > 3 * first.received,
        "without push.negotiate the client resends the branch ({} then {} bytes); \
         if Git stopped doing that, this test no longer shows anything",
        first.received,
        last.received
    );
    let stored_early = costs[..10].iter().map(|c| c.stored).max().unwrap();
    let stored_late = costs[PUSHES - 10..].iter().map(|c| c.stored).max().unwrap();
    assert!(
        stored_late <= stored_early + 64,
        "stored bytes grew with the resent history: {:?}",
        costs.iter().map(|c| c.stored).collect::<Vec<_>>()
    );
    assert_eq!(s.svc.metrics().widened_total(), 0);
}

/// Invariant: with `push.negotiate`, a push whose acknowledged commits lie
/// outside the pushed ref's dependencies lands, and every ref stays
/// connected (its closure holds everything its tip reaches) and exact
/// where it was before; trunk stays exact after a merge of a branch; a
/// fresh clone of every ref passes `git fsck --strict`. Each push still
/// sends only its own commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negotiated_pushes_built_on_other_refs_land_connected() {
    let s = Server::start().await;
    let url = s.url();
    let w = s.clone_dir("w");
    w.git(&["config", "push.negotiate", "true"]);
    w.commit("base.txt", &"base line\n".repeat(200), "base");
    assert!(push(&w, &url, true, &["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "feature"]);
    for i in 0..5 {
        commit(&w, i);
    }
    let (ok, out) = push(&w, &url, true, &["feature"]).await;
    assert!(ok, "{out}");
    let one_commit = {
        commit(&w, 5);
        let (ok, out) = push(&w, &url, true, &["feature"]).await;
        assert!(ok, "{out}");
        let v = s.live("refs/heads/feature").await;
        cost_of(&s, &v, Duration::ZERO).await.received
    };
    let widened = || s.svc.metrics().widened_total();

    // Each shape's acknowledged commits are on `feature` or `stacked`,
    // which none of them depends on: tags and pushes of several refs depend
    // on trunk alone, a forced update does not depend on the branch's old
    // version, and a new branch has none. The merge comes last, since
    // afterwards trunk holds both branches.
    let shapes: Vec<Shape> = vec![
        (
            "a branch created from another branch",
            vec!["stacked"],
            Box::new(|w: &Work| {
                w.git(&["checkout", "-q", "-b", "stacked", "feature"]);
                commit(w, 100);
            }),
        ),
        (
            "an amended commit, forced",
            vec!["-o", "force", "feature"],
            Box::new(|w: &Work| {
                w.git(&["checkout", "-q", "feature"]);
                w.git(&["commit", "-q", "--amend", "-m", "push 0005, amended"]);
            }),
        ),
        (
            "an annotated tag of a branch commit",
            vec!["refs/tags/v1"],
            Box::new(|w: &Work| {
                w.git(&["tag", "-a", "v1", "-m", "v1", "feature~1"]);
            }),
        ),
        (
            "a lightweight tag of a branch commit",
            vec!["refs/tags/light"],
            Box::new(|w: &Work| {
                w.git(&["tag", "light", "feature~2"]);
            }),
        ),
        (
            "a new branch at another branch's commit",
            vec!["feature~1:refs/heads/copy"],
            Box::new(|_: &Work| {}),
        ),
        (
            "two branches at once",
            vec!["stacked", "feature"],
            Box::new(|w: &Work| {
                w.git(&["checkout", "-q", "stacked"]);
                commit(w, 200);
                w.git(&["checkout", "-q", "feature"]);
                commit(w, 300);
            }),
        ),
        (
            "a merge into trunk",
            vec!["main"],
            Box::new(|w: &Work| {
                w.git(&["checkout", "-q", "main"]);
                w.git(&["merge", "-q", "--no-ff", "-m", "merge stacked", "stacked"]);
            }),
        ),
    ];
    for (what, args, prepare) in shapes {
        prepare(&w);
        let before = widened();
        let (ok, out) = push(&w, &url, true, &args).await;
        assert!(ok, "{what}: {out}");
        assert!(!negotiation_failed(&out), "{what}: {out}");
        assert!(
            widened() > before,
            "{what}: the push did not build on an acknowledged commit outside its \
             dependencies, so it does not test that"
        );
    }

    for r in [
        "refs/heads/main",
        "refs/heads/feature",
        "refs/heads/stacked",
        "refs/heads/copy",
        "refs/tags/v1",
        "refs/tags/light",
    ] {
        s.check_ref(r, &w).await;
    }
    let main = s.live("refs/heads/main").await;
    assert_eq!(
        main.value.closure(),
        Some(Closure::Exact),
        "trunk must stay exact after a merge of a branch"
    );
    // The negotiation left the merged branch's history out: the merge push
    // sent about one commit's worth (the merge commit and its tree), not
    // the branch.
    let merge_cost = cost_of(&s, &main, Duration::ZERO).await.received;
    assert!(
        merge_cost <= 2 * one_commit,
        "the merge push sent {merge_cost} bytes, one commit is {one_commit}"
    );

    // Every ref fetches back whole.
    let c = s.clone_dir("check");
    c.git(&["fetch", "-q", &url, "+refs/*:refs/remotes/aspen/*"]);
    c.git(&["fsck", "--strict", "--no-dangling"]);
    for (r, local) in [
        ("refs/heads/main", "main"),
        ("refs/heads/feature", "feature"),
        ("refs/heads/stacked", "stacked"),
        ("refs/heads/copy", "feature~2"),
        ("refs/tags/v1", "v1"),
        ("refs/tags/light", "light"),
    ] {
        assert_eq!(
            c.rev(&format!("refs/remotes/aspen/{}", &r["refs/".len()..])),
            w.rev(local),
            "{r}"
        );
    }
}

/// Invariant: what a push's writes copy from storage because it built on
/// acknowledged commits of other refs counts against the push's own byte
/// limit, so a tiny push naming a large branch cannot make the server copy
/// more than the client could have sent; the refusal says how to push
/// instead, and that push lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copying_left_out_history_is_bounded_by_the_push_limit() {
    let s = Server::start_with(
        |c| c.push_limits.max_pack_bytes = 16 * 1024,
        aspen_core::repo::ForcePolicy::Branches,
    )
    .await;
    let url = s.url();
    let w = s.clone_dir("w");
    w.commit("base.txt", "base\n", "base");
    assert!(push(&w, &url, true, &["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "feature"]);
    // Each commit is a few hundred bytes compressed and about 5 KB whole.
    for i in 0..8 {
        commit(&w, i);
        let (ok, out) = push(&w, &url, true, &["feature"]).await;
        assert!(ok, "{out}");
    }
    w.git(&["checkout", "-q", "-b", "stacked"]);
    commit(&w, 100);
    let (ok, out) = push(&w, &url, true, &["stacked"]).await;
    assert!(!ok, "the copy is over the limit: {out}");
    assert!(
        out.contains("push.negotiate=false"),
        "the refusal says what to do: {out}"
    );
    assert!(s.current("refs/heads/stacked").await.is_none());
    let (ok, out) = push(&w, &url, false, &["stacked"]).await;
    assert!(ok, "without push.negotiate the client sends it: {out}");
    s.check_ref("refs/heads/stacked", &w).await;
}

/// A push shape: what it is, the `git push` arguments, and how to make it.
type Shape<'a> = (&'a str, Vec<&'a str>, Box<dyn Fn(&Work)>);

/// POSTs one protocol v2 `fetch` command to the plain URL; returns the
/// response's pkt-lines.
async fn upload_pack(s: &Server, lines: &[String]) -> Vec<String> {
    let mut body = Vec::new();
    for l in lines {
        if l == "0000" || l == "0001" {
            body.extend_from_slice(l.as_bytes());
        } else {
            body.extend_from_slice(format!("{:04x}{l}\n", l.len() + 5).as_bytes());
        }
    }
    let url = format!("{}/git-upload-pack", s.url());
    blocking(move || {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("body");
        std::fs::write(&f, body).unwrap();
        let out = std::process::Command::new("curl")
            .args(["-sS", "-f", "-X", "POST"])
            .args(["-H", "Content-Type: application/x-git-upload-pack-request"])
            .args(["-H", "Git-Protocol: version=2"])
            .arg("--data-binary")
            .arg(format!("@{}", f.display()))
            .arg(url)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "curl: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let mut lines = Vec::new();
        let mut b = &out.stdout[..];
        while b.len() >= 4 {
            let n = usize::from_str_radix(std::str::from_utf8(&b[..4]).unwrap(), 16).unwrap();
            if n < 4 {
                lines.push(format!("{n:04}"));
                b = &b[4..];
                continue;
            }
            lines.push(String::from_utf8_lossy(&b[4..n]).trim_end().to_owned());
            b = &b[n..];
        }
        lines
    })
    .await
}

/// Invariant: a negotiation-only round (what `git fetch --negotiate-only`
/// sends) is answered with an `ACK` for each have that has a
/// `commit_index` row, whichever ref it is on, `NAK` when none has, and
/// never `ready` or a pack.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_negotiation_round_acknowledges_exactly_the_indexed_commits() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    let base = w.commit("base.txt", "base\n", "base");
    assert!(push(&w, &s.url(), false, &["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "side"]);
    let side = w.commit("side.txt", "side\n", "side");
    assert!(push(&w, &s.url(), false, &["side"]).await.0);
    let unknown = "1234567890123456789012345678901234567890".to_owned();
    let round = |haves: &[&String]| {
        let mut l = vec![
            "command=fetch".to_owned(),
            "agent=git/2.39.5".to_owned(),
            "object-format=sha1".to_owned(),
            "0001".to_owned(),
            "wait-for-done".to_owned(),
        ];
        l.extend(haves.iter().map(|h| format!("have {h}")));
        l.push("0000".to_owned());
        l
    };
    let mut acked = vec![format!("ACK {base}"), format!("ACK {side}")];
    acked.sort();
    let got = upload_pack(&s, &round(&[&unknown, &side, &base])).await;
    assert_eq!(got[0], "acknowledgments");
    let mut acks = got[1..got.len() - 1].to_vec();
    acks.sort();
    assert_eq!(acks, acked);
    assert_eq!(got.last().unwrap(), "0000", "{got:?}");
    assert!(!got.iter().any(|l| l == "ready" || l == "packfile"));

    let got = upload_pack(&s, &round(&[&unknown])).await;
    assert_eq!(got, ["acknowledgments", "NAK", "0000"]);

    // The advertisement says it, or Git does not try.
    let out = w.git(&["ls-remote", &s.url(), "refs/heads/main"]);
    assert!(out.contains(&base), "{out}");
    let caps = blocking({
        let url = format!("{}/info/refs?service=git-upload-pack", s.url());
        move || {
            std::process::Command::new("curl")
                .args(["-sS", "-H", "Git-Protocol: version=2", &url])
                .output()
                .unwrap()
                .stdout
        }
    })
    .await;
    assert!(
        String::from_utf8_lossy(&caps).contains("fetch=shallow wait-for-done"),
        "{}",
        String::from_utf8_lossy(&caps)
    );
}

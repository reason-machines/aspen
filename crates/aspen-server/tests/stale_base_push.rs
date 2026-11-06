//! A client whose base is an older trunk version pushes only its own
//! objects, with stock `git` and without `push.negotiate` (item 10).
//!
//! Through the plain URL a push advertisement shows trunk and earlier trunk
//! tips as `.have` lines (S2). Stock `git push` leaves out everything the
//! newest advertised tip it holds reaches, and when it holds none of them it
//! sends its branch's whole history. With 8 log-spaced tips (1, 2, 4, ...,
//! 128 versions back) that happened to every client more than 128 landings
//! behind: about 13 s at the monorepo benchmark's 9.6 pushes/s, where a
//! one-commit push sent 10.9 million objects. Now every one of the newest
//! 64 versions is advertised, then tips about a quarter apart down to the
//! oldest version a push searches (`aspen_core::meta::have_tip_seqs`).
//!
//! The test builds a trunk of `landings` one-file pushes (each changing a
//! file 8 directories deep), then pushes one commit to a new branch from
//! clients `behind` landings back, and counts the objects each push sends
//! (git's own `Total` line). It asserts:
//!
//! - in the dense window (5 behind), exactly the commit's own objects;
//! - beyond it, at most a quarter of the landings behind, resent, never the
//!   repository (which the old advertisement made every client past 128
//!   behind send);
//! - every branch lands connected;
//! - the advertisement has at most 108 `.have` lines however long trunk is.
//!
//! Required CI runs a 4,608-file repository with 150 landings; the nightly
//! suite runs the ignored 301,806-file one (the monorepo benchmark's file
//! count and depth) with 400 landings and a client 300 behind.

mod receive_support;

use std::io::Write as _;
use std::process::Stdio;
use std::time::Instant;

use receive_support::*;

/// A synthetic repository: `fanout` subdirectories per directory for
/// `depth` levels, and `files` files in every directory at the bottom.
#[derive(Debug, Clone, Copy)]
struct Shape {
    fanout: usize,
    depth: u32,
    files: usize,
}

impl Shape {
    fn leaves(self) -> usize {
        self.fanout.pow(self.depth)
    }

    fn file_count(self) -> usize {
        self.leaves() * self.files
    }

    /// The directory path of leaf `n`.
    fn leaf_dir(self, mut n: usize) -> String {
        let mut parts = Vec::with_capacity(self.depth as usize);
        for level in 0..self.depth {
            parts.push(format!("d{level}x{}", n % self.fanout));
            n /= self.fanout;
        }
        parts.reverse();
        parts.join("/")
    }
}

/// Required CI's repository: 4,608 files, 8 levels deep.
const SMALL: Shape = Shape {
    fanout: 2,
    depth: 8,
    files: 18,
};

/// The nightly suite's: 301,806 files, 8 levels deep.
const HUGE: Shape = Shape {
    fanout: 3,
    depth: 8,
    files: 46,
};

/// Feeds `stream` to `git fast-import` in `w`.
fn fast_import(w: &Work, stream: Vec<u8>) {
    let mut child = w
        .command()
        .args(["fast-import", "--quiet", "--done"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(&stream).unwrap();
    stdin.write_all(b"done\n").unwrap();
    drop(stdin);
    assert!(child.wait().unwrap().success(), "fast-import failed");
}

/// The initial commit of `shape` on `main`, without a work tree.
fn import_initial(w: &Work, shape: Shape) {
    let mut s = Vec::with_capacity(shape.file_count() * 80);
    s.extend_from_slice(
        b"commit refs/heads/main\ncommitter A <a@a> 1700000000 +0000\ndata 4\ninit\n",
    );
    for leaf in 0..shape.leaves() {
        let dir = shape.leaf_dir(leaf);
        for f in 0..shape.files {
            let body = format!("{dir}/f{f}\n");
            write!(
                s,
                "M 100644 inline {dir}/f{f}.txt\ndata {}\n{body}",
                body.len()
            )
            .unwrap();
        }
    }
    s.push(b'\n');
    fast_import(w, s);
}

/// One commit on `branch` that changes one file in leaf directory `leaf`.
fn import_change(w: &Work, shape: Shape, branch: &str, leaf: usize, round: usize) {
    let dir = shape.leaf_dir(leaf);
    let body = format!(
        "{dir}/f0\n{}{branch} round {round}\n",
        "unchanged line\n".repeat(20)
    );
    let mut s = Vec::new();
    write!(
        s,
        "commit refs/heads/{branch}\ncommitter A <a@a> {} +0000\ndata 7\nchange\n\
         from refs/heads/{branch}^0\nM 100644 inline {dir}/f0.txt\ndata {}\n{body}\n",
        1_700_000_000 + round + 1,
        body.len()
    )
    .unwrap();
    fast_import(w, s);
}

/// Runs `git -c push.negotiate=false push --progress --porcelain <args>`;
/// returns success and the output.
async fn push(w: &Work, url: &str, args: &[&str]) -> (bool, String) {
    let mut cmd = w.command();
    cmd.args([
        "-c",
        "push.negotiate=false",
        "push",
        "--progress",
        "--porcelain",
        url,
    ])
    .args(args);
    blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await
}

/// The object count of git's `Total <n> (delta ...)` line: what the push
/// sent.
fn total_sent(out: &str) -> u64 {
    out.split(['\n', '\r'])
        .find_map(|l| {
            l.trim()
                .strip_prefix("Total ")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
        .unwrap_or_else(|| panic!("no `Total` line in: {out}"))
}

/// One stale client's push.
#[derive(Debug)]
struct Stale {
    behind: usize,
    /// Objects the push sent.
    sent: u64,
    /// Objects of the commit itself (`git rev-list --objects <branch> ^base`).
    own: u64,
}

async fn run(shape: Shape, landings: usize, behinds: &[usize]) -> Vec<Stale> {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    import_initial(&w, shape);
    let started = Instant::now();
    // No deltas: the initial push only has to be fast to make.
    let mut cmd = w.command();
    cmd.args([
        "-c",
        "pack.window=0",
        "push",
        "--porcelain",
        &s.url(),
        "main",
    ]);
    let o = blocking(move || cmd.output().unwrap()).await;
    assert!(o.status.success(), "initial push failed");
    eprintln!(
        "{} files: initial push in {:?}",
        shape.file_count(),
        started.elapsed()
    );

    // Each client fetches trunk when it is `behind` landings from the end.
    let clients: Vec<(usize, Work)> = behinds
        .iter()
        .map(|&b| (b, s.clone_dir(&format!("behind-{b}"))))
        .collect();
    let mut lcg: u64 = 0x2545_f491_4f6c_dd1d;
    let started = Instant::now();
    for round in 0..landings {
        for (b, c) in &clients {
            if landings - round == *b {
                c.git(&[
                    "fetch",
                    "-q",
                    w.path.to_str().unwrap(),
                    "main:refs/heads/base",
                ]);
            }
        }
        lcg = lcg
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let leaf = (lcg >> 33) as usize % shape.leaves();
        import_change(&w, shape, "main", leaf, round);
        let mut cmd = w.command();
        cmd.args(["push", "-q", "--porcelain", &s.url(), "main"]);
        let o = blocking(move || cmd.output().unwrap()).await;
        assert!(
            o.status.success(),
            "landing {round}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
    }
    eprintln!("{landings} landings in {:?}", started.elapsed());

    let body = s
        .svc
        .advertise(&s.repo, None, aspen_proto::service::ProtocolVersion::V0)
        .await
        .unwrap();
    let haves = String::from_utf8_lossy(&body).matches(" .have").count();
    eprintln!("advertisement: {haves} .have lines, {} bytes", body.len());
    assert!(haves <= 108, "{haves} .have lines for {landings} landings");

    let mut out = Vec::new();
    for (behind, c) in &clients {
        let branch = format!("agent-{behind}");
        c.git(&["branch", &branch, "base"]);
        import_change(c, shape, &branch, 0, landings + 1);
        let own = c
            .git(&["rev-list", "--objects", &branch, "^base"])
            .lines()
            .count() as u64;
        let (ok, text) = push(c, &s.url(), &[&format!("{branch}:refs/heads/{branch}")]).await;
        assert!(ok, "{behind} behind: {text}");
        let sent = total_sent(&text);
        eprintln!(
            "{} files, {behind} landings behind: sent {sent} objects, the commit has {own}",
            shape.file_count()
        );
        let v = s.live(&format!("refs/heads/{branch}")).await;
        assert!(v.value.tip().is_some());
        out.push(Stale {
            behind: *behind,
            sent,
            own,
        });
    }
    // Trunk and the last branch are stored connected.
    s.check_ref("refs/heads/main", &w).await;
    let (last, c) = clients.last().unwrap();
    s.check_ref(&format!("refs/heads/agent-{last}"), c).await;
    out
}

fn check(shape: Shape, results: &[Stale]) {
    // Commit, root tree, `depth` trees and the blob.
    let per_landing = u64::from(shape.depth) + 3;
    for r in results {
        if r.behind <= 64 {
            assert_eq!(r.sent, r.own, "{} behind: {r:?}", r.behind);
        } else {
            let resent = (r.behind as u64) / 4 + 1;
            assert!(
                r.sent <= r.own + resent * per_landing,
                "{} behind sent {} objects, more than its own {} plus {resent} landings: {r:?}",
                r.behind,
                r.sent,
                r.own
            );
        }
        assert!(
            r.sent < shape.file_count() as u64 / 4,
            "{} behind sent {} objects in a {}-file repository",
            r.behind,
            r.sent,
            shape.file_count()
        );
    }
}

/// Invariant: a one-commit push from a stale base sends only (or little
/// more than) its own objects, past the old advertisement's 128-version
/// reach, without `push.negotiate`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_from_a_stale_base_sends_only_its_own_objects() {
    let results = run(SMALL, 150, &[140, 110, 5]).await;
    check(SMALL, &results);
}

/// The same in a 301,806-file repository (nightly), 300 landings behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow: a few minutes; run by the nightly suite"]
async fn a_push_from_a_stale_base_sends_only_its_own_objects_at_monorepo_scale() {
    let results = run(HUGE, 400, &[300, 5]).await;
    check(HUGE, &results);
}

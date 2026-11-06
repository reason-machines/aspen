//! Merge-queue landings: N clients each enqueue changes with Aspen's merge
//! queue (`ASPEN_MERGE_QUEUE=on`) and wait for each to land on `main`.
//!
//! Each client loops: fetch `main`, commit a change on top of it, push the
//! commit to `refs/queue/main/<run>/c<client>/<n>` through the plain URL,
//! and wait for its outcome ref, `refs/queue-done/main/<run>/c<client>/<n>`
//! or `refs/queue-rejected/...`.
//!
//! **Overlap.** `--overlap DISJOINT,DIR,FILE` (percentages, default
//! `70,25,5`) picks what each change touches, from a generator seeded by
//! the run, the client and the change number:
//! - *disjoint*: a new file of the client's own,
//!   `bench/<run>/c<client>/<n>.txt`;
//! - *dir*: a new file in one of 8 directories every client shares,
//!   `bench/<run>/shared/d<k>/c<client>-<n>.txt`;
//! - *file*: a rewrite of one of 4 files every client edits,
//!   `bench/<run>/hot/h<k>.txt`.
//!
//! The queue judges conflicts per file, so only *file* changes can be
//! rejected (another change rewrote the same file since the client's
//! base). A rejected *disjoint* or *dir* change is an error. The result
//! reports the rejection rate.
//!
//! **Outcomes** are read by one poller for the whole run: every `--poll-ms`
//! it sends one protocol v2 `ls-refs` request with `ref-prefix` for the
//! run's outcome refs (with `curl`; stock `git ls-remote <url> <ref>`
//! sends no prefix, so the server would list every ref), and hands each
//! outcome to the client waiting for it.
//!
//! **Verification** (`--verify-landed`): after the measured phase, every
//! landed commit L of a change C is checked with stock git:
//! - L has one parent P, and L is on `main`'s first-parent line;
//! - no two changes landed as the same commit (each counts once);
//! - `git merge-tree --write-tree P C` merges cleanly and its tree is L's
//!   tree (L is C's change merged onto the `main` it landed on);
//! - `git fsck --strict` passes on a clone of `main`.
//!
//! Any failure counts in `verify_failed` and fails the run (exit status 3).
//!
//! Measured:
//! - `enqueue`: the push;
//! - `visible`: from the push's acknowledgement until the outcome is
//!   advertised (land request to visible on `main`; outcome refs move in
//!   the same atomic update as `main`), polling included;
//! - `landed`: from the push's start until the outcome is advertised;
//! - `refetch`: each client's fetch of `main` before a change.
//!
//! Throughput: `merged_per_second` (changes landed on trunk, one trunk
//! commit each), `rejected_per_second`.
//!
//! Commits are dated at the current time, as real clients date theirs (a
//! commit dated before its parent makes the server's date-ordered ancestry
//! walks visit more history, issue #284).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Barrier, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::git;
use crate::results::{Errors, RunResult, SCHEMA, now_unix_ms};
use crate::stats::Summary;
use crate::synth::{self, Rng};
use crate::target::Setup;

/// Shared directories of *dir* changes, and files of *file* changes.
const SHARED_DIRS: u64 = 8;
const HOT_FILES: u64 = 4;

/// What each change touches, in percent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Overlap {
    pub disjoint: u8,
    pub dir: u8,
    pub file: u8,
}

impl Default for Overlap {
    fn default() -> Self {
        Self {
            disjoint: 70,
            dir: 25,
            file: 5,
        }
    }
}

impl std::str::FromStr for Overlap {
    type Err = String;

    /// `DISJOINT,DIR,FILE` percentages summing to 100.
    fn from_str(s: &str) -> Result<Self, String> {
        let parts: Vec<u8> = s
            .split(',')
            .map(|p| p.trim().parse::<u8>())
            .collect::<Result<_, _>>()
            .map_err(|e| format!("--overlap {s:?}: {e}"))?;
        let [disjoint, dir, file] = parts[..] else {
            return Err(format!(
                "--overlap {s:?}: expected DISJOINT,DIR,FILE percentages"
            ));
        };
        if u32::from(disjoint) + u32::from(dir) + u32::from(file) != 100 {
            return Err(format!("--overlap {s:?}: the percentages must sum to 100"));
        }
        Ok(Self {
            disjoint,
            dir,
            file,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Disjoint,
    Dir,
    File,
}

impl Overlap {
    fn pick(self, rng: &mut Rng) -> Kind {
        let r = rng.below(100) as u8;
        if r < self.disjoint {
            Kind::Disjoint
        } else if r < self.disjoint + self.dir {
            Kind::Dir
        } else {
            Kind::File
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueueOpts {
    pub clients: u32,
    pub duration: Duration,
    pub commit_bytes: usize,
    /// Give up waiting for an outcome after this long.
    pub visibility_timeout: Duration,
    /// Pause between two polls of the outcomes.
    pub poll: Duration,
    pub overlap: Overlap,
    /// Check every landed commit with stock git after the run.
    pub verify_landed: bool,
    /// Enqueue with `git push -o land` of a branch instead of a push to
    /// `refs/queue/...` (the server writes the same queue ref).
    pub land_option: bool,
}

impl QueueOpts {
    pub fn params(&self) -> BTreeMap<String, Value> {
        let mut m = BTreeMap::new();
        m.insert("clients".to_owned(), self.clients.into());
        m.insert(
            "duration_secs".to_owned(),
            self.duration.as_secs_f64().into(),
        );
        m.insert("commit_bytes".to_owned(), self.commit_bytes.into());
        m.insert(
            "visibility_timeout_secs".to_owned(),
            self.visibility_timeout.as_secs_f64().into(),
        );
        m.insert("poll_ms".to_owned(), (self.poll.as_millis() as u64).into());
        m.insert(
            "overlap".to_owned(),
            format!(
                "{},{},{}",
                self.overlap.disjoint, self.overlap.dir, self.overlap.file
            )
            .into(),
        );
        m.insert("verify_landed".to_owned(), self.verify_landed.into());
        m.insert("land_option".to_owned(), self.land_option.into());
        m
    }
}

/// What became of one change.
#[derive(Debug, Clone)]
enum Outcome {
    /// Landed as this trunk commit.
    Done(String),
    Rejected,
}

/// Outcomes not yet handed to their client, by outcome suffix
/// (`main/<run>/c<client>/<n>`).
type Waiting = Mutex<HashMap<String, Sender<Outcome>>>;

#[derive(Debug, Default)]
struct ClientOut {
    enqueue: Vec<Duration>,
    visible: Vec<Duration>,
    landed: Vec<Duration>,
    refetch: Vec<Duration>,
    merged: u64,
    rejected: u64,
    rejected_file: u64,
    unseen: u64,
    failed: u64,
    by_kind: [u64; 3],
    /// (the change's commit, the trunk commit it landed as).
    landed_commits: Vec<(String, String)>,
    errors: Errors,
}

pub fn run(
    setup: &Setup,
    opts: &QueueOpts,
    mut params: BTreeMap<String, Value>,
) -> Result<RunResult, String> {
    if opts.clients == 0 {
        return Err("--clients must be at least 1".to_owned());
    }
    params.extend(opts.params());
    eprintln!("aspen-bench: preparing {} queue clients", opts.clients);
    let repos: Vec<PathBuf> = (0..opts.clients)
        .map(|i| setup.path(&format!("q{i}.git")))
        .collect();
    for r in &repos {
        git::shared_clone(&setup.git, &setup.seed_repo, r)?;
    }
    let poll_body = setup.path("ls-refs.body");
    std::fs::write(
        &poll_body,
        git::ls_refs_request(&[
            &format!("refs/queue-done/main/{}/", setup.run_id),
            &format!("refs/queue-rejected/main/{}/", setup.run_id),
        ]),
    )
    .map_err(|e| format!("writing {}: {e}", poll_body.display()))?;
    let waiting = Waiting::default();
    let stop = AtomicBool::new(false);
    let barrier = Arc::new(Barrier::new(opts.clients as usize + 1));
    let started_at_unix_ms = now_unix_ms();
    let (outs, wall, poll_errors) = thread::scope(|s| {
        let (waiting, stop) = (&waiting, &stop);
        let poller = s.spawn(move || poll(setup, opts, &poll_body, waiting, stop));
        let handles: Vec<_> = repos
            .iter()
            .enumerate()
            .map(|(i, repo)| {
                let barrier = Arc::clone(&barrier);
                s.spawn(move || client(setup, opts, i as u64, repo, &barrier, waiting))
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        let outs: Vec<ClientOut> = handles
            .into_iter()
            .map(|h| h.join().unwrap_or_default())
            .collect();
        let wall = start.elapsed();
        stop.store(true, Ordering::Relaxed);
        let poll_errors = poller.join().unwrap_or_default();
        (outs, wall, poll_errors)
    });
    let mut all = ClientOut::default();
    let mut per_client = Vec::new();
    for (i, o) in outs.into_iter().enumerate() {
        all.enqueue.extend(o.enqueue);
        all.visible.extend(o.visible);
        all.landed.extend(o.landed);
        all.refetch.extend(o.refetch);
        all.merged += o.merged;
        all.rejected += o.rejected;
        all.rejected_file += o.rejected_file;
        all.unseen += o.unseen;
        all.failed += o.failed;
        for k in 0..3 {
            all.by_kind[k] += o.by_kind[k];
        }
        all.errors.merge(o.errors);
        per_client.push((repos[i].clone(), o.landed_commits));
    }
    all.errors.merge(poll_errors);
    let wall_seconds = wall.as_secs_f64().max(f64::EPSILON);
    let mut counts = BTreeMap::new();
    if opts.verify_landed {
        eprintln!(
            "aspen-bench: verifying {} landed commits with stock git",
            all.merged
        );
        let v = verify(setup, &per_client);
        counts.insert("verified".to_owned(), v.ok);
        counts.insert("verify_failed".to_owned(), v.failed);
        all.errors.merge(v.errors);
    }
    let mut latencies_ms = BTreeMap::new();
    latencies_ms.insert("enqueue".to_owned(), Summary::from_durations(&all.enqueue));
    latencies_ms.insert("visible".to_owned(), Summary::from_durations(&all.visible));
    latencies_ms.insert("landed".to_owned(), Summary::from_durations(&all.landed));
    latencies_ms.insert("refetch".to_owned(), Summary::from_durations(&all.refetch));
    let mut throughput = BTreeMap::new();
    throughput.insert(
        "merged_per_second".to_owned(),
        all.merged as f64 / wall_seconds,
    );
    throughput.insert(
        "rejected_per_second".to_owned(),
        all.rejected as f64 / wall_seconds,
    );
    let decided = all.merged + all.rejected;
    throughput.insert(
        "reject_rate".to_owned(),
        all.rejected as f64 / decided.max(1) as f64,
    );
    counts.insert("merged".to_owned(), all.merged);
    counts.insert("rejected".to_owned(), all.rejected);
    counts.insert("rejected_file".to_owned(), all.rejected_file);
    counts.insert("unseen".to_owned(), all.unseen);
    counts.insert("enqueue_failed".to_owned(), all.failed);
    counts.insert("changes_disjoint".to_owned(), all.by_kind[0]);
    counts.insert("changes_dir".to_owned(), all.by_kind[1]);
    counts.insert("changes_file".to_owned(), all.by_kind[2]);
    Ok(RunResult {
        schema: SCHEMA.to_owned(),
        workload: "queue".to_owned(),
        target: setup.info.clone(),
        params,
        environment: setup.environment.clone(),
        started_at_unix_ms,
        wall_seconds,
        latencies_ms,
        throughput,
        counts,
        errors: all.errors,
    })
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Lists the run's outcome refs every `opts.poll` and hands each to the
/// client waiting for it, until `stop`.
fn poll(
    setup: &Setup,
    opts: &QueueOpts,
    body: &Path,
    waiting: &Waiting,
    stop: &AtomicBool,
) -> Errors {
    let mut errors = Errors::default();
    let done = format!("refs/queue-done/main/{}/", setup.run_id);
    let rejected = format!("refs/queue-rejected/main/{}/", setup.run_id);
    while !stop.load(Ordering::Relaxed) {
        if !lock(waiting).is_empty() {
            match setup.remote.ls_refs(body) {
                Ok(refs) => {
                    let mut w = lock(waiting);
                    for (oid, name) in refs {
                        let (suffix, outcome) = if let Some(s) = name.strip_prefix(&done) {
                            (s, Outcome::Done(oid))
                        } else if let Some(s) = name.strip_prefix(&rejected) {
                            (s, Outcome::Rejected)
                        } else {
                            continue;
                        };
                        if let Some(tx) = w.remove(&format!("main/{}/{suffix}", setup.run_id)) {
                            let _ = tx.send(outcome);
                        }
                    }
                }
                Err(e) => errors.push(format!("poller: {e}")),
            }
        }
        thread::sleep(opts.poll);
    }
    errors
}

fn client(
    setup: &Setup,
    opts: &QueueOpts,
    index: u64,
    repo: &Path,
    barrier: &Barrier,
    waiting: &Waiting,
) -> ClientOut {
    let mut out = ClientOut::default();
    let run = &setup.run_id;
    barrier.wait();
    let start = Instant::now();
    let mut n = 0u64;
    while start.elapsed() < opts.duration {
        let fetched = Instant::now();
        let base = match fetch_main(setup, repo) {
            Ok(b) => b,
            Err(e) => {
                out.errors.push(format!("client {index}: fetch: {e}"));
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        out.refetch.push(fetched.elapsed());
        let mut rng = Rng::derive(setup.run_id_seed(), &[index, n]);
        let kind = opts.overlap.pick(&mut rng);
        let path = match kind {
            Kind::Disjoint => format!("bench/{run}/c{index}/{n}.txt"),
            Kind::Dir => format!(
                "bench/{run}/shared/d{}/c{index}-{n}.txt",
                rng.below(SHARED_DIRS)
            ),
            Kind::File => format!("bench/{run}/hot/h{}.txt", rng.below(HOT_FILES)),
        };
        out.by_kind[kind as usize] += 1;
        let content = synth::text(setup.run_id_seed(), &[index, n], opts.commit_bytes);
        let commit = synth::NewCommit {
            branch: "queue",
            parent: &base,
            path: &path,
            content: content.as_bytes(),
            message: &format!("bench queue client {index} change {n}"),
            timestamp: now_unix_ms() / 1000,
        };
        let tip = match synth::commit_on(&setup.git, repo, &commit) {
            Ok(t) => t,
            Err(e) => {
                out.errors.push(format!("client {index}: commit: {e}"));
                break;
            }
        };
        let suffix = format!("main/{run}/c{index}/{n}");
        n += 1;
        // Registered before the push, so an outcome listed the moment the
        // push is acknowledged is not missed.
        let (tx, rx): (Sender<Outcome>, Receiver<Outcome>) = mpsc::channel();
        lock(waiting).insert(suffix.clone(), tx);
        let pushed = Instant::now();
        let mut cmd = setup.git.cmd(repo);
        cmd.args(["push", "--quiet", "--no-verify"]);
        match suffix.split_once('/').filter(|_| opts.land_option) {
            // `-o land` of branch `<rest>` is queued as `refs/queue/main/<rest>`.
            Some((_, rest)) => cmd.args([
                "-o",
                "land",
                &setup.remote.url,
                &format!("{tip}:refs/heads/{rest}"),
            ]),
            None => cmd.args([&setup.remote.url, &format!("{tip}:refs/queue/{suffix}")]),
        };
        let (took, res) = git::run_timed(&mut cmd);
        if let Err(e) = res {
            lock(waiting).remove(&suffix);
            out.failed += 1;
            out.errors.push(format!("client {index}: enqueue: {e}"));
            continue;
        }
        let acked = Instant::now();
        out.enqueue.push(took);
        match rx.recv_timeout(opts.visibility_timeout.saturating_sub(took)) {
            Ok(Outcome::Done(landed)) => {
                out.visible.push(acked.elapsed());
                out.landed.push(pushed.elapsed());
                out.merged += 1;
                out.landed_commits.push((tip, landed));
            }
            Ok(Outcome::Rejected) => {
                out.visible.push(acked.elapsed());
                out.rejected += 1;
                if kind == Kind::File {
                    out.rejected_file += 1;
                } else {
                    out.errors.push(format!(
                        "client {index}: {suffix} ({kind:?}, {path}) was rejected, \
                         but no other change touches its file"
                    ));
                }
            }
            Err(_) => {
                lock(waiting).remove(&suffix);
                out.unseen += 1;
                out.errors.push(format!(
                    "client {index}: no outcome for {suffix} after {:?}",
                    opts.visibility_timeout
                ));
            }
        }
    }
    out
}

fn fetch_main(setup: &Setup, repo: &Path) -> Result<String, String> {
    git::run(setup.git.cmd(repo).args([
        "fetch",
        "--quiet",
        "--no-tags",
        "--no-write-fetch-head",
        &setup.remote.url,
        "+refs/heads/main:refs/remotes/origin/main",
    ]))?;
    git::run(
        setup
            .git
            .cmd(repo)
            .args(["rev-parse", "refs/remotes/origin/main"]),
    )
}

#[derive(Debug, Default)]
struct Verified {
    ok: u64,
    failed: u64,
    errors: Errors,
}

impl Verified {
    fn fail(&mut self, msg: String) {
        self.failed += 1;
        self.errors.push(format!("verify: {msg}"));
    }

    fn merge(&mut self, o: Verified) {
        self.ok += o.ok;
        self.failed += o.failed;
        self.errors.merge(o.errors);
    }
}

/// Checks every landed commit with stock git (module docs).
fn verify(setup: &Setup, per_client: &[(PathBuf, Vec<(String, String)>)]) -> Verified {
    let mut v = Verified::default();
    let Some((first, _)) = per_client.first() else {
        return v;
    };
    let main = match fetch_main(setup, first) {
        Ok(m) => m,
        Err(e) => {
            v.fail(format!("fetching main: {e}"));
            return v;
        }
    };
    // Each landed commit is on main's first-parent line, once.
    let line: HashSet<String> =
        match git::run(
            setup
                .git
                .cmd(first)
                .args(["rev-list", "--first-parent", &main]),
        ) {
            Ok(out) => out.lines().map(str::to_owned).collect(),
            Err(e) => {
                v.fail(format!("listing main: {e}"));
                return v;
            }
        };
    let mut seen = HashSet::new();
    for (_, landed) in per_client {
        for (c, l) in landed {
            if !line.contains(l) {
                v.fail(format!(
                    "{c} landed as {l}, which is not on main's first-parent line"
                ));
            }
            if !seen.insert(l.clone()) {
                v.fail(format!(
                    "{l} was reported as the landed commit of two changes"
                ));
            }
        }
    }
    // Re-merge each change onto the parent it landed on.
    let results = Mutex::new(Verified::default());
    let next = Mutex::new(per_client.iter());
    let workers = per_client.len().clamp(1, 16);
    thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| {
                loop {
                    let Some((repo, landed)) = lock(&next).next() else {
                        return;
                    };
                    let r = verify_client(setup, repo, landed);
                    lock(&results).merge(r);
                }
            });
        }
    });
    v.merge(results.into_inner().unwrap_or_else(PoisonError::into_inner));
    if let Err(e) = git::run(
        setup
            .git
            .cmd(first)
            .args(["fsck", "--strict", "--no-dangling"]),
    ) {
        v.fail(format!("git fsck --strict: {e}"));
    }
    v
}

fn verify_client(setup: &Setup, repo: &Path, landed: &[(String, String)]) -> Verified {
    let mut v = Verified::default();
    if landed.is_empty() {
        return v;
    }
    if let Err(e) = fetch_main(setup, repo) {
        v.fail(format!("fetching main into {}: {e}", repo.display()));
        return v;
    }
    for (c, l) in landed {
        let parents =
            match git::run(
                setup
                    .git
                    .cmd(repo)
                    .args(["rev-list", "--parents", "-n", "1", l]),
            ) {
                Ok(p) => p,
                Err(e) => {
                    v.fail(format!("{l}: {e}"));
                    continue;
                }
            };
        let parents: Vec<&str> = parents.split_whitespace().skip(1).collect();
        let [p] = parents[..] else {
            v.fail(format!(
                "{l} (change {c}) has {} parents, not one",
                parents.len()
            ));
            continue;
        };
        let merged = match git::run(
            setup
                .git
                .cmd(repo)
                .args(["merge-tree", "--write-tree", p, c]),
        ) {
            Ok(out) => out.lines().next().unwrap_or_default().to_owned(),
            Err(e) => {
                v.fail(format!(
                    "change {c} does not merge cleanly onto {p}, the parent it landed on as {l}: {e}"
                ));
                continue;
            }
        };
        match git::run(
            setup
                .git
                .cmd(repo)
                .args(["rev-parse", &format!("{l}^{{tree}}")]),
        ) {
            Ok(tree) if tree == merged => v.ok += 1,
            Ok(tree) => v.fail(format!(
                "{l} (change {c} onto {p}) has tree {tree}, but merging the change gives {merged}"
            )),
            Err(e) => v.fail(format!("{l}: {e}")),
        }
    }
    v
}

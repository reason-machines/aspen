//! OpenAI's simulated workload: N writers, each pushing new commits to its
//! own branch, while independent readers fetch every acknowledged tip.
//!
//! Measured:
//!
//! - `push`: duration of each `git push` (spawn to exit), as the writer sees
//!   it.
//! - `push_to_fetch`: time from a push's acknowledgement (the writer's `git
//!   push` exiting successfully) until a separate reader's `git fetch` of that
//!   branch has completed and returned the new tip or a descendant of it.
//!   Includes the reader's fetch time, which is what a collaborating agent
//!   waits for. A reader retries until the tip is visible or
//!   `visibility_timeout` passes (then the push counts as `unseen`, an
//!   error).
//! - `reader_fetch`: duration of every reader `git fetch`.
//!
//! - `reader_queue`: time from acknowledgement until a reader started on the
//!   push. It stays near zero unless the reader cap is reached; if it grows,
//!   `push_to_fetch` is measuring the load generator, not the server.
//!
//! Every acknowledged push gets its own independent reader, started at once:
//! readers come from an elastic pool of local repositories (two per writer to
//! begin with, growing up to `max_readers`). Each writer and each reader
//! repository shares the seed repository's objects, so clients start with
//! `main` and every push or fetch transfers only new commits.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Barrier, Condvar, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::git::{self, Git};
use crate::results::{Errors, RunResult, SCHEMA, now_unix_ms};
use crate::stats::Summary;
use crate::synth::{self, EPOCH};
use crate::target::Setup;

#[derive(Debug, Clone)]
pub struct WritersOpts {
    pub writers: u32,
    /// Most readers running at once; `None` means four per writer.
    pub max_readers: Option<u32>,
    /// Stop pushing after this long.
    pub duration: Option<Duration>,
    /// Stop each writer after this many pushes.
    pub pushes_per_writer: Option<u32>,
    /// Pause between a writer's pushes.
    pub interval: Duration,
    /// Size of the file each commit rewrites.
    pub commit_bytes: usize,
    pub visibility_timeout: Duration,
    /// Delete the benchmark branches afterwards.
    pub cleanup: bool,
}

impl WritersOpts {
    fn max_readers(&self) -> u32 {
        self.max_readers
            .unwrap_or(self.writers.saturating_mul(4))
            .max(1)
    }

    pub fn params(&self) -> BTreeMap<String, Value> {
        let mut m = BTreeMap::new();
        m.insert("writers".to_owned(), self.writers.into());
        m.insert("max_readers".to_owned(), self.max_readers().into());
        m.insert(
            "duration_secs".to_owned(),
            self.duration.map(|d| d.as_secs_f64()).into(),
        );
        m.insert(
            "pushes_per_writer".to_owned(),
            self.pushes_per_writer.into(),
        );
        m.insert(
            "interval_ms".to_owned(),
            (self.interval.as_millis() as u64).into(),
        );
        m.insert("commit_bytes".to_owned(), self.commit_bytes.into());
        m.insert(
            "visibility_timeout_secs".to_owned(),
            self.visibility_timeout.as_secs_f64().into(),
        );
        m
    }
}

/// A push the readers must observe.
#[derive(Debug)]
struct Acked {
    branch: String,
    tip: String,
    at: Instant,
}

#[derive(Debug, Default)]
struct WriterOut {
    push: Vec<Duration>,
    pushes_ok: u64,
    pushes_failed: u64,
    errors: Errors,
}

#[derive(Debug, Default)]
struct ReaderOut {
    push_to_fetch: Vec<Duration>,
    queue: Vec<Duration>,
    fetch: Vec<Duration>,
    seen: u64,
    unseen: u64,
    errors: Errors,
}

pub fn run(
    setup: &Setup,
    opts: &WritersOpts,
    mut params: BTreeMap<String, Value>,
) -> Result<RunResult, String> {
    if opts.writers == 0 {
        return Err("--writers must be at least 1".to_owned());
    }
    if opts.duration.is_none() && opts.pushes_per_writer.is_none() {
        return Err("set --duration-secs, --pushes-per-writer, or both".to_owned());
    }
    params.extend(opts.params());

    eprintln!(
        "aspen-bench: preparing {} writer repositories",
        opts.writers
    );
    let writer_repos = make_clones(setup, "w", 0, opts.writers)?;
    let initial_readers = opts.writers.saturating_mul(2).min(opts.max_readers());
    let pool = ReaderPool {
        state: Mutex::new((
            make_clones(setup, "r", 0, initial_readers)?,
            initial_readers,
        )),
        freed: Condvar::new(),
        max: opts.max_readers(),
    };
    let branches: Vec<String> = (0..opts.writers)
        .map(|i| format!("bench/{}/w{i}", setup.run_id))
        .collect();

    let (tx, rx) = mpsc::channel::<Acked>();
    let barrier = Arc::new(Barrier::new(opts.writers as usize + 1));
    let readers_out = Mutex::new(ReaderOut::default());
    let started_at_unix_ms = now_unix_ms();

    let (writer_outs, wall) = thread::scope(|s| {
        let (pool, readers_out) = (&pool, &readers_out);
        // Starts one reader per acknowledged push, as soon as it arrives.
        s.spawn(move || {
            for acked in rx {
                let repo = match pool.take(setup) {
                    Ok(repo) => repo,
                    Err(e) => {
                        lock(readers_out).errors.push(format!("reader setup: {e}"));
                        continue;
                    }
                };
                s.spawn(move || {
                    let out = observe(setup, &repo, &acked, opts.visibility_timeout);
                    pool.give(repo);
                    lock(readers_out).merge(out);
                });
            }
        });
        let writers: Vec<_> = writer_repos
            .iter()
            .zip(&branches)
            .enumerate()
            .map(|(i, (repo, branch))| {
                let tx = tx.clone();
                let barrier = Arc::clone(&barrier);
                s.spawn(move || writer(setup, opts, i as u64, repo, branch, &tx, &barrier))
            })
            .collect();
        drop(tx);
        barrier.wait();
        let start = Instant::now();
        let writer_outs: Vec<WriterOut> = writers
            .into_iter()
            .map(|h| h.join().unwrap_or_default())
            .collect();
        // Readers still finishing are joined when the scope ends.
        (writer_outs, start.elapsed())
    });
    let readers_out = readers_out
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);
    let readers_created = pool.state.into_inner().map_or(0, |s| s.1);

    if opts.cleanup {
        setup.delete_branches(&branches);
    }

    let mut errors = Errors::default();
    let mut push = Vec::new();
    let (mut pushes_ok, mut pushes_failed) = (0, 0);
    for w in writer_outs {
        push.extend(w.push);
        pushes_ok += w.pushes_ok;
        pushes_failed += w.pushes_failed;
        errors.merge(w.errors);
    }
    let ReaderOut {
        push_to_fetch,
        queue,
        fetch,
        seen,
        unseen,
        errors: reader_errors,
    } = readers_out;
    errors.merge(reader_errors);

    let wall_seconds = wall.as_secs_f64();
    let mut latencies_ms = BTreeMap::new();
    latencies_ms.insert("push".to_owned(), Summary::from_durations(&push));
    latencies_ms.insert(
        "push_to_fetch".to_owned(),
        Summary::from_durations(&push_to_fetch),
    );
    latencies_ms.insert("reader_fetch".to_owned(), Summary::from_durations(&fetch));
    latencies_ms.insert("reader_queue".to_owned(), Summary::from_durations(&queue));
    let mut throughput = BTreeMap::new();
    throughput.insert(
        "pushes_per_second".to_owned(),
        pushes_ok as f64 / wall_seconds.max(f64::EPSILON),
    );
    let mut counts = BTreeMap::new();
    counts.insert("pushes_ok".to_owned(), pushes_ok);
    counts.insert("pushes_failed".to_owned(), pushes_failed);
    counts.insert("reader_fetches".to_owned(), fetch.len() as u64);
    counts.insert("seen".to_owned(), seen);
    counts.insert("unseen".to_owned(), unseen);
    counts.insert("readers_created".to_owned(), u64::from(readers_created));

    Ok(RunResult {
        schema: SCHEMA.to_owned(),
        workload: "writers".to_owned(),
        target: setup.info.clone(),
        params,
        environment: setup.environment.clone(),
        started_at_unix_ms,
        wall_seconds,
        latencies_ms,
        throughput,
        counts,
        errors,
    })
}

/// Shared clones of the seed repository named `<prefix><i>.git`, for `i` in
/// `first..first + count`.
fn make_clones(
    setup: &Setup,
    prefix: &str,
    first: u32,
    count: u32,
) -> Result<Vec<PathBuf>, String> {
    let paths: Vec<PathBuf> = (first..first + count)
        .map(|i| setup.path(&format!("{prefix}{i}.git")))
        .collect();
    // Cloning is local and cheap, but thousands of clients are still worth
    // preparing in parallel.
    let chunk = paths.len().div_ceil(16).max(1);
    thread::scope(|s| {
        let handles: Vec<_> = paths
            .chunks(chunk)
            .map(|group| {
                s.spawn(move || {
                    group
                        .iter()
                        .try_for_each(|p| git::shared_clone(&setup.git, &setup.seed_repo, p))
                })
            })
            .collect();
        handles.into_iter().try_for_each(|h| {
            h.join()
                .unwrap_or_else(|_| Err("clone thread panicked".to_owned()))
        })
    })?;
    Ok(paths)
}

fn writer(
    setup: &Setup,
    opts: &WritersOpts,
    index: u64,
    repo: &Path,
    branch: &str,
    tx: &Sender<Acked>,
    barrier: &Barrier,
) -> WriterOut {
    let mut out = WriterOut::default();
    let file = format!("bench/{}/w{index}.txt", setup.run_id);
    let mut parent = setup.main.clone();
    let push_url = setup.push_url(branch);
    barrier.wait();
    let start = Instant::now();
    let mut seq = 0u32;
    loop {
        if opts.pushes_per_writer.is_some_and(|max| seq >= max)
            || opts.duration.is_some_and(|d| start.elapsed() >= d)
        {
            break;
        }
        // Commit content depends on the run, the writer and the sequence
        // number, so every push carries new objects.
        let content = synth::text(
            setup.run_id_seed(),
            &[index, u64::from(seq)],
            opts.commit_bytes,
        );
        let commit = synth::NewCommit {
            branch: "bench",
            parent: &parent,
            path: &file,
            content: content.as_bytes(),
            message: &format!("bench writer {index} push {seq}"),
            timestamp: EPOCH + u64::from(seq),
        };
        let tip = match synth::commit_on(&setup.git, repo, &commit) {
            Ok(tip) => tip,
            Err(e) => {
                out.errors
                    .push(format!("writer {index}: making a commit: {e}"));
                break;
            }
        };
        let (took, res) = git::run_timed(setup.git.cmd(repo).args([
            "push",
            "--quiet",
            "--no-verify",
            &push_url,
            &format!("{tip}:refs/heads/{branch}"),
        ]));
        match res {
            Ok(_) => {
                let at = Instant::now();
                out.push.push(took);
                out.pushes_ok += 1;
                parent = tip.clone();
                let _ = tx.send(Acked {
                    branch: branch.to_owned(),
                    tip,
                    at,
                });
            }
            Err(e) => {
                out.pushes_failed += 1;
                out.errors.push(format!("writer {index}: {e}"));
                // The branch may or may not have moved; re-read it so the
                // next commit builds on whatever the server has.
                if let Ok(line) = git::run(setup.git.cmd(repo).args([
                    "ls-remote",
                    &push_url,
                    &format!("refs/heads/{branch}"),
                ])) && let Some(oid) = line.split_whitespace().next()
                    && git::run(setup.git.cmd(repo).args(["cat-file", "-e", oid])).is_ok()
                {
                    parent = oid.to_owned();
                }
            }
        }
        seq += 1;
        if !opts.interval.is_zero() {
            thread::sleep(opts.interval);
        }
    }
    out
}

/// Locks a mutex, carrying on if another thread panicked while holding it
/// (the data is plain counters and samples).
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl ReaderOut {
    fn merge(&mut self, other: ReaderOut) {
        self.push_to_fetch.extend(other.push_to_fetch);
        self.queue.extend(other.queue);
        self.fetch.extend(other.fetch);
        self.seen += other.seen;
        self.unseen += other.unseen;
        self.errors.merge(other.errors);
    }
}

/// Reader repositories: idle ones and how many exist, capped at `max`.
#[derive(Debug)]
struct ReaderPool {
    state: Mutex<(Vec<PathBuf>, u32)>,
    freed: Condvar,
    max: u32,
}

impl ReaderPool {
    /// An idle reader repository, creating one if none is idle and the cap
    /// allows, otherwise waiting for one to be returned.
    fn take(&self, setup: &Setup) -> Result<PathBuf, String> {
        let mut state = lock(&self.state);
        loop {
            if let Some(repo) = state.0.pop() {
                return Ok(repo);
            }
            if state.1 < self.max {
                let n = state.1;
                state.1 += 1;
                drop(state);
                return make_clones(setup, "r", n, 1).map(|mut v| v.remove(0));
            }
            state = self
                .freed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn give(&self, repo: PathBuf) {
        lock(&self.state).0.push(repo);
        self.freed.notify_one();
    }
}

/// Fetches `acked.branch` into `repo` until it shows `acked.tip`.
fn observe(setup: &Setup, repo: &Path, acked: &Acked, timeout: Duration) -> ReaderOut {
    let mut out = ReaderOut::default();
    out.queue.push(acked.at.elapsed());
    let local = format!("refs/bench/{}", acked.branch.replace('/', "-"));
    let mut last_error = None;
    loop {
        let (took, res) = git::run_timed(setup.git.cmd(repo).args([
            "fetch",
            "--quiet",
            "--no-tags",
            "--no-write-fetch-head",
            &setup.remote.url,
            &format!("+refs/heads/{}:{local}", acked.branch),
        ]));
        let done = Instant::now();
        out.fetch.push(took);
        let pause = match res.and_then(|_| visible(&setup.git, repo, &local, &acked.tip)) {
            Ok(true) => {
                out.push_to_fetch.push(done - acked.at);
                out.seen += 1;
                return out;
            }
            Ok(false) => Duration::from_millis(10),
            Err(e) => {
                last_error = Some(e);
                Duration::from_millis(100)
            }
        };
        if acked.at.elapsed() >= timeout {
            // One error per unseen push, however many fetches failed.
            out.unseen += 1;
            out.errors.push(format!(
                "reader: {} at {} not visible after {:?}{}",
                acked.branch,
                acked.tip,
                timeout,
                last_error
                    .map(|e| format!("; last error: {e}"))
                    .unwrap_or_default()
            ));
            return out;
        }
        thread::sleep(pause);
    }
}

/// Whether the fetched `local` ref is `tip` or a descendant of it.
fn visible(git: &Git, repo: &Path, local: &str, tip: &str) -> Result<bool, String> {
    let fetched = git::run(git.cmd(repo).args(["rev-parse", local]))?;
    if fetched == tip {
        return Ok(true);
    }
    let status = git
        .cmd(repo)
        .args(["merge-base", "--is-ancestor", tip, &fetched])
        .status()
        .map_err(|e| format!("spawning git merge-base: {e}"))?;
    Ok(status.success())
}

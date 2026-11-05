//! Fetch storm: M jobs fetch the same new commit at the same moment, the way
//! CI jobs do after a push.
//!
//! Each round pushes one new commit on top of `main` to a fresh branch, then
//! releases all M jobs at once through a barrier. Every job runs `git fetch`
//! of that branch and checks it got the expected commit.
//!
//! - `warm` jobs already have `main` (like CI runners with a cached
//!   checkout), so each fetch transfers one commit's worth of objects.
//! - `cold` jobs start from an empty repository (like fresh CI containers),
//!   so each fetch transfers the full history.
//!
//! `--depth N` makes every job a shallow fetch (`git fetch --depth=N`). With
//! `--mode cold --depth 1` this is the `actions/checkout` default, the
//! depth-1 fetch storm of section 4.6.
//!
//! Measured: `fetch` latency per job, fetches per second over the rounds'
//! wall time, and received bytes (the packs each job stored), in total and
//! per fetch.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::git;
use crate::results::{Errors, RunResult, SCHEMA, now_unix_ms};
use crate::stats::Summary;
use crate::synth::{self, EPOCH};
use crate::target::Setup;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Mode {
    Warm,
    Cold,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Warm => "warm",
            Self::Cold => "cold",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StormOpts {
    pub jobs: u32,
    pub rounds: u32,
    pub mode: Mode,
    /// Shallow fetch depth; `None` fetches full history.
    pub depth: Option<u32>,
    pub cleanup: bool,
}

impl StormOpts {
    pub fn params(&self) -> BTreeMap<String, Value> {
        let mut m = BTreeMap::new();
        m.insert("jobs".to_owned(), self.jobs.into());
        m.insert("rounds".to_owned(), self.rounds.into());
        m.insert("mode".to_owned(), self.mode.as_str().into());
        m.insert("depth".to_owned(), self.depth.into());
        m
    }
}

#[derive(Debug, Default)]
struct JobOut {
    took: Option<Duration>,
    bytes: u64,
    error: Option<String>,
}

pub fn run(
    setup: &Setup,
    opts: &StormOpts,
    mut params: BTreeMap<String, Value>,
) -> Result<RunResult, String> {
    if opts.jobs == 0 || opts.rounds == 0 {
        return Err("--jobs and --rounds must be at least 1".to_owned());
    }
    if opts.depth == Some(0) {
        return Err("--depth must be at least 1".to_owned());
    }
    params.extend(opts.params());
    let started_at_unix_ms = now_unix_ms();

    let pusher = setup.path("storm-pusher.git");
    git::shared_clone(&setup.git, &setup.seed_repo, &pusher)?;
    let mut fetch = Vec::new();
    let mut bytes = 0u64;
    let mut errors = Errors::default();
    let mut storm_wall = Duration::ZERO;
    let mut branches = Vec::new();

    for round in 0..opts.rounds {
        // Each round's commit sits directly on main, so every round transfers
        // a similar amount.
        let branch = format!("bench/{}/storm{round}", setup.run_id);
        let content = synth::text(setup.run_id_seed(), &[0x5354, u64::from(round)], 4096);
        let tip = synth::commit_on(
            &setup.git,
            &pusher,
            &synth::NewCommit {
                branch: "storm",
                parent: &setup.main,
                path: &format!("bench/{}/storm.txt", setup.run_id),
                content: content.as_bytes(),
                message: &format!("bench fetch storm round {round}"),
                timestamp: EPOCH + u64::from(round),
            },
        )?;
        git::run(setup.git.cmd(&pusher).args([
            "push",
            "--quiet",
            "--no-verify",
            &setup.push_url(&branch),
            &format!("{tip}:refs/heads/{branch}"),
        ]))?;
        branches.push(branch.clone());

        let repos = prepare_jobs(setup, opts, round)?;
        eprintln!(
            "aspen-bench: fetch storm round {}/{}: {} {} jobs",
            round + 1,
            opts.rounds,
            opts.jobs,
            opts.mode.as_str()
        );
        let barrier = Arc::new(Barrier::new(repos.len() + 1));
        let (outs, wall) = thread::scope(|s| {
            let handles: Vec<_> = repos
                .iter()
                .map(|repo| {
                    let barrier = Arc::clone(&barrier);
                    let (branch, tip) = (&branch, &tip);
                    s.spawn(move || job(setup, repo, branch, tip, opts.depth, &barrier))
                })
                .collect();
            barrier.wait();
            let start = Instant::now();
            let outs: Vec<JobOut> = handles
                .into_iter()
                .map(|h| h.join().unwrap_or_default())
                .collect();
            (outs, start.elapsed())
        });
        storm_wall += wall;
        for o in outs {
            if let Some(t) = o.took {
                fetch.push(t);
            }
            bytes += o.bytes;
            if let Some(e) = o.error {
                errors.push(e);
            }
        }
        // Job repositories can be large in cold mode; free the disk now.
        for repo in &repos {
            let _ = std::fs::remove_dir_all(repo);
        }
    }

    if opts.cleanup {
        setup.delete_branches(&branches);
    }

    let wall_seconds = storm_wall.as_secs_f64();
    let ok = fetch.len() as u64;
    let mut latencies_ms = BTreeMap::new();
    latencies_ms.insert("fetch".to_owned(), Summary::from_durations(&fetch));
    let mut throughput = BTreeMap::new();
    let secs = wall_seconds.max(f64::EPSILON);
    throughput.insert("fetches_per_second".to_owned(), ok as f64 / secs);
    throughput.insert(
        "received_mib_per_second".to_owned(),
        bytes as f64 / (1024.0 * 1024.0) / secs,
    );
    let mut counts = BTreeMap::new();
    counts.insert("fetches_ok".to_owned(), ok);
    counts.insert("received_bytes".to_owned(), bytes);
    counts.insert(
        "received_bytes_per_fetch".to_owned(),
        bytes.checked_div(ok).unwrap_or(0),
    );
    Ok(RunResult {
        schema: SCHEMA.to_owned(),
        workload: "fetch-storm".to_owned(),
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

fn prepare_jobs(setup: &Setup, opts: &StormOpts, round: u32) -> Result<Vec<PathBuf>, String> {
    let paths: Vec<PathBuf> = (0..opts.jobs)
        .map(|i| setup.path(&format!("storm{round}-j{i}.git")))
        .collect();
    let chunk = paths.len().div_ceil(16).max(1);
    thread::scope(|s| {
        let handles: Vec<_> = paths
            .chunks(chunk)
            .map(|group| {
                s.spawn(move || {
                    group.iter().try_for_each(|p| match opts.mode {
                        Mode::Warm => git::shared_clone(&setup.git, &setup.seed_repo, p),
                        Mode::Cold => git::init_bare(&setup.git, p),
                    })
                })
            })
            .collect();
        handles.into_iter().try_for_each(|h| {
            h.join()
                .unwrap_or_else(|_| Err("job setup thread panicked".to_owned()))
        })
    })?;
    Ok(paths)
}

fn job(
    setup: &Setup,
    repo: &Path,
    branch: &str,
    tip: &str,
    depth: Option<u32>,
    barrier: &Barrier,
) -> JobOut {
    let before = git::pack_bytes(repo);
    let mut cmd = setup.git.cmd(repo);
    cmd.args(["fetch", "--quiet", "--no-tags", "--no-write-fetch-head"]);
    if let Some(depth) = depth {
        cmd.arg(format!("--depth={depth}"));
    }
    cmd.arg(&setup.remote.url)
        .arg(format!("+refs/heads/{branch}:refs/storm"));
    barrier.wait();
    let (took, res) = git::run_timed(&mut cmd);
    let checked = res.and_then(|_| git::run(setup.git.cmd(repo).args(["rev-parse", "refs/storm"])));
    match checked {
        Ok(got) if got == tip => JobOut {
            took: Some(took),
            bytes: git::pack_bytes(repo).saturating_sub(before),
            error: None,
        },
        Ok(got) => JobOut {
            error: Some(format!("fetch storm: got {got}, expected {tip}")),
            ..JobOut::default()
        },
        Err(e) => JobOut {
            error: Some(format!("fetch storm: {e}")),
            ..JobOut::default()
        },
    }
}

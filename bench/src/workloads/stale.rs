//! Stale-branch fetch: a client current on trunk fetches a branch that was
//! created N trunk pushes ago (PLAN section 4.6, fetch path step 2).
//!
//! The workload pushes `max(N)` commits to `main`, one push each, so the
//! server records one trunk ref version per push. Before trunk push number
//! `max(N) - N` it pushes one commit on a new branch `stale-N`, built on the
//! trunk commit of that moment. After the last trunk push, for each N, a
//! fresh client that holds exactly `main` at its tip fetches `stale-N`, and
//! the workload checks it got the branch's commit.
//!
//! What it reports, per N (`n<N>` in the result keys):
//!
//! - `stale_fetch_n<N>`: wall time of each `git fetch`;
//! - `received_bytes.n<N>`: bytes of the pack the client stored (the
//!   branch's one commit, tree and blob are all it lacks);
//! - with `--metrics-url` (Aspen only), read from `/-/metrics` around each
//!   fetch: `visited.n<N>` (`PlanStats.visited`), `loaded_versions.n<N>`
//!   (ref versions read from the metadata store to plan it),
//!   `chain_skips.n<N>` (`PlanStats.chain_skips`), `plans.n<N>` (fetches
//!   planned; one per `git fetch`), and `plan_n<N>`, the planning time.
//!   These are the last repetition's counts (they are deterministic for a
//!   given server state; the latencies are summarised over every
//!   repetition).
//!
//! It also reports the trunk pushes themselves (`trunk_push`, and the first
//! and last tenth separately, so growth with history length shows).
//!
//! With `--metrics-url`, the workload waits for the server's background
//! jobs (`aspen_jobs`) to drain before fetching, so a server that compacts
//! is measured after compaction has caught up: no job queued or running
//! for more than 30 s, or the run fails. Run Aspen once normally and once
//! with `aspen dev --no-background-worker` (and `--no-background-worker`
//! here, which accepts jobs that stay queued) for the plan's "with and
//! without compaction".

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::git::{self, AUTHOR};
use crate::results::{Errors, RunResult, SCHEMA, now_unix_ms};
use crate::stats::Summary;
use crate::synth::EPOCH;
use crate::target::Setup;

#[derive(Debug, Clone)]
pub struct StaleOpts {
    /// Branch ages N, in trunk pushes. The largest is how many trunk pushes
    /// the run makes.
    pub points: Vec<u32>,
    /// Fetches per point, each from a fresh trunk-current client.
    pub repeats: u32,
    /// Aspen's `/-/metrics` URL (plain `http://` only).
    pub metrics_url: Option<String>,
    /// Longest wait for background jobs to drain before fetching.
    pub settle_timeout: Duration,
    /// Whether the server runs its background worker. With it, the run
    /// waits until no job is queued or running, and fails if that does not
    /// happen in time; without it (`aspen dev --no-background-worker`),
    /// jobs stay queued and the run waits only until none is running.
    pub background_worker: bool,
    pub cleanup: bool,
}

impl StaleOpts {
    pub fn params(&self) -> BTreeMap<String, Value> {
        let mut m = BTreeMap::new();
        let points: Vec<Value> = self.points.iter().map(|&n| n.into()).collect();
        m.insert("points".to_owned(), Value::Array(points));
        m.insert("repeats".to_owned(), self.repeats.into());
        if self.metrics_url.is_some() {
            m.insert(
                "background_worker".to_owned(),
                self.background_worker.into(),
            );
        }
        m
    }

    fn validate(&mut self) -> Result<(), String> {
        self.points.sort_unstable();
        self.points.dedup();
        if self.points.is_empty() || self.points[0] == 0 {
            return Err("--points must list branch ages of at least 1".to_owned());
        }
        if self.repeats == 0 {
            return Err("--repeats must be at least 1".to_owned());
        }
        if let Some(url) = &self.metrics_url {
            MetricsEndpoint::parse(url)?;
        }
        Ok(())
    }

    fn max(&self) -> u32 {
        self.points.last().copied().unwrap_or(0)
    }
}

/// The fast-import stream for the run's history, written on top of `main`:
/// `refs/heads/trunk` gets `trunk` commits in a line, and `refs/heads/stale-N`
/// one commit on the trunk commit made after `max - N` trunk commits (the
/// seed's `main` when `N` is the largest point). Marks: `:1..=:trunk` are the
/// trunk commits, `:(trunk + 1 + i)` the branch of `points[i]`.
fn history_stream(main: &str, run_id: &str, points: &[u32], trunk: u32) -> Vec<u8> {
    let mut s = String::new();
    let base = |done: u32| {
        if done == 0 {
            main.to_owned()
        } else {
            format!(":{done}")
        }
    };
    let mut branch_mark = trunk + 1;
    for done in 0..=trunk {
        for &n in points.iter().filter(|&&n| trunk - n == done) {
            let _ = writeln!(s, "commit refs/heads/stale-{n}");
            let _ = writeln!(s, "mark :{branch_mark}");
            let _ = writeln!(s, "committer {AUTHOR} {} +0000", when(done) + 30);
            data(
                &mut s,
                &format!("stale branch created {n} trunk pushes ago"),
            );
            let _ = writeln!(s, "from {}", base(done));
            let _ = writeln!(s, "M 100644 inline bench/{run_id}/stale-{n}.txt");
            data(&mut s, &format!("branch {n} of run {run_id}\n"));
            s.push('\n');
            branch_mark += 1;
        }
        if done == trunk {
            break;
        }
        let i = done + 1;
        let _ = writeln!(s, "commit refs/heads/trunk");
        let _ = writeln!(s, "mark :{i}");
        let _ = writeln!(s, "committer {AUTHOR} {} +0000", when(i));
        data(&mut s, &format!("trunk push {i}"));
        let _ = writeln!(s, "from {}", base(done));
        let _ = writeln!(s, "M 100644 inline bench/{run_id}/trunk.txt");
        data(&mut s, &format!("trunk version {i} of run {run_id}\n"));
        s.push('\n');
    }
    s.into_bytes()
}

/// Commit time of trunk commit `i`, a minute apart and after every seed
/// commit. Git's negotiator sends haves newest first, so a history dated
/// before its seed would make clients offer seed commits ahead of trunk.
fn when(i: u32) -> u64 {
    EPOCH + 100_000_000 + u64::from(i) * 60
}

fn data(s: &mut String, body: &str) {
    let _ = writeln!(s, "data {}", body.len());
    s.push_str(body);
    s.push('\n');
}

pub fn run(
    setup: &Setup,
    opts: &StaleOpts,
    mut params: BTreeMap<String, Value>,
) -> Result<RunResult, String> {
    let mut opts = opts.clone();
    opts.validate()?;
    params.extend(opts.params());
    let started_at_unix_ms = now_unix_ms();
    let started = Instant::now();
    let trunk = opts.max();
    let metrics = opts
        .metrics_url
        .as_deref()
        .map(MetricsEndpoint::parse)
        .transpose()?;

    // All commits are written up front, in a repository that borrows the
    // seed's objects: the pushes then measure the server, not commit
    // creation. Branch commits live in `history`, never in the clients.
    eprintln!(
        "aspen-bench: writing {trunk} trunk commits and {} branches",
        opts.points.len()
    );
    let history = setup.path("stale-history.git");
    git::shared_clone(&setup.git, &setup.seed_repo, &history)?;
    let stream = history_stream(&setup.main, &setup.run_id, &opts.points, trunk);
    git::run_with_input(
        setup
            .git
            .cmd(&history)
            .args(["fast-import", "--quiet", "--force"]),
        &stream,
    )?;
    let trunk_commits: Vec<String> = {
        let out = git::run(setup.git.cmd(&history).args([
            "rev-list",
            "--reverse",
            "--first-parent",
            &format!("{}..refs/heads/trunk", setup.main),
        ]))?;
        out.lines().map(str::to_owned).collect()
    };
    if trunk_commits.len() != trunk as usize {
        return Err(format!(
            "expected {trunk} trunk commits, found {}",
            trunk_commits.len()
        ));
    }
    let mut branch_tips = BTreeMap::new();
    for &n in &opts.points {
        let tip = git::run(
            setup
                .git
                .cmd(&history)
                .args(["rev-parse", &format!("refs/heads/stale-{n}")]),
        )?;
        branch_tips.insert(n, tip);
    }

    let mut errors = Errors::default();
    let mut pushes = Vec::with_capacity(trunk as usize);
    let mut branches = Vec::new();
    let push = |url: &str, refspec: String| -> (Duration, Result<String, String>) {
        git::run_timed(setup.git.cmd(&history).args([
            "push",
            "--quiet",
            "--no-verify",
            url,
            &refspec,
        ]))
    };
    let progress_every = (trunk / 10).max(1);
    for done in 0..=trunk {
        for &n in opts.points.iter().filter(|&&n| trunk - n == done) {
            let branch = format!("bench/{}/stale-{n}", setup.run_id);
            let (_, res) = push(
                &setup.push_url(&branch),
                format!("{}:refs/heads/{branch}", branch_tips[&n]),
            );
            res.map_err(|e| format!("pushing branch {branch}: {e}"))?;
            branches.push(branch);
        }
        if done == trunk {
            break;
        }
        let commit = &trunk_commits[done as usize];
        let (took, res) = push(&setup.remote.url, format!("{commit}:refs/heads/main"));
        match res {
            Ok(_) => pushes.push(took),
            // Every later version builds on this one, so stop here.
            Err(e) => return Err(format!("trunk push {} of {trunk}: {e}", done + 1)),
        }
        if (done + 1) % progress_every == 0 {
            eprintln!("aspen-bench: {} of {trunk} trunk pushes", done + 1);
        }
    }
    let push_seconds = started.elapsed().as_secs_f64();

    if let Some(m) = &metrics {
        settle(m, opts.settle_timeout, opts.background_worker)?;
    }

    // The trunk-current client: `main` at the last trunk commit, and no
    // other refs. Each fetch gets its own cheap copy.
    let trunk_tip = trunk_commits
        .last()
        .cloned()
        .unwrap_or_else(|| setup.main.clone());
    let client = setup.path("stale-client.git");
    git::shared_clone(&setup.git, &setup.seed_repo, &client)?;
    git::run(setup.git.cmd(&client).args([
        "fetch",
        "--quiet",
        "--no-tags",
        &setup.remote.url,
        "+refs/heads/main:refs/heads/main",
    ]))?;
    let got = git::run(
        setup
            .git
            .cmd(&client)
            .args(["rev-parse", "refs/heads/main"]),
    )?;
    if got != trunk_tip {
        return Err(format!("the server's main is {got}, expected {trunk_tip}"));
    }

    let fetch_started = Instant::now();
    let mut latencies_ms = BTreeMap::new();
    let mut counts = BTreeMap::new();
    let mut all_fetches = Vec::new();
    for &n in &opts.points {
        let branch = format!("bench/{}/stale-{n}", setup.run_id);
        let mut took = Vec::new();
        let mut plan_ms = Vec::new();
        let mut last: Option<PlanDelta> = None;
        let mut bytes = 0;
        for r in 0..opts.repeats {
            let repo = setup.path(&format!("stale-{n}-{r}.git"));
            git::shared_clone(&setup.git, &client, &repo)?;
            // A shared clone would also offer the tips of the repositories
            // it borrows from as haves; a real client has only its own refs.
            git::run(
                setup
                    .git
                    .cmd(&repo)
                    .args(["config", "core.alternateRefsCommand", "true"]),
            )?;
            let before_bytes = git::pack_bytes(&repo);
            let before = metrics
                .as_ref()
                .map(MetricsEndpoint::plan_totals)
                .transpose()?;
            let mut cmd = setup.git.cmd(&repo);
            cmd.args(["fetch", "--quiet", "--no-tags", "--no-write-fetch-head"])
                .arg(&setup.remote.url)
                .arg(format!("+refs/heads/{branch}:refs/heads/stale"));
            let (t, res) = git::run_timed(&mut cmd);
            let after = metrics
                .as_ref()
                .map(MetricsEndpoint::plan_totals)
                .transpose()?;
            let checked = res.and_then(|_| {
                git::run(setup.git.cmd(&repo).args(["rev-parse", "refs/heads/stale"]))
            });
            match checked {
                Ok(got) if got == branch_tips[&n] => {
                    took.push(t);
                    all_fetches.push(t);
                    bytes = git::pack_bytes(&repo).saturating_sub(before_bytes);
                    if let (Some(b), Some(a)) = (before, after) {
                        let d = a.minus(&b);
                        plan_ms.push(d.plan_seconds * 1000.0);
                        last = Some(d);
                    }
                }
                Ok(got) => errors.push(format!(
                    "stale-{n}: got {got}, expected {}",
                    branch_tips[&n]
                )),
                Err(e) => errors.push(format!("stale-{n}: {e}")),
            }
            let _ = std::fs::remove_dir_all(&repo);
        }
        latencies_ms.insert(format!("stale_fetch_n{n}"), Summary::from_durations(&took));
        counts.insert(format!("received_bytes.n{n}"), bytes);
        if let Some(d) = last {
            latencies_ms.insert(format!("plan_n{n}"), Summary::from_millis(&plan_ms));
            counts.insert(format!("visited.n{n}"), d.visited);
            counts.insert(format!("loaded_versions.n{n}"), d.loaded);
            counts.insert(format!("chain_skips.n{n}"), d.chain_skips);
            counts.insert(format!("plans.n{n}"), d.plans);
        }
        eprintln!(
            "aspen-bench: stale branch N={n}: {} fetches{}",
            took.len(),
            last.map_or_else(String::new, |d| format!(
                ", visited {}, loaded {}, chain skips {}",
                d.visited, d.loaded, d.chain_skips
            ))
        );
    }
    let fetch_seconds = fetch_started.elapsed().as_secs_f64();

    if opts.cleanup {
        setup.delete_branches(&branches);
    }

    let tenth = (pushes.len() / 10).max(1).min(pushes.len());
    latencies_ms.insert("trunk_push".to_owned(), Summary::from_durations(&pushes));
    latencies_ms.insert(
        "trunk_push_first_tenth".to_owned(),
        Summary::from_durations(&pushes[..tenth]),
    );
    latencies_ms.insert(
        "trunk_push_last_tenth".to_owned(),
        Summary::from_durations(&pushes[pushes.len() - tenth..]),
    );
    counts.insert("trunk_pushes".to_owned(), pushes.len() as u64);
    counts.insert("fetches_ok".to_owned(), all_fetches.len() as u64);
    let mut throughput = BTreeMap::new();
    throughput.insert(
        "trunk_pushes_per_second".to_owned(),
        pushes.len() as f64 / push_seconds.max(f64::EPSILON),
    );
    Ok(RunResult {
        schema: SCHEMA.to_owned(),
        workload: "stale-branch".to_owned(),
        target: setup.info.clone(),
        params,
        environment: setup.environment.clone(),
        started_at_unix_ms,
        wall_seconds: push_seconds + fetch_seconds,
        latencies_ms,
        throughput,
        counts,
        errors,
    })
}

/// Waits until the server's background work is idle ([`idle`]) and stays
/// so across more than 30 s (the server reads these gauges at most every
/// 30 s). With a worker, a stalled compactor leaves jobs queued and the run
/// fails at the timeout instead of being measured as "with compaction".
fn settle(m: &MetricsEndpoint, timeout: Duration, worker: bool) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut stable: Option<(Instant, u64)> = None;
    eprintln!("aspen-bench: waiting for the server's background jobs to settle");
    loop {
        let (queued, running) = m.open_jobs()?;
        match stable {
            Some((since, q)) if idle(worker, queued, running) && q == queued => {
                if since.elapsed() > Duration::from_secs(31) {
                    if queued > 0 {
                        eprintln!(
                            "aspen-bench: {queued} jobs stay queued (the background worker is off)"
                        );
                    }
                    return Ok(());
                }
            }
            _ if idle(worker, queued, running) => stable = Some((Instant::now(), queued)),
            _ => stable = None,
        }
        if Instant::now() > deadline {
            let want = if worker { "drain" } else { "stop running" };
            return Err(format!(
                "background jobs did not {want} within {}s ({queued} queued, {running} running)",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_secs(5));
    }
}

/// Whether one reading of the job gauges counts as idle: nothing running,
/// and, when the server has a background worker, nothing queued either.
fn idle(worker: bool, queued: u64, running: u64) -> bool {
    running == 0 && (!worker || queued == 0)
}

/// Planner totals from `/-/metrics`.
#[derive(Debug, Clone, Copy, Default)]
struct PlanDelta {
    plans: u64,
    visited: u64,
    loaded: u64,
    chain_skips: u64,
    plan_seconds: f64,
}

impl PlanDelta {
    fn minus(&self, before: &Self) -> Self {
        Self {
            plans: self.plans.saturating_sub(before.plans),
            visited: self.visited.saturating_sub(before.visited),
            loaded: self.loaded.saturating_sub(before.loaded),
            chain_skips: self.chain_skips.saturating_sub(before.chain_skips),
            plan_seconds: (self.plan_seconds - before.plan_seconds).max(0.0),
        }
    }
}

/// A Prometheus text endpoint reached with one plain HTTP/1.1 GET.
#[derive(Debug, Clone)]
pub struct MetricsEndpoint {
    host_port: String,
    path: String,
}

impl MetricsEndpoint {
    pub fn parse(url: &str) -> Result<Self, String> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| format!("--metrics-url must be a plain http:// URL (got {url:?})"))?;
        let (host_port, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        if host_port.is_empty() || host_port.contains('@') {
            return Err(format!(
                "--metrics-url {url:?}: no host, or credentials in it"
            ));
        }
        Ok(Self {
            host_port: host_port.to_owned(),
            path: path.to_owned(),
        })
    }

    fn get(&self) -> Result<String, String> {
        let mut conn = TcpStream::connect(&self.host_port)
            .map_err(|e| format!("connecting to {}: {e}", self.host_port))?;
        conn.set_read_timeout(Some(Duration::from_secs(60)))
            .map_err(|e| e.to_string())?;
        write!(
            conn,
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.path, self.host_port
        )
        .map_err(|e| format!("requesting {}: {e}", self.path))?;
        let mut body = String::new();
        conn.read_to_string(&mut body)
            .map_err(|e| format!("reading {}: {e}", self.path))?;
        if !body.starts_with("HTTP/1.1 200") {
            let status = body.lines().next().unwrap_or("");
            return Err(format!("GET {}: {status}", self.path));
        }
        Ok(body)
    }

    fn plan_totals(&self) -> Result<PlanDelta, String> {
        let text = self.get()?;
        let value = |name: &str| -> Result<f64, String> {
            sample(&text, name).ok_or_else(|| format!("metric {name} missing from {}", self.path))
        };
        Ok(PlanDelta {
            plans: value("aspen_fetch_plan_visited_versions_count")? as u64,
            visited: value("aspen_fetch_plan_visited_versions_sum")? as u64,
            loaded: value("aspen_fetch_plan_loaded_versions_sum")? as u64,
            chain_skips: value("aspen_fetch_plan_chain_skips_total")? as u64,
            plan_seconds: value("aspen_fetch_plan_duration_seconds_sum")?,
        })
    }

    /// Jobs queued and jobs running, each summed over kinds.
    fn open_jobs(&self) -> Result<(u64, u64), String> {
        let text = self.get()?;
        job_counts(&text).ok_or_else(|| format!("no aspen_jobs gauges at {}", self.path))
    }
}

/// Queued and running jobs from the `aspen_jobs{kind,state}` gauges.
fn job_counts(text: &str) -> Option<(u64, u64)> {
    let (mut queued, mut running, mut seen) = (0.0, 0.0, false);
    for line in text.lines() {
        let Some((labels, v)) = line
            .strip_prefix("aspen_jobs{")
            .and_then(|rest| rest.split_once("} "))
        else {
            continue;
        };
        seen = true;
        let v = v.trim().parse::<f64>().unwrap_or(0.0);
        if labels.contains("state=\"queued\"") {
            queued += v;
        } else if labels.contains("state=\"running\"") {
            running += v;
        }
    }
    seen.then_some((queued as u64, running as u64))
}

/// The value of an unlabelled sample `name` in Prometheus text.
fn sample(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' '))
        .and_then(|v| v.trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_history_puts_each_branch_n_pushes_before_the_last() {
        let s = String::from_utf8(history_stream("abc", "r", &[1, 3], 3)).unwrap();
        // stale-3 is created on the seed's main, before any trunk push.
        let stale3 = s.find("commit refs/heads/stale-3").unwrap();
        assert!(stale3 < s.find("mark :1\n").unwrap());
        assert!(s[stale3..].contains("from abc\n"));
        // stale-1 is created on trunk commit 2, before the third push.
        let stale1 = s.find("commit refs/heads/stale-1").unwrap();
        assert!(s[stale1..].starts_with("commit refs/heads/stale-1\nmark :5\n"));
        assert!(s[stale1..].contains("from :2\n"));
        assert!(stale1 < s.find("mark :3\n").unwrap());
        assert_eq!(s.matches("commit refs/heads/trunk").count(), 3);
        // Trunk is dated after the seed, and each branch between the trunk
        // commit it is built on and the next one.
        assert!(s.contains(&format!("committer {AUTHOR} {} +0000", when(1))));
        assert!(s[stale1..].contains(&format!("committer {AUTHOR} {} +0000", when(2) + 30)));
        // After the largest synthetic preset (1,000 commits a minute apart)
        // and after any real history up to 2027.
        assert!(when(1) > EPOCH + 1_000 * 60);
        assert!(when(1) > 1_798_000_000);
    }

    #[test]
    fn samples_and_endpoints_parse() {
        let text = "# HELP x\nfoo_sum 12.5\nfoo_sum_extra 3\nfoo 7\n";
        assert_eq!(sample(text, "foo_sum"), Some(12.5));
        assert_eq!(sample(text, "foo"), Some(7.0));
        assert_eq!(sample(text, "bar"), None);
        let m = MetricsEndpoint::parse("http://127.0.0.1:8086/-/metrics").unwrap();
        assert_eq!(
            (m.host_port.as_str(), m.path.as_str()),
            ("127.0.0.1:8086", "/-/metrics")
        );
        assert!(MetricsEndpoint::parse("https://h/-/metrics").is_err());
        assert!(MetricsEndpoint::parse("http://u:p@h/-/metrics").is_err());
        let jobs = "aspen_jobs{kind=\"compact\",state=\"queued\"} 3\n\
                    aspen_jobs{kind=\"index_commits\",state=\"queued\"} 1\n\
                    aspen_jobs{kind=\"compact\",state=\"running\"} 1\n\
                    aspen_jobs{kind=\"compact\",state=\"dead\"} 7\n";
        assert_eq!(job_counts(jobs), Some((4, 1)));
        assert_eq!(job_counts("foo 1\n"), None);
    }

    #[test]
    fn points_are_sorted_and_checked() {
        let mut o = StaleOpts {
            points: vec![100, 10, 100],
            repeats: 1,
            metrics_url: None,
            settle_timeout: Duration::from_secs(1),
            background_worker: true,
            cleanup: true,
        };
        o.validate().unwrap();
        assert_eq!(o.points, vec![10, 100]);
        assert_eq!(o.max(), 100);
        o.points = vec![0];
        assert!(o.validate().is_err());
    }

    #[test]
    fn queued_jobs_block_settling_only_with_a_worker() {
        // A stalled compactor: jobs queued, none running.
        assert!(!idle(true, 3, 0));
        assert!(idle(false, 3, 0));
        // Drained.
        assert!(idle(true, 0, 0));
        // A job still running is never idle.
        assert!(!idle(true, 0, 1));
        assert!(!idle(false, 3, 1));
    }
}

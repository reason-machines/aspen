//! The `aspen-bench` command. See `bench/README.md`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use aspen_bench::compare::{self, Tolerances};
use aspen_bench::git::{self, Git};
use aspen_bench::results::RunResult;
use aspen_bench::synth::{self, RepoSpec};
use aspen_bench::target::{self, CommonOpts, Setup};
use aspen_bench::workloads::{clone, queue, stale, storm, writers};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "aspen-bench",
    version,
    about = "Load generator for Git HTTP servers: writers, fetch storms, clones, stale-branch fetches"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// N writers push to their own branches; readers measure push→fetch latency.
    Writers {
        #[command(flatten)]
        common: Common,
        /// Concurrent writers, each on its own branch.
        #[arg(long, default_value_t = 4)]
        writers: u32,
        /// Most readers running at once [default: 4 per writer]. Each
        /// acknowledged push gets its own reader until this cap is reached.
        #[arg(long)]
        max_readers: Option<u32>,
        /// Stop pushing after this many seconds.
        #[arg(long)]
        duration_secs: Option<f64>,
        /// Stop each writer after this many pushes.
        #[arg(long)]
        pushes_per_writer: Option<u32>,
        /// Pause between one writer's pushes, in milliseconds.
        #[arg(long, default_value_t = 0)]
        interval_ms: u64,
        /// Bytes of the file each commit rewrites.
        #[arg(long, default_value_t = 512)]
        commit_bytes: usize,
        /// Give up on seeing a pushed tip after this many seconds.
        #[arg(long, default_value_t = 30.0)]
        visibility_timeout_secs: f64,
        /// Leave the benchmark branches on the server.
        #[arg(long)]
        keep_branches: bool,
    },
    /// N clients enqueue changes with Aspen's merge queue and wait for each
    /// to land on main (the server needs ASPEN_MERGE_QUEUE=on).
    Queue {
        #[command(flatten)]
        common: Common,
        /// Concurrent clients.
        #[arg(long, default_value_t = 4)]
        clients: u32,
        /// Length of the measured phase, in seconds.
        #[arg(long, default_value_t = 60.0)]
        duration_secs: f64,
        /// Bytes of the file each commit rewrites.
        #[arg(long, default_value_t = 512)]
        commit_bytes: usize,
        /// Give up waiting for a change's outcome after this many seconds.
        #[arg(long, default_value_t = 60.0)]
        visibility_timeout_secs: f64,
        /// Pause between two polls of the outcomes, in milliseconds.
        #[arg(long, default_value_t = 20)]
        poll_ms: u64,
        /// What changes touch, as DISJOINT,DIR,FILE percentages: a new file
        /// of the client's own, a new file in a directory all clients
        /// share, or a rewrite of a file all clients edit (only these can
        /// conflict).
        #[arg(long, default_value = "70,25,5")]
        overlap: queue::Overlap,
        /// After the run, check every landed commit with stock git (re-merge
        /// with `git merge-tree --write-tree`, compare trees, `git fsck
        /// --strict`); any mismatch fails the run (exit status 3).
        #[arg(long)]
        verify_landed: bool,
    },
    /// M jobs fetch the same new commit at once, for several rounds.
    FetchStorm {
        #[command(flatten)]
        common: Common,
        /// Concurrent fetching jobs per round.
        #[arg(long, default_value_t = 16)]
        jobs: u32,
        #[arg(long, default_value_t = 3)]
        rounds: u32,
        /// `warm`: jobs already have main; `cold`: jobs start empty.
        #[arg(long, value_enum, default_value_t = storm::Mode::Warm)]
        mode: storm::Mode,
        /// Shallow fetches of this depth (`--mode cold --depth 1` is the
        /// `actions/checkout` default).
        #[arg(long)]
        depth: Option<u32>,
        /// Leave the benchmark branches on the server.
        #[arg(long)]
        keep_branches: bool,
    },
    /// Concurrent full clones.
    Clone {
        #[command(flatten)]
        common: Common,
        /// Concurrent cloning clients.
        #[arg(long, default_value_t = 4)]
        concurrency: u32,
        /// Total clones.
        #[arg(long, default_value_t = 8)]
        clones: u32,
    },
    /// A client current on trunk fetches branches created N trunk pushes ago.
    StaleBranch {
        #[command(flatten)]
        common: Common,
        /// Branch ages N, in trunk pushes, comma-separated. The run makes
        /// as many trunk pushes as the largest.
        #[arg(long, value_delimiter = ',', default_value = "10,100")]
        points: Vec<u32>,
        /// Fetches per branch, each from a fresh trunk-current client.
        #[arg(long, default_value_t = 3)]
        repeats: u32,
        /// Aspen's metrics URL (`http://host:port/-/metrics`): record the
        /// planner's visited and loaded versions, chain skips and planning
        /// time per fetch, and wait for background jobs to drain first.
        #[arg(long)]
        metrics_url: Option<String>,
        /// Longest wait for the server's background jobs, in seconds.
        #[arg(long, default_value_t = 1800.0)]
        settle_timeout_secs: f64,
        /// The server runs without a background worker (`aspen dev
        /// --no-background-worker`): accept jobs that stay queued. Without
        /// this flag, any job still queued at the settle timeout fails the run.
        #[arg(long)]
        no_background_worker: bool,
        /// Leave the benchmark branches on the server.
        #[arg(long)]
        keep_branches: bool,
    },
    /// Push the synthetic repository's main to a URL and exit.
    Seed {
        #[command(flatten)]
        common: Common,
    },
    /// Write the synthetic repository to a local bare repository and print its tip.
    Generate {
        #[command(flatten)]
        repo: RepoArgs,
        /// Path of the bare repository to create (must not exist).
        #[arg(long)]
        out: PathBuf,
    },
    /// Print Markdown tables for a campaign directory
    /// (`<group>/<target>/<workload>.json`).
    Report {
        /// The campaign directory.
        dir: PathBuf,
        /// Target whose bytes the others are compared with.
        #[arg(long, default_value = "local")]
        reference: String,
    },
    /// Compare a result against a baseline; exits 1 on a regression.
    Compare {
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        current: PathBuf,
        /// Tolerance rules (JSON) [default: bench/tolerances.json, built in].
        #[arg(long)]
        tolerances: Option<PathBuf>,
        /// Also write the report as JSON here.
        #[arg(long)]
        json_report: Option<PathBuf>,
        /// Compare even if workload or parameters differ.
        #[arg(long)]
        allow_incompatible: bool,
    },
}

#[derive(Debug, Args)]
struct Common {
    /// Repository URL (`http(s)://[user:password@]host/path.git`), or `local`
    /// to start an in-process `git http-backend` server.
    #[arg(long)]
    url: String,
    /// Target name recorded in the result [default: from the URL].
    #[arg(long)]
    label: Option<String>,
    #[command(flatten)]
    repo: RepoArgs,
    /// Git wire protocol version for every client.
    #[arg(long, default_value_t = 2)]
    protocol: u8,
    /// Don't push the synthetic main first (the target already has it).
    #[arg(long)]
    no_seed: bool,
    /// Keep client repositories under this directory instead of a temporary one.
    #[arg(long)]
    work_dir: Option<PathBuf>,
    /// Push each benchmark branch through its branch-scoped URL,
    /// `<url>/-/refs/heads/<branch>`, instead of `--url`. Required against
    /// Aspen, whose plain URL advertises only trunk for pushes. Recorded in
    /// the result as `push_url`.
    #[arg(long)]
    branch_scoped_push: bool,
    /// Write the JSON result here instead of stdout.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Exit with status 3 if the run recorded any error.
    #[arg(long)]
    fail_on_errors: bool,
}

#[derive(Debug, Args)]
struct RepoArgs {
    /// Synthetic repository preset: tiny, small, medium or large.
    #[arg(long, default_value = "small")]
    repo_size: String,
    /// Seed for the synthetic repository's content.
    #[arg(long, default_value_t = 1)]
    repo_seed: u64,
    /// Override the preset's number of files.
    #[arg(long)]
    repo_files: Option<u32>,
    /// Override the preset's approximate file size in bytes.
    #[arg(long)]
    repo_file_bytes: Option<u32>,
    /// Override the preset's number of commits on main.
    #[arg(long)]
    repo_commits: Option<u32>,
    /// Override the preset's files changed per commit.
    #[arg(long)]
    repo_changes_per_commit: Option<u32>,
    /// Use this existing bare repository's `main` as the seed history
    /// instead of a synthetic one (for example a pinned clone of a real
    /// project); the `--repo-*` shape options are then ignored.
    #[arg(long)]
    repo_source: Option<PathBuf>,
}

impl RepoArgs {
    fn spec(&self) -> Result<RepoSpec, String> {
        let base = RepoSpec::preset(&self.repo_size, self.repo_seed).ok_or_else(|| {
            format!(
                "unknown --repo-size {:?}; use tiny, small, medium or large",
                self.repo_size
            )
        })?;
        Ok(RepoSpec {
            files: self.repo_files.unwrap_or(base.files).max(1),
            file_bytes: self.repo_file_bytes.unwrap_or(base.file_bytes),
            commits: self.repo_commits.unwrap_or(base.commits).max(1),
            changes_per_commit: self
                .repo_changes_per_commit
                .unwrap_or(base.changes_per_commit),
            ..base
        })
    }
}

impl Common {
    fn opts(&self) -> Result<CommonOpts, String> {
        Ok(CommonOpts {
            url: self.url.clone(),
            label: self.label.clone(),
            repo: self.repo.spec()?,
            protocol: self.protocol,
            seed: !self.no_seed,
            repo_source: self.repo.repo_source.clone(),
            work_dir: self.work_dir.clone(),
            branch_scoped_push: self.branch_scoped_push,
        })
    }
}

fn secs(s: f64, flag: &str) -> Result<Duration, String> {
    Duration::try_from_secs_f64(s).map_err(|_| format!("{flag} must be a non-negative number"))
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("aspen-bench: {e}");
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<ExitCode, String> {
    match cli.command {
        Command::Writers {
            common,
            writers,
            max_readers,
            duration_secs,
            pushes_per_writer,
            interval_ms,
            commit_bytes,
            visibility_timeout_secs,
            keep_branches,
        } => {
            let opts = writers::WritersOpts {
                writers,
                max_readers,
                duration: duration_secs
                    .map(|s| secs(s, "--duration-secs"))
                    .transpose()?,
                pushes_per_writer,
                interval: Duration::from_millis(interval_ms),
                commit_bytes,
                visibility_timeout: secs(visibility_timeout_secs, "--visibility-timeout-secs")?,
                cleanup: !keep_branches,
            };
            workload(&common, |setup, params| writers::run(setup, &opts, params))
        }
        Command::Queue {
            common,
            clients,
            duration_secs,
            commit_bytes,
            visibility_timeout_secs,
            poll_ms,
            overlap,
            verify_landed,
        } => {
            let opts = queue::QueueOpts {
                clients,
                duration: secs(duration_secs, "--duration-secs")?,
                commit_bytes,
                visibility_timeout: secs(visibility_timeout_secs, "--visibility-timeout-secs")?,
                poll: Duration::from_millis(poll_ms),
                overlap,
                verify_landed,
            };
            workload(&common, |setup, params| queue::run(setup, &opts, params))
        }
        Command::FetchStorm {
            common,
            jobs,
            rounds,
            mode,
            depth,
            keep_branches,
        } => {
            let opts = storm::StormOpts {
                jobs,
                rounds,
                mode,
                depth,
                cleanup: !keep_branches,
            };
            workload(&common, |setup, params| storm::run(setup, &opts, params))
        }
        Command::Clone {
            common,
            concurrency,
            clones,
        } => {
            let opts = clone::CloneOpts {
                concurrency,
                clones,
            };
            workload(&common, |setup, params| clone::run(setup, &opts, params))
        }
        Command::StaleBranch {
            common,
            points,
            repeats,
            metrics_url,
            settle_timeout_secs,
            no_background_worker,
            keep_branches,
        } => {
            let opts = stale::StaleOpts {
                points,
                repeats,
                metrics_url,
                settle_timeout: secs(settle_timeout_secs, "--settle-timeout-secs")?,
                background_worker: !no_background_worker,
                cleanup: !keep_branches,
            };
            workload(&common, |setup, params| stale::run(setup, &opts, params))
        }
        Command::Seed { common } => {
            let mut opts = common.opts()?;
            opts.seed = true;
            let setup = Setup::new(&opts)?;
            println!("{}", setup.main);
            Ok(ExitCode::SUCCESS)
        }
        Command::Generate { repo, out } => {
            let spec = repo.spec()?;
            let home = tempfile::tempdir().map_err(|e| format!("temporary directory: {e}"))?;
            let tip = synth::generate(&Git::new(home.path(), 2), &spec, &out)?;
            println!("{tip}");
            Ok(ExitCode::SUCCESS)
        }
        Command::Report { dir, reference } => {
            let campaign = aspen_bench::report::load(&dir)?;
            print!("{}", aspen_bench::report::render(&campaign, &reference));
            Ok(ExitCode::SUCCESS)
        }
        Command::Compare {
            baseline,
            current,
            tolerances,
            json_report,
            allow_incompatible,
        } => {
            let baseline = RunResult::load(&baseline)?;
            let current = RunResult::load(&current)?;
            let tolerances = match tolerances {
                Some(p) => Tolerances::load(&p)?,
                None => Tolerances::default(),
            };
            let report = compare::compare(&baseline, &current, &tolerances);
            print!("{}", report.render());
            if let Some(p) = json_report {
                let json = serde_json::to_string_pretty(&report).unwrap_or_default();
                std::fs::write(&p, json).map_err(|e| format!("writing {}: {e}", p.display()))?;
            }
            let incompatible = !report.incompatibilities.is_empty() && !allow_incompatible;
            if incompatible {
                eprintln!(
                    "aspen-bench: the runs are not comparable (pass --allow-incompatible to compare anyway)"
                );
            }
            Ok(if report.has_regressions() || incompatible {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            })
        }
    }
}

type Params = BTreeMap<String, serde_json::Value>;

fn workload(
    common: &Common,
    body: impl FnOnce(&Setup, Params) -> Result<RunResult, String>,
) -> Result<ExitCode, String> {
    let opts = common.opts()?;
    if common.url != target::LOCAL {
        // Fail fast with a clear message rather than inside a worker thread.
        git::Remote::parse(&common.url)?;
    }
    let setup = Setup::new(&opts)?;
    let mut params = opts.params();
    if let Some(tip) = &setup.source_tip {
        params.insert("repo_source_tip".to_owned(), tip.clone().into());
    }
    let result = body(&setup, params)?;
    eprint!("{}", result.human_summary());
    let json = result.to_json_pretty();
    match &common.out {
        Some(path) => {
            if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
                std::fs::create_dir_all(dir)
                    .map_err(|e| format!("creating {}: {e}", dir.display()))?;
            }
            std::fs::write(path, json + "\n")
                .map_err(|e| format!("writing {}: {e}", path.display()))?;
            eprintln!("aspen-bench: wrote {}", path.display());
        }
        None => println!("{json}"),
    }
    // A failed verification of what landed fails the run whatever the
    // error policy (`queue --verify-landed`).
    let verify_failed = result.counts.get("verify_failed").copied().unwrap_or(0) > 0;
    Ok(
        if verify_failed || (common.fail_on_errors && result.errors.count > 0) {
            ExitCode::from(3)
        } else {
            ExitCode::SUCCESS
        },
    )
}

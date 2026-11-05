//! `aspen-chaos`: run the chaos suite from the command line.
//!
//! ```text
//! aspen-chaos run --scenario mixed --duration 30m
//! aspen-chaos run --scenario all --epochs 3 --epoch 40s
//! aspen-chaos scenarios
//! ```
//!
//! Exit status: 0 when every invariant held, 1 when one broke, 2 when the
//! suite could not run (a missing binary or service).

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use aspen_chaos::workload::Weights;
use aspen_chaos::{Cluster, ClusterConfig, RunConfig, S3Endpoint, Scenario, Stop, find_aspen_bin};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "aspen-chaos",
    about = "Aspen's chaos suite (docs/PLAN.md section 4, node D1)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Runs one scenario, or all of them in turn.
    Run(RunArgs),
    /// Lists the scenarios.
    Scenarios,
}

#[derive(Debug, clap::Args)]
struct RunArgs {
    /// A scenario name (see `scenarios`), or `all`.
    #[arg(long, default_value = "mixed")]
    scenario: String,
    /// Run each scenario for this long (`90s`, `10m`, `24h`), in whole epochs.
    #[arg(long, value_parser = parse_duration, conflicts_with = "epochs")]
    duration: Option<Duration>,
    /// Run each scenario for this many epochs.
    #[arg(long, default_value_t = 3)]
    epochs: usize,
    /// Length of each epoch's active (faulted) phase.
    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    epoch: Duration,
    /// Length of each epoch's quiet phase before the checks.
    #[arg(long, value_parser = parse_duration, default_value = "3s")]
    quiet: Duration,
    /// Concurrent git clients.
    #[arg(long, default_value_t = 6)]
    clients: usize,
    /// `aspen serve` processes behind the client proxy.
    #[arg(long, default_value_t = 3)]
    instances: usize,
    /// Seed for the fault schedule and the workload's choices (random if
    /// unset). Timing still differs between runs.
    #[arg(long, value_parser = parse_seed)]
    seed: Option<u64>,
    /// Start a fresh repository after this many epochs.
    #[arg(long, default_value_t = 20)]
    repo_epochs: usize,
    /// The linearizability search's budget per ref and epoch, in visited
    /// states; a search that runs out fails the run.
    #[arg(long, default_value_t = aspen_chaos::lin::DEFAULT_BUDGET)]
    lin_budget: usize,
    /// Use the local file store even when ASPEN_TEST_S3_* names a bucket
    /// (the s3, mixed and smoke scenarios then refuse to run).
    #[arg(long)]
    local_blobs: bool,
    /// Where to keep logs, histories and violation dumps (a temporary
    /// directory, kept, if unset).
    #[arg(long)]
    out: Option<PathBuf>,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = num.parse().map_err(|_| format!("not a duration: {s}"))?;
    let secs = match unit {
        "" | "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        _ => return Err(format!("unknown unit in {s}; use s, m or h")),
    };
    Ok(Duration::from_secs(secs))
}

fn parse_seed(s: &str) -> Result<u64, String> {
    match s.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => s.parse(),
    }
    .map_err(|e| format!("bad seed {s}: {e}"))
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Scenarios => {
            for s in Scenario::ALL {
                println!("{:<11} {}", s.name(), s.describe());
            }
            ExitCode::SUCCESS
        }
        Command::Run(args) => match run(&args) {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => ExitCode::from(1),
            Err(e) => {
                eprintln!("aspen-chaos: {e}");
                ExitCode::from(2)
            }
        },
    }
}

fn run(args: &RunArgs) -> Result<bool, String> {
    let scenarios: Vec<Scenario> = if args.scenario == "all" {
        Scenario::ALL.to_vec()
    } else {
        vec![
            Scenario::parse(&args.scenario)
                .ok_or_else(|| format!("no scenario {:?}", args.scenario))?,
        ]
    };
    let aspen_bin = find_aspen_bin().map_err(|e| e.to_string())?;
    let pg_bin = aspen_chaos::pg::find_bin()
        .ok_or("PostgreSQL binaries not found; set ASPEN_TEST_PG_BIN to a directory with initdb, pg_ctl, pg_basebackup and psql")?;
    let s3 = if args.local_blobs {
        None
    } else {
        S3Endpoint::from_env()
            .transpose()
            .map_err(|e| e.to_string())?
    };
    let out = match &args.out {
        Some(p) => p.clone(),
        None => tempfile::Builder::new()
            .prefix("aspen-chaos-")
            .tempdir()
            .map_err(|e| e.to_string())?
            .keep(),
    };
    let seed = args
        .seed
        .unwrap_or_else(aspen_testkit::oracle::seed_from_clock);
    let mut passed = true;
    for scenario in scenarios {
        if scenario.needs_s3() && s3.is_none() {
            return Err(format!(
                "scenario {} needs an S3 store: set ASPEN_TEST_S3_* (crates/aspen-blob/scripts/test-minio.sh)",
                scenario.name()
            ));
        }
        let root = out.join(scenario.name());
        eprintln!(
            "aspen-chaos: {} ({}) with {} instance(s), {} client(s), {} blobs, seed {seed:#x}; logs in {}",
            scenario.name(),
            scenario.describe(),
            args.instances,
            args.clients,
            if s3.is_some() { "S3" } else { "local" },
            root.display()
        );
        let cluster = Cluster::start(ClusterConfig {
            aspen_bin: aspen_bin.clone(),
            pg_bin: pg_bin.clone(),
            root: root.join("deployment"),
            instances: args.instances,
            s3: s3.clone(),
            standby: scenario.needs_standby(),
            seed,
            extra_env: Vec::new(),
        })
        .map_err(|e| format!("starting the deployment: {e}"))?;
        let mut cfg = RunConfig::new(scenario.name(), seed, root.join("run"));
        cfg.clients = args.clients;
        cfg.active = args.epoch;
        cfg.quiet = args.quiet;
        cfg.repo_epochs = args.repo_epochs;
        cfg.verbose = true;
        cfg.unscoped_branch_pushes = true;
        cfg.lin_budget = args.lin_budget;
        cfg.expect_packfile_uris = true;
        cfg.stop = match args.duration {
            Some(d) => Stop::After(d),
            None => Stop::Epochs(args.epochs),
        };
        if scenario == Scenario::Compaction {
            cfg.weights = Weights::COMPACTION;
            cfg.fault_gap = (Duration::from_millis(200), Duration::from_millis(1000));
        }
        if scenario == Scenario::Failover {
            cfg.fault_gap = (Duration::from_secs(4), Duration::from_secs(10));
        }
        let mut injector = scenario.injector(&cluster);
        let report = aspen_chaos::run(&cluster, Some(&cluster), Some(injector.as_mut()), &cfg)
            .map_err(|e| format!("scenario {}: {e}", scenario.name()))?;
        println!("{report}");
        passed &= report.passed();
    }
    Ok(passed)
}

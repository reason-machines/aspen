//! `aspen-testkit`: command-line front end for the test kit.
//!
//! ```text
//! aspen-testkit diff  [--seed N] [--sequences N] [--steps N] ...   run the differential oracle
//! aspen-testkit serve --root DIR [--addr 127.0.0.1:PORT]           serve bare repos via git http-backend
//! ```

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use aspen_testkit::aspen::sibling_binary;
use aspen_testkit::oracle::{seed_from_clock, seeds};
use aspen_testkit::{
    AspenDatabase, AspenOptions, AspenServer, Config, Fault, FaultyServer, GitHttpBackend,
    GitServer, KnownDivergence, Oracle, ScopedServer, StockServer,
};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "aspen-testkit", about = "Aspen test tooling")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the differential oracle: apply seeded random operation sequences to
    /// a reference and a candidate server and compare them after every step.
    Diff(DiffArgs),
    /// Serve the bare repositories under a directory over HTTP through stock
    /// `git http-backend`, until interrupted. Used by the client matrix.
    Serve {
        /// Directory holding bare repositories (`<name>.git`).
        #[arg(long)]
        root: PathBuf,
        /// Address to listen on. The server has no authentication: keep it on
        /// loopback.
        #[arg(long, default_value = "127.0.0.1:0")]
        addr: String,
    },
}

#[derive(Debug, clap::Args)]
struct DiffArgs {
    /// First seed; sequence `i` uses `seed + i`. Defaults to a random seed,
    /// which is printed so the run can be replayed.
    #[arg(long, value_parser = parse_u64)]
    seed: Option<u64>,
    /// How many sequences to run.
    #[arg(long, default_value_t = 10)]
    sequences: usize,
    /// Operations per sequence.
    #[arg(long, default_value_t = 20)]
    steps: usize,
    /// Client clones per sequence.
    #[arg(long, default_value_t = 3)]
    clones: usize,
    /// Branches in play, including `main`.
    #[arg(long, default_value_t = 4)]
    branches: usize,
    /// Allow sequences to delete `main`.
    #[arg(long)]
    delete_trunk: bool,
    /// Also push and race through the plain repository URL, where Aspen
    /// sees a zero old value for every branch but trunk and decides
    /// fast-forwards itself (issue #135). Changes the generated sequences,
    /// so a seed replays only with the same setting. Not for
    /// `--candidate scoped`, whose repository URL refuses hidden refs.
    #[arg(long)]
    plain_url_pushes: bool,
    /// Replays to spend shrinking a divergence (0 turns shrinking off).
    #[arg(long, default_value_t = 40)]
    shrink_budget: usize,
    /// The server to compare against stock `git http-backend`. `stock` and
    /// `scoped` check the oracle itself; the `fault-*` servers must be caught.
    #[arg(long, value_enum, default_value_t = Candidate::Stock)]
    candidate: Candidate,
    /// Print every step (and every resync after a race) as it runs.
    #[arg(long)]
    log_steps: bool,
    /// The `aspen` binary for `--candidate aspen`. Defaults to the one next
    /// to this executable (`cargo build -p aspen-cli -p aspen-testkit`).
    #[arg(long, env = "ASPEN_BIN")]
    aspen_bin: Option<PathBuf>,
    /// With `--candidate aspen`: a PostgreSQL URL (no password; set
    /// `PGPASSWORD`) for Aspen's metadata instead of a fresh SQLite file.
    /// An empty value (flag or variable) is refused, never read as SQLite.
    #[arg(
        long,
        env = "ASPEN_DIFF_DATABASE_URL",
        hide_env_values = true,
        value_parser = clap::builder::NonEmptyStringValueParser::new()
    )]
    aspen_database_url: Option<String>,
    /// Step over a filed divergence instead of stopping at it (repeatable).
    /// Each one matches a single exact pattern and is counted in the report;
    /// see `KnownDivergence` in `src/oracle.rs` for the issue each tracks.
    #[arg(long = "known", value_enum)]
    known: Vec<Known>,
}

/// The values of `--known`, one per `KnownDivergence`. None today (the last
/// one, issue #133, was fixed), so every value is refused.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum Known {}

impl From<Known> for KnownDivergence {
    fn from(k: Known) -> Self {
        match k {}
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Candidate {
    /// A second stock `git http-backend`.
    Stock,
    /// Stock `git http-backend` with Aspen's URL layout: branches hidden
    /// except through their branch-scoped URL.
    Scoped,
    /// A real Aspen: the `aspen` binary running `aspen serve` (SQLite, or
    /// PostgreSQL with `--aspen-database-url`, and a local blob store).
    Aspen,
    /// Loses the ref updates of the 3rd push.
    FaultDropRef,
    /// Deletes an object of the 3rd push.
    FaultLoseObject,
    /// Corrupts an object of the 3rd push.
    FaultCorruptObject,
    /// Rejects the 3rd push.
    FaultRejectPush,
    /// Acknowledges every racing push and keeps the last (no compare-and-swap).
    FaultLastWriterWins,
}

fn parse_u64(s: &str) -> Result<u64, String> {
    let parsed = match s.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => s.parse(),
    };
    parsed.map_err(|e| format!("not a u64: {e}"))
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Command::Diff(args) = &cli.command
        && args.plain_url_pushes
        && matches!(args.candidate, Candidate::Scoped)
    {
        Cli::command()
            .error(
                clap::error::ErrorKind::ArgumentConflict,
                "--plain-url-pushes cannot be used with --candidate scoped: its repository \
                 URL refuses updates of hidden refs (stock receive-pack with transfer.hideRefs)",
            )
            .exit();
    }
    let result = match cli.command {
        Command::Diff(args) => diff(args),
        Command::Serve { root, addr } => serve(root, &addr),
    };
    match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("aspen-testkit: {err}");
            ExitCode::FAILURE
        }
    }
}

fn diff(args: DiffArgs) -> std::io::Result<ExitCode> {
    let config = Config {
        steps: args.steps,
        clones: args.clones,
        branches: args.branches,
        protect_trunk: !args.delete_trunk,
        plain_url_pushes: args.plain_url_pushes,
    };
    let base = args.seed.unwrap_or_else(seed_from_clock);
    let reference = StockServer::start()?;
    let candidate: Box<dyn GitServer> = match args.candidate {
        Candidate::Stock => Box::new(StockServer::start_named("git-http-backend (candidate)")?),
        Candidate::Scoped => Box::new(ScopedServer::start()?),
        Candidate::Aspen => {
            let binary = match args.aspen_bin {
                Some(path) => path,
                None => sibling_binary()?,
            };
            let mut options = AspenOptions::new(binary);
            if let Some(url) = args.aspen_database_url {
                options.database = AspenDatabase::Postgres(url);
            }
            let server = AspenServer::start(options)?;
            println!(
                "diff: aspen serve at {} (files in {})",
                server.base_url(),
                server.dir().display()
            );
            Box::new(server)
        }
        Candidate::FaultDropRef => {
            Box::new(FaultyServer::start(Fault::DropRefUpdate { nth_push: 3 })?)
        }
        Candidate::FaultLoseObject => {
            Box::new(FaultyServer::start(Fault::LoseObject { nth_push: 3 })?)
        }
        Candidate::FaultCorruptObject => {
            Box::new(FaultyServer::start(Fault::CorruptObject { nth_push: 3 })?)
        }
        Candidate::FaultRejectPush => {
            Box::new(FaultyServer::start(Fault::RejectPush { nth_push: 3 })?)
        }
        Candidate::FaultLastWriterWins => Box::new(FaultyServer::start(Fault::LastWriterWins)?),
    };
    println!(
        "{}\ndiff: {} vs {}, seeds {base:#x}..+{}, {} steps x {} clones x {} branches",
        aspen_testkit::git::version()?,
        reference.name(),
        candidate.name(),
        args.sequences,
        config.steps,
        config.clones,
        config.branches,
    );
    let oracle = Oracle::new(&reference, candidate.as_ref(), config)
        .shrink_budget(args.shrink_budget)
        .log_steps(args.log_steps)
        .known_divergences(args.known.iter().map(|&k| k.into()));
    let started = Instant::now();
    let report = oracle.check_seeds(seeds(base, args.sequences))?;
    let elapsed = started.elapsed();
    println!(
        "diff: {} clean sequence(s), {} op(s) in {:.1?}\ndiff: reference outcomes: {}",
        report.clean_sequences, report.clean_steps, elapsed, report.stats
    );
    match report.divergence {
        None => {
            println!("diff: no divergence");
            Ok(ExitCode::SUCCESS)
        }
        Some(divergence) => {
            println!("{divergence}");
            Ok(ExitCode::FAILURE)
        }
    }
}

fn serve(root: PathBuf, addr: &str) -> std::io::Result<ExitCode> {
    let server = GitHttpBackend::bind(addr, &root)?;
    // Scripts read this line to learn the port.
    println!("listening on {}", server.base_url());
    loop {
        std::thread::park();
    }
}

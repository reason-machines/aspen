//! The guards that keep a run from passing without checking anything:
//! each is triggered on purpose here, and the run must fail.
//!
//! - an exhausted linearizability search (`Invariant::Inconclusive`);
//! - an epoch with no acknowledged push, a faulted scenario that injects
//!   nothing or a part of a composite one that never does, an epoch that
//!   reached its limit before what it waits for happened, a fault step
//!   that errors, a client stuck on local errors (`Vacuous`, `Unchecked`);
//! - alert counters that cannot be read, and an instance that exits
//!   without the suite killing it, during the run or before the final
//!   scrape (`Alert`, `Crashed`).

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aspen_chaos::history::History;
use aspen_chaos::report::Tally;
use aspen_chaos::workload::{Client, Keys, LOCAL_ERRORS_IN_A_ROW, Target, Weights, run_git};
use aspen_chaos::{
    ActivePhase, ActiveUntil, Cluster, ClusterConfig, Deployment, Injector, Invariant, Report,
    RunConfig, Scenario, Stop, find_aspen_bin,
};
use aspen_testkit::rng::Rng;
use aspen_testkit::{GitServer, StockServer};

fn config(name: &str, root: &std::path::Path) -> RunConfig {
    let mut cfg = RunConfig::new(name, 0xD1_6A, root.to_path_buf());
    cfg.clients = 3;
    cfg.active = Duration::from_secs(2);
    cfg.quiet = Duration::from_millis(200);
    cfg.stop = Stop::Epochs(1);
    cfg.fault_gap = (Duration::from_millis(100), Duration::from_millis(300));
    cfg.weights = Weights {
        push_trunk: 3,
        push_shared: 4,
        push_own: 1,
        read: 2,
        shallow_fetch: 0,
        uri_fetch: 0,
    };
    cfg
}

fn run_stock(cfg: &RunConfig, injector: Option<&mut dyn Injector>) -> Report {
    let server = StockServer::start().unwrap();
    let report = aspen_chaos::run(&server, None, injector, cfg).unwrap();
    eprintln!("{report}");
    report
}

#[test]
fn an_epoch_that_waits_in_vain_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config("waits-in-vain", dir.path());
    cfg.active = Duration::from_millis(500);
    let asked = Arc::new(AtomicUsize::new(0));
    let counter = asked.clone();
    cfg.active_until = Some(ActiveUntil {
        what: "a condition that never holds held".to_owned(),
        holds: Arc::new(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
            false
        }),
        limit: Duration::from_secs(2),
    });
    let started = Instant::now();
    let report = run_stock(&cfg, None);
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "the epoch ran to its limit"
    );
    assert!(asked.load(Ordering::Relaxed) > 0, "the condition was asked");
    assert!(!report.passed(), "{report}");
    assert!(report.broke(Invariant::Vacuous), "{report}");
    assert!(
        report
            .violations
            .iter()
            .any(|f| f.detail.contains("a condition that never holds held")),
        "{report}"
    );
}

#[test]
fn an_exhausted_linearizability_search_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config("tiny-budget", dir.path());
    cfg.lin_budget = 1;
    let report = run_stock(&cfg, None);
    assert!(!report.passed(), "{report}");
    assert!(report.broke(Invariant::Inconclusive), "{report}");
}

#[test]
fn an_epoch_without_an_acknowledged_push_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config("reads-only", dir.path());
    cfg.weights = Weights {
        push_trunk: 0,
        push_shared: 0,
        push_own: 0,
        read: 1,
        shallow_fetch: 0,
        uri_fetch: 0,
    };
    let report = run_stock(&cfg, None);
    assert!(report.broke(Invariant::Vacuous), "{report}");
}

/// An injector that never finds anything to do, or always fails.
struct Broken {
    fails: bool,
}

impl Injector for Broken {
    fn name(&self) -> &'static str {
        "broken"
    }
    fn step(&mut self, _: &mut Rng) -> io::Result<Option<String>> {
        if self.fails {
            Err(io::Error::other("no such fault"))
        } else {
            Ok(None)
        }
    }
    fn heal(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_scenario_that_injects_nothing_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config("never-injects", dir.path());
    let report = run_stock(&cfg, Some(&mut Broken { fails: false }));
    assert!(report.broke(Invariant::Vacuous), "{report}");
}

/// A composite injector (like `mixed`) one of whose parts never injected.
struct PartlyIdle;

impl Injector for PartlyIdle {
    fn name(&self) -> &'static str {
        "partly idle"
    }
    fn step(&mut self, _: &mut Rng) -> io::Result<Option<String>> {
        Ok(Some("the part that works".into()))
    }
    fn heal(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn idle_parts(&self) -> Vec<&'static str> {
        vec!["compaction kill"]
    }
}

#[test]
fn a_part_that_never_injects_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config("partly-idle", dir.path());
    let report = run_stock(&cfg, Some(&mut PartlyIdle));
    assert!(report.get("fault: the part that works") > 0, "{report}");
    assert!(
        report
            .violations
            .iter()
            .any(|f| f.invariant == Invariant::Vacuous && f.detail.contains("compaction kill")),
        "{report}"
    );
}

/// A client whose own repository cannot take a commit (a directory sits
/// where `fast-import` writes its ref) pushes nothing; after
/// `LOCAL_ERRORS_IN_A_ROW` failures in a row it reports that once.
#[test]
fn a_client_stuck_on_local_errors_is_a_finding() {
    let dir = tempfile::tempdir().unwrap();
    let server = StockServer::start().unwrap();
    let repo_url = server.create_repo("local-errors.git").unwrap();
    let keys = Keys::new(1, 0);
    let seed = dir.path().join("seed");
    std::fs::create_dir_all(&seed).unwrap();
    run_git(&seed, &seed, &["init", "-q", "--initial-branch=main"]).unwrap();
    run_git(
        &seed,
        &seed,
        &["commit", "-q", "--allow-empty", "-m", "first"],
    )
    .unwrap();
    run_git(
        &seed,
        &seed,
        &["push", "-q", &repo_url, "HEAD:refs/heads/main"],
    )
    .unwrap();

    let mut client = Client::new(&dir.path().join("clients"), 0, 1).unwrap();
    // A valid ref at `refs/chaos/work/blocker`, so fetches still work but
    // `refs/chaos/work` cannot be created.
    let home = dir.path().join("clients/client-0");
    let repo = home.join("repo");
    run_git(&home, &repo, &["commit", "-q", "--allow-empty", "-m", "x"]).unwrap();
    run_git(
        &home,
        &repo,
        &["update-ref", "refs/chaos/work/blocker", "HEAD"],
    )
    .unwrap();

    let target = Target {
        server: &server,
        repo_url,
        keys,
        unscoped_branch_pushes: false,
    };
    let weights = Weights {
        push_trunk: 1,
        push_shared: 0,
        push_own: 0,
        read: 0,
        shallow_fetch: 0,
        uri_fetch: 0,
    };
    let (history, findings, tally) = (History::new(), Mutex::new(Vec::new()), Tally::default());
    client.run(
        &target,
        weights,
        &history,
        &findings,
        &tally,
        0,
        &ActivePhase::lasting(Duration::from_secs(3)),
    );
    let mut report = Report::default();
    tally.drain_into(&mut report);
    let findings = findings.into_inner().unwrap();
    assert!(
        report.get("client local errors") >= LOCAL_ERRORS_IN_A_ROW as u64,
        "{report}"
    );
    assert_eq!(findings.len(), 1, "reported once: {findings:?}");
    assert_eq!(findings[0].invariant, Invariant::Unchecked);
    assert!(
        findings[0].detail.contains("in a row") && findings[0].detail.contains("fast-import"),
        "{}",
        findings[0]
    );
    assert!(
        !history
            .ops()
            .iter()
            .any(|o| matches!(o.call, aspen_chaos::history::Call::Push { .. })),
        "no push was attempted"
    );
}

#[test]
fn a_fault_step_that_errors_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config("failing-fault", dir.path());
    let report = run_stock(&cfg, Some(&mut Broken { fails: true }));
    assert!(report.broke(Invariant::Unchecked), "{report}");
}

/// The `calm` scenario, plus one SIGKILL of process `pid` at its first
/// fault step, behind the suite's back (no [`Cluster::kill_instance`], so
/// the suite does not know it happened). A run asks for fault steps only
/// during an epoch's active phase, which starts once the repository is
/// seeded and ends before the epoch heals, so the kill always lands
/// mid-epoch with clients running, however slow the machine is (#241).
struct KillOnFirstStep<'a> {
    calm: Box<dyn Injector + 'a>,
    pid: Option<u32>,
}

impl Injector for KillOnFirstStep<'_> {
    fn name(&self) -> &'static str {
        self.calm.name()
    }
    fn step(&mut self, rng: &mut Rng) -> io::Result<Option<String>> {
        if let Some(pid) = self.pid.take() {
            let killed = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .status()?;
            assert!(killed.success(), "kill -9 {pid}: {killed}");
        }
        self.calm.step(rng)
    }
    fn heal(&mut self) -> io::Result<()> {
        self.calm.heal()
    }
    fn injects(&self) -> bool {
        self.calm.injects()
    }
}

/// Against real `aspen serve`: metrics on a port nobody listens on (so
/// every alert scrape fails), and the instance SIGKILLed from outside the
/// suite mid-epoch. The run must report both, not restart the instance
/// quietly and read the missing counters as zero. Also checks that the
/// `s3` scenario refuses to fault a deployment without S3.
#[test]
fn unreadable_alerts_and_an_unexpected_exit_fail_the_run() {
    let Some(pg_bin) = aspen_chaos::pg::find_bin() else {
        assert!(
            std::env::var_os("CI").is_none(),
            "PostgreSQL binaries not found; set ASPEN_TEST_PG_BIN"
        );
        eprintln!("skipping: PostgreSQL binaries not found (set ASPEN_TEST_PG_BIN)");
        return;
    };
    let dir = tempfile::Builder::new()
        .prefix("aspen-chaos-guards-")
        .tempdir()
        .unwrap();
    let cluster = Cluster::start(ClusterConfig {
        aspen_bin: find_aspen_bin().unwrap(),
        pg_bin,
        root: dir.path().join("deployment"),
        instances: 1,
        s3: None,
        standby: false,
        seed: 0xD1_6B,
        // Aspen's metrics go to a port the suite does not scrape.
        extra_env: vec![("ASPEN_METRICS_LISTEN".into(), "127.0.0.1:0".into())],
    })
    .unwrap();
    let mut cfg = config("guards", &dir.path().join("run"));
    cfg.active = Duration::from_secs(4);
    // Killed once the epoch is under way, not at a fixed delay: seeding
    // the repository can take longer than any delay on a loaded machine,
    // and a kill during seeding fails the run before it starts (#241).
    let mut injector = KillOnFirstStep {
        calm: Scenario::Calm.injector(&cluster),
        pid: Some(cluster.pid(0).expect("instance 0 runs")),
    };
    let report = aspen_chaos::run(&cluster, Some(&cluster), Some(&mut injector), &cfg).unwrap();
    eprintln!("{report}");
    assert!(injector.pid.is_none(), "the epoch never took a fault step");
    assert!(report.broke(Invariant::Crashed), "{report}");
    assert!(report.broke(Invariant::Alert), "{report}");

    // An instance that dies after the last heal (a panic while the final
    // checks drain the job queue) is down at the final scrape: that is a
    // crash, not an instance with nothing to read. Reported once.
    let pid = cluster
        .pid(0)
        .expect("final_checks brought instance 0 back");
    let killed = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let deadline = Instant::now() + Duration::from_secs(10);
    while cluster.is_running(0) {
        assert!(Instant::now() < deadline, "pid {pid} did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = cluster.alerts();
    let found = cluster.take_findings(usize::MAX);
    assert!(
        found
            .iter()
            .any(|f| f.invariant == Invariant::Crashed && f.detail.contains("final")),
        "{found:?}"
    );
    cluster.heal().unwrap();
    let again = cluster.take_findings(usize::MAX);
    assert!(
        !again.iter().any(|f| f.invariant == Invariant::Crashed),
        "the exit was reported once: {again:?}"
    );

    let mut s3 = Scenario::S3.injector(&cluster);
    assert!(
        s3.step(&mut Rng::new(1)).is_err(),
        "S3 faults without S3 must fail, not do nothing"
    );
}

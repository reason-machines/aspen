//! The chaos checks against servers whose behaviour is known: stock
//! `git http-backend` must pass, and testkit's deliberately broken servers
//! must each fail the invariant they break. This is what makes a green
//! chaos run against Aspen mean something.
//!
//! A broken server's run lasts until its fault has fired, not a fixed
//! time: a fault armed for the 10th push must fire even on a machine so
//! busy that fewer pushes fit into the usual window (#163). And each test
//! checks that its fault fired before it checks that the fault was caught.

use std::sync::Arc;
use std::time::Duration;

use aspen_chaos::workload::Weights;
use aspen_chaos::{ActiveUntil, Invariant, Report, RunConfig, Stop};
use aspen_testkit::{Fault, FaultyServer, GitServer, ScopedServer, StockServer};

fn config(name: &str, root: &std::path::Path, epochs: usize) -> RunConfig {
    let mut cfg = RunConfig::new(name, 0xD1, root.to_path_buf());
    cfg.clients = 3;
    cfg.active = Duration::from_secs(3);
    cfg.quiet = Duration::from_millis(200);
    cfg.stop = Stop::Epochs(epochs);
    // Contended pushes and reads only: the quickest way to a race.
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

/// The longest a broken server's epoch may run while waiting for its
/// fault to fire. Unloaded, it fires within the first 3 seconds.
const FAULT_LIMIT: Duration = Duration::from_secs(180);

/// Two epochs for a server that must pass (so carried-over state is
/// exercised).
fn run(server: &dyn GitServer, name: &str, epochs: usize) -> Report {
    let dir = tempfile::tempdir().unwrap();
    run_with(server, &config(name, dir.path(), epochs))
}

fn run_with(server: &dyn GitServer, cfg: &RunConfig) -> Report {
    let report = aspen_chaos::run(server, None, None, cfg).unwrap();
    eprintln!("{report}");
    assert!(report.get("push ok") > 0, "the workload pushed: {report}");
    report
}

/// One epoch against a server that must fail, lasting until its fault has
/// fired. Fails unless the fault fired, so a test that goes on to check
/// the fault was caught cannot pass without it.
fn run_faulty(server: &FaultyServer, name: &str) -> Report {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(name, dir.path(), 1);
    let firings = server.firings();
    cfg.active_until = Some(ActiveUntil {
        what: format!("the fault of {} fired", server.name()),
        holds: Arc::new(move |_| firings.count() > 0),
        limit: FAULT_LIMIT,
    });
    let report = run_with(server, &cfg);
    let fired = server.fired();
    eprintln!("{}: the fault fired {fired} time(s)", server.name());
    assert!(
        fired > 0,
        "{}'s fault never fired, so this run cannot show that the checks catch it: {report}",
        server.name()
    );
    report
}

#[test]
fn stock_git_passes_every_check() {
    let server = StockServer::start().unwrap();
    let report = run(&server, "stock", 2);
    assert!(report.passed(), "{report}");
    assert!(
        report.get("push fail") > 0,
        "some pushes lost a race: {report}"
    );
}

#[test]
fn stock_git_in_aspens_url_layout_passes_every_check() {
    let server = ScopedServer::start().unwrap();
    let report = run(&server, "scoped", 2);
    assert!(report.passed(), "{report}");
}

#[test]
fn a_server_that_lets_the_last_writer_win_is_caught() {
    let server = FaultyServer::start(Fault::LastWriterWins).unwrap();
    let report = run_faulty(&server, "last-writer-wins");
    assert!(
        report.broke(Invariant::NotLinearizable) && report.broke(Invariant::AckedPushLost),
        "{report}"
    );
}

#[test]
fn a_server_that_drops_an_acknowledged_update_is_caught() {
    let server = FaultyServer::start(Fault::DropRefUpdate { nth_push: 10 }).unwrap();
    let report = run_faulty(&server, "drop-ref-update");
    assert!(report.broke(Invariant::AckedPushLost), "{report}");
}

#[test]
fn a_server_that_loses_an_object_is_caught() {
    let server = FaultyServer::start(Fault::LoseObject { nth_push: 10 }).unwrap();
    let report = run_faulty(&server, "lose-object");
    assert!(report.broke(Invariant::Unfetchable), "{report}");
}

//! The chaos smoke run against real `aspen serve` processes: two
//! instances on Postgres with a synchronous standby, S3 when
//! `ASPEN_TEST_S3_*` names a bucket and the local file store otherwise,
//! and each fault but the compaction kill in turn (`smoke` scenario).
//! Each epoch runs until Aspen has acknowledged [`ACKED_PER_EPOCH`]
//! pushes, not for a fixed time: how many fit into a few seconds of
//! faults depends on the machine (#197). Under `CI` the S3 settings
//! are required, so a green run there went through MinIO. Required CI runs
//! this; the long scenarios run nightly through the `aspen-chaos` binary.
//!
//! Needs the PostgreSQL binaries (`ASPEN_TEST_PG_BIN`, or the runner's
//! `/usr/lib/postgresql/<n>/bin`): without them it skips locally and
//! fails under CI.

use std::time::Duration;

use aspen_chaos::workload::Weights;
use aspen_chaos::{
    ActiveUntil, Cluster, ClusterConfig, RunConfig, S3Endpoint, Scenario, Stop, find_aspen_bin,
};

const EPOCHS: usize = 2;

/// Acknowledged pushes each epoch must check before it ends: enough for
/// its linearizability and durability checks to mean something.
const ACKED_PER_EPOCH: u64 = 10;

/// The longest an epoch's active phase waits for them; an epoch that
/// reaches it fails the run as vacuous. With a fixed 6-second phase, a
/// debug build on 2 CPUs acknowledged 18 to 34 pushes in two epochs
/// (#197), so 60 s leaves a wide margin for a healthy server.
const ACKED_LIMIT: Duration = Duration::from_secs(60);

#[test]
fn aspen_keeps_every_invariant_under_each_fault() {
    let Some(pg_bin) = aspen_chaos::pg::find_bin() else {
        assert!(
            std::env::var_os("CI").is_none(),
            "PostgreSQL binaries not found; set ASPEN_TEST_PG_BIN"
        );
        eprintln!("skipping: PostgreSQL binaries not found (set ASPEN_TEST_PG_BIN)");
        return;
    };
    let s3 = S3Endpoint::from_env().transpose().unwrap();
    // CI starts MinIO for this job (crates/aspen-blob/scripts/test-minio.sh);
    // there the run must go through S3, not quietly use the local store.
    assert!(
        s3.is_some() || std::env::var_os("CI").is_none(),
        "ASPEN_TEST_S3_ENDPOINT is not set; under CI the chaos smoke needs S3"
    );
    let dir = tempfile::Builder::new()
        .prefix("aspen-chaos-smoke-")
        .tempdir()
        .unwrap();
    let seed = 0xD1_5E_ED;
    let cluster = Cluster::start(ClusterConfig {
        aspen_bin: find_aspen_bin().unwrap(),
        pg_bin,
        root: dir.path().join("deployment"),
        instances: 2,
        s3: s3.clone(),
        standby: true,
        seed,
        extra_env: Vec::new(),
    })
    .unwrap();
    let mut cfg = RunConfig::new("smoke", seed, dir.path().join("run"));
    cfg.clients = 3;
    cfg.active = Duration::from_secs(6);
    cfg.active_until = Some(ActiveUntil::acked_pushes(ACKED_PER_EPOCH, ACKED_LIMIT));
    cfg.quiet = Duration::from_secs(1);
    cfg.stop = Stop::Epochs(EPOCHS);
    cfg.fault_gap = (Duration::from_millis(300), Duration::from_millis(800));
    cfg.unscoped_branch_pushes = true;
    cfg.expect_packfile_uris = true;
    cfg.weights = Weights {
        shallow_fetch: 1,
        uri_fetch: 1,
        ..Weights::DEFAULT
    };
    let mut injector = Scenario::Smoke.injector(&cluster);
    let report = aspen_chaos::run(&cluster, Some(&cluster), Some(injector.as_mut()), &cfg).unwrap();
    eprintln!("{report}");
    if !report.passed() {
        // Keep the logs for the failure report.
        let kept = dir.keep();
        panic!(
            "chaos smoke failed; logs and histories in {}\n{report}",
            kept.display()
        );
    }
    // Each epoch waited for its pushes, and one that ran out of time
    // failed the run above; this checks that the wait worked.
    assert!(
        report.get("push ok") >= ACKED_PER_EPOCH * EPOCHS as u64,
        "too little work to mean anything:\n{report}"
    );
    let faults: u64 = report
        .counts
        .iter()
        .filter(|(k, _)| k.starts_with("fault: "))
        .map(|(_, v)| v)
        .sum();
    assert!(faults >= 4, "too few faults injected:\n{report}");
    assert!(report.get("fault: postgres failover") >= 1, "{report}");
    assert!(
        report.get("instance starts") > 2,
        "an instance was killed and restarted:\n{report}"
    );
    if s3.is_some() {
        assert!(report.get("s3: requests") > 0, "{report}");
    }
}

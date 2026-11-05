//! Aspen's chaos suite (node D1 in `docs/PLAN.md`).
//!
//! It runs real `aspen serve` processes against Postgres and an
//! S3-compatible store, drives them with stock `git` clients, injects
//! faults, and checks the three invariants of section 4 of the plan:
//!
//! - an acknowledged push is never lost;
//! - no ref ever points at objects that cannot be fetched;
//! - each ref's history is linearizable (a compare-and-swap register).
//!
//! The suite is black-box: it talks to Aspen only over Git's HTTP protocol,
//! and uses `psql` to read the job queue and to end job leases (a killed
//! instance's, and live workers' if the final queue drain stalls). It works
//! against any [`aspen_testkit::GitServer`], which is how its own tests
//! prove that the checks catch real bugs (stock `git http-backend` with
//! testkit's faults: a lost update, a dropped ref update, a lost object).
//!
//! See `tests/chaos/README.md` for scenarios, how to run them and how to
//! read a failure.

pub mod checks;
pub mod cluster;
pub mod faults;
pub mod history;
pub mod lin;
pub mod pg;
pub mod process;
pub mod proxy;
pub mod report;
pub mod run;
pub mod s3proxy;
pub mod workload;

pub use cluster::{Cluster, ClusterConfig, S3Endpoint, find_aspen_bin};
pub use faults::{Injector, Scenario};
pub use report::{Finding, Invariant, Report};
pub use run::{ActivePhase, ActiveUntil, Deployment, RunConfig, Stop, run};

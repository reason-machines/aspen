//! `aspen-bench`: a load generator for Git HTTP servers.
//!
//! It drives stock `git` clients against any Git HTTP repository URL (Aspen,
//! `git http-backend`, Gitea, Forgejo, GitLab, ...) and writes one JSON
//! result per run. The workloads follow section 4.6 of `docs/PLAN.md`:
//!
//! - [`workloads::writers`]: OpenAI's simulated workload, N writers on
//!   distinct branches, measuring push→fetch latency;
//! - [`workloads::storm`]: a fetch storm, M jobs fetching one commit at once;
//! - [`workloads::clone`]: clone throughput;
//! - [`workloads::queue`]: merge-queue landings, N clients enqueueing
//!   changes and waiting for each to land on trunk;
//! - [`workloads::stale`]: the stale-branch fetch, a client current on trunk
//!   fetching a branch created N trunk pushes ago.
//!
//! [`compare`] checks a run against a stored baseline; [`report`] turns a
//! campaign directory into Markdown tables. See `bench/README.md`
//! for usage and for reproducing the baselines.

pub mod compare;
pub mod git;
pub mod report;
pub mod results;
pub mod stats;
pub mod synth;
pub mod target;
pub mod workloads;

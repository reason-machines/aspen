//! Test tooling for Aspen.
//!
//! - [`git`]: isolated `git` invocations, so tests ignore the machine's Git
//!   configuration and behave the same everywhere.
//! - [`GitHttpBackend`]: a reference Git HTTP server that hands each request
//!   to stock `git http-backend`.
//! - [`GitServer`]: the "server under test" abstraction. [`StockServer`] is
//!   the reference, [`ScopedServer`] is stock Git in Aspen's branch-scoped
//!   URL layout, and [`AspenServer`] is a real Aspen: the `aspen` binary
//!   running `aspen serve` (node L0 in `docs/PLAN.md`).
//! - [`Oracle`]: the differential oracle from section 4.1 of the plan. It
//!   applies seeded random operation sequences to two servers and compares
//!   them after every step.
//! - [`FaultyServer`]: deliberately broken servers that prove the oracle
//!   catches real bugs.
//!
//! The crate is tooling: product crates may use it only as a
//! dev-dependency (`cargo xtask check-deps` lists it among the tooling
//! crates and rejects a normal dependency on it from a product crate).

pub mod aspen;
pub mod fault;
pub mod git;
pub mod http_backend;
pub mod ops;
pub mod oracle;
pub mod rng;
pub mod server;
mod world;

pub use aspen::{AspenDatabase, AspenOptions, AspenServer};
pub use fault::{Fault, FaultyServer, Firings};
pub use git::{GitClient, NO_AUTO_MAINTENANCE, init_bare, isolated_git};
pub use http_backend::{GitHttpBackend, RefVisibility};
pub use ops::{Branch, Config, Op};
pub use oracle::{Divergence, DivergenceKind, KnownDivergence, Oracle, Report, Run, Stats, Step};
pub use server::{GitServer, RefMap, ScopedServer, StockServer};
pub use world::{Outcome, RACE_LOSS_REASONS};

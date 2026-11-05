//! Shared types and traits for Aspen.
//!
//! This crate is the bottom of the dependency graph: it depends on no other
//! Aspen crate (`cargo xtask check-deps` enforces this) and holds no I/O.
//! Every Wave 1 crate builds against these contracts:
//!
//! | Module | Contract | Implemented by |
//! |---|---|---|
//! | [`id`], [`name`] | Object IDs, pack checksums, row IDs, ref and repository names | everyone |
//! | [`repo`] | Repository settings: trunk, force policy, writable namespaces | `aspen-meta` |
//! | [`refs`] | Ref versions, push commands and options, the commit check | `aspen-server` (receive-pack) |
//! | [`meta`] | [`meta::MetaStore`]: refs compare-and-swap, packs, commit index, covers, jobs | `aspen-meta` |
//! | [`blob`] | [`blob::BlobStore`]: upload a local file, ranged reads, presigned URLs | `aspen-blob` |
//! | [`pack`] | Pack rows, blob keys, the per-entry table, pack invariants | `aspen-pack` |
//! | [`ingest`] | Base lookup, prefetch, object location, the completion sequence | `aspen-pack` |
//! | [`fetch`] | Fetch request, [`fetch::Planner`] inputs and outputs, URI rules | `aspen-planner`, `aspen-server` |
//! | [`auth`] | Credentials and principals (HTTP and SSH decide access on `aspen_auth::Identity`) | `aspen-auth` |
//! | [`error`] | Error enums and their single mapping to what a Git client sees | everyone |
//!
//! `docs/PLAN.md` (section 2) explains the architecture these contracts
//! encode; each spike's README under `bench/spikes/` holds the evidence.
//!
//! Async contracts return `impl Future + Send` and are implemented with
//! `async fn`; composition roots (`aspen-server`, `aspen-cli`) use them
//! through generics rather than trait objects.

#![deny(missing_docs)]

pub mod auth;
pub mod blob;
pub mod error;
pub mod fetch;
pub mod id;
pub mod ingest;
pub mod meta;
pub mod name;
pub mod pack;
pub mod refs;
pub mod repo;

/// Aspen's version, as recorded in the workspace manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

//! Aspen's metadata store.
//!
//! [`SqlStore`] implements [`aspen_core::meta::MetaStore`] on PostgreSQL
//! (production and `docker compose`) and on SQLite (`aspen dev`), with
//! schema v1 from `migrations/`. The table reference is `SCHEMA.md` next to
//! this crate's manifest. Migration 0002 adds the `tokens` table for API
//! tokens ([`TokenRow`], node B3), 0003 the `mirrors` and `mirror_refs`
//! tables for mirror mode ([`MirrorRow`], [`MirrorRefRow`], node C4) and
//! 0004 the `ssh_keys` table for SSH keys ([`SshKeyRow`], node C3); none
//! changes schema v1. Like the token methods, the mirror methods are
//! inherent [`SqlStore`] methods, used through `aspen-server`'s adapter
//! for `aspen_mirror::MirrorStore`.
//! Every migration's version prefix is unique within its dialect and both
//! dialects have the same versions (`tests/schema.rs`).
//!
//! Migration 0005 (node C1) adds cover chains (the verified dependency
//! chains that let a fetch skip a cover's range on the have side), the
//! registration times, tombstones and pack generations that garbage
//! collection of orphaned packs relies on, and the indexes the sweep needs;
//! of the schema v1 tables it only adds `packs.generation` (default 0, so
//! every existing row keeps its keys). `src/gc.rs` explains why the sweep never deletes a pack
//! that a ref version, cover, job or shallow pack recording names.
//!
//! Migration 0006 (node C8) adds `shallow_packs`, which records the exact
//! pack of a shallow fetch of one ref version at one depth
//! ([`aspen_core::meta::MetaStore::set_shallow_pack`]); it changes no
//! earlier table. `src/shallow.rs` explains how a recording keeps its pack
//! from the sweep and how eviction hands it back.
//!
//! Migration 0007 (issue #108) adds `shallow_packs.used_at_us`, the last
//! time a fetch used a recording, so garbage collection evicts recordings
//! nobody fetches and each repository's recorded packs stay under a byte
//! cap, least recently used first (`src/shallow.rs`).
//!
//! # One implementation, two dialects
//!
//! Both dialects run the same code through sqlx's `Any` driver. The schema
//! files define the same tables, columns and constraint names, so almost
//! every statement is identical; the few places that differ are explicit
//! [`Dialect`] branches:
//!
//! - **Write transactions.** On SQLite every transaction that writes starts
//!   with `BEGIN IMMEDIATE`, which takes the write lock up front and waits
//!   for it (the busy timeout). A deferred transaction that reads first and
//!   writes later can instead fail with `SQLITE_BUSY` when another writer
//!   commits in between, which would turn a lost race into a storage error.
//!   Postgres uses an ordinary `BEGIN` (read committed).
//! - **Row locks.** Claiming a job locks its row with
//!   `FOR UPDATE SKIP LOCKED` on Postgres, so concurrent workers pass over
//!   each other's candidates; SQLite has one writer at a time anyway.
//! - **Single-statement writes.** On Postgres a push's commit of one ref
//!   (compare-and-swap, version, commit rows, jobs) and most pack
//!   registrations are each one statement with data-modifying `WITH`
//!   clauses, which run without `BEGIN` and `COMMIT`, and need no
//!   savepoint inside a group-commit batch (`src/push.rs`, `src/gc.rs`).
//!   SQLite cannot write in a `WITH` clause and runs the same writes as
//!   several statements in a transaction.
//! - **Constraint names in errors.** Postgres names the violated
//!   constraint; SQLite reports a unique violation by table and columns
//!   (`UNIQUE constraint failed: ref_versions.repo_id, ...`). The store
//!   maps both to the same constraint, so a lost compare-and-swap is
//!   [`aspen_core::meta::CommitOutcome::CasLost`] on both, never a storage
//!   error.
//!
//! # Connecting
//!
//! [`SqlStore::connect`] takes a `postgres://` or `sqlite:` URL. Postgres
//! connections follow the deploy contract (`deploy/README.md`): the URL
//! carries no password, which comes from `PGPASSWORD`, and `sslmode=require`
//! is honoured over rustls (no OpenSSL), as RDS with `rds.force_ssl = 1`
//! needs. `tests/tls.rs` checks both against a server that refuses
//! plaintext.
//!
//! Every pooled Postgres connection runs with a `lock_timeout` and a
//! `statement_timeout` ([`StoreOptions`]; 10 s and 60 s by default), and
//! SQLite with a busy timeout equal to the lock timeout. Either limit
//! firing is [`aspen_core::error::StoreError::Unavailable`], the retryable
//! kind. Migrations run on a separate connection without limits.
//!
//! # Ending transactions
//!
//! Every write runs inside one helper that owns its transaction from
//! `BEGIN` to `COMMIT` or `ROLLBACK`, and waits for the rollback before it
//! returns an error. A transaction is never left for sqlx to end when it is
//! dropped: sqlx only queues that rollback, so the locks would outlive the
//! failed request. If a write future is cancelled or panics, its
//! transaction guard spawns the rollback at once (`src/tx.rs`).
//!
//! # Time
//!
//! Three clocks are involved, and the caller chooses one of them:
//!
//! - **The caller's `now`.** Events and rows that carry a time use it as
//!   given: [`aspen_core::meta::JobEvent::Renew`] and
//!   [`aspen_core::meta::JobEvent::Fail`] (the new lease end, or the
//!   backoff before a retry), and `run_after` of
//!   [`aspen_core::meta::NewJob`] and of the jobs a push enqueues.
//! - **The store's clock** ([`SqlStore::with_clock`], by default the
//!   process's system clock). [`aspen_core::meta::MetaStore::claim_job`]
//!   reads it, once the claim's transaction has started, to choose due jobs
//!   and expired leases and to set the new lease's end;
//!   [`SqlStore::requeue_dead_job`] uses it as the new job's `run_after`;
//!   [`aspen_core::meta::MetaStore::register_pack`] stamps each pack's
//!   registration with it and
//!   [`aspen_core::meta::MetaStore::sweep_orphan_packs`] measures the grace
//!   period against it, and [`aspen_core::meta::MetaStore::acquire_lease`]
//!   a lease's expiry.
//!   [`aspen_core::meta::MetaStore::set_shallow_pack`] stamps each
//!   recording with it and
//!   [`aspen_core::meta::MetaStore::evict_shallow_packs`] measures a
//!   recording's age against it.
//!   Workers on different hosts must keep their clocks in sync (NTP): a
//!   skew shortens or lengthens leases by the skew.
//! - **The database's clock.** Only row creation times (`created_at_us`)
//!   come from it.
//!
//! Both dialects define the same tables, columns and constraints; the
//! crate's tests apply each one to a real database and check that, and run
//! the same store test suite against both.

#![deny(missing_docs)]

use sqlx::migrate::Migrator;

mod admission;
mod conn;
mod covers;
mod error;
mod gc;
mod jobs;
mod lease;
mod mirrors;
mod push;
mod ref_changes;
mod rows;
mod shallow;
mod sql;
mod ssh_keys;
mod stats;
mod store;
mod tokens;
mod tx;

pub use gc::{SWEEP_AUDIT_SQL, SWEEP_WALK_SQL};
pub use mirrors::{ConflictRow, MirrorRefRow, MirrorRefState, MirrorRow};
pub use ssh_keys::SshKeyRow;
pub use stats::{JobCount, OpenJobState, TrunkClosures};
pub use store::{Clock, Dialect, MAX_ACQUIRE_TIMEOUT, SqlStore, StoreOptions};
pub use tokens::TokenRow;

/// Schema migrations for PostgreSQL (production and `docker compose`).
pub static POSTGRES_MIGRATIONS: Migrator = sqlx::migrate!("migrations/postgres");

/// Schema migrations for SQLite (`aspen dev`).
pub static SQLITE_MIGRATIONS: Migrator = sqlx::migrate!("migrations/sqlite");

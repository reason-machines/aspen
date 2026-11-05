//! Interop test support: run stock `git` clients against a Git HTTP server.
//!
//! Until Aspen can serve Git (nodes B1 and B2 in `docs/PLAN.md`), the smoke
//! test runs against [`GitHttpBackend`], a tiny HTTP server that hands every
//! request to stock `git http-backend` over CGI. When Aspen serves Git, the
//! same scenario runs against it as well.
//!
//! The harness itself lives in `aspen-testkit`, shared with the differential
//! oracle; this crate re-exports what the interop tests use.

pub use aspen_testkit::{GitHttpBackend, init_bare, isolated_git};

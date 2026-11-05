//! Mirror counters, rendered in the Prometheus text format; `aspen-server`
//! adds them to `/-/metrics`. A dependency-free stand-in until node C5,
//! like the fetch counters; the names are meant to survive that move.
//!
//! `aspen_alert_mirror_conflicts` is the one that pages: a ref in conflict
//! needs an operator (`aspen mirror resolve`). A mirror whose passes keep
//! failing shows in `aspen_mirror_sync_passes_total{result="failed"}`, and a
//! `mirror_push` job that keeps failing ends as a dead job (C5 pages on
//! those).

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counters for one process.
#[derive(Debug, Default)]
pub struct Metrics {
    passes_ok: AtomicU64,
    passes_failed: AtomicU64,
    imports: AtomicU64,
    pushes: AtomicU64,
    step_failures: AtomicU64,
    conflicts_found: AtomicU64,
    conflicts_open: AtomicU64,
    forwarded_accepted: AtomicU64,
    forwarded_rejected: AtomicU64,
    forwarded_ahead: AtomicU64,
    forwarded_unavailable: AtomicU64,
    forwarded_not_mirror: AtomicU64,
    ledger_write_failures: AtomicU64,
}

/// The upstream's answer to one forwarded ref update (proxy mode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Forwarded {
    /// The upstream took it.
    Accepted,
    /// The upstream refused it; its reason.
    Rejected(String),
    /// The upstream's value is not Aspen's: the mirror is behind, or the ref
    /// changed at the upstream. Not sent; a sync is started.
    Ahead,
    /// The upstream could not be reached, or failed; the reason.
    Unavailable(String),
    /// The repository stopped being a mirror after the push started, so
    /// nothing was forwarded. Aspen decides the update alone, as for any
    /// repository; it is not an upstream acceptance.
    NotMirror,
}

fn add(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

impl Metrics {
    pub(crate) fn pass(&self, ok: bool) {
        add(if ok {
            &self.passes_ok
        } else {
            &self.passes_failed
        });
    }

    pub(crate) fn imported(&self) {
        add(&self.imports);
    }

    pub(crate) fn pushed(&self) {
        add(&self.pushes);
    }

    pub(crate) fn step_failed(&self) {
        add(&self.step_failures);
    }

    pub(crate) fn conflict_found(&self) {
        add(&self.conflicts_found);
    }

    pub(crate) fn ledger_write_failed(&self) {
        add(&self.ledger_write_failures);
    }

    pub(crate) fn set_open_conflicts(&self, n: u64) {
        self.conflicts_open.store(n, Ordering::Relaxed);
    }

    pub(crate) fn forwarded(&self, f: &Forwarded) {
        add(match f {
            Forwarded::Accepted => &self.forwarded_accepted,
            Forwarded::Rejected(_) => &self.forwarded_rejected,
            Forwarded::Ahead => &self.forwarded_ahead,
            Forwarded::Unavailable(_) => &self.forwarded_unavailable,
            Forwarded::NotMirror => &self.forwarded_not_mirror,
        });
    }

    /// Refs in conflict over every mirror, as of the last pass.
    pub fn open_conflicts(&self) -> u64 {
        self.conflicts_open.load(Ordering::Relaxed)
    }

    /// Appends the counters in the Prometheus text format.
    pub fn render(&self, out: &mut String) {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let mut counter = |name: &str, help: &str, rows: &[(&str, u64)]| {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            for (labels, v) in rows {
                let _ = writeln!(out, "{name}{labels} {v}");
            }
        };
        counter(
            "aspen_mirror_sync_passes_total",
            "Mirror sync passes, by result.",
            &[
                ("{result=\"ok\"}", get(&self.passes_ok)),
                ("{result=\"failed\"}", get(&self.passes_failed)),
            ],
        );
        counter(
            "aspen_mirror_imports_total",
            "Ref updates imported from upstreams.",
            &[("", get(&self.imports))],
        );
        counter(
            "aspen_mirror_upstream_pushes_total",
            "Ref updates pushed to upstreams in the background (async mode).",
            &[("", get(&self.pushes))],
        );
        counter(
            "aspen_mirror_step_failures_total",
            "Ref sync steps that failed (refused or unreachable); retried by the next pass.",
            &[("", get(&self.step_failures))],
        );
        counter(
            "aspen_mirror_conflicts_found_total",
            "Refs found changed on both sides since they last agreed (async mode).",
            &[("", get(&self.conflicts_found))],
        );
        counter(
            "aspen_mirror_forwarded_total",
            "Ref updates forwarded to upstreams by proxy-mode pushes, by outcome.",
            &[
                ("{result=\"accepted\"}", get(&self.forwarded_accepted)),
                ("{result=\"rejected\"}", get(&self.forwarded_rejected)),
                ("{result=\"ahead\"}", get(&self.forwarded_ahead)),
                ("{result=\"unavailable\"}", get(&self.forwarded_unavailable)),
                ("{result=\"not_mirror\"}", get(&self.forwarded_not_mirror)),
            ],
        );
        counter(
            "aspen_mirror_ledger_write_failures_total",
            "Updates an upstream accepted whose ledger record failed; the next pass records them.",
            &[("", get(&self.ledger_write_failures))],
        );
        let _ = writeln!(
            out,
            "# HELP aspen_alert_mirror_conflicts Refs in conflict over every mirror; each needs `aspen mirror resolve`."
        );
        let _ = writeln!(out, "# TYPE aspen_alert_mirror_conflicts gauge");
        let _ = writeln!(
            out,
            "aspen_alert_mirror_conflicts {}",
            get(&self.conflicts_open)
        );
    }
}

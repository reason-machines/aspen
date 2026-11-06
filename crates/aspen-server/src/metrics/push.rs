//! Push counters (the receive-pack service, node B1), owned by
//! [`crate::receive::ReceivePack`] and rendered beside the fetch families.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aspen_core::error::RejectReason;
use aspen_core::refs::Closure;

use super::expo::{self, DURATION_MICROS, Histogram, Kind, MICROS_PER_SECOND};
use crate::receive::Timings;

/// How a push request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PushResult {
    /// A lone flush (git's probe before a large push); not a push.
    #[default]
    Probe,
    /// The request did not parse; answered with a fatal error.
    Fatal,
    /// The pack was refused (`unpack error`); no ref was updated.
    UnpackFailed,
    /// The report went out; each command has its own status.
    Reported,
    /// A storage error; answered `503`, retryable.
    StoreError,
}

impl PushResult {
    const ALL: [PushResult; 4] = [
        Self::Fatal,
        Self::UnpackFailed,
        Self::Reported,
        Self::StoreError,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Probe => "probe",
            Self::Fatal => "fatal",
            Self::UnpackFailed => "unpack_failed",
            Self::Reported => "reported",
            Self::StoreError => "store_error",
        }
    }
}

/// The `result` label of a refused ref update: the reason, in snake case.
/// An exhaustive match, so a new reason cannot go unlabelled.
fn reject_label(r: RejectReason) -> &'static str {
    match r {
        RejectReason::NotWritable => "not_writable",
        RejectReason::PermissionDenied => "permission_denied",
        RejectReason::LeaseMismatch => "lease_mismatch",
        RejectReason::NoSuchRef => "no_such_ref",
        RejectReason::TrunkDelete => "trunk_delete",
        RejectReason::StaleOld => "stale_old",
        RejectReason::DeleteNeedsForce => "delete_needs_force",
        RejectReason::ForceNotAllowed => "force_not_allowed",
        RejectReason::RefExists => "ref_exists",
        RejectReason::TagExists => "tag_exists",
        RejectReason::NonFastForward => "non_fast_forward",
        RejectReason::AncestryUndecided => "ancestry_undecided",
        RejectReason::CasLost => "cas_lost",
        RejectReason::TrunkRewritten => "trunk_rewritten",
        RejectReason::AtomicPeerRejected => "atomic_peer_rejected",
        RejectReason::UnpackFailed => "unpack_failed",
        RejectReason::NotACommit => "not_a_commit",
        RejectReason::UpstreamRejected => "upstream_rejected",
        RejectReason::UpstreamUnavailable => "upstream_unavailable",
        RejectReason::UpstreamAhead => "upstream_ahead",
        RejectReason::PushRule => "push_rule",
    }
}

/// Where `r` sits in [`RejectReason::ALL`], and so in
/// `PushMetrics::refs_rejected`. An exhaustive match, so a new reason
/// cannot be left without a slot; the constant below checks at compile
/// time that it agrees with `ALL`.
const fn reject_index(r: RejectReason) -> usize {
    match r {
        RejectReason::NotWritable => 0,
        RejectReason::PermissionDenied => 1,
        RejectReason::LeaseMismatch => 2,
        RejectReason::NoSuchRef => 3,
        RejectReason::TrunkDelete => 4,
        RejectReason::StaleOld => 5,
        RejectReason::DeleteNeedsForce => 6,
        RejectReason::ForceNotAllowed => 7,
        RejectReason::RefExists => 8,
        RejectReason::TagExists => 9,
        RejectReason::NonFastForward => 10,
        RejectReason::AncestryUndecided => 11,
        RejectReason::CasLost => 12,
        RejectReason::TrunkRewritten => 13,
        RejectReason::AtomicPeerRejected => 14,
        RejectReason::UnpackFailed => 15,
        RejectReason::NotACommit => 16,
        RejectReason::UpstreamRejected => 17,
        RejectReason::UpstreamUnavailable => 18,
        RejectReason::UpstreamAhead => 19,
        RejectReason::PushRule => 20,
    }
}

const _: () = {
    let mut i = 0;
    while i < RejectReason::ALL.len() {
        assert!(reject_index(RejectReason::ALL[i]) == i);
        i += 1;
    }
};

/// Push counters. All are monotonic.
#[derive(Debug)]
pub struct PushMetrics {
    /// Per [`PushResult::ALL`].
    results: [AtomicU64; PushResult::ALL.len()],
    duration: Histogram,
    received_bytes: AtomicU64,
    refs_ok: AtomicU64,
    refs_bad_options: AtomicU64,
    /// Per [`RejectReason::ALL`].
    refs_rejected: [AtomicU64; RejectReason::ALL.len()],
    trunk_exact: AtomicU64,
    trunk_wider: AtomicU64,
    /// Versions of refs other than trunk, by closure (issue #203).
    branch_exact: AtomicU64,
    branch_wider: AtomicU64,
    /// Pushes refused because their pack was garbage-collected before
    /// they committed (issue #203).
    grace_exceeded: AtomicU64,
}

impl Default for PushMetrics {
    fn default() -> Self {
        Self {
            results: Default::default(),
            duration: Histogram::new(&DURATION_MICROS, MICROS_PER_SECOND),
            received_bytes: AtomicU64::new(0),
            refs_ok: AtomicU64::new(0),
            refs_bad_options: AtomicU64::new(0),
            refs_rejected: Default::default(),
            trunk_exact: AtomicU64::new(0),
            trunk_wider: AtomicU64::new(0),
            branch_exact: AtomicU64::new(0),
            branch_wider: AtomicU64::new(0),
            grace_exceeded: AtomicU64::new(0),
        }
    }
}

impl PushMetrics {
    /// One push request ended. Probes are not pushes and are not counted.
    pub(crate) fn finished(&self, result: PushResult, took: Duration, received_bytes: u64) {
        let Some(i) = PushResult::ALL.iter().position(|r| *r == result) else {
            return;
        };
        self.results[i].fetch_add(1, Ordering::Relaxed);
        self.duration.observe(expo::micros(took));
        self.received_bytes
            .fetch_add(received_bytes, Ordering::Relaxed);
    }

    /// One ref command's final status: updated (or a no-op), or refused.
    pub(crate) fn ref_result(&self, outcome: RefOutcome) {
        let counter = match outcome {
            RefOutcome::Ok => Some(&self.refs_ok),
            RefOutcome::BadPushOptions => Some(&self.refs_bad_options),
            // `get`, not indexing: counting can never panic a push.
            RefOutcome::Rejected(r) => self.refs_rejected.get(reject_index(r)),
        };
        if let Some(n) = counter {
            n.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A trunk version was committed with this closure.
    pub(crate) fn trunk_committed(&self, closure: Closure) {
        match closure {
            Closure::Exact => &self.trunk_exact,
            Closure::Wider => &self.trunk_wider,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// A version of a ref other than trunk was committed with this
    /// closure.
    pub(crate) fn branch_committed(&self, closure: Closure) {
        match closure {
            Closure::Exact => &self.branch_exact,
            Closure::Wider => &self.branch_wider,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// Versions of refs other than trunk committed with a wider closure so
    /// far.
    pub fn branch_wider(&self) -> u64 {
        self.branch_wider.load(Ordering::Relaxed)
    }

    /// Versions of refs other than trunk committed with an exact closure so
    /// far.
    pub fn branch_exact(&self) -> u64 {
        self.branch_exact.load(Ordering::Relaxed)
    }

    /// A push was refused because garbage collection deleted its pack
    /// before it committed.
    pub(crate) fn grace_exceeded(&self) {
        self.grace_exceeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Pushes refused so far because their pack outlived the grace period
    /// before they committed.
    pub fn grace_exceeded_total(&self) -> u64 {
        self.grace_exceeded.load(Ordering::Relaxed)
    }

    /// Trunk versions committed with a wider closure so far.
    pub fn trunk_wider(&self) -> u64 {
        self.trunk_wider.load(Ordering::Relaxed)
    }

    /// Trunk versions committed with an exact closure so far.
    pub fn trunk_exact(&self) -> u64 {
        self.trunk_exact.load(Ordering::Relaxed)
    }

    /// Ref updates accepted so far (including no-ops).
    pub fn refs_ok(&self) -> u64 {
        self.refs_ok.load(Ordering::Relaxed)
    }

    /// Ref updates refused for `reason` so far.
    pub fn refs_rejected(&self, reason: RejectReason) -> u64 {
        self.refs_rejected
            .get(reject_index(reason))
            .map_or(0, |n| n.load(Ordering::Relaxed))
    }

    /// Writes the push families. `timings` are the stage times summed over
    /// every push; `gate` is the prefetch gate's (slots, in use, peak,
    /// granted, timed out).
    pub(crate) fn render(&self, out: &mut String, timings: &Timings, gate: GateStats) {
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed) as f64;
        expo::family(
            out,
            "aspen_pushes_total",
            "Push requests (POST git-receive-pack) by how they ended: reported (each ref has a status), unpack_failed, fatal (unparseable request) or store_error (503, retryable). Probes are not counted.",
            Kind::Counter,
            PushResult::ALL
                .iter()
                .zip(&self.results)
                .map(|(r, n)| (vec![("result", r.label())], get(n))),
        );
        self.duration.render(
            out,
            "aspen_push_duration_seconds",
            "Time from the first byte of a push request to its report.",
        );
        expo::counter(
            out,
            "aspen_push_received_bytes_total",
            "Pack bytes received from clients.",
            self.received_bytes.load(Ordering::Relaxed),
        );
        expo::family(
            out,
            "aspen_push_ref_updates_total",
            "Ref commands by final status: ok (updated or a no-op), or the reason it was refused.",
            Kind::Counter,
            [
                (vec![("result", "ok")], get(&self.refs_ok)),
                (
                    vec![("result", "push_options_invalid")],
                    get(&self.refs_bad_options),
                ),
            ]
            .into_iter()
            .chain(
                RejectReason::ALL
                    .iter()
                    .zip(&self.refs_rejected)
                    .map(|(r, n)| (vec![("result", reject_label(*r))], get(n))),
            ),
        );
        expo::family(
            out,
            "aspen_push_trunk_versions_total",
            "Trunk versions committed, by closure. Any wider one means every later fetch of that repository is a full clone: investigate.",
            Kind::Counter,
            [
                (vec![("closure", "exact")], get(&self.trunk_exact)),
                (vec![("closure", "wider")], get(&self.trunk_wider)),
            ],
        );
        expo::family(
            out,
            "aspen_push_branch_versions_total",
            "Versions of refs other than trunk committed, by closure. A wider one makes incremental fetches of that ref send more than they need.",
            Kind::Counter,
            [
                (vec![("closure", "exact")], get(&self.branch_exact)),
                (vec![("closure", "wider")], get(&self.branch_wider)),
            ],
        );
        expo::family(
            out,
            "aspen_push_grace_exceeded_total",
            "Pushes refused because garbage collection deleted their pack before they committed: registering to committing took longer than gc_grace. The client retries.",
            Kind::Counter,
            [(vec![], get(&self.grace_exceeded))],
        );
        let stages: [(&str, Duration); 10] = [
            ("search", timings.search),
            ("receive", timings.receive),
            ("gate_wait", timings.gate_wait),
            ("prefetch", timings.prefetch),
            ("complete", timings.complete),
            ("graph", timings.graph),
            ("check", timings.check),
            ("hook", timings.hook),
            ("upload", timings.upload),
            ("commit", timings.commit),
        ];
        expo::family(
            out,
            "aspen_push_stage_seconds_total",
            "Time spent in each push stage, summed over pushes (push path, docs/PLAN.md).",
            Kind::Counter,
            stages
                .iter()
                .map(|(s, d)| (vec![("stage", *s)], d.as_secs_f64())),
        );
        expo::counter(
            out,
            "aspen_push_ancestry_commits_total",
            "Commits the push path's ancestry walks expanded, summed over pushes (each a commit_index or pack read unless cached within the push).",
            timings.ancestry_commits,
        );
        expo::gauge(
            out,
            "aspen_push_prefetch_slots",
            "Pushes the prefetch gate admits at once while each holds its floor (prefetch_memory_bytes / prefetch_floor_bytes).",
            gate.slots as f64,
        );
        expo::gauge(
            out,
            "aspen_push_prefetch_slots_in_use",
            "Prefetch slots held now.",
            gate.in_use as f64,
        );
        expo::gauge(
            out,
            "aspen_push_prefetch_slots_peak",
            "Most prefetch slots held at once since the process started.",
            gate.peak as f64,
        );
        expo::counter(
            out,
            "aspen_push_prefetch_slots_acquired_total",
            "Prefetch slots granted.",
            gate.acquired,
        );
        expo::counter(
            out,
            "aspen_push_prefetch_wait_timeouts_total",
            "Waits for a prefetch slot that gave up (the push got a retryable error).",
            gate.timeouts,
        );
        expo::counter(
            out,
            "aspen_push_prefetch_escalations_total",
            "Reads of stored packs that needed more than the floor, so their slot grew to the full per-push budget.",
            gate.escalations,
        );
    }
}

/// The final status of one ref command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefOutcome {
    /// Updated, or a no-op.
    Ok,
    /// Refused for this reason.
    Rejected(RejectReason),
    /// The push options did not parse, so every command was refused.
    BadPushOptions,
}

/// The prefetch gate's numbers, for [`PushMetrics::render`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct GateStats {
    pub(crate) slots: usize,
    pub(crate) in_use: usize,
    pub(crate) peak: usize,
    pub(crate) acquired: u64,
    pub(crate) timeouts: u64,
    pub(crate) escalations: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every reject reason has its own label, so no two reasons share a
    /// series.
    #[test]
    fn reject_labels_are_distinct() {
        let mut labels: Vec<_> = RejectReason::ALL.iter().map(|r| reject_label(*r)).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), RejectReason::ALL.len());
        assert!(!labels.contains(&"ok"));
        assert!(!labels.contains(&"push_options_invalid"));
    }
}

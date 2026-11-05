//! Findings and the run report.

use std::collections::BTreeMap;
use std::fmt;

/// The invariant a finding breaks (section 4 of `docs/PLAN.md`, chaos
/// testing), plus the liveness checks the suite adds and the guards that
/// fail a run which could not check what it claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Invariant {
    /// "An acknowledged push is never lost": a commit whose push got an
    /// `ok` is not in the history of its ref's current tip.
    AckedPushLost,
    /// "History per ref is linearizable": no order of the operations on a
    /// ref explains what clients saw.
    NotLinearizable,
    /// "No ref ever points at objects that can't be fetched": a fetch of a
    /// ref failed, or its result failed `git fsck --strict`, after every
    /// fault was healed.
    Unfetchable,
    /// A fetch failed during faults in a way only a server bug explains
    /// (objects missing from a complete pack, a corrupt object).
    CorruptFetch,
    /// The deployment did not answer reads within a minute of every fault
    /// being healed.
    Unavailable,
    /// Background work (compaction, commit indexing) did not finish after
    /// the faults stopped.
    Liveness,
    /// An `aspen_alert_*` counter (corruption, forward delta base) moved,
    /// or a running instance's counters could not be read.
    Alert,
    /// An `aspen serve` process exited without the suite killing it.
    Crashed,
    /// The linearizability search for a ref ran out of budget, so the
    /// ref's history was not checked.
    Inconclusive,
    /// A check, a fault step or a client's own (local) work could not run,
    /// so the run proves nothing about what it would have covered.
    Unchecked,
    /// The run did not exercise what it claims: an epoch with no
    /// acknowledged push, a faulted scenario that injected no fault (or a
    /// part of it that never did), or no fetch that used packfile URIs.
    Vacuous,
}

impl fmt::Display for Invariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AckedPushLost => "acknowledged push lost",
            Self::NotLinearizable => "ref history not linearizable",
            Self::Unfetchable => "ref points at objects that cannot be fetched",
            Self::CorruptFetch => "fetch returned corrupt or incomplete data",
            Self::Unavailable => "unavailable after faults were healed",
            Self::Liveness => "background work did not finish",
            Self::Alert => "alert counter moved or could not be read",
            Self::Crashed => "instance exited without being killed",
            Self::Inconclusive => "linearizability search ran out of budget",
            Self::Unchecked => "a check or fault step could not run",
            Self::Vacuous => "the run did not exercise what it claims",
        })
    }
}

/// One broken invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub invariant: Invariant,
    /// The epoch it was found in (`usize::MAX` for the final checks).
    pub epoch: usize,
    pub detail: String,
}

impl Finding {
    pub fn new(invariant: Invariant, epoch: usize, detail: impl Into<String>) -> Self {
        Self {
            invariant,
            epoch,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.epoch == usize::MAX {
            write!(f, "[final] {}: {}", self.invariant, self.detail)
        } else {
            write!(
                f,
                "[epoch {}] {}: {}",
                self.epoch, self.invariant, self.detail
            )
        }
    }
}

/// What one run did and found.
#[derive(Debug, Default, Clone)]
pub struct Report {
    pub scenario: String,
    pub seed: u64,
    pub epochs: usize,
    pub seconds: f64,
    /// Counts by label: operations by kind and outcome, faults by kind.
    pub counts: BTreeMap<String, u64>,
    /// Broken invariants.
    pub violations: Vec<Finding>,
    /// Things worth reading that break no invariant (dead jobs, jobs a
    /// worker dropped without recording an outcome).
    pub warnings: Vec<String>,
    /// Extra facts (alert totals, covers built, instance restarts).
    pub notes: Vec<String>,
}

/// Counters several threads add to during an epoch.
#[derive(Debug, Default)]
pub struct Tally(std::sync::Mutex<BTreeMap<String, u64>>);

impl Tally {
    /// Adds `n` to counter `label`.
    pub fn add(&self, label: &str, n: u64) {
        *self
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(label.to_owned())
            .or_default() += n;
    }

    /// Moves every counter into `report`.
    pub fn drain_into(&self, report: &mut Report) {
        let taken = std::mem::take(&mut *self.0.lock().unwrap_or_else(|e| e.into_inner()));
        for (k, v) in taken {
            report.count(k, v);
        }
    }
}

impl Report {
    /// Adds `n` to counter `label`.
    pub fn count(&mut self, label: impl Into<String>, n: u64) {
        *self.counts.entry(label.into()).or_default() += n;
    }

    /// The value of counter `label`.
    pub fn get(&self, label: &str) -> u64 {
        self.counts.get(label).copied().unwrap_or(0)
    }

    /// Whether no invariant broke.
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    /// Whether some finding breaks `invariant`.
    pub fn broke(&self, invariant: Invariant) -> bool {
        self.violations.iter().any(|f| f.invariant == invariant)
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "chaos scenario {}: {} after {} epoch(s), {:.0} s (seed {:#018x})",
            self.scenario,
            if self.passed() { "PASSED" } else { "FAILED" },
            self.epochs,
            self.seconds,
            self.seed
        )?;
        for (k, v) in &self.counts {
            writeln!(f, "  {k}: {v}")?;
        }
        for n in &self.notes {
            writeln!(f, "  note: {n}")?;
        }
        for w in &self.warnings {
            writeln!(f, "  warning: {w}")?;
        }
        if !self.violations.is_empty() {
            writeln!(f, "  {} violation(s):", self.violations.len())?;
            for v in &self.violations {
                for (i, line) in v.to_string().lines().enumerate() {
                    writeln!(f, "{}{line}", if i == 0 { "  - " } else { "      " })?;
                }
            }
            writeln!(
                f,
                "  replay the schedule with --seed {:#x} (timing differs between runs)",
                self.seed
            )?;
        }
        Ok(())
    }
}

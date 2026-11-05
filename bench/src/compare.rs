//! Regression checker: compare a run against a stored baseline.
//!
//! Both sides are [`RunResult`]s, flattened to `section.name[.field]` keys
//! (see [`RunResult::flatten`]). A list of [`Rule`]s says, per metric key,
//! which direction is better and how much worse the current run may be:
//!
//! - lower is better: regressed if `current > baseline * (1 + pct/100) + slack`
//! - higher is better: regressed if `current < baseline * (1 - pct/100) - slack`
//!
//! The first rule whose pattern matches a key wins. A pattern is a
//! dot-separated key where a `*` segment matches any one segment, so
//! `latencies_ms.*.p99` matches `latencies_ms.push.p99`. Keys no rule
//! matches are not checked. A checked metric that the baseline has and the
//! current run lacks counts as a regression, so a renamed or broken metric
//! cannot pass silently.
//!
//! Before comparing numbers, [`compatibility`] checks that both runs used the
//! same workload and parameters; comparing a 10-writer run against a
//! 100-writer baseline would be meaningless.

use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::results::RunResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tolerances {
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rule {
    /// Metric key pattern; `*` matches one dot-separated segment.
    pub metric: String,
    pub better: Better,
    /// Allowed relative regression, in percent of the baseline value.
    #[serde(default)]
    pub max_regression_pct: f64,
    /// Allowed absolute regression on top of the relative one, in the
    /// metric's own unit. Keeps tiny values (a 2 ms latency) from failing on
    /// noise.
    #[serde(default)]
    pub abs_slack: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Better {
    Lower,
    Higher,
    /// Matched but deliberately not checked (for example, sample counts that
    /// depend on how long the run lasted).
    Ignore,
}

/// The committed tolerance file, `bench/tolerances.json`, which is also what
/// `aspen-bench compare` uses without `--tolerances`. Keeping one copy means
/// the nightly check and a local run never disagree.
pub const DEFAULT_TOLERANCES_JSON: &str = include_str!("../tolerances.json");

impl Default for Tolerances {
    /// The rules in `bench/tolerances.json` (embedded at build time), loose
    /// enough for shared CI runners. A `--tolerances` file replaces them
    /// completely.
    fn default() -> Self {
        serde_json::from_str(DEFAULT_TOLERANCES_JSON)
            .expect("bench/tolerances.json is valid (checked by a unit test)")
    }
}

impl Tolerances {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text =
            fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
        serde_json::from_str(&text).map_err(|e| format!("parsing {}: {e}", path.display()))
    }

    /// The first rule whose pattern matches `key`.
    pub fn rule_for(&self, key: &str) -> Option<&Rule> {
        self.rules.iter().find(|r| pattern_matches(&r.metric, key))
    }
}

/// Whether a dot-separated pattern matches a key; `*` matches one segment.
pub fn pattern_matches(pattern: &str, key: &str) -> bool {
    let mut p = pattern.split('.');
    let mut k = key.split('.');
    loop {
        match (p.next(), k.next()) {
            (None, None) => return true,
            (Some(ps), Some(ks)) if ps == "*" || ps == ks => {}
            _ => return false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    /// Within tolerance.
    Ok,
    /// Better than the baseline by more than the tolerance (informational).
    Improved,
    /// Worse than the tolerance allows.
    Regressed,
    /// Checked metric present in the baseline but missing from the run.
    Missing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Check {
    pub metric: String,
    pub better: Better,
    pub baseline: f64,
    pub current: Option<f64>,
    /// The worst value that still passes.
    pub limit: f64,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub workload: String,
    pub baseline_target: String,
    pub current_target: String,
    /// Reasons the two runs are not comparable; empty when they are.
    pub incompatibilities: Vec<String>,
    pub checks: Vec<Check>,
}

impl Report {
    pub fn regressions(&self) -> impl Iterator<Item = &Check> {
        self.checks
            .iter()
            .filter(|c| matches!(c.verdict, Verdict::Regressed | Verdict::Missing))
    }

    pub fn has_regressions(&self) -> bool {
        self.regressions().next().is_some()
    }

    /// A plain-text table for logs.
    pub fn render(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(
            s,
            "compare {}: baseline {} vs current {}",
            self.workload, self.baseline_target, self.current_target
        );
        for i in &self.incompatibilities {
            let _ = writeln!(s, "  incompatible: {i}");
        }
        let _ = writeln!(
            s,
            "  {:<36} {:>12} {:>12} {:>12}  verdict",
            "metric", "baseline", "current", "limit"
        );
        for c in &self.checks {
            let current = c
                .current
                .map_or_else(|| "missing".to_owned(), |v| format!("{v:.2}"));
            let _ = writeln!(
                s,
                "  {:<36} {:>12.2} {:>12} {:>12.2}  {:?}",
                c.metric, c.baseline, current, c.limit, c.verdict
            );
        }
        let n = self.regressions().count();
        let _ = writeln!(
            s,
            "{}",
            if n == 0 {
                "no regressions".to_owned()
            } else {
                format!("{n} regression(s)")
            }
        );
        s
    }
}

/// Reasons `current` cannot be compared with `baseline`: a different
/// workload or different load parameters.
pub fn compatibility(baseline: &RunResult, current: &RunResult) -> Vec<String> {
    let mut out = Vec::new();
    if baseline.workload != current.workload {
        out.push(format!(
            "workload differs: baseline {:?}, current {:?}",
            baseline.workload, current.workload
        ));
    }
    let keys = baseline.params.keys().chain(current.params.keys());
    let mut seen = std::collections::BTreeSet::new();
    for key in keys {
        if !seen.insert(key) {
            continue;
        }
        let (b, c) = (baseline.params.get(key), current.params.get(key));
        if b != c {
            let show =
                |v: Option<&serde_json::Value>| v.map_or("unset".to_owned(), |v| v.to_string());
            out.push(format!(
                "param {key} differs: baseline {}, current {}",
                show(b),
                show(c)
            ));
        }
    }
    out
}

/// Compares every metric of `baseline` that a rule checks.
pub fn compare(baseline: &RunResult, current: &RunResult, tolerances: &Tolerances) -> Report {
    let base = baseline.flatten();
    let cur = current.flatten();
    let mut checks = Vec::new();
    for (key, &b) in &base {
        let Some(rule) = tolerances.rule_for(key) else {
            continue;
        };
        let pct = rule.max_regression_pct / 100.0;
        let limit = match rule.better {
            Better::Ignore => continue,
            Better::Lower => b * (1.0 + pct) + rule.abs_slack,
            Better::Higher => b * (1.0 - pct) - rule.abs_slack,
        };
        let current = cur.get(key).copied();
        let verdict = match current {
            None => Verdict::Missing,
            Some(c) => judge(rule, b, c, limit),
        };
        checks.push(Check {
            metric: key.clone(),
            better: rule.better,
            baseline: b,
            current,
            limit,
            verdict,
        });
    }
    Report {
        workload: current.workload.clone(),
        baseline_target: baseline.target.label.clone(),
        current_target: current.target.label.clone(),
        incompatibilities: compatibility(baseline, current),
        checks,
    }
}

fn judge(rule: &Rule, baseline: f64, current: f64, limit: f64) -> Verdict {
    let pct = rule.max_regression_pct / 100.0;
    match rule.better {
        Better::Lower if current > limit => Verdict::Regressed,
        Better::Lower if current < baseline * (1.0 - pct) - rule.abs_slack => Verdict::Improved,
        Better::Higher if current < limit => Verdict::Regressed,
        Better::Higher if current > baseline * (1.0 + pct) + rule.abs_slack => Verdict::Improved,
        _ => Verdict::Ok,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::results::sample;
    use crate::stats::Summary;

    fn with_push_latency(samples: &[f64]) -> RunResult {
        let mut r = sample();
        r.latencies_ms
            .insert("push".to_owned(), Summary::from_millis(samples));
        r
    }

    fn verdict(report: &Report, metric: &str) -> Verdict {
        report
            .checks
            .iter()
            .find(|c| c.metric == metric)
            .unwrap_or_else(|| panic!("no check for {metric}: {report:?}"))
            .verdict
    }

    #[test]
    fn identical_runs_pass() {
        let r = sample();
        let report = compare(&r, &r, &Tolerances::default());
        assert!(!report.has_regressions(), "{}", report.render());
        assert!(report.incompatibilities.is_empty());
        assert!(!report.checks.is_empty());
    }

    #[test]
    fn latency_within_tolerance_passes_and_beyond_fails() {
        let base = with_push_latency(&[100.0; 10]);
        // Default p99: lower is better, 150% plus 50 ms, so the limit is 300 ms.
        let ok = with_push_latency(&[300.0; 10]);
        let bad = with_push_latency(&[301.0; 10]);
        let tol = Tolerances::default();
        assert_eq!(
            verdict(&compare(&base, &ok, &tol), "latencies_ms.push.p99"),
            Verdict::Ok
        );
        let report = compare(&base, &bad, &tol);
        assert_eq!(
            verdict(&report, "latencies_ms.push.p99"),
            Verdict::Regressed
        );
        assert!(report.has_regressions());
        assert!(report.render().contains("regression(s)"));
    }

    #[test]
    fn faster_latency_is_an_improvement_not_a_failure() {
        let base = with_push_latency(&[100.0; 10]);
        let fast = with_push_latency(&[10.0; 10]);
        let report = compare(&base, &fast, &Tolerances::default());
        assert_eq!(verdict(&report, "latencies_ms.push.p50"), Verdict::Improved);
        assert!(!report.has_regressions());
    }

    #[test]
    fn throughput_is_higher_is_better() {
        let base = sample(); // 5 pushes/s
        let mut slow = sample();
        slow.throughput.insert("pushes_per_second".to_owned(), 2.4);
        let mut ok = sample();
        ok.throughput.insert("pushes_per_second".to_owned(), 2.6);
        let tol = Tolerances::default(); // 50% => limit 2.5
        let key = "throughput.pushes_per_second";
        assert_eq!(
            verdict(&compare(&base, &slow, &tol), key),
            Verdict::Regressed
        );
        assert_eq!(verdict(&compare(&base, &ok, &tol), key), Verdict::Ok);
    }

    #[test]
    fn any_new_error_is_a_regression() {
        let base = sample();
        let mut cur = sample();
        cur.errors.push("push failed".to_owned());
        let report = compare(&base, &cur, &Tolerances::default());
        assert_eq!(verdict(&report, "errors.count"), Verdict::Regressed);
    }

    #[test]
    fn a_checked_metric_that_disappears_is_a_regression() {
        let base = sample();
        let mut cur = sample();
        cur.throughput.clear();
        let report = compare(&base, &cur, &Tolerances::default());
        assert_eq!(
            verdict(&report, "throughput.pushes_per_second"),
            Verdict::Missing
        );
        assert!(report.has_regressions());
    }

    #[test]
    fn ignored_and_unmatched_metrics_are_not_checked() {
        let base = sample();
        let mut cur = sample();
        cur.counts.insert("pushes".to_owned(), 1);
        cur.wall_seconds = 1000.0;
        let report = compare(&base, &cur, &Tolerances::default());
        assert!(report.checks.iter().all(|c| c.metric != "counts.pushes"));
        assert!(report.checks.iter().all(|c| c.metric != "wall_seconds"));
        assert!(!report.has_regressions());
        let none = Tolerances { rules: vec![] };
        assert!(compare(&base, &cur, &none).checks.is_empty());
    }

    #[test]
    fn first_matching_rule_wins() {
        let tol: Tolerances = serde_json::from_str(
            r#"{"rules": [
                {"metric": "latencies_ms.push.p99", "better": "lower", "max_regression_pct": 0},
                {"metric": "latencies_ms.*.*", "better": "ignore"}
            ]}"#,
        )
        .unwrap();
        let base = with_push_latency(&[100.0]);
        let cur = with_push_latency(&[101.0]);
        let report = compare(&base, &cur, &tol);
        assert_eq!(report.checks.len(), 1, "{report:?}");
        assert_eq!(
            verdict(&report, "latencies_ms.push.p99"),
            Verdict::Regressed
        );
    }

    #[test]
    fn patterns_match_whole_segments() {
        assert!(pattern_matches(
            "latencies_ms.*.p99",
            "latencies_ms.push.p99"
        ));
        assert!(pattern_matches(
            "throughput.*",
            "throughput.pushes_per_second"
        ));
        assert!(pattern_matches("errors.count", "errors.count"));
        assert!(!pattern_matches("latencies_ms.*", "latencies_ms.push.p99"));
        assert!(!pattern_matches(
            "latencies_ms.*.p99",
            "latencies_ms.push.p9"
        ));
        assert!(!pattern_matches("throughput.*", "throughput"));
    }

    #[test]
    fn different_params_or_workload_are_incompatible() {
        let base = sample();
        let mut cur = sample();
        cur.workload = "clone".to_owned();
        cur.params.insert("writers".to_owned(), 10.into());
        let issues = compatibility(&base, &cur);
        assert_eq!(issues.len(), 2, "{issues:?}");
        assert!(issues[0].contains("workload"));
        assert!(issues[1].contains("writers") && issues[1].contains("unset"));
    }

    #[test]
    fn default_tolerances_are_the_committed_file() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tolerances.json");
        let tol = Tolerances::load(&path).unwrap();
        assert_eq!(tol, Tolerances::default());
        assert!(tol.rule_for("errors.count").is_some());
        assert!(tol.rule_for("latencies_ms.push_to_fetch.p99").is_some());
    }

    #[test]
    fn plain_and_branch_scoped_push_runs_are_incompatible() {
        let base = sample();
        let mut cur = sample();
        let key = "push_url".to_owned();
        cur.params.insert(key, "branch-scoped".into());
        let issues = compatibility(&base, &cur);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].contains("push_url"));
    }
}

//! The machine-readable result of one workload run.
//!
//! One run writes one JSON document (see `bench/README.md` for an example).
//! The same format is the baseline format: a baseline is a stored result that
//! later runs are compared against (see [`crate::compare`]).
//!
//! Metric names use `snake_case` and never contain dots, because the
//! regression checker addresses metrics as dot-separated keys such as
//! `latencies_ms.push_to_fetch.p99`.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::stats::Summary;

/// Schema identifier written into every result. Bump the version when a
/// change would make old baselines compare incorrectly.
pub const SCHEMA: &str = "aspen-bench.result/v1";

/// Most error messages kept in a result; the count is always exact.
pub const MAX_ERROR_SAMPLES: usize = 20;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunResult {
    pub schema: String,
    /// `writers`, `fetch-storm` or `clone`.
    pub workload: String,
    pub target: TargetInfo,
    /// Every knob that shapes the load, so two results can be checked for
    /// comparability.
    pub params: BTreeMap<String, Value>,
    pub environment: Environment,
    pub started_at_unix_ms: u64,
    /// Wall-clock time of the measured phase (setup excluded).
    pub wall_seconds: f64,
    pub latencies_ms: BTreeMap<String, Summary>,
    /// Rates over the measured phase, for example `pushes_per_second`.
    pub throughput: BTreeMap<String, f64>,
    pub counts: BTreeMap<String, u64>,
    pub errors: Errors,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetInfo {
    /// Free-form name such as `gitea` or `local-git-http-backend`.
    pub label: String,
    /// Repository URL with any credentials removed.
    pub url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Errors {
    pub count: u64,
    /// The first few error messages, for diagnosis.
    pub samples: Vec<String>,
}

impl Errors {
    pub fn push(&mut self, message: String) {
        self.count += 1;
        if self.samples.len() < MAX_ERROR_SAMPLES {
            self.samples.push(message);
        }
    }

    pub fn merge(&mut self, other: Errors) {
        self.count += other.count;
        for s in other.samples {
            if self.samples.len() >= MAX_ERROR_SAMPLES {
                break;
            }
            self.samples.push(s);
        }
    }
}

/// Where the load generator ran. Benchmarks are only comparable on similar
/// machines, so this travels with every result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Environment {
    pub aspen_bench_version: String,
    /// Commit of this repository the tool was built from, when known
    /// (`ASPEN_BENCH_COMMIT` or `GITHUB_SHA`).
    pub aspen_commit: Option<String>,
    pub git_version: String,
    pub os: String,
    pub arch: String,
    pub kernel: Option<String>,
    pub cpu_model: Option<String>,
    pub cpus: usize,
    pub memory_bytes: Option<u64>,
    /// `github-actions` when run there, otherwise `None`.
    pub ci: Option<String>,
    pub runner: Option<String>,
}

impl Environment {
    /// Describes the current machine; `git_version` comes from the caller,
    /// which knows which `git` binary drives the load.
    pub fn detect(git_version: String) -> Self {
        let cpuinfo = fs::read_to_string("/proc/cpuinfo").ok();
        let meminfo = fs::read_to_string("/proc/meminfo").ok();
        let github = std::env::var_os("GITHUB_ACTIONS").is_some();
        Self {
            aspen_bench_version: env!("CARGO_PKG_VERSION").to_owned(),
            aspen_commit: std::env::var("ASPEN_BENCH_COMMIT")
                .or_else(|_| std::env::var("GITHUB_SHA"))
                .ok()
                .filter(|s| !s.is_empty()),
            git_version,
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            kernel: fs::read_to_string("/proc/sys/kernel/osrelease")
                .ok()
                .map(|s| s.trim().to_owned()),
            cpu_model: cpuinfo.as_deref().and_then(cpu_model),
            cpus: std::thread::available_parallelism().map_or(1, |n| n.get()),
            memory_bytes: meminfo.as_deref().and_then(mem_total_bytes),
            ci: github.then(|| "github-actions".to_owned()),
            runner: if github {
                std::env::var("RUNNER_NAME").ok()
            } else {
                None
            },
        }
    }
}

fn cpu_model(cpuinfo: &str) -> Option<String> {
    cpuinfo
        .lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_owned())
}

fn mem_total_bytes(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

impl RunResult {
    /// Every numeric metric as a flat `section.name[.field]` key. This is
    /// what the regression checker compares. `None` summary fields (no
    /// samples) are left out.
    pub fn flatten(&self) -> BTreeMap<String, f64> {
        let mut out = BTreeMap::new();
        for (name, summary) in &self.latencies_ms {
            for (field, value) in summary.fields() {
                if let Some(v) = value {
                    out.insert(format!("latencies_ms.{name}.{field}"), v);
                }
            }
        }
        for (name, v) in &self.throughput {
            out.insert(format!("throughput.{name}"), *v);
        }
        for (name, v) in &self.counts {
            out.insert(format!("counts.{name}"), *v as f64);
        }
        out.insert("errors.count".to_owned(), self.errors.count as f64);
        out.insert("wall_seconds".to_owned(), self.wall_seconds);
        out
    }

    pub fn to_json_pretty(&self) -> String {
        // Serialising plain data cannot fail.
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text =
            fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
        let result: Self =
            serde_json::from_str(&text).map_err(|e| format!("parsing {}: {e}", path.display()))?;
        if result.schema != SCHEMA {
            return Err(format!(
                "{} has schema {:?}, this tool reads {SCHEMA:?}",
                path.display(),
                result.schema
            ));
        }
        Ok(result)
    }

    /// A short human-readable summary, printed to stderr after a run.
    pub fn human_summary(&self) -> String {
        let mut s = format!(
            "{} against {} ({}): {:.2}s, {} errors\n",
            self.workload, self.target.label, self.target.url, self.wall_seconds, self.errors.count
        );
        for (name, l) in &self.latencies_ms {
            let f = |v: Option<f64>| v.map_or_else(|| "-".to_owned(), |v| format!("{v:.1}"));
            s.push_str(&format!(
                "  {name:<16} n={:<6} p50={}ms p90={}ms p99={}ms max={}ms\n",
                l.count,
                f(l.p50),
                f(l.p90),
                f(l.p99),
                f(l.max)
            ));
        }
        for (name, v) in &self.throughput {
            s.push_str(&format!("  {name:<16} {v:.2}\n"));
        }
        for sample in self.errors.samples.iter().take(3) {
            s.push_str(&format!("  error: {sample}\n"));
        }
        s
    }
}

/// A small, valid result for unit tests in this crate.
#[cfg(test)]
pub(crate) fn sample() -> RunResult {
    let mut latencies_ms = BTreeMap::new();
    latencies_ms.insert("push".to_owned(), Summary::from_millis(&[10.0, 20.0]));
    latencies_ms.insert("empty".to_owned(), Summary::from_millis(&[]));
    let mut throughput = BTreeMap::new();
    throughput.insert("pushes_per_second".to_owned(), 5.0);
    let mut counts = BTreeMap::new();
    counts.insert("pushes".to_owned(), 2);
    RunResult {
        schema: SCHEMA.to_owned(),
        workload: "writers".to_owned(),
        target: TargetInfo {
            label: "test".to_owned(),
            url: "http://127.0.0.1/r.git".to_owned(),
        },
        params: BTreeMap::new(),
        environment: Environment::detect("git version 2.0".to_owned()),
        started_at_unix_ms: 1,
        wall_seconds: 0.4,
        latencies_ms,
        throughput,
        counts,
        errors: Errors::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_names_every_metric_and_skips_missing_values() {
        let flat = sample().flatten();
        assert_eq!(flat["latencies_ms.push.p50"], 10.0);
        assert_eq!(flat["latencies_ms.push.p99"], 20.0);
        assert_eq!(flat["latencies_ms.push.count"], 2.0);
        assert_eq!(flat["latencies_ms.empty.count"], 0.0);
        assert!(!flat.contains_key("latencies_ms.empty.p50"));
        assert_eq!(flat["throughput.pushes_per_second"], 5.0);
        assert_eq!(flat["counts.pushes"], 2.0);
        assert_eq!(flat["errors.count"], 0.0);
        assert_eq!(flat["wall_seconds"], 0.4);
    }

    #[test]
    fn json_round_trip_preserves_the_result() {
        let r = sample();
        let back: RunResult = serde_json::from_str(&r.to_json_pretty()).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn load_rejects_another_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.json");
        let mut r = sample();
        r.schema = "aspen-bench.result/v0".to_owned();
        fs::write(&path, r.to_json_pretty()).unwrap();
        let err = RunResult::load(&path).unwrap_err();
        assert!(err.contains("schema"), "{err}");
    }

    #[test]
    fn error_samples_are_capped_but_counted() {
        let mut e = Errors::default();
        for i in 0..(MAX_ERROR_SAMPLES + 5) {
            e.push(format!("e{i}"));
        }
        let mut other = Errors::default();
        other.push("x".to_owned());
        e.merge(other);
        assert_eq!(e.count, MAX_ERROR_SAMPLES as u64 + 6);
        assert_eq!(e.samples.len(), MAX_ERROR_SAMPLES);
    }

    #[test]
    fn proc_parsers_read_linux_formats() {
        assert_eq!(
            cpu_model("processor\t: 0\nmodel name\t: Example CPU @ 3.0GHz\n").as_deref(),
            Some("Example CPU @ 3.0GHz")
        );
        assert_eq!(
            mem_total_bytes("MemTotal:       16384 kB\nMemFree: 1 kB\n"),
            Some(16384 * 1024)
        );
        assert_eq!(mem_total_bytes("nothing"), None);
    }
}

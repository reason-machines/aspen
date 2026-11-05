//! `aspen-bench report`: Markdown tables from a campaign directory.
//!
//! A campaign directory holds `<group>/<target>/<workload>.json`, as
//! `bench/campaign/run.sh` writes it (one group per repository and load
//! profile). For each group the report prints one table per workload with a
//! row per target, and the stale-branch fetch as a table per N. Bytes are
//! also shown as a ratio to the reference target (stock `git upload-pack`,
//! `local` by default), the number section 4.6 of `docs/PLAN.md` asks for.
//!
//! Every row also shows the memory of the machine its result came from
//! (`environment.memory_bytes`), since one campaign can mix machine sizes
//! and a latency is only comparable between rows of the same size.
//!
//! Every number comes from the result files as they are; nothing is
//! averaged across runs or edited, so the published tables can be
//! regenerated from the committed JSON.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use crate::results::RunResult;
use crate::stats::Summary;

/// `group -> target -> workload -> result`.
type Campaign = BTreeMap<String, BTreeMap<String, BTreeMap<String, RunResult>>>;

/// Reads every `<group>/<target>/<workload>.json` under `dir`.
pub fn load(dir: &Path) -> Result<Campaign, String> {
    let mut out = Campaign::new();
    for group in sorted_dirs(dir)? {
        for target in sorted_dirs(&dir.join(&group))? {
            let tdir = dir.join(&group).join(&target);
            let entries =
                std::fs::read_dir(&tdir).map_err(|e| format!("reading {}: {e}", tdir.display()))?;
            for e in entries.filter_map(Result::ok) {
                let p = e.path();
                if p.extension().is_some_and(|x| x == "json") {
                    let r = RunResult::load(&p)?;
                    let name = p
                        .file_stem()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    out.entry(group.clone())
                        .or_default()
                        .entry(target.clone())
                        .or_default()
                        .insert(name, r);
                }
            }
        }
    }
    Ok(out)
}

fn sorted_dirs(dir: &Path) -> Result<Vec<String>, String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .map_err(|e| format!("reading {}: {e}", dir.display()))?
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    Ok(v)
}

/// The Markdown report. `reference` names the target whose bytes the
/// others are divided by.
pub fn render(c: &Campaign, reference: &str) -> String {
    let mut s = String::new();
    for (group, targets) in c {
        let _ = writeln!(s, "## {group}\n");
        let mut workloads: Vec<&String> = targets.values().flat_map(|w| w.keys()).collect();
        workloads.sort();
        workloads.dedup();
        for w in workloads {
            let rows: Vec<(&String, &RunResult)> = targets
                .iter()
                .filter_map(|(t, ws)| ws.get(w).map(|r| (t, r)))
                .collect();
            if w == "stale-branch" {
                stale_tables(&mut s, &rows);
            } else {
                workload_table(&mut s, w, &rows, reference);
            }
        }
    }
    s
}

fn ms(v: Option<f64>) -> String {
    match v {
        None => "-".to_owned(),
        Some(v) if v >= 10_000.0 => format!("{:.1} s", v / 1000.0),
        Some(v) if v >= 100.0 => format!("{v:.0} ms"),
        Some(v) => format!("{v:.1} ms"),
    }
}

fn bytes(n: u64) -> String {
    let f = n as f64;
    if f >= 1e9 {
        format!("{:.2} GB", f / 1e9)
    } else if f >= 1e6 {
        format!("{:.2} MB", f / 1e6)
    } else if f >= 1e3 {
        format!("{:.1} kB", f / 1e3)
    } else {
        format!("{n} B")
    }
}

/// The memory of the machine a result came from, or `-` when it wasn't
/// recorded.
fn memory(r: &RunResult) -> String {
    r.environment
        .memory_bytes
        .map_or_else(|| "-".to_owned(), bytes)
}

/// The headline latency of a workload and the bytes each operation received.
fn headline(w: &str, r: &RunResult) -> (&'static str, Option<Summary>, Option<u64>) {
    let lat = |k: &str| r.latencies_ms.get(k).cloned();
    let count = |k: &str| r.counts.get(k).copied();
    match w {
        "writers" => ("push→fetch", lat("push_to_fetch"), None),
        "clone" => (
            "clone",
            lat("clone"),
            count("received_bytes")
                .zip(count("clones_ok"))
                .and_then(|(b, n)| b.checked_div(n)),
        ),
        _ => ("fetch", lat("fetch"), count("received_bytes_per_fetch")),
    }
}

fn workload_table(s: &mut String, w: &str, rows: &[(&String, &RunResult)], reference: &str) {
    let Some((_, first)) = rows.first() else {
        return;
    };
    let (what, _, _) = headline(w, first);
    let params = describe_params(first);
    let _ = writeln!(s, "### {w} ({params})\n");
    let _ = writeln!(
        s,
        "| Target | Memory | Errors | {what} p50 | p90 | p99 | Throughput | Bytes per op | Bytes vs {reference} |"
    );
    let _ = writeln!(s, "|---|---|---|---|---|---|---|---|---|");
    let ref_bytes = rows
        .iter()
        .find(|(t, _)| t.as_str() == reference)
        .and_then(|(_, r)| headline(w, r).2);
    for (t, r) in rows {
        let (_, lat, b) = headline(w, r);
        let lat = lat.unwrap_or_else(|| Summary::from_millis(&[]));
        let ops = r
            .counts
            .get("fetches_ok")
            .or(r.counts.get("clones_ok"))
            .or(r.counts.get("pushes_ok"))
            .copied()
            .unwrap_or(0);
        let tput = r
            .throughput
            .iter()
            .find(|(k, _)| k.ends_with("_per_second") && !k.contains("mib"))
            .map_or_else(
                || "-".to_owned(),
                |(k, v)| {
                    format!(
                        "{v:.2} {}",
                        k.trim_end_matches("_per_second").replace('_', " ") + "/s"
                    )
                },
            );
        let ratio = match (b, ref_bytes) {
            (Some(b), Some(rb)) if rb > 0 => format!("{:.4}×", b as f64 / rb as f64),
            _ => "-".to_owned(),
        };
        let _ = writeln!(
            s,
            "| {t} | {} | {} of {} | {} | {} | {} | {tput} | {} | {ratio} |",
            memory(r),
            r.errors.count,
            ops + r.errors.count,
            ms(lat.p50),
            ms(lat.p90),
            ms(lat.p99),
            b.map_or_else(|| "-".to_owned(), bytes),
        );
    }
    s.push('\n');
    if w == "writers" {
        let _ = writeln!(
            s,
            "| Target | Memory | push p50 | p90 | p99 |\n|---|---|---|---|---|"
        );
        for (t, r) in rows {
            let p = r
                .latencies_ms
                .get("push")
                .cloned()
                .unwrap_or_else(|| Summary::from_millis(&[]));
            let _ = writeln!(
                s,
                "| {t} | {} | {} | {} | {} |",
                memory(r),
                ms(p.p50),
                ms(p.p90),
                ms(p.p99)
            );
        }
        s.push('\n');
    }
}

fn describe_params(r: &RunResult) -> String {
    let keys = [
        "writers",
        "duration_secs",
        "jobs",
        "rounds",
        "mode",
        "depth",
        "concurrency",
        "clones",
    ];
    let mut parts: Vec<String> = keys
        .iter()
        .filter_map(|k| {
            let v = r.params.get(*k)?;
            (!v.is_null()).then(|| format!("{k} {}", v.to_string().trim_matches('"')))
        })
        .collect();
    match r.params.get("repo_source_tip") {
        Some(tip) => parts.push(format!(
            "repository {}",
            &tip.to_string().trim_matches('"')[..12]
        )),
        None => {
            if let (Some(f), Some(c)) = (r.params.get("repo_files"), r.params.get("repo_commits")) {
                parts.push(format!("synthetic {f} files, {c} commits"));
            }
        }
    }
    parts.join(", ")
}

fn stale_tables(s: &mut String, rows: &[(&String, &RunResult)]) {
    let mut points: Vec<u64> = rows
        .iter()
        .flat_map(|(_, r)| {
            r.params
                .get("points")
                .and_then(|p| p.as_array())
                .cloned()
                .unwrap_or_default()
        })
        .filter_map(|v| v.as_u64())
        .collect();
    points.sort_unstable();
    points.dedup();
    let _ = writeln!(s, "### stale-branch\n");
    let _ = writeln!(
        s,
        "| Target | Memory | N | visited | loaded versions | chain skips | planning p50 | fetch p50 | bytes received |"
    );
    let _ = writeln!(s, "|---|---|---|---|---|---|---|---|---|");
    for (t, r) in rows {
        for n in &points {
            let c = |k: &str| {
                r.counts
                    .get(&format!("{k}.n{n}"))
                    .map_or_else(|| "-".to_owned(), u64::to_string)
            };
            let l = |k: &str| r.latencies_ms.get(&format!("{k}_n{n}")).and_then(|x| x.p50);
            let b = r.counts.get(&format!("received_bytes.n{n}")).copied();
            let _ = writeln!(
                s,
                "| {t} | {} | {n} | {} | {} | {} | {} | {} | {} |",
                memory(r),
                c("visited"),
                c("loaded_versions"),
                c("chain_skips"),
                ms(l("plan")),
                ms(l("stale_fetch")),
                b.map_or_else(|| "-".to_owned(), bytes)
            );
        }
    }
    s.push('\n');
    let _ = writeln!(
        s,
        "| Target | Memory | trunk pushes | push p50, first tenth | push p50, last tenth | p99, last tenth |\n|---|---|---|---|---|---|"
    );
    for (t, r) in rows {
        let p = |k: &str| {
            r.latencies_ms
                .get(k)
                .cloned()
                .unwrap_or_else(|| Summary::from_millis(&[]))
        };
        let (first, last) = (p("trunk_push_first_tenth"), p("trunk_push_last_tenth"));
        let _ = writeln!(
            s,
            "| {t} | {} | {} | {} | {} | {} |",
            memory(r),
            r.counts.get("trunk_pushes").copied().unwrap_or(0),
            ms(first.p50),
            ms(last.p50),
            ms(last.p99)
        );
    }
    s.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_campaign_renders_bytes_against_the_reference() {
        let dir = tempfile::tempdir().unwrap();
        for (t, b) in [("aspen", 110u64), ("local", 100u64)] {
            let mut r = crate::results::sample();
            r.workload = "fetch-storm".to_owned();
            r.counts.insert("received_bytes_per_fetch".to_owned(), b);
            r.counts.insert("fetches_ok".to_owned(), 2);
            r.latencies_ms
                .insert("fetch".to_owned(), Summary::from_millis(&[10.0, 30.0]));
            let d = dir.path().join("g").join(t);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("fetch-storm.json"), r.to_json_pretty()).unwrap();
        }
        let out = render(&load(dir.path()).unwrap(), "local");
        assert!(out.contains("## g"), "{out}");
        assert!(out.contains("| aspen | "), "{out}");
        assert!(out.contains(" | 0 of 2 | 10.0 ms |"), "{out}");
        assert!(out.contains("| 110 B | 1.1000× |"), "{out}");
        assert!(out.contains("| 100 B | 1.0000× |"), "{out}");
    }

    /// One campaign can mix machine sizes (D2's ran at 8.35 GB, then
    /// 16.94 GB): every table shows each row's own memory, and `-` for a
    /// result that didn't record it.
    #[test]
    fn every_row_shows_the_memory_of_its_own_machine() {
        let dir = tempfile::tempdir().unwrap();
        let sizes = [
            ("aspen", Some(8_349_429_760u64)),
            ("local", Some(16_939_364_352u64)),
            ("other", None),
        ];
        for (t, mem) in sizes {
            let d = dir.path().join("g").join(t);
            std::fs::create_dir_all(&d).unwrap();
            let mut w = crate::results::sample();
            w.workload = "writers".to_owned();
            w.environment.memory_bytes = mem;
            w.counts.insert("pushes_ok".to_owned(), 3);
            w.latencies_ms
                .insert("push".to_owned(), Summary::from_millis(&[5.0]));
            std::fs::write(d.join("writers.json"), w.to_json_pretty()).unwrap();
            let mut st = crate::results::sample();
            st.workload = "stale-branch".to_owned();
            st.environment.memory_bytes = mem;
            st.params
                .insert("points".to_owned(), serde_json::json!([10]));
            st.counts.insert("visited.n10".to_owned(), 4);
            st.counts.insert("trunk_pushes".to_owned(), 7);
            std::fs::write(d.join("stale-branch.json"), st.to_json_pretty()).unwrap();
        }
        let out = render(&load(dir.path()).unwrap(), "local");
        for (t, mem) in [("aspen", "8.35 GB"), ("local", "16.94 GB"), ("other", "-")] {
            // writers: main table, push table; stale-branch: per-N table,
            // trunk-push table.
            for row in [
                format!("| {t} | {mem} | 0 of 3 |"),
                format!("| {t} | {mem} | 5.0 ms |"),
                format!("| {t} | {mem} | 10 | 4 |"),
                format!("| {t} | {mem} | 7 |"),
            ] {
                assert!(out.contains(&row), "missing {row:?} in\n{out}");
            }
        }
    }
}

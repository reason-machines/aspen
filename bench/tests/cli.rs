//! End-to-end smoke of the `aspen-bench` binary against the in-process
//! `git http-backend` bridge (`--url local`). Runs every workload at a tiny
//! size, so it takes seconds and belongs in required CI; real benchmark runs
//! are nightly (see `.github/workflows/nightly.yml`).

use std::path::Path;
use std::process::Command;

use serde_json::Value;

fn bench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_aspen-bench"))
}

fn run_ok(cmd: &mut Command) -> String {
    let out = cmd.output().expect("spawn aspen-bench");
    assert!(
        out.status.success(),
        "{cmd:?} failed with {}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn load(path: &Path) -> Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn p50(v: &Value, latency: &str) -> f64 {
    v["latencies_ms"][latency]["p50"]
        .as_f64()
        .unwrap_or_else(|| panic!("no p50 for {latency}: {v:#}"))
}

#[test]
fn every_workload_runs_against_git_http_backend() {
    let tmp = tempfile::tempdir().unwrap();
    let out = |name: &str| tmp.path().join(name);
    let common = ["--url", "local", "--repo-size", "tiny", "--fail-on-errors"];

    run_ok(
        bench()
            .arg("writers")
            .args(common)
            .args(["--writers", "2", "--pushes-per-writer", "3", "--out"])
            .arg(out("writers.json")),
    );
    let w = load(&out("writers.json"));
    assert_eq!(w["schema"], "aspen-bench.result/v1");
    assert_eq!(w["workload"], "writers");
    assert_eq!(w["target"]["label"], "local-git-http-backend");
    assert_eq!(w["counts"]["pushes_ok"], 6);
    assert_eq!(w["counts"]["seen"], 6);
    assert_eq!(w["errors"]["count"], 0);
    assert_eq!(w["latencies_ms"]["push_to_fetch"]["count"], 6);
    assert!(p50(&w, "push") > 0.0);
    // A reader's view includes its own fetch, so it can't beat the fetch.
    assert!(p50(&w, "push_to_fetch") >= w["latencies_ms"]["reader_fetch"]["min"].as_f64().unwrap());
    assert_eq!(w["params"]["writers"], 2);
    assert_eq!(w["params"]["repo_files"], 20);
    assert!(
        w["environment"]["git_version"]
            .as_str()
            .unwrap()
            .starts_with("git version")
    );

    for mode in ["warm", "cold"] {
        let path = out(&format!("storm-{mode}.json"));
        run_ok(
            bench()
                .arg("fetch-storm")
                .args(common)
                .args(["--jobs", "3", "--rounds", "2", "--mode", mode, "--out"])
                .arg(&path),
        );
        let s = load(&path);
        assert_eq!(s["counts"]["fetches_ok"], 6, "{mode}");
        assert!(
            s["counts"]["received_bytes"].as_u64().unwrap() > 0,
            "{mode}"
        );
        assert_eq!(s["params"]["mode"], mode);
    }

    // Depth-1 storm: shallow clones receive less than full ones.
    let shallow = out("storm-depth1.json");
    run_ok(
        bench()
            .arg("fetch-storm")
            .args(common)
            .args([
                "--jobs", "2", "--rounds", "1", "--mode", "cold", "--depth", "1", "--out",
            ])
            .arg(&shallow),
    );
    let d = load(&shallow);
    assert_eq!(d["counts"]["fetches_ok"], 2);
    assert_eq!(d["params"]["depth"], 1);
    let full = load(&out("storm-cold.json"));
    assert!(
        d["counts"]["received_bytes_per_fetch"].as_u64().unwrap()
            < full["counts"]["received_bytes_per_fetch"].as_u64().unwrap(),
        "depth 1 {d:#} vs full {full:#}"
    );

    run_ok(
        bench()
            .arg("clone")
            .args(common)
            .args(["--concurrency", "2", "--clones", "3", "--out"])
            .arg(out("clone.json")),
    );
    let c = load(&out("clone.json"));
    assert_eq!(c["counts"]["clones_ok"], 3);
    assert!(c["throughput"]["received_mib_per_second"].as_f64().unwrap() > 0.0);

    // The stale-branch fetch: 4 trunk pushes, branches 2 and 4 pushes old.
    let campaign = tmp.path().join("campaign");
    let stale = campaign.join("g").join("local").join("stale-branch.json");
    run_ok(
        bench()
            .arg("stale-branch")
            .args(common)
            .args(["--points", "4,2", "--repeats", "2", "--out"])
            .arg(&stale),
    );
    let st = load(&stale);
    assert_eq!(st["workload"], "stale-branch");
    assert_eq!(st["counts"]["trunk_pushes"], 4);
    assert_eq!(st["counts"]["fetches_ok"], 4);
    assert_eq!(st["params"]["points"], serde_json::json!([2, 4]));
    for n in [2, 4] {
        assert_eq!(st["latencies_ms"][format!("stale_fetch_n{n}")]["count"], 2);
        // The branch's one commit, tree and blob, not the history.
        let got = st["counts"][format!("received_bytes.n{n}")]
            .as_u64()
            .unwrap();
        assert!(got > 0 && got < 4096, "N={n}: {got} bytes");
    }
    let report = run_ok(bench().arg("report").arg(&campaign));
    assert!(report.contains("### stale-branch"), "{report}");
    // Target, then this machine's memory, then N with no planner counters.
    assert!(
        report
            .lines()
            .any(|l| l.starts_with("| local | ") && l.contains(" | 4 | - | - | - |")),
        "{report}"
    );

    // A run compared with itself passes the regression checker.
    let report = run_ok(
        bench()
            .arg("compare")
            .arg("--baseline")
            .arg(out("writers.json"))
            .arg("--current")
            .arg(out("writers.json"))
            .arg("--tolerances")
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tolerances.json")),
    );
    assert!(report.contains("no regressions"), "{report}");
}

#[test]
fn compare_exits_nonzero_on_a_regression_and_on_incompatible_runs() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("base.json");
    run_ok(
        bench()
            .args([
                "clone",
                "--url",
                "local",
                "--repo-size",
                "tiny",
                "--clones",
                "1",
                "--concurrency",
                "1",
                "--out",
            ])
            .arg(&path),
    );
    let base = load(&path);

    // Ten times slower clones and a tenth of the throughput.
    let mut slow = base.clone();
    for f in ["p50", "p90", "p99"] {
        let v = slow["latencies_ms"]["clone"][f].as_f64().unwrap();
        slow["latencies_ms"]["clone"][f] = (v * 10.0 + 1000.0).into();
    }
    let t = slow["throughput"]["clones_per_second"].as_f64().unwrap();
    slow["throughput"]["clones_per_second"] = (t / 10.0).into();
    let slow_path = tmp.path().join("slow.json");
    std::fs::write(&slow_path, slow.to_string()).unwrap();
    let out = bench()
        .arg("compare")
        .arg("--baseline")
        .arg(&path)
        .arg("--current")
        .arg(&slow_path)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let report = String::from_utf8_lossy(&out.stdout);
    assert!(report.contains("Regressed"), "{report}");

    // Different parameters: refused unless explicitly allowed.
    let mut other = base.clone();
    other["params"]["clones"] = 99.into();
    let other_path = tmp.path().join("other.json");
    std::fs::write(&other_path, other.to_string()).unwrap();
    let refused = bench()
        .arg("compare")
        .arg("--baseline")
        .arg(&path)
        .arg("--current")
        .arg(&other_path)
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(1));
    run_ok(
        bench()
            .arg("compare")
            .arg("--baseline")
            .arg(&path)
            .arg("--current")
            .arg(&other_path)
            .arg("--allow-incompatible"),
    );
}

#[test]
fn generate_is_deterministic_across_processes() {
    let tmp = tempfile::tempdir().unwrap();
    let a = run_ok(
        bench()
            .args([
                "generate",
                "--repo-size",
                "tiny",
                "--repo-seed",
                "5",
                "--out",
            ])
            .arg(tmp.path().join("a.git")),
    );
    let b = run_ok(
        bench()
            .args([
                "generate",
                "--repo-size",
                "tiny",
                "--repo-seed",
                "5",
                "--out",
            ])
            .arg(tmp.path().join("b.git")),
    );
    assert_eq!(a, b);
    assert_eq!(a.trim().len(), 40);
}

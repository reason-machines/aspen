//! Repository maintenance tasks. Run `cargo xtask help` for the list.
//!
//! `check-deps` enforces the crate layering from section 2 of `docs/PLAN.md`:
//!
//! 1. `aspen-core` depends on no other workspace crate.
//! 2. `aspen-planner` and `aspen-auth` are pure: their only workspace
//!    dependency is `aspen-core`. `aspen-express` (node C2) does I/O but
//!    serves blobs only through the `aspen-core` traits, so its only
//!    workspace dependency is `aspen-core` too. `aspen-mirror` (node C4)
//!    is a Git client of upstream servers: besides `aspen-core` it may use
//!    only `aspen-proto` (pkt-lines, side-band), and it reaches Aspen's own
//!    storage and push path through traits the server implements.
//!    `aspen-compact` (node C1) writes compacted packs and runs garbage
//!    collection through the `aspen-core` traits: besides `aspen-core` it
//!    may use only `aspen-pack` (pack reading and writing).
//! 3. `aspen-server` and `aspen-cli` are the composition roots of Aspen
//!    itself, and `aspen-place` (the `place` demo's back end, node C7) is an
//!    application built on them. No other crate may depend on a root, and
//!    only roots may depend on the concrete storage backends (`aspen-meta`,
//!    `aspen-blob`); everything else goes through the traits in
//!    `aspen-core`. Nothing but tooling may depend on an application, not
//!    even another root.
//! 4. Product crates (`aspen-*` libraries and binaries) never depend on
//!    test-only or tooling crates.
//!
//! Only normal and build dependencies count. Dev-dependencies are free, so a
//! crate's tests may use the test kit or a real backend.
//!
//! `check-workflows` enforces the GitHub Actions supply-chain rules (issue
//! #122); see `workflows.rs`.

mod workflows;

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, ExitCode};

use serde_json::Value;

const CORE: &str = "aspen-core";
const PURE: &[&str] = &["aspen-planner", "aspen-auth"];
/// Service crates that are not pure but still use only the `aspen-core`
/// traits, so a composition root can give them any backend.
const CORE_ONLY: &[&str] = &["aspen-express"];
/// Clients of other Git servers: `aspen-core` plus the wire format only.
const PROTOCOL_CLIENTS: &[&str] = &["aspen-mirror"];
const PROTO: &str = "aspen-proto";
/// Background workers over stored packs: `aspen-core` plus the pack
/// format only.
const PACK_WORKERS: &[&str] = &["aspen-compact"];
const PACK: &str = "aspen-pack";
const COMPOSITION_ROOTS: &[&str] = &["aspen-server", "aspen-cli", "aspen-place"];
/// Applications built on Aspen (also composition roots): nothing but
/// tooling may depend on them, not even another root.
const APPLICATIONS: &[&str] = &["aspen-place"];
const BACKENDS: &[&str] = &["aspen-meta", "aspen-blob"];
const TOOLING: &[&str] = &[
    "aspen-bench",
    "aspen-chaos",
    "aspen-fuzz",
    "aspen-interop",
    "aspen-testkit",
    "xtask",
];

fn main() -> ExitCode {
    let task = std::env::args().nth(1);
    match task.as_deref() {
        Some("check-deps") => check_deps(),
        Some("check-workflows") => check_workflows(),
        Some("help") | None => {
            eprintln!(
                "usage: cargo xtask <task>\n\ntasks:\n  check-deps       enforce the crate layering rules\n  check-workflows  pin GitHub Actions by commit and verify downloaded tools"
            );
            ExitCode::SUCCESS
        }
        Some(other) => {
            eprintln!("unknown task `{other}`; run `cargo xtask help`");
            ExitCode::FAILURE
        }
    }
}

fn check_deps() -> ExitCode {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let output = match Command::new(cargo)
        .args(["metadata", "--format-version", "1", "--no-deps", "--locked"])
        .output()
    {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            eprintln!(
                "cargo metadata failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return ExitCode::FAILURE;
        }
        Err(err) => {
            eprintln!("could not run cargo metadata: {err}");
            return ExitCode::FAILURE;
        }
    };
    let metadata: Value = match serde_json::from_slice(&output.stdout) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("cargo metadata printed invalid JSON: {err}");
            return ExitCode::FAILURE;
        }
    };
    let graph = workspace_graph(&metadata);
    let violations = violations(&graph);
    if violations.is_empty() {
        println!("check-deps: {} workspace crates, layering OK", graph.len());
        ExitCode::SUCCESS
    } else {
        for v in &violations {
            eprintln!("check-deps: {v}");
        }
        eprintln!("see the dependency rule in docs/PLAN.md section 2");
        ExitCode::FAILURE
    }
}

fn check_workflows() -> ExitCode {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in the repository root");
    match workflows::check_repo(root) {
        Ok((files, violations)) if violations.is_empty() => {
            println!(
                "check-workflows: {files} workflow files, actions pinned, no unchecked downloads"
            );
            ExitCode::SUCCESS
        }
        Ok((_, violations)) => {
            for v in &violations {
                eprintln!("check-workflows: {v}");
            }
            eprintln!("see xtask/src/workflows.rs for the rules (issue #122)");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("check-workflows: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Workspace crate name to the workspace crates it uses as normal or build
/// dependencies.
type Graph = BTreeMap<String, Vec<String>>;

fn workspace_graph(metadata: &Value) -> Graph {
    let packages = metadata["packages"].as_array().cloned().unwrap_or_default();
    let names: Vec<String> = packages
        .iter()
        .filter_map(|p| p["name"].as_str().map(str::to_owned))
        .collect();
    packages
        .iter()
        .filter_map(|p| {
            let name = p["name"].as_str()?.to_owned();
            let deps = p["dependencies"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|d| d["kind"].as_str() != Some("dev"))
                .filter_map(|d| d["name"].as_str())
                .filter(|d| names.iter().any(|n| n == d))
                .map(str::to_owned)
                .collect();
            Some((name, deps))
        })
        .collect()
}

fn violations(graph: &Graph) -> Vec<String> {
    let mut out = Vec::new();
    for (krate, deps) in graph {
        let is_tooling = TOOLING.contains(&krate.as_str());
        let is_root = COMPOSITION_ROOTS.contains(&krate.as_str());
        for dep in deps {
            let dep = dep.as_str();
            if krate == CORE {
                out.push(format!(
                    "{CORE} must not depend on any workspace crate, but depends on {dep}"
                ));
            } else if PURE.contains(&krate.as_str()) && dep != CORE {
                out.push(format!(
                    "{krate} is pure and may only depend on {CORE}, but depends on {dep}"
                ));
            } else if CORE_ONLY.contains(&krate.as_str()) && dep != CORE {
                out.push(format!(
                    "{krate} serves blobs through the {CORE} traits and may only depend on \
                     {CORE}, but depends on {dep}"
                ));
            } else if PROTOCOL_CLIENTS.contains(&krate.as_str()) && dep != CORE && dep != PROTO {
                out.push(format!(
                    "{krate} is a Git client and may only depend on {CORE} and {PROTO} (reach \
                     Aspen's storage through traits the server implements), but depends on {dep}"
                ));
            } else if PACK_WORKERS.contains(&krate.as_str()) && dep != CORE && dep != PACK {
                out.push(format!(
                    "{krate} works on stored packs through the {CORE} traits and may only depend \
                     on {CORE} and {PACK}, but depends on {dep}"
                ));
            }
            if APPLICATIONS.contains(&dep) && !is_tooling {
                out.push(format!(
                    "{krate} depends on application {dep}; applications are built on Aspen, \
                     never the other way round"
                ));
            } else if COMPOSITION_ROOTS.contains(&dep) && !is_root && !is_tooling {
                out.push(format!(
                    "{krate} depends on composition root {dep}; only tooling and other roots may"
                ));
            }
            if BACKENDS.contains(&dep) && !is_root && !is_tooling {
                out.push(format!(
                    "{krate} depends on storage backend {dep}; use the {CORE} traits instead, \
                     and let aspen-server or aspen-cli wire the backend in"
                ));
            }
            if TOOLING.contains(&dep) && !is_tooling {
                out.push(format!(
                    "product crate {krate} depends on tooling crate {dep}; \
                     make it a dev-dependency"
                ));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph(edges: &[(&str, &[&str])]) -> Graph {
        edges
            .iter()
            .map(|(k, ds)| {
                (
                    (*k).to_owned(),
                    ds.iter().map(|d| (*d).to_owned()).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn planned_layering_is_accepted() {
        let g = graph(&[
            ("aspen-core", &[]),
            ("aspen-proto", &["aspen-core"]),
            ("aspen-pack", &["aspen-core"]),
            ("aspen-meta", &["aspen-core"]),
            ("aspen-blob", &["aspen-core"]),
            ("aspen-planner", &["aspen-core"]),
            ("aspen-express", &["aspen-core"]),
            ("aspen-mirror", &["aspen-core", "aspen-proto"]),
            ("aspen-compact", &["aspen-core", "aspen-pack"]),
            (
                "aspen-server",
                &[
                    "aspen-core",
                    "aspen-proto",
                    "aspen-meta",
                    "aspen-blob",
                    "aspen-express",
                ],
            ),
            ("aspen-cli", &["aspen-core", "aspen-server", "aspen-meta"]),
            (
                "aspen-place",
                &["aspen-core", "aspen-server", "aspen-meta", "aspen-blob"],
            ),
            ("aspen-testkit", &["aspen-server", "aspen-meta"]),
            ("aspen-bench", &["aspen-interop"]),
            ("xtask", &[]),
        ]);
        assert_eq!(violations(&g), Vec::<String>::new());
    }

    #[test]
    fn core_with_any_internal_dependency_is_rejected() {
        let v = violations(&graph(&[("aspen-core", &["aspen-proto"])]));
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("aspen-core must not depend"));
    }

    #[test]
    fn impure_planner_is_rejected() {
        let v = violations(&graph(&[("aspen-planner", &["aspen-core", "aspen-pack"])]));
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("pure"));
    }

    #[test]
    fn impure_auth_is_rejected() {
        let v = violations(&graph(&[("aspen-auth", &["aspen-core", "aspen-proto"])]));
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("aspen-auth is pure"));
    }

    #[test]
    fn express_beyond_core_is_rejected() {
        let v = violations(&graph(&[("aspen-express", &["aspen-core", "aspen-pack"])]));
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("aspen-express serves blobs"), "{v:?}");
        let v = violations(&graph(&[("aspen-express", &["aspen-core", "aspen-blob"])]));
        assert_eq!(v.len(), 2, "{v:?}");
    }

    #[test]
    fn mirror_beyond_core_and_proto_is_rejected() {
        let v = violations(&graph(&[(
            "aspen-mirror",
            &["aspen-core", "aspen-proto", "aspen-pack"],
        )]));
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("aspen-mirror is a Git client"), "{v:?}");
        let v = violations(&graph(&[("aspen-mirror", &["aspen-core", "aspen-server"])]));
        assert_eq!(v.len(), 2, "{v:?}");
    }

    #[test]
    fn library_using_a_backend_or_root_is_rejected() {
        let v = violations(&graph(&[(
            "aspen-pack",
            &["aspen-core", "aspen-meta", "aspen-server"],
        )]));
        assert_eq!(v.len(), 2, "{v:?}");
    }

    #[test]
    fn compaction_beyond_core_and_pack_is_rejected() {
        for dep in ["aspen-planner", "aspen-proto", "aspen-express"] {
            let v = violations(&graph(&[(
                "aspen-compact",
                &["aspen-core", "aspen-pack", dep],
            )]));
            assert_eq!(v.len(), 1, "{v:?}");
            assert!(
                v[0].contains("aspen-compact works on stored packs"),
                "{v:?}"
            );
        }
        // A backend breaks two rules: the compaction one and the backend one.
        let v = violations(&graph(&[("aspen-compact", &["aspen-core", "aspen-meta"])]));
        assert_eq!(v.len(), 2, "{v:?}");
    }

    #[test]
    fn nothing_may_depend_on_the_place_application() {
        for krate in ["aspen-server", "aspen-cli", "aspen-pack"] {
            let v = violations(&graph(&[(krate, &["aspen-core", "aspen-place"])]));
            assert_eq!(v.len(), 1, "{v:?}");
            assert!(v[0].contains("depends on application aspen-place"), "{v:?}");
        }
        assert_eq!(
            violations(&graph(&[("aspen-testkit", &["aspen-place"])])),
            Vec::<String>::new()
        );
    }

    #[test]
    fn product_crate_using_tooling_is_rejected() {
        let v = violations(&graph(&[("aspen-server", &["aspen-testkit"])]));
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("dev-dependency"));
    }

    #[test]
    fn dev_dependencies_are_ignored_when_reading_metadata() {
        let metadata = serde_json::json!({
            "packages": [
                { "name": "aspen-core", "dependencies": [
                    { "name": "aspen-testkit", "kind": "dev" },
                    { "name": "serde", "kind": null }
                ]},
                { "name": "aspen-testkit", "dependencies": [
                    { "name": "aspen-core", "kind": null }
                ]}
            ]
        });
        let g = workspace_graph(&metadata);
        assert_eq!(g["aspen-core"], Vec::<String>::new());
        assert_eq!(g["aspen-testkit"], vec!["aspen-core".to_owned()]);
    }
}

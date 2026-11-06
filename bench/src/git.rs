//! Running stock `git` as the load-generating client.
//!
//! Every `git` process is isolated from the machine's own configuration
//! (`HOME` is a scratch directory, system and global config are ignored), so
//! a run measures the same client behaviour on every machine. Credentials
//! never appear on a command line: [`Remote::parse`] strips them from the URL
//! and they are passed as a URL-scoped `http.<url>.extraHeader` through
//! `GIT_CONFIG_*` environment variables.
//!
//! The header is scoped to the credential-free repository URL because git
//! sends a plain `http.extraHeader` on every HTTP request its process tree
//! makes, including the `git http-fetch` it spawns for `packfile-uri`
//! downloads. Unscoped, the target's credential would reach a CDN or object
//! store (and S3 rejects a presigned GET that also carries `Authorization`).
//! Git's `http.<url>.*` matching applies the scoped header to the repository
//! URL and every path below it, such as the branch-scoped push URL
//! `<repo>/-/refs/heads/<branch>`, and to nothing else.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// Identity used for every commit the tool creates.
pub const AUTHOR: &str = "Aspen Bench <bench@aspen.invalid>";

/// A Git HTTP repository URL, split into a credential-free URL and an
/// optional `Authorization` header value.
#[derive(Clone)]
pub struct Remote {
    /// The URL without user information; safe to print and store.
    pub url: String,
    auth_header: Option<String>,
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("url", &self.url)
            .field("auth", &self.auth_header.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Remote {
    /// Parses `scheme://[user[:password]@]host/path`. User and password may
    /// be percent-encoded.
    pub fn parse(url: &str) -> Result<Self, String> {
        let Some((scheme, rest)) = url.split_once("://") else {
            return Err("--url is not a URL: expected scheme://host/path".to_owned());
        };
        let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
        let Some((userinfo, host)) = authority.rsplit_once('@') else {
            return Ok(Self {
                url: url.to_owned(),
                auth_header: None,
            });
        };
        let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
        let credentials = format!("{}:{}", percent_decode(user)?, percent_decode(password)?);
        Ok(Self {
            url: format!("{scheme}://{host}{path}"),
            auth_header: Some(format!("Basic {}", base64(credentials.as_bytes()))),
        })
    }
}

impl Remote {
    /// The `GIT_CONFIG_*` key and value that send the credential to this
    /// repository only.
    fn auth_config(&self) -> Option<(String, String)> {
        let header = self.auth_header.as_ref()?;
        let value = format!("Authorization: {header}");
        Some((auth_header_key(&self.url), value))
    }
}

impl Remote {
    /// The refs under `prefixes` (oid, name), from one protocol v2 `ls-refs`
    /// request whose body (from [`ls_refs_request`]) is the file `body`.
    /// Sent with `curl`: `git ls-remote <url> <ref>` sends no `ref-prefix`,
    /// so the server would list every ref of the repository. The
    /// credential goes to curl on stdin, never in its arguments.
    pub fn ls_refs(&self, body: &Path) -> Result<Vec<(String, String)>, String> {
        let quote = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        let mut config = format!(
            "url = \"{}/git-upload-pack\"\nheader = \"Git-Protocol: version=2\"\n\
             header = \"Content-Type: application/x-git-upload-pack-request\"\n\
             header = \"Accept: application/x-git-upload-pack-result\"\n\
             data-binary = \"@{}\"\nsilent\nshow-error\nfail\n",
            quote(self.url.trim_end_matches('/')),
            quote(&body.to_string_lossy()),
        );
        if let Some(h) = &self.auth_header {
            config.push_str(&format!("header = \"Authorization: {}\"\n", quote(h)));
        }
        let mut child = Command::new("curl")
            .args(["-K", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawning curl: {e}"))?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(config.as_bytes())
                .map_err(|e| format!("writing to curl: {e}"))?;
        }
        let out = child
            .wait_with_output()
            .map_err(|e| format!("waiting for curl: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "ls-refs failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        let mut refs = Vec::new();
        let mut rest = &out.stdout[..];
        while rest.len() >= 4 {
            let len = std::str::from_utf8(&rest[..4])
                .ok()
                .and_then(|h| usize::from_str_radix(h, 16).ok())
                .ok_or("ls-refs: a malformed pkt-line")?;
            if len < 4 {
                rest = &rest[4..];
                continue;
            }
            if len > rest.len() {
                return Err("ls-refs: a truncated answer".to_owned());
            }
            let line = String::from_utf8_lossy(&rest[4..len]);
            if let Some((oid, name)) = line.trim_end().split_once(' ') {
                refs.push((
                    oid.to_owned(),
                    name.split(' ').next().unwrap_or(name).to_owned(),
                ));
            }
            rest = &rest[len..];
        }
        Ok(refs)
    }
}

/// The body of a protocol v2 `ls-refs` request for the refs under
/// `prefixes`.
pub fn ls_refs_request(prefixes: &[&str]) -> Vec<u8> {
    fn pkt(out: &mut Vec<u8>, line: &str) {
        out.extend_from_slice(format!("{:04x}{line}", line.len() + 4).as_bytes());
    }
    let mut out = Vec::new();
    pkt(&mut out, "command=ls-refs\n");
    out.extend_from_slice(b"0001");
    for p in prefixes {
        pkt(&mut out, &format!("ref-prefix {p}\n"));
    }
    out.extend_from_slice(b"0000");
    out
}

/// The URL with any user information replaced, for error messages.
pub fn redact(url: &str) -> String {
    match Remote::parse(url) {
        Ok(r) => r.url,
        Err(_) => "<unparseable url>".to_owned(),
    }
}

fn percent_decode(s: &str) -> Result<String, String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| "bad percent-encoding in URL credentials".to_owned())?;
            out.push(hex);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "URL credentials are not UTF-8".to_owned())
}

/// Standard base64 with padding (RFC 4648), for the Basic auth header.
pub fn base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(TABLE[((n >> shift) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Factory for isolated `git` commands.
#[derive(Debug, Clone)]
pub struct Git {
    home: PathBuf,
    protocol: u8,
    remote: Option<Remote>,
}

impl Git {
    /// `home` must exist; it becomes `$HOME` for every command.
    pub fn new(home: &Path, protocol: u8) -> Self {
        Self {
            home: home.to_path_buf(),
            protocol,
            remote: None,
        }
    }

    /// Commands from the returned factory authenticate to `remote`.
    pub fn with_remote(&self, remote: &Remote) -> Self {
        Self {
            remote: Some(remote.clone()),
            ..self.clone()
        }
    }

    /// A `git` command running in `dir`.
    pub fn cmd(&self, dir: &Path) -> Command {
        let mut cmd = Command::new("git");
        cmd.current_dir(dir)
            .env("HOME", &self.home)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", "true")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_PROTOCOL")
            .env_remove("GIT_TRACE")
            .env_remove("GIT_TRACE_PACKET")
            // Set by a parent `git -c ...` (for example when run from a hook).
            .env_remove("GIT_CONFIG_PARAMETERS");
        // Settings that keep client-side housekeeping out of the timings.
        let mut config: Vec<(String, String)> = [
            ("protocol.version", self.protocol.to_string()),
            ("gc.auto", "0".to_owned()),
            ("maintenance.auto", "false".to_owned()),
            ("fetch.writeCommitGraph", "false".to_owned()),
            // Always keep received packs as packs, so received bytes can be
            // measured from pack sizes.
            ("transfer.unpackLimit", "1".to_owned()),
            ("advice.detachedHead", "false".to_owned()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();
        config.extend(self.remote.as_ref().and_then(Remote::auth_config));
        cmd.env("GIT_CONFIG_COUNT", config.len().to_string());
        for (i, (k, v)) in config.into_iter().enumerate() {
            cmd.env(format!("GIT_CONFIG_KEY_{i}"), k)
                .env(format!("GIT_CONFIG_VALUE_{i}"), v);
        }
        cmd
    }
}

/// The config key that sends the auth header only to `repo_url` and paths
/// below it (git's `http.<url>.*` matching), never to other hosts.
fn auth_header_key(repo_url: &str) -> String {
    format!("http.{}.extraHeader", repo_url.trim_end_matches('/'))
}

/// Runs `cmd`, returning trimmed stdout, or an error with the exit status and
/// the tail of stderr.
pub fn run(cmd: &mut Command) -> Result<String, String> {
    let out = cmd
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("spawning {}: {e}", describe(cmd)))?;
    check(cmd, out)
}

/// Like [`run`], and also returns how long the process took.
pub fn run_timed(cmd: &mut Command) -> (Duration, Result<String, String>) {
    let start = Instant::now();
    let res = run(cmd);
    (start.elapsed(), res)
}

/// Runs `cmd` with `input` on stdin.
pub fn run_with_input(cmd: &mut Command, input: &[u8]) -> Result<String, String> {
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning {}: {e}", describe(cmd)))?;
    let write_result = child.stdin.take().map(|mut s| s.write_all(input));
    let out = child
        .wait_with_output()
        .map_err(|e| format!("waiting for {}: {e}", describe(cmd)))?;
    let res = check(cmd, out);
    if let (Ok(_), Some(Err(e))) = (&res, write_result) {
        return Err(format!("writing to {}: {e}", describe(cmd)));
    }
    res
}

fn check(cmd: &Command, out: Output) -> Result<String, String> {
    if out.status.success() {
        return Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned());
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    let tail: Vec<&str> = stderr.lines().rev().take(4).collect();
    let tail: Vec<&str> = tail.into_iter().rev().collect();
    Err(format!(
        "{} failed ({}): {}",
        describe(cmd),
        out.status,
        tail.join(" | ")
    ))
}

/// `git <first two args>`, enough to identify a failing command without
/// printing URLs.
fn describe(cmd: &Command) -> String {
    let args: Vec<String> = cmd
        .get_args()
        .take(2)
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    format!("git {}", args.join(" "))
}

/// `git version` output, for the environment metadata.
pub fn version() -> String {
    Command::new("git")
        .arg("version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".to_owned(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_owned(),
        )
}

/// Whether `git version` output such as `git version 2.43.0` (or
/// `git version 2.39.5 (Apple Git-154)`) is at least `min` (major, minor).
pub fn version_at_least(version: &str, min: (u32, u32)) -> bool {
    let Some(number) = version.split_whitespace().nth(2) else {
        return false;
    };
    let mut parts = number.split('.').map(|p| p.parse::<u32>().ok());
    match (parts.next().flatten(), parts.next().flatten()) {
        (Some(major), Some(minor)) => (major, minor) >= min,
        _ => false,
    }
}

/// Total size of the pack files in a repository's object directory
/// (`<git_dir>/objects/pack/*.pack`).
pub fn pack_bytes(git_dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(git_dir.join("objects/pack")) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "pack"))
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

/// Creates an empty bare repository at `path`.
pub fn init_bare(git: &Git, path: &Path) -> Result<(), String> {
    let parent = path.parent().unwrap_or(Path::new("."));
    run(git
        .cmd(parent)
        .args(["init", "--quiet", "--bare", "--initial-branch=main"])
        .arg(path))
    .map(drop)
}

/// A bare repository that borrows `source`'s objects through alternates and
/// has its `main` branch. Cheap to make, so every simulated client gets its
/// own.
pub fn shared_clone(git: &Git, source: &Path, dest: &Path) -> Result<(), String> {
    let parent = dest.parent().unwrap_or(Path::new("."));
    run(git
        .cmd(parent)
        .args(["clone", "--quiet", "--bare", "--shared", "--no-tags"])
        .arg(source)
        .arg(dest))
    .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_rfc4648_vectors() {
        let cases = [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ];
        for (input, want) in cases {
            assert_eq!(base64(input.as_bytes()), want, "{input:?}");
        }
    }

    #[test]
    fn credentials_are_moved_out_of_the_url() {
        let r = Remote::parse("http://bench:p%40ss@127.0.0.1:3000/bench/repo.git").unwrap();
        assert_eq!(r.url, "http://127.0.0.1:3000/bench/repo.git");
        assert_eq!(
            r.auth_header.as_deref(),
            Some(format!("Basic {}", base64(b"bench:p@ss")).as_str())
        );
        assert!(!format!("{r:?}").contains("Basic"));
    }

    #[test]
    fn plain_urls_are_untouched() {
        let r = Remote::parse("https://git.example.com/a/b.git").unwrap();
        assert_eq!(r.url, "https://git.example.com/a/b.git");
        assert!(r.auth_header.is_none());
        assert!(Remote::parse("no-scheme").is_err());
        // An @ in the path is not user information.
        let r = Remote::parse("http://h/a@b.git").unwrap();
        assert_eq!(r.url, "http://h/a@b.git");
    }

    #[test]
    fn version_comparison_reads_git_version_output() {
        assert!(version_at_least("git version 2.43.0", (2, 31)));
        assert!(version_at_least("git version 2.31.1", (2, 31)));
        assert!(version_at_least("git version 3.0.0", (2, 31)));
        assert!(version_at_least(
            "git version 2.39.5 (Apple Git-154)",
            (2, 31)
        ));
        assert!(!version_at_least("git version 2.30.9", (2, 31)));
        assert!(!version_at_least("unknown", (2, 31)));
    }

    #[test]
    fn redact_hides_passwords() {
        assert_eq!(redact("http://u:secret@h/r.git"), "http://h/r.git");
    }

    #[test]
    fn git_commands_carry_isolated_config() {
        let remote = Remote::parse("http://u:p@h/r.git").unwrap();
        let git = Git::new(Path::new("/tmp"), 2).with_remote(&remote);
        let cmd = git.cmd(Path::new("/tmp"));
        let envs: Vec<(String, String)> = cmd
            .get_envs()
            .filter_map(|(k, v)| Some((k.to_str()?.to_owned(), v?.to_str()?.to_owned())))
            .collect();
        let get = |k: &str| envs.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
        assert_eq!(get("GIT_CONFIG_GLOBAL").as_deref(), Some("/dev/null"));
        let n: usize = get("GIT_CONFIG_COUNT").unwrap().parse().unwrap();
        let pairs: Vec<(String, String)> = (0..n)
            .map(|i| {
                (
                    get(&format!("GIT_CONFIG_KEY_{i}")).unwrap(),
                    get(&format!("GIT_CONFIG_VALUE_{i}")).unwrap(),
                )
            })
            .collect();
        assert!(pairs.contains(&("protocol.version".to_owned(), "2".to_owned())));
        // The auth header is scoped to the repository URL ...
        let scoped = pairs
            .iter()
            .find(|(k, _)| k == "http.http://h/r.git.extraHeader");
        let (_, value) = scoped.expect("a URL-scoped auth header");
        assert!(value.starts_with("Authorization: Basic "));
        // ... and never set unscoped, which git would send to every host.
        assert!(
            pairs
                .iter()
                .all(|(k, _)| !k.eq_ignore_ascii_case("http.extraHeader"))
        );
        // A parent `git -c` cannot leak configuration in.
        let removed: Vec<_> = cmd.get_envs().filter(|(_, v)| v.is_none()).collect();
        assert!(removed.iter().any(|(k, _)| *k == "GIT_CONFIG_PARAMETERS"));
        // Credentials are never arguments.
        assert!(cmd.get_args().all(|a| !a.to_string_lossy().contains("u:p")));
    }

    /// Asks real `git` which `http.extraHeader` applies to each URL, using
    /// exactly the environment the benchmark's clients get.
    #[test]
    fn auth_header_reaches_only_the_repository_urls() {
        let home = tempfile::tempdir().unwrap();
        let remote = Remote::parse("http://u:p@127.0.0.1:3000/org/repo.git").unwrap();
        let git = Git::new(home.path(), 2).with_remote(&remote);
        let applies = |url: &str| {
            let mut cmd = git.cmd(home.path());
            cmd.args(["config", "--get-urlmatch", "http.extraHeader", url]);
            cmd.output().unwrap().status.success()
        };
        let repo = "http://127.0.0.1:3000/org/repo.git";
        assert!(applies(&format!("{repo}/info/refs")));
        assert!(applies(&format!("{repo}/git-upload-pack")));
        let scoped = format!("{repo}/-/refs/heads/bench/w0/git-receive-pack");
        assert!(applies(&scoped));
        // packfile-uri downloads and other repositories get no credential.
        assert!(!applies("https://d111.cloudfront.net/packs/p.pack"));
        assert!(!applies("https://bucket.s3.amazonaws.com/p.pack"));
        assert!(!applies("http://127.0.0.1:3001/org/repo.git/info/refs"));
        assert!(!applies("http://127.0.0.1:3000/org/repo.git2/info/refs"));
        assert!(!applies("http://127.0.0.1:3000/org/other.git/info/refs"));
    }
}

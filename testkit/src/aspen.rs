//! A real Aspen server as a [`GitServer`]: the `aspen` binary, spawned as a
//! child process running `aspen serve`.
//!
//! [`AspenServer`] is how the differential oracle and the client matrix test
//! Aspen itself rather than stock Git in Aspen's URL layout
//! ([`ScopedServer`](crate::ScopedServer)). It runs the shipped binary with
//! the same environment contract as any deployment (`deploy/README.md`):
//! metadata in SQLite (a file in a temporary directory) or in a PostgreSQL
//! database given by URL, packs in a local blob store, and an anonymous
//! `write:*` policy, since the oracle pushes without credentials. The
//! compaction worker and exact shallow packs run as they do in production.
//!
//! One setting differs from a deployment's defaults: the per-caller caps on
//! concurrent work are off ([`ORACLE_LIMITS`]). Every oracle client is the
//! same anonymous caller (`127.0.0.1`), and stock `git http-backend` has no
//! such cap, so with the defaults (4 pushes at once) a step that runs more
//! concurrent pushes than that sees Aspen refuse one with `429` before it
//! reports any ref, which is admission working as designed (issue #118),
//! not a ref-semantics divergence. The instance-wide limits stay on; they
//! queue instead of refusing.
//!
//! The testkit does not link Aspen: it takes the path of an `aspen` binary
//! built from the same checkout, so it stays a std-only tool and the layering
//! rule (only tooling and roots depend on a root) is not touched.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;

use crate::git::GitClient;
use crate::http_backend::SCOPE_SEGMENT;
use crate::server::{GitServer, RefMap, parse_ref_lines};

/// How long `aspen serve` may take to migrate and start listening.
const START_TIMEOUT: Duration = Duration::from_secs(60);

/// `ASPEN_LIMITS` for the server under test: no per-caller cap on
/// concurrent fetches, pushes or stitches (module docs), and otherwise the
/// defaults (no request rates or push quotas).
pub const ORACLE_LIMITS: &str =
    r#"{"concurrent": {"fetches": null, "pushes": null, "stitches": null}}"#;

/// Where an [`AspenServer`] keeps its metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AspenDatabase {
    /// A fresh SQLite file in the server's temporary directory.
    Sqlite,
    /// A PostgreSQL database URL without a password (`aspen serve` refuses
    /// one); the password, if any, comes from `PGPASSWORD` in this process's
    /// environment. The database may be shared between runs: every
    /// repository is created under a namespace unique to the server
    /// ([`AspenServer::namespace`]).
    Postgres(String),
}

/// How to start an [`AspenServer`].
#[derive(Debug, Clone)]
pub struct AspenOptions {
    /// The `aspen` binary to run.
    pub binary: PathBuf,
    /// Where metadata lives.
    pub database: AspenDatabase,
    /// A label for reports; defaults to `aspen (sqlite)` or `aspen (postgres)`.
    pub name: Option<String>,
}

impl AspenOptions {
    /// SQLite metadata and a local blob store, running `binary`.
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            database: AspenDatabase::Sqlite,
            name: None,
        }
    }
}

/// A running `aspen serve` process and the temporary directory holding its
/// blobs, spool, log and (with SQLite) metadata. The process is killed and
/// the directory deleted when this is dropped.
#[derive(Debug)]
pub struct AspenServer {
    // Field order matters: stop the process before deleting its files.
    child: Mutex<Child>,
    base_url: String,
    database_url: String,
    binary: PathBuf,
    namespace: String,
    name: String,
    log: PathBuf,
    dir: TempDir,
}

impl AspenServer {
    /// Starts `aspen serve` from `options` on an ephemeral loopback port and
    /// waits until it listens.
    pub fn start(options: AspenOptions) -> io::Result<Self> {
        if !options.binary.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "no aspen binary at {} (build it with `cargo build -p aspen-cli`)",
                    options.binary.display()
                ),
            ));
        }
        let dir = tempfile::Builder::new().prefix("aspen-diff-").tempdir()?;
        let (database_url, default_name) = match &options.database {
            AspenDatabase::Sqlite => (
                format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display()),
                "aspen (sqlite)",
            ),
            AspenDatabase::Postgres(url) => (url.clone(), "aspen (postgres)"),
        };
        let log = dir.path().join("aspen.log");
        let mut cmd = Command::new(&options.binary);
        cmd.arg("serve")
            .env("DATABASE_URL", &database_url)
            .env(
                "ASPEN_BLOB_URL",
                format!("file://{}", dir.path().join("blobs").display()),
            )
            .env("ASPEN_SPOOL_DIR", dir.path().join("spool"))
            .env("ASPEN_LISTEN", "127.0.0.1:0")
            .env("ASPEN_METRICS_LISTEN", "off")
            .env("ASPEN_POLICY", r#"{"anonymous":["write:*"]}"#)
            .env_remove("ASPEN_POLICY_FILE")
            .env("ASPEN_LIMITS", ORACLE_LIMITS)
            .env_remove("ASPEN_LIMITS_FILE")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log)?);
        let child = cmd.spawn()?;
        let mut server = Self {
            child: Mutex::new(child),
            base_url: String::new(),
            database_url,
            binary: options.binary,
            namespace: unique_namespace(),
            name: options.name.unwrap_or_else(|| default_name.to_owned()),
            log,
            dir,
        };
        server.base_url = server.wait_listening()?;
        Ok(server)
    }

    /// The server's base URL, `http://127.0.0.1:<port>`.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The prefix under which every repository of this server is created,
    /// so several servers can share one PostgreSQL database.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The temporary directory holding the server's blobs, spool and log.
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// The last `lines` lines of the server's log, for failure reports.
    pub fn log_tail(&self, lines: usize) -> String {
        let text = fs::read_to_string(&self.log).unwrap_or_default();
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }

    /// Waits for the "listening on http://..." line `aspen serve` logs.
    fn wait_listening(&self) -> io::Result<String> {
        let deadline = Instant::now() + START_TIMEOUT;
        loop {
            let text = fs::read_to_string(&self.log).unwrap_or_default();
            if let Some(addr) = text
                .lines()
                .find_map(|l| l.split("listening on http://").nth(1))
                .and_then(|rest| rest.split_whitespace().next())
            {
                return Ok(format!("http://{addr}"));
            }
            let exited = self
                .child
                .lock()
                .map_err(|_| io::Error::other("aspen child lock poisoned"))?
                .try_wait()?;
            if let Some(status) = exited {
                return Err(io::Error::other(format!(
                    "aspen serve exited with {status} before listening:\n{}",
                    self.log_tail(30)
                )));
            }
            if Instant::now() > deadline {
                return Err(io::Error::other(format!(
                    "aspen serve did not listen within {START_TIMEOUT:?}:\n{}",
                    self.log_tail(30)
                )));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Fails if the server process has exited, with the end of its log.
    fn check_alive(&self) -> io::Result<()> {
        let exited = self
            .child
            .lock()
            .map_err(|_| io::Error::other("aspen child lock poisoned"))?
            .try_wait()?;
        match exited {
            None => Ok(()),
            Some(status) => Err(io::Error::other(format!(
                "aspen serve exited with {status}:\n{}",
                self.log_tail(30)
            ))),
        }
    }
}

impl Drop for AspenServer {
    fn drop(&mut self) {
        if let Ok(child) = self.child.get_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl GitServer for AspenServer {
    fn name(&self) -> &str {
        &self.name
    }

    /// Runs `aspen repo create <namespace>/<name>` against the server's
    /// database (trunk `refs/heads/main`, force policy `branches`, the
    /// defaults), and returns `<base>/<namespace>/<name>.git`.
    fn create_repo(&self, name: &str) -> io::Result<String> {
        self.check_alive()?;
        let bare = name.strip_suffix(".git").unwrap_or(name);
        let repo = format!("{}/{bare}", self.namespace);
        let out = Command::new(&self.binary)
            .args(["repo", "create", &repo])
            .env("DATABASE_URL", &self.database_url)
            .env("ASPEN_LOG", "warn")
            .stdin(Stdio::null())
            .output()?;
        if !out.status.success() {
            return Err(io::Error::other(format!(
                "aspen repo create {repo} failed ({}):\n{}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(format!("{}/{repo}.git", self.base_url))
    }

    /// `<repo>/-/<refname>`: Aspen's branch-scoped URL (spike S2).
    fn branch_url(&self, repo_url: &str, refname: &str) -> String {
        format!("{repo_url}{SCOPE_SEGMENT}{refname}")
    }

    /// Aspen's full ref listing: protocol v2 `ls-refs` on the repository URL,
    /// which reads every ref from the metadata store (only push and v0
    /// advertisements are narrowed to trunk; decisions log, B2).
    fn list_refs(&self, repo_url: &str) -> io::Result<RefMap> {
        self.check_alive()?;
        let home = self.dir.path().join("lister");
        fs::create_dir_all(&home)?;
        let out = GitClient::new(&home)
            .with_config("protocol.version", "2")
            .run(&home, ["ls-remote", "--refs", repo_url])?;
        parse_ref_lines(&out)
    }
}

/// `t<seconds><nanos><pid>`: unique per server, a valid repository-name
/// segment, and sortable by start time in a shared database.
fn unique_namespace() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "t{}{:09}p{}",
        now.as_secs(),
        now.subsec_nanos(),
        std::process::id()
    )
}

/// The `aspen` binary next to the running executable (`target/<profile>/`),
/// where `cargo build -p aspen-cli` puts it beside `aspen-testkit`.
pub fn sibling_binary() -> io::Result<PathBuf> {
    let exe = std::env::current_exe()?;
    let dir = exe
        .parent()
        .ok_or_else(|| io::Error::other("the running executable has no directory"))?;
    // Test binaries live one level deeper, in `target/<profile>/deps/`.
    let dir = if dir.ends_with("deps") {
        dir.parent().unwrap_or(dir)
    } else {
        dir
    };
    Ok(dir.join(format!("aspen{}", std::env::consts::EXE_SUFFIX)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaces_are_valid_and_distinct() {
        let a = unique_namespace();
        std::thread::sleep(Duration::from_millis(2));
        let b = unique_namespace();
        assert_ne!(a, b);
        for ns in [a, b] {
            assert!(
                ns.chars().all(|c| c.is_ascii_alphanumeric()),
                "{ns} must be a plain repository-name segment"
            );
        }
    }

    #[test]
    fn a_missing_binary_is_named() {
        let err = AspenServer::start(AspenOptions::new("/nonexistent/aspen")).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/aspen"), "{err}");
    }
}

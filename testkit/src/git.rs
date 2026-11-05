//! Isolated `git` invocations.
//!
//! Every command built here ignores the machine's system and global Git
//! configuration, never prompts for credentials, uses a fixed author and
//! committer identity, and runs no automatic maintenance
//! ([`NO_AUTO_MAINTENANCE`]), so tests behave the same on a laptop and in CI.

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// Global `-c` options that stop `git` from starting automatic maintenance.
///
/// `git fetch`, `commit`, `merge`, `am`, `rebase` and `receive-pack` end by
/// starting `git maintenance run --auto`, detached by default. Since Git 2.54
/// that runs a `git repack -d -l --cruft --write-midx` whenever a
/// repository holds two or more packs that are not in a geometric size
/// progression, which a fetch with packfile URIs (one pack per URI) almost
/// always leaves. The repack rewrites `objects/pack` in the background, so a
/// `git fsck --strict` the test runs next fails now and then with `packfile
/// .../.tmp-<pid>-pack-<hash>.pack index not opened` or `unable to load
/// rev-index`: fsck lists an `.idx` the repack then renames or deletes
/// (issues #72 and #207). With maintenance off, a test checks exactly what
/// the server delivered.
///
/// `maintenance.auto=false` turns it off; `gc.auto=0` does on Git older than
/// 2.29, which runs `git gc --auto` instead. Pass these before the
/// subcommand: `git <NO_AUTO_MAINTENANCE> fetch ...`. [`isolated_git`]
/// already does; a suite that builds its own `git` command must add them.
pub const NO_AUTO_MAINTENANCE: [&str; 4] = ["-c", "maintenance.auto=false", "-c", "gc.auto=0"];

/// A `git` command isolated from the machine's system and global config, with
/// `home` as its home directory, a fixed author identity and no automatic
/// maintenance ([`NO_AUTO_MAINTENANCE`]).
pub fn isolated_git(home: &Path) -> Command {
    let mut cmd = Command::new("git");
    cmd.env("HOME", home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_AUTHOR_NAME", "Aspen Interop")
        .env("GIT_AUTHOR_EMAIL", "interop@aspen.invalid")
        .env("GIT_COMMITTER_NAME", "Aspen Interop")
        .env("GIT_COMMITTER_EMAIL", "interop@aspen.invalid")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_PROTOCOL")
        .env_remove("GIT_TRACE")
        .env_remove("GIT_TRACE_PACKET")
        .args(NO_AUTO_MAINTENANCE);
    cmd
}

/// Creates an empty bare repository at `root/name` that accepts pushes over
/// HTTP, and returns its path.
pub fn init_bare(root: &Path, name: &str) -> io::Result<PathBuf> {
    let path = root.join(name);
    run(isolated_git(root)
        .args(["init", "--quiet", "--bare", "--initial-branch=main"])
        .arg(&path))?;
    run(isolated_git(root)
        .args(["config", "http.receivepack", "true"])
        .current_dir(&path))?;
    Ok(path)
}

/// Runs `cmd` and returns its stdout. A non-zero exit becomes an error whose
/// message holds the command line, the exit status, stdout and stderr.
pub fn run(cmd: &mut Command) -> io::Result<String> {
    let out = cmd.output()?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(io::Error::other(describe_failure(cmd, &out)))
    }
}

/// A readable account of a failed command, for error messages.
pub fn describe_failure(cmd: &Command, out: &Output) -> String {
    format!(
        "{} failed with {}\nstdout:\n{}\nstderr:\n{}",
        command_line(cmd),
        out.status,
        String::from_utf8_lossy(&out.stdout).trim_end(),
        String::from_utf8_lossy(&out.stderr).trim_end()
    )
}

fn command_line(cmd: &Command) -> String {
    let mut line = cmd.get_program().to_string_lossy().into_owned();
    for arg in cmd.get_args() {
        line.push(' ');
        line.push_str(&arg.to_string_lossy());
    }
    if let Some(dir) = cmd.get_current_dir() {
        line.push_str(&format!(" (in {})", dir.display()));
    }
    line
}

/// The `git --version` line of the `git` on `PATH`.
pub fn version() -> io::Result<String> {
    Ok(run(Command::new("git").arg("--version"))?.trim().to_owned())
}

/// A git client with its own home directory and optional `-c` settings,
/// for tests that run many commands as one user.
#[derive(Debug, Clone)]
pub struct GitClient {
    home: PathBuf,
    config: Vec<String>,
}

impl GitClient {
    /// A client whose `HOME` is `home`. The directory must exist.
    pub fn new(home: &Path) -> Self {
        Self {
            home: home.to_path_buf(),
            config: Vec::new(),
        }
    }

    /// Adds a `-c key=value` setting to every command this client builds,
    /// for example `protocol.version=2`.
    pub fn with_config(mut self, key: &str, value: &str) -> Self {
        self.config.push(format!("{key}={value}"));
        self
    }

    /// The client's home directory.
    pub fn home(&self) -> &Path {
        &self.home
    }

    /// An isolated `git` command that runs in `dir`.
    pub fn command(&self, dir: &Path) -> Command {
        let mut cmd = isolated_git(&self.home);
        cmd.current_dir(dir);
        for setting in &self.config {
            cmd.arg("-c").arg(setting);
        }
        cmd
    }

    /// Runs `git <args>` in `dir` and returns stdout; fails on a non-zero exit.
    pub fn run<I, S>(&self, dir: &Path, args: I) -> io::Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        run(self.command(dir).args(args))
    }

    /// Runs `git <args>` in `dir` and returns the raw output, whatever the
    /// exit status. Fails only if `git` could not be started.
    pub fn output<I, S>(&self, dir: &Path, args: I) -> io::Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.command(dir).args(args).output()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_name_the_command_and_its_stderr() {
        let tmp = tempfile::tempdir().unwrap();
        let client = GitClient::new(tmp.path());
        let err = client
            .run(tmp.path(), ["rev-parse", "--verify", "no-such-ref"])
            .unwrap_err()
            .to_string();
        let line = format!(
            "git {} rev-parse --verify no-such-ref",
            NO_AUTO_MAINTENANCE.join(" ")
        );
        assert!(err.contains(&line), "{err}");
        assert!(err.contains("stderr:"), "{err}");
    }

    #[test]
    fn init_bare_accepts_http_pushes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = init_bare(tmp.path(), "r.git").unwrap();
        let value = GitClient::new(tmp.path())
            .run(&path, ["config", "http.receivepack"])
            .unwrap();
        assert_eq!(value.trim(), "true");
    }

    /// A fetch into a repository that already holds several packs starts
    /// no `git maintenance` (#72, #207). Git decides whether to start it
    /// from config alone, before looking at the packs, so this holds on
    /// every Git version; on Git 2.54 and later the maintenance it would
    /// start repacks these packs.
    #[test]
    fn a_fetch_starts_no_automatic_maintenance() {
        let tmp = tempfile::tempdir().unwrap();
        let client = GitClient::new(tmp.path());
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        for repo in [&src, &dst] {
            run(client
                .command(tmp.path())
                .args(["init", "--quiet", "-b", "main"])
                .arg(repo))
            .unwrap();
        }
        let trace = tmp.path().join("trace2");
        for i in 0..3 {
            std::fs::write(src.join("f"), format!("{i}\n")).unwrap();
            client.run(&src, ["add", "f"]).unwrap();
            client.run(&src, ["commit", "--quiet", "-m", "c"]).unwrap();
            // `fetch.unpackLimit=1` keeps every fetched pack as a pack.
            run(client
                .command(&dst)
                .env("GIT_TRACE2", &trace)
                .args(["-c", "fetch.unpackLimit=1", "fetch", "--quiet"])
                .arg(&src)
                .arg("main:refs/remotes/origin/main"))
            .unwrap();
        }
        let trace = std::fs::read_to_string(&trace).unwrap();
        assert!(
            trace.contains(" fetch "),
            "the trace saw the fetches:\n{trace}"
        );
        assert!(
            !trace.contains("maintenance run") && !trace.contains("gc --auto"),
            "a fetch started automatic maintenance:\n{trace}"
        );
        let packs = std::fs::read_dir(dst.join(".git/objects/pack"))
            .unwrap()
            .filter(|e| e.as_ref().unwrap().path().extension() == Some(OsStr::new("pack")))
            .count();
        assert_eq!(
            packs, 3,
            "each fetch kept its pack, and nothing repacked them"
        );
    }

    #[test]
    fn global_config_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(".gitconfig"),
            "[user]\n\tname = Someone Else\n",
        )
        .unwrap();
        let name = GitClient::new(tmp.path())
            .output(tmp.path(), ["config", "user.name"])
            .unwrap();
        assert!(!name.status.success(), "global config leaked in");
    }
}

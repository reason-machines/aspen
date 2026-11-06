//! Deliberately broken servers, used to prove the oracle catches real bugs.
//!
//! A [`FaultyServer`] is a [`StockServer`] whose repositories carry a hook
//! that misbehaves. Most faults act *after* the push has been accepted, so
//! the client sees a normal, successful push. Only comparing the server's
//! state afterwards reveals the fault, which is exactly what the oracle is for.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::git::GitClient;
use crate::server::{GitServer, RefMap, StockServer};

/// How a [`FaultyServer`] misbehaves. Faults with an `nth_push` fire once per
/// repository, on that push (counting from 1, and counting the oracle's
/// setup push of the initial commit). Those act after the push is accepted,
/// so the client sees a normal, successful push, except
/// [`Fault::RejectPush`].
///
/// Every fault except [`Fault::SlowRacer`] records each time it acts, so a
/// test can tell a fault that was caught from one that never fired (see
/// [`FaultyServer::fired`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Silently undoes every ref update of that push: an acknowledged push is
    /// lost.
    DropRefUpdate {
        /// Which push triggers the fault, counting from 1.
        nth_push: u32,
    },
    /// Deletes the root tree of each new tip from the object store, so the
    /// ref points at history that can no longer be fetched. It records an
    /// action only for a tree the repository can no longer read afterwards.
    LoseObject {
        /// Which push triggers the fault, counting from 1.
        nth_push: u32,
    },
    /// Overwrites the root tree of each new tip with the bytes of a different
    /// object, so the stored object no longer matches its name.
    CorruptObject {
        /// Which push triggers the fault, counting from 1.
        nth_push: u32,
    },
    /// Rejects that push from a `pre-receive` hook, so the client sees a
    /// failure the reference server would not report. Unlike the other
    /// faults, the client notices this one.
    RejectPush {
        /// Which push attempt triggers the fault, counting from 1.
        nth_push: u32,
    },
    /// Applies every ref update without checking the ref's old value, and
    /// reports success: when two pushes race on one branch, both are
    /// acknowledged and the last one to land silently replaces the other (a
    /// lost update). Sequential pushes behave normally, because the client
    /// already refuses stale updates itself. Built with a `proc-receive` hook
    /// (git 2.29 or later), which holds racers' pushes for half a second so
    /// that every racer is in flight before the first update lands.
    LastWriterWins,
    /// Not a bug: delays, by one second before the ref update, every push of
    /// a commit made by racer `racer` of an [`Op::Race`](crate::Op::Race), so
    /// that racer reliably loses. Servers that disagree only in this way must
    /// not diverge; tests use it to force different race winners and exercise
    /// the oracle's resync.
    ///
    /// With `trunk_fast_forward_only`, the server also refuses every
    /// non-fast-forward update of `refs/heads/main`, forced or not, the way
    /// Aspen does by default (S2: `allow_force = branches`). Tests use that to
    /// prove the oracle never asks the candidate to rewrite trunk.
    SlowRacer {
        /// The index of the racer, in [`Op::Race`](crate::Op::Race)'s `clones`.
        racer: usize,
        /// Refuse non-fast-forward updates of `refs/heads/main`.
        trunk_fast_forward_only: bool,
    },
}

impl Fault {
    /// The shell fragment run for each `<old> <new> <ref>` line of the hook's
    /// input once an `nth_push` fault fires.
    fn action(self) -> &'static str {
        match self {
            Fault::DropRefUpdate { .. } => {
                r#"if [ "$old" = "$zero" ]; then
    git update-ref -d "$ref"
  else
    git update-ref "$ref" "$old"
  fi
  fired"#
            }
            Fault::LoseObject { .. } => {
                r#"[ "$new" = "$zero" ] && continue
  tree=$(git rev-parse "$new^{tree}")
  rm -f "$(loose "$tree")"
  if ! git cat-file -e "$tree" 2>/dev/null; then fired; fi"#
            }
            Fault::CorruptObject { .. } => {
                r#"[ "$new" = "$zero" ] && continue
  tree=$(git rev-parse "$new^{tree}")
  other=$(printf 'aspen-testkit: corrupted object\n' | git hash-object -w --stdin)
  rm -f "$(loose "$tree")"
  cp "$(loose "$other")" "$(loose "$tree")"
  fired"#
            }
            Fault::RejectPush { .. } => {
                r#"fired
  echo "aspen-testkit: injected rejection of $ref" >&2
  exit 1"#
            }
            Fault::LastWriterWins | Fault::SlowRacer { .. } => unreachable!("no nth_push"),
        }
    }

    /// The hook that carries the fault.
    fn hook_name(self) -> &'static str {
        match self {
            Fault::RejectPush { .. } | Fault::SlowRacer { .. } => "pre-receive",
            Fault::LastWriterWins => "proc-receive",
            _ => "post-receive",
        }
    }

    /// Extra repository configuration the fault needs.
    fn config(self) -> &'static [(&'static str, &'static str)] {
        match self {
            // Keep every pushed object loose so the object faults can find it.
            Fault::LoseObject { .. } | Fault::CorruptObject { .. } => {
                &[("receive.unpackLimit", "1000000")]
            }
            Fault::LastWriterWins => &[("receive.procReceiveRefs", "refs/heads")],
            _ => &[],
        }
    }

    fn hook(self) -> String {
        let nth = match self {
            Fault::DropRefUpdate { nth_push }
            | Fault::LoseObject { nth_push }
            | Fault::CorruptObject { nth_push }
            | Fault::RejectPush { nth_push } => nth_push,
            Fault::LastWriterWins => return LAST_WRITER_WINS.to_owned(),
            Fault::SlowRacer {
                racer,
                trunk_fast_forward_only,
            } => {
                let trunk = if trunk_fast_forward_only {
                    TRUNK_FAST_FORWARD_ONLY
                } else {
                    ""
                };
                return format!(
                    r#"#!/bin/sh
# Installed by aspen-testkit: delays pushes of racer {racer}'s commits.
zero=0000000000000000000000000000000000000000
while read -r old new ref; do
{trunk}  [ "$new" = "$zero" ] && continue
  case "$(git log -1 --format=%s "$new")" in
    *": racer {racer} on "*) sleep 1 ;;
  esac
done
"#
                );
            }
        };
        format!(
            r#"#!/bin/sh
# Installed by aspen-testkit: a deliberately faulty server for oracle tests.
set -e
zero=0000000000000000000000000000000000000000
loose() {{ echo "objects/$(echo "$1" | cut -c1-2)/$(echo "$1" | cut -c3-)"; }}
fired() {{ echo "$ref" >> aspen-fault-fired; }}
{count}if [ "$n" -ne {nth} ]; then
  cat > /dev/null
  exit 0
fi
while read -r old new ref; do
  {action}
done
"#,
            count = COUNT_PUSH,
            action = self.action(),
        )
    }
}

/// The hook fragment that numbers this push in `$n`, counting from 1.
///
/// `aspen-fault-count` is append-only: each hook appends one line holding
/// its own process ID, and its push number is the number of that line.
/// Concurrent pushes run the hook at once, and a read-modify-write of a
/// count loses counts, so the nth push fires late, twice or never (issue
/// #171). An append cannot lose one: the shell opens the file with
/// `O_APPEND`, so each `echo` is one write placed at the end of the file,
/// and no lock is needed. Nothing is ever truncated or locked, so a hook
/// killed at any point, even with SIGKILL, leaves either its whole line or
/// nothing, and later hooks are not held up (issue #250).
///
/// A hook finds its line as the *last* line equal to its process ID: an
/// earlier line with the same ID is from an earlier hook whose ID the
/// system has reused, and no later one can exist while this hook is alive.
/// That holds while every hook of a repository runs in one PID namespace,
/// as it does for every server in this crate.
const COUNT_PUSH: &str = r#"echo "$$" >> aspen-fault-count
n=$(awk -v me="$$" '$0 == me { n = NR } END { print n }' aspen-fault-count)
if [ -z "$n" ]; then
  echo "aspen-testkit: this hook's line ($$) is missing from $(pwd)/aspen-fault-count" >&2
  exit 1
fi
"#;

/// A `pre-receive` loop fragment that refuses any update of
/// `refs/heads/main` that is not a fast-forward (deletes included).
const TRUNK_FAST_FORWARD_ONLY: &str = r#"  if [ "$ref" = refs/heads/main ] && [ "$old" != "$zero" ]; then
    if [ "$new" = "$zero" ] || ! git merge-base --is-ancestor "$old" "$new"; then
      echo "aspen-testkit: refusing a non-fast-forward update of $ref" >&2
      exit 1
    fi
  fi
"#;

/// A `proc-receive` hook (see githooks(5)) that performs every ref update
/// itself with an unconditional `git update-ref`, ignoring the old value the
/// client sent, and reports each one as accepted.
const LAST_WRITER_WINS: &str = r#"#!/bin/sh
# Installed by aspen-testkit: accepts every update and keeps the last one.
zero=0000000000000000000000000000000000000000
# Reads one pkt-line into $pkt; fails on a flush packet.
readpkt() {
  len=$(dd bs=1 count=4 2>/dev/null)
  [ -n "$len" ] || exit 1
  n=$((0x$len))
  [ "$n" -eq 0 ] && return 1
  pkt=$(dd bs=1 count=$((n - 4)) 2>/dev/null | tr '\000\n' '  ')
  return 0
}
writepkt() { printf '%04x%s\n' $((${#1} + 5)) "$1"; }
apply() {
  if [ "$2" = "$zero" ]; then git update-ref -d "$3"; else git update-ref "$3" "$2"; fi
}
while readpkt; do :; done
writepkt "version=1"
printf 0000
refs=
while readpkt; do
  set -- $pkt
  # Widen the race window so racing pushes are all in flight at once.
  case "$(git log -1 --format=%s "$2" 2>/dev/null)" in
    *": racer "*) sleep 0.5 ;;
  esac
  cur=$(git rev-parse -q --verify "$3" || echo "$zero")
  tries=0
  until apply "$@" 2>/dev/null; do
    tries=$((tries + 1))
    [ "$tries" -lt 100 ] || break
    sleep 0.05
  done
  # The fault fired: an update a stock server refuses as stale landed and
  # replaced a value (an update to the value the ref holds loses nothing).
  if [ "$tries" -lt 100 ] && [ "$cur" != "$1" ] && [ "$cur" != "$2" ]; then
    echo "$3" >> aspen-fault-fired
  fi
  refs="$refs $3"
done
for ref in $refs; do writepkt "ok $ref"; done
printf 0000
"#;

/// The file, in a repository, where its fault's hook appends a line each
/// time the fault acts (one per ref it breaks).
const FIRED_FILE: &str = "aspen-fault-fired";

/// A [`StockServer`] whose repositories exhibit a [`Fault`].
#[derive(Debug)]
pub struct FaultyServer {
    inner: StockServer,
    fault: Fault,
    name: String,
    firings: Firings,
}

/// How often a [`FaultyServer`]'s fault has acted, readable without
/// borrowing the server (for example from a run's stop condition).
#[derive(Debug, Clone, Default)]
pub struct Firings {
    repos: Arc<Mutex<Vec<PathBuf>>>,
}

impl Firings {
    /// How many times the fault has acted so far, over every repository the
    /// server created: once per ref it broke (for [`Fault::RejectPush`],
    /// per ref of a rejected push; for [`Fault::LastWriterWins`], per stale
    /// update it applied). Always 0 for [`Fault::SlowRacer`], which is not
    /// a bug.
    pub fn count(&self) -> usize {
        let repos = self.repos.lock().unwrap_or_else(|e| e.into_inner());
        repos
            .iter()
            .filter_map(|r| fs::read_to_string(r.join(FIRED_FILE)).ok())
            .map(|text| text.lines().count())
            .sum()
    }
}

impl FaultyServer {
    /// Starts a stock server whose new repositories exhibit `fault`.
    pub fn start(fault: Fault) -> io::Result<Self> {
        Ok(Self {
            inner: StockServer::start()?,
            fault,
            name: format!("faulty({fault:?})"),
            firings: Firings::default(),
        })
    }

    /// How many times the fault has acted so far; see [`Firings::count`].
    /// A test that expects its checks to catch the fault must first check
    /// this is not 0: a fault that never fired proves nothing.
    pub fn fired(&self) -> usize {
        self.firings.count()
    }

    /// A handle that reads [`FaultyServer::fired`] without borrowing the
    /// server.
    pub fn firings(&self) -> Firings {
        self.firings.clone()
    }
}

impl GitServer for FaultyServer {
    fn name(&self) -> &str {
        &self.name
    }

    fn create_repo(&self, name: &str) -> io::Result<String> {
        let url = self.inner.init(name)?;
        let path = self.inner.repo_path(name);
        let client = GitClient::new(self.inner.root());
        for (key, value) in self.fault.config() {
            client.run(&path, ["config", key, value])?;
        }
        let hook = path.join("hooks").join(self.fault.hook_name());
        write_executable(&hook, self.fault.hook())?;
        self.firings
            .repos
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(path);
        Ok(url)
    }

    fn branch_url(&self, repo_url: &str, refname: &str) -> String {
        self.inner.branch_url(repo_url, refname)
    }

    fn list_refs(&self, repo_url: &str) -> io::Result<RefMap> {
        self.inner.list_refs(repo_url)
    }
}

/// Writes `contents` to `path` as an executable file that can be run at
/// once, even while other threads of this process start programs.
///
/// A child `sh` writes the file, never this process. A file this process
/// opened for writing is also open in every child another thread forks
/// before the close, until that child runs its own program, and Linux
/// refuses to run a file that is open for writing ("Text file busy"). The
/// writing child's descriptor is never shared with this process's other
/// children.
#[cfg(unix)]
fn write_executable(path: &std::path::Path, contents: impl AsRef<[u8]>) -> io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Command, Stdio};

    let mut child = Command::new("sh")
        .args(["-c", r#"cat > "$1""#, "sh"])
        .arg(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .expect("stdin is piped")
        .write_all(contents.as_ref())?;
    let out = child.wait_with_output()?;
    if !out.status.success() {
        return Err(io::Error::other(format!(
            "writing {}: sh exited with {}: {}",
            path.display(),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))
}

#[cfg(not(unix))]
fn write_executable(_path: &std::path::Path, _contents: impl AsRef<[u8]>) -> io::Result<()> {
    Err(io::Error::other(
        "FaultyServer needs a Unix shell for its hook",
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::Write;
    use std::path::Path;
    use std::process::{Child, Command, Output, Stdio};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::git::isolated_git;

    const ZERO: &str = "0000000000000000000000000000000000000000";

    /// The file in which [`COUNT_PUSH`] numbers a repository's pushes.
    const COUNT_FILE: &str = "aspen-fault-count";

    /// How long a hook may run in these tests before it counts as stuck.
    /// A hook that has to wait out a lock left behind takes about 30 s.
    const LIMIT: Duration = Duration::from_secs(10);

    /// Writes `fault`'s hook into `dir` as `name` and returns its path.
    fn install(dir: &Path, name: &str, fault: Fault) -> PathBuf {
        let hook = dir.join(name);
        write_executable(&hook, fault.hook()).unwrap();
        hook
    }

    /// Starts `hook` in `dir`, as git would start it in a repository, with
    /// `input` on its standard input and no machine-wide git configuration.
    fn spawn_hook(hook: &Path, dir: &Path, input: &[u8]) -> Child {
        let mut child = Command::new(hook)
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env_remove("GIT_DIR")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(input).unwrap();
        child
    }

    /// Runs `hook` to the end; fails the test if it runs longer than
    /// [`LIMIT`].
    fn run_hook(hook: &Path, dir: &Path, input: &[u8]) -> Output {
        let mut child = spawn_hook(hook, dir, input);
        let start = Instant::now();
        while child.try_wait().unwrap().is_none() {
            if start.elapsed() > LIMIT {
                child.kill().unwrap();
                let out = child.wait_with_output().unwrap();
                panic!(
                    "the hook was still running after {LIMIT:?}; stderr:\n{}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        child.wait_with_output().unwrap()
    }

    /// One `<old> <new> <ref>` line of a `pre-receive` or `post-receive`
    /// hook's input: a create of `refname` at `new`.
    fn create_line(new: &str, refname: &str) -> Vec<u8> {
        format!("{ZERO} {new} {refname}\n").into_bytes()
    }

    fn stderr(out: &Output) -> String {
        String::from_utf8_lossy(&out.stderr).into_owned()
    }

    fn firings(dir: &Path) -> usize {
        fs::read_to_string(dir.join(FIRED_FILE))
            .unwrap_or_default()
            .lines()
            .count()
    }

    /// Asserts that the next push to the repository at `dir` is push number
    /// `n`, by running a hook that rejects exactly push `n`.
    fn assert_next_push_is(dir: &Path, n: u32) {
        let probe = install(dir, "probe", Fault::RejectPush { nth_push: n });
        let out = run_hook(
            &probe,
            dir,
            &create_line(&"1".repeat(40), "refs/heads/probe"),
        );
        assert!(
            !out.status.success() && stderr(&out).contains("injected rejection"),
            "the next push is not push {n}; the probe exited with {} and stderr:\n{}",
            out.status,
            stderr(&out)
        );
    }

    /// Many concurrent pushes each run the hook once: every one is counted,
    /// and the fault fires on exactly one of them (issue #171). The hook's
    /// counter runs before any git command, so the test runs the real hook
    /// in a plain directory, with one `<old> <new> <ref>` line as its input.
    #[test]
    fn concurrent_hooks_count_every_push_and_fire_once() {
        const PUSHES: u32 = 64;
        const ROUNDS: usize = 3;
        let line = create_line(&"1".repeat(40), "refs/heads/b");
        for round in 0..ROUNDS {
            let dir = tempfile::tempdir().unwrap();
            let hook = install(
                dir.path(),
                "hook",
                Fault::RejectPush {
                    nth_push: PUSHES / 2,
                },
            );
            let children: Vec<_> = (0..PUSHES)
                .map(|_| spawn_hook(&hook, dir.path(), &line))
                .collect();
            let rejected = children
                .into_iter()
                .map(|c| c.wait_with_output().unwrap())
                .filter(|out| !out.status.success())
                .count();

            assert_eq!(rejected, 1, "round {round}: pushes rejected");
            assert_eq!(firings(dir.path()), 1, "round {round}: firings recorded");
            assert_next_push_is(dir.path(), PUSHES + 1);
        }
    }

    /// A count file with nothing in it means no push has been counted yet
    /// (issue #250). The hook used to stop at it with an error that did not
    /// say why, on every later push.
    #[test]
    fn an_empty_count_file_counts_no_pushes() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(COUNT_FILE), "").unwrap();
        let hook = install(dir.path(), "hook", Fault::RejectPush { nth_push: 2 });
        let line = create_line(&"1".repeat(40), "refs/heads/b");

        let first = run_hook(&hook, dir.path(), &line);
        assert!(
            first.status.success(),
            "push 1 failed with {}; stderr:\n{}",
            first.status,
            stderr(&first)
        );
        let second = run_hook(&hook, dir.path(), &line);
        assert!(
            !second.status.success() && stderr(&second).contains("injected rejection"),
            "push 2 was not rejected by the fault; stderr:\n{}",
            stderr(&second)
        );
        assert_eq!(firings(dir.path()), 1);
    }

    /// Hooks killed with SIGKILL while others run, at any point of their
    /// run, leave nothing behind that stops or slows a later push (issue
    /// #250). A lock held by a killed hook used to make every later hook of
    /// that repository wait about 30 s and fail.
    #[test]
    fn hooks_killed_at_any_point_leave_later_pushes_working() {
        const HOOKS: usize = 64;
        const ROUNDS: usize = 5;
        let line = create_line(&"1".repeat(40), "refs/heads/b");
        for round in 0..ROUNDS {
            let dir = tempfile::tempdir().unwrap();
            // A fault that never fires, so every push that runs to the end
            // succeeds.
            let hook = install(dir.path(), "hook", Fault::RejectPush { nth_push: u32::MAX });
            let mut children: Vec<_> = (0..HOOKS)
                .map(|_| spawn_hook(&hook, dir.path(), &line))
                .collect();
            // Kill every other hook, a little later each time, so that the
            // kills land at different points of the hooks' runs.
            for child in children.iter_mut().step_by(2) {
                std::thread::sleep(Duration::from_micros(300));
                child.kill().unwrap();
            }
            for child in children {
                child.wait_with_output().unwrap();
            }

            for push in 0..3 {
                let out = run_hook(&hook, dir.path(), &line);
                assert!(
                    out.status.success(),
                    "round {round}, push {push} after the kills failed with {}; stderr:\n{}",
                    out.status,
                    stderr(&out)
                );
            }
            assert_eq!(firings(dir.path()), 0, "round {round}");
        }
    }

    /// A bare repository at `root/repo` holding one commit per entry of
    /// `files` (each a tree with one file of that content), returned in
    /// order, with no ref pointing at any of them.
    fn repo_with_commits(root: &Path, files: &[&str]) -> (PathBuf, Vec<String>) {
        let repo = crate::git::init_bare(root, "repo").unwrap();
        let git = |args: &[&str], input: &str| -> String {
            let mut child = isolated_git(root)
                .args(args)
                .current_dir(&repo)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.as_bytes())
                .unwrap();
            let out = child.wait_with_output().unwrap();
            assert!(out.status.success(), "git {args:?} failed");
            String::from_utf8(out.stdout).unwrap().trim().to_owned()
        };
        let commits = files
            .iter()
            .map(|content| {
                let blob = git(&["hash-object", "-w", "--stdin"], content);
                let tree = git(&["mktree"], &format!("100644 blob {blob}\tfile\n"));
                git(&["commit-tree", &tree, "-m", content], "")
            })
            .collect();
        (repo, commits)
    }

    /// One pkt-line holding `payload`.
    fn pkt(payload: &str) -> String {
        format!("{:04x}{payload}", payload.len() + 4)
    }

    /// [`Fault::LastWriterWins`] counts an update only when it replaced a
    /// value the client did not expect. An update to the value the ref
    /// already holds loses nothing, so it is not counted (issue #171's
    /// first nit, tracked in #250).
    #[test]
    fn last_writer_wins_counts_only_updates_that_replace_another_value() {
        let root = tempfile::tempdir().unwrap();
        let (repo, c) = repo_with_commits(root.path(), &["x", "y", "z"]);
        let hook = install(root.path(), "proc-receive", Fault::LastWriterWins);
        let git = GitClient::new(root.path());
        let update = |old: &str, new: &str| -> String {
            let input = format!(
                "{}0000{}0000",
                pkt("version=1\n"),
                pkt(&format!("{old} {new} refs/heads/b\n"))
            );
            let out = run_hook(&hook, &repo, input.as_bytes());
            assert!(out.status.success(), "stderr:\n{}", stderr(&out));
            git.run(&repo, ["rev-parse", "refs/heads/b"])
                .unwrap()
                .trim()
                .to_owned()
        };

        // The ref is already at the new value: a no-op, not a lost update.
        git.run(&repo, ["update-ref", "refs/heads/b", &c[1]])
            .unwrap();
        assert_eq!(update(&c[0], &c[1]), c[1]);
        assert_eq!(firings(&repo), 0, "a no-op update was counted");

        // The ref holds a third value: the update overwrites it.
        git.run(&repo, ["update-ref", "refs/heads/b", &c[2]])
            .unwrap();
        assert_eq!(update(&c[0], &c[1]), c[1]);
        assert_eq!(firings(&repo), 1, "a lost update was not counted");
    }

    /// [`Fault::LoseObject`] counts a tree only once the repository can no
    /// longer read it. Deleting the loose copy of a tree that is also in a
    /// pack loses nothing, so it is not counted (issue #171's second nit,
    /// tracked in #250).
    #[test]
    fn lose_object_counts_only_a_tree_it_made_unreadable() {
        for packed in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (repo, c) = repo_with_commits(root.path(), &["x"]);
            let git = GitClient::new(root.path());
            let tree = git
                .run(&repo, ["rev-parse", &format!("{}^{{tree}}", c[0])])
                .unwrap()
                .trim()
                .to_owned();
            if packed {
                // Pack the commit's objects and keep their loose copies.
                git.run(&repo, ["update-ref", "refs/heads/b", &c[0]])
                    .unwrap();
                git.run(&repo, ["repack", "-a", "-q"]).unwrap();
            }
            let loose = repo.join("objects").join(&tree[..2]).join(&tree[2..]);
            assert!(loose.exists(), "packed {packed}: the tree is not loose");

            let hook = install(
                root.path(),
                "post-receive",
                Fault::LoseObject { nth_push: 1 },
            );
            let out = run_hook(&hook, &repo, &create_line(&c[0], "refs/heads/b"));
            assert!(out.status.success(), "stderr:\n{}", stderr(&out));

            let readable = git
                .output(&repo, ["cat-file", "-e", &tree])
                .unwrap()
                .status
                .success();
            assert_eq!(readable, packed, "packed {packed}: tree readable");
            assert_eq!(
                firings(&repo),
                usize::from(!packed),
                "packed {packed}: firings recorded"
            );
        }
    }

    /// A hook can be run the moment it is installed, even while other
    /// threads of the process start programs. A child forked between the
    /// hook's write and its close holds the file open for writing until it
    /// runs its own program, and Linux refuses to run a file open for
    /// writing ("Text file busy"). The test threads of this crate fork all
    /// the time, so hooks written by this process used to fail to start now
    /// and then (main CI, 2026-11-06).
    #[test]
    fn a_hook_runs_at_once_while_other_threads_fork() {
        use std::sync::atomic::{AtomicBool, Ordering};

        const FORKERS: usize = 4;
        const HOOKS: usize = 300;
        let stop = Arc::new(AtomicBool::new(false));
        let forkers: Vec<_> = (0..FORKERS)
            .map(|_| {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        Command::new("true").status().unwrap();
                    }
                })
            })
            .collect();

        let dir = tempfile::tempdir().unwrap();
        let line = create_line(&"1".repeat(40), "refs/heads/b");
        let busy = (0..HOOKS)
            .filter(|i| {
                let hook = dir.path().join(format!("hook-{i}"));
                let fault = Fault::RejectPush { nth_push: u32::MAX };
                write_executable(&hook, fault.hook()).unwrap();
                match Command::new(&hook)
                    .current_dir(dir.path())
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                {
                    Ok(mut child) => {
                        child.stdin.take().unwrap().write_all(&line).unwrap();
                        child.wait().unwrap();
                        false
                    }
                    Err(e) if e.kind() == io::ErrorKind::ExecutableFileBusy => true,
                    Err(e) => panic!("hook {i} did not start: {e}"),
                }
            })
            .count();

        stop.store(true, Ordering::Relaxed);
        for forker in forkers {
            forker.join().unwrap();
        }
        assert_eq!(busy, 0, "hooks of {HOOKS} refused as Text file busy");
    }
}

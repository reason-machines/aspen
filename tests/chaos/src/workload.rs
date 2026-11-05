//! The clients: stock `git` processes doing what agents and CI do.
//!
//! Each client has its own repository and, in a loop:
//!
//! - **pushes** to a ref: fetches it (a read of its tip `P`), builds one to
//!   three commits on `P` with `git fast-import` and pushes the last one.
//!   Refs are trunk (`main`, through the repository URL), shared branches
//!   every client pushes to (contended), and the client's own branch (no
//!   contention). Pushes only fast-forward, and each commit is unique, so a
//!   push is a compare-and-swap of the ref from `P` to the new tip;
//! - **reads** a ref's tip with `git ls-remote`;
//! - **fetches** a ref with `--depth=1`, or with packfile URIs enabled.
//!
//! Every fetch runs with `fetch.fsckObjects`, so Git checks each object it
//! receives. A fetch that fails the way a server bug would (objects
//! missing from a complete pack, a corrupt object) is recorded as a
//! violation even while faults are active; a fetch that fails the way a
//! fault would (a closed connection, an early EOF, a retryable error) is not.

use std::fmt::Write as _;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;

use aspen_testkit::GitServer;
use aspen_testkit::git::isolated_git;
use aspen_testkit::rng::Rng;

use crate::history::{Call, History, Oid, Outcome};
use crate::process::{Finished, run_for};
use crate::report::{Finding, Invariant, Tally};
use crate::run::ActivePhase;

/// Lines of stderr that mean the server sent something wrong, as opposed
/// to a transport failure.
pub const CORRUPTION_SIGNS: [&str; 10] = [
    "did not send all necessary objects",
    "missing blob",
    "missing tree",
    "missing commit",
    "hash mismatch",
    "fsck error",
    "error in object",
    "unresolved delta",
    "bad object",
    "is corrupt",
];

/// Rejection reasons that mean the push did not land because the ref was
/// not at the push's base: the client or the server compared it with the
/// current tip and refused ([`Outcome::Fail`]).
pub const STALE_REJECTIONS: [&str; 6] = [
    "non-fast-forward",
    "fetch first",
    "incorrect old value",
    "failed to update ref",
    "reference already exists",
    "stale info",
];

/// Rejection reasons that mean the push did not land for a reason that
/// says nothing about the ref's value: the server could not lock the ref or
/// could not decide ancestry yet ([`Outcome::NoEffect`]). Stock Git's
/// "cannot lock ref ... but expected ..." is a compare-and-swap loss and
/// counts as stale.
pub const NO_EFFECT_REJECTIONS: [&str; 4] = [
    "cannot lock ref",
    "failed to lock",
    "cannot decide ancestry",
    "trunk was rewritten",
];

/// How `git push --porcelain` marks a rejection the server made, as
/// opposed to one the client made from the advertisement (`[rejected]`).
const REMOTE_REJECTED: &str = "[remote rejected]";

/// Counter for pushes whose server-side rejection came after libcurl
/// resent the request (issue #132): their outcome is unknown.
pub const REJECTED_AFTER_RESEND: &str = "push rejected after a resend (unknown)";

/// Counter for fetches that asked for packfile URIs.
pub const URI_FETCHES: &str = "fetches asking for packfile URIs";

/// Counter for packfile URIs servers sent (and clients then downloaded).
pub const URIS_USED: &str = "packfile URIs used";

/// A client with this many local errors in a row (its own repository,
/// `fast-import`) reports an [`Invariant::Unchecked`] finding.
pub const LOCAL_ERRORS_IN_A_ROW: usize = 3;

/// Which refs exist and who pushes to them.
#[derive(Debug, Clone)]
pub struct Keys {
    /// `refs/heads/main`: pushed through the repository URL.
    pub trunk: String,
    /// Branches every client pushes to.
    pub shared: Vec<String>,
    /// One branch per client.
    pub own: Vec<String>,
}

impl Keys {
    /// Trunk, `shared` shared branches and one branch per client.
    pub fn new(clients: usize, shared: usize) -> Self {
        Self {
            trunk: "refs/heads/main".into(),
            shared: (0..shared)
                .map(|i| format!("refs/heads/shared-{i}"))
                .collect(),
            own: (0..clients)
                .map(|i| format!("refs/heads/agent-{i}"))
                .collect(),
        }
    }

    /// Every ref, trunk first.
    pub fn all(&self) -> Vec<String> {
        let mut v = vec![self.trunk.clone()];
        v.extend(self.shared.iter().cloned());
        v.extend(self.own.iter().cloned());
        v
    }
}

/// How often each action runs, relative to the others.
#[derive(Debug, Clone, Copy)]
pub struct Weights {
    pub push_trunk: u32,
    pub push_shared: u32,
    pub push_own: u32,
    pub read: u32,
    pub shallow_fetch: u32,
    pub uri_fetch: u32,
}

impl Weights {
    /// A mix of contended and uncontended pushes plus reads.
    pub const DEFAULT: Self = Self {
        push_trunk: 3,
        push_shared: 3,
        push_own: 3,
        read: 2,
        shallow_fetch: 1,
        uri_fetch: 1,
    };

    /// Many small pushes, so covers fill quickly and compaction runs often.
    pub const COMPACTION: Self = Self {
        push_trunk: 4,
        push_shared: 1,
        push_own: 6,
        read: 1,
        shallow_fetch: 0,
        uri_fetch: 1,
    };
}

/// Where a client talks to.
#[derive(Debug)]
pub struct Target<'a> {
    pub server: &'a dyn GitServer,
    pub repo_url: String,
    pub keys: Keys,
    /// Push branches through the repository URL now and then, not only
    /// through their branch URL. On a server that advertises only trunk
    /// there (Aspen, spike S2) the client sends a zero old value, so the
    /// server's own fast-forward check decides the push. Off for servers
    /// that refuse such pushes (stock Git with hidden refs).
    pub unscoped_branch_pushes: bool,
}

impl Target<'_> {
    /// The URL for pushing and fetching `key`: the repository URL for
    /// trunk, the branch URL for every other ref.
    pub fn url_for(&self, key: &str) -> String {
        if key == self.keys.trunk {
            self.repo_url.clone()
        } else {
            self.server.branch_url(&self.repo_url, key)
        }
    }
}

/// Git settings for every client command.
const CLIENT_CONFIG: [(&str, &str); 8] = [
    ("protocol.version", "2"),
    ("fetch.fsckObjects", "true"),
    ("transfer.fsckObjects", "true"),
    // Give up on a stalled transfer instead of waiting for TCP to time out.
    ("http.lowSpeedLimit", "1"),
    ("http.lowSpeedTime", "20"),
    ("core.compression", "1"),
    ("gc.auto", "0"),
    // Large enough that git never sends its authentication probe POST
    // ahead of a push, so a second POST in a trace is a resend.
    ("http.postBuffer", "67108864"),
];

/// A git command for client work in `dir`.
pub fn git(home: &Path, dir: &Path) -> Command {
    let mut c = isolated_git(home);
    for (k, v) in CLIENT_CONFIG {
        c.arg("-c").arg(format!("{k}={v}"));
    }
    c.current_dir(dir);
    c
}

/// How long one client command may take before it counts as an unknown
/// outcome.
pub const OP_TIMEOUT: Duration = Duration::from_secs(90);

/// One client.
#[derive(Debug)]
pub struct Client {
    pub index: usize,
    home: PathBuf,
    repo: PathBuf,
    rng: Rng,
    commits: u64,
}

/// What a fetch saw.
enum Fetched {
    Tip(Option<Oid>),
    Failed,
}

impl Client {
    /// Creates client `index`'s repository under `root`.
    pub fn new(root: &Path, index: usize, seed: u64) -> io::Result<Self> {
        let home = root.join(format!("client-{index}"));
        let repo = home.join("repo");
        std::fs::create_dir_all(&repo)?;
        let c = Self {
            index,
            rng: Rng::new(seed ^ (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)),
            home,
            repo,
            commits: 0,
        };
        run_git(&c.home, &c.repo, &["init", "-q", "--initial-branch=main"])?;
        Ok(c)
    }

    /// Runs actions until `phase` is over, recording each in `history`,
    /// findings in `findings` and counts in `tally`.
    #[allow(clippy::too_many_arguments)]
    pub fn run(
        &mut self,
        target: &Target<'_>,
        weights: Weights,
        history: &History,
        findings: &Mutex<Vec<Finding>>,
        tally: &Tally,
        epoch: usize,
        phase: &ActivePhase<'_>,
    ) {
        let mut errors_in_a_row = 0;
        let mut reported = false;
        let w = [
            weights.push_trunk,
            weights.push_shared,
            weights.push_own,
            weights.read,
            weights.shallow_fetch,
            weights.uri_fetch,
        ];
        while !phase.over() {
            let action = self.rng.weighted(&w);
            let key = match action {
                0 => target.keys.trunk.clone(),
                1 if !target.keys.shared.is_empty() => {
                    target.keys.shared[self.rng.below(target.keys.shared.len())].clone()
                }
                1 | 2 => target.keys.own[self.index].clone(),
                _ => {
                    let all = target.keys.all();
                    all[self.rng.below(all.len())].clone()
                }
            };
            let result = match action {
                0..=2 => self.push(target, &key, history, findings, tally, epoch),
                3 => self.read(target, &key, history),
                4 => self.shallow_fetch(target, &key, history, findings, epoch),
                _ => self
                    .fetch(target, &key, true, history, findings, tally, epoch)
                    .map(drop),
            };
            match result {
                Ok(()) => errors_in_a_row = 0,
                Err(e) => {
                    // Local trouble (the client's own repository, a
                    // `fast-import` that failed): not the server's fault,
                    // but while it lasts the client checks nothing.
                    eprintln!("chaos: client {} local error: {e}", self.index);
                    tally.add("client local errors", 1);
                    errors_in_a_row += 1;
                    if errors_in_a_row >= LOCAL_ERRORS_IN_A_ROW && !reported {
                        reported = true;
                        lock(findings).push(Finding::new(
                            Invariant::Unchecked,
                            epoch,
                            format!(
                                "client {} failed {errors_in_a_row} times in a row on its own \
                                 repository; last error: {e}",
                                self.index
                            ),
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    /// `git ls-remote` of `key`: a read.
    fn read(&mut self, target: &Target<'_>, key: &str, history: &History) -> io::Result<()> {
        let url = target.url_for(key);
        let invoked = history.now();
        let done = run_for(
            git(&self.home, &self.repo).args(["ls-remote", &url, key]),
            OP_TIMEOUT,
        )?;
        if done.success() {
            let value = done.stdout.lines().find_map(|l| {
                l.split_once('\t')
                    .filter(|(_, n)| *n == key)
                    .map(|(o, _)| o.to_owned())
            });
            history.record(
                self.index,
                key,
                Call::Read { value },
                Outcome::Ok,
                invoked,
                "ls-remote",
            );
        }
        Ok(())
    }

    /// Fetches `key` into `refs/chaos/<key>`; records the tip it saw as a
    /// read. With `uris`, counts the packfile URIs the server sent.
    #[allow(clippy::too_many_arguments)]
    fn fetch(
        &mut self,
        target: &Target<'_>,
        key: &str,
        uris: bool,
        history: &History,
        findings: &Mutex<Vec<Finding>>,
        tally: &Tally,
        epoch: usize,
    ) -> io::Result<Fetched> {
        let url = target.url_for(key);
        let local = format!("refs/chaos/{}", key.trim_start_matches("refs/"));
        let mut cmd = git(&self.home, &self.repo);
        let trace = self.home.join("packet-trace");
        if uris {
            let _ = std::fs::remove_file(&trace);
            cmd.args(["-c", "fetch.uriprotocols=http,https"])
                .env("GIT_TRACE_PACKET", &trace);
        }
        cmd.args([
            "fetch",
            "--no-tags",
            "--quiet",
            &url,
            &format!("+{key}:{local}"),
        ]);
        let invoked = history.now();
        let done = run_for(&mut cmd, OP_TIMEOUT)?;
        if uris {
            tally.add(URI_FETCHES, 1);
            let sent = std::fs::read_to_string(&trace)
                .map(|t| packfile_uris_in_trace(&t))
                .unwrap_or(0);
            tally.add(URIS_USED, sent as u64);
        }
        if done.success() {
            let tip = rev_parse(&self.home, &self.repo, &local)?;
            let note = if uris {
                "fetch (packfile URIs)"
            } else {
                "fetch"
            };
            history.record(
                self.index,
                key,
                Call::Read {
                    value: Some(tip.clone()),
                },
                Outcome::Ok,
                invoked,
                note,
            );
            return Ok(Fetched::Tip(Some(tip)));
        }
        if done.stderr.contains("couldn't find remote ref") {
            history.record(
                self.index,
                key,
                Call::Read { value: None },
                Outcome::Ok,
                invoked,
                "fetch: no such ref",
            );
            return Ok(Fetched::Tip(None));
        }
        note_corruption(
            &done,
            &format!("client {} fetch of {key}", self.index),
            findings,
            epoch,
        );
        Ok(Fetched::Failed)
    }

    /// A `--depth=1` fetch of `key` into a scratch repository, which is
    /// then checked with `git fsck --strict`.
    fn shallow_fetch(
        &mut self,
        target: &Target<'_>,
        key: &str,
        history: &History,
        findings: &Mutex<Vec<Finding>>,
        epoch: usize,
    ) -> io::Result<()> {
        let scratch = self.home.join("shallow");
        let _ = std::fs::remove_dir_all(&scratch);
        std::fs::create_dir_all(&scratch)?;
        run_git(&self.home, &scratch, &["init", "-q"])?;
        let url = target.url_for(key);
        let invoked = history.now();
        let done = run_for(
            git(&self.home, &scratch).args([
                "fetch",
                "--no-tags",
                "--quiet",
                "--depth=1",
                &url,
                key,
            ]),
            OP_TIMEOUT,
        )?;
        if !done.success() {
            note_corruption(
                &done,
                &format!("client {} depth-1 fetch of {key}", self.index),
                findings,
                epoch,
            );
            return Ok(());
        }
        let tip = rev_parse(&self.home, &scratch, "FETCH_HEAD")?;
        history.record(
            self.index,
            key,
            Call::Read { value: Some(tip) },
            Outcome::Ok,
            invoked,
            "fetch --depth=1",
        );
        let fsck = run_for(
            git(&self.home, &scratch).args(["fsck", "--strict", "--no-dangling"]),
            OP_TIMEOUT,
        )?;
        if !fsck.success() {
            findings
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Finding::new(
                    Invariant::CorruptFetch,
                    epoch,
                    format!(
                        "client {}: a depth-1 fetch of {key} succeeded but `git fsck --strict` failed:\n{}",
                        self.index,
                        fsck.describe()
                    ),
                ));
        }
        Ok(())
    }

    /// Fetch, commit, push `key`.
    fn push(
        &mut self,
        target: &Target<'_>,
        key: &str,
        history: &History,
        findings: &Mutex<Vec<Finding>>,
        tally: &Tally,
        epoch: usize,
    ) -> io::Result<()> {
        let uris = self.rng.below(4) == 0;
        let Fetched::Tip(Some(base)) =
            self.fetch(target, key, uris, history, findings, tally, epoch)?
        else {
            return Ok(());
        };
        let commits = 1 + self.rng.below(3);
        let tip = self.commit(&base, commits)?;
        let unscoped =
            target.unscoped_branch_pushes && key != target.keys.trunk && self.rng.below(2) == 0;
        let url = if unscoped {
            target.repo_url.clone()
        } else {
            target.url_for(key)
        };
        // libcurl resends a request whose reused connection dies before an
        // answer, so one `git push` can deliver its POST twice: the first
        // copy may land and the second be refused by the server (issue
        // #132). The trace shows the resend; such a server-side rejection
        // is not definite. A rejection the client made itself from the
        // advertisement (`[rejected]`) still is.
        let trace = self.home.join("curl-trace");
        let _ = std::fs::remove_file(&trace);
        let invoked = history.now();
        let done = run_for(
            git(&self.home, &self.repo)
                .env("GIT_TRACE_CURL", &trace)
                .env("GIT_TRACE_CURL_NO_DATA", "1")
                .args(["push", "--porcelain", &url, &format!("{tip}:{key}")]),
            OP_TIMEOUT,
        )?;
        let posts = std::fs::read_to_string(&trace)
            .map(|t| receive_pack_posts(&t))
            .unwrap_or(0);
        let (mut outcome, mut note) = classify_push(&done, key);
        if let Some((o, n)) = reclassify_after_resend(outcome, &note, posts) {
            (outcome, note) = (o, n);
            tally.add(REJECTED_AFTER_RESEND, 1);
        }
        history.record(
            self.index,
            key,
            Call::Push {
                from: Some(base),
                to: tip,
            },
            outcome,
            invoked,
            format!(
                "{commits} commit(s){}; {note}",
                if unscoped {
                    " via the repository URL"
                } else {
                    ""
                }
            ),
        );
        Ok(())
    }

    /// Builds `n` commits on `base` with `git fast-import`; returns the
    /// last one. Each touches a shared file (so packs get deltas), adds a
    /// file of its own, and now and then a larger blob.
    fn commit(&mut self, base: &str, n: usize) -> io::Result<Oid> {
        let mut stream = String::new();
        for i in 0..n {
            self.commits += 1;
            let token = format!(
                "c{}-{}-{:016x}",
                self.index,
                self.commits,
                self.rng.next_u64()
            );
            let msg = format!("chaos {token}\n");
            let when = 1_700_000_000 + self.commits;
            let _ = write!(
                stream,
                "commit refs/chaos/work\ncommitter Chaos <chaos@aspen.invalid> {when} +0000\ndata {}\n{msg}",
                msg.len()
            );
            if i == 0 {
                let _ = writeln!(stream, "from {base}");
            }
            let mut shared = String::new();
            let changed = self.rng.below(200);
            for line in 0..200 {
                if line == changed {
                    let _ = writeln!(shared, "line {line}: {token}");
                } else {
                    let _ = writeln!(
                        shared,
                        "line {line}: the quick brown fox jumps over the lazy dog"
                    );
                }
            }
            let own = format!("{token}\n");
            let _ = write!(
                stream,
                "M 100644 inline shared.txt\ndata {}\n{shared}\nM 100644 inline c{}/{}.txt\ndata {}\n{own}\n",
                shared.len(),
                self.index,
                self.commits,
                own.len()
            );
            if self.rng.below(8) == 0 {
                let size = 16 * 1024 + self.rng.below(112 * 1024);
                let mut blob = String::with_capacity(size + 1);
                while blob.len() < size {
                    let _ = write!(blob, "{:016x}", self.rng.next_u64());
                }
                blob.push('\n');
                let _ = write!(
                    stream,
                    "M 100644 inline c{}/blob-{}.bin\ndata {}\n{blob}\n",
                    self.index,
                    self.commits,
                    blob.len()
                );
            }
        }
        let mut child = git(&self.home, &self.repo)
            .args(["fast-import", "--quiet", "--force"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("no stdin"))?
            .write_all(stream.as_bytes())?;
        let out = child.wait_with_output()?;
        if !out.status.success() {
            return Err(io::Error::other(format!(
                "fast-import: {}",
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        rev_parse(&self.home, &self.repo, "refs/chaos/work")
    }
}

/// Classifies a `git push --porcelain` of one ref.
pub fn classify_push(done: &Finished, key: &str) -> (Outcome, String) {
    if done.status.is_none() {
        return (Outcome::Unknown, "timed out".into());
    }
    let line = done.stdout.lines().find_map(|l| {
        let mut parts = l.splitn(3, '\t');
        let flag = parts.next()?;
        let refs = parts.next()?;
        let summary = parts.next().unwrap_or("");
        refs.ends_with(&format!(":{key}"))
            .then(|| (flag.to_owned(), summary.to_owned()))
    });
    let last_err = done
        .stderr
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .to_owned();
    match line {
        Some((flag, summary)) => match flag.as_str() {
            " " | "*" | "+" | "=" => (Outcome::Ok, summary),
            "!" if summary.contains("cannot lock ref") && summary.contains("but expected") => {
                (Outcome::Fail, summary)
            }
            "!" if NO_EFFECT_REJECTIONS.iter().any(|r| summary.contains(r)) => {
                (Outcome::NoEffect, summary)
            }
            "!" if STALE_REJECTIONS.iter().any(|r| summary.contains(r)) => (Outcome::Fail, summary),
            _ => (Outcome::Unknown, format!("{summary}; {last_err}")),
        },
        None => (Outcome::Unknown, last_err),
    }
}

/// The outcome of a push that `classify_push` called `outcome` (with
/// `note`), given that its trace shows `posts` receive-pack POSTs. A
/// rejection the server made (`[remote rejected]`) after libcurl resent
/// the request may answer the second copy while the first one landed
/// (issue #132), so it becomes [`Outcome::Unknown`]; `None` leaves the
/// outcome as it is. A rejection the client made from the advertisement
/// (`[rejected]`) never reached the server and stays definite.
pub fn reclassify_after_resend(
    outcome: Outcome,
    note: &str,
    posts: usize,
) -> Option<(Outcome, String)> {
    (matches!(outcome, Outcome::Fail | Outcome::NoEffect)
        && posts > 1
        && note.contains(REMOTE_REJECTED))
    .then(|| {
        (
            Outcome::Unknown,
            format!("{note}; rejected after the client sent its request {posts} times"),
        )
    })
}

/// How many times a `GIT_TRACE_CURL` trace sent a `git-receive-pack`
/// POST.
pub fn receive_pack_posts(trace: &str) -> usize {
    trace
        .lines()
        .filter(|l| l.contains("=> Send header: POST ") && l.contains("git-receive-pack"))
        .count()
}

/// How many packfile URIs a `GIT_TRACE_PACKET` trace of a protocol v2
/// fetch received: the lines of its `packfile-uris` section, each a pack
/// hash and a URL.
pub fn packfile_uris_in_trace(trace: &str) -> usize {
    let mut in_section = false;
    let mut n = 0;
    for line in trace.lines() {
        let Some((_, pkt)) = line.split_once("fetch< ") else {
            continue;
        };
        let pkt = pkt.trim_end();
        if pkt == "packfile-uris" {
            in_section = true;
            continue;
        }
        if !in_section {
            continue;
        }
        let mut words = pkt.splitn(2, ' ');
        let hash = words.next().unwrap_or("");
        match words.next() {
            Some(url)
                if hash.len() == 40
                    && hash.bytes().all(|b| b.is_ascii_hexdigit())
                    && (url.starts_with("http://") || url.starts_with("https://")) =>
            {
                n += 1;
            }
            // A delimiter, flush or the next section ends it.
            _ => in_section = false,
        }
    }
    n
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Records a [`Invariant::CorruptFetch`] when a failed fetch's output shows
/// a sign of a server bug.
pub fn note_corruption(done: &Finished, what: &str, findings: &Mutex<Vec<Finding>>, epoch: usize) {
    let text = format!("{}\n{}", done.stdout, done.stderr).to_lowercase();
    if let Some(sign) = CORRUPTION_SIGNS.iter().find(|s| text.contains(*s)) {
        findings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Finding::new(
                Invariant::CorruptFetch,
                epoch,
                format!("{what} failed with {sign:?}:\n{}", done.describe()),
            ));
    }
}

/// Runs `git args` in `dir` and returns stdout.
pub fn run_git(home: &Path, dir: &Path, args: &[&str]) -> io::Result<String> {
    let done = run_for(git(home, dir).args(args), OP_TIMEOUT)?;
    if done.success() {
        Ok(done.stdout)
    } else {
        Err(io::Error::other(format!(
            "git {args:?}: {}",
            done.describe()
        )))
    }
}

/// `git rev-parse --verify <rev>`.
pub fn rev_parse(home: &Path, dir: &Path, rev: &str) -> io::Result<Oid> {
    Ok(
        run_git(home, dir, &["rev-parse", "--verify", "--quiet", rev])?
            .trim()
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finished(code: Option<i32>, stdout: &str, stderr: &str) -> Finished {
        use std::os::unix::process::ExitStatusExt as _;
        Finished {
            status: code.map(|c| std::process::ExitStatus::from_raw(c << 8)),
            stdout: stdout.into(),
            stderr: stderr.into(),
        }
    }

    #[test]
    fn push_outcomes_are_classified() {
        let k = "refs/heads/main";
        let ok = finished(
            Some(0),
            "To http://x/r.git\n \tabc:refs/heads/main\t1111..2222\nDone\n",
            "",
        );
        assert_eq!(classify_push(&ok, k).0, Outcome::Ok);
        let stale = finished(
            Some(1),
            "To http://x\n!\tabc:refs/heads/main\t[rejected] (fetch first)\nDone\n",
            "",
        );
        assert_eq!(classify_push(&stale, k).0, Outcome::Fail);
        let cas = finished(
            Some(1),
            "!\tabc:refs/heads/main\t[remote rejected] (failed to update ref: it moved during the push, retry)\n",
            "",
        );
        assert_eq!(classify_push(&cas, k).0, Outcome::Fail);
        // A storage error may come after the write: unknown.
        let storage = finished(
            Some(1),
            "!\tabc:refs/heads/main\t[remote rejected] (internal storage error; retry)\n",
            "",
        );
        assert_eq!(classify_push(&storage, k).0, Outcome::Unknown);
        let hangup = finished(
            Some(128),
            "",
            "fatal: the remote end hung up unexpectedly\n",
        );
        assert_eq!(classify_push(&hangup, k).0, Outcome::Unknown);
        assert_eq!(
            classify_push(&finished(None, "", ""), k).0,
            Outcome::Unknown
        );
        // Refusals that do not compare against the ref.
        let undecided = finished(
            Some(1),
            "!\tabc:refs/heads/main\t[remote rejected] (cannot decide ancestry yet; history is still being indexed, retry)\n",
            "",
        );
        assert_eq!(classify_push(&undecided, k).0, Outcome::NoEffect);
        let locked = finished(
            Some(1),
            "!\tabc:refs/heads/main\t[remote rejected] (cannot lock ref 'refs/heads/main': Unable to create '/x/refs/heads/main.lock': File exists.)\n",
            "",
        );
        assert_eq!(classify_push(&locked, k).0, Outcome::NoEffect);
        // Stock Git's compare-and-swap loss is stale.
        let moved = finished(
            Some(1),
            "!\tabc:refs/heads/main\t[remote rejected] (cannot lock ref 'refs/heads/main': is at 1111 but expected 2222)\n",
            "",
        );
        assert_eq!(classify_push(&moved, k).0, Outcome::Fail);
        // Another ref's line does not count.
        let other = finished(Some(0), " \tabc:refs/heads/other\t1..2\n", "");
        assert_eq!(classify_push(&other, k).0, Outcome::Unknown);
    }

    #[test]
    fn a_server_rejection_after_a_resend_is_unknown() {
        let k = "refs/heads/main";
        let cas = finished(
            Some(1),
            "!\tabc:refs/heads/main\t[remote rejected] (incorrect old value provided)\n",
            "",
        );
        let (outcome, note) = classify_push(&cas, k);
        assert_eq!(outcome, Outcome::Fail);
        // Sent once: the rejection is the server's answer to the only copy.
        assert_eq!(reclassify_after_resend(outcome, &note, 1), None);
        // Sent twice: the first copy may have landed.
        let (after, why) = reclassify_after_resend(outcome, &note, 2).unwrap();
        assert_eq!(after, Outcome::Unknown);
        assert!(why.contains("2 times"), "{why}");
        // A refusal that never compared against the ref, after a resend,
        // still says nothing about the first copy.
        let locked = finished(
            Some(1),
            "!\tabc:refs/heads/main\t[remote rejected] (cannot decide ancestry yet; retry)\n",
            "",
        );
        let (outcome, note) = classify_push(&locked, k);
        assert_eq!(outcome, Outcome::NoEffect);
        assert_eq!(
            reclassify_after_resend(outcome, &note, 3).map(|r| r.0),
            Some(Outcome::Unknown)
        );
        // The client's own rejection from the advertisement never reached
        // the server, however many POSTs the trace shows.
        let stale = finished(
            Some(1),
            "!\tabc:refs/heads/main\t[rejected] (fetch first)\n",
            "",
        );
        let (outcome, note) = classify_push(&stale, k);
        assert_eq!(reclassify_after_resend(outcome, &note, 2), None);
        // Accepted and unknown outcomes are left alone.
        assert_eq!(
            reclassify_after_resend(Outcome::Ok, "[remote rejected]", 2),
            None
        );
        assert_eq!(
            reclassify_after_resend(Outcome::Unknown, "[remote rejected]", 2),
            None
        );
    }

    #[test]
    fn receive_pack_posts_are_counted_from_a_curl_trace() {
        let trace = "\
12:00:00.000001 http.c:756              => Send header: GET /r.git/info/refs?service=git-receive-pack HTTP/1.1
12:00:00.000002 http.c:756              <= Recv header: HTTP/1.1 200 OK
12:00:00.000003 http.c:756              => Send header: POST /r.git/git-receive-pack HTTP/1.1
12:00:00.000004 http.c:756              == Info: Connection died, retrying a fresh connect
12:00:00.000005 http.c:756              => Send header: POST /r.git/git-receive-pack HTTP/1.1
12:00:00.000006 http.c:756              => Send header: POST /r.git/git-upload-pack HTTP/1.1
12:00:00.000007 http.c:756              <= Recv header: POST /r.git/git-receive-pack HTTP/1.1
";
        assert_eq!(receive_pack_posts(trace), 2);
        assert_eq!(receive_pack_posts(""), 0);
    }

    #[test]
    fn packfile_uris_are_counted_from_a_packet_trace() {
        let trace = "\
12:00:00.000001 pkt-line.c:86 packet: fetch< acknowledgments
12:00:00.000002 pkt-line.c:86 packet: fetch< ready
12:00:00.000003 pkt-line.c:86 packet: fetch< 0001
12:00:00.000004 pkt-line.c:86 packet: fetch< packfile-uris
12:00:00.000005 pkt-line.c:86 packet: fetch< 0123456789abcdef0123456789abcdef01234567 http://127.0.0.1:1/-/express/k1/x.pack
12:00:00.000006 pkt-line.c:86 packet: fetch< 89abcdef0123456789abcdef0123456789abcdef https://cdn.example/y.pack
12:00:00.000007 pkt-line.c:86 packet: fetch< 0001
12:00:00.000008 pkt-line.c:86 packet: fetch< packfile
12:00:00.000009 pkt-line.c:86 packet: fetch< 0123456789abcdef0123456789abcdef01234567 http://not-a-uri-line
";
        assert_eq!(packfile_uris_in_trace(trace), 2);
        assert_eq!(packfile_uris_in_trace("packet: fetch< packfile\n"), 0);
    }
}

//! The checks that run at the end of every epoch, once every fault is
//! healed and the deployment has had time to settle.
//!
//! - **Reads**: each ref's tip, through the same URL clients use. They are
//!   recorded as operations, so they end the epoch's history for the
//!   linearizability check.
//! - **Durability** ("an acknowledged push is never lost"): every commit
//!   whose push was acknowledged, since the repository was created, is in
//!   the history of its ref's tip. Pushes only fast-forward, so nothing
//!   legitimately removes one.
//! - **Fetchability** ("no ref ever points at objects that can't be
//!   fetched"): every ref is fetched into a fresh repository (the first ref
//!   as a full clone, the rest against what is already there), into a
//!   persistent observer repository (incremental fetches, which exercise
//!   have-based planning and the cover skip list), and with packfile URIs
//!   on every other epoch; trunk is also fetched with `--depth=1`. Each
//!   result must pass `git fsck --strict`.
//!
//! A check that cannot run (a scratch repository that cannot be created,
//! a `rev-list` that fails) is an [`Invariant::Unchecked`] finding, never
//! a silent skip.

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::history::{Call, History, Oid, Op, Outcome, short};
use crate::process::{Finished, run_for};
use crate::report::{Finding, Invariant, Tally};
use crate::workload::{
    OP_TIMEOUT, Target, URI_FETCHES, URIS_USED, git, packfile_uris_in_trace, rev_parse, run_git,
};

/// How long a healed deployment may take to answer the boundary reads.
pub const PATIENCE: Duration = Duration::from_secs(60);

/// The suite's own client for checks.
#[derive(Debug)]
pub struct Checker {
    home: PathBuf,
    observer: PathBuf,
    scratch: PathBuf,
}

impl Checker {
    /// A checker working under `root`.
    pub fn new(root: &Path) -> io::Result<Self> {
        let home = root.join("checker");
        std::fs::create_dir_all(&home)?;
        let c = Self {
            observer: home.join("observer.git"),
            scratch: home.join("scratch"),
            home,
        };
        c.reset()?;
        Ok(c)
    }

    /// Starts over with an empty observer repository (for a new
    /// repository on the server).
    pub fn reset(&self) -> io::Result<()> {
        let _ = std::fs::remove_dir_all(&self.observer);
        std::fs::create_dir_all(&self.observer)?;
        run_git(&self.home, &self.observer, &["init", "-q", "--bare"])?;
        Ok(())
    }

    /// Reads every ref's tip, retrying each for up to [`PATIENCE`]. Each
    /// successful read is recorded in `history`.
    pub fn read_tips(
        &self,
        target: &Target<'_>,
        history: &History,
        epoch: usize,
    ) -> Result<BTreeMap<String, Option<Oid>>, Finding> {
        let mut tips = BTreeMap::new();
        for key in target.keys.all() {
            let url = target.url_for(&key);
            let deadline = Instant::now() + PATIENCE;
            loop {
                let invoked = history.now();
                let done = run_for(
                    git(&self.home, &self.home).args(["ls-remote", &url, &key]),
                    OP_TIMEOUT,
                );
                match done {
                    Ok(d) if d.success() => {
                        let value = d.stdout.lines().find_map(|l| {
                            l.split_once('\t')
                                .filter(|(_, n)| *n == key)
                                .map(|(o, _)| o.to_owned())
                        });
                        history.record(
                            usize::MAX,
                            &key,
                            Call::Read {
                                value: value.clone(),
                            },
                            Outcome::Ok,
                            invoked,
                            "boundary read",
                        );
                        tips.insert(key.clone(), value);
                        break;
                    }
                    other => {
                        if Instant::now() > deadline {
                            let why = match other {
                                Ok(d) => d.describe(),
                                Err(e) => e.to_string(),
                            };
                            return Err(Finding::new(
                                Invariant::Unavailable,
                                epoch,
                                format!(
                                    "`git ls-remote {url} {key}` kept failing for {PATIENCE:?}:\n{why}"
                                ),
                            ));
                        }
                        std::thread::sleep(Duration::from_millis(500));
                    }
                }
            }
        }
        Ok(tips)
    }

    /// Fetches `key` from `url` into `repo` (as `refs/check/<key>`), with
    /// one retry; the error holds both attempts' output. A fetch with
    /// `fetch.uriprotocols` set counts the packfile URIs it received.
    fn fetch(
        &self,
        repo: &Path,
        url: &str,
        key: &str,
        extra: &[&str],
        tally: &Tally,
    ) -> Result<(), String> {
        let local = format!("refs/check/{}", key.trim_start_matches("refs/"));
        let uris = extra.iter().any(|e| e.starts_with("fetch.uriprotocols"));
        let trace = self.home.join("packet-trace");
        let mut last = String::new();
        for attempt in 0..2 {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(2));
            }
            let mut cmd = git(&self.home, repo);
            for e in extra {
                cmd.arg("-c").arg(e);
            }
            if uris {
                let _ = std::fs::remove_file(&trace);
                cmd.env("GIT_TRACE_PACKET", &trace);
            }
            cmd.args([
                "fetch",
                "--no-tags",
                "--quiet",
                url,
                &format!("+{key}:{local}"),
            ]);
            let done = run_for(&mut cmd, OP_TIMEOUT);
            if uris {
                tally.add(URI_FETCHES, 1);
                let sent = std::fs::read_to_string(&trace)
                    .map(|t| packfile_uris_in_trace(&t))
                    .unwrap_or(0);
                tally.add(URIS_USED, sent as u64);
            }
            match done {
                Ok(d) if d.success() => return Ok(()),
                Ok(d) => last.push_str(&format!("attempt {}: {}\n", attempt + 1, d.describe())),
                Err(e) => last.push_str(&format!("attempt {}: {e}\n", attempt + 1)),
            }
        }
        Err(last)
    }

    fn fsck(&self, repo: &Path, args: &[&str]) -> Option<Finished> {
        let mut cmd = git(&self.home, repo);
        cmd.arg("fsck").args(args);
        match run_for(&mut cmd, Duration::from_secs(600)) {
            Ok(d) if d.success() => None,
            Ok(d) => Some(d),
            Err(e) => Some(Finished {
                status: None,
                stdout: String::new(),
                stderr: e.to_string(),
            }),
        }
    }

    /// The fetchability checks for every ref at `tips`.
    pub fn fetchability(
        &self,
        target: &Target<'_>,
        tips: &BTreeMap<String, Option<Oid>>,
        epoch: usize,
        uris: bool,
        tally: &Tally,
    ) -> Vec<Finding> {
        let mut found = Vec::new();
        let keys: Vec<&String> = tips
            .iter()
            .filter(|(_, t)| t.is_some())
            .map(|(k, _)| k)
            .collect();
        if keys.is_empty() {
            return found;
        }
        let mut order = keys.clone();
        order.rotate_left(epoch % keys.len());
        let mut variants: Vec<(&str, Vec<&str>)> = vec![("fresh repository", vec![])];
        if uris {
            variants.push((
                "fresh repository with packfile URIs",
                vec!["fetch.uriprotocols=http,https"],
            ));
        }
        for (label, extra) in variants {
            let repo = self.scratch.join("fresh.git");
            if let Err(e) = fresh_repo(&self.home, &repo, true) {
                found.push(Finding::new(
                    Invariant::Unchecked,
                    epoch,
                    format!("{label}: could not create the scratch repository: {e}"),
                ));
                continue;
            }
            for key in &order {
                if let Err(e) = self.fetch(&repo, &target.url_for(key), key, &extra, tally) {
                    found.push(Finding::new(
                        Invariant::Unfetchable,
                        epoch,
                        format!(
                            "{label}: fetching {key} (tip {}) failed:\n{e}",
                            tip_of(tips, key)
                        ),
                    ));
                }
            }
            if let Some(d) = self.fsck(&repo, &["--strict", "--no-dangling"]) {
                found.push(Finding::new(
                    Invariant::Unfetchable,
                    epoch,
                    format!(
                        "{label}: `git fsck --strict` failed after fetching every ref:\n{}",
                        d.describe()
                    ),
                ));
            }
        }
        // Incremental: the observer keeps what earlier epochs fetched.
        for key in &keys {
            if let Err(e) = self.fetch(&self.observer, &target.url_for(key), key, &[], tally) {
                found.push(Finding::new(
                    Invariant::Unfetchable,
                    epoch,
                    format!(
                        "incremental fetch of {key} (tip {}) failed:\n{e}",
                        tip_of(tips, key)
                    ),
                ));
            }
        }
        if let Some(d) = self.fsck(&self.observer, &["--strict", "--no-dangling"]) {
            found.push(Finding::new(
                Invariant::Unfetchable,
                epoch,
                format!(
                    "observer repository fails `git fsck --strict` after incremental fetches:\n{}",
                    d.describe()
                ),
            ));
        }
        // Depth 1, the `actions/checkout` default (exact shallow packs, C8).
        let shallow = self.scratch.join("shallow");
        if let Err(e) = fresh_repo(&self.home, &shallow, false) {
            found.push(Finding::new(
                Invariant::Unchecked,
                epoch,
                format!("depth-1 check: could not create the scratch repository: {e}"),
            ));
        } else {
            let trunk = &target.keys.trunk;
            let done = run_for(
                git(&self.home, &shallow).args([
                    "fetch",
                    "--no-tags",
                    "--quiet",
                    "--depth=1",
                    &target.url_for(trunk),
                    trunk,
                ]),
                OP_TIMEOUT,
            );
            match done {
                Ok(d) if d.success() => {
                    if let Some(f) = self.fsck(&shallow, &["--strict", "--no-dangling"]) {
                        found.push(Finding::new(
                            Invariant::Unfetchable,
                            epoch,
                            format!(
                                "`git fsck --strict` failed after a depth-1 fetch of {trunk}:\n{}",
                                f.describe()
                            ),
                        ));
                    }
                }
                Ok(d) => found.push(Finding::new(
                    Invariant::Unfetchable,
                    epoch,
                    format!("depth-1 fetch of {trunk} failed:\n{}", d.describe()),
                )),
                Err(e) => found.push(Finding::new(Invariant::Unfetchable, epoch, e.to_string())),
            }
        }
        found
    }

    /// Checks that every acknowledged push in `acked` is in the history of
    /// its ref's tip: the exact commit the boundary read returned, walked
    /// in the observer repository. Needs [`Checker::fetchability`] to have
    /// run first this epoch (it fills the observer repository); if the tip
    /// is not there, the check cannot run and says so.
    pub fn durability(
        &self,
        tips: &BTreeMap<String, Option<Oid>>,
        acked: &[Op],
        epoch: usize,
    ) -> Vec<Finding> {
        let mut found = Vec::new();
        let mut by_key: BTreeMap<&str, Vec<&Op>> = BTreeMap::new();
        for op in acked {
            by_key.entry(&op.key).or_default().push(op);
        }
        for (key, ops) in by_key {
            let tip = tips.get(key).cloned().flatten();
            let history: HashSet<String> = match &tip {
                Some(tip) => match run_git(&self.home, &self.observer, &["rev-list", tip]) {
                    Ok(out) => out.lines().map(str::to_owned).collect(),
                    Err(e) => {
                        found.push(Finding::new(
                            Invariant::Unchecked,
                            epoch,
                            format!(
                                "durability of {} acknowledged push(es) to {key} could not be \
                                 checked: `git rev-list {}` failed in the observer repository \
                                 (did its fetch fail?): {e}",
                                ops.len(),
                                short(Some(tip))
                            ),
                        ));
                        continue;
                    }
                },
                None => HashSet::new(),
            };
            let lost: Vec<&&Op> = ops
                .iter()
                .filter(|op| match &op.call {
                    Call::Push { to, .. } => !history.contains(to),
                    Call::Read { .. } => false,
                })
                .collect();
            if !lost.is_empty() {
                let mut text = format!(
                    "{} acknowledged push(es) to {key} are not in the history of its tip {}:\n",
                    lost.len(),
                    short(tip.as_deref())
                );
                for op in lost.iter().take(20) {
                    text.push_str(&format!("    {op}\n"));
                }
                found.push(Finding::new(Invariant::AckedPushLost, epoch, text));
            }
        }
        found
    }

    /// The tip of `key` in the observer repository, if fetched.
    pub fn observed_tip(&self, key: &str) -> Option<Oid> {
        let local = format!("refs/check/{}", key.trim_start_matches("refs/"));
        rev_parse(&self.home, &self.observer, &local).ok()
    }
}

/// Deletes `dir` and creates an empty repository there.
fn fresh_repo(home: &Path, dir: &Path, bare: bool) -> io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    std::fs::create_dir_all(dir)?;
    let args: &[&str] = if bare {
        &["init", "-q", "--bare"]
    } else {
        &["init", "-q"]
    };
    run_git(home, dir, args).map(drop)
}

fn tip_of(tips: &BTreeMap<String, Option<Oid>>, key: &str) -> String {
    short(tips.get(key).cloned().flatten().as_deref())
}

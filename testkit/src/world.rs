//! One repository on one server, plus the client clones that act on it.
//!
//! The oracle builds two worlds per sequence, one per server, and applies the
//! same ops to both. Everything that ends up in an object (file names and
//! contents, messages, author and committer dates) is derived from the step
//! number, so identical behaviour on both servers yields identical object IDs
//! and the worlds can be compared byte for byte.
//!
//! Clones never talk to "the repository URL". Each clone has one remote per
//! branch, named `b-<branch>`, whose URL is [`GitServer::branch_url`] and
//! whose only refspec maps that branch to `refs/remotes/origin/<branch>`. So
//! a clone sees the same refs on a server that shows every branch on one URL
//! and on one that hides branches behind branch-scoped URLs (Aspen).
//!
//! The one exception is [`Op::PlainPush`] and [`Op::PlainRace`], which push
//! (never fetch) through the remote `plain`, the repository URL itself. On
//! Aspen that URL advertises only trunk, so those pushes reach Aspen's own
//! fast-forward check with a zero old value (issue #135).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;

use crate::git::GitClient;
use crate::ops::{Branch, Op};
use crate::server::{GitServer, RefMap};

/// 2023-11-14T22:13:20Z; commit dates count up from here.
const EPOCH: u64 = 1_700_000_000;

/// The remote, in every clone, whose URL is the repository URL. Only
/// [`Op::PlainPush`] and [`Op::PlainRace`] push through it; nothing fetches
/// through it. Its refspec maps every branch to the same remote-tracking ref
/// as that branch's own remote, so a push through either updates the clone's
/// view the same way.
const PLAIN_REMOTE: &str = "plain";

/// Which URL a push goes through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    /// The branch's own URL ([`GitServer::branch_url`]).
    Branch,
    /// The plain repository URL ([`PLAIN_REMOTE`]).
    Repository,
}

impl Route {
    /// The clone's remote for pushing `branch` this way.
    fn remote(self, branch: Branch) -> String {
        match self {
            Route::Branch => remote(branch),
            Route::Repository => PLAIN_REMOTE.to_owned(),
        }
    }
}

/// What the client saw when it ran one op. Two correct servers produce equal
/// outcomes. Push results record only accepted or rejected per ref, not the
/// summary text: servers may word a rejection differently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The op did not reach the server because the clone's own view ruled it
    /// out (for example, merging a branch the clone has never seen).
    Skipped(&'static str),
    /// A local step failed before the push (should not happen; reported so a
    /// harness bug cannot hide behind equal outcomes).
    LocalFailure(&'static str),
    /// `git push` ran.
    Push {
        /// Whether `git push` exited with status 0.
        ok: bool,
        /// `(destination ref, accepted)` per ref the client tried to update.
        refs: Vec<(String, bool)>,
    },
    /// The clone refreshed every branch through its branch URL.
    Fetch {
        /// Whether every per-branch fetch succeeded.
        ok: bool,
        /// `<oid> <ref>` for every remote-tracking ref afterwards.
        remote_refs: Vec<String>,
    },
    /// One outcome per push of a [`Op::ConcurrentPushes`], in op order.
    Concurrent(Vec<Outcome>),
    /// A [`Op::Race`] ran. Which racer won may differ between correct
    /// servers, so only what must agree is recorded; the oracle checks the
    /// race's invariants separately.
    Race {
        /// The tip every racer started from (`None` if the branch did not
        /// exist), or `Err` with the error if a racer could not refresh.
        base: Result<Option<String>, String>,
        /// How many clones raced.
        racers: usize,
    },
}

/// How one racer of a [`Op::Race`] fared.
#[derive(Debug, Clone)]
pub(crate) struct Racer {
    /// The clone that pushed.
    pub(crate) clone: usize,
    /// The commit it tried to make the branch tip.
    pub(crate) commit: String,
    /// Whether the server accepted the update.
    pub(crate) accepted: bool,
    /// The `git push --porcelain` summary for the ref, such as
    /// `[remote rejected] (failed to update ref)`, or the local failure.
    pub(crate) summary: String,
}

/// Everything the oracle needs to check a race: the comparable outcome, and
/// each racer's fate.
#[derive(Debug, Clone)]
pub(crate) struct RaceRun {
    pub(crate) branch: Branch,
    pub(crate) racers: Vec<Racer>,
}

/// What applying one op produced ([`World::apply`]).
#[derive(Debug)]
pub(crate) struct Applied {
    /// What the client saw; compared between servers.
    pub(crate) outcome: Outcome,
    /// For a [`Op::Race`], each racer's fate, for the oracle's invariant
    /// checks.
    pub(crate) race: Option<RaceRun>,
    /// For an op that is a single `git push` of one ref spec ([`Op::Delete`],
    /// [`Op::CreateBranch`]), the porcelain summary of each ref, in the order
    /// of [`Outcome::Push`]'s `refs`, such as `[remote rejected] (no such
    /// ref)`; empty for every other op. Not compared: servers may word a
    /// rejection differently. Only a [`crate::KnownDivergence`] reads it.
    pub(crate) summaries: Vec<String>,
}

impl RaceRun {
    /// The racer whose push the server accepted, if exactly one was.
    pub(crate) fn winner(&self) -> Option<&Racer> {
        let mut accepted = self.racers.iter().filter(|r| r.accepted);
        match (accepted.next(), accepted.next()) {
            (Some(w), None) => Some(w),
            _ => None,
        }
    }
}

/// The state of a repository: every ref as the server lists it, and a fresh
/// copy built from per-branch fetches. Each field is `Err(message)` if the
/// check itself failed.
#[derive(Debug, Clone)]
pub(crate) struct Observation {
    /// [`GitServer::list_refs`].
    pub(crate) refs: Result<RefMap, String>,
    /// Every object reachable from any ref, in a fresh repository that
    /// fetched each listed ref through its branch URL.
    pub(crate) objects: Result<BTreeSet<String>, String>,
    /// `git fsck --strict` on that repository.
    pub(crate) fsck: Result<(), String>,
}

#[derive(Debug)]
pub(crate) struct World<'s> {
    server: &'s dyn GitServer,
    url: String,
    branches: Vec<Branch>,
    /// [`GitServer::branch_url`] of each branch, by index.
    branch_urls: Vec<String>,
    dir: PathBuf,
    client: GitClient,
    clones: Vec<PathBuf>,
}

impl<'s> World<'s> {
    /// Creates repository `repo` on `server`, makes `clones` clones under
    /// `dir` that know `branches` branches, and pushes an initial commit to
    /// `main` from clone 0.
    pub(crate) fn setup(
        server: &'s dyn GitServer,
        repo: &str,
        dir: &Path,
        clones: usize,
        branches: usize,
    ) -> io::Result<Self> {
        let url = server.create_repo(repo)?;
        let home = dir.join("home");
        fs::create_dir_all(&home)?;
        let client = GitClient::new(&home)
            .with_config("protocol.version", "2")
            .with_config("init.defaultBranch", "main")
            .with_config("advice.detachedHead", "false");
        let branches: Vec<Branch> = (0..branches).map(|b| Branch(b as u8)).collect();
        let branch_urls: Vec<String> = branches
            .iter()
            .map(|b| server.branch_url(&url, &format!("refs/heads/{b}")))
            .collect();
        let mut remotes = String::new();
        for (&branch, branch_url) in branches.iter().zip(&branch_urls) {
            write!(
                remotes,
                "[remote \"{remote}\"]\n\turl = {branch_url}\n\tfetch = +refs/heads/{branch}:refs/remotes/origin/{branch}\n",
                remote = remote(branch),
            )
            .expect("writing to a String cannot fail");
        }
        write!(
            remotes,
            "[remote \"{PLAIN_REMOTE}\"]\n\turl = {url}\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
        )
        .expect("writing to a String cannot fail");
        let mut paths = Vec::with_capacity(clones);
        for i in 0..clones {
            let path = dir.join(format!("clone-{i}"));
            crate::git::run(client.command(dir).args(["init", "--quiet"]).arg(&path))?;
            let config = path.join(".git").join("config");
            let mut text = fs::read_to_string(&config)?;
            text.push_str(&remotes);
            fs::write(&config, text)?;
            paths.push(path);
        }
        let world = Self {
            server,
            url,
            branches,
            branch_urls,
            dir: dir.to_path_buf(),
            client,
            clones: paths,
        };
        let first = &world.clones[0];
        fs::write(
            first.join("README"),
            "Seeded by the Aspen differential oracle.\n",
        )?;
        world.git(first, None, ["add", "README"])?;
        world.git(
            first,
            Some(EPOCH),
            ["commit", "--quiet", "-m", "initial commit"],
        )?;
        world.git(
            first,
            None,
            [
                "push",
                "--quiet",
                &remote(Branch::TRUNK),
                "main:refs/heads/main",
            ],
        )?;
        for clone in 0..clones {
            if let Err(e) = world.refresh_all(clone)? {
                return Err(io::Error::other(format!("setup fetch failed: {e}")));
            }
        }
        Ok(world)
    }

    /// Applies `op` as step `step` of the sequence.
    pub(crate) fn apply(&self, step: usize, op: &Op) -> io::Result<Applied> {
        let mut summaries = Vec::new();
        let outcome = match op {
            Op::Push {
                clone,
                branch,
                commits,
            } => self.push(*clone, *branch, step, 0, *commits, false, Route::Branch)?,
            Op::ForcePush { clone, branch } => {
                self.push(*clone, *branch, step, 0, 1, true, Route::Branch)?
            }
            Op::PlainPush {
                clone,
                branch,
                commits,
                force,
            } => self.push(
                *clone,
                *branch,
                step,
                0,
                *commits,
                *force,
                Route::Repository,
            )?,
            Op::Delete { clone, branch } => {
                let spec = format!(":refs/heads/{branch}");
                let (outcome, pushed) =
                    self.push_spec(&self.clones[*clone], &remote(*branch), &spec, false)?;
                summaries = pushed;
                outcome
            }
            Op::CreateBranch { clone, new, from } => {
                let dir = &self.clones[*clone];
                if !self.has_remote_branch(dir, *from)? {
                    return Ok(Applied {
                        outcome: Outcome::Skipped("source branch not in the clone's view"),
                        race: None,
                        summaries,
                    });
                }
                let spec = format!("refs/remotes/origin/{from}:refs/heads/{new}");
                let (outcome, pushed) = self.push_spec(dir, &remote(*new), &spec, false)?;
                summaries = pushed;
                outcome
            }
            Op::Merge { clone, into, from } => self.merge(*clone, *into, *from, step)?,
            Op::Fetch { clone } => self.fetch(*clone)?,
            Op::ConcurrentPushes { pushes } => {
                let outcomes = thread::scope(|scope| {
                    let handles: Vec<_> = pushes
                        .iter()
                        .enumerate()
                        .map(|(sub, &(clone, branch))| {
                            scope.spawn(move || {
                                self.push(clone, branch, step, sub, 1, false, Route::Branch)
                            })
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|h| h.join().expect("push thread panicked"))
                        .collect::<io::Result<Vec<_>>>()
                })?;
                Outcome::Concurrent(outcomes)
            }
            Op::Race { clones, branch } => {
                let (outcome, race) = self.race(clones, *branch, step, Route::Branch)?;
                return Ok(Applied {
                    outcome,
                    race,
                    summaries,
                });
            }
            Op::PlainRace { clones, branch } => {
                let (outcome, race) = self.race(clones, *branch, step, Route::Repository)?;
                return Ok(Applied {
                    outcome,
                    race,
                    summaries,
                });
            }
        };
        Ok(Applied {
            outcome,
            race: None,
            summaries,
        })
    }

    /// Looks at the repository: lists its refs, then fetches each one
    /// through its branch URL into a fresh repository and checks that. `tag`
    /// names the scratch repository, which is removed afterwards.
    pub(crate) fn observe(&self, tag: &str) -> io::Result<Observation> {
        let refs = match self.server.list_refs(&self.url) {
            Ok(refs) => refs,
            Err(e) => {
                return Ok(Observation {
                    refs: Err(e.to_string()),
                    objects: Err("no ref list to fetch".to_owned()),
                    fsck: Err("nothing fetched to check".to_owned()),
                });
            }
        };
        let home = self.client.home();
        let check = self.dir.join(format!("check-{tag}"));
        let check_str = check.to_string_lossy().into_owned();
        self.git(home, None, ["init", "--quiet", "--bare", &check_str])?;
        // One remote per distinct branch URL, fetching every ref served
        // there: one fetch for a server that shows everything on one URL,
        // one per branch for a branch-scoped server.
        let mut by_url: BTreeMap<String, Vec<&str>> = BTreeMap::new();
        for refname in refs.keys() {
            let url = self.server.branch_url(&self.url, refname);
            by_url.entry(url).or_default().push(refname);
        }
        let mut remotes = Vec::with_capacity(by_url.len());
        let mut config = fs::read_to_string(check.join("config"))?;
        for (i, (url, refnames)) in by_url.iter().enumerate() {
            let name = format!("r{i}");
            write!(config, "[remote \"{name}\"]\n\turl = {url}\n")
                .expect("writing to a String cannot fail");
            for refname in refnames {
                writeln!(config, "\tfetch = +{refname}:{refname}")
                    .expect("writing to a String cannot fail");
            }
            remotes.push(name);
        }
        fs::write(check.join("config"), config)?;
        let fetched = if remotes.is_empty() {
            Ok(String::new())
        } else {
            let mut args = vec!["fetch", "--quiet", "--no-tags", "--multiple"];
            args.extend(remotes.iter().map(String::as_str));
            self.try_git(&check, None, args)?
        };
        let (objects, fsck) = match fetched {
            Err(e) => (
                Err(format!("per-branch fetch failed: {e}")),
                Err("nothing fetched to check".to_owned()),
            ),
            Ok(_) => {
                let objects = self
                    .try_git(&check, None, ["rev-list", "--objects", "--all"])?
                    .map(|out| {
                        out.lines()
                            .filter_map(|l| l.split_whitespace().next())
                            .map(str::to_owned)
                            .collect()
                    });
                let fsck = self
                    .try_git(
                        &check,
                        None,
                        ["fsck", "--strict", "--no-progress", "--no-dangling"],
                    )?
                    .map(|_| ());
                (objects, fsck)
            }
        };
        let _ = fs::remove_dir_all(&check);
        Ok(Observation {
            refs: Ok(refs),
            objects,
            fsck,
        })
    }

    /// Checks a race's invariants against the server's committed state:
    /// exactly one racer was accepted, the branch tip is its commit, and
    /// every other racer was rejected as non-fast-forward or for a lost
    /// compare-and-swap. The same holds for a race through the plain
    /// repository URL ([`Op::PlainRace`]), where Aspen refuses each loser
    /// itself once the winner has moved the branch (`non-fast-forward`, or
    /// a retryable lost compare-and-swap). Returns one line per broken
    /// invariant.
    pub(crate) fn race_problems(&self, run: &RaceRun) -> io::Result<Vec<String>> {
        let mut problems = Vec::new();
        let accepted: Vec<_> = run.racers.iter().filter(|r| r.accepted).collect();
        if accepted.len() != 1 {
            let who: Vec<String> = accepted
                .iter()
                .map(|r| format!("clone {} ({})", r.clone, short(&r.commit)))
                .collect();
            problems.push(format!(
                "{} of {} racing pushes to {} were accepted, expected exactly 1{}",
                accepted.len(),
                run.racers.len(),
                run.branch,
                if who.is_empty() {
                    String::new()
                } else {
                    format!(": {}", who.join(", "))
                }
            ));
        }
        let refname = format!("refs/heads/{}", run.branch);
        let tip = match self.server.list_refs(&self.url) {
            Ok(refs) => refs.get(&refname).cloned(),
            Err(e) => {
                problems.push(format!("listing refs after the race failed: {e}"));
                return Ok(problems);
            }
        };
        if let Some(winner) = run.winner()
            && tip.as_deref() != Some(winner.commit.as_str())
        {
            problems.push(format!(
                "{} is at {} but the accepted push (clone {}) was {}",
                run.branch,
                tip.as_deref().map_or("nothing", short),
                winner.clone,
                short(&winner.commit)
            ));
        }
        if accepted.len() > 1 {
            let kept = run
                .racers
                .iter()
                .find(|r| Some(r.commit.as_str()) == tip.as_deref());
            if let Some(kept) = kept {
                problems.push(format!(
                    "{} kept clone {}'s commit {}; the other acknowledged pushes were lost",
                    run.branch,
                    kept.clone,
                    short(&kept.commit)
                ));
            }
        }
        for loser in run.racers.iter().filter(|r| !r.accepted) {
            if !is_race_loss(&loser.summary) {
                problems.push(format!(
                    "clone {} lost the race with an unexpected result: {}",
                    loser.clone, loser.summary
                ));
            }
        }
        Ok(problems)
    }

    /// Makes `branch` point at `commit`, which clone `clone` holds, with a
    /// forced push through the branch URL. The oracle calls it only on the
    /// reference world, to adopt the candidate's winner after a race the two
    /// servers resolved differently. Returns git's error if the push failed.
    pub(crate) fn resync(
        &self,
        clone: usize,
        branch: Branch,
        commit: &str,
    ) -> io::Result<Result<(), String>> {
        let spec = format!("{commit}:refs/heads/{branch}");
        let dir = &self.clones[clone];
        let (outcome, _) = self.push_spec(dir, &remote(branch), &spec, true)?;
        Ok(match outcome {
            Outcome::Push { ok: true, .. } => Ok(()),
            other => Err(format!(
                "forced push of {} failed: {other:?}",
                short(commit)
            )),
        })
    }

    /// Refreshes each racer's view of the race's branch, so that clones
    /// agree across worlds whichever racer won.
    pub(crate) fn settle_race(&self, run: &RaceRun) -> io::Result<Result<(), String>> {
        for racer in &run.racers {
            if let Err(e) = self.refresh(racer.clone, &[run.branch])? {
                return Ok(Err(e));
            }
        }
        Ok(Ok(()))
    }

    /// Every racer refreshes `branch` through its branch URL, then all push
    /// at once through `route`.
    fn race(
        &self,
        clones: &[usize],
        branch: Branch,
        step: usize,
        route: Route,
    ) -> io::Result<(Outcome, Option<RaceRun>)> {
        let mut base = Ok(None);
        for (i, &clone) in clones.iter().enumerate() {
            if let Err(e) = self.refresh(clone, &[branch])? {
                base = Err(e);
                break;
            }
            let tip = self.remote_tip(&self.clones[clone], branch)?;
            if i == 0 {
                base = Ok(tip);
            } else if base.as_ref().ok() != Some(&tip) {
                base = Err(format!("racers saw different tips of {branch}"));
                break;
            }
        }
        let outcome = Outcome::Race {
            base: base.clone(),
            racers: clones.len(),
        };
        if base.is_err() {
            return Ok((outcome, None));
        }
        let racers = thread::scope(|scope| {
            let handles: Vec<_> = clones
                .iter()
                .enumerate()
                .map(|(sub, &clone)| {
                    scope.spawn(move || self.race_push(clone, branch, step, sub, route))
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("race thread panicked"))
                .collect::<io::Result<Vec<_>>>()
        })?;
        Ok((outcome, Some(RaceRun { branch, racers })))
    }

    /// One racer: commit on top of the refreshed view of `branch`, push
    /// through `route`.
    fn race_push(
        &self,
        clone: usize,
        branch: Branch,
        step: usize,
        sub: usize,
        route: Route,
    ) -> io::Result<Racer> {
        let dir = &self.clones[clone];
        let failed = |what: &str| Racer {
            clone,
            commit: String::new(),
            accepted: false,
            summary: format!("local failure: {what}"),
        };
        if !self.checkout_base(dir, branch)? {
            return Ok(failed("checkout"));
        }
        let label = format!("racer {sub}");
        if !self.commit(dir, clone, branch, step, sub, 0, &label)? {
            return Ok(failed("commit"));
        }
        let commit = self
            .git(dir, None, ["rev-parse", "HEAD"])?
            .trim()
            .to_owned();
        let spec = format!("refs/heads/{branch}:refs/heads/{branch}");
        let (outcome, summaries) = self.push_spec(dir, &route.remote(branch), &spec, false)?;
        let accepted =
            matches!(&outcome, Outcome::Push { refs, .. } if refs.iter().any(|(_, a)| *a));
        let summary = summaries
            .into_iter()
            .next()
            .unwrap_or_else(|| format!("no ref status from git push: {outcome:?}"));
        Ok(Racer {
            clone,
            commit,
            accepted,
            summary,
        })
    }

    /// Adds `commits` commits to the clone's view of `branch` and pushes
    /// through `route`. With `force`, the new commits replace the old tip
    /// instead of extending it, and the push uses `--force`.
    #[allow(clippy::too_many_arguments)]
    fn push(
        &self,
        clone: usize,
        branch: Branch,
        step: usize,
        sub: usize,
        commits: u8,
        force: bool,
        route: Route,
    ) -> io::Result<Outcome> {
        let dir = &self.clones[clone];
        if !self.checkout_base(dir, branch)? {
            return Ok(Outcome::LocalFailure("checkout"));
        }
        if force
            && self
                .try_git(dir, None, ["rev-parse", "--verify", "--quiet", "HEAD"])?
                .is_ok()
        {
            let has_parent = self
                .try_git(dir, None, ["rev-parse", "--verify", "--quiet", "HEAD~1"])?
                .is_ok();
            let reset = if has_parent {
                self.try_git(dir, None, ["reset", "--quiet", "--hard", "HEAD~1"])?
                    .is_ok()
            } else {
                self.start_empty(dir, branch)?
            };
            if !reset {
                return Ok(Outcome::LocalFailure("reset"));
            }
        }
        for k in 0..usize::from(commits) {
            let label = format!("commit {k}");
            if !self.commit(dir, clone, branch, step, sub, k, &label)? {
                return Ok(Outcome::LocalFailure("commit"));
            }
        }
        let spec = format!("refs/heads/{branch}:refs/heads/{branch}");
        Ok(self.push_spec(dir, &route.remote(branch), &spec, force)?.0)
    }

    fn merge(&self, clone: usize, into: Branch, from: Branch, step: usize) -> io::Result<Outcome> {
        let dir = &self.clones[clone];
        if !self.has_remote_branch(dir, from)? {
            return Ok(Outcome::Skipped("merge source not in the clone's view"));
        }
        if !self.checkout_base(dir, into)? {
            return Ok(Outcome::LocalFailure("checkout"));
        }
        let source = format!("refs/remotes/origin/{from}");
        let merged = self.try_git(
            dir,
            Some(date(step, 0, 0)),
            [
                "merge",
                "--quiet",
                "--no-edit",
                "--no-ff",
                "--allow-unrelated-histories",
                &source,
            ],
        )?;
        if merged.is_err() {
            let _ = self.try_git(dir, None, ["merge", "--abort"])?;
            return Ok(Outcome::LocalFailure("merge"));
        }
        let spec = format!("refs/heads/{into}:refs/heads/{into}");
        Ok(self.push_spec(dir, &remote(into), &spec, false)?.0)
    }

    fn fetch(&self, clone: usize) -> io::Result<Outcome> {
        let dir = &self.clones[clone];
        let ok = self.refresh_all(clone)?.is_ok();
        let remote_refs = self
            .git(
                dir,
                None,
                [
                    "for-each-ref",
                    "--format=%(objectname) %(refname)",
                    "refs/remotes/origin/",
                ],
            )?
            .lines()
            .filter(|l| !l.ends_with("/HEAD"))
            .map(str::to_owned)
            .collect();
        Ok(Outcome::Fetch { ok, remote_refs })
    }

    /// Refreshes the clone's view of every branch.
    fn refresh_all(&self, clone: usize) -> io::Result<Result<(), String>> {
        self.refresh(clone, &self.branches)
    }

    /// Brings `refs/remotes/origin/<branch>` of each of `branches` up to date
    /// through its branch URL: fetches the branches that moved and deletes
    /// the remote-tracking refs of branches that are gone, like
    /// `git fetch --prune`. (`git fetch <remote>` alone cannot do this: it
    /// fails, without pruning, when an exact refspec names a missing branch.)
    /// Branches that share a URL are listed and fetched together.
    ///
    /// Every listing and fetch names a branch URL explicitly, never a remote
    /// name, `--all` or `--prune`, and nothing else in this file fetches
    /// through [`PLAIN_REMOTE`]: on Aspen the plain URL advertises only
    /// trunk, so a fetch or prune through it would drop every topic
    /// branch's remote-tracking ref.
    fn refresh(&self, clone: usize, branches: &[Branch]) -> io::Result<Result<(), String>> {
        let dir = &self.clones[clone];
        let mut by_url: BTreeMap<&str, Vec<Branch>> = BTreeMap::new();
        for &branch in branches {
            by_url
                .entry(&self.branch_urls[usize::from(branch.0)])
                .or_default()
                .push(branch);
        }
        for (url, branches) in by_url {
            let refnames: Vec<String> =
                branches.iter().map(|b| format!("refs/heads/{b}")).collect();
            let mut args = vec!["ls-remote", url];
            args.extend(refnames.iter().map(String::as_str));
            let listed = match self.try_git(dir, None, args)? {
                Ok(out) => crate::server::parse_ref_lines(&out)?,
                Err(e) => return Ok(Err(e)),
            };
            let mut specs = Vec::new();
            for (branch, refname) in branches.iter().zip(&refnames) {
                let tracking = format!("refs/remotes/origin/{branch}");
                let local = self.remote_tip(dir, *branch)?;
                match listed.get(refname) {
                    Some(tip) if local.as_ref() == Some(tip) => {}
                    Some(_) => specs.push(format!("+{refname}:{tracking}")),
                    None if local.is_some() => {
                        self.git(dir, None, ["update-ref", "-d", &tracking])?;
                    }
                    None => {}
                }
            }
            if !specs.is_empty() {
                let mut args = vec!["fetch", "--quiet", "--no-tags", url];
                args.extend(specs.iter().map(String::as_str));
                if let Err(e) = self.try_git(dir, None, args)? {
                    return Ok(Err(e));
                }
            }
        }
        Ok(Ok(()))
    }

    /// The clone's view of `branch`, if it has one.
    fn remote_tip(&self, dir: &Path, branch: Branch) -> io::Result<Option<String>> {
        let name = format!("refs/remotes/origin/{branch}");
        Ok(self
            .try_git(dir, None, ["rev-parse", "--verify", "--quiet", &name])?
            .ok()
            .map(|out| out.trim().to_owned()))
    }

    /// Checks out a local `branch` starting from the clone's view of it, or
    /// of `main`, or from an empty tree. Returns false if git refused.
    fn checkout_base(&self, dir: &Path, branch: Branch) -> io::Result<bool> {
        let start = if self.has_remote_branch(dir, branch)? {
            Some(format!("refs/remotes/origin/{branch}"))
        } else if self.has_remote_branch(dir, Branch::TRUNK)? {
            Some("refs/remotes/origin/main".to_owned())
        } else {
            None
        };
        match start {
            Some(start) => Ok(self
                .try_git(
                    dir,
                    None,
                    // `--no-track`: the remote-tracking ref is mapped by
                    // both the branch's remote and `plain`, so git could not
                    // pick an upstream; pushes name their remote anyway.
                    [
                        "checkout",
                        "--quiet",
                        "--force",
                        "--no-track",
                        "-B",
                        &branch.name(),
                        &start,
                    ],
                )?
                .is_ok()),
            None => self.start_empty(dir, branch),
        }
    }

    /// Makes `branch` an unborn branch with an empty index and work tree.
    fn start_empty(&self, dir: &Path, branch: Branch) -> io::Result<bool> {
        let head = format!("refs/heads/{branch}");
        let steps: [&[&str]; 4] = [
            &["symbolic-ref", "HEAD", &head],
            &["update-ref", "-d", &head],
            &["read-tree", "--empty"],
            &["clean", "-fdxq"],
        ];
        for args in steps {
            if self.try_git(dir, None, args.iter().copied())?.is_err() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Adds one file and commits it. Names, contents and dates depend only on
    /// the arguments, so both worlds create the same commit. The subject is
    /// `step <step>: <label> on <branch>`.
    #[allow(clippy::too_many_arguments)]
    fn commit(
        &self,
        dir: &Path,
        clone: usize,
        branch: Branch,
        step: usize,
        sub: usize,
        k: usize,
        label: &str,
    ) -> io::Result<bool> {
        let name = format!("files/step-{step}-{sub}-{k}.txt");
        fs::create_dir_all(dir.join("files"))?;
        fs::write(
            dir.join(&name),
            format!("step {step}, push {sub}, commit {k}, by clone {clone} on {branch}\n"),
        )?;
        if self.try_git(dir, None, ["add", &name])?.is_err() {
            return Ok(false);
        }
        let message = format!("step {step}: {label} on {branch}");
        Ok(self
            .try_git(
                dir,
                Some(date(step, sub, k)),
                ["commit", "--quiet", "-m", &message],
            )?
            .is_ok())
    }

    /// Pushes `spec` to the clone's remote `remote` (a branch's remote, or
    /// [`PLAIN_REMOTE`]), and returns the outcome plus the porcelain summary
    /// of each ref. With `force`, the push uses `--force` and the push option
    /// `force` (Aspen's force intent).
    fn push_spec(
        &self,
        dir: &Path,
        remote: &str,
        spec: &str,
        force: bool,
    ) -> io::Result<(Outcome, Vec<String>)> {
        let mut args = vec!["push", "--porcelain"];
        if force {
            args.extend(["--force", "--push-option=force"]);
        }
        args.extend([remote, spec]);
        let out = self.client.output(dir, &args)?;
        let mut statuses: Vec<RefStatus> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(parse_porcelain)
            .collect();
        statuses.sort_by(|a, b| a.dst.cmp(&b.dst));
        let refs = statuses
            .iter()
            .map(|s| (s.dst.clone(), s.accepted))
            .collect();
        let summaries = statuses.into_iter().map(|s| s.summary).collect();
        Ok((
            Outcome::Push {
                ok: out.status.success(),
                refs,
            },
            summaries,
        ))
    }

    /// Where clone `clone` last saw `branch` (its remote-tracking ref), if
    /// anywhere.
    pub(crate) fn tracking(&self, clone: usize, branch: Branch) -> io::Result<Option<String>> {
        let name = format!("refs/remotes/origin/{branch}");
        Ok(self
            .try_git(
                &self.clones[clone],
                None,
                ["rev-parse", "--verify", "--quiet", &name],
            )?
            .ok()
            .map(|oid| oid.trim().to_owned()))
    }

    /// Sets clone `clone`'s remote-tracking ref of `branch` to `oid`, or
    /// deletes it. Used after stepping over a known divergence, so the
    /// candidate's clone sees what the reference's clone sees.
    pub(crate) fn set_tracking(
        &self,
        clone: usize,
        branch: Branch,
        oid: Option<&str>,
    ) -> io::Result<()> {
        let name = format!("refs/remotes/origin/{branch}");
        let dir = &self.clones[clone];
        match oid {
            Some(oid) => self.git(dir, None, ["update-ref", &name, oid])?,
            None => self.git(dir, None, ["update-ref", "-d", &name])?,
        };
        Ok(())
    }

    fn has_remote_branch(&self, dir: &Path, branch: Branch) -> io::Result<bool> {
        let name = format!("refs/remotes/origin/{branch}");
        Ok(self
            .try_git(dir, None, ["rev-parse", "--verify", "--quiet", &name])?
            .is_ok())
    }

    /// Runs git; a non-zero exit is an `io::Error`.
    fn git<const N: usize>(
        &self,
        dir: &Path,
        date: Option<u64>,
        args: [&str; N],
    ) -> io::Result<String> {
        self.try_git(dir, date, args)?.map_err(io::Error::other)
    }

    /// Runs git. The outer error means git could not be started; the inner
    /// one carries stderr of a non-zero exit.
    fn try_git<'a>(
        &self,
        dir: &Path,
        date: Option<u64>,
        args: impl IntoIterator<Item = &'a str>,
    ) -> io::Result<Result<String, String>> {
        let mut cmd = self.client.command(dir);
        cmd.args(args);
        if let Some(secs) = date {
            let date = format!("{secs} +0000");
            cmd.env("GIT_AUTHOR_DATE", &date)
                .env("GIT_COMMITTER_DATE", &date);
        }
        let out = cmd.output()?;
        Ok(if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(crate::git::describe_failure(&cmd, &out))
        })
    }
}

/// The commit date for commit `k` of push `sub` in step `step`.
fn date(step: usize, sub: usize, k: usize) -> u64 {
    EPOCH + 1000 * step as u64 + 10 * sub as u64 + k as u64 + 1
}

/// The name of the remote, in every clone, for `branch`'s URL.
fn remote(branch: Branch) -> String {
    format!("b-{branch}")
}

/// An object ID shortened for messages.
fn short(oid: &str) -> &str {
    oid.get(..12).unwrap_or(oid)
}

/// Phrases that mark a server-side rejection (`ng <ref> <reason>`, shown by
/// the client as `[remote rejected] (<reason>)`) as a legitimate loss of a
/// compare-and-swap race. The differential oracle accepts a racer's server
/// rejection only if its reason contains one of these.
///
/// Stock Git says "failed to update ref" or "cannot lock ref". Aspen (node B1,
/// see `docs/PLAN.md` section 8) must word the `ng` reason of a lost
/// compare-and-swap or a non-fast-forward update so that it contains one of
/// these phrases; spike S2 suggests "retry" for the retryable case.
pub const RACE_LOSS_REASONS: [&str; 8] = [
    "non-fast-forward",
    "fetch first",
    "failed to update ref",
    "cannot lock ref",
    "failed to lock",
    "incorrect old value",
    "reference already exists",
    "retry",
];

/// Whether a rejected racer's porcelain summary is a legitimate way to lose
/// a compare-and-swap race: the client saw the winner's tip and refused
/// (`[rejected] (fetch first)`, `(non-fast-forward)`), or the server refused
/// because the ref moved under it, with a reason in [`RACE_LOSS_REASONS`].
fn is_race_loss(summary: &str) -> bool {
    if summary.starts_with("[rejected]") {
        return summary.contains("non-fast-forward") || summary.contains("fetch first");
    }
    summary.starts_with("[remote rejected]")
        && RACE_LOSS_REASONS.iter().any(|s| summary.contains(s))
}

/// One ref line of `git push --porcelain`.
#[derive(Debug, PartialEq, Eq)]
struct RefStatus {
    dst: String,
    accepted: bool,
    summary: String,
}

/// Parses one `git push --porcelain` ref line, `<flag>\t<src>:<dst>\t<summary>`.
/// Other lines (`To <url>`, `Done`) give `None`.
fn parse_porcelain(line: &str) -> Option<RefStatus> {
    let mut fields = line.split('\t');
    let flag = fields.next()?;
    let spec = fields.next()?;
    let accepted = match flag {
        " " | "+" | "-" | "*" | "=" => true,
        "!" => false,
        _ => return None,
    };
    let dst = spec.rsplit_once(':').map_or(spec, |(_, dst)| dst);
    Some(RefStatus {
        dst: dst.to_owned(),
        accepted,
        summary: fields.next().unwrap_or("").to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(line: &str) -> Option<(String, bool)> {
        parse_porcelain(line).map(|s| (s.dst, s.accepted))
    }

    #[test]
    fn porcelain_lines_parse() {
        assert_eq!(
            status("*\trefs/heads/a:refs/heads/a\t[new branch]"),
            Some(("refs/heads/a".to_owned(), true))
        );
        assert_eq!(
            status("!\trefs/heads/a:refs/heads/a\t[rejected] (fetch first)"),
            Some(("refs/heads/a".to_owned(), false))
        );
        assert_eq!(
            status("-\t:refs/heads/b\t[deleted]"),
            Some(("refs/heads/b".to_owned(), true))
        );
        assert_eq!(
            status("=\trefs/heads/a:refs/heads/a\t[up to date]"),
            Some(("refs/heads/a".to_owned(), true))
        );
        assert_eq!(
            parse_porcelain(
                "!\trefs/heads/a:refs/heads/a\t[remote rejected] (failed to update ref)"
            )
            .unwrap()
            .summary,
            "[remote rejected] (failed to update ref)"
        );
        assert_eq!(parse_porcelain("To http://127.0.0.1:1/r.git"), None);
        assert_eq!(parse_porcelain("Done"), None);
    }

    #[test]
    fn race_losses_are_classified() {
        assert!(is_race_loss("[rejected] (fetch first)"));
        assert!(is_race_loss("[rejected] (non-fast-forward)"));
        assert!(is_race_loss("[remote rejected] (failed to update ref)"));
        assert!(is_race_loss(
            "[remote rejected] (cannot lock ref 'refs/heads/a': is at 1 but expected 2)"
        ));
        assert!(is_race_loss("[remote rejected] (ref moved, retry)"));
        // Aspen's refusal of a plain-URL racer that lost (zero old value):
        // `RejectReason::NonFastForward`'s text.
        assert!(is_race_loss(
            "[remote rejected] (non-fast-forward (fetch and rebase, or push with -o force))"
        ));
        assert!(!is_race_loss("[rejected] (stale info)"));
        assert!(!is_race_loss(
            "[remote rejected] (pre-receive hook declined)"
        ));
        assert!(!is_race_loss("local failure: commit"));
        assert!(!is_race_loss(""));
    }

    #[test]
    fn dates_are_unique_per_commit() {
        let mut seen = BTreeSet::new();
        for step in 0..50 {
            for sub in 0..5 {
                for k in 0..3 {
                    assert!(seen.insert(date(step, sub, k)));
                }
            }
        }
        assert!(date(0, 0, 0) > EPOCH, "seed commit keeps EPOCH to itself");
    }
}

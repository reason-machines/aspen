//! The differential oracle (section 4.1 of `docs/PLAN.md`).
//!
//! For each seed, the oracle generates a sequence of [`Op`]s and applies it,
//! step by step, to a fresh repository on two servers: a *reference* (stock
//! `git http-backend`) and a *candidate* (eventually Aspen). Clients push and
//! fetch each branch through [`GitServer::branch_url`], so servers that hide
//! branches (Aspen) and servers that do not are driven identically. After
//! every step it checks that
//!
//! 1. the clients saw the same [`Outcome`] (same refs accepted or rejected),
//! 2. [`GitServer::list_refs`] lists the same refs at the same object IDs,
//! 3. a fresh repository that fetched every listed ref through its branch URL
//!    holds the same set of reachable objects on both, and
//! 4. `git fsck --strict` passes on both of those repositories.
//!
//! With [`Config::plain_url_pushes`], [`Op::PlainPush`] and [`Op::PlainRace`]
//! push through the plain repository URL instead (never fetch): on Aspen it
//! advertises only trunk, so those pushes reach Aspen's own fast-forward
//! check with a zero old value, and must still agree with stock, where the
//! client sees the ref and refuses a stale push itself (issue #135).
//!
//! A [`Op::Race`] (several clones pushing the same branch at once) may be won
//! by a different clone on each server, so its outcome is checked against
//! invariants instead: on each server exactly one push is accepted, the
//! branch tip is that push's commit, and every other push is rejected as
//! non-fast-forward or for a lost compare-and-swap. If the winners differ,
//! the oracle force-pushes the candidate's winner to the reference (a
//! *resync*, noted in the step log) before checks 2 to 4. It never asks the
//! candidate to rewrite a branch: Aspen refuses forced trunk updates by
//! default, while the stock reference accepts a forced update anywhere.
//!
//! The first step that breaks any of these is a [`Divergence`]. Its report
//! carries the seed and the step, so the failure replays exactly, and, when
//! shrinking is on, the smallest sub-sequence found that still diverges.

use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use crate::ops::{self, Branch, Config, Op};
use crate::server::{GitServer, RefMap};
use crate::world::{Observation, Outcome, RaceRun, World};

/// Compares a candidate server against a reference server.
#[derive(Debug)]
pub struct Oracle<'a> {
    reference: &'a dyn GitServer,
    candidate: &'a dyn GitServer,
    config: Config,
    shrink_budget: usize,
    log_steps: bool,
    known: Vec<KnownDivergence>,
}

/// A divergence of the candidate that is understood and filed, which the
/// oracle may be told to step over ([`Oracle::known_divergences`]) so that a
/// run goes on to find the next one. Each variant matches one exact pattern
/// and names its issue, and the servers' state is still compared after the
/// step, so stepping over one can never hide a lost update or a missing
/// object. Every step stepped over is counted in [`Stats::known`] and noted
/// in [`Run::notes`].
///
/// There are none today: the last one, a delete of a missing ref (issue
/// #133), was fixed. A new one gets a variant here, an arm in
/// [`KnownDivergence::issue`] and `KnownDivergence::matches`, and a
/// value of `aspen-testkit diff --known`; delete it again once its issue
/// is fixed, and the run proves the fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownDivergence {}

impl KnownDivergence {
    /// The issue that tracks it.
    pub fn issue(self) -> &'static str {
        match self {}
    }

    /// Whether the outcomes `a` (reference) and `b` (candidate) of `op`
    /// differ only in this way. `b_summaries` are the candidate's porcelain
    /// summaries ([`crate::world::Applied::summaries`]).
    fn matches(self, op: &Op, a: &Outcome, b: &Outcome, b_summaries: &[String]) -> bool {
        // While the enum has no variants, `match self {}` reads none of the
        // arguments; this binding keeps them used. Drop it with the next
        // variant.
        let _ = (op, a, b, b_summaries);
        match self {}
    }
}

/// Where in a sequence a divergence showed up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Right after setup (repository created, cloned, initial commit pushed).
    Setup,
    /// After the op at this index of the sequence.
    Op(usize),
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::Setup => f.write_str("setup"),
            Step::Op(i) => write!(f, "step {i}"),
        }
    }
}

/// What differed between the two servers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DivergenceKind {
    /// The clients saw different results for the same op.
    Outcome {
        /// What the client of the reference saw.
        reference: Outcome,
        /// What the client of the candidate saw.
        candidate: Outcome,
    },
    /// [`GitServer::list_refs`] disagreed.
    Refs {
        /// Lines only the reference listed.
        only_reference: Vec<String>,
        /// Lines only the candidate listed.
        only_candidate: Vec<String>,
    },
    /// The reachable object sets of fresh clones disagreed.
    Objects {
        /// Object IDs only the reference served.
        only_reference: Vec<String>,
        /// Object IDs only the candidate served.
        only_candidate: Vec<String>,
    },
    /// A check that must always pass failed on one server (or both).
    CheckFailed {
        /// The server's [`GitServer::name`].
        server: String,
        /// Which check: `setup` (the candidate's first push or its clones'
        /// first fetch), `list refs`, `per-branch fetch`, `fsck --strict`,
        /// `race settle` or `resync`.
        check: &'static str,
        /// Git's error output.
        error: String,
    },
    /// A server broke an invariant of a [`Op::Race`]: not exactly one push
    /// accepted, a tip other than the accepted push, or a loser rejected for
    /// a reason other than a lost compare-and-swap.
    RaceInvariant {
        /// The server's [`GitServer::name`].
        server: String,
        /// One line per broken invariant.
        problems: Vec<String>,
    },
}

/// A sequence on which the two servers behaved differently.
#[derive(Debug, Clone)]
pub struct Divergence {
    /// The reference server's name.
    pub reference: String,
    /// The candidate server's name.
    pub candidate: String,
    /// The seed that generated the sequence.
    pub seed: u64,
    /// The generator config the seed was used with.
    pub config: Config,
    /// The sequence up to and including the step after which the servers
    /// first differed.
    pub ops: Vec<Op>,
    /// The first step that differed.
    pub step: Step,
    /// What differed.
    pub kind: DivergenceKind,
    /// The smallest sub-sequence found that still diverges, and how it
    /// diverged, if shrinking found anything smaller than `ops`.
    pub shrunk: Option<(Vec<Op>, Step, DivergenceKind)>,
    /// What the oracle itself did between steps of `ops`, such as resyncs
    /// after races: `(step index, note)`.
    pub notes: Vec<(usize, String)>,
}

/// The result of [`Oracle::check_seeds`].
#[derive(Debug)]
pub struct Report {
    /// Sequences run to completion without a divergence.
    pub clean_sequences: usize,
    /// Ops applied in those sequences.
    pub clean_steps: usize,
    /// What the reference's clients saw, over every op applied (including
    /// the diverging sequence up to its divergence). A run where nothing was
    /// pushed or rejected proves little, so tests check these.
    pub stats: Stats,
    /// The first divergence, which stopped the run.
    pub divergence: Option<Divergence>,
}

/// Tally of the [`Outcome`]s the reference server's clients saw.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Ref updates the server accepted (including deletions).
    pub refs_accepted: usize,
    /// Ref updates the server rejected (for example, non-fast-forward pushes
    /// from stale clones).
    pub refs_rejected: usize,
    /// `git fetch` runs.
    pub fetches: usize,
    /// Ops the clone's own view ruled out before contacting the server.
    pub skipped: usize,
    /// Ops whose local preparation failed. Always 0 unless the harness has a
    /// bug.
    pub local_failures: usize,
    /// [`Op::Race`]s whose pushes ran (each counts its accepted and rejected
    /// updates above too).
    pub races: usize,
    /// Races the two servers resolved with different winners, after which
    /// the oracle forced the reference to the candidate's winner.
    pub resyncs: usize,
    /// Steps whose outcomes differed only by a [`KnownDivergence`] the
    /// oracle was told to step over. Always 0 unless it was told to.
    pub known: usize,
    /// Ref updates pushed through the plain repository URL
    /// ([`Op::PlainPush`], [`Op::PlainRace`]), trunk included; each also
    /// counts as accepted or rejected above. Always 0 unless
    /// [`Config::plain_url_pushes`] is on.
    ///
    /// Like every field here, counted from the reference's outcomes. On a
    /// step that did not diverge the candidate's agree: a plain push's
    /// accepted/rejected per ref is compared, and each server's race had
    /// exactly one winner.
    pub plain_url_updates: usize,
    /// Those of them that updated a branch other than trunk and were
    /// rejected. The plain URL advertises trunk, so a trunk push there is
    /// checked by the client as through trunk's branch URL; any other
    /// branch is not advertised on Aspen, so the client sent a zero old
    /// value and the rejection is Aspen's own fast-forward check (or its
    /// lost compare-and-swap). A run that proves that check needs this
    /// above 0.
    pub plain_url_branch_rejected: usize,
}

impl Stats {
    fn add(&mut self, outcome: &Outcome) {
        match outcome {
            Outcome::Skipped(_) => self.skipped += 1,
            Outcome::LocalFailure(_) => self.local_failures += 1,
            Outcome::Push { refs, .. } => {
                for (_, accepted) in refs {
                    if *accepted {
                        self.refs_accepted += 1;
                    } else {
                        self.refs_rejected += 1;
                    }
                }
            }
            Outcome::Fetch { .. } => self.fetches += 1,
            Outcome::Concurrent(all) => all.iter().for_each(|o| self.add(o)),
            Outcome::Race { .. } => {}
        }
    }

    fn add_race(&mut self, run: &RaceRun, plain: bool) {
        self.races += 1;
        for racer in &run.racers {
            if racer.accepted {
                self.refs_accepted += 1;
            } else {
                self.refs_rejected += 1;
            }
            if plain {
                self.plain_url_updates += 1;
                if run.branch != Branch::TRUNK && !racer.accepted {
                    self.plain_url_branch_rejected += 1;
                }
            }
        }
    }

    /// Counts the ref updates of an [`Op::PlainPush`]'s outcome.
    fn add_plain(&mut self, outcome: &Outcome) {
        if let Outcome::Push { refs, .. } = outcome {
            let trunk = format!("refs/heads/{}", Branch::TRUNK);
            self.plain_url_updates += refs.len();
            self.plain_url_branch_rejected += refs
                .iter()
                .filter(|(name, accepted)| !accepted && *name != trunk)
                .count();
        }
    }

    fn merge(&mut self, other: Stats) {
        self.refs_accepted += other.refs_accepted;
        self.refs_rejected += other.refs_rejected;
        self.fetches += other.fetches;
        self.skipped += other.skipped;
        self.local_failures += other.local_failures;
        self.races += other.races;
        self.resyncs += other.resyncs;
        self.known += other.known;
        self.plain_url_updates += other.plain_url_updates;
        self.plain_url_branch_rejected += other.plain_url_branch_rejected;
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ref update(s) accepted, {} rejected, {} fetch(es), {} race(s), {} resync(s), {} skipped, {} local failure(s)",
            self.refs_accepted,
            self.refs_rejected,
            self.fetches,
            self.races,
            self.resyncs,
            self.skipped,
            self.local_failures
        )?;
        if self.plain_url_updates > 0 {
            write!(
                f,
                ", {} through the repository URL ({} rejected off trunk)",
                self.plain_url_updates, self.plain_url_branch_rejected
            )?;
        }
        if self.known > 0 {
            write!(f, ", {} known divergence(s) stepped over", self.known)?;
        }
        Ok(())
    }
}

impl<'a> Oracle<'a> {
    /// An oracle comparing `candidate` against `reference`, with shrinking
    /// on (up to 40 replays per divergence).
    pub fn new(reference: &'a dyn GitServer, candidate: &'a dyn GitServer, config: Config) -> Self {
        Self {
            reference,
            candidate,
            config,
            shrink_budget: 40,
            log_steps: false,
            known: Vec::new(),
        }
    }

    /// Step over these filed divergences instead of stopping at them (see
    /// [`KnownDivergence`]). Off by default: a run against a new server
    /// should stop at everything.
    pub fn known_divergences(mut self, known: impl IntoIterator<Item = KnownDivergence>) -> Self {
        self.known = known.into_iter().collect();
        self
    }

    /// Print every step, and every resync, to stderr as it runs.
    pub fn log_steps(mut self, on: bool) -> Self {
        self.log_steps = on;
        self
    }

    /// The most sequences to replay while shrinking a divergence; 0 turns
    /// shrinking off.
    pub fn shrink_budget(mut self, replays: usize) -> Self {
        self.shrink_budget = replays;
        self
    }

    /// The generator config.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Runs the sequences for `seeds` in order and stops at the first
    /// divergence. Errors only if the harness itself fails (for example,
    /// `git` cannot be started); server misbehaviour is a divergence.
    pub fn check_seeds(&self, seeds: impl IntoIterator<Item = u64>) -> io::Result<Report> {
        let mut report = Report {
            clean_sequences: 0,
            clean_steps: 0,
            stats: Stats::default(),
            divergence: None,
        };
        for seed in seeds {
            let (divergence, stats) = self.check_seed_with_stats(seed)?;
            report.stats.merge(stats);
            match divergence {
                Some(divergence) => {
                    report.divergence = Some(divergence);
                    break;
                }
                None => {
                    report.clean_sequences += 1;
                    report.clean_steps += self.config.steps;
                }
            }
        }
        Ok(report)
    }

    /// Runs the sequence for `seed`, shrinking any divergence.
    pub fn check_seed(&self, seed: u64) -> io::Result<Option<Divergence>> {
        Ok(self.check_seed_with_stats(seed)?.0)
    }

    fn check_seed_with_stats(&self, seed: u64) -> io::Result<(Option<Divergence>, Stats)> {
        let ops = ops::generate(seed, &self.config);
        if self.log_steps {
            eprintln!("seed {seed:#018x}:");
        }
        let run = self.run_ops(&ops, self.log_steps)?;
        let stats = run.stats;
        let Some((step, kind)) = run.divergence else {
            return Ok((None, stats));
        };
        let failing = prefix(&ops, step);
        let notes = run.notes;
        let shrunk = self.shrink(failing.clone(), step, kind.clone())?;
        let divergence = Divergence {
            reference: self.reference.name().to_owned(),
            candidate: self.candidate.name().to_owned(),
            seed,
            config: self.config.clone(),
            ops: failing,
            step,
            kind,
            shrunk,
            notes,
        };
        Ok((Some(divergence), stats))
    }

    /// Applies `ops` to a fresh repository on each server and returns the
    /// first step at which they differ, if any.
    pub fn check_ops(&self, ops: &[Op]) -> io::Result<Option<(Step, DivergenceKind)>> {
        Ok(self.run_ops(ops, false)?.divergence)
    }

    /// Like [`Oracle::check_ops`], also returning what the reference's
    /// clients saw and the oracle's notes (resyncs).
    pub fn run(&self, ops: &[Op]) -> io::Result<Run> {
        self.run_ops(ops, self.log_steps)
    }

    fn run_ops(&self, ops: &[Op], log: bool) -> io::Result<Run> {
        let mut run = Run {
            divergence: None,
            stats: Stats::default(),
            notes: Vec::new(),
        };
        let scratch = tempfile::Builder::new().prefix("aspen-oracle-").tempdir()?;
        let repo = unique_repo_name();
        let (reference, candidate) = both(
            || {
                World::setup(
                    self.reference,
                    &repo,
                    &scratch.path().join("reference"),
                    self.config.clones,
                    self.config.branches,
                )
            },
            || {
                World::setup(
                    self.candidate,
                    &repo,
                    &scratch.path().join("candidate"),
                    self.config.clones,
                    self.config.branches,
                )
            },
        );
        // The reference must set up; a candidate that cannot (its first
        // push or the clones' first fetch fails) has diverged at setup.
        let reference = reference?;
        let candidate = match candidate {
            Ok(world) => world,
            Err(error) => {
                run.divergence = Some((
                    Step::Setup,
                    DivergenceKind::CheckFailed {
                        server: self.candidate.name().to_owned(),
                        check: "setup",
                        error: error.to_string(),
                    },
                ));
                return Ok(run);
            }
        };
        if let Some(kind) = self.compare_state(&reference, &candidate, "setup")? {
            run.divergence = Some((Step::Setup, kind));
            return Ok(run);
        }
        for (i, op) in ops.iter().enumerate() {
            if log {
                eprintln!("  {i:>3}: {op}");
            }
            let (a, b) = both(|| reference.apply(i, op), || candidate.apply(i, op));
            let (a, b) = (a?, b?);
            let (race_a, race_b) = (a.race, b.race);
            let (a, b, b_summaries) = (a.outcome, b.outcome, b.summaries);
            run.stats.add(&a);
            if matches!(op, Op::PlainPush { .. }) {
                run.stats.add_plain(&a);
            }
            if a != b {
                if let Some(known) = self
                    .known
                    .iter()
                    .find(|k| k.matches(op, &a, &b, &b_summaries))
                {
                    // The clients' views differ too (git drops the
                    // remote-tracking ref of a branch it deleted): give the
                    // candidate's clone the reference clone's view, or its
                    // next push would build on other history.
                    if let Op::Delete { clone, branch } = op {
                        let seen = reference.tracking(*clone, *branch)?;
                        candidate.set_tracking(*clone, *branch, seen.as_deref())?;
                    }
                    let note = format!("known divergence {known:?} ({})", known.issue());
                    if log {
                        eprintln!("       {note}");
                    }
                    run.stats.known += 1;
                    run.notes.push((i, note));
                } else {
                    let kind = DivergenceKind::Outcome {
                        reference: a,
                        candidate: b,
                    };
                    run.divergence = Some((Step::Op(i), kind));
                    return Ok(run);
                }
            }
            if let (Some(race_a), Some(race_b)) = (&race_a, &race_b) {
                run.stats
                    .add_race(race_a, matches!(op, Op::PlainRace { .. }));
                let checked = self.check_race(&reference, race_a, &candidate, race_b)?;
                match checked {
                    Err(kind) => {
                        run.divergence = Some((Step::Op(i), kind));
                        return Ok(run);
                    }
                    Ok(Some(note)) => {
                        if log {
                            eprintln!("       {note}");
                        }
                        run.stats.resyncs += 1;
                        run.notes.push((i, note));
                    }
                    Ok(None) => {}
                }
            }
            if let Some(kind) = self.compare_state(&reference, &candidate, &i.to_string())? {
                run.divergence = Some((Step::Op(i), kind));
                return Ok(run);
            }
        }
        drop(scratch);
        Ok(run)
    }

    /// Checks a race's invariants on both servers, then makes them agree:
    /// if the winners differ, force-pushes the candidate's winning commit to
    /// the reference, so the candidate is never asked to rewrite a branch
    /// (trunk included). Returns a note if it resynced.
    fn check_race(
        &self,
        reference: &World,
        race_a: &RaceRun,
        candidate: &World,
        race_b: &RaceRun,
    ) -> io::Result<Result<Option<String>, DivergenceKind>> {
        let (problems_a, problems_b) = both(
            || reference.race_problems(race_a),
            || candidate.race_problems(race_b),
        );
        // Report the candidate first: the reference is expected to be healthy.
        for (server, problems) in [(self.candidate, problems_b?), (self.reference, problems_a?)] {
            if !problems.is_empty() {
                return Ok(Err(DivergenceKind::RaceInvariant {
                    server: server.name().to_owned(),
                    problems,
                }));
            }
        }
        let (Some(win_a), Some(win_b)) = (race_a.winner(), race_b.winner()) else {
            unreachable!("race_problems requires exactly one winner");
        };
        let mut note = None;
        if win_a.commit != win_b.commit {
            // Commits are deterministic across worlds, so the reference's
            // clone `win_b.clone` holds the candidate's winning commit too.
            if let Err(error) = reference.resync(win_b.clone, race_b.branch, &win_b.commit)? {
                return Ok(Err(DivergenceKind::CheckFailed {
                    server: self.reference.name().to_owned(),
                    check: "resync",
                    error,
                }));
            }
            note = Some(format!(
                "resync: {} won the race on {} with clone {}'s push, {} with clone {}'s; forced {} on {} to the candidate's winner {}",
                self.reference.name(),
                race_a.branch,
                win_a.clone,
                self.candidate.name(),
                win_b.clone,
                race_a.branch,
                self.reference.name(),
                &win_b.commit[..win_b.commit.len().min(12)],
            ));
        }
        let (settled_a, settled_b) = both(
            || reference.settle_race(race_a),
            || candidate.settle_race(race_b),
        );
        for (server, settled) in [(self.candidate, settled_b?), (self.reference, settled_a?)] {
            if let Err(error) = settled {
                return Ok(Err(DivergenceKind::CheckFailed {
                    server: server.name().to_owned(),
                    check: "race settle",
                    error,
                }));
            }
        }
        Ok(Ok(note))
    }

    fn compare_state(
        &self,
        reference: &World,
        candidate: &World,
        tag: &str,
    ) -> io::Result<Option<DivergenceKind>> {
        let (a, b) = both(|| reference.observe(tag), || candidate.observe(tag));
        Ok(compare(
            self.reference.name(),
            &a?,
            self.candidate.name(),
            &b?,
        ))
    }

    /// Greedy delta debugging: drop one op at a time, newest first, and keep
    /// the drop whenever the servers still diverge.
    fn shrink(
        &self,
        mut ops: Vec<Op>,
        mut step: Step,
        mut kind: DivergenceKind,
    ) -> io::Result<Option<(Vec<Op>, Step, DivergenceKind)>> {
        let original_len = ops.len();
        let mut replays = 0;
        let mut i = ops.len();
        while i > 0 && replays < self.shrink_budget {
            i -= 1;
            let mut trial = ops.clone();
            trial.remove(i);
            replays += 1;
            if let Some((s, k)) = self.check_ops(&trial)? {
                ops = prefix(&trial, s);
                step = s;
                kind = k;
                i = i.min(ops.len());
            }
        }
        Ok((ops.len() < original_len).then_some((ops, step, kind)))
    }
}

/// The result of [`Oracle::run`].
#[derive(Debug)]
pub struct Run {
    /// The first step at which the servers differed, and how.
    pub divergence: Option<(Step, DivergenceKind)>,
    /// What the reference's clients saw.
    pub stats: Stats,
    /// What the oracle did between steps, such as resyncs: `(step, note)`.
    pub notes: Vec<(usize, String)>,
}

/// `ops` up to and including `step` (nothing for [`Step::Setup`]).
fn prefix(ops: &[Op], step: Step) -> Vec<Op> {
    match step {
        Step::Setup => Vec::new(),
        Step::Op(i) => ops[..=i].to_vec(),
    }
}

/// Runs two closures in parallel.
fn both<A: Send, B: Send>(a: impl FnOnce() -> A + Send, b: impl FnOnce() -> B + Send) -> (A, B) {
    thread::scope(|scope| {
        let b = scope.spawn(b);
        let a = a();
        (a, b.join().expect("oracle worker panicked"))
    })
}

fn unique_repo_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "oracle-{}-{}.git",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn compare(
    ref_name: &str,
    a: &Observation,
    cand_name: &str,
    b: &Observation,
) -> Option<DivergenceKind> {
    let failed = |server: &str, check: &'static str, error: &String| {
        Some(DivergenceKind::CheckFailed {
            server: server.to_owned(),
            check,
            error: error.clone(),
        })
    };
    // Report the candidate first: the reference is expected to be healthy.
    for (name, obs) in [(cand_name, b), (ref_name, a)] {
        if let Err(e) = &obs.refs {
            return failed(name, "list refs", e);
        }
        if let Err(e) = &obs.objects {
            return failed(name, "per-branch fetch", e);
        }
        if let Err(e) = &obs.fsck {
            return failed(name, "fsck --strict", e);
        }
    }
    let (Ok(refs_a), Ok(refs_b), Ok(objs_a), Ok(objs_b)) =
        (&a.refs, &b.refs, &a.objects, &b.objects)
    else {
        unreachable!("errors returned above");
    };
    if refs_a != refs_b {
        let only = |x: &RefMap, y: &RefMap| -> Vec<String> {
            x.iter()
                .filter(|(name, oid)| y.get(*name) != Some(oid))
                .map(|(name, oid)| format!("{oid} {name}"))
                .collect()
        };
        return Some(DivergenceKind::Refs {
            only_reference: only(refs_a, refs_b),
            only_candidate: only(refs_b, refs_a),
        });
    }
    if objs_a != objs_b {
        return Some(DivergenceKind::Objects {
            only_reference: objs_a.difference(objs_b).cloned().collect(),
            only_candidate: objs_b.difference(objs_a).cloned().collect(),
        });
    }
    None
}

impl fmt::Display for DivergenceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DivergenceKind::Outcome {
                reference,
                candidate,
            } => write!(
                f,
                "the clients saw different results\n      reference: {reference:?}\n      candidate: {candidate:?}"
            ),
            DivergenceKind::Refs {
                only_reference,
                only_candidate,
            } => {
                f.write_str("the servers list different refs")?;
                write_lines(f, "only on the reference", only_reference)?;
                write_lines(f, "only on the candidate", only_candidate)
            }
            DivergenceKind::Objects {
                only_reference,
                only_candidate,
            } => {
                f.write_str("fresh per-branch fetches hold different reachable objects")?;
                write_lines(f, "only on the reference", only_reference)?;
                write_lines(f, "only on the candidate", only_candidate)
            }
            DivergenceKind::CheckFailed {
                server,
                check,
                error,
            } => {
                write!(f, "`{check}` failed on {server}:")?;
                for line in error.lines().take(20) {
                    write!(f, "\n      {line}")?;
                }
                Ok(())
            }
            DivergenceKind::RaceInvariant { server, problems } => {
                write!(f, "{server} broke a race invariant:")?;
                for line in problems {
                    write!(f, "\n      {line}")?;
                }
                Ok(())
            }
        }
    }
}

fn write_lines(f: &mut fmt::Formatter<'_>, label: &str, lines: &[String]) -> fmt::Result {
    write!(f, "\n    {label} ({}):", lines.len())?;
    for line in lines.iter().take(10) {
        write!(f, "\n      {line}")?;
    }
    if lines.len() > 10 {
        write!(f, "\n      ...")?;
    }
    Ok(())
}

fn write_ops(f: &mut fmt::Formatter<'_>, ops: &[Op], notes: &[(usize, String)]) -> fmt::Result {
    for (i, op) in ops.iter().enumerate() {
        write!(f, "\n    {i:>3}: {op}")?;
        for (_, note) in notes.iter().filter(|(step, _)| *step == i) {
            write!(f, "\n         {note}")?;
        }
    }
    Ok(())
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let c = &self.config;
        write!(
            f,
            "differential oracle: {} diverged from {}\n  seed: {:#018x} (steps={}, clones={}, branches={}, protect_trunk={}, plain_url_pushes={})\n  first divergence after {}",
            self.candidate,
            self.reference,
            self.seed,
            c.steps,
            c.clones,
            c.branches,
            c.protect_trunk,
            c.plain_url_pushes,
            self.step,
        )?;
        if let Step::Op(i) = self.step {
            write!(f, ": {}", self.ops[i])?;
        }
        write!(f, "\n  what: {}", self.kind)?;
        write!(f, "\n  sequence up to the divergence:")?;
        write_ops(f, &self.ops, &self.notes)?;
        if let Some((ops, step, kind)) = &self.shrunk {
            write!(
                f,
                "\n  shrunk to {} op(s), diverging after {step}: {kind}",
                ops.len()
            )?;
            write_ops(f, ops, &[])?;
        }
        write!(
            f,
            "\n  replay: cargo run -p aspen-testkit -- diff --seed {:#x} --sequences 1 --steps {} --clones {} --branches {}{}{} --candidate <server>",
            self.seed,
            c.steps,
            c.clones,
            c.branches,
            if c.protect_trunk {
                ""
            } else {
                " --delete-trunk"
            },
            if c.plain_url_pushes {
                " --plain-url-pushes"
            } else {
                ""
            },
        )
    }
}

/// The seeds `base, base + 1, ...`: one per sequence, each replayable alone.
pub fn seeds(base: u64, count: usize) -> impl Iterator<Item = u64> {
    (0..count as u64).map(move |i| base.wrapping_add(i))
}

/// A seed from the clock, for runs that should explore new sequences.
pub fn seed_from_clock() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    crate::rng::Rng::new(nanos ^ u64::from(std::process::id())).next_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(refs: &[&str], objects: &[&str]) -> Observation {
        Observation {
            refs: Ok(refs
                .iter()
                .map(|line| {
                    let (oid, name) = line.split_once(' ').unwrap();
                    (name.to_owned(), oid.to_owned())
                })
                .collect()),
            objects: Ok(objects.iter().map(|s| (*s).to_owned()).collect()),
            fsck: Ok(()),
        }
    }

    #[test]
    fn equal_observations_do_not_diverge() {
        let a = obs(&["1 refs/heads/main"], &["1", "2"]);
        assert_eq!(compare("ref", &a, "cand", &a.clone()), None);
    }

    #[test]
    fn ref_differences_are_listed_per_side() {
        let a = obs(&["1 refs/heads/main", "2 refs/heads/x"], &["1"]);
        let b = obs(&["1 refs/heads/main", "3 refs/heads/x"], &["1"]);
        assert_eq!(
            compare("ref", &a, "cand", &b),
            Some(DivergenceKind::Refs {
                only_reference: vec!["2 refs/heads/x".to_owned()],
                only_candidate: vec!["3 refs/heads/x".to_owned()],
            })
        );
    }

    #[test]
    fn object_differences_are_caught_even_when_refs_agree() {
        let a = obs(&["1 refs/heads/main"], &["1", "2"]);
        let b = obs(&["1 refs/heads/main"], &["1", "9"]);
        assert_eq!(
            compare("ref", &a, "cand", &b),
            Some(DivergenceKind::Objects {
                only_reference: vec!["2".to_owned()],
                only_candidate: vec!["9".to_owned()],
            })
        );
    }

    #[test]
    fn fsck_failure_alone_is_a_divergence() {
        let a = obs(&["1 refs/heads/main"], &["1"]);
        let mut b = a.clone();
        b.fsck = Err("zeroPaddedFilemode".to_owned());
        assert_eq!(
            compare("ref", &a, "cand", &b),
            Some(DivergenceKind::CheckFailed {
                server: "cand".to_owned(),
                check: "fsck --strict",
                error: "zeroPaddedFilemode".to_owned(),
            })
        );
        // The same failure on the reference is reported too: the oracle
        // never trusts either side blindly.
        assert!(matches!(
            compare("ref", &b, "cand", &a),
            Some(DivergenceKind::CheckFailed { server, .. }) if server == "ref"
        ));
    }

    #[test]
    fn prefix_keeps_the_diverging_step() {
        let ops = ops::generate(5, &Config::default());
        assert_eq!(prefix(&ops, Step::Setup), Vec::<Op>::new());
        assert_eq!(prefix(&ops, Step::Op(2)), ops[..3].to_vec());
    }
}

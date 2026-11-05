//! A chaos run: epochs of load under faults, each ending in checks.
//!
//! An epoch has three phases:
//!
//! 1. **Active**: every client runs its loop, and the injector takes a
//!    fault step every so often. The phase lasts [`RunConfig::active`],
//!    or longer until an [`ActiveUntil`] condition holds. At its end the
//!    injector heals everything and clients finish the command they are
//!    in.
//! 2. **Quiet**: nothing runs for a while, so work a fault left in flight
//!    on the server (a push whose client gave up) can end.
//! 3. **Checks**: the boundary reads, linearizability of each ref's
//!    history over the epoch, durability of every acknowledged push, and
//!    fetchability of every ref.
//!
//! A push with an unknown outcome may still land after the epoch ends
//! (its server kept going after the client gave up). So it is carried
//! into the next epoch's history for as long as it could still apply,
//! which is while its ref is still at the push's base: values never
//! repeat, so once the ref moves on the push can never land.
//!
//! A long run starts a fresh repository every `repo_epochs` epochs, which
//! keeps each epoch's full-clone check about the same size.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aspen_testkit::GitServer;
use aspen_testkit::rng::Rng;

use crate::checks::Checker;
use crate::cluster::write_file;
use crate::faults::{Injector, between};
use crate::history::{Call, History, Oid, Op, Outcome};
use crate::lin::{self, Verdict};
use crate::report::{Finding, Invariant, Report, Tally};
use crate::workload::{Client, Keys, Target, URI_FETCHES, URIS_USED, Weights, rev_parse, run_git};

/// The deployment side of a run: healing and the checks that need to see
/// inside it.
pub trait Deployment: Sync {
    /// Clears every fault and brings every process back.
    fn heal(&self) -> io::Result<()>;

    /// Checks after the last epoch (background work finished, alerts),
    /// added to `report`.
    fn final_checks(&self, report: &mut Report);

    /// Problems the deployment saw on its own since the last call (an
    /// instance that exited without being killed, alert counters that
    /// could not be read), as findings of epoch `epoch`.
    fn take_findings(&self, epoch: usize) -> Vec<Finding>;
}

/// When a run ends.
#[derive(Debug, Clone, Copy)]
pub enum Stop {
    Epochs(usize),
    After(Duration),
}

/// What an epoch's active phase has done so far, as an [`ActiveUntil`]
/// condition sees it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Progress {
    /// Pushes the server acknowledged since the phase began.
    pub acked_pushes: u64,
}

/// A condition that keeps each epoch's active phase going past
/// [`RunConfig::active`] until it holds, so that what a run must exercise
/// happens however slow the machine is. Tests of deliberately broken
/// servers use it to run until the server's fault has fired: a fault armed
/// for the 10th push must not depend on 10 pushes fitting into a fixed
/// number of seconds. A run that must check a minimum amount of work
/// waits for it the same way ([`ActiveUntil::acked_pushes`]), rather than
/// hoping it fits into the phase's minimum length.
#[derive(Clone)]
pub struct ActiveUntil {
    /// What the condition waits for, in words, for the report.
    pub what: String,
    /// Whether it holds now, given the phase's progress. Called often,
    /// from every client thread.
    pub holds: Arc<dyn Fn(&Progress) -> bool + Send + Sync>,
    /// The longest an active phase may last in all. An epoch that reaches
    /// it before the condition holds fails the run as
    /// [`Invariant::Vacuous`].
    pub limit: Duration,
}

impl ActiveUntil {
    /// Until the server has acknowledged `n` pushes in the epoch, for at
    /// most `limit`.
    pub fn acked_pushes(n: u64, limit: Duration) -> Self {
        Self {
            what: format!("the server acknowledged {n} push(es) in the epoch"),
            holds: Arc::new(move |p| p.acked_pushes >= n),
            limit,
        }
    }
}

impl std::fmt::Debug for ActiveUntil {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveUntil")
            .field("what", &self.what)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// One epoch's active phase: it lasts at least its minimum, then until its
/// [`ActiveUntil`] condition holds (if any), and never past its limit.
/// Once over it stays over, so every client and the injector agree.
#[derive(Debug)]
pub struct ActivePhase<'a> {
    earliest: Instant,
    limit: Instant,
    until: Option<&'a ActiveUntil>,
    /// The run's history and how many pushes it had acknowledged when the
    /// phase began, for [`Progress`].
    history: Option<(&'a History, u64)>,
    state: AtomicU8,
}

const RUNNING: u8 = 0;
const ENDED: u8 = 1;
const TIMED_OUT: u8 = 2;

impl<'a> ActivePhase<'a> {
    /// A phase that ends `d` from now.
    pub fn lasting(d: Duration) -> ActivePhase<'static> {
        let now = Instant::now();
        ActivePhase {
            earliest: now + d,
            limit: now + d,
            until: None,
            history: None,
            state: AtomicU8::new(RUNNING),
        }
    }

    /// A phase that lasts at least `min`, then until `until` holds, with
    /// its progress read from `history`.
    pub fn new(min: Duration, until: Option<&'a ActiveUntil>, history: &'a History) -> Self {
        let now = Instant::now();
        let limit = until.map_or(min, |u| u.limit.max(min));
        Self {
            earliest: now + min,
            limit: now + limit,
            until,
            history: Some((history, history.acked_pushes())),
            state: AtomicU8::new(RUNNING),
        }
    }

    /// What the phase has done so far.
    pub fn progress(&self) -> Progress {
        Progress {
            acked_pushes: self
                .history
                .map_or(0, |(h, start)| h.acked_pushes().saturating_sub(start)),
        }
    }

    /// Whether the phase is over.
    pub fn over(&self) -> bool {
        if self.state.load(Ordering::Acquire) != RUNNING {
            return true;
        }
        let now = Instant::now();
        if now < self.earliest {
            return false;
        }
        let state = match self.until {
            None => ENDED,
            Some(u) if (u.holds)(&self.progress()) => ENDED,
            Some(_) if now >= self.limit => TIMED_OUT,
            Some(_) => return false,
        };
        // The first thread to see the end decides how it ended.
        let _ = self
            .state
            .compare_exchange(RUNNING, state, Ordering::AcqRel, Ordering::Acquire);
        true
    }

    /// Whether the phase reached its limit before its condition held.
    pub fn timed_out(&self) -> bool {
        self.state.load(Ordering::Acquire) == TIMED_OUT
    }

    /// Sleeps for `d` or until the phase is over, whichever comes first;
    /// returns whether it is over.
    pub fn sleep(&self, d: Duration) -> bool {
        let wake = Instant::now() + d;
        loop {
            if self.over() {
                return true;
            }
            let now = Instant::now();
            if now >= wake {
                return false;
            }
            std::thread::sleep((wake - now).min(Duration::from_millis(50)));
        }
    }
}

/// How to run.
#[derive(Debug, Clone)]
pub struct RunConfig {
    pub scenario: String,
    pub seed: u64,
    pub clients: usize,
    pub shared_branches: usize,
    pub weights: Weights,
    /// Length of each epoch's active phase (its minimum, with
    /// `active_until`).
    pub active: Duration,
    /// Keeps each active phase going past `active` until a condition holds.
    pub active_until: Option<ActiveUntil>,
    /// Length of each epoch's quiet phase.
    pub quiet: Duration,
    pub stop: Stop,
    /// Time between fault steps (each step may also last a while).
    pub fault_gap: (Duration, Duration),
    /// Start a fresh repository after this many epochs.
    pub repo_epochs: usize,
    /// Working directory for clients, checks and the history file.
    pub root: PathBuf,
    /// Print a line per epoch.
    pub verbose: bool,
    /// See [`Target::unscoped_branch_pushes`].
    pub unscoped_branch_pushes: bool,
    /// The linearizability search's budget per ref and epoch, in visited
    /// states. A search that runs out fails the run.
    pub lin_budget: usize,
    /// Fail the run if no fetch received a packfile URI (the server is
    /// meant to send them, and Git falls back silently when it does not).
    pub expect_packfile_uris: bool,
}

impl RunConfig {
    /// Defaults for scenario `scenario` with seed `seed`, working in
    /// `root`.
    pub fn new(scenario: &str, seed: u64, root: PathBuf) -> Self {
        Self {
            scenario: scenario.to_owned(),
            seed,
            clients: 6,
            shared_branches: 1,
            weights: Weights::DEFAULT,
            active: Duration::from_secs(30),
            active_until: None,
            quiet: Duration::from_secs(3),
            stop: Stop::Epochs(3),
            fault_gap: (Duration::from_millis(300), Duration::from_millis(2500)),
            repo_epochs: 20,
            root,
            verbose: false,
            unscoped_branch_pushes: false,
            lin_budget: lin::DEFAULT_BUDGET,
            expect_packfile_uris: false,
        }
    }
}

/// Per-repository state.
struct Generation {
    repo_url: String,
    keys: Keys,
    clients: Vec<Client>,
    /// Each ref's value at the start of the current epoch.
    initial: BTreeMap<String, Option<Oid>>,
    /// Unknown pushes that may still land, per ref.
    carried: BTreeMap<String, Vec<Op>>,
    /// Every acknowledged push since the repository was created.
    acked: Vec<Op>,
}

/// Runs the workload against `server`, with `injector` breaking things
/// and `deployment` healing them, until `cfg.stop`.
pub fn run(
    server: &dyn GitServer,
    deployment: Option<&dyn Deployment>,
    mut injector: Option<&mut (dyn Injector + '_)>,
    cfg: &RunConfig,
) -> io::Result<Report> {
    let started = Instant::now();
    let mut report = Report {
        scenario: cfg.scenario.clone(),
        seed: cfg.seed,
        ..Report::default()
    };
    std::fs::create_dir_all(&cfg.root)?;
    let history = History::new();
    let findings = Mutex::new(Vec::new());
    let tally = Tally::default();
    let mut faults_injected = 0u64;
    let checker = Checker::new(&cfg.root)?;
    let mut rng = Rng::new(cfg.seed);
    let mut generation: Option<Generation> = None;
    let mut epoch = 0;
    loop {
        let done = match cfg.stop {
            Stop::Epochs(n) => epoch >= n,
            Stop::After(d) => epoch > 0 && started.elapsed() >= d,
        };
        if done {
            break;
        }
        if epoch % cfg.repo_epochs.max(1) == 0 {
            let g = epoch / cfg.repo_epochs.max(1);
            // The previous repository's checkouts are no longer needed
            // (its data on the server stays).
            if g > 0 {
                let _ = std::fs::remove_dir_all(cfg.root.join(format!("repo-{}", g - 1)));
            }
            generation = Some(new_generation(server, cfg, g, &checker)?);
        }
        let gen_state = generation.as_mut().expect("set above");
        let target = Target {
            server,
            repo_url: gen_state.repo_url.clone(),
            keys: gen_state.keys.clone(),
            unscoped_branch_pushes: cfg.unscoped_branch_pushes,
        };
        let first_op = history.len();

        // 1. Active.
        let phase = ActivePhase::new(cfg.active, cfg.active_until.as_ref(), &history);
        let mut fault_labels: Vec<String> = Vec::new();
        let mut fault_errors: Vec<String> = Vec::new();
        let mut inj_rng = Rng::new(rng.next_u64());
        std::thread::scope(|s| {
            for c in gen_state.clients.iter_mut() {
                let (t, h, f, n, p) = (&target, &history, &findings, &tally, &phase);
                s.spawn(move || c.run(t, cfg.weights, h, f, n, epoch, p));
            }
            if let Some(inj) = injector.as_mut() {
                let (labels, errors, p) = (&mut fault_labels, &mut fault_errors, &phase);
                s.spawn(move || {
                    loop {
                        let gap = between(&mut inj_rng, cfg.fault_gap.0, cfg.fault_gap.1);
                        if p.sleep(gap) {
                            break;
                        }
                        match inj.step(&mut inj_rng) {
                            Ok(Some(l)) => labels.push(l),
                            Ok(None) => {}
                            Err(e) => errors.push(e.to_string()),
                        }
                    }
                    if let Err(e) = inj.heal() {
                        errors.push(format!("heal: {e}"));
                    }
                });
            }
        });
        if phase.timed_out()
            && let Some(u) = &cfg.active_until
        {
            lock(&findings).push(Finding::new(
                Invariant::Vacuous,
                epoch,
                format!(
                    "the active phase reached its limit of {:?} before {}",
                    u.limit, u.what
                ),
            ));
        }
        faults_injected += fault_labels.len() as u64;
        for l in fault_labels {
            report.count(format!("fault: {l}"), 1);
        }
        for e in fault_errors {
            lock(&findings).push(Finding::new(
                Invariant::Unchecked,
                epoch,
                format!("a fault step failed, so the fault it meant to inject did not run: {e}"),
            ));
        }

        // 2. Quiet.
        if let Some(d) = deployment
            && let Err(e) = d.heal()
        {
            lock(&findings).push(Finding::new(
                Invariant::Unavailable,
                epoch,
                format!("the deployment could not be healed: {e}"),
            ));
            break;
        }
        if let Some(d) = deployment {
            lock(&findings).extend(d.take_findings(epoch));
        }
        std::thread::sleep(cfg.quiet);

        // 3. Checks.
        let tips = match checker.read_tips(&target, &history, epoch) {
            Ok(t) => t,
            Err(f) => {
                lock(&findings).push(f);
                epoch += 1;
                break;
            }
        };
        let ops = history.ops_since(first_op);
        count_ops(&mut report, &ops);
        let boundary_start = ops
            .iter()
            .filter(|o| o.client == usize::MAX)
            .map(|o| o.invoked)
            .min()
            .unwrap_or_else(|| history.now());
        for key in target.keys.all() {
            let mut key_ops: Vec<Op> = gen_state.carried.remove(&key).unwrap_or_default();
            key_ops.extend(ops.iter().filter(|o| o.key == key).cloned());
            let initial = gen_state.initial.get(&key).cloned().flatten();
            match lin::check_with_budget(initial.as_deref(), &key_ops, cfg.lin_budget) {
                Verdict::Linearizable => {}
                Verdict::Violation(why) => {
                    let file = cfg.root.join(format!(
                        "violation-epoch{epoch}-{}.txt",
                        key.replace('/', "_")
                    ));
                    let mut dump = format!("initial value: {initial:?}\n");
                    for o in &key_ops {
                        dump.push_str(&format!("{o}\n"));
                    }
                    let _ = write_file(&file, &dump);
                    lock(&findings).push(Finding::new(
                        Invariant::NotLinearizable,
                        epoch,
                        format!(
                            "{key}: {why}    full history of the ref this epoch: {}",
                            file.display()
                        ),
                    ));
                }
                Verdict::Inconclusive { visited } => lock(&findings).push(Finding::new(
                    Invariant::Inconclusive,
                    epoch,
                    format!(
                        "{key}: the search stopped after {visited} states ({} operations), so \
                         this ref's history was not checked",
                        key_ops.len()
                    ),
                )),
            }
            // Unknown pushes that could still land: the ref is still at
            // their base. They cannot have landed before the boundary read,
            // so they start there.
            let tip = tips.get(&key).cloned().flatten();
            let still_possible: Vec<Op> = key_ops
                .into_iter()
                .filter(|o| {
                    o.outcome == Outcome::Unknown
                        && matches!(&o.call, Call::Push { from, .. } if *from == tip)
                })
                .map(|mut o| {
                    o.invoked = o.invoked.max(boundary_start);
                    o
                })
                .collect();
            gen_state.carried.insert(key.clone(), still_possible);
        }
        let acked_now = ops
            .iter()
            .filter(|o| o.outcome == Outcome::Ok && matches!(o.call, Call::Push { .. }))
            .count();
        if acked_now == 0 {
            lock(&findings).push(Finding::new(
                Invariant::Vacuous,
                epoch,
                format!(
                    "no push was acknowledged this epoch ({} operations), so it checked nothing \
                     about writes",
                    ops.len()
                ),
            ));
        }
        gen_state.acked.extend(
            ops.iter()
                .filter(|o| o.outcome == Outcome::Ok && matches!(o.call, Call::Push { .. }))
                .cloned(),
        );
        let fetch_findings = checker.fetchability(&target, &tips, epoch, epoch % 2 == 1, &tally);
        let durable = checker.durability(&tips, &gen_state.acked, epoch);
        {
            let mut f = lock(&findings);
            f.extend(fetch_findings);
            f.extend(durable);
        }
        gen_state.initial = tips;
        if cfg.verbose {
            let pushes = ops
                .iter()
                .filter(|o| matches!(o.call, Call::Push { .. }))
                .count();
            eprintln!(
                "chaos {}: epoch {epoch} done at {:.0} s: {} ops ({pushes} pushes), {} violation(s) so far",
                cfg.scenario,
                started.elapsed().as_secs_f64(),
                ops.len(),
                lock(&findings).len()
            );
        }
        epoch += 1;
    }
    report.epochs = epoch;
    tally.drain_into(&mut report);
    if let Some(inj) = injector.as_ref() {
        if inj.injects() && faults_injected == 0 {
            report.violations.push(Finding::new(
                Invariant::Vacuous,
                usize::MAX,
                format!(
                    "scenario {} injected no fault in {epoch} epoch(s)",
                    cfg.scenario
                ),
            ));
        }
        let idle = inj.idle_parts();
        if !idle.is_empty() {
            report.violations.push(Finding::new(
                Invariant::Vacuous,
                usize::MAX,
                format!(
                    "scenario {}: these faults never injected anything: {}",
                    cfg.scenario,
                    idle.join(", ")
                ),
            ));
        }
    }
    if cfg.expect_packfile_uris && report.get(URIS_USED) == 0 {
        report.violations.push(Finding::new(
            Invariant::Vacuous,
            usize::MAX,
            format!(
                "no fetch received a packfile URI ({} fetches asked for them), so the URI path \
                 went unchecked",
                report.get(URI_FETCHES)
            ),
        ));
    }
    if let Some(d) = deployment {
        d.final_checks(&mut report);
        report.violations.extend(d.take_findings(usize::MAX));
    }
    let mut all = std::mem::take(&mut *lock(&findings));
    all.append(&mut report.violations);
    report.violations = all;
    let mut text = String::new();
    for op in history.ops() {
        text.push_str(&format!("{op}\n"));
    }
    write_file(&cfg.root.join("history.txt"), &text)?;
    report.seconds = started.elapsed().as_secs_f64();
    Ok(report)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn count_ops(report: &mut Report, ops: &[Op]) {
    for o in ops {
        let kind = match o.call {
            Call::Push { .. } => "push",
            Call::Read { .. } => "read",
        };
        report.count(format!("{kind} {:?}", o.outcome).to_lowercase(), 1);
    }
}

/// Creates repository number `g` and gives every ref its first commit,
/// with no faults active.
fn new_generation(
    server: &dyn GitServer,
    cfg: &RunConfig,
    g: usize,
    checker: &Checker,
) -> io::Result<Generation> {
    let dir = cfg.root.join(format!("repo-{g}"));
    let _ = std::fs::remove_dir_all(&dir);
    let repo_url = server.create_repo(&format!("chaos-{g}.git"))?;
    let keys = Keys::new(cfg.clients, cfg.shared_branches);
    let mut clients = Vec::new();
    for i in 0..cfg.clients {
        clients.push(Client::new(&dir, i, cfg.seed ^ ((g as u64) << 40))?);
    }
    // The first commit, pushed to every ref through its URL.
    let seed_home = dir.join("seed");
    let seed_repo = seed_home.join("repo");
    std::fs::create_dir_all(&seed_repo)?;
    run_git(
        &seed_home,
        &seed_repo,
        &["init", "-q", "--initial-branch=main"],
    )?;
    std::fs::write(seed_repo.join("README"), format!("chaos repository {g}\n"))?;
    run_git(&seed_home, &seed_repo, &["add", "README"])?;
    run_git(
        &seed_home,
        &seed_repo,
        &["commit", "-q", "-m", "chaos: first commit"],
    )?;
    let tip = rev_parse(&seed_home, &seed_repo, "HEAD")?;
    let target = Target {
        server,
        repo_url: repo_url.clone(),
        keys: keys.clone(),
        unscoped_branch_pushes: false,
    };
    let mut initial = BTreeMap::new();
    for key in keys.all() {
        let url = target.url_for(&key);
        let mut last = None;
        for _ in 0..10 {
            match run_git(
                &seed_home,
                &seed_repo,
                &["push", "-q", &url, &format!("{tip}:{key}")],
            ) {
                Ok(_) => {
                    last = None;
                    break;
                }
                Err(e) => {
                    last = Some(e);
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
        }
        if let Some(e) = last {
            return Err(io::Error::other(format!("seeding {key}: {e}")));
        }
        initial.insert(key, Some(tip.clone()));
    }
    checker.reset()?;
    Ok(Generation {
        repo_url,
        keys,
        clients,
        initial,
        carried: BTreeMap::new(),
        acked: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;

    fn until(holds: Arc<AtomicBool>, limit: Duration) -> ActiveUntil {
        ActiveUntil {
            what: "the flag was set".to_owned(),
            holds: Arc::new(move |_| holds.load(Ordering::SeqCst)),
            limit,
        }
    }

    #[test]
    fn a_plain_phase_ends_after_its_length() {
        let phase = ActivePhase::lasting(Duration::from_millis(100));
        assert!(!phase.over());
        assert!(phase.sleep(Duration::from_secs(10)));
        assert!(!phase.timed_out());
    }

    #[test]
    fn a_phase_waits_past_its_minimum_until_the_condition_holds() {
        let flag = Arc::new(AtomicBool::new(false));
        let u = until(flag.clone(), Duration::from_secs(60));
        let history = History::new();
        let phase = ActivePhase::new(Duration::from_millis(50), Some(&u), &history);
        // Past the minimum, but the condition does not hold yet.
        assert!(!phase.sleep(Duration::from_millis(200)));
        assert!(!phase.over());
        flag.store(true, Ordering::SeqCst);
        assert!(phase.over());
        assert!(!phase.timed_out());
        // Over stays over, even if the condition stops holding.
        flag.store(false, Ordering::SeqCst);
        assert!(phase.over());
    }

    #[test]
    fn a_condition_that_holds_early_does_not_cut_the_minimum() {
        let flag = Arc::new(AtomicBool::new(true));
        let u = until(flag, Duration::from_secs(60));
        let history = History::new();
        let phase = ActivePhase::new(Duration::from_millis(300), Some(&u), &history);
        assert!(!phase.over());
        assert!(!phase.sleep(Duration::from_millis(100)));
        assert!(phase.sleep(Duration::from_secs(10)));
        assert!(!phase.timed_out());
    }

    #[test]
    fn a_phase_whose_condition_never_holds_times_out_at_its_limit() {
        let u = until(Arc::new(AtomicBool::new(false)), Duration::from_millis(300));
        let started = Instant::now();
        let history = History::new();
        let phase = ActivePhase::new(Duration::from_millis(50), Some(&u), &history);
        assert!(phase.sleep(Duration::from_secs(10)));
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(phase.timed_out());
    }

    fn push(history: &History, outcome: Outcome) {
        let to = format!("{:040x}", history.len());
        history.record(
            0,
            "refs/heads/main",
            Call::Push { from: None, to },
            outcome,
            0,
            "",
        );
    }

    #[test]
    fn a_phase_can_wait_for_acknowledged_pushes_of_its_own() {
        let history = History::new();
        // Acknowledged before the phase began: not this phase's work.
        push(&history, Outcome::Ok);
        push(&history, Outcome::Ok);
        let u = ActiveUntil::acked_pushes(2, Duration::from_secs(60));
        let phase = ActivePhase::new(Duration::from_millis(50), Some(&u), &history);
        assert!(!phase.sleep(Duration::from_millis(200)));
        // Only an acknowledged push counts: not a read, a lost race, a
        // push with no effect or one whose outcome is unknown.
        history.record(
            0,
            "refs/heads/main",
            Call::Read { value: None },
            Outcome::Ok,
            0,
            "",
        );
        for outcome in [
            Outcome::Fail,
            Outcome::NoEffect,
            Outcome::Unknown,
            Outcome::Ok,
        ] {
            push(&history, outcome);
        }
        assert_eq!(phase.progress(), Progress { acked_pushes: 1 });
        assert!(!phase.over());
        push(&history, Outcome::Ok);
        assert_eq!(phase.progress(), Progress { acked_pushes: 2 });
        assert!(phase.over());
        assert!(!phase.timed_out());
    }

    #[test]
    fn a_phase_short_of_acknowledged_pushes_times_out() {
        let history = History::new();
        let u = ActiveUntil::acked_pushes(1, Duration::from_millis(300));
        let phase = ActivePhase::new(Duration::from_millis(50), Some(&u), &history);
        push(&history, Outcome::Unknown);
        assert!(phase.sleep(Duration::from_secs(10)));
        assert!(phase.timed_out());
        assert_eq!(u.what, "the server acknowledged 1 push(es) in the epoch");
    }
}

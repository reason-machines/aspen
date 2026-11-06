//! Storage under a mixed workload (issue #203), end to end with stock `git`.
//!
//! The chaos suite's `mixed` workload (D1) stored 60 to 120 times its
//! repository in the bucket and reclaimed nothing for at least 24 hours.
//! Two things made it grow:
//!
//! - a branch push through the plain repository URL was built by Git
//!   against trunk (the only advertised ref, S2), so it resent the branch's
//!   whole history, and receive-pack stored all of it again in every
//!   version's pack: a branch's storage grew with the square of its length;
//! - every `git fetch --depth=1` of a new tip stored an exact shallow pack
//!   (C8), and a superseded version's pack was kept for 24 hours after it
//!   was recorded plus a 24-hour grace period, and one garbage-collection
//!   pass evicted and deleted at most one batch.
//!
//! The tests here drive that workload pattern in miniature and check the
//! invariant: with passes every `gc_every`, out of step with every
//! deadline, everything no current ref needs is deleted within
//! `CompactConfig::reclaim_bound` (`shallow_ttl + gc_grace + 2 ×
//! gc_every`, 2 h 20 min by default) of when it stopped being needed, and
//! the bucket then holds a small multiple of the repository's packed size.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use aspen_auth::PolicyConfig;
use aspen_blob::LocalBlobStore;
use aspen_compact::{CompactConfig, Compactor, GcReport, JobOutcome, Worker, WorkerOptions};
use aspen_core::id::UnixMicros;
use aspen_meta::{Clock, SqlStore};
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::testing::TestServer;

/// The storage budget: the bucket may hold at most this many times the
/// repository's packed size once garbage collection has caught up. Level-0
/// packs and covers each hold about one copy of the history (2x), every
/// ref's current version keeps its exact shallow pack, and Aspen's packs
/// compress less well than `git repack -ad`, which measures the
/// repository. Measured: 3.56x to 3.61x with git 2.39.5 (the test prints
/// the git version and the ratio). The denominator depends on the client's
/// `repack`, so a git release that packs this history much tighter could
/// move it; that would show in the printed line before it fails.
const MAX_BUCKET_MULTIPLE: f64 = 4.0;

struct Env {
    tmp: tempfile::TempDir,
    clock: std::cell::Cell<i64>,
    rng: std::cell::Cell<u64>,
}

impl Env {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("home")).unwrap();
        Self {
            tmp,
            clock: std::cell::Cell::new(1_700_000_000),
            rng: std::cell::Cell::new(0x2030_0203),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.tmp.path().join(name)
    }

    fn git(&self, dir: &Path) -> Command {
        let mut c = Command::new("git");
        c.current_dir(dir)
            .env("HOME", self.path("home"))
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_AUTHOR_NAME", "Aspen Test")
            .env("GIT_AUTHOR_EMAIL", "test@aspen.invalid")
            .env("GIT_COMMITTER_NAME", "Aspen Test")
            .env("GIT_COMMITTER_EMAIL", "test@aspen.invalid")
            .env_remove("GIT_DIR")
            .env_remove("GIT_PROTOCOL")
            .args(["-c", "protocol.version=2", "-c", "init.defaultBranch=main"])
            .args(["-c", "gc.auto=0"]);
        c
    }

    fn next(&self) -> u64 {
        // SplitMix64, so the history is the same on every run.
        let mut z = self.rng.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        self.rng.set(z);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// One commit on `dir`'s HEAD, as the chaos clients make them: a line of
    /// a shared file changes (so packs get deltas), a small file of the
    /// commit's own is added, and now and then a larger random blob.
    fn commit(&self, dir: &Path, tag: &str, n: usize) -> String {
        let changed = self.next() % 200;
        let shared: String = (0..200)
            .map(|i| {
                if i == changed {
                    format!("line {i}: {tag}-{n}\n")
                } else {
                    format!("line {i}: the quick brown fox jumps over the lazy dog\n")
                }
            })
            .collect();
        fs::write(dir.join("shared.txt"), shared).unwrap();
        fs::create_dir_all(dir.join(tag)).unwrap();
        fs::write(dir.join(format!("{tag}/{n}.txt")), format!("{tag} {n}\n")).unwrap();
        if n.is_multiple_of(4) {
            let blob: String = (0..1024).map(|_| format!("{:016x}", self.next())).collect();
            fs::write(dir.join(format!("{tag}/blob-{n}.bin")), blob).unwrap();
        }
        run(self.git(dir).args(["add", "-A"]));
        let t = self.clock.get() + 1;
        self.clock.set(t);
        let date = format!("@{t} +0000");
        run(self
            .git(dir)
            .env("GIT_AUTHOR_DATE", &date)
            .env("GIT_COMMITTER_DATE", &date)
            .args(["commit", "--quiet", "-m", &format!("{tag} {n}")]));
        run(self.git(dir).args(["rev-parse", "HEAD"]))
            .trim()
            .to_owned()
    }

    /// A `git fetch --depth=1` of `tip` into a fresh repository, checked
    /// with `fsck --strict`.
    fn shallow_fetch(&self, url: &str, tip: &str, name: &str) {
        let d = self.path(name);
        fs::create_dir_all(&d).unwrap();
        run(self.git(&d).args(["init", "--quiet"]));
        run(self
            .git(&d)
            .args(["fetch", "--quiet", "--no-tags", "--depth=1", url, tip]));
        run(self.git(&d).args(["fsck", "--strict", "--no-progress"]));
        fs::remove_dir_all(&d).unwrap();
    }
}

fn run(cmd: &mut Command) -> String {
    let out = cmd.output().expect("spawn git");
    assert!(
        out.status.success(),
        "{cmd:?} failed with {}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn writable() -> TestServer {
    let policy = PolicyConfig {
        anonymous: vec!["write:*".parse().unwrap()],
        ..PolicyConfig::default()
    };
    TestServer::start_with_policy(policy, |_| {})
}

/// Bytes of every blob in the store (packs, `.idx` files and entry
/// tables), whether or not a row names it.
fn bucket_bytes(blobs: &LocalBlobStore) -> u64 {
    fn walk(p: &Path) -> u64 {
        fs::read_dir(p)
            .map(|rd| {
                rd.filter_map(Result::ok)
                    .map(|e| {
                        let m = e.metadata().unwrap();
                        if m.is_dir() { walk(&e.path()) } else { m.len() }
                    })
                    .sum()
            })
            .unwrap_or(0)
    }
    walk(blobs.root())
}

/// The repository's size: every ref fetched from Aspen into one bare
/// repository (checked with `fsck --strict`), packed with `git repack -ad`.
fn repo_bytes(env: &Env, url: &str, refs: &[&str]) -> u64 {
    let m = env.path("measure.git");
    run(env
        .git(env.tmp.path())
        .args(["init", "--quiet", "--bare", m.to_str().unwrap()]));
    for r in refs {
        run(env.git(&m).args([
            "fetch",
            "--quiet",
            "--no-tags",
            &format!("{url}/-/{r}"),
            &format!("+{r}:{r}"),
        ]));
    }
    run(env.git(&m).args(["fsck", "--strict", "--no-progress"]));
    run(env.git(&m).args(["repack", "-adq"]));
    let kib: u64 = run(env.git(&m).args(["count-objects", "-v"]))
        .lines()
        .find_map(|l| l.strip_prefix("size-pack: "))
        .unwrap()
        .parse()
        .unwrap();
    kib * 1024
}

/// A compactor on the server's stores, whose store clock runs `offset`
/// microseconds ahead of the system clock the server stamps rows with.
fn compactor(
    s: &TestServer,
    offset: &Arc<AtomicI64>,
    configure: impl FnOnce(&mut CompactConfig),
) -> Compactor<SqlStore, LocalBlobStore> {
    let spool = s.blobs.root().parent().unwrap().join("compact");
    fs::create_dir_all(&spool).unwrap();
    let mut config = CompactConfig::new(
        spool,
        MaxObjectBytes::new(512 << 20).unwrap(),
        MaxInflatedBytes::new(8 << 30).unwrap(),
    );
    configure(&mut config);
    let offset = Arc::clone(offset);
    let meta = s.meta.clone().with_clock(Clock::from_fn(move || {
        UnixMicros(UnixMicros::now().0 + offset.load(Ordering::SeqCst))
    }));
    Compactor::new(meta, s.blobs.clone(), config)
}

/// Runs every due job (compaction, commit indexing).
fn drain(s: &TestServer, offset: &Arc<AtomicI64>) {
    let mut worker = Worker::new(compactor(s, offset, |_| {}), WorkerOptions::new("test"));
    while let Some(outcome) = s.block_on(worker.run_one()).unwrap() {
        if let JobOutcome::Failed { error, .. } = outcome {
            panic!("a job failed: {error}");
        }
    }
}

fn micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap()
}

/// The default compaction settings and pass interval, and the bound they
/// state.
fn defaults() -> (CompactConfig, Duration, Duration) {
    let config = CompactConfig::new(
        "/nonexistent",
        MaxObjectBytes::new(1 << 20).unwrap(),
        MaxInflatedBytes::new(1 << 20).unwrap(),
    );
    let every = WorkerOptions::new("test").gc_every;
    let bound = config.reclaim_bound(every);
    assert_eq!(bound, Duration::from_secs(2 * 3600 + 20 * 60));
    (config, every, bound)
}

/// Garbage-collection passes every `every` from `first` while they are
/// within `until`, on a store clock moved by `offset` (nothing waits);
/// what they did in all, and when the last pass that deleted a pack ran.
fn passes(
    s: &TestServer,
    gc: &Compactor<SqlStore, LocalBlobStore>,
    offset: &Arc<AtomicI64>,
    first: Duration,
    every: Duration,
    until: Duration,
) -> (GcReport, Option<Duration>) {
    let mut total = GcReport::default();
    let mut deleted_at = None;
    let mut at = first;
    while at <= until {
        offset.store(micros(at), Ordering::SeqCst);
        let r = s.block_on(gc.collect_garbage()).unwrap();
        if !r.packs_deleted.is_empty() {
            deleted_at = Some(at);
        }
        assert!(r.blobs_left.is_empty(), "{:?}", r.blobs_left);
        assert!(!r.budget_spent, "{r:?}");
        total.shallow_evicted += r.shallow_evicted;
        total.packs_deleted.extend(r.packs_deleted);
        total.bytes_deleted += r.bytes_deleted;
        total.deletes_finished += r.deletes_finished;
        at += every;
    }
    (total, deleted_at)
}

fn git_version() -> String {
    run(Command::new("git").arg("--version")).trim().to_owned()
}

/// Invariant (issue #203): under the chaos `mixed` workload pattern (pushes
/// to trunk, to a branch through the plain repository URL and to a shared
/// branch through its scoped URL; a depth-1 fetch of each new tip, which
/// the next push supersedes; compaction), once garbage collection with the
/// default settings has run `shallow_ttl` and then `gc_grace` after the
/// last fetch, every superseded version's exact shallow pack and every
/// orphaned pack is deleted, blobs included, and the bucket holds at most
/// `MAX_BUCKET_MULTIPLE` times the repository's packed size. Before the
/// fix the plain-URL branch's packs alone held its history about
/// `ROUNDS / 2` times, and nothing was reclaimed for 48 hours.
#[test]
fn a_mixed_workload_keeps_the_bucket_a_small_multiple_of_the_repository() {
    const ROUNDS: usize = 34;
    let env = Env::new();
    let s = writable();
    let url = s.url("demo");
    let work = env.path("work");
    fs::create_dir_all(&work).unwrap();
    run(env.git(&work).args(["init", "--quiet", "-b", "main"]));
    env.commit(&work, "seed", 0);
    let _ = s.seeder("demo", "refs/heads/main", &work);
    for branch in ["agent", "shared"] {
        run(env.git(&work).args(["branch", branch, "main"]));
        run(env.git(&work).args([
            "push",
            "--quiet",
            &format!("{url}/-/refs/heads/{branch}"),
            &format!("{branch}:refs/heads/{branch}"),
        ]));
    }
    let offset = Arc::new(AtomicI64::new(0));
    for n in 1..=ROUNDS {
        for (branch, push_url) in [
            ("main", url.clone()),
            // The plain repository URL: the client sends a zero old value
            // and only knows trunk, so it resends the branch's history.
            ("agent", url.clone()),
            ("shared", format!("{url}/-/refs/heads/shared")),
        ] {
            run(env.git(&work).args(["checkout", "--quiet", branch]));
            let tip = env.commit(&work, branch, n);
            run(env
                .git(&work)
                .args(["push", "--quiet", &push_url])
                .arg(format!("{tip}:refs/heads/{branch}")));
            env.shallow_fetch(&url, &tip, &format!("shallow-{branch}-{n}"));
        }
        if n.is_multiple_of(8) {
            drain(&s, &offset);
        }
    }
    drain(&s, &offset);
    assert!(
        s.metrics().shallow_builds_recorded() >= ROUNDS as u64,
        "the fetches stored exact shallow packs"
    );
    let before = bucket_bytes(&s.blobs);

    // Garbage collection with the defaults, a pass every
    // `WorkerOptions::gc_every` as the worker runs them, until the bound
    // after the last fetch. The passes are out of step with the deadlines:
    // the first runs a second before the last recording's `shallow_ttl`
    // is up, so it misses it, and the eviction (which restarts the pack's
    // grace period) and the sweep each wait most of a pass interval, the
    // worst case the bound allows for.
    let (config, every, bound) = defaults();
    let gc = compactor(&s, &offset, |_| {});
    let first = config.shallow_ttl - Duration::from_secs(1);
    let (total, _) = passes(&s, &gc, &offset, first, every, bound);
    let after = bucket_bytes(&s.blobs);
    let repo = repo_bytes(
        &env,
        &url,
        &["refs/heads/main", "refs/heads/agent", "refs/heads/shared"],
    );
    let multiple = after as f64 / repo as f64;
    // How many of each branch's versions are `Wider`: a plain-URL push of
    // a branch that does not contain trunk's tip is `Wider` through its
    // trunk dependency whatever else it depends on.
    let wider = |branch: &str| -> (usize, usize) {
        use aspen_core::meta::MetaStore;
        use aspen_core::refs::Closure;
        let repo = s
            .block_on(
                s.meta
                    .repo(&aspen_core::name::RepoName::new("demo").unwrap()),
            )
            .unwrap()
            .unwrap();
        let name = aspen_core::name::RefName::new(format!("refs/heads/{branch}")).unwrap();
        let mut v = s.block_on(s.meta.current_version(repo.id, &name)).unwrap();
        let (mut n, mut wide) = (0, 0);
        while let Some(version) = v {
            n += 1;
            wide += usize::from(version.value.closure() == Some(Closure::Wider));
            v = version
                .parent
                .and_then(|p| s.block_on(s.meta.ref_version(p)).unwrap());
        }
        (wide, n)
    };
    for branch in ["agent", "shared"] {
        let (wide, n) = wider(branch);
        println!("{branch}: {wide} of {n} versions wider");
    }
    let git = git_version();
    println!(
        "{git}: bucket {before} bytes before garbage collection, {after} after passes \
         every {every:?} up to {bound:?} (evicted {} recordings, deleted {} packs, {} bytes); \
         repository {repo} bytes; {multiple:.2}x",
        total.shallow_evicted,
        total.packs_deleted.len(),
        total.bytes_deleted
    );
    // 3 refs: each keeps the recording of its current version only.
    assert_eq!(
        total.shallow_evicted,
        s.metrics().shallow_builds_recorded() - 3,
        "every superseded version's exact shallow pack is evicted within the bound"
    );
    assert!(
        total.packs_deleted.len() >= total.shallow_evicted as usize,
        "and swept"
    );
    assert!(
        multiple <= MAX_BUCKET_MULTIPLE,
        "{git}: the bucket holds {multiple:.2} times the repository ({after} against {repo} \
         bytes), over the {MAX_BUCKET_MULTIPLE}x budget"
    );

    // Everything still fetches: a fresh clone of each ref, and a depth-1
    // fetch of trunk's current tip (its recording stayed).
    let tip = run(env.git(&work).args(["rev-parse", "main"]))
        .trim()
        .to_owned();
    env.shallow_fetch(&url, &tip, "shallow-after-gc");
}

/// Invariant (issue #203): one garbage-collection pass evicts and deletes
/// everything that is due, not one batch of it. Before the fix a pass
/// evicted at most `gc_batch` recordings and swept at most `gc_batch`
/// packs, so a repository whose tips moved faster than one batch per pass
/// fell further behind with every pass.
#[test]
fn one_pass_collects_more_than_one_batch() {
    const VERSIONS: usize = 7;
    let env = Env::new();
    let s = writable();
    let url = s.url("demo");
    let work = env.path("work");
    fs::create_dir_all(&work).unwrap();
    run(env.git(&work).args(["init", "--quiet", "-b", "main"]));
    env.commit(&work, "seed", 0);
    let _ = s.seeder("demo", "refs/heads/main", &work);
    for n in 1..=VERSIONS {
        let tip = env.commit(&work, "main", n);
        run(env
            .git(&work)
            .args(["push", "--quiet", &url])
            .arg(format!("{tip}:refs/heads/main")));
        env.shallow_fetch(&url, &tip, &format!("shallow-{n}"));
    }
    assert_eq!(s.metrics().shallow_builds_recorded(), VERSIONS as u64);
    // Each version after the first was built from the previous one's
    // recorded pack and a patch pack (a layered answer), which nothing names
    // once its build is recorded: one more orphan pack per such build.
    let patches = s.metrics().shallow_builds_incremental() as usize;
    let offset = Arc::new(AtomicI64::new(0));
    let gc = compactor(&s, &offset, |c| {
        c.gc_batch = 2;
        c.shallow_ttl = Duration::ZERO;
        c.gc_grace = Duration::ZERO;
    });
    let before = bucket_bytes(&s.blobs);
    let report = s.block_on(gc.collect_garbage()).unwrap();
    assert_eq!(
        report.shallow_evicted,
        VERSIONS as u64 - 1,
        "every superseded recording, in one pass"
    );
    assert_eq!(
        report.packs_deleted.len(),
        VERSIONS - 1 + patches,
        "and every pack, the orphan patch packs included"
    );
    assert_eq!(report.deletes_finished, VERSIONS - 1 + patches);
    assert!(bucket_bytes(&s.blobs) < before);
    let again = s.block_on(gc.collect_garbage()).unwrap();
    assert_eq!(again.shallow_evicted, 0, "the current version's stays");
    assert!(again.packs_deleted.is_empty());
}

/// Invariant (issue #203 review): a superseded version's exact shallow
/// pack is deleted, blobs included, within
/// `CompactConfig::reclaim_bound(gc_every)` (`shallow_ttl + gc_grace + 2 ×
/// gc_every`) of being superseded, whatever the passes' phase against the
/// deadlines, and not before about `shallow_ttl + gc_grace` (the
/// recording is made seconds before the version is superseded, so the
/// check allows 30 s). Passes come a second less than `gc_every` apart,
/// so they drift against the grace period (a whole number of intervals)
/// instead of landing on its end, and the phases put the first pass just
/// before, just after and halfway between the deadlines. With the first
/// phase the eviction just misses one pass and the sweep just misses
/// another, so deletion comes nearly two intervals after `shallow_ttl +
/// gc_grace`: a bound of one interval (the claim this replaces) fails.
#[test]
fn a_superseded_shallow_pack_is_deleted_within_the_bound_whatever_the_phase() {
    let (config, every, bound) = defaults();
    let not_before = config.shallow_ttl + config.gc_grace;
    // Passes 1 s under `gc_every` drift against the deadlines; passes at
    // exactly `gc_every` with the first one just before `shallow_ttl` are
    // the bound's worst case: eviction comes almost one interval late and
    // the sweep up to one more.
    let drift = every - Duration::from_secs(1);
    for (phase, step) in [
        (config.shallow_ttl - Duration::from_secs(10), drift),
        (config.shallow_ttl + Duration::from_secs(1), drift),
        (config.shallow_ttl + every / 2, drift),
        (config.shallow_ttl - Duration::from_secs(1), every),
    ] {
        let env = Env::new();
        let s = writable();
        let url = s.url("demo");
        let work = env.path("work");
        fs::create_dir_all(&work).unwrap();
        run(env.git(&work).args(["init", "--quiet", "-b", "main"]));
        env.commit(&work, "seed", 0);
        let _ = s.seeder("demo", "refs/heads/main", &work);
        let tip = env.commit(&work, "main", 1);
        run(env
            .git(&work)
            .args(["push", "--quiet", &url])
            .arg(format!("{tip}:refs/heads/main")));
        env.shallow_fetch(&url, &tip, "shallow-1");
        let tip = env.commit(&work, "main", 2);
        run(env
            .git(&work)
            .args(["push", "--quiet", &url])
            .arg(format!("{tip}:refs/heads/main")));
        // Superseded now (the store clock's zero), recorded just before.
        let offset = Arc::new(AtomicI64::new(0));
        let gc = compactor(&s, &offset, |_| {});
        let (early, _) = passes(
            &s,
            &gc,
            &offset,
            phase,
            step,
            not_before - Duration::from_secs(30),
        );
        assert!(
            early.packs_deleted.is_empty(),
            "phase {phase:?}: nothing deleted before shallow_ttl + gc_grace: {early:?}"
        );
        let mut next = phase;
        while next <= not_before - Duration::from_secs(30) {
            next += step;
        }
        let (late, deleted_at) = passes(&s, &gc, &offset, next, step, bound);
        let one_interval = not_before + every;
        println!(
            "phase {phase:?}, passes every {step:?}: deleted by the pass at {:?} (bound {bound:?}, one interval would be {one_interval:?})",
            deleted_at
        );
        assert_eq!(
            early.shallow_evicted + late.shallow_evicted,
            1,
            "phase {phase:?}"
        );
        assert_eq!(late.packs_deleted.len(), 1, "phase {phase:?}: {late:?}");
        assert_eq!(late.deletes_finished, 1, "phase {phase:?}: blobs deleted");
    }
}

/// Invariant (issue #203 review): a pass past its time budget stops
/// repeating its steps, but every step still runs one batch, so the next
/// pass continues where it stopped and nothing starves.
#[test]
fn a_pass_past_its_budget_runs_one_batch_per_step() {
    const VERSIONS: usize = 7;
    let env = Env::new();
    let s = writable();
    let url = s.url("demo");
    let work = env.path("work");
    fs::create_dir_all(&work).unwrap();
    run(env.git(&work).args(["init", "--quiet", "-b", "main"]));
    env.commit(&work, "seed", 0);
    let _ = s.seeder("demo", "refs/heads/main", &work);
    for n in 1..=VERSIONS {
        let tip = env.commit(&work, "main", n);
        run(env
            .git(&work)
            .args(["push", "--quiet", &url])
            .arg(format!("{tip}:refs/heads/main")));
        env.shallow_fetch(&url, &tip, &format!("shallow-{n}"));
    }
    // Orphan patch packs of incremental builds (see
    // `one_pass_collects_more_than_one_batch`).
    let patches = s.metrics().shallow_builds_incremental() as usize;
    let offset = Arc::new(AtomicI64::new(0));
    let gc = compactor(&s, &offset, |c| {
        c.gc_batch = 2;
        c.shallow_ttl = Duration::ZERO;
        c.gc_grace = Duration::ZERO;
        c.gc_pass_budget = Duration::ZERO;
    });
    let first = s.block_on(gc.collect_garbage()).unwrap();
    assert!(first.budget_spent, "{first:?}");
    assert_eq!(first.shallow_evicted, 2, "one batch: {first:?}");
    assert!(
        first.packs_deleted.len() >= 2,
        "the sweep still ran: {first:?}"
    );
    assert_eq!(
        first.deletes_finished, 2,
        "and the blob deletions: {first:?}"
    );
    let mut evicted = first.shallow_evicted;
    let mut deleted = first.packs_deleted.len();
    for _ in 0..VERSIONS {
        let r = s.block_on(gc.collect_garbage()).unwrap();
        evicted += r.shallow_evicted;
        deleted += r.packs_deleted.len();
    }
    assert_eq!(evicted, VERSIONS as u64 - 1, "later passes finish the work");
    assert_eq!(deleted, VERSIONS - 1 + patches);
}

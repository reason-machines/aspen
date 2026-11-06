//! Exact cached shallow packs (node C8), end to end with stock `git`.
//!
//! Every shallow fetch here is differential: the same command runs against
//! stock `git upload-pack` (over `file://`) and against Aspen, and the two
//! clones must have the same `.git/shallow`, the same tip, and pass
//! `git fsck --strict`. The storm test is the plan's depth-1 fetch storm in
//! miniature: bytes served per fetch must be at most 1.2 times stock Git's.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use aspen_auth::PolicyConfig;
use aspen_compact::{CompactConfig, Compactor, ShallowBuild, ShallowSpec};
use aspen_core::id::ObjectId;
use aspen_core::meta::{MetaStore, ShallowDepth};
use aspen_core::name::{RefName, RepoName};
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::ServerConfig;
use aspen_server::testing::{Push, TestServer};
use aspen_testkit::NO_AUTO_MAINTENANCE;

/// A scratch area with an isolated `HOME` for every git command.
struct Env {
    tmp: tempfile::TempDir,
    clock: std::cell::Cell<i64>,
}

impl Env {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("home")).unwrap();
        Self {
            tmp,
            clock: std::cell::Cell::new(1_700_000_000),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.tmp.path().join(name)
    }

    fn git(&self, dir: &Path) -> Command {
        git_in(&self.path("home"), dir)
    }

    /// A non-bare repository to make history in. Stock `upload-pack`
    /// serves any commit by ID, as Aspen does.
    fn source(&self) -> PathBuf {
        let src = self.path("src");
        fs::create_dir_all(&src).unwrap();
        run(self.git(&src).args(["init", "--quiet", "-b", "main"]));
        run(self
            .git(&src)
            .args(["config", "uploadpack.allowAnySHA1InWant", "true"]));
        src
    }

    /// Writes `files` (path, contents) and commits them with the next clock
    /// tick.
    fn commit(&self, dir: &Path, files: &[(String, String)], message: &str) -> String {
        for (path, contents) in files {
            let p = dir.join(path);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, contents).unwrap();
            run(self.git(dir).args(["add", path]));
        }
        let t = self.clock.get() + 1000;
        self.clock.set(t);
        let date = format!("@{t} +0000");
        run(self
            .git(dir)
            .env("GIT_AUTHOR_DATE", &date)
            .env("GIT_COMMITTER_DATE", &date)
            .args(["commit", "--quiet", "-m", message]));
        rev(self, dir, "HEAD")
    }
}

fn git_in(home: &Path, dir: &Path) -> Command {
    let mut c = Command::new("git");
    c.current_dir(dir)
        .env("HOME", home)
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
        // No background repack while `fsck` reads the pack directory (#207).
        .args(NO_AUTO_MAINTENANCE);
    c
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

fn rev(env: &Env, dir: &Path, r: &str) -> String {
    run(env.git(dir).args(["rev-parse", r])).trim().to_owned()
}

/// The sorted `.git/shallow` of a repository (empty when not shallow).
fn shallow_file(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_to_string(dir.join(".git/shallow"))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect();
    v.sort();
    v
}

/// Bytes of the packs a repository received.
fn pack_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir.join(".git/objects/pack"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "pack"))
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

/// A file of `n` numbered lines, distinct per `tag`.
fn lines(n: usize, tag: &str) -> String {
    (0..n)
        .map(|i| format!("line {i} of {tag}: some text that compresses like source code\n"))
        .collect()
}

/// A history where the full closure is much larger than one snapshot: 24
/// files in three directories, then `commits` commits that each rewrite
/// four of them. Returns the source and every commit, oldest first.
fn long_history(env: &Env, commits: usize) -> (PathBuf, Vec<String>) {
    let src = env.source();
    let mut all = Vec::new();
    let initial: Vec<(String, String)> = (0..24)
        .map(|i| {
            (
                format!("dir{}/file{i}.txt", i % 3),
                lines(150, &format!("f{i} v0")),
            )
        })
        .collect();
    all.push(env.commit(&src, &initial, "initial"));
    for c in 1..=commits {
        let files: Vec<(String, String)> = (0..4)
            .map(|k| {
                let i = (c * 5 + k * 7) % 24;
                (
                    format!("dir{}/file{i}.txt", i % 3),
                    lines(150 + c, &format!("f{i} v{c}")),
                )
            })
            .collect();
        all.push(env.commit(&src, &files, &format!("change {c}")));
    }
    (src, all)
}

fn writable(configure: impl FnOnce(&mut ServerConfig)) -> TestServer {
    let policy = PolicyConfig {
        anonymous: vec!["write:*".parse().unwrap()],
        ..PolicyConfig::default()
    };
    TestServer::start_with_policy(policy, configure)
}

/// Pushes every commit of `commits` to the server's `demo` trunk with stock
/// `git push`, one push per commit, as a busy trunk receives them.
fn push_each(env: &Env, s: &TestServer, src: &Path, commits: &[String]) {
    let _ = s.seeder("demo", "refs/heads/main", src);
    for c in commits {
        run(env
            .git(src)
            .args(["push", "--quiet", &s.url("demo")])
            .arg(format!("{c}:refs/heads/main")));
    }
}

/// `jobs` concurrent `git fetch --depth=<depth> <url> <tip>` into fresh,
/// empty repositories (the `actions/checkout` pattern). Checks each one
/// (tip, `fsck --strict`) and returns their directories.
fn storm(env: &Env, name: &str, url: &str, tip: &str, depth: u32, jobs: usize) -> Vec<PathBuf> {
    let dirs: Vec<PathBuf> = (0..jobs)
        .map(|j| {
            let d = env.path(&format!("{name}-{j}"));
            fs::create_dir_all(&d).unwrap();
            run(env.git(&d).args(["init", "--quiet"]));
            d
        })
        .collect();
    let home = env.path("home");
    std::thread::scope(|scope| {
        let handles: Vec<_> = dirs
            .iter()
            .map(|d| {
                let home = home.clone();
                scope.spawn(move || {
                    // Keep what arrives as a pack, however small, so its
                    // size is the bytes served.
                    // `fetch.fsckObjects`: index-pack checks every link of
                    // every object received, as CI fleets that turn it on
                    // do; a pack that names an object it does not send
                    // fails here, not only in a later `fsck`.
                    run(git_in(&home, d)
                        .args(["-c", "fetch.unpackLimit=1"])
                        .args(["-c", "fetch.fsckObjects=true"])
                        .args(["fetch", "--quiet", "--no-tags"])
                        .arg(format!("--depth={depth}"))
                        .arg(url)
                        .arg(format!("+{tip}:refs/remotes/origin/main")));
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    });
    for d in &dirs {
        assert_eq!(rev(env, d, "refs/remotes/origin/main"), tip);
        run(env.git(d).args(["fsck", "--strict", "--no-progress"]));
    }
    dirs
}

/// Every object a repository holds, sorted.
fn objects(env: &Env, dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = run(env.git(dir).args([
        "cat-file",
        "--batch-all-objects",
        "--batch-check=%(objectname)",
    ]))
    .lines()
    .map(str::to_owned)
    .collect();
    v.sort();
    v
}

/// Average received pack bytes over `dirs`.
fn per_fetch(dirs: &[PathBuf]) -> u64 {
    dirs.iter().map(|d| pack_bytes(d)).sum::<u64>() / dirs.len() as u64
}

/// Invariant (C8 done criterion, in miniature): a depth-1 fetch storm of a
/// just-pushed trunk tip is served exact shallow packs. Every fetch gets the
/// same boundary as stock `git upload-pack` and passes `fsck --strict`;
/// bytes served per fetch are at most 1.2 times stock Git's; the pack is
/// built once for the whole storm (single flight), and the storm needs no
/// full-closure fallback. The same storm against a server without exact
/// packs sends far more, so the bound is not met by accident.
#[test]
fn a_depth_one_fetch_storm_is_served_exact_packs_within_stock_bytes() {
    let env = Env::new();
    let (src, commits) = long_history(&env, 30);
    let tip = commits.last().unwrap().clone();
    let jobs = 8;

    let stock = storm(
        &env,
        "stock",
        &format!("file://{}", src.display()),
        &tip,
        1,
        jobs,
    );

    let s = writable(|_| {});
    push_each(&env, &s, &src, &commits);
    let aspen = storm(&env, "aspen", &s.url("demo"), &tip, 1, jobs);
    for (a, b) in aspen.iter().zip(&stock) {
        assert_eq!(shallow_file(a), shallow_file(b));
        assert_eq!(shallow_file(a), vec![tip.clone()]);
        // Exact: the objects stock Git sends, no more and no fewer.
        assert_eq!(objects(&env, a), objects(&env, b));
    }
    let m = s.metrics();
    assert_eq!(m.shallow_builds_recorded(), 1, "one build for the storm");
    assert_eq!(m.shallow_builds_not_recorded(), 0);
    assert_eq!(m.shallow_cached() + m.shallow_built(), jobs as u64);
    assert_eq!(m.shallow_fallbacks(), 0);

    // Over-fetch, for contrast: the same storm, no exact packs.
    let env2 = Env::new();
    let (src2, commits2) = long_history(&env2, 30);
    let s2 = writable(|c| c.shallow_cache = None);
    push_each(&env2, &s2, &src2, &commits2);
    let full = storm(
        &env2,
        "full",
        &s2.url("demo"),
        commits2.last().unwrap(),
        1,
        2,
    );

    let (stock_b, aspen_b, full_b) = (per_fetch(&stock), per_fetch(&aspen), per_fetch(&full));
    eprintln!(
        "depth-1 storm, bytes per fetch: stock {stock_b}, aspen exact {aspen_b} ({:.3}x), \
         aspen full closure {full_b} ({:.3}x)",
        aspen_b as f64 / stock_b as f64,
        full_b as f64 / stock_b as f64
    );
    assert!(stock_b > 0 && aspen_b > 0, "every fetch keeps its pack");
    assert!(
        aspen_b * 10 <= stock_b * 12,
        "exact packs: {aspen_b} bytes per fetch against stock's {stock_b}"
    );
    assert!(
        full_b > stock_b * 2,
        "the fixture must make the full closure costly: {full_b} against {stock_b}"
    );
}

/// Runs the same commands against stock (file://) and Aspen; the two must
/// agree on the shallow file and the tip, and pass `fsck --strict`.
fn same_as_stock(env: &Env, src: &Path, s: &TestServer, name: &str, steps: &[&[&str]]) {
    compare_with_stock(env, src, s, name, steps, false);
}

/// [`same_as_stock`] for a clone served exactly: Aspen must also send the
/// very objects stock Git sends.
fn exactly_as_stock(env: &Env, src: &Path, s: &TestServer, name: &str, steps: &[&[&str]]) {
    compare_with_stock(env, src, s, name, steps, true);
}

fn compare_with_stock(
    env: &Env,
    src: &Path,
    s: &TestServer,
    name: &str,
    steps: &[&[&str]],
    exact: bool,
) {
    let stock = env.path(&format!("{name}-stock"));
    let aspen = env.path(&format!("{name}-aspen"));
    for (url, dir) in [
        (format!("file://{}", src.display()), &stock),
        (s.url("demo"), &aspen),
    ] {
        let (first, rest) = steps.split_first().unwrap();
        run(env
            .git(env.tmp.path())
            .args(["clone", "--quiet", "--no-local"])
            .args(*first)
            .arg(&url)
            .arg(dir));
        for step in rest {
            run(env.git(dir).args(*step));
        }
        run(env.git(dir).args(["fsck", "--strict", "--no-progress"]));
    }
    assert_eq!(shallow_file(&aspen), shallow_file(&stock), "{name}");
    assert_eq!(rev(env, &aspen, "HEAD"), rev(env, &stock, "HEAD"), "{name}");
    if exact {
        assert_eq!(
            objects(env, &aspen),
            objects(env, &stock),
            "{name}: objects"
        );
    }
}

/// A trunk with a merge, a second branch and an annotated tag, seeded one
/// version per trunk commit.
fn branchy(configure: impl FnOnce(&mut ServerConfig)) -> (Env, PathBuf, TestServer, Vec<String>) {
    let env = Env::new();
    let src = env.source();
    let mut trunk = Vec::new();
    for i in 0..3 {
        trunk.push(env.commit(
            &src,
            &[("a/file.txt".into(), lines(40 + i, &format!("m{i}")))],
            "m",
        ));
    }
    run(env
        .git(&src)
        .args(["switch", "--quiet", "-c", "side", &trunk[0]]));
    env.commit(&src, &[("b/side.txt".into(), lines(30, "s1"))], "s1");
    env.commit(&src, &[("b/side.txt".into(), lines(31, "s2"))], "s2");
    run(env.git(&src).args(["switch", "--quiet", "main"]));
    let t = env.clock.get() + 1000;
    env.clock.set(t);
    run(env
        .git(&src)
        .env("GIT_COMMITTER_DATE", format!("@{t} +0000"))
        .env("GIT_AUTHOR_DATE", format!("@{t} +0000"))
        .args(["merge", "--quiet", "--no-ff", "-m", "merge side", "side"]));
    trunk.push(rev(&env, &src, "HEAD"));
    for i in 0..3 {
        trunk.push(env.commit(
            &src,
            &[("a/file.txt".into(), lines(60 + i, &format!("n{i}")))],
            "n",
        ));
    }
    run(env.git(&src).args(["branch", "old", &trunk[1]]));
    // Aspen gets main, old and v1 only; stock must list the same refs.
    run(env.git(&src).args(["branch", "-D", "side"]));
    run(env
        .git(&src)
        .args(["tag", "-a", "-m", "release", "v1", &trunk[4]]));
    let s = TestServer::start(configure);
    let seed = s.seeder("demo", "refs/heads/main", &src);
    for t in &trunk {
        s.push(&seed, &Push::new("refs/heads/main", t));
    }
    s.push(&seed, &Push::new("refs/heads/old", &trunk[1]));
    s.push(&seed, &Push::new("refs/tags/v1", "v1"));
    (env, src, s, trunk)
}

/// Invariant: a request with several wants (a clone of every branch and
/// tag) is served the union of their exact packs, and gets stock Git's
/// boundary; an annotated tag want is served its tag ref's pack; v0 and v1
/// clients are served exact packs too; a want whose region is its whole
/// closure (a depth past the root) records nothing and is sent that
/// closure, which is exact, without being built again.
#[test]
fn several_wants_tags_and_v0_are_served_exact_packs() {
    let (env, src, s, _) = branchy(|_| {});
    exactly_as_stock(
        &env,
        &src,
        &s,
        "all-branches",
        &[&["--depth=2", "--no-single-branch"]],
    );
    let m = s.metrics();
    assert_eq!(m.shallow_fallbacks(), 0);
    // main and v1 get packs; old (trunk's second commit, two commits deep)
    // is its whole closure, which is sent as it is.
    assert_eq!(m.shallow_builds_recorded(), 2, "main and v1");
    assert_eq!(m.shallow_builds_not_recorded(), 1, "old: not needed");
    exactly_as_stock(&env, &src, &s, "tag", &[&["--depth=1", "--branch=v1"]]);
    let before = m.shallow_cached();
    exactly_as_stock(
        &env,
        &src,
        &s,
        "v0",
        &[&["-c", "protocol.version=0", "--depth=2"]],
    );
    assert!(
        m.shallow_cached() > before,
        "the v0 clone reused main's pack"
    );
    let recorded = m.shallow_builds_recorded();
    same_as_stock(&env, &src, &s, "past-root", &[&["--depth=50"]]);
    same_as_stock(&env, &src, &s, "past-root-again", &[&["--depth=50"]]);
    assert_eq!(m.shallow_builds_recorded(), recorded);
    assert_eq!(
        m.shallow_builds_not_recorded(),
        2,
        "built once, then remembered"
    );
    assert_eq!(m.shallow_closure(), 2);
    assert_eq!(m.shallow_fallbacks(), 0);
}

/// Invariant: requests exact packs cannot serve get the full closure, with
/// stock Git's boundary: a want that is not a version's tip, deepen forms
/// other than a plain depth (`--deepen`, `--shallow-since`,
/// `--shallow-exclude`, `--unshallow`), and depths over the limit.
#[test]
fn other_shallow_requests_get_the_full_closure() {
    let (env, src, s, trunk) = branchy(|c| {
        c.shallow_cache.as_mut().unwrap().max_depth = 3;
    });
    let m = s.metrics();
    // A side-branch commit by ID: pushed inside the merge's version, whose
    // tip is the merge.
    let non_tip = rev(&env, &src, &format!("{}^2~1", trunk[3]));
    let dir = env.path("non-tip");
    fs::create_dir_all(&dir).unwrap();
    run(env.git(&dir).args(["init", "--quiet"]));
    run(env
        .git(&dir)
        .args(["fetch", "--quiet", "--depth=1", &s.url("demo")])
        .arg(format!("{non_tip}:refs/heads/x")));
    run(env.git(&dir).args(["fsck", "--strict", "--no-progress"]));
    assert_eq!(shallow_file(&dir), vec![non_tip]);
    assert_eq!(m.shallow_fallbacks(), 1, "not a tip");
    assert_eq!(m.shallow_builds_recorded(), 0);
    // The tip of that same version (the merge) at the same depth gets the
    // merge's own pack: nothing the non-tip fetch did is recorded for it.
    let merge = env.path("merge-tip");
    fs::create_dir_all(&merge).unwrap();
    run(env.git(&merge).args(["init", "--quiet"]));
    run(env
        .git(&merge)
        .args(["fetch", "--quiet", "--depth=1", &s.url("demo")])
        .arg(format!("{}:refs/heads/x", trunk[3])));
    run(env.git(&merge).args(["fsck", "--strict", "--no-progress"]));
    assert_eq!(shallow_file(&merge), vec![trunk[3].clone()]);
    assert_eq!(m.shallow_builds_recorded(), 1);

    same_as_stock(
        &env,
        &src,
        &s,
        "deepen",
        &[&["--depth=1"], &["fetch", "--quiet", "--deepen=2"]],
    );
    let since = format!("--shallow-since={}", env.clock.get() - 2500);
    same_as_stock(&env, &src, &s, "since", &[&[since.as_str()]]);
    same_as_stock(&env, &src, &s, "exclude", &[&["--shallow-exclude=old"]]);
    same_as_stock(
        &env,
        &src,
        &s,
        "unshallow",
        &[&["--depth=1"], &["fetch", "--quiet", "--unshallow"]],
    );
    same_as_stock(&env, &src, &s, "too-deep", &[&["--depth=4"]]);
    // Besides the merge, only the two `--depth=1` clones of main were
    // eligible, and they share one pack.
    assert_eq!(m.shallow_builds_recorded(), 2);
    assert_eq!(m.shallow_fallbacks(), 1);
}

/// Invariant: a build that records nothing serves the full closure, and the
/// same version and depth is not built again until `retry_after` passes.
#[test]
fn a_build_that_records_nothing_falls_back_and_is_not_retried_at_once() {
    let (env, src, s, _) = branchy(|c| {
        c.shallow_cache.as_mut().unwrap().max_source_bytes = 1;
    });
    let m = s.metrics();
    same_as_stock(&env, &src, &s, "first", &[&["--depth=1"]]);
    assert_eq!(m.shallow_builds_not_recorded(), 1, "too large");
    assert_eq!(m.shallow_fallbacks(), 1);
    same_as_stock(&env, &src, &s, "second", &[&["--depth=1"]]);
    assert_eq!(m.shallow_builds_not_recorded(), 1, "no second build");
    assert_eq!(m.shallow_fallbacks(), 2);
    assert_eq!(m.shallow_builds_recorded(), 0);
}

/// Invariant: a request that stops waiting for a build is served the full
/// closure while the build goes on; once it is recorded, later requests
/// are served the pack without building.
#[test]
fn a_build_slower_than_the_wait_finishes_for_later_fetches() {
    let (env, src, s, trunk) = branchy(|c| {
        c.shallow_cache.as_mut().unwrap().build_wait = Duration::ZERO;
    });
    let m = s.metrics();
    same_as_stock(&env, &src, &s, "first", &[&["--depth=1"]]);
    assert_eq!(m.shallow_fallbacks(), 1, "did not wait");
    let deadline = Instant::now() + Duration::from_secs(60);
    while m.shallow_builds_recorded() == 0 {
        assert!(Instant::now() < deadline, "the build never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
    let repo = s
        .block_on(s.meta.repo(&RepoName::new("demo").unwrap()))
        .unwrap()
        .unwrap();
    let main = s
        .block_on(
            s.meta
                .current_version(repo.id, &RefName::new("refs/heads/main").unwrap()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(main.value.tip().unwrap().to_hex(), trunk[6]);
    let one = ShallowDepth::new(1).unwrap();
    assert!(
        s.block_on(s.meta.shallow_pack(repo.id, main.id, one))
            .unwrap()
            .is_some()
    );
    same_as_stock(&env, &src, &s, "second", &[&["--depth=1"]]);
    assert_eq!(m.shallow_cached(), 1);
    assert_eq!(m.shallow_builds_recorded(), 1);
}

/// Invariant: garbage collection evicts the recording of a version that is
/// no longer current (never a current one), the sweep then deletes its
/// pack and blobs (and the patch pack of the layered answer the second
/// build started from, which nothing names), and a later depth-1 fetch of
/// that old tip builds the pack again (the checksum's next generation) and
/// is served correctly.
#[test]
fn evicted_packs_are_swept_and_built_again() {
    let env = Env::new();
    let (src, commits) = long_history(&env, 4);
    let s = writable(|_| {});
    push_each(&env, &s, &src, &commits[..4]);
    let old = commits[3].clone();
    storm(&env, "old", &s.url("demo"), &old, 1, 1);
    push_each_more(&env, &s, &src, &commits[4..]);
    let new = commits[4].clone();
    storm(&env, "new", &s.url("demo"), &new, 1, 1);
    assert_eq!(s.metrics().shallow_builds_recorded(), 2);

    let dir = tempfile::tempdir().unwrap();
    let mut config = CompactConfig::new(
        dir.path(),
        MaxObjectBytes::new(512 << 20).unwrap(),
        MaxInflatedBytes::new(8 << 30).unwrap(),
    );
    config.shallow_ttl = Duration::ZERO;
    config.gc_grace = Duration::ZERO;
    let compactor = Compactor::new(s.meta.clone(), s.blobs.clone(), config);
    let report = s.block_on(compactor.collect_garbage()).unwrap();
    assert_eq!(report.shallow_evicted, 1, "only the superseded version's");
    assert_eq!(
        report.packs_deleted.len(),
        2,
        "the evicted pack and the orphan patch pack"
    );
    let again = s.block_on(compactor.collect_garbage()).unwrap();
    assert_eq!(again.shallow_evicted, 0, "the current version's stays");

    let rebuilt = storm(&env, "old-again", &s.url("demo"), &old, 1, 1);
    assert_eq!(shallow_file(&rebuilt[0]), vec![old]);
    assert_eq!(s.metrics().shallow_builds_recorded(), 3);
}

/// Pushes more commits to an existing `demo` repository.
fn push_each_more(env: &Env, s: &TestServer, src: &Path, commits: &[String]) {
    for c in commits {
        run(env
            .git(src)
            .args(["push", "--quiet", &s.url("demo")])
            .arg(format!("{c}:refs/heads/main")));
    }
}

/// Invariant (the compactor's build, called directly): a closure the region
/// reaches entirely records nothing (`NotNeeded`); a closure over the size
/// limit downloads nothing (`TooLarge`); and a pack whose bytes equal a
/// stored pack at another level (here the first push's own pack, which
/// holds exactly the region) records that pack rather than a conflicting
/// second row.
#[test]
fn the_compactor_reuses_a_byte_identical_pack_and_skips_needless_builds() {
    let env = Env::new();
    let src = env.source();
    let a = env.commit(&src, &[("x.txt".into(), lines(50, "a"))], "a");
    let b = env.commit(&src, &[("y.txt".into(), lines(50, "b"))], "b");
    let s = TestServer::start(|c| c.shallow_cache = None);
    let seed = s.seeder("demo", "refs/heads/main", &src);
    let va = s.push(&seed, &Push::new("refs/heads/main", &a));
    let vb = s.push(&seed, &Push::new("refs/heads/main", &b));
    let (pa, pb) = (va.value.pack().unwrap(), vb.value.pack().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let compactor = Compactor::new(
        s.meta.clone(),
        s.blobs.clone(),
        CompactConfig::new(
            dir.path(),
            MaxObjectBytes::new(512 << 20).unwrap(),
            MaxInflatedBytes::new(8 << 30).unwrap(),
        ),
    );
    let spec = |sources| ShallowSpec {
        repo: seed.repo().id,
        version: va.id,
        depth: ShallowDepth::new(1).unwrap(),
        want: ObjectId::parse_hex(&a).unwrap(),
        region: vec![ObjectId::parse_hex(&a).unwrap()],
        sources,
        closure: true,
        max_source_bytes: u64::MAX,
        max_objects: 1000,
        max_repo_bytes: u64::MAX,
    };
    assert_eq!(
        s.block_on(compactor.build_shallow_pack(&spec(vec![pa])))
            .unwrap(),
        ShallowBuild::NotNeeded
    );
    let mut small = spec(vec![pa, pb]);
    small.max_source_bytes = 1;
    assert!(matches!(
        s.block_on(compactor.build_shallow_pack(&small)).unwrap(),
        ShallowBuild::TooLarge { .. }
    ));
    match s
        .block_on(compactor.build_shallow_pack(&spec(vec![pa, pb])))
        .unwrap()
    {
        ShallowBuild::Recorded { pack, evicted } => {
            assert_eq!(pack.id, pa, "the push's own pack");
            assert_eq!(evicted, 0);
        }
        other => panic!("expected a recorded pack, got {other:?}"),
    }
    let recorded = s
        .block_on(
            s.meta
                .shallow_pack(seed.repo().id, va.id, ShallowDepth::new(1).unwrap()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(recorded.id, pa);
}

// ---------------------------------------------------------------------------
// Storage bounds (issue #108)

/// Every recording: (version, depth, pack, the pack's stored bytes).
fn recordings(s: &TestServer) -> Vec<(i64, i64, i64, u64)> {
    use sqlx::Row as _;
    s.block_on(
        sqlx::query(
            "SELECT sp.version_id AS v, sp.depth AS d, sp.pack_id AS p, \
             pk.size + pk.idx_size + pk.entries_size AS bytes \
             FROM shallow_packs sp JOIN packs pk ON pk.id = sp.pack_id \
             ORDER BY sp.version_id, sp.depth",
        )
        .fetch_all(s.meta.pool()),
    )
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get::<i64, _>("v"),
            r.get::<i64, _>("d"),
            r.get::<i64, _>("p"),
            r.get::<i64, _>("bytes") as u64,
        )
    })
    .collect()
}

/// The version `demo`'s trunk points at (its only ref).
fn trunk_version(s: &TestServer) -> i64 {
    use sqlx::Row as _;
    s.block_on(sqlx::query("SELECT version_id FROM refs").fetch_one(s.meta.pool()))
        .unwrap()
        .get("version_id")
}

/// Stored bytes of the distinct packs the recordings name: what the cap
/// bounds.
fn recorded_bytes(s: &TestServer) -> u64 {
    let mut seen = std::collections::BTreeMap::new();
    for (_, _, p, bytes) in recordings(s) {
        seen.insert(p, bytes);
    }
    seen.values().sum()
}

/// Old tips and depths a CI fleet might fetch: depths 1, 2 and 4 of every
/// other trunk tip from the seventh on, oldest first. Every region is
/// smaller than its closure, so every one is a build.
fn depth_walk(commits: &[String]) -> Vec<(String, u32)> {
    commits[6..]
        .iter()
        .step_by(2)
        .flat_map(|tip| [1, 2, 4].map(|d| (tip.clone(), d)))
        .collect()
}

/// A shallow fetch of `tip` at `depth` from Aspen, checked against stock
/// Git's fetch of the same (boundary, tip, `fsck --strict` in `storm`).
fn fetch_like_stock(env: &Env, s: &TestServer, src: &Path, name: &str, tip: &str, depth: u32) {
    let aspen = storm(env, name, &s.url("demo"), tip, depth, 1);
    let stock = storm(
        env,
        &format!("{name}-stock"),
        &format!("file://{}", src.display()),
        tip,
        depth,
        1,
    );
    assert_eq!(shallow_file(&aspen[0]), shallow_file(&stock[0]), "{name}");
}

/// A compactor on the server's stores whose clock runs `offset` ahead of
/// the server's.
fn gc_at(s: &TestServer, offset: Duration) -> aspen_compact::GcReport {
    let dir = tempfile::tempdir().unwrap();
    let config = CompactConfig::new(
        dir.path(),
        MaxObjectBytes::new(512 << 20).unwrap(),
        MaxInflatedBytes::new(8 << 30).unwrap(),
    );
    let ahead = i64::try_from(offset.as_micros()).unwrap();
    let meta = s
        .meta
        .clone()
        .with_clock(aspen_meta::Clock::from_fn(move || {
            aspen_core::id::UnixMicros(aspen_core::id::UnixMicros::now().0 + ahead)
        }));
    let report = s
        .block_on(Compactor::new(meta, s.blobs.clone(), config).collect_garbage())
        .unwrap();
    assert!(report.blobs_left.is_empty(), "{report:?}");
    report
}

/// Whether all three blobs of pack `id` are in the bucket.
fn blobs_present(s: &TestServer, id: i64) -> bool {
    let id = aspen_core::id::PackId::new(id).unwrap();
    let rows = match s.block_on(s.meta.packs(&[id])) {
        Ok(rows) => rows,
        // The sweep deleted the row.
        Err(aspen_core::error::StoreError::NotFound { .. }) => return false,
        Err(e) => panic!("{e}"),
    };
    let Some(row) = rows.first() else {
        return false;
    };
    let keys = row.keys();
    [keys.pack, keys.idx, keys.entries]
        .iter()
        .all(|k| s.blobs.path_of(k).exists())
}

/// Invariant (issue #108): however many depths and old tips clients fetch,
/// a repository's recorded exact shallow packs stay within
/// `max_repo_bytes`, while every fetch is still served correctly (stock
/// Git's boundary, `fsck --strict`). The cap is a third of what the same
/// fetches record without one, so it must evict; it evicts least recently
/// used packs and counts them. An evicted pack keeps its blobs until
/// garbage collection, a whole grace period after its eviction, and
/// recordings no fetch uses are evicted after `shallow_unused`, the
/// current tip's included.
#[test]
fn many_depths_and_old_tips_stay_under_the_repository_cap() {
    let env = Env::new();
    let (src, commits) = long_history(&env, 12);
    let walk = depth_walk(&commits);
    assert_eq!(walk.len(), 12);

    // Without a cap (the default 8 GiB is far away): what the walk records.
    let free = writable(|_| {});
    push_each(&env, &free, &src, &commits);
    for (i, (tip, depth)) in walk.iter().enumerate() {
        fetch_like_stock(&env, &free, &src, &format!("free{i}"), tip, *depth);
    }
    let all = recordings(&free);
    assert_eq!(all.len(), walk.len(), "one pack per tip and depth");
    let uncapped = recorded_bytes(&free);
    let largest = all.iter().map(|r| r.3).max().unwrap();
    let cap = largest.max(uncapped / 3);
    assert!(cap < uncapped, "the cap must bind: {cap} of {uncapped}");
    drop(free);

    let s = writable(|c| {
        c.shallow_cache.as_mut().unwrap().max_repo_bytes = cap;
    });
    push_each(&env, &s, &src, &commits);
    let mut ever = std::collections::BTreeSet::new();
    for (i, (tip, depth)) in walk.iter().enumerate() {
        fetch_like_stock(&env, &s, &src, &format!("capped{i}"), tip, *depth);
        let bytes = recorded_bytes(&s);
        eprintln!("after fetch {i} ({tip:.8} at depth {depth}): {bytes} of {cap} bytes recorded");
        assert!(bytes <= cap, "fetch {i}: {bytes} bytes recorded, cap {cap}");
        ever.extend(recordings(&s).iter().map(|r| r.2));
    }
    let m = s.metrics();
    assert_eq!(m.shallow_builds_recorded(), walk.len() as u64);
    assert_eq!(m.shallow_builds_over_limit(), 0);
    let left: std::collections::BTreeSet<i64> = recordings(&s).iter().map(|r| r.2).collect();
    let evicted: Vec<i64> = ever.difference(&left).copied().collect();
    assert!(!evicted.is_empty());
    assert_eq!(
        m.shallow_limit_evictions_total(),
        (walk.len() - recordings(&s).len()) as u64
    );
    // The least recently used went: the walk's last fetch (the current tip
    // at depth 4) is still there.
    let current_version = trunk_version(&s);
    let latest = recordings(&s);
    assert_eq!(walk.last().unwrap(), &(commits.last().unwrap().clone(), 4));
    assert!(
        latest.iter().any(|r| r.0 == current_version && r.1 == 4),
        "{latest:?}"
    );

    // Evicted packs keep their rows and blobs until a whole grace period
    // after their eviction, then garbage collection takes them.
    let config = CompactConfig::new(
        "/nonexistent",
        MaxObjectBytes::new(1 << 20).unwrap(),
        MaxInflatedBytes::new(1 << 20).unwrap(),
    );
    let grace = config.gc_grace;
    let early = gc_at(&s, grace - Duration::from_secs(60));
    for p in &evicted {
        assert!(!early.packs_deleted.iter().any(|d| d.get() == *p));
        assert!(blobs_present(&s, *p), "pack {p} lost its blobs early");
    }
    let late = gc_at(&s, grace + Duration::from_secs(60));
    for p in &evicted {
        assert!(late.packs_deleted.iter().any(|d| d.get() == *p), "pack {p}");
        assert!(!blobs_present(&s, *p));
    }

    // The current tip's recordings are never superseded; unused for
    // `shallow_unused`, they go too.
    let current: Vec<_> = recordings(&s)
        .into_iter()
        .filter(|r| r.0 == current_version)
        .collect();
    assert!(
        !current.is_empty(),
        "the current tip's recordings survived the old tips'"
    );
    gc_at(&s, config.shallow_unused - Duration::from_secs(60));
    assert_eq!(
        recordings(&s).len(),
        current.len(),
        "only the current tip's are left"
    );
    let unused = gc_at(&s, config.shallow_unused + Duration::from_secs(60));
    assert_eq!(unused.shallow_evicted, current.len() as u64);
    assert!(recordings(&s).is_empty());
}

/// Invariant (issue #108): a pack larger than the repository's cap on its
/// own is not stored, and the fetch is served the full history with stock
/// Git's boundary; the build counts as `over_limit`.
#[test]
fn a_pack_over_the_cap_is_not_stored_and_the_fetch_gets_full_history() {
    let env = Env::new();
    let (src, commits) = long_history(&env, 4);
    let s = writable(|c| c.shallow_cache.as_mut().unwrap().max_repo_bytes = 1);
    push_each(&env, &s, &src, &commits);
    same_as_stock(&env, &src, &s, "over", &[&["--depth=1"]]);
    let m = s.metrics();
    assert_eq!(m.shallow_builds_over_limit(), 1);
    assert_eq!(m.shallow_builds_recorded(), 0);
    assert!(recordings(&s).is_empty());
}

/// Invariant (issue #108): a fetch served a recorded pack updates the
/// recording's last use, so garbage collection keeps packs in use; one
/// process updates it at most once a minute per recording.
#[test]
fn a_fetch_served_a_recorded_pack_updates_its_last_use() {
    use sqlx::Row as _;
    let env = Env::new();
    let (src, commits) = long_history(&env, 4);
    let s = writable(|_| {});
    push_each(&env, &s, &src, &commits);
    let tip = commits.last().unwrap();
    let used = || -> (i64, i64) {
        let r = s
            .block_on(
                sqlx::query("SELECT recorded_at_us, used_at_us FROM shallow_packs")
                    .fetch_one(s.meta.pool()),
            )
            .unwrap();
        (r.get("recorded_at_us"), r.get("used_at_us"))
    };
    storm(&env, "build", &s.url("demo"), tip, 1, 1);
    let (recorded, first) = used();
    assert_eq!(first, recorded, "a recording counts as a use");
    storm(&env, "hit", &s.url("demo"), tip, 1, 1);
    let deadline = Instant::now() + Duration::from_secs(10);
    let second = loop {
        let (_, u) = used();
        if u > first || Instant::now() > deadline {
            break u;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(second > first, "the served fetch updated last use");
    storm(&env, "again", &s.url("demo"), tip, 1, 1);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(used().1, second, "not again within the minute");
    assert_eq!(s.metrics().shallow_cached(), 2);
}

/// Invariant: once a tip of a busy branch has its exact pack, the next
/// tips' packs are built from that pack and the packs pushed since, not
/// from the full closure, and are still exactly what stock Git sends. On
/// the public `place` demo `main` moves every few seconds, and building
/// each new tip's depth-1 pack from the whole closure made `git clone
/// --depth=1` take 5 s when idle and fall back to the full history (20 s
/// wait) during a burst. A tip whose tree brings back an object from below
/// the previous pack's region still gets its exact pack incrementally: the
/// object comes from the closure into the layered answer's patch pack,
/// which is one of the build's sources.
#[test]
fn later_tips_build_their_packs_from_the_last_recorded_pack() {
    let env = Env::new();
    let (src, mut commits) = long_history(&env, 12);
    let s = writable(|_| {});
    push_each(&env, &s, &src, &commits[..10]);
    fetch_like_stock(&env, &s, &src, "first", &commits[9], 1);
    assert_eq!(s.metrics().shallow_builds_recorded(), 1);
    assert_eq!(
        s.metrics().shallow_builds_incremental(),
        0,
        "nothing to start from"
    );

    // Two more pushes, then a fetch of the newest tip: built from the
    // first tip's pack and the two pushes' packs.
    push_each_more(&env, &s, &src, &commits[10..]);
    fetch_like_stock(&env, &s, &src, "next", &commits[12], 1);
    assert_eq!(s.metrics().shallow_builds_recorded(), 2);
    assert_eq!(s.metrics().shallow_builds_incremental(), 1);

    // dir0/file0.txt goes back to its first content, rewritten in change 2,
    // which no source but the closure holds.
    let back = vec![("dir0/file0.txt".to_owned(), lines(150, "f0 v0"))];
    commits.push(env.commit(&src, &back, "bring back an old blob"));
    push_each_more(&env, &s, &src, &commits[13..]);
    fetch_like_stock(&env, &s, &src, "back", &commits[13], 1);
    assert_eq!(s.metrics().shallow_builds_recorded(), 3);
    assert_eq!(
        s.metrics().shallow_builds_incremental(),
        2,
        "the old blob came from the closure in a patch, not a closure build"
    );

    // Depth 2 starts its own chain.
    fetch_like_stock(&env, &s, &src, "deep", &commits[13], 2);
    assert_eq!(s.metrics().shallow_builds_incremental(), 2);
}

// ---------------------------------------------------------------------------
// Layered answers (a new tip whose exact pack is not built yet)

/// A server whose builds never end within a request's wait, as a build of
/// a monorepo-sized snapshot does not (about 20 s for 300,000 files):
/// every request that could wait for one stops at once.
fn impatient() -> TestServer {
    writable(|c| {
        let sc = c.shallow_cache.as_mut().unwrap();
        sc.build_wait = Duration::ZERO;
        sc.layered_wait = Duration::ZERO;
    })
}

/// Waits until `n` exact packs are recorded.
fn wait_recorded(s: &TestServer, n: u64) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while s.metrics().shallow_builds_recorded() < n {
        assert!(Instant::now() < deadline, "the build never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Invariant (monorepo benchmark, item 9): right after trunk moves, a
/// depth-1 fetch of the new tip, whose exact pack is still being built, is
/// served at once the last recorded pack plus the packs pushed since: a
/// valid shallow clone with stock Git's boundary, holding everything stock
/// Git sends, at about one snapshot's size, never the full history (which
/// was what such a fetch got when the build outlasted its wait). Once the
/// build ends, fetches get the exact pack.
#[test]
fn a_new_tip_is_served_the_last_recorded_pack_and_the_pushes_since() {
    let env = Env::new();
    let (src, commits) = long_history(&env, 30);
    let s = impatient();
    let m = s.metrics();
    push_each(&env, &s, &src, &commits[..29]);

    // A cold start: nothing recorded to build on, so the first fetch gets
    // the full history while the first build goes on.
    let cold = storm(&env, "cold", &s.url("demo"), &commits[28], 1, 1);
    assert_eq!(m.shallow_fallbacks(), 1);
    let full_b = per_fetch(&cold);
    wait_recorded(&s, 1);

    // Trunk moves twice; a storm of the new tip does not wait for its
    // build and is not sent the full history.
    push_each_more(&env, &s, &src, &commits[29..]);
    let tip = commits.last().unwrap();
    let cached = m.shallow_cached();
    let layered = storm(&env, "layered", &s.url("demo"), tip, 1, 4);
    let stock = storm(
        &env,
        "stock",
        &format!("file://{}", src.display()),
        tip,
        1,
        1,
    );
    assert_eq!(m.shallow_fallbacks(), 1, "no fetch fell back");
    // A fetch that arrives after the build ended gets the exact pack.
    assert!(m.shallow_layered() >= 1);
    assert_eq!(m.shallow_layered() + m.shallow_cached() - cached, 4);
    let want: std::collections::BTreeSet<_> = objects(&env, &stock[0]).into_iter().collect();
    for d in &layered {
        assert_eq!(shallow_file(d), vec![tip.clone()]);
        let got: std::collections::BTreeSet<_> = objects(&env, d).into_iter().collect();
        assert!(got.is_superset(&want), "everything stock Git sends");
    }
    let (layered_b, stock_b) = (per_fetch(&layered), per_fetch(&stock));
    eprintln!(
        "depth-1 after a trunk move, bytes per fetch: stock {stock_b}, layered {layered_b}, \
         full history {full_b}"
    );
    assert!(
        layered_b * 10 <= stock_b * 15,
        "about one snapshot: {layered_b} against stock's {stock_b}"
    );
    assert!(
        full_b > stock_b * 4,
        "the fixture's history is large: {full_b}"
    );

    // The build goes on; once it is recorded, fetches get the exact pack.
    wait_recorded(&s, 2);
    assert_eq!(
        m.shallow_builds_incremental(),
        1,
        "built from the layered answer"
    );
    let exact = storm(&env, "exact", &s.url("demo"), tip, 1, 1);
    assert_eq!(objects(&env, &exact[0]), objects(&env, &stock[0]));
    assert_eq!(m.shallow_fallbacks(), 1);
}

/// Invariant: a new tip that brings back an object only older history
/// holds (a revert) still gets a layered answer: what the last recorded
/// pack and the pushes since miss comes from the version's closure into
/// the patch pack. Without it the fetch fails (`missing blob object`).
#[test]
fn a_revert_is_patched_into_the_layered_answer() {
    let env = Env::new();
    let (src, mut commits) = long_history(&env, 12);
    let s = impatient();
    let m = s.metrics();
    push_each(&env, &s, &src, &commits[..11]);
    storm(&env, "cold", &s.url("demo"), &commits[10], 1, 1);
    wait_recorded(&s, 1);

    // dir0/file0.txt goes back to its first content, which only the
    // initial commit's pack holds.
    let back = vec![("dir0/file0.txt".to_owned(), lines(150, "f0 v0"))];
    commits.push(env.commit(&src, &back, "bring back an old blob"));
    push_each_more(&env, &s, &src, &commits[11..]);
    let tip = commits.last().unwrap();
    let (fallbacks, cached) = (m.shallow_fallbacks(), m.shallow_cached());
    let got = storm(&env, "revert", &s.url("demo"), tip, 1, 2);
    assert_eq!(m.shallow_fallbacks(), fallbacks, "no fetch fell back");
    assert_eq!(m.shallow_layered() + m.shallow_cached() - cached, 2);
    let stock = storm(
        &env,
        "revert-stock",
        &format!("file://{}", src.display()),
        tip,
        1,
        1,
    );
    let want: std::collections::BTreeSet<_> = objects(&env, &stock[0]).into_iter().collect();
    for d in &got {
        assert_eq!(shallow_file(d), shallow_file(&stock[0]));
        let have: std::collections::BTreeSet<_> = objects(&env, d).into_iter().collect();
        assert!(have.is_superset(&want));
    }
}

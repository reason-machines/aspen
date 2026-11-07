//! Stock `git` (protocol v2, smart HTTP) fetching from Aspen.
//!
//! Each test seeds a repository's stores with `aspen_server::testing` (the
//! push path is node B1, so the stores are filled the way B1 will fill
//! them), then runs real `git clone` and `git fetch` against a running
//! server and checks the result with `git fsck --strict`. Shallow cases
//! are differential: the same command against stock `git upload-pack`
//! (over `file://`) must leave the same `.git/shallow`.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;

use aspen_server::testing::{Index, Push, TestServer};
use aspen_testkit::NO_AUTO_MAINTENANCE;

/// A scratch area with an isolated `HOME` for every git command.
struct Env {
    tmp: tempfile::TempDir,
    /// Committer and author time for the next commit, seconds.
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
            .env_remove("GIT_TRACE_PACKET")
            .args(["-c", "protocol.version=2", "-c", "init.defaultBranch=main"])
            // No automatic maintenance: after a fetch that brought several
            // packfile-URI packs it repacks in the background while `fsck`
            // reads the pack directory (#72; see `NO_AUTO_MAINTENANCE`).
            .args(NO_AUTO_MAINTENANCE);
        c
    }

    /// A non-bare repository to make history in.
    fn source(&self) -> PathBuf {
        let src = self.path("src");
        fs::create_dir_all(&src).unwrap();
        run(self.git(&src).args(["init", "--quiet", "-b", "main"]));
        src
    }

    /// Commits `contents` to `file` in `dir` with the next clock tick.
    fn commit(&self, dir: &Path, file: &str, contents: &str) -> String {
        fs::write(dir.join(file), contents).unwrap();
        run(self.git(dir).args(["add", file]));
        let t = self.clock.get() + 1000;
        self.clock.set(t);
        let date = format!("@{t} +0000");
        run(self
            .git(dir)
            .env("GIT_AUTHOR_DATE", &date)
            .env("GIT_COMMITTER_DATE", &date)
            .args(["commit", "--quiet", "-m", &format!("{file}: {contents}")]));
        rev(self, dir, "HEAD")
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

/// Runs `cmd`, expects it to fail, returns its stderr.
fn run_fails(cmd: &mut Command) -> String {
    let out = cmd.output().expect("spawn git");
    assert!(!out.status.success(), "{cmd:?} succeeded but should fail");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn rev(env: &Env, dir: &Path, r: &str) -> String {
    run(env.git(dir).args(["rev-parse", r])).trim().to_owned()
}

fn fsck(env: &Env, dir: &Path) {
    run(env.git(dir).args(["fsck", "--strict", "--no-progress"]));
}

/// A file with many lines, so successive versions delta against each other.
fn lines(n: usize, tag: &str) -> String {
    (0..n)
        .map(|i| format!("line {i} of the file {tag}\n"))
        .collect()
}

/// The sorted `.git/shallow` of a clone (empty when it is not shallow).
fn shallow_file(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_to_string(dir.join(".git/shallow"))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect();
    v.sort();
    v
}

/// One raw HTTP/1.1 exchange, for what `git` never sends.
fn http(base: &str, request: &str, body: &[u8]) -> String {
    let addr = base.strip_prefix("http://").unwrap();
    let mut s = TcpStream::connect(addr).unwrap();
    s.write_all(request.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).unwrap();
    String::from_utf8_lossy(&out).into_owned()
}

/// Invariant: a stock v2 client can clone a repository Aspen serves from
/// stitched stored packs, then fetch incrementally: its exact-tip have
/// is acknowledged, only the new versions' packs are sent, and both
/// repositories pass `fsck --strict`. `PlanStats.visited` is exported.
#[test]
fn clone_then_incremental_fetch_with_stitched_packs() {
    let env = Env::new();
    let src = env.source();
    let a1 = env.commit(&src, "file.txt", &lines(200, "a1"));
    let a2 = env.commit(&src, "file.txt", &lines(210, "a2"));
    let a3 = env.commit(&src, "other.txt", "three\n");
    let s = TestServer::start(|_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    for tip in [&a1, &a2, &a3] {
        s.push(&seed, &Push::new("refs/heads/main", tip));
    }

    let reader = env.path("reader");
    let trace = env.path("clone.trace");
    run(env
        .git(env.tmp.path())
        .env("GIT_TRACE_PACKET", &trace)
        .args(["clone", "--quiet", &s.url("demo")])
        .arg(&reader));
    let trace = fs::read_to_string(trace).unwrap();
    assert!(trace.contains("< version 2"), "protocol v2 was not used");
    assert_eq!(rev(&env, &reader, "HEAD"), a3);
    fsck(&env, &reader);
    assert_eq!(s.metrics().packs_stitched(), 3, "one stored pack per push");
    assert_eq!(s.metrics().packs_by_uri(), 0);

    // Two more trunk commits in one push, and a branch off a3.
    env.commit(&src, "file.txt", &lines(220, "a4"));
    let a5 = env.commit(&src, "file.txt", &lines(230, "a5"));
    run(env
        .git(&src)
        .args(["switch", "--quiet", "-c", "topic", &a3]));
    let t1 = env.commit(&src, "topic.txt", "topic\n");
    s.push(&seed, &Push::new("refs/heads/main", &a5));
    s.push(&seed, &Push::new("refs/heads/topic", &t1));

    let trace = env.path("fetch.trace");
    run(env
        .git(&reader)
        .env("GIT_TRACE_PACKET", &trace)
        .args(["fetch", "--quiet", "origin"]));
    assert_eq!(rev(&env, &reader, "origin/main"), a5);
    assert_eq!(rev(&env, &reader, "origin/topic"), t1);
    fsck(&env, &reader);
    let trace = fs::read_to_string(trace).unwrap();
    assert!(
        trace.contains(&format!("ACK {a3}")),
        "the exact tip the client has is acknowledged:\n{trace}"
    );
    assert_eq!(
        s.metrics().packs_stitched(),
        5,
        "the fetch sent only the two new versions' packs"
    );
    assert!(s.metrics().haves_used() >= 1);
    let metrics = http(
        &s.base_url(),
        "GET /-/metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        b"",
    );
    assert!(
        metrics.contains("aspen_fetch_plan_visited_versions_count 2"),
        "{metrics}"
    );
    assert!(
        metrics.contains("aspen_fetch_packs_stitched_total 5"),
        "{metrics}"
    );
}

/// Invariant: a client that opted in with `fetch.uriprotocols` gets every
/// planned pack as a packfile URI (served by Aspen's signed blob route)
/// and an empty inline pack; a client that did not gets the stitched pack
/// from the same server; both pass `fsck --strict`.
#[test]
fn packfile_uri_clone_and_fetch_and_stitched_fallback() {
    let env = Env::new();
    let src = env.source();
    let mut tips = Vec::new();
    for i in 0..3 {
        tips.push(env.commit(&src, "file.txt", &lines(100 + i * 10, &format!("v{i}"))));
    }
    let s = TestServer::start(|c| c.packfile_uris = true);
    let seed = s.seeder("demo", "refs/heads/main", &src);
    for t in &tips {
        s.push(&seed, &Push::new("refs/heads/main", t));
    }
    let uri_client = env.path("uri");
    let trace = env.path("uri.trace");
    run(env
        .git(env.tmp.path())
        .env("GIT_TRACE_PACKET", &trace)
        .args([
            "-c",
            "fetch.uriprotocols=http",
            "clone",
            "--quiet",
            &s.url("demo"),
        ])
        .arg(&uri_client));
    let trace = fs::read_to_string(trace).unwrap();
    assert!(trace.contains("packfile-uris"), "{trace}");
    assert!(trace.contains("/-/blobs/repos/"), "{trace}");
    assert_eq!(rev(&env, &uri_client, "HEAD"), tips[2]);
    fsck(&env, &uri_client);
    assert_eq!(
        (s.metrics().packs_by_uri(), s.metrics().packs_stitched()),
        (3, 0)
    );

    let t3 = env.commit(&src, "file.txt", &lines(140, "v3"));
    s.push(&seed, &Push::new("refs/heads/main", &t3));
    run(env.git(&uri_client).args([
        "-c",
        "fetch.uriprotocols=http",
        "fetch",
        "--quiet",
        "origin",
    ]));
    assert_eq!(rev(&env, &uri_client, "origin/main"), t3);
    fsck(&env, &uri_client);
    assert_eq!(
        (s.metrics().packs_by_uri(), s.metrics().packs_stitched()),
        (4, 0)
    );

    // The same server, a client that did not opt in: stitched.
    let plain = env.path("plain");
    run(env
        .git(env.tmp.path())
        .args(["clone", "--quiet", &s.url("demo")])
        .arg(&plain));
    assert_eq!(rev(&env, &plain, "HEAD"), t3);
    fsck(&env, &plain);
    assert_eq!(
        (s.metrics().packs_by_uri(), s.metrics().packs_stitched()),
        (4, 4)
    );

    // A scheme the client did not list is never sent (S3: clients don't
    // filter): https only, so everything is stitched.
    let https_only = env.path("https-only");
    run(env
        .git(env.tmp.path())
        .args([
            "-c",
            "fetch.uriprotocols=https",
            "clone",
            "--quiet",
            &s.url("demo"),
        ])
        .arg(&https_only));
    fsck(&env, &https_only);
    assert_eq!(
        (s.metrics().packs_by_uri(), s.metrics().packs_stitched()),
        (4, 8)
    );
}

/// Invariant: past the URI cap, the remaining packs are stitched into the
/// inline pack, and the mix of URI packs and inline pack is a valid fetch.
#[test]
fn packs_beyond_the_uri_cap_are_stitched_inline() {
    let env = Env::new();
    let src = env.source();
    let s = TestServer::start(|c| {
        c.packfile_uris = true;
        c.max_uris_per_response = 1;
    });
    let seed = s.seeder("demo", "refs/heads/main", &src);
    let mut last = String::new();
    for i in 0..3 {
        last = env.commit(&src, "f.txt", &lines(50 + i, "cap"));
        s.push(&seed, &Push::new("refs/heads/main", &last));
    }
    let client = env.path("client");
    run(env
        .git(env.tmp.path())
        .args([
            "-c",
            "fetch.uriprotocols=http",
            "clone",
            "--quiet",
            &s.url("demo"),
        ])
        .arg(&client));
    assert_eq!(rev(&env, &client, "HEAD"), last);
    fsck(&env, &client);
    assert_eq!(
        (s.metrics().packs_by_uri(), s.metrics().packs_stitched()),
        (1, 2)
    );
}

/// Invariant (HaveVersion, N1 review): a have that is the tip of a version
/// whose closure is wider than its tip must be dropped. Here the branch
/// `side` was created at trunk's first commit while trunk was at T2, so
/// its closure holds T2's objects. A client holding `side`'s tip but not
/// T2 fetches trunk at T3: if the server let `side`'s tip stand for its
/// version, it would subtract T2's pack and the fetch would fail. The
/// server must acknowledge only T1 and send T2's and T3's packs.
#[test]
fn a_have_of_a_wider_version_is_dropped() {
    let env = Env::new();
    let src = env.source();
    let t1 = env.commit(&src, "file.txt", &lines(100, "t1"));
    let t2 = env.commit(&src, "two.txt", "only in T2\n");
    run(env.git(&src).args(["branch", "t1", &t1]));
    run(env.git(&src).args(["switch", "--quiet", "-c", "side", &t1]));
    let c = env.commit(&src, "side.txt", "side\n");
    run(env.git(&src).args(["switch", "--quiet", "main"]));
    let t3 = env.commit(&src, "three.txt", "three\n");
    let s = TestServer::start(|_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    s.push(&seed, &Push::new("refs/heads/main", &t1));
    s.push(&seed, &Push::new("refs/heads/main", &t2));
    let side = s.push(&seed, &Push::new("refs/heads/side", &c));
    assert_eq!(
        side.value.closure(),
        Some(aspen_core::refs::Closure::Wider),
        "the branch depends on T2, which its tip does not reach"
    );
    s.push(&seed, &Push::new("refs/heads/main", &t3));

    // The client has T1 and side's tip, but not T2.
    let client = env.path("client");
    fs::create_dir_all(&client).unwrap();
    run(env.git(&client).args(["init", "--quiet"]));
    run(env
        .git(&client)
        .arg("fetch")
        .arg("--quiet")
        .arg(&src)
        .args([
            "refs/heads/t1:refs/remotes/origin/main",
            "refs/heads/side:refs/heads/side",
        ]));
    let missing = Command::new("git")
        .current_dir(&client)
        .args(["cat-file", "-e", &t2])
        .status()
        .unwrap();
    assert!(!missing.success(), "the client must not have T2");
    run(env
        .git(&client)
        .args(["remote", "add", "origin", &s.url("demo")]));
    let before = s.metrics().packs_stitched();
    let trace = env.path("fetch.trace");
    run(env
        .git(&client)
        .env("GIT_TRACE_PACKET", &trace)
        .args(["fetch", "--quiet", "origin", "main"]));
    let trace = fs::read_to_string(trace).unwrap();
    assert!(
        trace.contains(&format!("have {c}")),
        "side's tip was offered:\n{trace}"
    );
    assert!(
        !trace.contains(&format!("ACK {c}")),
        "a wider version's tip is never acknowledged"
    );
    assert!(
        trace.contains(&format!("ACK {t1}")),
        "T1 stands for its exact version"
    );
    assert_eq!(rev(&env, &client, "origin/main"), t3);
    fsck(&env, &client);
    assert_eq!(
        s.metrics().packs_stitched() - before,
        2,
        "T2's and T3's packs: T2's would be missing if side's have were used"
    );
    assert!(s.metrics().haves_dropped() >= 1);
}

/// Invariant (A1b): v0 and v1 clients are served. The advertisement shows
/// only `HEAD`, trunk and the ref of a branch-scoped URL; a clone and an
/// incremental fetch pass `fsck --strict`, the fetch's exact-tip have is
/// acknowledged and only the new versions' packs are sent; a v0 shallow
/// clone and `--deepen` leave the same boundary as stock git over v0.
#[test]
fn v0_and_v1_fetches_are_served() {
    let env = Env::new();
    let src = env.source();
    let a1 = env.commit(&src, "file.txt", &lines(100, "a1"));
    let a2 = env.commit(&src, "file.txt", &lines(110, "a2"));
    run(env.git(&src).args(["switch", "--quiet", "-c", "side", &a1]));
    let s1 = env.commit(&src, "side.txt", "side\n");
    run(env.git(&src).args(["switch", "--quiet", "main"]));
    let s = TestServer::start(|_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    s.push(&seed, &Push::new("refs/heads/main", &a1));
    s.push(&seed, &Push::new("refs/heads/main", &a2));
    s.push(&seed, &Push::new("refs/heads/side", &s1));
    let v0 = |dir: &Path, version: &str| {
        let mut c = env.git(dir);
        c.args(["-c", &format!("protocol.version={version}")]);
        c
    };

    for version in ["0", "1"] {
        let listed = run(v0(env.tmp.path(), version).args(["ls-remote", &s.url("demo")]));
        assert_eq!(
            listed,
            format!("{a2}\tHEAD\n{a2}\trefs/heads/main\n"),
            "v{version}: HEAD and trunk only"
        );
        let scoped = format!("{}/-/refs/heads/side", s.url("demo"));
        let listed = run(v0(env.tmp.path(), version).args(["ls-remote", &scoped]));
        assert!(
            listed.contains(&format!("{s1}\trefs/heads/side")),
            "{listed}"
        );
        let dir = env.path(&format!("v{version}"));
        let trace = env.path(&format!("v{version}.trace"));
        run(v0(env.tmp.path(), version)
            .env("GIT_TRACE_PACKET", &trace)
            .args(["clone", "--quiet", &s.url("demo")])
            .arg(&dir));
        let trace = fs::read_to_string(trace).unwrap();
        assert!(!trace.contains("< version 2"), "v{version} was used");
        assert_eq!(rev(&env, &dir, "HEAD"), a2);
        fsck(&env, &dir);
    }

    let a3 = env.commit(&src, "file.txt", &lines(120, "a3"));
    let a4 = env.commit(&src, "other.txt", "four\n");
    s.push(&seed, &Push::new("refs/heads/main", &a3));
    s.push(&seed, &Push::new("refs/heads/main", &a4));
    let before = s.metrics().packs_stitched();
    let dir = env.path("v0");
    let trace = env.path("fetch.trace");
    run(v0(&dir, "0")
        .env("GIT_TRACE_PACKET", &trace)
        .args(["fetch", "--quiet", "origin"]));
    assert_eq!(rev(&env, &dir, "origin/main"), a4);
    fsck(&env, &dir);
    let trace = fs::read_to_string(trace).unwrap();
    assert!(
        trace.contains(&format!("ACK {a2} common")),
        "the exact tip the client has is acknowledged:\n{trace}"
    );
    assert_eq!(
        s.metrics().packs_stitched() - before,
        2,
        "only a3's and a4's packs"
    );

    // Shallow over v0, against stock git over v0.
    let stock = env.path("stock-shallow");
    let aspen = env.path("aspen-shallow");
    for (url, dir) in [
        (format!("file://{}", src.display()), &stock),
        (s.url("demo"), &aspen),
    ] {
        run(v0(env.tmp.path(), "0")
            .args([
                "clone",
                "--quiet",
                "--no-local",
                "--depth=2",
                "--branch=main",
            ])
            .arg(&url)
            .arg(dir));
        fsck(&env, dir);
        run(v0(dir, "0").args(["fetch", "--quiet", "--deepen=1"]));
        fsck(&env, dir);
    }
    assert_eq!(shallow_file(&aspen), shallow_file(&stock));
    assert_eq!(rev(&env, &aspen, "HEAD"), a4);
}

/// Builds a history with a merge and a second branch, seeded one push per
/// trunk commit. Returns the env, source, server and trunk commits.
fn shallow_fixture(index: Index) -> (Env, PathBuf, TestServer, Vec<String>) {
    shallow_fixture_with(index, |_| {})
}

/// [`shallow_fixture`] on a server `configure` adjusts.
fn shallow_fixture_with(
    index: Index,
    configure: impl FnOnce(&mut aspen_server::ServerConfig),
) -> (Env, PathBuf, TestServer, Vec<String>) {
    let env = Env::new();
    let src = env.source();
    let mut trunk = Vec::new();
    for i in 0..3 {
        trunk.push(env.commit(&src, "file.txt", &lines(40 + i, &format!("m{i}"))));
    }
    run(env
        .git(&src)
        .args(["switch", "--quiet", "-c", "side", &trunk[0]]));
    env.commit(&src, "side.txt", "s1\n");
    env.commit(&src, "side.txt", "s2\n");
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
        trunk.push(env.commit(&src, "file.txt", &lines(60 + i, &format!("n{i}"))));
    }
    run(env.git(&src).args(["branch", "old", &trunk[1]]));
    run(env
        .git(&src)
        .args(["tag", "-a", "-m", "release", "v1", &trunk[4]]));
    let s = TestServer::start(configure);
    let seed = s.seeder("demo", "refs/heads/main", &src);
    for t in &trunk {
        let mut p = Push::new("refs/heads/main", t);
        p.index = index;
        s.push(&seed, &p);
    }
    s.push(&seed, &Push::new("refs/heads/old", &trunk[1]));
    s.push(&seed, &Push::new("refs/tags/v1", "v1"));
    (env, src, s, trunk)
}

/// Runs the same commands against stock `git upload-pack` (file://) and
/// Aspen; the clones must have the same shallow boundary, the same tip and
/// pass `fsck --strict`.
fn same_shallow_as_stock(env: &Env, src: &Path, s: &TestServer, name: &str, steps: &[&[&str]]) {
    let stock = env.path(&format!("{name}-stock"));
    let aspen = env.path(&format!("{name}-aspen"));
    let urls = [
        (format!("file://{}", src.display()), &stock),
        (s.url("demo"), &aspen),
    ];
    for (url, dir) in &urls {
        let (first, rest) = steps.split_first().unwrap();
        run(env
            .git(env.tmp.path())
            .args(["clone", "--quiet", "--no-local"])
            .args(*first)
            .arg(url)
            .arg(dir));
        for step in rest {
            run(env.git(dir).args(*step));
        }
        fsck(env, dir);
    }
    assert_eq!(
        shallow_file(&aspen),
        shallow_file(&stock),
        "{name}: shallow boundary differs from stock git"
    );
    assert_eq!(rev(env, &aspen, "HEAD"), rev(env, &stock, "HEAD"), "{name}");
}

/// Invariant (S5, A5 review, C8): shallow requests get the same boundary
/// stock `git upload-pack` computes, for every deepen form Git sends, and
/// a repository that passes `fsck --strict`: plain depths from exact
/// cached shallow packs (C8, on by default), the other forms from the full
/// closure. [`shallow_fetches_match_stock_git_from_the_full_closure`] runs
/// the same matrix with exact packs off.
#[test]
fn shallow_fetches_match_stock_git() {
    let (env, src, s, trunk) = shallow_fetch_matrix(|_| {});
    let m = s.metrics();
    assert!(m.shallow_builds_recorded() >= 3, "depth 1, 2 and 3 packs");
    assert!(m.shallow_cached() >= 1, "a recorded pack was reused");
    assert_eq!(m.shallow_fallbacks(), 0);
    assert_eq!(rev(&env, &env.path("depth1-aspen"), "HEAD"), trunk[6]);
    drop(src);
}

/// Invariant (S5, A5 review): with exact shallow packs off, every shallow
/// request is served the full closure and still gets stock Git's boundary.
#[test]
fn shallow_fetches_match_stock_git_from_the_full_closure() {
    let (_env, _src, s, _) = shallow_fetch_matrix(|c| c.shallow_cache = None);
    let m = s.metrics();
    assert_eq!(m.shallow_builds_recorded() + m.shallow_cached(), 0);
}

/// Every deepen form Git sends, against stock and Aspen.
fn shallow_fetch_matrix(
    configure: impl FnOnce(&mut aspen_server::ServerConfig),
) -> (Env, PathBuf, TestServer, Vec<String>) {
    let (env, src, s, trunk) = shallow_fixture_with(Index::All, configure);
    let since = (env.clock.get() - 2500).to_string();
    same_shallow_as_stock(&env, &src, &s, "depth1", &[&["--depth=1"]]);
    same_shallow_as_stock(&env, &src, &s, "depth3", &[&["--depth=3"]]);
    same_shallow_as_stock(
        &env,
        &src,
        &s,
        "deepen",
        &[&["--depth=2"], &["fetch", "--quiet", "--deepen=2"]],
    );
    same_shallow_as_stock(
        &env,
        &src,
        &s,
        "depth-again",
        &[&["--depth=1"], &["fetch", "--quiet", "--depth=4"]],
    );
    same_shallow_as_stock(
        &env,
        &src,
        &s,
        "unshallow",
        &[&["--depth=1"], &["fetch", "--quiet", "--unshallow"]],
    );
    let since_arg = format!("--shallow-since={since}");
    same_shallow_as_stock(&env, &src, &s, "since", &[&[since_arg.as_str()]]);
    same_shallow_as_stock(&env, &src, &s, "exclude", &[&["--shallow-exclude=old"]]);
    same_shallow_as_stock(&env, &src, &s, "tag", &[&["--depth=1", "--branch=v1"]]);
    (env, src, s, trunk)
}

/// Invariant: a shallow walk that reaches commits with no `commit_index`
/// row yet (the indexing job has not run) reads them from the packs of
/// the wants' closure instead of treating them as having no parents.
#[test]
fn shallow_walks_read_unindexed_commits_from_packs() {
    let (env, src, s, _) = shallow_fixture(Index::TipOnly);
    same_shallow_as_stock(&env, &src, &s, "depth6", &[&["--depth=6"]]);
    same_shallow_as_stock(&env, &src, &s, "exclude", &[&["--shallow-exclude=old"]]);
}

/// Invariant: annotated tags are served (a want of a tag object resolves
/// through the tag refs), and the branch-scoped URL serves fetches like the
/// plain URL.
#[test]
fn tags_and_branch_scoped_urls() {
    let (env, _src, s, trunk) = shallow_fixture(Index::All);
    let client = env.path("full");
    run(env
        .git(env.tmp.path())
        .args(["clone", "--quiet", &s.url("demo")])
        .arg(&client));
    fsck(&env, &client);
    assert_eq!(
        run(env.git(&client).args(["cat-file", "-t", "v1"])).trim(),
        "tag"
    );
    assert_eq!(rev(&env, &client, "v1^{commit}"), trunk[4]);
    let scoped = format!("{}/-/refs/heads/old", s.url("demo"));
    let listed = run(env.git(env.tmp.path()).args(["ls-remote", &scoped]));
    assert!(
        listed.contains(&format!("{}\trefs/heads/old", trunk[1])),
        "{listed}"
    );
    assert!(listed.contains("HEAD"), "{listed}");
    let fetched = env.path("scoped");
    run(env
        .git(env.tmp.path())
        .args(["clone", "--quiet", "--branch=old", &scoped])
        .arg(&fetched));
    assert_eq!(rev(&env, &fetched, "HEAD"), trunk[1]);
    fsck(&env, &fetched);
}

/// Invariant: unknown repositories and anonymous pushes are refused before
/// any Git exchange, a writer gets the push advertisement; `ls-remote`
/// honours `ref-prefix`.
#[test]
fn refusals_and_ref_prefixes() {
    let env = Env::new();
    let src = env.source();
    let a = env.commit(&src, "f", "x\n");
    let s = TestServer::start(|_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    s.push(&seed, &Push::new("refs/heads/main", &a));
    s.push(&seed, &Push::new("refs/heads/feature", &a));
    let err = run_fails(env.git(env.tmp.path()).args(["ls-remote", &s.url("nope")]));
    assert!(err.contains("not found"), "{err}");
    let missing = http(
        &s.base_url(),
        "GET /nope.git/info/refs?service=git-upload-pack HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        b"",
    );
    assert!(
        missing.starts_with("HTTP/1.1 404 Not Found\r\n"),
        "{missing}"
    );
    assert!(
        missing.ends_with("\r\n\r\nrepository not found\n"),
        "{missing}"
    );
    // `git fetch <url> <ref>` sends `ref-prefix` (git 2.39's ls-remote
    // does not), and only matching refs come back.
    let client = env.path("client");
    fs::create_dir_all(&client).unwrap();
    run(env.git(&client).args(["init", "--quiet"]));
    let trace = env.path("ls.trace");
    run(env.git(&client).env("GIT_TRACE_PACKET", &trace).args([
        "fetch",
        "--quiet",
        &s.url("demo"),
        "refs/heads/feature",
    ]));
    assert_eq!(rev(&env, &client, "FETCH_HEAD"), a);
    let trace = fs::read_to_string(trace).unwrap();
    assert!(trace.contains("ref-prefix refs/heads/feature"), "{trace}");
    assert!(
        !trace.contains(&format!("< {a} refs/heads/main")),
        "main was not listed:\n{trace}"
    );
    // A push: the guard asks an anonymous caller to sign in, and lets a
    // writer through to the receive-pack advertisement (node B1).
    let push_advert = |auth: &str| {
        http(
            &s.base_url(),
            &format!(
                "GET /demo.git/info/refs?service=git-receive-pack HTTP/1.1\r\nHost: x\r\n\
                 Connection: close\r\n{auth}\r\n"
            ),
            b"",
        )
    };
    let anonymous = push_advert("");
    assert!(anonymous.starts_with("HTTP/1.1 401"), "{anonymous}");
    let token = mint(&s, &["write:demo"]);
    let writer = push_advert(&format!("Authorization: {}\r\n", basic_token(&token)));
    assert!(writer.starts_with("HTTP/1.1 200"), "{writer}");
    assert!(
        writer.contains("application/x-git-receive-pack-advertisement"),
        "{writer}"
    );
    assert!(
        writer.contains(&format!("{a} refs/heads/main\0report-status")),
        "trunk is advertised with the push capabilities: {writer}"
    );
    assert!(!writer.contains("refs/heads/feature"), "{writer}");
}

/// Stores a token for subject `agent:test` with `grants`; returns its text.
fn mint(s: &TestServer, grants: &[&str]) -> String {
    use aspen_auth::token::issue;
    use aspen_auth::{Grants, NewToken};
    let issued = issue(
        NewToken {
            subject: "agent:test".into(),
            grants: Grants::new(grants.iter().map(|g| g.parse().unwrap())),
            description: String::new(),
            expires_at: None,
        },
        aspen_core::id::UnixMicros::now(),
    )
    .unwrap();
    s.access.auth().tokens().insert(issued.record);
    issued.token.expose().to_owned()
}

/// Hex SHA-1 of `bytes`, from `sha1sum` (the test needs no hash crate).
fn sha1_hex(bytes: &[u8]) -> String {
    let mut child = Command::new("sha1sum")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(bytes).unwrap();
    let out = child.wait_with_output().unwrap();
    String::from_utf8(out.stdout).unwrap()[..40].to_owned()
}

fn zlib(data: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}

/// Invariant (S1, fetch path step 4): a stored pack that breaks the
/// base-first rule (here a REF_DELTA before its base) is never repaired
/// at fetch time: the fetch ends with a retryable band-3 error, the
/// paging counter goes up, and one `rewrite_pack` job is enqueued for the
/// pack, however many fetches hit it.
#[test]
fn a_stored_pack_with_a_forward_base_pages_and_enqueues_a_rewrite() {
    use aspen_core::blob::BlobStore;
    use aspen_core::id::{ObjectId, PackChecksum};
    use aspen_core::meta::{JobKind, JobTag, MetaStore, PushCommit, RefWrite};
    use aspen_core::name::RefName;
    use aspen_core::pack::{NewPack, PackOrigin, entry_table_len};
    use aspen_core::refs::{Closure, Deps, RefValue};

    let env = Env::new();
    let src = env.source();
    let c1 = env.commit(&src, "f", "x\n");
    let s = TestServer::start(|_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    let trunk = s.push(&seed, &Push::new("refs/heads/main", &c1));
    // The bad version's own tip, so a want of it resolves to that version.
    let c2 = env.commit(&src, "g", "y\n");

    // Two blobs: the delta first, its base second.
    let base = b"the base blob of a forward delta\n".to_vec();
    let result = b"the delta's result\n".to_vec();
    let blob_id = |data: &[u8]| {
        let mut h = format!("blob {}\0", data.len()).into_bytes();
        h.extend_from_slice(data);
        ObjectId::parse_hex(&sha1_hex(&h)).unwrap()
    };
    let (base_id, result_id) = (blob_id(&base), blob_id(&result));
    let mut delta = vec![base.len() as u8, result.len() as u8, result.len() as u8];
    delta.extend_from_slice(&result);
    let mut pack = b"PACK\0\0\0\x02\0\0\0\x02".to_vec();
    let entry = |pack: &mut Vec<u8>, head: Vec<u8>, data: &[u8]| {
        let offset = pack.len() as u64;
        let mut bytes = head;
        bytes.extend_from_slice(&zlib(data));
        let mut crc = flate2::Crc::new();
        crc.update(&bytes);
        pack.extend_from_slice(&bytes);
        (offset, crc.sum())
    };
    // Type 7 is REF_DELTA, type 3 a blob.
    let mut ref_head = Vec::new();
    aspen_pack::header::encode_type_and_size(7, delta.len() as u64, &mut ref_head);
    ref_head.extend_from_slice(base_id.as_bytes());
    let (delta_at, delta_crc) = entry(&mut pack, ref_head, &delta);
    let mut blob_head = Vec::new();
    aspen_pack::header::encode_type_and_size(3, base.len() as u64, &mut blob_head);
    let (base_at, base_crc) = entry(&mut pack, blob_head, &base);
    let checksum = PackChecksum::parse_hex(&sha1_hex(&pack)).unwrap();
    pack.extend_from_slice(checksum.as_bytes());
    let idx = aspen_pack::idx::write(
        vec![
            aspen_pack::idx::IdxEntry {
                id: result_id,
                offset: delta_at,
                crc32: delta_crc,
            },
            aspen_pack::idx::IdxEntry {
                id: base_id,
                offset: base_at,
                crc32: base_crc,
            },
        ],
        &checksum,
    );
    let parsed = aspen_pack::idx::IdxV2::parse(&idx, Some(pack.len() as u64)).unwrap();
    let table = aspen_pack::entries::build(std::io::Cursor::new(&pack), pack.len() as u64, &parsed)
        .unwrap();
    let repo = seed.repo().id;
    let new = NewPack {
        repo,
        checksum,
        size: pack.len() as u64,
        idx_size: idx.len() as u64,
        entries_size: entry_table_len(2),
        object_count: 2,
        ref_delta_count: 1,
        origin: PackOrigin::Push {
            received_bytes: pack.len() as u64,
        },
    };
    let pack_id = s.block_on(s.meta.register_pack(&new)).unwrap();
    let keys = pack_id.keys();
    let pack_id = pack_id.id;
    for (key, bytes) in [
        (&keys.pack, pack.clone()),
        (&keys.idx, idx.clone()),
        (&keys.entries, table.encode()),
    ] {
        let file = env.path("blob");
        fs::write(&file, bytes).unwrap();
        s.block_on(s.blobs.upload_file(key, &file)).unwrap();
    }
    s.block_on(s.meta.commit_push(&PushCommit {
        repo,
        writes: vec![RefWrite {
            name: RefName::new("refs/heads/bad").unwrap(),
            expected: None,
            value: RefValue::Live {
                tip: ObjectId::parse_hex(&c2).unwrap(),
                pack: Some(pack_id),
                closure: Closure::Wider,
            },
            deps: Deps {
                trunk: Some(trunk.id),
                scoped_old: None,
            },
            commits: vec![aspen_core::meta::NewCommit {
                commit: ObjectId::parse_hex(&c2).unwrap(),
                parents: vec![ObjectId::parse_hex(&c1).unwrap()],
                committer_time: 0,
                generation: None,
                root_tree: None,
            }],
            jobs: Vec::new(),
        }],
    }))
    .unwrap();

    for _ in 0..2 {
        let client = env.path(&format!("client-{}", s.metrics().forward_base_alerts()));
        fs::create_dir_all(&client).unwrap();
        run(env.git(&client).args(["init", "--quiet"]));
        let err = run_fails(
            env.git(&client)
                .args(["fetch", &s.url("demo"), "refs/heads/bad"]),
        );
        assert!(err.contains("forward delta base"), "{err}");
        assert!(err.contains("retry"), "{err}");
    }
    assert_eq!(s.metrics().forward_base_alerts(), 2);
    let job = s
        .block_on(s.meta.claim_job(
            &[JobTag::RewritePack],
            "test",
            std::time::Duration::from_secs(60),
        ))
        .unwrap()
        .expect("a rewrite_pack job");
    assert_eq!(job.kind, JobKind::RewritePack { pack: pack_id });
    let second = s
        .block_on(s.meta.claim_job(
            &[JobTag::RewritePack],
            "test",
            std::time::Duration::from_secs(60),
        ))
        .unwrap();
    assert!(second.is_none(), "one job per pack, not one per fetch");
}

/// The object ID of a loose object of `kind` (`blob`, `tree`, `commit`).
fn object_id(kind: &str, data: &[u8]) -> aspen_core::id::ObjectId {
    let mut h = format!("{kind} {}\0", data.len()).into_bytes();
    h.extend_from_slice(data);
    aspen_core::id::ObjectId::parse_hex(&sha1_hex(&h)).unwrap()
}

/// A pack of `entries` (type code, base for a REF_DELTA, data), in order,
/// with its `.idx` and entry table. Nothing checks base-first: that is the
/// point.
fn hand_pack(
    entries: &[(
        u8,
        Option<aspen_core::id::ObjectId>,
        Vec<u8>,
        aspen_core::id::ObjectId,
    )],
) -> (Vec<u8>, Vec<u8>, aspen_core::pack::EntryTable) {
    use aspen_core::id::PackChecksum;
    let mut pack = b"PACK\0\0\0\x02".to_vec();
    pack.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    let mut idx = Vec::new();
    for (ty, base, data, id) in entries {
        let offset = pack.len() as u64;
        let mut bytes = Vec::new();
        aspen_pack::header::encode_type_and_size(*ty, data.len() as u64, &mut bytes);
        if let Some(b) = base {
            bytes.extend_from_slice(b.as_bytes());
        }
        bytes.extend_from_slice(&zlib(data));
        let mut crc = flate2::Crc::new();
        crc.update(&bytes);
        pack.extend_from_slice(&bytes);
        idx.push(aspen_pack::idx::IdxEntry {
            id: *id,
            offset,
            crc32: crc.sum(),
        });
    }
    let checksum = PackChecksum::parse_hex(&sha1_hex(&pack)).unwrap();
    pack.extend_from_slice(checksum.as_bytes());
    idx.sort_by_key(|e| e.id);
    let idx = aspen_pack::idx::write(idx, &checksum);
    let parsed = aspen_pack::idx::IdxV2::parse(&idx, Some(pack.len() as u64)).unwrap();
    let table = aspen_pack::entries::build(std::io::Cursor::new(&pack), pack.len() as u64, &parsed)
        .unwrap();
    (pack, idx, table)
}

/// Invariant (reason-machines/aspen#10; S1, fetch path step 4): the
/// `rewrite_pack` job a forward-base page enqueues is claimed by the
/// production worker (`aspen_compact::Worker` with its default options)
/// and repairs the pack: afterwards the same fetch succeeds and the clone
/// passes `git fsck --strict`. The repair is a new pack row that passes
/// `verify_pack` and holds exactly the old pack's objects; every version
/// that named the old pack names the new one, and nothing names the old
/// one. Running the job again, or a job on the new pack, finds nothing to
/// do and writes nothing.
#[test]
fn the_rewrite_pack_job_repairs_a_stored_pack_and_the_fetch_succeeds() {
    use aspen_blob::LocalBlobStore;
    use aspen_compact::{
        CompactConfig, Compactor, JobOutcome, JobReport, RewriteReport, RewriteSkipped, Worker,
        WorkerOptions,
    };
    use aspen_core::blob::BlobStore;
    use aspen_core::id::ObjectId;
    use aspen_core::meta::{JobKind, MetaStore, NewJob, PushCommit, RefWrite};
    use aspen_core::name::RefName;
    use aspen_core::pack::{NewPack, PackOrigin, entry_table_len};
    use aspen_core::refs::{Closure, Deps, RefValue};
    use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};

    let env = Env::new();
    let src = env.source();
    let c1 = env.commit(&src, "f", "x\n");
    let s = TestServer::start(|_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    let trunk = s.push(&seed, &Push::new("refs/heads/main", &c1));

    // refs/heads/bad: one commit adding two files, the second's blob a
    // delta of the first's. Its pack holds the commit, its tree and both
    // blobs, but the delta comes before its base.
    let base = b"the base blob of a forward delta, long enough to delta against\n".to_vec();
    let result = b"the delta's result\n".to_vec();
    fs::write(src.join("base.txt"), &base).unwrap();
    fs::write(src.join("result.txt"), &result).unwrap();
    run(env.git(&src).args(["add", "base.txt", "result.txt"]));
    run(env
        .git(&src)
        .args(["commit", "--quiet", "-m", "forward delta"]));
    let c2 = rev(&env, &src, "HEAD");
    let cat = |kind: &str, what: &str| {
        let out = env
            .git(&src)
            .args(["cat-file", kind, what])
            .output()
            .unwrap();
        assert!(out.status.success());
        out.stdout
    };
    let commit = cat("commit", &c2);
    let tree = cat("tree", &format!("{c2}^{{tree}}"));
    let (base_id, result_id) = (object_id("blob", &base), object_id("blob", &result));
    let mut delta = vec![base.len() as u8, result.len() as u8, result.len() as u8];
    delta.extend_from_slice(&result);
    let c2_id = ObjectId::parse_hex(&c2).unwrap();
    // Type 1 is a commit, 2 a tree, 3 a blob and 7 a REF_DELTA.
    let (pack, idx, table) = hand_pack(&[
        (1, None, commit, c2_id),
        (2, None, tree.clone(), object_id("tree", &tree)),
        (7, Some(base_id), delta, result_id),
        (3, None, base, base_id),
    ]);
    assert!(table.check_layout().is_err(), "the pack is not base-first");
    let repo = seed.repo().id;
    let old = s
        .block_on(s.meta.register_pack(&NewPack {
            repo,
            checksum: *table.checksum(),
            size: pack.len() as u64,
            idx_size: idx.len() as u64,
            entries_size: entry_table_len(4),
            object_count: 4,
            ref_delta_count: 1,
            origin: PackOrigin::Push {
                received_bytes: pack.len() as u64,
            },
        }))
        .unwrap();
    let keys = old.keys();
    for (key, bytes) in [
        (&keys.pack, pack.clone()),
        (&keys.idx, idx),
        (&keys.entries, table.encode()),
    ] {
        let file = env.path("blob");
        fs::write(&file, bytes).unwrap();
        s.block_on(s.blobs.upload_file(key, &file)).unwrap();
    }
    let bad = s
        .block_on(s.meta.commit_push(&PushCommit {
            repo,
            writes: vec![RefWrite {
                name: RefName::new("refs/heads/bad").unwrap(),
                expected: None,
                value: RefValue::Live {
                    tip: c2_id,
                    pack: Some(old.id),
                    closure: Closure::Wider,
                },
                deps: Deps {
                    trunk: Some(trunk.id),
                    scoped_old: None,
                },
                commits: vec![aspen_core::meta::NewCommit {
                    commit: c2_id,
                    parents: vec![ObjectId::parse_hex(&c1).unwrap()],
                    committer_time: 0,
                    generation: None,
                    root_tree: None,
                }],
                jobs: Vec::new(),
            }],
        }))
        .unwrap();
    let bad = match bad {
        aspen_core::meta::CommitOutcome::Committed(mut v) => v.remove(0),
        other => panic!("{other:?}"),
    };

    let fetch = |name: &str| {
        let client = env.path(name);
        fs::create_dir_all(&client).unwrap();
        run(env.git(&client).args(["init", "--quiet"]));
        let mut cmd = env.git(&client);
        cmd.args([
            "-c",
            "fetch.fsckObjects=true",
            "fetch",
            &s.url("demo"),
            "refs/heads/bad:refs/heads/bad",
        ]);
        (client, cmd)
    };
    let (_, mut cmd) = fetch("before");
    let err = run_fails(&mut cmd);
    assert!(err.contains("forward delta base"), "{err}");
    assert_eq!(s.metrics().forward_base_alerts(), 1);

    // The production worker, as `aspen serve` runs it.
    let spool = env.path("spool");
    fs::create_dir_all(&spool).unwrap();
    let blobs: LocalBlobStore = s.blobs.clone();
    let config = CompactConfig::new(
        &spool,
        MaxObjectBytes::new(1 << 20).unwrap(),
        MaxInflatedBytes::new(1 << 24).unwrap(),
    );
    let mut worker = Worker::new(
        Compactor::new(s.meta.clone(), blobs, config),
        WorkerOptions::new("test-worker"),
    );
    let outcome = s.block_on(worker.run_one()).unwrap().expect("a job is due");
    let JobOutcome::Done {
        job,
        report:
            JobReport::Rewrite(RewriteReport::Rewritten {
                new_pack,
                moved,
                replaced,
            }),
    } = outcome
    else {
        panic!("{outcome:?}");
    };
    assert_eq!(moved, 2, "the delta and its base swapped places");
    assert_eq!(replaced.versions, 1);
    let done = s.block_on(s.meta.job(job)).unwrap().unwrap();
    assert_eq!(done.kind, JobKind::RewritePack { pack: old.id });
    assert_eq!(done.attempts, 1);
    assert_eq!(done.state, aspen_core::meta::JobState::Done);

    // The version names the new pack, which verifies and holds exactly the
    // old pack's objects; nothing names the old one, whose blobs remain.
    let now = s.block_on(s.meta.ref_version(bad.id)).unwrap().unwrap();
    assert_eq!(now.value.pack(), Some(new_pack));
    assert!(
        s.block_on(s.meta.pack_references(old.id))
            .unwrap()
            .is_empty()
    );
    let new = s.block_on(s.meta.packs(&[new_pack])).unwrap().remove(0);
    assert_eq!(
        (new.pack.repo, new.pack.object_count, new.pack.origin),
        (repo, 4, old.pack.origin)
    );
    assert_ne!(new.pack.checksum, old.pack.checksum);
    let read = |key, size| {
        s.block_on(
            s.blobs
                .read_exact_range(key, aspen_core::blob::ByteRange::new(0, size).unwrap()),
        )
        .unwrap()
        .to_vec()
    };
    let new_keys = new.keys();
    let stored = env.path("new.pack");
    fs::write(&stored, read(&new_keys.pack, new.pack.size)).unwrap();
    let scratch = env.path("verify");
    fs::create_dir_all(&scratch).unwrap();
    let verified = aspen_pack::verify::verify_pack(
        &stored,
        &scratch,
        MaxObjectBytes::new(1 << 20).unwrap(),
        MaxInflatedBytes::new(1 << 24).unwrap(),
    )
    .expect("the rewritten pack verifies");
    assert_eq!(verified.idx, read(&new_keys.idx, new.pack.idx_size));
    assert_eq!(
        verified.table.encode(),
        read(&new_keys.entries, new.pack.entries_size)
    );
    let ids = |t: &aspen_core::pack::EntryTable| {
        let mut v: Vec<ObjectId> = t.entries().iter().map(|e| e.id).collect();
        v.sort();
        v
    };
    assert_eq!(ids(&verified.table), ids(&table));
    assert_eq!(read(&keys.pack, old.pack.size), pack, "old blobs stay");

    // The same fetch now succeeds and the clone is sound.
    let (client, mut cmd) = fetch("after");
    run(&mut cmd);
    assert_eq!(rev(&env, &client, "refs/heads/bad"), c2);
    fsck(&env, &client);
    assert_eq!(
        run(env
            .git(&client)
            .args(["cat-file", "blob", "refs/heads/bad:result.txt"])),
        "the delta's result\n"
    );
    assert_eq!(s.metrics().forward_base_alerts(), 1);

    // Idempotent: a rewrite of the replaced pack (a requeue, or a fetch
    // that planned before the swap) and one of the new, base-first pack
    // each find nothing to do and write no pack.
    let packs_before = s.block_on(async {
        sqlx::query("SELECT COUNT(*) AS n FROM packs")
            .fetch_one(s.meta.pool())
            .await
            .unwrap()
    });
    let count = |row: &sqlx::any::AnyRow| sqlx::Row::get::<i64, _>(row, "n");
    for (pack, why) in [
        (old.id, RewriteSkipped::Unreferenced),
        (new_pack, RewriteSkipped::AlreadyBaseFirst),
    ] {
        let id = s
            .block_on(s.meta.enqueue_jobs(&[NewJob {
                repo,
                kind: JobKind::RewritePack { pack },
                run_after: aspen_core::id::UnixMicros(0),
            }]))
            .unwrap()[0];
        let outcome = s.block_on(worker.run_one()).unwrap();
        assert_eq!(
            outcome,
            Some(JobOutcome::Done {
                job: id,
                report: JobReport::Rewrite(RewriteReport::Skipped(why)),
            })
        );
    }
    let packs_after = s.block_on(async {
        sqlx::query("SELECT COUNT(*) AS n FROM packs")
            .fetch_one(s.meta.pool())
            .await
            .unwrap()
    });
    assert_eq!(count(&packs_after), count(&packs_before), "nothing written");
    assert_eq!(
        s.block_on(s.meta.ref_version(bad.id))
            .unwrap()
            .unwrap()
            .value
            .pack(),
        Some(new_pack)
    );
}

/// Invariant (coordinator review of #34): a clone of many branches, each
/// with two branch-scoped pushes (so each branch's older version is reached
/// only through its `scoped_old` dependency), plans in a few loading rounds
/// and succeeds. Loading from one missing version per round needed a round
/// per branch and failed past 40 branches with a corruption alert.
#[test]
fn a_clone_of_many_scoped_branches_plans() {
    let env = Env::new();
    let src = env.source();
    let base = env.commit(&src, "base.txt", "base\n");
    let s = TestServer::start(|_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    s.push(&seed, &Push::new("refs/heads/main", &base));
    const BRANCHES: usize = 50;
    for i in 0..BRANCHES {
        run(env
            .git(&src)
            .args(["switch", "--quiet", "-c", &format!("b{i}"), &base]));
        for k in 0..2 {
            let tip = env.commit(&src, &format!("b{i}.txt"), &format!("{k}\n"));
            let mut p = Push::new(&format!("refs/heads/b{i}"), &tip);
            p.scoped = true;
            let v = s.push(&seed, &p);
            assert_eq!(v.deps.scoped_old.is_some(), k == 1);
        }
    }
    let client = env.path("client");
    run(env
        .git(env.tmp.path())
        .args(["clone", "--quiet", &s.url("demo")])
        .arg(&client));
    fsck(&env, &client);
    let branches = run(env.git(&client).args(["branch", "-r"]));
    assert_eq!(
        branches.lines().filter(|l| l.contains("origin/b")).count(),
        BRANCHES
    );
    assert_eq!(s.metrics().corruption_alerts(), 0);
    assert_eq!(s.metrics().packs_stitched(), 1 + 2 * BRANCHES as u64);
}

/// Invariant: `max_plan_versions` bounds the versions one fetch holds
/// (its wants', its haves' and what the planner loads), and a fetch over
/// it is an ordinary refusal (an `ERR` naming the limit), never a
/// corruption alert. A limit equal to the clone's closure (five versions)
/// is enough; one less is refused.
#[test]
fn a_fetch_over_the_version_limit_is_refused_not_corrupt() {
    let env = Env::new();
    let src = env.source();
    let mut tips = Vec::new();
    for i in 0..5 {
        tips.push(env.commit(&src, "f.txt", &lines(20 + i, "limit")));
    }
    for (limit, fits) in [(5, true), (4, false)] {
        let s = TestServer::start(|c| c.max_plan_versions = limit);
        let seed = s.seeder("demo", "refs/heads/main", &src);
        for t in &tips {
            s.push(&seed, &Push::new("refs/heads/main", t));
        }
        let dir = env.path(&format!("clone-{limit}"));
        let mut clone = env.git(env.tmp.path());
        clone.args(["clone", "--quiet", &s.url("demo")]).arg(&dir);
        if fits {
            run(&mut clone);
            fsck(&env, &dir);
        } else {
            let err = run_fails(&mut clone);
            assert!(
                err.contains("more than 4 ref versions") && err.contains("max_plan_versions"),
                "{err}"
            );
        }
        assert_eq!(s.metrics().corruption_alerts(), 0);
    }
}

/// Invariant: `max_shallow_commits` bounds a shallow walk; a request over
/// it gets an `ERR` naming the limit (no alert), one under it is served.
#[test]
fn a_shallow_walk_over_the_commit_limit_is_refused() {
    let env = Env::new();
    let src = env.source();
    let s = TestServer::start(|c| c.max_shallow_commits = 2);
    let seed = s.seeder("demo", "refs/heads/main", &src);
    for i in 0..5 {
        let t = env.commit(&src, "f.txt", &lines(20 + i, "shallow-limit"));
        s.push(&seed, &Push::new("refs/heads/main", &t));
    }
    let ok = env.path("ok");
    run(env
        .git(env.tmp.path())
        .args(["clone", "--quiet", "--depth=1", &s.url("demo")])
        .arg(&ok));
    fsck(&env, &ok);
    let err = run_fails(
        env.git(env.tmp.path())
            .args(["clone", "--depth=4", &s.url("demo")])
            .arg(env.path("deep")),
    );
    assert!(err.contains("max_shallow_commits"), "{err}");
    assert_eq!(s.metrics().corruption_alerts(), 0);
}

/// Invariant: resolving annotated-tag wants scans at most `max_tag_scan`
/// tags, and a want still unresolved past it gets an `ERR` naming the
/// limit; a fetch that wants no tag is unaffected.
#[test]
fn tag_want_scans_are_capped() {
    let env = Env::new();
    let src = env.source();
    let a = env.commit(&src, "f", "x\n");
    run(env.git(&src).args(["tag", "-a", "-m", "one", "v1", &a]));
    run(env.git(&src).args(["tag", "-a", "-m", "two", "v2", &a]));
    let s = TestServer::start(|c| c.max_tag_scan = 1);
    let seed = s.seeder("demo", "refs/heads/main", &src);
    s.push(&seed, &Push::new("refs/heads/main", &a));
    s.push(&seed, &Push::new("refs/tags/v1", "v1"));
    s.push(&seed, &Push::new("refs/tags/v2", "v2"));
    let err = run_fails(
        env.git(env.tmp.path())
            .args(["clone", &s.url("demo")])
            .arg(env.path("clone")),
    );
    assert!(err.contains("max_tag_scan"), "{err}");
    let client = env.path("client");
    fs::create_dir_all(&client).unwrap();
    run(env.git(&client).args(["init", "--quiet"]));
    run(env.git(&client).args([
        "fetch",
        "--quiet",
        "--no-tags",
        &s.url("demo"),
        "refs/heads/main",
    ]));
    assert_eq!(rev(&env, &client, "FETCH_HEAD"), a);
}

/// Invariant: a want of a commit that is not a tip and has no
/// `commit_index` row yet (its push is still being indexed) is refused
/// with a message that says to retry, not that it is "not a commit".
#[test]
fn a_want_of_an_unindexed_non_tip_commit_says_retry() {
    let env = Env::new();
    let src = env.source();
    let c1 = env.commit(&src, "f", "1\n");
    let c2 = env.commit(&src, "f", "2\n");
    let s = TestServer::start(|_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    let mut p = Push::new("refs/heads/main", &c2);
    p.index = Index::TipOnly;
    s.push(&seed, &p);
    let client = env.path("client");
    fs::create_dir_all(&client).unwrap();
    run(env.git(&client).args(["init", "--quiet"]));
    let err = run_fails(env.git(&client).args(["fetch", &s.url("demo"), &c1]));
    assert!(
        err.contains(&format!("want {c1} is not a ref tip")),
        "{err}"
    );
    assert!(err.contains("retry once the server has indexed"), "{err}");
    assert!(!err.contains("not a commit"), "{err}");
}

/// A pkt-line of `s`.
fn pkt(s: &str) -> String {
    format!("{:04x}{s}", s.len() + 4)
}

/// Splits a raw HTTP response into its head and body.
fn split_response(response: &str) -> (&str, &str) {
    response.split_once("\r\n\r\n").unwrap()
}

/// Invariant: `ls-refs` bytes, exactly: an empty repository's `HEAD` is
/// `unborn` with its `symref-target` when the client asks for both, and
/// a born `HEAD` carries its tip and `symref-target`; refs come back
/// without one.
#[test]
fn ls_refs_bytes_for_unborn_and_symrefs() {
    let env = Env::new();
    let src = env.source();
    let a = env.commit(&src, "f", "x\n");
    let s = TestServer::start(|_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    s.push(&seed, &Push::new("refs/heads/main", &a));
    let _empty = s.seeder("empty", "refs/heads/main", &src);
    let ls_refs = |repo: &str, args: &[&str]| {
        let mut body = pkt("command=ls-refs\n") + "0001";
        for a in args {
            body += &pkt(&format!("{a}\n"));
        }
        body += "0000";
        let response = http(
            &s.base_url(),
            &format!(
                "POST /{repo}.git/git-upload-pack HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\
                 Git-Protocol: version=2\r\n\
                 Content-Type: application/x-git-upload-pack-request\r\nContent-Length: {}\r\n\r\n",
                body.len()
            ),
            body.as_bytes(),
        );
        let (head, body) = split_response(&response);
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        body.to_owned()
    };
    assert_eq!(
        ls_refs("empty", &["symrefs", "unborn", "ref-prefix HEAD"]),
        pkt("unborn HEAD symref-target:refs/heads/main\n") + "0000"
    );
    assert_eq!(
        ls_refs("empty", &["symrefs", "ref-prefix HEAD"]),
        "0000",
        "an unborn HEAD is left out unless the client asks for it"
    );
    assert_eq!(
        ls_refs("demo", &["symrefs", "unborn"]),
        pkt(&format!("{a} HEAD symref-target:refs/heads/main\n"))
            + &pkt(&format!("{a} refs/heads/main\n"))
            + "0000"
    );
    assert_eq!(
        ls_refs("demo", &["ref-prefix HEAD"]),
        pkt(&format!("{a} HEAD\n")) + "0000"
    );

    // At the `ref-prefix` cap the listing is still answered: the store
    // reads its ranges a bounded number per statement. One statement with
    // all 1,024 was past SQLite's expression depth limit, so this answered
    // `ERR` (#119). One line more is refused before the store is asked.
    let mut prefixes: Vec<String> = (0..1023)
        .map(|i| format!("ref-prefix refs/heads/x{i}"))
        .collect();
    prefixes.push("ref-prefix refs/heads/main".into());
    let args: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    assert_eq!(
        ls_refs("demo", &args),
        pkt(&format!("{a} refs/heads/main\n")) + "0000"
    );
    prefixes.push("ref-prefix refs/tags/".into());
    let args: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    let over = ls_refs("demo", &args);
    assert!(
        over.contains("ERR ") && over.contains("ref-prefix lines"),
        "{over}"
    );
}

/// The response without its `date` header, which differs between calls.
fn without_date(response: &str) -> String {
    response
        .split("\r\n")
        .filter(|l| !l.to_ascii_lowercase().starts_with("date:"))
        .collect::<Vec<_>>()
        .join("\r\n")
}

/// `Basic` credentials for a token (Git's credential form), encoded by hand.
fn basic_token(token: &str) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input = format!("x-access-token:{token}");
    let mut out = String::new();
    for chunk in input.as_bytes().chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            out.push(if i <= chunk.len() {
                T[((n >> (18 - 6 * i)) & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    format!("Basic {out}")
}

/// Invariant: behind B3's guard, a repository the caller may not read
/// answers byte for byte like one that does not exist (names don't leak),
/// on the upload-pack advertisement and on a fetch POST; and the server's
/// own routes (`/-/metrics`, `/-/blobs/...`) are outside the guard, so
/// they reach their handlers even when every Git endpoint is closed to the
/// caller.
#[test]
fn hidden_repositories_look_missing_and_server_routes_skip_the_guard() {
    use aspen_auth::PolicyConfig;

    let env = Env::new();
    let src = env.source();
    let a = env.commit(&src, "f", "x\n");
    // Nobody reads anonymously; the token reads only `other`.
    let s = TestServer::start_with_policy(PolicyConfig::default(), |_| {});
    let seed = s.seeder("demo", "refs/heads/main", &src);
    s.push(&seed, &Push::new("refs/heads/main", &a));
    let auth = basic_token(&mint(&s, &["read:other"]));
    let request = |method: &str, path: &str, body: &str| {
        without_date(&http(
            &s.base_url(),
            &format!(
                "{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\
                 Authorization: {auth}\r\nGit-Protocol: version=2\r\n\
                 Content-Type: application/x-git-upload-pack-request\r\n\
                 Content-Length: {}\r\n\r\n",
                body.len()
            ),
            body.as_bytes(),
        ))
    };
    let ls = pkt("command=ls-refs\n") + "0000";
    for (method, suffix, body) in [
        ("GET", "info/refs?service=git-upload-pack", ""),
        ("POST", "git-upload-pack", ls.as_str()),
    ] {
        let hidden = request(method, &format!("/demo.git/{suffix}"), body);
        let missing = request(method, &format!("/nope.git/{suffix}"), body);
        assert_eq!(hidden, missing, "{suffix}");
        assert!(hidden.starts_with("HTTP/1.1 404 Not Found\r\n"), "{hidden}");
        assert!(
            hidden.ends_with("\r\n\r\nrepository not found\n"),
            "{hidden}"
        );
    }

    let metrics = request("GET", "/-/metrics", "");
    assert!(metrics.starts_with("HTTP/1.1 200"), "{metrics}");
    assert!(metrics.contains("aspen_fetches_total"), "{metrics}");
    // The blob route's own refusal (a bad signature), not the guard's 404.
    let blob = request(
        "GET",
        "/-/blobs/repos/x/packs/y.pack?expires=1&signature=00",
        "",
    );
    assert!(blob.starts_with("HTTP/1.1 403"), "{blob}");
    assert!(blob.ends_with("\r\n\r\nforbidden\n"), "{blob}");
}

/// Invariant: the `/-/blobs` route that serves packfile URIs refuses a
/// URL whose signature does not verify (403, no detail), honours
/// `Range` with `206` and an exact `Content-Range` (a last byte of
/// `u64::MAX` included), and answers a range past the end with an empty
/// `206`, never `416`.
#[test]
fn the_blob_route_checks_signatures_and_serves_ranges() {
    let env = Env::new();
    let src = env.source();
    let a = env.commit(&src, "f.txt", &lines(100, "blob"));
    let s = TestServer::start(|c| c.packfile_uris = true);
    let seed = s.seeder("demo", "refs/heads/main", &src);
    s.push(&seed, &Push::new("refs/heads/main", &a));
    let trace = env.path("uri.trace");
    run(env
        .git(env.tmp.path())
        .env("GIT_TRACE_PACKET", &trace)
        .args([
            "-c",
            "fetch.uriprotocols=http",
            "clone",
            "--quiet",
            &s.url("demo"),
        ])
        .arg(env.path("client")));
    let trace = fs::read_to_string(trace).unwrap();
    let at = trace.find("http://127.0.0.1").expect("a packfile URI");
    let uri: String = trace[at..]
        .chars()
        .take_while(|c| !c.is_whitespace())
        .collect();
    let path = &uri[uri.find("/-/blobs/").unwrap()..];
    let get = |path: &str, range: Option<&str>| {
        let range = range.map_or(String::new(), |r| format!("Range: {r}\r\n"));
        http(
            &s.base_url(),
            &format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n{range}\r\n"),
            b"",
        )
    };
    let header = |response: &str, name: &str| -> String {
        split_response(response)
            .0
            .split("\r\n")
            .find_map(|l| {
                let (k, v) = l.split_once(": ")?;
                k.eq_ignore_ascii_case(name).then(|| v.to_owned())
            })
            .unwrap_or_default()
    };
    let whole = get(path, None);
    assert!(whole.starts_with("HTTP/1.1 200"), "{whole}");
    // A pusher chose these bytes; browsers must not render them (D4).
    assert_eq!(header(&whole, "x-content-type-options"), "nosniff");
    assert_eq!(header(&whole, "content-security-policy"), "sandbox");
    let size: u64 = header(&whole, "content-length").parse().unwrap();
    assert!(size > 32);

    let part = get(path, Some("bytes=0-9"));
    assert!(part.starts_with("HTTP/1.1 206"), "{part}");
    assert_eq!(header(&part, "content-range"), format!("bytes 0-9/{size}"));
    assert_eq!(header(&part, "content-length"), "10");
    assert!(split_response(&part).1.starts_with("PACK"));

    let max = get(path, Some("bytes=5-18446744073709551615"));
    assert!(max.starts_with("HTTP/1.1 206"), "{max}");
    assert_eq!(
        header(&max, "content-range"),
        format!("bytes 5-{}/{size}", size - 1)
    );

    let past = get(path, Some(&format!("bytes={size}-")));
    assert!(past.starts_with("HTTP/1.1 206"), "{past}");
    assert_eq!(header(&past, "content-range"), format!("bytes */{size}"));
    assert_eq!(header(&past, "content-length"), "0");

    let (base, sig) = path.split_at(path.rfind("signature=").unwrap() + "signature=".len());
    let flipped = if sig.starts_with('0') { "1" } else { "0" };
    let forged = format!("{base}{flipped}{}", &sig[1..]);
    let refused = get(&forged, None);
    assert!(refused.starts_with("HTTP/1.1 403"), "{refused}");
    assert_eq!(split_response(&refused).1, "forbidden\n");
}

/// Invariant (issue #203): no packfile URL outlives
/// `aspen_core::pack::MAX_PACK_URL_LIFETIME`, whatever `presign_ttl` is
/// configured; garbage collection's grace period is checked against that
/// cap (`aspen_compact::CompactConfig::validate`), so a pack an orphan's
/// URL points at is never deleted while the URL works.
#[test]
fn presigned_urls_never_outlive_the_pack_url_cap() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let env = Env::new();
    let src = env.source();
    let a = env.commit(&src, "f.txt", &lines(100, "blob"));
    let s = TestServer::start(|c| {
        c.packfile_uris = true;
        // A week: what a backend accepts, far over the cap.
        c.presign_ttl = Duration::from_secs(7 * 24 * 3600);
    });
    let seed = s.seeder("demo", "refs/heads/main", &src);
    s.push(&seed, &Push::new("refs/heads/main", &a));
    let trace = env.path("uri.trace");
    let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    run(env
        .git(env.tmp.path())
        .env("GIT_TRACE_PACKET", &trace)
        .args([
            "-c",
            "fetch.uriprotocols=http",
            "clone",
            "--quiet",
            &s.url("demo"),
        ])
        .arg(env.path("client")));
    let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let trace = fs::read_to_string(trace).unwrap();
    let at = trace.find("http://127.0.0.1").expect("a packfile URI");
    let uri: String = trace[at..]
        .chars()
        .take_while(|c| !c.is_whitespace())
        .collect();
    let expires: u64 = uri
        .split(['?', '&'])
        .find_map(|p| p.strip_prefix("expires="))
        .expect("an expiry")
        .parse()
        .unwrap();
    // The blob store's presigned URL (`upload_pack::deliver::split`, which
    // clamps the TTL), not an express one: this server has no express
    // routes, and its local store signs under `/-/blobs`.
    assert!(
        uri.contains("/-/blobs/") && !uri.contains("/-/express"),
        "{uri} is not a blob-store presigned URL"
    );
    let cap = aspen_core::pack::MAX_PACK_URL_LIFETIME;
    assert!(
        expires <= (after + cap).as_secs() + 1,
        "{uri} expires {}s after the fetch, over the {cap:?} cap",
        expires.saturating_sub(after.as_secs())
    );
    assert!(
        expires >= (before + cap).as_secs(),
        "clamped to the cap, not below"
    );
}

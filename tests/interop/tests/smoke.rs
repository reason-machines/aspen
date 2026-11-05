//! Interop smoke: a stock `git` client clones, pushes and fetches over HTTP,
//! with protocol v2 for fetches, and the result passes `git fsck --strict`.
//!
//! This is the fast interop check that runs on every pull request. The full
//! client matrix runs nightly (see `.github/workflows/nightly.yml`).
//!
//! Two servers: stock `git http-backend` (clone, push, fetch), and Aspen.
//! One Aspen case seeds Aspen's stores one simulated push at a time with
//! `aspen_server::testing`, then clones and fetches with stock `git`:
//! stitched packs, packfile URIs, a depth-1 clone, `fsck --strict`. The
//! other pushes with stock `git` through Aspen's receive-pack (node B1) and
//! clones the result back.

use std::fs;
use std::path::Path;
use std::process::Command;

use aspen_interop::{GitHttpBackend, init_bare, isolated_git};

/// Runs `cmd`, panics with its output if it fails, and returns stdout.
fn run(cmd: &mut Command) -> String {
    let out = cmd.output().expect("spawn git");
    assert!(
        out.status.success(),
        "{cmd:?} failed with {}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8 output")
}

fn git(home: &Path, dir: &Path) -> Command {
    let mut cmd = isolated_git(home);
    cmd.current_dir(dir).args(["-c", "protocol.version=2"]);
    cmd
}

fn commit_file(home: &Path, work: &Path, name: &str, contents: &str) -> String {
    fs::write(work.join(name), contents).unwrap();
    run(git(home, work).args(["add", name]));
    run(git(home, work).args(["commit", "--quiet", "-m", &format!("add {name}")]));
    run(git(home, work).args(["rev-parse", "HEAD"]))
        .trim()
        .to_owned()
}

#[test]
fn clone_push_fetch_over_http_with_protocol_v2() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let served = tmp.path().join("served");
    fs::create_dir_all(&home).unwrap();
    fs::create_dir_all(&served).unwrap();
    init_bare(&served, "demo.git").unwrap();

    let server = GitHttpBackend::start(&served).unwrap();
    let url = format!("{}/demo.git", server.base_url());

    // Writer: clone the empty repository, commit, push a new branch.
    let writer = tmp.path().join("writer");
    run(git(&home, tmp.path())
        .args(["clone", "--quiet", &url])
        .arg(&writer));
    run(git(&home, &writer).args(["switch", "--quiet", "-c", "main"]));
    let first = commit_file(&home, &writer, "README", "hello\n");
    run(git(&home, &writer).args(["push", "--quiet", "origin", "main"]));

    // Reader: clone with packet tracing to prove protocol v2 was negotiated.
    let reader = tmp.path().join("reader");
    let trace = tmp.path().join("trace.log");
    run(git(&home, tmp.path())
        .env("GIT_TRACE_PACKET", &trace)
        .args(["clone", "--quiet", &url])
        .arg(&reader));
    let trace = fs::read_to_string(&trace).unwrap();
    assert!(
        trace.contains("< version 2"),
        "server did not answer with protocol v2; packet trace:\n{trace}"
    );
    let cloned = run(git(&home, &reader).args(["rev-parse", "HEAD"]));
    assert_eq!(cloned.trim(), first);

    // Writer updates main and creates a second branch; reader fetches both.
    let second = commit_file(&home, &writer, "two.txt", "two\n");
    run(git(&home, &writer).args(["push", "--quiet", "origin", "main"]));
    run(git(&home, &writer).args(["switch", "--quiet", "-c", "topic"]));
    let topic = commit_file(&home, &writer, "three.txt", "three\n");
    run(git(&home, &writer).args(["push", "--quiet", "origin", "topic"]));

    run(git(&home, &reader).args(["fetch", "--quiet", "origin"]));
    let fetched_main = run(git(&home, &reader).args(["rev-parse", "origin/main"]));
    let fetched_topic = run(git(&home, &reader).args(["rev-parse", "origin/topic"]));
    assert_eq!(fetched_main.trim(), second);
    assert_eq!(fetched_topic.trim(), topic);

    // `ls-remote` must agree with what was pushed.
    let refs = run(git(&home, &reader).args(["ls-remote", "origin", "refs/heads/*"]));
    assert!(
        refs.contains(&format!("{second}\trefs/heads/main")),
        "{refs}"
    );
    assert!(
        refs.contains(&format!("{topic}\trefs/heads/topic")),
        "{refs}"
    );

    run(git(&home, &reader).args(["fsck", "--strict", "--no-progress"]));
}

#[test]
fn clone_and_fetch_from_aspen_over_http_with_protocol_v2() {
    use aspen_server::testing::{Push, TestServer};
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let src = tmp.path().join("src");
    fs::create_dir_all(&home).unwrap();
    fs::create_dir_all(&src).unwrap();
    run(git(&home, &src).args(["init", "--quiet", "-b", "main"]));
    let first = commit_file(&home, &src, "README", "hello\n");
    let second = commit_file(&home, &src, "two.txt", "two\n");

    let aspen = TestServer::start(|c| c.packfile_uris = true);
    let seed = aspen.seeder("demo", "refs/heads/main", &src);
    aspen.push(&seed, &Push::new("refs/heads/main", &first));
    aspen.push(&seed, &Push::new("refs/heads/main", &second));
    let url = aspen.url("demo");

    // Stitched clone, with packet tracing to prove protocol v2.
    let reader = tmp.path().join("reader");
    let trace = tmp.path().join("trace.log");
    run(git(&home, tmp.path())
        .env("GIT_TRACE_PACKET", &trace)
        .args(["clone", "--quiet", &url])
        .arg(&reader));
    let trace = fs::read_to_string(&trace).unwrap();
    assert!(trace.contains("< version 2"), "{trace}");
    assert_eq!(
        run(git(&home, &reader).args(["rev-parse", "HEAD"])).trim(),
        second
    );
    run(git(&home, &reader).args(["fsck", "--strict", "--no-progress"]));

    // Incremental fetch of a new trunk push and a new branch.
    let third = commit_file(&home, &src, "three.txt", "three\n");
    aspen.push(&seed, &Push::new("refs/heads/main", &third));
    aspen.push(&seed, &Push::new("refs/heads/topic", &second));
    run(git(&home, &reader).args(["fetch", "--quiet", "origin"]));
    assert_eq!(
        run(git(&home, &reader).args(["rev-parse", "origin/main"])).trim(),
        third
    );
    assert_eq!(
        run(git(&home, &reader).args(["rev-parse", "origin/topic"])).trim(),
        second
    );
    run(git(&home, &reader).args(["fsck", "--strict", "--no-progress"]));
    assert_eq!(aspen.metrics().packs_by_uri(), 0);

    // Packfile URIs for a client that opts in.
    let uri = tmp.path().join("uri");
    run(git(&home, tmp.path())
        .args(["-c", "fetch.uriprotocols=http", "clone", "--quiet", &url])
        .arg(&uri));
    run(git(&home, &uri).args(["fsck", "--strict", "--no-progress"]));
    assert_eq!(aspen.metrics().packs_by_uri(), 3, "one URI per stored pack");

    // Depth-1 clone: full closure plus an exact shallow boundary.
    let shallow = tmp.path().join("shallow");
    run(git(&home, tmp.path())
        .args(["clone", "--quiet", "--depth=1", &url])
        .arg(&shallow));
    assert_eq!(
        fs::read_to_string(shallow.join(".git/shallow"))
            .unwrap()
            .trim(),
        third
    );
    run(git(&home, &shallow).args(["fsck", "--strict", "--no-progress"]));
}

#[test]
fn the_latest_git_pushes_to_aspen_and_clones_it_back() {
    use aspen_server::testing::TestServer;
    let s = TestServer::start_with_policy(
        aspen_auth::PolicyConfig {
            anonymous: vec!["write:*".parse().unwrap()],
            ..Default::default()
        },
        |_| {},
    );
    s.block_on(aspen_core::meta::MetaStore::create_repo(
        &s.meta,
        &aspen_core::name::RepoName::new("demo").unwrap(),
        &aspen_core::repo::RepoSettings::new(
            aspen_core::name::RefName::new("refs/heads/main").unwrap(),
        )
        .unwrap(),
    ))
    .unwrap();
    let url = s.url("demo");
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&home).unwrap();
    fs::create_dir_all(&work).unwrap();
    run(git(&home, &work).args(["init", "--quiet", "--initial-branch=main"]));
    let big: String = (0..3000).map(|i| format!("line {i}\n")).collect();
    commit_file(&home, &work, "big.txt", &big);
    run(git(&home, &work).args(["push", "--quiet", &url, "main"]));
    // A thin push (a delta against big.txt), a branch and an annotated tag
    // in one atomic push.
    let main = commit_file(&home, &work, "big.txt", &format!("{big}more\n"));
    run(git(&home, &work).args(["tag", "-a", "v1", "-m", "v1"]));
    run(git(&home, &work).args(["switch", "--quiet", "-c", "topic"]));
    let topic = commit_file(&home, &work, "t.txt", "topic\n");
    run(git(&home, &work).args([
        "push",
        "--quiet",
        "--atomic",
        "--follow-tags",
        &url,
        "main",
        "topic",
    ]));
    let tag = run(git(&home, &work).args(["rev-parse", "v1"]));
    // A rejected non-fast-forward reports Aspen's reason.
    run(git(&home, &work).args(["commit", "--quiet", "--amend", "-m", "rewritten"]));
    let out = git(&home, &work)
        .args(["push", &url, "topic"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("non-fast-forward"), "{err}");

    // Clone it back with protocol v2 and check everything arrived.
    let back = tmp.path().join("back");
    run(git(&home, tmp.path())
        .args(["clone", "--quiet", "--mirror", &url])
        .arg(&back));
    let refs = run(git(&home, &back).args(["for-each-ref", "--format=%(objectname) %(refname)"]));
    for (tip, name) in [
        (main.as_str(), "refs/heads/main"),
        (topic.as_str(), "refs/heads/topic"),
        (tag.trim(), "refs/tags/v1"),
    ] {
        assert!(refs.contains(&format!("{tip} {name}")), "{name}: {refs}");
    }
    run(git(&home, &back).args(["fsck", "--strict", "--no-progress", "--no-dangling"]));
}

//! A fetch storm against a small metadata pool (issue #150).
//!
//! D2's benchmark campaign ran 1,000 concurrent `git fetch --depth=1` jobs
//! of git.git against `aspen dev` and about half failed with "internal
//! storage error; retry": each fetch's handful of metadata reads queued
//! behind every other fetch's for one of the pool's 10 connections, and a
//! read that waited more than sqlx's fixed 30 s acquire deadline failed,
//! although every query took milliseconds once it had a connection. The
//! store now queues its operations itself and fails a waiter only when the
//! pool stops making progress (`aspen_meta`'s `src/admission.rs`).
//!
//! The test here is that storm in miniature. The server's store has one
//! connection and a 2 s acquire timeout. Thousands of depth-1 protocol v2
//! fetches run through the server's router at once, in a loop, while a few
//! probes time single store reads and a few stock `git fetch --depth=1`
//! clients fetch over HTTP. The storm grows until a probe's read has waited
//! more than two acquire timeouts, which a deadline counted from the start
//! of each wait fails; every fetch, every probe read and every git client
//! must succeed. The full 1,000-job storm of git.git is too slow for
//! required CI; it is the `real` step of `bench/campaign/run.sh`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aspen_auth::PolicyConfig;
use aspen_core::meta::MetaStore;
use aspen_core::name::RepoName;
use aspen_meta::StoreOptions;
use aspen_server::testing::TestServer;
use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt as _;
use tower::ServiceExt as _;

/// The store's acquire timeout in this test.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);

/// Stock git clients per round, over HTTP.
const CLIENTS: usize = 8;

/// Tasks that time single store reads.
const PROBES: usize = 16;

fn git(home: &Path, dir: &Path) -> Command {
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
        .args(["-c", "protocol.version=2", "-c", "init.defaultBranch=main"]);
    c
}

fn run(cmd: &mut Command) -> String {
    let out = cmd.output().expect("spawn git");
    assert!(
        out.status.success(),
        "{cmd:?} failed with {}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

/// A small history of `commits` commits on `main`, pushed one by one to the
/// server's `demo` repository. Returns the tip.
fn seed(tmp: &Path, home: &Path, s: &TestServer, commits: usize) -> String {
    let src = tmp.join("src");
    fs::create_dir_all(&src).unwrap();
    run(git(home, &src).args(["init", "--quiet", "-b", "main"]));
    let _ = s.seeder("demo", "refs/heads/main", &src);
    for c in 0..commits {
        for f in 0..4 {
            let body: String = (0..100)
                .map(|i| format!("line {i} of file {f}, commit {c}\n"))
                .collect();
            fs::write(src.join(format!("file{f}.txt")), body).unwrap();
        }
        run(git(home, &src).args(["add", "."]));
        run(git(home, &src).args(["commit", "--quiet", "-m", &format!("commit {c}")]));
        run(git(home, &src)
            .args(["push", "--quiet", &s.url("demo")])
            .arg("HEAD:refs/heads/main"));
    }
    run(git(home, &src).args(["rev-parse", "HEAD"]))
        .trim()
        .to_owned()
}

fn pkt(s: &str) -> String {
    format!("{:04x}{s}", s.len() + 4)
}

/// The body of a protocol v2 `git fetch --depth=1` of `tip`, as stock git
/// sends it.
fn depth_one_fetch(tip: &str) -> Vec<u8> {
    let mut body = pkt("command=fetch\n") + "0001";
    for line in [
        "thin-pack",
        "no-progress",
        "ofs-delta",
        &format!("want {tip}"),
        "deepen 1",
        "done",
    ] {
        body += &pkt(&format!("{line}\n"));
    }
    body += "0000";
    body.into_bytes()
}

/// What one round saw.
#[derive(Default)]
struct Seen {
    fetches: AtomicU64,
    fetch_failures: AtomicU64,
    reads: AtomicU64,
    read_failures: AtomicU64,
    longest_read_us: AtomicU64,
    first_failure: Mutex<Option<String>>,
}

impl Seen {
    fn fail(&self, counter: &AtomicU64, why: String) {
        counter.fetch_add(1, Ordering::Relaxed);
        self.first_failure.lock().unwrap().get_or_insert(why);
    }
}

/// Invariant (issue #150): a fetch storm whose metadata reads queue for the
/// pool far longer than the acquire timeout is served in full. Every
/// depth-1 fetch succeeds, in process and from stock git, at a load where
/// a single store read waited more than two acquire timeouts.
#[test]
fn a_fetch_storm_queues_for_a_small_pool_instead_of_failing() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let policy = PolicyConfig {
        anonymous: vec!["write:*".parse().unwrap()],
        ..PolicyConfig::default()
    };
    let store = StoreOptions {
        max_connections: 1,
        acquire_timeout: ACQUIRE_TIMEOUT,
        ..StoreOptions::default()
    };
    let s = TestServer::start_with_store(policy, &store, |_| {}, |server, _, _| server);
    let tip = seed(tmp.path(), &home, &s, 5);
    let name: RepoName = "demo".parse().unwrap();
    let router = s.server().router(Arc::clone(&s.access));
    let request = Arc::new(depth_one_fetch(&tip));

    let mut fetches = 1024usize;
    for round in 0.. {
        let stop = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(Seen::default());
        let dirs: Vec<PathBuf> = (0..CLIENTS)
            .map(|j| {
                let d = tmp.path().join(format!("r{round}-{j}"));
                fs::create_dir_all(&d).unwrap();
                run(git(&home, &d).args(["init", "--quiet"]));
                d
            })
            .collect();
        let clients: Vec<Result<(), String>> = std::thread::scope(|scope| {
            // The storm and the probes run on the server's runtime, as
            // fetch handlers do, and share its store.
            let storm = {
                let (stop, seen, meta, name) = (
                    Arc::clone(&stop),
                    Arc::clone(&seen),
                    s.meta.clone(),
                    name.clone(),
                );
                let (router, request, s) = (router.clone(), Arc::clone(&request), &s);
                scope.spawn(move || {
                    s.block_on(async move {
                        let mut tasks = tokio::task::JoinSet::new();
                        for _ in 0..fetches {
                            let (stop, seen, router, request) = (
                                Arc::clone(&stop),
                                Arc::clone(&seen),
                                router.clone(),
                                Arc::clone(&request),
                            );
                            tasks.spawn(async move {
                                while !stop.load(Ordering::Relaxed) {
                                    let req = Request::post("/demo.git/git-upload-pack")
                                        .header("git-protocol", "version=2")
                                        .header(
                                            "content-type",
                                            "application/x-git-upload-pack-request",
                                        )
                                        .body(Body::from(request.as_ref().clone()))
                                        .unwrap();
                                    let resp = router.clone().oneshot(req).await.unwrap();
                                    let status = resp.status();
                                    let body =
                                        resp.into_body().collect().await.map(|b| b.to_bytes());
                                    seen.fetches.fetch_add(1, Ordering::Relaxed);
                                    match body {
                                        Ok(body)
                                            if status.is_success()
                                                && body.ends_with(b"0000")
                                                && !body.windows(4).any(|w| w == b"ERR ") =>
                                        {
                                            let text = String::from_utf8_lossy(&body);
                                            if text.contains("error") {
                                                seen.fail(&seen.fetch_failures, text.into_owned());
                                            }
                                        }
                                        Ok(body) => seen.fail(
                                            &seen.fetch_failures,
                                            format!("{status}: {}", String::from_utf8_lossy(&body)),
                                        ),
                                        Err(e) => seen.fail(&seen.fetch_failures, e.to_string()),
                                    }
                                }
                            });
                        }
                        for _ in 0..PROBES {
                            let (stop, seen, meta, name) = (
                                Arc::clone(&stop),
                                Arc::clone(&seen),
                                meta.clone(),
                                name.clone(),
                            );
                            tasks.spawn(async move {
                                while !stop.load(Ordering::Relaxed) {
                                    let started = Instant::now();
                                    let read = meta.repo(&name).await;
                                    let waited = started.elapsed().as_micros() as u64;
                                    seen.longest_read_us.fetch_max(waited, Ordering::Relaxed);
                                    seen.reads.fetch_add(1, Ordering::Relaxed);
                                    if let Err(e) = read {
                                        seen.fail(&seen.read_failures, e.to_string());
                                    }
                                }
                            });
                        }
                        while tasks.join_next().await.is_some() {}
                    })
                })
            };
            // Let the queue fill before the git clients arrive.
            std::thread::sleep(ACQUIRE_TIMEOUT * 2);
            let clients: Vec<_> = dirs
                .iter()
                .map(|d| {
                    let (home, url, tip) = (home.clone(), s.url("demo"), tip.clone());
                    scope.spawn(move || {
                        let out = git(&home, d)
                            .args(["fetch", "--quiet", "--no-tags", "--depth=1", &url])
                            .arg(format!("+{tip}:refs/remotes/origin/main"))
                            .output()
                            .expect("spawn git");
                        if out.status.success() {
                            Ok(())
                        } else {
                            Err(String::from_utf8_lossy(&out.stderr).into_owned())
                        }
                    })
                })
                .collect();
            let done = clients.into_iter().map(|c| c.join().unwrap()).collect();
            stop.store(true, Ordering::Relaxed);
            storm.join().unwrap();
            done
        });

        let longest = Duration::from_micros(seen.longest_read_us.load(Ordering::Relaxed));
        let failed_clients: Vec<&String> =
            clients.iter().filter_map(|r| r.as_ref().err()).collect();
        let (n, nf) = (
            seen.fetches.load(Ordering::Relaxed),
            seen.fetch_failures.load(Ordering::Relaxed),
        );
        let (r, rf) = (
            seen.reads.load(Ordering::Relaxed),
            seen.read_failures.load(Ordering::Relaxed),
        );
        eprintln!(
            "{fetches} concurrent depth-1 fetches: {n} served, {nf} failed; {r} probe reads, \
             {rf} failed, longest {longest:?}; {} of {CLIENTS} git clients failed",
            failed_clients.len()
        );
        assert_eq!(
            (nf, rf),
            (0, 0),
            "fetches or store reads failed at {fetches} concurrent fetches (longest read \
             {longest:?}); first: {:?}",
            seen.first_failure.lock().unwrap()
        );
        assert!(
            failed_clients.is_empty(),
            "{} of {CLIENTS} git clients failed; first: {}",
            failed_clients.len(),
            failed_clients[0]
        );
        for d in &dirs {
            let got = run(git(&home, d).args(["rev-parse", "refs/remotes/origin/main"]));
            assert_eq!(got.trim(), tip);
            assert_eq!(
                fs::read_to_string(d.join(".git/shallow")).unwrap().trim(),
                tip,
                "a depth-1 fetch"
            );
        }
        assert!(n > 0, "the storm served fetches");
        if longest > ACQUIRE_TIMEOUT * 2 {
            break;
        }
        assert!(
            fetches < 1 << 17,
            "no storm made a store read wait past two acquire timeouts"
        );
        fetches *= 2;
    }
}

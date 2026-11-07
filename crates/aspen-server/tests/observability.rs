//! Observability (node C5): `/-/metrics` as Prometheus scrapes it, driven
//! by stock `git` pushing to and fetching from a running server.
//!
//! Every test that reads the endpoint also lints the whole exposition
//! against the text format (one `HELP`/`TYPE` per family, samples inside
//! their family, valid names and labels, cumulative histograms), and the
//! shipped dashboard and alert rules are checked against the names the
//! server actually exports.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use aspen_auth::{AuthService, Limits, LimitsConfig, MemoryTokenStore, PolicyConfig};
use aspen_blob::{AnyBlobStore, LocalBlobStore};
use aspen_core::blob::{BlobCaller, BlobKey, BlobStore as _, ByteRange};
use aspen_core::error::RejectReason;
use aspen_core::id::JobId;
use aspen_core::id::PackChecksum;
use aspen_core::meta::{JobKind, JobState, JobTag, MAX_JOB_ATTEMPTS, MetaStore, NewJob};
use aspen_core::pack::{NewPack, PackOrigin, entry_table_len};
use aspen_meta::SqlStore;
use aspen_pack::MaxObjectBytes;
use aspen_server::access::{Access, ClientAddr};
use aspen_server::testing::{Push, TestServer};
use aspen_server::{Server, ServerConfig};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::ServiceExt as _;

// ---------------------------------------------------------------------------
// The exposition, parsed and linted

#[derive(Debug)]
struct Family {
    kind: String,
    /// (sample name, labels, value)
    samples: Vec<(String, BTreeMap<String, String>, f64)>,
}

#[derive(Debug)]
struct Exposition {
    families: BTreeMap<String, Family>,
}

fn valid_name(name: &str, label: bool) -> bool {
    let mut c = name.chars();
    let first = |ch: char| ch.is_ascii_alphabetic() || ch == '_' || (!label && ch == ':');
    c.next().is_some_and(first) && c.all(|ch| first(ch) || ch.is_ascii_digit())
}

/// Parses `name{k="v",...} value`, undoing the format's escapes.
fn parse_sample(line: &str) -> (String, BTreeMap<String, String>, f64) {
    let name_end = line.find(['{', ' ']).unwrap_or_else(|| panic!("{line}"));
    let name = line[..name_end].to_owned();
    let mut labels = BTreeMap::new();
    let mut rest = &line[name_end..];
    if let Some(r) = rest.strip_prefix('{') {
        let mut chars = r.char_indices().peekable();
        loop {
            let start = chars.peek().map(|(i, _)| *i).unwrap();
            if r[start..].starts_with('}') {
                chars.next();
                break;
            }
            let eq = r[start..].find('=').unwrap_or_else(|| panic!("{line}")) + start;
            let key = &r[start..eq];
            assert!(valid_name(key, true), "label name {key:?} in {line}");
            while chars.peek().is_some_and(|(i, _)| *i <= eq) {
                chars.next();
            }
            assert_eq!(chars.next().map(|(_, c)| c), Some('"'), "{line}");
            let mut value = String::new();
            loop {
                match chars
                    .next()
                    .unwrap_or_else(|| panic!("unterminated: {line}"))
                {
                    (_, '\\') => match chars.next().map(|(_, c)| c) {
                        Some('\\') => value.push('\\'),
                        Some('"') => value.push('"'),
                        Some('n') => value.push('\n'),
                        other => panic!("bad escape {other:?} in {line}"),
                    },
                    (_, '"') => break,
                    (_, c) => value.push(c),
                }
            }
            assert!(
                labels.insert(key.to_owned(), value).is_none(),
                "repeated label {key} in {line}"
            );
            match chars.next().map(|(_, c)| c) {
                Some(',') => {}
                Some('}') => break,
                other => panic!("expected , or }} after a label, got {other:?}: {line}"),
            }
        }
        let consumed = chars.peek().map_or(r.len(), |(i, _)| *i);
        rest = &r[consumed..];
    }
    let value = rest
        .strip_prefix(' ')
        .unwrap_or_else(|| panic!("no value: {line}"));
    let value = match value {
        "+Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        "NaN" => f64::NAN,
        v => v.parse().unwrap_or_else(|_| panic!("value {v:?}: {line}")),
    };
    (name, labels, value)
}

/// Parses the text format, panicking on anything Prometheus would reject
/// or that breaks the conventions Aspen keeps.
fn lint(text: &str) -> Exposition {
    let mut families: BTreeMap<String, Family> = BTreeMap::new();
    let mut current: Option<String> = None;
    let mut lines = text.lines().peekable();
    let mut series = BTreeSet::new();
    while let Some(line) = lines.next() {
        if let Some(h) = line.strip_prefix("# HELP ") {
            let (name, help) = h.split_once(' ').unwrap_or_else(|| panic!("{line}"));
            assert!(valid_name(name, false), "family name {name:?}");
            assert!(name.starts_with("aspen_"), "unprefixed family {name}");
            assert!(!help.trim().is_empty(), "{name} has no help");
            let t = lines
                .next()
                .unwrap_or_else(|| panic!("no TYPE after {line}"));
            let kind = t
                .strip_prefix(&format!("# TYPE {name} "))
                .unwrap_or_else(|| panic!("TYPE of {name} must follow its HELP: {t}"));
            assert!(
                ["counter", "gauge", "histogram"].contains(&kind),
                "{name}: {kind}"
            );
            if kind == "counter" {
                assert!(
                    name.ends_with("_total"),
                    "counter {name} must end in _total"
                );
            }
            assert!(
                families
                    .insert(
                        name.to_owned(),
                        Family {
                            kind: kind.to_owned(),
                            samples: Vec::new()
                        }
                    )
                    .is_none(),
                "family {name} appears twice"
            );
            current = Some(name.to_owned());
            continue;
        }
        assert!(!line.starts_with('#'), "stray comment {line}");
        assert!(!line.is_empty(), "empty line");
        let fam_name = current
            .clone()
            .unwrap_or_else(|| panic!("sample before any family: {line}"));
        let (name, labels, value) = parse_sample(line);
        let fam = families.get_mut(&fam_name).unwrap();
        let allowed: Vec<String> = if fam.kind == "histogram" {
            ["_bucket", "_sum", "_count"]
                .iter()
                .map(|s| format!("{fam_name}{s}"))
                .collect()
        } else {
            vec![fam_name.clone()]
        };
        assert!(
            allowed.contains(&name),
            "{name} outside its family {fam_name}"
        );
        if fam.kind == "counter" {
            assert!(value >= 0.0, "negative counter {line}");
        }
        assert!(
            series.insert(format!("{name}{labels:?}")),
            "series repeated: {line}"
        );
        fam.samples.push((name, labels, value));
    }
    for (name, f) in &families {
        if f.kind != "histogram" {
            continue;
        }
        let buckets: Vec<(f64, f64)> = f
            .samples
            .iter()
            .filter(|(n, _, _)| n.ends_with("_bucket"))
            .map(|(_, l, v)| {
                let le = &l["le"];
                let le = if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().unwrap()
                };
                (le, *v)
            })
            .collect();
        assert!(
            buckets
                .windows(2)
                .all(|w| w[0].0 < w[1].0 && w[0].1 <= w[1].1),
            "{name}: buckets must grow"
        );
        assert_eq!(buckets.last().map(|b| b.0), Some(f64::INFINITY), "{name}");
        let count = f
            .samples
            .iter()
            .find(|(n, _, _)| n.ends_with("_count"))
            .unwrap()
            .2;
        assert_eq!(
            buckets.last().unwrap().1,
            count,
            "{name}: +Inf equals _count"
        );
    }
    Exposition { families }
}

impl Exposition {
    /// The value of the one sample of `name` carrying `labels`.
    fn value(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        let family = self
            .families
            .get(name)
            .or_else(|| {
                ["_bucket", "_sum", "_count"]
                    .iter()
                    .find_map(|s| self.families.get(name.strip_suffix(s)?))
            })
            .unwrap_or_else(|| panic!("no family for {name}"));
        let want: BTreeMap<String, String> = labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        family
            .samples
            .iter()
            .find(|(n, l, _)| n == name && *l == want)
            .unwrap_or_else(|| panic!("no sample {name}{labels:?}"))
            .2
    }

    /// Like [`Exposition::value`], but 0 when the sample is not exported
    /// yet.
    fn value_or_zero(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        let want: BTreeMap<String, String> = labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        self.families
            .get(name)
            .and_then(|f| f.samples.iter().find(|(n, l, _)| n == name && *l == want))
            .map_or(0.0, |s| s.2)
    }

    /// Whether `token` (a family, or a histogram's `_bucket`, `_sum` or
    /// `_count`) is exported.
    fn exports(&self, token: &str) -> bool {
        self.families.contains_key(token)
            || ["_bucket", "_sum", "_count"].iter().any(|s| {
                token
                    .strip_suffix(s)
                    .and_then(|f| self.families.get(f))
                    .is_some_and(|f| f.kind == "histogram")
            })
    }
}

// ---------------------------------------------------------------------------
// Git helpers

struct Env {
    tmp: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("home")).unwrap();
        Self { tmp }
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
            .args(["-c", "protocol.version=2", "-c", "init.defaultBranch=main"]);
        c
    }

    fn source(&self) -> PathBuf {
        let src = self.path("src");
        fs::create_dir_all(&src).unwrap();
        run(self.git(&src).args(["init", "--quiet", "-b", "main"]));
        src
    }

    fn commit(&self, dir: &Path, file: &str, contents: &str) -> String {
        fs::write(dir.join(file), contents).unwrap();
        run(self.git(dir).args(["add", file]));
        run(self
            .git(dir)
            .args(["commit", "--quiet", "-m", &format!("{file}: {contents}")]));
        run(self.git(dir).args(["rev-parse", "HEAD"]))
            .trim()
            .to_owned()
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

/// A test server anyone may push to and read from.
fn writable_server() -> TestServer {
    let policy = PolicyConfig {
        anonymous: vec!["write:*".parse().unwrap()],
        ..PolicyConfig::default()
    };
    TestServer::start_with_policy(policy, |_| {})
}

/// `GET /-/metrics` on the server's listener: status, content type, body.
fn scrape(s: &TestServer) -> (u16, String, String) {
    let server = s.server().clone();
    s.block_on(async move {
        let response = server
            .metrics_router()
            .oneshot(Request::get("/-/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status().as_u16();
        let ct = response
            .headers()
            .get("content-type")
            .map(|v| v.to_str().unwrap().to_owned())
            .unwrap_or_default();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, ct, String::from_utf8(body.to_vec()).unwrap())
    })
}

fn metrics(s: &TestServer) -> Exposition {
    let (status, ct, body) = scrape(s);
    assert_eq!(status, 200);
    assert_eq!(ct, "text/plain; version=0.0.4; charset=utf-8");
    lint(&body)
}

// ---------------------------------------------------------------------------
// Tests

/// By default `/-/metrics` is not on the Git listener (the public one):
/// there it is a non-Git path and the access guard answers `404`, while
/// [`Server::metrics_router`] serves it, and only it, on a listener of its
/// own. `metrics_on_git_listener` puts it back on the Git router.
#[tokio::test]
async fn metrics_are_served_on_their_own_listener_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let meta = SqlStore::connect(&url).await.unwrap();
    meta.migrate().await.unwrap();
    let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let access = || {
        Arc::new(Access::new(
            AuthService::new(PolicyConfig::default(), MemoryTokenStore::new()).unwrap(),
            Limits::new(LimitsConfig::default()),
            meta.clone(),
            ClientAddr::Peer,
        ))
    };
    let get = |router: axum::Router, path: &'static str| async move {
        let r = router
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = r.status();
        let body = r.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    };
    let config = ServerConfig::new(MaxObjectBytes::new(1 << 20).unwrap());
    assert!(!config.metrics_on_git_listener);
    let server =
        Server::new(meta.clone(), blobs.clone(), config.clone()).with_store_gauges(Duration::ZERO);
    let (status, _) = get(server.router(access()), "/-/metrics").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "not on the Git listener");
    let (status, body) = get(server.metrics_router(), "/-/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let e = lint(&body);
    assert_eq!(e.value("aspen_meta_stats_up", &[]), 1.0);
    let (status, _) = get(server.metrics_router(), "/demo.git/info/refs").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the metrics listener serves nothing else"
    );

    let mut on_git = config;
    on_git.metrics_on_git_listener = true;
    let server = Server::new(meta.clone(), blobs, on_git);
    let (status, body) = get(server.router(access()), "/-/metrics").await;
    assert_eq!(status, StatusCode::OK);
    lint(&body);
}

/// Stock `git` pushes and fetches are counted as they happened: pushes by
/// result, ref updates by status (an accepted update, a branch create, and
/// a non-fast-forward refused with its reason), the trunk version's
/// closure, push durations and the fetch planner's `visited`; the
/// database gauges count the repository's exact trunk. The whole
/// exposition passes the format lint.
#[test]
fn pushes_and_fetches_are_counted_by_result() {
    let env = Env::new();
    let s = writable_server();
    let src = env.source();
    s.seeder("acme/obs", "refs/heads/main", &src);
    let url = s.url("acme/obs");
    env.commit(&src, "a.txt", "one");
    run(env.git(&src).args(["push", "--quiet", &url, "main"]));
    env.commit(&src, "b.txt", "two");
    run(env
        .git(&src)
        .args(["push", "--quiet", &url, "main:refs/heads/topic"]));
    // A divergent topic: the client never saw topic (trunk-only
    // advertisement), so the server refuses it as a non-fast-forward.
    run(env
        .git(&src)
        .args(["checkout", "--quiet", "-b", "other", "main~1"]));
    env.commit(&src, "c.txt", "three");
    let out = env
        .git(&src)
        .args(["push", &url, "other:refs/heads/topic"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("non-fast-forward"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    run(env
        .git(env.tmp.path())
        .args(["clone", "--quiet", &url, "clone"]));

    let m = metrics(&s);
    assert_eq!(
        m.value("aspen_pushes_total", &[("result", "reported")]),
        3.0
    );
    for r in ["fatal", "unpack_failed", "store_error"] {
        assert_eq!(m.value("aspen_pushes_total", &[("result", r)]), 0.0, "{r}");
    }
    assert_eq!(m.value("aspen_push_duration_seconds_count", &[]), 3.0);
    assert!(m.value("aspen_push_received_bytes_total", &[]) > 0.0);
    assert_eq!(
        m.value("aspen_push_ref_updates_total", &[("result", "ok")]),
        2.0
    );
    assert_eq!(
        m.value(
            "aspen_push_ref_updates_total",
            &[("result", "non_fast_forward")]
        ),
        1.0
    );
    assert_eq!(
        m.value("aspen_push_trunk_versions_total", &[("closure", "exact")]),
        1.0,
        "the topic pushes are not trunk"
    );
    assert_eq!(
        m.value("aspen_push_trunk_versions_total", &[("closure", "wider")]),
        0.0
    );
    assert!(m.value("aspen_push_stage_seconds_total", &[("stage", "complete")]) > 0.0);
    assert!(m.value("aspen_push_prefetch_slots", &[]) >= 1.0);
    assert_eq!(m.value("aspen_trunk_repos", &[("closure", "exact")]), 1.0);
    assert_eq!(m.value("aspen_trunk_repos", &[("closure", "wider")]), 0.0);
    assert!(m.value("aspen_fetches_total", &[]) >= 1.0);
    assert_eq!(
        m.value("aspen_fetch_plan_visited_versions_count", &[]),
        m.value("aspen_fetches_total", &[])
    );
    assert_eq!(
        m.value("aspen_fetch_plan_duration_seconds_count", &[]),
        m.value("aspen_fetches_total", &[])
    );
    assert_eq!(
        m.value("aspen_build_info", &[("version", aspen_core::VERSION)]),
        1.0
    );
    let rp = s.server().receive_pack().unwrap().metrics();
    assert_eq!(rp.refs_rejected(RejectReason::NonFastForward), 1);
}

/// Wider trunk versions are visible two ways: a repository whose current
/// trunk version is wider is counted by the database gauge, and a push
/// that commits a trunk version inheriting that wider closure is counted
/// (and logged) as it happens. The gauge counts only the current trunk.
#[test]
fn wider_trunk_versions_show_in_the_gauge_and_the_push_counter() {
    let env = Env::new();
    let s = writable_server();
    let src = env.source();
    let base = env.commit(&src, "base.txt", "trunk");
    run(env.git(&src).args(["checkout", "--quiet", "-b", "side"]));
    let loose = env.commit(&src, "loose.txt", "never reachable from main");
    run(env.git(&src).args(["checkout", "--quiet", "main"]));
    let seeder = s.seeder("acme/wide", "refs/heads/main", &src);
    // Seed a wider trunk: its pack also holds an object main never reaches.
    let mut push = Push::new("refs/heads/main", &base);
    push.extra = vec![loose];
    s.push(&seeder, &push);
    // A clean repository next to it.
    let clean = env.path("clean");
    fs::create_dir_all(&clean).unwrap();
    run(env.git(&clean).args(["init", "--quiet", "-b", "main"]));
    env.commit(&clean, "x.txt", "x");
    s.seeder("acme/clean", "refs/heads/main", &clean);
    run(env
        .git(&clean)
        .args(["push", "--quiet", &s.url("acme/clean"), "main"]));

    let m = metrics(&s);
    assert_eq!(m.value("aspen_trunk_repos", &[("closure", "wider")]), 1.0);
    assert_eq!(m.value("aspen_trunk_repos", &[("closure", "exact")]), 1.0);
    assert_eq!(
        m.value("aspen_push_trunk_versions_total", &[("closure", "wider")]),
        0.0,
        "seeding is not a push"
    );

    // A stock fast-forward on top inherits the wider closure.
    env.commit(&src, "next.txt", "next");
    run(env
        .git(&src)
        .args(["push", "--quiet", &s.url("acme/wide"), "main"]));
    let m = metrics(&s);
    assert_eq!(
        m.value("aspen_push_trunk_versions_total", &[("closure", "wider")]),
        1.0
    );
    assert_eq!(
        m.value("aspen_push_trunk_versions_total", &[("closure", "exact")]),
        1.0,
        "acme/clean's push"
    );
    assert_eq!(m.value("aspen_trunk_repos", &[("closure", "wider")]), 1.0);
}

/// Dead jobs are exported per kind (`aspen_jobs{state="dead"}`, which the
/// shipped alert pages on), every kind and state is present even at zero,
/// and requeueing the job (what `aspen doctor jobs requeue` runs) clears
/// it.
#[test]
fn dead_jobs_are_exported_per_kind_until_requeued() {
    let s = writable_server();
    let m = metrics(&s);
    for kind in JobTag::ALL {
        for state in ["queued", "running", "dead"] {
            assert_eq!(
                m.value("aspen_jobs", &[("kind", kind.as_str()), ("state", state)]),
                0.0
            );
        }
        assert_eq!(
            m.value("aspen_jobs_queued_lag_seconds", &[("kind", kind.as_str())]),
            0.0
        );
    }
    let meta = s.meta.clone();
    let dead = s.block_on(async move { kill_a_job(&meta, "acme/jobs").await });
    let m = metrics(&s);
    assert_eq!(
        m.value("aspen_jobs", &[("kind", "rewrite_pack"), ("state", "dead")]),
        1.0
    );
    assert_eq!(
        m.value(
            "aspen_jobs",
            &[("kind", "index_commits"), ("state", "dead")]
        ),
        0.0
    );
    let meta = s.meta.clone();
    s.block_on(async move { meta.requeue_dead_job(dead).await.unwrap() });
    let m = metrics(&s);
    assert_eq!(
        m.value("aspen_jobs", &[("kind", "rewrite_pack"), ("state", "dead")]),
        0.0
    );
    assert_eq!(
        m.value(
            "aspen_jobs",
            &[("kind", "rewrite_pack"), ("state", "queued")]
        ),
        1.0
    );
}

/// Creates `repo` with one pack and a `rewrite_pack` job on it, then
/// claims the job until it is dead, as a worker that crashes every time
/// would (each lease expires).
async fn kill_a_job(meta: &SqlStore, repo: &str) -> JobId {
    let repo = meta
        .create_repo(
            &aspen_core::name::RepoName::new(repo).unwrap(),
            &aspen_core::repo::RepoSettings::new(
                aspen_core::name::RefName::new("refs/heads/main").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let pack = meta
        .register_pack(&NewPack {
            repo: repo.id,
            checksum: PackChecksum::from_bytes([7; 20]),
            size: 100,
            idx_size: 1072 + 28,
            entries_size: entry_table_len(1),
            object_count: 1,
            ref_delta_count: 0,
            origin: PackOrigin::Push { received_bytes: 80 },
        })
        .await
        .unwrap()
        .id;
    let id = meta
        .enqueue_jobs(&[NewJob {
            repo: repo.id,
            kind: JobKind::RewritePack { pack },
            run_after: aspen_core::id::UnixMicros(0),
        }])
        .await
        .unwrap()[0];
    for _ in 0..=MAX_JOB_ATTEMPTS {
        let _ = meta
            .claim_job(&[JobTag::RewritePack], "crashing", Duration::from_micros(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(meta.job(id).await.unwrap().unwrap().state, JobState::Dead);
    id
}

/// A SQLite store in `dir`, migrated, with a pool of at most
/// `connections`.
async fn sqlite_store(dir: &Path, connections: u32) -> SqlStore {
    let url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
    let opts = aspen_meta::StoreOptions {
        max_connections: connections,
        ..aspen_meta::StoreOptions::default()
    };
    let meta = SqlStore::connect_with(&url, &opts).await.unwrap();
    meta.migrate().await.unwrap();
    meta
}

fn server_config() -> ServerConfig {
    ServerConfig::new(MaxObjectBytes::new(1 << 20).unwrap())
}

/// Requests behind trusted proxies that are keyed by their TCP peer
/// because no forwarded address could be read are counted by reason on
/// `/-/metrics` (issue #121), so a missing proxy hop or a direct path
/// past the proxy shows up instead of passing silently.
#[tokio::test]
async fn client_address_fallbacks_are_exported_by_reason() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let meta = SqlStore::connect(&url).await.unwrap();
    meta.migrate().await.unwrap();
    let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let access = Arc::new(Access::new(
        AuthService::new(PolicyConfig::default(), MemoryTokenStore::new()).unwrap(),
        Limits::new(LimitsConfig::default()),
        meta.clone(),
        ClientAddr::from_settings(1, Some("10.0.0.0/8")).unwrap(),
    ));
    let server = Server::new(meta, blobs, server_config());
    let git = server.router(access);
    let scrape = || async {
        let r = server
            .metrics_router()
            .oneshot(Request::get("/-/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = r.into_body().collect().await.unwrap().to_bytes();
        lint(std::str::from_utf8(&body).unwrap())
    };
    let fallbacks = |e: &Exposition, reason: &str| {
        e.value(
            "aspen_client_address_fallbacks_total",
            &[("reason", reason)],
        )
    };
    let e = scrape().await;
    for reason in ["untrusted_peer", "missing", "malformed"] {
        assert_eq!(fallbacks(&e, reason), 0.0, "{reason}");
    }
    for (peer, xff) in [
        ("198.51.100.7:1", Some("203.0.113.9")),
        ("10.0.0.2:1", None),
        ("10.0.0.2:1", Some("unknown")),
        ("10.0.0.2:1", Some("203.0.113.9")),
    ] {
        let mut r = Request::get("/acme/x.git/info/refs?service=git-upload-pack");
        if let Some(v) = xff {
            r = r.header("x-forwarded-for", v);
        }
        let mut r = r.body(Body::empty()).unwrap();
        r.extensions_mut().insert(axum::extract::ConnectInfo(
            peer.parse::<std::net::SocketAddr>().unwrap(),
        ));
        git.clone().oneshot(r).await.unwrap();
    }
    let e = scrape().await;
    for reason in ["untrusted_peer", "missing", "malformed"] {
        assert_eq!(fallbacks(&e, reason), 1.0, "{reason}");
    }
}

/// The database gauges are read at most once per refresh interval: a job
/// that dies after a scrape does not show on the next scrape within the
/// interval, and the read's timestamp does not move. A server over the
/// same database that refreshes on every scrape does see it, so the job
/// really died.
#[tokio::test]
async fn store_gauges_are_read_at_most_once_per_refresh_interval() {
    let dir = tempfile::tempdir().unwrap();
    let meta = sqlite_store(dir.path(), 10).await;
    let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let dead = [("kind", "rewrite_pack"), ("state", "dead")];
    let hourly = Server::new(meta.clone(), blobs.clone(), server_config())
        .with_store_gauges(Duration::from_secs(3600));
    let first = lint(&hourly.render_metrics().await);
    assert_eq!(first.value("aspen_meta_stats_up", &[]), 1.0);
    assert_eq!(first.value("aspen_jobs", &dead), 0.0);
    let read_at = first.value("aspen_meta_stats_timestamp_seconds", &[]);

    kill_a_job(&meta, "acme/jobs").await;
    // Long enough for a fresh read's timestamp (microseconds) to differ.
    tokio::time::sleep(Duration::from_millis(5)).await;

    let second = lint(&hourly.render_metrics().await);
    assert_eq!(second.value("aspen_jobs", &dead), 0.0, "not read again");
    assert_eq!(
        second.value("aspen_meta_stats_timestamp_seconds", &[]),
        read_at
    );
    assert_eq!(second.value("aspen_meta_stats_up", &[]), 1.0);

    let every = Server::new(meta, blobs, server_config()).with_store_gauges(Duration::ZERO);
    let fresh = lint(&every.render_metrics().await);
    assert_eq!(fresh.value("aspen_jobs", &dead), 1.0);
    assert!(fresh.value("aspen_meta_stats_timestamp_seconds", &[]) > read_at);
}

/// A read of the database gauges that hangs (here: every pooled connection
/// is held, so the read waits on the pool) is abandoned after
/// `store_gauges_timeout`: the scrape answers in time with the last
/// values, `aspen_meta_stats_up` 0 and the failure counted. Once the
/// database answers again, so do the gauges.
#[tokio::test]
async fn a_hung_database_read_times_out_and_serves_the_last_values() {
    let dir = tempfile::tempdir().unwrap();
    let meta = sqlite_store(dir.path(), 1).await;
    let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let mut config = server_config();
    config.store_gauges_timeout = Duration::from_millis(200);
    let server = Server::new(meta.clone(), blobs, config).with_store_gauges(Duration::ZERO);
    let before = lint(&server.render_metrics().await);
    assert_eq!(before.value("aspen_meta_stats_up", &[]), 1.0);
    let read_at = before.value("aspen_meta_stats_timestamp_seconds", &[]);

    let held = meta.pool().acquire().await.unwrap();
    let started = std::time::Instant::now();
    let during = lint(&server.render_metrics().await);
    let took = started.elapsed();
    // sqlx would wait 30 s for a connection; the timeout answers first.
    assert!(took < Duration::from_secs(5), "the scrape took {took:?}");
    assert_eq!(during.value("aspen_meta_stats_up", &[]), 0.0);
    assert_eq!(during.value("aspen_meta_stats_failures_total", &[]), 1.0);
    assert_eq!(
        during.value("aspen_meta_stats_timestamp_seconds", &[]),
        read_at,
        "the last values are kept"
    );
    assert_eq!(
        during.value("aspen_jobs", &[("kind", "rewrite_pack"), ("state", "dead")]),
        0.0
    );

    drop(held);
    let after = lint(&server.render_metrics().await);
    assert_eq!(after.value("aspen_meta_stats_up", &[]), 1.0);
    assert!(after.value("aspen_meta_stats_timestamp_seconds", &[]) > read_at);
}

fn repo_file(rel: &str) -> String {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(rel);
    fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

/// Every `aspen_*` name in `text`.
fn referenced(text: &str) -> BTreeSet<String> {
    let bytes = text.as_bytes();
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = BTreeSet::new();
    let mut i = 0;
    while let Some(off) = text[i..].find("aspen_") {
        let start = i + off;
        let mut end = start;
        while end < bytes.len() && ident(bytes[end]) {
            end += 1;
        }
        if start == 0 || !ident(bytes[start - 1]) {
            out.insert(text[start..end].to_owned());
        }
        i = end.max(start + 1);
    }
    out
}

/// The shipped Grafana dashboard and Prometheus alert rules use only
/// names the server (with express routes and the express service, C2)
/// exports, so a renamed metric cannot leave a silent panel or an alert
/// that never fires; every paging counter (`aspen_alert_*`) and the
/// dead-job and wider-trunk gauges have an alert rule.
#[test]
fn the_dashboard_and_alerts_use_only_exported_metrics() {
    let s = writable_server();
    let (_, _, mut text) = scrape(&s);
    // The express families: a server signing express URLs and serving the
    // express route renders these too.
    let key = format!("k1:{}", "ab".repeat(32));
    let routes = aspen_express::settings::routes(|k| match k {
        "ASPEN_EXPRESS_URLS" => Some("http://127.0.0.1:9/-/express".to_owned()),
        "ASPEN_EXPRESS_KEYS" => Some(key.clone()),
        _ => None,
    })
    .unwrap()
    .unwrap();
    routes.render(&mut text);
    aspen_express::ExpressMetrics::default().render(&mut text);
    let exported = lint(&text);

    let dashboard = repo_file("deploy/observability/grafana/aspen.json");
    let doc: serde_json::Value = serde_json::from_str(&dashboard).expect("dashboard is JSON");
    let panels = doc["panels"].as_array().expect("panels");
    let mut exprs = 0;
    for p in panels {
        if p["type"] == "row" {
            continue;
        }
        let targets = p["targets"].as_array().unwrap_or_else(|| panic!("{p}"));
        assert!(!targets.is_empty(), "panel {} has no query", p["title"]);
        for t in targets {
            assert!(!t["expr"].as_str().unwrap().trim().is_empty());
            exprs += 1;
        }
    }
    assert!(exprs >= 20, "{exprs} queries");
    let alerts = repo_file("deploy/observability/prometheus/aspen-alerts.yml");
    for (file, text) in [("dashboard", &dashboard), ("alerts", &alerts)] {
        for name in referenced(text) {
            assert!(
                exported.exports(&name),
                "{file} uses {name}, which is not exported"
            );
        }
    }
    let alerted = referenced(&alerts);
    for name in exported.families.keys() {
        if name.starts_with("aspen_alert_") {
            assert!(
                alerted.contains(name),
                "no alert rule for paging counter {name}"
            );
        }
    }
    for name in [
        "aspen_jobs",
        "aspen_trunk_repos",
        "aspen_push_trunk_versions_total",
        "aspen_meta_stats_up",
    ] {
        assert!(alerted.contains(name), "no alert rule uses {name}");
    }
    assert!(
        alerts.contains("state=\"dead\""),
        "the dead-job alert selects dead jobs"
    );
}

/// The linter itself catches what it is for: a family declared twice, a
/// sample outside its family, a counter without `_total`, and a histogram
/// whose `+Inf` bucket disagrees with its count.
#[test]
fn the_lint_rejects_malformed_expositions() {
    let good = "# HELP aspen_x_total x\n# TYPE aspen_x_total counter\naspen_x_total 1\n";
    lint(good);
    for bad in [
        format!("{good}{good}"),
        "# HELP aspen_x_total x\n# TYPE aspen_x_total counter\naspen_y_total 1\n".to_owned(),
        "# HELP aspen_x x\n# TYPE aspen_x counter\naspen_x 1\n".to_owned(),
        "# HELP aspen_h h\n# TYPE aspen_h histogram\naspen_h_bucket{le=\"1\"} 1\naspen_h_bucket{le=\"+Inf\"} 2\naspen_h_sum 1\naspen_h_count 3\n".to_owned(),
        "# HELP aspen_x_total x\n# TYPE aspen_x_total counter\naspen_x_total{a=\"1\",a=\"2\"} 1\n".to_owned(),
    ] {
        assert!(
            std::panic::catch_unwind(|| lint(&bad)).is_err(),
            "accepted:\n{bad}"
        );
    }
}

/// Invariant (12a): a request through [`AnyBlobStore`] (the store `aspen
/// serve` runs on) is exported at `/-/metrics` under the caller whose
/// scope sent it and the file its key names, with the bytes it read.
#[test]
fn blob_requests_are_exported_by_caller_and_file() {
    let s = writable_server();
    let dir = tempfile::tempdir().unwrap();
    let store = AnyBlobStore::Local(LocalBlobStore::open(dir.path().join("blobs")).unwrap());
    let src = dir.path().join("src");
    fs::write(&src, vec![7u8; 100]).unwrap();
    let key = BlobKey::new("repos/1/packs/observability-12a.pack").unwrap();
    s.block_on(async { store.upload_file(&key, &src).await.unwrap() });

    let requests = (
        "aspen_blob_requests_total",
        [("caller", "fetch"), ("file", "pack"), ("kind", "get")],
    );
    let bytes = (
        "aspen_blob_get_bytes_total",
        [("caller", "fetch"), ("file", "pack")],
    );
    // Counts are process-wide, so compare against a scrape taken first.
    let m = metrics(&s);
    let before = (
        m.value_or_zero(requests.0, &requests.1),
        m.value_or_zero(bytes.0, &bytes.1),
    );
    s.block_on(BlobCaller::Fetch.scope(async {
        let r = ByteRange::new(10, 40).unwrap();
        store.read_range(&key, r).await.unwrap();
        store.read_range(&key, r).await.unwrap();
    }));

    let m = metrics(&s);
    assert!(
        m.value(requests.0, &requests.1) >= before.0 + 2.0,
        "two fetch GETs of a pack"
    );
    assert!(
        m.value(bytes.0, &bytes.1) >= before.1 + 60.0,
        "60 bytes read by fetch from a pack"
    );
}

//! Black-box tests of `aspen doctor jobs` and of the binary's telemetry
//! (node C5): the binary runs against a real SQLite metadata database
//! that the test fills through `aspen-meta`, and an OTLP/HTTP capture
//! server stands in for an OpenTelemetry Collector.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use aspen_core::id::{JobId, PackChecksum, UnixMicros};
use aspen_core::meta::{JobEvent, JobKind, JobState, JobTag, MAX_JOB_ATTEMPTS, MetaStore, NewJob};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{NewPack, PackOrigin, entry_table_len};
use aspen_core::repo::RepoSettings;
use aspen_meta::{Clock, SqlStore};

/// The environment variables the binary reads, cleared so the host's
/// settings cannot change a test.
const ENV: &[&str] = &[
    "DATABASE_URL",
    "ASPEN_LOG",
    "ASPEN_LOG_FORMAT",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_SDK_DISABLED",
    "OTEL_TRACES_EXPORTER",
    "OTEL_SERVICE_NAME",
    "OTEL_RESOURCE_ATTRIBUTES",
];

fn aspen(url: &str, args: &[&str]) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_aspen"));
    for v in ENV {
        c.env_remove(v);
    }
    c.args(["doctor", "jobs", "--database-url", url]).args(args);
    c
}

fn run(c: &mut Command) -> (Option<i32>, String, String) {
    let Output {
        status,
        stdout,
        stderr,
    } = c.output().unwrap();
    (
        status.code(),
        String::from_utf8(stdout).unwrap(),
        String::from_utf8(stderr).unwrap(),
    )
}

/// A migrated SQLite database with one repository and one pack, and a
/// store whose clock the test moves.
struct Db {
    _dir: tempfile::TempDir,
    url: String,
    rt: tokio::runtime::Runtime,
    store: SqlStore,
    time: Arc<AtomicI64>,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let time = Arc::new(AtomicI64::new(1_000_000));
        let t = Arc::clone(&time);
        let store = rt.block_on(async {
            let s = SqlStore::connect(&url)
                .await
                .unwrap()
                .with_clock(Clock::from_fn(move || UnixMicros(t.load(Ordering::SeqCst))));
            s.migrate().await.unwrap();
            s
        });
        Self {
            _dir: dir,
            url,
            rt,
            store,
            time,
        }
    }

    /// A `rewrite_pack` job in repository `acme/demo`, queued now.
    fn rewrite_job(&self, n: u8) -> JobId {
        self.rt.block_on(async {
            let s = &self.store;
            let name = RepoName::new("acme/demo").unwrap();
            let repo = match s.repo(&name).await.unwrap() {
                Some(r) => r,
                None => s
                    .create_repo(
                        &name,
                        &RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap(),
                    )
                    .await
                    .unwrap(),
            };
            let pack = s
                .register_pack(&NewPack {
                    repo: repo.id,
                    checksum: PackChecksum::from_bytes([n; 20]),
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
            s.enqueue_jobs(&[NewJob {
                repo: repo.id,
                kind: JobKind::RewritePack { pack },
                run_after: UnixMicros(self.time.load(Ordering::SeqCst)),
            }])
            .await
            .unwrap()[0]
        })
    }

    /// Lets every claim of the job's lease expire until the queue parks
    /// it as dead (what a worker that crashes on it every time causes).
    fn kill(&self, id: JobId) {
        self.rt.block_on(async {
            for _ in 0..=MAX_JOB_ATTEMPTS {
                let _ = self
                    .store
                    .claim_job(
                        &[JobTag::RewritePack],
                        "crashing-worker",
                        Duration::from_micros(10),
                    )
                    .await
                    .unwrap();
                self.time.fetch_add(10, Ordering::SeqCst);
            }
            let job = self.store.job(id).await.unwrap().unwrap();
            assert_eq!(job.state, JobState::Dead);
        });
    }

    /// Fails every attempt of the job with `error` until it is dead.
    fn fail_until_dead(&self, id: JobId, error: &str) {
        self.rt.block_on(async {
            for attempt in 1..=MAX_JOB_ATTEMPTS {
                let job = self
                    .store
                    .claim_job(&[JobTag::RewritePack], "w", Duration::from_secs(60))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!((job.id, job.attempts), (id, attempt));
                self.store
                    .update_job(
                        id,
                        JobEvent::Fail {
                            owner: "w".into(),
                            attempt,
                            now: UnixMicros(self.time.load(Ordering::SeqCst)),
                            error: error.into(),
                            backoff: Duration::ZERO,
                        },
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(
                self.store.job(id).await.unwrap().unwrap().state,
                JobState::Dead
            );
        });
    }

    fn state(&self, id: JobId) -> JobState {
        self.rt.block_on(self.store.job(id)).unwrap().unwrap().state
    }
}

fn json(stdout: &str) -> serde_json::Value {
    serde_json::from_str(stdout).unwrap_or_else(|e| panic!("{e}: {stdout}"))
}

/// An empty queue is healthy: exit 0 and a plain statement.
#[test]
fn an_empty_queue_exits_0() {
    let db = Db::new();
    let (code, out, err) = run(&mut aspen(&db.url, &[]));
    assert_eq!(code, Some(0), "{err}");
    assert_eq!(out, "No jobs are queued, running or dead.\n");
}

/// The listing shows every open job per kind and state, and each dead job
/// with its repository, target, attempts and last error; it exits 1 while
/// any job is dead, so a cron check or readiness script can use it.
#[test]
fn dead_jobs_are_listed_with_their_last_error_and_exit_1() {
    let db = Db::new();
    let dead = db.rewrite_job(1);
    db.kill(dead);
    db.rewrite_job(2);
    let (code, out, err) = run(&mut aspen(&db.url, &["list"]));
    assert_eq!(code, Some(1), "{out}{err}");
    let row = out
        .lines()
        .find(|l| l.starts_with("rewrite_pack"))
        .unwrap_or_else(|| panic!("{out}"));
    assert_eq!(
        row.split_whitespace().take(4).collect::<Vec<_>>(),
        ["rewrite_pack", "1", "0", "1"],
        "queued, running, dead: {out}"
    );
    assert!(
        out.contains(&format!(
            "job {}: rewrite_pack pack 1 in acme/demo, {MAX_JOB_ATTEMPTS} attempts",
            dead.get()
        )),
        "{out}"
    );
    assert!(
        out.lines()
            .any(|l| l.contains("last error:") && l.contains("expired")),
        "{out}"
    );
    assert!(out.contains("aspen doctor jobs requeue <id>"), "{out}");

    let (code, out, _) = run(&mut aspen(&db.url, &["--json"]));
    assert_eq!(code, Some(1));
    let doc = json(&out);
    assert_eq!(doc["dead_total"], 1);
    assert_eq!(doc["dead"][0]["id"], dead.get());
    assert_eq!(doc["dead"][0]["repo"], "acme/demo");
    assert_eq!(doc["dead"][0]["kind"], "rewrite_pack");
    assert_eq!(doc["dead"][0]["target"]["pack"], 1);
    assert_eq!(doc["dead"][0]["attempts"], MAX_JOB_ATTEMPTS);
}

/// A stored error is printed on one line: a worker's multi-line error
/// cannot forge lines of the listing (another dead job, or advice), and
/// the JSON keeps it intact.
#[test]
fn a_multi_line_error_stays_on_one_line() {
    let db = Db::new();
    let dead = db.rewrite_job(1);
    db.fail_until_dead(
        dead,
        "disk full\n  job 999: compact version 1 in acme/other, 1 attempts",
    );
    let (code, out, _) = run(&mut aspen(&db.url, &[]));
    assert_eq!(code, Some(1));
    assert!(!out.lines().any(|l| l.starts_with("  job 999")), "{out}");
    assert!(
        out.contains("last error: disk full   job 999: compact version 1"),
        "{out}"
    );
    let (_, out, _) = run(&mut aspen(&db.url, &["--json"]));
    assert!(
        json(&out)["dead"][0]["last_error"]
            .as_str()
            .unwrap()
            .contains("disk full\n  job 999"),
        "{out}"
    );
}

/// `requeue` follows the dead-job procedure (resolve, then enqueue a fresh
/// job, in one transaction): the dead row is done, a new job with a new ID
/// and attempt 0 holds the same kind and target, and the listing is
/// healthy again.
#[test]
fn requeue_replaces_a_dead_job_with_a_fresh_one() {
    let db = Db::new();
    let dead = db.rewrite_job(1);
    db.kill(dead);
    let (code, out, err) = run(&mut aspen(
        &db.url,
        &["--json", "requeue", &dead.get().to_string()],
    ));
    assert_eq!(code, Some(0), "{err}");
    let doc = json(&out);
    assert_eq!(doc["resolved"], dead.get());
    let fresh = JobId::new(doc["requeued"]["id"].as_i64().unwrap()).unwrap();
    assert_ne!(fresh, dead);
    assert_eq!(doc["requeued"]["attempts"], 0);
    assert_eq!(doc["requeued"]["target"]["pack"], 1);
    assert_eq!(db.state(dead), JobState::Done);
    assert!(matches!(db.state(fresh), JobState::Queued { .. }));

    let (code, out, _) = run(&mut aspen(&db.url, &[]));
    assert_eq!(code, Some(0), "{out}");
    assert!(!out.contains("dead job"), "{out}");
}

/// `resolve` marks the dead job done and enqueues nothing.
#[test]
fn resolve_marks_a_dead_job_done_without_new_work() {
    let db = Db::new();
    let dead = db.rewrite_job(1);
    db.kill(dead);
    let (code, out, err) = run(&mut aspen(&db.url, &["resolve", &dead.get().to_string()]));
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("nothing was requeued"), "{out}");
    assert_eq!(db.state(dead), JobState::Done);
    let (code, out, _) = run(&mut aspen(&db.url, &[]));
    assert_eq!(
        (code, out.as_str()),
        (Some(0), "No jobs are queued, running or dead.\n")
    );
}

/// Only dead jobs can be requeued or resolved; anything else, an unknown
/// ID or an unreachable database is an error (exit 2) that changes
/// nothing.
#[test]
fn requeue_and_resolve_refuse_jobs_that_are_not_dead() {
    let db = Db::new();
    let queued = db.rewrite_job(1);
    for action in ["requeue", "resolve"] {
        let (code, _, err) = run(&mut aspen(&db.url, &[action, &queued.get().to_string()]));
        assert_eq!(code, Some(2), "{action}");
        assert!(err.contains("is queued, not dead"), "{action}: {err}");
        assert!(matches!(db.state(queued), JobState::Queued { .. }));
        let (code, _, err) = run(&mut aspen(&db.url, &[action, "9999"]));
        assert_eq!(code, Some(2), "{action}");
        assert!(err.contains("9999"), "{err}");
    }
    let missing = format!(
        "sqlite://{}?mode=ro",
        db._dir.path().join("nope.db").display()
    );
    let (code, _, err) = run(&mut aspen(&missing, &[]));
    assert_eq!(code, Some(2), "{err}");
}

/// Operator actions are logged: with `ASPEN_LOG_FORMAT=json`, a requeue
/// writes one JSON line naming the old and the new job.
#[test]
fn a_requeue_is_logged_as_structured_json() {
    let db = Db::new();
    let dead = db.rewrite_job(1);
    db.kill(dead);
    let (code, _, err) =
        run(aspen(&db.url, &["requeue", &dead.get().to_string()]).env("ASPEN_LOG_FORMAT", "json"));
    assert_eq!(code, Some(0), "{err}");
    let line = err
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|v| v["fields"]["message"] == "requeued a dead job as a fresh job")
        .unwrap_or_else(|| panic!("no JSON log line: {err}"));
    assert_eq!(line["level"], "INFO");
    assert_eq!(line["fields"]["job"], dead.get());
    assert!(line["fields"]["new_job"].as_i64().unwrap() > dead.get());
    assert_eq!(line["span"]["name"], "doctor_jobs");
}

/// One HTTP request as an OTLP collector receives it.
struct Request {
    head: String,
    body: Vec<u8>,
}

/// Accepts connections on `listener` until `until` says the client is
/// done (checked whenever no connection is waiting), answering every request
/// `200 OK` and collecting it.
fn collect(listener: &TcpListener, mut until: impl FnMut() -> bool) -> Vec<Request> {
    listener.set_nonblocking(true).unwrap();
    let mut got = Vec::new();
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(stream);
                loop {
                    let mut head = String::new();
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 {
                            break;
                        }
                        if line == "\r\n" {
                            break;
                        }
                        head.push_str(&line);
                    }
                    if head.is_empty() {
                        break;
                    }
                    let len = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().unwrap())
                        })
                        .expect("OTLP/HTTP requests carry a Content-Length");
                    let mut body = vec![0; len];
                    reader.read_exact(&mut body).unwrap();
                    reader
                        .get_mut()
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                        .unwrap();
                    got.push(Request { head, body });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if until() {
                    return got;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("accept: {e}"),
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Runs `aspen doctor jobs` with `env` against a capture server and
/// returns what it received once the process exited.
fn traced_run(db_url: &str, env: &[(&str, &str)]) -> (Option<i32>, Vec<Request>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mut cmd = aspen(db_url, &[]);
    cmd.env("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut status = None;
    let got = collect(&listener, || {
        if status.is_none()
            && let Some(st) = child.try_wait().unwrap()
        {
            status = Some(st.code());
        }
        status.is_some()
    });
    let code = status.unwrap();
    (code, got)
}

/// With `OTEL_EXPORTER_OTLP_ENDPOINT` set, the command's spans reach the
/// collector over OTLP/HTTP (protobuf, `/v1/traces`) before the process
/// exits, labelled with the service name and version.
#[test]
fn spans_are_exported_over_otlp_http_before_exit() {
    let db = Db::new();
    let (code, got) = traced_run(&db.url, &[]);
    assert_eq!(code, Some(0));
    let traces: Vec<&Request> = got
        .iter()
        .filter(|r| r.head.starts_with("POST /v1/traces "))
        .collect();
    assert!(
        !traces.is_empty(),
        "no export reached the collector: {:?}",
        got.iter().map(|r| &r.head).collect::<Vec<_>>()
    );
    for r in &traces {
        assert!(
            r.head
                .to_ascii_lowercase()
                .contains("content-type: application/x-protobuf"),
            "{}",
            r.head
        );
    }
    let body: Vec<u8> = traces.iter().flat_map(|r| r.body.clone()).collect();
    assert!(
        contains(&body, b"doctor_jobs"),
        "the command's span is exported"
    );
    assert!(contains(&body, b"service.name"));
    assert!(contains(&body, b"aspen"));
    assert!(contains(&body, b"service.version"));
}

/// `OTEL_SDK_DISABLED=true` keeps an endpoint from being contacted.
#[test]
fn the_sdk_switch_turns_export_off() {
    let db = Db::new();
    let (code, got) = traced_run(&db.url, &[("OTEL_SDK_DISABLED", "true")]);
    assert_eq!(code, Some(0));
    assert_eq!(got.len(), 0, "nothing is sent");
}

/// A protocol other than OTLP/HTTP protobuf is refused at startup (exit
/// 2), not silently ignored.
#[test]
fn an_unsupported_otlp_protocol_is_refused() {
    let db = Db::new();
    let (code, _, err) = run(aspen(&db.url, &[])
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"));
    assert_eq!(code, Some(2));
    assert!(err.contains("http/protobuf"), "{err}");
}

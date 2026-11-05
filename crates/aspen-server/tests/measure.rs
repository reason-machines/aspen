//! B1's push-latency measurement (`docs/PLAN.md`, push path: "B1 measures
//! push latency end to end, against a real bucket"): the same `git push`
//! sequence against Aspen and against stock `git receive-pack` (through
//! `git http-backend`), with Aspen's time per stage.
//!
//! Ignored by default; the nightly `--include-ignored` run executes it with
//! MinIO (`ASPEN_TEST_S3_*`, as aspen-blob's S3 conformance suite reads
//! them). Without those settings it uses the local blob store. Run by hand:
//!
//! ```text
//! cargo test --release -p aspen-server --test measure -- --ignored --nocapture
//! ```

mod receive_support;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aspen_blob::object_store::aws::AmazonS3Builder;
use aspen_blob::{LocalBlobStore, ObjectBlobStore, UploadOptions};
use aspen_core::blob::BlobStore;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::receive::{ReceiveConfig, Timings};
use aspen_testkit::GitServer;
use aspen_testkit::git::isolated_git;

const FILES: usize = 300;
const ROUNDS: usize = 20;

fn git(home: &Path, dir: &Path, args: &[&str]) {
    let out = isolated_git(home)
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn timed_push(home: &Path, dir: &Path, url: &str) -> Duration {
    let started = Instant::now();
    git(home, dir, &["push", "-q", url, "main"]);
    started.elapsed()
}

fn summary(label: &str, mut d: Vec<Duration>) {
    d.sort();
    let ms = |x: Duration| x.as_secs_f64() * 1000.0;
    let mean = d.iter().map(|x| ms(*x)).sum::<f64>() / d.len() as f64;
    println!(
        "{label:<28} n={:<3} p50={:>7.1} ms  p90={:>7.1} ms  mean={:>7.1} ms",
        d.len(),
        ms(d[d.len() / 2]),
        ms(d[d.len() * 9 / 10]),
        mean
    );
}

fn per_push(label: &str, t: &Timings) {
    let n = t.pushes.max(1) as f64;
    let ms = |x: Duration| x.as_secs_f64() * 1000.0 / n;
    println!(
        "{label:<28} search={:.1} receive={:.1} gate={:.1} prefetch={:.1} complete={:.1} graph={:.1} check={:.1} hook={:.1} upload={:.1} commit={:.1} (ms per push)",
        ms(t.search),
        ms(t.receive),
        ms(t.gate_wait),
        ms(t.prefetch),
        ms(t.complete),
        ms(t.graph),
        ms(t.check),
        ms(t.hook),
        ms(t.upload),
        ms(t.commit)
    );
}

fn minus(a: Timings, b: Timings) -> Timings {
    Timings {
        pushes: a.pushes - b.pushes,
        search: a.search - b.search,
        receive: a.receive - b.receive,
        gate_wait: a.gate_wait - b.gate_wait,
        prefetch: a.prefetch - b.prefetch,
        complete: a.complete - b.complete,
        graph: a.graph - b.graph,
        check: a.check - b.check,
        hook: a.hook - b.hook,
        upload: a.upload - b.upload,
        commit: a.commit - b.commit,
    }
}

async fn measure<B: BlobStore + Clone + 'static>(blobs: B, store: &str) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let meta = SqlStore::connect(&url).await.unwrap();
    meta.migrate().await.unwrap();
    let cfg = ReceiveConfig::new(
        dir.path().join("spool"),
        MaxObjectBytes::new(512 << 20).unwrap(),
        MaxInflatedBytes::new(8 << 30).unwrap(),
    );
    let server = aspen_server::Server::new(
        meta.clone(),
        blobs,
        aspen_server::ServerConfig::new(MaxObjectBytes::new(512 << 20).unwrap()),
    )
    .with_receive_pack(cfg)
    .unwrap();
    let svc = server.receive_pack().unwrap().clone();
    meta.create_repo(
        &RepoName::new("bench").unwrap(),
        &RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let access = std::sync::Arc::new(aspen_server::access::Access::new(
        aspen_auth::AuthService::new(
            aspen_auth::PolicyConfig {
                anonymous: vec!["write:*".parse().unwrap()],
                ..Default::default()
            },
            aspen_auth::MemoryTokenStore::new(),
        )
        .unwrap(),
        aspen_auth::Limits::new(aspen_auth::LimitsConfig::default()),
        meta.clone(),
        aspen_server::access::ClientAddr::Peer,
    ));
    let app = server.router(access);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let aspen_url = format!("http://{addr}/bench.git");
    let stock = aspen_testkit::StockServer::start().unwrap();
    let stock_url = stock.create_repo("bench.git").unwrap();

    let home = dir.path().join("home");
    let work = dir.path().join("work");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    let (h, w) = (home.clone(), work.clone());
    let svc2: Arc<_> = svc.clone();
    let result = tokio::task::spawn_blocking(move || {
        git(&h, &w, &["init", "-q", "--initial-branch=main"]);
        for f in 0..FILES {
            let body: String = (0..200)
                .map(|i| format!("file {f} line {i} some text to make it compressible\n"))
                .collect();
            std::fs::write(w.join(format!("f{f:03}.txt")), body).unwrap();
        }
        git(&h, &w, &["add", "-A"]);
        git(&h, &w, &["commit", "-q", "-m", "initial"]);
        let first_aspen = timed_push(&h, &w, &aspen_url);
        let first_stock = timed_push(&h, &w, &stock_url);
        let before = svc2.timings();
        let (mut a, mut s) = (Vec::new(), Vec::new());
        for round in 0..ROUNDS {
            for k in 0..5 {
                let f = w.join(format!("f{:03}.txt", (round * 7 + k * 31) % FILES));
                let mut body = std::fs::read_to_string(&f).unwrap();
                body.push_str(&format!("round {round} edit {k}\n"));
                std::fs::write(f, body).unwrap();
            }
            git(&h, &w, &["commit", "-q", "-am", &format!("round {round}")]);
            // Alternate which server goes first, so neither always gets a
            // warm page cache.
            if round % 2 == 0 {
                a.push(timed_push(&h, &w, &aspen_url));
                s.push(timed_push(&h, &w, &stock_url));
            } else {
                s.push(timed_push(&h, &w, &stock_url));
                a.push(timed_push(&h, &w, &aspen_url));
            }
        }
        (first_aspen, first_stock, before, a, s)
    })
    .await
    .unwrap();
    let (first_aspen, first_stock, before, a, s) = result;
    println!(
        "--- push latency, blob store: {store} ({FILES} files, {ROUNDS} thin pushes of 5 edits) ---"
    );
    println!(
        "first push (full history)    aspen={:.1} ms  git receive-pack={:.1} ms",
        first_aspen.as_secs_f64() * 1000.0,
        first_stock.as_secs_f64() * 1000.0
    );
    summary("aspen, thin pushes", a);
    summary("git receive-pack, thin", s);
    per_push("aspen stages, thin pushes", &minus(svc.timings(), before));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement; run nightly or by hand with --nocapture"]
async fn push_latency_against_git_receive_pack() {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    if let Some(endpoint) = var("ASPEN_TEST_S3_ENDPOINT") {
        let builder = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_region(var("ASPEN_TEST_S3_REGION").unwrap_or_else(|| "us-east-1".into()))
            .with_bucket_name(var("ASPEN_TEST_S3_BUCKET").expect("bucket"))
            .with_access_key_id(var("ASPEN_TEST_S3_ACCESS_KEY_ID").expect("key"))
            .with_secret_access_key(var("ASPEN_TEST_S3_SECRET_ACCESS_KEY").expect("secret"));
        let prefix = format!("b1-measure-{}", std::process::id());
        let store = ObjectBlobStore::s3(builder, UploadOptions::default())
            .unwrap()
            .with_prefix(&prefix)
            .unwrap();
        measure(store, "S3 (MinIO)").await;
    }
    let dir = tempfile::tempdir().unwrap();
    measure(LocalBlobStore::open(dir.path()).unwrap(), "local disk").await;
}

/// Storage of a first push of trunk plus many tags into an empty
/// repository (coordinator review of #37: one pack per write cost
/// O(writes x history)). Prints, for annotated and for lightweight tags:
/// the objects and bytes the client sent, what Aspen stored (packs,
/// objects, bytes), and what one pack per write would have stored (each
/// write's whole reach, computed with `git rev-list`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement; run nightly or by hand with --nocapture"]
async fn storage_of_a_first_push_with_many_tags() {
    use receive_support::{Server, blocking};
    const TAGS: usize = 200;
    const COMMITS: usize = 100;
    for annotated in [true, false] {
        let s = Server::start().await;
        let w = s.clone_dir("w");
        let path = w.path.clone();
        let w2 = receive_support::Work {
            path: path.clone(),
            home: w.home.clone(),
        };
        blocking(move || {
            for f in 0..FILES {
                let body: String = (0..200)
                    .map(|i| format!("file {f} line {i} some text to make it compressible\n"))
                    .collect();
                std::fs::write(path.join(format!("f{f:03}.txt")), body).unwrap();
            }
            w2.git(&["add", "-A"]);
            w2.git(&["commit", "-q", "-m", "initial"]);
            for c in 0..COMMITS {
                for k in 0..3 {
                    let f = path.join(format!("f{:03}.txt", (c * 7 + k * 31) % FILES));
                    let mut body = std::fs::read_to_string(&f).unwrap();
                    body.push_str(&format!("commit {c} edit {k}\n"));
                    std::fs::write(f, body).unwrap();
                }
                w2.git(&["commit", "-q", "-am", &format!("c{c}")]);
                for t in 0..TAGS / COMMITS {
                    let name = format!("v{c}.{t}");
                    if annotated {
                        w2.git(&["tag", "-a", &name, "-m", &format!("release {name}")]);
                    } else {
                        w2.git(&["tag", &name]);
                    }
                }
            }
        })
        .await;
        let mut cmd = w.command();
        cmd.args(["push", "-q", &s.url(), "main", "--tags"]);
        let started = Instant::now();
        assert!(blocking(move || cmd.status().unwrap()).await.success());
        let took = started.elapsed();
        let sent = w.objects("--all").len();
        let mut old = w.objects("main").len();
        let mut packs = Vec::new();
        packs.extend(s.live("refs/heads/main").await.value.pack());
        let (mut exact, mut wider) = (0, 0);
        for c in 0..COMMITS {
            for t in 0..TAGS / COMMITS {
                let name = format!("v{c}.{t}");
                old += w.objects(&name).len();
                let v = s.live(&format!("refs/tags/{name}")).await;
                packs.extend(v.value.pack());
                match v.value.closure() {
                    Some(aspen_core::refs::Closure::Exact) => exact += 1,
                    _ => wider += 1,
                }
            }
        }
        packs.sort_unstable();
        packs.dedup();
        let rows = s.pack_rows(&packs).await;
        let objects: u64 = rows.iter().map(|r| u64::from(r.pack.object_count)).sum();
        let bytes: u64 = rows.iter().map(|r| r.pack.size).sum();
        let received: u64 = rows
            .iter()
            .map(|r| match r.pack.origin {
                aspen_core::pack::PackOrigin::Push { received_bytes } => received_bytes,
                _ => 0,
            })
            .sum();
        println!(
            "--- first push, main + {TAGS} {} tags, {COMMITS} commits over {FILES} files ({:.0} ms) ---",
            if annotated {
                "annotated"
            } else {
                "lightweight"
            },
            took.as_secs_f64() * 1000.0
        );
        println!("sent:   {sent} objects, {received} bytes");
        println!(
            "stored: {} packs, {objects} objects ({:.2}x sent), {bytes} bytes ({:.2}x sent); tags exact {exact}, wider {wider}",
            rows.len(),
            objects as f64 / sent as f64,
            bytes as f64 / received as f64
        );
        println!(
            "one pack per write would store {old} objects ({:.1}x sent)",
            old as f64 / sent as f64
        );
        s.check_ref("refs/heads/main", &w).await;
        s.check_ref(&format!("refs/tags/v{}.0", COMMITS - 1), &w)
            .await;
        s.check_ref("refs/tags/v0.0", &w).await;
    }
}

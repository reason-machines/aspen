//! Push bodies sent with `Content-Encoding: gzip`, as JGit sends every
//! smart-HTTP push (issue #134): receive-pack inflates them as it reads
//! them, and the push quota counts each push as the larger of its sent and
//! inflated bytes (decisions log, "#134").

mod receive_support;

use std::io::Write;
use std::time::Duration;

use aspen_auth::{LimitsConfig, PushQuota};
use aspen_core::id::ObjectId;
use aspen_core::repo::ForcePolicy;
use receive_support::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const Z: ObjectId = ObjectId::ZERO;
const MAIN: &str = "refs/heads/main";
const GZIP: &str = "Content-Encoding: gzip";

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}

/// The pack `git pack-objects` writes for `revs` (`^<rev>` excludes), as
/// a client sends it.
fn pack_of(w: &Work, revs: &[&str]) -> Vec<u8> {
    let mut child = w
        .command()
        .args(["pack-objects", "--revs", "--stdout", "-q"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    for r in revs {
        writeln!(stdin, "{r}").unwrap();
    }
    drop(stdin);
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    out.stdout
}

/// A request that creates `branches` refs named `<prefix>NNNN` at `tip`,
/// plus `extra`: its command section is large and compresses well.
fn many_branches(
    extra: &[(ObjectId, ObjectId, &str)],
    prefix: &str,
    branches: usize,
    tip: ObjectId,
    pack: &[u8],
) -> Vec<u8> {
    let names: Vec<String> = (0..branches)
        .map(|i| format!("refs/heads/{prefix}{i:04}"))
        .collect();
    let mut commands = extra.to_vec();
    commands.extend(names.iter().map(|n| (Z, tip, n.as_str())));
    request(&commands, &[], pack)
}

fn anonymous_writer() -> serde_json::Value {
    serde_json::json!({ "anonymous": ["write:demo"] })
}

/// Invariant (#134): a gzip-compressed push body, which JGit sends for
/// every push, is inflated and stored as the same push sent uncompressed
/// would be. `x-gzip` is the same encoding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gzip_push_body_is_inflated_and_stored() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "one\n", "one");
    let first = w.commit("b/c.txt", &"two\n".repeat(200), "two");
    let body = request(&[(Z, oid(&first), MAIN)], &[], &pack_of(&w, &["main"]));
    let (status, report) = post_with(s.url(), gzip(&body), &[GZIP]).await;
    assert_eq!(status, 200, "{report:?}");
    assert_eq!(report[0], "unpack ok", "{report:?}");
    assert_eq!(report[1], format!("ok {MAIN}"), "{report:?}");
    s.check_ref(MAIN, &w).await;

    // A thin pack on top of what is stored, as a client sends it.
    let second = w.commit("b/c.txt", &"two\n".repeat(201), "three");
    let not_first = format!("^{first}");
    let body = request(
        &[(oid(&first), oid(&second), MAIN)],
        &[],
        &pack_of(&w, &["main", &not_first]),
    );
    let (status, report) = post_with(s.url(), gzip(&body), &["Content-Encoding: x-gzip"]).await;
    assert_eq!(status, 200, "{report:?}");
    assert_eq!(report[1], format!("ok {MAIN}"), "{report:?}");
    let v = s.check_ref(MAIN, &w).await;
    assert_eq!(v.value.tip(), Some(oid(&second)));
}

/// Invariant: a gzip body that does not inflate (cut short) fails the push
/// and stores nothing; an encoding receive-pack does not know is `415`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_or_unknown_encoding_stores_nothing() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    let tip = w.commit("a.txt", &"one\n".repeat(500), "one");
    let body = request(&[(Z, oid(&tip), MAIN)], &[], &pack_of(&w, &["main"]));
    let mut cut = gzip(&body);
    cut.truncate(cut.len() - 30);
    let (status, report) = post_with(s.url(), cut, &[GZIP]).await;
    assert_eq!(status, 200, "{report:?}");
    assert!(
        report[0].starts_with("unpack ") && report[0] != "unpack ok",
        "{report:?}"
    );
    let (status, report) = post_with(s.url(), body, &["Content-Encoding: br"]).await;
    assert_eq!(status, 415, "{report:?}");
    assert!(s.current(MAIN).await.is_none());
    assert_eq!(s.stored_pack_files(), 0);
}

/// Invariant (#134, the B3 obligation): the per-push cap bounds the
/// inflated body, not only the compressed one. A body under the cap as sent
/// (so its declared length passes the `413` check) that inflates past it is
/// refused while it is read, and nothing is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_push_cap_bounds_the_inflated_body() {
    const CAP: u64 = 16 * 1024;
    let limits = LimitsConfig {
        quota: PushQuota {
            max_pack_bytes: Some(CAP),
            ..PushQuota::default()
        },
        ..LimitsConfig::default()
    };
    let s = Server::start_limited(
        |_| {},
        ForcePolicy::Branches,
        anonymous_writer(),
        |b| b,
        limits,
    )
    .await;
    let w = s.clone_dir("w");
    let tip = oid(&w.commit("a.txt", "one\n", "one"));
    let body = many_branches(&[(Z, tip, MAIN)], "b", 300, tip, &pack_of(&w, &["main"]));
    let sent = gzip(&body);
    assert!((sent.len() as u64) < CAP && body.len() as u64 > CAP);
    let (status, report) = post_with(s.url(), sent, &[GZIP]).await;
    assert_eq!(status, 200, "{report:?}");
    assert!(report.join("\n").contains("push too large"), "{report:?}");
    assert!(s.current(MAIN).await.is_none());
    assert_eq!(s.stored_pack_files(), 0);
}

/// Invariant: a push body that is not compressed (no `Content-Encoding`, or
/// an explicit `identity`) is still metered by the access guard's body
/// wrapper. Streamed with no declared length, so the `413` check cannot
/// refuse it up front, a body over the cap fails while it is read and
/// stores nothing; one under the cap is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_push_body_is_still_capped_as_it_streams() {
    const CAP: u64 = 16 * 1024;
    let limits = LimitsConfig {
        quota: PushQuota {
            max_pack_bytes: Some(CAP),
            ..PushQuota::default()
        },
        ..LimitsConfig::default()
    };
    let s = Server::start_limited(
        |_| {},
        ForcePolicy::Branches,
        anonymous_writer(),
        |b| b,
        limits,
    )
    .await;
    let w = s.clone_dir("w");
    let tip = oid(&w.commit("a.txt", "one\n", "one"));
    let pack = pack_of(&w, &["main"]);
    let over = many_branches(&[(Z, tip, MAIN)], "b", 300, tip, &pack);
    assert!(over.len() as u64 > CAP);
    let chunked = "Transfer-Encoding: chunked";
    for headers in [&[chunked][..], &[chunked, "Content-Encoding: identity"][..]] {
        let (status, report) = post_with(s.url(), over.clone(), headers).await;
        assert_eq!(status, 200, "{headers:?}: {report:?}");
        assert!(
            report.join("\n").contains("push too large"),
            "{headers:?}: {report:?}"
        );
        assert!(s.current(MAIN).await.is_none());
        assert_eq!(s.stored_pack_files(), 0);
    }
    let under = request(&[(Z, tip, MAIN)], &[], &pack);
    assert!((under.len() as u64) < CAP);
    let (status, report) =
        post_with(s.url(), under, &[chunked, "Content-Encoding: identity"]).await;
    assert_eq!(status, 200, "{report:?}");
    assert_eq!(report[1], format!("ok {MAIN}"), "{report:?}");
}

/// Invariant (#134): a compressed push spends its inflated bytes of the
/// caller's pushed-bytes budget. Two pushes whose compressed bodies together
/// fit the budget but whose inflated bodies do not: the first is stored,
/// the second fails part-way and stores nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gzip_push_spends_its_inflated_bytes_of_the_budget() {
    const BUDGET: u64 = 40_000;
    let limits = LimitsConfig {
        quota: PushQuota {
            bytes_per_hour: Some(BUDGET),
            ..PushQuota::default()
        },
        ..LimitsConfig::default()
    };
    let s = Server::start_limited(
        |_| {},
        ForcePolicy::Branches,
        anonymous_writer(),
        |b| b,
        limits,
    )
    .await;
    let w = s.clone_dir("w");
    let first = oid(&w.commit("a.txt", "one\n", "one"));
    let one = many_branches(
        &[(Z, first, MAIN)],
        "a",
        300,
        first,
        &pack_of(&w, &["main"]),
    );
    let second = oid(&w.commit("a.txt", "two\n", "two"));
    let not_first = format!("^{first}");
    let two = many_branches(&[], "b", 300, second, &pack_of(&w, &["main", &not_first]));
    let (one_sent, two_sent) = (gzip(&one), gzip(&two));
    assert!(((one_sent.len() + two_sent.len()) as u64) < BUDGET);
    assert!((one.len() as u64) < BUDGET && ((one.len() + two.len()) as u64) > BUDGET);

    let (status, report) = post_with(s.url(), one_sent, &[GZIP]).await;
    assert_eq!(status, 200, "{report:?}");
    assert_eq!(report[1], format!("ok {MAIN}"), "{report:?}");
    let stored = s.stored_pack_files();

    let (status, report) = post_with(s.url(), two_sent, &[GZIP]).await;
    assert_eq!(status, 200, "{report:?}");
    assert!(report.join("\n").contains("push quota"), "{report:?}");
    assert!(s.current("refs/heads/b0000").await.is_none());
    assert_eq!(s.stored_pack_files(), stored);
}

/// Invariant: a gzip stream that never inflates to anything (empty deflate
/// blocks, 5 bytes each, forever) is cut off. With no quota, once it has
/// sent as many bytes as an uncompressed body could carry (the pack limit
/// plus twice the request limit); with a per-push cap, once it has sent the
/// cap, since a push is charged its sent bytes when they are more than its
/// inflated ones. Without those bounds the server would read it for as long
/// as the client sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gzip_body_that_never_inflates_is_cut_off() {
    let s = Server::start_with(
        |c| {
            c.push_limits.max_pack_bytes = 64 * 1024;
            c.request_limits.max_request_bytes = 16 * 1024;
        },
        ForcePolicy::Branches,
    )
    .await;
    let answer = never_inflating_push(&s).await;
    assert!(answer.contains("as sent"), "{answer}");

    let limits = LimitsConfig {
        quota: PushQuota {
            max_pack_bytes: Some(16 * 1024),
            ..PushQuota::default()
        },
        ..LimitsConfig::default()
    };
    // The service's own limits are the defaults here (8 GiB packs), far
    // past what the client sends.
    let s = Server::start_limited(
        |_| {},
        ForcePolicy::Branches,
        anonymous_writer(),
        |b| b,
        limits,
    )
    .await;
    let answer = never_inflating_push(&s).await;
    assert!(answer.contains("push too large"), "{answer}");
    assert_eq!(s.stored_pack_files(), 0);
}

/// Sends a gzip push body of empty deflate blocks (64 MiB of them, then it
/// stays open sending nothing) and returns the server's answer: the HTTP
/// response up to its error line.
async fn never_inflating_push(s: &Server) -> String {
    let addr = s.base.trim_start_matches("http://").to_owned();
    let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let (mut rx, mut tx) = stream.into_split();
    let writer = tokio::spawn(async move {
        let head = format!(
            "POST /demo.git/git-receive-pack HTTP/1.1\r\nHost: {addr}\r\n\
             Content-Type: application/x-git-receive-pack-request\r\n\
             {GZIP}\r\nTransfer-Encoding: chunked\r\n\r\n"
        );
        tx.write_all(head.as_bytes()).await?;
        let gzip_header = [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff];
        let empty_blocks: Vec<u8> = [0u8, 0, 0, 0xff, 0xff].repeat(4096);
        let mut sent = 0u64;
        for i in 0u64.. {
            let chunk: &[u8] = if i == 0 { &gzip_header } else { &empty_blocks };
            tx.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await?;
            tx.write_all(chunk).await?;
            tx.write_all(b"\r\n").await?;
            sent += chunk.len() as u64;
            if sent > 64 << 20 {
                // Far past either bound: keep the body open without sending.
                std::future::pending::<()>().await;
            }
        }
        Ok::<_, std::io::Error>(())
    });
    let mut response = Vec::new();
    let mut buf = [0u8; 4096];
    let read = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let n = rx.read(&mut buf).await.unwrap();
            response.extend_from_slice(&buf[..n]);
            let text = String::from_utf8_lossy(&response);
            if n == 0 || text.contains("as sent") || text.contains("push too large") {
                break;
            }
        }
    })
    .await;
    writer.abort();
    let text = String::from_utf8_lossy(&response).into_owned();
    assert!(read.is_ok(), "no answer while the body kept coming: {text}");
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    text
}

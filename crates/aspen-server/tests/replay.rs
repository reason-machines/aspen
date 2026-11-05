//! Issue #132: libcurl resends a request when a reused keep-alive
//! connection dies before any answer, so one `git push` over smart HTTP can
//! deliver its receive-pack POST twice. The first copy commits and its
//! answer is lost; the second names an old value that is no longer the tip.
//! When the ref's last move has the request's old and new values, and the
//! request's own rules would have accepted that move, the second copy is
//! answered `ok` and writes nothing. Here the same request body is posted
//! twice, which is what the server sees from such a resend.

mod receive_support;

use std::io::Write as _;

use aspen_core::id::ObjectId;
use receive_support::*;

const Z: ObjectId = ObjectId::ZERO;
const MAIN: &str = "refs/heads/main";
const TOPIC: &str = "refs/heads/topic";

/// A pack of the objects `tip` reaches and `not` does not.
fn pack_of(w: &Work, tip: &str, not: &str) -> Vec<u8> {
    let mut child = w
        .command()
        .args(["pack-objects", "--stdout", "--revs", "-q"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(format!("{tip}\n^{not}\n").as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    out.stdout
}

/// Posts `body` twice to `url`, as a resent POST arrives; returns both
/// reports' status lines for `name`.
async fn post_twice(url: String, body: Vec<u8>, name: &str) -> (String, String) {
    let line = |r: Vec<String>| {
        r.into_iter()
            .find(|l| l.ends_with(name) || l.contains(&format!(" {name} ")))
            .unwrap_or_else(|| panic!("no status for {name}"))
    };
    let first = line(post(url.clone(), body.clone()).await);
    let second = line(post(url, body).await);
    (first, second)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resent_push_that_already_landed_is_reported_ok() {
    let s = Server::start().await;
    let w = s.clone_dir("w");
    let t1 = w.commit("a.txt", "one\n", "one");
    w.git(&["push", "-q", &s.url(), "main", "main:refs/heads/topic"]);

    // Trunk: the client sent the tip it was shown.
    let t2 = w.commit("a.txt", "two\n", "two");
    let body = request(&[(oid(&t1), oid(&t2), MAIN)], &[], &pack_of(&w, &t2, &t1));
    let (first, second) = post_twice(s.url(), body.clone(), MAIN).await;
    assert_eq!(first, format!("ok {MAIN}"));
    let landed = s.check_ref(MAIN, &w).await;
    let files = s.stored_pack_files();
    assert_eq!(second, format!("ok {MAIN}"), "the resent copy is ok");
    let after = s.live(MAIN).await;
    assert_eq!(after.id, landed.id, "the resent copy writes no version");
    assert_eq!(s.stored_pack_files(), files, "nor a pack");

    // A branch-scoped URL: the client sent the branch's tip.
    w.git(&["checkout", "-q", "-b", "topic", &t1]);
    let b2 = w.commit("b.txt", "topic\n", "topic");
    let body = request(&[(oid(&t1), oid(&b2), TOPIC)], &[], &pack_of(&w, &b2, &t2));
    let url = s.scoped_url("topic");
    let first = post(url.clone(), body.clone()).await;
    assert!(first.contains(&format!("ok {TOPIC}")), "{first:?}");
    let landed = s.check_ref(TOPIC, &w).await;
    let second = post(url, body).await;
    assert!(second.contains(&format!("ok {TOPIC}")), "{second:?}");
    let topic = s.check_ref(TOPIC, &w).await;
    assert_eq!(topic.value.tip(), Some(oid(&b2)));
    assert_eq!(topic.id, landed.id, "the resent copy writes no version");

    // A compare-and-swap delete through the scoped URL, resent.
    let body = request(&[(oid(&b2), Z, TOPIC)], &[], &[]);
    let (first, second) = post_twice(s.scoped_url("topic"), body, TOPIC).await;
    assert_eq!(first, format!("ok {TOPIC}"));
    assert_eq!(second, format!("ok {TOPIC}"));
    let deleted = s.current(TOPIC).await.unwrap();
    assert_eq!(deleted.value.tip(), None);
    assert_eq!(deleted.parent, Some(topic.id), "deleted once");

    // Not a replay once the ref has moved on: the old request is stale.
    w.git(&["checkout", "-q", "main"]);
    let t3 = w.commit("a.txt", "three\n", "three");
    w.git(&["push", "-q", &s.url(), "main"]);
    assert_eq!(s.live(MAIN).await.value.tip(), Some(oid(&t3)));
    let r = post(s.url(), body_stale(&w, &t1, &t2)).await;
    assert!(
        r.iter()
            .any(|l| l.starts_with(&format!("ng {MAIN} incorrect old value"))),
        "{r:?}"
    );
    assert_eq!(s.live(MAIN).await.value.tip(), Some(oid(&t3)));
}

/// The trunk request of the test above, `t1 -> t2`, rebuilt.
fn body_stale(w: &Work, t1: &str, t2: &str) -> Vec<u8> {
    request(&[(oid(t1), oid(t2), MAIN)], &[], &pack_of(w, t2, t1))
}

/// [`request`] with the `atomic` capability.
fn atomic_request(commands: &[(ObjectId, ObjectId, &str)], pack: &[u8]) -> Vec<u8> {
    let plain = request(commands, &[], pack);
    let caps = b"report-status ofs-delta";
    let at = plain
        .windows(caps.len())
        .position(|w| w == caps)
        .expect("capabilities in the first command");
    let mut first_len = usize::from_str_radix(std::str::from_utf8(&plain[..4]).unwrap(), 16)
        .expect("pkt-line length");
    let mut body = Vec::with_capacity(plain.len() + 7);
    first_len += " atomic".len();
    body.extend_from_slice(format!("{first_len:04x}").as_bytes());
    body.extend_from_slice(&plain[4..at + caps.len()]);
    body.extend_from_slice(b" atomic");
    body.extend_from_slice(&plain[at + caps.len()..]);
    body
}

/// Under `--atomic`, a replayed command is one more accepted command: a
/// refused peer refuses it too (and writes nothing), and an accepted peer
/// is written while the replay writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_atomic_replay_goes_with_its_batch() {
    const OTHER: &str = "refs/heads/other";
    let s = Server::start().await;
    let w = s.clone_dir("w");
    let t1 = w.commit("a.txt", "one\n", "one");
    w.git(&["push", "-q", &s.url(), "main", "main:refs/heads/other"]);
    let t2 = w.commit("a.txt", "two\n", "two");
    let pack = pack_of(&w, &t2, &t1);
    let replay = (oid(&t1), oid(&t2), MAIN);
    let first = post(s.url(), atomic_request(&[replay], &pack)).await;
    assert!(first.contains(&format!("ok {MAIN}")), "{first:?}");
    let main = s.check_ref(MAIN, &w).await;
    let other = s.check_ref(OTHER, &w).await;

    // The replay with a stale peer: both refused, nothing written.
    let stale = (oid(&t2), oid(&t2), OTHER);
    let r = post(s.url(), atomic_request(&[replay, stale], &pack)).await;
    for name in [MAIN, OTHER] {
        assert!(
            r.iter().any(|l| l.starts_with(&format!("ng {name} "))),
            "{name}: {r:?}"
        );
    }
    assert_eq!(s.live(MAIN).await.id, main.id);
    assert_eq!(s.live(OTHER).await.id, other.id);

    // The replay with a new update: the update is written, the replay is
    // ok and writes nothing.
    let update = (oid(&t1), oid(&t2), OTHER);
    let r = post(s.url(), atomic_request(&[replay, update], &pack)).await;
    for name in [MAIN, OTHER] {
        assert!(r.contains(&format!("ok {name}")), "{name}: {r:?}");
    }
    assert_eq!(
        s.live(MAIN).await.id,
        main.id,
        "the replay writes no version"
    );
    let moved = s.check_ref(OTHER, &w).await;
    assert_eq!(moved.value.tip(), Some(oid(&t2)));
    assert_eq!(moved.parent, Some(other.id));
}

/// Sets the repository's force policy, as an operator changing it between
/// two pushes would.
async fn set_force_policy(s: &Server, policy: &str) {
    sqlx::query("UPDATE repos SET allow_force = $1 WHERE id = $2")
        .bind(policy)
        .bind(s.repo.id.get())
        .execute(s.meta.pool())
        .await
        .unwrap();
}

/// A resent command is `ok` only if its own rules accept the move it
/// matches: here trunk was forced while the policy allowed it, and the same
/// request arriving after the policy stopped allowing it is refused, as it
/// would have been had it arrived first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replay_its_own_rules_refuse_is_refused() {
    let s = Server::start_with(|_| {}, aspen_core::repo::ForcePolicy::All).await;
    let w = s.clone_dir("w");
    let t1 = w.commit("a.txt", "one\n", "one");
    w.git(&["push", "-q", &s.url(), "main"]);
    // An unrelated tip: moving trunk there is a force.
    w.git(&["checkout", "-q", "--orphan", "side"]);
    let x = w.commit("b.txt", "side\n", "side");
    let body = request(&[(oid(&t1), oid(&x), MAIN)], &[], &pack_of(&w, &x, &t1));
    let first = post(s.url(), body.clone()).await;
    assert!(first.contains(&format!("ok {MAIN}")), "{first:?}");
    let forced = s.check_ref(MAIN, &w).await;
    assert_eq!(forced.value.tip(), Some(oid(&x)));

    set_force_policy(&s, "branches").await;
    let r = post(s.url(), body.clone()).await;
    assert!(
        r.iter()
            .any(|l| l.starts_with(&format!("ng {MAIN} incorrect old value"))),
        "trunk may not be forced now: {r:?}"
    );
    assert_eq!(s.live(MAIN).await.id, forced.id);

    // The same request where the policy allows the move is the replay.
    set_force_policy(&s, "all").await;
    let r = post(s.url(), body).await;
    assert!(r.contains(&format!("ok {MAIN}")), "{r:?}");
    assert_eq!(s.live(MAIN).await.id, forced.id, "and writes nothing");
}

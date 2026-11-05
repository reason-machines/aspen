//! Push hooks (`receive::hook`) through stock `git push`: what a hook is
//! asked, against which stored tip, and what a refusal does.

mod receive_support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::id::ObjectId;
use aspen_core::ingest::ObjectKind;
use aspen_core::repo::{ForcePolicy, Repo};
use aspen_server::receive::hook::{
    Caller, CommitRange, CommittedUpdate, HookFuture, HookObjects, HookUpdate, HookVerdict,
    PushHook,
};
use receive_support::*;

/// One `check` call as the hook saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Asked {
    name: String,
    stored: Option<ObjectId>,
    new: Option<ObjectId>,
    trunk: Option<ObjectId>,
    /// `range(new, [trunk, stored])`, oldest first.
    range: Option<Vec<ObjectId>>,
    caller: Caller,
}

#[derive(Debug, Default)]
struct Recorder {
    asked: Mutex<Vec<Asked>>,
    committed: Mutex<Vec<Vec<CommittedUpdate>>>,
    /// Refs to refuse.
    refuse: Vec<String>,
    /// Refs whose check fails with a storage error.
    fail: Vec<String>,
    /// Calls for this ref wait here (the first two only), and a call for
    /// the second commit sleeps after it, so the first commit wins.
    race: Option<(String, Arc<tokio::sync::Barrier>, ObjectId)>,
}

impl PushHook for Recorder {
    fn check<'a>(
        &'a self,
        update: HookUpdate<'a>,
        objects: &'a mut dyn HookObjects,
    ) -> HookFuture<'a, Result<HookVerdict, StoreError>> {
        Box::pin(async move {
            let range = match update.new {
                None => None,
                Some(new) => {
                    // The new tip is readable, as a commit.
                    let (kind, _) = objects.read(new).await?.expect("the new tip is readable");
                    assert_eq!(kind, ObjectKind::Commit);
                    let exclude: Vec<_> = update.trunk.into_iter().chain(update.stored).collect();
                    match objects.range(new, &exclude, 100).await? {
                        CommitRange::Commits(c) => Some(c.into_iter().map(|c| c.id).collect()),
                        other => panic!("{other:?}"),
                    }
                }
            };
            let first_calls = {
                let mut asked = self.asked.lock().unwrap();
                asked.push(Asked {
                    name: update.name.as_str().to_owned(),
                    stored: update.stored,
                    new: update.new,
                    trunk: update.trunk,
                    range,
                    caller: update.caller.clone(),
                });
                asked.iter().filter(|a| a.name.ends_with("race")).count() <= 2
            };
            if let Some((name, barrier, slow)) = &self.race
                && update.name.as_str() == name
                && first_calls
            {
                barrier.wait().await;
                if update.new == Some(*slow) {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
            if self.fail.iter().any(|r| r == update.name.as_str()) {
                return Err(StoreError::Unavailable {
                    reason: "the test hook's store is down".into(),
                });
            }
            Ok(if self.refuse.iter().any(|r| r == update.name.as_str()) {
                HookVerdict::Reject {
                    reason: "refused by the test hook".into(),
                    details: vec!["first detail line".into(), "second detail line".into()],
                }
            } else {
                HookVerdict::Accept
            })
        })
    }

    fn committed<'a>(
        &'a self,
        _repo: &'a Repo,
        _caller: &'a Caller,
        updates: &'a [CommittedUpdate],
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            self.committed.lock().unwrap().push(updates.to_vec());
        })
    }
}

async fn start(hook: Arc<Recorder>) -> Server {
    Server::start_with(
        move |c| c.hook = Some(hook as Arc<dyn PushHook>),
        ForcePolicy::Branches,
    )
    .await
}

/// A server with `hook`, beside `other` (whose working directories stay
/// usable, since they live in `other`'s directory).
async fn start_in(_other: &Server, hook: Arc<Recorder>) -> Server {
    start(hook).await
}

async fn push(w: &Work, args: &[&str]) -> (bool, String) {
    let mut cmd = w.command();
    cmd.arg("push").args(args);
    blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await
}

fn oid(hex: &str) -> ObjectId {
    hex.trim().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_shows_its_reason_and_details_and_stores_nothing_for_that_ref() {
    let hook = Arc::new(Recorder {
        refuse: vec!["refs/heads/bad".into()],
        ..Recorder::default()
    });
    let s = start(hook.clone()).await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "a\n", "a");
    w.git(&["branch", "bad"]);
    let (ok, out) = push(&w, &[&s.url(), "main", "bad"]).await;
    assert!(!ok, "{out}");
    assert!(out.contains("remote: aspen: first detail line"), "{out}");
    assert!(out.contains("remote: aspen: second detail line"), "{out}");
    assert!(
        out.contains("[remote rejected] bad -> bad (refused by the test hook)"),
        "{out}"
    );
    assert!(s.current("refs/heads/main").await.is_some(), "main landed");
    assert_eq!(
        s.current("refs/heads/bad").await,
        None,
        "the refused ref was not written"
    );
    // Told about main only, once; anonymous callers carry their address key.
    let committed = hook.committed.lock().unwrap().clone();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].len(), 1);
    assert_eq!(committed[0][0].name.as_str(), "refs/heads/main");
    let asked = hook.asked.lock().unwrap().clone();
    assert_eq!(asked[0].caller.principal, "anonymous");
    // This harness serves without connect info, so the address is unknown;
    // the key is still the access layer's anonymous form.
    assert!(
        asked[0].caller.key.starts_with("ip:"),
        "{:?}",
        asked[0].caller
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hook_sees_the_stored_tip_not_the_clients_zero_old_value_and_the_exact_range() {
    let hook = Arc::new(Recorder::default());
    let s = start(hook.clone()).await;
    let w = s.clone_dir("w");
    let c0 = oid(&w.commit("a.txt", "a\n", "c0"));
    assert!(push(&w, &[&s.url(), "main"]).await.0);
    w.git(&["checkout", "-q", "-b", "topic"]);
    let c1 = oid(&w.commit("b.txt", "b\n", "c1"));
    assert!(push(&w, &[&s.url(), "topic"]).await.0);
    let c2 = oid(&w.commit("c.txt", "c\n", "c2"));
    let c3 = oid(&w.commit("d.txt", "d\n", "c3"));
    // topic is not advertised on the plain URL: git sends a zero old value.
    assert!(push(&w, &[&s.url(), "topic"]).await.0);
    let asked = hook.asked.lock().unwrap().clone();
    let topic: Vec<_> = asked
        .iter()
        .filter(|a| a.name == "refs/heads/topic")
        .collect();
    assert_eq!(topic.len(), 2);
    assert_eq!((topic[0].stored, topic[0].trunk), (None, Some(c0)));
    assert_eq!(topic[0].range.as_deref(), Some(&[c1][..]));
    assert_eq!(
        topic[1].stored,
        Some(c1),
        "the server's stored tip, not the zero old value"
    );
    assert_eq!(topic[1].new, Some(c3));
    assert_eq!(
        topic[1].range.as_deref(),
        Some(&[c2, c3][..]),
        "parents first, c1 excluded"
    );
    // Deletes are asked about with no new tip, and reported committed.
    assert!(
        push(
            &w,
            &[&s.scoped_url("topic"), "--delete", "refs/heads/topic"]
        )
        .await
        .0
    );
    let last = hook.asked.lock().unwrap().last().cloned().unwrap();
    assert_eq!((last.stored, last.new), (Some(c3), None));
    let committed = hook.committed.lock().unwrap().clone();
    assert_eq!(committed.last().unwrap()[0].new, None);
    assert_eq!(committed.last().unwrap()[0].old, Some(c3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_fails_an_atomic_push_as_a_whole() {
    let hook = Arc::new(Recorder {
        refuse: vec!["refs/heads/topic".into()],
        ..Recorder::default()
    });
    let s = start(hook.clone()).await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "a\n", "a");
    w.git(&["branch", "topic"]);
    let (ok, out) = push(&w, &["--atomic", &s.url(), "main", "topic"]).await;
    assert!(!ok, "{out}");
    assert!(out.contains("atomic push failed"), "{out}");
    assert_eq!(s.current("refs/heads/main").await, None);
    assert!(hook.committed.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_lost_compare_and_swap_the_hook_is_asked_again_against_the_new_stored_tip() {
    // Two clients create one branch at once: c1, and c2 on top of c1. The
    // hook holds both checks until both arrive, then lets c1 commit first,
    // so c2's compare-and-swap is lost and its verdict recomputed.
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let s = Server::start().await;
    let a = s.clone_dir("a");
    let base = oid(&a.commit("a.txt", "a\n", "base"));
    let c1 = oid(&a.commit("b.txt", "b\n", "c1"));
    let c2 = oid(&a.commit("c.txt", "c\n", "c2"));
    let b = s.clone_dir("b");
    b.git(&["fetch", "-q", a.path.to_str().unwrap(), "main"]);
    let hook = Arc::new(Recorder {
        race: Some(("refs/heads/race".into(), barrier, c2)),
        ..Recorder::default()
    });
    let s = start_in(&s, hook.clone()).await;
    assert!(
        push(&a, &[&s.url(), &format!("{base}:refs/heads/main")])
            .await
            .0
    );
    let url = s.url();
    let (spec1, spec2) = (
        format!("{c1}:refs/heads/race"),
        format!("{c2}:refs/heads/race"),
    );
    let (args1, args2) = (
        [url.as_str(), spec1.as_str()],
        [url.as_str(), spec2.as_str()],
    );
    let (ra, rb) = tokio::join!(push(&a, &args1), push(&b, &args2));
    assert!(ra.0, "{}", ra.1);
    assert!(
        rb.0,
        "the second push is a fast-forward of the first: {}",
        rb.1
    );
    assert_eq!(s.live("refs/heads/race").await.value.tip(), Some(c2));
    let asked: Vec<Asked> = hook
        .asked
        .lock()
        .unwrap()
        .iter()
        .filter(|x| x.name == "refs/heads/race")
        .cloned()
        .collect();
    let for_c2: Vec<_> = asked.iter().filter(|x| x.new == Some(c2)).collect();
    assert_eq!(
        for_c2.len(),
        2,
        "asked again after the lost swap: {asked:?}"
    );
    assert_eq!((for_c2[0].stored, for_c2[1].stored), (None, Some(c1)));
    assert_eq!(
        for_c2[1].range.as_deref(),
        Some(&[c2][..]),
        "c1 is now excluded"
    );
    // Each committed update is reported once.
    let committed = hook.committed.lock().unwrap().clone();
    let race: Vec<_> = committed
        .iter()
        .flatten()
        .filter(|u| u.name.as_str() == "refs/heads/race")
        .collect();
    assert_eq!(race.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_no_op_in_a_committed_atomic_push_is_neither_asked_about_nor_reported() {
    let hook = Arc::new(Recorder::default());
    let s = start(hook.clone()).await;
    let w = s.clone_dir("w");
    let c1 = oid(&w.commit("a.txt", "a\n", "c1"));
    w.git(&["branch", "topic"]);
    assert!(push(&w, &[&s.url(), "main", "topic"]).await.0);
    // topic is not advertised, so git sends it again (zero old value, its
    // current tip): a no-op, in the same atomic group as main's update.
    let c2 = oid(&w.commit("b.txt", "b\n", "c2"));
    let (ok, out) = push(&w, &["--atomic", &s.url(), "main", "topic"]).await;
    assert!(ok, "{out}");
    let last = hook.committed.lock().unwrap().last().cloned().unwrap();
    assert_eq!(last.len(), 1, "{last:?}");
    assert_eq!(
        (last[0].name.as_str(), last[0].old, last[0].new),
        ("refs/heads/main", Some(c1), Some(c2))
    );
    let asked = hook.asked.lock().unwrap().clone();
    assert_eq!(
        asked
            .iter()
            .filter(|a| a.name == "refs/heads/topic")
            .count(),
        1,
        "only its first push"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hook_error_is_a_retryable_unpack_failure_that_stores_nothing() {
    let hook = Arc::new(Recorder {
        fail: vec!["refs/heads/topic".into()],
        ..Recorder::default()
    });
    let s = start(hook.clone()).await;
    let w = s.clone_dir("w");
    w.commit("a.txt", "a\n", "a");
    w.git(&["branch", "topic"]);
    // Not atomic: main's check passed, but every check runs before any
    // commit, so the error stops main too.
    let (ok, out) = push(&w, &[&s.url(), "main", "topic"]).await;
    assert!(!ok, "{out}");
    assert!(
        out.contains("unpack failed: internal storage error; retry"),
        "{out}"
    );
    assert!(!out.contains("the test hook's store is down"), "{out}");
    assert_eq!(s.current("refs/heads/main").await, None);
    assert_eq!(s.current("refs/heads/topic").await, None);
    assert!(
        hook.committed.lock().unwrap().is_empty(),
        "committed() not called"
    );
}

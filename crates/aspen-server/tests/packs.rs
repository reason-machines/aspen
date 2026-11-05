//! Push step 2's test obligations (`docs/PLAN.md`), end to end through the
//! HTTP service with hand-built packs: the all-REF_DELTA shapes libgit2
//! sends, a resend of objects the dependencies hold, forward bases, a base
//! that is nowhere and a pack that repeats an object. Every stored pack must
//! index under stock `git index-pack` without `--fix-thin`, with no ID twice
//! (`Server::check_ref` and `Server::verify_stored`).

mod receive_support;

use aspen_core::id::ObjectId;
use aspen_core::refs::Closure;
use receive_support::*;

const Z: ObjectId = ObjectId::ZERO;
const MAIN: &str = "refs/heads/main";

/// A blob large enough that its delta is much smaller than it.
fn text(tag: &str) -> Vec<u8> {
    (0..400)
        .map(|i| format!("{tag} line {i}\n"))
        .collect::<String>()
        .into_bytes()
}

struct History {
    b1: Obj,
    b2: Obj,
    t1: Obj,
    c1: Obj,
}

/// c1: tree { a = b1, b = b2 }, where b2 = b1 + a suffix.
fn history() -> History {
    let b1 = Obj::blob(text("base"));
    let mut b2d = b1.data.clone();
    b2d.extend_from_slice(b"second\n");
    let b2 = Obj::blob(b2d);
    let t1 = Obj::tree(&[("100644", "a", b1.id()), ("100644", "b", b2.id())]);
    let c1 = Obj::commit(t1.id(), &[], "c1", 1_700_000_000);
    History { b1, b2, t1, c1 }
}

async fn report(s: &Server, commands: &[(ObjectId, ObjectId, &str)], pack: &[u8]) -> Vec<String> {
    post(s.url(), request(commands, &[], pack)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_all_ref_delta_pack_with_in_pack_bases_is_accepted() {
    // An ordinary libgit2 push: every delta a REF_DELTA against a base in
    // the same pack; every candidate misses in the (empty) dependencies.
    let s = Server::start().await;
    let h = history();
    let pack = build_pack(&[
        Entry::Full(h.b1.clone()),
        Entry::RefDelta {
            base: h.b1.clone(),
            target: h.b2.clone(),
        },
        Entry::Full(h.t1.clone()),
        Entry::Full(h.c1.clone()),
    ]);
    let r = report(&s, &[(Z, h.c1.id(), MAIN)], &pack).await;
    assert_eq!(r[0], "unpack ok", "{r:?}");
    assert_eq!(r[1], format!("ok {MAIN}"));
    let w = s.work_from_packs("check", &[&pack]);
    let v = s.check_ref(MAIN, &w).await;
    assert_eq!(v.value.closure(), Some(Closure::Exact));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_all_ref_delta_resend_of_held_objects_stores_no_duplicate() {
    // S4's dup.sh model: the client resends history the server has, all
    // REF_DELTA, including a delta against a deltified object of its pack.
    let s = Server::start().await;
    let h = history();
    let first = build_pack(&[
        Entry::Full(h.b1.clone()),
        Entry::RefDelta {
            base: h.b1.clone(),
            target: h.b2.clone(),
        },
        Entry::Full(h.t1.clone()),
        Entry::Full(h.c1.clone()),
    ]);
    assert_eq!(
        report(&s, &[(Z, h.c1.id(), MAIN)], &first).await[0],
        "unpack ok"
    );
    let mut b3d = h.b2.data.clone();
    b3d.extend_from_slice(b"third\n");
    let b3 = Obj::blob(b3d);
    let t2 = Obj::tree(&[
        ("100644", "a", h.b1.id()),
        ("100644", "b", h.b2.id()),
        ("100644", "c", b3.id()),
    ]);
    let c2 = Obj::commit(t2.id(), &[h.c1.id()], "c2", 1_700_000_100);
    let resend = build_pack(&[
        Entry::Full(h.b1.clone()),
        // b2 against b1, and b3 against b2 (itself a delta here): both
        // bases are also in the dependencies.
        Entry::RefDelta {
            base: h.b1.clone(),
            target: h.b2.clone(),
        },
        Entry::RefDelta {
            base: h.b2.clone(),
            target: b3.clone(),
        },
        Entry::Full(h.t1.clone()),
        Entry::Full(t2.clone()),
        Entry::Full(h.c1.clone()),
        Entry::Full(c2.clone()),
    ]);
    let r = report(&s, &[(h.c1.id(), c2.id(), MAIN)], &resend).await;
    assert_eq!(r[0], "unpack ok", "{r:?}");
    assert_eq!(r[1], format!("ok {MAIN}"));
    let w = s.work_from_packs("check", &[&first, &resend]);
    let v = s.check_ref(MAIN, &w).await;
    assert_eq!(v.value.closure(), Some(Closure::Exact));
    // The resent objects the dependencies hold are not stored again: the
    // new version's pack holds c2, t2 and b3 (plus at most the bases its
    // deltas need), never a second copy of anything.
    let row = &s.pack_rows(&[v.value.pack().unwrap()]).await[0];
    let own = s.pack_objects(row);
    for id in [c2.id(), t2.id(), b3.id()] {
        assert!(own.contains(&id));
    }
    assert!(!own.contains(&h.c1.id()) && !own.contains(&h.t1.id()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forward_ref_delta_is_stored_base_first() {
    let s = Server::start().await;
    let h = history();
    // b2's delta comes before its base b1, which is only in the pack.
    let pack = build_pack(&[
        Entry::RefDelta {
            base: h.b1.clone(),
            target: h.b2.clone(),
        },
        Entry::Full(h.b1.clone()),
        Entry::Full(h.t1.clone()),
        Entry::Full(h.c1.clone()),
    ]);
    let r = report(&s, &[(Z, h.c1.id(), MAIN)], &pack).await;
    assert_eq!(r[0], "unpack ok", "{r:?}");
    let w = s.work_from_packs("check", &[&pack]);
    s.check_ref(MAIN, &w).await;

    // The same shape when the base is also in the dependencies: a second
    // commit resends b1 after a delta against it.
    let mut b3d = h.b1.data.clone();
    b3d.extend_from_slice(b"forward\n");
    let b3 = Obj::blob(b3d);
    let t2 = Obj::tree(&[("100644", "a", h.b1.id()), ("100644", "z", b3.id())]);
    let c2 = Obj::commit(t2.id(), &[h.c1.id()], "c2", 1_700_000_200);
    let pack2 = build_pack(&[
        Entry::RefDelta {
            base: h.b1.clone(),
            target: b3.clone(),
        },
        Entry::Full(h.b1.clone()),
        Entry::Full(t2.clone()),
        Entry::Full(c2.clone()),
    ]);
    let r = report(&s, &[(h.c1.id(), c2.id(), MAIN)], &pack2).await;
    assert_eq!(r[0], "unpack ok", "{r:?}");
    let w = s.work_from_packs("check2", &[&pack, &pack2]);
    let v = s.check_ref(MAIN, &w).await;
    assert_eq!(v.value.closure(), Some(Closure::Exact));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_base_in_neither_the_pack_nor_the_dependencies_refuses_the_pack() {
    let s = Server::start().await;
    let h = history();
    let pack = build_pack(&[
        Entry::RefDelta {
            base: h.b1.clone(),
            target: h.b2.clone(),
        },
        Entry::Full(h.t1.clone()),
        Entry::Full(h.c1.clone()),
    ]);
    let r = report(&s, &[(Z, h.c1.id(), MAIN)], &pack).await;
    assert!(r[0].starts_with("unpack missing delta base"), "{r:?}");
    assert!(r[0].contains(&h.b1.id().to_hex()));
    assert_eq!(r[1], format!("ng {MAIN} unpacker error"));
    assert_eq!(s.stored_pack_files(), 0, "no pack is stored");
    assert!(s.current(MAIN).await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pack_holding_an_object_twice_is_refused_naming_it() {
    let s = Server::start().await;
    let h = history();
    let pack = build_pack(&[
        Entry::Full(h.b1.clone()),
        Entry::Full(h.b1.clone()),
        Entry::Full(h.b2.clone()),
        Entry::Full(h.t1.clone()),
        Entry::Full(h.c1.clone()),
    ]);
    let r = report(&s, &[(Z, h.c1.id(), MAIN)], &pack).await;
    assert!(r[0].starts_with("unpack "), "{r:?}");
    assert!(
        r[0].contains("twice") && r[0].contains(&h.b1.id().to_hex()),
        "{r:?}"
    );
    assert_eq!(s.stored_pack_files(), 0);
    assert!(s.current(MAIN).await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objects_no_tip_reaches_are_not_stored() {
    let s = Server::start().await;
    let h = history();
    let stray = Obj::blob(b"nobody references me\n".to_vec());
    let pack = build_pack(&[
        Entry::Full(h.b1.clone()),
        Entry::Full(h.b2.clone()),
        Entry::Full(stray.clone()),
        Entry::Full(h.t1.clone()),
        Entry::Full(h.c1.clone()),
    ]);
    let r = report(&s, &[(Z, h.c1.id(), MAIN)], &pack).await;
    assert_eq!(r[0], "unpack ok", "{r:?}");
    let w = s.work_from_packs("check", &[&pack]);
    let v = s.check_ref(MAIN, &w).await;
    assert_eq!(v.value.closure(), Some(Closure::Exact));
    let own = s.pack_objects(&s.pack_rows(&[v.value.pack().unwrap()]).await[0]);
    assert!(!own.contains(&stray.id()));
    assert_eq!(own.len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_referenced_object_refuses_the_pack() {
    // The connectivity check: c1's tree names b2, which is nowhere.
    let s = Server::start().await;
    let h = history();
    let pack = build_pack(&[
        Entry::Full(h.b1.clone()),
        Entry::Full(h.t1.clone()),
        Entry::Full(h.c1.clone()),
    ]);
    let r = report(&s, &[(Z, h.c1.id(), MAIN)], &pack).await;
    assert!(r[0].starts_with("unpack missing object"), "{r:?}");
    assert!(r[0].contains(&h.b2.id().to_hex()));
    assert!(s.current(MAIN).await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_flush_probe_gets_an_empty_answer() {
    // git sends `0000` first when a push is larger than http.postBuffer.
    let s = Server::start().await;
    let r = post(s.url(), b"0000".to_vec()).await;
    assert!(r.is_empty(), "{r:?}");
}

/// An object as `git cat-file` shows it.
fn cat(w: &Work, rev: &str) -> Obj {
    let kind = match w.git(&["cat-file", "-t", rev]).trim() {
        "commit" => "commit",
        "tree" => "tree",
        "blob" => "blob",
        _ => "tag",
    };
    let out = w.command().args(["cat-file", kind, rev]).output().unwrap();
    assert!(out.status.success());
    Obj {
        kind,
        data: out.stdout,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forced_trunk_update_copies_objects_only_skipped_versions_hold() {
    // Stock git resends such objects (it excludes only its edge commits'
    // trees), so this pack is hand-built: trunk is rewound from t3 to t1
    // plus a commit whose tree reuses a blob only t2 brought. The client
    // leaves that blob out, because the trunk it was shown (t3) reaches it.
    let s = Server::start_with(|_| {}, aspen_core::repo::ForcePolicy::All).await;
    let w = s.clone_dir("w");
    let url = s.url();
    let mut first = None;
    for (file, text, msg) in [
        ("a.txt", "one\n", "t1"),
        ("b.txt", "only in t2\n", "t2"),
        ("c.txt", "t3\n", "t3"),
    ] {
        w.commit(file, text, msg);
        let mut cmd = w.command();
        cmd.args(["push", "-q", &url, "main"]);
        assert!(blocking(move || cmd.status().unwrap()).await.success());
        first.get_or_insert(s.live(MAIN).await.id);
    }
    let t3 = oid(&w.rev("main"));
    let t1 = first;
    w.git(&["reset", "-q", "--hard", "HEAD~2"]);
    let new = w.commit("b2.txt", "only in t2\n", "t1'");
    let blob = w.rev("HEAD:b2.txt");
    let pack = build_pack(&[
        Entry::Full(cat(&w, "HEAD^{tree}")),
        Entry::Full(cat(&w, "HEAD")),
    ]);
    let r = post(s.url(), request(&[(t3, oid(&new), MAIN)], &[], &pack)).await;
    assert_eq!(r[1], format!("ok {MAIN}"), "{r:?}");
    let v = s.check_ref(MAIN, &w).await;
    assert_eq!(v.value.closure(), Some(Closure::Exact));
    assert_eq!(v.deps.trunk, t1, "t1 is the newest ancestor");
    let own = s.pack_objects(&s.pack_rows(&[v.value.pack().unwrap()]).await[0]);
    assert!(own.contains(&oid(&blob)), "the skipped blob is copied in");
}

/// Invariant: every read of stored packs holds a prefetch slot, and no
/// code path takes a slot while it holds one. The hand-built forced trunk
/// update above (it copies a skipped blob), with ancestry walks that may
/// read unindexed commits from the packs, completes under a one-slot gate
/// (a nested acquisition would time out), and its reads were gated: the
/// skipped-object read took a slot besides completion's. (Whether the
/// walk reads a commit from a pack depends on how equal committer times
/// order its queue, so only those two are counted.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forced_trunk_update_runs_under_a_one_slot_gate() {
    let s = Server::start_with(
        |c| {
            c.prefetch_memory_bytes = c.locate.max_prefetch_bytes;
            c.prefetch_wait = std::time::Duration::from_secs(3);
            // Only tips get rows, so ancestry reads commits from packs.
            c.sync_commit_rows = 0;
        },
        aspen_core::repo::ForcePolicy::All,
    )
    .await;
    assert_eq!(s.svc.prefetch_slots(), 1);
    let w = s.clone_dir("w");
    let url = s.url();
    w.commit("z.txt", "zero\n", "t0");
    let mut first = None;
    for (file, text, msg) in [
        ("a.txt", "one\n", "t1"),
        ("b.txt", "only in t2\n", "t2"),
        ("c.txt", "t3\n", "t3"),
    ] {
        w.commit(file, text, msg);
        let mut cmd = w.command();
        cmd.args(["push", "-q", &url, "main"]);
        assert!(blocking(move || cmd.status().unwrap()).await.success());
        first.get_or_insert(s.live(MAIN).await.id);
    }
    let t3 = oid(&w.rev("main"));
    w.git(&["reset", "-q", "--hard", "HEAD~2"]);
    let new = w.commit("b2.txt", "only in t2\n", "t1'");
    let blob = w.rev("HEAD:b2.txt");
    let pack = build_pack(&[
        Entry::Full(cat(&w, "HEAD^{tree}")),
        Entry::Full(cat(&w, "HEAD")),
    ]);
    let before = s.svc.prefetch_slots_acquired();
    let r = post(s.url(), request(&[(t3, oid(&new), MAIN)], &[], &pack)).await;
    assert_eq!(r[1], format!("ok {MAIN}"), "{r:?}");
    let gated = s.svc.prefetch_slots_acquired() - before;
    let v = s.check_ref(MAIN, &w).await;
    assert_eq!(v.value.closure(), Some(Closure::Exact));
    assert_eq!(v.deps.trunk, first);
    let own = s.pack_objects(&s.pack_rows(&[v.value.pack().unwrap()]).await[0]);
    assert!(own.contains(&oid(&blob)), "the skipped blob is copied in");
    assert!(gated >= 2, "only {gated} reads took a slot");
    assert_eq!(s.svc.prefetch_peak(), 1);
}

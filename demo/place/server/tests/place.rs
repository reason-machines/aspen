//! `place` end to end: stock `git` and HTTP against the assembled app.
//! Each test states the invariant it pins.

mod support;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use support::*;

const RED: &str = "#d6262b";
const BLUE: &str = "#3690ea";
const YELLOW: &str = "#ffd635";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_git_push_paints_a_branch_and_the_merger_fast_forwards_main() {
    let app = App::start(|_| {}).await;
    let main_before = app
        .tip("refs/heads/main")
        .await
        .expect("main is created at start");
    let w = app.clone_work("ada").await;
    assert!(
        w.path.join("README.md").exists(),
        "main starts with a README"
    );
    w.paint(&[(1, 1, RED), (2, 1, BLUE)], "two tiles");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/ada"]).await;
    assert!(ok, "{out}");

    // The push is on the canvas as an overlay and in the feed.
    let canvas = app.get("/api/v1/canvas").await;
    let overlay = &canvas["branches"][0];
    assert_eq!(overlay["name"], "paint/ada");
    assert_eq!(
        overlay["actor"],
        json!({ "kind": "git", "label": "paint/ada" })
    );
    assert_eq!(overlay["tiles"].as_array().unwrap().len(), 2);
    let mut feed = app.events("?after=0", &[]).await;
    let got = feed
        .until(Duration::from_secs(5), |e| e.kind == "push")
        .await;
    let push = &got.last().unwrap().data;
    assert_eq!(
        push["tiles"],
        json!([{ "x": 1, "y": 1, "color": RED }, { "x": 2, "y": 1, "color": BLUE }])
    );
    let push_seq = push["seq"].as_u64().unwrap();

    // The merger folds it into main as one commit on top of main's tip.
    let merge_seq = app.place.merge_once().await.unwrap().expect("a merge");
    let merge = feed
        .until(Duration::from_secs(5), |e| e.kind == "merge")
        .await;
    let merge = &merge.last().unwrap().data;
    assert_eq!(merge["seq"].as_u64(), Some(merge_seq));
    assert_eq!(merge["throughSeq"].as_u64(), Some(push_seq));
    assert_eq!(merge["branches"], json!(["paint/ada"]));
    let main_after = app.tip("refs/heads/main").await.unwrap();
    assert_eq!(merge["commit"], main_after);
    let check = app.clone_work("check").await;
    let parents = check.git(&["rev-list", "--parents", "-n1", "HEAD"]);
    assert_eq!(
        parents.split_whitespace().collect::<Vec<_>>(),
        [&main_after[..], &main_before[..]]
    );
    assert_eq!(
        std::fs::read(check.path.join("tiles/1_1.txt")).unwrap(),
        b"#d6262b\n"
    );
    assert_eq!(
        std::fs::read(check.path.join("tiles/2_1.txt")).unwrap(),
        b"#3690ea\n"
    );
    // Only tile files changed, and the history is valid for stock git.
    let changed = check.git(&["diff", "--name-only", &main_before, "HEAD"]);
    assert_eq!(
        changed.lines().collect::<Vec<_>>(),
        ["tiles/1_1.txt", "tiles/2_1.txt"]
    );
    check.git(&["fsck", "--strict", "--no-progress"]);

    // main's pixels and the merged overlay.
    let canvas = app.get("/api/v1/canvas").await;
    assert_eq!(canvas["mergedThroughSeq"].as_u64(), Some(push_seq));
    assert_eq!(canvas["branches"], json!([]));
    let pixels = canvas["pixels"].as_str().unwrap();
    assert_eq!(
        &pixels[16 + 1..16 + 3],
        "5c",
        "red is palette 5, blue 12 (c)"
    );
    // Nothing pending: no empty merge commit.
    assert_eq!(app.place.merge_once().await.unwrap(), None);
    assert_eq!(app.tip("refs/heads/main").await.unwrap(), main_after);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_merger_replays_pushes_in_seq_order_and_main_stays_linear() {
    let app = App::start(|_| {}).await;
    let a = app.clone_work("a").await;
    let b = app.clone_work("b").await;
    a.paint(&[(3, 3, RED), (4, 4, RED)], "a");
    assert!(a.push(&[&app.url(), "HEAD:refs/heads/paint/a"]).await.0);
    b.paint(&[(3, 3, BLUE)], "b");
    assert!(b.push(&[&app.url(), "HEAD:refs/heads/paint/b"]).await.0);
    // a repaints the tile b painted, after b: a's later push wins.
    a.paint(&[(3, 3, YELLOW)], "a again");
    assert!(a.push(&[&app.url(), "HEAD:refs/heads/paint/a"]).await.0);
    app.place.merge_once().await.unwrap().unwrap();
    let c = app.clone_work("c").await;
    assert_eq!(
        std::fs::read(c.path.join("tiles/3_3.txt")).unwrap(),
        b"#ffd635\n"
    );
    assert_eq!(
        std::fs::read(c.path.join("tiles/4_4.txt")).unwrap(),
        b"#d6262b\n"
    );
    // Two commits on main, each with one parent.
    let log = c.git(&["log", "--format=%p", "main"]);
    let parents: Vec<usize> = log.lines().map(|l| l.split_whitespace().count()).collect();
    assert_eq!(parents, [1, 0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_tile_rule_is_enforced_through_git_push_and_a_refused_push_stores_no_ref() {
    let app = App::start(|_| {}).await;
    let w = app.clone_work("w").await;
    // One commit with five problems: all five are reported, one per line.
    w.write("README.md", b"changed\n");
    w.write("tiles/1_1.txt", b"#D6262B\n");
    w.write("tiles/2_2.txt", b"#d6262b\r\n");
    w.write("tiles/99_1.txt", b"#d6262b\n");
    w.write("tiles/01_1.txt", b"#d6262b\n");
    w.write("tiles/3_3.txt", b"#123456\n");
    w.git(&["add", "-A"]);
    w.git(&["commit", "-q", "-m", "bad"]);
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/bad"]).await;
    assert!(!ok, "{out}");
    for line in [
        "README.md: only files under tiles/ may change",
        "tiles/01_1.txt: tile files are named tiles/<x>_<y>.txt",
        "tiles/1_1.txt: use lowercase hex digits",
        "tiles/2_2.txt: use LF line endings, not CRLF",
        "tiles/3_3.txt: #123456 is not in the palette",
        "tiles/99_1.txt: (99, 1) is outside the 16x16 canvas",
    ] {
        assert!(out.contains(line), "missing {line:?} in:\n{out}");
    }
    assert!(out.contains("6 problems with this push"), "{out}");
    assert_eq!(
        app.current("refs/heads/paint/bad").await,
        None,
        "nothing is written"
    );

    // Modes, deletes, merges and refs, each on its own.
    let fresh = |name: &'static str| {
        let app = &app;
        async move { app.clone_work(name).await }
    };
    let exec = fresh("exec").await;
    exec.write("tiles/1_1.txt", b"#d6262b\n");
    exec.git(&["add", "-A"]);
    exec.git(&["update-index", "--chmod=+x", "tiles/1_1.txt"]);
    exec.git(&["commit", "-q", "-m", "x"]);
    let (ok, out) = exec.push(&[&app.url(), "HEAD:refs/heads/paint/x"]).await;
    assert!(!ok && out.contains("not mode 100755"), "{out}");

    let link = fresh("link").await;
    std::os::unix::fs::symlink("../README.md", link.path.join("tiles/1_1.txt")).ok();
    if !link.path.join("tiles").exists() {
        std::fs::create_dir_all(link.path.join("tiles")).unwrap();
        std::os::unix::fs::symlink("../README.md", link.path.join("tiles/1_1.txt")).unwrap();
    }
    link.git(&["add", "-A"]);
    link.git(&["commit", "-q", "-m", "link"]);
    let (ok, out) = link.push(&[&app.url(), "HEAD:refs/heads/paint/link"]).await;
    assert!(!ok && out.contains("not mode 120000"), "{out}");

    // A merge commit, and a delete of a tile main has.
    let m = fresh("merge").await;
    m.paint(&[(5, 5, RED)], "paint");
    assert!(m.push(&[&app.url(), "HEAD:refs/heads/paint/m"]).await.0);
    app.place.merge_once().await.unwrap().unwrap();
    m.git(&["checkout", "-q", "-b", "side", "HEAD~1"]);
    m.paint(&[(6, 6, RED)], "side");
    m.git(&["checkout", "-q", "-"]);
    m.git(&["merge", "-q", "--no-edit", "side"]);
    let (ok, out) = m.push(&[&app.url(), "HEAD:refs/heads/paint/m2"]).await;
    assert!(
        !ok && out.contains("merge commits are not accepted"),
        "{out}"
    );
    let d = fresh("delete").await;
    d.git(&["rm", "-q", "tiles/5_5.txt"]);
    d.git(&["commit", "-q", "-m", "rm"]);
    let (ok, out) = d.push(&[&app.url(), "HEAD:refs/heads/paint/d"]).await;
    assert!(
        !ok && out.contains("tiles/5_5.txt: tiles can be repainted but not deleted"),
        "{out}"
    );

    // Refs: main, the server's namespaces, tags. The anonymous policy may
    // write only refs/heads/*, so the tag goes with a token that may write
    // any ref, to reach the rules themselves.
    let r = fresh("refs").await;
    r.paint(&[(7, 7, RED)], "ok");
    let token = app.token("dev", &["write:canvas"]);
    let header = format!("http.extraHeader=Authorization: Bearer {token}");
    for (target, text, auth) in [
        (
            "HEAD:refs/heads/main",
            "main is written by the merger bot",
            false,
        ),
        (
            "HEAD:refs/heads/main",
            "main is written by the merger bot",
            true,
        ),
        (
            "HEAD:refs/heads/visitors/abc",
            "belong to the web page and its agents",
            false,
        ),
        (
            "HEAD:refs/heads/swarm/agent-0001",
            "belong to the web page and its agents",
            false,
        ),
        (
            "HEAD:refs/heads/agent/moss-12",
            "belong to the web page and its agents",
            true,
        ),
        ("HEAD:refs/tags/v1", "permission denied", false),
        ("HEAD:refs/tags/v1", "only branches", true),
    ] {
        let mut args = vec![];
        if auth {
            args.extend(["-c", header.as_str()]);
        }
        let (ok, out) = r.push_with(&args, &[&app.url(), target]).await;
        assert!(!ok && out.contains(text), "{target}: {out}");
    }
    // The same commit is fine on the pusher's own branch.
    assert!(r.push(&[&app.url(), "HEAD:refs/heads/paint/r"]).await.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_is_checked_over_commits_new_to_it_not_from_the_client_old_value() {
    // TILES.md's worked example with a cap of 4 tiles per push, on a
    // branch outside `paint/` so its step 3 force-push is allowed.
    let app = App::start(|c| c.rules.tiles_per_push = 4).await;
    let w = app.clone_work("ada").await;
    let t0 = w.git(&["rev-parse", "HEAD"]).trim().to_owned();
    w.paint(&[(0, 0, RED), (1, 0, RED), (2, 0, RED)], "A1");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/ada"]).await.0);
    // The merger moves trunk meanwhile.
    app.place.merge_once().await.unwrap().unwrap();
    // A2 on A1: the branch is not advertised, so git sends a zero old
    // value; the server excludes its stored tip A1, so 2 tiles count.
    w.paint(&[(3, 0, RED), (4, 0, RED)], "A2");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/ada"]).await;
    assert!(ok, "counted A1 again: {out}");
    // A3 alone paints 5 tiles: refused.
    w.paint(
        &[
            (5, 0, BLUE),
            (6, 0, BLUE),
            (7, 0, BLUE),
            (8, 0, BLUE),
            (9, 0, BLUE),
        ],
        "A3",
    );
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/ada"]).await;
    assert!(
        !ok && out.contains("this push changes 5 tiles; the limit is 4"),
        "{out}"
    );
    // A rewrite on T0 (4 tiles), force-pushed: only B1 counts.
    w.git(&["reset", "-q", "--hard", &t0]);
    w.paint(
        &[
            (0, 5, YELLOW),
            (1, 5, YELLOW),
            (2, 5, YELLOW),
            (3, 5, YELLOW),
        ],
        "B1",
    );
    let (ok, out) = w
        .push(&["-o", "force", &app.url(), "+HEAD:refs/heads/ada"])
        .await;
    assert!(ok, "{out}");
    let canvas = app.get("/api/v1/canvas").await;
    let overlay = canvas["branches"].as_array().unwrap();
    // A2's two tiles (still unmerged) and B1's four, newest per position.
    assert_eq!(overlay[0]["tiles"].as_array().unwrap().len(), 6);
}

/// Invariant: a Git client can create and fast-forward a `paint/`
/// branch, but cannot delete it or rewrite it, however it asks (`-o
/// force`, `--force` through the branch URL, a rewind to an ancestor, a
/// correct `--force-with-lease`, or inside an `--atomic` push whose other
/// update is valid); the refusal tells it to push to a new branch, and no
/// branch the push names moves.
/// The server's own handling of `paint/` branches (merging, expiry's
/// delete) is not affected, and other branches keep Aspen's policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paint_branches_only_move_forward_for_git_clients() {
    let app = App::start(|c| c.branch_ttl = Duration::from_millis(300)).await;
    let bob = "refs/heads/paint/bob";
    let w = app.clone_work("bob").await;
    let t0 = w.git(&["rev-parse", "HEAD"]).trim().to_owned();
    let a1 = w.paint(&[(0, 0, RED)], "A1");
    let (ok, out) = w.push(&[&app.url(), &format!("HEAD:{bob}")]).await;
    assert!(ok, "creation refused: {out}");
    let a2 = w.paint(&[(1, 0, RED)], "A2");
    let (ok, out) = w.push(&[&app.url(), &format!("HEAD:{bob}")]).await;
    assert!(ok, "fast-forward refused: {out}");
    assert_eq!(app.tip(bob).await.as_deref(), Some(&a2[..]));

    // Someone else's rewrite of the branch, on T0.
    let m = app.clone_work("mallory").await;
    m.git(&["reset", "-q", "--hard", &t0]);
    let b1 = m.paint(&[(5, 5, BLUE)], "B1");
    let scoped = format!("{}/-/{bob}", app.url());
    let forward = "paint/ branches only move forward";
    let attempts: [(&str, Vec<String>); 3] = [
        (
            "-o force",
            vec![
                "-o".into(),
                "force".into(),
                app.url(),
                format!("+HEAD:{bob}"),
            ],
        ),
        (
            "--force through the branch URL",
            vec!["--force".into(), scoped.clone(), format!("HEAD:{bob}")],
        ),
        (
            "a rewind to an ancestor",
            vec![
                "-o".into(),
                "force".into(),
                app.url(),
                format!("+{a1}:{bob}"),
            ],
        ),
    ];
    for (what, args) in &attempts {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let (ok, out) = m.push(&args).await;
        assert!(!ok, "{what} accepted: {out}");
        assert!(
            out.contains(forward) && out.contains("push your history to a new branch"),
            "{what}: {out}"
        );
        assert_eq!(app.tip(bob).await.as_deref(), Some(&a2[..]), "{what}");
    }
    // Without `-o force`, Aspen's own ref-update rule (spike S2) refuses
    // a delete or a rewrite of a branch this client was not shown, before
    // the place hook is asked; the branch is untouched either way.
    let plain: [(&str, Vec<String>); 2] = [
        ("a plain delete", vec![app.url(), format!(":{bob}")]),
        (
            "a plain --force",
            vec!["--force".into(), app.url(), format!("HEAD:{bob}")],
        ),
    ];
    for (what, args) in &plain {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let (ok, out) = m.push(&args).await;
        assert!(!ok, "{what} accepted: {out}");
        assert_eq!(app.tip(bob).await.as_deref(), Some(&a2[..]), "{what}");
    }
    let deletes: [(&str, Vec<String>); 2] = [
        (
            "a delete with -o force",
            vec!["-o".into(), "force".into(), app.url(), format!(":{bob}")],
        ),
        (
            "--delete through the branch URL",
            vec!["--delete".into(), scoped.clone(), bob.into()],
        ),
    ];
    for (what, args) in &deletes {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let (ok, out) = m.push(&args).await;
        assert!(!ok, "{what} accepted: {out}");
        assert!(
            out.contains("paint/ branches cannot be deleted") && out.contains("new branch"),
            "{what}: {out}"
        );
        assert_eq!(app.tip(bob).await.as_deref(), Some(&a2[..]), "{what}");
    }

    // A correct lease (the stored tip) does not make a rewrite a
    // fast-forward. The branch URL advertises the branch, so git checks
    // the lease, sends the update, and the hook refuses it. (On the main
    // URL the branch is not advertised and git itself refuses the lease as
    // stale before sending anything.)
    let lease = format!("--force-with-lease={bob}:{a2}");
    let (ok, out) = m.push(&[&lease, &scoped, &format!("HEAD:{bob}")]).await;
    assert!(!ok, "a rewrite with a correct lease accepted: {out}");
    assert!(
        out.contains(forward) && out.contains("push your history to a new branch"),
        "a rewrite with a correct lease: {out}"
    );
    assert_eq!(app.tip(bob).await.as_deref(), Some(&a2[..]), "lease");
    // The rewrite goes to a new branch instead.
    let (ok, out) = m.push(&[&app.url(), "HEAD:refs/heads/paint/mallory"]).await;
    assert!(ok, "{out}");
    assert_eq!(
        app.tip("refs/heads/paint/mallory").await.as_deref(),
        Some(&b1[..])
    );
    // An atomic push that pairs a valid fast-forward of another branch
    // with a rewrite of paint/bob is refused whole: neither tip moves.
    let b2 = m.paint(&[(5, 6, BLUE)], "B2");
    let (ok, out) = m
        .push(&[
            "--atomic",
            "-o",
            "force",
            &app.url(),
            "HEAD:refs/heads/paint/mallory",
            &format!("+{b1}:{bob}"),
        ])
        .await;
    assert!(!ok, "atomic push accepted: {out}");
    assert!(out.contains(forward), "atomic push: {out}");
    assert_eq!(
        app.tip("refs/heads/paint/mallory").await.as_deref(),
        Some(&b1[..]),
        "the atomic push's fast-forward to {b2} landed: {out}"
    );
    assert_eq!(app.tip(bob).await.as_deref(), Some(&a2[..]), "atomic push");
    m.git(&["reset", "-q", "--hard", &b1]);
    // Outside `paint/`, a delete is still Aspen's ordinary policy.
    let (ok, out) = m.push(&[&app.url(), "HEAD:refs/heads/scratch"]).await;
    assert!(ok, "{out}");
    let (ok, out) = m
        .push(&["-o", "force", &app.url(), ":refs/heads/scratch"])
        .await;
    assert!(ok, "{out}");
    assert_eq!(app.tip("refs/heads/scratch").await, None);

    // The merger folds both paint/ branches into main, and expiry deletes
    // them through receive-pack as before.
    app.place.merge_once().await.unwrap().expect("a merge");
    let check = app.clone_work("check").await;
    for (path, color) in [
        ("tiles/0_0.txt", RED),
        ("tiles/1_0.txt", RED),
        ("tiles/5_5.txt", BLUE),
    ] {
        assert_eq!(
            std::fs::read(check.path.join(path)).unwrap(),
            format!("{color}\n").as_bytes(),
            "{path}"
        );
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    // Both paint/ branches, and `scratch`, which the delete above left
    // to expiry to forget.
    assert_eq!(app.place.expire_once().await.unwrap(), 3);
    assert_eq!(app.tip(bob).await, None);
    assert_eq!(app.tip("refs/heads/paint/mallory").await, None);
    assert_eq!(app.get("/api/v1/branches").await["branches"], json!([]));
    // A name expiry freed can be created again.
    let (ok, out) = w.push(&[&app.url(), &format!("HEAD:{bob}")]).await;
    assert!(ok, "{out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn git_pushes_are_limited_per_address_and_branch() {
    let app = App::start(|c| c.pushes_per_minute = 2).await;
    let w = app.clone_work("w").await;
    for i in 0..2 {
        w.paint(&[(i, 0, RED)], "ok");
        let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/q"]).await;
        assert!(ok, "{out}");
    }
    w.paint(&[(5, 0, RED)], "third");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/q"]).await;
    assert!(
        !ok && out.contains("rate limited: 2 pushes a minute per address and branch"),
        "{out}"
    );
    // Another branch has its own budget.
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/q2"]).await;
    assert!(ok, "{out}");
}

/// Opens a web session; returns its branch and its `Authorization` value.
async fn web_session(app: &App) -> (String, String) {
    let s = app.http("POST", "/api/v1/sessions", &[], None).await;
    assert_eq!(s.status, 201, "{}", s.text());
    let s = s.json();
    (
        s["branch"].as_str().unwrap().to_owned(),
        format!("Bearer {}", s["token"].as_str().unwrap()),
    )
}

/// One web stroke of one tile at `(x, 0)` on the session's branch.
async fn web_push(app: &App, (branch, auth): &(String, String), x: u32) -> Response {
    let stroke = json!({ "branch": branch, "tiles": [{ "x": x, "y": 0, "color": RED }] });
    app.http(
        "POST",
        "/api/v1/pushes",
        &[("Authorization", auth.as_str())],
        Some(&stroke),
    )
    .await
}

/// Invariant: one server-wide push ceiling counts web strokes and Git
/// updates together. Past it a web push is `429` `rate-limited` with
/// `Retry-After` and a message that the demo is at its limit, a Git push
/// is `remote rejected` with the same message, and nothing of a refused
/// push lands. The rate is far below one push per test, so no token
/// refills while it runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn web_and_git_pushes_share_one_server_wide_ceiling() {
    let app = App::start(|c| {
        c.global_pushes_per_second = 0.001;
        c.global_push_burst = 3;
    })
    .await;
    let w = app.clone_work("w").await;
    let session = web_session(&app).await;

    // Three pushes, from both sides, spend the burst.
    let r = web_push(&app, &session, 0).await;
    assert_eq!(r.status, 201, "{}", r.text());
    w.paint(&[(1, 0, RED)], "one");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/a"]).await;
    assert!(ok, "{out}");
    let r = web_push(&app, &session, 2).await;
    assert_eq!(r.status, 201, "{}", r.text());

    // Git is refused next, though its own branch has never pushed.
    let refused = w.paint(&[(3, 0, RED)], "two");
    let stored_before = stored_files(&app.dir.join("blobs"));
    // The pushes above stored packs, so an unchanged store is not vacuous.
    assert!(
        !stored_before.is_empty(),
        "the blob store has the earlier packs"
    );
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/b"]).await;
    assert!(
        !ok && out.contains("remote rejected")
            && out.contains(
                "place is at its limit: about 0.001 pushes a second from everyone together"
            )
            && out.contains("retry in "),
        "{out}"
    );
    assert_eq!(
        app.tip("refs/heads/paint/b").await,
        None,
        "nothing of it landed"
    );
    // Nor were its objects stored: no pack was written, and its commit is not
    // indexed. The `NotIndexed` lookup is the decisive check; the stock fetch
    // by id below is only supplementary, since a fetch can fail for other
    // reasons.
    assert_eq!(
        stored_files(&app.dir.join("blobs")),
        stored_before,
        "the blob store is unchanged"
    );
    use aspen_core::meta::MetaStore;
    let id: aspen_core::id::ObjectId = refused.parse().unwrap();
    let lookup = app
        .meta
        .lookup_commits(
            app.place.repo().id,
            &aspen_core::ingest::SortedIds::from_unsorted(vec![id]),
        )
        .await
        .unwrap();
    assert!(
        matches!(lookup[..], [aspen_core::meta::CommitLookup::NotIndexed]),
        "{lookup:?}"
    );
    let (fetched, out) = app
        .clone_work("probe")
        .await
        .try_git(&["fetch", &app.url(), &refused])
        .await;
    assert!(!fetched && out.contains(&refused), "{out}");

    // And so is the web, from a fresh session with its whole own budget.
    let fresh = web_session(&app).await;
    let before = app.tip(&format!("refs/heads/{}", fresh.0)).await;
    let r = web_push(&app, &fresh, 4).await;
    assert_eq!(r.status, 429, "{}", r.text());
    let retry: u64 = r
        .header("retry-after")
        .expect("Retry-After on a refusal")
        .parse()
        .unwrap();
    assert!(retry >= 1, "{retry}");
    let body = r.json();
    assert_eq!(body["error"], "rate-limited");
    assert_eq!(body["retryAfterSeconds"], retry);
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .starts_with("place is at its limit"),
        "{body}"
    );
    assert_eq!(app.tip(&format!("refs/heads/{}", fresh.0)).await, before);

    // The session's own quota was not charged for the refused push.
    let q = app
        .http(
            "GET",
            "/api/v1/quota",
            &[("Authorization", fresh.1.as_str())],
            None,
        )
        .await;
    assert_eq!(q.status, 200, "{}", q.text());
    assert_eq!(q.json()["remaining"], 30);
}

/// Invariant: a push its caller's own quota refuses (a web session's, or a
/// Git caller's per branch) gives its token back, so it does not count
/// toward the server-wide ceiling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_its_own_quota_refuses_does_not_spend_the_ceiling() {
    let app = App::start(|c| {
        c.pushes_per_minute = 1;
        c.global_pushes_per_second = 0.001;
        c.global_push_burst = 3;
    })
    .await;
    let w = app.clone_work("w").await;
    let session = web_session(&app).await;

    w.paint(&[(1, 0, RED)], "one");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/q"]).await;
    assert!(ok, "{out}");
    w.paint(&[(2, 0, RED)], "two");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/q"]).await;
    assert!(
        !ok && out.contains("rate limited: 1 pushes a minute"),
        "{out}"
    );

    let r = web_push(&app, &session, 3).await;
    assert_eq!(r.status, 201, "{}", r.text());
    let r = web_push(&app, &session, 4).await;
    assert_eq!(r.status, 429, "{}", r.text());
    assert!(
        r.text().contains("You can push 1 times a minute"),
        "{}",
        r.text()
    );

    // Two of the three tokens are spent; the two refusals took none.
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/q2"]).await;
    assert!(ok, "{out}");
    w.paint(&[(5, 0, RED)], "three");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/q3"]).await;
    assert!(!ok && out.contains("place is at its limit"), "{out}");
}

/// Invariant: only a push that passes every rule of its own takes a token
/// from the server-wide ceiling. A Git push that breaks a tile rule and a
/// delete of a paint/ branch are refused for their own reasons and leave
/// the last token for the next valid push.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_refused_for_a_rule_takes_no_token_from_the_ceiling() {
    let app = App::start(|c| {
        c.global_pushes_per_second = 0.001;
        c.global_push_burst = 2;
    })
    .await;
    let w = app.clone_work("w").await;
    w.paint(&[(1, 0, RED)], "one");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/a"]).await;
    assert!(ok, "{out}");
    // One token left.

    let bad = app.clone_work("bad").await;
    bad.write("README.md", b"changed\n");
    bad.git(&["add", "-A"]);
    bad.git(&["commit", "-q", "-m", "bad"]);
    let (ok, out) = bad.push(&[&app.url(), "HEAD:refs/heads/paint/bad"]).await;
    assert!(
        !ok && out.contains("README.md: only files under tiles/ may change"),
        "{out}"
    );
    let (ok, out) = w
        .push(&["-o", "force", &app.url(), ":refs/heads/paint/a"])
        .await;
    assert!(
        !ok && out.contains("paint/ branches cannot be deleted"),
        "{out}"
    );
    assert!(app.tip("refs/heads/paint/a").await.is_some());

    // The last token is still there, and then it is gone.
    w.paint(&[(2, 0, RED)], "two");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/c"]).await;
    assert!(ok, "{out}");
    w.paint(&[(3, 0, RED)], "three");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/d"]).await;
    assert!(!ok && out.contains("place is at its limit"), "{out}");
}

/// Every file under `dir`, recursively.
fn stored_files(dir: &std::path::Path) -> std::collections::BTreeSet<std::path::PathBuf> {
    let mut out = std::collections::BTreeSet::new();
    let mut stack = vec![dir.to_owned()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.insert(p);
            }
        }
    }
    out
}

/// Invariant (issue #121): the API's per-address limits read
/// `X-Forwarded-For` only from a trusted proxy and parse its entries
/// strictly. A client whose connection does not come from a trusted proxy
/// is its TCP address whatever it claims; behind a trusted proxy, an entry
/// with a port is the same client as one without.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_limits_key_on_the_address_a_trusted_proxy_recorded() {
    use aspen_server::access::ClientAddr;
    async fn session(app: &App, xff: &str) -> u16 {
        app.http(
            "POST",
            "/api/v1/sessions",
            &[("x-forwarded-for", xff)],
            None,
        )
        .await
        .status
    }
    // Loopback (the test client) is not in this list, so the header is
    // ignored and every session counts against 127.0.0.1.
    let app = App::start(|c| {
        c.sessions_per_minute = 2;
        c.client_addr = ClientAddr::from_settings(1, Some("10.0.0.0/8")).unwrap();
    })
    .await;
    assert_eq!(session(&app, "203.0.113.1").await, 201);
    assert_eq!(session(&app, "203.0.113.2").await, 201);
    assert_eq!(session(&app, "203.0.113.3").await, 429);
    // Loopback is a trusted proxy by default: the recorded client counts,
    // with or without a port.
    let app = App::start(|c| {
        c.sessions_per_minute = 2;
        c.client_addr = ClientAddr::from_settings(1, None).unwrap();
    })
    .await;
    assert_eq!(session(&app, "203.0.113.1").await, 201);
    assert_eq!(session(&app, "6.6.6.6, 203.0.113.1:4711").await, 201);
    assert_eq!(session(&app, "203.0.113.1:4712").await, 429);
    assert_eq!(session(&app, "203.0.113.2").await, 201);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn web_strokes_are_real_commits_on_the_visitors_branch() {
    let app = App::start(|c| c.pushes_per_minute = 3).await;
    let s = app.http("POST", "/api/v1/sessions", &[], None).await;
    assert_eq!(s.status, 201, "{}", s.text());
    let s = s.json();
    let (branch, token) = (
        s["branch"].as_str().unwrap().to_owned(),
        s["token"].as_str().unwrap().to_owned(),
    );
    assert!(branch.starts_with("visitors/"));
    assert!(token.len() >= 16);
    let auth = format!("Bearer {token}");
    let h = [("Authorization", auth.as_str())];
    let main = app.tip("refs/heads/main").await.unwrap();

    let stroke = json!({ "branch": branch, "tiles": [{ "x": 2, "y": 3, "color": RED }], "message": "hello" });
    let r = app.http("POST", "/api/v1/pushes", &h, Some(&stroke)).await;
    assert_eq!(r.status, 201, "{}", r.text());
    let first = r.json();
    assert_eq!(first["parent"], main, "a branch's first push sits on main");
    assert_eq!(first["quota"]["remaining"], 2);
    let commit = first["commit"].as_str().unwrap().to_owned();
    assert_eq!(
        app.tip(&format!("refs/heads/{branch}")).await.as_deref(),
        Some(&commit[..])
    );

    // The commit is a real one: fetch it with stock git and read the tile.
    let w = app.clone_work("look").await;
    w.run(&["fetch", "-q", &app.url(), &format!("refs/heads/{branch}")])
        .await;
    assert_eq!(w.git(&["show", "FETCH_HEAD:tiles/2_3.txt"]), "#d6262b\n");
    assert_eq!(
        w.git(&["log", "-1", "--format=%s", "FETCH_HEAD"]).trim(),
        "hello"
    );
    let r = app
        .http(
            "POST",
            "/api/v1/pushes",
            &h,
            Some(&json!({ "branch": branch, "tiles": [{ "x": 4, "y": 4, "color": BLUE }] })),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    assert_eq!(
        r.json()["parent"].as_str(),
        Some(&commit[..]),
        "the next one sits on the branch"
    );

    // The contract's errors.
    let r = app.http("POST", "/api/v1/pushes", &[], Some(&stroke)).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (401, json!("unauthorized"))
    );
    let other = json!({ "branch": "visitors/zzz", "tiles": [{ "x": 1, "y": 1, "color": RED }] });
    let r = app.http("POST", "/api/v1/pushes", &h, Some(&other)).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (403, json!("forbidden"))
    );
    let dup = json!({ "branch": branch, "tiles": [{ "x": 1, "y": 1, "color": RED }, { "x": 1, "y": 1, "color": RED }] });
    let r = app.http("POST", "/api/v1/pushes", &h, Some(&dup)).await;
    assert_eq!(r.status, 422);
    assert_eq!(r.json()["rejections"][0]["code"], "duplicate-tile");
    let r = app
        .http(
            "POST",
            "/api/v1/pushes",
            &h,
            Some(&json!({ "branch": branch, "tiles": "no" })),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (400, json!("bad-request"))
    );
    // The third accepted push uses the last of 3 a minute; the fourth is 429.
    let ok = json!({ "branch": branch, "tiles": [{ "x": 5, "y": 5, "color": RED }] });
    assert_eq!(
        app.http("POST", "/api/v1/pushes", &h, Some(&ok))
            .await
            .status,
        201
    );
    let r = app.http("POST", "/api/v1/pushes", &h, Some(&ok)).await;
    assert_eq!(r.status, 429);
    assert!(r.header("retry-after").is_some());
    assert_eq!(r.header("access-control-allow-origin"), Some("*"));
    assert_eq!(r.header("cache-control"), Some("no-store"));
    let q = app.http("GET", "/api/v1/quota", &h, None).await.json();
    assert_eq!(q["remaining"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_event_stream_lines_up_with_snapshots_resumes_and_resyncs() {
    let app = App::start(|c| c.event_backlog = 3).await;
    let w = app.clone_work("w").await;
    for i in 0..5 {
        w.paint(&[(i, 0, RED)], "p");
        assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/e"]).await.0);
    }
    let snap = app.get("/api/v1/canvas").await;
    assert_eq!(snap["seq"], 5);
    // Live from the snapshot: nothing old, then the next event.
    let mut live = app.events("?after=5", &[]).await;
    assert_eq!(live.status, 200);
    assert!(
        live.headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("content-type")
                && v.starts_with("text/event-stream"))
    );
    w.paint(&[(9, 9, RED)], "p");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/e"]).await.0);
    let e = live.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        (e.id, e.kind.as_str(), e.data["seq"].as_u64()),
        (Some(6), "push", Some(6))
    );
    assert!(live.text().is_empty() || !live.text().contains("id: 5"));
    // Resume with Last-Event-ID inside the backlog (4, 5, 6 kept).
    let mut again = app.events("", &[("Last-Event-ID", "4")]).await;
    let got: Vec<u64> = vec![
        again
            .next(Duration::from_secs(5))
            .await
            .unwrap()
            .id
            .unwrap(),
        again
            .next(Duration::from_secs(5))
            .await
            .unwrap()
            .id
            .unwrap(),
    ];
    assert_eq!(got, [5, 6]);
    // Too old: one resync event naming the current seq.
    let mut old = app.events("?after=1", &[]).await;
    let e = old.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        (e.kind.as_str(), e.data["seq"].as_u64()),
        ("resync", Some(6))
    );
    // From the future (a client of an earlier server run): resync too.
    let mut future = app.events("?after=99", &[]).await;
    assert_eq!(
        future.next(Duration::from_secs(5)).await.unwrap().kind,
        "resync"
    );
    let bad = app.http("GET", "/api/v1/events?after=-1", &[], None).await;
    assert_eq!(bad.status, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_kill_switch_takes_an_admin_grant_and_stops_the_swarm_and_ambient_agents() {
    let app = App::start(|c| {
        c.ambient_agents = 3;
        c.swarm.pushes_per_second_cap = 4;
        c.swarm.max_agents = 5;
        c.swarm.ramp_ms = 1;
        c.swarm.run_seconds = 60;
    })
    .await;
    let operator = format!("Bearer {}", app.token("ops", &["admin:canvas"]));
    let writer = format!("Bearer {}", app.token("dev", &["write:canvas"]));
    let engage = json!({ "engaged": true, "reason": "Maintenance" });
    for headers in [
        vec![],
        vec![("Authorization", writer.as_str())],
        vec![("Authorization", "Bearer mock-operator-token")],
    ] {
        let r = app
            .http("POST", "/api/v1/admin/kill-switch", &headers, Some(&engage))
            .await;
        assert_eq!(
            (r.status, r.json()["error"].clone()),
            (403, json!("forbidden")),
            "{headers:?}"
        );
    }

    let started = app.http("POST", "/api/v1/swarm", &[], None).await;
    assert_eq!(started.status, 202, "{}", started.text());
    assert_eq!(started.json()["state"], "running");
    let mut feed = app.events("?after=0", &[]).await;
    // Agents paint for real until the switch is engaged.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut seen_swarm = false;
    let mut seen_ambient = false;
    while !(seen_swarm && seen_ambient) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "agents did not push"
        );
        app.place.agents_tick();
        while let Some(e) = feed.next(Duration::from_millis(100)).await {
            if e.kind == "push" {
                let b = e.data["branch"].as_str().unwrap();
                seen_swarm |= b.starts_with("swarm/");
                seen_ambient |= b.starts_with("agent/");
            }
        }
    }
    let r = app
        .http(
            "POST",
            "/api/v1/admin/kill-switch",
            &[("Authorization", operator.as_str())],
            Some(&engage),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let status = r.json();
    assert_eq!(status["state"], "disabled");
    assert_eq!(
        status["killSwitch"],
        json!({ "engaged": true, "reason": "Maintenance" })
    );
    // Let pushes already in flight land, then nothing new may start.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while app.place.agent_pushes_in_flight() > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent pushes never finished"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    while feed.next(Duration::from_millis(200)).await.is_some() {}
    let quiet_from = app.get("/api/v1/canvas").await["seq"].as_u64().unwrap();
    for _ in 0..30 {
        app.place.agents_tick();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    while let Some(e) = feed.next(Duration::from_millis(300)).await {
        assert!(
            e.kind != "push" || e.data["seq"].as_u64().unwrap() <= quiet_from,
            "an agent pushed after the kill switch: {e:?}"
        );
    }
    let r = app.http("POST", "/api/v1/swarm", &[], None).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (503, json!("kill-switch"))
    );
    assert!(r.header("retry-after").is_some());
    // Releasing it lets a later run start (after the cooldown).
    let r = app
        .http(
            "POST",
            "/api/v1/admin/kill-switch",
            &[("Authorization", operator.as_str())],
            Some(&json!({ "engaged": false })),
        )
        .await;
    assert_eq!(r.json()["killSwitch"]["engaged"], false);
    assert_eq!(
        app.http("POST", "/api/v1/swarm", &[], None).await.json()["error"],
        "cooldown"
    );
}

/// Invariant (C7 review obligation 1, D4 review): bad operator
/// credentials drain the same failed-authentication bucket Git requests
/// from the address draw on. Once it is empty, the kill switch answers 429
/// before it checks a credential (no token lookup, and a valid operator
/// token is refused too), and so does Git.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_operator_credentials_drain_the_git_failure_bucket() {
    let limits = aspen_auth::LimitsConfig {
        auth_failures: Some(aspen_auth::RateLimit {
            burst: 3,
            per_second: 0.001,
        }),
        ..aspen_auth::LimitsConfig::default()
    };
    let app = App::start_with_limits(limits, |_| {}).await;
    let good = app.token("ops", &["admin:canvas"]);
    // The right token id with a wrong secret: a store lookup, then a
    // refusal.
    let mut wrong = good.clone();
    let last = if wrong.ends_with('0') { "1" } else { "0" };
    wrong.replace_range(wrong.len() - 1.., last);
    let operator = format!("Bearer {good}");
    let bad = format!("Bearer {wrong}");
    let engage = json!({ "engaged": true });
    async fn kill(app: &App, auth: &str, engage: &serde_json::Value) -> Response {
        app.http(
            "POST",
            "/api/v1/admin/kill-switch",
            &[("Authorization", auth)],
            Some(engage),
        )
        .await
    }
    for _ in 0..3 {
        assert_eq!(kill(&app, &bad, &engage).await.status, 403);
    }
    assert_eq!(app.tokens.lookups(), 3);
    for auth in [&bad, &operator] {
        let r = kill(&app, auth, &engage).await;
        assert_eq!(
            (r.status, r.json()["error"].clone()),
            (429, json!("rate-limited")),
            "{}",
            r.text()
        );
        assert!(r.header("retry-after").is_some());
    }
    assert_eq!(app.tokens.lookups(), 3, "a limited credential was checked");
    assert!(!app.place.with_state(|s| s.kill.engaged));
    // The Git guard sees the same bucket.
    use base64::Engine as _;
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("x:{good}"));
    let auth = format!("Basic {basic}");
    let git = app
        .http(
            "GET",
            "/canvas.git/info/refs?service=git-upload-pack",
            &[("Authorization", auth.as_str())],
            None,
        )
        .await;
    assert_eq!(git.status, 429, "{}", git.text());
    assert_eq!(app.tokens.lookups(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_swarm_is_one_global_run_capped_by_the_server() {
    let app = App::start(|c| {
        c.swarm.pushes_per_second_cap = 5;
        c.swarm.max_agents = 50;
        c.swarm.ramp_ms = 1;
        c.swarm.run_seconds = 2;
        c.swarm.cooldown_seconds = 1;
        c.swarm_starts_per_hour = 2;
    })
    .await;
    assert_eq!(
        app.http("POST", "/api/v1/swarm", &[], None).await.status,
        202
    );
    // A second start while it runs is refused, whatever the client sends.
    let r = app
        .http(
            "POST",
            "/api/v1/swarm",
            &[],
            Some(&json!({ "maxAgents": 100000 })),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (429, json!("cooldown"))
    );
    let deadline = tokio::time::Instant::now() + Duration::from_millis(3500);
    while tokio::time::Instant::now() < deadline {
        app.place.agents_tick();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // Let in-flight pushes land.
    while app.place.agent_pushes_in_flight() > 0 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let status = app.get("/api/v1/swarm").await;
    let pushes = status["pushes"].as_u64().unwrap();
    assert!(pushes >= 1, "the swarm pushed nothing");
    assert!(
        pushes <= 5 * 2 + 1,
        "{pushes} pushes in a 2 s run capped at 5/s"
    );
    let branches = app.get("/api/v1/branches").await;
    let swarm_pushes: usize = branches["branches"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|b| b["name"].as_str().unwrap().starts_with("swarm/"))
        .count();
    assert!(swarm_pushes >= 1);
    // Cooldown over: one more start for this address, then the hourly cap.
    assert_eq!(
        app.http("POST", "/api/v1/swarm", &[], None).await.status,
        202
    );
    for _ in 0..30 {
        app.place.agents_tick();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    app.place.agents_tick();
    let r = app.http("POST", "/api/v1/swarm", &[], None).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (429, json!("rate-limited")),
        "{}",
        r.text()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn git_pushes_and_strokes_leave_nothing_waiting_in_the_hook() {
    let app = App::start(|_| {}).await;
    let w = app.clone_work("w").await;
    let empty = aspen_place::place::HookBacklog {
        accepted: 0,
        committed: 0,
    };
    for n in 0..5 {
        w.paint(&[(n, 3, RED)], "p");
        let (ok, out) = w
            .push(&[&app.url(), &format!("HEAD:refs/heads/paint/n{n}")])
            .await;
        assert!(ok, "{out}");
        assert_eq!(app.place.hook_backlog(), empty, "after git push {n}");
    }
    // The server's own writes take their entries back too.
    let s = app.http("POST", "/api/v1/sessions", &[], None).await.json();
    let auth = format!("Bearer {}", s["token"].as_str().unwrap());
    let stroke = json!({ "branch": s["branch"], "tiles": [{ "x": 9, "y": 9, "color": RED }] });
    let r = app
        .http(
            "POST",
            "/api/v1/pushes",
            &[("Authorization", &auth)],
            Some(&stroke),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    assert_eq!(app.place.hook_backlog(), empty, "after a web stroke");
    assert_eq!(
        app.get("/api/v1/branches").await["branches"]
            .as_array()
            .unwrap()
            .len(),
        6
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_branch_deleted_behind_the_services_back_still_expires() {
    use aspen_core::meta::{CommitOutcome, MetaStore, PushCommit, RefWrite};
    use aspen_core::refs::{Deps, RefValue};

    let app = App::start(|c| c.branch_ttl = Duration::from_millis(300)).await;
    let w = app.clone_work("w").await;
    w.paint(&[(2, 2, RED)], "p");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/gone"]).await.0);
    app.place.merge_once().await.unwrap().unwrap();
    // Deleted straight in the metadata store (another instance, an
    // operator): the hook never hears of it, so expiry's delete is
    // refused with `no such ref`.
    let current = app.current("refs/heads/paint/gone").await.unwrap();
    let deleted = app
        .meta
        .commit_push(&PushCommit {
            repo: app.place.repo().id,
            writes: vec![RefWrite {
                name: current.name.clone(),
                expected: Some(current.id),
                value: RefValue::Deleted,
                deps: Deps::default(),
                commits: Vec::new(),
                jobs: Vec::new(),
            }],
        })
        .await
        .unwrap();
    assert!(matches!(deleted, CommitOutcome::Committed(_)));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(app.place.expire_once().await.unwrap(), 1);
    assert_eq!(app.get("/api/v1/branches").await["branches"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branches_expire_only_after_their_tiles_merge_and_their_ttl() {
    let app = App::start(|c| c.branch_ttl = Duration::from_millis(800)).await;
    let w = app.clone_work("w").await;
    w.paint(&[(1, 1, RED)], "p");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/x"]).await.0);
    tokio::time::sleep(Duration::from_millis(900)).await;
    // Past its TTL but unmerged: kept.
    assert_eq!(app.place.expire_once().await.unwrap(), 0);
    assert!(app.tip("refs/heads/paint/x").await.is_some());
    app.place.merge_once().await.unwrap().unwrap();
    let mut feed = app
        .events(
            &format!("?after={}", app.get("/api/v1/canvas").await["seq"]),
            &[],
        )
        .await;
    assert_eq!(app.place.expire_once().await.unwrap(), 1);
    assert_eq!(app.tip("refs/heads/paint/x").await, None, "deleted in Git");
    let e = feed.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        (e.kind.as_str(), e.data["branch"].clone()),
        ("expire", json!("paint/x"))
    );
    // Exactly one expire event, from the delete's own commit.
    assert!(feed.next(Duration::from_millis(300)).await.is_none());
    assert_eq!(app.get("/api/v1/branches").await["branches"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_reloads_main_and_branches_from_git_and_resumes_painting() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let (pixels, token_branch) = {
        let app = App::start_in(path.clone(), None, |_| {}).await;
        let w = app.clone_work("w").await;
        w.paint(&[(1, 2, RED), (3, 4, BLUE)], "p");
        assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/r"]).await.0);
        app.place.merge_once().await.unwrap().unwrap();
        // A web stroke left unmerged.
        let s = app.http("POST", "/api/v1/sessions", &[], None).await.json();
        let auth = format!("Bearer {}", s["token"].as_str().unwrap());
        let stroke =
            json!({ "branch": s["branch"], "tiles": [{ "x": 7, "y": 7, "color": YELLOW }] });
        assert_eq!(
            app.http(
                "POST",
                "/api/v1/pushes",
                &[("Authorization", &auth)],
                Some(&stroke)
            )
            .await
            .status,
            201
        );
        (
            app.get("/api/v1/canvas").await["pixels"].clone(),
            s["branch"].as_str().unwrap().to_owned(),
        )
    };
    let app = App::start_in(path, Some(dir), |_| {}).await;
    let snap = app.get("/api/v1/canvas").await;
    assert_eq!(snap["pixels"], pixels, "main's tiles come back from Git");
    let names: Vec<String> = app.get("/api/v1/branches").await["branches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["name"].as_str().unwrap().to_owned())
        .collect();
    assert!(
        names.contains(&"paint/r".to_owned()) && names.contains(&token_branch),
        "{names:?}"
    );
    // The server can still write on a branch it did not write this run:
    // it reads the branch's tree from Git.
    let painted = app
        .place
        .paint(
            &token_branch,
            aspen_place::state::Actor::agent("t"),
            &[aspen_place::rules::Tile {
                x: 8,
                y: 8,
                color: 0,
            }],
            "",
        )
        .await
        .unwrap();
    let w = app.clone_work("after").await;
    w.run(&[
        "fetch",
        "-q",
        &app.url(),
        &format!("refs/heads/{token_branch}"),
    ])
    .await;
    assert_eq!(
        w.git(&["show", "FETCH_HEAD:tiles/7_7.txt"]),
        "#ffd635\n",
        "the earlier stroke is kept"
    );
    assert_eq!(w.git(&["show", "FETCH_HEAD:tiles/8_8.txt"]), "#ffffff\n");
    assert_eq!(
        w.git(&["rev-parse", "FETCH_HEAD"]).trim(),
        painted.commit.to_string()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_background_loop_merges_on_its_own_and_cors_preflight_answers() {
    let app = App::start(|c| c.merge_interval = Duration::from_millis(200)).await;
    let (stop, rx) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(app.place.clone().run(async move {
        let _ = rx.await;
    }));
    let w = app.clone_work("w").await;
    w.paint(&[(2, 2, RED)], "p");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/bg"]).await.0);
    let mut feed = app.events("?after=0", &[]).await;
    feed.until(Duration::from_secs(5), |e| e.kind == "merge")
        .await;
    let r = app
        .http(
            "OPTIONS",
            "/api/v1/pushes",
            &[("Origin", "https://example.org")],
            None,
        )
        .await;
    assert_eq!(r.status, 204);
    assert_eq!(r.header("access-control-allow-origin"), Some("*"));
    assert!(
        r.header("access-control-allow-headers")
            .unwrap()
            .contains("Authorization")
    );
    let r = app.http("GET", "/api/v1/nope", &[], None).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (404, json!("not-found"))
    );
    let _ = stop.send(());
    run.await.unwrap();
}

/// Wraps the real place hook. Asked about the stroke on `paint/stroke`
/// (caller key `place:internal`), it first runs more than `MAX_ACCEPTED` Git checks
/// through the place hook that pass and never commit (each on its own new
/// branch, as an `--atomic` push of many branches with a refused peer
/// leaves them), then answers for the stroke itself.
#[derive(Debug)]
struct FloodingHook {
    inner: aspen_place::hook::PlaceHook,
    /// `main`'s tip, which each flood update is a branch on top of.
    main: std::sync::OnceLock<aspen_core::id::ObjectId>,
    flooded: std::sync::atomic::AtomicBool,
}

impl aspen_server::receive::hook::PushHook for FloodingHook {
    fn check<'a>(
        &'a self,
        update: aspen_server::receive::hook::HookUpdate<'a>,
        objects: &'a mut dyn aspen_server::receive::hook::HookObjects,
    ) -> aspen_server::receive::hook::HookFuture<
        'a,
        Result<aspen_server::receive::hook::HookVerdict, aspen_core::error::StoreError>,
    > {
        use aspen_server::receive::hook::{Caller, HookUpdate, HookVerdict};
        Box::pin(async move {
            if update.caller.key == aspen_place::hook::INTERNAL_KEY
                && update.name.as_str() == "refs/heads/paint/stroke"
                && !self.flooded.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                // Every flood entry is strictly newer than the stroke's,
                // so evicting the oldest would take the stroke's first.
                tokio::time::sleep(Duration::from_millis(5)).await;
                let caller = Caller {
                    principal: "anonymous".into(),
                    key: "ip:203.0.113.9".into(),
                };
                for n in 0..aspen_place::hook::MAX_ACCEPTED + 10 {
                    let name =
                        aspen_core::name::RefName::new(format!("refs/heads/flood/{n}")).unwrap();
                    let flood = HookUpdate {
                        caller: &caller,
                        name: &name,
                        stored: None,
                        trunk: self.main.get().copied(),
                        ..update
                    };
                    let verdict = self.inner.check(flood, &mut *objects).await.unwrap();
                    assert_eq!(verdict, HookVerdict::Accept, "flood {n}");
                }
            }
            self.inner.check(update, objects).await
        })
    }

    fn committed<'a>(
        &'a self,
        repo: &'a aspen_core::repo::Repo,
        caller: &'a aspen_server::receive::hook::Caller,
        updates: &'a [aspen_server::receive::hook::CommittedUpdate],
    ) -> aspen_server::receive::hook::HookFuture<'a, ()> {
        self.inner.committed(repo, caller, updates)
    }
}

#[derive(Debug)]
struct NoOperators;

impl aspen_place::place::Operators for NoOperators {
    fn is_operator<'a>(
        &'a self,
        _authorization: Option<&'a [u8]>,
        _address: Option<std::net::IpAddr>,
    ) -> aspen_server::receive::hook::HookFuture<'a, Result<bool, aspen_auth::LimitError>> {
        Box::pin(async { Ok(false) })
    }
}

/// Invariant: Git checks that never commit, however many, cannot evict an
/// in-flight stroke's entry. The stroke still gets its sequence number
/// and its tiles, and the Git entries stay capped at `MAX_ACCEPTED`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn git_checks_that_never_commit_cannot_evict_a_pending_stroke() {
    use aspen_place::hook::{MAX_ACCEPTED, PlaceHook};
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;

    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let meta = aspen_meta::SqlStore::connect(&url).await.unwrap();
    meta.migrate().await.unwrap();
    let blobs = aspen_blob::LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let config = support::config();
    let repo = aspen_place::place::ensure_repo(&meta, &config.repo)
        .await
        .unwrap();
    let shared = aspen_place::place::shared(
        &repo,
        &config,
        Arc::new(aspen_place::hook::IndexedMain {
            meta: meta.clone(),
            repo: repo.id,
        }),
    );
    let hook = Arc::new(FloodingHook {
        inner: PlaceHook(shared.clone()),
        main: Default::default(),
        flooded: Default::default(),
    });
    let mut receive = aspen_server::receive::ReceiveConfig::new(
        dir.path().join("spool"),
        aspen_pack::MaxObjectBytes::new(16 << 20).unwrap(),
        aspen_pack::MaxInflatedBytes::new(256 << 20).unwrap(),
    );
    receive.hook = Some(hook.clone());
    let server = aspen_server::Server::new(
        meta.clone(),
        blobs,
        aspen_server::ServerConfig::new(aspen_pack::MaxObjectBytes::new(16 << 20).unwrap()),
    )
    .with_receive_pack(receive)
    .unwrap();
    let svc = server.receive_pack().unwrap().clone();
    let place = aspen_place::Place::open(
        config,
        shared.clone(),
        meta,
        svc,
        repo,
        Arc::new(NoOperators),
    )
    .await
    .unwrap();
    hook.main.set(place.with_state(|s| s.main.commit)).unwrap();

    let painted = place
        .paint(
            "paint/stroke",
            Actor::agent("stroke"),
            &[Tile {
                x: 4,
                y: 5,
                color: 1,
            }],
            "",
        )
        .await
        .expect("the stroke commits and gets its sequence number");
    assert!(
        hook.flooded.load(std::sync::atomic::Ordering::SeqCst),
        "the flood ran"
    );
    let branch = place.with_state(|s| s.branches.get("paint/stroke").cloned());
    let branch = branch.expect("the stroke's branch is on the canvas");
    assert_eq!(branch.head, painted.commit);
    // The flood's own entries never committed and stay capped.
    assert_eq!(
        place.hook_backlog(),
        aspen_place::place::HookBacklog {
            accepted: MAX_ACCEPTED,
            committed: 0,
        }
    );
}

/// Paints one tile through the web API, as a fresh session; returns the
/// push's `seq`.
async fn web_stroke(app: &App, x: u32) -> u64 {
    let s = app.http("POST", "/api/v1/sessions", &[], None).await;
    assert_eq!(s.status, 201, "{}", s.text());
    let s = s.json();
    let auth = format!("Bearer {}", s["token"].as_str().unwrap());
    let stroke = json!({ "branch": s["branch"], "tiles": [{ "x": x, "y": 0, "color": RED }] });
    let r = app
        .http(
            "POST",
            "/api/v1/pushes",
            &[("Authorization", auth.as_str())],
            Some(&stroke),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    r.json()["seq"].as_u64().unwrap()
}

/// Invariant (#124, C7 review obligation 2): open event streams are capped
/// per client address and in all, both checked when a stream opens; a
/// refused stream is 429 with `Retry-After`, and a stream whose client went
/// away gives its slot back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_streams_are_capped_per_address_and_in_total() {
    let app = App::start(|c| {
        c.max_event_streams_per_address = 2;
        c.max_event_streams = 3;
        // Tests speak for several addresses through X-Forwarded-For, sent
        // from loopback, which the default trusted proxies include.
        c.client_addr = aspen_server::access::ClientAddr::from_settings(1, None).unwrap();
    })
    .await;
    let from = |ip: &'static str| [("X-Forwarded-For", ip)];
    let a1 = app.events("", &from("192.0.2.1")).await;
    let a2 = app.events("", &from("192.0.2.1")).await;
    assert_eq!((a1.status, a2.status), (200, 200));
    let a3 = app.events("", &from("192.0.2.1")).await;
    assert_eq!(
        a3.status,
        429,
        "a third stream from one address: {}",
        a3.text()
    );
    // The refusal is the contract's Error body (read in full; the stream
    // reader only decodes chunked bodies).
    let r = app
        .http("GET", "/api/v1/events", &from("192.0.2.1"), None)
        .await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (429, json!("rate-limited"))
    );
    assert!(
        a3.headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("retry-after") && v.parse::<u64>().is_ok())
    );
    // An IPv6 caller counts by its /64: two addresses in one /64 share.
    let v1 = app.events("", &from("2001:db8:1:2::1")).await;
    assert_eq!(v1.status, 200);
    // Three open in all: a new address is refused too.
    let c = app.events("", &from("192.0.2.3")).await;
    assert_eq!(c.status, 429, "a fourth stream in all: {}", c.text());
    // A client that hangs up frees its slot once a write to it fails, here
    // on the next events (in production at the latest on a heartbeat).
    drop((a1, a2));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut x = 0;
    loop {
        web_stroke(&app, x % 16).await;
        x += 1;
        let c = app.events("", &from("192.0.2.3")).await;
        if c.status == 200 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "closed streams never gave their slots back"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let v2 = app.events("", &from("2001:db8:1:2::2")).await;
    let v3 = app.events("", &from("2001:db8:1:2::3")).await;
    assert_eq!(
        (v2.status, v3.status),
        (200, 429),
        "one /64 holds at most two"
    );
    // v1, v2 and the last loop stream (dropped, but holding its slot until
    // a write to it fails) fill the total too, so the refusal must name the
    // per-address cap, or a broken /64 key would pass unnoticed.
    let refused = app
        .http("GET", "/api/v1/events", &from("2001:db8:1:2::4"), None)
        .await;
    assert_eq!(refused.status, 429);
    let message = refused.json()["message"].as_str().unwrap().to_owned();
    assert!(
        message.contains("from this address"),
        "refused by the per-address cap, not the total: {message}"
    );
}

/// Invariant (#124): a stream ends at its age limit, which frees its slot,
/// and a client that reconnects with the `seq` of the last event it saw
/// misses nothing that happened while it was away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_streams_end_at_their_age_limit_and_resume_without_loss() {
    let app = App::start(|c| {
        c.event_stream_max_age = Duration::from_secs(1);
        c.max_event_streams_per_address = 1;
    })
    .await;
    let from = app.get("/api/v1/canvas").await["seq"].as_u64().unwrap();
    // Pushed before the stream opens and replayed from `after`, so a slow
    // push cannot outlast the 1 s stream.
    let first = web_stroke(&app, 1).await;
    let opened = tokio::time::Instant::now();
    let mut s = app.events(&format!("?after={from}"), &[]).await;
    assert_eq!(s.status, 200);
    // The one slot is taken while it runs.
    assert_eq!(app.events("", &[]).await.status, 429);
    let e = s.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(e.id, Some(first));
    assert!(
        s.ended(Duration::from_secs(5)).await,
        "the stream never ended"
    );
    let age = opened.elapsed();
    assert!(
        age >= Duration::from_millis(900),
        "ended after {age:?}, before its 1 s age limit"
    );
    // Missed while disconnected, then replayed on resume.
    let second = web_stroke(&app, 2).await;
    let mut again = app.events(&format!("?after={}", e.id.unwrap()), &[]).await;
    assert_eq!(again.status, 200, "the ended stream kept its slot");
    let e = again.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!((e.id, e.kind.as_str()), (Some(second), "push"));
}

/// Invariant (#124, C7 review obligation 3): the busy check and the
/// address's start charge are one step, so a burst of concurrent starts
/// from one address starts one run and spends one of its starts, and
/// refused starts spend none: over `STARTS` bursts the address gets
/// exactly `STARTS` runs, then is refused as `rate-limited`.
///
/// Each burst is lined up behind the web sessions' lock, which the start
/// charge takes. Before #124 the handler checked "busy", then charged the
/// start under that lock, then started the run, three separate steps, so
/// every start of a burst passed the check and spent a start while one ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_swarm_starts_from_one_address_spend_one_start() {
    const STARTS: usize = 3;
    const BURST: usize = 8;
    let app = App::start(|c| {
        c.swarm.pushes_per_second_cap = 1;
        c.swarm.max_agents = 1;
        c.swarm.run_seconds = 3600;
        c.swarm.cooldown_seconds = 0;
        c.swarm_starts_per_hour = STARTS;
    })
    .await;
    for round in 1..=STARTS + 1 {
        let place = app.place.clone();
        let hold = tokio::task::spawn_blocking(move || {
            let _sessions = place.lock_sessions();
            std::thread::sleep(Duration::from_millis(300));
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let responses = burst(&app.base, "POST", "/api/v1/swarm", BURST).await;
        hold.await.unwrap();
        let mut accepted = 0;
        let mut refusals = Vec::new();
        for r in responses {
            match r.status {
                202 => accepted += 1,
                429 => refusals.push(r.json()["error"].as_str().unwrap().to_owned()),
                other => panic!("round {round}: {other}: {}", r.text()),
            }
        }
        if round <= STARTS {
            assert_eq!(
                (accepted, refusals.iter().all(|e| e == "cooldown")),
                (1, true),
                "round {round} of {STARTS}: one run, the rest refused as busy \
                 without spending a start: {refusals:?}"
            );
        } else {
            assert_eq!(accepted, 0);
            assert!(
                refusals.iter().all(|e| e == "rate-limited"),
                "after {STARTS} runs: {refusals:?}"
            );
        }
        // End the run; with no cooldown the next burst may start one.
        app.place
            .with_state(|s| s.end_swarm_run(aspen_place::state::now_ms()));
    }
}

/// Every merge is a push to `main`, and a clone plans the whole of
/// `main`'s history. Without Aspen's background worker each of those
/// pushes stays its own pack, so a clone stitches one stored pack per
/// merge, one blob read after another: the public demo's `main` reached
/// 4,941 packs and a full clone took 253 s. The worker `build` returns
/// repacks them into covers, which the planner picks instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_background_worker_compacts_main_into_covers() {
    use aspen_core::meta::MetaStore;
    use aspen_core::name::RefName;
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;

    let app = App::start(|_| {}).await;
    // The default layout covers every 16 versions of a ref at level 1.
    for i in 0..20u32 {
        app.place
            .paint(
                "paint/compact",
                Actor::agent("compact"),
                &[Tile {
                    x: i % 16,
                    y: i / 16,
                    color: (i % 8) as u8,
                }],
                "stroke",
            )
            .await
            .unwrap();
        app.place.merge_once().await.unwrap().expect("a merge");
    }
    let main = RefName::new("refs/heads/main").unwrap();
    let covered = |covers: &[aspen_core::meta::Cover]| covers.iter().any(|c| c.pack.is_some());
    let before = app.meta.covers(app.place.repo().id, &main).await.unwrap();
    assert!(!covered(&before), "nothing compacts before the worker runs");

    let mut worker = app.worker.lock().await;
    let mut ran = 0;
    while let Some(outcome) = worker.run_one().await.unwrap() {
        assert!(
            matches!(outcome, aspen_compact::JobOutcome::Done { .. }),
            "{outcome:?}"
        );
        ran += 1;
    }
    assert!(ran > 0, "pushes enqueue compaction and indexing jobs");
    let after = app.meta.covers(app.place.repo().id, &main).await.unwrap();
    assert!(
        covered(&after),
        "main's first 16 versions are one pack: {after:?}"
    );

    // The compacted history still clones.
    let check = app.clone_work("check").await;
    assert_eq!(
        check.git(&["rev-parse", "HEAD"]).trim(),
        app.tip("refs/heads/main").await.unwrap()
    );
}

/// Ambient strokes alone are merged at most once per
/// `ambient_merge_interval`, a person's push is merged on the next run and
/// takes them along, and an ambient agent's branch is deleted as soon as
/// its strokes are in `main`. Every merge adds a whole `tiles/` tree to
/// `main`, and every advertised branch's strokes go to every clone: on the
/// public demo a merge every 3 s and ever-growing agent branches made a
/// full clone grow by hundreds of megabytes a day.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ambient_strokes_merge_rarely_and_their_branches_go_once_merged() {
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;

    let app = App::start(|c| {
        c.merge_interval = Duration::from_millis(100);
        c.ambient_merge_interval = Duration::from_secs(3600);
    })
    .await;
    let (stop, rx) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(app.place.clone().run(async move {
        let _ = rx.await;
    }));
    let place = app.place.clone();
    let stroke = |x| {
        let place = place.clone();
        async move {
            place
                .paint(
                    "agent/wren-12",
                    Actor::agent("agent/wren-12"),
                    &[Tile { x, y: 0, color: 3 }],
                    "Ambient stroke",
                )
                .await
                .unwrap()
        }
    };
    let mut feed = app.events("?after=0", &[]).await;

    // The first ambient-only merge runs at once; the branch then goes.
    stroke(1).await;
    feed.until(Duration::from_secs(5), |e| e.kind == "expire")
        .await;
    assert_eq!(app.tip("refs/heads/agent/wren-12").await, None);
    let main = app.tip("refs/heads/main").await;

    // The next one waits for the interval.
    stroke(2).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(app.tip("refs/heads/main").await, main, "not merged alone");
    assert!(app.tip("refs/heads/agent/wren-12").await.is_some());

    // A person's push is merged on the next run, with the ambient stroke.
    let w = app.clone_work("w").await;
    w.paint(&[(5, 5, RED)], "p");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/bg"]).await.0);
    let merged = feed
        .until(Duration::from_secs(5), |e| {
            e.kind == "merge" && e.data["branches"].as_array().is_some_and(|b| b.len() == 2)
        })
        .await;
    assert!(!merged.is_empty());
    feed.until(Duration::from_secs(5), |e| e.kind == "expire")
        .await;
    assert_eq!(app.tip("refs/heads/agent/wren-12").await, None);
    assert!(
        app.tip("refs/heads/paint/bg").await.is_some(),
        "a person's branch keeps the TTL"
    );
    let _ = stop.send(());
    run.await.unwrap();
}

/// An operator can replace `main`'s history with one root commit holding
/// the same canvas: the picture is unchanged, merged branches (which hold
/// the old history) are deleted, a fresh clone has one commit and passes
/// `fsck`, and painting carries on on top of the new root. Everyone else is
/// refused. The public demo's `main` grew to thousands of whole-tree
/// commits before its growth was bounded, and a full clone took minutes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operator_resets_main_history_to_one_commit_with_the_same_canvas() {
    let app = App::start(|_| {}).await;
    let w = app.clone_work("ada").await;
    for (i, color) in [RED, BLUE, YELLOW].into_iter().enumerate() {
        w.paint(&[(i as u32, 1, color)], "stroke");
        assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/ada"]).await.0);
        app.place.merge_once().await.unwrap().unwrap();
    }
    // One more stroke, still pending: the reset merges it first.
    w.paint(&[(9, 9, RED)], "pending");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/ada"]).await.0);
    let before = app.get("/api/v1/canvas").await;

    let r = app
        .http("POST", "/api/v1/admin/reset-history", &[], None)
        .await;
    assert_eq!(r.status, 403);
    let operator = format!("Bearer {}", app.token("ops", &["admin:canvas"]));
    let r = app
        .http(
            "POST",
            "/api/v1/admin/reset-history",
            &[("Authorization", &operator)],
            None,
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let root = r.json()["commit"].as_str().unwrap().to_owned();
    assert_eq!(app.tip("refs/heads/main").await.as_deref(), Some(&root[..]));
    assert_eq!(
        app.tip("refs/heads/paint/ada").await,
        None,
        "merged branches go"
    );

    // The same picture, with the pending stroke merged (16 x 16 canvas).
    let after = app.get("/api/v1/canvas").await;
    let (before, after) = (
        before["pixels"].as_str().unwrap().to_owned(),
        after["pixels"].as_str().unwrap().to_owned(),
    );
    let pending = 9 * 16 + 9;
    assert_eq!(&before[pending..=pending], ".");
    assert_ne!(&after[pending..=pending], ".");
    assert_eq!(before[..pending], after[..pending]);
    assert_eq!(before[pending + 1..], after[pending + 1..]);
    let check = app.clone_work("check").await;
    assert_eq!(check.git(&["rev-list", "--count", "--all"]).trim(), "1");
    check.git(&["fsck", "--strict", "--no-dangling"]);
    assert_eq!(
        std::fs::read(check.path.join("tiles/9_9.txt")).unwrap(),
        format!("{RED}\n").as_bytes()
    );
    assert!(check.path.join("README.md").exists());

    // Painting carries on from the new root.
    check.paint(&[(3, 3, BLUE)], "after");
    assert!(
        check
            .push(&[&app.url(), "HEAD:refs/heads/paint/bob"])
            .await
            .0
    );
    app.place.merge_once().await.unwrap().unwrap();
    let again = app.clone_work("again").await;
    assert_eq!(again.git(&["rev-list", "--count", "HEAD"]).trim(), "2");
    assert_eq!(
        again.git(&["rev-list", "--max-parents=0", "HEAD"]).trim(),
        root
    );
}

/// The canvas's README names where `main` is mirrored and says pushes are
/// public; a restart with other settings rewrites it in one commit on
/// `main` that leaves the tiles alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_readme_names_the_mirror_and_follows_the_settings() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let app = App::start_in(path.clone(), None, |_| {}).await;
    let w = app.clone_work("a").await;
    let readme = std::fs::read_to_string(w.path.join("README.md")).unwrap();
    assert!(readme.contains("Pushes are public"), "{readme}");
    assert!(readme.contains("/api/v1/canvas.svg)"), "{readme}");
    assert!(!readme.contains("read-only mirror"), "{readme}");
    w.paint(&[(1, 1, RED)], "p");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/ada"]).await.0);
    app.place.merge_once().await.unwrap().unwrap();
    let before = app.tip("refs/heads/main").await.unwrap();
    drop(app);

    let app = App::start_in(path, Some(dir), |c| {
        c.mirror_url = Some("https://github.com/example/place-canvas".into());
    })
    .await;
    let w = app.clone_work("b").await;
    let readme = std::fs::read_to_string(w.path.join("README.md")).unwrap();
    assert!(
        readme.contains("This repository is a read-only mirror of `main`."),
        "{readme}"
    );
    assert_eq!(
        w.git(&["rev-parse", "HEAD^"]).trim(),
        before,
        "one commit on top of main"
    );
    assert_eq!(
        w.git(&["diff", "--name-only", "HEAD^", "HEAD"]).trim(),
        "README.md"
    );
    assert_eq!(
        std::fs::read(w.path.join("tiles/1_1.txt")).unwrap(),
        format!("{RED}\n").as_bytes()
    );
}

/// `GET /api/v1/canvas.svg` is `main`'s tiles as an SVG for READMEs: `no-cache`
/// with `main`'s commit as the ETag (so GitHub's image proxy revalidates
/// and a README shows the live canvas), 304 for a matching
/// `If-None-Match`, and a new image once a merge moves `main`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_canvas_svg_follows_main_and_revalidates_by_etag() {
    let app = App::start(|_| {}).await;
    let r = app.http("GET", "/api/v1/canvas.svg", &[], None).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("image/svg+xml"));
    assert_eq!(r.header("cache-control"), Some("no-cache, max-age=0"));
    let main = app.tip("refs/heads/main").await.unwrap();
    let etag = format!("\"{main}\"");
    assert_eq!(r.header("etag"), Some(&etag[..]));
    assert!(r.text().starts_with("<svg "), "{}", r.text());
    assert!(!r.text().contains("<path"), "nothing painted yet");

    let r = app
        .http(
            "GET",
            "/api/v1/canvas.svg",
            &[("If-None-Match", &etag)],
            None,
        )
        .await;
    assert_eq!(r.status, 304);
    assert_eq!(r.header("etag"), Some(&etag[..]));

    let w = app.clone_work("w").await;
    w.paint(&[(2, 3, RED)], "p");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/ada"]).await.0);
    app.place.merge_once().await.unwrap().unwrap();
    let r = app
        .http(
            "GET",
            "/api/v1/canvas.svg",
            &[("If-None-Match", &etag)],
            None,
        )
        .await;
    assert_eq!(r.status, 200, "main moved");
    assert_ne!(r.header("etag"), Some(&etag[..]));
    assert!(
        r.text()
            .contains(&format!(r#"<path fill="{RED}" d="M2 3h1v1h-1z"/>"#)),
        "{}",
        r.text()
    );
}

/// Ambient agents paint whole sprites from `place::sprites`, not noise:
/// with one agent and a quick merge, `main` ends up holding exactly one
/// sprite's tiles, and never inside a keep area.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ambient_agent_paints_one_whole_sprite() {
    let app = App::start(|c| {
        c.rules.width = 32;
        c.rules.height = 32;
        c.ambient_agents = 1;
        c.ambient_interval = Duration::from_millis(40);
        c.ambient_sprite_pause = Duration::from_secs(3600);
        c.merge_interval = Duration::from_millis(50);
        c.ambient_merge_interval = Duration::from_millis(0);
        c.keep_areas = vec![(0, 0, 32, 10)];
    })
    .await;
    let (stop, rx) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(app.place.clone().run(async move {
        let _ = rx.await;
    }));
    let palette = aspen_place::rules::default_palette();
    let mut found = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let snap = app.get("/api/v1/canvas").await;
        let pixels = snap["pixels"].as_str().unwrap().to_owned();
        let painted: Vec<(u32, u32, u8)> = pixels
            .chars()
            .enumerate()
            .filter(|(_, c)| *c != '.')
            .map(|(i, c)| {
                (
                    (i % 32) as u32,
                    (i / 32) as u32,
                    c.to_digit(36).unwrap() as u8,
                )
            })
            .collect();
        if painted.is_empty() {
            continue;
        }
        let x0 = painted.iter().map(|p| p.0).min().unwrap();
        let y0 = painted.iter().map(|p| p.1).min().unwrap();
        let mut got: Vec<(u32, u32, u8)> = painted.clone();
        got.sort();
        for s in aspen_place::sprites::SPRITES {
            for v in 0..s.variants.len() {
                let mut want: Vec<(u32, u32, u8)> = s
                    .tiles(v, x0, y0, &palette)
                    .unwrap()
                    .into_iter()
                    .map(|t| (t.x, t.y, t.color))
                    .collect();
                want.sort();
                if want == got {
                    found = Some((s.name, y0));
                }
            }
        }
        if found.is_some() {
            break;
        }
    }
    let (name, y0) = found.expect("main holds exactly one whole sprite");
    assert!(y0 >= 10, "{name} was painted inside the keep area");
    let _ = stop.send(());
    run.await.unwrap();
}

/// Merge commits record which tiles people and ambient sprites painted,
/// and a restarted server rebuilds its memory of recent paint from them,
/// so ambient agents keep clear of that work after every deploy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_memory_of_recent_paint_survives_a_restart() {
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let app = App::start_in(path.clone(), None, |_| {}).await;
    let w = app.clone_work("w").await;
    w.paint(&[(2, 3, RED), (4, 5, BLUE)], "people");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/ada"]).await.0);
    app.place
        .paint(
            "agent/wren-12",
            Actor::agent("agent/wren-12"),
            &[Tile {
                x: 9,
                y: 9,
                color: 3,
            }],
            "Ambient sprite",
        )
        .await
        .unwrap();
    app.place.merge_once().await.unwrap().unwrap();
    let head = w.git(&["ls-remote", &app.url(), "refs/heads/main"]);
    assert!(!head.is_empty());
    drop(app);

    let app = App::start_in(path, Some(dir), |_| {}).await;
    let empty = app
        .place
        .with_state(|s| s.human_tiles.is_empty() && s.sprite_tiles.is_empty());
    assert!(empty, "a fresh start remembers nothing");
    app.place.rebuild_memory().await.unwrap();
    let (people, sprites) = app.place.with_state(|s| {
        let mut p: Vec<usize> = s.human_tiles.keys().copied().collect();
        let mut q: Vec<usize> = s.sprite_tiles.keys().copied().collect();
        p.sort();
        q.sort();
        (p, q)
    });
    assert_eq!(people, vec![3 * 16 + 2, 5 * 16 + 4]);
    assert_eq!(sprites, vec![9 * 16 + 9]);
    let check = app.clone_work("check").await;
    let msg = check.git(&["log", "-1", "--format=%B", "HEAD"]);
    assert!(msg.contains("People-tiles: 2,3 4,5"), "{msg}");
    assert!(msg.contains("Sprite-tiles: 9,9"), "{msg}");
}

/// Personas paint as agents: each push is one tile on an `agent/<slug>-<n>`
/// branch, the feed names the persona, its commit's author is the persona
/// (the server commits), and none of them paints a keep area or a tile a
/// person just painted. The ambient sprite agents stay off while personas
/// are on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn personas_paint_one_tile_per_push_as_agents_and_keep_off_peoples_work() {
    use aspen_place::personas::{PersonaSettings, ROSTER};

    let app = App::start(|c| {
        c.rules.width = 32;
        c.rules.height = 32;
        // Would start six sprite agents if personas did not replace them.
        c.ambient_agents = 6;
        c.ambient_interval = Duration::from_millis(40);
        c.ambient_merge_interval = Duration::from_secs(3600);
        c.keep_areas = vec![(0, 0, 32, 8)];
        c.personas = Some(PersonaSettings {
            min_active: 3,
            max_active: 4,
            pushes_per_second: 40.0,
            push_gap_ms: (20, 60),
            pause_percent: 0,
            ..PersonaSettings::default()
        });
    })
    .await;
    // A person paints a block before the personas start.
    let w = app.clone_work("w").await;
    let block: Vec<(u32, u32, &str)> = (12..20)
        .flat_map(|y| (12..20).map(move |x| (x, y, RED)))
        .collect();
    w.paint(&block, "a person's block");
    assert!(w.push(&[&app.url(), "HEAD:refs/heads/paint/me"]).await.0);
    let mut feed = app.events("?after=0", &[]).await;
    let (stop, rx) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(app.place.clone().run(async move {
        let _ = rx.await;
    }));

    let mut pushes = Vec::new();
    while pushes.len() < 60 {
        let e = feed
            .next(Duration::from_secs(20))
            .await
            .expect("persona pushes");
        if e.kind == "push" && e.data["branch"].as_str().unwrap().starts_with("agent/") {
            pushes.push(e);
        }
    }
    let mut branches = std::collections::HashSet::new();
    for e in &pushes {
        let branch = e.data["branch"].as_str().unwrap();
        let label = e.data["actor"]["label"].as_str().unwrap();
        assert_eq!(e.data["actor"]["kind"], "agent", "{e:?}");
        let persona = ROSTER
            .iter()
            .find(|p| p.name == label)
            .unwrap_or_else(|| panic!("{label} is not a persona (an old sprite agent?)"));
        let n = branch
            .strip_prefix(&format!("agent/{}-", persona.slug))
            .unwrap_or_else(|| panic!("{branch} is not {label}'s"));
        assert!(n.parse::<u64>().is_ok(), "{branch}");
        let tiles = e.data["tiles"].as_array().unwrap();
        assert_eq!(tiles.len(), 1, "one tile per push: {e:?}");
        let (x, y) = (
            tiles[0]["x"].as_u64().unwrap(),
            tiles[0]["y"].as_u64().unwrap(),
        );
        assert!(y >= 8, "painted the keep area at {x},{y}");
        assert!(
            !((12..20).contains(&x) && (12..20).contains(&y)),
            "painted over the person's block at {x},{y}"
        );
        branches.insert((branch.to_owned(), persona.author()));
    }
    assert!(branches.len() >= 3, "{branches:?}");
    let active = app.place.personas_active();
    assert!((3..=4).contains(&active.len()), "{active:?}");
    for (name, branch) in &active {
        let p = ROSTER.iter().find(|p| p.name == *name).unwrap();
        assert!(
            branch.starts_with(&format!("agent/{}-", p.slug)),
            "{name} {branch}"
        );
    }

    // The commit on a persona's branch is by the persona.
    let (branch, author) = branches.iter().next().unwrap().clone();
    let r = format!("refs/heads/{branch}");
    w.git(&["fetch", "-q", &app.url(), &format!("{r}:{r}")]);
    let who = w.git(&["log", "-1", "--format=%an <%ae>|%cn <%ce>", &r]);
    assert_eq!(
        who.trim(),
        format!("{author}|place <place@aspen.invalid>"),
        "{branch}"
    );
    let _ = stop.send(());
    run.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn personas_resume_automatically_after_a_restart_without_visitor_budgets() {
    fn tune(c: &mut aspen_place::PlaceConfig) {
        c.rules.width = 32;
        c.rules.height = 32;
        c.pushes_per_minute = 1;
        c.ambient_merge_interval = Duration::from_millis(100);
        c.personas = Some(aspen_place::personas::PersonaSettings {
            min_active: 1,
            max_active: 1,
            pushes_per_second: 10.0,
            push_gap_ms: (20, 60),
            pause_percent: 0,
            ..Default::default()
        });
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let mut before = None;
    for boot in 0..2 {
        let app = App::start_in(path.clone(), None, tune).await;
        if let Some(ref pixels) = before {
            assert_eq!(&app.get("/api/v1/canvas").await["pixels"], pixels);
        }
        let mut feed = app.events("", &[]).await;
        let (stop, rx) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn(app.place.clone().run(async move {
            let _ = rx.await;
        }));
        for _ in 0..5 {
            feed.until(Duration::from_secs(20), |e| {
                e.kind == "push" && e.data["branch"].as_str().unwrap().starts_with("agent/")
            })
            .await;
        }
        let _ = stop.send(());
        run.await.unwrap();
        before = Some(app.get("/api/v1/canvas").await["pixels"].clone());
        assert!(
            before
                .as_ref()
                .unwrap()
                .as_str()
                .unwrap()
                .chars()
                .any(|p| p != '.'),
            "boot {boot} did not paint"
        );
    }
}

/// Personas never spend the server-wide push ceiling (#345): while they
/// paint, people still find every token of it; once people have used it up
/// (less than a tenth of its burst left), the personas stop pushing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn personas_leave_the_push_ceiling_to_people_and_back_off_at_it() {
    use aspen_place::personas::PersonaSettings;

    let app = App::start(|c| {
        c.rules.width = 32;
        c.rules.height = 32;
        c.pushes_per_minute = 100;
        c.ambient_merge_interval = Duration::from_secs(3600);
        c.global_pushes_per_second = 0.001;
        c.global_push_burst = 10;
        c.personas = Some(PersonaSettings {
            min_active: 3,
            max_active: 3,
            pushes_per_second: 40.0,
            push_gap_ms: (20, 60),
            pause_percent: 0,
            backoff_ms: 50,
            ..PersonaSettings::default()
        });
    })
    .await;
    let (stop, rx) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(app.place.clone().run(async move {
        let _ = rx.await;
    }));
    let agent_pushes = |evs: &[Ev]| {
        evs.iter()
            .filter(|e| {
                e.kind == "push" && e.data["branch"].as_str().unwrap().starts_with("agent/")
            })
            .count()
    };
    let mut feed = app.events("", &[]).await;
    let mut seen = Vec::new();
    while agent_pushes(&seen) < 10 {
        seen.push(
            feed.next(Duration::from_secs(20))
                .await
                .expect("persona pushes"),
        );
    }
    assert!(!app.place.push_ceiling_busy());

    // People find the whole burst: the personas took none of it.
    let s = app.http("POST", "/api/v1/sessions", &[], None).await.json();
    let auth = format!("Bearer {}", s["token"].as_str().unwrap());
    let h = [("Authorization", auth.as_str())];
    for x in 0..10 {
        let stroke = json!({ "branch": s["branch"], "tiles": [{ "x": x, "y": 31, "color": RED }] });
        let r = app.http("POST", "/api/v1/pushes", &h, Some(&stroke)).await;
        assert_eq!(r.status, 201, "push {x}: {}", r.text());
    }
    let stroke = json!({ "branch": s["branch"], "tiles": [{ "x": 20, "y": 31, "color": RED }] });
    let r = app.http("POST", "/api/v1/pushes", &h, Some(&stroke)).await;
    assert_eq!(r.status, 429, "the ceiling is spent: {}", r.text());
    assert!(app.place.push_ceiling_busy());

    // At the ceiling the personas push nothing (after any push already in
    // flight lands).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut quiet = app.events("", &[]).await;
    let after = events_for(&mut quiet, Duration::from_secs(2)).await;
    assert_eq!(agent_pushes(&after), 0, "{after:?}");
    let _ = stop.send(());
    run.await.unwrap();
}

/// Every event the stream delivers within `window`, read against one
/// fixed deadline: a stream that never goes quiet still ends on time, so a
/// "nothing happened" check fails instead of hanging.
async fn events_for(stream: &mut Sse, window: Duration) -> Vec<Ev> {
    let deadline = std::time::Instant::now() + window;
    let mut seen = Vec::new();
    while let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) {
        match stream.next(remaining).await {
            Some(e) => seen.push(e),
            None => break,
        }
    }
    seen
}

/// The kill switch stops the personas: no persona push lands while it is
/// engaged (after any push already in flight), and they carry on once it
/// is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_kill_switch_stops_the_personas() {
    use aspen_place::personas::PersonaSettings;

    let app = App::start(|c| {
        c.rules.width = 32;
        c.rules.height = 32;
        c.ambient_merge_interval = Duration::from_secs(3600);
        c.personas = Some(PersonaSettings {
            min_active: 3,
            max_active: 4,
            pushes_per_second: 40.0,
            push_gap_ms: (20, 60),
            pause_percent: 0,
            ..PersonaSettings::default()
        });
    })
    .await;
    let (stop, rx) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(app.place.clone().run(async move {
        let _ = rx.await;
    }));
    let is_persona =
        |e: &Ev| e.kind == "push" && e.data["branch"].as_str().unwrap().starts_with("agent/");
    let mut feed = app.events("", &[]).await;
    feed.until(Duration::from_secs(20), is_persona).await;

    app.place.set_kill_switch(true, Some("test".into()));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut quiet = app.events("", &[]).await;
    let during = events_for(&mut quiet, Duration::from_secs(2)).await;
    assert!(!during.iter().any(is_persona), "{during:?}");

    app.place.set_kill_switch(false, None);
    quiet.until(Duration::from_secs(20), is_persona).await;
    let _ = stop.send(());
    run.await.unwrap();
}

/// Invariant: the binary's object limit
/// ([`aspen_place::defaults::max_object_bytes`], here the 64 KiB floor of
/// the test canvas) refuses a commit whose message is larger, so commit
/// messages and author lines can no longer reach 16 MiB, and stores
/// nothing for it; an ordinary stroke still lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_message_over_the_object_limit_is_refused() {
    let app = App::start(|_| {}).await;
    let limit = aspen_place::defaults::max_object_bytes(&config().rules).get();
    assert_eq!(limit, aspen_place::defaults::MIN_OBJECT_BYTES);
    let w = app.clone_work("w").await;
    let refused = w.paint(&[(1, 1, RED)], &"m".repeat(limit as usize + 1));
    let stored_before = stored_files(&app.dir.join("blobs"));
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/long"]).await;
    assert!(
        !ok && out.contains(&format!("over the {limit}-byte object limit")),
        "{out}"
    );
    assert!(app.current("refs/heads/paint/long").await.is_none());
    assert_nothing_stored(&app, &stored_before, &refused).await;

    w.git(&["reset", "-q", "--hard", "HEAD~1"]);
    w.paint(&[(1, 1, RED)], "short");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/short"]).await;
    assert!(ok, "{out}");

    // The limit refuses only what is larger: a commit of exactly `limit`
    // bytes lands, and the same commit one byte longer does not.
    let size = |w: &Work| -> u64 { w.git(&["cat-file", "-s", "HEAD"]).trim().parse().unwrap() };
    w.paint(&[(2, 2, RED)], "probe");
    let message = "m".repeat((limit - size(&w)) as usize + "probe".len());
    w.git(&["commit", "-q", "--amend", "-m", &message]);
    assert_eq!(size(&w), limit, "a commit of exactly the limit");
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/exact"]).await;
    assert!(ok, "{out}");
    w.git(&["commit", "-q", "--amend", "-m", &format!("{message}m")]);
    assert_eq!(size(&w), limit + 1);
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/over"]).await;
    assert!(
        !ok && out.contains(&format!("over the {limit}-byte object limit")),
        "{out}"
    );
}

/// Invariant: with the binary's default limits
/// ([`aspen_place::defaults::limits`]) a pushed pack over
/// [`aspen_place::defaults::MAX_PACK_BYTES`] (1 MiB) is refused before it
/// is stored, even when every object in it is under the object limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pack_over_the_pack_limit_is_refused() {
    let app = App::start_with_limits(aspen_place::defaults::limits(), |_| {}).await;
    let w = app.clone_work("w").await;
    // 24 incompressible 48 KiB files: about 1.2 MiB of pack.
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    for i in 0..24 {
        let bytes: Vec<u8> = (0..48 << 10)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect();
        w.write(&format!("junk/{i}.bin"), &bytes);
    }
    w.git(&["add", "-A"]);
    w.git(&["commit", "-q", "-m", "too big"]);
    let refused = w.git(&["rev-parse", "HEAD"]).trim().to_owned();
    let stored_before = stored_files(&app.dir.join("blobs"));
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/big"]).await;
    // Past the limit the server stops reading the body, so git either
    // reads the refusal or sees the connection cut while still sending
    // (`curl 55 Send failure`, `curl 56` / `Connection reset by peer`,
    // `remote end hung up`): all are a refused push, and none stores
    // anything.
    assert!(!ok, "{out}");
    assert!(
        out.contains("push too large: at most 1048576 bytes per push")
            || out.contains("hung up")
            || out.contains("Broken pipe")
            || out.contains("Connection reset by peer")
            || out.contains("curl 56"),
        "{out}"
    );
    assert!(app.current("refs/heads/paint/big").await.is_none());
    assert_nothing_stored(&app, &stored_before, &refused).await;
}

/// A refused push stored nothing: the blob store holds the same files as
/// `before` (which must not be empty, so a wrong path cannot pass), and
/// its commit `refused` is not indexed.
async fn assert_nothing_stored(
    app: &App,
    before: &std::collections::BTreeSet<std::path::PathBuf>,
    refused: &str,
) {
    assert!(!before.is_empty(), "the blob store path holds the canvas");
    assert_eq!(
        &stored_files(&app.dir.join("blobs")),
        before,
        "the blob store is unchanged"
    );
    use aspen_core::meta::MetaStore;
    let id: aspen_core::id::ObjectId = refused.parse().unwrap();
    let lookup = app
        .meta
        .lookup_commits(
            app.place.repo().id,
            &aspen_core::ingest::SortedIds::from_unsorted(vec![id]),
        )
        .await
        .unwrap();
    assert!(
        matches!(lookup[..], [aspen_core::meta::CommitLookup::NotIndexed]),
        "{lookup:?}"
    );
}

/// Invariant: on a canvas whose full `tiles/` tree is over the 64 KiB
/// floor (64 x 64: 150,272 bytes, the live demo) a full canvas still takes
/// strokes over stock Git, merges, restarts its history (`bound_history`)
/// and reopens after a process restart. The second stroke reaches the
/// server as a delta of the full tree; with the object limit at exactly
/// the tree size it was refused as `internal storage error`, which is why
/// the limit has [`aspen_place::defaults::DELTA_HEADROOM`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_canvas_still_paints_merges_and_restarts_its_history() {
    let tune = |c: &mut aspen_place::PlaceConfig| {
        (c.rules.width, c.rules.height) = (64, 64);
        // Filled in one push; and every merge restarts the history.
        c.rules.tiles_per_push = 64 * 64;
        c.history_max_bytes = 1;
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let app = App::start_in(path.clone(), None, tune).await;
    let mut rules = config().rules;
    (rules.width, rules.height) = (64, 64);
    let limit = aspen_place::defaults::max_object_bytes(&rules).get();
    let full = aspen_place::defaults::full_tiles_tree_bytes(64, 64);
    assert_eq!(full, 150_272);
    assert!(full > aspen_place::defaults::MIN_OBJECT_BYTES);
    assert_eq!(limit, aspen_place::defaults::DELTA_HEADROOM * full);

    // A stock-Git stroke that fills every tile: its `tiles/` tree is the
    // full tree.
    let w = app.clone_work("ada").await;
    let all: Vec<(u32, u32, &str)> = (0..64)
        .flat_map(|x| (0..64).map(move |y| (x, y, RED)))
        .collect();
    w.paint(&all, "fill");
    assert_eq!(
        w.git(&["cat-file", "-s", "HEAD:tiles"]).trim(),
        full.to_string()
    );
    let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/ada"]).await;
    assert!(ok, "{out}");

    // The merger writes `main`'s full tree, then restarts its history.
    let old_main = app.tip("refs/heads/main").await.unwrap();
    app.place.merge_once().await.unwrap().unwrap();
    let merged = app.tip("refs/heads/main").await.unwrap();
    assert_ne!(merged, old_main);
    let root = app.place.bound_history().await.unwrap().expect("restarted");
    assert_eq!(app.tip("refs/heads/main").await, Some(root.to_string()));
    let check = app.clone_work("check").await;
    assert_eq!(
        check.git(&["cat-file", "-s", "HEAD:tiles"]).trim(),
        full.to_string()
    );
    assert_eq!(check.git(&["rev-list", "--count", "HEAD"]).trim(), "1");

    // Another stroke on the full canvas lands and merges onto the root.
    check.paint(&[(5, 5, BLUE)], "repaint");
    // Its `tiles/` tree goes out as a delta of the full tree: the thin
    // pack `git push` builds (`pack-objects --thin` against what the
    // server has), completed and listed, shows the tree with a delta base.
    let tree = check.git(&["rev-parse", "HEAD:tiles"]).trim().to_owned();
    let listed = blocking({
        let (dir, git_dir) = (check.path.clone(), check.path.join(".git"));
        move || {
            let script = format!(
                "printf 'HEAD\\n^origin/main\\n' | git pack-objects --revs --thin --stdout > '{g}/thin.pack' \\
                 && git index-pack --stdin --fix-thin '{g}/fixed.pack' < '{g}/thin.pack' > /dev/null \\
                 && git verify-pack -v '{g}/fixed.idx'",
                g = git_dir.display()
            );
            let o = std::process::Command::new("sh")
                .args(["-c", &script])
                .current_dir(dir)
                .output()
                .unwrap();
            assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
            String::from_utf8(o.stdout).unwrap()
        }
    })
    .await;
    let line = listed
        .lines()
        .find(|l| l.starts_with(&tree))
        .unwrap_or_else(|| panic!("{tree} not in the pack:\n{listed}"));
    let fields: Vec<&str> = line.split_whitespace().collect();
    assert!(
        fields.len() == 7 && fields[1] == "tree",
        "the stroke's tree is a delta (sha type size packed offset depth base): {line}"
    );
    let (ok, out) = check.push(&[&app.url(), "HEAD:refs/heads/paint/bob"]).await;
    assert!(ok, "{out}");
    app.place.merge_once().await.unwrap().unwrap();
    let after = app.tip("refs/heads/main").await.unwrap();
    assert_ne!(after, root.to_string());
    drop(app);

    // A process restart reads the full `main` back and keeps painting.
    let app = App::start_in(path, Some(dir), tune).await;
    assert_eq!(app.tip("refs/heads/main").await, Some(after));
    let again = app.clone_work("again").await;
    assert_eq!(
        std::fs::read(again.path.join("tiles/5_5.txt")).unwrap(),
        format!("{BLUE}\n").as_bytes()
    );
    again.paint(&[(6, 6, BLUE)], "after restart");
    let (ok, out) = again.push(&[&app.url(), "HEAD:refs/heads/paint/cy"]).await;
    assert!(ok, "{out}");
    app.place.merge_once().await.unwrap().unwrap();
}

/// Invariant: under the binary's default Git limits
/// ([`aspen_place::defaults::limits`] and `instance_limits`) one address
/// may clone and push many times at once. 24 stock `git push`es from one
/// address, all in flight together, all land; the old defaults (10 push
/// requests at once, so 5 pushes, and 4 running per caller) refused most.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_address_may_push_many_branches_at_once_under_the_default_limits() {
    let app = App::start_with_limits(aspen_place::defaults::limits(), |_| {}).await;
    let w = app.clone_work("w").await;
    let mut tips = Vec::new();
    for i in 0..24u32 {
        w.git(&["checkout", "-q", "-B", &format!("b{i}"), "origin/main"]);
        tips.push(w.paint(&[(i % 16, i / 16, RED)], &format!("stroke {i}")));
    }
    let pushes = (0..24u32).map(|i| {
        let (w, url) = (w.clone(), app.url());
        async move {
            w.push(&[&url, &format!("b{i}:refs/heads/paint/many-{i}")])
                .await
        }
    });
    for (i, (ok, out)) in futures_util::future::join_all(pushes)
        .await
        .into_iter()
        .enumerate()
    {
        assert!(ok, "push {i}: {out}");
    }
    for (i, tip) in tips.iter().enumerate() {
        assert_eq!(
            app.tip(&format!("refs/heads/paint/many-{i}"))
                .await
                .as_deref(),
            Some(tip.as_str())
        );
    }
}

/// Two instances on one store at once, as during a start-first ECS deploy
/// (docs/PLAN.md decisions log, "place deploys start-first"). The old
/// instance's last merge loses the race for `main` to the new one's merger
/// and must still land its own pending pushes; a server paint on a branch
/// the other instance moved must not stay refused. The test helpers never
/// run [`Place::run`], so neither instance's background merger or expiry
/// runs: every merge here is the one the test calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_instances_overlapping_lose_no_paint() {
    use aspen_place::place::PlaceError;
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let old = App::start_in(path.clone(), None, |_| {}).await;
    let tile = |x, color| [Tile { x, y: 0, color }];
    // The old instance paints a branch: pending in its memory only.
    old.place
        .paint("agent/old", Actor::agent("old"), &tile(1, 5), "")
        .await
        .unwrap();
    // The new instance starts beside it and lists that branch with
    // nothing pending, then paints and merges its own stroke: `main` moves
    // under the old instance.
    let new = App::start_in(path, Some(dir), |_| {}).await;
    new.place
        .paint("agent/new", Actor::agent("new"), &tile(2, 12), "")
        .await
        .unwrap();
    new.place.merge_once().await.unwrap().unwrap();

    // The old instance's last merge is refused once (stale `main`), then
    // lands on the new `main`.
    old.place.final_merge().await;
    assert!(
        old.place.with_state(|s| s.pending.is_empty()),
        "the old instance's last merge landed"
    );
    let main = new.clone_work("check").await;
    assert_eq!(
        main.git(&["show", "HEAD:tiles/1_0.txt"]).trim(),
        RED,
        "the old instance's pending tile reached main"
    );
    assert_eq!(
        main.git(&["show", "HEAD:tiles/2_0.txt"]).trim(),
        BLUE,
        "the new instance's tile stayed on main"
    );

    // Both paint one branch: the old instance's cached tip goes stale. Its
    // first paint is refused; the next reads the tip from the store and
    // lands, on top of the new instance's stroke.
    new.place
        .paint("agent/old", Actor::agent("old"), &tile(3, 12), "")
        .await
        .unwrap();
    let refused = old
        .place
        .paint("agent/old", Actor::agent("old"), &tile(4, 5), "")
        .await;
    assert!(
        matches!(refused, Err(PlaceError::Refused(_))),
        "a paint on a stale tip is refused, not failed: {refused:?}"
    );
    let painted = old
        .place
        .paint("agent/old", Actor::agent("old"), &tile(4, 5), "")
        .await
        .expect("a refused paint does not leave a stale tip cached");
    let w = new.clone_work("branch").await;
    w.run(&["fetch", "-q", &new.url(), "refs/heads/agent/old"])
        .await;
    assert_eq!(
        w.git(&["rev-parse", "FETCH_HEAD"]).trim(),
        painted.commit.to_string()
    );
    assert_eq!(w.git(&["show", "FETCH_HEAD:tiles/3_0.txt"]).trim(), BLUE);
    assert_eq!(w.git(&["show", "FETCH_HEAD:tiles/4_0.txt"]).trim(), RED);
}

/// During the overlap the new instance follows the old one's merges: its
/// canvas and its memory of people's tiles include what the old one merged,
/// so its personas keep off fresh human paint. Without catching up, the new
/// instance only learns of them when its own merge is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_new_instance_follows_the_old_ones_merges_into_its_memory() {
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let old = App::start_in(path.clone(), None, |_| {}).await;
    let new = App::start_in(path, Some(dir), |_| {}).await;
    // A person paints through the old instance, which merges it.
    old.place
        .paint(
            "visitors/ada",
            Actor::visitor("ada"),
            &[Tile {
                x: 5,
                y: 1,
                color: 5,
            }],
            "",
        )
        .await
        .unwrap();
    old.place.merge_once().await.unwrap().unwrap();
    let i = 16 + 5; // (5, 1) on the tests' 16-wide canvas
    assert!(!new.place.with_state(|s| s.human_tiles.contains_key(&i)));

    new.place.follow_main().await.unwrap();
    let (pixel, remembered, main) = new.place.with_state(|s| {
        (
            s.main.pixels[i],
            s.human_tiles.contains_key(&i),
            s.main.commit,
        )
    });
    assert_eq!(
        pixel, 5,
        "the new instance's canvas has the old one's merge"
    );
    assert!(remembered, "the new instance remembers the person's tile");
    assert_eq!(main, old.place.with_state(|s| s.main.commit));
    // Its personas' protected tiles include it.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert!(
        new.place
            .with_state(|s| s.protected_tiles(now))
            .contains(&i)
    );
    // Nothing moved since: following again is a no-op.
    new.place.follow_main().await.unwrap();
}

/// A branch the new instance found at start is not expired during the
/// overlap, even an `agent/` one with nothing pending (which this instance
/// would otherwise delete at once): the old instance may still paint on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branches_found_at_start_outlive_the_overlap() {
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let old = App::start_in(path.clone(), None, |_| {}).await;
    old.place
        .paint(
            "agent/old",
            Actor::agent("old"),
            &[Tile {
                x: 1,
                y: 0,
                color: 5,
            }],
            "",
        )
        .await
        .unwrap();
    let new = App::start_in(path, Some(dir), |_| {}).await;
    assert_eq!(new.place.expire_once().await.unwrap(), 0);
    assert!(
        new.place
            .with_state(|s| s.branches.contains_key("agent/old"))
    );
    // The old instance's next paint on it lands.
    old.place
        .paint(
            "agent/old",
            Actor::agent("old"),
            &[Tile {
                x: 2,
                y: 0,
                color: 5,
            }],
            "",
        )
        .await
        .unwrap();
}

/// Feeds of two instances side by side are numbered from different points
/// (`first_seq`), so a client that moves from one to the other with the
/// `seq` it last saw gets a `resync`, never a silent gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_moving_to_another_instance_is_told_to_resync() {
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let old = App::start_in(path.clone(), None, |c| c.first_seq = 1_000).await;
    let new = App::start_in(path, Some(dir), |c| c.first_seq = 2_000).await;
    for x in 0..3 {
        old.place
            .paint(
                "agent/old",
                Actor::agent("old"),
                &[Tile { x, y: 0, color: 5 }],
                "",
            )
            .await
            .unwrap();
    }
    let seen = old.get("/api/v1/canvas").await["seq"].as_u64().unwrap();
    assert_eq!(seen, 1_003);
    new.place
        .paint(
            "agent/new",
            Actor::agent("new"),
            &[Tile {
                x: 9,
                y: 0,
                color: 5,
            }],
            "",
        )
        .await
        .unwrap();
    // Old -> new: below the new feed's start.
    let mut s = new.events(&format!("?after={seen}"), &[]).await;
    let e = s.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(e.kind, "resync", "{e:?}");
    assert_eq!(e.data["seq"].as_u64(), Some(2_001));
    // New -> old: past the old feed's end.
    let mut s = old.events("?after=2001", &[]).await;
    assert_eq!(s.next(Duration::from_secs(5)).await.unwrap().kind, "resync");
}

/// A merge the other instance made reaches this instance's subscribers
/// when it follows `main`: one `merge` event with the tiles that changed.
/// Without it their canvases keep the old color until a resync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribers_see_the_merges_this_instance_follows() {
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let old = App::start_in(path.clone(), None, |_| {}).await;
    let new = App::start_in(path, Some(dir), |_| {}).await;
    let seen = new.get("/api/v1/canvas").await["seq"].as_u64().unwrap();
    let mut s = new.events(&format!("?after={seen}"), &[]).await;
    old.place
        .paint(
            "visitors/ada",
            Actor::visitor("ada"),
            &[Tile {
                x: 5,
                y: 1,
                color: 5,
            }],
            "",
        )
        .await
        .unwrap();
    old.place.merge_once().await.unwrap().unwrap();
    new.place.follow_main().await.unwrap();
    let e = s.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(e.kind, "merge", "{e:?}");
    assert_eq!(
        e.data["commit"].as_str(),
        Some(old.place.with_state(|s| s.main.commit).to_string().as_str())
    );
    assert_eq!(e.data["throughSeq"].as_u64(), Some(seen));
    let tiles = e.data["tiles"].as_array().unwrap();
    assert_eq!(tiles.len(), 1, "{tiles:?}");
    assert_eq!(
        (tiles[0]["x"].as_u64(), tiles[0]["y"].as_u64()),
        (Some(5), Some(1))
    );
    assert_eq!(tiles[0]["color"].as_str(), Some(RED));
    // The snapshot agrees.
    let snap = new.get("/api/v1/canvas").await;
    assert_eq!(snap["seq"].as_u64(), e.id);
    assert_eq!(snap["pixels"].as_str().unwrap().as_bytes()[16 + 5], b'5');
    // Nothing moved since: following again emits nothing.
    new.place.follow_main().await.unwrap();
    assert_eq!(new.place.with_state(|s| s.seq), e.id.unwrap());
}

/// An agent's stroke still pending on the old instance does not land on a
/// tile a person painted through the new one in the meantime: once the
/// old instance follows the person's merge, its last merge drops the
/// agent's tile there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_agent_stroke_never_lands_on_fresher_human_paint() {
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let old = App::start_in(path.clone(), None, |_| {}).await;
    let new = App::start_in(path, Some(dir), |_| {}).await;
    let tile = |x, color| [Tile { x, y: 0, color }];
    // The old instance's agent queues blue on (3, 0) and (4, 0).
    old.place
        .paint(
            "agent/old-1",
            Actor::agent("old"),
            &[tile(3, 12)[0], tile(4, 12)[0]],
            "",
        )
        .await
        .unwrap();
    // A person paints (3, 0) red through the new instance, merged there.
    new.place
        .paint("visitors/ada", Actor::visitor("ada"), &tile(3, 5), "")
        .await
        .unwrap();
    new.place.merge_once().await.unwrap().unwrap();
    // The old instance follows (its merger's tick), then shuts down.
    old.place.follow_main().await.unwrap();
    old.place.final_merge().await;
    assert!(old.place.with_state(|s| s.pending.is_empty()));
    let main = new.clone_work("check").await;
    assert_eq!(
        main.git(&["show", "HEAD:tiles/3_0.txt"]).trim(),
        RED,
        "the person's tile stays"
    );
    assert_eq!(
        main.git(&["show", "HEAD:tiles/4_0.txt"]).trim(),
        BLUE,
        "the agent's other tile lands"
    );
}

/// Feeds numbered from overlapping points: a cursor from the old
/// instance's feed falls inside the new one's backlog. Naming its
/// instance gets the client a `resync`; read as the new feed's, it would
/// silently skip the new feed's first events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cursor_from_another_instance_is_resynced_even_inside_this_feeds_range() {
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let old = App::start_in(path.clone(), None, |c| c.first_seq = 1_000).await;
    let new = App::start_in(path, Some(dir), |c| c.first_seq = 1_000).await;
    let paint = |app: &App, x| {
        let place = app.place.clone();
        async move {
            place
                .paint(
                    "agent/a-1",
                    Actor::agent("a"),
                    &[Tile { x, y: 0, color: 5 }],
                    "",
                )
                .await
                .unwrap();
        }
    };
    for x in 0..2 {
        paint(&old, x).await;
    }
    for x in 0..4 {
        paint(&new, x + 4).await;
    }
    let snap = old.get("/api/v1/canvas").await;
    let (seen, instance) = (
        snap["seq"].as_u64().unwrap(),
        snap["instance"].as_str().unwrap().to_owned(),
    );
    assert_eq!(seen, 1_002);
    let new_instance = new.get("/api/v1/canvas").await["instance"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(instance, new_instance);
    // A client naming the old feed: resync, with the new feed's instance.
    let mut s = new
        .events(&format!("?after={seen}&instance={instance}"), &[])
        .await;
    let e = s.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(e.kind, "resync", "{e:?}");
    assert_eq!(e.data["seq"].as_u64(), Some(1_004));
    assert_eq!(e.data["instance"].as_str(), Some(new_instance.as_str()));
    // Naming the new feed itself: the backlog as usual.
    let mut s = new
        .events(&format!("?after={seen}&instance={new_instance}"), &[])
        .await;
    let e = s.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!((e.kind.as_str(), e.id), ("push", Some(1_003)));
}

/// Every path that reloads a `main` another writer moved also remembers
/// its merges, even when the canvas got there first: a refused history
/// reset reloads the canvas, and the person's tile merged by the other
/// instance is still remembered (and stays so on the next follow).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_history_reset_still_remembers_the_other_instances_merges() {
    use aspen_place::place::PlaceError;
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let old = App::start_in(path.clone(), None, |_| {}).await;
    let new = App::start_in(path, Some(dir), |_| {}).await;
    new.place.rebuild_memory().await.unwrap();
    old.place
        .paint(
            "visitors/ada",
            Actor::visitor("ada"),
            &[Tile {
                x: 5,
                y: 1,
                color: 5,
            }],
            "",
        )
        .await
        .unwrap();
    old.place.merge_once().await.unwrap().unwrap();
    let i = 16 + 5;
    let refused = new.place.reset_history().await;
    assert!(
        matches!(refused, Err(PlaceError::Refused(_))),
        "{refused:?}"
    );
    assert_eq!(new.place.with_state(|s| s.main.pixels[i]), 5);
    new.place.follow_main().await.unwrap();
    assert!(
        new.place.with_state(|s| s.human_tiles.contains_key(&i)),
        "the person's tile is remembered"
    );
}

//! `main`'s history stays bounded: the merger starts it again from one
//! root commit once it outgrows `history_max_bytes`, without losing a
//! merge, and clones and branches from before a reset keep pushing.
//! Each test states the invariant it pins.

mod support;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aspen_place::rules::Tile;
use aspen_place::state::Actor;
use support::*;

const RED: &str = "#d6262b";
const BLUE: &str = "#3690ea";
const YELLOW: &str = "#ffd635";

/// `git clone --bare <url> <name>` with `args` added, in the app's
/// directory.
async fn clone_bare(app: &App, name: &str, args: &[&str]) -> Work {
    let (url, home, dir) = (app.url(), app.home.clone(), app.dir.clone());
    let mut all: Vec<String> = vec!["clone".into(), "-q".into(), "--bare".into()];
    all.extend(args.iter().map(|a| (*a).to_owned()));
    all.extend([url, name.to_owned()]);
    blocking(move || {
        let a: Vec<&str> = all.iter().map(String::as_str).collect();
        git_ok(&home, &dir, &a);
    })
    .await;
    Work {
        path: app.dir.join(name),
        home: app.home.clone(),
    }
}

/// A clone's pack size in KiB, as `git count-objects -v` reports it.
fn pack_kib(w: &Work) -> u64 {
    w.git(&["count-objects", "-v"])
        .lines()
        .find_map(|l| l.strip_prefix("size-pack: "))
        .and_then(|v| v.trim().parse().ok())
        .expect("size-pack")
}

/// Every tile of `main` in a bare clone, row-major, as its file's color.
fn tiles_of(w: &Work, width: u32, height: u32) -> Vec<Option<String>> {
    let by_blob: std::collections::HashMap<String, String> = colors()
        .into_iter()
        .map(|c| {
            let id = aspen_place::git::object_id(
                aspen_core::ingest::ObjectKind::Blob,
                format!("{c}\n").as_bytes(),
            );
            (id.to_string(), c)
        })
        .collect();
    let listing = w.git(&["ls-tree", "-r", "main", "tiles/"]);
    let mut out = vec![None; (width * height) as usize];
    for line in listing.lines() {
        let (meta, path) = line.split_once('\t').unwrap();
        let blob = meta.split(' ').nth(2).unwrap();
        let (x, y) = path
            .strip_prefix("tiles/")
            .and_then(|p| p.strip_suffix(".txt"))
            .and_then(|p| p.split_once('_'))
            .unwrap();
        let (x, y): (u32, u32) = (x.parse().unwrap(), y.parse().unwrap());
        let color = by_blob.get(blob).cloned().unwrap_or_else(|| {
            String::from_utf8_lossy(w.git(&["cat-file", "blob", blob]).trim().as_bytes())
                .into_owned()
        });
        out[(y * width + x) as usize] = Some(color);
    }
    out
}

fn colors() -> Vec<String> {
    aspen_place::rules::default_palette()
        .into_iter()
        .map(|p| p.color)
        .collect()
}

/// Paints the whole 64 x 64 canvas, then `strokes` strokes of 8 tiles,
/// each merged on its own (with the history bound checked after each
/// merge, as the merger does). Returns the canvas it expects (palette
/// indexes) and how many times the history was started again.
async fn paint_and_merge(app: &App, strokes: u32) -> (Vec<u8>, usize) {
    let mut expected = vec![0u8; 64 * 64];
    for y in 0..64u32 {
        let tiles: Vec<Tile> = (0..64u32)
            .map(|x| Tile {
                x,
                y,
                color: ((x + y) % 16) as u8,
            })
            .collect();
        for t in &tiles {
            expected[(t.y * 64 + t.x) as usize] = t.color;
        }
        app.place
            .paint("agent/fill", Actor::agent("agent/fill"), &tiles, "fill")
            .await
            .unwrap();
    }
    app.place.merge_once().await.unwrap().unwrap();
    let mut resets = 0;
    for i in 0..strokes {
        let tiles: Vec<Tile> = (0..8u32)
            .map(|k| Tile {
                x: (i * 8 + k) % 64,
                y: (i * 8 + k) / 64 % 64,
                color: ((i + k) % 7) as u8,
            })
            .collect();
        for t in &tiles {
            expected[(t.y * 64 + t.x) as usize] = t.color;
        }
        // A server painter's branch builds on its own last commit, so it
        // reaches the history it started on; `agent/` branches expire once
        // merged, like the ambient agents'.
        let branch = format!("agent/s{}", i % 3);
        app.place
            .paint(&branch, Actor::agent(&branch), &tiles, "stroke")
            .await
            .unwrap();
        app.place.merge_once().await.unwrap().unwrap();
        if app.place.bound_history().await.unwrap().is_some() {
            resets += 1;
        }
        app.place.expire_once().await.unwrap();
    }
    (expected, resets)
}

/// The merger keeps a full clone of `main` within `history_max_bytes`
/// (plus at most one merge), however many merges there are, and every
/// merge across the resets is in the final canvas. Without the bound the
/// clone grows with every merge (each stores a whole `tiles/` tree).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_clone_stays_within_the_history_limit_after_many_merges() {
    const STROKES: u32 = 120;
    const LIMIT: u64 = 256 << 10;
    let config = |max: u64| {
        move |c: &mut aspen_place::PlaceConfig| {
            c.rules.width = 64;
            c.rules.height = 64;
            c.history_max_bytes = max;
        }
    };
    let unbounded = App::start(config(0)).await;
    let (expected_u, resets_u) = paint_and_merge(&unbounded, STROKES).await;
    assert_eq!(resets_u, 0, "0 never starts the history again");
    let bounded = App::start(config(LIMIT)).await;
    let (mut expected_b, resets_b) = paint_and_merge(&bounded, STROKES).await;
    // Fill the current history up to just under the limit, the largest a
    // clone gets, before the next merge starts it again.
    let mut i = STROKES;
    while bounded.place.with_state(|s| s.main.history.bytes) + (16 << 10) < LIMIT {
        let tile = Tile {
            x: i % 64,
            y: 63,
            color: (i % 5) as u8,
        };
        expected_b[(63 * 64 + i % 64) as usize] = tile.color;
        bounded
            .place
            .paint("agent/top", Actor::agent("agent/top"), &[tile], "top up")
            .await
            .unwrap();
        bounded.place.merge_once().await.unwrap().unwrap();
        assert_eq!(bounded.place.bound_history().await.unwrap(), None);
        i += 1;
    }
    bounded.place.expire_once().await.unwrap();
    let mut expected_u = expected_u;
    for j in STROKES..i {
        let tile = Tile {
            x: j % 64,
            y: 63,
            color: (j % 5) as u8,
        };
        expected_u[(63 * 64 + j % 64) as usize] = tile.color;
        unbounded
            .place
            .paint("agent/top", Actor::agent("agent/top"), &[tile], "top up")
            .await
            .unwrap();
        unbounded.place.merge_once().await.unwrap().unwrap();
    }
    unbounded.place.expire_once().await.unwrap();
    assert_eq!(expected_u, expected_b);

    let full_u = clone_bare(&unbounded, "full", &[]).await;
    let full_b = clone_bare(&bounded, "full", &[]).await;
    let depth1 = clone_bare(&bounded, "depth1", &["--depth=1"]).await;
    let (u, b, d) = (pack_kib(&full_u), pack_kib(&full_b), pack_kib(&depth1));
    let history = bounded.place.with_state(|s| s.main.history);
    eprintln!(
        "after {i} merges on a full 64 x 64 canvas: full clone {u} KiB without a limit, \
         {b} KiB with a {} KiB limit ({resets_b} resets; main has {} commits since its root, \
         {} KiB of packs); --depth=1 clone {d} KiB",
        LIMIT >> 10,
        history.commits,
        history.bytes >> 10,
    );
    // One merge's pack on a full canvas is about 12 KiB.
    assert!(u > 4 * (LIMIT >> 10), "the unbounded clone grows: {u} KiB");
    assert!(
        b <= (LIMIT >> 10) + 32,
        "the bounded clone is {b} KiB, over {} KiB plus one merge",
        LIMIT >> 10
    );
    assert!(b >= (LIMIT >> 10) / 2, "the history was filled up: {b} KiB");
    assert!(resets_b >= 4, "{resets_b} resets");
    assert!(history.bytes < LIMIT);
    let rev_count: u64 = full_b
        .git(&["rev-list", "--count", "main"])
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        rev_count,
        history.commits + 1,
        "the root and what follows it"
    );

    // No merge is lost across the resets: main holds every stroke.
    let colors = colors();
    let want: Vec<Option<String>> = expected_b
        .iter()
        .map(|&c| Some(colors[usize::from(c)].clone()))
        .collect();
    assert_eq!(tiles_of(&full_b, 64, 64), want);
    assert_eq!(tiles_of(&full_u, 64, 64), want);
}

/// A history reset loses nothing a client relies on: pending pushes merge
/// first, a branch built on the old history keeps pushing and merging, a
/// full and a `--depth=1` clone made two resets ago push new branches, the
/// old history stays fetchable by the tip the root commit names, the
/// tile rules still apply to pushes on old history, and a restart reads
/// the history's size back from `main`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clones_and_branches_from_before_a_reset_still_push_and_merge() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    // A 1-byte limit: every check after a merge starts the history again.
    let app = App::start_in(path.clone(), None, |c| c.history_max_bytes = 1).await;
    let ada = app.clone_work("ada").await;
    let url = app.url();
    {
        let (home, d, u) = (app.home.clone(), app.dir.clone(), url.clone());
        blocking(move || git_ok(&home, &d, &["clone", "-q", "--depth=1", &u, "bob"])).await;
    }
    let bob = Work {
        path: app.dir.join("bob"),
        home: app.home.clone(),
    };
    bob.git(&["config", "user.name", "Bob"]);
    bob.git(&["config", "user.email", "bob@example.com"]);
    // Some history to reset.
    for x in 4..8 {
        ada.paint(&[(x, 5, BLUE)], "history");
        assert!(ada.push(&[&url, "HEAD:refs/heads/paint/ada"]).await.0);
        app.place.merge_once().await.unwrap().unwrap();
    }

    // Ada's stroke is pending (in flight) when the history is reset.
    ada.paint(&[(0, 0, RED)], "a1");
    assert!(ada.push(&[&url, "HEAD:refs/heads/paint/ada"]).await.0);
    let before_reset = clone_bare(&app, "before", &["--single-branch", "--branch", "main"]).await;
    let old1_count = before_reset.git(&["rev-list", "--count", "main"]);
    let root1 = app
        .place
        .bound_history()
        .await
        .unwrap()
        .expect("over 1 byte");
    assert_eq!(app.tip("refs/heads/main").await, Some(root1.to_string()));
    // The reset merged the pending stroke first: the root holds it.
    let at_root1 = clone_bare(&app, "root1", &["--single-branch", "--branch", "main"]).await;
    let root_tiles = tiles_of(&at_root1, 16, 16);
    assert_eq!(root_tiles[0].as_deref(), Some(RED));
    let old1 = before_reset.git(&["rev-parse", "main"]).trim().to_owned();
    assert_ne!(
        app.tip("refs/heads/main").await.unwrap(),
        old1,
        "main moved to the root"
    );
    // The reset keeps branches: Ada's, built on the old history, pushes
    // and merges on.
    assert!(app.tip("refs/heads/paint/ada").await.is_some());
    ada.paint(&[(1, 0, BLUE)], "a2");
    assert!(ada.push(&[&url, "HEAD:refs/heads/paint/ada"]).await.0);
    app.place.merge_once().await.unwrap().unwrap();
    let old2 = app.tip("refs/heads/main").await.unwrap();
    let root2 = app
        .place
        .bound_history()
        .await
        .unwrap()
        .expect("over 1 byte");

    // Bob's --depth=1 clone and Ada's full clone are two resets old; both
    // push new branches, checked from the old main they build on.
    bob.paint(&[(2, 0, YELLOW)], "b1");
    let (ok, out) = bob.push(&[&url, "HEAD:refs/heads/paint/bob"]).await;
    assert!(ok, "{out}");
    let (ok, out) = ada.push(&[&url, "HEAD:refs/heads/paint/ada-2"]).await;
    assert!(ok, "{out}");
    // A push built on old history is still checked in full: a file that is
    // not a tile is refused, and so is a branch with no main at all.
    bob.write("notes.txt", b"hi\n");
    bob.git(&["add", "notes.txt"]);
    bob.git(&["commit", "-q", "-m", "not a tile"]);
    let (ok, out) = bob.push(&[&url, "HEAD:refs/heads/paint/bob-notes"]).await;
    assert!(!ok && out.contains("notes.txt"), "{out}");
    bob.git(&["reset", "-q", "--hard", "HEAD~1"]);
    let orphan = app.clone_work("orphan").await;
    orphan.git(&["checkout", "-q", "--orphan", "fresh"]);
    orphan.write("notes.txt", b"hi\n");
    orphan.git(&["add", "-A"]);
    orphan.git(&["commit", "-q", "-m", "unrelated"]);
    let (ok, out) = orphan.push(&[&url, "HEAD:refs/heads/paint/orphan"]).await;
    assert!(!ok && out.contains("notes.txt"), "{out}");

    app.place.merge_once().await.unwrap().unwrap();
    let check = clone_bare(&app, "check", &["--single-branch", "--branch", "main"]).await;
    let tiles = tiles_of(&check, 16, 16);
    assert_eq!(tiles[0].as_deref(), Some(RED));
    assert_eq!(tiles[1].as_deref(), Some(BLUE));
    assert_eq!(tiles[2].as_deref(), Some(YELLOW));
    assert_eq!(
        check.git(&["rev-list", "--count", "main"]).trim(),
        "2",
        "the second root and one merge"
    );
    assert_eq!(
        check.git(&["rev-list", "--max-parents=0", "main"]).trim(),
        root2.to_string()
    );

    // The old histories stay in the repository, named by the roots.
    let message = check.git(&["log", "-1", "--format=%B", &root2.to_string()]);
    assert!(message.contains(&old2), "{message}");
    // The first reset merged Ada's pending stroke on top of `old1`, then
    // started again from that merge, which its root names.
    let message = at_root1.git(&["log", "-1", "--format=%B", &root1.to_string()]);
    let named = message
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("Start the canvas again from main at "))
        .expect("the root names the old tip")
        .to_owned();
    let old = clone_bare(&app, "old", &["--single-branch", "--branch", "main"]).await;
    old.git(&["fetch", "-q", &url, &named]);
    old.git(&["merge-base", "--is-ancestor", &old1, &named]);
    let count: u64 = old
        .git(&["rev-list", "--count", &named])
        .trim()
        .parse()
        .unwrap();
    assert_eq!(count, old1_count.trim().parse::<u64>().unwrap() + 1);
    old.git(&["fsck", "--strict", "--no-dangling"]);

    // A restart reads the history's size back from main's tip.
    let before = app.place.with_state(|s| s.main.history);
    drop(app);
    let app = App::start_in(path, Some(dir), |c| c.history_max_bytes = 1).await;
    let after = app.place.with_state(|s| s.main.history);
    assert_eq!((after.root, after.commits), (before.root, before.commits));
    assert!(
        after.bytes < before.bytes && after.bytes > 0,
        "{after:?} {before:?}"
    );
}

/// Under the background loop, with the history reset every few merges
/// while strokes and Git pushes land concurrently, every accepted push
/// reaches `main`: none is lost in a reset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_push_is_lost_when_the_history_resets_under_concurrent_pushes() {
    let app = Arc::new(
        App::start(|c| {
            c.merge_interval = Duration::from_millis(20);
            c.busy_pushes_per_second = 1_000_000;
            c.history_max_bytes = 4 << 10;
        })
        .await,
    );
    let (stop, rx) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(app.place.clone().run(async move {
        let _ = rx.await;
    }));
    // Roots seen while it runs.
    let roots = Arc::new(Mutex::new(HashSet::new()));
    let watching = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let watcher = {
        let (app, roots, watching) = (app.clone(), roots.clone(), watching.clone());
        tokio::spawn(async move {
            while watching.load(std::sync::atomic::Ordering::SeqCst) {
                let root = app.place.with_state(|s| s.main.history.root);
                roots.lock().unwrap().insert(root);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };
    // 8 painters, 28 strokes each, every stroke a tile no one else paints
    // (rows 0 to 13 of the 16 x 16 canvas).
    let mut tasks = Vec::new();
    for p in 0..8u32 {
        let app = app.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..28u32 {
                let n = p * 28 + i;
                let tile = Tile {
                    x: n % 16,
                    y: n / 16,
                    color: (n % 15 + 1) as u8,
                };
                let branch = format!("paint/p{p}");
                app.place
                    .paint(&branch, Actor::agent(&branch), &[tile], "stroke")
                    .await
                    .unwrap();
            }
        }));
    }
    // A Git client cloned at the start pushes rows 14 and 15 meanwhile,
    // through stock git, one tile per push.
    let w = app.clone_work("git").await;
    for x in 0..16u32 {
        w.paint(&[(x, 14 + x % 2, YELLOW)], "git stroke");
        let (ok, out) = w.push(&[&app.url(), "HEAD:refs/heads/paint/git"]).await;
        assert!(ok, "{out}");
    }
    for t in tasks {
        t.await.unwrap();
    }
    // Everything accepted merges.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let done = app
            .place
            .with_state(|s| s.pending.is_empty() && s.merged_through > 0);
        if done {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pushes never merged"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    watching.store(false, std::sync::atomic::Ordering::SeqCst);
    watcher.await.unwrap();
    let _ = stop.send(());
    run.await.unwrap();

    let check = clone_bare(&app, "check", &["--single-branch", "--branch", "main"]).await;
    let tiles = tiles_of(&check, 16, 16);
    let colors = colors();
    for n in 0..224u32 {
        assert_eq!(
            tiles[n as usize].as_deref(),
            Some(colors[(n % 15 + 1) as usize].as_str()),
            "stroke {n} lost"
        );
    }
    for x in 0..16u32 {
        let i = ((14 + x % 2) * 16 + x) as usize;
        assert_eq!(tiles[i].as_deref(), Some(YELLOW), "git stroke {x} lost");
    }
    let roots = roots.lock().unwrap().len();
    assert!(roots >= 3, "the history was started again {roots} times");
}

/// The merger runs every `merge_interval` up to `busy_pushes_per_second`,
/// waits longer in proportion above it, and never longer than
/// `merge_interval_max`.
#[test]
fn the_merge_interval_lengthens_with_load_up_to_its_maximum() {
    let c = aspen_place::PlaceConfig::new("http://localhost/canvas.git");
    let wait = |pushes: usize, ms: u64| {
        aspen_place::place::merge_wait(&c, pushes, Duration::from_millis(ms)).as_millis()
    };
    assert_eq!(wait(0, 3000), 3000);
    assert_eq!(wait(15, 3000), 3000, "5 pushes a second");
    assert_eq!(wait(30, 3000), 6000, "10 pushes a second");
    assert_eq!(wait(150, 3000), 30_000, "50 pushes a second");
    assert_eq!(wait(1500, 3000), 30_000, "500 pushes a second");
    assert_eq!(wait(1, 0), 30_000, "no time at all");
    assert_eq!(
        aspen_place::place::merge_wait(&c, 100_000, Duration::MAX),
        Duration::from_secs(3),
        "the first run, with no merge before it"
    );
}

/// Sustained load, run by hand in release mode (it takes a minute):
///
/// ```text
/// cargo test --release -p aspen-place --test history -- --ignored --nocapture
/// ```
///
/// Strokes arrive at `PLACE_LOAD_RATE` a second (default 500) for
/// `PLACE_LOAD_SECONDS` (default 60), each an in-process push of one to
/// three random tiles through receive-pack on one of 64 branches, while
/// the background loop merges (every 300 ms, up to 3 s under load) and
/// starts `main`'s history again whenever it passes
/// `PLACE_LOAD_HISTORY_KIB` (default 64), a stress setting that resets
/// every few merges. Then every accepted stroke must
/// be in `main`, replayed in sequence order, and a full clone of `main`
/// must be within the limit. Prints the rate reached, stroke latency,
/// merges and resets.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "load test; run by hand in release mode"]
async fn sustained_load_loses_no_push_and_keeps_the_history_bounded() {
    let env = |k: &str, d: u64| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    let rate = env("PLACE_LOAD_RATE", 500);
    let seconds = env("PLACE_LOAD_SECONDS", 60);
    // `PLACE_LOAD_HISTORY_KIB=0` turns resets off, for comparison.
    let limit_kib = env("PLACE_LOAD_HISTORY_KIB", 64);
    let limit: u64 = limit_kib << 10;
    let app = Arc::new(
        App::start(|c| {
            c.rules.width = 64;
            c.rules.height = 64;
            c.merge_interval = Duration::from_millis(300);
            c.merge_interval_max = Duration::from_secs(3);
            c.history_max_bytes = limit;
        })
        .await,
    );
    let (stop, rx) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(app.place.clone().run(async move {
        let _ = rx.await;
    }));
    let roots = Arc::new(Mutex::new(HashSet::new()));
    let merges = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let watching = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let watcher = {
        let (app, roots, merges, watching) =
            (app.clone(), roots.clone(), merges.clone(), watching.clone());
        tokio::spawn(async move {
            let mut last = None;
            let mut max_pending = 0usize;
            while watching.load(std::sync::atomic::Ordering::SeqCst) {
                let (root, commit, pending) = app
                    .place
                    .with_state(|s| (s.main.history.root, s.main.commit, s.pending.len()));
                roots.lock().unwrap().insert(root);
                if last != Some(commit) {
                    merges.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    last = Some(commit);
                }
                max_pending = max_pending.max(pending);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            max_pending
        })
    };
    const WORKERS: u64 = 64;
    let started = std::time::Instant::now();
    let mut tasks = Vec::new();
    for w in 0..WORKERS {
        let app = app.clone();
        tasks.push(tokio::spawn(async move {
            let mut done = Vec::new();
            let mut latencies = Vec::new();
            let mut seed = w.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            let mut next = || {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed
            };
            let every = Duration::from_secs_f64(WORKERS as f64 / rate as f64);
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let branch = format!("paint/load-{w}");
            let end = std::time::Instant::now() + Duration::from_secs(seconds);
            let mut refused = 0u64;
            while std::time::Instant::now() < end {
                tick.tick().await;
                let mut tiles: Vec<Tile> = Vec::new();
                for _ in 0..1 + next() % 3 {
                    let (x, y) = ((next() % 64) as u32, (next() % 64) as u32);
                    if tiles.iter().all(|t| (t.x, t.y) != (x, y)) {
                        tiles.push(Tile {
                            x,
                            y,
                            color: (next() % 16) as u8,
                        });
                    }
                }
                let t0 = std::time::Instant::now();
                match app
                    .place
                    .paint(&branch, Actor::agent(&branch), &tiles, "load")
                    .await
                {
                    Ok(p) => {
                        latencies.push(t0.elapsed());
                        done.push((p.seq, tiles));
                    }
                    Err(_) => refused += 1,
                }
            }
            (done, latencies, refused)
        }));
    }
    let mut strokes = Vec::new();
    let mut latencies = Vec::new();
    let mut refused = 0;
    for t in tasks {
        let (d, l, r) = t.await.unwrap();
        strokes.extend(d);
        latencies.extend(l);
        refused += r;
    }
    let elapsed = started.elapsed();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !app.place.with_state(|s| s.pending.is_empty()) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "pushes never merged"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    watching.store(false, std::sync::atomic::Ordering::SeqCst);
    let max_pending = watcher.await.unwrap();
    let _ = stop.send(());
    run.await.unwrap();

    latencies.sort();
    let pct = |p: f64| latencies[((latencies.len() as f64 - 1.0) * p) as usize];
    let resets = roots.lock().unwrap().len() - 1;
    eprintln!(
        "{} strokes in {:.1} s ({:.0}/s, target {rate}/s), {refused} refused; stroke latency p50 {:?} p99 {:?} max {:?}; \
         {} merges, {resets} history resets, at most {max_pending} pushes pending",
        strokes.len(),
        elapsed.as_secs_f64(),
        strokes.len() as f64 / elapsed.as_secs_f64(),
        pct(0.5),
        pct(0.99),
        latencies.last().unwrap(),
        merges.load(std::sync::atomic::Ordering::SeqCst),
    );
    assert_eq!(refused, 0, "a stroke was refused");

    // Every stroke is in main, replayed in sequence order.
    strokes.sort_by_key(|(seq, _)| *seq);
    let mut expected: Vec<Option<String>> = vec![None; 64 * 64];
    let colors = colors();
    for (_, tiles) in &strokes {
        for t in tiles {
            expected[(t.y * 64 + t.x) as usize] = Some(colors[usize::from(t.color)].clone());
        }
    }
    let check = clone_bare(&app, "check", &["--single-branch", "--branch", "main"]).await;
    assert_eq!(
        tiles_of(&check, 64, 64),
        expected,
        "a merged stroke is missing"
    );
    let kib = pack_kib(&check);
    eprintln!("full clone of main afterwards: {kib} KiB (limit {limit_kib} KiB)");
    if limit > 0 {
        assert!(kib <= limit_kib + 32);
        assert!(resets >= 1, "no history reset under load");
    }
}

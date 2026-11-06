//! Depth-1 clones of a moving trunk at monorepo scale (nightly).
//!
//! The fixture has the shape of the monorepo benchmark's trunk: 300,000
//! files eight levels deep, a full closure several snapshots large, and
//! small landings on top. After the trunk moves, a storm of concurrent
//! `git fetch --depth=1` (the `actions/checkout` default) of the new tip
//! must never be sent the full history, must each finish within the build
//! wait (the budget a shallow fetch would otherwise wait for a build), and
//! must keep the server's memory bounded. The last storm, once the last
//! tip's exact pack is recorded, is the floor: the transfer and the
//! clients' own indexing, which on one machine dominate the clones' times.
//!
//! Slow (minutes, about 3 GB of local disk), so it is ignored and also
//! skipped unless `ASPEN_SHALLOW_SCALE=1`; nightly runs it in release
//! (`.github/workflows/nightly.yml`, `shallow-scale`). Knobs:
//! `ASPEN_SHALLOW_SCALE_FILES` (300,000), `ASPEN_SHALLOW_SCALE_CLONES` (16),
//! `ASPEN_SHALLOW_SCALE_DIR` (the system temporary directory).

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use aspen_auth::PolicyConfig;
use aspen_pack::MaxInflatedBytes;
use aspen_server::testing::TestServer;
use aspen_server::upload_pack::exact::ShallowCacheConfig;
use aspen_testkit::NO_AUTO_MAINTENANCE;

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn git(home: &Path, dir: &Path) -> Command {
    let mut c = Command::new("git");
    c.current_dir(dir)
        .env("HOME", home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env_remove("GIT_DIR")
        .env_remove("GIT_PROTOCOL")
        .args(["-c", "protocol.version=2", "-c", "init.defaultBranch=main"])
        .args(NO_AUTO_MAINTENANCE);
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

/// File `i`'s path: seven directory levels of fanout 4 above about 18
/// files each, so the tree is eight levels deep with 21,845 directories
/// at 300,000 files.
fn path_of(i: usize) -> String {
    let mut leaf = i / 18;
    let mut p = String::new();
    for _ in 0..7 {
        p.push_str(&format!("d{}/", leaf % 4));
        leaf /= 4;
    }
    format!("{p}f{i}.txt")
}

/// Version `v` of file `i`: 1,400 pseudo-random hex digits (about 700
/// bytes compressed), so a snapshot is about 200 MB of pack, the size of
/// the monorepo benchmark's depth-1 pack.
fn content(i: usize, v: usize) -> String {
    let mut x = (i as u64) << 20 ^ v as u64 ^ 0x9e37_79b9_7f4a_7c15;
    let mut s = String::with_capacity(1_420);
    for line in 0..20 {
        for _ in 0..70 {
            x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            s.push(char::from_digit(((z ^ (z >> 31)) & 15) as u32, 16).unwrap());
        }
        let _ = line;
        s.push('\n');
    }
    s
}

/// Commit 0 writes every file, commits 1 and 2 rewrite every file (so the
/// full closure is three snapshots), then `landings` commits change 10
/// files each; the last of them also restores file 7's first version,
/// which only the oldest history holds (a revert). Returns every commit,
/// oldest first.
fn fixture(home: &Path, dir: &Path, files: usize, landings: usize) -> Vec<String> {
    fs::create_dir_all(dir).unwrap();
    run(git(home, dir).args(["init", "--quiet", "-b", "main"]));
    let mut child = git(home, dir)
        .args(["fast-import", "--quiet"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let mut w = std::io::BufWriter::with_capacity(1 << 20, child.stdin.take().unwrap());
        let mut n = 0usize;
        let mut commit = |changes: &mut dyn Iterator<Item = (usize, usize)>| {
            n += 1;
            let t = 1_700_000_000 + n * 1000;
            write!(w, "commit refs/heads/main\nmark :{n}\n").unwrap();
            write!(
                w,
                "committer A <a@example.com> {t} +0000\ndata 7\nland {n:>2}\n"
            )
            .unwrap();
            if n > 1 {
                writeln!(w, "from :{}", n - 1).unwrap();
            }
            for (i, v) in changes {
                let c = content(i, v);
                write!(w, "M 100644 inline {}\ndata {}\n{c}\n", path_of(i), c.len()).unwrap();
            }
            writeln!(w).unwrap();
        };
        for v in 0..3 {
            commit(&mut (0..files).map(|i| (i, v)));
        }
        for l in 0..landings {
            let mut changes: Vec<(usize, usize)> = (0..10)
                .map(|k| ((l * 7_919 + k * 104_729 + 11) % files, 100 + l))
                .collect();
            if l + 1 == landings {
                changes.push((7, 0));
            }
            commit(&mut changes.into_iter());
        }
        w.flush().unwrap();
    }
    assert!(child.wait().unwrap().success(), "fast-import");
    run(git(home, dir).args(["rev-list", "--reverse", "main"]))
        .lines()
        .map(str::to_owned)
        .collect()
}

fn pack_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir.join(".git/objects/pack"))
        .map(|d| {
            d.filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|x| x == "pack"))
                .map(|e| e.metadata().unwrap().len())
                .sum()
        })
        .unwrap_or(0)
}

/// One `git fetch --depth=1 <url> <tip>` into a fresh repository: its time
/// and the bytes of the pack it kept.
fn fetch(home: &Path, dir: &Path, url: &str, tip: &str) -> (Duration, u64) {
    fs::create_dir_all(dir).unwrap();
    run(git(home, dir).args(["init", "--quiet"]));
    let t = Instant::now();
    run(git(home, dir)
        .args(["-c", "fetch.unpackLimit=1", "-c", "fetch.fsckObjects=true"])
        .args(["fetch", "--quiet", "--no-tags", "--depth=1", url])
        .arg(format!("+{tip}:refs/remotes/origin/main")));
    let took = t.elapsed();
    let shallow = fs::read_to_string(dir.join(".git/shallow")).unwrap();
    assert_eq!(shallow.trim(), tip, "the boundary is the tip");
    (took, pack_bytes(dir))
}

/// `clones` concurrent depth-1 fetches of `tip`; each one's time and bytes.
/// The repositories are removed afterwards, except the first.
fn storm(
    home: &Path,
    root: &Path,
    name: &str,
    url: &str,
    tip: &str,
    clones: usize,
) -> Vec<(Duration, u64)> {
    let dirs: Vec<PathBuf> = (0..clones)
        .map(|j| root.join(format!("{name}-{j}")))
        .collect();
    let out = std::thread::scope(|scope| {
        let handles: Vec<_> = dirs
            .iter()
            .map(|d| scope.spawn(move || fetch(home, d, url, tip)))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    run(git(home, &dirs[0]).args(["fsck", "--strict", "--no-progress"]));
    for d in &dirs[1..] {
        fs::remove_dir_all(d).unwrap();
    }
    out
}

/// A field of `/proc/self/status` in bytes (Linux).
fn status(name: &str) -> u64 {
    let status = fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find_map(|l| l.strip_prefix(name))
        .and_then(|v| v.trim().trim_end_matches(" kB").trim().parse::<u64>().ok())
        .unwrap()
        * 1024
}

/// Runs `f` while sampling this process's anonymous resident memory (heap
/// and stacks, not the page cache of mapped pack files, which the kernel
/// reclaims) every 20 ms; `f`'s result and the highest sample.
fn peak_anon<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let sampler = scope.spawn(|| {
            let mut peak = status("RssAnon:");
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                peak = peak.max(status("RssAnon:"));
                std::thread::sleep(Duration::from_millis(20));
            }
            peak
        });
        let out = f();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        (out, sampler.join().unwrap())
    })
}

fn mb(b: u64) -> f64 {
    b as f64 / 1e6
}

#[test]
#[ignore = "minutes and gigabytes: nightly shallow-scale job"]
fn depth_one_clones_after_a_trunk_move_at_monorepo_scale() {
    if std::env::var("ASPEN_SHALLOW_SCALE").as_deref() != Ok("1") {
        eprintln!("skipped: set ASPEN_SHALLOW_SCALE=1 (nightly shallow-scale job)");
        return;
    }
    let files = env_or("ASPEN_SHALLOW_SCALE_FILES", 300_000usize);
    let clones = env_or("ASPEN_SHALLOW_SCALE_CLONES", 16usize);
    let tmp = match std::env::var_os("ASPEN_SHALLOW_SCALE_DIR") {
        Some(d) => tempfile::tempdir_in(d).unwrap(),
        None => tempfile::tempdir().unwrap(),
    };
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let src = tmp.path().join("src");

    let t = Instant::now();
    let commits = fixture(&home, &src, files, 6);
    eprintln!(
        "fixture: {files} files, {} commits, {:.1} s",
        commits.len(),
        t.elapsed().as_secs_f64()
    );

    // The server's defaults (`TestServer` waits the whole build wait
    // before a layered answer, for tests that compare exact objects). The
    // build wait (20 s) is the budget: what a depth-1 fetch waited before
    // it was sent the full history.
    let defaults = ShallowCacheConfig::new(tmp.path(), MaxInflatedBytes::new(1).unwrap());
    let budget = defaults.build_wait;
    let s = TestServer::start_with_policy(
        PolicyConfig {
            anonymous: vec!["write:*".parse().unwrap()],
            ..PolicyConfig::default()
        },
        |c| c.shallow_cache.as_mut().unwrap().layered_wait = defaults.layered_wait,
    );
    let url = s.url("demo");
    let _ = s.seeder("demo", "refs/heads/main", &src);
    let push = |c: &str| {
        let t = Instant::now();
        run(git(&home, &src)
            .args(["push", "--quiet", &url])
            .arg(format!("{c}:refs/heads/main")));
        t.elapsed()
    };

    // History, then the first landing, whose depth-1 pack is built once
    // from the full closure (a cold start: nothing recorded to build on).
    eprintln!("push history: {:.1} s", push(&commits[2]).as_secs_f64());
    push(&commits[3]);
    let (took, full) = fetch(&home, &tmp.path().join("cold"), &url, &commits[3]);
    eprintln!(
        "cold depth-1 fetch: {:.1} s, {:.1} MB",
        took.as_secs_f64(),
        mb(full)
    );
    let deadline = Instant::now() + Duration::from_secs(1800);
    while s.metrics().shallow_builds_recorded() == 0 {
        assert!(Instant::now() < deadline, "the cold build never finished");
        std::thread::sleep(Duration::from_millis(200));
    }
    let stock_dir = tmp.path().join("stock");
    let (_, snapshot) = fetch(
        &home,
        &stock_dir,
        &format!("file://{}", src.display()),
        &commits[3],
    );
    fs::remove_dir_all(&stock_dir).unwrap();
    let full_history = {
        let d = tmp.path().join("full");
        fs::create_dir_all(&d).unwrap();
        run(git(&home, &d).args(["init", "--quiet"]));
        run(git(&home, &d)
            .args(["-c", "fetch.unpackLimit=1", "fetch", "--quiet", "--no-tags"])
            .arg(format!("file://{}", src.display()))
            .arg(format!("+{}:refs/remotes/origin/main", commits[2])));
        let b = pack_bytes(&d);
        fs::remove_dir_all(&d).unwrap();
        b
    };
    eprintln!(
        "stock depth-1 pack {:.1} MB, full history {:.1} MB",
        mb(snapshot),
        mb(full_history)
    );

    let m = s.metrics();
    let fallbacks = m.shallow_fallbacks();
    let mut report = Vec::new();
    let last = commits.len() - 1;
    // Right after one landing, right after five more and a revert, and,
    // for comparison, once the last tip's exact pack is recorded (what
    // every clone costs at best: the transfer and the client's indexing).
    for (label, from, to) in [
        ("after one landing", 4, 4),
        ("after five landings and a revert", 5, last),
        ("recorded tip (floor)", last + 1, last),
    ] {
        if from > to {
            let deadline = Instant::now() + Duration::from_secs(600);
            while m.shallow_builds_recorded() < 3 {
                assert!(
                    Instant::now() < deadline,
                    "the last tip's build never finished"
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
        for c in commits.iter().take(to + 1).skip(from) {
            push(c);
        }
        let tip = &commits[to];
        m.take_shallow_wait_max();
        let anon0 = status("RssAnon:");
        let t = Instant::now();
        let (results, peak) = peak_anon(|| {
            storm(
                &home,
                tmp.path(),
                &format!("storm{from}"),
                &url,
                tip,
                clones,
            )
        });
        let wall = t.elapsed();
        let waited = m.take_shallow_wait_max();
        let slowest = results.iter().map(|r| r.0).max().unwrap();
        let largest = results.iter().map(|r| r.1).max().unwrap();
        let line = format!(
            "{label}: {clones} concurrent depth-1 clones in {:.1} s, slowest {:.1} s \
             (longest server wait for packs {:.2} s), largest {:.1} MB (stock {:.1} MB, \
             full history {:.1} MB), server heap +{:.0} MB at peak over {:.0} MB",
            wall.as_secs_f64(),
            slowest.as_secs_f64(),
            waited.as_secs_f64(),
            mb(largest),
            mb(snapshot),
            mb(full_history),
            mb(peak.saturating_sub(anon0)),
            mb(anon0),
        );
        eprintln!("{line}");
        report.push((line, waited, largest, peak.saturating_sub(anon0)));
    }
    eprintln!(
        "during the storms: {} fallbacks to the full closure, {} fetches served a layered \
         answer, {} objects patched in",
        m.shallow_fallbacks() - fallbacks,
        m.shallow_layered(),
        m.shallow_layered_patch_objects(),
    );
    // A build of this snapshot takes 10 to 20 s on a 4-vCPU machine with
    // local storage (more against S3), and a fetch used to wait for it, up
    // to the whole build wait. A fetch now waits at most `layered_wait`
    // (500 ms) plus its layered answer's check: a quarter of the build
    // wait leaves room for a slow runner and none for a build.
    let wait_budget = budget / 4;
    for (line, waited, largest, grew) in &report {
        assert!(*largest < full_history / 2, "sent the full history: {line}");
        assert!(
            *waited < wait_budget,
            "over the {wait_budget:?} wait budget: {line}"
        );
        assert!(*grew < 1 << 30, "memory not bounded: {line}");
    }
    assert_eq!(m.shallow_fallbacks(), fallbacks, "no full-closure fallback");
    assert!(
        m.shallow_layered() > 0,
        "the trunk moves were served layered answers"
    );
    assert!(
        m.shallow_layered_patch_objects() > 0,
        "the revert was patched in"
    );
}

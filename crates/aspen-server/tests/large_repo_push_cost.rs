//! What one push costs must not grow with the size of the repository
//! (throughput work item 2): a push that changes one file 8 directories deep
//! in a repository of hundreds of thousands of files must cost about what
//! the same push costs in a git.git-sized one (about 4,600 files).
//!
//! Two things used to grow with the repository:
//!
//! - **Whole `.idx` reads.** Every lookup in a stored pack went through its
//!   whole `.idx`, read from the blob store and kept in a 256 MiB cache. A
//!   repository whose `.idx` files do not fit (about 7 million objects, a
//!   Linux-sized one) read them whole on every push. Large `.idx` files are
//!   now sampled (`aspen_pack::sampled`): a lookup reads a few kilobytes.
//! - **Lookups per push.** Connectivity looked up every entry of every tree
//!   the push touched, and the "already held" test every object of the
//!   pack. The walk of the changed paths (`aspen_pack::changed`) diffs each
//!   pushed tree against its parent's, kept in memory for trunk's newest
//!   tips, and looks up only what changed.
//!
//! The test runs both repositories against a server whose `.idx` cache is
//! scaled down with them (1 MiB, with `.idx` files over 256 KiB sampled), so
//! the large repository's `.idx` outgrows the cache the way a Linux-sized
//! one outgrows the default 256 MiB, while the small one's fits. It asserts
//! the large repository's per-push CPU time and objects touched in stored
//! packs stay within 1.5 times the small one's. Required CI runs a
//! 65,610-file repository; the nightly suite runs the ignored 301,806-file
//! one.

mod receive_support;

use std::io::Write as _;
use std::process::Stdio;
use std::time::Instant;

use receive_support::*;

/// A synthetic repository: `fanout` subdirectories per directory for
/// `depth` levels, and `files` files in every directory at the bottom.
#[derive(Debug, Clone, Copy)]
struct Shape {
    fanout: usize,
    depth: u32,
    files: usize,
}

impl Shape {
    fn leaves(self) -> usize {
        self.fanout.pow(self.depth)
    }

    fn file_count(self) -> usize {
        self.leaves() * self.files
    }

    /// The directory path of leaf `n`.
    fn leaf_dir(self, mut n: usize) -> String {
        let mut parts = Vec::with_capacity(self.depth as usize);
        for level in 0..self.depth {
            parts.push(format!("d{level}x{}", n % self.fanout));
            n /= self.fanout;
        }
        parts.reverse();
        parts.join("/")
    }
}

/// The git.git-sized repository: 4,608 files, 8 levels deep.
const SMALL: Shape = Shape {
    fanout: 2,
    depth: 8,
    files: 18,
};

/// Required CI's large repository: 65,610 files, 8 levels deep.
const LARGE: Shape = Shape {
    fanout: 3,
    depth: 8,
    files: 10,
};

/// The nightly suite's: 301,806 files, 8 levels deep.
const HUGE: Shape = Shape {
    fanout: 3,
    depth: 8,
    files: 46,
};

/// Pushes measured per repository, after one warm-up push.
const PUSHES: usize = 24;

/// CPU time this process has used, in microseconds: the server runs in it,
/// `git` runs in children, which are not counted. Linux only (`/proc`);
/// elsewhere 0, and the CPU bound is not checked.
fn cpu_micros() -> u64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    // Fields after the command name, which is in parentheses.
    let rest = stat.rsplit_once(") ").map(|(_, r)| r).unwrap_or("");
    let f: Vec<&str> = rest.split_whitespace().collect();
    let ticks: u64 = f.get(11).and_then(|u| u.parse().ok()).unwrap_or(0)
        + f.get(12).and_then(|s| s.parse().ok()).unwrap_or(0);
    // USER_HZ is 100 on every Linux the tests run on.
    ticks * 10_000
}

/// Feeds `stream` to `git fast-import` in `w`.
fn fast_import(w: &Work, stream: Vec<u8>) {
    let mut child = w
        .command()
        .args(["fast-import", "--quiet", "--done"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(&stream).unwrap();
    stdin.write_all(b"done\n").unwrap();
    drop(stdin);
    assert!(child.wait().unwrap().success(), "fast-import failed");
}

/// The initial commit of `shape` on `main`, without a work tree.
fn import_initial(w: &Work, shape: Shape) {
    let mut s = Vec::with_capacity(shape.file_count() * 80);
    s.extend_from_slice(
        b"commit refs/heads/main\ncommitter A <a@a> 1700000000 +0000\ndata 4\ninit\n",
    );
    for leaf in 0..shape.leaves() {
        let dir = shape.leaf_dir(leaf);
        for f in 0..shape.files {
            let body = format!("{dir}/f{f}\n");
            write!(
                s,
                "M 100644 inline {dir}/f{f}.txt\ndata {}\n{body}",
                body.len()
            )
            .unwrap();
        }
    }
    s.push(b'\n');
    fast_import(w, s);
}

/// One commit on `main` that changes one file in leaf directory `leaf`.
fn import_change(w: &Work, shape: Shape, leaf: usize, round: usize) {
    let dir = shape.leaf_dir(leaf);
    // Long enough that git sends it as a delta against the old version.
    let body = format!("{dir}/f0\n{}round {round}\n", "unchanged line\n".repeat(20));
    let mut s = Vec::new();
    write!(
        s,
        "commit refs/heads/main\ncommitter A <a@a> {} +0000\ndata 6\nchange\nfrom refs/heads/main^0\n\
         M 100644 inline {dir}/f0.txt\ndata {}\n{body}\n",
        1_700_000_000 + round + 1,
        body.len()
    )
    .unwrap();
    fast_import(w, s);
}

async fn push(w: &Work, args: &[String]) -> (bool, String) {
    let mut cmd = w.command();
    cmd.args(args);
    blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await
}

/// Per-push averages over the measured pushes.
#[derive(Debug, Clone, Copy)]
struct Cost {
    /// Server CPU time.
    cpu_us: u64,
    /// Objects touched in stored packs: IDs looked up plus entries read.
    touched: u64,
    /// Bytes of `.idx` read from the blob store.
    idx_bytes: u64,
    /// IDs the push path looked up for connectivity and held objects.
    located: u64,
    /// Tree entries the walk of the changed paths compared.
    entries: u64,
}

async fn measure(shape: Shape) -> Cost {
    let s = Server::start_with(
        |c| {
            // Scaled down with the test repositories (module docs).
            c.locate.idx_cache_bytes = 1 << 20;
            c.locate.max_whole_idx_bytes = 256 << 10;
        },
        aspen_core::repo::ForcePolicy::Branches,
    )
    .await;
    let w = s.clone_dir("w");
    let started = Instant::now();
    import_initial(&w, shape);
    let generated = started.elapsed();
    let started = Instant::now();
    // No deltas: the initial push only has to be fast to make.
    let (ok, out) = push(
        &w,
        &[
            "-c".into(),
            "pack.window=0".into(),
            "push".into(),
            "--porcelain".into(),
            s.url(),
            "main".into(),
        ],
    )
    .await;
    assert!(ok, "initial push: {out}");
    eprintln!(
        "{} files: generated in {generated:?}, initial push in {:?}",
        shape.file_count(),
        started.elapsed()
    );
    let mut lcg: u64 = 0x2545_f491_4f6c_dd1d;
    let args = ["push".into(), "--porcelain".into(), s.url(), "main".into()];
    let mut before = None;
    for round in 0..=PUSHES {
        if round == 1 {
            // Round 0 warms the caches (a server's first push reads every
            // search pack's `.idx` once, whatever its size).
            before = Some((cpu_micros(), s.svc.timings(), s.svc.locate_stats()));
        }
        lcg = lcg
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let leaf = (lcg >> 33) as usize % shape.leaves();
        import_change(&w, shape, leaf, round);
        let (ok, out) = push(&w, &args).await;
        assert!(ok, "push {round}: {out}");
    }
    let (cpu0, t0, l0) = before.expect("measured");
    let (cpu1, t1, l1) = (cpu_micros(), s.svc.timings(), s.svc.locate_stats());
    let n = PUSHES as u64;
    let touched = (l1.ids_looked_up - l0.ids_looked_up) + (l1.entries_read - l0.entries_read);
    let cost = Cost {
        cpu_us: (cpu1 - cpu0) / n,
        touched: touched / n,
        idx_bytes: (l1.idx_bytes_read - l0.idx_bytes_read) / n,
        located: (t1.located - t0.located) / n,
        entries: (t1.changed_entries - t0.changed_entries) / n,
    };
    eprintln!("{} files, per push: {cost:?}", shape.file_count());
    // The last push stored a correct, connected version.
    s.check_ref("refs/heads/main", &w).await;
    cost
}

async fn compare(large: Shape) {
    let small = measure(SMALL).await;
    let big = measure(large).await;
    eprintln!(
        "{} vs {} files: cpu {:.2}x, objects touched {:.2}x, .idx bytes read per push {} vs {}, \
         IDs located per push {} vs {}, tree entries compared per push {} vs {}",
        large.file_count(),
        SMALL.file_count(),
        big.cpu_us as f64 / small.cpu_us.max(1) as f64,
        big.touched as f64 / small.touched.max(1) as f64,
        big.idx_bytes,
        small.idx_bytes,
        big.located,
        small.located,
        big.entries,
        small.entries,
    );
    assert!(
        big.touched * 2 <= small.touched * 3,
        "objects touched in stored packs per push: {} for {} files against {} for {}",
        big.touched,
        large.file_count(),
        small.touched,
        SMALL.file_count(),
    );
    // A lookup in a sampled `.idx` reads one stretch of 256 IDs (5 KiB)
    // and an offset; a whole read of the large repository's `.idx`, which
    // every push did before, is over 2 MiB.
    assert!(
        big.idx_bytes < 256 << 10,
        "{} bytes of .idx read per push",
        big.idx_bytes
    );
    // CPU time is measured in 10 ms ticks over all measured pushes; allow
    // one tick of slack on top of 1.5 times.
    if small.cpu_us > 0 {
        assert!(
            big.cpu_us * 2 <= small.cpu_us * 3 + 2 * 10_000 / PUSHES as u64,
            "CPU per push: {} us for {} files against {} us for {}",
            big.cpu_us,
            large.file_count(),
            small.cpu_us,
            SMALL.file_count(),
        );
    }
}

/// Invariant: per-push work in a 65,610-file repository stays within 1.5
/// times that of a 4,608-file one (module docs).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_push_work_does_not_grow_with_the_repository() {
    compare(LARGE).await;
}

/// The same with a 301,806-file repository (nightly).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow: about a minute; run by the nightly suite"]
async fn per_push_work_does_not_grow_with_a_300k_file_repository() {
    compare(HUGE).await;
}

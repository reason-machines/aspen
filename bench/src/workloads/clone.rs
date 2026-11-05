//! Clone throughput: C concurrent clients repeatedly clone the repository.
//!
//! Each clone is `git clone --bare --no-tags`: it measures transfer and
//! `index-pack`, not checking out a work tree, which is client-side disk work
//! that says nothing about the server. Measured: `clone` latency, clones per
//! second, and received MiB per second (pack bytes stored by the clients
//! over the measured wall time).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::git;
use crate::results::{Errors, RunResult, SCHEMA, now_unix_ms};
use crate::stats::Summary;
use crate::target::Setup;

#[derive(Debug, Clone)]
pub struct CloneOpts {
    pub concurrency: u32,
    /// Total clones across all clients.
    pub clones: u32,
}

impl CloneOpts {
    pub fn params(&self) -> BTreeMap<String, Value> {
        let mut m = BTreeMap::new();
        m.insert("concurrency".to_owned(), self.concurrency.into());
        m.insert("clones".to_owned(), self.clones.into());
        m
    }
}

#[derive(Debug, Default)]
struct ClientOut {
    took: Vec<Duration>,
    bytes: u64,
    errors: Errors,
}

pub fn run(
    setup: &Setup,
    opts: &CloneOpts,
    mut params: BTreeMap<String, Value>,
) -> Result<RunResult, String> {
    if opts.concurrency == 0 || opts.clones == 0 {
        return Err("--concurrency and --clones must be at least 1".to_owned());
    }
    params.extend(opts.params());
    let started_at_unix_ms = now_unix_ms();
    let next = AtomicU32::new(0);
    let barrier = Arc::new(Barrier::new(opts.concurrency as usize + 1));

    let (outs, wall) = thread::scope(|s| {
        let handles: Vec<_> = (0..opts.concurrency)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let next = &next;
                s.spawn(move || client(setup, opts.clones, next, &barrier))
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        let outs: Vec<ClientOut> = handles
            .into_iter()
            .map(|h| h.join().unwrap_or_default())
            .collect();
        (outs, start.elapsed())
    });

    let mut took = Vec::new();
    let mut bytes = 0;
    let mut errors = Errors::default();
    for o in outs {
        took.extend(o.took);
        bytes += o.bytes;
        errors.merge(o.errors);
    }
    let wall_seconds = wall.as_secs_f64();
    let secs = wall_seconds.max(f64::EPSILON);
    let ok = took.len() as u64;
    let mut latencies_ms = BTreeMap::new();
    latencies_ms.insert("clone".to_owned(), Summary::from_durations(&took));
    let mut throughput = BTreeMap::new();
    throughput.insert("clones_per_second".to_owned(), ok as f64 / secs);
    throughput.insert(
        "received_mib_per_second".to_owned(),
        bytes as f64 / (1024.0 * 1024.0) / secs,
    );
    let mut counts = BTreeMap::new();
    counts.insert("clones_ok".to_owned(), ok);
    counts.insert("received_bytes".to_owned(), bytes);
    Ok(RunResult {
        schema: SCHEMA.to_owned(),
        workload: "clone".to_owned(),
        target: setup.info.clone(),
        params,
        environment: setup.environment.clone(),
        started_at_unix_ms,
        wall_seconds,
        latencies_ms,
        throughput,
        counts,
        errors,
    })
}

fn client(setup: &Setup, total: u32, next: &AtomicU32, barrier: &Barrier) -> ClientOut {
    let mut out = ClientOut::default();
    barrier.wait();
    loop {
        let n = next.fetch_add(1, Ordering::SeqCst);
        if n >= total {
            break;
        }
        let dest = setup.path(&format!("clone{n}.git"));
        let (took, res) = git::run_timed(
            setup
                .git
                .cmd(&setup.dir)
                .args(["clone", "--quiet", "--bare", "--no-tags", &setup.remote.url])
                .arg(&dest),
        );
        let verified =
            res.and_then(|_| git::run(setup.git.cmd(&dest).args(["rev-parse", "refs/heads/main"])));
        match verified {
            Ok(tip) if tip == setup.main => {
                out.took.push(took);
                out.bytes += git::pack_bytes(&dest);
            }
            Ok(tip) => out
                .errors
                .push(format!("clone {n}: main is {tip}, expected {}", setup.main)),
            Err(e) => out.errors.push(format!("clone {n}: {e}")),
        }
        let _ = std::fs::remove_dir_all(&dest);
    }
    out
}

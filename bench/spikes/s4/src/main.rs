//! Spike S4: measure streaming `gix-pack` ingest of a multi-GB pack.
//!
//! Three subcommands, each a stage that Aspen's push path could run. Every
//! subcommand prints one JSON object on stdout with timings and memory peaks.
//!
//! * `stream <pack|->`: pass 1 only. Parse the pack as a stream
//!   (`BytesToEntriesIter`): inflate every entry to find its end, CRC32 it,
//!   and verify the trailing SHA-1 over the whole pack (with SHA-1 collision
//!   detection). Every byte read is also copied to `--spool <file>`, standing
//!   in for the blob-store upload. Bounded memory, no random access.
//! * `index <pack|-> --out-dir <dir>`: the full `index-pack` equivalent,
//!   `gix_pack::Bundle::write_to_directory`. Pass 1 as above while building a
//!   delta tree, then resolve every delta over an mmap of the spooled pack to
//!   compute each object ID, and write a v2 `.idx`.
//! * `commits --pack <p> --idx <i>`: a second traversal over an indexed pack
//!   that extracts each commit's ID, tree and parents (what `commit_index`
//!   needs). gix-pack 0.77 does not expose a hook to do this inside `index`.
//! * `thin <pack|-> --base-idx <idx> --out-dir <dir>`: `index`, plus thin-pack
//!   completion. Each missing delta base is looked up in the pack beside
//!   `--base-idx` (standing in for the trunk version's covering packs), and
//!   gix inserts it before its first delta. Because that changes the header
//!   count and trailer, gix then rewrites the header and re-reads the whole
//!   spooled pack to recompute its SHA-1 before resolving deltas.
//!   `--dedupe` applies Aspen's duplicate-object rule (docs/PLAN.md, push
//!   step 2): scan the sorted `.idx` for an ID that appears twice and, if
//!   one does, run completion again with a lookup that refuses those IDs, so
//!   their deltas resolve against the copy the client sent.
//! * `rehash <file> [--copy-to <file>]`: one sequential SHA-1 (with collision
//!   detection) over a file, optionally copying it: the cost of push step 2's
//!   "pass 2" (hash and upload) on its own.
//!
//! Memory is sampled from `/proc/self/status` every 10 ms. `RssAnon` is heap
//! (what an operator must provision); `RssFile` is page cache mapped into the
//! process (the pack mmap), which the kernel can reclaim under pressure.
//! `RssShmem` is non-zero only when the spool sits on tmpfs, which is RAM:
//! never spool a large pack there. Linux only.

use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use gix_pack::data::input;
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const USAGE: &str = "usage:
  aspen-spike-s4 stream  <pack|-> [--spool <file>]
  aspen-spike-s4 index   <pack|-> --out-dir <dir> [--threads N]
  aspen-spike-s4 thin    <pack|-> --base-idx <idx> --out-dir <dir> [--threads N] [--dedupe]
  aspen-spike-s4 commits --pack <file> --idx <file> [--threads N] [--out <tsv>]
  aspen-spike-s4 rehash  <file> [--copy-to <file>]";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let sampler = MemSampler::start();
    let started = Instant::now();
    let result = match args.first().map(String::as_str) {
        Some("stream") => stream(&args[1..]),
        Some("index") => index(&args[1..], false),
        Some("thin") => index(&args[1..], true),
        Some("rehash") => rehash(&args[1..]),
        Some("commits") => commits(&args[1..]),
        _ => Err(USAGE.into()),
    };
    let wall = started.elapsed();
    let mem = sampler.stop();
    match result {
        Ok(mut out) => {
            out["wall_s"] = json!(round2(wall.as_secs_f64()));
            out["memory"] = mem;
            out["cpu"] = cpu_times();
            println!(
                "{}",
                serde_json::to_string_pretty(&out).expect("serializable")
            );
        }
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(1);
        }
    }
}

/// Opens `-` as stdin, anything else as a file. Pipes prove the code path
/// never seeks the input.
fn open_input(path: &str) -> Result<Box<dyn Read + Send>> {
    Ok(if path == "-" {
        Box::new(io::stdin())
    } else {
        Box::new(File::open(path)?)
    })
}

fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn threads(args: &[String]) -> Result<Option<usize>> {
    Ok(match flag(args, "--threads") {
        Some(n) => Some(n.parse()?),
        None => None,
    })
}

/// Copies every byte read to `tee` (the stand-in for the blob upload), counts
/// them, and remembers when the last byte arrived: in `index`, that instant
/// is the end of pass 1, because pass 2 works only on the spooled copy.
struct Tee<R> {
    inner: R,
    tee: Option<BufWriter<File>>,
    bytes: Arc<AtomicU64>,
    started: Instant,
    last_byte_at_us: Arc<AtomicU64>,
}

impl<R> Tee<R> {
    fn new(inner: R, tee: Option<File>) -> Self {
        Tee {
            inner,
            tee: tee.map(|f| BufWriter::with_capacity(1 << 20, f)),
            bytes: Arc::default(),
            started: Instant::now(),
            last_byte_at_us: Arc::default(),
        }
    }
}

impl<R: Read> Read for Tee<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if let Some(tee) = &mut self.tee {
            tee.write_all(&buf[..n])?;
        }
        if n > 0 {
            self.bytes.fetch_add(n as u64, Ordering::Relaxed);
            let us = self.started.elapsed().as_micros() as u64;
            self.last_byte_at_us.store(us, Ordering::Relaxed);
        }
        Ok(n)
    }
}

fn stream(args: &[String]) -> Result<Value> {
    let input = args.first().ok_or(USAGE)?;
    let spool = flag(args, "--spool").map(File::create).transpose()?;
    let tee = Tee::new(open_input(input)?, spool);
    let bytes = tee.bytes.clone();
    let reader = BufReader::with_capacity(64 * 1024, tee);
    let iter = input::BytesToEntriesIter::new_from_header(
        reader,
        input::Mode::Verify,
        input::EntryDataMode::Crc32,
        gix_hash::Kind::Sha1,
    )?;
    let declared = iter.size_hint().0;

    let mut counts = [0u64; 6]; // commit, tree, blob, tag, ofs-delta, ref-delta
    let mut inflated: u64 = 0;
    let mut trailer = None;
    let mut max_inflated: u64 = 0;
    for entry in iter {
        let entry = entry?;
        use gix_pack::data::entry::Header::*;
        let slot = match entry.header {
            Commit => 0,
            Tree => 1,
            Blob => 2,
            Tag => 3,
            OfsDelta { .. } => 4,
            RefDelta { .. } => 5,
        };
        counts[slot] += 1;
        inflated += entry.decompressed_size;
        max_inflated = max_inflated.max(entry.decompressed_size);
        trailer = entry.trailer.or(trailer);
    }
    let total = bytes.load(Ordering::Relaxed);
    Ok(json!({
        "stage": "stream",
        "input": input,
        "pack_bytes": total,
        "objects": declared,
        "entries": {
            "commit": counts[0], "tree": counts[1], "blob": counts[2], "tag": counts[3],
            "ofs_delta": counts[4], "ref_delta": counts[5],
        },
        "inflated_entry_bytes": inflated,
        "largest_entry_inflated_bytes": max_inflated,
        "trailer_verified": trailer.map(|t| t.to_string()),
    }))
}

fn index(args: &[String], thin: bool) -> Result<Value> {
    let input = args.first().ok_or(USAGE)?;
    let out_dir = PathBuf::from(flag(args, "--out-dir").ok_or(USAGE)?);
    std::fs::create_dir_all(&out_dir)?;
    let tee = Tee::new(open_input(input)?, None);
    let (bytes, last_byte_at_us) = (tee.bytes.clone(), tee.last_byte_at_us.clone());
    let started = tee.started;
    let options = gix_pack::bundle::write::Options {
        thread_limit: threads(args)?,
        ..Default::default()
    };
    let bases = match (thin, flag(args, "--base-idx")) {
        (true, Some(idx)) => Some(BaseStore::open(idx)?),
        (true, None) => return Err(USAGE.into()),
        (false, _) => None,
    };
    let dedupe = args.iter().any(|a| a == "--dedupe");
    if dedupe && (input == "-" || bases.is_none()) {
        return Err("--dedupe needs `thin` and a pack file, not stdin".into());
    }
    let mut outcome = complete(tee, &out_dir, bases.as_ref(), options.clone())?;
    let mut dedupe_report = Value::Null;
    if dedupe {
        let first = std::time::Instant::now();
        let dups = duplicate_ids(&outcome.index_path)?;
        let scan_s = first.elapsed().as_secs_f64();
        let mut rerun_s = 0.0;
        if !dups.is_empty() {
            for path in [&outcome.index_path, &outcome.data_path]
                .into_iter()
                .flatten()
            {
                std::fs::remove_file(path)?;
            }
            let bases = bases.as_ref().expect("checked above");
            bases.refuse.borrow_mut().extend(dups.iter().cloned());
            let rerun = std::time::Instant::now();
            outcome = complete(
                Tee::new(open_input(input)?, None),
                &out_dir,
                Some(bases),
                options,
            )?;
            rerun_s = rerun.elapsed().as_secs_f64();
            let left = duplicate_ids(&outcome.index_path)?;
            if !left.is_empty() {
                return Err(format!("{} duplicate IDs remain after the rerun", left.len()).into());
            }
        }
        dedupe_report = json!({
            "duplicates_first_pass": dups.len(),
            "scan_s": round2(scan_s),
            "rerun_s": round2(rerun_s),
        });
    }
    let total_s = started.elapsed().as_secs_f64();
    let pass1_s = last_byte_at_us.load(Ordering::Relaxed) as f64 / 1e6;
    let pack_bytes_out = match &outcome.data_path {
        Some(path) => std::fs::metadata(path)?.len(),
        None => 0,
    };
    Ok(json!({
        "stage": if thin { "thin" } else { "index" },
        "pack_bytes_out": pack_bytes_out,
        "base_lookups": bases.as_ref().map(BaseStore::stats),
        "dedupe": dedupe_report,
        "pass1_stream_s": round2(pass1_s),
        "pass2_resolve_and_write_s": round2(total_s - pass1_s),
        "input": input,
        "threads": threads(args)?,
        "pack_bytes": bytes.load(Ordering::Relaxed),
        "objects": outcome.index.num_objects,
        "pack_hash": outcome.index.data_hash.to_string(),
        "index_hash": outcome.index.index_hash.to_string(),
        "index_path": outcome.index_path,
    }))
}

/// Runs `index-pack` (and, with `bases`, thin-pack completion) on `input`.
fn complete<R: Read>(
    input: R,
    out_dir: &std::path::Path,
    bases: Option<&BaseStore>,
    options: gix_pack::bundle::write::Options,
) -> Result<gix_pack::bundle::write::Outcome> {
    let mut reader = BufReader::with_capacity(64 * 1024, input);
    let interrupt = AtomicBool::new(false);
    let outcome = gix_pack::Bundle::write_to_directory(
        &mut reader as &mut dyn BufRead,
        Some(out_dir),
        &mut gix_utils::progress::Discard,
        &interrupt,
        bases,
        gix_hash::Kind::Sha1,
        options,
    )?;
    if let Some(keep) = &outcome.keep_path {
        std::fs::remove_file(keep)?;
    }
    Ok(outcome)
}

/// Object IDs that appear more than once in a `.idx`. The index is sorted by
/// ID, so this is one linear scan comparing neighbours.
fn duplicate_ids(idx: &Option<PathBuf>) -> Result<Vec<gix_hash::ObjectId>> {
    let Some(idx) = idx else {
        return Ok(Vec::new());
    };
    let index = gix_pack::index::File::at(idx, gix_hash::Kind::Sha1)?;
    let mut dups = Vec::new();
    let mut prev: Option<gix_hash::ObjectId> = None;
    for entry in index.iter() {
        if prev.as_ref() == Some(&entry.oid) && dups.last() != Some(&entry.oid) {
            dups.push(entry.oid);
        }
        prev = Some(entry.oid);
    }
    Ok(dups)
}

/// Looks up thin-pack delta bases in an existing pack, counting lookups and
/// the time they take. In Aspen this probes the `.idx` of the trunk version's
/// covering packs and reads the object from the blob store; here the pack is
/// local, so the time is a lower bound (no network round trips).
struct BaseStore {
    bundle: gix_pack::Bundle,
    inflate: std::cell::RefCell<gix_zlib::Inflate>,
    found: std::cell::Cell<u64>,
    missing: std::cell::Cell<u64>,
    bytes: std::cell::Cell<u64>,
    nanos: std::cell::Cell<u64>,
    /// Pack entries read per base (1 + its delta chain length). Each one is a
    /// separate range read when the pack lives in a blob store.
    chain_reads: std::cell::RefCell<Vec<u32>>,
    /// IDs the lookup answers "not found" for (`--dedupe`): the pack itself
    /// holds them.
    refuse: std::cell::RefCell<std::collections::HashSet<gix_hash::ObjectId>>,
    refused: std::cell::Cell<u64>,
}

impl BaseStore {
    fn open(idx: &str) -> Result<Self> {
        Ok(BaseStore {
            bundle: gix_pack::Bundle::at(idx, gix_hash::Kind::Sha1)?,
            inflate: Default::default(),
            found: Default::default(),
            missing: Default::default(),
            bytes: Default::default(),
            nanos: Default::default(),
            chain_reads: Default::default(),
            refuse: Default::default(),
            refused: Default::default(),
        })
    }

    /// Counts the pack entries a lookup of `id` reads: the object itself
    /// plus every delta base down to a full object.
    fn entries_read(&self, id: &gix_hash::oid) -> gix_error::Result<u32> {
        use gix_pack::data::entry::Header;
        let mut next = self
            .bundle
            .index
            .lookup(id)
            .map(|i| self.bundle.index.pack_offset_at_index(i));
        let mut reads = 0;
        while let Some(offset) = next {
            reads += 1;
            let entry = self.bundle.pack.entry(offset)?;
            next = match entry.header {
                Header::OfsDelta { base_distance } => Some(entry.base_pack_offset(base_distance)),
                Header::RefDelta { base_id } => self
                    .bundle
                    .index
                    .lookup(base_id)
                    .map(|i| self.bundle.index.pack_offset_at_index(i)),
                _ => None,
            };
        }
        Ok(reads)
    }

    fn stats(&self) -> Value {
        let mut reads = self.chain_reads.borrow().clone();
        reads.sort_unstable();
        let pct = |p: f64| {
            reads
                .get(((reads.len() as f64 - 1.0) * p).round() as usize)
                .copied()
                .unwrap_or(0)
        };
        json!({
            "found": self.found.get(),
            "not_found": self.missing.get(),
            "refused": self.refused.get(),
            "decoded_bytes": self.bytes.get(),
            "total_s": round2(self.nanos.get() as f64 / 1e9),
            "entries_read_total": reads.iter().map(|&r| u64::from(r)).sum::<u64>(),
            "entries_read_per_base": { "p50": pct(0.5), "p90": pct(0.9), "p99": pct(0.99), "max": pct(1.0) },
        })
    }
}

impl gix_object::Find for &BaseStore {
    fn try_find<'a>(
        &self,
        id: &gix_hash::oid,
        buffer: &'a mut Vec<u8>,
    ) -> gix_error::Result<Option<gix_object::Data<'a>>> {
        if self.refuse.borrow().contains(id) {
            self.refused.set(self.refused.get() + 1);
            return Ok(None);
        }
        let started = Instant::now();
        let found = self.bundle.find(
            id,
            buffer,
            &mut self.inflate.borrow_mut(),
            &mut gix_pack::cache::Never,
        )?;
        self.nanos
            .set(self.nanos.get() + started.elapsed().as_nanos() as u64);
        Ok(match found {
            Some((data, _location)) => {
                // Not timed: this re-walks the chain only to count reads.
                self.chain_reads.borrow_mut().push(self.entries_read(id)?);
                self.found.set(self.found.get() + 1);
                self.bytes.set(self.bytes.get() + data.data.len() as u64);
                Some(data)
            }
            None => {
                self.missing.set(self.missing.get() + 1);
                None
            }
        })
    }
}

fn rehash(args: &[String]) -> Result<Value> {
    let path = args.first().ok_or(USAGE)?;
    let mut input = File::open(path)?;
    let len = input.metadata()?.len();
    let mut copy = flag(args, "--copy-to")
        .map(File::create)
        .transpose()?
        .map(|f| BufWriter::with_capacity(1 << 20, f));
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        if let Some(copy) = &mut copy {
            copy.write_all(&buf[..n])?;
        }
    }
    if let Some(copy) = &mut copy {
        copy.flush()?;
    }
    Ok(json!({
        "stage": "rehash",
        "bytes": len,
        "copied": copy.is_some(),
        "sha1": hasher.try_finalize()?.to_string(),
    }))
}

fn commits(args: &[String]) -> Result<Value> {
    let pack_path = flag(args, "--pack").ok_or(USAGE)?;
    let idx_path = flag(args, "--idx").ok_or(USAGE)?;
    let index = gix_pack::index::File::at(idx_path, gix_hash::Kind::Sha1)?;
    let pack = gix_pack::data::File::at(pack_path, gix_hash::Kind::Sha1)?;
    let found = Arc::new(collect::Commits::default());
    let processor = {
        let found = found.clone();
        move |kind: gix_object::Kind,
              data: &[u8],
              entry: &gix_pack::index::Entry,
              _: &dyn gix_utils::progress::Progress|
              -> gix_error::Result {
            if kind == gix_object::Kind::Commit {
                found.record(entry.oid, data)?;
            }
            Ok(())
        }
    };
    let interrupt = AtomicBool::new(false);
    let outcome = index.traverse(
        &pack,
        &mut gix_utils::progress::Discard,
        &interrupt,
        processor,
        gix_pack::index::traverse::Options {
            traversal: gix_pack::index::traverse::Algorithm::DeltaTreeLookup,
            thread_limit: threads(args)?,
            // Object IDs were verified by `index`; re-hashing would double the work.
            check: gix_pack::index::traverse::SafetyCheck::SkipFileAndObjectChecksumVerification,
            alloc_limit_bytes: None,
            make_pack_lookup_cache: || gix_pack::cache::Never,
        },
    )?;
    if let Some(out) = flag(args, "--out") {
        let mut w = BufWriter::new(File::create(out)?);
        for line in found.lines() {
            writeln!(w, "{line}")?;
        }
        w.flush()?;
    }
    Ok(json!({
        "stage": "commits",
        "pack": pack_path,
        "threads": threads(args)?,
        "objects": index.num_objects(),
        "commits": found.len(),
        "index_checksum": outcome.actual_index_checksum.to_string(),
    }))
}

/// A tiny lock-protected collector, kept free of extra dependencies.
mod collect {
    use std::sync::Mutex;

    /// Compact binary records: `oid(20) tree(20) n_parents(1) parent(20)*n`,
    /// roughly what a `commit_index` row costs before it reaches the database.
    #[derive(Default)]
    pub(crate) struct Commits(Mutex<(Vec<u8>, usize)>);

    impl Commits {
        pub(crate) fn record(&self, oid: gix_hash::ObjectId, data: &[u8]) -> gix_error::Result {
            use gix_object::commit::ref_iter::Token;
            let mut rec = Vec::with_capacity(81);
            rec.extend_from_slice(oid.as_slice());
            let mut parents = 0u8;
            for token in gix_object::CommitRefIter::from_bytes(data, oid.kind()) {
                match token? {
                    Token::Tree { id } => {
                        rec.extend_from_slice(id.as_slice());
                    }
                    Token::Parent { id } => {
                        rec.extend_from_slice(id.as_slice());
                        parents = parents.saturating_add(1);
                    }
                    _ => break,
                }
            }
            // The parent count goes between the tree and the parents.
            rec.insert(40, parents);
            let mut guard = self.0.lock().expect("not poisoned");
            guard.0.extend_from_slice(&rec);
            guard.1 += 1;
            Ok(())
        }

        pub(crate) fn len(&self) -> usize {
            self.0.lock().expect("not poisoned").1
        }

        /// Formats every record as `oid tree parent...`, sorted.
        pub(crate) fn lines(&self) -> Vec<String> {
            let guard = self.0.lock().expect("not poisoned");
            let hex = |b: &[u8]| gix_hash::ObjectId::from_bytes_or_panic(b).to_string();
            let (mut out, mut buf) = (Vec::with_capacity(guard.1), &guard.0[..]);
            while !buf.is_empty() {
                let n = buf[40] as usize;
                let mut line = format!("{} {}", hex(&buf[..20]), hex(&buf[20..40]));
                for p in 0..n {
                    line.push(' ');
                    line.push_str(&hex(&buf[41 + 20 * p..61 + 20 * p]));
                }
                out.push(line);
                buf = &buf[41 + 20 * n..];
            }
            out.sort_unstable();
            out
        }
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

fn cpu_times() -> Value {
    // utime and stime are fields 14 and 15 of /proc/self/stat, in clock ticks.
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let after_comm = stat.rsplit(')').next().unwrap_or("");
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    let tick = 100.0; // CLK_TCK is 100 on Linux x86_64 and aarch64.
    let get = |i: usize| {
        fields
            .get(i)
            .and_then(|f| f.parse::<f64>().ok())
            .unwrap_or(0.0)
            / tick
    };
    json!({ "user_s": round2(get(11)), "sys_s": round2(get(12)) })
}

/// Samples peak memory from /proc/self/status on a background thread.
struct MemSampler {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<[u64; 4]>,
}

fn status_kib(status: &str, key: &str) -> u64 {
    status
        .lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

impl MemSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = stop.clone();
            thread::spawn(move || {
                const KEYS: [&str; 4] = ["RssAnon:", "RssFile:", "RssShmem:", "VmRSS:"];
                let mut peaks = [0u64; 4];
                loop {
                    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
                        for (peak, key) in peaks.iter_mut().zip(KEYS) {
                            *peak = (*peak).max(status_kib(&s, key));
                        }
                    }
                    if stop.load(Ordering::Relaxed) {
                        return peaks;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            })
        };
        MemSampler { stop, handle }
    }

    fn stop(self) -> Value {
        self.stop.store(true, Ordering::Relaxed);
        let [anon, file, shmem, total] = self.handle.join().unwrap_or_default();
        let hwm = std::fs::read_to_string("/proc/self/status")
            .map(|s| status_kib(&s, "VmHWM:"))
            .unwrap_or(0);
        let mib = |k: u64| round2(k as f64 / 1024.0);
        json!({
            "peak_rss_anon_mib": mib(anon),
            "peak_rss_file_mib": mib(file),
            "peak_rss_shmem_mib": mib(shmem),
            "peak_rss_mib": mib(total.max(hwm)),
        })
    }
}

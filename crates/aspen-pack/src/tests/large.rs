//! A pack past 2 GiB: ingest's `.idx` must be byte-identical to `git
//! index-pack`'s, including the 64-bit offset table that only packs over
//! 2 GiB use. Deferred from A2 to D2 (decisions log, "A2 (coordinator
//! re-review of #21): deferred to D2 and D3 nightly"), because the fixture
//! is 2.2 GiB.
//!
//! Ignored in required CI. The nightly `bench` job runs it:
//!
//! ```text
//! cargo test --release -p aspen-pack -- --ignored over_2_gib --nocapture
//! ```
//!
//! It needs about 7 GiB of real disk (not tmpfs) under the target directory,
//! or under `ASPEN_LARGE_PACK_DIR` when set, and takes a minute or two.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use aspen_core::ingest::InMemoryBases;
use aspen_core::pack::PushLimits;

use crate::ingest::{CompleteOptions, complete, receive};
use crate::testutil::git;
use crate::{MaxInflatedBytes, MaxObjectBytes};

/// Incompressible blobs of 8 MiB each: 280 of them put the pack at about
/// 2.2 GiB, past the 2^31 offset where `.idx` v2 switches to 64-bit offsets.
const BIG_BLOBS: u32 = 280;
const BIG_BLOB_BYTES: usize = 8 << 20;
/// Similar text blobs written after the big ones: `fast-import` stores them
/// as OFS_DELTA entries, so deltas past the 2 GiB mark are covered too.
const TEXT_BLOBS: u32 = 64;

/// SplitMix64, so the fixture is the same on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

fn work_dir() -> tempfile::TempDir {
    let base = std::env::var_os("ASPEN_LARGE_PACK_DIR").map_or_else(
        || PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target")),
        PathBuf::from,
    );
    fs::create_dir_all(&base).unwrap();
    tempfile::Builder::new()
        .prefix("aspen-large-pack-")
        .tempdir_in(base)
        .unwrap()
}

/// Writes the fixture with `git fast-import` into a new bare repository and
/// returns the path of its one pack.
fn write_fixture(repo: &Path) -> PathBuf {
    git(repo, &["init", "--quiet", "--bare"]);
    let mut child = Command::new("git")
        .current_dir(repo)
        .args(["fast-import", "--quiet"])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let mut input = BufWriter::with_capacity(1 << 20, child.stdin.take().unwrap());
        let mut rng = Rng(0x2_6100);
        let mut blob = vec![0u8; BIG_BLOB_BYTES];
        for _ in 0..BIG_BLOBS {
            rng.fill(&mut blob);
            write!(input, "blob\ndata {}\n", blob.len()).unwrap();
            input.write_all(&blob).unwrap();
            input.write_all(b"\n").unwrap();
        }
        let mut text = String::new();
        for i in 0..TEXT_BLOBS {
            for j in 0..64 {
                text.push_str(&format!("line {i} {j}: past the 2 GiB mark\n"));
            }
            write!(input, "blob\ndata {}\n{text}\n", text.len()).unwrap();
        }
        input.flush().unwrap();
    }
    assert!(child.wait().unwrap().success(), "git fast-import failed");
    let packs: Vec<PathBuf> = fs::read_dir(repo.join("objects/pack"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "pack"))
        .collect();
    assert_eq!(packs.len(), 1, "fast-import wrote one pack: {packs:?}");
    packs.into_iter().next().unwrap()
}

/// Entries of a v2 `.idx` whose offset is in the 64-bit table.
fn large_offsets(idx: &[u8]) -> usize {
    let n = u32::from_be_bytes(idx[8 + 255 * 4..8 + 256 * 4].try_into().unwrap()) as usize;
    let small = 8 + 256 * 4 + n * (20 + 4 + 4);
    (idx.len() - small - 40) / 8
}

#[test]
#[ignore = "slow: writes a 2.2 GiB pack; the nightly bench job runs it"]
fn a_pack_over_2_gib_indexes_byte_identical_to_git() {
    let dir = work_dir();
    let repo = dir.path().join("repo.git");
    fs::create_dir(&repo).unwrap();
    let t = Instant::now();
    let pack_path = write_fixture(&repo);
    let size = fs::metadata(&pack_path).unwrap().len();
    eprintln!("fixture: {size} bytes in {:.1}s", t.elapsed().as_secs_f64());
    assert!(size > 1 << 31, "the fixture is past 2 GiB: {size}");

    let t = Instant::now();
    let git_idx = dir.path().join("git.idx");
    git(
        dir.path(),
        &[
            "index-pack",
            "-o",
            git_idx.to_str().unwrap(),
            pack_path.to_str().unwrap(),
        ],
    );
    eprintln!("git index-pack: {:.1}s", t.elapsed().as_secs_f64());

    let t = Instant::now();
    let limits = PushLimits {
        max_objects: 10_000,
        max_pack_bytes: 8 << 30,
    };
    let received = receive(
        &mut File::open(&pack_path).unwrap(),
        &dir.path().join("spool"),
        &limits,
    )
    .unwrap();
    assert_eq!(received.received_bytes, size);
    let out = dir.path().join("out");
    fs::create_dir(&out).unwrap();
    let stored = complete(
        &received,
        &InMemoryBases::default(),
        &out,
        &CompleteOptions::new(
            MaxObjectBytes::new(16 << 20).unwrap(),
            MaxInflatedBytes::new(4 << 30).unwrap(),
        ),
    )
    .unwrap();
    eprintln!(
        "aspen receive + complete: {:.1}s",
        t.elapsed().as_secs_f64()
    );
    fs::remove_file(&received.spool).unwrap();

    // Nothing to complete, so the stored bytes are the received bytes.
    assert_eq!(stored.size, size);
    assert_eq!(stored.object_count, BIG_BLOBS + TEXT_BLOBS);
    assert!(
        stored.max_delta_depth > 0,
        "the text blobs past 2 GiB are deltas"
    );
    let ours = fs::read(&stored.idx_path).unwrap();
    let theirs = fs::read(&git_idx).unwrap();
    let large = large_offsets(&theirs);
    eprintln!("idx: {} bytes, {large} 64-bit offsets", theirs.len());
    assert!(large > 0, "git's .idx uses the 64-bit offset table");
    assert_eq!(
        ours.len(),
        theirs.len(),
        "the .idx sizes differ ({large} 64-bit offsets in git's)"
    );
    assert!(
        ours == theirs,
        "the stored .idx is byte-identical to git index-pack's for a pack past 2 GiB"
    );
}

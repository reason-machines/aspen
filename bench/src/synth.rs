//! Deterministic synthetic repositories.
//!
//! The same [`RepoSpec`] always produces the same objects and the same `main`
//! tip on every machine and Git version: content comes from a seeded
//! SplitMix64 generator, commits have fixed authors and timestamps, and the
//! history is written with `git fast-import`.
//!
//! The first commit adds `files` text files of about `file_bytes` each. Every
//! later commit edits `changes_per_commit` files by rewriting a window of
//! lines, so packs contain realistic deltas rather than unrelated blobs.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::git::{self, AUTHOR, Git};

/// Fixed timestamp of the first synthetic commit (2023-11-14T22:13:20Z).
pub const EPOCH: u64 = 1_700_000_000;

/// SplitMix64: tiny, fast and fully specified, so the sequence is stable
/// across platforms and Rust versions.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// A generator derived from a seed and a list of keys, so independent
    /// streams (one per file, line or writer) don't share state.
    pub fn derive(seed: u64, keys: &[u64]) -> Self {
        let mut r = Self(seed);
        for &k in keys {
            r = Self(r.next_u64() ^ k.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        }
        r
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform-ish integer in `0..n` (`n > 0`); the modulo bias is
    /// irrelevant for generating test content.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

/// Shape of a synthetic repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoSpec {
    pub seed: u64,
    /// Files added by the first commit.
    pub files: u32,
    /// Approximate size of each file.
    pub file_bytes: u32,
    /// Commits on `main`, including the first.
    pub commits: u32,
    /// Files edited by each commit after the first.
    pub changes_per_commit: u32,
}

impl RepoSpec {
    /// Named presets. Raw (uncompressed) content sizes are roughly
    /// `files * file_bytes`: small ~0.8 MB, medium ~40 MB, large ~400 MB.
    pub fn preset(name: &str, seed: u64) -> Option<Self> {
        let (files, file_bytes, commits, changes_per_commit) = match name {
            "tiny" => (20, 1024, 3, 2),
            "small" => (200, 4096, 20, 10),
            "medium" => (5_000, 8192, 200, 50),
            "large" => (50_000, 8192, 1_000, 100),
            _ => return None,
        };
        Some(Self {
            seed,
            files,
            file_bytes,
            commits,
            changes_per_commit,
        })
    }

    pub fn params(&self) -> BTreeMap<String, serde_json::Value> {
        let mut m = BTreeMap::new();
        m.insert("repo_seed".to_owned(), self.seed.into());
        m.insert("repo_files".to_owned(), self.files.into());
        m.insert("repo_file_bytes".to_owned(), self.file_bytes.into());
        m.insert("repo_commits".to_owned(), self.commits.into());
        m.insert(
            "repo_changes_per_commit".to_owned(),
            self.changes_per_commit.into(),
        );
        m
    }
}

const LINE_BYTES: u32 = 64;

/// A vocabulary of 512 pseudo-words, fixed by the seed.
fn vocabulary(seed: u64) -> Vec<String> {
    let mut rng = Rng::derive(seed, &[0x76_6f63_6162]);
    (0..512)
        .map(|_| {
            let len = 2 + rng.below(8) as usize;
            (0..len)
                .map(|_| char::from(b'a' + rng.below(26) as u8))
                .collect()
        })
        .collect()
}

/// One line of text of roughly `LINE_BYTES`, determined by its keys.
fn line(vocab: &[String], seed: u64, keys: &[u64], out: &mut String) {
    let mut rng = Rng::derive(seed, keys);
    let start = out.len();
    while out.len() - start < LINE_BYTES as usize - 8 {
        if out.len() > start {
            out.push(' ');
        }
        out.push_str(&vocab[rng.below(vocab.len() as u64) as usize]);
    }
    out.push('\n');
}

/// Path of synthetic file `i`, in directories of 100 files.
pub fn file_path(i: u32) -> String {
    format!("src/d{:03}/f{:05}.txt", i / 100, i)
}

/// An edit to one file: lines `start..start + len` were rewritten by edit
/// number `id`.
#[derive(Debug, Clone, Copy)]
struct Edit {
    start: u32,
    len: u32,
    id: u64,
}

fn file_content(vocab: &[String], spec: &RepoSpec, file: u32, edits: &[Edit]) -> String {
    let lines = (spec.file_bytes / LINE_BYTES).max(1);
    let mut out = String::with_capacity(spec.file_bytes as usize + 64);
    for j in 0..lines {
        // The most recent edit covering this line decides its content.
        let version = edits
            .iter()
            .rev()
            .find(|e| j >= e.start && j < e.start + e.len)
            .map_or(0, |e| e.id);
        line(
            vocab,
            spec.seed,
            &[u64::from(file), u64::from(j), version],
            &mut out,
        );
    }
    out
}

fn push_data(stream: &mut Vec<u8>, data: &[u8]) {
    stream.extend_from_slice(format!("data {}\n", data.len()).as_bytes());
    stream.extend_from_slice(data);
    stream.push(b'\n');
}

/// The `git fast-import` stream for `spec`, writing `refs/heads/main`.
pub fn fast_import_stream(spec: &RepoSpec) -> Vec<u8> {
    let vocab = vocabulary(spec.seed);
    let lines = (spec.file_bytes / LINE_BYTES).max(1);
    let mut edits: Vec<Vec<Edit>> = vec![Vec::new(); spec.files as usize];
    let mut rng = Rng::derive(spec.seed, &[0x6564_6974]);
    let mut stream = Vec::new();
    let mut edit_id = 0u64;
    for c in 0..spec.commits.max(1) {
        let changed: Vec<u32> = if c == 0 {
            (0..spec.files).collect()
        } else {
            let mut picked: Vec<u32> = (0..spec.changes_per_commit.min(spec.files))
                .map(|_| rng.below(u64::from(spec.files)) as u32)
                .collect();
            picked.sort_unstable();
            picked.dedup();
            for &f in &picked {
                edit_id += 1;
                let len = 1 + rng.below(u64::from(lines.min(8))) as u32;
                let start = rng.below(u64::from(lines)) as u32;
                edits[f as usize].push(Edit {
                    start,
                    len,
                    id: edit_id,
                });
            }
            picked
        };
        let mut header = String::new();
        let ts = EPOCH + u64::from(c) * 60;
        let _ = writeln!(header, "commit refs/heads/main");
        let _ = writeln!(header, "author {AUTHOR} {ts} +0000");
        let _ = writeln!(header, "committer {AUTHOR} {ts} +0000");
        stream.extend_from_slice(header.as_bytes());
        push_data(
            &mut stream,
            format!(
                "synthetic commit {c} (seed {}, {} files changed)",
                spec.seed,
                changed.len()
            )
            .as_bytes(),
        );
        for f in changed {
            let content = file_content(&vocab, spec, f, &edits[f as usize]);
            stream.extend_from_slice(format!("M 100644 inline {}\n", file_path(f)).as_bytes());
            push_data(&mut stream, content.as_bytes());
        }
        stream.push(b'\n');
    }
    stream
}

/// Generates the repository into a new bare repository at `dest` and returns
/// the `main` tip.
pub fn generate(git: &Git, spec: &RepoSpec, dest: &Path) -> Result<String, String> {
    git::init_bare(git, dest)?;
    let stream = fast_import_stream(spec);
    git::run_with_input(git.cmd(dest).args(["fast-import", "--quiet"]), &stream)?;
    // Repack once so the seed repository looks like a server-side repository
    // and later shared clones read one pack.
    git::run(git.cmd(dest).args(["repack", "-a", "-d", "-q"]))?;
    git::run(git.cmd(dest).args(["rev-parse", "refs/heads/main"]))
}

/// A commit for [`commit_on`] to create.
#[derive(Debug, Clone, Copy)]
pub struct NewCommit<'a> {
    /// Local branch (without `refs/heads/`) the commit is written to.
    pub branch: &'a str,
    pub parent: &'a str,
    /// The single file the commit sets.
    pub path: &'a str,
    pub content: &'a [u8],
    pub message: &'a str,
    pub timestamp: u64,
}

/// Writes `commit` into the bare repository `repo` and returns its id.
///
/// Used by the workloads to make the commits they push: one `fast-import`
/// process per commit, no work tree.
pub fn commit_on(git: &Git, repo: &Path, commit: &NewCommit<'_>) -> Result<String, String> {
    let mut stream = Vec::new();
    stream.extend_from_slice(format!("commit refs/heads/{}\n", commit.branch).as_bytes());
    stream.extend_from_slice(format!("committer {AUTHOR} {} +0000\n", commit.timestamp).as_bytes());
    push_data(&mut stream, commit.message.as_bytes());
    stream.extend_from_slice(format!("from {}\n", commit.parent).as_bytes());
    stream.extend_from_slice(format!("M 100644 inline {}\n", commit.path).as_bytes());
    push_data(&mut stream, commit.content);
    stream.push(b'\n');
    git::run_with_input(
        git.cmd(repo).args(["fast-import", "--quiet", "--force"]),
        &stream,
    )?;
    git::run(
        git.cmd(repo)
            .args(["rev-parse", &format!("refs/heads/{}", commit.branch)]),
    )
}

/// `bytes` of word-like text determined by `keys`.
pub fn text(seed: u64, keys: &[u64], bytes: usize) -> String {
    let vocab = vocabulary(seed);
    let mut out = String::with_capacity(bytes + LINE_BYTES as usize);
    let mut j = 0u64;
    while out.len() < bytes {
        let mut k = keys.to_vec();
        k.push(j);
        line(&vocab, seed, &k, &mut out);
        j += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix_sequence_is_stable() {
        // Reference values for SplitMix64 seeded with 0.
        let mut r = Rng::new(0);
        assert_eq!(r.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(r.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(r.next_u64(), 0x06C4_5D18_8009_454F);
    }

    #[test]
    fn stream_is_deterministic_and_seed_dependent() {
        let spec = RepoSpec::preset("tiny", 7).unwrap();
        assert_eq!(fast_import_stream(&spec), fast_import_stream(&spec));
        let other = RepoSpec { seed: 8, ..spec };
        assert_ne!(fast_import_stream(&spec), fast_import_stream(&other));
    }

    #[test]
    fn files_have_roughly_the_requested_size() {
        let spec = RepoSpec::preset("small", 1).unwrap();
        let vocab = vocabulary(spec.seed);
        let content = file_content(&vocab, &spec, 3, &[]);
        let len = content.len() as f64;
        assert!(
            (len - f64::from(spec.file_bytes)).abs() < f64::from(spec.file_bytes) * 0.1,
            "{len}"
        );
    }

    #[test]
    fn an_edit_changes_only_its_window() {
        let spec = RepoSpec::preset("small", 1).unwrap();
        let vocab = vocabulary(spec.seed);
        let before = file_content(&vocab, &spec, 0, &[]);
        let after = file_content(
            &vocab,
            &spec,
            0,
            &[Edit {
                start: 5,
                len: 2,
                id: 1,
            }],
        );
        let changed: Vec<usize> = before
            .lines()
            .zip(after.lines())
            .enumerate()
            .filter(|(_, (a, b))| a != b)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(changed, vec![5, 6]);
    }

    #[test]
    fn presets_exist_and_unknown_names_do_not() {
        for p in ["tiny", "small", "medium", "large"] {
            assert!(RepoSpec::preset(p, 0).is_some(), "{p}");
        }
        assert!(RepoSpec::preset("huge", 0).is_none());
    }

    #[test]
    fn generated_repository_has_the_same_tip_every_time() {
        let tmp = tempfile::tempdir().unwrap();
        let git = Git::new(tmp.path(), 2);
        let spec = RepoSpec::preset("tiny", 42).unwrap();
        let a = generate(&git, &spec, &tmp.path().join("a.git")).unwrap();
        let b = generate(&git, &spec, &tmp.path().join("b.git")).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), 40, "{a}");
        let c = generate(
            &git,
            &RepoSpec { seed: 43, ..spec },
            &tmp.path().join("c.git"),
        )
        .unwrap();
        assert_ne!(a, c);
        let count = git::run(
            git.cmd(&tmp.path().join("a.git"))
                .args(["rev-list", "--count", "main"]),
        )
        .unwrap();
        assert_eq!(count, spec.commits.to_string());
        let files = git::run(git.cmd(&tmp.path().join("a.git")).args([
            "ls-tree",
            "-r",
            "--name-only",
            "main",
        ]))
        .unwrap();
        assert_eq!(files.lines().count(), spec.files as usize);
        git::run(
            git.cmd(&tmp.path().join("a.git"))
                .args(["fsck", "--strict", "--no-progress"]),
        )
        .unwrap();
    }

    #[test]
    fn commit_on_extends_a_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let git = Git::new(tmp.path(), 2);
        let repo = tmp.path().join("r.git");
        let main = generate(&git, &RepoSpec::preset("tiny", 1).unwrap(), &repo).unwrap();
        let commit = |parent: &str, content: &[u8]| {
            commit_on(
                &git,
                &repo,
                &NewCommit {
                    branch: "w",
                    parent,
                    path: "bench/w.txt",
                    content,
                    message: "test",
                    timestamp: EPOCH,
                },
            )
            .unwrap()
        };
        let c1 = commit(&main, b"one\n");
        let c2 = commit(&c1, b"two\n");
        let parent = git::run(git.cmd(&repo).args(["rev-parse", &format!("{c2}^")])).unwrap();
        assert_eq!(parent, c1);
        let blob = git::run(git.cmd(&repo).args(["show", &format!("{c2}:bench/w.txt")])).unwrap();
        assert_eq!(blob, "two");
    }
}

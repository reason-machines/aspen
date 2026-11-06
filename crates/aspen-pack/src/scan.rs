//! Reading objects out of a pack: one traversal that yields every commit's
//! `commit_index` row and the objects the pack references but does not
//! contain, plus a single-object read for the tip commit.
//!
//! - **Connectivity check** (push path step 2): only objects the incoming
//!   pack references (commit trees and parents, tree entries, tag targets)
//!   but does not contain are checked, by the caller, against the push's
//!   dependency closure with `aspen_core::ingest::ObjectLocator`. Nothing
//!   reachable from them is walked: every committed ref version is
//!   connected, so existence implies connectivity. [`ScanOutcome::missing`]
//!   is that list; [`check_connectivity`] runs the lookup.
//! - **Commit rows**: the tip's row needs one `.idx` lookup
//!   ([`read_object`] then [`parse_commit`]); the rest of a large push is
//!   written later by the `index_commits` job from a second traversal of the
//!   stored pack ([`scan`] with only [`ScanOptions::commits`], S4).
//!
//! [`references`] is the pure building block both use. It is also what a
//! pack-reachability walk (whether every object in a write's pack is
//! reachable from its tip, `refs::Closure`) would be built on; that walk is
//! B1's and is not implemented here.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use aspen_core::error::{StoreError, UnpackError};
use aspen_core::id::ObjectId;
use aspen_core::ingest::{ObjectKind, ObjectLocator, SearchPacks, SortedIds};
use aspen_core::meta::NewCommit;

use crate::ingest::{core_kind, gix_chain};
use crate::{MaxInflatedBytes, MaxObjectBytes, from_gix, to_gix};

/// Why an object does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?} {id}: {reason}")]
pub struct ObjectError {
    /// The object.
    pub id: ObjectId,
    /// Its type.
    pub kind: ObjectKind,
    /// What is wrong.
    pub reason: String,
}

/// Calls `f` with every object ID that `data`, an object of type `kind`,
/// references: a commit's tree and parents, a tree's entries (except
/// gitlinks, which name commits in another repository), a tag's target.
/// Blobs reference nothing. Pure.
pub fn references(
    kind: ObjectKind,
    data: &[u8],
    mut f: impl FnMut(ObjectId),
) -> Result<(), String> {
    match kind {
        ObjectKind::Blob => {}
        ObjectKind::Commit => {
            let header = CommitHeader::parse(data)?;
            f(header.tree);
            header.parents.into_iter().for_each(f);
        }
        ObjectKind::Tree => {
            for entry in gix_object::TreeRefIter::from_bytes(data, gix_hash::Kind::Sha1) {
                let entry = entry.map_err(|e| gix_chain(&e))?;
                if !entry.mode.is_commit() {
                    f(from_gix(entry.oid));
                }
            }
        }
        ObjectKind::Tag => {
            let target = gix_object::TagRefIter::from_bytes(data, gix_hash::Kind::Sha1)
                .target_id()
                .map_err(|e| gix_chain(&e))?;
            f(from_gix(&target));
        }
    }
    Ok(())
}

/// A commit's parents (in order) and committer time, as `commit_index`
/// stores them. Pure.
///
/// Parsed the way Git's `parse_commit_buffer` and `parse_commit_date` do
/// (git 2.51 `commit.c`), so the row agrees with what `git rev-list` and
/// `--shallow-since` see, and no parent Git would follow is missed by the
/// connectivity check:
///
/// - a `tree <hex>\n` line first, with more bytes after it; hex is read
///   case-insensitively, as `get_oid_hex` does;
/// - then every line that starts with `parent ` must be `parent <hex>\n`
///   followed by more bytes, or the commit is an error (Git: "bad
///   parents"). Git silently stops at a `parent ` line shorter than a full
///   one at the very end of the buffer; this parser refuses it instead, so
///   it is never looser than Git;
/// - the committer time is read from a `committer` line that directly
///   follows the `author` line: the number after the line's last `>`.
///   Anything else about the time (missing, garbage, no newline) gives 0,
///   as in Git.
///
/// Git stores the time as an unsigned 64-bit value (`strtoumax`, so `-5`
/// wraps and an overflow is the maximum); a value of 2^63 or more is
/// stored here as `i64::MAX`, which keeps its order against every real
/// time.
pub fn parse_commit(data: &[u8]) -> Result<(Vec<ObjectId>, i64), String> {
    let h = CommitHeader::parse(data)?;
    Ok((h.parents, h.committer_time))
}

/// A commit's root tree, parsed as [`parse_commit`] parses the rest of its
/// header. Pure.
pub fn commit_tree(data: &[u8]) -> Result<ObjectId, String> {
    Ok(CommitHeader::parse(data)?.tree)
}

struct CommitHeader {
    tree: ObjectId,
    parents: Vec<ObjectId>,
    committer_time: i64,
}

/// `tree ` + 40 hex digits.
const TREE_ENTRY_LEN: usize = 45;
/// `parent ` + 40 hex digits.
const PARENT_ENTRY_LEN: usize = 47;

impl CommitHeader {
    fn parse(data: &[u8]) -> Result<Self, String> {
        if data.len() <= TREE_ENTRY_LEN + 1
            || !data.starts_with(b"tree ")
            || data[TREE_ENTRY_LEN] != b'\n'
        {
            return Err("bogus commit: no tree line".into());
        }
        let tree = hex_id(&data[5..TREE_ENTRY_LEN]).ok_or("bad tree pointer")?;
        let mut pos = TREE_ENTRY_LEN + 1;
        let mut parents = Vec::new();
        while data[pos..].starts_with(b"parent ") {
            let line = &data[pos..];
            if line.len() <= PARENT_ENTRY_LEN + 1 || line[PARENT_ENTRY_LEN] != b'\n' {
                return Err("bad parents".into());
            }
            parents.push(hex_id(&line[7..PARENT_ENTRY_LEN]).ok_or("bad parents")?);
            pos += PARENT_ENTRY_LEN + 1;
        }
        Ok(Self {
            tree,
            parents,
            committer_time: commit_date(&data[pos..]),
        })
    }
}

/// Exactly 40 hex digits, either case (Git's `get_oid_hex`).
fn hex_id(hex: &[u8]) -> Option<ObjectId> {
    if hex.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    let (pairs, _) = hex.as_chunks::<2>();
    for (o, [hi, lo]) in out.iter_mut().zip(pairs) {
        let digit = |c: u8| (c as char).to_digit(16);
        *o = (digit(*hi)? * 16 + digit(*lo)?) as u8;
    }
    Some(ObjectId::from_bytes(out))
}

/// Git's `parse_commit_date`, on the bytes after the parent lines.
fn commit_date(buf: &[u8]) -> i64 {
    if buf.len() <= 6 || !buf.starts_with(b"author") {
        return 0;
    }
    let Some(nl) = buf.iter().position(|&b| b == b'\n') else {
        return 0;
    };
    let buf = &buf[nl + 1..];
    if buf.len() <= 9 || !buf.starts_with(b"committer") {
        return 0;
    }
    let Some(eol) = buf.iter().position(|&b| b == b'\n') else {
        return 0;
    };
    // Walk back from the end of the line to just after its last `>`.
    let Some(gt) = buf[..eol].iter().rposition(|&b| b == b'>') else {
        return 0;
    };
    // Git's isspace (space, tab, CR, LF), then a digit or `-` is required.
    let rest = &buf[gt + 1..eol];
    let rest = &rest[rest
        .iter()
        .position(|b| !matches!(b, b' ' | b'\t' | b'\r'))
        .unwrap_or(rest.len())..];
    let (negative, digits) = match rest.first() {
        Some(b'-') => (true, &rest[1..]),
        Some(b) if b.is_ascii_digit() => (false, rest),
        _ => return 0,
    };
    // strtoumax: as many digits as there are; overflow is the maximum
    // whatever the sign; a minus sign negates modulo 2^64.
    let mut t: u64 = 0;
    let mut overflow = false;
    for d in digits.iter().take_while(|b| b.is_ascii_digit()) {
        match t
            .checked_mul(10)
            .and_then(|t| t.checked_add(u64::from(d - b'0')))
        {
            Some(v) => t = v,
            None => overflow = true,
        }
    }
    let t = if overflow {
        u64::MAX
    } else if negative {
        t.wrapping_neg()
    } else {
        t
    };
    i64::try_from(t).unwrap_or(i64::MAX)
}

/// A commit found in a pack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitInfo {
    /// The commit.
    pub id: ObjectId,
    /// Parents, in order.
    pub parents: Vec<ObjectId>,
    /// Committer time, seconds since the epoch.
    pub committer_time: i64,
}

impl CommitInfo {
    /// The `commit_index` row for the version a write creates.
    pub fn to_new_commit(&self) -> NewCommit {
        NewCommit {
            commit: self.id,
            parents: self.parents.clone(),
            committer_time: self.committer_time,
        }
    }
}

/// What [`scan`] collects, and its object limit (no `Default`: the limit
/// comes from the server's configuration, as for ingest).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanOptions {
    /// Collect every commit's row.
    pub commits: bool,
    /// Collect the objects referenced but not contained.
    pub missing: bool,
    /// Threads; `None` uses every core.
    pub thread_limit: Option<usize>,
    /// The largest object or delta result the scan inflates; a pack entry
    /// that claims more fails the scan before anything that size is
    /// allocated. Use the same value as ingest.
    pub max_object_bytes: MaxObjectBytes,
    /// The most the pack may inflate to in total ([`MaxInflatedBytes`]),
    /// checked from the declared sizes before gix resolves anything. Use
    /// the same value as ingest.
    pub max_inflated_bytes: MaxInflatedBytes,
    /// Verify the pack's trailer checksum, the `.idx` checksum and every
    /// object's CRC32 and ID. Ingest's own output was just verified, so it
    /// may skip this; set it for a pack read back from storage (a blob that
    /// no longer matches its row must not yield commit rows).
    pub verify_checksums: bool,
}

/// The result of a [`scan`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScanOutcome {
    /// Every commit, sorted by ID (empty unless [`ScanOptions::commits`]).
    pub commits: Vec<CommitInfo>,
    /// Objects referenced by the pack's objects and not in the pack (empty
    /// unless [`ScanOptions::missing`]).
    pub missing: SortedIds,
}

/// Why a scan failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScanError {
    /// An object does not parse, so its references are unknown.
    #[error("malformed object: {0}")]
    Object(ObjectError),
    /// The pack or `.idx` cannot be read.
    #[error("cannot read the pack: {0}")]
    Pack(String),
}

impl From<ScanError> for UnpackError {
    /// On a received pack, an object that does not parse refuses the push
    /// (its references cannot be checked); a pack that cannot be read is a
    /// local failure.
    fn from(e: ScanError) -> Self {
        match e {
            ScanError::Object(o) => UnpackError::Malformed {
                reason: o.to_string(),
            },
            ScanError::Pack(reason) => UnpackError::Store(StoreError::LocalIo { reason }),
        }
    }
}

/// One parallel traversal of the pack at `pack` (with its `.idx` at `idx`)
/// that decodes every object once.
///
/// Memory is proportional to what is collected: 20 bytes per reference for
/// `missing` (sorted and merged against the `.idx` at the end), and one row
/// per commit for `commits`.
pub fn scan(pack: &Path, idx: &Path, options: ScanOptions) -> Result<ScanOutcome, ScanError> {
    let pack_err = |e: gix_error::Error| ScanError::Pack(gix_chain(&e));
    let index = gix_pack::index::File::at(idx, gix_hash::Kind::Sha1).map_err(pack_err)?;
    check_declared_sizes(pack, &index, &options)?;
    let alloc_limit = options
        .max_object_bytes
        .gix_alloc_limit(u64::from(index.num_objects()));
    let data = gix_pack::data::File::at(pack, gix_hash::Kind::Sha1)
        .map_err(pack_err)?
        .with_alloc_limit_bytes(Some(alloc_limit));
    #[derive(Default)]
    struct Found {
        commits: Vec<CommitInfo>,
        refs: Vec<ObjectId>,
        error: Option<ObjectError>,
    }
    let found = Mutex::new(Found::default());
    let processor = |kind: gix_object::Kind,
                     bytes: &[u8],
                     entry: &gix_pack::index::Entry,
                     _: &dyn gix_utils::progress::Progress|
     -> gix_error::Result {
        let kind = core_kind(kind);
        let id = from_gix(&entry.oid);
        let fail = |reason: String| ObjectError { id, kind, reason };
        let mut local_refs = Vec::new();
        let mut commit = None;
        let parsed = (|| {
            if options.missing {
                references(kind, bytes, |r| local_refs.push(r)).map_err(fail)?;
            }
            if options.commits && kind == ObjectKind::Commit {
                let (parents, committer_time) = parse_commit(bytes).map_err(fail)?;
                commit = Some(CommitInfo {
                    id,
                    parents,
                    committer_time,
                });
            }
            Ok::<(), ObjectError>(())
        })();
        let mut f = found.lock().expect("not poisoned");
        match parsed {
            Ok(()) => {
                f.refs.extend(local_refs);
                f.commits.extend(commit);
                Ok(())
            }
            Err(e) => {
                let msg = e.to_string();
                f.error.get_or_insert(e);
                Err(gix_error::message!("{msg}").validation_error())
            }
        }
    };
    let interrupt = AtomicBool::new(false);
    let result = index.traverse(
        &data,
        &mut gix_utils::progress::Discard,
        &interrupt,
        processor,
        gix_pack::index::traverse::Options {
            traversal: gix_pack::index::traverse::Algorithm::DeltaTreeLookup,
            thread_limit: options.thread_limit,
            check: if options.verify_checksums {
                gix_pack::index::traverse::SafetyCheck::All
            } else {
                gix_pack::index::traverse::SafetyCheck::SkipFileAndObjectChecksumVerification
            },
            alloc_limit_bytes: Some(alloc_limit),
            make_pack_lookup_cache: || gix_pack::cache::Never,
        },
    );
    let mut found = found.into_inner().expect("not poisoned");
    if let Some(e) = found.error.take() {
        return Err(ScanError::Object(e));
    }
    result.map_err(pack_err)?;
    let mut missing = SortedIds::default();
    if options.missing {
        let referenced = SortedIds::from_unsorted(std::mem::take(&mut found.refs));
        let out: Vec<ObjectId> = referenced
            .as_slice()
            .iter()
            .filter(|id| index.lookup(to_gix(id)).is_none())
            .copied()
            .collect();
        missing = SortedIds::from_unsorted(out);
    }
    found.commits.sort_unstable_by_key(|c| c.id);
    Ok(ScanOutcome {
        commits: found.commits,
        missing,
    })
}

/// Checks every entry's declared sizes against the object limit, and their
/// sum against the total limit (see `crate::declared`), at the offsets the
/// `.idx` lists, before gix decodes the pack under a limit that grows with
/// its object count.
pub(crate) fn check_declared_sizes(
    pack: &Path,
    index: &gix_pack::index::File,
    options: &ScanOptions,
) -> Result<(), ScanError> {
    let mut offsets: Vec<u64> = (0..index.num_objects())
        .map(|i| index.pack_offset_at_index(i))
        .collect();
    offsets.sort_unstable();
    let file = std::fs::File::open(pack).map_err(|e| ScanError::Pack(e.to_string()))?;
    let err = |e: crate::declared::DeclaredError| ScanError::Pack(e.to_string());
    let totals = crate::declared::check_at(file, offsets, options.max_object_bytes).map_err(err)?;
    crate::declared::check_total(totals.all(), options.max_inflated_bytes, ",").map_err(err)
}

/// Reads one object, with its delta chain resolved, by one `.idx` lookup.
/// `None` if the pack does not contain it. An object or delta result over
/// `max_object_bytes` is an error, found before it is allocated.
pub fn read_object(
    pack: &Path,
    idx: &Path,
    id: &ObjectId,
    max_object_bytes: MaxObjectBytes,
) -> Result<Option<(ObjectKind, Vec<u8>)>, ScanError> {
    let pack_err = |e: gix_error::Error| ScanError::Pack(gix_chain(&e));
    let mut bundle = gix_pack::Bundle::at(idx, gix_hash::Kind::Sha1).map_err(pack_err)?;
    let _ = pack; // The bundle opens the pack next to its `.idx`.
    // One object at a time: no delta tree, so the object limit alone.
    bundle.pack = bundle
        .pack
        .with_alloc_limit_bytes(Some(max_object_bytes.gix_alloc_limit(0)));
    let mut buf = Vec::new();
    let mut inflate = gix_zlib::Inflate::default();
    let found = bundle
        .find(
            &to_gix(id),
            &mut buf,
            &mut inflate,
            &mut gix_pack::cache::Never,
        )
        .map_err(pack_err)?;
    Ok(found.map(|(data, _)| (core_kind(data.kind), data.data.to_vec())))
}

/// The connectivity check: every ID in `missing` must be found in `packs`
/// (the push's dependency closure) by `locator`. The first one that is not
/// refuses the push with [`UnpackError::MissingObject`].
pub async fn check_connectivity<L: ObjectLocator>(
    locator: &L,
    packs: &SearchPacks,
    missing: &SortedIds,
) -> Result<(), UnpackError> {
    if missing.is_empty() {
        return Ok(());
    }
    let found = locator
        .locate(packs, missing)
        .await
        .map_err(UnpackError::Store)?;
    if found.len() != missing.len() {
        return Err(UnpackError::Store(StoreError::Corrupt {
            what: format!(
                "object locator answered {} of {} IDs",
                found.len(),
                missing.len()
            ),
        }));
    }
    match missing
        .as_slice()
        .iter()
        .zip(&found)
        .find(|(_, f)| f.is_none())
    {
        Some((id, _)) => Err(UnpackError::MissingObject { id: *id }),
        None => Ok(()),
    }
}

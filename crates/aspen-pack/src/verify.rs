//! Checks the stored-pack invariants on any pack file: ingest output,
//! compaction output (C1 should run it on every pack it writes) and stitched
//! packs (the stitcher's property tests and the D3 fuzz target).
//!
//! A pack passes when:
//!
//! - it parses: header, every entry's zlib stream, trailer (with SHA-1
//!   collision detection);
//! - it is **self-contained**: gix resolves every delta inside the pack with
//!   no external lookup;
//! - it holds **no object twice**;
//! - it is **base-first**: every delta's base entry comes before it.
//!
//! This is the oracle S1 (README, item 6) asks A2 to assert; it indexes the
//! pack fully, so it costs what `git index-pack` costs.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use aspen_core::id::ObjectId;
use aspen_core::pack::{EntryKind, EntryTable};

use crate::idx::IdxV2;
use crate::ingest::gix_chain;
use crate::{LayoutError, MaxInflatedBytes, MaxObjectBytes, entries};

/// Why a pack fails verification.
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    /// The pack does not parse, or a delta base is not in it (thin).
    #[error("pack does not index on its own: {0}")]
    Index(String),
    /// Layout: an object twice, a forward base, or a pack/`.idx` mismatch.
    #[error(transparent)]
    Layout(#[from] LayoutError),
    /// An entry, delta base or delta result declares a size over the object
    /// limit, or the declared sizes sum to more than the total limit.
    #[error("{0}")]
    OverLimit(String),
}

/// A pack that passed [`verify_pack`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPack {
    /// Its entry table.
    pub table: EntryTable,
    /// Its `.idx`, as gix wrote it (byte-identical to `git index-pack`).
    pub idx: Vec<u8>,
    /// Longest delta chain (0 when no entry is a delta).
    pub max_delta_depth: u32,
    /// Longest delta chain of an object that is not a blob (a tree, commit
    /// or tag): compaction bounds these lower (item 11).
    pub max_tree_delta_depth: u32,
}

/// Verifies the pack at `pack`, using `scratch` (an existing directory) for
/// the temporary `.idx`. Every size the pack declares (entry, delta base,
/// delta result) is checked against `max_object_bytes`, and their sum
/// against `max_inflated_bytes`, by a header walk before gix reads the
/// pack, so this is safe on untrusted input (D3's fuzz target): nothing it
/// allocates is sized past the object limit, apart from gix's delta tree,
/// which the file size bounds, and what gix holds at once is bounded by the
/// total (see [`MaxInflatedBytes`]).
pub fn verify_pack(
    pack: &Path,
    scratch: &Path,
    max_object_bytes: MaxObjectBytes,
    max_inflated_bytes: MaxInflatedBytes,
) -> Result<VerifiedPack, VerifyError> {
    let declared_error = |e| match e {
        crate::declared::DeclaredError::Io(e) => VerifyError::Layout(LayoutError::Io(e)),
        e @ (crate::declared::DeclaredError::OverLimit { .. }
        | crate::declared::DeclaredError::OverTotal { .. }) => {
            VerifyError::OverLimit(e.to_string())
        }
        e => VerifyError::Index(e.to_string()),
    };
    let totals =
        crate::declared::check_stream(File::open(pack).map_err(LayoutError::Io)?, max_object_bytes)
            .map_err(declared_error)?;
    crate::declared::check_total(totals.all(), max_inflated_bytes, ",").map_err(declared_error)?;
    let dir = tempdir_in(scratch)?;
    let mut reader = BufReader::new(File::open(pack).map_err(LayoutError::Io)?);
    // gix sizes its delta tree from the header's count; an entry takes at
    // least 2 bytes, so the file size bounds it on untrusted input.
    let size = fs::metadata(pack).map_err(LayoutError::Io)?.len();
    let mut head = [0u8; 12];
    let declared =
        match File::open(pack).and_then(|mut f| std::io::Read::read_exact(&mut f, &mut head)) {
            Ok(()) => u64::from(u32::from_be_bytes(head[8..12].try_into().expect("4 bytes"))),
            Err(_) => 0,
        };
    let object_count = declared.min(size / 2);
    let interrupt = AtomicBool::new(false);
    let outcome = gix_pack::Bundle::write_to_directory(
        &mut reader as &mut dyn std::io::BufRead,
        Some(&dir),
        &mut gix_utils::progress::Discard,
        &interrupt,
        None::<gix_object::find::Never>,
        gix_hash::Kind::Sha1,
        gix_pack::bundle::write::Options {
            alloc_limit_bytes: Some(max_object_bytes.gix_alloc_limit(object_count)),
            ..Default::default()
        },
    );
    let result = (|| {
        let outcome = outcome.map_err(|e| VerifyError::Index(gix_chain(&e)))?;
        let idx_path = outcome
            .index_path
            .ok_or_else(|| VerifyError::Index("no index written".into()))?;
        let idx_bytes = fs::read(idx_path).map_err(LayoutError::Io)?;
        let idx = IdxV2::parse(&idx_bytes, Some(size)).map_err(|e| {
            // gix lists a repeated object twice, which the parser rejects as
            // "not strictly increasing": report it as what it is.
            if e.0.contains("strictly increasing") {
                LayoutError::mismatch(format!("the pack holds an object twice ({e})"))
            } else {
                LayoutError::Idx(e)
            }
        })?;
        let table = entries::build(File::open(pack).map_err(LayoutError::Io)?, size, &idx)?;
        table.check_layout().map_err(LayoutError::Table)?;
        let depths = delta_depths(&table);
        Ok(VerifiedPack {
            table,
            idx: idx_bytes,
            max_delta_depth: depths.all,
            max_tree_delta_depth: depths.tree,
        })
    })();
    let _ = fs::remove_dir_all(&dir);
    result
}

/// The longest delta chain of a base-first pack: 0 for a full object, one
/// more than its base for a delta. Linear, because every base comes first.
pub fn max_delta_depth(table: &EntryTable) -> u32 {
    delta_depths(table).all
}

/// The longest delta chains of a base-first pack, of every object and of
/// the objects that are not blobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeltaDepths {
    /// Of every object.
    pub all: u32,
    /// Of trees, commits and tags: a delta is the kind of its chain's full
    /// object.
    pub tree: u32,
}

/// [`DeltaDepths`] of a base-first pack. Linear, because every base comes
/// first; a base the table lacks counts as a full object of unknown kind
/// (a blob).
pub fn delta_depths(table: &EntryTable) -> DeltaDepths {
    let entries = table.entries();
    // (depth, is a blob) by offset and by ID.
    let mut by_offset: HashMap<u64, (u32, bool)> = HashMap::with_capacity(entries.len());
    let mut by_id: HashMap<ObjectId, (u32, bool)> = HashMap::with_capacity(entries.len());
    let mut out = DeltaDepths::default();
    for e in entries {
        let (d, blob) = match e.kind {
            EntryKind::OfsDelta { base_offset } => by_offset
                .get(&base_offset)
                .map_or((0, true), |&(d, b)| (d + 1, b)),
            EntryKind::RefDelta { base } => {
                by_id.get(&base).map_or((0, true), |&(d, b)| (d + 1, b))
            }
            EntryKind::Blob => (0, true),
            EntryKind::Tree | EntryKind::Commit | EntryKind::Tag => (0, false),
        };
        by_offset.insert(e.offset, (d, blob));
        by_id.insert(e.id, (d, blob));
        out.all = out.all.max(d);
        if !blob {
            out.tree = out.tree.max(d);
        }
    }
    out
}

fn tempdir_in(parent: &Path) -> Result<std::path::PathBuf, VerifyError> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = parent.join(format!(
        "verify-{}-{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&dir).map_err(LayoutError::Io)?;
    Ok(dir)
}

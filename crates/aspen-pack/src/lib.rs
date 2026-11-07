//! Pack handling for Aspen: push ingest, `.idx` v2, entry tables, object
//! scans, and the deduplicating stitcher.
//!
//! Every stored pack is **self-contained**, **base-first** and holds **no
//! object twice** ([`aspen_core::pack`]). This crate is where those
//! invariants are established and relied on:
//!
//! | Module | What it does | Plan reference |
//! |---|---|---|
//! | [`ingest`] | Pass 1 receive, thin-pack completion with the duplicate rerun, forward-base repair, the `.idx` and entry table | push path step 2 |
//! | [`scan`] | One traversal of a pack: commit rows (`commit_index`) and the objects it references but lacks (connectivity check) | push path step 2, S4 |
//! | [`stitch`] | Keep-first, single-pass stitching of stored packs for clients without packfile URIs | fetch path step 4, S1 |
//! | [`locate`] | `ObjectLocator` and `BasePrefetcher` over stored packs: cached `.idx` probes, delta chains read in parallel rounds | push path step 2, S3, S4 |
//! | [`graph`] | Every object's references by `.idx` position, and the reachability walk behind the per-write split | push path step 2, B1 |
//! | [`subset`] | Writes a pack holding a subset of a completed pack's entries (undeltifying deltas whose base is left out) plus extra full objects | push path, B1 per-write packs |
//! | [`repack`] | Writes one pack from several stored packs: keep-first union (or a kept subset), base-first, delta depth capped by undeltifying | compaction, C1; shallow packs, C8 |
//! | [`rewrite`] | Rewrites one stored pack base-first, for the `rewrite_pack` repair job, and verifies the result | fetch path step 4, S1 |
//! | [`shallow`] | The object set of an exact shallow pack: the wants, the region's commits and everything their trees reach | fetch path step 5, C8 |
//! | [`verify`] | Checks the three invariants on any pack file (ingest output, compaction output, stitched packs) | S1 item 6 |
//! | [`idx`], [`entries`], [`header`] | `.idx` v2 parse and write, the entry-table header walk, entry header codecs | |
//!
//! The crate depends only on `aspen-core` among Aspen crates; it reaches
//! storage only through the `aspen-core` traits ([`locate`]). Uploads and
//! metadata writes are the caller's (`aspen-server`, node B1). Everything
//! except [`locate`] is synchronous CPU and local-disk work; callers on an
//! async runtime run it on a blocking thread.

#![deny(missing_docs)]

mod base_cache;
pub mod boundary;
pub mod chains;
pub mod changed;
mod declared;
pub mod entries;
pub mod graph;
pub mod header;
pub mod idx;
pub mod ingest;
pub mod locate;
pub mod repack;
pub mod rewrite;
pub mod sampled;
pub mod scan;
pub mod shallow;
pub mod stitch;
pub mod subset;
mod thin;
pub mod verify;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod testutil;

use aspen_core::id::{ObjectId, SHA1_LEN};
use aspen_core::pack::EntryTableError;

/// Why a pack, its `.idx` and its entry table do not fit together.
#[derive(Debug, thiserror::Error)]
pub enum LayoutError {
    /// Reading or writing a local file failed.
    #[error("local I/O: {0}")]
    Io(#[from] std::io::Error),
    /// An entry header does not decode.
    #[error("entry at offset {offset}: {source}")]
    Header {
        /// Offset of the entry.
        offset: u64,
        /// What is wrong.
        source: header::HeaderError,
    },
    /// The `.idx` is invalid.
    #[error(transparent)]
    Idx(#[from] idx::IdxError),
    /// The entries break an entry-table rule.
    #[error(transparent)]
    Table(#[from] EntryTableError),
    /// The pack and its `.idx` disagree.
    #[error("{0}")]
    Mismatch(String),
    /// The work would go over a configured limit (for example
    /// [`shallow::shallow_objects`]'s `max_objects`). Nothing is wrong with
    /// the data: a caller refuses the work rather than raising an alert.
    #[error("{0}")]
    OverLimit(String),
}

impl LayoutError {
    fn mismatch(what: impl Into<String>) -> Self {
        Self::Mismatch(what.into())
    }
}

/// The largest object, or delta result, this server inflates: the bound on
/// every allocation sized from bytes a pack declares.
///
/// Pack entries and delta headers state the size of what they inflate to,
/// and a few bytes can claim terabytes. gix sizes its buffers from those
/// claims. Every function in this crate that decodes pack data checks each
/// declared size against this limit itself (`declared`) before gix reads
/// the pack, then passes gix a limit that also covers gix's per-entry
/// allocations. There is deliberately no default. The server's
/// configuration (node B1) supplies it; Git's own `core.bigFileThreshold`
/// default of 512 MiB is a reasonable order of magnitude.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxObjectBytes(u64);

impl MaxObjectBytes {
    /// A limit of `bytes`; `None` for zero.
    pub const fn new(bytes: u64) -> Option<Self> {
        if bytes == 0 { None } else { Some(Self(bytes)) }
    }

    /// The limit in bytes.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// gix's `alloc_limit_bytes` for a pack that may hold up to
    /// `object_slots` entries.
    ///
    /// gix applies one limit to every allocation it sizes from the pack,
    /// including its delta tree (one item per entry, allocated up front), so
    /// the limit passed is the larger of this object limit and the tree's
    /// allowance, [`DELTA_TREE_BYTES_PER_ENTRY`] per slot. That makes gix's
    /// limit grow with the entry count, so it does not enforce the object
    /// limit: callers must run `declared`'s check first, which does. The
    /// slot count comes from a header already checked against
    /// `PushLimits::max_objects` (or from a stored `.idx`), so both terms are
    /// bounded.
    pub(crate) fn gix_alloc_limit(self, object_slots: u64) -> usize {
        let tree = object_slots.saturating_mul(DELTA_TREE_BYTES_PER_ENTRY);
        usize::try_from(self.0.max(tree)).unwrap_or(usize::MAX)
    }
}

/// The most a single pack may inflate to in total: the bound on what a
/// delta resolver can hold at once.
///
/// gix resolves a pack's deltas as a tree and keeps each resolved object
/// with children alive until its last child is done, working depth first.
/// A pack whose every level has a tiny leaf child before the next level
/// keeps every level on the path alive, so a push of a few hundred bytes
/// per level can hold depth × [`MaxObjectBytes`] (measured: 84 KB of 8 MiB
/// levels, 64 deep, held 521 MiB on one thread). No object or depth limit
/// stops that. This one does: before gix reads a pack, the sum of every
/// full entry's size and every delta's declared result size (plus, at
/// ingest, the thin bases completion inserts) is checked against it. What
/// gix holds at once is then at most that sum, plus about threads × 3 ×
/// the object limit of working buffers.
///
/// Two memory costs sit outside this total: the bases
/// `StoredPacks::prefetch_bases` holds for completion (bounded by
/// [`LocateOptions::max_prefetch_bytes`](crate::locate::LocateOptions::max_prefetch_bytes))
/// and gix's delta tree (bounded by [`DELTA_TREE_BYTES_PER_ENTRY`] per
/// slot, see [`MaxObjectBytes`]).
///
/// It also bounds very large legitimate pushes (a first push of a big
/// repository inflates to its full size), so set it with that in mind; the
/// decisions log (A2) records the longer-term fix, a byte-budgeted
/// resolver. There is no default; B1's configuration supplies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxInflatedBytes(u64);

impl MaxInflatedBytes {
    /// A limit of `bytes`; `None` for zero.
    pub const fn new(bytes: u64) -> Option<Self> {
        if bytes == 0 { None } else { Some(Self(bytes)) }
    }

    /// The limit in bytes.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Bytes allowed per entry for gix's delta tree when deriving the
/// allocation limit ([`MaxObjectBytes`]). gix 0.77's tree item is well
/// under this (pinned by `a_large_push_fits_the_tree_allowance`).
pub const DELTA_TREE_BYTES_PER_ENTRY: u64 = 256;

/// Git's maximum delta chain depth: `pack-objects` clamps `--depth` to it
/// and breaks longer reused chains, so no stock client sends a deeper one.
/// Ingest refuses a pushed pack whose chains are longer (decisions log, A2).
pub const MAX_DELTA_DEPTH: u32 = 4095;

/// Streaming SHA-1 with Git's collision detection (`sha1dc`).
#[derive(Clone)]
pub(crate) struct Sha1(gix_hash::Hasher);

impl std::fmt::Debug for Sha1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Sha1")
    }
}

/// A SHA-1 collision attack was detected in hashed data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("SHA-1 collision attack detected")]
pub struct Collision;

impl Sha1 {
    pub(crate) fn new() -> Self {
        Self(gix_hash::hasher(gix_hash::Kind::Sha1))
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    pub(crate) fn finish(self) -> Result<[u8; SHA1_LEN], Collision> {
        let id = self.0.try_finalize().map_err(|_| Collision)?;
        Ok(id.as_slice().try_into().expect("SHA-1 is 20 bytes"))
    }
}

/// SHA-1 of `bytes` as an [`ObjectId`]-shaped digest. A detected collision
/// attack yields the all-zero digest, which never equals a stored checksum,
/// so every comparison against it fails closed.
pub(crate) fn sha1(bytes: &[u8]) -> ObjectId {
    let mut h = Sha1::new();
    h.update(bytes);
    ObjectId::from_bytes(h.finish().unwrap_or([0; SHA1_LEN]))
}

pub(crate) fn to_gix(id: &ObjectId) -> gix_hash::ObjectId {
    gix_hash::ObjectId::from_bytes_or_panic(id.as_bytes())
}

pub(crate) fn from_gix(id: &gix_hash::oid) -> ObjectId {
    ObjectId::from_slice(id.as_bytes()).expect("SHA-1 IDs are 20 bytes")
}

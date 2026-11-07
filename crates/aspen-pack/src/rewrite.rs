//! Rewriting a stored pack base-first: the work of a `rewrite_pack` job
//! (`docs/PLAN.md`, fetch path step 4; reason-machines/aspen#10).
//!
//! Every stored pack must be base-first ([`aspen_core::pack`]): each
//! delta's base entry comes before the delta. Ingest and compaction write
//! packs that way, so a stored pack that is not base-first is a bug or old
//! data. The fetch path finds it when the stitcher meets a delta whose base
//! comes later, fails the fetch, and enqueues a `rewrite_pack` job;
//! [`rewrite_base_first`] is what that job runs on the pack's bytes.
//!
//! The rewrite is the one ingest applies to a completed pack whose
//! REF_DELTA comes before its base: entries keep their order, except that
//! each entry whose base comes later (and, transitively, any entry based on
//! it) moves to just after its base. Compressed data is copied unchanged,
//! so the object set, the entry kinds and every delta chain stay the same;
//! only OFS_DELTA distances, CRCs, the trailing checksum and the `.idx` are
//! new. The result is then checked as compaction checks its packs: it must
//! pass [`verify_pack`], whose `.idx` and entry table must equal the
//! writer's byte for byte, and hold exactly the input's objects.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use aspen_core::id::{ObjectId, PackChecksum};
use aspen_core::pack::{EntryTable, NewPack, entry_table_len};

use crate::verify::{VerifyError, verify_pack};
use crate::{LayoutError, MaxInflatedBytes, MaxObjectBytes};

/// A pack written by [`rewrite_base_first`], on local disk, ready to
/// upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewrittenPack {
    /// `<dir>/<checksum>.pack`.
    pub pack_path: PathBuf,
    /// `<dir>/<checksum>.idx`.
    pub idx_path: PathBuf,
    /// `<dir>/<checksum>.entries`.
    pub entries_path: PathBuf,
    /// Trailing SHA-1 of the new bytes. It differs from the input's
    /// whenever an entry moved.
    pub checksum: PackChecksum,
    /// Size in bytes. OFS_DELTA distances are re-encoded, so it may differ
    /// from the input's by a few bytes.
    pub size: u64,
    /// `.idx` size.
    pub idx_size: u64,
    /// Objects: the input's.
    pub object_count: u32,
    /// REF_DELTA entries: the input's (no entry changes kind).
    pub ref_delta_count: u32,
    /// Entries whose position in the pack changed: each delta that moved
    /// after its base, and the entries it moved past.
    pub moved: u32,
}

impl RewrittenPack {
    /// The `packs` row for the rewritten pack of the pack registered as
    /// `old`: the same repository and origin (so the same level, which a
    /// cover's foreign key requires), with this pack's checksum and sizes.
    pub fn new_pack(&self, old: &NewPack) -> NewPack {
        NewPack {
            repo: old.repo,
            checksum: self.checksum,
            size: self.size,
            idx_size: self.idx_size,
            entries_size: entry_table_len(self.object_count),
            object_count: self.object_count,
            ref_delta_count: self.ref_delta_count,
            origin: old.origin,
        }
    }
}

/// Rewrites the pack at `pack`, described by `table` (its stored entry
/// table), base-first into `out_dir`, named by its new checksum, and checks
/// the result (module docs). `out_dir` must exist; scratch files go to a
/// subdirectory of it that is removed before this returns.
///
/// `table` must describe a self-contained pack (every REF_DELTA base is in
/// it), as every stored pack is. Fails with [`VerifyError::Layout`] when it
/// is not, when the bytes and the table disagree, or when the delta bases
/// form a cycle, and with [`VerifyError`] when the written pack fails its
/// checks (a bug in the writer or corrupt input); nothing is written to
/// `out_dir` then but scratch files, which are removed.
///
/// A table that already passes [`EntryTable::check_layout`] is rewritten
/// too (to the same bytes); callers skip it.
pub fn rewrite_base_first(
    pack: &Path,
    table: &EntryTable,
    out_dir: &Path,
    max_object_bytes: MaxObjectBytes,
    max_inflated_bytes: MaxInflatedBytes,
) -> Result<RewrittenPack, VerifyError> {
    let scratch = out_dir.join(format!(
        "rewrite-{}-{}.tmp",
        std::process::id(),
        table.checksum().to_hex()
    ));
    fs::create_dir_all(&scratch).map_err(LayoutError::Io)?;
    let result = rewrite_in(
        pack,
        table,
        out_dir,
        &scratch,
        max_object_bytes,
        max_inflated_bytes,
    );
    let _ = fs::remove_dir_all(&scratch);
    result
}

fn rewrite_in(
    pack: &Path,
    table: &EntryTable,
    out_dir: &Path,
    scratch: &Path,
    max_object_bytes: MaxObjectBytes,
    max_inflated_bytes: MaxInflatedBytes,
) -> Result<RewrittenPack, VerifyError> {
    let written = scratch.join("rewritten.pack");
    let (idx_bytes, new_table) = crate::ingest::repair(pack, table, &written)?;
    let moved = table
        .entries()
        .iter()
        .zip(new_table.entries())
        .filter(|(a, b)| a.id != b.id)
        .count();

    let verified = verify_pack(&written, scratch, max_object_bytes, max_inflated_bytes)?;
    if verified.idx != idx_bytes {
        return Err(LayoutError::Mismatch(
            "the rewritten .idx differs from the one the pack itself gives".into(),
        )
        .into());
    }
    let entries = new_table.encode();
    if verified.table.encode() != entries {
        return Err(LayoutError::Mismatch(
            "the rewritten entry table differs from the pack's own entries".into(),
        )
        .into());
    }
    let before: BTreeSet<ObjectId> = table.entries().iter().map(|e| e.id).collect();
    let after: BTreeSet<ObjectId> = new_table.entries().iter().map(|e| e.id).collect();
    if before != after || after.len() != new_table.entries().len() {
        return Err(LayoutError::Mismatch(format!(
            "the rewritten pack holds {} entries of {} distinct objects; the input {} entries",
            new_table.entries().len(),
            after.len(),
            table.entries().len()
        ))
        .into());
    }

    let checksum = *new_table.checksum();
    let base = out_dir.join(checksum.to_hex());
    let paths = |ext| base.with_extension(ext);
    let (pack_path, idx_path, entries_path) = (paths("pack"), paths("idx"), paths("entries"));
    fs::rename(&written, &pack_path).map_err(LayoutError::Io)?;
    fs::write(&idx_path, &idx_bytes).map_err(LayoutError::Io)?;
    fs::write(&entries_path, &entries).map_err(LayoutError::Io)?;
    let object_count = u32::try_from(new_table.entries().len())
        .map_err(|_| LayoutError::Mismatch("more than 2^32 objects".into()))?;
    Ok(RewrittenPack {
        pack_path,
        idx_path,
        entries_path,
        checksum,
        size: new_table.pack_size(),
        idx_size: idx_bytes.len() as u64,
        object_count,
        ref_delta_count: new_table.ref_delta_count(),
        moved: u32::try_from(moved).unwrap_or(u32::MAX),
    })
}

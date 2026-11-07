//! The `rewrite_pack` job: repairs a stored pack that broke the base-first
//! invariant (`docs/PLAN.md`, fetch path step 4; reason-machines/aspen#10).

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::PackId;
use aspen_core::meta::{MetaStore, PackReferences};
use aspen_core::pack::{EntryTable, EntryTableError};
use aspen_pack::rewrite::rewrite_base_first;

use crate::{CompactError, Compactor, local_io};

/// What a `rewrite_pack` job did ([`Compactor::rewrite_pack`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewriteReport {
    /// Nothing to repair; nothing was written.
    Skipped(RewriteSkipped),
    /// The pack was rewritten base-first and replaced everywhere it was
    /// named.
    Rewritten {
        /// The base-first pack that now stands where the old one was named.
        new_pack: PackId,
        /// Entries whose position in the pack changed
        /// ([`aspen_pack::rewrite::RewrittenPack::moved`]).
        moved: u32,
        /// The rows that now name `new_pack` instead of the old pack.
        replaced: PackReferences,
    },
}

/// Why a `rewrite_pack` job had nothing to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteSkipped {
    /// No ref version, cover or shallow pack recording names the pack, so
    /// no fetch is served it: it was already replaced (a rerun of the job,
    /// or a job a fetch enqueued from a plan made just before the swap), or
    /// it is an orphan garbage collection will delete.
    Unreferenced,
    /// The pack's entry table already passes the base-first check.
    AlreadyBaseFirst,
}

impl<M, B> Compactor<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    /// The work of a `rewrite_pack` job: replaces `pack`, a stored pack
    /// with a delta before its base, with its base-first rewrite.
    ///
    /// It downloads the pack and its entry table, rewrites the pack
    /// base-first ([`aspen_pack::rewrite::rewrite_base_first`], which moves
    /// entries without recompressing anything and verifies the result),
    /// registers the result as a new `packs` row of the same repository
    /// and origin (so at the same level), uploads its pack, `.idx` and entry
    /// table to that row's keys, and then moves every ref version, cover
    /// and shallow pack recording from `pack` to the new row in one
    /// transaction ([`MetaStore::replace_pack`]): register, then upload,
    /// then reference.
    ///
    /// The new pack holds exactly the old one's objects, so every closure,
    /// plan and cover stays correct. It gets a new ID rather than new bytes
    /// under the old ID, so no in-process cache keyed by pack ID or checksum
    /// can pair the old `.idx` or offsets with the new bytes. The old pack
    /// keeps its row and blobs: this job's row names it, which keeps it from
    /// garbage collection, because a long-running push may still look bases
    /// up in it by ID.
    ///
    /// Idempotent, since a lease expiry or a requeue can run it again: a
    /// pack nothing names any more, or one that is already base-first,
    /// finishes without writing anything ([`RewriteReport::Skipped`]). A
    /// run that died after registering or uploading registers the same
    /// bytes again, which returns the same row.
    pub async fn rewrite_pack(&self, pack: PackId) -> Result<RewriteReport, CompactError> {
        let row = self
            .meta
            .packs(&[pack])
            .await?
            .pop()
            .ok_or_else(|| StoreError::NotFound {
                what: format!("pack {pack} of a rewrite_pack job"),
            })?;
        if self.meta.pack_references(pack).await?.is_empty() {
            return Ok(RewriteReport::Skipped(RewriteSkipped::Unreferenced));
        }
        let keys = row.keys();
        let entries = self.read_all(&keys.entries, row.pack.entries_size).await?;
        let table = EntryTable::decode_for(&entries, &row)?;
        match table.check_layout() {
            Ok(()) => return Ok(RewriteReport::Skipped(RewriteSkipped::AlreadyBaseFirst)),
            Err(EntryTableError::ForwardBase { .. }) => {}
            // A base outside the pack cannot be fixed by reordering it.
            Err(e) => {
                return Err(StoreError::Corrupt {
                    what: format!("pack {pack} of a rewrite_pack job: {e}"),
                }
                .into());
            }
        }

        let work = tempfile::Builder::new()
            .prefix("rewrite-")
            .tempdir_in(&self.config.spool_dir)
            .map_err(|e| local_io("creating a spool directory", e))?;
        let source = work.path().join("source.pack");
        self.fetch_to(&keys.pack, row.pack.size, &source).await?;
        let out_dir = work.path().join("out");
        std::fs::create_dir_all(&out_dir)
            .map_err(|e| local_io("creating the output directory", e))?;
        let (max_object, max_inflated) =
            (self.config.max_object_bytes, self.config.max_inflated_bytes);
        let written = tokio::task::spawn_blocking(move || {
            rewrite_base_first(&source, &table, &out_dir, max_object, max_inflated)
        })
        .await
        .map_err(|e| local_io("rewrite task", e))??;

        let record = self
            .meta
            .register_pack(&written.new_pack(&row.pack))
            .await?;
        let new_keys = record.keys();
        tokio::try_join!(
            self.blobs.upload_file(&new_keys.pack, &written.pack_path),
            self.blobs.upload_file(&new_keys.idx, &written.idx_path),
            self.blobs
                .upload_file(&new_keys.entries, &written.entries_path),
        )?;
        let replaced = self.meta.replace_pack(pack, record.id).await?;
        Ok(RewriteReport::Rewritten {
            new_pack: record.id,
            moved: written.moved,
            replaced,
        })
    }
}

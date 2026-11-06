//! Exact shallow packs (node C8): the pack a `git fetch --depth=N` of one
//! ref version needs, built once and recorded so later shallow fetches of
//! that tip are served from it instead of from the version's full closure.
//!
//! The caller (the fetch path in `aspen-server`) decides what the pack holds
//! and hands it over as a [`ShallowSpec`]: the version's tip (the want), the
//! **region** (the commits fewer than `depth` parents below the tip,
//! computed with the same breadth-first walk that writes `shallow-info`),
//! and the packs of the version's full closure (the planner's
//! `PlanMode::FullClosure` plan), which hold every object the region
//! reaches. [`Compactor::build_shallow_pack`] then:
//!
//! 1. downloads the closure's packs to the spool directory (refusing when
//!    they add up to more than [`ShallowSpec::max_source_bytes`]);
//! 2. computes the pack's object set from them
//!    ([`aspen_pack::shallow::shallow_objects`]): the want (and any tag
//!    objects on the way to its commit), every region commit, and every
//!    tree and blob the region commits' trees reach, never a parent outside
//!    the region;
//! 3. writes those objects with the compaction repack writer
//!    ([`aspen_pack::repack::write_repack_subset`]): first copy in plan
//!    order, compressed data copied unchanged, a delta whose base is not in
//!    the set stored whole, delta chains capped at
//!    [`crate::CompactConfig::max_delta_depth`];
//! 4. checks the result before anything is uploaded, as a cover's pack is
//!    checked: it verifies on its own (self-contained, base-first, no object
//!    twice), its `.idx` and entry table equal what verification rebuilds
//!    from the bytes, its depth is within the cap, and its objects are
//!    exactly the set from step 2;
//! 5. refuses a pack whose stored bytes alone exceed
//!    [`ShallowSpec::max_repo_bytes`] ([`ShallowBuild::OverLimit`]; nothing
//!    is registered or uploaded);
//! 6. registers it (a [`PackOrigin::Compaction`] pack at level 1, named by
//!    no cover), uploads its blobs to the registered row's keys, and only
//!    then records it with [`MetaStore::set_shallow_pack`] (register, then
//!    upload, then reference; decisions log, C1 pack generations), which
//!    evicts the repository's least recently used recordings to keep its
//!    recorded packs within `max_repo_bytes` (issue #108).
//!
//! When the object set is the whole closure (a young repository, or a depth
//! past the root), there is nothing to save: the build records nothing and
//! reports [`ShallowBuild::NotNeeded`], and the fetch path keeps sending the
//! closure, which is then exact.
//!
//! Building is idempotent and safe to run on several servers at once: the
//! same sources in the same order give the same bytes, registration and
//! uploads of the same pack are no-ops, and the first recording wins.
//!
//! [`PackOrigin::Compaction`]: aspen_core::pack::PackOrigin::Compaction

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, PackId, RefVersionId, RepoId};
use aspen_core::meta::{MetaStore, ShallowDepth, ShallowRecorded};
use aspen_core::pack::{CoverLevel, PackRecord};
use aspen_pack::idx::IdxV2;
use aspen_pack::repack::{RepackOptions, RepackSource, RepackedPack, write_repack_subset};
use aspen_pack::shallow::{ShallowRoots, shallow_objects};
use aspen_pack::verify::verify_pack;

use crate::build::LocalSource;
use crate::{CompactConfig, CompactError, Compactor, local_io};

/// What to build: one ref version's exact pack at one depth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShallowSpec {
    /// Repository.
    pub repo: RepoId,
    /// The version the pack is recorded for. Its tip is the want.
    pub version: RefVersionId,
    /// The depth (`deepen <n>`).
    pub depth: ShallowDepth,
    /// The version's tip: a commit, or an annotated tag that peels to one.
    pub want: ObjectId,
    /// The region: every commit fewer than `depth` parents below the
    /// want's commit, that commit included. Its parents beyond the region
    /// are the client's boundary and are not sent.
    pub region: Vec<ObjectId>,
    /// Packs that together hold every object the region reaches, in plan
    /// order: the version's full closure, or (when [`ShallowSpec::closure`]
    /// is false) a smaller set the caller expects to hold them, such as the
    /// previous version's exact pack plus the packs pushed since. A source
    /// set that misses an object fails the build with
    /// [`aspen_pack::LayoutError::Mismatch`], and the caller retries with
    /// the full closure.
    pub sources: Vec<PackId>,
    /// Whether [`ShallowSpec::sources`] is the version's full closure. Only
    /// then does a pack holding every source object mean the closure is
    /// already exact ([`ShallowBuild::NotNeeded`]); otherwise that pack is
    /// written and recorded like any other.
    pub closure: bool,
    /// The most the sources may add up to (pack bytes); a larger closure is
    /// not built ([`ShallowBuild::TooLarge`]).
    pub max_source_bytes: u64,
    /// The most objects the pack may hold.
    pub max_objects: usize,
    /// The most bytes the repository's recorded shallow packs may take
    /// together ([`MetaStore::set_shallow_pack`]); a pack larger than this
    /// on its own is not stored ([`ShallowBuild::OverLimit`]).
    pub max_repo_bytes: u64,
}

/// How a build ended, short of an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShallowBuild {
    /// The pack is recorded for the version and depth. The row is the one
    /// the store holds, which is another build's when that one recorded
    /// first.
    Recorded {
        /// The recorded pack's row.
        pack: PackRecord,
        /// Recordings evicted to keep the repository within
        /// [`ShallowSpec::max_repo_bytes`].
        evicted: u64,
    },
    /// The region reaches every object of the closure: sending the closure
    /// is already exact, so nothing was stored.
    NotNeeded,
    /// The closure's packs add up to more than the limit; nothing was
    /// downloaded.
    TooLarge {
        /// Bytes of the closure's packs.
        bytes: u64,
    },
    /// The pack alone takes more than [`ShallowSpec::max_repo_bytes`]:
    /// nothing was recorded, and the fetch is sent the full closure.
    OverLimit {
        /// The pack's stored bytes (pack, `.idx` and entry table).
        bytes: u64,
        /// The repository's cap.
        limit: u64,
    },
}

impl<M, B> Compactor<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    /// Builds, verifies, stores and records the exact shallow pack of
    /// `spec` (see the module docs). Errors are a store failure, a local
    /// disk error, or a pack that fails its checks (nothing is uploaded
    /// then); [`CompactError::is_corruption`] tells which ones page.
    pub async fn build_shallow_pack(
        &self,
        spec: &ShallowSpec,
    ) -> Result<ShallowBuild, CompactError> {
        let rows = self.meta.packs(&spec.sources).await?;
        let bytes: u64 = rows.iter().map(|r| r.pack.size).sum();
        if bytes > spec.max_source_bytes {
            return Ok(ShallowBuild::TooLarge { bytes });
        }
        std::fs::create_dir_all(&self.config.spool_dir)
            .map_err(|e| local_io("creating the spool directory", e))?;
        let work = tempfile::Builder::new()
            .prefix("shallow-")
            .tempdir_in(&self.config.spool_dir)
            .map_err(|e| local_io("creating a spool directory", e))?;
        let mut local = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            if row.pack.repo != spec.repo {
                return Err(StoreError::Corrupt {
                    what: format!(
                        "pack {} of the closure of version {} is in another repository",
                        row.id, spec.version
                    ),
                }
                .into());
            }
            local.push(self.download(row, work.path(), i).await?);
        }
        let config = self.config.clone();
        let out_dir = work.path().join("out");
        let roots = ShallowRoots {
            wants: vec![spec.want],
            commits: spec.region.clone(),
        };
        let max_objects = spec.max_objects;
        let closure = spec.closure;
        let written = tokio::task::spawn_blocking(move || {
            write_and_check(&local, &roots, max_objects, closure, &out_dir, &config)
        })
        .await
        .map_err(|e| local_io("shallow pack task", e))??;
        let Some(written) = written else {
            return Ok(ShallowBuild::NotNeeded);
        };

        let level = CoverLevel::new(1).expect("level 1 exists");
        let new = written.new_pack(spec.repo, level);
        // Checked again, with the bytes already recorded, by
        // `set_shallow_pack`; here it saves the upload of a pack that could
        // never be recorded.
        if new.stored_bytes() > spec.max_repo_bytes {
            return Ok(ShallowBuild::OverLimit {
                bytes: new.stored_bytes(),
                limit: spec.max_repo_bytes,
            });
        }
        let record = match self.meta.pack_by_checksum(spec.repo, &new.checksum).await? {
            // The same bytes are already stored at another level: a push
            // pack (or a cover's pack) that holds exactly this set. It is
            // as exact as a new row would be, and registering ours would be
            // a conflict, so record that row. Its blobs are uploaded again
            // (a no-op when present: the same keys and sizes) in case its
            // writer died before uploading.
            Some(existing) if existing.pack.level() != level.into() => existing,
            // Register, then upload, then record (decisions log, C1 pack
            // generations). Registering an existing row refreshes its
            // registration time, so the sweep leaves it alone meanwhile.
            _ => self.meta.register_pack(&new).await?,
        };
        let keys = record.keys();
        self.blobs
            .upload_file(&keys.pack, &written.pack_path)
            .await?;
        self.blobs.upload_file(&keys.idx, &written.idx_path).await?;
        self.blobs
            .upload_file(&keys.entries, &written.entries_path)
            .await?;
        let recorded = self
            .meta
            .set_shallow_pack(
                spec.repo,
                spec.version,
                spec.depth,
                record.id,
                spec.max_repo_bytes,
            )
            .await?;
        Ok(match recorded {
            ShallowRecorded::Recorded { pack, evicted } => ShallowBuild::Recorded { pack, evicted },
            ShallowRecorded::OverLimit { bytes, limit } => ShallowBuild::OverLimit { bytes, limit },
        })
    }
}

/// Computes the object set, and writes and checks the pack holding exactly
/// it; `None` when the sources are the full closure and the set is every
/// object of them.
fn write_and_check(
    sources: &[LocalSource],
    roots: &ShallowRoots,
    max_objects: usize,
    closure: bool,
    out_dir: &Path,
    config: &CompactConfig,
) -> Result<Option<RepackedPack>, CompactError> {
    std::fs::create_dir_all(out_dir).map_err(|e| local_io("creating the output directory", e))?;
    let refs: Vec<RepackSource<'_>> = sources
        .iter()
        .map(|s| RepackSource {
            pack: &s.pack,
            idx: &s.idx,
            table: &s.table,
        })
        .collect();
    let keep: HashSet<ObjectId> =
        shallow_objects(&refs, roots, config.max_object_bytes, max_objects)?;

    if closure {
        let mut union = BTreeSet::<ObjectId>::new();
        for s in sources {
            let bytes = std::fs::read(&s.idx).map_err(|e| local_io("reading a source .idx", e))?;
            let parsed = IdxV2::parse(&bytes, Some(s.size)).map_err(|e| StoreError::Corrupt {
                what: format!("a source .idx: {e}"),
            })?;
            union.extend(parsed.ids());
        }
        if keep.len() == union.len() {
            // `shallow_objects` only returns objects the sources hold, so
            // equal sizes mean equal sets.
            return Ok(None);
        }
    }

    let options = RepackOptions {
        max_delta_depth: config.max_delta_depth,
        max_tree_delta_depth: config.max_tree_delta_depth,
        max_object_bytes: config.max_object_bytes,
    };
    let written = write_repack_subset(&refs, &keep, out_dir, &options)?;

    let verified = verify_pack(
        &written.pack_path,
        out_dir,
        config.max_object_bytes,
        config.max_inflated_bytes,
    )?;
    let idx = std::fs::read(&written.idx_path).map_err(|e| local_io("reading the new .idx", e))?;
    if verified.idx != idx {
        return Err(CompactError::Verify(
            "the shallow pack's .idx differs from the one the pack itself gives".into(),
        ));
    }
    let entries = std::fs::read(&written.entries_path)
        .map_err(|e| local_io("reading the new entry table", e))?;
    if verified.table.encode() != entries {
        return Err(CompactError::Verify(
            "the shallow pack's entry table differs from the pack's own entries".into(),
        ));
    }
    if verified.max_delta_depth > config.max_delta_depth {
        return Err(CompactError::Verify(format!(
            "shallow pack delta depth {} is over the bound {}",
            verified.max_delta_depth, config.max_delta_depth
        )));
    }
    if verified.max_tree_delta_depth > config.max_tree_delta_depth {
        return Err(CompactError::Verify(format!(
            "shallow pack tree delta depth {} is over the bound {}",
            verified.max_tree_delta_depth, config.max_tree_delta_depth
        )));
    }
    let out = IdxV2::parse(&idx, Some(written.size)).map_err(|e| StoreError::Corrupt {
        what: format!("the new .idx: {e}"),
    })?;
    let mut expected: Vec<ObjectId> = keep.into_iter().collect();
    expected.sort_unstable();
    if out.len() != expected.len() || !out.ids().eq(expected.iter().copied()) {
        return Err(CompactError::Verify(format!(
            "the shallow pack holds {} objects, its object set {}",
            out.len(),
            expected.len()
        )));
    }
    Ok(Some(written))
}

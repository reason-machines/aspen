//! Building covers: their ranges, their chains and their compaction packs.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use aspen_core::blob::{BlobStore, ByteRange};
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, PackId, RefVersionId, RepoId, Seq};
use aspen_core::meta::{Cover, MetaStore, NewCover, SeqRange, VerifiedRange};
use aspen_core::name::RefName;
use aspen_core::pack::{CoverLevel, EntryTable, PackRecord};
use aspen_core::refs::RefVersion;
use aspen_pack::idx::IdxV2;
use aspen_pack::repack::{RepackOptions, RepackSource, RepackedPack, write_repack};
use aspen_pack::verify::verify_pack;
use tokio::io::AsyncWriteExt;

use crate::{CompactConfig, CompactError, local_io};

/// Bytes per blob read while downloading a source.
const READ_CHUNK: u64 = 8 << 20;

/// What one call did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactReport {
    /// Covers created (rows that did not exist).
    pub covers_created: usize,
    /// Of those, covers that carry a chain.
    pub chains_recorded: usize,
    /// Compaction packs written, verified, registered, uploaded and
    /// recorded on their cover.
    pub packs_written: usize,
    /// Covers left without a pack because their range has fewer than two
    /// distinct source packs (nothing to merge), or because the repack is
    /// byte-identical to an existing pack at another level.
    pub covers_without_pack: usize,
    /// Objects in the packs written.
    pub objects_written: u64,
    /// Entries stored undeltified to keep the delta depth bound.
    pub undeltified: u64,
}

impl CompactReport {
    fn add(&mut self, o: &CompactReport) {
        self.covers_created += o.covers_created;
        self.chains_recorded += o.chains_recorded;
        self.packs_written += o.packs_written;
        self.covers_without_pack += o.covers_without_pack;
        self.objects_written += o.objects_written;
        self.undeltified += o.undeltified;
    }
}

/// The compactor: builds covers and their packs through the `aspen-core`
/// store traits.
#[derive(Debug, Clone)]
pub struct Compactor<M, B> {
    pub(crate) meta: M,
    pub(crate) blobs: B,
    pub(crate) config: CompactConfig,
}

impl<M, B> Compactor<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    /// A compactor over these stores.
    pub fn new(meta: M, blobs: B, config: CompactConfig) -> Self {
        Self {
            meta,
            blobs,
            config,
        }
    }

    /// The configuration.
    pub fn config(&self) -> &CompactConfig {
        &self.config
    }

    /// The work of a `compact` job: every cover of the version's ref that
    /// ends at the last level-1 boundary at or below the version's `seq`
    /// (usually the version itself, since the push path enqueues the job
    /// exactly there), with the covers below them.
    pub async fn compact_version(
        &self,
        version: RefVersionId,
    ) -> Result<CompactReport, CompactError> {
        let v = self
            .meta
            .ref_version(version)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                what: format!("ref version {version} of a compact job"),
            })?;
        let fanout = self.config.layout.fanout();
        let end = v.seq.get() - v.seq.get() % fanout;
        let mut refs = RefCovers::load(&self.meta, v.repo, &v.name).await?;
        let mut report = CompactReport::default();
        if let Ok(end) = Seq::new(end) {
            self.ensure_ending_at(&mut refs, end, &mut report).await?;
        }
        Ok(report)
    }

    /// Builds every cover of `name` that ends at or below `through`: for
    /// repositories that predate the compactor, or after `compact` jobs were
    /// lost.
    pub async fn compact_through(
        &self,
        repo: RepoId,
        name: &RefName,
        through: Seq,
    ) -> Result<CompactReport, CompactError> {
        let fanout = self.config.layout.fanout();
        let mut refs = RefCovers::load(&self.meta, repo, name).await?;
        let mut report = CompactReport::default();
        let mut end = fanout;
        while end <= through.get() {
            let seq = Seq::new(end).expect("in range");
            self.ensure_ending_at(&mut refs, seq, &mut report).await?;
            end += fanout;
        }
        Ok(report)
    }

    async fn ensure_ending_at(
        &self,
        refs: &mut RefCovers,
        end: Seq,
        report: &mut CompactReport,
    ) -> Result<(), CompactError> {
        for (level, range) in self.config.layout.ending_at(end) {
            let mut r = CompactReport::default();
            self.ensure_cover(refs, level, range, &mut r).await?;
            report.add(&r);
        }
        Ok(())
    }

    /// Makes sure the cover at `level` over `range` exists, carries its
    /// chain if it has one, and has its pack if it should have one. The
    /// covers one level below are ensured first.
    fn ensure_cover<'a>(
        &'a self,
        refs: &'a mut RefCovers,
        level: CoverLevel,
        range: SeqRange,
        report: &'a mut CompactReport,
    ) -> Pin<Box<dyn Future<Output = Result<Cover, CompactError>> + Send + 'a>>
    where
        M: Sync,
        B: Sync,
    {
        Box::pin(async move {
            let parts = self.config.layout.parts(level, range);
            for &(l, r) in &parts {
                self.ensure_cover(refs, l, r, report).await?;
            }
            let cover = match refs.cover(level, range.first()) {
                Some(c) if c.range == range => c.clone(),
                Some(c) => {
                    return Err(StoreError::Conflict {
                        what: format!(
                            "{} L{} cover at {} spans {}..={}, not the layout's {}..={}",
                            refs.name,
                            level.get(),
                            range.first(),
                            c.range.first(),
                            c.range.last(),
                            range.first(),
                            range.last()
                        ),
                    }
                    .into());
                }
                None => {
                    let chain = self.chain_of(refs, level, range, &parts).await?;
                    let cover = self
                        .meta
                        .insert_cover(&NewCover {
                            repo: refs.repo,
                            name: refs.name.clone(),
                            level,
                            range,
                            chain: chain.map(|c| c.chain()),
                        })
                        .await?;
                    report.covers_created += 1;
                    report.chains_recorded += usize::from(cover.chain.is_some());
                    refs.put(cover.clone());
                    cover
                }
            };
            if cover.pack.is_some() {
                return Ok(cover);
            }
            let sources = self.sources(refs, level, range).await?;
            if sources.len() < 2 {
                report.covers_without_pack += 1;
                return Ok(cover);
            }
            let cover = match self.repack(refs.repo, &cover, &sources, report).await? {
                Some(c) => c,
                None => {
                    report.covers_without_pack += 1;
                    cover
                }
            };
            refs.put(cover.clone());
            Ok(cover)
        })
    }

    /// The chain of a new cover, if its range is a dependency chain: checked
    /// version by version at level 1, and above by joining the chains of the
    /// covers below, reading only the first version of each one after the
    /// first.
    async fn chain_of(
        &self,
        refs: &mut RefCovers,
        level: CoverLevel,
        range: SeqRange,
        parts: &[(CoverLevel, SeqRange)],
    ) -> Result<Option<VerifiedRange>, CompactError> {
        if level.get() == 1 {
            let versions = refs.versions(&self.meta, range).await?;
            return Ok(VerifiedRange::verify(&refs.name, range, &versions).ok());
        }
        let mut chained = Vec::with_capacity(parts.len());
        for &(l, r) in parts {
            match refs.cover(l, r.first()).and_then(VerifiedRange::of_cover) {
                Some(v) => chained.push(v),
                None => return Ok(None),
            }
        }
        let junctions: Vec<Seq> = chained.iter().skip(1).map(|c| c.range().first()).collect();
        let found = self
            .meta
            .versions_by_seq(refs.repo, &refs.name, &junctions)
            .await?;
        let mut chained = chained.into_iter();
        let mut acc = chained.next().expect("a level above 1 has parts");
        for upper in chained {
            let Some(junction) = found.iter().find(|v| v.seq == upper.range().first()) else {
                return Ok(None);
            };
            match acc.join(&upper, junction) {
                Ok(j) => acc = j,
                Err(_) => return Ok(None),
            }
        }
        Ok(Some(acc))
    }

    /// The distinct packs whose union is the objects of the range's
    /// versions, in `seq` order: the packs of the covers one level below
    /// where they have one, else (recursively) what those covers are made
    /// of, down to the versions' own packs.
    fn sources<'a>(
        &'a self,
        refs: &'a mut RefCovers,
        level: CoverLevel,
        range: SeqRange,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PackId>, CompactError>> + Send + 'a>>
    where
        M: Sync,
        B: Sync,
    {
        Box::pin(async move {
            let mut out = Vec::new();
            if level.get() == 1 {
                for v in refs.versions(&self.meta, range).await? {
                    out.extend(v.value.pack());
                }
            } else {
                for (l, r) in self.config.layout.parts(level, range) {
                    match refs.cover(l, r.first()).and_then(|c| c.pack) {
                        Some(p) => out.push(p),
                        None => out.extend(self.sources(refs, l, r).await?),
                    }
                }
            }
            let mut seen = BTreeSet::new();
            out.retain(|p| seen.insert(*p));
            Ok(out)
        })
    }

    /// Repacks `sources` into the cover's pack and records it. `None` when
    /// the result is byte-identical to a pack registered at another level,
    /// which a cover cannot name (its pack must be at the cover's level);
    /// the planner then sends the cover's sources, which hold the same
    /// objects.
    async fn repack(
        &self,
        repo: RepoId,
        cover: &Cover,
        sources: &[PackId],
        report: &mut CompactReport,
    ) -> Result<Option<Cover>, CompactError> {
        let rows = self.meta.packs(sources).await?;
        let work = tempfile::Builder::new()
            .prefix("compact-")
            .tempdir_in(&self.config.spool_dir)
            .map_err(|e| local_io("creating a spool directory", e))?;
        let mut local = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            if row.pack.repo != repo {
                return Err(StoreError::Corrupt {
                    what: format!("pack {} of a cover is in another repository", row.id),
                }
                .into());
            }
            local.push(self.download(row, work.path(), i).await?);
        }
        let options = RepackOptions {
            max_delta_depth: self.config.max_delta_depth,
            max_object_bytes: self.config.max_object_bytes,
        };
        let config = self.config.clone();
        let out_dir = work.path().join("out");
        let level = cover.level;
        let written = tokio::task::spawn_blocking(move || {
            write_and_check(&local, &out_dir, &options, &config)
        })
        .await
        .map_err(|e| local_io("repack task", e))??;

        let new = written.new_pack(repo, level);
        // A row with this checksum at another level is some other pack's
        // (a push pack that happens to hold exactly these objects); leave
        // the cover for a later pass.
        let existing = self.meta.pack_by_checksum(repo, &new.checksum).await?;
        if existing
            .as_ref()
            .is_some_and(|e| e.pack.level() != level.into())
        {
            return Ok(None);
        }
        // Register, then upload, then record: the row fixes the keys (its
        // generation), and registering refreshes the pack's registration
        // time, so an old orphan with these bytes is not swept while the
        // cover is recorded. The blobs are uploaded even when the row
        // already existed, because whoever registered it may have died
        // before uploading (issue #48).
        let record = self.meta.register_pack(&new).await?;
        let keys = record.keys();
        self.blobs
            .upload_file(&keys.pack, &written.pack_path)
            .await?;
        self.blobs.upload_file(&keys.idx, &written.idx_path).await?;
        self.blobs
            .upload_file(&keys.entries, &written.entries_path)
            .await?;
        if existing.is_none() {
            report.packs_written += 1;
            report.objects_written += u64::from(written.object_count);
            report.undeltified += u64::from(written.undeltified);
        }
        let pack_id = record.id;
        let cover = self.meta.set_cover_pack(cover.id, pack_id).await?;
        Ok(Some(cover))
    }

    /// Downloads a source pack, its `.idx` and its entry table into `dir`.
    pub(crate) async fn download(
        &self,
        row: &PackRecord,
        dir: &Path,
        i: usize,
    ) -> Result<LocalSource, CompactError> {
        let keys = row.keys();
        let pack = dir.join(format!("{i}.pack"));
        let idx = dir.join(format!("{i}.idx"));
        self.fetch_to(&keys.pack, row.pack.size, &pack).await?;
        self.fetch_to(&keys.idx, row.pack.idx_size, &idx).await?;
        let entries = self.read_all(&keys.entries, row.pack.entries_size).await?;
        let table = EntryTable::decode_for(&entries, row)?;
        Ok(LocalSource {
            pack,
            idx,
            table,
            size: row.pack.size,
        })
    }

    pub(crate) async fn fetch_to(
        &self,
        key: &aspen_core::blob::BlobKey,
        size: u64,
        path: &Path,
    ) -> Result<(), CompactError> {
        let mut file = tokio::fs::File::create(path)
            .await
            .map_err(|e| local_io(&format!("creating {}", path.display()), e))?;
        let mut at = 0;
        while at < size {
            let end = (at + READ_CHUNK).min(size);
            let range = ByteRange::new(at, end).expect("non-empty");
            let bytes = self.blobs.read_exact_range(key, range).await?;
            file.write_all(&bytes)
                .await
                .map_err(|e| local_io(&format!("writing {}", path.display()), e))?;
            at = end;
        }
        file.flush()
            .await
            .map_err(|e| local_io(&format!("writing {}", path.display()), e))?;
        Ok(())
    }

    async fn read_all(
        &self,
        key: &aspen_core::blob::BlobKey,
        size: u64,
    ) -> Result<Vec<u8>, CompactError> {
        let mut out = Vec::new();
        let mut at = 0;
        while at < size {
            let end = (at + READ_CHUNK).min(size);
            let range = ByteRange::new(at, end).expect("non-empty");
            out.extend(self.blobs.read_exact_range(key, range).await?);
            at = end;
        }
        Ok(out)
    }
}

/// A source pack downloaded to the spool.
#[derive(Debug)]
pub(crate) struct LocalSource {
    pub(crate) pack: PathBuf,
    pub(crate) idx: PathBuf,
    pub(crate) table: EntryTable,
    pub(crate) size: u64,
}

/// Writes the compaction pack and checks it before anything is uploaded:
/// it must verify on its own (self-contained, base-first, no repeats), its
/// `.idx` and entry table must equal what verification rebuilds from the
/// pack bytes alone, its depth must be within the bound, and its objects
/// must be exactly the union of the sources' `.idx` files.
fn write_and_check(
    sources: &[LocalSource],
    out_dir: &Path,
    options: &RepackOptions,
    config: &CompactConfig,
) -> Result<RepackedPack, CompactError> {
    std::fs::create_dir_all(out_dir).map_err(|e| local_io("creating the output directory", e))?;
    let refs: Vec<RepackSource<'_>> = sources
        .iter()
        .map(|s| RepackSource {
            pack: &s.pack,
            idx: &s.idx,
            table: &s.table,
        })
        .collect();
    let written = write_repack(&refs, out_dir, options)?;

    let verified = verify_pack(
        &written.pack_path,
        out_dir,
        config.max_object_bytes,
        config.max_inflated_bytes,
    )?;
    let idx = std::fs::read(&written.idx_path).map_err(|e| local_io("reading the new .idx", e))?;
    if verified.idx != idx {
        return Err(CompactError::Verify(
            "the written .idx differs from the one the pack itself gives".into(),
        ));
    }
    let entries = std::fs::read(&written.entries_path)
        .map_err(|e| local_io("reading the new entry table", e))?;
    if verified.table.encode() != entries {
        return Err(CompactError::Verify(
            "the written entry table differs from the pack's own entries".into(),
        ));
    }
    if verified.max_delta_depth > config.max_delta_depth {
        return Err(CompactError::Verify(format!(
            "delta depth {} is over the bound {}",
            verified.max_delta_depth, config.max_delta_depth
        )));
    }
    let mut union = BTreeSet::<ObjectId>::new();
    for s in sources {
        let bytes = std::fs::read(&s.idx).map_err(|e| local_io("reading a source .idx", e))?;
        let parsed = IdxV2::parse(&bytes, Some(s.size)).map_err(|e| StoreError::Corrupt {
            what: format!("a source .idx: {e}"),
        })?;
        union.extend(parsed.ids());
    }
    let out = IdxV2::parse(&idx, Some(written.size)).map_err(|e| StoreError::Corrupt {
        what: format!("the new .idx: {e}"),
    })?;
    if out.len() != union.len() || !out.ids().eq(union.iter().copied()) {
        return Err(CompactError::Verify(format!(
            "the pack holds {} objects, its sources {} distinct ones",
            out.len(),
            union.len()
        )));
    }
    Ok(written)
}

/// One ref's covers, and the versions read so far, for one call.
#[derive(Debug)]
struct RefCovers {
    repo: RepoId,
    name: RefName,
    covers: HashMap<(CoverLevel, Seq), Cover>,
    versions: HashMap<Seq, RefVersion>,
}

impl RefCovers {
    async fn load<M: MetaStore>(
        meta: &M,
        repo: RepoId,
        name: &RefName,
    ) -> Result<Self, StoreError> {
        let covers = meta
            .covers(repo, name)
            .await?
            .into_iter()
            .map(|c| ((c.level, c.range.first()), c))
            .collect();
        Ok(Self {
            repo,
            name: name.clone(),
            covers,
            versions: HashMap::new(),
        })
    }

    fn cover(&self, level: CoverLevel, first: Seq) -> Option<&Cover> {
        self.covers.get(&(level, first))
    }

    fn put(&mut self, cover: Cover) {
        self.covers
            .insert((cover.level, cover.range.first()), cover);
    }

    /// The versions of the range, in `seq` order; every one must exist (a
    /// cover is only built over versions that were committed).
    async fn versions<M: MetaStore>(
        &mut self,
        meta: &M,
        range: SeqRange,
    ) -> Result<Vec<RefVersion>, StoreError> {
        let missing: Vec<Seq> = (range.first().get()..=range.last().get())
            .map(|s| Seq::new(s).expect("in range"))
            .filter(|s| !self.versions.contains_key(s))
            .collect();
        if !missing.is_empty() {
            for v in meta
                .versions_by_seq(self.repo, &self.name, &missing)
                .await?
            {
                self.versions.insert(v.seq, v);
            }
        }
        (range.first().get()..=range.last().get())
            .map(|s| {
                let seq = Seq::new(s).expect("in range");
                self.versions
                    .get(&seq)
                    .cloned()
                    .ok_or_else(|| StoreError::Corrupt {
                        what: format!("{} has no version {seq} below a later one", self.name),
                    })
            })
            .collect()
    }
}

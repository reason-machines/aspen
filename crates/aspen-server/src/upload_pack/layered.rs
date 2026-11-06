//! Layered answers: a shallow fetch of a tip whose exact pack is not built
//! yet is served the exact pack recorded for an earlier version of the same
//! ref at the same depth, without that version's own commits, plus a small
//! patch pack of everything else the new tip's region needs (decisions log,
//! "Layered shallow answers").
//!
//! Without this, a `git fetch --depth=1` of a new trunk tip waits for its
//! exact pack (`super::exact`), and a build is proportional to the
//! snapshot, not to what changed: on a 300,000-file trunk 10 to 20 s per
//! tip, longer than a fetch should wait, after which the fetch was sent the
//! full history (1.7 GB and about 3 GB of server memory per clone on the
//! monorepo benchmark). Trunk moves on every landing, so most depth-1 clones
//! of a busy trunk asked for a tip that had no pack yet.
//!
//! # What is sent
//!
//! For want `W`, the tip of version `V` of ref `R`, at depth `d`, one
//! stitched pack (never packfile URIs) of:
//!
//! - **patch**, first: every object `W`'s region reaches that is not a tree
//!   or blob of the base, written whole: `W`'s commits and tags, the trees
//!   and blobs changed since the base, and anything only older history
//!   holds (a revert), read from the packs pushed since or from `V`'s
//!   closure. Registered and uploaded like any pack (a level-1 compaction
//!   pack that no cover or recording names, so garbage collection sweeps
//!   it a grace period after its last registration);
//! - **base**: the recorded exact pack of `U`, the newest earlier version
//!   of `R` (at most [`MAX_LAYERED_VERSIONS`] back) that has one at depth
//!   `d`, with **its commits and tags left out**
//!   (`aspen_pack::stitch::StitchPlan::leaving_out`).
//!
//! Every object sent is then something a client checking links
//! (`fetch.fsckObjects`, `index-pack --strict`) can resolve: the base's
//! trees and blobs are closed under reachability (an exact pack holds every
//! tree and blob below each tree it holds), the patch's trees name only
//! objects of the patch or the base, and the only commits are `W`'s region,
//! whose parents outside the region are the client's shallow boundary.
//! Sending `U`'s commits, or the intermediate commits and trees of the
//! pushes since, would not be: their parents and entries are not sent, and
//! a strict client refuses the pack ("did not receive expected object").
//! The base's trees and blobs that `W` no longer reaches are extra bytes
//! (at most one snapshot plus what changed), never missing links. Nothing
//! kept can be a delta against a left-out entry: a delta's base has the
//! delta's type, and only trees and blobs of the base are kept.
//!
//! # Why the check is cheap
//!
//! The walk that finds the patch stops at any tree or blob the base holds,
//! so it reads only the commits and trees that changed since `U` (about
//! the tree's depth times the files changed), one batch per tree level.
//! Blobs are looked up in the `.idx` files and read only when they go in
//! the patch.
//!
//! # Bounds
//!
//! Objects read per tree level, and the patch's objects, are each bounded
//! by [`super::exact::ShallowCacheConfig::max_layered_bytes`]; over it there
//! is no layered answer and the fetch waits for its build as before. The
//! base's commits are found from its entry table, remembered per pack.
//! Serving the answer is an ordinary stitch of two stored packs.

use std::collections::{HashMap, HashSet};

use std::sync::Arc;

use aspen_core::blob::{BlobStore, ByteRange};
use aspen_core::error::{FetchError, StoreError};
use aspen_core::id::{ObjectId, PackId, RefVersionId, Seq};
use aspen_core::ingest::{
    BaseLookup, CandidateBases, ObjectKind, ObjectLocator, SearchPacks, SortedIds,
};
use aspen_core::meta::{MetaStore, ShallowDepth};
use aspen_core::pack::{
    CoverLevel, EntryKind, EntryTable, NewPack, PackOrigin, PackRecord, entry_table_len,
};
use aspen_core::refs::RefVersion;
use aspen_core::repo::Repo;
use aspen_pack::locate::PrefetchError;
use aspen_pack::subset::{ExtraObject, write_subset};
use aspen_pack::verify::verify_pack;

use super::Failure;
use super::exact::ShallowCache;
use super::resolve::Want;
use crate::Server;

/// How far back along a ref's versions a layered answer looks for a base,
/// and so the most increments it sends.
pub(crate) const MAX_LAYERED_VERSIONS: u64 = 256;

/// Versions probed for a recorded pack when no hint names one: one
/// `shallow_pack` read each, newest first.
const PROBE_VERSIONS: u64 = 64;

/// A layered answer for one want.
#[derive(Debug)]
pub(crate) struct Layered {
    /// The patch pack if any, then the base.
    pub(crate) rows: Vec<PackRecord>,
    /// The base's commits and tags that the patch does not hold: every copy
    /// is left out of the stitched pack. Sorted.
    pub(crate) leave_out: Vec<ObjectId>,
    /// The want's region (as for its exact pack).
    pub(crate) region: Vec<ObjectId>,
    /// The version whose recorded pack is the base.
    pub(crate) base: RefVersionId,
    /// Versions after the base (whose packs the patch is read from).
    pub(crate) increments: usize,
    /// Objects in the patch pack.
    pub(crate) patched: usize,
}

/// The layered answer for `want` (the tip of `version`) at `depth`, or
/// `None` when there is none: no recorded base within reach, a region or
/// patch over the limits, or an object the version's closure lacks.
pub(crate) async fn compute<M, B>(
    server: &Server<M, B>,
    cache: &ShallowCache<M, B>,
    repo: &Repo,
    want: &Want,
    version: &RefVersion,
    depth: ShallowDepth,
) -> Result<Option<Layered>, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let Some((base, base_version, after)) = find_base(server, cache, repo, version, depth).await?
    else {
        return Ok(None);
    };
    let mut ids = vec![base.id];
    for v in &after {
        if let Some(p) = v.value.pack()
            && !ids.contains(&p)
        {
            ids.push(p);
        }
    }
    let Some(left) = base_commits(server, cache, &base).await? else {
        return Ok(None);
    };
    let Ok(Some(region)) = super::exact::region(server, repo, want, depth, ids.clone()).await
    else {
        return Ok(None);
    };
    let limit = cache.config().max_layered_bytes;
    let search = SearchPacks::new(ids);
    let mut walk = Walk {
        server,
        repo,
        want,
        version: version.id,
        search,
        base: base.id,
        base_commits: &left,
        closure: None,
        limit,
        patch: Vec::new(),
        patch_bytes: 0,
    };
    let mut roots: HashSet<ObjectId> = region.iter().copied().collect();
    roots.insert(want.id);
    if !walk.run(roots).await? {
        return Ok(None);
    }
    let patched = walk.patch.len();
    let patch_ids: HashSet<ObjectId> = walk.patch.iter().map(|o| o.id).collect();
    let leave_out: Vec<ObjectId> = left
        .iter()
        .filter(|id| !patch_ids.contains(id))
        .copied()
        .collect();
    let mut rows = Vec::with_capacity(2);
    if patched > 0 {
        rows.push(write_patch(server, cache, repo, std::mem::take(&mut walk.patch)).await?);
    }
    rows.push(base);
    Ok(Some(Layered {
        rows,
        leave_out,
        region,
        base: base_version,
        increments: after.len(),
        patched,
    }))
}

/// The base, its version, and the versions after it (ascending, ending with `version`):
/// from the cache's hint when it names a recorded version within reach,
/// else by probing the nearest earlier versions newest first.
async fn find_base<M, B>(
    server: &Server<M, B>,
    cache: &ShallowCache<M, B>,
    repo: &Repo,
    version: &RefVersion,
    depth: ShallowDepth,
) -> Result<Option<(PackRecord, RefVersionId, Vec<RefVersion>)>, FetchError>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let meta = server.meta();
    let seq = version.seq.get();
    if seq <= 1 {
        return Ok(None);
    }
    if let Some((hseq, hid)) = cache.hint(repo.id, &version.name, depth)
        && hseq < seq
        && seq - hseq <= MAX_LAYERED_VERSIONS
        && let Some(row) = meta
            .shallow_pack(repo.id, hid, depth)
            .await
            .map_err(FetchError::Store)?
        && let Some(after) = between(server, repo, version, hseq).await?
    {
        return Ok(Some((row, hid, after)));
    }
    let lo = seq.saturating_sub(PROBE_VERSIONS).max(1);
    let seqs: Vec<Seq> = (lo..seq).filter_map(|s| Seq::new(s).ok()).collect();
    let mut earlier = meta
        .versions_by_seq(repo.id, &version.name, &seqs)
        .await
        .map_err(FetchError::Store)?;
    earlier.sort_by_key(|v| std::cmp::Reverse(v.seq.get()));
    for (i, v) in earlier.iter().enumerate() {
        if v.repo != repo.id || v.name != version.name {
            return Err(FetchError::Store(StoreError::Corrupt {
                what: format!("version {} is not of ref {}", v.id, version.name),
            }));
        }
        if let Some(row) = meta
            .shallow_pack(repo.id, v.id, depth)
            .await
            .map_err(FetchError::Store)?
        {
            cache.note_recorded(repo.id, v, depth);
            let mut after: Vec<RefVersion> = earlier[..i].iter().rev().cloned().collect();
            after.push(version.clone());
            if !contiguous(&after, v.seq.get()) {
                return Ok(None);
            }
            return Ok(Some((row, v.id, after)));
        }
    }
    Ok(None)
}

/// `version`'s ref's versions after `seq` up to `version`, ascending; `None`
/// when any is missing.
async fn between<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    version: &RefVersion,
    seq: u64,
) -> Result<Option<Vec<RefVersion>>, FetchError>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let seqs: Vec<Seq> = (seq + 1..version.seq.get())
        .filter_map(|s| Seq::new(s).ok())
        .collect();
    let mut after = if seqs.is_empty() {
        Vec::new()
    } else {
        server
            .meta()
            .versions_by_seq(repo.id, &version.name, &seqs)
            .await
            .map_err(FetchError::Store)?
    };
    after.sort_by_key(|v| v.seq.get());
    after.push(version.clone());
    Ok(contiguous(&after, seq).then_some(after))
}

/// Whether `after` is exactly the versions `base + 1 ..` in order.
fn contiguous(after: &[RefVersion], base: u64) -> bool {
    after
        .iter()
        .enumerate()
        .all(|(i, v)| v.seq.get() == base + 1 + i as u64)
}

/// The completeness walk (module docs, "Why the check is cheap").
struct Walk<'a, M, B> {
    server: &'a Server<M, B>,
    repo: &'a Repo,
    want: &'a Want,
    version: RefVersionId,
    search: SearchPacks,
    base: PackId,
    /// The base's commits and tags, which are not sent from the base.
    base_commits: &'a [ObjectId],
    /// The packs of `version`'s closure, once a patch needs them.
    closure: Option<SearchPacks>,
    limit: u64,
    patch: Vec<ExtraObject>,
    patch_bytes: u64,
}

impl<M, B> Walk<'_, M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// Walks from `roots`; `false` when the answer is over a limit or an
    /// object is in none of the packs (then there is no layered answer).
    async fn run(&mut self, roots: HashSet<ObjectId>) -> Result<bool, Failure> {
        let mut seen = roots.clone();
        let mut frontier = roots;
        while !frontier.is_empty() {
            let ids = SortedIds::from_unsorted(frontier.into_iter().collect());
            let found = self
                .server
                .objects()
                .locate(&self.search, &ids)
                .await
                .map_err(FetchError::Store)?;
            let mut here = Vec::new();
            let mut elsewhere = Vec::new();
            for (id, at) in ids.as_slice().iter().zip(found) {
                match at {
                    // The base's trees and blobs are closed under
                    // reachability and are sent; its commits and tags are
                    // not.
                    Some(at)
                        if at.pack == self.base && self.base_commits.binary_search(id).is_err() => {
                    }
                    Some(_) => here.push(*id),
                    None => elsewhere.push(*id),
                }
            }
            let mut next = HashSet::new();
            for (packs, ids) in [(None, here), (Some(()), elsewhere)] {
                if ids.is_empty() {
                    continue;
                }
                let packs = match packs {
                    None => self.search.clone(),
                    Some(()) => {
                        if self.closure.is_none() {
                            self.closure = Some(self.closure_packs().await?);
                        }
                        self.closure.clone().expect("set above")
                    }
                };
                let Some(read) = self.read(&packs, ids).await? else {
                    return Ok(false);
                };
                for (id, (kind, data)) in read {
                    expand(kind, &data, &mut next)?;
                    self.patch_bytes += data.len() as u64;
                    if self.patch_bytes > self.limit {
                        return Ok(false);
                    }
                    self.patch.push(ExtraObject { id, kind, data });
                }
            }
            next.retain(|id| seen.insert(*id));
            frontier = next;
        }
        Ok(true)
    }

    /// Full objects of `ids` from `packs`, under the limit; `None` when one
    /// is missing or they are over the limit.
    async fn read(
        &self,
        packs: &SearchPacks,
        ids: Vec<ObjectId>,
    ) -> Result<Option<HashMap<ObjectId, (ObjectKind, Vec<u8>)>>, Failure> {
        let wanted = SortedIds::from_unsorted(ids);
        let (fetched, _) = match self
            .server
            .objects()
            .prefetch_bases_within(packs, &CandidateBases(wanted.clone()), self.limit)
            .await
        {
            Ok(f) => f,
            Err(PrefetchError::OverBudget { .. }) => return Ok(None),
            Err(PrefetchError::Store(e)) => return Err(FetchError::Store(e).into()),
        };
        let mut out = HashMap::with_capacity(wanted.len());
        for id in wanted.as_slice() {
            let mut data = Vec::new();
            match fetched
                .bases
                .find_base(id, &mut data)
                .map_err(FetchError::Store)?
            {
                Some(kind) => {
                    out.insert(*id, (kind, data));
                }
                None => {
                    tracing::warn!(
                        repo = self.repo.name.as_str(),
                        version = self.version.get(),
                        object = %id,
                        "an object a shallow region reaches is in no pack of its closure"
                    );
                    return Ok(None);
                }
            }
        }
        Ok(Some(out))
    }

    /// The packs of the version's closure: from receive-pack's closure
    /// cache when the server takes pushes (cheap: every push computes its
    /// trunk's), else from a full-closure plan.
    async fn closure_packs(&self) -> Result<SearchPacks, Failure> {
        if let Some(receive) = self.server.receive_pack() {
            return Ok(receive
                .closure_of(self.repo.id, &[self.version])
                .await
                .map_err(FetchError::Store)?);
        }
        let (rows, _) = super::fetch::plan_quietly(
            self.server,
            self.repo,
            aspen_core::fetch::PlanMode::FullClosure,
            std::slice::from_ref(self.want),
            Vec::new(),
            super::graph::LoadedGraph::default(),
        )
        .await?;
        Ok(SearchPacks::new(rows.iter().map(|r| r.id)))
    }
}

/// Adds what a read object needs to `next`: a commit's tree (never its
/// parents: the region is the caller's), a tag's target, a tree's entries.
fn expand(kind: ObjectKind, data: &[u8], next: &mut HashSet<ObjectId>) -> Result<(), FetchError> {
    let corrupt = |why: String| {
        FetchError::Store(StoreError::Corrupt {
            what: format!("a stored {kind:?} does not parse: {why}"),
        })
    };
    match kind {
        ObjectKind::Blob => {}
        ObjectKind::Commit => {
            let tree = aspen_pack::scan::commit_tree(data).map_err(corrupt)?;
            next.insert(tree);
        }
        ObjectKind::Tag | ObjectKind::Tree => aspen_pack::scan::references(kind, data, |id| {
            next.insert(id);
        })
        .map_err(corrupt)?,
    }
    Ok(())
}

/// The commits and tags of `base` (sorted), from its entry table: a
/// delta's type is its base's, followed back through the chain. Remembered
/// per pack: an exact pack is the base of every later version until the
/// next one is recorded. `None` when the table cannot be read.
async fn base_commits<M, B>(
    server: &Server<M, B>,
    cache: &ShallowCache<M, B>,
    base: &PackRecord,
) -> Result<Option<Arc<Vec<ObjectId>>>, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    if let Some(known) = cache.base_commits(base.id) {
        return Ok(Some(known));
    }
    let Some(range) = ByteRange::new(0, base.pack.entries_size) else {
        return Ok(None);
    };
    let bytes = server
        .blobs()
        .read_exact_range(&base.keys().entries, range)
        .await
        .map_err(FetchError::Store)?;
    let table = EntryTable::decode_for(&bytes, base).map_err(FetchError::Store)?;
    let entries = table.entries();
    let by_id: HashMap<ObjectId, usize> = if table.ref_delta_count() > 0 {
        entries.iter().enumerate().map(|(i, e)| (e.id, i)).collect()
    } else {
        HashMap::new()
    };
    // Resolved type per entry; stored packs are base-first, so one pass.
    let mut commit_like = vec![false; entries.len()];
    let mut out = Vec::new();
    for (i, e) in entries.iter().enumerate() {
        let base_of = match e.kind {
            EntryKind::Commit | EntryKind::Tag => {
                commit_like[i] = true;
                out.push(e.id);
                continue;
            }
            EntryKind::Tree | EntryKind::Blob => continue,
            EntryKind::OfsDelta { base_offset } => entries
                .binary_search_by_key(&base_offset, |b| b.offset)
                .ok(),
            EntryKind::RefDelta { base } => by_id.get(&base).copied(),
        };
        match base_of {
            Some(b) if b < i => {
                if commit_like[b] {
                    commit_like[i] = true;
                    out.push(e.id);
                }
            }
            // Not base-first: the stitcher refuses this pack anyway.
            _ => return Ok(None),
        }
    }
    out.sort_unstable();
    let out = Arc::new(out);
    cache.note_base_commits(base.id, Arc::clone(&out));
    Ok(Some(out))
}

/// Writes `objects` as a pack of whole objects, checks it, registers it (a
/// level-1 compaction pack, as exact packs are), uploads it, and returns
/// its row.
async fn write_patch<M, B>(
    server: &Server<M, B>,
    cache: &ShallowCache<M, B>,
    repo: &Repo,
    mut objects: Vec<ExtraObject>,
) -> Result<PackRecord, Failure>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let local = |what: &str, e: std::io::Error| -> Failure {
        FetchError::Store(StoreError::Unavailable {
            reason: format!("{what}: {e}"),
        })
        .into()
    };
    // Deterministic bytes, whoever writes it: several servers write the
    // same patch for the same answer, and registration is then a no-op.
    objects.sort_unstable_by_key(|o| o.id);
    let spool = cache.config().spool_dir.join("layered");
    std::fs::create_dir_all(&spool).map_err(|e| local("creating the spool directory", e))?;
    let dir = tempfile::Builder::new()
        .prefix("patch-")
        .tempdir_in(&spool)
        .map_err(|e| local("creating a spool directory", e))?;
    let max_object_bytes = server.config().max_object_bytes;
    let max_inflated = cache.config().max_inflated_bytes;
    let out = dir.path().to_path_buf();
    let written = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let written = write_subset(None, objects.as_slice(), &out, max_object_bytes)
            .map_err(|e| e.to_string())?;
        let verified = verify_pack(&written.pack_path, &out, max_object_bytes, max_inflated)
            .map_err(|e| e.to_string())?;
        let idx = std::fs::read(&written.idx_path).map_err(|e| e.to_string())?;
        if verified.idx != idx {
            return Err("the patch pack's .idx differs from the one its bytes give".into());
        }
        Ok(written)
    })
    .await
    .map_err(|e| local("patch pack task", std::io::Error::other(e)))?
    .map_err(|why| {
        FetchError::Store(StoreError::Corrupt {
            what: format!("a shallow patch pack failed its checks: {why}"),
        })
    })?;
    let level = CoverLevel::new(1).expect("level 1 exists");
    let new = NewPack {
        repo: repo.id,
        checksum: written.checksum,
        size: written.size,
        idx_size: written.idx_size,
        entries_size: entry_table_len(written.object_count),
        object_count: written.object_count,
        ref_delta_count: written.ref_delta_count,
        origin: PackOrigin::Compaction { level },
    };
    let meta = server.meta();
    let record = match meta
        .pack_by_checksum(repo.id, &new.checksum)
        .await
        .map_err(FetchError::Store)?
    {
        // The same bytes stored at another level: serve that row.
        Some(existing) if existing.pack.level() != level.into() => existing,
        // Register (or refresh the registration), then upload.
        _ => meta.register_pack(&new).await.map_err(FetchError::Store)?,
    };
    let keys = record.keys();
    let blobs = server.blobs();
    for (key, path) in [
        (&keys.pack, &written.pack_path),
        (&keys.idx, &written.idx_path),
        (&keys.entries, &written.entries_path),
    ] {
        blobs
            .upload_file(key, path)
            .await
            .map_err(FetchError::Store)?;
    }
    Ok(record)
}

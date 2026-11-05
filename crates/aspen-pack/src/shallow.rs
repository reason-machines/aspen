//! The object set of an exact shallow pack (node C8): what a client that
//! fetches `wants` with `deepen <n>` must receive, computed from stored
//! packs on local disk.
//!
//! A shallow fetch of depth n sends the commits fewer than n parents below
//! the wanted tips (the **region**, which the caller computes from
//! `commit_index` parents, as for `shallow-info`; fetch path step 5), and
//! every tree and blob those commits' root trees reach. The commits at the
//! region's lower edge become the client's shallow boundary: their parents
//! are not sent, and neither is anything only those parents reach. So the
//! set is:
//!
//! - each want, and each annotated tag it peels through (at most
//!   [`MAX_PEELED_TAGS`]) to a commit, which must be in the region;
//! - each region commit, its root tree, and every subtree and blob that
//!   tree reaches. Gitlinks (mode `160000`, submodule commits) name commits
//!   of another repository and are skipped, as Git does.
//!
//! Parents are never followed: the region is the caller's. An old blob
//! restored in a region commit is in the set because that commit's tree
//! reaches it, whatever commit first introduced it.
//!
//! [`shallow_objects`] returns the set; the pack is then written with
//! [`crate::repack::write_repack_subset`] over the same sources, which
//! stores a kept delta whose base is not kept undeltified.
//!
//! Cost: one decode per distinct commit, tag and tree of the region (gix,
//! with a delta-base cache of up to 96 MiB, so trees that are deltas of
//! each other share their chains). Blobs are never decoded: each needs
//! only an `.idx` lookup per source until one holds it. Memory is the set
//! (one ID per object), the walk's stack of trees, and the cache.

use std::collections::HashSet;
use std::fs;

use aspen_core::id::ObjectId;
use aspen_core::ingest::ObjectKind;

use crate::base_cache::{BaseCache, DEFAULT_BASE_CACHE_BYTES};
use crate::ingest::{core_kind, gix_chain};
use crate::repack::{RepackSource, open_bundle};
use crate::scan::{commit_tree, references};
use crate::{LayoutError, MaxObjectBytes, from_gix, to_gix};

/// The most annotated tags a want may peel through before it reaches a
/// commit; a longer chain is refused.
pub const MAX_PEELED_TAGS: usize = 16;

/// What a shallow pack is built for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShallowRoots {
    /// The objects the client wants: commits, or annotated tags that peel
    /// to commits. Each must peel to a commit in [`ShallowRoots::commits`].
    pub wants: Vec<ObjectId>,
    /// The region: every commit the client receives (for `deepen <n>`, the
    /// commits fewer than n parents below the wanted commits). Only these
    /// commits' trees are walked; their parents are not.
    pub commits: Vec<ObjectId>,
}

/// The objects of the exact shallow pack for `roots`, read from `sources`
/// (the stored packs of the wants' closure, which hold every object it
/// needs).
///
/// Fails with [`LayoutError::Mismatch`] when:
///
/// - a want peels to a tree or a blob, peels through more than
///   [`MAX_PEELED_TAGS`] tags, or reaches a commit that is not in the
///   region (the caller then serves the full closure instead);
/// - a region commit is not a commit, or a tree the walk reads is not a
///   tree;
/// - an object the set needs is in no source;
/// - the set would hold more than `max_objects` objects
///   ([`LayoutError::OverLimit`], the only error that is not about the
///   data);
/// - a source's `.idx` or pack file is not the pack its entry table names,
///   or an object does not decode or parse.
///
/// `max_object_bytes` bounds every object decoded.
pub fn shallow_objects(
    sources: &[RepackSource<'_>],
    roots: &ShallowRoots,
    max_object_bytes: MaxObjectBytes,
    max_objects: usize,
) -> Result<HashSet<ObjectId>, LayoutError> {
    let mut store = Objects::open(sources, max_object_bytes)?;
    let mut set = Kept {
        ids: HashSet::new(),
        max_objects,
    };
    let region: HashSet<ObjectId> = roots.commits.iter().copied().collect();

    for want in &roots.wants {
        let mut id = *want;
        let mut tags = 0;
        loop {
            let (kind, data) = store.read(&id)?;
            match kind {
                ObjectKind::Tag => {
                    if tags == MAX_PEELED_TAGS {
                        return Err(LayoutError::mismatch(format!(
                            "want {want} peels through more than {MAX_PEELED_TAGS} tags"
                        )));
                    }
                    tags += 1;
                    set.insert(id)?;
                    let mut target = None;
                    references(ObjectKind::Tag, &data, |t| target = Some(t))
                        .map_err(|e| LayoutError::mismatch(format!("tag {id}: {e}")))?;
                    id = target.expect("a parsed tag has a target");
                }
                ObjectKind::Commit if region.contains(&id) => break,
                ObjectKind::Commit => {
                    return Err(LayoutError::mismatch(format!(
                        "want {want} peels to commit {id}, which is not in the region"
                    )));
                }
                ObjectKind::Tree | ObjectKind::Blob => {
                    return Err(LayoutError::mismatch(format!(
                        "want {want} peels to {id}, a {kind:?}, not a commit"
                    )));
                }
            }
        }
    }

    let mut trees = Vec::new();
    for commit in &roots.commits {
        let (kind, data) = store.read(commit)?;
        if kind != ObjectKind::Commit {
            return Err(LayoutError::mismatch(format!(
                "region commit {commit} is a {kind:?}"
            )));
        }
        set.insert(*commit)?;
        let tree = commit_tree(&data)
            .map_err(|e| LayoutError::mismatch(format!("commit {commit}: {e}")))?;
        if set.insert(tree)? {
            trees.push(tree);
        }
    }

    while let Some(tree) = trees.pop() {
        let (kind, data) = store.read(&tree)?;
        if kind != ObjectKind::Tree {
            return Err(LayoutError::mismatch(format!(
                "{tree} is read as a tree but is a {kind:?}"
            )));
        }
        for entry in gix_object::TreeRefIter::from_bytes(&data, gix_hash::Kind::Sha1) {
            let entry = entry
                .map_err(|e| LayoutError::mismatch(format!("tree {tree}: {}", gix_chain(&e))))?;
            if entry.mode.is_commit() {
                continue;
            }
            let id = from_gix(entry.oid);
            if !set.insert(id)? {
                continue;
            }
            if entry.mode.is_tree() {
                trees.push(id);
            } else {
                store.require(&id)?;
            }
        }
    }
    Ok(set.ids)
}

/// The set being built, bounded by `max_objects`.
struct Kept {
    ids: HashSet<ObjectId>,
    max_objects: usize,
}

impl Kept {
    /// Adds `id`; whether it was new. Fails past the bound.
    fn insert(&mut self, id: ObjectId) -> Result<bool, LayoutError> {
        if !self.ids.insert(id) {
            return Ok(false);
        }
        if self.ids.len() > self.max_objects {
            return Err(LayoutError::OverLimit(format!(
                "the shallow pack would hold more than {} objects",
                self.max_objects
            )));
        }
        Ok(true)
    }
}

/// The sources as gix bundles, read in source order.
struct Objects {
    bundles: Vec<gix_pack::Bundle>,
    cache: BaseCache,
    buf: Vec<u8>,
    inflate: gix_zlib::Inflate,
}

impl Objects {
    /// Opens every source, checking that its `.idx` and pack file are the
    /// pack its entry table names (the entry table is not otherwise used).
    fn open(
        sources: &[RepackSource<'_>],
        max_object_bytes: MaxObjectBytes,
    ) -> Result<Self, LayoutError> {
        let mut bundles = Vec::with_capacity(sources.len());
        for (i, s) in sources.iter().enumerate() {
            let size = fs::metadata(s.pack)?.len();
            let bundle = open_bundle(s, max_object_bytes)?;
            let named = to_gix(&ObjectId::from_bytes(*s.table.checksum().as_bytes()));
            if size != s.table.pack_size()
                || bundle.index.pack_checksum() != named
                || bundle.pack.checksum() != named
            {
                return Err(LayoutError::mismatch(format!(
                    "shallow source {i}: the pack, its .idx and its entry table disagree"
                )));
            }
            bundles.push(bundle);
        }
        Ok(Self {
            bundles,
            cache: BaseCache::new(DEFAULT_BASE_CACHE_BYTES),
            buf: Vec::new(),
            inflate: gix_zlib::Inflate::default(),
        })
    }

    /// The first source whose `.idx` lists `id`.
    fn source_of(&self, id: &ObjectId) -> Result<usize, LayoutError> {
        let oid = to_gix(id);
        self.bundles
            .iter()
            .position(|b| b.index.lookup(oid).is_some())
            .ok_or_else(|| LayoutError::mismatch(format!("{id} is in no shallow source")))
    }

    /// Fails unless some source holds `id`; decodes nothing.
    fn require(&self, id: &ObjectId) -> Result<(), LayoutError> {
        self.source_of(id).map(|_| ())
    }

    /// Decodes `id` from the first source that holds it.
    fn read(&mut self, id: &ObjectId) -> Result<(ObjectKind, Vec<u8>), LayoutError> {
        let i = self.source_of(id)?;
        let mut cache = self.cache.for_source(i as u32);
        let (data, _) = self.bundles[i]
            .find(&to_gix(id), &mut self.buf, &mut self.inflate, &mut cache)
            .map_err(|e| LayoutError::mismatch(format!("{id}: {}", gix_chain(&e))))?
            .ok_or_else(|| LayoutError::mismatch(format!("{id} is not in its .idx's pack")))?;
        Ok((core_kind(data.kind), data.data.to_vec()))
    }
}

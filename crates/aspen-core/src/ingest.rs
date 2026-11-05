//! Push ingest contracts (`docs/PLAN.md`, push path step 2).
//!
//! The pipeline `aspen-pack` implements, in order:
//!
//! 1. **Receive** (pass 1): stream the pack to a local spool on a real disk
//!    while parsing it; refuse early on the header's object count
//!    ([`crate::pack::PushLimits`]). Collect the base IDs of every REF_DELTA
//!    as [`CandidateBases`].
//! 2. **Prefetch** ([`BasePrefetcher`]): look the candidates up, with their
//!    delta chains, in parallel, in the packs of the push's dependencies.
//!    A candidate the dependencies lack is expected to be in the pack; it is
//!    not an error.
//! 3. **Complete and index** ([`CompletionRun`]): gix re-reads the spool with
//!    a [`BaseLookup`] that answers only from the prefetched bases, inserts
//!    each base it gets just before its first delta, and writes the `.idx`.
//!    A scan of the sorted `.idx` ([`crate::pack::find_repeated`]) decides
//!    whether the run is accepted, rerun once refusing the repeated IDs, or
//!    the push is refused because the client sent an object twice.
//! 4. **Forward-base repair**, only when [`Completed::forward_base_check`]
//!    says so: a REF_DELTA that still points forward is moved to just after
//!    its base.
//! 5. **Connectivity check**: objects the pack references but lacks must be
//!    found by [`ObjectLocator`] in the dependencies.
//! 6. **Register** the pack ([`crate::meta::MetaStore::register_pack`]),
//!    then **upload** the pack, `.idx` and entry table to the keys of the
//!    returned row ([`crate::blob::BlobStore::upload_file`]). Only then may
//!    a version name the row.
//!
//! The lookup gix calls during step 3 is synchronous and is called from
//! inside its parse, one base at a time. That is why [`BaseLookup`] answers
//! from local data only, and why all network I/O happens before, in the
//! async prefetch (S4).

use std::collections::HashMap;
use std::future::Future;

use crate::error::{StoreError, UnpackError};
use crate::id::{ObjectId, PackId};

/// The type of a Git object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    /// A commit.
    Commit,
    /// A tree.
    Tree,
    /// A blob.
    Blob,
    /// An annotated tag.
    Tag,
}

/// A sorted list of object IDs without repeats.
///
/// Batched lookups take this form so that an implementation can merge the
/// list against each sorted `.idx` sequentially (S2, "For N1", item 5).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SortedIds(Vec<ObjectId>);

impl SortedIds {
    /// Sorts and removes repeats.
    pub fn from_unsorted(mut ids: Vec<ObjectId>) -> Self {
        ids.sort_unstable();
        ids.dedup();
        Self(ids)
    }

    /// The IDs, ascending.
    pub fn as_slice(&self) -> &[ObjectId] {
        &self.0
    }

    /// Number of IDs.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `id` is in the list.
    pub fn contains(&self, id: &ObjectId) -> bool {
        self.0.binary_search(id).is_ok()
    }
}

/// The base IDs of every REF_DELTA entry in a received pack, collected
/// during pass 1.
///
/// A candidate is either a thin base (in the push's dependencies) or an
/// object in the same pack: libgit2 writes every delta as REF_DELTA, mostly
/// against bases in the pack (S1), and pass 1 cannot tell the two apart
/// because a deltified entry's ID is known only once it is resolved (S4).
/// Stock `git push` sends OFS_DELTA inside the pack, so its candidates are
/// exactly its thin bases.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CandidateBases(pub SortedIds);

/// Where an object is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectLocation {
    /// The pack.
    pub pack: PackId,
    /// Offset of the object's entry in that pack.
    pub offset: u64,
}

/// The packs to search for objects, in probe order: newest level first
/// (uncompacted L0 packs, then L1 to L4 covers), as the planner returns for
/// a full fetch of each dependency of the push (trunk, plus the scoped
/// branch for a branch-scoped URL). Each pack appears once.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchPacks(Vec<PackId>);

impl SearchPacks {
    /// Builds the list, dropping later repeats of a pack.
    pub fn new(packs: impl IntoIterator<Item = PackId>) -> Self {
        let mut seen = std::collections::HashSet::new();
        Self(packs.into_iter().filter(|p| seen.insert(*p)).collect())
    }

    /// The packs, in probe order.
    pub fn as_slice(&self) -> &[PackId] {
        &self.0
    }
}

/// Finds objects in stored packs by probing each pack's `.idx` (fanout
/// table, then binary search) in [`SearchPacks`] order, stopping at the
/// first hit. `.idx` files are immutable and cached locally, so a warm
/// lookup reads no object storage.
///
/// One mechanism serves both thin-base completion (which needs the
/// location) and the connectivity check (which needs only presence); there
/// is no second lookup and no global `oid → pack` table (S3).
pub trait ObjectLocator: Send + Sync {
    /// Locates each ID; the result has one entry per ID, in the same order.
    fn locate(
        &self,
        packs: &SearchPacks,
        ids: &SortedIds,
    ) -> impl Future<Output = Result<Vec<Option<ObjectLocation>>, StoreError>> + Send;
}

/// The synchronous base lookup gix calls while completing a pack.
///
/// Implementations answer from **local data only** (memory or a local
/// file): gix calls this from inside its parse, one base at a time, and a
/// blob-store read here would serialize thousands of round trips per push
/// (S4 counted 5,431 entry reads for a 100-commit Linux push). `aspen-pack`
/// adapts this trait to `gix_object::Find`.
pub trait BaseLookup: Send + Sync {
    /// Writes the full (undeltified) object to `out` and returns its kind,
    /// or returns `None` if this lookup does not have it. `None` is not an
    /// error: the base may be in the pack itself.
    fn find_base(&self, id: &ObjectId, out: &mut Vec<u8>)
    -> Result<Option<ObjectKind>, StoreError>;
}

/// A [`BaseLookup`] that answers "not found" for some IDs, so their deltas
/// resolve against the copy the client sent. Used by the duplicate rerun.
#[derive(Debug)]
pub struct Refusing<'a, L> {
    inner: &'a L,
    refused: &'a SortedIds,
}

impl<'a, L: BaseLookup> Refusing<'a, L> {
    /// Wraps `inner`, refusing every ID in `refused`.
    pub fn new(inner: &'a L, refused: &'a SortedIds) -> Self {
        Self { inner, refused }
    }
}

impl<L: BaseLookup> BaseLookup for Refusing<'_, L> {
    fn find_base(
        &self,
        id: &ObjectId,
        out: &mut Vec<u8>,
    ) -> Result<Option<ObjectKind>, StoreError> {
        if self.refused.contains(id) {
            Ok(None)
        } else {
            self.inner.find_base(id, out)
        }
    }
}

/// Prefetched bases held in memory.
#[derive(Debug, Clone, Default)]
pub struct InMemoryBases(HashMap<ObjectId, (ObjectKind, Vec<u8>)>);

impl InMemoryBases {
    /// Adds a full object.
    pub fn insert(&mut self, id: ObjectId, kind: ObjectKind, data: Vec<u8>) {
        self.0.insert(id, (kind, data));
    }

    /// Number of objects held.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether none are held.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl BaseLookup for InMemoryBases {
    fn find_base(
        &self,
        id: &ObjectId,
        out: &mut Vec<u8>,
    ) -> Result<Option<ObjectKind>, StoreError> {
        Ok(self.0.get(id).map(|(kind, data)| {
            out.clear();
            out.extend_from_slice(data);
            *kind
        }))
    }
}

/// The result of a prefetch.
#[derive(Debug, Clone)]
pub struct Prefetched<B> {
    /// The bases found, ready for gix.
    pub bases: B,
    /// Candidates the dependencies do not have. Expected to be in the pack;
    /// the push fails only if gix then cannot resolve them there
    /// ([`UnpackError::MissingBase`]).
    pub not_in_deps: SortedIds,
}

/// The async step that fetches candidate bases, and their delta chains, from
/// the blob store before completion starts. Reads are issued in parallel;
/// a chain takes a few dependent rounds (p99 3 to 7 entries, S4), never one
/// round trip per base.
pub trait BasePrefetcher: Send + Sync {
    /// Where the prefetched bases live (memory, or a local file).
    type Bases: BaseLookup;

    /// Prefetches every candidate found in `packs`.
    fn prefetch(
        &self,
        packs: &SearchPacks,
        candidates: &CandidateBases,
    ) -> impl Future<Output = Result<Prefetched<Self::Bases>, StoreError>> + Send;
}

/// What one completion run reports back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    /// REF_DELTA bases the lookup was asked for and did not answer (prefetch
    /// misses plus refused IDs). gix resolves those inside the pack.
    pub unanswered: u64,
    /// IDs that appear more than once in the run's sorted `.idx`.
    pub repeated: Vec<ObjectId>,
}

/// One completion run over the spool, and the decision that follows it.
///
/// The first run refuses nothing. gix asks the lookup about every REF_DELTA
/// base it did not insert itself, without checking whether the pack already
/// holds that object, so a pack carrying X plus a REF_DELTA against X, with
/// X also in the dependencies, comes out with X twice (S4, `dup.sh`). If the
/// `.idx` scan finds repeats, one rerun refuses exactly those IDs. A repeat
/// that survives the rerun is the client's own and the push is refused:
///
/// ```text
/// first run ──repeats──► rerun(refuse repeats) ──repeats──► UnpackError::DuplicateObject
///     │                         │
///     └──none──► Completed ◄────┘ none
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionRun {
    refused: SortedIds,
    attempt: u8,
}

/// After a run: rerun, or done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AfterRun {
    /// Run again with the returned run's refusals.
    Rerun(CompletionRun),
    /// Indexed without repeats.
    Completed(Completed),
}

/// A completed, indexed pack with no repeated object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completed {
    /// Whether the forward-base check must run. Decided by the **final** run:
    /// skipped only when its lookup answered every candidate (no prefetch
    /// miss and no refused ID), in which case no REF_DELTA is left and none
    /// can point forward. That is every stock `git push`. A rerun refuses
    /// IDs on purpose, so after a rerun the check always runs.
    pub forward_base_check: bool,
    /// Whether a duplicate rerun happened.
    pub reran: bool,
}

impl Default for CompletionRun {
    fn default() -> Self {
        Self::first()
    }
}

impl CompletionRun {
    /// The first run: the lookup refuses nothing.
    pub fn first() -> Self {
        Self {
            refused: SortedIds::default(),
            attempt: 1,
        }
    }

    /// IDs the lookup must refuse in this run; wrap the prefetched bases in
    /// [`Refusing`] with them.
    pub fn refused(&self) -> &SortedIds {
        &self.refused
    }

    /// 1 for the first run, 2 for the rerun.
    pub fn attempt(&self) -> u8 {
        self.attempt
    }

    /// Decides what follows this run.
    pub fn finish(self, report: RunReport) -> Result<AfterRun, UnpackError> {
        if let Some(&first) = report.repeated.first() {
            return if self.attempt == 1 {
                Ok(AfterRun::Rerun(CompletionRun {
                    refused: SortedIds::from_unsorted(report.repeated),
                    attempt: 2,
                }))
            } else {
                Err(UnpackError::DuplicateObject { id: first })
            };
        }
        Ok(AfterRun::Completed(Completed {
            forward_base_check: report.unanswered > 0 || !self.refused.is_empty(),
            reran: self.attempt > 1,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }

    fn report(unanswered: u64, repeated: &[u8]) -> RunReport {
        RunReport {
            unanswered,
            repeated: repeated.iter().map(|&n| oid(n)).collect(),
        }
    }

    #[test]
    fn stock_push_skips_the_forward_base_check() {
        let after = CompletionRun::first().finish(report(0, &[])).unwrap();
        assert_eq!(
            after,
            AfterRun::Completed(Completed {
                forward_base_check: false,
                reran: false
            })
        );
    }

    #[test]
    fn a_prefetch_miss_requires_the_forward_base_check() {
        // libgit2 push: REF_DELTAs against bases in the pack.
        let AfterRun::Completed(c) = CompletionRun::first().finish(report(9, &[])).unwrap() else {
            panic!()
        };
        assert!(c.forward_base_check);
    }

    #[test]
    fn repeats_trigger_exactly_one_rerun_that_refuses_them() {
        let AfterRun::Rerun(rerun) = CompletionRun::first().finish(report(0, &[5, 3])).unwrap()
        else {
            panic!()
        };
        assert_eq!(rerun.attempt(), 2);
        assert_eq!(rerun.refused().as_slice(), &[oid(3), oid(5)]);
        // After a rerun the check always runs, even if every remaining
        // lookup was answered.
        let AfterRun::Completed(c) = rerun.finish(report(0, &[])).unwrap() else {
            panic!()
        };
        assert!(c.forward_base_check && c.reran);
    }

    #[test]
    fn a_repeat_that_survives_the_rerun_is_the_clients_and_refuses_the_push() {
        let AfterRun::Rerun(rerun) = CompletionRun::first().finish(report(0, &[3])).unwrap() else {
            panic!()
        };
        assert_eq!(
            rerun.finish(report(1, &[3])),
            Err(UnpackError::DuplicateObject { id: oid(3) })
        );
    }

    #[test]
    fn refusing_lookup_hides_refused_ids() {
        let mut bases = InMemoryBases::default();
        bases.insert(oid(1), ObjectKind::Blob, b"one".to_vec());
        bases.insert(oid(2), ObjectKind::Tree, b"two".to_vec());
        let refused = SortedIds::from_unsorted(vec![oid(2)]);
        let lookup = Refusing::new(&bases, &refused);
        let mut buf = Vec::new();
        assert_eq!(
            lookup.find_base(&oid(1), &mut buf).unwrap(),
            Some(ObjectKind::Blob)
        );
        assert_eq!(buf, b"one");
        assert_eq!(lookup.find_base(&oid(2), &mut buf).unwrap(), None);
        assert_eq!(lookup.find_base(&oid(9), &mut buf).unwrap(), None);
        assert_eq!(
            bases.find_base(&oid(2), &mut buf).unwrap(),
            Some(ObjectKind::Tree)
        );
    }

    #[test]
    fn sorted_ids_sort_and_dedupe() {
        let s = SortedIds::from_unsorted(vec![oid(3), oid(1), oid(3), oid(2)]);
        assert_eq!(s.as_slice(), &[oid(1), oid(2), oid(3)]);
        assert!(s.contains(&oid(2)) && !s.contains(&oid(4)));
        assert_eq!(s.len(), 3);
    }

    #[test]
    fn search_packs_keep_first_occurrence_order() {
        let p = |n| PackId::new(n).unwrap();
        let s = SearchPacks::new([p(5), p(2), p(5), p(1), p(2)]);
        assert_eq!(s.as_slice(), &[p(5), p(2), p(1)]);
    }
}

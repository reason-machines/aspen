//! The object graph of one pack: every object's kind and the objects it
//! references, kept compactly so a push can walk it in memory.
//!
//! The push path (node B1) needs three answers about a completed pack that a
//! plain [`crate::scan::scan`] does not give:
//!
//! - **Which objects each write's tip reaches inside the pack.** A multi-ref
//!   push stores one pack per write (decisions log, "B1 splits a multi-ref
//!   push into one pack per write"), holding exactly the objects that write's
//!   tip reaches and its dependencies lack. [`PackGraph::reach`] is that
//!   walk.
//! - **Whether every object a write stores is reachable from its tip**, the
//!   `pack_reachable` input of `aspen_core::refs::Closure::of_new_version`.
//!   Completion bases are excluded from the walk (N1 review of #18): they are
//!   in the dependencies' closures already.
//! - **What a write references outside its own objects**, so the
//!   connectivity check can be run per write against that write's
//!   dependencies.
//!
//! One traversal of the pack decodes every object once (in parallel, as
//! [`crate::scan::scan`] does) and records each reference as the target's
//! position in the sorted `.idx` when the target is in the pack, or as its
//! ID when it is not. Memory is about 4 bytes per in-pack reference plus 20
//! per external one, plus a row per commit. That has not been measured on a
//! Linux-sized push; S4 measured 140 bytes per object for gix's own tree.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use aspen_core::id::ObjectId;
use aspen_core::ingest::{ObjectKind, SortedIds};

use crate::ingest::{core_kind, gix_chain};
use crate::scan::{CommitInfo, ObjectError, ScanError, ScanOptions, parse_commit, references};
use crate::{from_gix, to_gix};

/// One pack's objects and references, by `.idx` position (ascending ID).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackGraph {
    ids: Vec<ObjectId>,
    kinds: Vec<ObjectKind>,
    /// `internal[internal_at[i]..internal_at[i + 1]]` are the positions
    /// object `i` references inside the pack.
    internal_at: Vec<u64>,
    internal: Vec<u32>,
    /// The same for references to objects outside the pack.
    external_at: Vec<u64>,
    external: Vec<ObjectId>,
    /// Every commit in the pack, sorted by ID.
    commits: Vec<CommitInfo>,
    /// The bytes of every tree, by position, when
    /// [`PackGraph::scan_keeping_trees`] kept them; `None` when it was not
    /// asked to or they were over its limit.
    trees: Option<HashMap<u32, Arc<[u8]>>>,
}

/// What one walk of [`PackGraph::reach`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reach {
    /// Positions reached, one flag per object of the pack.
    pub members: Vec<bool>,
    /// How many flags are set.
    pub count: usize,
    /// Objects the reached objects reference that the walk did not enter:
    /// objects outside the pack, and objects in the pack that the caller
    /// marked as held by the dependencies. With the tip itself when the tip
    /// is not in the pack or is held. These must be in the write's
    /// dependency closure (connectivity check).
    pub boundary: SortedIds,
}

impl PackGraph {
    /// Builds the graph of the pack at `pack` with its `.idx` at `idx`, in
    /// one parallel traversal. The options' `commits` and `missing` flags
    /// are ignored: both are always collected. The declared-size checks of
    /// [`crate::scan::scan`] run first, under the same limits.
    pub fn scan(pack: &Path, idx: &Path, options: ScanOptions) -> Result<Self, ScanError> {
        Self::scan_keeping_trees(pack, idx, options, 0)
    }

    /// [`PackGraph::scan`], also keeping every tree's bytes while they add
    /// up to at most `max_tree_bytes` ([`PackGraph::tree_data`]), for a
    /// walk of the paths a push changed (`crate::changed`). Over the limit
    /// none are kept, and memory goes back to what `scan` uses.
    pub fn scan_keeping_trees(
        pack: &Path,
        idx: &Path,
        options: ScanOptions,
        max_tree_bytes: u64,
    ) -> Result<Self, ScanError> {
        let pack_err = |e: gix_error::Error| ScanError::Pack(gix_chain(&e));
        let index = gix_pack::index::File::at(idx, gix_hash::Kind::Sha1).map_err(pack_err)?;
        crate::scan::check_declared_sizes(pack, &index, &options)?;
        let n = index.num_objects() as usize;
        let alloc_limit = options
            .max_object_bytes
            .gix_alloc_limit(u64::from(index.num_objects()));
        let data = gix_pack::data::File::at(pack, gix_hash::Kind::Sha1)
            .map_err(pack_err)?
            .with_alloc_limit_bytes(Some(alloc_limit));
        let ids: Vec<ObjectId> = (0..index.num_objects())
            .map(|i| from_gix(index.oid_at_index(i)))
            .collect();
        struct Node {
            kind: ObjectKind,
            internal: Vec<u32>,
            external: Vec<ObjectId>,
        }
        #[derive(Default)]
        struct Found {
            nodes: Vec<Option<Node>>,
            commits: Vec<CommitInfo>,
            error: Option<ObjectError>,
            trees: Option<HashMap<u32, Arc<[u8]>>>,
            tree_bytes: u64,
        }
        let found = Mutex::new(Found {
            nodes: (0..n).map(|_| None).collect(),
            trees: (max_tree_bytes > 0).then(HashMap::new),
            ..Found::default()
        });
        let processor = |kind: gix_object::Kind,
                         bytes: &[u8],
                         entry: &gix_pack::index::Entry,
                         _: &dyn gix_utils::progress::Progress|
         -> gix_error::Result {
            let kind = core_kind(kind);
            let id = from_gix(&entry.oid);
            let fail = |reason: String| ObjectError { id, kind, reason };
            let parsed = (|| {
                let mut node = Node {
                    kind,
                    internal: Vec::new(),
                    external: Vec::new(),
                };
                let mut refs = Vec::new();
                references(kind, bytes, |r| refs.push(r)).map_err(fail)?;
                for r in refs {
                    match index.lookup(to_gix(&r)) {
                        Some(p) => node.internal.push(p),
                        None => node.external.push(r),
                    }
                }
                let commit = if kind == ObjectKind::Commit {
                    let (parents, committer_time) = parse_commit(bytes).map_err(fail)?;
                    Some(CommitInfo {
                        id,
                        parents,
                        committer_time,
                    })
                } else {
                    None
                };
                let at = index
                    .lookup(entry.oid)
                    .ok_or_else(|| fail("not in its own .idx".into()))?;
                Ok::<_, ObjectError>((at as usize, node, commit))
            })();
            let mut f = found.lock().expect("not poisoned");
            match parsed {
                Ok((at, node, commit)) => {
                    if node.kind == ObjectKind::Tree && f.trees.is_some() {
                        f.tree_bytes += bytes.len() as u64;
                        if f.tree_bytes > max_tree_bytes {
                            f.trees = None;
                        } else if let Some(t) = f.trees.as_mut() {
                            t.insert(at as u32, Arc::from(bytes));
                        }
                    }
                    f.nodes[at] = Some(node);
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
        let mut kinds = Vec::with_capacity(n);
        let mut internal_at = Vec::with_capacity(n + 1);
        let mut internal = Vec::new();
        let mut external_at = Vec::with_capacity(n + 1);
        let mut external = Vec::new();
        internal_at.push(0);
        external_at.push(0);
        for (i, node) in found.nodes.into_iter().enumerate() {
            let node = node.ok_or_else(|| {
                ScanError::Pack(format!("the traversal never decoded object {}", ids[i]))
            })?;
            kinds.push(node.kind);
            internal.extend(node.internal);
            external.extend(node.external);
            internal_at.push(internal.len() as u64);
            external_at.push(external.len() as u64);
        }
        found.commits.sort_unstable_by_key(|c| c.id);
        Ok(Self {
            ids,
            kinds,
            internal_at,
            internal,
            external_at,
            external,
            commits: found.commits,
            trees: found.trees,
        })
    }

    /// The bytes of the tree at `at`, when the scan kept them
    /// ([`PackGraph::scan_keeping_trees`]).
    pub fn tree_data(&self, at: usize) -> Option<&Arc<[u8]>> {
        self.trees.as_ref()?.get(&(at as u32))
    }

    /// Whether the scan kept every tree's bytes.
    pub fn trees_kept(&self) -> bool {
        self.trees.is_some()
    }

    /// Objects in the pack.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether the pack has no objects.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// The object at a position.
    pub fn id(&self, at: usize) -> ObjectId {
        self.ids[at]
    }

    /// Every object, ascending (the `.idx` order).
    pub fn ids(&self) -> &[ObjectId] {
        &self.ids
    }

    /// The position of `id`, if the pack holds it.
    pub fn position(&self, id: &ObjectId) -> Option<usize> {
        self.ids.binary_search(id).ok()
    }

    /// The type of the object at a position.
    pub fn kind(&self, at: usize) -> ObjectKind {
        self.kinds[at]
    }

    /// Positions the object at `at` references inside the pack.
    pub fn internal_refs(&self, at: usize) -> &[u32] {
        &self.internal[self.internal_at[at] as usize..self.internal_at[at + 1] as usize]
    }

    /// Objects the object at `at` references outside the pack.
    pub fn external_refs(&self, at: usize) -> &[ObjectId] {
        &self.external[self.external_at[at] as usize..self.external_at[at + 1] as usize]
    }

    /// Every commit in the pack, sorted by ID.
    pub fn commits(&self) -> &[CommitInfo] {
        &self.commits
    }

    /// The commit row of `id`, if it is a commit in the pack.
    pub fn commit(&self, id: &ObjectId) -> Option<&CommitInfo> {
        self.commits
            .binary_search_by_key(id, |c| c.id)
            .ok()
            .map(|i| &self.commits[i])
    }

    /// Every object referenced by some object of the pack and not in it:
    /// the connectivity check's input for the whole pack.
    pub fn missing(&self) -> SortedIds {
        SortedIds::from_unsorted(self.external.clone())
    }

    /// Walks from `tip` through references, entering only objects of the
    /// pack that `held` does not mark (`held[i]`: the dependencies already
    /// have object `i`, so everything it reaches is in their closure too,
    /// by the connected invariant).
    ///
    /// `held` must have one flag per object.
    pub fn reach(&self, tip: &ObjectId, held: &[bool]) -> Reach {
        assert_eq!(held.len(), self.len(), "one held flag per object");
        let mut members = vec![false; self.len()];
        let mut count = 0;
        let mut boundary = Vec::new();
        let mut queue = VecDeque::new();
        match self.position(tip) {
            Some(at) if !held[at] => {
                members[at] = true;
                count += 1;
                queue.push_back(at);
            }
            _ => boundary.push(*tip),
        }
        while let Some(at) = queue.pop_front() {
            boundary.extend_from_slice(self.external_refs(at));
            for &next in self.internal_refs(at) {
                let next = next as usize;
                if held[next] {
                    boundary.push(self.ids[next]);
                } else if !members[next] {
                    members[next] = true;
                    count += 1;
                    queue.push_back(next);
                }
            }
        }
        Reach {
            members,
            count,
            boundary: SortedIds::from_unsorted(boundary),
        }
    }

    /// The positions the object at `tip` reaches without entering an
    /// object flagged in `held`, as [`PackGraph::reach`] would flag them,
    /// or `None` as soon as there are more than `limit`. `seen` is scratch
    /// space, one flag per object, all clear on entry and on return, so a
    /// caller walking many tips allocates it once and each walk costs what
    /// it visits, not the size of the pack.
    pub fn reach_within(
        &self,
        tip: usize,
        held: &[bool],
        limit: usize,
        seen: &mut [bool],
    ) -> Option<Vec<u32>> {
        assert_eq!(held.len(), self.len(), "one held flag per object");
        assert_eq!(seen.len(), self.len(), "one scratch flag per object");
        let mut out: Vec<u32> = Vec::new();
        if held[tip] {
            return Some(out);
        }
        seen[tip] = true;
        out.push(tip as u32);
        let mut next_at = 0;
        let mut over = out.len() > limit;
        while !over && next_at < out.len() {
            let at = out[next_at] as usize;
            next_at += 1;
            for &r in self.internal_refs(at) {
                let r = r as usize;
                if !held[r] && !seen[r] {
                    seen[r] = true;
                    out.push(r as u32);
                }
            }
            over = out.len() > limit;
        }
        for &at in &out {
            seen[at as usize] = false;
        }
        (!over).then_some(out)
    }
}

/// Objects several writes of one push store in one pack: the union of what
/// each added tip reaches without entering held objects.
///
/// Each [`SharedSet::add`] walks only objects the set does not hold yet, so
/// adding any number of tips costs one pass over the union, and the set
/// stays closed under references (except into held objects): a tip already
/// in the set reaches nothing outside it. The set also tracks which added
/// tips another added tip reaches, which is what decides whether one
/// write's own reach is the whole set ([`SharedSet::spanning_tip`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedSet {
    /// Per object: 0 if outside the set, else the number of the add that
    /// brought it in (1 for the first).
    stamp: Vec<u32>,
    adds: u32,
    count: usize,
    /// Every distinct tip added, by position: whether another added tip
    /// reaches it.
    tips: std::collections::HashMap<u32, bool>,
}

/// What one [`SharedSet::add`] did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Added {
    /// Positions this call added, the tip first if it was new.
    pub added: Vec<u32>,
    /// Whether the walk met objects the set already held, so that part of
    /// the tip's reach was added by earlier calls.
    pub met_existing: bool,
}

impl SharedSet {
    /// An empty set over a pack of `len` objects.
    pub fn new(len: usize) -> Self {
        Self {
            stamp: vec![0; len],
            adds: 0,
            count: 0,
            tips: std::collections::HashMap::new(),
        }
    }

    /// Whether the object at `at` is in the set.
    pub fn contains(&self, at: usize) -> bool {
        self.stamp[at] != 0
    }

    /// One flag per object of the pack.
    pub fn members(&self) -> Vec<bool> {
        self.stamp.iter().map(|s| *s != 0).collect()
    }

    /// How many objects the set holds.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Adds what the object at `tip` reaches in `g` without entering an
    /// object flagged in `held` (which must not flag `tip`).
    pub fn add(&mut self, g: &PackGraph, tip: usize, held: &[bool]) -> Added {
        assert_eq!(held.len(), g.len(), "one held flag per object");
        assert_eq!(self.stamp.len(), g.len(), "one member flag per object");
        assert!(!held[tip], "a held tip adds nothing");
        let mut out = Added::default();
        let key = tip as u32;
        if self.contains(tip) {
            // Some earlier tip reaches this one; a repeat of an earlier tip
            // changes nothing.
            self.tips.entry(key).or_insert(true);
            out.met_existing = true;
            return out;
        }
        self.adds += 1;
        let this = self.adds;
        self.tips.insert(key, false);
        self.stamp[tip] = this;
        self.count += 1;
        out.added.push(key);
        let mut next_at = 0;
        while next_at < out.added.len() {
            let at = out.added[next_at] as usize;
            next_at += 1;
            for &r in g.internal_refs(at) {
                let ru = r as usize;
                if held[ru] {
                    continue;
                }
                if self.stamp[ru] == this {
                    continue;
                }
                if self.stamp[ru] != 0 {
                    out.met_existing = true;
                    if r != key
                        && let Some(reached) = self.tips.get_mut(&r)
                    {
                        *reached = true;
                    }
                    continue;
                }
                self.stamp[ru] = this;
                self.count += 1;
                out.added.push(r);
            }
        }
        out
    }

    /// The added tip whose reach is the whole set, if there is one: the
    /// only added tip that no other added tip reaches. Every other tip is
    /// then reached from it, so its reach holds theirs. With two or more
    /// such tips, no single tip reaches the whole set.
    pub fn spanning_tip(&self) -> Option<usize> {
        let mut top = self.tips.iter().filter(|(_, reached)| !**reached);
        match (top.next(), top.next()) {
            (Some((&at, _)), None) => Some(at as usize),
            _ => None,
        }
    }
}

#[cfg(test)]
impl PackGraph {
    /// A graph of `n` blobs where object `i` references `edges[i]`, for
    /// tests of the walks.
    pub(crate) fn from_edges(edges: &[Vec<u32>]) -> Self {
        let n = edges.len();
        let mut internal_at = vec![0u64];
        let mut internal = Vec::new();
        for e in edges {
            internal.extend_from_slice(e);
            internal_at.push(internal.len() as u64);
        }
        Self {
            ids: (0..n)
                .map(|i| {
                    let mut b = [0u8; 20];
                    b[..8].copy_from_slice(&(i as u64).to_be_bytes());
                    ObjectId::from_bytes(b)
                })
                .collect(),
            kinds: vec![ObjectKind::Blob; n],
            internal_at,
            internal,
            external_at: vec![0; n + 1],
            external: Vec::new(),
            commits: Vec::new(),
            trees: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small deterministic generator (no test dependency needed).
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % n
        }
    }

    /// A random DAG: object `i` references only objects with a smaller
    /// index, so there are no cycles.
    fn dag(rng: &mut Lcg, n: usize) -> Vec<Vec<u32>> {
        (0..n)
            .map(|i| {
                let k = if i == 0 { 0 } else { rng.next(4) };
                let mut e: Vec<u32> = (0..k).map(|_| rng.next(i as u64) as u32).collect();
                e.sort_unstable();
                e.dedup();
                e
            })
            .collect()
    }

    fn brute(edges: &[Vec<u32>], tip: usize, held: &[bool]) -> Vec<bool> {
        let mut m = vec![false; edges.len()];
        let mut stack = vec![tip];
        while let Some(at) = stack.pop() {
            if held[at] || m[at] {
                continue;
            }
            m[at] = true;
            stack.extend(edges[at].iter().map(|&r| r as usize));
        }
        m
    }

    /// Invariant: `reach_within` returns exactly `reach`'s members, or
    /// `None` exactly when there are more than the limit, and leaves its
    /// scratch clear either way.
    #[test]
    fn reach_within_matches_reach_and_respects_its_limit() {
        let mut rng = Lcg(7);
        for _ in 0..200 {
            let n = 1 + rng.next(40) as usize;
            let edges = dag(&mut rng, n);
            let g = PackGraph::from_edges(&edges);
            let held: Vec<bool> = (0..n).map(|_| rng.next(5) == 0).collect();
            let tip = rng.next(n as u64) as usize;
            let want = brute(&edges, tip, &held);
            let count = want.iter().filter(|m| **m).count();
            let mut seen = vec![false; n];
            let got = g.reach_within(tip, &held, count, &mut seen).unwrap();
            let mut flags = vec![false; n];
            for at in got {
                flags[at as usize] = true;
            }
            assert_eq!(flags, want);
            assert_eq!(g.reach(&g.id(tip), &held).members, want);
            assert!(seen.iter().all(|s| !s));
            if count > 0 {
                assert!(g.reach_within(tip, &held, count - 1, &mut seen).is_none());
                assert!(seen.iter().all(|s| !s));
            }
        }
    }

    /// Invariant: after any sequence of adds, a shared set holds exactly
    /// the union of what its tips reach, and `spanning_tip` names the one
    /// tip whose own reach is that whole union, or none when no tip's is.
    #[test]
    fn a_shared_set_is_the_union_and_finds_its_spanning_tip() {
        let mut rng = Lcg(11);
        let mut spanning = 0;
        for _ in 0..500 {
            let n = 1 + rng.next(30) as usize;
            let edges = dag(&mut rng, n);
            let g = PackGraph::from_edges(&edges);
            let held: Vec<bool> = (0..n).map(|_| rng.next(6) == 0).collect();
            let mut set = SharedSet::new(n);
            let mut union = vec![false; n];
            let mut tips = Vec::new();
            for _ in 0..1 + rng.next(5) {
                let tip = rng.next(n as u64) as usize;
                if held[tip] {
                    continue;
                }
                let before = set.members();
                let r = brute(&edges, tip, &held);
                let added = set.add(&g, tip, &held);
                for (i, m) in r.iter().enumerate() {
                    union[i] |= *m;
                }
                // It added exactly what the tip reaches beyond the set.
                let mut flags = vec![false; n];
                for &at in &added.added {
                    flags[at as usize] = true;
                }
                let new: Vec<bool> = (0..n).map(|i| r[i] && !before[i]).collect();
                assert_eq!(flags, new);
                let met = (0..n).any(|i| r[i] && before[i]);
                assert_eq!(added.met_existing, met);
                tips.push(tip);
            }
            assert_eq!(set.members(), union);
            assert_eq!(set.count(), union.iter().filter(|m| **m).count());
            let whole: Vec<usize> = tips
                .iter()
                .copied()
                .filter(|&t| brute(&edges, t, &held) == union)
                .collect();
            let want = whole.first().copied();
            assert!(whole.iter().all(|&t| Some(t) == want), "one spanning tip");
            assert_eq!(set.spanning_tip(), want, "edges {edges:?} tips {tips:?}");
            spanning += usize::from(want.is_some() && tips.len() > 1);
        }
        assert!(spanning > 20, "the generator exercises spanning tips");
    }
}

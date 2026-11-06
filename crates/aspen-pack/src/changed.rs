//! The paths a push changed: a walk that diffs each pushed commit's tree
//! against its first parent's and descends only into subtrees that differ,
//! so the connectivity check and the "already held" test cost what the push
//! changed, not what the repository holds.
//!
//! **Why it is enough.** Every committed ref version is connected: every
//! object its tip reaches exists in its closure. So an entry of a pushed
//! tree that is the same object as the entry under the same name in the
//! parent's tree exists if the parent's tree does, and needs no lookup.
//! Only entries that differ are checked: one in the pushed pack is entered
//! (a tree) or present (a blob), one outside it goes to the connectivity
//! check. A parent outside the pack exists when it is the tip of one of the
//! push's dependencies ([`ChangedInput::known`]); any other is checked.
//! For a parent inside the pack, the same argument applies one commit
//! further down, so by induction over the pack's commits every reference is
//! accounted for. A merge is diffed against its first parent only; entries
//! that come from another parent are checked, which costs lookups, never
//! correctness. An old tree the walk cannot read counts as empty, which
//! costs the same.
//!
//! **Held objects.** An object of the pack that equals an entry of a tree
//! reached from a dependency's tip, under the same path, is in that
//! dependency's closure: the caller need not look it up to know it is held.
//!
//! Objects the walk does not reach (tags, completion bases, anything a
//! client sent that no commit reaches) are not accounted for here; the
//! caller checks their references as before. [`TreeCache`] keeps the trees
//! the walk reads from stored packs, and the trees trunk's pushes bring, so
//! the next push to trunk diffs against them without reading anything.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, RepoId};
use aspen_core::ingest::ObjectKind;
use hashlink::LinkedHashMap;

use crate::from_gix;
use crate::graph::PackGraph;

/// Reads what the walk needs from the push's dependencies.
pub trait OldTrees: Send {
    /// The bytes of each tree, which the dependencies hold; `None` for one
    /// that cannot be read (the walk treats it as empty).
    fn trees(
        &mut self,
        ids: &[ObjectId],
    ) -> impl Future<Output = Result<Vec<Option<Arc<[u8]>>>, StoreError>> + Send;

    /// The root tree of each commit, which the dependencies hold; `None`
    /// for one that cannot be read.
    fn commit_trees(
        &mut self,
        ids: &[ObjectId],
    ) -> impl Future<Output = Result<Vec<Option<ObjectId>>, StoreError>> + Send;
}

/// What the walk is given.
#[derive(Debug, Clone, Copy)]
pub struct ChangedInput<'a> {
    /// The completed pack's graph, scanned with its trees kept
    /// ([`PackGraph::scan_keeping_trees`]).
    pub graph: &'a PackGraph,
    /// Commits known to be in the dependencies' closures: their tips.
    pub known: &'a [ObjectId],
}

/// Work the walk did, for metrics and tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangedStats {
    /// Pushed commits diffed.
    pub commits: u64,
    /// Pushed trees entered.
    pub trees: u64,
    /// Old trees read (from the pack, the caller's cache or storage).
    pub old_trees: u64,
    /// Tree entries compared.
    pub entries: u64,
}

/// What the walk found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changed {
    /// Per object of the pack: every object it references is accounted
    /// for (in the pack, equal to the parent's, or in [`Changed::check`]).
    pub visited: Vec<bool>,
    /// Per object of the pack: proven in the dependencies' closure.
    pub held: Vec<bool>,
    /// Objects outside the pack that must be in the dependencies' closure.
    pub check: Vec<ObjectId>,
    /// Pushed trees the walk entered, by position.
    pub trees: Vec<usize>,
    /// Work done.
    pub stats: ChangedStats,
}

/// The old side of a diff.
#[derive(Debug, Clone, Copy)]
struct Old {
    id: ObjectId,
    /// Reached from a dependency's tip, so everything under it is held.
    from_deps: bool,
}

/// The root tree of the commit at `at`: the one reference that is not a
/// parent.
fn tree_of(g: &PackGraph, at: usize) -> Option<ObjectId> {
    let c = g.commit(&g.id(at))?;
    g.internal_refs(at)
        .iter()
        .map(|&p| g.id(p as usize))
        .chain(g.external_refs(at).iter().copied())
        .find(|r| !c.parents.contains(r))
}

/// Entries of a tree: name to (is a tree, ID), gitlinks left out.
fn entries(data: &[u8]) -> Result<HashMap<&[u8], (bool, ObjectId)>, String> {
    let mut out = HashMap::new();
    for e in gix_object::TreeRefIter::from_bytes(data, gix_hash::Kind::Sha1) {
        let e = e.map_err(|e| e.to_string())?;
        if !e.mode.is_commit() {
            let name: &[u8] = e.filename.as_ref();
            out.insert(name, (e.mode.is_tree(), from_gix(e.oid)));
        }
    }
    Ok(out)
}

/// Walks the paths the pack's commits changed. `None` when the graph did
/// not keep its trees: the caller checks every reference instead.
pub async fn changed_paths<S: OldTrees>(
    input: ChangedInput<'_>,
    source: &mut S,
) -> Result<Option<Changed>, StoreError> {
    let g = input.graph;
    if !g.trees_kept() {
        return Ok(None);
    }
    let corrupt = |what: String| StoreError::Corrupt {
        what: format!("pushed pack: {what}"),
    };
    let n = g.len();
    let mut out = Changed {
        visited: vec![false; n],
        held: vec![false; n],
        check: Vec::new(),
        trees: Vec::new(),
        stats: ChangedStats::default(),
    };
    let known = |id: &ObjectId| input.known.contains(id);

    // Roots: each pushed commit's tree against its first parent's.
    let mut need_commit_trees = Vec::new();
    for c in g.commits() {
        if let Some(p) = c.parents.first()
            && g.position(p).is_none()
            && known(p)
        {
            need_commit_trees.push(*p);
        }
    }
    need_commit_trees.sort_unstable();
    need_commit_trees.dedup();
    let dep_roots: HashMap<ObjectId, ObjectId> = if need_commit_trees.is_empty() {
        HashMap::new()
    } else {
        let got = source.commit_trees(&need_commit_trees).await?;
        need_commit_trees
            .iter()
            .zip(got)
            .filter_map(|(c, t)| Some((*c, t?)))
            .collect()
    };
    let mut frontier: Vec<(usize, Option<Old>)> = Vec::new();
    for c in g.commits() {
        out.stats.commits += 1;
        let at = g.position(&c.id).expect("a commit of the pack");
        out.visited[at] = true;
        for p in &c.parents {
            if g.position(p).is_none() && !known(p) {
                out.check.push(*p);
            }
        }
        let tree = tree_of(g, at).ok_or_else(|| corrupt(format!("commit {} has no tree", c.id)))?;
        let old = match c.parents.first() {
            None => None,
            Some(p) => match g.position(p) {
                Some(pp) if g.kind(pp) == ObjectKind::Commit => tree_of(g, pp).map(|id| Old {
                    id,
                    from_deps: false,
                }),
                Some(_) => None,
                None => dep_roots.get(p).map(|&id| Old {
                    id,
                    from_deps: true,
                }),
            },
        };
        if let Some(o) = old
            && o.id == tree
        {
            if o.from_deps
                && let Some(t) = g.position(&tree)
            {
                out.held[t] = true;
            }
            continue;
        }
        match g.position(&tree) {
            Some(t) if g.kind(t) == ObjectKind::Tree => {
                if !out.visited[t] {
                    out.visited[t] = true;
                    frontier.push((t, old));
                }
            }
            Some(_) => {}
            None => out.check.push(tree),
        }
    }

    // One level of the walk at a time, so old trees are read in batches.
    while !frontier.is_empty() {
        let mut want: Vec<ObjectId> = frontier
            .iter()
            .filter_map(|(_, o)| o.map(|o| o.id))
            .filter(|id| g.position(id).and_then(|p| g.tree_data(p)).is_none())
            .collect();
        want.sort_unstable();
        want.dedup();
        let read: HashMap<ObjectId, Arc<[u8]>> = if want.is_empty() {
            HashMap::new()
        } else {
            let got = source.trees(&want).await?;
            want.iter()
                .zip(got)
                .filter_map(|(id, d)| Some((*id, d?)))
                .collect()
        };
        let mut next = Vec::new();
        for (at, old) in frontier {
            out.stats.trees += 1;
            out.trees.push(at);
            let new = g
                .tree_data(at)
                .ok_or_else(|| corrupt(format!("tree {} was not kept", g.id(at))))?;
            let old_data = old.and_then(|o| {
                g.position(&o.id)
                    .and_then(|p| g.tree_data(p))
                    .or_else(|| read.get(&o.id))
            });
            if old_data.is_some() {
                out.stats.old_trees += 1;
            }
            let old_entries = match old_data {
                Some(d) => entries(d).unwrap_or_default(),
                None => HashMap::new(),
            };
            let from_deps = old.is_some_and(|o| o.from_deps);
            for e in gix_object::TreeRefIter::from_bytes(new, gix_hash::Kind::Sha1) {
                let e = e.map_err(|e| corrupt(format!("tree {}: {e}", g.id(at))))?;
                out.stats.entries += 1;
                if e.mode.is_commit() {
                    continue;
                }
                let id = from_gix(e.oid);
                let name: &[u8] = e.filename.as_ref();
                let before = old_entries.get(name).copied();
                if before.is_some_and(|(_, o)| o == id) {
                    if from_deps && let Some(p) = g.position(&id) {
                        out.held[p] = true;
                    }
                    continue;
                }
                match g.position(&id) {
                    Some(p) => match g.kind(p) {
                        ObjectKind::Tree if !out.visited[p] => {
                            out.visited[p] = true;
                            let old = before
                                .filter(|(is_tree, _)| *is_tree)
                                .map(|(_, id)| Old { id, from_deps });
                            next.push((p, old));
                        }
                        ObjectKind::Blob => out.visited[p] = true,
                        _ => {}
                    },
                    None => out.check.push(id),
                }
            }
        }
        frontier = next;
    }
    out.check.sort_unstable();
    out.check.dedup();
    Ok(Some(out))
}

/// Trees and commits' root trees, bounded in bytes, least recently used
/// evicted first: what [`changed_paths`] reads most, the trees under
/// trunk's newest tips. Keyed by repository as well as ID, so one
/// repository's trees never answer for another's. Objects are immutable,
/// so a hit is always right; what makes a tree count as held is the walk
/// reaching it from a dependency's tip, never the cache.
#[derive(Debug)]
pub struct TreeCache {
    inner: Mutex<TreeCacheInner>,
}

#[derive(Debug)]
struct TreeCacheInner {
    trees: LinkedHashMap<(RepoId, ObjectId), Arc<[u8]>>,
    roots: LinkedHashMap<(RepoId, ObjectId), ObjectId>,
    bytes: u64,
    budget: u64,
}

/// What a cached commit-to-tree mapping costs, counted against the budget.
const ROOT_COST: u64 = 96;

/// Fixed cost of a cached tree beside its bytes.
const TREE_COST: u64 = 64;

impl TreeCache {
    /// A cache of at most `budget` bytes; 0 keeps nothing.
    pub fn new(budget: u64) -> Self {
        Self {
            inner: Mutex::new(TreeCacheInner {
                trees: LinkedHashMap::new(),
                roots: LinkedHashMap::new(),
                bytes: 0,
                budget,
            }),
        }
    }

    /// The tree `id` of `repo`, if cached.
    pub fn tree(&self, repo: RepoId, id: &ObjectId) -> Option<Arc<[u8]>> {
        let mut c = self.inner.lock().expect("not poisoned");
        c.trees.to_back(&(repo, *id)).cloned()
    }

    /// The root tree of commit `id` of `repo`, if cached.
    pub fn root(&self, repo: RepoId, id: &ObjectId) -> Option<ObjectId> {
        let mut c = self.inner.lock().expect("not poisoned");
        c.roots.to_back(&(repo, *id)).copied()
    }

    /// Keeps tree `id` of `repo`, whose bytes are `data`.
    pub fn insert_tree(&self, repo: RepoId, id: ObjectId, data: Arc<[u8]>) {
        let mut c = self.inner.lock().expect("not poisoned");
        let cost = data.len() as u64 + TREE_COST;
        if cost > c.budget || c.trees.contains_key(&(repo, id)) {
            return;
        }
        c.bytes += cost;
        c.trees.insert((repo, id), data);
        c.trim();
    }

    /// Keeps that commit `commit` of `repo` has root tree `tree`.
    pub fn insert_root(&self, repo: RepoId, commit: ObjectId, tree: ObjectId) {
        let mut c = self.inner.lock().expect("not poisoned");
        if ROOT_COST > c.budget || c.roots.contains_key(&(repo, commit)) {
            return;
        }
        c.bytes += ROOT_COST;
        c.roots.insert((repo, commit), tree);
        c.trim();
    }

    /// Bytes held.
    pub fn bytes(&self) -> u64 {
        self.inner.lock().expect("not poisoned").bytes
    }
}

impl TreeCacheInner {
    fn trim(&mut self) {
        while self.bytes > self.budget {
            // Trees go first: a root is tiny and is what every walk starts
            // from.
            if let Some((_, d)) = self.trees.pop_front() {
                self.bytes -= d.len() as u64 + TREE_COST;
            } else if self.roots.pop_front().is_some() {
                self.bytes -= ROOT_COST;
            } else {
                break;
            }
        }
    }
}

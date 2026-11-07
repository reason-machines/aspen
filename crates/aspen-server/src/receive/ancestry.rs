//! "Is commit `a` an ancestor of commit `b`?", answered exactly from the
//! pushed pack, `commit_index`, and the packs of the versions involved.
//!
//! The walk paints the ancestors of `a` with one flag and those of `b` with
//! another, newest committer time first, like Git's `paint_down_to_common`.
//! `a` is an ancestor of `b` exactly when `a` gets `b`'s flag. The walk stops
//! as soon as no queued commit carries `b`'s flag without `a`'s: every path
//! from `b` to `a` runs through such a commit, because a commit with both
//! flags that reached `a` would be both above and below `a`. Committer time
//! only orders the walk; a skewed clock costs visits, never correctness.
//!
//! Among commits with equal committer times (common: an agent commits and
//! pushes several times a second), commits with both flags go first, then
//! the rest first in, first out, `b` before `a`. First in, first out moves
//! both sides one generation at a time, so a walk costs about the distance
//! between the two commits: a fast-forward by one commit expands one. Both
//! flags first lets the paint that ends the walk catch up with `b`'s side
//! when `a` is the newer commit, which in a first-in-first-out queue stays
//! one generation behind it down to the root. Ties used to be broken by
//! object ID, and then the old tip's side of a fast-forward check walked
//! down every same-second ancestor whose ID sorted above the new tip's.
//!
//! **Generation numbers** (decisions log, "12c: generation numbers"). When
//! both commits have one (Git's commit-graph topological level, from
//! `commit_index` or computed from the pushed pack), the walk instead goes
//! down from `b` only through commits whose generation is larger than
//! `a`'s, highest first: every ancestor of a commit has a smaller
//! generation, so nothing below `a`'s generation can lead to `a`. A writer
//! pushing to its own branch, based on a trunk tip that has moved on
//! since, asks whether trunk's tip is an ancestor of its new tip; trunk's
//! tip then usually has the larger generation and the answer needs no walk
//! at all, where the paint walk went down to the branch's base, a cost
//! that grew with every trunk landing since. A commit on the way without a
//! generation (a row written before migration 0011, or one the backfill has
//! not reached) sends the question to the paint walk, which needs none.
//!
//! A commit the walk cannot resolve (no `commit_index` row, and not in any
//! pack it may read) makes the answer [`Ancestry::Undecided`], as does a
//! walk past its visit budget. The commit check turns that into a
//! retryable `ng`, never a guessed verdict (decisions log, S5 review).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::future::Future;

use aspen_core::error::StoreError;
use aspen_core::id::ObjectId;
use aspen_core::ingest::SortedIds;
use aspen_core::refs::Ancestry;

/// A commit's parents and committer time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitNode {
    /// Parents, in commit order.
    pub(crate) parents: Vec<ObjectId>,
    /// Committer time, seconds since the epoch.
    pub(crate) time: i64,
    /// Generation number, when known (`commit_index`'s, or computed by
    /// [`Walker::generation`] for a commit of the pushed pack).
    pub(crate) generation: Option<u64>,
}

/// What a source knows about one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Lookup {
    /// A commit.
    Commit(CommitNode),
    /// An object that is not a commit: it has no ancestors.
    NotCommit,
    /// Unknown here: not indexed and not readable.
    Unknown,
}

/// Where commits come from.
pub(crate) trait CommitSource: Send + Sync {
    /// One answer per ID, in order.
    fn lookup(
        &self,
        ids: &SortedIds,
    ) -> impl Future<Output = Result<Vec<Lookup>, StoreError>> + Send;
}

const FROM_A: u8 = 1;
const FROM_B: u8 = 2;
const BOTH: u8 = FROM_A | FROM_B;

/// The walk's queue: newest committer time first; among equal times,
/// commits with both flags first, then first in, first out.
#[derive(Default)]
struct Queue {
    heap: BinaryHeap<(i64, bool, Reverse<u64>, ObjectId)>,
    pushed: u64,
}

impl Queue {
    fn push(&mut self, time: i64, flags: u8, id: ObjectId) {
        self.heap
            .push((time, flags == BOTH, Reverse(self.pushed), id));
        self.pushed += 1;
    }

    fn pop(&mut self) -> Option<ObjectId> {
        self.heap.pop().map(|(_, _, _, id)| id)
    }
}

/// An ancestry walker with a cache of the commits it has seen, shared by
/// every question of one push.
#[derive(Debug)]
pub(crate) struct Walker {
    known: HashMap<ObjectId, Lookup>,
    /// Commits of the pushed pack ([`Walker::seed`]): the only ones whose
    /// generation the walker computes rather than reads.
    pushed: HashSet<ObjectId>,
    /// Generation numbers of stored commits known without reading their
    /// rows ([`Walker::remember_generation`]: the tips of the refs the push
    /// read). They feed generation numbers and the walks' cut-off, never
    /// parents: such a commit is still looked up when a walk expands it.
    generations: HashMap<ObjectId, u64>,
    /// Most commits one question may expand.
    budget: usize,
    /// Commits expanded by every question so far.
    expanded: u64,
}

impl Walker {
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            known: HashMap::new(),
            pushed: HashSet::new(),
            generations: HashMap::new(),
            budget,
            expanded: 0,
        }
    }

    /// Commits expanded by every question this walker answered.
    pub(crate) fn expanded(&self) -> u64 {
        self.expanded
    }

    /// Seeds the cache with commits known without a lookup (the pushed
    /// pack's).
    pub(crate) fn seed(&mut self, id: ObjectId, node: CommitNode) {
        self.pushed.insert(id);
        self.known.insert(id, Lookup::Commit(node));
    }

    /// Records the generation number of a stored commit, read with
    /// something else: a ref's tip, whose generation the push's read of the
    /// ref returns (`MetaStore::current_versions`). A fast-forward's new
    /// commits sit on such a tip, so their generations need no lookup.
    pub(crate) fn remember_generation(&mut self, id: ObjectId, generation: u64) {
        self.generations.insert(id, generation);
    }

    fn generation_of(&self, id: &ObjectId) -> Option<u64> {
        match self.known.get(id) {
            Some(Lookup::Commit(n)) if n.generation.is_some() => n.generation,
            _ => self.generations.get(id).copied(),
        }
    }

    /// `id`'s generation number, if it can be known cheaply: a stored
    /// commit's comes from its `commit_index` row (or is unknown); a commit
    /// of the pushed pack's is computed from the pushed commits below it
    /// and their parents outside the pack, whose rows are read in one
    /// lookup. Nothing walks stored history: a parent outside the pack
    /// whose row has no generation leaves the commit without one.
    pub(crate) async fn generation<S: CommitSource>(
        &mut self,
        source: &S,
        id: ObjectId,
    ) -> Result<Option<u64>, StoreError> {
        if let Some(g) = self.generation_of(&id) {
            return Ok(Some(g));
        }
        if !self.pushed.contains(&id) {
            self.fetch(source, [id]).await?;
            return Ok(self.generation_of(&id));
        }
        let missing = self.unread_for_generation(id);
        self.fetch(source, missing).await?;
        Ok(self.known_generation(id))
    }

    /// `id`'s generation if it needs no lookup: a known commit's, or a
    /// pushed commit's whose parents outside the pack are all known
    /// already. The push path writes a row's generation only this way, so
    /// that knowing it never costs a statement (decisions log, "12c:
    /// generation numbers"); a row left without one gets it from the
    /// `index_commits` job.
    pub(crate) fn known_generation(&mut self, id: ObjectId) -> Option<u64> {
        if let Some(g) = self.generation_of(&id) {
            return Some(g);
        }
        if !self.pushed.contains(&id) {
            return None;
        }
        let (inside, outside) = self.pushed_below(id);
        if outside.iter().any(|p| self.generation_of(p).is_none()) {
            return None;
        }
        let parents: Vec<(ObjectId, Vec<ObjectId>)> = inside
            .iter()
            .filter_map(|c| match self.known.get(c) {
                Some(Lookup::Commit(n)) => Some((*c, n.parents.clone())),
                _ => None,
            })
            .collect();
        let gens = aspen_core::generation::generations(
            parents.iter().map(|(c, ps)| (*c, ps.as_slice())),
            |p| self.generation_of(p),
        );
        for (c, g) in gens {
            if let (Some(g), Some(Lookup::Commit(n))) = (g, self.known.get_mut(&c)) {
                n.generation = Some(g);
            }
        }
        self.generation_of(&id)
    }

    /// The commits whose rows computing `id`'s generation needs and the
    /// walker has not read: none for a commit with a known generation, the
    /// commit itself for a stored one, and the unread parents outside the
    /// pack of the pushed commits below a pushed one.
    fn unread_for_generation(&self, id: ObjectId) -> Vec<ObjectId> {
        if self.generation_of(&id).is_some() {
            return Vec::new();
        }
        if !self.pushed.contains(&id) {
            return if self.known.contains_key(&id) {
                Vec::new()
            } else {
                vec![id]
            };
        }
        let (_, outside) = self.pushed_below(id);
        outside
            .into_iter()
            .filter(|p| !self.known.contains_key(p) && self.generation_of(p).is_none())
            .collect()
    }

    /// The pushed commits without a generation that `id` reaches without
    /// leaving the pack (`id` first), and the commits just outside it.
    fn pushed_below(&self, id: ObjectId) -> (Vec<ObjectId>, HashSet<ObjectId>) {
        let mut inside: Vec<ObjectId> = Vec::new();
        let mut outside: HashSet<ObjectId> = HashSet::new();
        let mut seen: HashSet<ObjectId> = HashSet::from([id]);
        let mut stack = vec![id];
        while let Some(c) = stack.pop() {
            let Some(Lookup::Commit(n)) = self.known.get(&c) else {
                continue;
            };
            if n.generation.is_some() {
                continue;
            }
            inside.push(c);
            for p in &n.parents {
                if self.pushed.contains(p) {
                    if seen.insert(*p) {
                        stack.push(*p);
                    }
                } else {
                    outside.insert(*p);
                }
            }
        }
        (inside, outside)
    }

    async fn fetch<S: CommitSource>(
        &mut self,
        source: &S,
        ids: impl IntoIterator<Item = ObjectId>,
    ) -> Result<(), StoreError> {
        let want: Vec<ObjectId> = ids
            .into_iter()
            .filter(|id| !self.known.contains_key(id))
            .collect();
        if want.is_empty() {
            return Ok(());
        }
        let want = SortedIds::from_unsorted(want);
        let got = source.lookup(&want).await?;
        if got.len() != want.len() {
            return Err(StoreError::Corrupt {
                what: format!("commit lookup answered {} of {} IDs", got.len(), want.len()),
            });
        }
        for (id, l) in want.as_slice().iter().zip(got) {
            self.known.insert(*id, l);
        }
        Ok(())
    }

    /// Whether `a` is an ancestor of (or equal to) `b`.
    pub(crate) async fn is_ancestor<S: CommitSource>(
        &mut self,
        source: &S,
        a: ObjectId,
        b: ObjectId,
    ) -> Result<Ancestry, StoreError> {
        if a == b {
            return Ok(Ancestry::Descendant);
        }
        // The rows the generations of `a` and `b` need ride along with a
        // lookup this question makes anyway, and never cost one of their
        // own: when both are known already (the pushed pack holds them),
        // a question with no known generation paints, as before
        // generations, and the paint walk reads only what it reaches.
        let mut want: Vec<ObjectId> = [a, b]
            .into_iter()
            .filter(|id| !self.known.contains_key(id))
            .collect();
        if !want.is_empty() {
            want.extend(self.unread_for_generation(a));
            want.extend(self.unread_for_generation(b));
        }
        self.fetch(source, want).await?;
        match (self.known.get(&a), self.known.get(&b)) {
            (Some(Lookup::Unknown), _) | (_, Some(Lookup::Unknown)) => {
                return Ok(Ancestry::Undecided);
            }
            // A non-commit has no ancestors and is no commit's ancestor.
            (Some(Lookup::NotCommit), _) | (_, Some(Lookup::NotCommit)) => {
                return Ok(Ancestry::NotDescendant);
            }
            _ => {}
        }
        let ga = self.known_generation(a);
        let gb = self.known_generation(b);
        if let (Some(ga), Some(gb)) = (ga, gb)
            && let Some(answer) = self.generation_walk(source, a, ga, b, gb).await?
        {
            return Ok(answer);
        }
        self.paint_walk(source, a, b).await
    }

    /// The walk with generation numbers: down from `b` through commits
    /// whose generation is larger than `ga`, highest first. `None` when a
    /// commit on the way has no generation (the caller then paints).
    async fn generation_walk<S: CommitSource>(
        &mut self,
        source: &S,
        a: ObjectId,
        ga: u64,
        b: ObjectId,
        gb: u64,
    ) -> Result<Option<Ancestry>, StoreError> {
        if gb <= ga {
            // Every ancestor of `b` has a smaller generation than `b`, and
            // `a != b`.
            return Ok(Some(Ancestry::NotDescendant));
        }
        let mut heap: BinaryHeap<(u64, Reverse<u64>, ObjectId)> = BinaryHeap::new();
        let mut order = 0u64;
        let mut queued: HashSet<ObjectId> = HashSet::from([b]);
        heap.push((gb, Reverse(order), b));
        let mut expanded = 0usize;
        while let Some((_, _, c)) = heap.pop() {
            expanded += 1;
            self.expanded += 1;
            if expanded > self.budget {
                return Ok(Some(Ancestry::Undecided));
            }
            let parents = match self.known.get(&c) {
                Some(Lookup::Commit(n)) => n.parents.clone(),
                _ => unreachable!("only resolved commits are queued"),
            };
            if parents.contains(&a) {
                return Ok(Some(Ancestry::Descendant));
            }
            let fresh: Vec<ObjectId> = parents
                .into_iter()
                .filter(|p| !queued.contains(p))
                .collect();
            if fresh.iter().any(|p| !self.known.contains_key(p)) {
                self.fetch(source, fresh.clone()).await?;
            }
            for p in fresh {
                match self.known.get(&p) {
                    Some(Lookup::Commit(_)) => {}
                    // As in the paint walk: history below an unresolvable
                    // parent cannot be decided.
                    _ => return Ok(Some(Ancestry::Undecided)),
                }
                let gp = match self.generation_of(&p) {
                    Some(g) => g,
                    None if self.pushed.contains(&p) => match self.generation(source, p).await? {
                        Some(g) => g,
                        None => return Ok(None),
                    },
                    None => return Ok(None),
                };
                queued.insert(p);
                if gp > ga {
                    order += 1;
                    heap.push((gp, Reverse(order), p));
                }
            }
        }
        Ok(Some(Ancestry::NotDescendant))
    }

    /// The paint walk, which needs no generation numbers.
    async fn paint_walk<S: CommitSource>(
        &mut self,
        source: &S,
        a: ObjectId,
        b: ObjectId,
    ) -> Result<Ancestry, StoreError> {
        let time = |w: &Self, id: &ObjectId| match w.known.get(id) {
            Some(Lookup::Commit(n)) => Some(n.time),
            _ => None,
        };
        let mut flags: HashMap<ObjectId, u8> = HashMap::from([(a, FROM_A), (b, FROM_B)]);
        // Each commit is queued at most once; one whose flags grow after it
        // was expanded is queued again. `b_only` counts queued commits whose
        // flags are exactly FROM_B: the walk ends when it reaches zero.
        // Newest first; among equal times, both flags first, then first
        // queued first. A queued commit that gets both flags is pushed again
        // with the higher rank, and its older entry is skipped when popped.
        let mut heap = Queue::default();
        let mut queued: HashSet<ObjectId> = HashSet::new();
        let mut b_only = 1usize;
        for (id, f) in [(b, FROM_B), (a, FROM_A)] {
            heap.push(time(self, &id).expect("a commit"), f, id);
            queued.insert(id);
        }
        let mut expanded = 0usize;
        while b_only > 0 {
            let Some(c) = heap.pop() else { break };
            if !queued.remove(&c) {
                continue;
            }
            let f = flags[&c];
            if f == FROM_B {
                b_only -= 1;
            }
            expanded += 1;
            self.expanded += 1;
            if expanded > self.budget {
                return Ok(Ancestry::Undecided);
            }
            let parents = match self.known.get(&c) {
                Some(Lookup::Commit(n)) => n.parents.clone(),
                _ => unreachable!("only resolved commits are queued"),
            };
            let fresh: Vec<ObjectId> = parents
                .iter()
                .copied()
                .filter(|p| {
                    flags.get(p).copied().unwrap_or(0) | f != flags.get(p).copied().unwrap_or(0)
                })
                .collect();
            if fresh.iter().any(|p| !self.known.contains_key(p)) {
                // Fetch these and the next unresolved commits at once.
                let batch: Vec<ObjectId> = fresh.clone();
                self.fetch(source, batch).await?;
            }
            for p in fresh {
                let old = flags.get(&p).copied().unwrap_or(0);
                let new = old | f;
                if p == a && new & FROM_B != 0 {
                    return Ok(Ancestry::Descendant);
                }
                let t = match self.known.get(&p) {
                    Some(Lookup::Commit(n)) => n.time,
                    // A parent that is not a commit, or that no source has:
                    // the history below it cannot be decided.
                    _ => return Ok(Ancestry::Undecided),
                };
                flags.insert(p, new);
                if queued.contains(&p) {
                    if old == FROM_B && new != FROM_B {
                        b_only -= 1;
                    }
                    if new == BOTH {
                        heap.push(t, new, p);
                    }
                } else {
                    queued.insert(p);
                    heap.push(t, new, p);
                    if new == FROM_B {
                        b_only += 1;
                    }
                }
            }
        }
        Ok(Ancestry::NotDescendant)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(n: u32) -> ObjectId {
        let mut b = [0u8; 20];
        b[..4].copy_from_slice(&n.to_be_bytes());
        b[19] = 1;
        ObjectId::from_bytes(b)
    }

    /// An in-memory history: commit `n` has the listed parents and time.
    struct Mem(
        HashMap<ObjectId, CommitNode>,
        std::sync::atomic::AtomicUsize,
    );

    impl Mem {
        fn new(edges: &[(u32, &[u32], i64)]) -> Self {
            Self(
                edges
                    .iter()
                    .map(|(c, ps, t)| {
                        (
                            oid(*c),
                            CommitNode {
                                parents: ps.iter().map(|p| oid(*p)).collect(),
                                time: *t,
                                generation: None,
                            },
                        )
                    })
                    .collect(),
                Default::default(),
            )
        }

        /// The same history with every commit's generation number, as
        /// `commit_index` holds them once backfilled.
        fn with_generations(mut self) -> Self {
            let gens = aspen_core::generation::generations(
                self.0.iter().map(|(id, n)| (*id, n.parents.as_slice())),
                |_| None,
            );
            for (id, n) in &mut self.0 {
                n.generation = gens[id];
            }
            self
        }

        /// Drops the generation of each commit `keep` refuses (rows not
        /// backfilled yet).
        fn dropping_generations(mut self, mut keep: impl FnMut(&ObjectId) -> bool) -> Self {
            for (id, n) in &mut self.0 {
                if !keep(id) {
                    n.generation = None;
                }
            }
            self
        }

        fn lookups(&self) -> usize {
            self.1.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl CommitSource for Mem {
        async fn lookup(&self, ids: &SortedIds) -> Result<Vec<Lookup>, StoreError> {
            self.1
                .fetch_add(ids.len(), std::sync::atomic::Ordering::Relaxed);
            Ok(ids
                .as_slice()
                .iter()
                .map(|id| match self.0.get(id) {
                    Some(n) => Lookup::Commit(n.clone()),
                    None => Lookup::Unknown,
                })
                .collect())
        }
    }

    fn ask(mem: &Mem, a: u32, b: u32) -> Ancestry {
        let mut w = Walker::new(10_000);
        futures_util::FutureExt::now_or_never(w.is_ancestor(mem, oid(a), oid(b)))
            .expect("in-memory source is ready")
            .unwrap()
    }

    /// Reachability by brute force, the oracle.
    fn reaches(mem: &Mem, from: u32, to: u32) -> bool {
        let mut stack = vec![oid(from)];
        let mut seen = std::collections::HashSet::new();
        while let Some(c) = stack.pop() {
            if c == oid(to) {
                return true;
            }
            if seen.insert(c)
                && let Some(n) = mem.0.get(&c)
            {
                stack.extend(n.parents.iter().copied());
            }
        }
        false
    }

    #[test]
    fn a_linear_history_is_ordered() {
        let mem = Mem::new(&[(1, &[], 10), (2, &[1], 20), (3, &[2], 30)]);
        assert_eq!(ask(&mem, 1, 3), Ancestry::Descendant);
        assert_eq!(ask(&mem, 3, 1), Ancestry::NotDescendant);
        assert_eq!(ask(&mem, 2, 2), Ancestry::Descendant);
    }

    /// Invariant: the answer equals brute-force reachability on every pair
    /// of a random DAG whose committer times are scrambled (clock skew), so
    /// the stopping rule never depends on time order.
    #[test]
    fn answers_match_reachability_under_clock_skew() {
        let mut rng = aspen_testkit::rng::Rng::new(0xA5_B1);
        for _ in 0..40 {
            let n = 2 + rng.below(30) as u32;
            let mut edges: Vec<(u32, Vec<u32>, i64)> = Vec::new();
            for c in 1..=n {
                let mut ps = Vec::new();
                if c > 1 {
                    for _ in 0..(1 + rng.below(2)) {
                        // Some roots, mostly one or two parents.
                        if rng.below(8) != 0 {
                            ps.push(1 + rng.below(c as usize - 1) as u32);
                        }
                    }
                }
                ps.dedup();
                edges.push((c, ps, rng.below(1000) as i64));
            }
            let refs: Vec<(u32, &[u32], i64)> = edges
                .iter()
                .map(|(c, ps, t)| (*c, ps.as_slice(), *t))
                .collect();
            let mem = Mem::new(&refs);
            for a in 1..=n {
                for b in 1..=n {
                    let want = if reaches(&mem, b, a) {
                        Ancestry::Descendant
                    } else {
                        Ancestry::NotDescendant
                    };
                    assert_eq!(ask(&mem, a, b), want, "a={a} b={b} edges={edges:?}");
                }
            }
        }
    }

    #[test]
    fn the_walk_stops_near_the_merge_base_not_at_the_root() {
        // A long shared history 1..=1000, then two short forks.
        let mut edges: Vec<(u32, Vec<u32>, i64)> = (1..=1000)
            .map(|c| (c, if c == 1 { vec![] } else { vec![c - 1] }, c as i64))
            .collect();
        edges.push((2001, vec![1000], 2001));
        edges.push((2002, vec![2001], 2002));
        edges.push((3001, vec![1000], 2001));
        let refs: Vec<(u32, &[u32], i64)> = edges
            .iter()
            .map(|(c, ps, t)| (*c, ps.as_slice(), *t))
            .collect();
        let mem = Mem::new(&refs);
        assert_eq!(ask(&mem, 3001, 2002), Ancestry::NotDescendant);
        let looked_up = mem.1.load(std::sync::atomic::Ordering::Relaxed);
        assert!(looked_up < 50, "looked up {looked_up} commits");
        assert_eq!(ask(&mem, 990, 2002), Ancestry::Descendant);
    }

    /// Expansions of one fresh walk of "is `a` an ancestor of `b`".
    fn cost(mem: &Mem, a: u32, b: u32) -> (Ancestry, u64) {
        let mut w = Walker::new(10_000);
        let got = futures_util::FutureExt::now_or_never(w.is_ancestor(mem, oid(a), oid(b)))
            .expect("in-memory source is ready")
            .unwrap();
        (got, w.expanded())
    }

    /// Commit `k`'s label in [`Mem`]: object IDs are hashes, so their order
    /// says nothing about the history's.
    fn scrambled(k: u32) -> u32 {
        k.wrapping_mul(0x9E37_79B1)
    }

    /// Invariant: with equal committer times (an agent commits and pushes
    /// several times a second), what a walk costs follows the distance
    /// between the commits, not their object IDs. Ties used to fall back
    /// to the ID, so the old tip's side of a fast-forward check went down
    /// every same-second ancestor whose ID was larger than the new tip's
    /// (`branch_push_cost.rs` failed intermittently on that).
    #[test]
    fn equal_commit_times_cost_the_distance_not_the_history() {
        let s = scrambled;
        let mut edges: Vec<(u32, Vec<u32>, i64)> = (1..=300)
            .map(|c| (s(c), if c == 1 { vec![] } else { vec![s(c - 1)] }, 1_000))
            .collect();
        let mem = |edges: &[(u32, Vec<u32>, i64)]| {
            let refs: Vec<(u32, &[u32], i64)> = edges
                .iter()
                .map(|(c, ps, t)| (*c, ps.as_slice(), *t))
                .collect();
            Mem::new(&refs)
        };
        let linear = mem(&edges);
        for b in 2..=300 {
            // A fast-forward by one commit, the push path's common case.
            assert_eq!(
                cost(&linear, s(b - 1), s(b)),
                (Ancestry::Descendant, 1),
                "b={b}"
            );
            for d in [2, 5, 20] {
                if b > d {
                    let (got, n) = cost(&linear, s(b - d), s(b));
                    assert_eq!(got, Ancestry::Descendant);
                    assert!(n <= 2 * u64::from(d), "{d} back from {b}: {n} expansions");
                }
            }
            // The reverse question stops near the two commits too.
            let (got, n) = cost(&linear, s(b), s(b - 1));
            assert_eq!(got, Ancestry::NotDescendant);
            assert!(n <= 4, "{b} above {}: {n} expansions", b - 1);
        }
        // Two forks off a shared same-second history stop near their merge
        // base, not at the root.
        edges.push((s(1001), vec![s(300)], 1_000));
        edges.push((s(1002), vec![s(1001)], 1_000));
        edges.push((s(2001), vec![s(300)], 1_000));
        let forks = mem(&edges);
        for (a, b) in [(2001, 1002), (1002, 2001), (1001, 2001)] {
            let (got, n) = cost(&forks, s(a), s(b));
            assert_eq!(got, Ancestry::NotDescendant);
            assert!(n <= 12, "{a} and {b}: {n} expansions");
        }
    }

    /// Invariant: exact on random DAGs where most commits share a few
    /// committer times, so the tie order decides the walk.
    #[test]
    fn answers_match_reachability_when_times_tie() {
        let mut rng = aspen_testkit::rng::Rng::new(0x71E5);
        for _ in 0..40 {
            let n = 2 + rng.below(30) as u32;
            let mut edges: Vec<(u32, Vec<u32>, i64)> = Vec::new();
            for c in 1..=n {
                let mut ps = Vec::new();
                if c > 1 {
                    for _ in 0..(1 + rng.below(2)) {
                        if rng.below(8) != 0 {
                            ps.push(1 + rng.below(c as usize - 1) as u32);
                        }
                    }
                }
                ps.dedup();
                edges.push((c, ps, rng.below(3) as i64));
            }
            let refs: Vec<(u32, &[u32], i64)> = edges
                .iter()
                .map(|(c, ps, t)| (*c, ps.as_slice(), *t))
                .collect();
            let mem = Mem::new(&refs);
            for a in 1..=n {
                for b in 1..=n {
                    let want = if reaches(&mem, b, a) {
                        Ancestry::Descendant
                    } else {
                        Ancestry::NotDescendant
                    };
                    assert_eq!(ask(&mem, a, b), want, "a={a} b={b} edges={edges:?}");
                }
            }
        }
    }

    #[test]
    fn an_unresolvable_commit_is_undecided_never_guessed() {
        // 3's parent 2 is unknown: 1 might be below it.
        let mem = Mem::new(&[(1, &[], 10), (3, &[2], 30)]);
        assert_eq!(ask(&mem, 1, 3), Ancestry::Undecided);
        // Unknown endpoints too.
        assert_eq!(ask(&mem, 1, 9), Ancestry::Undecided);
    }

    #[test]
    fn a_walk_past_its_budget_is_undecided() {
        let edges: Vec<(u32, Vec<u32>, i64)> = (1..=100)
            .map(|c| (c, if c == 1 { vec![] } else { vec![c - 1] }, c as i64))
            .collect();
        let refs: Vec<(u32, &[u32], i64)> = edges
            .iter()
            .map(|(c, ps, t)| (*c, ps.as_slice(), *t))
            .collect();
        let mem = Mem::new(&refs);
        let mut w = Walker::new(10);
        let got = futures_util::FutureExt::now_or_never(w.is_ancestor(&mem, oid(1), oid(100)))
            .unwrap()
            .unwrap();
        assert_eq!(got, Ancestry::Undecided);
        assert_eq!(ask(&mem, 1, 100), Ancestry::Descendant);
    }

    /// A random DAG: commit `c` has parents below it, usually one or two,
    /// one in ten an octopus merge of three to five; a few are roots, and
    /// committer times are drawn from `times` values.
    fn random_dag(rng: &mut aspen_testkit::rng::Rng, times: usize) -> Vec<(u32, Vec<u32>, i64)> {
        let n = 2 + rng.below(30) as u32;
        let mut edges = Vec::new();
        for c in 1..=n {
            let mut ps: Vec<u32> = Vec::new();
            if c > 1 {
                let want = if rng.below(10) == 0 {
                    3 + rng.below(3)
                } else {
                    1 + rng.below(2)
                };
                for _ in 0..want {
                    if rng.below(8) != 0 {
                        let p = 1 + rng.below(c as usize - 1) as u32;
                        if !ps.contains(&p) {
                            ps.push(p);
                        }
                    }
                }
            }
            edges.push((c, ps, rng.below(times) as i64));
        }
        edges
    }

    fn mem_of(edges: &[(u32, Vec<u32>, i64)]) -> Mem {
        let refs: Vec<(u32, &[u32], i64)> = edges
            .iter()
            .map(|(c, ps, t)| (*c, ps.as_slice(), *t))
            .collect();
        Mem::new(&refs)
    }

    /// Invariant: with every, some or no commit carrying a generation
    /// number, and with some commits seeded as the pushed pack's (their
    /// generations computed by the walker, never read), every answer
    /// equals brute-force reachability.
    #[test]
    fn answers_match_reachability_with_all_some_or_no_generations() {
        let mut rng = aspen_testkit::rng::Rng::new(0x12C0);
        for round in 0..60 {
            let times = 1 + rng.below(1000);
            let edges = random_dag(&mut rng, times);
            let plain = mem_of(&edges);
            let n = edges.len() as u32;
            // Which commits keep a generation, and which are "pushed".
            let keep: Vec<bool> = (0..=n).map(|_| rng.below(3) != 0).collect();
            let pushed: Vec<bool> = (0..=n).map(|_| rng.below(4) == 0).collect();
            let label =
                |id: &ObjectId| u32::from_be_bytes(id.as_bytes()[..4].try_into().unwrap()) as usize;
            let sources = [
                ("none", mem_of(&edges)),
                ("all", mem_of(&edges).with_generations()),
                (
                    "some",
                    mem_of(&edges)
                        .with_generations()
                        .dropping_generations(|id| keep[label(id)]),
                ),
            ];
            for (what, mem) in &sources {
                for a in 1..=n {
                    for b in 1..=n {
                        let want = if reaches(&plain, b, a) {
                            Ancestry::Descendant
                        } else {
                            Ancestry::NotDescendant
                        };
                        let mut w = Walker::new(10_000);
                        for (c, ps, t) in &edges {
                            if pushed[*c as usize] {
                                // The pushed pack's commits come without a
                                // generation; the walker computes it.
                                w.seed(
                                    oid(*c),
                                    CommitNode {
                                        parents: ps.iter().map(|p| oid(*p)).collect(),
                                        time: *t,
                                        generation: None,
                                    },
                                );
                            }
                        }
                        let got = futures_util::FutureExt::now_or_never(w.is_ancestor(
                            mem,
                            oid(a),
                            oid(b),
                        ))
                        .expect("in-memory source is ready")
                        .unwrap();
                        assert_eq!(
                            got, want,
                            "round {round} ({what}): a={a} b={b} edges={edges:?}"
                        );
                    }
                }
            }
        }
    }

    /// A pushed commit's generation is computed from the pushed commits
    /// below it and its stored parents' generations, reading only those
    /// parents; a stored parent without a generation leaves it unknown.
    #[test]
    fn a_pushed_commit_gets_its_generation_from_its_parents_rows() {
        let edges: Vec<(u32, Vec<u32>, i64)> = (1..=500)
            .map(|c| (c, if c == 1 { vec![] } else { vec![c - 1] }, c as i64))
            .collect();
        let mem = mem_of(&edges).with_generations();
        let mut w = Walker::new(10_000);
        // Pushed: 1001 <- 1002 <- 1003 on top of stored 500.
        for (c, p) in [(1001, 500), (1002, 1001), (1003, 1002)] {
            w.seed(
                oid(c),
                CommitNode {
                    parents: vec![oid(p)],
                    time: 2_000,
                    generation: None,
                },
            );
        }
        let g = futures_util::FutureExt::now_or_never(w.generation(&mem, oid(1003)))
            .unwrap()
            .unwrap();
        assert_eq!(g, Some(503));
        assert_eq!(mem.lookups(), 1, "only the stored parent is read");

        let without = mem_of(&edges);
        let mut w = Walker::new(10_000);
        w.seed(
            oid(1001),
            CommitNode {
                parents: vec![oid(500)],
                time: 2_000,
                generation: None,
            },
        );
        let g = futures_util::FutureExt::now_or_never(w.generation(&without, oid(1001)))
            .unwrap()
            .unwrap();
        assert_eq!(g, None, "nothing walks stored history for a generation");
        assert_eq!(without.lookups(), 1);
    }

    /// Invariant (the push shape 12c is for): a writer pushes to its own
    /// branch, based on a trunk tip that has since moved on by `moved`
    /// commits, and the push asks whether trunk's tip is an ancestor of the
    /// writer's new tip. With generation numbers the commits read and
    /// expanded stay flat however far trunk moved and however long the
    /// history is; without them the walk goes down to the branch's base.
    #[test]
    fn a_writer_branch_question_costs_the_same_however_far_trunk_moved() {
        let mut flat = Vec::new();
        for (history, moved) in [(200u32, 10u32), (2_000, 100), (5_000, 1_000)] {
            // Trunk 1..=history+moved; the writer's branch has 3 commits on
            // top of trunk commit `history`, the last one being pushed.
            let top = history + moved;
            let mut edges: Vec<(u32, Vec<u32>, i64)> = (1..=top)
                .map(|c| (c, if c == 1 { vec![] } else { vec![c - 1] }, 1_000))
                .collect();
            edges.push((100_001, vec![history], 1_000));
            edges.push((100_002, vec![100_001], 1_000));
            let edges_ = edges.clone();
            for (what, mem) in [
                ("generations", mem_of(&edges_).with_generations()),
                ("none", mem_of(&edges_)),
            ] {
                let mut w = Walker::new(1_000_000);
                w.seed(
                    oid(100_003),
                    CommitNode {
                        parents: vec![oid(100_002)],
                        time: 1_000,
                        generation: None,
                    },
                );
                let got = futures_util::FutureExt::now_or_never(w.is_ancestor(
                    &mem,
                    oid(top),
                    oid(100_003),
                ))
                .unwrap()
                .unwrap();
                assert_eq!(got, Ancestry::NotDescendant);
                if what == "generations" {
                    flat.push((mem.lookups(), w.expanded()));
                } else {
                    assert!(
                        w.expanded() >= u64::from(moved),
                        "the paint walk goes down to the branch's base: {}",
                        w.expanded()
                    );
                }
            }
        }
        // Read: trunk's tip and the writer's new tip's parent; nothing
        // expanded.
        assert!(
            flat.iter().all(|&(l, e)| l <= 3 && e == 0),
            "per history: {flat:?}"
        );
        // A trunk fast-forward by one commit expands one commit.
        let edges: Vec<(u32, Vec<u32>, i64)> = (1..=5_000)
            .map(|c| (c, if c == 1 { vec![] } else { vec![c - 1] }, 1_000))
            .collect();
        let mem = mem_of(&edges).with_generations();
        assert_eq!(cost(&mem, 4_999, 5_000), (Ancestry::Descendant, 1));
        assert_eq!(cost(&mem, 4_990, 5_000), (Ancestry::Descendant, 10));
    }
}

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
//! A commit the walk cannot resolve (no `commit_index` row, and not in any
//! pack it may read) makes the answer [`Ancestry::Undecided`], as does a
//! walk past its visit budget. The commit check turns that into a
//! retryable `ng`, never a guessed verdict (decisions log, S5 review).

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

/// An ancestry walker with a cache of the commits it has seen, shared by
/// every question of one push.
#[derive(Debug)]
pub(crate) struct Walker {
    known: HashMap<ObjectId, Lookup>,
    /// Most commits one question may expand.
    budget: usize,
    /// Commits expanded by every question so far.
    expanded: u64,
}

impl Walker {
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            known: HashMap::new(),
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
        self.known.insert(id, Lookup::Commit(node));
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
        self.fetch(source, [a, b]).await?;
        let time = |w: &Self, id: &ObjectId| match w.known.get(id) {
            Some(Lookup::Commit(n)) => Some(n.time),
            _ => None,
        };
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
        let mut flags: HashMap<ObjectId, u8> = HashMap::from([(a, FROM_A), (b, FROM_B)]);
        // Each commit is queued at most once; one whose flags grow after it
        // was expanded is queued again. `b_only` counts queued commits whose
        // flags are exactly FROM_B: the walk ends when it reaches zero.
        let mut heap: BinaryHeap<(i64, ObjectId)> = BinaryHeap::new();
        let mut queued: HashSet<ObjectId> = HashSet::new();
        let mut b_only = 1usize;
        for id in [a, b] {
            heap.push((time(self, &id).expect("a commit"), id));
            queued.insert(id);
        }
        let mut expanded = 0usize;
        while b_only > 0 {
            let Some((_, c)) = heap.pop() else { break };
            queued.remove(&c);
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
                } else {
                    queued.insert(p);
                    heap.push((t, p));
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
                            },
                        )
                    })
                    .collect(),
                Default::default(),
            )
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
}

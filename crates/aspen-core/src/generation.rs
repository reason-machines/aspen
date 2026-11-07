//! Commit generation numbers (Git's commit-graph "topological level").
//!
//! A commit without parents has generation 1; any other commit has one more
//! than the largest generation of its parents. So every ancestor of a
//! commit has a smaller generation than the commit, and a commit whose
//! generation is not larger than `a`'s cannot have `a` as an ancestor
//! unless it is `a`. An ancestry walk uses that to stop at `a`'s
//! generation instead of walking down to the merge base (decisions log,
//! "12c: generation numbers").
//!
//! A generation is derived data: it depends only on the commit's history,
//! so every writer that knows it computes the same value, and a value
//! written once never needs to change.

use std::collections::HashMap;

use crate::id::ObjectId;

/// What [`CommitGraph::solve`] found for one commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// The commit's generation number.
    Known(u64),
    /// Not known yet: computing it needs the generation of `waits_on`, a
    /// commit whose generation was not available. `waits_on` is outside the
    /// graph (a parent `known` gave nothing for), or reached from this
    /// commit through commits in the graph that wait on it too. Once
    /// `waits_on` has a generation, this commit may be computable, or wait
    /// on another commit.
    Unknown {
        /// The commit it waits on.
        waits_on: ObjectId,
    },
}

impl Level {
    /// The generation, if known.
    pub fn known(self) -> Option<u64> {
        match self {
            Self::Known(g) => Some(g),
            Self::Unknown { .. } => None,
        }
    }
}

/// Commits and their parents, held compactly for [`CommitGraph::solve`]:
/// 28 bytes per commit and 20 per parent, and 13 more per commit while it
/// solves. The `index_commits` job's backfill holds up to millions of
/// commits in one, so it stores no per-commit allocation.
#[derive(Debug, Default)]
pub struct CommitGraph {
    nodes: Vec<Node>,
    parents: Vec<ObjectId>,
    sorted: bool,
}

#[derive(Debug, Clone, Copy)]
struct Node {
    id: ObjectId,
    first: u32,
    count: u32,
}

impl CommitGraph {
    /// An empty graph with room for `commits` commits.
    pub fn with_capacity(commits: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(commits),
            parents: Vec::with_capacity(commits + commits / 8),
            sorted: true,
        }
    }

    /// Adds a commit. A commit added twice keeps its first parent list.
    ///
    /// # Panics
    ///
    /// When the graph would hold 2^32 parents or more.
    pub fn push(&mut self, id: ObjectId, parents: &[ObjectId]) {
        let first = u32::try_from(self.parents.len()).expect("fewer than 2^32 parents held");
        let count = u32::try_from(parents.len()).expect("fewer than 2^32 parents of a commit");
        if self.nodes.last().is_some_and(|n| n.id >= id) {
            self.sorted = false;
        }
        self.parents.extend_from_slice(parents);
        self.nodes.push(Node { id, first, count });
    }

    /// Commits held.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether it holds no commit.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    fn sort(&mut self) {
        if !self.sorted {
            // Stable, so of several entries for one commit the first stays.
            self.nodes.sort_by_key(|n| n.id);
            self.nodes.dedup_by_key(|n| n.id);
            self.sorted = true;
        }
    }

    fn index(&self, id: &ObjectId) -> Option<usize> {
        self.nodes.binary_search_by_key(id, |n| n.id).ok()
    }

    fn parents_of(&self, i: usize) -> &[ObjectId] {
        let n = self.nodes[i];
        &self.parents[n.first as usize..(n.first + n.count) as usize]
    }

    /// The parents of held commits that are not held themselves, each once:
    /// the commits whose generations [`CommitGraph::solve`] asks `known`
    /// for.
    pub fn outside_parents(&mut self) -> Vec<ObjectId> {
        self.sort();
        let mut out: Vec<ObjectId> = self
            .parents
            .iter()
            .filter(|p| self.index(p).is_none())
            .copied()
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Every held commit's generation, or the commit it waits on.
    ///
    /// A parent outside the graph takes its generation from `known`; a
    /// commit is [`Level::Unknown`] when any parent's generation is
    /// unknown, and then waits on the first such parent's `waits_on` (or on
    /// that parent itself, when it is outside the graph). Nothing else is
    /// looked up: the caller decides how far history is searched.
    ///
    /// Iterative, so a long linear history cannot overflow the stack. A
    /// cycle, which a hash-addressed history cannot contain, leaves its
    /// commits unknown rather than looping.
    pub fn solve(mut self, known: impl Fn(&ObjectId) -> Option<u64>) -> Solved {
        self.sort();
        const NEW: u8 = 0;
        const OPEN: u8 = 1;
        const DONE: u8 = 2;
        let n = self.nodes.len();
        let mut state = vec![NEW; n];
        // Generation of each commit; 0 while unknown (real ones start at 1).
        let mut gens = vec![0u64; n];
        // For an unknown commit, the index in `waits_on` of what it waits on.
        let mut waits = vec![0u32; n];
        let mut waits_on: Vec<ObjectId> = Vec::new();
        let mut stack: Vec<(usize, usize)> = Vec::new();
        for start in 0..n {
            if state[start] != NEW {
                continue;
            }
            state[start] = OPEN;
            stack.push((start, 0));
            while let Some(&(c, mut next)) = stack.last() {
                // Descend into the first held parent not entered yet.
                let ps = self.parents_of(c);
                let mut child = None;
                while next < ps.len() {
                    let p = ps[next];
                    next += 1;
                    if let Some(pi) = self.index(&p)
                        && state[pi] == NEW
                    {
                        child = Some(pi);
                        break;
                    }
                }
                if let Some(top) = stack.last_mut() {
                    top.1 = next;
                }
                if let Some(pi) = child {
                    state[pi] = OPEN;
                    stack.push((pi, 0));
                    continue;
                }
                // Every held parent is done (or open: a cycle).
                let mut max = 0u64;
                let mut blocked: Option<u32> = None;
                for p in self.parents_of(c) {
                    let g = match self.index(p) {
                        Some(pi) if state[pi] == DONE && gens[pi] > 0 => Some(gens[pi]),
                        Some(pi) if state[pi] == DONE => {
                            blocked.get_or_insert(waits[pi]);
                            None
                        }
                        // On the stack: a cycle through `p`.
                        Some(_) => None,
                        None => known(p),
                    };
                    match g {
                        Some(g) => max = max.max(g),
                        None => {
                            if blocked.is_none() {
                                blocked = Some(push_waits_on(&mut waits_on, *p));
                            }
                        }
                    }
                }
                match (blocked, max.checked_add(1)) {
                    (None, Some(g)) => gens[c] = g,
                    (Some(w), _) => waits[c] = w,
                    // A generation past `u64::MAX`: it waits on itself,
                    // forever.
                    (None, None) => waits[c] = push_waits_on(&mut waits_on, self.nodes[c].id),
                }
                state[c] = DONE;
                stack.pop();
            }
        }
        Solved {
            graph: self,
            gens,
            waits,
            waits_on,
        }
    }
}

fn push_waits_on(waits_on: &mut Vec<ObjectId>, id: ObjectId) -> u32 {
    waits_on.push(id);
    u32::try_from(waits_on.len() - 1).expect("fewer than 2^32 commits waited on")
}

/// The result of [`CommitGraph::solve`].
#[derive(Debug)]
pub struct Solved {
    graph: CommitGraph,
    gens: Vec<u64>,
    waits: Vec<u32>,
    waits_on: Vec<ObjectId>,
}

impl Solved {
    fn level(&self, i: usize) -> Level {
        match self.gens[i] {
            0 => Level::Unknown {
                waits_on: self.waits_on[self.waits[i] as usize],
            },
            g => Level::Known(g),
        }
    }

    /// Each held commit, in ID order, with its level.
    pub fn iter(&self) -> impl Iterator<Item = (ObjectId, Level)> + '_ {
        (0..self.gens.len()).map(|i| (self.graph.nodes[i].id, self.level(i)))
    }

    /// One commit's level, if it is held.
    pub fn get(&self, id: &ObjectId) -> Option<Level> {
        self.graph.index(id).map(|i| self.level(i))
    }
}

/// The generation numbers of `commits` (each an ID and its parents).
///
/// A parent outside `commits` takes its generation from `known`; a commit
/// gets `None` when any parent's generation is unknown (outside `commits`
/// and `None` from `known`, or itself `None`). Nothing else is looked up:
/// the caller decides how far history is searched. [`CommitGraph::solve`]
/// does the work, and also says what each unknown commit waits on.
pub fn generations<'a, I, F>(commits: I, known: F) -> HashMap<ObjectId, Option<u64>>
where
    I: IntoIterator<Item = (ObjectId, &'a [ObjectId])>,
    F: Fn(&ObjectId) -> Option<u64>,
{
    let commits = commits.into_iter();
    let mut graph = CommitGraph::with_capacity(commits.size_hint().0);
    for (id, parents) in commits {
        graph.push(id, parents);
    }
    graph
        .solve(known)
        .iter()
        .map(|(id, l)| (id, l.known()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(n: u32) -> ObjectId {
        let mut b = [0u8; 20];
        b[..4].copy_from_slice(&n.to_be_bytes());
        b[19] = 7;
        ObjectId::from_bytes(b)
    }

    #[test]
    fn a_long_linear_history_counts_up_without_recursion() {
        // Deep enough to overflow a recursive walk's stack.
        let n = 200_000u32;
        let parents: Vec<Vec<ObjectId>> = (1..=n)
            .map(|c| if c == 1 { vec![] } else { vec![oid(c - 1)] })
            .collect();
        let g = generations(
            (1..=n).map(|c| (oid(c), parents[c as usize - 1].as_slice())),
            |_| None,
        );
        assert_eq!(g.len(), n as usize);
        assert_eq!(g[&oid(1)], Some(1));
        assert_eq!(g[&oid(n)], Some(u64::from(n)));
    }

    #[test]
    fn a_merge_is_one_more_than_its_highest_parent() {
        // 1 <- 2 <- 3, 1 <- 4, merge 5 of 3 and 4; 6 on top of a known
        // outside commit 100 at generation 41.
        let p2 = [oid(1)];
        let p3 = [oid(2)];
        let p4 = [oid(1)];
        let p5 = [oid(4), oid(3)];
        let p6 = [oid(100)];
        let g = generations(
            [
                (oid(1), &[][..]),
                (oid(2), &p2[..]),
                (oid(3), &p3[..]),
                (oid(4), &p4[..]),
                (oid(5), &p5[..]),
                (oid(6), &p6[..]),
            ],
            |id| (*id == oid(100)).then_some(41),
        );
        assert_eq!(g[&oid(4)], Some(2));
        assert_eq!(g[&oid(5)], Some(4));
        assert_eq!(g[&oid(6)], Some(42));
    }

    #[test]
    fn an_unknown_parent_leaves_its_descendants_unknown() {
        // 2's parent 9 is outside and unknown; 3 is above 2; 4 is a root.
        let p2 = [oid(9)];
        let p3 = [oid(2)];
        let p5 = [oid(4), oid(3)];
        let g = generations(
            [
                (oid(2), &p2[..]),
                (oid(3), &p3[..]),
                (oid(4), &[][..]),
                (oid(5), &p5[..]),
            ],
            |_| None,
        );
        assert_eq!(g[&oid(2)], None);
        assert_eq!(g[&oid(3)], None);
        assert_eq!(g[&oid(4)], Some(1));
        assert_eq!(g[&oid(5)], None);
    }

    #[test]
    fn an_octopus_merge_is_one_more_than_its_highest_of_up_to_five_parents() {
        // Roots 1 and 2; chain 2 <- 3 <- 4; 5 merges 1, 4 and 3 (three
        // parents); 6 merges 5, 1, 2, 3 and outside 100 at 7 (five); 7
        // merges four with the highest last.
        let p3 = [oid(2)];
        let p4 = [oid(3)];
        let p5 = [oid(1), oid(4), oid(3)];
        let p6 = [oid(5), oid(1), oid(2), oid(3), oid(100)];
        let p7 = [oid(1), oid(2), oid(3), oid(6)];
        let g = generations(
            [
                (oid(1), &[][..]),
                (oid(2), &[][..]),
                (oid(3), &p3[..]),
                (oid(4), &p4[..]),
                (oid(5), &p5[..]),
                (oid(6), &p6[..]),
                (oid(7), &p7[..]),
            ],
            |id| (*id == oid(100)).then_some(7),
        );
        assert_eq!(g[&oid(4)], Some(3));
        assert_eq!(g[&oid(5)], Some(4));
        assert_eq!(g[&oid(6)], Some(8), "the outside parent is the highest");
        assert_eq!(g[&oid(7)], Some(9));
    }

    #[test]
    fn an_unknown_commit_waits_on_the_outside_commit_that_blocks_it() {
        // 2 <- 3 <- 4 above outside 9 (unknown); 5 merges known root 1, 4
        // and outside 8 (unknown): it waits on 9 through 4, its first
        // unknown parent, not on 8. 6 has five parents, all known but 8.
        let p2 = [oid(9)];
        let p3 = [oid(2)];
        let p4 = [oid(3)];
        let p5 = [oid(1), oid(4), oid(8)];
        let p6 = [oid(1), oid(1), oid(100), oid(8), oid(1)];
        let mut graph = CommitGraph::with_capacity(6);
        for (id, ps) in [
            (oid(6), &p6[..]),
            (oid(5), &p5[..]),
            (oid(4), &p4[..]),
            (oid(3), &p3[..]),
            (oid(2), &p2[..]),
            (oid(1), &[][..]),
        ] {
            graph.push(id, ps);
        }
        assert_eq!(graph.outside_parents(), vec![oid(8), oid(9), oid(100)]);
        let solved = graph.solve(|id| (*id == oid(100)).then_some(3));
        let waits = |w| Some(Level::Unknown { waits_on: oid(w) });
        assert_eq!(solved.get(&oid(1)), Some(Level::Known(1)));
        for c in [2, 3, 4, 5] {
            assert_eq!(solved.get(&oid(c)), waits(9), "{c}");
        }
        assert_eq!(solved.get(&oid(6)), waits(8));
        assert_eq!(solved.get(&oid(7)), None);
        let ids: Vec<ObjectId> = solved.iter().map(|(id, _)| id).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "ID order");
        assert_eq!(ids.len(), 6);
    }

    #[test]
    fn a_cycle_leaves_its_commits_unknown() {
        let p1 = [oid(2)];
        let p2 = [oid(1)];
        let p3 = [oid(2)];
        let g = generations(
            [(oid(1), &p1[..]), (oid(2), &p2[..]), (oid(3), &p3[..])],
            |_| None,
        );
        assert!(g.values().all(Option::is_none));
    }
}

//! The dependency walk: which versions' packs a fetch must send.
//!
//! # The have-side skip through cover chains
//!
//! A cover that carries a [`CoverChain`] spans a **dependency chain**: every
//! version in its range after the first depends on its parent (the same
//! ref's version at `seq - 1`). A version's closure contains the closures
//! of its dependencies, so in a chain each version's closure contains the
//! closure of every earlier version in the range. A client that holds the
//! version at `seq = s` inside such a range therefore holds every version
//! of that ref from the range's first up to `s`.
//!
//! The walk uses that to skip the have side. When it expands a held version
//! `X` and a chained cover of `X`'s ref contains `X.seq` above its first
//! seq, it registers the **held range** `[first, X.seq]` of that ref
//! (smallest `first` over such covers), paints the chain's first version
//! held instead of `X`'s parent, and paints `X`'s other dependencies as
//! usual. The versions strictly between are never expanded unless a want
//! reaches them; when one is, it lies in a registered held range and is
//! held. A held version strictly inside a registered range does not paint
//! its parent either: the range's first version was painted when the range
//! was registered.
//!
//! Why it is sound: a held range is registered only from a held version and
//! a chain, and the chain alone proves closure containment. It needs
//! neither exactness nor ancestry, only dependency edges, which are
//! immutable once committed. An unchained cover never affects the walk.
//!
//! Why the order works: a held range `[f, s]` is registered when the
//! version at `s` is expanded. Every version in `[f, s)` has a smaller ID
//! (a ref's versions are written in `seq` order), so it is expanded later,
//! when the range is already known. If a graph broke that order, a version
//! in the range could be expanded first and be sent although it is held:
//! bytes, never objects.
//!
//! What it costs: the walk no longer proves every have-side version held,
//! so it can send a version the client holds. That happens only for a
//! version a want reaches whose sole route to the haves runs through a
//! dependency of a skipped version other than its parent (for example a
//! scoped branch version's trunk dependency, when trunk was forced since).
//! The plan still holds every object the client lacks.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashMap};

use aspen_core::fetch::{HaveVersion, PlanError, VersionGraph};
use aspen_core::id::{RefVersionId, Seq};
use aspen_core::meta::{Cover, CoverChain};
use aspen_core::name::RefName;
use aspen_core::refs::RefVersion;

/// Reachable from a want.
const WANT: u8 = 1;
/// Reachable from a have: the client holds this version's closure.
const HAVE: u8 = 2;

#[derive(Debug, Clone, Copy)]
struct Paint {
    flags: u8,
    expanded: bool,
}

/// The versions the walk found reachable from a want and not held: exactly
/// the versions whose packs a fetch sends.
///
/// A version is held when a have reaches it through dependencies, or when
/// it lies in a held range the walk registered from a cover chain (module
/// docs). Every version reachable from a want and not from any have is
/// here; a version a have reaches is here only in the case the module docs
/// describe under "What it costs".
///
/// [`Needed::walk`] is the only way to build one, and nothing removes a
/// version from it. Cover selection ([`crate::covers`]) reads it and may
/// only replace the packs of a run of needed versions by one cover pack; it
/// cannot conclude that the client holds a version. Only the walk decides
/// what is held, and it trusts a cover for that only through its
/// [`CoverChain`].
#[derive(Debug)]
pub struct Needed<'g> {
    versions: BTreeMap<RefVersionId, &'g RefVersion>,
    visited: usize,
    chain_skips: usize,
}

impl<'g> Needed<'g> {
    /// Walks the dependency graph from `wants` and `haves` together.
    ///
    /// Versions are expanded from the highest ID down. A version's paint
    /// comes only from versions that depend on it, which have higher IDs and
    /// so were expanded first; by the time a version is expanded its paint
    /// is final. A version painted only by wants, and outside every
    /// registered held range of its ref, is needed; any other expanded
    /// version is held, and so is everything it depends on. The walk stops
    /// when every version still queued is held: whatever is reachable
    /// through them is held too.
    ///
    /// If the graph broke the ID order (a dependency with an ID at least its
    /// dependent's), a version could be expanded before all its paint
    /// arrived. A late `HAVE` then only leaves a held version in the needed
    /// set, which costs bytes, never objects; a late `WANT` reaches a
    /// version already expanded with a paint that includes `WANT` or `HAVE`,
    /// so nothing is lost either.
    pub fn walk(
        graph: &'g dyn VersionGraph,
        wants: &[RefVersionId],
        haves: &[HaveVersion],
    ) -> Result<Self, PlanError> {
        let mut walk = Walk::default();
        for &want in wants {
            walk.paint(want, WANT);
        }
        for have in haves {
            walk.paint(have.version(), HAVE);
        }
        let mut held: HashMap<&'g RefName, HeldRanges> = HashMap::new();
        let mut versions = BTreeMap::new();
        let mut visited = 0;
        let mut chain_skips = 0;
        while walk.want_only_queued > 0 {
            let Some(id) = walk.queue.pop() else { break };
            let paint = walk
                .paint
                .get_mut(&id)
                .expect("queued versions are painted");
            if paint.expanded {
                continue;
            }
            paint.expanded = true;
            let mut flags = paint.flags;
            if flags & HAVE == 0 {
                walk.want_only_queued -= 1;
            }
            let version = graph.version(id).ok_or(PlanError::MissingVersion(id))?;
            visited += 1;

            // The first seq of the held range of this ref that contains the
            // version, if any.
            let mut held_from = held
                .get(&version.name)
                .and_then(|r| r.containing(version.seq));
            if held_from.is_some() {
                flags |= HAVE;
            }
            if flags == WANT {
                versions.insert(id, version);
                for dep in version.deps.iter() {
                    walk.paint(dep, flags);
                }
                continue;
            }

            if let Some((first, chain)) = chained_cover_below(graph.covers(&version.name), version)
                && held_from.is_none_or(|f| first < f)
            {
                held.entry(&version.name)
                    .or_default()
                    .insert(first, version.seq);
                walk.paint(chain.first_version(), flags);
                held_from = Some(first);
                chain_skips += 1;
            }
            // Inside a held range above its first seq, the parent is held
            // through the range, and the range's first version carries the
            // paint below it.
            let parent_held = held_from.is_some_and(|f| f < version.seq);
            for dep in version.deps.iter() {
                if parent_held && Some(dep) == version.parent {
                    continue;
                }
                walk.paint(dep, flags);
            }
        }
        Ok(Self {
            versions,
            visited,
            chain_skips,
        })
    }

    /// The needed versions, by ID.
    pub fn versions(&self) -> impl ExactSizeIterator<Item = &'g RefVersion> + '_ {
        self.versions.values().copied()
    }

    /// Whether `id` is needed.
    pub fn contains(&self, id: RefVersionId) -> bool {
        self.versions.contains_key(&id)
    }

    /// How many versions are needed.
    pub fn len(&self) -> usize {
        self.versions.len()
    }

    /// Whether no version is needed (the haves hold every want).
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// How many versions the walk expanded, needed or held.
    pub fn visited(&self) -> usize {
        self.visited
    }

    /// How many times the walk registered a held range from a cover chain.
    pub fn chain_skips(&self) -> usize {
        self.chain_skips
    }
}

/// The chained cover of `version`'s ref whose range contains `version.seq`
/// above its first seq, with the smallest first seq: `(first seq, chain)`.
///
/// [`VersionGraph::covers`] orders covers by level, then first seq. Within
/// a level the store's covers are aligned and disjoint
/// ([`aspen_core::meta::CoverLayout`]), so at most one contains a seq: the
/// last one starting below it, which a binary search finds. If a level
/// holds overlapping covers, or the order is broken, this may miss a cover;
/// that costs visits, never objects, since every cover returned is checked
/// to contain the seq and to carry a chain.
fn chained_cover_below(covers: &[Cover], version: &RefVersion) -> Option<(Seq, CoverChain)> {
    let seq = version.seq;
    let mut best: Option<(Seq, CoverChain)> = None;
    let mut rest = covers;
    while let Some(head) = rest.first() {
        let level = head.level;
        // At least one, so the loop advances even if the order is broken.
        let end = rest.partition_point(|c| c.level <= level).max(1);
        let (group, tail) = rest.split_at(end);
        rest = tail;
        let below = group.partition_point(|c| c.range.first() < seq);
        let Some(cover) = below.checked_sub(1).map(|i| &group[i]) else {
            continue;
        };
        let Some(chain) = cover.chain else { continue };
        let (first, last) = (cover.range.first(), cover.range.last());
        if cover.name == version.name
            && first < seq
            && seq <= last
            && best.is_none_or(|(b, _)| first < b)
        {
            best = Some((first, chain));
        }
    }
    best
}

/// Held ranges of one ref: disjoint, non-adjacent `seq` intervals, keyed by
/// first seq.
#[derive(Debug, Default)]
struct HeldRanges(BTreeMap<Seq, Seq>);

impl HeldRanges {
    /// The first seq of the range containing `seq`.
    fn containing(&self, seq: Seq) -> Option<Seq> {
        self.0
            .range(..=seq)
            .next_back()
            .and_then(|(&first, &last)| (last >= seq).then_some(first))
    }

    /// Adds `first..=last`, merging it with the ranges it overlaps or
    /// touches.
    fn insert(&mut self, first: Seq, last: Seq) {
        let (mut lo, mut hi) = (first, last);
        let touching: Vec<Seq> = self
            .0
            .range(..=hi.next().unwrap_or(hi))
            .rev()
            .take_while(|&(_, &end)| end.next().unwrap_or(end) >= lo)
            .map(|(&start, _)| start)
            .collect();
        for start in touching {
            let end = self.0.remove(&start).expect("listed above");
            lo = lo.min(start);
            hi = hi.max(end);
        }
        self.0.insert(lo, hi);
    }
}

#[derive(Default)]
struct Walk {
    paint: HashMap<RefVersionId, Paint>,
    queue: BinaryHeap<RefVersionId>,
    /// Queued, unexpanded versions painted by wants only. The walk ends when
    /// this reaches zero.
    want_only_queued: usize,
}

impl Walk {
    fn paint(&mut self, id: RefVersionId, flags: u8) {
        match self.paint.entry(id) {
            Entry::Vacant(e) => {
                e.insert(Paint {
                    flags,
                    expanded: false,
                });
                self.queue.push(id);
                if flags & HAVE == 0 {
                    self.want_only_queued += 1;
                }
            }
            Entry::Occupied(mut e) => {
                let paint = e.get_mut();
                if paint.expanded {
                    return;
                }
                if paint.flags & HAVE == 0 && flags & HAVE != 0 {
                    self.want_only_queued -= 1;
                }
                paint.flags |= flags;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(n: u64) -> Seq {
        Seq::new(n).unwrap()
    }

    #[test]
    fn held_ranges_merge_overlapping_and_touching_ranges() {
        let mut r = HeldRanges::default();
        r.insert(s(10), s(20));
        r.insert(s(30), s(40));
        assert_eq!(r.containing(s(9)), None);
        assert_eq!(r.containing(s(10)), Some(s(10)));
        assert_eq!(r.containing(s(25)), None);
        assert_eq!(r.containing(s(40)), Some(s(30)));
        // Touching on the left.
        r.insert(s(21), s(22));
        assert_eq!(r.containing(s(22)), Some(s(10)));
        // Spanning two ranges.
        r.insert(s(5), s(35));
        assert_eq!(r.0.len(), 1);
        assert_eq!(r.containing(s(40)), Some(s(5)));
        assert_eq!(r.containing(s(41)), None);
        // Inside: no change.
        r.insert(s(6), s(7));
        assert_eq!(r.0.iter().collect::<Vec<_>>(), vec![(&s(5), &s(40))]);
    }
}

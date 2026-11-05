//! The packs of a ref version's closure: what a push's base lookup and
//! connectivity check search (push path step 2).
//!
//! They hold the objects of the plan `aspen-planner` makes for a full fetch
//! of the roots with no haves (`aspen_core::fetch::Planner`: "the same call
//! with no haves gives the search set of a push's base lookup"): every
//! version the roots reach through dependencies is needed, and a compacted
//! cover stands for its range when every version in the range is needed.
//! They are that plan's packs, except in the two cases below where they name
//! the same objects in more packs. They are computed without walking the
//! roots' whole history (issue #149), in two steps whose cost does not grow
//! with the history:
//!
//! 1. **Which versions.** A version's closure is its own pack plus its
//!    dependencies' closures, and versions never change, so the set of
//!    versions a root reaches is remembered per root as a [`Summary`]:
//!    for each ref, the `seq` ranges of the versions in the closure. A
//!    push's root is the current trunk version, whose dependency is the
//!    previous push's root; its summary is that summary plus one `seq`, so
//!    the walk reads one version. Only a root whose dependencies' closures
//!    are not remembered walks further, down to the nearest remembered one:
//!    the first push after a start, after another instance pushed many
//!    times, or after the cache dropped the repository's closures (below).
//!    Ordinary trunk history is one range per ref; each forced trunk update
//!    adds at most one.
//! 2. **Which packs.** Covers are aligned (`CoverLayout`: level k spans
//!    `fanout^k` versions), so each range splits into the largest aligned
//!    blocks inside it, at most `fanout - 1` per level at each end plus the
//!    top-level blocks. A block whose cover is compacted sends its cover's
//!    pack; any other block splits into the blocks one level below, and a
//!    level-1 block into its versions, which send their own packs. A
//!    compacted cover never changes (its pack is set once, and the sweep
//!    never deletes a pack a cover names), so it is remembered. The first
//!    time a block's cover is found compacted, its pack's row is read: a
//!    cover naming a pack with no row in the repository is corruption, as
//!    it is to the planner. A block found without a compacted cover is
//!    asked about again only after a wait that doubles (1 s to 60 s), so a
//!    push soon after compaction catches up uses the new cover and a
//!    repository that is never compacted is not asked about every block on
//!    every push. Until then the block's versions' own packs stand for it.
//!
//! **Where the packs differ from the planner's.** A block whose cover was
//! compacted during the block's wait, and a cover written under another
//! fanout (the planner uses any cover; the layout never places this one,
//! so it is never asked about): in both, the block's versions' own packs
//! are searched instead of the cover's pack. They hold the same objects
//! (C1's compaction contract) and are never deleted: ref versions are never
//! deleted and each names its pack (`ref_versions_pack_fk`), and the sweep
//! deletes only packs that no version, cover, job or shallow pack recording
//! names (`unreferenced` in `crates/aspen-meta/src/gc.rs`, pinned by
//! `the_sweep_never_deletes_a_referenced_pack` in aspen-meta's store
//! suite). So the difference costs packs, never objects.
//!
//! **Cost.** With compaction keeping up, a push reads the version it builds
//! on and the covers (with their pack rows) of the blocks that became
//! complete since it last looked, and nothing else; its search set holds at
//! most about `(fanout - 1) × levels × 2` packs plus one per top-level
//! block. Without compaction the search set is one pack per version, as
//! before, and a push still reads one version row, but it asks the store
//! about each block of the closure whose wait ran out: each block at most
//! once a minute once its wait has grown, so about `N / (fanout - 1)` keys
//! a minute for `N` versions, in keyed queries that return no rows. Blocks
//! whose covers were written under another fanout stay un-coverable under
//! the current one and are asked about once a minute the same way, until
//! covers of the current layout are written over them.
//!
//! **Memory.** Each cache (versions, version packs, closures, cover blocks)
//! holds about [`CACHE_LIMIT`] entries in two generations ([`Recent`]): an
//! entry used since the older generation was dropped survives the next
//! drop, so an active repository keeps its newest closure and blocks
//! however many other repositories push. A repository not pushed to while a
//! cache turned over twice walks its unseen history again on its next
//! push, as after a start. Without compaction, a closure of more than about
//! `CACHE_LIMIT / 2` versions does not fit, and its versions' packs are
//! read again on each push.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aspen_core::error::StoreError;
use aspen_core::id::{PackId, RefVersionId, RepoId, Seq};
use aspen_core::ingest::SearchPacks;
use aspen_core::meta::{CoverLayout, MetaStore};
use aspen_core::name::RefName;
use aspen_core::pack::{CoverLevel, MAX_LEVEL};
use aspen_core::refs::RefVersion;

/// Entries each cache holds, about: versions, version packs, cover blocks,
/// and closure ranges (summed over closures).
const CACHE_LIMIT: usize = 200_000;

/// The first wait before a block found without a compacted cover is asked
/// about again.
const RECHECK_FIRST: Duration = Duration::from_secs(1);

/// The longest such wait.
const RECHECK_MAX: Duration = Duration::from_secs(60);

/// A cache of about `limit` weight in two generations. A hit in the older
/// generation moves the entry to the newer one; when the newer one is full,
/// the older one is dropped and the newer one becomes the older. An entry
/// used since the last drop survives the next one, which approximates
/// least-recently-used eviction without ordering every hit.
#[derive(Debug)]
struct Recent<K, V> {
    hot: HashMap<K, V>,
    cold: HashMap<K, V>,
    hot_weight: usize,
    half: usize,
    weigh: fn(&V) -> usize,
}

impl<K: Eq + Hash, V: Clone> Recent<K, V> {
    fn new(limit: usize, weigh: fn(&V) -> usize) -> Self {
        Self {
            hot: HashMap::new(),
            cold: HashMap::new(),
            hot_weight: 0,
            half: (limit / 2).max(1),
            weigh,
        }
    }

    fn get(&mut self, k: &K) -> Option<V> {
        if let Some(v) = self.hot.get(k) {
            return Some(v.clone());
        }
        let (k, v) = self.cold.remove_entry(k)?;
        self.insert(k, v.clone());
        Some(v)
    }

    fn insert(&mut self, k: K, v: V) {
        let w = (self.weigh)(&v);
        if self.hot_weight + w > self.half && !self.hot.is_empty() {
            self.cold = std::mem::take(&mut self.hot);
            self.hot_weight = 0;
        }
        self.cold.remove(&k);
        if let Some(old) = self.hot.insert(k, v) {
            self.hot_weight -= (self.weigh)(&old);
        }
        self.hot_weight += w;
    }
}

/// Where [`Closures`] reads the time: `Instant::now`, or a test's clock.
#[derive(Clone)]
struct Clock(Arc<dyn Fn() -> Instant + Send + Sync>);

impl Clock {
    fn now(&self) -> Instant {
        (self.0)()
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self(Arc::new(Instant::now))
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Clock")
    }
}

/// The waits, cache size and clock of [`Closures`]; tests shrink them.
#[derive(Debug, Clone)]
struct Tuning {
    recheck_first: Duration,
    recheck_max: Duration,
    cache_limit: usize,
    clock: Clock,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            recheck_first: RECHECK_FIRST,
            recheck_max: RECHECK_MAX,
            cache_limit: CACHE_LIMIT,
            clock: Clock::default(),
        }
    }
}

/// The versions a closure holds: for each ref, disjoint `seq` ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Summary {
    repo: RepoId,
    refs: BTreeMap<RefName, Seqs>,
}

impl Summary {
    fn new(repo: RepoId) -> Self {
        Self {
            repo,
            refs: BTreeMap::new(),
        }
    }

    fn insert(&mut self, name: &RefName, seq: Seq) {
        match self.refs.get_mut(name) {
            Some(s) => s.insert(seq.get(), seq.get()),
            None => {
                let mut s = Seqs::default();
                s.insert(seq.get(), seq.get());
                self.refs.insert(name.clone(), s);
            }
        }
    }

    fn union(&mut self, other: &Summary) {
        for (name, seqs) in &other.refs {
            let mine = self.refs.entry(name.clone()).or_default();
            for (&first, &last) in &seqs.0 {
                mine.insert(first, last);
            }
        }
    }

    /// Ranges held, the cache's measure of size.
    fn weight(&self) -> usize {
        self.refs.values().map(|s| s.0.len()).sum()
    }
}

/// Disjoint, non-adjacent `first ..= last` ranges, keyed by `first`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Seqs(BTreeMap<u64, u64>);

impl Seqs {
    fn insert(&mut self, first: u64, last: u64) {
        let (mut lo, mut hi) = (first, last);
        if let Some((&s, &e)) = self.0.range(..=first).next_back()
            && e.saturating_add(1) >= first
        {
            lo = s;
            hi = hi.max(e);
        }
        while let Some((&s, &e)) = self.0.range(lo + 1..=hi.saturating_add(1)).next() {
            self.0.remove(&s);
            hi = hi.max(e);
        }
        self.0.insert(lo, hi);
    }
}

/// One aligned block of one ref's versions: the place of a cover.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BlockKey {
    repo: RepoId,
    name: RefName,
    level: u8,
    first: u64,
}

#[derive(Debug, Clone, Copy)]
enum Block {
    /// The block's cover has this pack, for good.
    Compacted(PackId),
    /// No compacted cover when last asked.
    Absent { asked: Instant, wait: Duration },
}

#[derive(Debug)]
struct State {
    versions: Recent<RefVersionId, RefVersion>,
    /// Each version's pack, by repository, ref and `seq`.
    packs: Recent<(RepoId, RefName, u64), Option<PackId>>,
    summaries: Recent<RefVersionId, Arc<Summary>>,
    blocks: Recent<BlockKey, Block>,
}

impl State {
    fn new(limit: usize) -> Self {
        Self {
            versions: Recent::new(limit, |_| 1),
            packs: Recent::new(limit, |_| 1),
            summaries: Recent::new(limit, |s| s.weight().max(1)),
            blocks: Recent::new(limit, |_| 1),
        }
    }

    fn put_version(&mut self, v: &RefVersion) {
        self.versions.insert(v.id, v.clone());
        self.put_pack_of(v);
    }

    fn put_pack_of(&mut self, v: &RefVersion) {
        self.packs
            .insert((v.repo, v.name.clone(), v.seq.get()), v.value.pack());
    }
}

/// What receive-pack remembers between pushes to compute search sets:
/// ref versions, the closures of earlier roots, and compacted covers. All
/// of it is immutable once committed, except a block's absence, which is
/// asked about again (module docs).
#[derive(Debug)]
pub(crate) struct Closures {
    layout: CoverLayout,
    tuning: Tuning,
    state: Mutex<State>,
}

impl Closures {
    /// Closures over covers placed by `layout`, the layout the compactor
    /// uses (`ReceiveConfig::cover_layout`).
    pub(crate) fn new(layout: CoverLayout) -> Self {
        Self::tuned(layout, Tuning::default())
    }

    fn tuned(layout: CoverLayout, tuning: Tuning) -> Self {
        Self {
            layout,
            state: Mutex::new(State::new(tuning.cache_limit)),
            tuning,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("not poisoned")
    }

    async fn version<M: MetaStore>(
        &self,
        meta: &M,
        id: RefVersionId,
    ) -> Result<RefVersion, StoreError> {
        if let Some(v) = self.lock().versions.get(&id) {
            return Ok(v);
        }
        let v = meta
            .ref_version(id)
            .await?
            .ok_or_else(|| StoreError::Corrupt {
                what: format!("ref version {id} is a dependency but has no row"),
            })?;
        self.lock().put_version(&v);
        Ok(v)
    }

    fn summary_of(
        &self,
        repo: RepoId,
        id: RefVersionId,
    ) -> Result<Option<Arc<Summary>>, StoreError> {
        match self.lock().summaries.get(&id) {
            Some(s) if s.repo != repo => Err(other_repo(id)),
            found => Ok(found),
        }
    }

    /// The versions `root` reaches, itself included.
    async fn summary<M: MetaStore>(
        &self,
        meta: &M,
        repo: RepoId,
        root: RefVersionId,
    ) -> Result<Arc<Summary>, StoreError> {
        if let Some(s) = self.summary_of(repo, root)? {
            return Ok(s);
        }
        let mut acc = Summary::new(repo);
        let mut queue = vec![root];
        let mut seen = HashSet::new();
        while let Some(id) = queue.pop() {
            if !seen.insert(id) {
                continue;
            }
            if id != root
                && let Some(s) = self.summary_of(repo, id)?
            {
                acc.union(&s);
                continue;
            }
            let v = self.version(meta, id).await?;
            if v.repo != repo {
                return Err(other_repo(id));
            }
            acc.insert(&v.name, v.seq);
            queue.extend(v.deps.iter());
        }
        let acc = Arc::new(acc);
        self.lock().summaries.insert(root, Arc::clone(&acc));
        Ok(acc)
    }

    /// The packs for the versions of `summary`: compacted covers for the
    /// aligned blocks that have one, the versions' own packs elsewhere.
    async fn select<M: MetaStore>(
        &self,
        meta: &M,
        summary: &Summary,
    ) -> Result<BTreeSet<PackId>, StoreError> {
        let repo = summary.repo;
        let mut packs = BTreeSet::new();
        for (name, seqs) in &summary.refs {
            // Blocks to look at, by level (index 0 unused), and versions.
            let mut levels: Vec<Vec<u64>> = vec![Vec::new(); usize::from(MAX_LEVEL) + 1];
            let mut singles: Vec<u64> = Vec::new();
            for (&first, &last) in &seqs.0 {
                self.split(first, last, &mut levels, &mut singles);
            }
            for level in (1..=MAX_LEVEL).rev() {
                let firsts = std::mem::take(&mut levels[usize::from(level)]);
                if firsts.is_empty() {
                    continue;
                }
                let open = self
                    .compacted(meta, repo, name, level, firsts, &mut packs)
                    .await?;
                let lvl = CoverLevel::new(level).expect("1 ..= MAX_LEVEL");
                for first in open {
                    if level > 1 {
                        let below = CoverLevel::new(level - 1).expect("1 ..= MAX_LEVEL");
                        let step = self.layout.span(below);
                        levels[usize::from(level - 1)]
                            .extend((0..self.layout.fanout()).map(|i| first + i * step));
                    } else {
                        singles.extend(first..first + self.layout.span(lvl));
                    }
                }
            }
            packs.extend(
                self.own_packs(meta, repo, name, singles)
                    .await?
                    .into_iter()
                    .flatten(),
            );
        }
        Ok(packs)
    }

    /// Splits `first ..= last` into the largest aligned blocks inside it
    /// (by level, into `levels`) and the versions no level-1 block covers.
    fn split(&self, first: u64, last: u64, levels: &mut [Vec<u64>], singles: &mut Vec<u64>) {
        let mut s = first;
        while s <= last {
            let block = (1..=MAX_LEVEL).rev().find_map(|level| {
                let span = self
                    .layout
                    .span(CoverLevel::new(level).expect("1 ..= MAX_LEVEL"));
                ((s - 1).is_multiple_of(span) && last - s >= span - 1).then_some((level, span))
            });
            match block {
                Some((level, span)) => {
                    levels[usize::from(level)].push(s);
                    s += span;
                }
                None => {
                    singles.push(s);
                    s += 1;
                }
            }
        }
    }

    /// Adds the packs of the blocks at `level` starting at `firsts` that
    /// have a compacted cover to `packs`; returns the other blocks.
    async fn compacted<M: MetaStore>(
        &self,
        meta: &M,
        repo: RepoId,
        name: &RefName,
        level: u8,
        firsts: Vec<u64>,
        packs: &mut BTreeSet<PackId>,
    ) -> Result<Vec<u64>, StoreError> {
        let key = |first| BlockKey {
            repo,
            name: name.clone(),
            level,
            first,
        };
        let now = self.tuning.clock.now();
        let mut open = Vec::new();
        let mut ask = Vec::new();
        {
            let mut st = self.lock();
            for first in firsts {
                match st.blocks.get(&key(first)) {
                    Some(Block::Compacted(p)) => {
                        packs.insert(p);
                    }
                    Some(Block::Absent { asked, wait }) if now < asked + wait => open.push(first),
                    _ => ask.push(first),
                }
            }
        }
        if ask.is_empty() {
            return Ok(open);
        }
        let lvl = CoverLevel::new(level).expect("1 ..= MAX_LEVEL");
        let span = self.layout.span(lvl);
        let seqs: Vec<Seq> = ask
            .iter()
            .map(|&f| Seq::new(f).expect("a version's seq"))
            .collect();
        let mut found = BTreeMap::new();
        for c in meta.covers_at(repo, name, lvl, &seqs).await? {
            let first = c.range.first().get();
            if c.repo == repo
                && &c.name == name
                && c.level == lvl
                && c.range.last().get() == first + (span - 1)
                && let Some(p) = c.pack
            {
                found.insert(first, p);
            }
        }
        self.check_rows(meta, repo, name, level, &found).await?;
        let mut st = self.lock();
        for first in ask {
            if let Some(&p) = found.get(&first) {
                st.blocks.insert(key(first), Block::Compacted(p));
                packs.insert(p);
                continue;
            }
            match st.blocks.get(&key(first)) {
                // Another search found the cover compacted while this one
                // asked: that is for good, never replaced by an absence.
                Some(Block::Compacted(p)) => {
                    packs.insert(p);
                }
                prev => {
                    let wait = match prev {
                        Some(Block::Absent { wait, .. }) => (wait * 2).min(self.tuning.recheck_max),
                        _ => self.tuning.recheck_first,
                    };
                    st.blocks
                        .insert(key(first), Block::Absent { asked: now, wait });
                    open.push(first);
                }
            }
        }
        Ok(open)
    }

    /// Checks that the packs of covers just found compacted, by first
    /// `seq`, have rows in `repo`, as the planner does for every pack it
    /// selects. Read once per cover: a compacted cover is then remembered.
    async fn check_rows<M: MetaStore>(
        &self,
        meta: &M,
        repo: RepoId,
        name: &RefName,
        level: u8,
        found: &BTreeMap<u64, PackId>,
    ) -> Result<(), StoreError> {
        if found.is_empty() {
            return Ok(());
        }
        let mut ids: Vec<PackId> = found.values().copied().collect();
        ids.sort_unstable();
        ids.dedup();
        let rows: HashSet<PackId> = meta
            .packs(&ids)
            .await?
            .into_iter()
            .filter(|r| r.pack.repo == repo)
            .map(|r| r.id)
            .collect();
        match found.iter().find(|(_, p)| !rows.contains(p)) {
            None => Ok(()),
            Some((first, p)) => Err(StoreError::Corrupt {
                what: format!(
                    "the level {level} cover of {name} from seq {first} names pack {p}, \
                     which has no row in its repository"
                ),
            }),
        }
    }

    /// The own packs of `name`'s versions at `seqs`, from the cache or the
    /// store.
    async fn own_packs<M: MetaStore>(
        &self,
        meta: &M,
        repo: RepoId,
        name: &RefName,
        seqs: Vec<u64>,
    ) -> Result<Vec<Option<PackId>>, StoreError> {
        let mut out = Vec::with_capacity(seqs.len());
        let mut missing = Vec::new();
        {
            let mut st = self.lock();
            for s in seqs {
                match st.packs.get(&(repo, name.clone(), s)) {
                    Some(p) => out.push(p),
                    None => missing.push(Seq::new(s).expect("a version's seq")),
                }
            }
        }
        // `versions_by_seq` keeps the order asked, which costs it the
        // square of the batch: keep batches short.
        for chunk in missing.chunks(256) {
            let got = meta.versions_by_seq(repo, name, chunk).await?;
            let mut st = self.lock();
            for v in &got {
                if v.repo != repo || &v.name != name {
                    return Err(other_repo(v.id));
                }
                st.put_pack_of(v);
                out.push(v.value.pack());
            }
            if got.len() != chunk.len() {
                return Err(StoreError::Corrupt {
                    what: format!("a version of {name} in a closure has no row"),
                });
            }
        }
        Ok(out)
    }
}

fn other_repo(id: RefVersionId) -> StoreError {
    StoreError::Corrupt {
        what: format!("ref version {id} belongs to another repository"),
    }
}

/// The packs holding the closures of `roots`, ascending by ID, each once:
/// the packs of a full-closure plan of `roots` (module docs). Empty for no
/// roots.
pub(crate) async fn closure_packs<M: MetaStore>(
    meta: &M,
    closures: &Closures,
    repo: RepoId,
    roots: &[RefVersionId],
) -> Result<SearchPacks, StoreError> {
    if roots.is_empty() {
        return Ok(SearchPacks::default());
    }
    let mut all = Summary::new(repo);
    for &root in roots {
        let s = closures.summary(meta, repo, root).await?;
        all.union(&s);
    }
    Ok(SearchPacks::new(closures.select(meta, &all).await?))
}

#[cfg(test)]
mod tests;

//! The push search set (issue #149): it holds the objects of the planner's
//! full-closure plan, and is that plan's packs except while a block waits
//! to be asked about again or under a cover of another fanout; the metadata
//! a push reads to compute it does not grow with the trunk's history.
//!
//! The store is an in-memory fake that counts what it is asked for (keys,
//! not rows found), so a test can bound a push's reads;
//! `SqlStore::covers_at` has its own case in aspen-meta's store suite.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aspen_core::error::StoreError;
use aspen_core::fetch::{PlanInput, PlanMode, VersionGraph};
use aspen_core::id::{
    CoverId, JobId, ObjectId, PackChecksum, PackId, RefVersionId, RepoId, Seq, UnixMicros,
};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{
    CommitLookup, CommitOutcome, CommitRow, Cover, CoverLayout, Job, JobEvent, JobTag, MetaStore,
    NewCover, NewJob, PackTombstone, PushCommit, RefListQuery, SeqRange, ShallowDepth,
    ShallowRecorded,
};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{
    CoverLevel, MAX_LEVEL, NewPack, PackGeneration, PackOrigin, PackRecord, entry_table_len,
};
use aspen_core::refs::{Closure, Deps, RefValue, RefVersion};
use aspen_core::repo::{Repo, RepoSettings};
use aspen_planner::CoverPlanner;
use aspen_testkit::rng::Rng;

use super::{Clock, Closures, Tuning, closure_packs};

const MAIN: &str = "refs/heads/main";

fn repo() -> RepoId {
    RepoId::new(1).unwrap()
}

fn rn(s: &str) -> RefName {
    RefName::new(s).unwrap()
}

#[derive(Debug, Default)]
struct History {
    versions: BTreeMap<RefVersionId, RefVersion>,
    current: HashMap<RefName, RefVersionId>,
    covers: Vec<Cover>,
    cover_keys: HashSet<(RefName, CoverLevel, u64)>,
    /// Each compaction pack's cover: ref and `seq` range.
    cover_packs: HashMap<PackId, (RefName, u64, u64)>,
    /// Packs whose rows are gone (`Fake::packs` leaves them out).
    no_row: HashSet<PackId>,
    packs: i64,
}

/// What the store was asked for since the last [`Fake::take_counts`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Counts {
    calls: usize,
    /// Ref versions asked for, by ID or by `seq`.
    versions: usize,
    /// Cover blocks asked about (`covers_at` firsts, or every cover of a
    /// ref read by `covers`), whether or not a cover was found.
    cover_keys: usize,
    /// Pack rows asked for.
    pack_rows: usize,
}

impl Counts {
    fn total(&self) -> usize {
        self.versions + self.cover_keys + self.pack_rows
    }
}

/// Ref versions and covers in memory; counts what it is asked for.
#[derive(Debug, Default)]
struct Fake {
    h: Mutex<History>,
    calls: AtomicUsize,
    versions: AtomicUsize,
    cover_keys: AtomicUsize,
    pack_rows: AtomicUsize,
    /// The blocks `covers_at` was asked about, in order.
    asks: Mutex<Vec<(CoverLevel, u64)>>,
    /// Taken by the next `covers_at`, which waits for it after reading.
    hold: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl Fake {
    fn read(&self, counter: &AtomicUsize, n: usize) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        counter.fetch_add(n, Ordering::SeqCst);
    }

    /// What the store was asked for since the last call.
    fn take_counts(&self) -> Counts {
        Counts {
            calls: self.calls.swap(0, Ordering::SeqCst),
            versions: self.versions.swap(0, Ordering::SeqCst),
            cover_keys: self.cover_keys.swap(0, Ordering::SeqCst),
            pack_rows: self.pack_rows.swap(0, Ordering::SeqCst),
        }
    }

    fn take_asks(&self) -> Vec<(CoverLevel, u64)> {
        std::mem::take(&mut *self.asks.lock().unwrap())
    }

    fn new_pack(h: &mut History) -> PackId {
        h.packs += 1;
        PackId::new(h.packs).unwrap()
    }

    fn current(&self, name: &str) -> Option<RefVersion> {
        let h = self.h.lock().unwrap();
        h.current.get(&rn(name)).map(|id| h.versions[id].clone())
    }

    fn live(&self, name: &str) -> Option<RefVersion> {
        self.current(name).filter(|v| v.value.tip().is_some())
    }

    /// Commits the next version of `name`.
    fn commit(&self, name: &str, value: Option<bool>, deps: Deps) -> RefVersion {
        let mut h = self.h.lock().unwrap();
        let name = rn(name);
        let parent = h.current.get(&name).map(|id| h.versions[id].clone());
        let id = RefVersionId::new(h.versions.len() as i64 + 1).unwrap();
        let value = match value {
            None => RefValue::Deleted,
            Some(packed) => RefValue::Live {
                tip: ObjectId::from_bytes([(id.get() % 251 + 1) as u8; 20]),
                pack: packed.then(|| Self::new_pack(&mut h)),
                closure: Closure::Exact,
            },
        };
        let v = RefVersion {
            id,
            repo: repo(),
            name: name.clone(),
            seq: parent
                .as_ref()
                .map_or(Seq::FIRST, |p| Seq::new(p.seq.get() + 1).unwrap()),
            parent: parent.map(|p| p.id),
            deps: if value.tip().is_some() {
                deps
            } else {
                Deps::default()
            },
            value,
            created_at: UnixMicros(0),
        };
        h.versions.insert(id, v.clone());
        h.current.insert(name, id);
        v
    }

    /// A trunk push on the current trunk version.
    fn trunk_push(&self, packed: bool) -> RefVersion {
        let trunk = self.live(MAIN).map(|v| v.id);
        self.commit(
            MAIN,
            Some(packed || trunk.is_none()),
            Deps {
                trunk,
                scoped_old: None,
            },
        )
    }

    /// Records the cover of `name` at `level` starting at `first`, with a
    /// new compaction pack when `packed`.
    fn cover(&self, layout: CoverLayout, name: &str, level: CoverLevel, first: u64, packed: bool) {
        let mut h = self.h.lock().unwrap();
        h.cover_keys.insert((rn(name), level, first));
        let first = Seq::new(first).unwrap();
        let last = Seq::new(first.get() + layout.span(level) - 1).unwrap();
        let id = CoverId::new(h.covers.len() as i64 + 1).unwrap();
        let pack = packed.then(|| Self::new_pack(&mut h));
        if let Some(p) = pack {
            h.cover_packs.insert(p, (rn(name), first.get(), last.get()));
        }
        h.covers.push(Cover {
            id,
            repo: repo(),
            name: rn(name),
            level,
            range: SeqRange::new(first, last).unwrap(),
            pack,
            chain: None,
        });
    }

    fn has_cover(&self, name: &str, level: CoverLevel, first: u64) -> bool {
        self.h
            .lock()
            .unwrap()
            .cover_keys
            .contains(&(rn(name), level, first))
    }

    /// The packs of the planner's full-closure plan of `roots`, over the
    /// whole history: what the search set must be.
    fn planned(&self, roots: &[RefVersionId]) -> Vec<PackId> {
        self.plan(roots).unwrap()
    }

    fn plan(&self, roots: &[RefVersionId]) -> Result<Vec<PackId>, aspen_core::fetch::PlanError> {
        let h = self.h.lock().unwrap();
        let mut covers: HashMap<RefName, Vec<Cover>> = HashMap::new();
        for c in &h.covers {
            covers.entry(c.name.clone()).or_default().push(c.clone());
        }
        let graph = Graph {
            versions: h.versions.clone(),
            covers,
            packs: (1..=h.packs)
                .map(|n| PackId::new(n).unwrap())
                .filter(|id| !h.no_row.contains(id))
                .map(|id| (id, record(id)))
                .collect(),
        };
        let input = PlanInput::new(PlanMode::FullClosure, roots.to_vec(), Vec::new());
        Ok(CoverPlanner.plan_with_stats(&graph, &input)?.0.packs)
    }

    /// The versions' own packs that `packs` stand for: a compaction pack
    /// holds exactly its cover's versions' packs (C1's compaction
    /// contract). Two search sets with the same objects give the same set.
    fn objects(&self, packs: &[PackId]) -> BTreeSet<PackId> {
        let h = self.h.lock().unwrap();
        let mut out = BTreeSet::new();
        for p in packs {
            match h.cover_packs.get(p) {
                None => {
                    out.insert(*p);
                }
                Some((name, first, last)) => out.extend(
                    h.versions
                        .values()
                        .filter(|v| &v.name == name && (*first..=*last).contains(&v.seq.get()))
                        .filter_map(|v| v.value.pack()),
                ),
            }
        }
        out
    }
}

fn record(id: PackId) -> PackRecord {
    PackRecord {
        id,
        pack: NewPack {
            repo: repo(),
            checksum: PackChecksum::from_bytes([1; 20]),
            size: 100,
            idx_size: 1100,
            entries_size: entry_table_len(1),
            object_count: 1,
            ref_delta_count: 0,
            origin: PackOrigin::Push { received_bytes: 80 },
        },
        generation: PackGeneration::FIRST,
        created_at: UnixMicros(0),
    }
}

struct Graph {
    versions: BTreeMap<RefVersionId, RefVersion>,
    covers: HashMap<RefName, Vec<Cover>>,
    packs: HashMap<PackId, PackRecord>,
}

impl VersionGraph for Graph {
    fn version(&self, id: RefVersionId) -> Option<&RefVersion> {
        self.versions.get(&id)
    }
    fn covers(&self, name: &RefName) -> &[Cover] {
        self.covers.get(name).map_or(&[], Vec::as_slice)
    }
    fn pack(&self, id: PackId) -> Option<&PackRecord> {
        self.packs.get(&id)
    }
}

impl MetaStore for Fake {
    async fn create_repo(&self, _: &RepoName, _: &RepoSettings) -> Result<Repo, StoreError> {
        unreachable!()
    }
    async fn repo(&self, _: &RepoName) -> Result<Option<Repo>, StoreError> {
        unreachable!()
    }
    async fn repo_by_id(&self, _: RepoId) -> Result<Option<Repo>, StoreError> {
        unreachable!()
    }
    async fn list_repos(&self, _: Option<RepoId>, _: u32) -> Result<Vec<Repo>, StoreError> {
        unreachable!()
    }
    async fn current_version(
        &self,
        _: RepoId,
        _: &RefName,
    ) -> Result<Option<RefVersion>, StoreError> {
        unreachable!()
    }
    async fn ref_version(&self, id: RefVersionId) -> Result<Option<RefVersion>, StoreError> {
        let v = self.h.lock().unwrap().versions.get(&id).cloned();
        self.read(&self.versions, 1);
        Ok(v)
    }
    async fn versions_by_seq(
        &self,
        repo: RepoId,
        name: &RefName,
        seqs: &[Seq],
    ) -> Result<Vec<RefVersion>, StoreError> {
        let h = self.h.lock().unwrap();
        let out: Vec<RefVersion> = seqs
            .iter()
            .filter_map(|s| {
                h.versions
                    .values()
                    .find(|v| v.repo == repo && &v.name == name && v.seq == *s)
                    .cloned()
            })
            .collect();
        self.read(&self.versions, seqs.len());
        Ok(out)
    }
    async fn list_refs(&self, _: RepoId, _: &RefListQuery) -> Result<Vec<RefVersion>, StoreError> {
        unreachable!()
    }
    async fn register_pack(&self, _: &NewPack) -> Result<PackRecord, StoreError> {
        unreachable!()
    }
    async fn packs(&self, ids: &[PackId]) -> Result<Vec<PackRecord>, StoreError> {
        self.read(&self.pack_rows, ids.len());
        let h = self.h.lock().unwrap();
        Ok(ids
            .iter()
            .filter(|id| !h.no_row.contains(id))
            .map(|&id| record(id))
            .collect())
    }
    async fn commit_push(&self, _: &PushCommit) -> Result<CommitOutcome, StoreError> {
        unreachable!()
    }
    async fn lookup_commits(
        &self,
        _: RepoId,
        _: &SortedIds,
    ) -> Result<Vec<CommitLookup>, StoreError> {
        unreachable!()
    }
    async fn index_commits(&self, _: RepoId, _: &[CommitRow]) -> Result<(), StoreError> {
        unreachable!()
    }
    async fn covers(&self, repo: RepoId, name: &RefName) -> Result<Vec<Cover>, StoreError> {
        let out: Vec<Cover> = self
            .h
            .lock()
            .unwrap()
            .covers
            .iter()
            .filter(|c| c.repo == repo && &c.name == name)
            .cloned()
            .collect();
        self.read(&self.cover_keys, out.len());
        Ok(out)
    }
    async fn covers_at(
        &self,
        repo: RepoId,
        name: &RefName,
        level: CoverLevel,
        firsts: &[Seq],
    ) -> Result<Vec<Cover>, StoreError> {
        let out: Vec<Cover> = self
            .h
            .lock()
            .unwrap()
            .covers
            .iter()
            .filter(|c| {
                c.repo == repo
                    && &c.name == name
                    && c.level == level
                    && firsts.contains(&c.range.first())
            })
            .cloned()
            .collect();
        self.read(&self.cover_keys, firsts.len());
        self.asks
            .lock()
            .unwrap()
            .extend(firsts.iter().map(|f| (level, f.get())));
        let hold = self.hold.lock().unwrap().take();
        if let Some(rx) = hold {
            let _ = rx.await;
        }
        Ok(out)
    }
    async fn insert_cover(&self, _: &NewCover) -> Result<Cover, StoreError> {
        unreachable!()
    }
    async fn set_cover_pack(&self, _: CoverId, _: PackId) -> Result<Cover, StoreError> {
        unreachable!()
    }
    async fn pack_by_checksum(
        &self,
        _: RepoId,
        _: &PackChecksum,
    ) -> Result<Option<PackRecord>, StoreError> {
        unreachable!()
    }
    async fn sweep_orphan_packs(&self, _: Duration, _: u32) -> Result<Vec<PackRecord>, StoreError> {
        unreachable!()
    }
    async fn pending_pack_deletes(&self, _: u32) -> Result<Vec<PackTombstone>, StoreError> {
        unreachable!()
    }
    async fn finish_pack_deletes(&self, _: &[PackTombstone]) -> Result<(), StoreError> {
        unreachable!()
    }
    async fn shallow_pack(
        &self,
        _: RepoId,
        _: RefVersionId,
        _: ShallowDepth,
    ) -> Result<Option<PackRecord>, StoreError> {
        unreachable!()
    }
    async fn set_shallow_pack(
        &self,
        _: RepoId,
        _: RefVersionId,
        _: ShallowDepth,
        _: PackId,
        _: u64,
    ) -> Result<ShallowRecorded, StoreError> {
        unreachable!()
    }
    async fn mark_shallow_pack_used(
        &self,
        _: RepoId,
        _: RefVersionId,
        _: ShallowDepth,
    ) -> Result<(), StoreError> {
        unreachable!()
    }
    async fn evict_shallow_packs(
        &self,
        _: Duration,
        _: Duration,
        _: u32,
    ) -> Result<u64, StoreError> {
        unreachable!()
    }
    async fn enqueue_jobs(&self, _: &[NewJob]) -> Result<Vec<JobId>, StoreError> {
        unreachable!()
    }
    async fn claim_job(
        &self,
        _: &[JobTag],
        _: &str,
        _: Duration,
    ) -> Result<Option<Job>, StoreError> {
        unreachable!()
    }
    async fn release_jobs(&self, _: &[JobTag], _: &str, _: &str) -> Result<u64, StoreError> {
        unreachable!()
    }
    async fn add_compaction_heat(
        &self,
        _: &[aspen_core::meta::CompactionHeat],
    ) -> Result<u64, StoreError> {
        unreachable!()
    }
    async fn update_job(&self, _: JobId, _: JobEvent) -> Result<Job, StoreError> {
        unreachable!()
    }
}

fn levels() -> impl Iterator<Item = CoverLevel> {
    (1..=MAX_LEVEL).map(|l| CoverLevel::new(l).unwrap())
}

/// A clock a test moves by hand.
#[derive(Debug, Clone)]
struct ManualClock(Arc<Mutex<Instant>>);

impl ManualClock {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Instant::now())))
    }

    fn advance(&self, d: Duration) {
        *self.0.lock().unwrap() += d;
    }

    fn clock(&self) -> Clock {
        let at = Arc::clone(&self.0);
        Clock(Arc::new(move || *at.lock().unwrap()))
    }
}

/// Closures whose blocks wait `first` then up to `max` before they are
/// asked about again.
fn with_recheck(layout: CoverLayout, first: Duration, max: Duration) -> Closures {
    Closures::tuned(
        layout,
        Tuning {
            recheck_first: first,
            recheck_max: max,
            ..Tuning::default()
        },
    )
}

async fn search(fake: &Fake, closures: &Closures, roots: &[RefVersionId]) -> Vec<PackId> {
    closure_packs(fake, closures, repo(), roots)
        .await
        .unwrap()
        .as_slice()
        .to_vec()
}

/// Pushes `n` trunk versions, one at a time as pushes arrive: before each
/// push, the search packs of the trunk version it builds on; with
/// `compact`, every cover is compacted as soon as its last version lands.
/// Calls `between` before each push. Returns what each search asked the
/// store for, by the trunk `seq` it searched.
async fn trunk_pushes(
    fake: &Fake,
    closures: &Closures,
    layout: CoverLayout,
    n: u64,
    compact: bool,
    mut between: impl FnMut(),
) -> Vec<Counts> {
    let mut counts = Vec::new();
    for _ in 0..n {
        between();
        let v = fake.trunk_push(true);
        if compact {
            for (level, range) in layout.ending_at(v.seq) {
                fake.cover(layout, MAIN, level, range.first().get(), true);
            }
        }
        fake.take_counts();
        let packs = search(fake, closures, &[v.id]).await;
        counts.push(fake.take_counts());
        if !compact {
            assert_eq!(packs.len() as u64, v.seq.get(), "one pack per version");
        }
    }
    counts
}

/// The most store keys any of `counts` asked for.
fn most(counts: &[Counts]) -> usize {
    counts.iter().map(Counts::total).max().unwrap()
}

/// Invariant (issue #149): with compaction keeping up, the metadata a push
/// reads to find its search packs does not grow with the trunk's history.
/// Each push reads the version it builds on and the covers (and their pack
/// rows) that became complete, never the versions, covers or pack rows
/// below them, and its search set stays small. It is still the planner's
/// full-closure plan.
#[tokio::test]
async fn a_push_reads_no_more_metadata_after_4000_trunk_pushes_than_after_300() {
    let layout = CoverLayout::new(4).unwrap();
    let fake = Fake::default();
    let closures = Closures::new(layout);
    let counts = trunk_pushes(&fake, &closures, layout, 4100, true, || {}).await;
    let early = most(&counts[10..300]);
    let late = most(&counts[4000..]);
    assert!(
        late <= early && late <= 1 + 2 * usize::from(MAX_LEVEL),
        "store keys per push: at most {early} up to 300 pushes, {late} after 4,000"
    );
    let trunk = fake.live(MAIN).unwrap();
    let packs = search(&fake, &closures, &[trunk.id]).await;
    assert_eq!(packs, fake.planned(&[trunk.id]));
    // Covers of 256 versions, then at most 3 per level and the tail.
    assert!(packs.len() <= 4100 / 256 + 3 * 4, "{} packs", packs.len());
}

/// Invariant (issue #149): when the caches turn over, an active
/// repository's next push still reads only the version it builds on. A
/// cache that dropped everything at its limit would make that push walk
/// the whole history again, once per turnover.
#[tokio::test]
async fn a_cache_turnover_does_not_make_a_push_walk_the_history_again() {
    let layout = CoverLayout::new(4).unwrap();
    let fake = Fake::default();
    let closures = Closures::tuned(
        layout,
        Tuning {
            cache_limit: 64,
            ..Tuning::default()
        },
    );
    // About 30 turnovers of each cache.
    let counts = trunk_pushes(&fake, &closures, layout, 1000, true, || {}).await;
    let late = most(&counts[100..]);
    assert!(
        late <= 1 + 2 * usize::from(MAX_LEVEL),
        "store keys per push after the caches turned over: up to {late}"
    );
    assert!(counts[100..].iter().all(|c| c.versions <= 1));
    let trunk = fake.live(MAIN).unwrap();
    assert_eq!(
        search(&fake, &closures, &[trunk.id]).await,
        fake.planned(&[trunk.id])
    );
}

/// Invariant (issue #149): without compaction the search set is one pack
/// per version, and a push still reads only the version it builds on. The
/// cover blocks of the closure are asked about, but each at most once a
/// minute once its wait has grown: the keys asked in a minute are bounded
/// by the closure's blocks (about `N / (fanout - 1)`), not by the pushes.
#[tokio::test]
async fn without_compaction_a_push_reads_one_version_and_asks_each_block_once_a_minute() {
    let layout = CoverLayout::new(4).unwrap();
    let fake = Fake::default();
    let time = ManualClock::new();
    let closures = Closures::tuned(
        layout,
        Tuning {
            clock: time.clock(),
            ..Tuning::default()
        },
    );
    let n = 2000;
    let mut asked: HashMap<(CoverLevel, u64), Vec<u64>> = HashMap::new();
    let mut second = 0;
    let counts = trunk_pushes(&fake, &closures, layout, n, false, || {
        time.advance(Duration::from_secs(1));
        second += 1;
        for key in fake.take_asks() {
            asked.entry(key).or_default().push(second);
        }
    })
    .await;
    for key in fake.take_asks() {
        asked.entry(key).or_default().push(second + 1);
    }
    let (at, c) = counts
        .iter()
        .enumerate()
        .max_by_key(|(_, c)| c.versions)
        .unwrap();
    assert!(
        c.versions <= 1 && c.pack_rows == 0,
        "{c:?} for the search before push {}",
        at + 2
    );
    // Waits of 1, 2, 4, 8, 16, 32 s, then 60 s.
    for (key, at) in &asked {
        for (i, pair) in at.windows(2).enumerate() {
            let gap = pair[1] - pair[0];
            let wait = (1u64 << i).min(60);
            assert!(gap >= wait, "block {key:?} asked at {at:?}");
        }
    }
    // In the last minute: each block of the closure at most once, plus
    // the shorter waits (at most 6 more asks) of blocks completed in it.
    let blocks: u64 = levels().map(|l| n / layout.span(l)).sum();
    let new: u64 = levels()
        .map(|l| n / layout.span(l) - (n - 60) / layout.span(l))
        .sum();
    let last_minute: usize = counts[(n - 60) as usize..]
        .iter()
        .map(|c| c.cover_keys)
        .sum();
    assert!(
        last_minute as u64 <= blocks + 6 * new,
        "{last_minute} keys in the last minute, {blocks} blocks, {new} new"
    );
}

/// The first search after a start walks the history it has not seen once;
/// the push after it reads one version.
#[tokio::test]
async fn a_cold_start_walks_the_history_once() {
    let layout = CoverLayout::new(4).unwrap();
    let fake = Fake::default();
    for _ in 0..300 {
        let v = fake.trunk_push(true);
        for (level, range) in layout.ending_at(v.seq) {
            fake.cover(layout, MAIN, level, range.first().get(), true);
        }
    }
    let closures = Closures::new(layout);
    let v = fake.live(MAIN).unwrap();
    assert_eq!(
        search(&fake, &closures, &[v.id]).await,
        fake.planned(&[v.id])
    );
    let cold = fake.take_counts();
    assert!(
        cold.versions >= 300,
        "the first search reads every version: {cold:?}"
    );
    let v = fake.trunk_push(true);
    assert_eq!(
        search(&fake, &closures, &[v.id]).await,
        fake.planned(&[v.id])
    );
    let warm = fake.take_counts();
    assert_eq!(warm.total(), 1, "the next one reads only the new version");
}

/// A block found without a compacted cover is not asked about again until
/// its wait is over, and its versions' own packs stand for it meanwhile;
/// once asked again, the cover's pack replaces them.
#[tokio::test]
async fn a_cover_compacted_after_a_push_looked_is_used_once_its_wait_is_over() {
    let layout = CoverLayout::new(4).unwrap();
    let level1 = CoverLevel::new(1).unwrap();
    for (wait, picked_up) in [(Duration::from_secs(3600), false), (Duration::ZERO, true)] {
        let fake = Fake::default();
        let closures = with_recheck(layout, wait, wait);
        let mut v = fake.trunk_push(true);
        for _ in 1..4 {
            v = fake.trunk_push(true);
        }
        let own = search(&fake, &closures, &[v.id]).await;
        assert_eq!(own.len(), 4);
        fake.cover(layout, MAIN, level1, 1, true);
        fake.take_counts();
        let after = search(&fake, &closures, &[v.id]).await;
        let counts = fake.take_counts();
        if picked_up {
            assert_eq!(after, fake.planned(&[v.id]));
            assert_eq!(after.len(), 1);
        } else {
            assert_eq!(
                after, own,
                "the same versions' packs until the wait is over"
            );
            assert_eq!(counts.calls, 0, "the block is not asked about again");
        }
    }
}

/// A search that asked about a block before its cover was compacted, and
/// heard back after another search found the cover, keeps the cover: a
/// compacted block is never recorded as absent again.
#[tokio::test]
async fn a_late_answer_does_not_forget_a_compacted_cover() {
    let layout = CoverLayout::new(4).unwrap();
    let fake = Fake::default();
    let closures = with_recheck(layout, Duration::from_secs(3600), Duration::from_secs(3600));
    let mut v = fake.trunk_push(true);
    for _ in 1..4 {
        v = fake.trunk_push(true);
    }
    let (tx, rx) = tokio::sync::oneshot::channel();
    *fake.hold.lock().unwrap() = Some(rx);
    let roots = [v.id];
    let late = search(&fake, &closures, &roots);
    let other = async {
        // Until the first search is waiting with its (empty) answer.
        while fake.hold.lock().unwrap().is_some() {
            tokio::task::yield_now().await;
        }
        fake.cover(layout, MAIN, CoverLevel::new(1).unwrap(), 1, true);
        let packs = search(&fake, &closures, &[v.id]).await;
        tx.send(()).unwrap();
        packs
    };
    let (_, found) = tokio::join!(late, other);
    assert_eq!(found, fake.planned(&[v.id]));
    assert_eq!(
        search(&fake, &closures, &[v.id]).await,
        fake.planned(&[v.id]),
        "the compacted cover is still used"
    );
}

/// Invariant: a cover whose pack has no row is never selected. It is
/// corruption, as it is to the planner (`PlanError::MissingPack`); the
/// sweep never deletes a pack a cover names, so a store never shows it.
#[tokio::test]
async fn a_cover_whose_pack_has_no_row_is_corrupt() {
    let layout = CoverLayout::new(4).unwrap();
    let fake = Fake::default();
    let closures = Closures::new(layout);
    let mut v = fake.trunk_push(true);
    for _ in 1..4 {
        v = fake.trunk_push(true);
    }
    fake.cover(layout, MAIN, CoverLevel::new(1).unwrap(), 1, true);
    let cover_pack = PackId::new(5).unwrap();
    fake.h.lock().unwrap().no_row.insert(cover_pack);
    assert!(fake.plan(&[v.id]).is_err(), "the planner refuses it too");
    let err = closure_packs(&fake, &closures, repo(), &[v.id])
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Corrupt { .. }), "{err}");
}

/// A cover in a block's place that the layout does not place (written
/// under another fanout) is not used: the versions' own packs, which hold
/// the same objects, are searched instead.
#[tokio::test]
async fn a_cover_of_another_layout_is_not_used() {
    let fake = Fake::default();
    let closures = with_recheck(CoverLayout::new(4).unwrap(), Duration::ZERO, Duration::ZERO);
    let mut v = fake.trunk_push(true);
    for _ in 1..8 {
        v = fake.trunk_push(true);
    }
    // Level 1 under fanout 8: versions 1 to 8.
    fake.cover(
        CoverLayout::new(8).unwrap(),
        MAIN,
        CoverLevel::new(1).unwrap(),
        1,
        true,
    );
    let packs = search(&fake, &closures, &[v.id]).await;
    let own: Vec<PackId> = (1..=8).map(|n| PackId::new(n).unwrap()).collect();
    assert_eq!(packs, own);
}

/// A root, or a remembered closure, of another repository is corruption.
#[tokio::test]
async fn a_version_of_another_repository_is_corrupt() {
    let layout = CoverLayout::new(4).unwrap();
    let fake = Fake::default();
    let closures = Closures::new(layout);
    let v = fake.trunk_push(true);
    let other = RepoId::new(2).unwrap();
    for _ in 0..2 {
        let err = closure_packs(&fake, &closures, other, &[v.id])
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Corrupt { .. }), "{err}");
        // Second round: the closure is remembered under repository 1.
        search(&fake, &closures, &[v.id]).await;
    }
}

/// Invariant: on random histories (trunk pushes, forced and orphan trunk
/// updates, plain and branch-scoped branch pushes, deletions, versions
/// without packs, covers compacted in any order or left without a pack),
/// the search packs of any roots are exactly the planner's full-closure
/// plan, while the remembered closures and covers carry over from one
/// search to the next as they do across pushes.
#[tokio::test]
async fn search_packs_are_the_full_closure_plan_on_random_histories() {
    for seed in 0..48u64 {
        assert_eq!(random_history(seed, false).await, 0);
    }
}

/// Invariant: with real waits (a block found without a compacted cover is
/// not asked about again for 1 s, then up to 8 s, while the clock moves 0
/// to 3 s a step) and with covers written under another fanout too, the
/// search packs hold exactly the objects of the planner's full-closure
/// plan: the same versions' packs, some of them through covers the planner
/// uses and the search does not.
#[tokio::test]
async fn search_packs_hold_the_plans_objects_while_blocks_wait_and_under_other_fanouts() {
    let mut differed = 0;
    for seed in 0..48u64 {
        differed += random_history(seed, true).await;
    }
    assert!(
        differed > 0,
        "no search ever differed from the plan's packs"
    );
}

/// A random history (module docs of the two tests above), checked every
/// fifth step: packs equal to the plan's when `waits` is false, objects
/// equal when it is true. Returns how many checks found other packs than
/// the plan's.
async fn random_history(seed: u64, waits: bool) -> usize {
    let mut differed = 0;
    {
        let mut rng = Rng::new(0x1490_0000 + seed + if waits { 1 << 16 } else { 0 });
        let layout = CoverLayout::new(2 + seed % 3).unwrap();
        let other = CoverLayout::new(3 + seed % 3).unwrap();
        let fake = Fake::default();
        let time = ManualClock::new();
        let closures = if waits {
            Closures::tuned(
                layout,
                Tuning {
                    recheck_first: Duration::from_secs(1),
                    recheck_max: Duration::from_secs(8),
                    clock: time.clock(),
                    ..Tuning::default()
                },
            )
        } else {
            with_recheck(layout, Duration::ZERO, Duration::ZERO)
        };
        let branches = ["refs/heads/b0", "refs/heads/b1", "refs/heads/b2"];
        let mut checked = 0;
        for step in 0..250 {
            let trunk = fake.live(MAIN);
            let b = branches[rng.below(branches.len())];
            let packed = rng.below(10) < 8;
            match rng.weighted(&[50, 5, 12, 25, 6]) {
                0 => {
                    fake.trunk_push(packed);
                }
                1 => {
                    // A forced or recomputed trunk update: some earlier
                    // live trunk version, or none (orphan history).
                    let h = fake.h.lock().unwrap();
                    let olders: Vec<RefVersionId> = h
                        .versions
                        .values()
                        .filter(|v| v.name == rn(MAIN) && v.value.tip().is_some())
                        .map(|v| v.id)
                        .collect();
                    drop(h);
                    let dep = (!olders.is_empty() && rng.below(4) > 0)
                        .then(|| olders[rng.below(olders.len())]);
                    let deps = Deps {
                        trunk: dep,
                        scoped_old: None,
                    };
                    fake.commit(MAIN, Some(packed || dep.is_none()), deps);
                }
                2 | 3 if trunk.is_some() => {
                    let scoped = if rng.below(2) == 0 {
                        fake.live(b).map(|v| v.id)
                    } else {
                        None
                    };
                    let deps = Deps {
                        trunk: trunk.map(|t| t.id),
                        scoped_old: scoped,
                    };
                    fake.commit(b, Some(packed), deps);
                }
                4 if fake.live(b).is_some() => {
                    fake.commit(b, None, Deps::default());
                }
                _ => {
                    fake.trunk_push(packed);
                }
            }
            // Compaction, partial and in no order; with `waits`, sometimes
            // under another fanout.
            if waits {
                time.advance(Duration::from_secs(rng.below(4) as u64));
            }
            if rng.below(3) == 0 {
                let name = [MAIN, branches[0], branches[1], branches[2]][rng.below(4)];
                let by = if waits && rng.below(4) == 0 {
                    other
                } else {
                    layout
                };
                if let Some(cur) = fake.current(name) {
                    for level in levels() {
                        let span = by.span(level);
                        let mut first = 1;
                        while first + span - 1 <= cur.seq.get() {
                            if !fake.has_cover(name, level, first) && rng.below(2) == 0 {
                                fake.cover(by, name, level, first, rng.below(8) > 0);
                            }
                            first += span;
                        }
                    }
                }
            }
            if step % 5 == 0 {
                let all: Vec<RefVersionId> =
                    fake.h.lock().unwrap().versions.keys().copied().collect();
                let mut roots: Vec<RefVersionId> =
                    fake.live(MAIN).map(|v| v.id).into_iter().collect();
                for _ in 0..rng.below(3) {
                    roots.push(all[rng.below(all.len())]);
                }
                if let Some(b) = fake.live(b) {
                    roots.push(b.id);
                }
                let packs = search(&fake, &closures, &roots).await;
                let planned = fake.planned(&roots);
                differed += usize::from(packs != planned);
                if waits {
                    assert_eq!(
                        fake.objects(&packs),
                        fake.objects(&planned),
                        "seed {seed}, step {step}, roots {roots:?}"
                    );
                } else {
                    assert_eq!(packs, planned, "seed {seed}, step {step}, roots {roots:?}");
                }
                checked += 1;
            }
        }
        assert!(checked > 0);
    }
    differed
}

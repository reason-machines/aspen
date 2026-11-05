//! The planner's in-memory view of version history, loaded on demand.
//!
//! `aspen_planner::CoverPlanner` is pure and reads a [`VersionGraph`]; it
//! reports the first version or pack it lacks. [`plan`] starts from the
//! wants and haves and, on each miss, loads breadth-first from the missing
//! version and from every dependency of the loaded set that is not loaded
//! yet (the frontier), with a budget that doubles each round, then plans
//! again. Seeding from the whole frontier matters for branch-scoped
//! histories: a scoped version reaches its predecessor only through its
//! `scoped_old` dependency, so a clone of many such branches has many
//! disjoint chains, and loading from one miss at a time would cost a round
//! per branch. With the frontier, a full clone loads its closure in
//! O(log n) rounds, and an incremental fetch loads at most about twice the
//! versions its walk visits (the have side's frontier is loaded too).
//!
//! A miss that is **not** in the frontier is the target of a have-side skip
//! through a verified cover chain (node C1): the planner jumped from a held
//! version to the first version of a chained cover's range, which no loaded
//! version depends on. Such a miss loads that version and its direct
//! dependencies only, and does not double the budget. A skip walk takes one
//! such jump per cover level it crosses, so doubling on each would load
//! exponentially more than the walk visits; loading breadth-first from the
//! target would load the very versions the skip avoids.
//!
//! A fetch loads at most [`ServerConfig::max_plan_versions`] versions; past
//! that it is refused with an ordinary `ERR`, never as corruption. Only a
//! version that a loaded version names and the store cannot return is
//! corruption.
//!
//! Loading costs one metadata read per version: schema v1's store has no
//! batch version read. `aspen_fetch_plan_loaded_versions` exports the count
//! per fetch next to `PlanStats.visited`; D2 measures both (bench/README.md).
//!
//! [`ServerConfig::max_plan_versions`]: crate::ServerConfig::max_plan_versions

use std::collections::{HashMap, HashSet, VecDeque};

use aspen_core::error::{FetchError, StoreError};
use aspen_core::fetch::{FetchPlan, PlanError, PlanInput, VersionGraph};
use aspen_core::id::{PackId, RefVersionId, RepoId};
use aspen_core::meta::{Cover, MetaStore};
use aspen_core::name::RefName;
use aspen_core::pack::PackRecord;
use aspen_core::refs::RefVersion;
use aspen_planner::{CoverPlanner, PlanStats};

use super::Failure;

/// Versions loaded in the first round after a miss.
const FIRST_BUDGET: usize = 64;

/// Versions, covers and pack rows loaded so far.
#[derive(Debug, Default)]
pub(crate) struct LoadedGraph {
    versions: HashMap<RefVersionId, RefVersion>,
    covers: HashMap<RefName, Vec<Cover>>,
    packs: HashMap<PackId, PackRecord>,
}

impl VersionGraph for LoadedGraph {
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

impl LoadedGraph {
    /// A loaded pack row.
    pub(crate) fn pack_row(&self, id: PackId) -> Option<&PackRecord> {
        self.packs.get(&id)
    }

    /// Ref versions loaded so far, each one metadata read.
    pub(crate) fn version_count(&self) -> usize {
        self.versions.len()
    }

    /// Inserts a version already read (a want's or a have's).
    pub(crate) fn insert(&mut self, v: RefVersion) {
        self.versions.insert(v.id, v);
    }

    /// Dependencies of loaded versions that are not loaded yet, in ID
    /// order (so rounds are deterministic).
    fn frontier(&self) -> Vec<RefVersionId> {
        let mut out: Vec<RefVersionId> = self
            .versions
            .values()
            .flat_map(|v| v.deps.iter())
            .filter(|d| !self.versions.contains_key(d))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Loads `from` and what it depends on, breadth-first, until `budget`
    /// new versions are loaded or nothing is left; then the covers of every
    /// ref seen and every pack row they and the versions name. Refuses once
    /// more than `max` versions would be loaded in total.
    async fn load<M: MetaStore>(
        &mut self,
        meta: &M,
        repo: RepoId,
        from: &[RefVersionId],
        budget: usize,
        max: usize,
    ) -> Result<(), Failure> {
        let mut queue: VecDeque<RefVersionId> = from.iter().copied().collect();
        let mut loaded = 0;
        let mut seen = HashSet::new();
        while let Some(id) = queue.pop_front() {
            if !seen.insert(id) {
                continue;
            }
            if !self.versions.contains_key(&id) {
                if loaded == budget {
                    break;
                }
                if self.versions.len() >= max {
                    return Err(too_many_versions(max));
                }
                let v = meta
                    .ref_version(id)
                    .await
                    .map_err(FetchError::Store)?
                    .ok_or_else(|| corrupt(format!("ref version {id} is named but missing")))?;
                if v.repo != repo {
                    return Err(
                        corrupt(format!("ref version {id} is in another repository")).into(),
                    );
                }
                self.versions.insert(id, v);
                loaded += 1;
            }
            queue.extend(self.versions[&id].deps.iter());
        }
        Ok(self.load_covers_and_packs(meta, repo).await?)
    }

    async fn load_covers_and_packs<M: MetaStore>(
        &mut self,
        meta: &M,
        repo: RepoId,
    ) -> Result<(), FetchError> {
        let names: HashSet<RefName> = self.versions.values().map(|v| v.name.clone()).collect();
        for name in names {
            if let std::collections::hash_map::Entry::Vacant(slot) = self.covers.entry(name) {
                let covers = meta
                    .covers(repo, slot.key())
                    .await
                    .map_err(FetchError::Store)?;
                slot.insert(covers);
            }
        }
        let mut missing: Vec<PackId> = self
            .versions
            .values()
            .filter_map(|v| v.value.pack())
            .chain(self.covers.values().flatten().filter_map(|c| c.pack))
            .filter(|p| !self.packs.contains_key(p))
            .collect();
        missing.sort_unstable();
        missing.dedup();
        if !missing.is_empty() {
            for row in meta.packs(&missing).await.map_err(FetchError::Store)? {
                if row.pack.repo != repo {
                    return Err(corrupt(format!("pack {} is in another repository", row.id)));
                }
                self.packs.insert(row.id, row);
            }
        }
        Ok(())
    }
}

/// Plans `input` with the cover planner, loading what it needs, at most
/// `max_versions` versions in all.
pub(crate) async fn plan<M: MetaStore>(
    meta: &M,
    repo: RepoId,
    graph: &mut LoadedGraph,
    input: &PlanInput,
    max_versions: usize,
) -> Result<(FetchPlan, PlanStats), Failure> {
    let start: Vec<RefVersionId> = input
        .wants()
        .iter()
        .copied()
        .chain(input.haves().iter().map(|h| h.version()))
        .collect();
    graph
        .load(meta, repo, &start, start.len(), max_versions)
        .await?;
    let mut budget = FIRST_BUDGET;
    // Terminates: every round loads at least one new version (or fails),
    // and at most `max_versions` are ever loaded.
    loop {
        match CoverPlanner.plan_with_stats(graph, input) {
            Ok(done) => return Ok(done),
            Err(PlanError::NoWants) => {
                unreachable!("the fetch service plans only requests with wants")
            }
            Err(PlanError::MissingVersion(id)) => {
                let before = graph.versions.len();
                let frontier = graph.frontier();
                if frontier.binary_search(&id).is_ok() {
                    let mut from = vec![id];
                    from.extend(frontier.into_iter().filter(|f| *f != id));
                    graph.load(meta, repo, &from, budget, max_versions).await?;
                    budget = budget.saturating_mul(2);
                } else {
                    // A chain skip's target: it and what it depends on.
                    graph.load(meta, repo, &[id], 1, max_versions).await?;
                    let deps: Vec<RefVersionId> = graph
                        .versions
                        .get(&id)
                        .map(|v| v.deps.iter().collect())
                        .unwrap_or_default();
                    graph
                        .load(meta, repo, &deps, deps.len(), max_versions)
                        .await?;
                }
                if graph.versions.len() == before {
                    return Err(corrupt(format!("ref version {id} cannot be loaded")).into());
                }
            }
            Err(PlanError::MissingPack(id)) => {
                return Err(corrupt(format!("pack {id} is named but has no row")).into());
            }
        }
    }
}

/// The refusal for a fetch whose history is over the configured limit.
fn too_many_versions(max: usize) -> Failure {
    Failure::refused(format!(
        "this fetch needs more than {max} ref versions of history to plan, over this \
         server's limit (max_plan_versions); ask its operator to raise it"
    ))
}

fn corrupt(what: String) -> FetchError {
    FetchError::Store(StoreError::Corrupt { what })
}

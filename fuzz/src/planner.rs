//! The planner target: random version histories, covers and queries
//! against [`CoverPlanner`], checked by an exact model.
//!
//! A5's property tests check the planner against real repositories and
//! `git rev-list`; they are slow, so they cover 64 seeds in required CI.
//! Here objects are abstract IDs and a version's closure is the union of
//! the packs of every version its dependencies reach, which is the
//! contract the planner works under (`ref_versions`, `covers` in
//! `docs/PLAN.md`): so the fuzzer can try millions of histories, with
//! deletions, forced and scoped pushes, shared and empty packs, chained
//! and unchained covers, and versions missing from the loaded view.

use std::collections::{BTreeMap, BTreeSet};

use aspen_core::fetch::{HaveVersion, PlanError, PlanInput, PlanMode, VersionGraph};
use aspen_core::id::{
    CoverId, ObjectId, PackChecksum, PackId, RefVersionId, RepoId, Seq, UnixMicros,
};
use aspen_core::meta::{Cover, CoverLayout, VerifiedRange};
use aspen_core::name::RefName;
use aspen_core::pack::{
    CoverLevel, NewPack, PackGeneration, PackOrigin, PackRecord, entry_table_len,
};
use aspen_core::refs::{Closure, Deps, RefValue, RefVersion};
use aspen_planner::CoverPlanner;

use crate::Reached;
use crate::input::Input;

const REFS: [&str; 4] = [
    "refs/heads/main",
    "refs/heads/a",
    "refs/heads/b",
    "refs/tags/t",
];

fn repo() -> RepoId {
    RepoId::new(1).expect("non-zero")
}

/// The whole history, plus each pack's objects (which the planner never
/// sees) and a set of versions hidden from the planner's view.
#[derive(Debug, Default)]
struct World {
    versions: BTreeMap<RefVersionId, RefVersion>,
    covers: BTreeMap<RefName, Vec<Cover>>,
    packs: BTreeMap<PackId, PackRecord>,
    objects: BTreeMap<PackId, BTreeSet<ObjectId>>,
    heads: BTreeMap<RefName, RefVersionId>,
    hidden: BTreeSet<RefVersionId>,
    next_object: u64,
}

impl VersionGraph for World {
    fn version(&self, id: RefVersionId) -> Option<&RefVersion> {
        if self.hidden.contains(&id) {
            return None;
        }
        self.versions.get(&id)
    }
    fn covers(&self, name: &RefName) -> &[Cover] {
        self.covers.get(name).map_or(&[], Vec::as_slice)
    }
    fn pack(&self, id: PackId) -> Option<&PackRecord> {
        self.packs.get(&id)
    }
}

impl World {
    fn object(&mut self) -> ObjectId {
        self.next_object += 1;
        let mut b = [0u8; 20];
        b[..8].copy_from_slice(&self.next_object.to_be_bytes());
        b[19] = 1;
        ObjectId::from_bytes(b)
    }

    fn add_pack(&mut self, objects: BTreeSet<ObjectId>, level: Option<CoverLevel>) -> PackId {
        let n = self.packs.len() as i64 + 1;
        let id = PackId::new(n).expect("non-zero");
        let mut checksum = [0u8; 20];
        checksum[..8].copy_from_slice(&n.to_be_bytes());
        let count = u32::try_from(objects.len()).expect("small");
        self.packs.insert(
            id,
            PackRecord {
                id,
                pack: NewPack {
                    repo: repo(),
                    checksum: PackChecksum::from_bytes(checksum),
                    size: 32 + 100 * u64::from(count),
                    idx_size: 1072 + 28 * u64::from(count),
                    entries_size: entry_table_len(count),
                    object_count: count,
                    ref_delta_count: 0,
                    origin: match level {
                        None => PackOrigin::Push { received_bytes: 0 },
                        Some(level) => PackOrigin::Compaction { level },
                    },
                },
                generation: PackGeneration::FIRST,
                created_at: UnixMicros(0),
            },
        );
        self.objects.insert(id, objects);
        id
    }

    fn push(&mut self, name: &RefName, value: RefValue, deps: Deps) -> RefVersionId {
        let id = RefVersionId::new(self.versions.len() as i64 + 1).expect("non-zero");
        let parent = self.heads.get(name).copied();
        let seq = match parent {
            None => Seq::FIRST,
            Some(p) => self.versions[&p].seq.next().expect("small"),
        };
        self.versions.insert(
            id,
            RefVersion {
                id,
                repo: repo(),
                name: name.clone(),
                seq,
                parent,
                value,
                deps,
                created_at: UnixMicros(0),
            },
        );
        self.heads.insert(name.clone(), id);
        id
    }

    fn live(&self, id: RefVersionId) -> bool {
        self.versions[&id].value.tip().is_some()
    }

    fn versions_of(&self, name: &RefName) -> Vec<&RefVersion> {
        let mut v: Vec<&RefVersion> = self.versions.values().filter(|v| &v.name == name).collect();
        v.sort_by_key(|v| v.seq);
        v
    }

    /// The covers that end at `seq` of `name`, as compaction builds them.
    fn add_covers(&mut self, name: &RefName, seq: Seq, layout: CoverLayout, input: &mut Input<'_>) {
        for (level, range) in layout.ending_at(seq) {
            let in_range: Vec<RefVersion> = self
                .versions_of(name)
                .into_iter()
                .filter(|v| range.contains(v.seq))
                .cloned()
                .collect();
            let chain = if input.one_in(4) {
                None
            } else {
                VerifiedRange::verify(name, range, &in_range)
                    .ok()
                    .map(|r| r.chain())
            };
            let pack = if input.one_in(4) {
                None
            } else {
                let objects: BTreeSet<ObjectId> = in_range
                    .iter()
                    .filter_map(|v| v.value.pack())
                    .flat_map(|p| self.objects[&p].iter().copied())
                    .collect();
                (!objects.is_empty()).then(|| self.add_pack(objects, Some(level)))
            };
            let covers = self.covers.entry(name.clone()).or_default();
            let id = CoverId::new(covers.len() as i64 + 1 + 1000 * i64::from(level.get()))
                .expect("non-zero");
            covers.push(Cover {
                id,
                repo: repo(),
                name: name.clone(),
                level,
                range,
                pack,
                chain,
            });
            covers.sort_by_key(|c| (c.level, c.range.first()));
        }
    }

    fn dep_closure(&self, ids: impl IntoIterator<Item = RefVersionId>) -> BTreeSet<RefVersionId> {
        let mut seen = BTreeSet::new();
        let mut stack: Vec<_> = ids.into_iter().collect();
        while let Some(id) = stack.pop() {
            if seen.insert(id) {
                stack.extend(self.versions[&id].deps.iter());
            }
        }
        seen
    }

    fn objects_of_versions(&self, ids: &BTreeSet<RefVersionId>) -> BTreeSet<ObjectId> {
        ids.iter()
            .filter_map(|id| self.versions[id].value.pack())
            .flat_map(|p| self.objects[&p].iter().copied())
            .collect()
    }
}

/// Builds a history from the input: pushes to trunk (fast-forward or
/// forced, onto an older trunk version or orphaned), plain-URL and scoped
/// branch pushes, tags, deletions, packs that are fresh, shared with
/// another version (B1's shared pack) or absent (a push that brought no
/// objects), objects repeated across packs, and some versions recorded
/// `Wider` (their tips can never stand for a have). Covers follow the
/// layout as the compactor writes them, with or without a pack and chain.
fn build(input: &mut Input<'_>) -> World {
    let layout = CoverLayout::new(2 + input.below(3) as u64).expect("fanout 2 to 4");
    let names: Vec<RefName> = REFS
        .iter()
        .map(|r| RefName::new(*r).expect("valid"))
        .collect();
    let mut w = World::default();
    for _ in 0..1 + input.below(80) {
        let r = if w.heads.contains_key(&names[0]) {
            input.below(names.len())
        } else {
            0
        };
        let name = names[r].clone();
        let trunk = w.heads.get(&names[0]).copied();
        let head = w.heads.get(&name).copied();
        let delete = r != 0 && head.is_some_and(|h| w.live(h)) && input.one_in(6);
        let (value, deps) = if delete {
            (RefValue::Deleted, Deps::default())
        } else {
            let deps = if r == 0 {
                // Trunk: its parent, or (forced) an older trunk version or
                // nothing (orphan history).
                let older: Vec<RefVersionId> = w
                    .versions
                    .values()
                    .filter(|v| v.name == names[0])
                    .map(|v| v.id)
                    .collect();
                let dep = match input.below(8) {
                    0 => None,
                    1 if !older.is_empty() => Some(older[input.below(older.len())]),
                    _ => trunk,
                };
                Deps {
                    trunk: dep,
                    scoped_old: None,
                }
            } else {
                Deps {
                    trunk,
                    scoped_old: head.filter(|h| w.live(*h) && input.flag()),
                }
            };
            let mut objects = BTreeSet::new();
            let tip = w.object();
            objects.insert(tip);
            for _ in 0..input.below(3) {
                let o = w.object();
                objects.insert(o);
            }
            // Repeat some objects of earlier packs (completion bases and
            // resends repeat objects across packs).
            let earlier: Vec<ObjectId> = w.objects.values().flatten().copied().collect();
            for _ in 0..input.below(3) {
                if !earlier.is_empty() {
                    objects.insert(earlier[input.below(earlier.len())]);
                }
            }
            let pack = match input.below(8) {
                // No pack: a push whose objects the deps already hold.
                0 if deps.trunk.is_some() || deps.scoped_old.is_some() => None,
                // A pack another version already names.
                1 if !w.packs.is_empty() => {
                    let push_packs: Vec<PackId> =
                        w.versions.values().filter_map(|v| v.value.pack()).collect();
                    if push_packs.is_empty() {
                        Some(w.add_pack(objects, None))
                    } else {
                        Some(push_packs[input.below(push_packs.len())])
                    }
                }
                _ => Some(w.add_pack(objects, None)),
            };
            let closure = if input.one_in(5) {
                Closure::Wider
            } else {
                Closure::Exact
            };
            (RefValue::Live { tip, pack, closure }, deps)
        };
        let id = w.push(&name, value, deps);
        let seq = w.versions[&id].seq;
        w.add_covers(&name, seq, layout, input);
    }
    w
}

/// `planner`: a random history and up to six queries (wants among any live
/// versions, haves through [`HaveVersion::exact_tip`], incremental or
/// full-closure). Each plan must lose no object the client lacks, never
/// send a pack twice, and send exactly the needed versions' objects, or,
/// after a chain skip, at least those and nothing outside the wants'
/// closure (A5's checks). With versions hidden from the view, the planner
/// must ask for them ([`PlanError::MissingVersion`]) or still lose nothing.
pub fn planner(data: &[u8]) {
    planner_reached(data);
}

/// [`planner`], returning the oracle paths the input reached.
pub fn planner_reached(data: &[u8]) -> BTreeSet<Reached> {
    let mut reached = BTreeSet::new();
    let mut input = Input::new(data);
    let mut w = build(&mut input);
    let live: Vec<RefVersionId> = w
        .versions
        .values()
        .filter(|v| v.value.tip().is_some())
        .map(|v| v.id)
        .collect();
    if live.is_empty() {
        return reached;
    }
    if input.one_in(6) {
        for _ in 0..1 + input.below(3) {
            let ids: Vec<RefVersionId> = w.versions.keys().copied().collect();
            w.hidden.insert(ids[input.below(ids.len())]);
        }
    }
    for _ in 0..1 + input.below(6) {
        let wants: Vec<RefVersionId> = (0..input.below(4))
            .map(|_| live[input.below(live.len())])
            .collect();
        let haves: Vec<HaveVersion> = (0..input.below(5))
            .filter_map(|_| {
                let v = &w.versions[&live[input.below(live.len())]];
                HaveVersion::exact_tip(v.value.tip()?, v)
            })
            .collect();
        let mode = if input.one_in(4) {
            PlanMode::FullClosure
        } else {
            PlanMode::Incremental
        };
        let q = PlanInput::new(mode, wants, haves);
        if let Some(path) = check(&w, &q) {
            reached.insert(path);
        }
    }
    reached
}

/// Checks one plan; the path it took, if it is one [`Reached`] names.
fn check(w: &World, q: &PlanInput) -> Option<Reached> {
    let result = CoverPlanner.plan_with_stats(w, q);
    if q.wants().is_empty() {
        assert_eq!(result.err(), Some(PlanError::NoWants));
        return Some(Reached::RefusedNoWants);
    }
    let (plan, stats) = match result {
        Ok(r) => r,
        Err(PlanError::MissingVersion(v)) => {
            assert!(
                w.hidden.contains(&v),
                "the planner asked for {v}, which is loaded"
            );
            return Some(Reached::MissingVersion);
        }
        Err(e) => panic!("plan failed on a complete graph: {e}"),
    };
    assert_eq!(stats.packs, plan.packs.len());
    let unique: BTreeSet<PackId> = plan.packs.iter().copied().collect();
    assert_eq!(unique.len(), plan.packs.len(), "a pack is planned twice");
    let sent: BTreeSet<ObjectId> = plan
        .packs
        .iter()
        .flat_map(|p| w.objects[p].iter().copied())
        .collect();
    let held = w.dep_closure(q.haves().iter().map(|h| h.version()));
    let wanted = w.dep_closure(q.wants().iter().copied());
    let lacking: BTreeSet<ObjectId> = w
        .objects_of_versions(&wanted)
        .difference(&w.objects_of_versions(&held))
        .copied()
        .collect();
    let lost: Vec<&ObjectId> = lacking.difference(&sent).collect();
    assert!(
        lost.is_empty(),
        "{} objects the client lacks are in no planned pack",
        lost.len()
    );
    if !w.hidden.is_empty() {
        return None;
    }
    let needed: BTreeSet<RefVersionId> = wanted.difference(&held).copied().collect();
    let expected = w.objects_of_versions(&needed);
    if stats.chain_skips == 0 {
        assert_eq!(
            sent, expected,
            "the plan's objects differ from the needed versions' packs"
        );
        Some(Reached::ExactAccept)
    } else {
        assert!(
            sent.is_superset(&expected),
            "after a chain skip, needed objects are missing"
        );
        assert!(
            sent.is_subset(&w.objects_of_versions(&wanted)),
            "after a chain skip, objects outside the wants' closure are planned"
        );
        None
    }
}

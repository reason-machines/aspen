//! An in-memory [`VersionGraph`], plus a builder for hand-written histories.

use std::collections::{BTreeMap, BTreeSet};

use aspen_core::fetch::VersionGraph;
use aspen_core::id::{
    CoverId, ObjectId, PackChecksum, PackId, RefVersionId, RepoId, Seq, UnixMicros,
};
use aspen_core::meta::{Cover, CoverChain, SeqRange, VerifiedRange};
use aspen_core::name::RefName;
use aspen_core::pack::{CoverLevel, NewPack, PackOrigin, PackRecord, entry_table_len};
use aspen_core::refs::{Closure, Deps, RefValue, RefVersion};

pub(crate) fn repo() -> RepoId {
    RepoId::new(1).unwrap()
}

pub(crate) fn name(s: &str) -> RefName {
    RefName::new(s).unwrap()
}

/// A graph that holds every version, cover and pack row, plus each pack's
/// objects, which the planner never sees but the checks need.
#[derive(Debug, Default)]
pub(crate) struct Graph {
    pub versions: BTreeMap<RefVersionId, RefVersion>,
    pub covers: BTreeMap<RefName, Vec<Cover>>,
    pub packs: BTreeMap<PackId, PackRecord>,
    pub objects: BTreeMap<PackId, BTreeSet<ObjectId>>,
    /// Current version of each ref.
    pub heads: BTreeMap<RefName, RefVersionId>,
    next_version: i64,
    next_pack: i64,
    next_cover: i64,
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

impl Graph {
    /// Registers a pack holding `objects`; `None` for an empty set (a push
    /// that brought no objects stores no pack).
    pub(crate) fn add_pack(
        &mut self,
        objects: BTreeSet<ObjectId>,
        level: Option<CoverLevel>,
    ) -> Option<PackId> {
        if objects.is_empty() {
            return None;
        }
        self.next_pack += 1;
        let id = PackId::new(self.next_pack).unwrap();
        let mut checksum = [0u8; 20];
        checksum[..8].copy_from_slice(&self.next_pack.to_be_bytes());
        let count = u32::try_from(objects.len()).unwrap();
        // The planner reads only that the row exists; the sizes are
        // plausible, not measured.
        let pack = NewPack {
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
        };
        self.packs.insert(
            id,
            PackRecord {
                id,
                pack,
                generation: aspen_core::pack::PackGeneration::FIRST,
                created_at: UnixMicros(0),
            },
        );
        self.objects.insert(id, objects);
        Some(id)
    }

    /// Appends a version to `ref_name`'s chain and makes it the head.
    pub(crate) fn push(&mut self, ref_name: &RefName, value: RefValue, deps: Deps) -> RefVersionId {
        self.next_version += 1;
        let id = RefVersionId::new(self.next_version).unwrap();
        let parent = self.heads.get(ref_name).copied();
        let seq = match parent {
            None => Seq::FIRST,
            Some(p) => self.versions[&p].seq.next().unwrap(),
        };
        self.versions.insert(
            id,
            RefVersion {
                id,
                repo: repo(),
                name: ref_name.clone(),
                seq,
                parent,
                value,
                deps,
                created_at: UnixMicros(0),
            },
        );
        self.heads.insert(ref_name.clone(), id);
        id
    }

    /// The version of `ref_name` with sequence number `seq`.
    pub(crate) fn at(&self, ref_name: &RefName, seq: u64) -> &RefVersion {
        self.versions
            .values()
            .find(|v| &v.name == ref_name && v.seq.get() == seq)
            .unwrap_or_else(|| panic!("{ref_name} has no version {seq}"))
    }

    /// Adds a cover over `first..=last` of `ref_name`, without a chain.
    /// When `compacted`, its pack holds exactly the objects of the range's
    /// packs, which is the contract the planner relies on from compaction
    /// (C1).
    pub(crate) fn add_cover(
        &mut self,
        ref_name: &RefName,
        level: u8,
        first: u64,
        last: u64,
        compacted: bool,
    ) {
        self.add_cover_with(ref_name, level, first, last, compacted, Chain::None);
    }

    /// Adds a cover as [`Graph::add_cover`] does, with a chain as `chain`
    /// says; returns the chain recorded.
    pub(crate) fn add_cover_with(
        &mut self,
        ref_name: &RefName,
        level: u8,
        first: u64,
        last: u64,
        compacted: bool,
        chain: Chain,
    ) -> Option<CoverChain> {
        let level = CoverLevel::new(level).unwrap();
        let range = SeqRange::new(Seq::new(first).unwrap(), Seq::new(last).unwrap()).unwrap();
        let chain = match chain {
            Chain::None => None,
            Chain::Verified => self.verify(ref_name, range).ok().map(|r| r.chain()),
            Chain::Forged => Some(CoverChain::recorded(self.at(ref_name, first).id)),
        };
        let pack = if compacted {
            let objects: BTreeSet<ObjectId> = self
                .versions
                .values()
                .filter(|v| &v.name == ref_name && range.contains(v.seq))
                .filter_map(|v| v.value.pack())
                .flat_map(|p| self.objects[&p].iter().copied())
                .collect();
            self.add_pack(objects, Some(level))
        } else {
            None
        };
        self.next_cover += 1;
        let covers = self.covers.entry(ref_name.clone()).or_default();
        covers.push(Cover {
            id: CoverId::new(self.next_cover).unwrap(),
            repo: repo(),
            name: ref_name.clone(),
            level,
            range,
            pack,
            chain,
        });
        covers.sort_by_key(|c| (c.level, c.range.first()));
        chain
    }

    /// [`VerifiedRange::verify`] over the graph's versions of `ref_name` in
    /// `range`, the way the compactor builds a chain.
    pub(crate) fn verify(
        &self,
        ref_name: &RefName,
        range: SeqRange,
    ) -> Result<VerifiedRange, aspen_core::meta::ChainError> {
        let mut versions: Vec<RefVersion> = self
            .versions
            .values()
            .filter(|v| &v.name == ref_name && range.contains(v.seq))
            .cloned()
            .collect();
        versions.sort_by_key(|v| v.seq);
        VerifiedRange::verify(ref_name, range, &versions)
    }

    /// Every version `ids` reach through dependencies, themselves included.
    pub(crate) fn dep_closure(
        &self,
        ids: impl IntoIterator<Item = RefVersionId>,
    ) -> BTreeSet<RefVersionId> {
        let mut seen = BTreeSet::new();
        let mut stack: Vec<_> = ids.into_iter().collect();
        while let Some(id) = stack.pop() {
            if seen.insert(id) {
                stack.extend(self.versions[&id].deps.iter());
            }
        }
        seen
    }

    /// The objects of the versions' packs.
    pub(crate) fn objects_of_versions(&self, ids: &BTreeSet<RefVersionId>) -> BTreeSet<ObjectId> {
        ids.iter()
            .filter_map(|id| self.versions[id].value.pack())
            .flat_map(|p| self.objects[&p].iter().copied())
            .collect()
    }

    /// The objects of `packs`.
    pub(crate) fn objects_of_packs(&self, packs: &[PackId]) -> BTreeSet<ObjectId> {
        packs
            .iter()
            .flat_map(|p| self.objects[p].iter().copied())
            .collect()
    }
}

/// Whether a new cover carries a chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Chain {
    /// No chain.
    None,
    /// A chain exactly when [`VerifiedRange::verify`] accepts the range.
    Verified,
    /// A chain whatever the range is ([`CoverChain::recorded`]): what a
    /// store bug or a forged row would give the planner.
    Forged,
}

/// A live value.
pub(crate) fn live(tip: ObjectId, pack: Option<PackId>, closure: Closure) -> RefValue {
    RefValue::Live { tip, pack, closure }
}

/// Dependencies.
pub(crate) fn deps(trunk: Option<RefVersionId>, scoped_old: Option<RefVersionId>) -> Deps {
    Deps { trunk, scoped_old }
}

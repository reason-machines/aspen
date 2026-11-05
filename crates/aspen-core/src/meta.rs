//! The metadata store contract: repositories, refs and their versions,
//! packs, the commit index, compaction covers and the job queue.
//!
//! `aspen-meta` implements it on Postgres and SQLite with schema v1
//! (`crates/aspen-meta/migrations/`, described in
//! `crates/aspen-meta/SCHEMA.md`).

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use crate::error::StoreError;
use crate::id::PackChecksum;
use crate::id::{CoverId, JobId, ObjectId, PackId, RefVersionId, RepoId, Seq, UnixMicros};
use crate::ingest::SortedIds;
use crate::name::{RefName, RepoName};
use crate::pack::{CoverLevel, NewPack, PackGeneration, PackKeys, PackRecord};
use crate::refs::{Closure, Deps, RefValue, RefVersion, Verdict, VerdictKind};
use crate::repo::{Repo, RepoSettings};

/// One `commit_index` row: what the fast-forward check and `shallow-info`
/// need about a commit, without reading packs (S5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRow {
    /// The commit.
    pub commit: ObjectId,
    /// A ref version whose **tip reaches** the commit, so the version's
    /// closure holds the commit and all its history: read the commit from
    /// that version's pack, then its dependencies, when needed.
    ///
    /// Usually the version is a descendant of the commit, not the commit
    /// itself. That is safe for resolving a want (send `closure(V)`, a
    /// superset), but never for a have: subtracting `closure(V)` would drop
    /// the objects between the commit and `V`'s tip. Haves go through
    /// [`crate::fetch::HaveVersion`].
    pub ref_version: RefVersionId,
    /// Parent IDs, in commit order.
    pub parents: Vec<ObjectId>,
    /// Committer time, seconds since the Unix epoch.
    pub committer_time: i64,
}

/// The answer for one commit from the commit index.
///
/// Rows for the tip are written with the push; rows for the rest of a large
/// push are written later by a job. `NotIndexed` therefore means "not known
/// yet", never "no parents" or "not an ancestor": the caller reads the
/// commit from the packs of the ref version its walk came from, and fails
/// with a retryable error if it is not there either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitLookup {
    /// The row.
    Indexed(CommitRow),
    /// No row yet.
    NotIndexed,
}

/// A commit index row for the version a [`RefWrite`] creates, whose ID the
/// store assigns inside [`MetaStore::commit_push`]. The commit must be
/// reachable from the write's tip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCommit {
    /// The commit.
    pub commit: ObjectId,
    /// Parent IDs, in commit order.
    pub parents: Vec<ObjectId>,
    /// Committer time, seconds since the Unix epoch.
    pub committer_time: i64,
}

/// The kinds of job that name a ref version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum VersionJobKind {
    /// [`JobKind::IndexCommits`].
    IndexCommits,
    /// [`JobKind::Compact`].
    Compact,
    /// [`JobKind::MirrorPush`].
    MirrorPush,
}

impl VersionJobKind {
    /// The job for one version.
    pub const fn for_version(self, version: RefVersionId) -> JobKind {
        match self {
            Self::IndexCommits => JobKind::IndexCommits { version },
            Self::Compact => JobKind::Compact { version },
            Self::MirrorPush => JobKind::MirrorPush { version },
        }
    }
}

/// A job about the version a [`RefWrite`] creates, enqueued in the same
/// transaction once the store knows the version's ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionJob {
    /// What to do.
    pub kind: VersionJobKind,
    /// Not before this time.
    pub run_after: UnixMicros,
}

/// One ref write of a push's commit transaction: the new version, plus the
/// commit index rows and jobs that name it. The version's ID does not exist
/// until the store inserts it, so rows and jobs are attached to the write
/// and the store fills the ID in ([`PushCommit::rows`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefWrite {
    /// The ref.
    pub name: RefName,
    /// The compare-and-swap expectation: the `refs` row must point at
    /// exactly this version (`None`: no row). Always the verdict's
    /// [`Verdict::against`].
    pub expected: Option<RefVersionId>,
    /// The new value.
    pub value: RefValue,
    /// The pack's dependencies.
    pub deps: Deps,
    /// Commit index rows for commits the new version's tip reaches; at
    /// least the tip when it is a commit the index lacks. Empty for a
    /// deletion.
    pub commits: Vec<NewCommit>,
    /// Jobs about the new version (for example indexing its non-tip
    /// commits). Deletions may carry only [`VersionJobKind::MirrorPush`].
    pub jobs: Vec<VersionJob>,
}

impl RefWrite {
    /// The write for an accepted verdict, or `None` for a no-op or a
    /// rejection. `pack` is **this write's** stored pack, if any, and
    /// `closure` comes from [`Closure::of_new_version`] for this write's own
    /// tip and pack; both are ignored for a delete, which also drops `deps`.
    /// Add commit rows and jobs to the result. The pack-reachability walk
    /// behind that closure covers the objects the client sent; completion
    /// bases must be excluded from it (they are in the dependencies'
    /// closures already).
    ///
    /// Each write names its own pack, so a multi-ref push may store one
    /// pack per write (several writes may also name the same pack when it
    /// is byte-identical, for example two branches at one new commit). B1
    /// splits every multi-ref push this way; see the decisions log.
    pub fn from_verdict(
        verdict: &Verdict,
        pack: Option<PackId>,
        deps: Deps,
        closure: Closure,
    ) -> Option<Self> {
        if !verdict.kind.writes() {
            return None;
        }
        let (value, deps) = match verdict.kind {
            VerdictKind::DeleteCas | VerdictKind::DeleteUnconditional => {
                (RefValue::Deleted, Deps::default())
            }
            _ => (
                RefValue::Live {
                    tip: verdict.new,
                    pack,
                    closure,
                },
                deps,
            ),
        };
        Some(Self {
            name: verdict.name.clone(),
            expected: verdict.against,
            value,
            deps,
            commits: Vec::new(),
            jobs: Vec::new(),
        })
    }
}

/// Why a [`PushCommit`] is malformed. These are bugs in the push path, not
/// client errors; the store refuses the transaction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PushCommitError {
    /// No writes.
    #[error("a push commit needs at least one write")]
    Empty,
    /// Two writes for one ref.
    #[error("two writes for {0}")]
    DuplicateRef(RefName),
    /// A live value with the zero ID.
    #[error("{0}: the tip is the zero ID")]
    ZeroTip(RefName),
    /// A live value with no pack and no dependency: its objects are nowhere
    /// (schema v1 `ref_versions_connected`).
    #[error("{0}: a live version needs a pack or a dependency")]
    Disconnected(RefName),
    /// A deletion with commit rows or an indexing or compaction job.
    #[error("{0}: a deletion has no commits to index or compact")]
    NothingToIndex(RefName),
    /// A deletion with a dependency. A deletion's closure is empty, so a
    /// dependency would mean nothing (schema v1
    /// `ref_versions_deletion_no_deps`).
    #[error("{0}: a deletion has no dependencies")]
    DeletionWithDeps(RefName),
    /// [`PushCommit::rows`] got the wrong number of version IDs.
    #[error("{expected} writes but {got} version IDs")]
    VersionCount {
        /// Writes in the push.
        expected: usize,
        /// IDs given.
        got: usize,
    },
}

/// A push's commit transaction: insert one ref version per write, move each
/// `refs` row with compare-and-swap, write each write's commit rows and
/// enqueue its jobs, atomically. A non-atomic push commits each ref on its
/// own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushCommit {
    /// Repository.
    pub repo: RepoId,
    /// Ref writes. The store takes ref rows in name order to avoid deadlocks.
    pub writes: Vec<RefWrite>,
}

/// The rows a [`PushCommit`] writes besides versions and refs, once the
/// versions' IDs are known.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PushRows {
    /// Commit index rows.
    pub commits: Vec<CommitRow>,
    /// Jobs to enqueue (idempotently, as [`MetaStore::enqueue_jobs`]).
    pub jobs: Vec<NewJob>,
}

impl PushCommit {
    /// Checks the rules the store relies on. Stores call this first and
    /// refuse a malformed commit before writing anything.
    pub fn validate(&self) -> Result<(), PushCommitError> {
        if self.writes.is_empty() {
            return Err(PushCommitError::Empty);
        }
        let mut names = BTreeSet::new();
        for w in &self.writes {
            if !names.insert(&w.name) {
                return Err(PushCommitError::DuplicateRef(w.name.clone()));
            }
            match w.value {
                RefValue::Live { tip, .. } if tip.is_zero() => {
                    return Err(PushCommitError::ZeroTip(w.name.clone()));
                }
                RefValue::Live { pack: None, .. } if w.deps.iter().next().is_none() => {
                    return Err(PushCommitError::Disconnected(w.name.clone()));
                }
                RefValue::Live { .. } => {}
                RefValue::Deleted => {
                    if w.deps.iter().next().is_some() {
                        return Err(PushCommitError::DeletionWithDeps(w.name.clone()));
                    }
                    if !w.commits.is_empty()
                        || w.jobs.iter().any(|j| j.kind != VersionJobKind::MirrorPush)
                    {
                        return Err(PushCommitError::NothingToIndex(w.name.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    /// The commit rows and jobs of every write, given the IDs the store
    /// assigned to the new versions, in `writes` order.
    pub fn rows(&self, created: &[RefVersionId]) -> Result<PushRows, PushCommitError> {
        if created.len() != self.writes.len() {
            return Err(PushCommitError::VersionCount {
                expected: self.writes.len(),
                got: created.len(),
            });
        }
        let mut rows = PushRows::default();
        for (w, &version) in self.writes.iter().zip(created) {
            rows.commits.extend(w.commits.iter().map(|c| CommitRow {
                commit: c.commit,
                ref_version: version,
                parents: c.parents.clone(),
                committer_time: c.committer_time,
            }));
            rows.jobs.extend(w.jobs.iter().map(|j| NewJob {
                repo: self.repo,
                kind: j.kind.for_version(version),
                run_after: j.run_after,
            }));
        }
        Ok(rows)
    }
}

/// Outcome of [`MetaStore::commit_push`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Everything was written; the new versions, in `writes` order.
    Committed(Vec<RefVersion>),
    /// A ref was not at its expected version; nothing was written. The
    /// caller recomputes the verdict against `current` (up to
    /// [`crate::refs::CAS_RECOMPUTE_LIMIT`] times) or answers with a
    /// retryable `ng`.
    CasLost {
        /// The ref that moved.
        name: RefName,
        /// Its version now.
        current: Option<RefVersionId>,
    },
}

/// A page of `ls-refs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefListQuery {
    /// Only refs starting with one of these (`ref-prefix`); empty means all.
    pub prefixes: Vec<String>,
    /// Only refs after this name (keyset pagination).
    pub after: Option<RefName>,
    /// At most this many refs.
    pub limit: u32,
}

/// Sequence numbers of the earlier versions to advertise as `.have` lines,
/// log-spaced back from `current`: 1, 2, 4, 8, ... versions earlier, at
/// most `n` of them, none below 1 (S2).
pub fn have_tip_seqs(current: Seq, n: u8) -> Vec<Seq> {
    (0..u32::from(n))
        .map_while(|i| 1u64.checked_shl(i))
        .map_while(|back| current.back(back))
        .collect()
}

/// An inclusive range of one ref's version numbers, `first <= last`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeqRange {
    first: Seq,
    last: Seq,
}

impl SeqRange {
    /// The range; `None` if `first > last` (schema v1 `covers_seq_order`).
    pub fn new(first: Seq, last: Seq) -> Option<Self> {
        (first <= last).then_some(Self { first, last })
    }

    /// First version in the range.
    pub fn first(&self) -> Seq {
        self.first
    }

    /// Last version in the range.
    pub fn last(&self) -> Seq {
        self.last
    }

    /// Whether `seq` is in the range.
    pub fn contains(&self, seq: Seq) -> bool {
        self.first <= seq && seq <= self.last
    }
}

/// The depth of a shallow fetch whose exact pack Aspen caches (node C8): a
/// `deepen <n>` with `n` from 1 to [`ShallowDepth::MAX`]. Git's
/// `deepen 2147483647` (`--unshallow`, `INFINITE_DEPTH`) is not a depth
/// and is never cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShallowDepth(u32);

impl ShallowDepth {
    /// The deepest depth there is: one below Git's `INFINITE_DEPTH`.
    pub const MAX: u32 = 0x7fff_fffe;

    /// A depth; `None` outside `1..=MAX` (migration 0006
    /// `shallow_packs_depth_range`).
    pub const fn new(depth: u32) -> Option<Self> {
        if depth >= 1 && depth <= Self::MAX {
            Some(Self(depth))
        } else {
            None
        }
    }

    /// The number of commits deep.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for ShallowDepth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// What [`MetaStore::set_shallow_pack`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShallowRecorded {
    /// The version and depth name this pack: the one just recorded, or the
    /// first recording's when there was one.
    Recorded {
        /// The recorded pack's row.
        pack: PackRecord,
        /// Recordings the repository's byte cap evicted to make room.
        evicted: u64,
    },
    /// The pack alone is larger than the repository's cap: nothing was
    /// recorded or evicted, and the fetch is served the full closure.
    OverLimit {
        /// The pack's [`NewPack::stored_bytes`].
        bytes: u64,
        /// The cap it exceeds.
        limit: u64,
    },
}

/// A `packs` row that garbage collection deleted and whose blobs it has
/// not yet finished deleting ([`MetaStore::sweep_orphan_packs`]). The
/// generation names the deleted row's keys; a later registration of the
/// same checksum gets a later generation and other keys, so these deletes
/// are safe to run, and to run again, at any time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackTombstone {
    /// Repository.
    pub repo: RepoId,
    /// The deleted row's checksum.
    pub checksum: PackChecksum,
    /// The deleted row's generation.
    pub generation: PackGeneration,
}

impl PackTombstone {
    /// The blobs to delete.
    pub fn keys(&self) -> PackKeys {
        PackKeys::new(self.repo, &self.checksum, self.generation)
    }
}

/// A compaction cover: a range of one ref's versions, repacked (once `pack`
/// is set) into one self-contained, base-first compaction pack at the
/// cover's level.
///
/// A cover follows the ref's version history (`seq`), while a version's
/// closure follows its dependencies, so a cover's objects need not be in the
/// closure of its last version. A fetch may treat a cover's range as held
/// for a have only under the rule on [`crate::fetch::HaveVersion`]: the
/// range must carry a [`CoverChain`] (node C1, migration 0005).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cover {
    /// Row ID.
    pub id: CoverId,
    /// Repository.
    pub repo: RepoId,
    /// The ref whose version chain this covers.
    pub name: RefName,
    /// Level 1 to 4.
    pub level: CoverLevel,
    /// The covered versions.
    pub range: SeqRange,
    /// The repacked pack, at `level`; `None` until compacted, and for good
    /// when no version in the range has a pack (or the repack would be
    /// byte-identical to a pack that already exists; see `aspen-compact`).
    pub pack: Option<PackId>,
    /// Set when the range is a verified dependency chain: what lets the
    /// planner skip the range on the have side.
    pub chain: Option<CoverChain>,
}

/// A cover to create ([`MetaStore::insert_cover`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCover {
    /// Repository.
    pub repo: RepoId,
    /// The ref whose version chain this covers.
    pub name: RefName,
    /// Level 1 to 4.
    pub level: CoverLevel,
    /// The covered versions.
    pub range: SeqRange,
    /// The range's chain, from [`VerifiedRange::verify`] or
    /// [`VerifiedRange::join`] over exactly `name` and `range`; `None` when
    /// the range is not a dependency chain.
    pub chain: Option<CoverChain>,
}

/// Proof that a cover's range is a **dependency chain**: every version in
/// the range after the first has its parent (the same ref's version at
/// `seq - 1`) among its dependencies.
///
/// A version's closure includes the closures of its dependencies, so in a
/// dependency chain each version's closure contains the closure of every
/// earlier version in the range. A client that holds the closure of the
/// version at `seq = s` inside the range therefore holds the closure of
/// every version from the range's first up to `s`. That is the have-side
/// skip of the planner: it needs no ancestry and no exactness, only the
/// dependency edges, which are immutable once committed (decisions log,
/// "C1: cover chains").
///
/// A forced branch update pushed through the plain URL depends on trunk
/// only, not on its parent, and B1's forced-trunk rule gives a forced trunk
/// update a dependency other than its parent, so both break a chain, which
/// is what the cover-subtraction rule requires.
///
/// The chain records the ID of the range's first version, where a skip
/// lands. Build one with [`VerifiedRange::verify`] or
/// [`VerifiedRange::join`]; [`CoverChain::recorded`] is only for a store
/// reading back a chain one of those built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CoverChain {
    first_version: RefVersionId,
}

impl CoverChain {
    /// A chain read back from the metadata store (`cover_chains`, migration
    /// 0005, whose foreign key ties `first_version` to the cover's ref and
    /// `first_seq`). Not a way to assert a chain: only rows written from a
    /// [`VerifiedRange`] exist.
    pub const fn recorded(first_version: RefVersionId) -> Self {
        Self { first_version }
    }

    /// The version at the range's first `seq`.
    pub const fn first_version(&self) -> RefVersionId {
        self.first_version
    }
}

/// A range of one ref's versions checked to be a dependency chain
/// ([`CoverChain`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedRange {
    name: RefName,
    range: SeqRange,
    chain: CoverChain,
}

/// Why a range is not a verified dependency chain.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChainError {
    /// The versions given are not exactly the range's versions of the ref,
    /// in order.
    #[error("the versions do not match {name} {first}..={last}")]
    WrongVersions {
        /// The ref.
        name: RefName,
        /// First seq of the range.
        first: Seq,
        /// Last seq of the range.
        last: Seq,
    },
    /// A version's parent is not among its dependencies.
    #[error("{name} version {seq} does not depend on its parent")]
    Broken {
        /// The ref.
        name: RefName,
        /// The version that breaks the chain.
        seq: Seq,
    },
    /// The two ranges given to [`VerifiedRange::join`] are not adjacent
    /// ranges of one ref, or the junction is not the upper range's first
    /// version.
    #[error("the ranges and the junction version do not fit together")]
    NotAdjacent,
}

impl VerifiedRange {
    /// Checks that `versions` are exactly the versions of `name` with `seq`
    /// in `range`, in `seq` order, with consistent parent links, and that
    /// each one after the first depends on its parent.
    pub fn verify(
        name: &RefName,
        range: SeqRange,
        versions: &[RefVersion],
    ) -> Result<Self, ChainError> {
        let wrong = || ChainError::WrongVersions {
            name: name.clone(),
            first: range.first(),
            last: range.last(),
        };
        let span = range.last().get() - range.first().get() + 1;
        if versions.len() as u64 != span {
            return Err(wrong());
        }
        let mut prev: Option<&RefVersion> = None;
        for (i, v) in versions.iter().enumerate() {
            if &v.name != name || v.seq.get() != range.first().get() + i as u64 {
                return Err(wrong());
            }
            if let Some(p) = prev {
                if v.parent != Some(p.id) || v.repo != p.repo {
                    return Err(wrong());
                }
                if !v.deps.iter().any(|d| d == p.id) {
                    return Err(ChainError::Broken {
                        name: name.clone(),
                        seq: v.seq,
                    });
                }
            }
            prev = Some(v);
        }
        Ok(Self {
            name: name.clone(),
            range,
            chain: CoverChain {
                first_version: versions[0].id,
            },
        })
    }

    /// The verified range of a cover that carries a chain.
    pub fn of_cover(cover: &Cover) -> Option<Self> {
        Some(Self {
            name: cover.name.clone(),
            range: cover.range,
            chain: cover.chain?,
        })
    }

    /// Joins this range with the adjacent range above it: the result is a
    /// chain from this range's first version to `upper`'s last. `junction`
    /// is `upper`'s first version, read from the store: the join holds when
    /// it depends on its parent, which is this range's last version.
    pub fn join(self, upper: &VerifiedRange, junction: &RefVersion) -> Result<Self, ChainError> {
        let adjacent = upper.name == self.name
            && self.range.last().next() == Some(upper.range.first())
            && junction.name == upper.name
            && junction.seq == upper.range.first()
            && junction.id == upper.chain.first_version;
        if !adjacent {
            return Err(ChainError::NotAdjacent);
        }
        match junction.parent {
            Some(p) if junction.deps.iter().any(|d| d == p) => {}
            _ => {
                return Err(ChainError::Broken {
                    name: self.name,
                    seq: junction.seq,
                });
            }
        }
        Ok(Self {
            range: SeqRange {
                first: self.range.first(),
                last: upper.range.last(),
            },
            ..self
        })
    }

    /// The ref.
    pub fn name(&self) -> &RefName {
        &self.name
    }

    /// The range.
    pub fn range(&self) -> SeqRange {
        self.range
    }

    /// The chain, to record on the cover.
    pub fn chain(&self) -> CoverChain {
        self.chain
    }
}

/// Where covers go: level `k` spans `fanout^k` versions of one ref, aligned,
/// so the cover at level `k` that ends at `seq = s` exists exactly when `s`
/// is a multiple of `fanout^k`, and covers `s - fanout^k + 1 ..= s`.
///
/// The push path (B1) enqueues a `compact` job for a version whose `seq`
/// ends a level-1 cover, and the compactor (`aspen-compact`) builds every
/// cover that ends there. Both must use the same layout; the default
/// fanout is 16, so the four levels span 16, 256, 4,096 and 65,536
/// versions, and a fetch of N versions of one ref sends at most about
/// `15 × 4 + N / 65,536` cover packs plus the newest versions' own packs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverLayout {
    fanout: u64,
}

impl Default for CoverLayout {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl CoverLayout {
    /// Fanout 16.
    pub const DEFAULT: Self = Self { fanout: 16 };

    /// A layout with `fanout` (2 to 1,024); `None` outside that range.
    pub const fn new(fanout: u64) -> Option<Self> {
        if fanout >= 2 && fanout <= 1024 {
            Some(Self { fanout })
        } else {
            None
        }
    }

    /// The fanout.
    pub const fn fanout(&self) -> u64 {
        self.fanout
    }

    /// Versions a cover at `level` spans.
    pub fn span(&self, level: CoverLevel) -> u64 {
        self.fanout.pow(u32::from(level.get()))
    }

    /// Whether `seq` ends a level-1 cover: the versions whose push enqueues
    /// a `compact` job.
    pub fn ends_a_cover(&self, seq: Seq) -> bool {
        seq.get().is_multiple_of(self.fanout)
    }

    /// The covers that end at `seq`, lowest level first.
    pub fn ending_at(&self, seq: Seq) -> Vec<(CoverLevel, SeqRange)> {
        (1..=crate::pack::MAX_LEVEL)
            .filter_map(CoverLevel::new)
            .map_while(|level| {
                let span = self.span(level);
                seq.get().is_multiple_of(span).then(|| {
                    let first = Seq::new(seq.get() - span + 1).expect("multiple of span");
                    (level, SeqRange::new(first, seq).expect("ordered"))
                })
            })
            .collect()
    }

    /// The covers one level below `level` that tile `range` (a range at
    /// `level` from [`CoverLayout::ending_at`]), in `seq` order; empty at
    /// level 1.
    pub fn parts(&self, level: CoverLevel, range: SeqRange) -> Vec<(CoverLevel, SeqRange)> {
        let Some(below) = CoverLevel::new(level.get() - 1) else {
            return Vec::new();
        };
        let span = self.span(below);
        let mut out = Vec::new();
        let mut first = range.first().get();
        while first <= range.last().get() {
            let last = (first + span - 1).min(range.last().get());
            let r = SeqRange::new(
                Seq::new(first).expect("in range"),
                Seq::new(last).expect("in range"),
            )
            .expect("ordered");
            out.push((below, r));
            first = last + 1;
        }
        out
    }
}

/// Background job kinds. Every job names the ref version or pack it is
/// about; the payload columns of `jobs` hold exactly that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobKind {
    /// Write `commit_index` rows for a version's non-tip commits, from a
    /// second traversal of its pack (S4).
    IndexCommits {
        /// The version.
        version: RefVersionId,
    },
    /// Consider building covers after this version.
    Compact {
        /// The version.
        version: RefVersionId,
    },
    /// Push this version to the upstream in mirror mode.
    MirrorPush {
        /// The version.
        version: RefVersionId,
    },
    /// Rewrite a stored pack that broke the base-first invariant (the
    /// fetch path found a forward base and paged).
    RewritePack {
        /// The pack.
        pack: PackId,
    },
}

/// The kind of a job without its payload; the `jobs.kind` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobTag {
    /// [`JobKind::IndexCommits`].
    IndexCommits,
    /// [`JobKind::Compact`].
    Compact,
    /// [`JobKind::MirrorPush`].
    MirrorPush,
    /// [`JobKind::RewritePack`].
    RewritePack,
}

impl JobTag {
    /// Every tag, in the order of schema v1's `CHECK` list.
    pub const ALL: [JobTag; 4] = [
        Self::IndexCommits,
        Self::Compact,
        Self::MirrorPush,
        Self::RewritePack,
    ];

    /// The value stored in `jobs.kind`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IndexCommits => "index_commits",
            Self::Compact => "compact",
            Self::MirrorPush => "mirror_push",
            Self::RewritePack => "rewrite_pack",
        }
    }
}

crate::repo::from_str_via_all!(JobTag, "jobs.kind");

impl JobKind {
    /// The tag.
    pub const fn tag(&self) -> JobTag {
        match self {
            Self::IndexCommits { .. } => JobTag::IndexCommits,
            Self::Compact { .. } => JobTag::Compact,
            Self::MirrorPush { .. } => JobTag::MirrorPush,
            Self::RewritePack { .. } => JobTag::RewritePack,
        }
    }
}

/// A job to enqueue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewJob {
    /// Repository.
    pub repo: RepoId,
    /// What to do.
    pub kind: JobKind,
    /// Not before this time.
    pub run_after: UnixMicros,
}

/// Attempts after which a failing job is parked as dead.
pub const MAX_JOB_ATTEMPTS: u32 = 10;

/// A job's state (`jobs.state` plus its lease and schedule columns).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobState {
    /// Waiting to run.
    Queued {
        /// Not before this time.
        run_after: UnixMicros,
    },
    /// Claimed by a worker until its lease ends. A worker that dies leaves
    /// the lease to expire, and another worker may then claim the job, so
    /// job work must be idempotent.
    Running {
        /// Worker ID.
        owner: String,
        /// Lease end.
        lease_until: UnixMicros,
    },
    /// Finished.
    Done,
    /// Failed [`MAX_JOB_ATTEMPTS`] times; needs an operator.
    Dead,
}

impl JobState {
    /// The value stored in `jobs.state`.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Queued { .. } => "queued",
            Self::Running { .. } => "running",
            Self::Done => "done",
            Self::Dead => "dead",
        }
    }

    /// Every stored value, in the order of schema v1's `CHECK` list.
    pub const STORED: [&'static str; 4] = ["queued", "running", "done", "dead"];
}

/// A job row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    /// Row ID.
    pub id: JobId,
    /// Repository.
    pub repo: RepoId,
    /// What to do.
    pub kind: JobKind,
    /// Where it is.
    pub state: JobState,
    /// How many times it has been claimed.
    pub attempts: u32,
    /// The error of the last failed attempt.
    pub last_error: Option<String>,
}

/// Something that happens to a job.
///
/// Every event from a worker names the attempt it holds, so a worker whose
/// lease expired and was reclaimed cannot complete, fail or extend the new
/// attempt, even if it reuses its worker ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobEvent {
    /// A worker claims the job: a due queued job, or a running job whose
    /// lease expired.
    Claim {
        /// Worker ID.
        owner: String,
        /// Current time.
        now: UnixMicros,
        /// Lease length.
        lease: Duration,
    },
    /// The lease holder extends its lease while it is still live.
    Renew {
        /// Worker ID.
        owner: String,
        /// The attempt the worker holds ([`Job::attempts`] when it claimed).
        attempt: u32,
        /// Current time.
        now: UnixMicros,
        /// New lease length, from `now`.
        lease: Duration,
    },
    /// The lease holder finished it.
    Complete {
        /// Worker ID.
        owner: String,
        /// The attempt the worker holds.
        attempt: u32,
    },
    /// The lease holder failed it.
    Fail {
        /// Worker ID.
        owner: String,
        /// The attempt the worker holds.
        attempt: u32,
        /// Current time.
        now: UnixMicros,
        /// What went wrong.
        error: String,
        /// Delay before the next attempt.
        backoff: Duration,
    },
}

/// An event that is not allowed in the job's state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JobTransitionError {
    /// Queued but not due, or running under a live lease.
    #[error("job is not claimable now")]
    NotClaimable,
    /// Only the worker holding the current attempt may renew, complete or
    /// fail a running job.
    #[error("job is not running under this worker's attempt")]
    NotHolder,
    /// A lease is renewed only while it is live; once it has expired another
    /// worker may already have reclaimed the job.
    #[error("lease already expired")]
    LeaseExpired,
    /// Done and dead are final.
    #[error("job is finished")]
    Finished,
}

impl Job {
    /// The job after `event`, following the queue's rules. Stores implement
    /// the same rules in SQL; this is their specification and test oracle.
    ///
    /// Reclaiming an expired lease counts as a new attempt. When the expired
    /// attempt was the [`MAX_JOB_ATTEMPTS`]th, the claim parks the job as
    /// [`JobState::Dead`] instead of handing it out, so a job that keeps
    /// crashing its worker stops. The store's `claim_job` then moves on to
    /// the next job.
    pub fn apply(&self, event: JobEvent) -> Result<Job, JobTransitionError> {
        let mut next = self.clone();
        let holds = |owner: &str, by: &str, attempt: u32| owner == by && attempt == self.attempts;
        match (&self.state, event) {
            (JobState::Done | JobState::Dead, _) => return Err(JobTransitionError::Finished),
            (JobState::Queued { run_after }, JobEvent::Claim { owner, now, lease })
                if now >= *run_after =>
            {
                next.state = JobState::Running {
                    owner,
                    lease_until: now.saturating_add(lease),
                };
                next.attempts += 1;
            }
            (JobState::Running { lease_until, .. }, JobEvent::Claim { owner, now, lease })
                if now >= *lease_until =>
            {
                if self.attempts >= MAX_JOB_ATTEMPTS {
                    next.state = JobState::Dead;
                    next.last_error = Some(format!(
                        "lease of attempt {} expired; parked after {MAX_JOB_ATTEMPTS} attempts",
                        self.attempts
                    ));
                } else {
                    next.state = JobState::Running {
                        owner,
                        lease_until: now.saturating_add(lease),
                    };
                    next.attempts += 1;
                }
            }
            (_, JobEvent::Claim { .. }) => return Err(JobTransitionError::NotClaimable),
            (
                JobState::Running { owner, lease_until },
                JobEvent::Renew {
                    owner: by,
                    attempt,
                    now,
                    lease,
                },
            ) if holds(owner, &by, attempt) => {
                if now >= *lease_until {
                    return Err(JobTransitionError::LeaseExpired);
                }
                next.state = JobState::Running {
                    owner: by,
                    lease_until: now.saturating_add(lease),
                };
            }
            (JobState::Running { owner, .. }, JobEvent::Complete { owner: by, attempt })
                if holds(owner, &by, attempt) =>
            {
                next.state = JobState::Done;
            }
            (
                JobState::Running { owner, .. },
                JobEvent::Fail {
                    owner: by,
                    attempt,
                    now,
                    error,
                    backoff,
                },
            ) if holds(owner, &by, attempt) => {
                next.state = if self.attempts >= MAX_JOB_ATTEMPTS {
                    JobState::Dead
                } else {
                    JobState::Queued {
                        run_after: now.saturating_add(backoff),
                    }
                };
                next.last_error = Some(error);
            }
            (_, JobEvent::Renew { .. } | JobEvent::Complete { .. } | JobEvent::Fail { .. }) => {
                return Err(JobTransitionError::NotHolder);
            }
        }
        Ok(next)
    }
}

/// The metadata store.
///
/// Every method is one short transaction. Methods return `Send` futures so
/// the server can spawn them; implement them with `async fn`. Errors are
/// [`StoreError`]; [`StoreError::Unavailable`] is the retryable one.
pub trait MetaStore: Send + Sync {
    /// Creates a repository. A taken name is [`StoreError::Conflict`].
    fn create_repo(
        &self,
        name: &RepoName,
        settings: &RepoSettings,
    ) -> impl Future<Output = Result<Repo, StoreError>> + Send;

    /// Looks a repository up by name.
    fn repo(
        &self,
        name: &RepoName,
    ) -> impl Future<Output = Result<Option<Repo>, StoreError>> + Send;

    /// Looks a repository up by ID (for jobs, which name only `repo_id`).
    fn repo_by_id(
        &self,
        id: RepoId,
    ) -> impl Future<Output = Result<Option<Repo>, StoreError>> + Send;

    /// The version the `refs` row of `name` points at; `None` if there is no
    /// row. A deleted ref returns its deletion version.
    fn current_version(
        &self,
        repo: RepoId,
        name: &RefName,
    ) -> impl Future<Output = Result<Option<RefVersion>, StoreError>> + Send;

    /// One version by ID.
    fn ref_version(
        &self,
        id: RefVersionId,
    ) -> impl Future<Output = Result<Option<RefVersion>, StoreError>> + Send;

    /// The versions of `name` with these sequence numbers (for `.have` tips,
    /// see [`have_tip_seqs`]); missing ones are left out.
    fn versions_by_seq(
        &self,
        repo: RepoId,
        name: &RefName,
        seqs: &[Seq],
    ) -> impl Future<Output = Result<Vec<RefVersion>, StoreError>> + Send;

    /// Current live versions of refs matching the query, in name order.
    /// Deleted refs are left out.
    fn list_refs(
        &self,
        repo: RepoId,
        query: &RefListQuery,
    ) -> impl Future<Output = Result<Vec<RefVersion>, StoreError>> + Send;

    /// Registers a pack, before its blobs are uploaded: the returned row's
    /// generation fixes the keys to upload to ([`PackRecord::keys`]).
    /// Callers upload after this returns and only then make a version or
    /// cover name the pack. Idempotent: registering the same
    /// `(repo, checksum)` again returns the existing row, and
    /// [`StoreError::Conflict`] if the sizes differ; a writer that gets an
    /// existing row uploads its blobs anyway (the bytes are the same), since
    /// the writer that registered it may have died before uploading.
    ///
    /// A registered pack that no version references is garbage for GC
    /// ([`MetaStore::sweep_orphan_packs`]): every call records the store's
    /// current time as the pack's last registration. A checksum whose
    /// earlier row GC deleted is registered as a new row with the next
    /// [`PackGeneration`], so none of its keys is one a pending deletion
    /// names.
    fn register_pack(
        &self,
        pack: &NewPack,
    ) -> impl Future<Output = Result<PackRecord, StoreError>> + Send;

    /// Pack rows by ID, in the order asked; a missing ID is
    /// [`StoreError::NotFound`].
    fn packs(
        &self,
        ids: &[PackId],
    ) -> impl Future<Output = Result<Vec<PackRecord>, StoreError>> + Send;

    /// Runs a push's commit transaction, after [`PushCommit::validate`]
    /// (a malformed commit is [`StoreError::Conflict`] and writes nothing).
    ///
    /// For each write, in ref-name order, inside one transaction:
    ///
    /// 1. insert the `ref_versions` row with `seq` = the expected version's
    ///    `seq + 1` (1 when no row is expected) and the expected version as
    ///    parent;
    /// 2. move the `refs` row: `UPDATE refs SET version_id = :new, seq =
    ///    seq + 1 WHERE repo_id = :repo AND ref_name = :name AND version_id =
    ///    :expected`, or `INSERT` it when no row was expected. Zero rows
    ///    updated (or a key conflict on insert) is a lost compare-and-swap:
    ///    roll back and return [`CommitOutcome::CasLost`]. A racing push
    ///    usually loses earlier, in step 1: both insert `seq + 1` for the
    ///    same ref, and the loser gets a `ref_versions_seq_unique` violation
    ///    (in Postgres after waiting for the winner's transaction to commit).
    ///    That violation is also [`CommitOutcome::CasLost`], never a
    ///    [`StoreError`], so the caller recomputes the verdict and answers
    ///    with the race-loss text rather than "internal storage error". The
    ///    `refs` foreign
    ///    key on `(version_id, repo_id, ref_name, seq)` makes the database
    ///    refuse any new version that is not the expected version's child;
    /// 3. write [`PushCommit::rows`] for the new IDs: commit rows (a commit
    ///    that already has a row keeps it) and jobs (idempotent, as
    ///    [`MetaStore::enqueue_jobs`]).
    fn commit_push(
        &self,
        push: &PushCommit,
    ) -> impl Future<Output = Result<CommitOutcome, StoreError>> + Send;

    /// Commit index rows, one answer per ID in `ids` order.
    fn lookup_commits(
        &self,
        repo: RepoId,
        ids: &SortedIds,
    ) -> impl Future<Output = Result<Vec<CommitLookup>, StoreError>> + Send;

    /// Writes commit index rows; a commit that already has a row keeps it.
    fn index_commits(
        &self,
        repo: RepoId,
        rows: &[CommitRow],
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Covers of one ref's version chain, ordered by level then `first_seq`,
    /// each with its chain if one was recorded.
    fn covers(
        &self,
        repo: RepoId,
        name: &RefName,
    ) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + Send;

    /// The covers of `name` at `level` whose range starts at one of
    /// `firsts`, each with its chain if one was recorded, in no particular
    /// order; firsts with no cover are left out. A keyed read of the
    /// `(repo, ref, level, first_seq)` unique key, so its cost follows
    /// `firsts`, not the ref's history: receive-pack uses it to find the
    /// compacted covers of a push's search set (issue #149).
    ///
    /// No provided body: a wrapper that forwarded [`MetaStore::covers`] but
    /// fell back to scanning it here would make every push read the ref's
    /// whole cover history again.
    fn covers_at(
        &self,
        repo: RepoId,
        name: &RefName,
        level: CoverLevel,
        firsts: &[Seq],
    ) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + Send;

    /// Creates a cover (and its chain, if given), idempotently: a cover with
    /// the same repository, ref, level and `first_seq` is returned as it is
    /// (its chain is not changed), and [`StoreError::Conflict`] if its
    /// `last_seq` differs. The chain's first version must be the ref's
    /// version at `first_seq` (a foreign key; [`StoreError::Conflict`]
    /// otherwise).
    fn insert_cover(
        &self,
        cover: &NewCover,
    ) -> impl Future<Output = Result<Cover, StoreError>> + Send;

    /// Records a cover's compaction pack, which must be a pack of the same
    /// repository at the cover's level. Idempotent for the same pack;
    /// [`StoreError::Conflict`] if the cover already has another one, and
    /// [`StoreError::NotFound`] if the cover does not exist.
    fn set_cover_pack(
        &self,
        cover: CoverId,
        pack: PackId,
    ) -> impl Future<Output = Result<Cover, StoreError>> + Send;

    /// The pack row with this checksum in the repository, if any.
    fn pack_by_checksum(
        &self,
        repo: RepoId,
        checksum: &crate::id::PackChecksum,
    ) -> impl Future<Output = Result<Option<PackRecord>, StoreError>> + Send;

    /// Garbage collection of orphaned packs (issue #48): deletes, in one
    /// transaction, up to `limit` `packs` rows (of any repository) that no
    /// ref version, cover or job names and that nobody has registered for
    /// at least `grace` (by the store's clock; [`MetaStore::register_pack`]
    /// of an existing row counts as registering it again, and so does
    /// [`MetaStore::evict_shallow_packs`]). A pack a shallow pack recording
    /// names ([`MetaStore::set_shallow_pack`]) counts as named. Returns the
    /// deleted rows.
    ///
    /// Each deleted row leaves a [`PackTombstone`], pending until
    /// [`MetaStore::finish_pack_deletes`]: the caller deletes the blobs of
    /// [`MetaStore::pending_pack_deletes`] and then finishes them. The
    /// tombstone also makes the next registration of the checksum a new
    /// generation, with new keys, so no deletion, however late, removes
    /// the blobs of a later row. `grace` only bounds how long a writer may
    /// take from registering to committing: a writer whose row was swept
    /// in between finds it gone, and its commit fails retryably
    /// ([`StoreError::Unavailable`]). A row registered before migration
    /// 0005 has no registration time: the first sweep that finds it
    /// orphaned records one, and a later sweep deletes it.
    fn sweep_orphan_packs(
        &self,
        grace: Duration,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<PackRecord>, StoreError>> + Send;

    /// Up to `limit` pending tombstones ([`MetaStore::sweep_orphan_packs`]),
    /// oldest first, of any repository and any sweep (a crashed worker's
    /// included).
    fn pending_pack_deletes(
        &self,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<PackTombstone>, StoreError>> + Send;

    /// Marks these tombstones' blobs as deleted. The tombstone row stays,
    /// recording the generation, so the checksum's next registration still
    /// gets a new one. Finishing a finished or unknown tombstone does
    /// nothing.
    fn finish_pack_deletes(
        &self,
        done: &[PackTombstone],
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The exact shallow pack cached for `version` at `depth` (node C8),
    /// if one was recorded: the pack holding exactly the objects of the
    /// commits fewer than `depth` parents below the version's tip, and of
    /// their trees, as [`MetaStore::set_shallow_pack`] recorded it.
    fn shallow_pack(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
    ) -> impl Future<Output = Result<Option<PackRecord>, StoreError>> + Send;

    /// Records `pack` as the exact shallow pack of `version` at `depth`,
    /// after the pack was registered and its blobs uploaded (writers
    /// register, then upload, then reference). The first recording wins:
    /// if the pair already names a pack, that row is returned and `pack` is
    /// left for garbage collection. A pack row that is gone (the sweep took
    /// it between registration and this call) is
    /// [`StoreError::Unavailable`]; a version or pack of another repository
    /// is [`StoreError::Conflict`], and a version that does not exist is
    /// [`StoreError::NotFound`].
    ///
    /// The repository's recorded packs stay within `max_repo_bytes`
    /// ([`NewPack::stored_bytes`] of every distinct pack its recordings
    /// name, each counted once however many recordings name it; issue
    /// #108). When the new pack does not fit, the same transaction evicts
    /// the repository's least recently used packs first (every recording
    /// naming one, last use being the latest
    /// [`MetaStore::mark_shallow_pack_used`] or recording among them) until
    /// it does, and touches each evicted pack as
    /// [`MetaStore::evict_shallow_packs`] does, so a fetch still streaming
    /// it keeps its blobs for a whole grace period. A pack larger than
    /// `max_repo_bytes` on its own is not recorded and evicts nothing
    /// ([`ShallowRecorded::OverLimit`]). Recordings of one repository are
    /// serialized, so concurrent builds cannot together overshoot the cap.
    ///
    /// While a recording names a pack, [`MetaStore::sweep_orphan_packs`]
    /// counts it as named.
    fn set_shallow_pack(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
        pack: PackId,
        max_repo_bytes: u64,
    ) -> impl Future<Output = Result<ShallowRecorded, StoreError>> + Send;

    /// Notes that a fetch was just served the recording of `version` at
    /// `depth`: its last use becomes now (by the store's clock). Unused
    /// recordings are evicted ([`MetaStore::evict_shallow_packs`]), and the
    /// least recently used make room for a new one under the repository's
    /// cap ([`MetaStore::set_shallow_pack`]). A missing recording (evicted
    /// meanwhile) is not an error. Callers rate-limit this: last use only
    /// needs to be right to within a small fraction of the unused window.
    fn mark_shallow_pack_used(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Deletes up to `limit` shallow pack recordings (of any repository),
    /// oldest recording first, and returns how many were deleted. A
    /// recording goes when it was made at least `superseded_after` ago (by
    /// the store's clock) and its version is no longer the current version
    /// of its ref, or when it was last used ([`MetaStore::mark_shallow_pack_used`],
    /// or its recording) at least `unused_after` ago, whether its version is
    /// current or not (issue #108). Each evicted pack's registration time is
    /// set to now, in the same transaction, so the sweep keeps its blobs for
    /// a whole grace period after eviction: a fetch that read the recording
    /// just before may still be streaming the pack.
    fn evict_shallow_packs(
        &self,
        superseded_after: Duration,
        unused_after: Duration,
        limit: u32,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Enqueues jobs, idempotently: a job whose kind and target (ref version
    /// or pack) already have a job that is not done is not added again, so
    /// a forward-base page that repeats on every fetch leaves one
    /// `rewrite_pack` job, not one per fetch. A dead job keeps its slot
    /// until an operator resolves it (procedure and alert in
    /// `crates/aspen-meta/SCHEMA.md`, "jobs"). Returns, per job in order, the ID of
    /// the new or existing job.
    fn enqueue_jobs(
        &self,
        jobs: &[NewJob],
    ) -> impl Future<Output = Result<Vec<JobId>, StoreError>> + Send;

    /// Claims the oldest due job of one of `tags` for `owner`, following
    /// [`Job::apply`] with [`JobEvent::Claim`]; `None` if none is due. An
    /// expired job that the claim parks as dead is not returned.
    fn claim_job(
        &self,
        tags: &[JobTag],
        owner: &str,
        lease: Duration,
    ) -> impl Future<Output = Result<Option<Job>, StoreError>> + Send;

    /// Gives back every running job of `tags` leased to `owner`, in one
    /// transaction: each is failed by its holder with no backoff
    /// ([`JobEvent::Fail`] with the job's current attempt, `reason` as the
    /// error), so it is due again at once, or dead if that was its last
    /// attempt. Returns how many jobs were given back.
    ///
    /// A worker that holds no job calls this after a `claim_job` whose
    /// answer it did not get (issue #131): the claim may have committed, and
    /// the job would otherwise stay leased to a worker that is not running
    /// it until the lease ran out. Only safe when no other worker leases
    /// jobs of `tags` under the same `owner`.
    fn release_jobs(
        &self,
        tags: &[JobTag],
        owner: &str,
        reason: &str,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Applies [`JobEvent::Renew`], [`JobEvent::Complete`] or
    /// [`JobEvent::Fail`] to a job, as one conditional update on its state,
    /// lease owner and `attempts`. [`StoreError::Conflict`] if [`Job::apply`]
    /// would refuse it.
    fn update_job(
        &self,
        id: JobId,
        event: JobEvent,
    ) -> impl Future<Output = Result<Job, StoreError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> Job {
        Job {
            id: JobId::new(1).unwrap(),
            repo: RepoId::new(1).unwrap(),
            kind: JobKind::IndexCommits {
                version: RefVersionId::new(3).unwrap(),
            },
            state: JobState::Queued {
                run_after: UnixMicros(100),
            },
            attempts: 0,
            last_error: None,
        }
    }
    fn claim(owner: &str, now: i64) -> JobEvent {
        JobEvent::Claim {
            owner: owner.into(),
            now: UnixMicros(now),
            lease: Duration::from_micros(50),
        }
    }
    fn fail(owner: &str, attempt: u32, now: i64) -> JobEvent {
        JobEvent::Fail {
            owner: owner.into(),
            attempt,
            now: UnixMicros(now),
            error: "boom".into(),
            backoff: Duration::from_micros(10),
        }
    }
    fn complete(owner: &str, attempt: u32) -> JobEvent {
        JobEvent::Complete {
            owner: owner.into(),
            attempt,
        }
    }
    fn renew(owner: &str, attempt: u32, now: i64) -> JobEvent {
        JobEvent::Renew {
            owner: owner.into(),
            attempt,
            now: UnixMicros(now),
            lease: Duration::from_micros(50),
        }
    }

    #[test]
    fn queued_jobs_are_claimable_only_when_due() {
        assert_eq!(
            job().apply(claim("w", 99)),
            Err(JobTransitionError::NotClaimable)
        );
        let running = job().apply(claim("w", 100)).unwrap();
        assert_eq!(
            running.state,
            JobState::Running {
                owner: "w".into(),
                lease_until: UnixMicros(150)
            }
        );
        assert_eq!(running.attempts, 1);
    }

    #[test]
    fn a_live_lease_blocks_other_workers_and_an_expired_one_does_not() {
        let running = job().apply(claim("a", 100)).unwrap();
        assert_eq!(
            running.apply(claim("b", 149)),
            Err(JobTransitionError::NotClaimable)
        );
        let stolen = running.apply(claim("b", 150)).unwrap();
        assert_eq!(stolen.attempts, 2);
        // The old owner can no longer finish it.
        assert_eq!(
            stolen.apply(complete("a", 1)),
            Err(JobTransitionError::NotHolder)
        );
        let done = stolen.apply(complete("b", 2)).unwrap();
        assert_eq!(done.state, JobState::Done);
        assert_eq!(
            done.apply(claim("c", 1000)),
            Err(JobTransitionError::Finished)
        );
    }

    #[test]
    fn events_are_fenced_on_the_attempt_not_only_the_worker_name() {
        // Worker `w` claims, its lease expires, and `w` (restarted, same
        // ID) reclaims. The first attempt's late Complete must not finish
        // the second attempt.
        let first = job().apply(claim("w", 100)).unwrap();
        let second = first.apply(claim("w", 150)).unwrap();
        assert_eq!(second.attempts, 2);
        for stale in [complete("w", 1), fail("w", 1, 160), renew("w", 1, 160)] {
            assert_eq!(second.apply(stale), Err(JobTransitionError::NotHolder));
        }
        assert_eq!(
            second.apply(complete("w", 2)).unwrap().state,
            JobState::Done
        );
    }

    #[test]
    fn only_the_holder_renews_and_only_while_the_lease_is_live() {
        let running = job().apply(claim("a", 100)).unwrap();
        let renewed = running.apply(renew("a", 1, 140)).unwrap();
        assert_eq!(
            renewed.state,
            JobState::Running {
                owner: "a".into(),
                lease_until: UnixMicros(190)
            }
        );
        assert_eq!(renewed.attempts, 1, "renewing is not a new attempt");
        // The renewal holds off other workers past the original lease end.
        assert_eq!(
            renewed.apply(claim("b", 160)),
            Err(JobTransitionError::NotClaimable)
        );
        assert_eq!(
            running.apply(renew("b", 1, 140)),
            Err(JobTransitionError::NotHolder)
        );
        assert_eq!(
            running.apply(renew("a", 1, 150)),
            Err(JobTransitionError::LeaseExpired)
        );
        assert_eq!(
            job().apply(renew("a", 0, 100)),
            Err(JobTransitionError::NotHolder),
            "a queued job has no lease"
        );
    }

    #[test]
    fn failures_back_off_then_park_the_job_as_dead() {
        let mut j = job();
        for attempt in 1..=MAX_JOB_ATTEMPTS {
            let now = j.state.clone();
            let JobState::Queued { run_after } = now else {
                panic!("{now:?}")
            };
            j = j.apply(claim("w", run_after.0)).unwrap();
            assert_eq!(j.attempts, attempt);
            j = j.apply(fail("w", attempt, run_after.0 + 1)).unwrap();
            assert_eq!(j.last_error.as_deref(), Some("boom"));
        }
        assert_eq!(j.state, JobState::Dead);
    }

    #[test]
    fn repeated_lease_expiries_end_in_dead() {
        // A job that crashes its worker every time never reports a failure;
        // only its lease expires.
        let mut j = job();
        let mut now = 100;
        for attempt in 1..=MAX_JOB_ATTEMPTS {
            j = j.apply(claim("w", now)).unwrap();
            assert_eq!(j.attempts, attempt);
            assert!(matches!(j.state, JobState::Running { .. }));
            now += 50; // the lease expires
        }
        let parked = j.apply(claim("w", now)).unwrap();
        assert_eq!(parked.state, JobState::Dead);
        assert_eq!(parked.attempts, MAX_JOB_ATTEMPTS);
        assert!(parked.last_error.unwrap().contains("expired"));
    }

    #[test]
    fn failure_requeues_after_the_backoff() {
        let j = job()
            .apply(claim("w", 100))
            .unwrap()
            .apply(fail("w", 1, 120))
            .unwrap();
        assert_eq!(
            j.state,
            JobState::Queued {
                run_after: UnixMicros(130)
            }
        );
        assert_eq!(
            job().apply(fail("w", 0, 120)),
            Err(JobTransitionError::NotHolder),
            "a queued job cannot fail"
        );
    }

    #[test]
    fn job_tags_round_trip() {
        for t in JobTag::ALL {
            assert_eq!(t.as_str().parse::<JobTag>().unwrap(), t);
        }
        assert_eq!(
            JobKind::RewritePack {
                pack: PackId::new(1).unwrap()
            }
            .tag(),
            JobTag::RewritePack
        );
        let v = RefVersionId::new(2).unwrap();
        for (k, t) in [
            (VersionJobKind::IndexCommits, JobTag::IndexCommits),
            (VersionJobKind::Compact, JobTag::Compact),
            (VersionJobKind::MirrorPush, JobTag::MirrorPush),
        ] {
            assert_eq!(k.for_version(v).tag(), t);
        }
    }

    #[test]
    fn have_tips_are_log_spaced() {
        let s = |n| Seq::new(n).unwrap();
        let seqs = |c, n| {
            have_tip_seqs(s(c), n)
                .into_iter()
                .map(Seq::get)
                .collect::<Vec<_>>()
        };
        assert_eq!(seqs(300, 8), vec![299, 298, 296, 292, 284, 268, 236, 172]);
        assert_eq!(seqs(5, 8), vec![4, 3, 1]);
        assert_eq!(seqs(1, 8), Vec::<u64>::new());
        assert_eq!(seqs(100, 0), Vec::<u64>::new());
        assert_eq!(have_tip_seqs(s(crate::id::DB_MAX), 32).len(), 32);
    }

    #[test]
    fn seq_ranges_are_ordered() {
        let s = |n| Seq::new(n).unwrap();
        let r = SeqRange::new(s(2), s(5)).unwrap();
        assert!(r.contains(s(2)) && r.contains(s(5)) && !r.contains(s(6)));
        assert_eq!((r.first(), r.last()), (s(2), s(5)));
        assert!(SeqRange::new(s(3), s(3)).is_some());
        assert!(SeqRange::new(s(4), s(3)).is_none());
    }

    fn name() -> RefName {
        RefName::new("refs/heads/x").unwrap()
    }
    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }
    fn trunk_dep() -> Deps {
        Deps {
            trunk: Some(RefVersionId::new(4).unwrap()),
            scoped_old: None,
        }
    }
    fn verdict(kind: VerdictKind) -> Verdict {
        Verdict {
            name: name(),
            against: Some(RefVersionId::new(4).unwrap()),
            new: oid(1),
            kind,
        }
    }

    #[test]
    fn ref_writes_follow_verdicts() {
        let pack = Some(PackId::new(2).unwrap());
        let w = RefWrite::from_verdict(
            &verdict(VerdictKind::FastForward),
            pack,
            trunk_dep(),
            Closure::Exact,
        )
        .unwrap();
        assert_eq!(w.expected, Some(RefVersionId::new(4).unwrap()));
        assert_eq!(
            w.value,
            RefValue::Live {
                tip: oid(1),
                pack,
                closure: Closure::Exact
            }
        );
        assert_eq!(w.deps, trunk_dep());
        let d = RefWrite::from_verdict(
            &verdict(VerdictKind::DeleteCas),
            pack,
            trunk_dep(),
            Closure::Exact,
        )
        .unwrap();
        assert_eq!(d.value, RefValue::Deleted);
        assert_eq!(d.deps, Deps::default(), "a deletion has no dependencies");
        assert!(
            RefWrite::from_verdict(
                &verdict(VerdictKind::Noop),
                pack,
                trunk_dep(),
                Closure::Exact
            )
            .is_none()
        );
        assert!(
            RefWrite::from_verdict(
                &verdict(VerdictKind::Reject(crate::error::RejectReason::StaleOld)),
                pack,
                trunk_dep(),
                Closure::Exact
            )
            .is_none()
        );
    }

    fn live_write() -> RefWrite {
        RefWrite::from_verdict(
            &verdict(VerdictKind::FastForward),
            Some(PackId::new(2).unwrap()),
            trunk_dep(),
            Closure::Exact,
        )
        .unwrap()
    }

    #[test]
    fn a_multi_ref_push_can_store_one_pack_per_write() {
        // `git push main --follow-tags`, split as B1 does it: each write
        // names its own pack and is judged exact on its own.
        let main = live_write();
        let tag = RefWrite::from_verdict(
            &Verdict {
                name: RefName::new("refs/tags/v1").unwrap(),
                against: None,
                new: oid(3),
                kind: VerdictKind::Create,
            },
            Some(PackId::new(4).unwrap()),
            trunk_dep(),
            Closure::Exact,
        )
        .unwrap();
        assert_ne!(main.value.pack(), tag.value.pack());
        let push = PushCommit {
            repo: RepoId::new(1).unwrap(),
            writes: vec![main.clone(), tag],
        };
        push.validate().unwrap();
        // Two writes may also share one pack when their split packs are
        // byte-identical: two branches moved to one new commit (x
        // fast-forwards, y is created), as in `git push origin
        // HEAD:refs/heads/x HEAD:refs/heads/y`. The tag above cannot share
        // main's pack, which lacks the tag object.
        let twin = RefWrite::from_verdict(
            &Verdict {
                name: RefName::new("refs/heads/y").unwrap(),
                against: None,
                new: main.value.tip().unwrap(),
                kind: VerdictKind::Create,
            },
            main.value.pack(),
            trunk_dep(),
            Closure::Exact,
        )
        .unwrap();
        assert_eq!(twin.value, main.value);
        PushCommit {
            repo: RepoId::new(1).unwrap(),
            writes: vec![main, twin],
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn a_push_commit_names_the_versions_it_creates() {
        // The tip's commit row and the indexing job both refer to the
        // version this push creates, whose ID the store assigns.
        let mut tip = live_write();
        tip.commits.push(NewCommit {
            commit: oid(1),
            parents: vec![oid(9)],
            committer_time: 1_700_000_000,
        });
        tip.jobs.push(VersionJob {
            kind: VersionJobKind::IndexCommits,
            run_after: UnixMicros(5),
        });
        let mut del = RefWrite::from_verdict(
            &Verdict {
                name: RefName::new("refs/heads/old").unwrap(),
                ..verdict(VerdictKind::DeleteCas)
            },
            None,
            Deps::default(),
            Closure::Exact,
        )
        .unwrap();
        del.jobs.push(VersionJob {
            kind: VersionJobKind::MirrorPush,
            run_after: UnixMicros(0),
        });
        let push = PushCommit {
            repo: RepoId::new(1).unwrap(),
            writes: vec![tip, del],
        };
        push.validate().unwrap();
        let (v10, v11) = (
            RefVersionId::new(10).unwrap(),
            RefVersionId::new(11).unwrap(),
        );
        let rows = push.rows(&[v10, v11]).unwrap();
        assert_eq!(
            rows.commits,
            vec![CommitRow {
                commit: oid(1),
                ref_version: v10,
                parents: vec![oid(9)],
                committer_time: 1_700_000_000,
            }]
        );
        assert_eq!(
            rows.jobs,
            vec![
                NewJob {
                    repo: RepoId::new(1).unwrap(),
                    kind: JobKind::IndexCommits { version: v10 },
                    run_after: UnixMicros(5),
                },
                NewJob {
                    repo: RepoId::new(1).unwrap(),
                    kind: JobKind::MirrorPush { version: v11 },
                    run_after: UnixMicros(0),
                },
            ]
        );
        assert_eq!(
            push.rows(&[v10]),
            Err(PushCommitError::VersionCount {
                expected: 2,
                got: 1
            })
        );
    }

    #[test]
    fn malformed_push_commits_are_refused() {
        let repo = RepoId::new(1).unwrap();
        let check = |writes: Vec<RefWrite>| PushCommit { repo, writes }.validate();
        assert_eq!(check(vec![]), Err(PushCommitError::Empty));
        assert_eq!(
            check(vec![live_write(), live_write()]),
            Err(PushCommitError::DuplicateRef(name()))
        );
        let mut zero = live_write();
        zero.value = RefValue::Live {
            tip: ObjectId::ZERO,
            pack: None,
            closure: Closure::Exact,
        };
        assert_eq!(check(vec![zero]), Err(PushCommitError::ZeroTip(name())));
        let mut nowhere = live_write();
        nowhere.value = RefValue::Live {
            tip: oid(1),
            pack: None,
            closure: Closure::Exact,
        };
        nowhere.deps = Deps::default();
        assert_eq!(
            check(vec![nowhere.clone()]),
            Err(PushCommitError::Disconnected(name()))
        );
        // With only a scoped dependency it is connected.
        nowhere.deps.scoped_old = Some(RefVersionId::new(3).unwrap());
        check(vec![nowhere]).unwrap();
        let mut del = live_write();
        del.value = RefValue::Deleted;
        del.deps = Deps {
            trunk: Some(RefVersionId::new(2).unwrap()),
            scoped_old: None,
        };
        assert_eq!(
            check(vec![del.clone()]),
            Err(PushCommitError::DeletionWithDeps(name()))
        );
        del.deps = Deps {
            trunk: None,
            scoped_old: Some(RefVersionId::new(3).unwrap()),
        };
        assert_eq!(
            check(vec![del.clone()]),
            Err(PushCommitError::DeletionWithDeps(name()))
        );
        del.deps = Deps::default();
        check(vec![del.clone()]).unwrap();
        del.jobs.push(VersionJob {
            kind: VersionJobKind::IndexCommits,
            run_after: UnixMicros(0),
        });
        assert_eq!(
            check(vec![del]),
            Err(PushCommitError::NothingToIndex(name()))
        );
    }

    fn chain_version(id: i64, seq: u64, deps: Deps) -> RefVersion {
        use crate::refs::RefValue;
        RefVersion {
            id: RefVersionId::new(id).unwrap(),
            repo: RepoId::new(1).unwrap(),
            name: name(),
            seq: Seq::new(seq).unwrap(),
            parent: (seq > 1).then(|| RefVersionId::new(id - 1).unwrap()),
            value: RefValue::Live {
                tip: ObjectId::from_bytes([seq as u8; 20]),
                pack: None,
                closure: Closure::Exact,
            },
            deps,
            created_at: UnixMicros(0),
        }
    }

    fn on_parent(id: i64) -> Deps {
        Deps {
            trunk: Some(RefVersionId::new(id - 1).unwrap()),
            scoped_old: None,
        }
    }

    fn range(a: u64, b: u64) -> SeqRange {
        SeqRange::new(Seq::new(a).unwrap(), Seq::new(b).unwrap()).unwrap()
    }

    /// Versions 10..=13 at seq 1..=4, each depending on its parent.
    fn chain() -> Vec<RefVersion> {
        (1..=4)
            .map(|s| {
                let id = 9 + s as i64;
                let deps = if s == 1 {
                    Deps::default()
                } else {
                    on_parent(id)
                };
                chain_version(id, s, deps)
            })
            .collect()
    }

    #[test]
    fn a_dependency_chain_verifies_and_records_its_first_version() {
        let v = chain();
        let r = VerifiedRange::verify(&name(), range(1, 4), &v).unwrap();
        assert_eq!(r.chain().first_version(), v[0].id);
        assert_eq!(r.range(), range(1, 4));
        // A sub-range is a chain too; its first version is where it starts.
        let r = VerifiedRange::verify(&name(), range(2, 3), &v[1..3]).unwrap();
        assert_eq!(r.chain().first_version(), v[1].id);
    }

    #[test]
    fn a_version_that_does_not_depend_on_its_parent_breaks_the_chain() {
        // A forced branch update through the plain URL depends on trunk,
        // not on its parent.
        let mut v = chain();
        v[2].deps = Deps {
            trunk: Some(RefVersionId::new(1).unwrap()),
            scoped_old: None,
        };
        assert_eq!(
            VerifiedRange::verify(&name(), range(1, 4), &v),
            Err(ChainError::Broken {
                name: name(),
                seq: Seq::new(3).unwrap()
            })
        );
        // A scoped dependency on the parent keeps the chain.
        v[2].deps = Deps {
            trunk: Some(RefVersionId::new(1).unwrap()),
            scoped_old: Some(v[1].id),
        };
        assert!(VerifiedRange::verify(&name(), range(1, 4), &v).is_ok());
        // The first version's own dependencies do not matter.
        let mut v = chain();
        v[0].deps = Deps {
            trunk: Some(RefVersionId::new(2).unwrap()),
            scoped_old: None,
        };
        assert!(VerifiedRange::verify(&name(), range(1, 4), &v).is_ok());
    }

    #[test]
    fn verify_refuses_versions_that_are_not_the_range() {
        let v = chain();
        let wrong = Err(ChainError::WrongVersions {
            name: name(),
            first: Seq::new(1).unwrap(),
            last: Seq::new(4).unwrap(),
        });
        // Too few, a gap, out of order, another ref, a forged parent link.
        assert_eq!(VerifiedRange::verify(&name(), range(1, 4), &v[..3]), wrong);
        let gap = vec![v[0].clone(), v[1].clone(), v[3].clone(), v[3].clone()];
        assert_eq!(VerifiedRange::verify(&name(), range(1, 4), &gap), wrong);
        let mut swapped = v.clone();
        swapped.swap(1, 2);
        assert_eq!(VerifiedRange::verify(&name(), range(1, 4), &swapped), wrong);
        let other = RefName::new("refs/heads/other").unwrap();
        assert!(matches!(
            VerifiedRange::verify(&other, range(1, 4), &v),
            Err(ChainError::WrongVersions { .. })
        ));
        let mut forged = v.clone();
        forged[2].parent = Some(RefVersionId::new(99).unwrap());
        forged[2].deps = on_parent(100);
        assert_eq!(VerifiedRange::verify(&name(), range(1, 4), &forged), wrong);
    }

    #[test]
    fn adjacent_chains_join_through_the_junction_version() {
        let v = chain();
        let lower = VerifiedRange::verify(&name(), range(1, 2), &v[..2]).unwrap();
        let upper = VerifiedRange::verify(&name(), range(3, 4), &v[2..]).unwrap();
        let joined = lower.clone().join(&upper, &v[2]).unwrap();
        assert_eq!(joined.range(), range(1, 4));
        assert_eq!(joined.chain().first_version(), v[0].id);
        // The junction must be the upper range's first version...
        assert_eq!(
            lower.clone().join(&upper, &v[3]),
            Err(ChainError::NotAdjacent)
        );
        // ...the ranges adjacent...
        let far = VerifiedRange::verify(&name(), range(4, 4), &v[3..]).unwrap();
        assert_eq!(
            lower.clone().join(&far, &v[3]),
            Err(ChainError::NotAdjacent)
        );
        // ...and the junction must depend on its parent.
        let mut broken = v[2].clone();
        broken.deps = Deps::default();
        let upper2 =
            VerifiedRange::verify(&name(), range(3, 4), &[broken.clone(), v[3].clone()]).unwrap();
        assert_eq!(
            lower.join(&upper2, &broken),
            Err(ChainError::Broken {
                name: name(),
                seq: Seq::new(3).unwrap()
            })
        );
    }

    #[test]
    fn the_layout_tiles_each_level_with_the_level_below() {
        let l = CoverLayout::new(4).unwrap();
        let lvl = |n| CoverLevel::new(n).unwrap();
        assert!(CoverLayout::new(1).is_none());
        assert!(CoverLayout::new(1025).is_none());
        assert_eq!(CoverLayout::default().fanout(), 16);
        assert_eq!(l.span(lvl(3)), 64);
        assert!(l.ends_a_cover(Seq::new(8).unwrap()));
        assert!(!l.ends_a_cover(Seq::new(9).unwrap()));
        assert!(l.ending_at(Seq::new(5).unwrap()).is_empty());
        assert_eq!(
            l.ending_at(Seq::new(4).unwrap()),
            vec![(lvl(1), range(1, 4))]
        );
        assert_eq!(
            l.ending_at(Seq::new(32).unwrap()),
            vec![(lvl(1), range(29, 32)), (lvl(2), range(17, 32))]
        );
        assert_eq!(l.ending_at(Seq::new(256).unwrap()).len(), 4);
        assert_eq!(
            l.parts(lvl(2), range(17, 32)),
            vec![
                (lvl(1), range(17, 20)),
                (lvl(1), range(21, 24)),
                (lvl(1), range(25, 28)),
                (lvl(1), range(29, 32)),
            ]
        );
        assert!(l.parts(lvl(1), range(1, 4)).is_empty());
    }
}

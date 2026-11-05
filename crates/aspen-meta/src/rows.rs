//! Decoding rows into `aspen-core` types.
//!
//! Every decoder checks the row against schema v1's rules again and reports
//! a row that breaks them as [`StoreError::Corrupt`]: the database enforces
//! the rules, so a violation means the data was changed behind Aspen's
//! back, and a reader must not act on it.

use aspen_core::error::StoreError;
use aspen_core::id::{
    CoverId, JobId, ObjectId, PackChecksum, PackId, RefVersionId, RepoId, Seq, UnixMicros,
};
use aspen_core::meta::{CommitRow, Cover, CoverChain, Job, JobKind, JobState, JobTag, SeqRange};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{CoverLevel, NewPack, PackGeneration, PackKeys, PackOrigin, PackRecord};
use aspen_core::refs::{Closure, Deps, RefValue, RefVersion};
use aspen_core::repo::{ForcePolicy, Repo, RepoSettings, ShallowPolicy};
use sqlx::Row as _;
use sqlx::any::AnyRow;

use crate::error::{corrupt, store_error};

/// Reads a column, mapping decode failures to [`StoreError::Corrupt`].
pub(crate) fn get<'r, T>(row: &'r AnyRow, col: &str) -> Result<T, StoreError>
where
    T: sqlx::Decode<'r, sqlx::Any> + sqlx::Type<sqlx::Any>,
{
    row.try_get(col).map_err(store_error)
}

fn bad(table: &str, col: &str, detail: impl std::fmt::Display) -> StoreError {
    corrupt(format!("{table}.{col}: {detail}"))
}

fn row_id<T>(
    row: &AnyRow,
    table: &str,
    col: &str,
    wrap: fn(i64) -> Result<T, aspen_core::id::IdError>,
) -> Result<T, StoreError> {
    let raw: i64 = get(row, col)?;
    wrap(raw).map_err(|e| bad(table, col, e))
}

fn opt_row_id<T>(
    row: &AnyRow,
    table: &str,
    col: &str,
    wrap: fn(i64) -> Result<T, aspen_core::id::IdError>,
) -> Result<Option<T>, StoreError> {
    let raw: Option<i64> = get(row, col)?;
    raw.map(wrap).transpose().map_err(|e| bad(table, col, e))
}

fn object_id(row: &AnyRow, table: &str, col: &str) -> Result<ObjectId, StoreError> {
    let raw: Vec<u8> = get(row, col)?;
    ObjectId::from_slice(&raw).map_err(|e| bad(table, col, e))
}

fn ref_name(row: &AnyRow, table: &str, col: &str) -> Result<RefName, StoreError> {
    let raw: String = get(row, col)?;
    RefName::new(raw).map_err(|e| bad(table, col, e))
}

fn seq(row: &AnyRow, table: &str, col: &str) -> Result<Seq, StoreError> {
    let raw: i64 = get(row, col)?;
    Seq::from_db(raw).map_err(|e| bad(table, col, e))
}

/// Columns of `repos` that [`repo`] reads.
pub(crate) const REPO_COLS: &str =
    "id, name, trunk_ref, allow_force, writable_refs, no_thin, have_tips, shallow";

pub(crate) fn repo(row: &AnyRow) -> Result<Repo, StoreError> {
    const T: &str = "repos";
    let name: String = get(row, "name")?;
    let name = RepoName::new(name).map_err(|e| bad(T, "name", e))?;
    let trunk = ref_name(row, T, "trunk_ref")?;
    let writable: String = get(row, "writable_refs")?;
    let writable =
        RepoSettings::parse_writable_column(&writable).map_err(|e| bad(T, "writable_refs", e))?;
    let allow_force: String = get(row, "allow_force")?;
    let allow_force: ForcePolicy = allow_force.parse().map_err(|e| bad(T, "allow_force", e))?;
    let shallow: String = get(row, "shallow")?;
    let shallow: ShallowPolicy = shallow.parse().map_err(|e| bad(T, "shallow", e))?;
    let no_thin: i64 = get(row, "no_thin")?;
    let have_tips: i64 = get(row, "have_tips")?;
    let have_tips = u8::try_from(have_tips).map_err(|e| bad(T, "have_tips", e))?;
    let settings = RepoSettings::builder(trunk, writable)
        .allow_force(allow_force)
        .no_thin(no_thin != 0)
        .have_tips(have_tips)
        .shallow(shallow)
        .build()
        .map_err(|e| bad(T, "settings", e))?;
    Ok(Repo {
        id: row_id(row, T, "id", RepoId::new)?,
        name,
        settings,
    })
}

/// Columns of `ref_versions` that [`version`] reads, each prefixed with
/// `v.` so a query can join `refs r` without ambiguity.
pub(crate) const VERSION_COLS: &str = "v.id, v.repo_id, v.ref_name, v.seq, v.parent_version_id, \
     v.tip_oid, v.closure_exact, v.pack_id, v.trunk_dep_id, v.scoped_dep_id, v.created_at_us";

/// The same columns unprefixed, for `RETURNING`.
pub(crate) const VERSION_RETURNING: &str = "id, repo_id, ref_name, seq, parent_version_id, \
     tip_oid, closure_exact, pack_id, trunk_dep_id, scoped_dep_id, created_at_us";

pub(crate) fn version(row: &AnyRow) -> Result<RefVersion, StoreError> {
    const T: &str = "ref_versions";
    let tip: Option<Vec<u8>> = get(row, "tip_oid")?;
    let closure: Option<i64> = get(row, "closure_exact")?;
    let pack = opt_row_id(row, T, "pack_id", PackId::new)?;
    let deps = Deps {
        trunk: opt_row_id(row, T, "trunk_dep_id", RefVersionId::new)?,
        scoped_old: opt_row_id(row, T, "scoped_dep_id", RefVersionId::new)?,
    };
    let value = match (tip, closure) {
        (Some(tip), Some(closure)) => {
            let tip = ObjectId::from_slice(&tip).map_err(|e| bad(T, "tip_oid", e))?;
            if tip.is_zero() {
                return Err(bad(T, "tip_oid", "zero ID"));
            }
            let closure = match closure {
                1 => Closure::Exact,
                0 => Closure::Wider,
                other => return Err(bad(T, "closure_exact", other)),
            };
            if pack.is_none() && deps.iter().next().is_none() {
                return Err(bad(T, "pack_id", "live version with no pack or dependency"));
            }
            RefValue::Live { tip, pack, closure }
        }
        (None, None) => {
            if pack.is_some() || deps.iter().next().is_some() {
                return Err(bad(T, "tip_oid", "deletion with a pack or dependency"));
            }
            RefValue::Deleted
        }
        _ => return Err(bad(T, "closure_exact", "set exactly for live versions")),
    };
    let seq = seq(row, T, "seq")?;
    let parent = opt_row_id(row, T, "parent_version_id", RefVersionId::new)?;
    if (seq == Seq::FIRST) != parent.is_none() {
        return Err(bad(T, "parent_version_id", "set exactly when seq > 1"));
    }
    Ok(RefVersion {
        id: row_id(row, T, "id", RefVersionId::new)?,
        repo: row_id(row, T, "repo_id", RepoId::new)?,
        name: ref_name(row, T, "ref_name")?,
        seq,
        parent,
        value,
        deps,
        created_at: UnixMicros(get(row, "created_at_us")?),
    })
}

/// Columns of `packs` that [`pack`] reads.
pub(crate) const PACK_COLS: &str = "id, repo_id, checksum, pack_key, size, idx_key, idx_size, \
     entries_key, entries_size, object_count, ref_delta_count, level, origin, received_bytes, \
     generation, created_at_us";

fn u64_col(row: &AnyRow, table: &str, col: &str) -> Result<u64, StoreError> {
    let raw: i64 = get(row, col)?;
    u64::try_from(raw).map_err(|e| bad(table, col, e))
}

fn u32_col(row: &AnyRow, table: &str, col: &str) -> Result<u32, StoreError> {
    let raw: i64 = get(row, col)?;
    u32::try_from(raw).map_err(|e| bad(table, col, e))
}

pub(crate) fn pack(row: &AnyRow) -> Result<PackRecord, StoreError> {
    const T: &str = "packs";
    let checksum: Vec<u8> = get(row, "checksum")?;
    let checksum = PackChecksum::from_slice(&checksum).map_err(|e| bad(T, "checksum", e))?;
    let level: i64 = get(row, "level")?;
    let origin: String = get(row, "origin")?;
    let received: Option<i64> = get(row, "received_bytes")?;
    let origin = match (origin.as_str(), received) {
        ("push", Some(received)) if level == 0 => PackOrigin::Push {
            received_bytes: u64::try_from(received).map_err(|e| bad(T, "received_bytes", e))?,
        },
        ("compaction", None) => PackOrigin::Compaction {
            level: u8::try_from(level)
                .ok()
                .and_then(CoverLevel::new)
                .ok_or_else(|| bad(T, "level", level))?,
        },
        (o, _) => return Err(bad(T, "origin", format!("{o} at level {level}"))),
    };
    let pack = NewPack {
        repo: row_id(row, T, "repo_id", RepoId::new)?,
        checksum,
        size: u64_col(row, T, "size")?,
        idx_size: u64_col(row, T, "idx_size")?,
        entries_size: u64_col(row, T, "entries_size")?,
        object_count: u32_col(row, T, "object_count")?,
        ref_delta_count: u32_col(row, T, "ref_delta_count")?,
        origin,
    };
    pack.validate().map_err(|e| bad(T, "row", e))?;
    let generation = PackGeneration::new(u32_col(row, T, "generation")?);
    // The stored keys are derived from repo, checksum and generation; a row
    // whose keys differ points at some other blob.
    let keys = PackKeys::new(pack.repo, &pack.checksum, generation);
    for (col, want) in [
        ("pack_key", &keys.pack),
        ("idx_key", &keys.idx),
        ("entries_key", &keys.entries),
    ] {
        let stored: String = get(row, col)?;
        if stored != want.as_str() {
            return Err(bad(T, col, format!("{stored:?} is not {want}")));
        }
    }
    Ok(PackRecord {
        id: row_id(row, T, "id", PackId::new)?,
        pack,
        generation,
        created_at: UnixMicros(get(row, "created_at_us")?),
    })
}

/// Columns of `commit_index` that [`commit`] reads.
pub(crate) const COMMIT_COLS: &str = "commit_oid, ref_version_id, parents, committer_time";

pub(crate) fn commit(row: &AnyRow) -> Result<CommitRow, StoreError> {
    const T: &str = "commit_index";
    let parents: Vec<u8> = get(row, "parents")?;
    if !parents.len().is_multiple_of(20) {
        return Err(bad(T, "parents", format!("{} bytes", parents.len())));
    }
    let parents = parents
        .as_chunks::<20>()
        .0
        .iter()
        .map(|p| ObjectId::from_bytes(*p))
        .collect();
    Ok(CommitRow {
        commit: object_id(row, T, "commit_oid")?,
        ref_version: row_id(row, T, "ref_version_id", RefVersionId::new)?,
        parents,
        committer_time: get(row, "committer_time")?,
    })
}

/// `parents` as stored: the IDs concatenated.
pub(crate) fn parents_column(parents: &[ObjectId]) -> Vec<u8> {
    parents.iter().flat_map(|p| *p.as_bytes()).collect()
}

/// Columns that [`cover`] reads, from [`COVER_FROM`]: the `covers` row and
/// its chain's first version, if it has a `cover_chains` row.
pub(crate) const COVER_COLS: &str = "c.id, c.repo_id, c.ref_name, c.level, c.first_seq, \
     c.last_seq, c.pack_id, ch.first_version_id AS chain_version_id";

/// The `FROM` clause for [`COVER_COLS`]; `c` is the cover.
pub(crate) const COVER_FROM: &str = "covers c LEFT JOIN cover_chains ch ON ch.cover_id = c.id";

pub(crate) fn cover(row: &AnyRow) -> Result<Cover, StoreError> {
    const T: &str = "covers";
    let level: i64 = get(row, "level")?;
    let level = u8::try_from(level)
        .ok()
        .and_then(CoverLevel::new)
        .ok_or_else(|| bad(T, "level", level))?;
    let range = SeqRange::new(seq(row, T, "first_seq")?, seq(row, T, "last_seq")?)
        .ok_or_else(|| bad(T, "last_seq", "before first_seq"))?;
    Ok(Cover {
        id: row_id(row, T, "id", CoverId::new)?,
        repo: row_id(row, T, "repo_id", RepoId::new)?,
        name: ref_name(row, T, "ref_name")?,
        level,
        range,
        pack: opt_row_id(row, T, "pack_id", PackId::new)?,
        chain: opt_row_id(row, "cover_chains", "chain_version_id", RefVersionId::new)?
            .map(CoverChain::recorded),
    })
}

/// Columns of `jobs` that [`job`] reads.
pub(crate) const JOB_COLS: &str = "id, repo_id, kind, ref_version_id, pack_id, state, attempts, \
     run_after_us, lease_owner, lease_expires_us, last_error";

pub(crate) fn job(row: &AnyRow) -> Result<Job, StoreError> {
    const T: &str = "jobs";
    let kind: String = get(row, "kind")?;
    let tag: JobTag = kind.parse().map_err(|e| bad(T, "kind", e))?;
    let version = opt_row_id(row, T, "ref_version_id", RefVersionId::new)?;
    let pack = opt_row_id(row, T, "pack_id", PackId::new)?;
    let kind = match (tag, version, pack) {
        (JobTag::IndexCommits, Some(version), None) => JobKind::IndexCommits { version },
        (JobTag::Compact, Some(version), None) => JobKind::Compact { version },
        (JobTag::MirrorPush, Some(version), None) => JobKind::MirrorPush { version },
        (JobTag::RewritePack, None, Some(pack)) => JobKind::RewritePack { pack },
        _ => return Err(bad(T, "kind", "target columns do not match the kind")),
    };
    let state: String = get(row, "state")?;
    let owner: Option<String> = get(row, "lease_owner")?;
    let lease: Option<i64> = get(row, "lease_expires_us")?;
    let state = match (state.as_str(), owner, lease) {
        ("queued", None, None) => JobState::Queued {
            run_after: UnixMicros(get(row, "run_after_us")?),
        },
        ("running", Some(owner), Some(lease)) => JobState::Running {
            owner,
            lease_until: UnixMicros(lease),
        },
        ("done", None, None) => JobState::Done,
        ("dead", None, None) => JobState::Dead,
        (s, _, _) => return Err(bad(T, "state", format!("{s} with inconsistent lease"))),
    };
    Ok(Job {
        id: row_id(row, T, "id", JobId::new)?,
        repo: row_id(row, T, "repo_id", RepoId::new)?,
        kind,
        state,
        attempts: u32_col(row, T, "attempts")?,
        last_error: get(row, "last_error")?,
    })
}

/// The target columns of a job kind: `(ref_version_id, pack_id)`.
pub(crate) fn job_target(kind: &JobKind) -> (Option<i64>, Option<i64>) {
    match kind {
        JobKind::IndexCommits { version }
        | JobKind::Compact { version }
        | JobKind::MirrorPush { version } => (Some(version.get()), None),
        JobKind::RewritePack { pack } => (None, Some(pack.get())),
    }
}

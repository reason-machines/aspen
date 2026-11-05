//! Wants and haves, resolved to ref versions (fetch path step 2).
//!
//! - A **want** resolves through its `commit_index` row: any version whose
//!   tip reaches the commit will do, because sending that version's closure
//!   sends a superset. An annotated tag has no row (the index holds commits
//!   only), so a want that has none is looked up among the `refs/tags/`
//!   tips, in one scan per request of at most
//!   [`ServerConfig::max_tag_scan`](crate::ServerConfig::max_tag_scan)
//!   tags (schema v1 cannot look a ref up by its tip). Anything else is
//!   refused with `ERR`: a blob or tree (S5: a lazy fetch from a partial
//!   clone), or a commit of a push whose history the indexing job has not
//!   reached yet (only tips are indexed synchronously). The server cannot
//!   tell the two apart without reading packs, so the message names both.
//!   Like `uploadpack.allowAnySHA1InWant`, a want may name any indexed
//!   commit, not only a current tip.
//! - A **have** stands for a version only through
//!   [`HaveVersion`]: the version's closure
//!   is exact and its tip is the have. The `commit_index` row of a have is
//!   read only to find a candidate version; [`HaveVersion::exact_tip`]
//!   decides. A have that is not a tip of an exact version (a commit the
//!   server never saw, a non-tip commit of a push, the tip of a wider
//!   version) is dropped, which only costs bytes.
//!
//! `HaveVersion::ancestor_of_have` (a version tip below the have) is not
//! used: proving ancestry would take a parent walk per have, and Git's
//! negotiation already sends the ancestors of its haves, so the client's
//! next have is usually an exact tip anyway.

use std::collections::{HashMap, HashSet};

use aspen_core::error::{FetchError, StoreError};
use aspen_core::fetch::HaveVersion;
use aspen_core::id::{ObjectId, RefVersionId, RepoId};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{CommitLookup, MetaStore, RefListQuery};
use aspen_core::refs::RefVersion;

use super::Failure;

/// Tags read from the store per page while resolving tag wants.
const TAG_PAGE: u32 = 1000;

/// A want, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Want {
    /// The wanted object.
    pub(crate) id: ObjectId,
    /// A version whose closure holds it.
    pub(crate) version: RefVersionId,
    /// Whether `id` is a commit with a `commit_index` row (otherwise it is
    /// the tip of a tag ref: an annotated tag, to peel for `shallow-info`).
    pub(crate) indexed_commit: bool,
}

/// Resolves every want, or fails on the first that is not a commit or tag
/// this server holds; scans at most `max_tag_scan` tags.
pub(crate) async fn wants<M: MetaStore>(
    meta: &M,
    repo: RepoId,
    ids: &[ObjectId],
    max_tag_scan: usize,
) -> Result<Vec<Want>, Failure> {
    let sorted = SortedIds::from_unsorted(ids.to_vec());
    let rows = meta
        .lookup_commits(repo, &sorted)
        .await
        .map_err(FetchError::Store)?;
    let mut out = Vec::with_capacity(sorted.len());
    let mut pending = Vec::new();
    for (id, row) in sorted.as_slice().iter().zip(rows) {
        match row {
            CommitLookup::Indexed(row) => out.push(Want {
                id: *id,
                version: row.ref_version,
                indexed_commit: true,
            }),
            CommitLookup::NotIndexed => pending.push(*id),
        }
    }
    if !pending.is_empty() {
        let (tags, complete) = tag_tips(meta, repo, &pending, max_tag_scan).await?;
        for id in pending {
            let Some(version) = tags.get(&id).copied() else {
                return Err(unresolved_want(id, complete, max_tag_scan));
            };
            out.push(Want {
                id,
                version,
                indexed_commit: false,
            });
        }
    }
    Ok(out)
}

/// The `ERR` for a want that is neither indexed nor a tag tip found by the
/// scan (`complete`: the scan saw every tag).
fn unresolved_want(id: ObjectId, complete: bool, max_tag_scan: usize) -> Failure {
    if !complete {
        return Failure::refused(format!(
            "want {id} is not an indexed commit, and this repository has more than \
             {max_tag_scan} tags to search for it (max_tag_scan)"
        ));
    }
    Failure::Refused {
        message: format!(
            "want {id} is not a ref tip, annotated tag or indexed commit of this \
             repository. If it is a commit pushed moments ago, retry once the server \
             has indexed that push; blobs and trees cannot be wanted (lazy fetches \
             from partial clones are not supported)"
        ),
        // A commit from a push still being indexed resolves later; a blob
        // never does. Retrying is the only way to find out.
        retryable: true,
    }
}

/// The versions of the `refs/tags/` refs whose tip is one of `ids`, from a
/// scan of at most `max` tags, and whether the scan saw every tag (or
/// stopped early because it found them all).
async fn tag_tips<M: MetaStore>(
    meta: &M,
    repo: RepoId,
    ids: &[ObjectId],
    max: usize,
) -> Result<(HashMap<ObjectId, RefVersionId>, bool), FetchError> {
    let wanted: HashSet<&ObjectId> = ids.iter().collect();
    let mut found = HashMap::new();
    let mut after = None;
    let mut scanned = 0usize;
    loop {
        let limit = TAG_PAGE.min(u32::try_from(max - scanned).unwrap_or(u32::MAX));
        if limit == 0 {
            return Ok((found, false));
        }
        let page = meta
            .list_refs(
                repo,
                &RefListQuery {
                    prefixes: vec!["refs/tags/".into()],
                    after: after.take(),
                    limit,
                },
            )
            .await
            .map_err(FetchError::Store)?;
        let full = page.len() == limit as usize;
        scanned += page.len();
        for v in page {
            if let Some(tip) = v.value.tip().filter(|t| wanted.contains(t)) {
                found.entry(tip).or_insert(v.id);
            }
            after = Some(v.name);
        }
        if !full || found.len() == wanted.len() {
            return Ok((found, true));
        }
    }
}

/// The haves that stand for a version, and how many were dropped. Each
/// candidate version is read once and handed back so the planner's graph
/// need not read it again.
pub(crate) async fn haves<M: MetaStore>(
    meta: &M,
    repo: RepoId,
    ids: &[ObjectId],
) -> Result<(Vec<(ObjectId, HaveVersion)>, Vec<RefVersion>, usize), FetchError> {
    let sorted = SortedIds::from_unsorted(ids.to_vec());
    if sorted.is_empty() {
        return Ok((Vec::new(), Vec::new(), 0));
    }
    let rows = meta
        .lookup_commits(repo, &sorted)
        .await
        .map_err(FetchError::Store)?;
    let mut cache: HashMap<RefVersionId, RefVersion> = HashMap::new();
    let mut used = Vec::new();
    for (id, row) in sorted.as_slice().iter().zip(rows) {
        let CommitLookup::Indexed(row) = row else {
            continue;
        };
        if !cache.contains_key(&row.ref_version) {
            let v = meta
                .ref_version(row.ref_version)
                .await
                .map_err(FetchError::Store)?
                .ok_or_else(|| {
                    FetchError::Store(StoreError::Corrupt {
                        what: format!(
                            "commit_index names ref version {} which is missing",
                            row.ref_version
                        ),
                    })
                })?;
            if v.repo != repo {
                return Err(FetchError::Store(StoreError::Corrupt {
                    what: format!("ref version {} is in another repository", v.id),
                }));
            }
            cache.insert(v.id, v);
        }
        if let Some(h) = HaveVersion::exact_tip(*id, &cache[&row.ref_version]) {
            used.push((*id, h));
        }
    }
    let dropped = sorted.len() - used.len();
    Ok((used, cache.into_values().collect(), dropped))
}

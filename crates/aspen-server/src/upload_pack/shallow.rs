//! `shallow-info` for shallow requests (fetch path step 5, S5).
//!
//! A shallow request is planned in `PlanMode::FullClosure` (the pack is the
//! full closure of the wants); only the boundary sent here is exact. It
//! follows `git upload-pack` (`upload-pack.c`, `shallow.c`):
//!
//! - `deepen <n>`: the commits `n - 1` parents below a want are the new
//!   boundary (`shallow`); the client's shallow commits above it become
//!   `unshallow`. `deepen 2147483647` (`git fetch --unshallow`) unshallows
//!   every client shallow commit the server knows.
//! - `deepen-relative`: the same, counted from the client's shallow commits
//!   that the wants reach, so the boundary moves `n` commits deeper.
//! - `deepen-since` / `deepen-not`: the included commits are those reachable
//!   from the wants, not reachable from a `deepen-not` ref, and (with
//!   `since`) committed at or after the time; an included commit with a
//!   parent outside that set is a boundary commit. Neither Git nor Aspen
//!   walks through a commit older than the time. They differ on a commit
//!   newer than the time that is reachable both through a new path and as
//!   an ancestor of an old commit (committer times going backwards along
//!   history): Git's `rev-list --max-age` marks the old commit's ancestors
//!   uninteresting and drops it, Aspen keeps it because the new path
//!   reaches it. Every object is sent either way (full closure), so the
//!   client's repository is valid.
//! - `shallow` lines without `deepen*`: an empty section, as Git sends.
//!
//! A walk loads at most [`ServerConfig::max_shallow_commits`] commits;
//! past that the request is refused with an `ERR` (not an alert).
//!
//! Parents and times come from `commit_index`. A commit that has no row yet
//! (the background job is still indexing a large push) is read from the
//! packs of the wants' closure; if it is in neither place the fetch fails
//! with a retryable `ERR` (`FetchError::CommitUnresolved`), never with a
//! boundary computed from a partial walk.
//!
//! [`ServerConfig::max_shallow_commits`]: crate::ServerConfig::max_shallow_commits

use std::collections::{HashMap, HashSet};

use aspen_core::blob::BlobStore;
use aspen_core::error::FetchError;
use aspen_core::fetch::{Deepen, FetchRequest, ShallowInfo};
use aspen_core::id::{ObjectId, PackId, RepoId};
use aspen_core::ingest::{BaseLookup, CandidateBases, ObjectKind, SearchPacks, SortedIds};
use aspen_core::meta::{CommitLookup, MetaStore};
use aspen_core::name::RefName;
use aspen_core::repo::Repo;
use aspen_pack::locate::{PrefetchError, StoredPacks};

use super::Failure;

/// `deepen` value Git sends for `--unshallow` (`INFINITE_DEPTH`).
pub(crate) const INFINITE_DEPTH: u32 = 0x7fff_ffff;

/// Most tag objects followed when peeling one want.
const MAX_TAG_CHAIN: usize = 16;

/// Commit limit for a reader that only peels tags (one commit per peel).
pub(crate) const PEEL_COMMITS: usize = 16;

#[derive(Debug, Clone)]
struct Commit {
    parents: Vec<ObjectId>,
    time: i64,
}

/// Reads commits (and tag objects) for the walk: `commit_index` first, the
/// search packs for anything not indexed yet.
pub(crate) struct Objects<'a, M, B> {
    meta: &'a M,
    stored: &'a StoredPacks<M, B>,
    repo: RepoId,
    search: SearchPacks,
    known: HashMap<ObjectId, Commit>,
    /// Looked up and found nowhere (or not a commit).
    absent: HashSet<ObjectId>,
    /// Most commits held in `known`.
    max_commits: usize,
}

impl<M, B> std::fmt::Debug for Objects<'_, M, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Objects")
            .field("known", &self.known.len())
            .finish_non_exhaustive()
    }
}

impl<'a, M: MetaStore, B: BlobStore> Objects<'a, M, B> {
    /// A reader that falls back to `search`: the packs of the closure of
    /// every version whose history the walk may enter. It refuses to hold
    /// more than `max_commits` commits.
    pub(crate) fn new(
        meta: &'a M,
        stored: &'a StoredPacks<M, B>,
        repo: RepoId,
        search: Vec<PackId>,
        max_commits: usize,
    ) -> Self {
        Self {
            meta,
            stored,
            repo,
            search: SearchPacks::new(search),
            known: HashMap::new(),
            absent: HashSet::new(),
            max_commits,
        }
    }

    /// Full objects of `ids` from the search packs.
    async fn read_from_packs(
        &self,
        ids: Vec<ObjectId>,
    ) -> Result<HashMap<ObjectId, (ObjectKind, Vec<u8>)>, FetchError> {
        let candidates = CandidateBases(SortedIds::from_unsorted(ids));
        let (prefetched, _) = self
            .stored
            .prefetch_bases(&self.search, &candidates)
            .await
            .map_err(|e| match e {
                PrefetchError::Store(e) => FetchError::Store(e),
                other => FetchError::Store(aspen_core::error::StoreError::Unavailable {
                    reason: other.to_string(),
                }),
            })?;
        let mut out = HashMap::new();
        for id in candidates.0.as_slice() {
            let mut data = Vec::new();
            if let Some(kind) = prefetched
                .bases
                .find_base(id, &mut data)
                .map_err(FetchError::Store)?
            {
                out.insert(*id, (kind, data));
            }
        }
        Ok(out)
    }

    /// Loads every commit of `ids` not yet known; ones found nowhere are
    /// recorded as absent. Refuses before holding more than the limit.
    async fn load(&mut self, ids: impl IntoIterator<Item = ObjectId>) -> Result<(), Failure> {
        let todo: Vec<ObjectId> = ids
            .into_iter()
            .filter(|id| !self.known.contains_key(id) && !self.absent.contains(id))
            .collect();
        if todo.is_empty() {
            return Ok(());
        }
        if self.known.len().saturating_add(todo.len()) > self.max_commits {
            return Err(Failure::refused(format!(
                "this shallow fetch walks more than {} commits, over this server's \
                 limit (max_shallow_commits); use a smaller --depth or a later \
                 --shallow-since",
                self.max_commits
            )));
        }
        let sorted = SortedIds::from_unsorted(todo);
        let rows = self
            .meta
            .lookup_commits(self.repo, &sorted)
            .await
            .map_err(FetchError::Store)?;
        let mut unindexed = Vec::new();
        for (id, row) in sorted.as_slice().iter().zip(rows) {
            match row {
                CommitLookup::Indexed(r) => {
                    self.known.insert(
                        *id,
                        Commit {
                            parents: r.parents,
                            time: r.committer_time,
                        },
                    );
                }
                CommitLookup::NotIndexed => unindexed.push(*id),
            }
        }
        if unindexed.is_empty() {
            return Ok(());
        }
        let mut found = self.read_from_packs(unindexed.clone()).await?;
        for id in unindexed {
            match found.remove(&id) {
                Some((ObjectKind::Commit, data)) => {
                    let (parents, time) = aspen_pack::scan::parse_commit(&data).map_err(|why| {
                        FetchError::Store(aspen_core::error::StoreError::Corrupt {
                            what: format!("stored commit {id} does not parse: {why}"),
                        })
                    })?;
                    self.known.insert(id, Commit { parents, time });
                }
                _ => {
                    self.absent.insert(id);
                }
            }
        }
        Ok(())
    }

    fn require(&self, id: &ObjectId) -> Result<&Commit, FetchError> {
        self.known
            .get(id)
            .ok_or(FetchError::CommitUnresolved { id: *id })
    }

    /// The commit an annotated tag (chain) points at; `None` if it ends at
    /// a tree or blob, as Git's `deref_tag` skips those.
    pub(crate) async fn peel(&mut self, mut id: ObjectId) -> Result<Option<ObjectId>, Failure> {
        for _ in 0..MAX_TAG_CHAIN {
            self.load([id]).await?;
            if self.known.contains_key(&id) {
                return Ok(Some(id));
            }
            let mut objects = self.read_from_packs(vec![id]).await?;
            match objects.remove(&id) {
                Some((ObjectKind::Tag, data)) => match tag_target(&data) {
                    Some(target) => id = target,
                    None => {
                        return Err(FetchError::Store(aspen_core::error::StoreError::Corrupt {
                            what: format!("stored tag {id} does not parse"),
                        })
                        .into());
                    }
                },
                Some(_) => return Ok(None),
                None => return Err(FetchError::CommitUnresolved { id }.into()),
            }
        }
        Ok(None)
    }
}

/// The `object <id>` line of a tag object.
fn tag_target(data: &[u8]) -> Option<ObjectId> {
    let line = data.split(|b| *b == b'\n').next()?;
    let hex = std::str::from_utf8(line.strip_prefix(b"object ")?).ok()?;
    ObjectId::parse_hex(hex).ok()
}

/// The `shallow-info` for a shallow request whose wants are `wants` (want
/// objects; tags are peeled here).
pub(crate) async fn compute<M: MetaStore, B: BlobStore>(
    objects: &mut Objects<'_, M, B>,
    repo: &Repo,
    request: &FetchRequest,
    wants: &[ObjectId],
) -> Result<ShallowInfo, Failure> {
    let Some(deepen) = &request.deepen else {
        // `shallow` lines (or a bare `deepen-relative`) without a depth:
        // Git registers the client's boundary and sends nothing.
        return Ok(ShallowInfo::default());
    };
    let mut heads = Vec::new();
    for w in wants {
        if let Some(c) = objects.peel(*w).await? {
            heads.push(c);
        }
    }
    heads.sort_unstable();
    heads.dedup();
    // The client's shallow commits this server knows; unknown ones are
    // ignored, as `git upload-pack` ignores them.
    objects.load(request.client_shallow.iter().copied()).await?;
    let client: HashSet<ObjectId> = request
        .client_shallow
        .iter()
        .filter(|c| objects.known.contains_key(c))
        .copied()
        .collect();
    let (border, not_shallow) = match deepen {
        Deepen::Depth(n) if n.get() >= INFINITE_DEPTH => (HashSet::new(), client.clone()),
        Deepen::Depth(n) => {
            let (from, depth) = if request.deepen_relative {
                (
                    reachable_client_shallows(objects, &heads, &client).await?,
                    n.get() + 1,
                )
            } else {
                (heads, n.get())
            };
            by_depth(objects, from, depth).await?
        }
        Deepen::Boundary { since, not } => {
            let mut not_tips = Vec::new();
            for rev in not {
                let tip = resolve_deepen_not(objects.meta, repo, rev).await?;
                if let Some(c) = objects.peel(tip).await? {
                    not_tips.push(c);
                }
            }
            by_boundary(objects, heads, *since, not_tips).await?
        }
    };
    let mut shallow: Vec<ObjectId> = border.difference(&client).copied().collect();
    let mut unshallow: Vec<ObjectId> = client.intersection(&not_shallow).copied().collect();
    shallow.sort_unstable();
    unshallow.sort_unstable();
    Ok(ShallowInfo { shallow, unshallow })
}

/// Breadth-first by depth from `from` (level 0): commits first reached at
/// level `depth - 1` are the boundary, those above it are not shallow.
pub(crate) async fn by_depth<M: MetaStore, B: BlobStore>(
    objects: &mut Objects<'_, M, B>,
    from: Vec<ObjectId>,
    depth: u32,
) -> Result<(HashSet<ObjectId>, HashSet<ObjectId>), Failure> {
    let mut border = HashSet::new();
    let mut not_shallow = HashSet::new();
    let mut seen: HashSet<ObjectId> = from.iter().copied().collect();
    let mut level = from;
    let mut d = 0;
    while !level.is_empty() {
        objects.load(level.iter().copied()).await?;
        if d + 1 >= depth {
            border.extend(level);
            break;
        }
        let mut next = Vec::new();
        for c in &level {
            for p in &objects.require(c)?.parents {
                if seen.insert(*p) {
                    next.push(*p);
                }
            }
        }
        not_shallow.extend(level);
        level = next;
        d += 1;
    }
    Ok((border, not_shallow))
}

/// The client's shallow commits reachable from `heads`: the walk stops at
/// each one (what lies below it, the client does not have).
async fn reachable_client_shallows<M: MetaStore, B: BlobStore>(
    objects: &mut Objects<'_, M, B>,
    heads: &[ObjectId],
    client: &HashSet<ObjectId>,
) -> Result<Vec<ObjectId>, Failure> {
    let mut found = Vec::new();
    let mut seen: HashSet<ObjectId> = heads.iter().copied().collect();
    let mut level: Vec<ObjectId> = heads.to_vec();
    while !level.is_empty() {
        objects.load(level.iter().copied()).await?;
        let mut next = Vec::new();
        for c in &level {
            if client.contains(c) {
                found.push(*c);
                continue;
            }
            for p in &objects.require(c)?.parents {
                if seen.insert(*p) {
                    next.push(*p);
                }
            }
        }
        level = next;
    }
    found.sort_unstable();
    Ok(found)
}

/// Every commit reachable from `tips`, inclusive.
async fn closure<M: MetaStore, B: BlobStore>(
    objects: &mut Objects<'_, M, B>,
    tips: Vec<ObjectId>,
) -> Result<HashSet<ObjectId>, Failure> {
    let mut seen: HashSet<ObjectId> = tips.iter().copied().collect();
    let mut level = tips;
    while !level.is_empty() {
        objects.load(level.iter().copied()).await?;
        let mut next = Vec::new();
        for c in &level {
            for p in &objects.require(c)?.parents {
                if seen.insert(*p) {
                    next.push(*p);
                }
            }
        }
        level = next;
    }
    Ok(seen)
}

/// `deepen-since` and `deepen-not`: included commits, and the border.
async fn by_boundary<M: MetaStore, B: BlobStore>(
    objects: &mut Objects<'_, M, B>,
    heads: Vec<ObjectId>,
    since: Option<i64>,
    not_tips: Vec<ObjectId>,
) -> Result<(HashSet<ObjectId>, HashSet<ObjectId>), Failure> {
    let excluded = closure(objects, not_tips).await?;
    let mut included = HashSet::new();
    let mut seen: HashSet<ObjectId> = heads.iter().copied().collect();
    let mut level = heads;
    while !level.is_empty() {
        objects.load(level.iter().copied()).await?;
        let mut next = Vec::new();
        for c in &level {
            let commit = objects.require(c)?;
            if excluded.contains(c) || since.is_some_and(|t| commit.time < t) {
                continue;
            }
            included.insert(*c);
            for p in &commit.parents {
                if seen.insert(*p) {
                    next.push(*p);
                }
            }
        }
        level = next;
    }
    if included.is_empty() {
        return Err(Failure::refused(
            "git upload-pack: no commits selected for shallow requests",
        ));
    }
    let mut border = HashSet::new();
    for c in &included {
        if objects
            .require(c)?
            .parents
            .iter()
            .any(|p| !included.contains(p))
        {
            border.insert(*c);
        }
    }
    let not_shallow = included.difference(&border).copied().collect();
    Ok((border, not_shallow))
}

/// Resolves a `deepen-not` value the way `git upload-pack`'s `expand_ref`
/// does: it must name exactly one ref under Git's lookup rules.
async fn resolve_deepen_not<M: MetaStore>(
    meta: &M,
    repo: &Repo,
    rev: &str,
) -> Result<ObjectId, Failure> {
    let mut tips = Vec::new();
    let candidates: Vec<String> = if rev == "HEAD" {
        vec![repo.settings.trunk().as_str().to_owned()]
    } else {
        [
            rev.to_owned(),
            format!("refs/{rev}"),
            format!("refs/tags/{rev}"),
            format!("refs/heads/{rev}"),
            format!("refs/remotes/{rev}"),
            format!("refs/remotes/{rev}/HEAD"),
        ]
        .into()
    };
    for c in candidates {
        let Ok(name) = RefName::new(c) else { continue };
        if let Some(tip) = meta
            .current_version(repo.id, &name)
            .await
            .map_err(FetchError::Store)?
            .and_then(|v| v.value.tip())
        {
            tips.push(tip);
        }
    }
    match tips.as_slice() {
        [one] => Ok(*one),
        [] => Err(Failure::refused(format!(
            "git upload-pack: deepen-not is not a ref: {}",
            aspen_proto::pkt::one_line(rev)
        ))),
        _ => Err(Failure::refused(format!(
            "git upload-pack: ambiguous deepen-not: {}",
            aspen_proto::pkt::one_line(rev)
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_targets() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let tag = format!("object {id}\ntype commit\ntag v1\ntagger A <a> 1 +0000\n\nmsg\n");
        assert_eq!(
            tag_target(tag.as_bytes()),
            Some(ObjectId::parse_hex(id).unwrap())
        );
        assert_eq!(tag_target(b"type commit\n"), None);
        assert_eq!(tag_target(b"object zz\n"), None);
    }
}

//! Where the ancestry walk reads commits during a push: `commit_index`
//! first, then the packs of the versions the question is about, for
//! commits whose rows are not written yet (decisions log: a missing row is
//! never read as "no parents"). Commits of the pushed pack are seeded into
//! the walker directly.

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{RefVersionId, RepoId};
use aspen_core::ingest::{BaseLookup, CandidateBases, ObjectKind, SearchPacks, SortedIds};
use aspen_core::meta::{CommitLookup, MetaStore};
use aspen_pack::locate::{PrefetchError, StoredPacks};

use super::gate::PrefetchGate;
use aspen_pack::scan::parse_commit;

use super::ancestry::{CommitNode, CommitSource, Lookup};
use super::closure::{Closures, closure_packs};

pub(crate) struct PushCommits<'a, M, B> {
    pub(crate) meta: &'a M,
    pub(crate) repo: RepoId,
    pub(crate) packs: &'a StoredPacks<M, B>,
    /// Every pack read holds a slot of this gate while it runs.
    pub(crate) gate: &'a PrefetchGate,
    pub(crate) closures: &'a Closures,
    /// Pack sets to read unindexed commits from, in order.
    pub(crate) fallback: Vec<SearchPacks>,
    /// Versions whose closures are searched after `fallback`, loaded only
    /// if a commit is still unknown by then.
    pub(crate) lazy_roots: Vec<RefVersionId>,
    pub(crate) lazy: tokio::sync::OnceCell<SearchPacks>,
}

impl<M, B> CommitSource for PushCommits<'_, M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    async fn lookup(&self, ids: &SortedIds) -> Result<Vec<Lookup>, StoreError> {
        let rows = self.meta.lookup_commits(self.repo, ids).await?;
        if rows.len() != ids.len() {
            return Err(StoreError::Corrupt {
                what: "commit index answered the wrong number of IDs".into(),
            });
        }
        let mut out: Vec<Lookup> = rows
            .into_iter()
            .map(|r| match r {
                CommitLookup::Indexed(row) => Lookup::Commit(CommitNode {
                    parents: row.parents,
                    time: row.committer_time,
                }),
                CommitLookup::NotIndexed => Lookup::Unknown,
            })
            .collect();
        for i in 0..=self.fallback.len() {
            let packs = match self.fallback.get(i) {
                Some(p) => p,
                None if self.lazy_roots.is_empty() => break,
                None => {
                    if !out.iter().any(|l| matches!(l, Lookup::Unknown)) {
                        break;
                    }
                    self.lazy
                        .get_or_try_init(|| {
                            closure_packs(self.meta, self.closures, self.repo, &self.lazy_roots)
                        })
                        .await?
                }
            };
            let unknown: Vec<_> = ids
                .as_slice()
                .iter()
                .zip(&out)
                .filter(|(_, l)| matches!(l, Lookup::Unknown))
                .map(|(id, _)| *id)
                .collect();
            if unknown.is_empty() {
                break;
            }
            let gate_slot = self.gate.acquire().await?;
            let fetched = match gate_slot
                .prefetch(
                    self.packs,
                    packs,
                    &CandidateBases(SortedIds::from_unsorted(unknown)),
                )
                .await
            {
                Ok((p, _)) => p.bases,
                Err(PrefetchError::Store(e)) => return Err(e),
                // A commit too large for the budget stays unknown, so the
                // question is undecided (a retryable `ng`), never guessed.
                Err(PrefetchError::OverBudget { .. }) => continue,
            };
            let mut buf = Vec::new();
            for (id, slot) in ids.as_slice().iter().zip(out.iter_mut()) {
                if !matches!(slot, Lookup::Unknown) {
                    continue;
                }
                match fetched.find_base(id, &mut buf)? {
                    Some(ObjectKind::Commit) => {
                        let (parents, time) =
                            parse_commit(&buf).map_err(|e| StoreError::Corrupt {
                                what: format!("stored commit {id}: {e}"),
                            })?;
                        *slot = Lookup::Commit(CommitNode { parents, time });
                    }
                    Some(_) => *slot = Lookup::NotCommit,
                    None => {}
                }
            }
            drop(gate_slot);
        }
        Ok(out)
    }
}

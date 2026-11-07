//! The `index_commits` job: `commit_index` rows for the commits of a large
//! push that the push itself did not write.
//!
//! The push path writes rows for up to `sync_commit_rows` commits per write
//! in its own transaction, and for a larger write only the tip's row plus an
//! `index_commits` job (decisions log, B1; S4 chose a second traversal of
//! the stored pack). Until the job runs, a walk that meets a commit without
//! a row reads it from the version's packs, so a missing row only costs
//! time; the job makes later fast-forward checks, `shallow-info` walks and
//! want resolutions cheap.
//!
//! The job also fills in generation numbers (decisions log, "12c:
//! generation numbers"): its own rows get them from the pack's commits and
//! their parents' rows, and then [`Compactor::backfill_generations`]
//! examines the repository's rows without one that no backfill has
//! examined yet, and revisits the rows waiting on a commit this job gave a
//! generation. A push writes the generation itself when it knows its
//! parents'; this job is how rows written before migration 0011, or after
//! a parent without a generation, get one.

use std::collections::{HashMap, VecDeque};

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::generation::{CommitGraph, Level, generations};
use aspen_core::id::{ObjectId, RefVersionId, RepoId};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{CommitLookup, CommitRow, MetaStore};
use aspen_pack::scan::{CommitInfo, ScanOptions, scan};

use crate::{CompactError, Compactor, local_io};

/// Rows written per `index_commits` call.
const BATCH: usize = 10_000;

/// Most rows the generation backfill reads per page; a page never takes
/// the rows held past [`crate::CompactConfig::backfill_held`].
const BACKFILL_PAGE: usize = 10_000;

/// What one `index_commits` job did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexReport {
    /// Commits of the version's pack that its tip reaches: the rows offered
    /// to the store (a commit that already has a row keeps it).
    pub rows: usize,
    /// Commits in the pack that the tip does not reach (another write's
    /// commits in a shared pack, or bases), which get no row from this
    /// version.
    pub unreached: usize,
    /// Rows of the repository given a generation number by this job, its
    /// own rows and the backfill's together.
    pub generations: usize,
}

impl<M, B> Compactor<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    /// The work of an `index_commits` job: a row, naming `version`, for
    /// every commit in the version's own pack that the version's tip
    /// reaches, read from the stored pack with every checksum verified (a
    /// blob that no longer matches its row must not yield rows; A2).
    ///
    /// Only commits the tip reaches get rows: `commit_index` maps a commit
    /// to a version whose tip reaches it, and a shared pack can hold other
    /// writes' commits. A version whose tip is not a commit (an annotated
    /// tag) gets no rows here; walks read its commits from its pack.
    pub async fn index_version(&self, version: RefVersionId) -> Result<IndexReport, CompactError> {
        let v = self
            .meta
            .ref_version(version)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                what: format!("ref version {version} of an index_commits job"),
            })?;
        let (Some(tip), Some(pack)) = (v.value.tip(), v.value.pack()) else {
            return Ok(IndexReport::default());
        };
        let row = self.meta.packs(&[pack]).await?.remove(0);
        let work = tempfile::Builder::new()
            .prefix("index-")
            .tempdir_in(&self.config.spool_dir)
            .map_err(|e| local_io("creating a spool directory", e))?;
        let (pack_path, idx_path) = (work.path().join("p.pack"), work.path().join("p.idx"));
        let keys = row.keys();
        self.fetch_to(&keys.pack, row.pack.size, &pack_path).await?;
        self.fetch_to(&keys.idx, row.pack.idx_size, &idx_path)
            .await?;
        let options = ScanOptions {
            commits: true,
            missing: false,
            thread_limit: None,
            max_object_bytes: self.config.max_object_bytes,
            max_inflated_bytes: self.config.max_inflated_bytes,
            verify_checksums: true,
        };
        let scanned = tokio::task::spawn_blocking(move || scan(&pack_path, &idx_path, options))
            .await
            .map_err(|e| local_io("scan task", e))?
            .map_err(|e| StoreError::Corrupt {
                what: format!("pack {}: {e}", row.id),
            })?;
        let total = scanned.commits.len();
        let mut rows = reached_rows(scanned.commits, tip, version);
        // Generations: the pack's commits from each other, and their
        // parents outside the pack from those parents' rows.
        let outside: Vec<ObjectId> = {
            let ours: std::collections::HashSet<ObjectId> = rows.iter().map(|r| r.commit).collect();
            rows.iter()
                .flat_map(|r| r.parents.iter().copied())
                .filter(|p| !ours.contains(p))
                .collect()
        };
        let known = self.known_generations(v.repo, outside).await?;
        let gens = generations(
            rows.iter().map(|r| (r.commit, r.parents.as_slice())),
            |id| known.get(id).copied(),
        );
        for r in &mut rows {
            r.generation = gens.get(&r.commit).copied().flatten();
        }
        for chunk in rows.chunks(BATCH) {
            self.meta.index_commits(v.repo, chunk).await?;
        }
        // A row that existed before (the tip's, which the push wrote
        // without a generation) keeps its columns on insert; give it the
        // generation separately. Never overwrites one.
        let set: Vec<(ObjectId, u64)> = rows
            .iter()
            .filter_map(|r| r.generation.map(|g| (r.commit, g)))
            .collect();
        for chunk in set.chunks(BATCH) {
            self.meta.set_generations(v.repo, chunk).await?;
        }
        let given: Vec<ObjectId> = set.iter().map(|(id, _)| *id).collect();
        let backfilled = self.backfill(v.repo, given).await?;
        Ok(IndexReport {
            rows: rows.len(),
            unreached: total - rows.len(),
            generations: set.len() + backfilled,
        })
    }

    /// The generations the commit index holds for `ids` (those with a row
    /// that has one).
    async fn known_generations(
        &self,
        repo: RepoId,
        ids: Vec<ObjectId>,
    ) -> Result<HashMap<ObjectId, u64>, CompactError> {
        let mut out = HashMap::new();
        let ids = SortedIds::from_unsorted(ids);
        for chunk in ids.as_slice().chunks(BATCH) {
            let chunk = SortedIds::from_unsorted(chunk.to_vec());
            let got = self.meta.lookup_commits(repo, &chunk).await?;
            for (id, l) in chunk.as_slice().iter().zip(got) {
                if let CommitLookup::Indexed(CommitRow {
                    generation: Some(g),
                    ..
                }) = l
                {
                    out.insert(*id, g);
                }
            }
        }
        Ok(out)
    }

    /// Gives rows of `repo` without a generation number one, where their
    /// parents' rows allow, computed from the rows' parent lists only
    /// (nothing reads packs or walks history beyond the rows), and written
    /// without overwriting a value another writer set. Returns how many rows
    /// it filled. An `index_commits` job runs it after writing its own rows.
    ///
    /// It examines each row once: the rows no backfill has examined yet
    /// ([`MetaStore::unexamined_commits`]), in rounds of at most
    /// [`crate::CompactConfig::backfill_held`] rows. A row it cannot fill
    /// records the commit it waits on, a parent (or an ancestor through
    /// parents that wait too) without a generation
    /// ([`MetaStore::set_generation_waits_on`]); it is looked at again only
    /// when a backfill gives that commit a generation
    /// ([`MetaStore::commits_waiting_on`]). So a row whose ancestor has no
    /// row at all costs one examination, not one per later job, and a row
    /// whose parent lies in a later round is filled when that round fills
    /// the parent: there is no pass over all rows to repeat.
    pub async fn backfill_generations(&self, repo: RepoId) -> Result<usize, CompactError> {
        self.backfill(repo, Vec::new()).await
    }

    /// [`Compactor::backfill_generations`], also revisiting the rows that
    /// wait on `given`, commits the caller just gave a generation.
    async fn backfill(&self, repo: RepoId, given: Vec<ObjectId>) -> Result<usize, CompactError> {
        let held = self.config.backfill_held.max(1);
        let mut filled = self.revisit(repo, given, held).await?;
        let mut cursor: Option<(RefVersionId, ObjectId)> = None;
        loop {
            let mut graph = CommitGraph::with_capacity(held.min(BACKFILL_PAGE));
            let mut exhausted = false;
            while graph.len() < held {
                let want = (held - graph.len()).min(BACKFILL_PAGE);
                let page = self.meta.unexamined_commits(repo, cursor, want).await?;
                if let Some(last) = page.last() {
                    cursor = Some((last.ref_version, last.commit));
                }
                for r in &page {
                    graph.push(r.commit, &r.parents);
                }
                if page.len() < want {
                    exhausted = true;
                    break;
                }
            }
            if !graph.is_empty() {
                let (n, now_known) = self.examine(repo, graph).await?;
                filled += n + self.revisit(repo, now_known, held).await?;
            }
            if exhausted {
                return Ok(filled);
            }
        }
    }

    /// Revisits the rows waiting on `frontier`, commits that have a
    /// generation now, and in turn the rows waiting on those it fills,
    /// until no row waits on a commit with one. Each fetched row leaves the
    /// set it was fetched from (it gets a generation, or waits on a commit
    /// without one), so this ends.
    async fn revisit(
        &self,
        repo: RepoId,
        mut frontier: Vec<ObjectId>,
        held: usize,
    ) -> Result<usize, CompactError> {
        let mut filled = 0;
        while !frontier.is_empty() {
            let take = frontier.len().min(BATCH);
            let ids = SortedIds::from_unsorted(frontier.split_off(frontier.len() - take));
            loop {
                let rows = self.meta.commits_waiting_on(repo, &ids, held).await?;
                let full = rows.len() >= held;
                if rows.is_empty() {
                    break;
                }
                let mut graph = CommitGraph::with_capacity(rows.len());
                for r in &rows {
                    graph.push(r.commit, &r.parents);
                }
                drop(rows);
                let (n, now_known) = self.examine(repo, graph).await?;
                filled += n;
                frontier.extend(now_known);
                if !full {
                    break;
                }
            }
        }
        Ok(filled)
    }

    /// Solves `graph`'s rows from their parents' stored generations, and
    /// writes each a generation or the commit it waits on. Returns how many
    /// rows it filled and the commits that now have a generation whose rows
    /// may be waited on: those it filled, and any commit it recorded a row
    /// waiting on that got a generation meanwhile (another job filled it
    /// after this one read it, and may have revisited its waiting rows
    /// before this one's wait was written).
    async fn examine(
        &self,
        repo: RepoId,
        mut graph: CommitGraph,
    ) -> Result<(usize, Vec<ObjectId>), CompactError> {
        let known = self
            .known_generations(repo, graph.outside_parents())
            .await?;
        let solved = graph.solve(|id| known.get(id).copied());
        drop(known);
        let mut now_known: Vec<ObjectId> = Vec::new();
        let mut waited: Vec<ObjectId> = Vec::new();
        let (mut set, mut waits) = (Vec::with_capacity(BATCH), Vec::with_capacity(BATCH));
        for (id, level) in solved.iter() {
            match level {
                Level::Known(g) => {
                    set.push((id, g));
                    now_known.push(id);
                }
                Level::Unknown { waits_on } => {
                    waits.push((id, waits_on));
                    waited.push(waits_on);
                }
            }
            if set.len() == BATCH {
                self.meta.set_generations(repo, &set).await?;
                set.clear();
            }
            if waits.len() == BATCH {
                self.meta.set_generation_waits_on(repo, &waits).await?;
                waits.clear();
            }
        }
        self.meta.set_generations(repo, &set).await?;
        self.meta.set_generation_waits_on(repo, &waits).await?;
        let filled = now_known.len();
        // Read after the waits are written: see above.
        now_known.extend(self.known_generations(repo, waited).await?.into_keys());
        Ok((filled, now_known))
    }
}

/// The rows for the commits of `commits` that `tip` reaches through
/// parents inside `commits`, in breadth-first order from the tip.
fn reached_rows(commits: Vec<CommitInfo>, tip: ObjectId, version: RefVersionId) -> Vec<CommitRow> {
    let mut by_id: HashMap<ObjectId, CommitInfo> = commits.into_iter().map(|c| (c.id, c)).collect();
    let mut out = Vec::new();
    let mut queue = VecDeque::from([tip]);
    while let Some(id) = queue.pop_front() {
        let Some(c) = by_id.remove(&id) else { continue };
        queue.extend(c.parents.iter().copied());
        out.push(CommitRow {
            commit: c.id,
            ref_version: version,
            parents: c.parents,
            committer_time: c.committer_time,
            // Filled in by the caller, which knows the parents' rows.
            generation: None,
            root_tree: Some(c.tree),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }

    fn c(n: u8, parents: &[u8]) -> CommitInfo {
        CommitInfo {
            id: id(n),
            parents: parents.iter().map(|&p| id(p)).collect(),
            committer_time: i64::from(n),
            tree: id(0),
        }
    }

    #[test]
    fn only_commits_the_tip_reaches_get_rows() {
        // 4 -> 3 -> 1 and 4 -> 2 (a merge), 5 is another write's commit in a
        // shared pack, and 1's parent 0 is in a dependency's pack.
        let commits = vec![
            c(1, &[0]),
            c(2, &[1]),
            c(3, &[1]),
            c(4, &[3, 2]),
            c(5, &[1]),
        ];
        let v = RefVersionId::new(9).unwrap();
        let rows = reached_rows(commits, id(4), v);
        let mut got: Vec<ObjectId> = rows.iter().map(|r| r.commit).collect();
        got.sort();
        assert_eq!(got, vec![id(1), id(2), id(3), id(4)]);
        assert!(rows.iter().all(|r| r.ref_version == v));
        let four = rows.iter().find(|r| r.commit == id(4)).unwrap();
        assert_eq!(four.parents, vec![id(3), id(2)], "parents keep their order");
        assert_eq!(four.committer_time, 4);
    }

    #[test]
    fn a_tip_outside_the_pack_gets_no_rows() {
        let rows = reached_rows(vec![c(1, &[])], id(7), RefVersionId::new(1).unwrap());
        assert!(rows.is_empty());
    }
}

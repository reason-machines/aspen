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

use std::collections::{HashMap, VecDeque};

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, RefVersionId};
use aspen_core::meta::{CommitRow, MetaStore};
use aspen_pack::scan::{CommitInfo, ScanOptions, scan};

use crate::{CompactError, Compactor, local_io};

/// Rows written per `index_commits` call.
const BATCH: usize = 10_000;

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
        let rows = reached_rows(scanned.commits, tip, version);
        let report = IndexReport {
            rows: rows.len(),
            unreached: total - rows.len(),
        };
        for chunk in rows.chunks(BATCH) {
            self.meta.index_commits(v.repo, chunk).await?;
        }
        Ok(report)
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

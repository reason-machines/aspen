//! Compaction heat: which refs fetches are waiting on compaction for.
//!
//! A fetch whose plan sends a version's own push pack, where a cover's pack
//! would have stood in for it once the cover is built, pays for compaction
//! being behind: more packs to stitch or to hand out as URIs. Each served
//! fetch counts, per ref, how many versions it sent that way and the
//! earliest of them (`heat_of`), and this module writes the sums to the
//! job queue ([`MetaStore::add_compaction_heat`]), which claims the hottest
//! `compact` jobs first. So when compaction has a backlog, the covers that
//! fetches read are built before the ones nobody is reading.
//!
//! The write is off the fetch's path and batched: a process sums heat in
//! memory and writes it at most once per [`HEAT_FLUSH_EVERY`], from a
//! spawned task, one transaction per write. Heat is advisory: a lost write
//! (a store error, a process that stops) only changes the order jobs run
//! in. At most [`MAX_PENDING_REFS`] refs are summed between writes; heat
//! for more is dropped until the next write.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use aspen_core::id::{PackId, RepoId, Seq};
use aspen_core::meta::{CompactionHeat, MetaStore};
use aspen_core::name::RefName;
use aspen_core::pack::{PackLevel, PackRecord};

use super::graph::LoadedGraph;

/// The longest one process keeps heat before writing it.
pub const HEAT_FLUSH_EVERY: Duration = Duration::from_secs(1);

/// Most refs one process sums heat for between writes.
pub const MAX_PENDING_REFS: usize = 10_000;

/// Heat summed in memory, and when it was last written.
#[derive(Debug, Default)]
pub(crate) struct HeatNotes {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    pending: HashMap<(RepoId, RefName), (Seq, u64)>,
    /// When the last write was started.
    last_write: Option<Instant>,
    /// Whether a write is running (at most one per process).
    writing: bool,
}

impl HeatNotes {
    /// Adds one fetch's heat. Returns the heat to write now, if a write is
    /// due (none running, and the last one started at least
    /// [`HEAT_FLUSH_EVERY`] ago); the caller writes it and then calls
    /// [`HeatNotes::written`].
    pub(crate) fn add(&self, heat: Vec<CompactionHeat>) -> Option<Vec<CompactionHeat>> {
        let mut s = self.state.lock().expect("heat lock");
        for h in heat {
            let full = s.pending.len() >= MAX_PENDING_REFS;
            let key = (h.repo, h.name);
            match s.pending.get_mut(&key) {
                Some((from, sum)) => {
                    *from = (*from).min(h.from);
                    *sum = sum.saturating_add(h.heat);
                }
                None if !full => {
                    s.pending.insert(key, (h.from, h.heat));
                }
                None => {}
            }
        }
        let due = s.last_write.is_none_or(|t| t.elapsed() >= HEAT_FLUSH_EVERY);
        if s.writing || !due || s.pending.is_empty() {
            return None;
        }
        s.writing = true;
        s.last_write = Some(Instant::now());
        Some(
            s.pending
                .drain()
                .map(|((repo, name), (from, heat))| CompactionHeat {
                    repo,
                    name,
                    from,
                    heat,
                })
                .collect(),
        )
    }

    /// Ends the write [`HeatNotes::add`] handed out.
    pub(crate) fn written(&self) {
        self.state.lock().expect("heat lock").writing = false;
    }
}

/// One plan's heat: per ref, the versions the graph holds whose own push
/// pack (level 0) the plan sends, and the earliest of them. A push pack
/// shared by several versions counts for each version naming it.
pub(crate) fn heat_of(
    repo: RepoId,
    graph: &LoadedGraph,
    rows: &[PackRecord],
) -> Vec<CompactionHeat> {
    let sent: HashSet<PackId> = rows
        .iter()
        .filter(|r| r.pack.level() == PackLevel::L0)
        .map(|r| r.id)
        .collect();
    if sent.is_empty() {
        return Vec::new();
    }
    let mut by_ref: HashMap<&RefName, (Seq, u64)> = HashMap::new();
    for v in graph.versions() {
        if v.value.pack().is_some_and(|p| sent.contains(&p)) {
            let e = by_ref.entry(&v.name).or_insert((v.seq, 0));
            e.0 = e.0.min(v.seq);
            e.1 += 1;
        }
    }
    by_ref
        .into_iter()
        .map(|(name, (from, heat))| CompactionHeat {
            repo,
            name: name.clone(),
            from,
            heat,
        })
        .collect()
}

/// Writes heat handed out by [`HeatNotes::add`], then ends the write.
pub(crate) async fn write<M: MetaStore>(
    meta: &M,
    notes: &HeatNotes,
    heat: Vec<CompactionHeat>,
) -> Result<u64, aspen_core::error::StoreError> {
    let result = meta.add_compaction_heat(&heat).await;
    notes.written();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(repo: i64, name: &str, from: u64, heat: u64) -> CompactionHeat {
        CompactionHeat {
            repo: RepoId::new(repo).unwrap(),
            name: RefName::new(name).unwrap(),
            from: Seq::new(from).unwrap(),
            heat,
        }
    }

    /// Invariant: heat is summed per ref with the earliest `from`, and
    /// handed out at most once per interval and never while a write runs.
    #[test]
    fn heat_is_summed_and_written_at_most_once_per_interval() {
        let notes = HeatNotes::default();
        let first = notes
            .add(vec![h(1, "refs/heads/main", 40, 3)])
            .expect("the first write is due at once");
        assert_eq!(first, vec![h(1, "refs/heads/main", 40, 3)]);
        // While it runs, and within the interval after it, heat is summed.
        assert!(notes.add(vec![h(1, "refs/heads/main", 20, 2)]).is_none());
        notes.written();
        assert!(notes.add(vec![h(1, "refs/heads/main", 30, 5)]).is_none());
        assert!(notes.add(vec![h(2, "refs/heads/main", 1, 1)]).is_none());
        std::thread::sleep(HEAT_FLUSH_EVERY);
        let mut next = notes.add(Vec::new()).expect("due after the interval");
        next.sort_by_key(|c| c.repo);
        assert_eq!(
            next,
            vec![
                h(1, "refs/heads/main", 20, 7),
                h(2, "refs/heads/main", 1, 1)
            ]
        );
        assert!(notes.add(Vec::new()).is_none(), "nothing pending");
    }

    /// Invariant: memory stays bounded however many refs are fetched.
    #[test]
    fn pending_refs_are_capped() {
        let notes = HeatNotes::default();
        notes.add(vec![h(1, "refs/heads/a", 1, 1)]).unwrap();
        let many = (0..MAX_PENDING_REFS + 10)
            .map(|i| h(1, &format!("refs/heads/b{i}"), 1, 1))
            .collect();
        assert!(notes.add(many).is_none());
        assert_eq!(notes.state.lock().unwrap().pending.len(), MAX_PENDING_REFS);
    }
}

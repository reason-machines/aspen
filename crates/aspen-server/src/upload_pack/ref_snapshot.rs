//! In-memory ref listings for `ls-refs` (throughput item 6c).
//!
//! Stock `git ls-remote <url> <ref>` sends no `ref-prefix` (its patterns
//! match the end of a ref name, not its start), so every poll asks for
//! every ref. Listing them from the store on each poll costs a scan of the
//! repository's refs. Instead each instance keeps, per repository, the
//! refs it last read with the store's [`RefCursor`], and every `ls-refs`
//! asks the store only what changed since ([`MetaStore::ref_changes`]):
//! one statement, which reads no ref rows when nothing moved.
//!
//! **Staleness: none.** A request never answers from what an earlier
//! request read. It answers from a store read that began after the request
//! arrived: its own, or one that another request started after it arrived
//! (concurrent requests for one repository share one read this way, so a
//! burst of polls costs about one statement per round, not one each). A
//! push is acknowledged only after its transaction committed, so any
//! `ls-refs` that reaches any instance after the acknowledgement lists it,
//! whichever instance took the push. The store's cursor is exact whatever
//! order concurrent pushes commit in (`aspen_meta`'s `ref_changes`
//! module), so applying each answer to the held refs gives exactly the refs
//! at the moment of that read.
//!
//! **Memory:** at most [`ServerConfig::ls_refs_cache_refs`] refs across all
//! repositories (each repository counts one more, for its entry), least
//! recently used repositories dropped first. A repository with more live
//! refs than that is listed from the store page by page, as before, and
//! checked again after [`TOO_LARGE_RETRY`].
//!
//! [`ServerConfig::ls_refs_cache_refs`]: crate::ServerConfig::ls_refs_cache_refs

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, RepoId};
use aspen_core::meta::{MetaStore, RefChanges, RefCursor};
use aspen_core::name::RefName;

/// A repository's live refs and their tips, in name order.
pub(crate) type Refs = Arc<BTreeMap<RefName, ObjectId>>;

/// How long a repository found to have more refs than the budget is
/// listed from the store before it is tried again.
pub(crate) const TOO_LARGE_RETRY: Duration = Duration::from_secs(60);

/// Every repository's held refs, on one instance.
#[derive(Debug)]
pub(crate) struct RefSnapshots {
    /// Most refs held across repositories.
    max_refs: usize,
    slots: Mutex<HashMap<RepoId, Arc<Slot>>>,
    /// Orders uses, for least-recently-used eviction.
    clock: AtomicU64,
    /// The store cannot answer `ref_changes`; every listing goes to it.
    unsupported: AtomicBool,
}

#[derive(Debug, Default)]
struct Slot {
    state: tokio::sync::Mutex<State>,
    last_used: AtomicU64,
    /// Refs held, for the budget.
    len: AtomicUsize,
}

#[derive(Debug, Default)]
struct State {
    held: Option<Held>,
    /// When the store read behind `held` began.
    read_at: Option<Instant>,
    /// Too many refs to hold until then.
    too_large_until: Option<Instant>,
}

#[derive(Debug)]
struct Held {
    cursor: RefCursor,
    refs: Refs,
}

impl RefSnapshots {
    /// Holds at most `max_refs` refs (at least 1).
    pub(crate) fn new(max_refs: usize) -> Self {
        Self {
            max_refs: max_refs.max(1),
            slots: Mutex::default(),
            clock: AtomicU64::new(0),
            unsupported: AtomicBool::new(false),
        }
    }

    /// The live refs of `repo` as of a store read that began after this
    /// call, or `None` when they are not held (the store cannot say what
    /// changed, or the repository has more refs than the budget): list
    /// them from the store instead.
    pub(crate) async fn refs<M: MetaStore>(
        &self,
        meta: &M,
        repo: RepoId,
    ) -> Result<Option<Refs>, StoreError> {
        let arrived = Instant::now();
        if self.unsupported.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let slot = self.slot(repo);
        let mut state = slot.state.lock().await;
        // A read that began after this request arrived saw every push
        // acknowledged before it: share it.
        if let (Some(held), Some(read_at)) = (&state.held, state.read_at)
            && read_at > arrived
        {
            return Ok(Some(Arc::clone(&held.refs)));
        }
        if state.too_large_until.is_some_and(|t| t > arrived) {
            return Ok(None);
        }
        let read_at = Instant::now();
        let limit = u32::try_from(self.max_refs).unwrap_or(u32::MAX);
        let since = state.held.as_ref().map(|h| &h.cursor);
        let Some(mut changes) = meta.ref_changes(repo, since, limit).await? else {
            self.unsupported.store(true, Ordering::Relaxed);
            return Ok(None);
        };
        if !changes.complete && since.is_some() {
            // More refs moved than the budget holds: start over.
            state.held = None;
            changes = meta
                .ref_changes(repo, None, limit)
                .await?
                .expect("the store answered ref_changes a moment ago");
        }
        let RefChanges {
            cursor,
            refs: changed,
            complete,
        } = changes;
        let mut refs = state.held.take().map_or_else(Refs::default, |h| h.refs);
        if complete && !changed.is_empty() {
            let map = Arc::make_mut(&mut refs);
            for change in changed {
                match change.tip {
                    Some(tip) => map.insert(change.name, tip),
                    None => map.remove(&change.name),
                };
            }
        }
        // A short list of changes can still grow the listing past the
        // budget: such a repository is not held either.
        if !complete || refs.len() > self.max_refs {
            *state = State {
                too_large_until: Some(read_at + TOO_LARGE_RETRY),
                ..State::default()
            };
            slot.len.store(0, Ordering::Relaxed);
            return Ok(None);
        }
        slot.len.store(refs.len(), Ordering::Relaxed);
        state.held = Some(Held {
            cursor,
            refs: Arc::clone(&refs),
        });
        state.read_at = Some(read_at);
        state.too_large_until = None;
        drop(state);
        self.evict(repo);
        Ok(Some(refs))
    }

    /// The slot of `repo`, created if needed, marked used now.
    fn slot(&self, repo: RepoId) -> Arc<Slot> {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut slots = self.slots.lock().expect("ref snapshot slots");
        let slot = Arc::clone(slots.entry(repo).or_default());
        slot.last_used.store(now, Ordering::Relaxed);
        slot
    }

    /// Drops least recently used repositories other than `keep` until the
    /// held refs fit the budget.
    fn evict(&self, keep: RepoId) {
        let mut slots = self.slots.lock().expect("ref snapshot slots");
        let weight = |s: &Slot| s.len.load(Ordering::Relaxed) + 1;
        let mut total: usize = slots.values().map(|s| weight(s)).sum();
        while total > self.max_refs {
            let Some((&victim, slot)) = slots
                .iter()
                .filter(|(id, _)| **id != keep)
                .min_by_key(|(_, s)| s.last_used.load(Ordering::Relaxed))
            else {
                return;
            };
            total -= weight(slot);
            slots.remove(&victim);
        }
    }

    /// Repositories held, for tests.
    #[cfg(test)]
    fn held(&self) -> Vec<RepoId> {
        let mut ids: Vec<RepoId> = self
            .slots
            .lock()
            .expect("ref snapshot slots")
            .keys()
            .copied()
            .collect();
        ids.sort();
        ids
    }
}

#[cfg(test)]
mod tests;

//! Eviction. Staleness, sharing and statement counts are tested end to end
//! in `tests/ls_refs_snapshot.rs`.

use super::*;

use aspen_core::name::RepoName;
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;

/// Invariant: held refs stay within the budget, each repository counting
/// one more than its refs, and the least recently used repository is
/// dropped first; a repository larger than the budget is not held at all.
#[tokio::test]
async fn the_budget_drops_the_least_recently_used_repository() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("m.db").display());
    let meta = SqlStore::connect(&url).await.unwrap();
    meta.migrate().await.unwrap();
    let main = RefName::new("refs/heads/main").unwrap();
    let mut repos = Vec::new();
    for name in ["a", "b", "c"] {
        let repo = meta
            .create_repo(
                &RepoName::new(name).unwrap(),
                &RepoSettings::new(main.clone()).unwrap(),
            )
            .await
            .unwrap();
        repos.push(repo.id);
    }
    // Empty repositories weigh 1 each: a budget of 2 holds two of them.
    let snapshots = RefSnapshots::new(2);
    for id in &repos {
        assert!(snapshots.refs(&meta, *id).await.unwrap().is_some());
    }
    assert_eq!(snapshots.held(), vec![repos[1], repos[2]]);
    // Using a again drops b, now the least recently used.
    snapshots.refs(&meta, repos[0]).await.unwrap();
    assert_eq!(snapshots.held(), vec![repos[0], repos[2]]);
}

/// Invariant: a repository with more live refs than the budget is not
/// held (the caller lists it from the store), and it is not read again
/// until [`TOO_LARGE_RETRY`] has passed; one that fits is held and its
/// changes applied, deletions included.
#[tokio::test]
async fn a_repository_over_the_budget_is_listed_from_the_store() {
    use aspen_core::id::PackChecksum;
    use aspen_core::meta::{CommitOutcome, PushCommit, RefWrite};
    use aspen_core::pack::{NewPack, PackOrigin, entry_table_len};
    use aspen_core::refs::{Closure, Deps, RefValue};

    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("m.db").display());
    let meta = SqlStore::connect(&url).await.unwrap();
    meta.migrate().await.unwrap();
    let main = RefName::new("refs/heads/main").unwrap();
    let repo = meta
        .create_repo(
            &RepoName::new("r").unwrap(),
            &RepoSettings::new(main.clone()).unwrap(),
        )
        .await
        .unwrap()
        .id;
    let pack = meta
        .register_pack(&NewPack {
            repo,
            checksum: PackChecksum::from_bytes([1; 20]),
            size: 100,
            idx_size: 1072 + 28,
            entries_size: entry_table_len(1),
            object_count: 1,
            ref_delta_count: 0,
            origin: PackOrigin::Push { received_bytes: 80 },
        })
        .await
        .unwrap()
        .id;
    let tip = ObjectId::from_bytes([7; 20]);
    let write = |name: &str, expected| RefWrite {
        name: RefName::new(name).unwrap(),
        expected,
        value: RefValue::Live {
            tip,
            pack: Some(pack),
            closure: Closure::Exact,
        },
        deps: Deps::default(),
        commits: Vec::new(),
        jobs: Vec::new(),
    };
    let commit = |writes| {
        let meta = meta.clone();
        async move {
            match meta
                .commit_push(&PushCommit { repo, writes })
                .await
                .unwrap()
            {
                CommitOutcome::Committed(v) => v,
                other => panic!("{other:?}"),
            }
        }
    };
    let created = commit(vec![
        write("refs/heads/a", None),
        write("refs/heads/b", None),
    ])
    .await;

    let snapshots = RefSnapshots::new(2);
    let held = snapshots.refs(&meta, repo).await.unwrap().unwrap();
    assert_eq!(held.len(), 2);

    // A deletion and a create: still two refs, applied as changes.
    let mut delete = write("refs/heads/a", Some(created[0].id));
    delete.value = RefValue::Deleted;
    commit(vec![delete, write("refs/heads/c", None)]).await;
    let held = snapshots.refs(&meta, repo).await.unwrap().unwrap();
    let names: Vec<&str> = held.keys().map(RefName::as_str).collect();
    assert_eq!(names, ["refs/heads/b", "refs/heads/c"]);

    // A third live ref: over the budget, so not held, and not retried at once.
    commit(vec![write("refs/heads/d", None)]).await;
    assert!(snapshots.refs(&meta, repo).await.unwrap().is_none());
    assert!(snapshots.refs(&meta, repo).await.unwrap().is_none());
    let slot = snapshots.slot(repo);
    let state = slot.state.lock().await;
    assert!(state.held.is_none());
    assert!(state.too_large_until.is_some());
}

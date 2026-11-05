//! Memory and history limits of the receive-pack service: the per-push
//! prefetch budget (A2 decisions row: refused, not retryable), the gate that
//! shares one budget across concurrent pushes (PLAN section 3, B1 row), and
//! fast-forward checks over commits whose index rows are not written yet.

mod receive_support;

use receive_support::*;

async fn push(w: &Work, url: &str, args: &[&str]) -> (bool, String) {
    let mut cmd = w.command();
    cmd.arg("push").arg(url).args(args);
    blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await
}

fn big(tag: &str) -> String {
    (0..20_000).map(|i| format!("{tag} {i}\n")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_whose_bases_exceed_the_budget_is_refused_for_good() {
    // The blob is about 200 KB; the budget holds less than that.
    let s = Server::start_with(
        |c| c.locate.max_prefetch_bytes = 64 * 1024,
        Default::default(),
    )
    .await;
    let w = s.clone_dir("w");
    let content = big("x");
    w.commit("big.txt", &content, "one");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let before = s.live("refs/heads/main").await;
    w.commit("big.txt", &format!("{content}tail\n"), "two");
    let (ok, out) = push(&w, &s.url(), &["main"]).await;
    assert!(!ok);
    assert!(
        out.contains("delta bases need more than 65536 bytes"),
        "{out}"
    );
    assert!(!out.contains("retry"), "not retryable: {out}");
    assert_eq!(s.live("refs/heads/main").await.id, before.id);
    assert_eq!(s.stored_pack_files(), 1, "nothing new is stored");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_gate_shares_one_budget_across_concurrent_pushes() {
    // A shared budget equal to one push's budget: one push at a time in
    // prefetch and completion; the others wait their turn and succeed.
    let s = Server::start_with(
        |c| {
            c.locate.max_prefetch_bytes = 32 * MIB;
            c.prefetch_memory_bytes = 32 * MIB;
        },
        Default::default(),
    )
    .await;
    assert_eq!(s.svc.prefetch_slots(), 1);
    let w = s.clone_dir("base");
    let content: String = (0..10).map(|i| big(&format!("shared {i}"))).collect();
    w.commit("big.txt", &content, "base");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let base = oid(&w.rev("HEAD:big.txt"));
    let base_obj = Obj::blob(content.clone().into_bytes());
    assert_eq!(base_obj.id(), base);
    // Eight thin pushes, each a REF_DELTA against the stored blob, sent to
    // the service at the same instant (no client in between to stagger
    // them).
    let mut bodies = Vec::new();
    for i in 0..8 {
        let blob = Obj::blob(format!("{content}writer {i}\n").into_bytes());
        let tree = Obj::tree(&[("100644", "big.txt", blob.id())]);
        let commit = Obj::commit(
            tree.id(),
            &[oid(&w.rev("HEAD"))],
            &format!("w{i}"),
            1_700_000_000,
        );
        let pack = build_pack(&[
            Entry::RefDelta {
                base: base_obj.clone(),
                target: blob,
            },
            Entry::Full(tree),
            Entry::Full(commit.clone()),
        ]);
        let name = format!("refs/heads/w{i}");
        bodies.push((
            name.clone(),
            commit.id(),
            request(
                &[(aspen_core::id::ObjectId::ZERO, commit.id(), &name)],
                &[],
                &pack,
            ),
        ));
    }
    let mut tasks = Vec::new();
    for (name, tip, body) in bodies {
        let svc = s.svc.clone();
        let repo = s.repo.clone();
        tasks.push(tokio::spawn(async move {
            let out = svc
                .push(
                    &repo,
                    None,
                    &aspen_server::receive::AnyRef,
                    None,
                    std::io::Cursor::new(body),
                )
                .await
                .unwrap();
            (
                name,
                tip,
                String::from_utf8_lossy(&out.response).into_owned(),
            )
        }));
    }
    for t in tasks {
        let (name, tip, report) = t.await.unwrap();
        assert!(report.contains(&format!("ok {name}")), "{report}");
        assert_eq!(s.live(&name).await.value.tip().unwrap(), tip);
    }
    assert_eq!(
        s.svc.prefetch_peak(),
        1,
        "never two pushes in prefetch at once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fast_forward_checks_read_unindexed_commits_from_the_packs() {
    // Only tips get commit rows, so the walk from c3 back to c1 must read
    // c2 from the stored pack (decisions log: a missing row is never "no
    // parents").
    let s = Server::start_with(|c| c.sync_commit_rows = 0, Default::default()).await;
    let w = s.clone_dir("w");
    let c1 = w.commit("a.txt", "1\n", "c1");
    assert!(
        push(&w, &s.url(), &["main", "main:refs/heads/topic"])
            .await
            .0
    );
    w.commit("a.txt", "2\n", "c2");
    let c3 = w.commit("a.txt", "3\n", "c3");
    assert!(push(&w, &s.url(), &["main"]).await.0);
    let rows = aspen_core::meta::MetaStore::lookup_commits(
        &s.meta,
        s.repo.id,
        &aspen_core::ingest::SortedIds::from_unsorted(vec![oid(&w.rev("HEAD~1"))]),
    )
    .await
    .unwrap();
    assert!(
        matches!(rows[0], aspen_core::meta::CommitLookup::NotIndexed),
        "c2 has no row"
    );
    // topic: c1 -> c3 is a fast-forward through the unindexed c2.
    let (ok, out) = push(&w, &s.url(), &["main:refs/heads/topic"]).await;
    assert!(ok, "{out}");
    assert_eq!(
        s.live("refs/heads/topic").await.value.tip().unwrap(),
        oid(&c3)
    );
    let _ = c1;
}

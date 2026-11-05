//! Pushes from libgit2 (through pygit2, as in A6's client matrix), which
//! writes every delta as a REF_DELTA, mostly against bases in its own pack
//! (S1's `push_order.py`).
//!
//! Runs when `python3` can import `pygit2` (set `ASPEN_TEST_PYGIT2_PATH` to
//! add a directory to `PYTHONPATH`); skips otherwise, unless
//! `ASPEN_TEST_LIBGIT2=1` demands it. The nightly client matrix runs it
//! with pygit2 installed.

mod receive_support;

use std::process::Command;

use aspen_core::refs::Closure;
use receive_support::*;

const SCRIPT: &str = r#"
import os, sys, pygit2
url, path, step = sys.argv[1], sys.argv[2], int(sys.argv[3])
sig = pygit2.Signature("Aspen libgit2", "libgit2@aspen.invalid", 1700000000 + step, 0)
if step == 0:
    repo = pygit2.init_repository(path, initial_head="main")
    repo.remotes.create("origin", url)
    parents = []
else:
    repo = pygit2.Repository(path)
    parents = [repo.head.target]
text = "".join(f"line {i}\n" for i in range(3000)) + "".join(f"step {s}\n" for s in range(step + 1))
for name, data in [("big.txt", text), (f"f{step}.txt", f"file {step}\n")]:
    with open(os.path.join(path, name), "w") as f:
        f.write(data)
    repo.index.add(name)
repo.index.write()
tree = repo.index.write_tree()
repo.create_commit("refs/heads/main", sig, sig, f"step {step}", tree, parents)

class Cb(pygit2.RemoteCallbacks):
    def push_update_reference(self, refname, message):
        if message:
            sys.exit(f"rejected {refname}: {message}")

repo.remotes["origin"].push(["refs/heads/main:refs/heads/main"], callbacks=Cb())
print(repo.head.target)
"#;

fn python() -> Option<Command> {
    let mut c = Command::new("python3");
    if let Some(p) = std::env::var_os("ASPEN_TEST_PYGIT2_PATH") {
        c.env("PYTHONPATH", p);
    }
    let ok = {
        let mut probe = Command::new("python3");
        if let Some(p) = std::env::var_os("ASPEN_TEST_PYGIT2_PATH") {
            probe.env("PYTHONPATH", p);
        }
        probe
            .args(["-c", "import pygit2"])
            .output()
            .is_ok_and(|o| o.status.success())
    };
    if !ok {
        assert!(
            std::env::var("ASPEN_TEST_LIBGIT2").as_deref() != Ok("1"),
            "ASPEN_TEST_LIBGIT2=1 but pygit2 is not importable"
        );
        eprintln!("skipping: pygit2 is not importable (set ASPEN_TEST_PYGIT2_PATH)");
        return None;
    }
    Some(c)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn libgit2_pushes_are_stored_self_contained_and_exact() {
    if python().is_none() {
        return;
    }
    let s = Server::start().await;
    let path = s.dir.path().join("lg2");
    let script = s.dir.path().join("push.py");
    std::fs::write(&script, SCRIPT).unwrap();
    for step in 0..3 {
        let mut cmd = python().unwrap();
        cmd.arg(&script)
            .arg(s.url())
            .arg(&path)
            .arg(step.to_string());
        let out = blocking(move || cmd.output().unwrap()).await;
        assert!(
            out.status.success(),
            "libgit2 push {step}: {}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let w = Work {
            path: path.clone(),
            home: s.home.clone(),
        };
        let v = s.check_ref("refs/heads/main", &w).await;
        assert_eq!(v.value.closure(), Some(Closure::Exact));
        assert_eq!(v.value.tip().unwrap(), oid(&w.rev("main")));
    }
}

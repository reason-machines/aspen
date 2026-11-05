//! Spike S1 client harness for gitoxide and libgit2.
//!
//! `s1-rust <gix|git2> <url> <repo-dir>` fetches
//! `+refs/heads/main:refs/remotes/origin/main` from `url` into an existing
//! repository and exits non-zero with the client's error on failure.
//! gix reads `protocol.version` from the repository's config.
//! `s1-rust git2-push <url> <repo-dir>` pushes `+refs/heads/main` with
//! libgit2 instead (for push_order.py; gix has no push).

use std::process::ExitCode;

const REFSPEC: &str = "+refs/heads/main:refs/remotes/origin/main";

fn fetch_gix(url: &str, dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let repo = gix::open(dir)?;
    let remote = repo
        .remote_at(url)?
        .with_refspecs(Some(REFSPEC), gix::remote::Direction::Fetch)?;
    let outcome = remote
        .connect(gix::remote::Direction::Fetch)?
        .prepare_fetch(gix::progress::Discard, Default::default())?
        .receive(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)?;
    match outcome.status {
        gix::remote::fetch::Status::Change { .. } => Ok(()),
        other => Err(format!("unexpected fetch status: {other:?}").into()),
    }
}

fn fetch_git2(url: &str, dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let repo = git2::Repository::open(dir)?;
    let mut remote = repo.remote_anonymous(url)?;
    remote.fetch(&[REFSPEC], None, None)?;
    Ok(())
}

fn push_git2(url: &str, dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let repo = git2::Repository::open(dir)?;
    let mut remote = repo.remote_anonymous(url)?;
    let rejected = std::cell::RefCell::new(None);
    let mut cb = git2::RemoteCallbacks::new();
    cb.push_update_reference(|r, status| {
        if let Some(s) = status {
            *rejected.borrow_mut() = Some(format!("{r}: {s}"));
        }
        Ok(())
    });
    let mut opts = git2::PushOptions::new();
    opts.remote_callbacks(cb);
    remote.push(&["+refs/heads/main:refs/heads/main"], Some(&mut opts))?;
    drop(opts);
    match rejected.into_inner() {
        Some(e) => Err(e.into()),
        None => Ok(()),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: s1-rust <gix|git2> <url> <repo-dir>");
        return ExitCode::from(2);
    }
    let result = match args[1].as_str() {
        "gix" => fetch_gix(&args[2], &args[3]),
        "git2" => fetch_git2(&args[2], &args[3]),
        "git2-push" => push_git2(&args[2], &args[3]),
        other => Err(format!("unknown client {other}").into()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            let mut msg = e.to_string();
            let mut src = e.source();
            while let Some(s) = src {
                msg.push_str(": ");
                msg.push_str(&s.to_string());
                src = s.source();
            }
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}

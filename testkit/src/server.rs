//! The "server under test" abstraction.
//!
//! The differential oracle talks to Git servers only over HTTP, through stock
//! `git` clients. A server must be able to create an empty repository, say
//! which URL to use for each branch, and list every ref it holds.
//! [`StockServer`] is the reference implementation (stock
//! `git http-backend`). [`ScopedServer`] is stock Git dressed in Aspen's URL
//! layout, which proves the oracle copes with servers that hide branches.
//! [`AspenServer`](crate::AspenServer) is a real Aspen (node L0 in
//! `docs/PLAN.md`).

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::git::{GitClient, init_bare};
use crate::http_backend::{GitHttpBackend, RefVisibility, SCOPE_SEGMENT};

/// Every ref of a repository: full ref name (`refs/heads/main`) to object ID.
pub type RefMap = BTreeMap<String, String>;

/// A Git HTTP server the oracle can drive.
///
/// Aspen does not advertise every branch on one URL: the repository URL
/// shows only the trunk, and each branch is pushed and fetched through its
/// own branch-scoped URL (spike S2). So the oracle never assumes that one URL
/// shows everything. Every push or fetch of a branch goes through
/// [`GitServer::branch_url`], and the server's full state is read through
/// [`GitServer::list_refs`].
///
/// Implementations must be safe to call from several threads: the oracle
/// creates repositories for many sequences, and drives the two servers it
/// compares in parallel.
pub trait GitServer: Send + Sync + fmt::Debug {
    /// A short label for reports, such as `git-http-backend` or `aspen`.
    fn name(&self) -> &str;

    /// Creates an empty repository called `name` (for example `seq-1.git`)
    /// that accepts anonymous pushes, and returns its URL. `name` is unique
    /// for the lifetime of the server. The trunk branch is `main`.
    ///
    /// The server must advertise the `push-options` capability on push:
    /// force-pushes carry `-o force` (Aspen's force intent, spike S2), and
    /// other servers ignore it.
    fn create_repo(&self, name: &str) -> io::Result<String>;

    /// The URL through which clients push and fetch `refname` (a full name
    /// such as `refs/heads/topic-1`) of the repository at `repo_url`. It must
    /// advertise that ref, if it exists, with its current object ID, and
    /// accept pushes that create, update or delete it. A server that shows
    /// every ref everywhere returns `repo_url`.
    fn branch_url(&self, repo_url: &str, refname: &str) -> String;

    /// Every ref of the repository at `repo_url`, hidden or not, with its
    /// object ID. `HEAD` and peeled tags are not refs. This is the oracle's
    /// view of the server's true state, so it must read the server's
    /// committed ref state (for Aspen, its full ref listing) rather than any
    /// one client's view.
    fn list_refs(&self, repo_url: &str) -> io::Result<RefMap>;
}

/// Stock `git http-backend` serving bare repositories from a temporary
/// directory. Stopped, and its repositories deleted, when dropped.
#[derive(Debug)]
pub struct StockServer {
    // Field order matters: stop the server before deleting its files.
    backend: GitHttpBackend,
    root: TempDir,
    name: String,
}

impl StockServer {
    /// Starts a server on an ephemeral port of `127.0.0.1`.
    pub fn start() -> io::Result<Self> {
        Self::start_named("git-http-backend")
    }

    /// Like [`StockServer::start`], with a custom label for reports.
    pub fn start_named(name: &str) -> io::Result<Self> {
        Self::start_with(name, RefVisibility::All)
    }

    fn start_with(name: &str, visibility: RefVisibility) -> io::Result<Self> {
        let root = tempfile::Builder::new().prefix("aspen-stock-").tempdir()?;
        let backend = GitHttpBackend::start_with(root.path(), visibility)?;
        Ok(Self {
            backend,
            root,
            name: name.to_owned(),
        })
    }

    /// The directory holding the served bare repositories.
    pub fn root(&self) -> &Path {
        self.root.path()
    }

    /// The on-disk path of repository `name`.
    pub fn repo_path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    /// The server's base URL.
    pub fn base_url(&self) -> String {
        self.backend.base_url()
    }

    /// Creates repository `name` on disk and returns its URL.
    pub(crate) fn init(&self, name: &str) -> io::Result<String> {
        let path = init_bare(self.root.path(), name)?;
        GitClient::new(self.root.path())
            .run(&path, ["config", "receive.advertisePushOptions", "true"])?;
        Ok(format!("{}/{name}", self.backend.base_url()))
    }

    /// The on-disk path of the repository at `repo_url`.
    pub(crate) fn path_of(&self, repo_url: &str) -> io::Result<PathBuf> {
        repo_url
            .strip_prefix(&self.backend.base_url())
            .and_then(|rest| rest.strip_prefix('/'))
            .map(|name| self.repo_path(name))
            .ok_or_else(|| io::Error::other(format!("{repo_url} is not served here")))
    }
}

impl GitServer for StockServer {
    fn name(&self) -> &str {
        &self.name
    }

    fn create_repo(&self, name: &str) -> io::Result<String> {
        self.init(name)
    }

    fn branch_url(&self, repo_url: &str, _refname: &str) -> String {
        repo_url.to_owned()
    }

    /// `git ls-remote --refs` of the repository URL, which shows every ref.
    fn list_refs(&self, repo_url: &str) -> io::Result<RefMap> {
        let out = GitClient::new(self.root.path())
            .run(self.root.path(), ["ls-remote", "--refs", repo_url])?;
        parse_ref_lines(&out)
    }
}

/// Stock `git http-backend` with Aspen's ref visibility (spike S2): the
/// repository URL advertises only `main`, and
/// `<repo>/-/refs/heads/<branch>` advertises `main` plus that branch. Built
/// with `transfer.hideRefs` (see [`RefVisibility::BranchScoped`]).
///
/// It behaves like [`StockServer`] for any client that uses
/// [`GitServer::branch_url`], so the oracle must report no divergence
/// between the two. Its refs are listed from disk, standing in for the full
/// ref listing Aspen exposes.
#[derive(Debug)]
pub struct ScopedServer {
    inner: StockServer,
}

impl ScopedServer {
    /// Starts a server on an ephemeral port of `127.0.0.1`.
    pub fn start() -> io::Result<Self> {
        Ok(Self {
            inner: StockServer::start_with(
                "branch-scoped",
                RefVisibility::BranchScoped {
                    trunk: "main".to_owned(),
                },
            )?,
        })
    }
}

impl GitServer for ScopedServer {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn create_repo(&self, name: &str) -> io::Result<String> {
        self.inner.init(name)
    }

    fn branch_url(&self, repo_url: &str, refname: &str) -> String {
        format!("{repo_url}{SCOPE_SEGMENT}{refname}")
    }

    fn list_refs(&self, repo_url: &str) -> io::Result<RefMap> {
        let path = self.inner.path_of(repo_url)?;
        let out = GitClient::new(self.inner.root()).run(
            &path,
            ["for-each-ref", "--format=%(objectname) %(refname)", "refs/"],
        )?;
        parse_ref_lines(&out)
    }
}

/// Parses `<oid> <ref>` lines (tab or space separated).
pub(crate) fn parse_ref_lines(out: &str) -> io::Result<RefMap> {
    out.lines()
        .filter(|l| !l.is_empty())
        .map(|line| {
            line.split_once(['\t', ' '])
                .map(|(oid, name)| (name.trim().to_owned(), oid.to_owned()))
                .ok_or_else(|| io::Error::other(format!("bad ref line {line:?}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_lines_parse() {
        let refs = parse_ref_lines("1111\trefs/heads/main\n2222 refs/heads/b\n").unwrap();
        assert_eq!(refs["refs/heads/main"], "1111");
        assert_eq!(refs["refs/heads/b"], "2222");
        assert!(parse_ref_lines("garbage").is_err());
        assert!(parse_ref_lines("").unwrap().is_empty());
    }
}

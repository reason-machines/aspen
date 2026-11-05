//! The server under test and the shared setup every workload needs: a
//! scratch directory, the synthetic seed repository, and `main` pushed to the
//! target.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use aspen_interop::GitHttpBackend;
use serde_json::Value;

use crate::git::{self, Git, Remote};
use crate::results::{Environment, TargetInfo};
use crate::synth::{self, RepoSpec};

/// `--url` value that starts an in-process `git http-backend` bridge.
pub const LOCAL: &str = "local";

/// Options shared by every workload.
#[derive(Debug, Clone)]
pub struct CommonOpts {
    /// A Git HTTP repository URL, or [`LOCAL`].
    pub url: String,
    /// Name recorded in the result; defaults from the URL.
    pub label: Option<String>,
    pub repo: RepoSpec,
    /// Git wire protocol version for every client (0, 1 or 2).
    pub protocol: u8,
    /// Push the synthetic `main` before measuring.
    pub seed: bool,
    /// An existing bare repository whose `main` replaces the synthetic
    /// history.
    pub repo_source: Option<PathBuf>,
    /// Keep the scratch directory here instead of a temporary one.
    pub work_dir: Option<PathBuf>,
    /// Push each benchmark branch through its branch-scoped URL
    /// ([`branch_scoped_url`]) instead of the plain repository URL.
    pub branch_scoped_push: bool,
}

impl CommonOpts {
    pub fn params(&self) -> BTreeMap<String, Value> {
        // A source repository's shape is recorded by its tip (`Setup`),
        // not by the synthetic options it ignores.
        let mut m = if self.repo_source.is_some() {
            BTreeMap::new()
        } else {
            self.repo.params()
        };
        m.insert("protocol".to_owned(), self.protocol.into());
        // Recorded only when set, so results from before the option existed
        // stay comparable with plain-URL runs, and `compare` refuses to mix
        // the two (a param set on one side only is an incompatibility).
        if self.branch_scoped_push {
            m.insert("push_url".to_owned(), "branch-scoped".into());
        }
        m
    }
}

/// The branch-scoped URL for `branch` in the repository at `repo_url`:
/// `<repo>/-/refs/heads/<branch>` (spike S2, PLAN section 4.2).
///
/// A server with a trunk-only push advertisement (Aspen) advertises trunk
/// plus exactly this branch there, so stock `git push` builds the pack
/// against the branch's current tip. Through the plain URL the branch is
/// never advertised, so every push would be built against trunk and resend
/// all the commits the branch has added since.
pub fn branch_scoped_url(repo_url: &str, branch: &str) -> String {
    let repo = repo_url.trim_end_matches('/');
    format!("{repo}/-/refs/heads/{branch}")
}

/// Everything a workload needs to run against a target.
#[derive(Debug)]
pub struct Setup {
    /// Isolated `git` with credentials for the target.
    pub git: Git,
    pub remote: Remote,
    pub info: TargetInfo,
    /// Scratch directory for client repositories.
    pub dir: PathBuf,
    /// Local bare repository holding the synthetic history.
    pub seed_repo: PathBuf,
    /// Tip of `main` in the seed repository.
    pub main: String,
    /// A token unique to this run, used in branch names and commit content so
    /// that repeated runs against one repository never collide.
    pub run_id: String,
    pub environment: Environment,
    /// `main` of `--repo-source`, when the seed is a real repository.
    pub source_tip: Option<String>,
    branch_scoped_push: bool,
    _tmp: Option<tempfile::TempDir>,
    _server: Option<GitHttpBackend>,
}

impl Setup {
    pub fn new(opts: &CommonOpts) -> Result<Self, String> {
        if !matches!(opts.protocol, 0..=2) {
            return Err(format!(
                "--protocol must be 0, 1 or 2, got {}",
                opts.protocol
            ));
        }
        let version = git::version();
        if !git::version_at_least(&version, (2, 31)) {
            return Err(format!(
                "aspen-bench needs git 2.31 or newer (it passes configuration through \
                 GIT_CONFIG_COUNT); found {version:?}"
            ));
        }
        let (tmp, dir) = match &opts.work_dir {
            Some(d) => {
                std::fs::create_dir_all(d).map_err(|e| format!("creating {}: {e}", d.display()))?;
                (None, d.clone())
            }
            None => {
                let t = tempfile::Builder::new()
                    .prefix("aspen-bench-")
                    .tempdir()
                    .map_err(|e| format!("creating a temporary directory: {e}"))?;
                let p = t.path().to_path_buf();
                (Some(t), p)
            }
        };
        let run_id = format!("{:x}", crate::results::now_unix_ms());
        let dir = dir.join(&run_id);
        let home = dir.join("home");
        std::fs::create_dir_all(&home).map_err(|e| format!("creating {}: {e}", home.display()))?;
        let base_git = Git::new(&home, opts.protocol);

        let (server, url, default_label) = if opts.url == LOCAL {
            let served = dir.join("served");
            std::fs::create_dir_all(&served)
                .map_err(|e| format!("creating {}: {e}", served.display()))?;
            aspen_interop::init_bare(&served, "bench.git")
                .map_err(|e| format!("creating the local repository: {e}"))?;
            let server = GitHttpBackend::start(&served)
                .map_err(|e| format!("starting the local git http-backend bridge: {e}"))?;
            let url = format!("{}/bench.git", server.base_url());
            (Some(server), url, "local-git-http-backend".to_owned())
        } else {
            (None, opts.url.clone(), String::new())
        };
        let remote = Remote::parse(&url)?;
        let label = opts.label.clone().unwrap_or_else(|| {
            if default_label.is_empty() {
                remote.url.clone()
            } else {
                default_label
            }
        });
        let git = base_git.with_remote(&remote);

        let seed_repo = dir.join("seed.git");
        let (main, source_tip) = match &opts.repo_source {
            Some(source) => {
                eprintln!("aspen-bench: using main of {}", source.display());
                let main = source_main(&git, source, &seed_repo)?;
                (main.clone(), Some(main))
            }
            None => {
                eprintln!(
                    "aspen-bench: generating the synthetic repository ({} files x {} bytes, {} commits)",
                    opts.repo.files, opts.repo.file_bytes, opts.repo.commits
                );
                (synth::generate(&git, &opts.repo, &seed_repo)?, None)
            }
        };
        if opts.seed {
            seed_target(&git, &seed_repo, &remote, &main)?;
        }
        Ok(Self {
            git,
            info: TargetInfo {
                label,
                url: remote.url.clone(),
            },
            remote,
            dir,
            seed_repo,
            main,
            run_id,
            environment: Environment::detect(version),
            source_tip,
            branch_scoped_push: opts.branch_scoped_push,
            _tmp: tmp,
            _server: server,
        })
    }

    /// A number derived from the run id, for seeding per-run content.
    pub fn run_id_seed(&self) -> u64 {
        u64::from_str_radix(&self.run_id, 16).unwrap_or(0)
    }

    /// A fresh path under the scratch directory.
    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// The URL to push `branch` to, and to read it back from with
    /// `ls-remote` after a failed push: the branch-scoped URL with
    /// `--branch-scoped-push`, otherwise the repository URL.
    pub fn push_url(&self, branch: &str) -> String {
        if self.branch_scoped_push {
            branch_scoped_url(&self.remote.url, branch)
        } else {
            self.remote.url.clone()
        }
    }

    /// Deletes the given branches on the target. Failures are reported but
    /// don't fail the run.
    ///
    /// Through the repository URL this is one push per 200 branches. With
    /// `--branch-scoped-push` each branch is deleted through its own scoped
    /// URL, where it is advertised, so the delete is a compare-and-swap
    /// rather than a full-name delete that Aspen gates like a force.
    pub fn delete_branches(&self, branches: &[String]) {
        if self.branch_scoped_push {
            for b in branches {
                let url = self.push_url(b);
                let mut cmd = self.git.cmd(&self.seed_repo);
                cmd.args(["push", "--quiet", &url, "--delete", b]);
                if let Err(e) = git::run(&mut cmd) {
                    eprintln!("aspen-bench: cleanup of benchmark branch {b} failed: {e}");
                }
            }
            return;
        }
        for chunk in branches.chunks(200) {
            let mut cmd = self.git.cmd(&self.seed_repo);
            cmd.args(["push", "--quiet", &self.remote.url]);
            for b in chunk {
                cmd.arg(format!(":refs/heads/{b}"));
            }
            if let Err(e) = git::run(&mut cmd) {
                eprintln!("aspen-bench: cleanup of benchmark branches failed: {e}");
            }
        }
    }
}

/// Makes `seed_repo` a bare repository holding exactly `main` of the
/// existing bare repository `source` (borrowing its objects) and returns
/// that commit.
fn source_main(git: &Git, source: &Path, seed_repo: &Path) -> Result<String, String> {
    let source = source
        .canonicalize()
        .map_err(|e| format!("--repo-source {}: {e}", source.display()))?;
    let parent = seed_repo.parent().unwrap_or(Path::new("."));
    git::run(
        git.cmd(parent)
            .args([
                "clone",
                "--quiet",
                "--bare",
                "--shared",
                "--no-tags",
                "--single-branch",
                "--branch",
                "main",
            ])
            .arg(&source)
            .arg(seed_repo),
    )
    .map_err(|e| format!("{e}\n--repo-source must be a bare repository with a main branch"))?;
    git::run(git.cmd(seed_repo).args(["rev-parse", "refs/heads/main"]))
}

/// Pushes the seed repository's `main` to the target, then checks the target
/// reports the expected tip.
pub fn seed_target(git: &Git, seed_repo: &Path, remote: &Remote, main: &str) -> Result<(), String> {
    eprintln!("aspen-bench: pushing main ({main}) to {}", remote.url);
    git::run(git.cmd(seed_repo).args([
        "push",
        "--quiet",
        &remote.url,
        "refs/heads/main:refs/heads/main",
    ]))
    .map_err(|e| {
        format!(
            "{e}\nseeding needs an empty repository or one already seeded with the same \
             --repo-* options and --seed"
        )
    })?;
    let advertised =
        git::run(
            git.cmd(seed_repo)
                .args(["ls-remote", &remote.url, "refs/heads/main"]),
        )?;
    if !advertised.starts_with(main) {
        return Err(format!(
            "target advertises {advertised:?} for main after seeding, expected {main}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_scoped_url_appends_the_ref_under_dash() {
        assert_eq!(
            branch_scoped_url("https://h/org/repo.git", "bench/abc/w0"),
            "https://h/org/repo.git/-/refs/heads/bench/abc/w0"
        );
        assert_eq!(
            branch_scoped_url("http://127.0.0.1:8080/repo.git/", "main"),
            "http://127.0.0.1:8080/repo.git/-/refs/heads/main"
        );
    }

    fn opts(branch_scoped_push: bool) -> CommonOpts {
        CommonOpts {
            url: LOCAL.to_owned(),
            label: None,
            repo: RepoSpec::preset("tiny", 1).unwrap(),
            protocol: 2,
            seed: true,
            repo_source: None,
            work_dir: None,
            branch_scoped_push,
        }
    }

    #[test]
    fn branch_scoped_push_is_recorded_only_when_set() {
        assert!(!opts(false).params().contains_key("push_url"));
        assert_eq!(
            opts(true).params().get("push_url"),
            Some(&Value::from("branch-scoped"))
        );
    }
}

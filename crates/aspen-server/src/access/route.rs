//! Repository routing: which repository, scope and Git endpoint a smart
//! HTTP request names.
//!
//! ```text
//! /<repo>[.git][/-/<ref>]/info/refs?service=git-upload-pack
//! /<repo>[.git][/-/<ref>]/info/refs?service=git-receive-pack
//! /<repo>[.git][/-/<ref>]/git-upload-pack        (POST)
//! /<repo>[.git][/-/<ref>]/git-receive-pack       (POST)
//! ```
//!
//! `<repo>` is a [`RepoName`] (one or more segments, such as `acme/mono`),
//! and `/-/<ref>` is the branch-scoped URL of spike S2. Repository segments
//! may not be `-` or end in `.git`, so the split is unambiguous. The path is
//! read as it arrives, with no percent-decoding: a repository name has no
//! character that needs encoding, and a scoped ref is taken literally.

use aspen_core::name::{RefName, RepoName};

/// A Git smart HTTP endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    /// `GET info/refs?service=git-upload-pack`: the fetch advertisement.
    UploadPackAdvertisement,
    /// `GET info/refs?service=git-receive-pack`: the push advertisement.
    ReceivePackAdvertisement,
    /// `POST git-upload-pack`: a fetch.
    UploadPack,
    /// `POST git-receive-pack`: a push.
    ReceivePack,
}

impl Endpoint {
    /// Whether the endpoint belongs to a push.
    pub fn is_push(self) -> bool {
        matches!(self, Self::ReceivePackAdvertisement | Self::ReceivePack)
    }

    /// Whether the endpoint is a `POST`; the advertisements are `GET`.
    pub fn is_post(self) -> bool {
        matches!(self, Self::UploadPack | Self::ReceivePack)
    }
}

/// What a Git request names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRoute {
    /// The repository.
    pub repo: RepoName,
    /// The ref of a branch-scoped URL, if any.
    pub scope: Option<RefName>,
    /// The endpoint.
    pub endpoint: Endpoint,
}

/// How a path failed to route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    /// Not a Git smart HTTP path at all. [`super::guard`] answers it 404;
    /// serve other routes from a router the guard does not wrap.
    NotGit,
    /// A Git endpoint under a path that is not a valid repository name or
    /// scope: answered 404, like a repository that does not exist.
    BadName,
    /// `info/refs` without a smart `service`: Aspen does not serve the dumb
    /// HTTP protocol.
    DumbHttp,
}

const INFO_REFS: &str = "/info/refs";
const UPLOAD_PACK: &str = "/git-upload-pack";
const RECEIVE_PACK: &str = "/git-receive-pack";
const SCOPE: &str = "/-/";

impl GitRoute {
    /// Routes a request path and query string.
    pub fn parse(path: &str, query: Option<&str>) -> Result<Self, RouteError> {
        let (base, endpoint) = if let Some(base) = path.strip_suffix(INFO_REFS) {
            let service = query
                .into_iter()
                .flat_map(|q| q.split('&'))
                .find_map(|kv| kv.strip_prefix("service="));
            let endpoint = match service {
                Some("git-upload-pack") => Endpoint::UploadPackAdvertisement,
                Some("git-receive-pack") => Endpoint::ReceivePackAdvertisement,
                _ => return Err(RouteError::DumbHttp),
            };
            (base, endpoint)
        } else if let Some(base) = path.strip_suffix(UPLOAD_PACK) {
            (base, Endpoint::UploadPack)
        } else if let Some(base) = path.strip_suffix(RECEIVE_PACK) {
            (base, Endpoint::ReceivePack)
        } else {
            return Err(RouteError::NotGit);
        };
        let base = base.strip_prefix('/').ok_or(RouteError::BadName)?;
        Self::from_repo_path(base, endpoint)
    }

    /// Routes a repository path without its leading `/`,
    /// `<repo>[.git][/-/<ref>]`, to `endpoint`. The SSH transport uses it
    /// for the path argument of `git-upload-pack` and `git-receive-pack`,
    /// so both transports name repositories and branch scopes alike.
    pub fn from_repo_path(path: &str, endpoint: Endpoint) -> Result<Self, RouteError> {
        let (repo, scope) = match path.split_once(SCOPE) {
            Some((repo, scope)) => (
                repo,
                Some(RefName::new(scope).map_err(|_| RouteError::BadName)?),
            ),
            None => (path, None),
        };
        let repo = repo.strip_suffix(".git").unwrap_or(repo);
        let repo = RepoName::new(repo).map_err(|_| RouteError::BadName)?;
        Ok(Self {
            repo,
            scope,
            endpoint,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(path: &str, query: Option<&str>) -> Result<GitRoute, RouteError> {
        GitRoute::parse(path, query)
    }

    #[test]
    fn routes_every_endpoint_with_and_without_dot_git() {
        for base in ["/acme/mono", "/acme/mono.git"] {
            let r = route(
                &format!("{base}/info/refs"),
                Some("service=git-upload-pack"),
            )
            .unwrap();
            assert_eq!(r.repo.as_str(), "acme/mono");
            assert_eq!(r.scope, None);
            assert_eq!(r.endpoint, Endpoint::UploadPackAdvertisement);
            let r = route(
                &format!("{base}/info/refs"),
                Some("x=1&service=git-receive-pack"),
            )
            .unwrap();
            assert_eq!(r.endpoint, Endpoint::ReceivePackAdvertisement);
            assert_eq!(
                route(&format!("{base}/git-upload-pack"), None)
                    .unwrap()
                    .endpoint,
                Endpoint::UploadPack
            );
            assert_eq!(
                route(&format!("{base}/git-receive-pack"), None)
                    .unwrap()
                    .endpoint,
                Endpoint::ReceivePack
            );
        }
    }

    #[test]
    fn routes_branch_scoped_urls() {
        let r = route("/acme/mono.git/-/refs/heads/agent/7/git-receive-pack", None).unwrap();
        assert_eq!(r.repo.as_str(), "acme/mono");
        assert_eq!(r.scope.unwrap().as_str(), "refs/heads/agent/7");
        let r = route(
            "/demo/-/refs/heads/x/info/refs",
            Some("service=git-upload-pack"),
        )
        .unwrap();
        assert_eq!(r.repo.as_str(), "demo");
        assert_eq!(r.scope.unwrap().as_str(), "refs/heads/x");
    }

    #[test]
    fn refuses_what_is_not_a_repository() {
        use RouteError::*;
        let cases = [
            ("/", NotGit),
            ("/healthz", NotGit),
            ("/acme/mono.git/HEAD", NotGit),
            ("/acme/mono.git/objects/info/packs", NotGit),
            ("/git-upload-pack", BadName),
            ("//git-upload-pack", BadName),
            ("/acme/../x/git-upload-pack", BadName),
            ("/acme/.x/git-upload-pack", BadName),
            ("/acme//x/git-upload-pack", BadName),
            ("/acme/mono.git.git/git-upload-pack", BadName),
            ("/acme%2Fmono/git-upload-pack", BadName),
            ("/acme/mono/-/git-upload-pack", BadName),
            ("/acme/mono/-/main/git-upload-pack", BadName),
            ("/acme/mono/-/refs/heads/../x/git-upload-pack", BadName),
            ("acme/mono/git-upload-pack", BadName),
        ];
        for (path, want) in cases {
            assert_eq!(route(path, None).unwrap_err(), want, "{path}");
        }
        assert_eq!(
            route("/acme/mono/info/refs", None).unwrap_err(),
            RouteError::DumbHttp
        );
        assert_eq!(
            route("/acme/mono/info/refs", Some("service=git-annex")).unwrap_err(),
            RouteError::DumbHttp
        );
    }
}

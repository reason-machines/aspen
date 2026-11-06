//! Test support (feature `testing`): fill a repository's stores from a local
//! Git repository, one simulated push at a time, the way the push path
//! (node B1) will.
//!
//! Each [`Seeder::push`] stores what a real push stores:
//!
//! - a pack of `git rev-list --objects <tip> --not <dependency tips>`, built
//!   by `git pack-objects` (never thin), ingested by `aspen_pack::ingest`
//!   (so it is self-contained and base-first and has its `.idx` and entry
//!   table), uploaded under [`PackKeys`](aspen_core::pack::PackKeys) and
//!   registered;
//! - `commit_index` rows for the pack's commits (all of them, or only the
//!   tip, as B1 writes synchronously before the indexing job runs);
//! - a ref version committed by compare-and-swap, whose closure comes from
//!   [`Closure::of_new_version`] with ancestry answered by
//!   `git merge-base --is-ancestor`.
//!
//! Dependencies follow S2: a push through the plain URL depends on the
//! trunk version current at the push; a branch-scoped push also depends on
//! the branch's previous version. Extra objects can be added to a push's
//! pack ([`Push::extra`]), which makes the version's closure wider than its
//! tip, the case `HaveVersion` must refuse.
//!
//! This is not the push path: it trusts the local repository, checks no
//! commit rules and writes all metadata in one go. Never use it in a
//! server.

use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use aspen_blob::{LocalBlobStore, LocalUrlSigner};
use aspen_meta::{SqlStore, StoreOptions};

use aspen_core::blob::BlobStore;
use aspen_core::id::ObjectId;
use aspen_core::ingest::InMemoryBases;
use aspen_core::meta::{CommitOutcome, MetaStore, NewCommit, PushCommit, RefWrite};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::PushLimits;
use aspen_core::refs::{Ancestry, AncestryAnswer, Closure, Deps, RefValue, RefVersion};
use aspen_core::repo::{Repo, RepoSettings};
use aspen_pack::ingest::{CompleteOptions, complete, receive};
use aspen_pack::scan::{ScanOptions, scan};
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};

use std::sync::Arc;

use aspen_auth::{AuthService, Limits, LimitsConfig, MemoryTokenStore, PolicyConfig};

use crate::access::{Access, ClientAddr};
use crate::{Server, ServerConfig};

/// A running Aspen server for tests: SQLite metadata and a local blob
/// store with a URL signer under the server's `/-/blobs` route, on
/// `127.0.0.1` with a free port, driven by its own Tokio runtime so tests
/// can run blocking `git` commands against it.
///
/// `/-/metrics` is served on the same listener
/// ([`ServerConfig::metrics_on_git_listener`]), with the database gauges
/// read on every scrape.
///
/// Git requests pass [`crate::access::guard`] with the policy the server
/// was started with; [`TestServer::start`] lets anyone read every
/// repository (anonymous `read:*`), with default limits. Pushes are served
/// by the receive-pack service (node B1) for callers the policy lets write.
#[derive(Debug)]
pub struct TestServer {
    /// The metadata store.
    pub meta: SqlStore,
    /// The blob store.
    pub blobs: LocalBlobStore,
    /// The access control in front of the Git endpoints (to mint tokens).
    pub access: Arc<Access<SqlStore, MemoryTokenStore>>,
    server: Server<SqlStore, LocalBlobStore>,
    addr: SocketAddr,
    runtime: tokio::runtime::Runtime,
    _dir: tempfile::TempDir,
}

/// Signing key for test servers' blob URLs. A fixed test value: these
/// servers listen on localhost only and live for one test.
const TEST_URL_SECRET: &[u8] = b"aspen-test-server-url-signing-key-not-a-secret";

impl TestServer {
    /// Starts a server that lets anyone read every repository (anonymous
    /// `read:*`); `configure` adjusts the configuration first.
    pub fn start(configure: impl FnOnce(&mut ServerConfig)) -> Self {
        let policy = PolicyConfig {
            anonymous: vec!["read:*".parse().expect("valid grant")],
            ..PolicyConfig::default()
        };
        Self::start_with_policy(policy, configure)
    }

    /// Starts a server whose Git endpoints are guarded by `policy`, with an
    /// in-memory token store and default limits.
    pub fn start_with_policy(
        policy: PolicyConfig,
        configure: impl FnOnce(&mut ServerConfig),
    ) -> Self {
        Self::start_extended(policy, configure, |server, _, _| server)
    }

    /// Like [`TestServer::start_with_policy`], and `extend` adds to the
    /// server before it starts serving (for example express routes, C2).
    /// It gets the server, its base URL (`http://127.0.0.1:<port>`) and
    /// its blob store.
    pub fn start_extended(
        policy: PolicyConfig,
        configure: impl FnOnce(&mut ServerConfig),
        extend: impl FnOnce(
            Server<SqlStore, LocalBlobStore>,
            &str,
            &LocalBlobStore,
        ) -> Server<SqlStore, LocalBlobStore>,
    ) -> Self {
        Self::start_with_store(policy, &StoreOptions::default(), configure, extend)
    }

    /// Like [`TestServer::start_extended`], with the metadata store's
    /// connection settings (for example a small pool, to test how
    /// requests queue for it).
    pub fn start_with_store(
        policy: PolicyConfig,
        store: &StoreOptions,
        configure: impl FnOnce(&mut ServerConfig),
        extend: impl FnOnce(
            Server<SqlStore, LocalBlobStore>,
            &str,
            &LocalBlobStore,
        ) -> Server<SqlStore, LocalBlobStore>,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");
        let dir = tempfile::tempdir().expect("temp dir");
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let meta = runtime.block_on(async {
            let meta = SqlStore::connect_with(&url, store).await.expect("sqlite");
            meta.migrate().await.expect("migrate");
            meta
        });
        let signer = LocalUrlSigner::new(&format!("http://{addr}/-/blobs"), TEST_URL_SECRET)
            .expect("signer");
        let blobs = LocalBlobStore::open(dir.path().join("blobs"))
            .expect("blob dir")
            .with_url_signer(signer);
        let mut config = ServerConfig::new(MaxObjectBytes::new(512 << 20).expect("non-zero"));
        // Tests read `/-/metrics` from the one listener they have.
        config.metrics_on_git_listener = true;
        // Shallow fetches use exact cached packs (node C8), as `aspen serve`
        // does by default; a test sets this to `None` for the full-closure
        // path.
        config.shallow_cache = Some(crate::upload_pack::exact::ShallowCacheConfig::new(
            dir.path().join("shallow"),
            MaxInflatedBytes::new(8 << 30).expect("non-zero"),
        ));
        // A build of a test repository ends in milliseconds, but a loaded
        // CI runner can stretch that past `layered_wait`; tests that check
        // exact objects must not see a layered answer by chance. Tests of
        // layered answers set it back.
        if let Some(c) = config.shallow_cache.as_mut() {
            c.layered_wait = c.build_wait;
        }
        configure(&mut config);
        let receive = crate::receive::ReceiveConfig::new(
            dir.path().join("spool"),
            MaxObjectBytes::new(512 << 20).expect("non-zero"),
            MaxInflatedBytes::new(8 << 30).expect("non-zero"),
        );
        let server = Server::new(meta.clone(), blobs.clone(), config)
            .with_store_gauges(std::time::Duration::ZERO)
            .with_local_blob_route(blobs.clone())
            .with_receive_pack(receive)
            .expect("spool dir");
        let server = extend(server, &format!("http://{addr}"), &blobs);
        let auth = AuthService::new(policy, MemoryTokenStore::new()).expect("valid policy");
        // No caps on concurrent work: tests storm the server on purpose
        // (`fetch_storm.rs`), and every request without an address shares
        // one caller key. Admission has its own tests (`admission.rs`, and
        // the `serve_bounds_*` tests of `aspen serve`).
        let access = Arc::new(
            Access::new(
                auth,
                Limits::new(LimitsConfig {
                    concurrent: aspen_auth::ConcurrentLimits::UNLIMITED,
                    ..LimitsConfig::default()
                }),
                meta.clone(),
                ClientAddr::Peer,
            )
            .with_instance_limits(crate::admission::InstanceLimits::UNLIMITED),
        );
        let router = server.router(Arc::clone(&access));
        runtime.spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .expect("serve");
        });
        Self {
            meta,
            blobs,
            access,
            server,
            addr,
            runtime,
            _dir: dir,
        }
    }

    /// The server's base URL, `http://127.0.0.1:<port>`.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The clone URL of `repo`.
    pub fn url(&self, repo: &str) -> String {
        format!("{}/{repo}.git", self.base_url())
    }

    /// The server's counters.
    pub fn metrics(&self) -> &crate::Metrics {
        self.server.metrics()
    }

    /// Runs a future on the server's runtime (for seeding).
    pub fn block_on<F: Future>(&self, f: F) -> F::Output {
        self.runtime.block_on(f)
    }

    /// Creates repository `name` seeded from the Git repository at
    /// `git_dir`.
    pub fn seeder(
        &self,
        name: &str,
        trunk: &str,
        git_dir: &Path,
    ) -> Seeder<'_, SqlStore, LocalBlobStore> {
        self.block_on(Seeder::create(
            &self.meta,
            &self.blobs,
            name,
            trunk,
            git_dir,
        ))
        .expect("create repository")
    }

    /// The server.
    pub fn server(&self) -> &Server<SqlStore, LocalBlobStore> {
        &self.server
    }

    /// Runs one seed push.
    pub fn push(&self, seeder: &Seeder<'_, SqlStore, LocalBlobStore>, push: &Push) -> RefVersion {
        self.block_on(seeder.push(push)).expect("seed push")
    }
}

/// A seeding failure; tests unwrap it.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// How many `commit_index` rows a push writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Index {
    /// Every commit in the pack (the state after B1's indexing job).
    All,
    /// Only the tip (the state before the job ran).
    TipOnly,
}

/// One simulated push.
#[derive(Debug, Clone)]
pub struct Push {
    /// The ref to update.
    pub name: String,
    /// The new tip, any revision the local repository resolves.
    pub tip: String,
    /// Push through the branch-scoped URL: also depend on the ref's
    /// previous version (S2).
    pub scoped: bool,
    /// Objects added to the pack although the tip does not reach them,
    /// which makes the version's closure [`Closure::Wider`].
    pub extra: Vec<String>,
    /// Which commit rows to write.
    pub index: Index,
}

impl Push {
    /// A plain-URL push of `tip` to `name`, indexing every commit.
    pub fn new(name: &str, tip: &str) -> Self {
        Self {
            name: name.to_owned(),
            tip: tip.to_owned(),
            scoped: false,
            extra: Vec::new(),
            index: Index::All,
        }
    }
}

/// Seeds one repository.
#[derive(Debug)]
pub struct Seeder<'a, M, B> {
    meta: &'a M,
    blobs: &'a B,
    repo: Repo,
    git_dir: PathBuf,
    scratch: tempfile::TempDir,
}

impl<'a, M: MetaStore, B: BlobStore> Seeder<'a, M, B> {
    /// Creates repository `name` with trunk `trunk`, to be seeded from the
    /// Git repository at `git_dir`.
    pub async fn create(
        meta: &'a M,
        blobs: &'a B,
        name: &str,
        trunk: &str,
        git_dir: &Path,
    ) -> Result<Self, Error> {
        let settings = RepoSettings::new(RefName::new(trunk)?)?;
        let repo = meta.create_repo(&RepoName::new(name)?, &settings).await?;
        Ok(Self {
            meta,
            blobs,
            repo,
            git_dir: git_dir.to_owned(),
            scratch: tempfile::tempdir()?,
        })
    }

    /// The repository row.
    pub fn repo(&self) -> &Repo {
        &self.repo
    }

    fn git(&self, args: &[&str]) -> Command {
        let mut c = Command::new("git");
        c.arg("-C").arg(&self.git_dir).args(args);
        c
    }

    fn rev(&self, rev: &str) -> Result<ObjectId, Error> {
        let out = self.git(&["rev-parse", "--verify", rev]).output()?;
        if !out.status.success() {
            return Err(format!("cannot resolve {rev}").into());
        }
        Ok(ObjectId::parse_hex(String::from_utf8(out.stdout)?.trim())?)
    }

    fn is_ancestor(&self, ancestor: ObjectId, descendant: ObjectId) -> Result<bool, Error> {
        let status = self
            .git(&[
                "merge-base",
                "--is-ancestor",
                &ancestor.to_hex(),
                &descendant.to_hex(),
            ])
            .status()?;
        Ok(status.success())
    }

    /// Runs one push and returns the new version.
    pub async fn push(&self, push: &Push) -> Result<RefVersion, Error> {
        let name = RefName::new(push.name.as_str())?;
        let tip = self.rev(&push.tip)?;
        let current = self.meta.current_version(self.repo.id, &name).await?;
        let trunk = self.repo.settings.trunk();
        let trunk_dep = self.meta.current_version(self.repo.id, trunk).await?;
        let mut deps = Deps {
            trunk: trunk_dep.as_ref().map(|v| v.id),
            scoped_old: None,
        };
        let mut dep_versions: Vec<RefVersion> = trunk_dep.into_iter().collect();
        if push.scoped && &name != trunk {
            deps.scoped_old = current.as_ref().map(|v| v.id);
            dep_versions.extend(current.clone());
        }
        let dep_tips: Vec<ObjectId> = dep_versions.iter().filter_map(|v| v.value.tip()).collect();

        // The pack: what the tip reaches beyond the dependencies, plus extras.
        let mut input = format!("{}\n", tip.to_hex());
        for t in &dep_tips {
            input.push_str(&format!("^{}\n", t.to_hex()));
        }
        let objects = self.objects(&input)?;
        let mut list: Vec<String> = objects.clone();
        for e in &push.extra {
            list.push(self.rev(e)?.to_hex());
        }
        let (pack, commits) = if list.is_empty() {
            (None, Vec::new())
        } else {
            let (id, commits) = self.store_pack(&list).await?;
            (Some(id), commits)
        };
        let pack_reachable = push.extra.is_empty();
        let answers: Vec<(RefValue, Option<AncestryAnswer>)> = dep_versions
            .iter()
            .map(|v| {
                let answer = v.value.tip().map(|t| {
                    let a = if self.is_ancestor(t, tip).unwrap_or(false) {
                        Ancestry::Descendant
                    } else {
                        Ancestry::NotDescendant
                    };
                    AncestryAnswer::new(t, tip, a)
                });
                (v.value, answer)
            })
            .collect();
        let closure =
            Closure::of_new_version(tip, pack_reachable, answers.iter().map(|(v, a)| (v, *a)))?;
        // Rows only for commits the tip reaches, never for extras.
        let reachable: std::collections::HashSet<&str> =
            objects.iter().map(String::as_str).collect();
        let commits = commits
            .into_iter()
            .filter(|c: &NewCommit| reachable.contains(c.commit.to_hex().as_str()))
            .filter(|c| push.index == Index::All || c.commit == tip)
            .collect();
        let write = RefWrite {
            name,
            expected: current.map(|v| v.id),
            value: RefValue::Live { tip, pack, closure },
            deps,
            commits,
            jobs: Vec::new(),
        };
        let outcome = self
            .meta
            .commit_push(&PushCommit {
                repo: self.repo.id,
                writes: vec![write],
            })
            .await?;
        match outcome {
            CommitOutcome::Committed(mut v) => Ok(v.remove(0)),
            other => Err(format!("seed push lost its compare-and-swap: {other:?}").into()),
        }
    }

    /// `git rev-list --objects` of the revisions on `input`, as IDs.
    fn objects(&self, input: &str) -> Result<Vec<String>, Error> {
        let mut child = self
            .git(&["rev-list", "--objects", "--stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .expect("piped")
            .write_all(input.as_bytes())?;
        let out = child.wait_with_output()?;
        if !out.status.success() {
            return Err("git rev-list failed".into());
        }
        Ok(String::from_utf8(out.stdout)?
            .lines()
            .map(|l| l.split(' ').next().unwrap_or_default().to_owned())
            .filter(|l| !l.is_empty())
            .collect())
    }

    /// Packs `objects` with `git pack-objects`, ingests, uploads and
    /// registers the pack; returns its ID and its commits' rows.
    async fn store_pack(
        &self,
        objects: &[String],
    ) -> Result<(aspen_core::id::PackId, Vec<NewCommit>), Error> {
        let mut child = self
            .git(&["pack-objects", "--stdout", "-q"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut stdin = child.stdin.take().expect("piped");
        let list = objects.join("\n") + "\n";
        let writer = std::thread::spawn(move || stdin.write_all(list.as_bytes()));
        let out = child.wait_with_output()?;
        writer.join().expect("writer thread")?;
        if !out.status.success() {
            return Err("git pack-objects failed".into());
        }
        let limits = PushLimits {
            max_objects: 10_000_000,
            max_pack_bytes: 1 << 34,
        };
        let dir = self.scratch.path();
        let spool = dir.join(format!("spool-{}", objects[0]));
        let received = receive(&mut out.stdout.as_slice(), &spool, &limits)?;
        let max = MaxObjectBytes::new(512 << 20).expect("non-zero");
        let inflated = MaxInflatedBytes::new(1 << 34).expect("non-zero");
        let pack = complete(
            &received,
            &InMemoryBases::default(),
            dir,
            &CompleteOptions::new(max, inflated),
        )?;
        let new = pack.new_pack(self.repo.id);
        // Register first: the row's generation fixes the keys.
        let record = self.meta.register_pack(&new).await?;
        let keys = record.keys();
        self.blobs.upload_file(&keys.pack, &pack.pack_path).await?;
        self.blobs.upload_file(&keys.idx, &pack.idx_path).await?;
        self.blobs
            .upload_file(&keys.entries, &pack.entries_path)
            .await?;
        let id = record.id;
        let scanned = scan(
            &pack.pack_path,
            &pack.idx_path,
            ScanOptions {
                commits: true,
                missing: false,
                thread_limit: Some(1),
                max_object_bytes: max,
                max_inflated_bytes: inflated,
                verify_checksums: false,
            },
        )?;
        Ok((
            id,
            scanned.commits.iter().map(|c| c.to_new_commit()).collect(),
        ))
    }
}

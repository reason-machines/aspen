//! The `place` service: everything that writes to the canvas repository.
//!
//! - [`Place::paint`] turns a web stroke (or an agent's) into one commit on
//!   the painter's branch and pushes it through Aspen's receive-pack, in
//!   process, through the branch-scoped URL with the branch's real tip as
//!   the old value, so a racing write fails its compare-and-swap instead
//!   of being overwritten.
//! - [`Place::merge_once`] is the merger bot: it replays every pending
//!   push's tiles onto `main`'s tiles in sequence order and writes one
//!   commit whose only parent is `main`'s tip, pushed as a fast-forward
//!   of `main` with `main`'s tip as the old value. It never runs
//!   `git merge` (A9 review obligation 4).
//! - [`Place::expire_once`] deletes branches whose tiles have all merged
//!   once they are idle for the branch TTL, with a compare-and-swap delete.
//! - [`Place::run`] drives the merger, expiry, the ambient agents and the
//!   swarm until shutdown.
//!
//! Nothing here writes refs or packs directly: every write is a push, so
//! it is checked and stored exactly like a client's.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aspen_core::blob::BlobStore;
use aspen_core::error::{RejectReason, StoreError};
use aspen_core::id::ObjectId;
use aspen_core::ingest::ObjectKind;
use aspen_core::meta::{MetaStore, RefListQuery};
use aspen_core::name::{RefName, RepoName};
use aspen_core::refs::RefVersion;
use aspen_core::repo::{Repo, RepoSettings};
use aspen_server::receive::hook::Caller;
use aspen_server::receive::{ReceivePack, RefAccess, ServiceError};
use tokio::sync::Semaphore;

use crate::git::{
    MODE_FILE, MODE_TREE, PackBuilder, Report, TreeEntry, parse_commit, parse_tree,
    receive_request, write_commit, write_tree,
};
use crate::hook::{Accepted, CheckLimits, INTERNAL_KEY, Shared, Waiting, Writer};
use crate::limits::{Slots, Windows};
use crate::rules::{self, Rules, TRUNK_REF, Tile};
use crate::state::{Actor, Main, State, SwarmLimits, UNSET, now_ms};

/// Settings of a `place` deployment.
#[derive(Debug, Clone)]
pub struct PlaceConfig {
    /// The canvas repository's name (`canvas` serves `/canvas.git`).
    pub repo: String,
    /// Canvas size, palette and tiles per push.
    pub rules: Rules,
    /// Pushes per web session, and per Git caller and branch, per minute.
    pub pushes_per_minute: usize,
    /// A branch idle this long, with all its tiles merged, is deleted.
    pub branch_ttl: Duration,
    /// How often the merger bot runs.
    pub merge_interval: Duration,
    /// How often, at most, the merger bot merges when only ambient agents
    /// (`agent/`) have pushes pending; a push from anyone else is merged on
    /// the next run and takes theirs along. Every merge stores a whole
    /// `tiles/` tree in `main`'s history (about 9 KB on a painted 64 x 64
    /// canvas, with no deltas yet), and a full clone fetches all of them,
    /// so this bounds how fast idle painting grows a clone: about 2.6 MB a
    /// day at the default of 5 minutes.
    pub ambient_merge_interval: Duration,
    /// Mean time between one ambient agent's strokes (each is spread
    /// uniformly over half to one and a half times this). Every stroke is a
    /// commit on the agent's branch, advertised until it merges.
    pub ambient_interval: Duration,
    /// What `GET /config` tells people to `git clone`.
    pub clone_url: String,
    /// The live dashboard, if there is one.
    pub dashboard_url: Option<String>,
    /// Background agents that paint a stroke every few seconds each.
    pub ambient_agents: usize,
    /// The swarm's limits.
    pub swarm: SwarmLimits,
    /// Swarm runs one client address may start per hour.
    pub swarm_starts_per_hour: usize,
    /// Sessions one client address may create per minute.
    pub sessions_per_minute: usize,
    /// Sessions kept at once; more are refused until some expire.
    pub max_sessions: usize,
    /// Events a reconnecting client can catch up from.
    pub event_backlog: usize,
    /// Event streams (`GET /events`) open at once, in all.
    pub max_event_streams: usize,
    /// Event streams open at once per client address.
    pub max_event_streams_per_address: usize,
    /// An event stream is ended after at most this long (at a random point
    /// in its last tenth, so streams do not all reconnect together); the
    /// client reconnects with the `seq` of the last event it saw and loses
    /// nothing.
    pub event_stream_max_age: Duration,
    /// Most commits one Git update may add.
    pub max_commits: usize,
    /// Most tree entries one Git update's diffs may visit. A commit that
    /// changes tiles visits every entry of its `tiles/` tree and its
    /// parent's (one per painted tile), so on a full 64 x 64 canvas the
    /// default of 200,000 admits about 24 such commits, well below
    /// [`Self::max_commits`].
    pub max_tree_entries: usize,
    /// Agent pushes in flight at once; a swarm or ambient push that finds
    /// none free is dropped, never queued.
    pub agent_concurrency: usize,
    /// Where client addresses come from: the TCP peer, or
    /// `X-Forwarded-For` behind trusted proxies (the same source as the
    /// Git side's, `ServerSettings::client_addr`).
    pub client_addr: aspen_server::access::ClientAddr,
    /// Engage the kill switch at start.
    pub kill_switch: bool,
}

impl PlaceConfig {
    /// The defaults of `api/openapi.yaml` and the mock: a 64 x 64 canvas,
    /// 64 tiles per push, 30 pushes a minute, 10-minute branches, a merge
    /// every 3 s, 6 ambient agents, and a swarm of 1,000 agents capped at
    /// 120 pushes/s for 20 s with a 40 s cooldown and 6 starts an hour per
    /// address.
    pub fn new(clone_url: impl Into<String>) -> Self {
        Self {
            repo: "canvas".to_owned(),
            rules: Rules {
                width: 64,
                height: 64,
                palette: rules::default_palette(),
                tiles_per_push: 64,
            },
            pushes_per_minute: 30,
            branch_ttl: Duration::from_secs(600),
            merge_interval: Duration::from_millis(3000),
            ambient_merge_interval: Duration::from_secs(300),
            ambient_interval: Duration::from_secs(30),
            clone_url: clone_url.into(),
            dashboard_url: None,
            ambient_agents: 6,
            swarm: SwarmLimits {
                max_agents: 1000,
                pushes_per_second_cap: 120,
                run_seconds: 20,
                cooldown_seconds: 40,
                ramp_ms: 3000,
            },
            swarm_starts_per_hour: 6,
            sessions_per_minute: 10,
            max_sessions: 100_000,
            event_backlog: 5000,
            max_event_streams: 4096,
            max_event_streams_per_address: 16,
            event_stream_max_age: Duration::from_secs(600),
            max_commits: 1000,
            max_tree_entries: 200_000,
            agent_concurrency: 16,
            client_addr: aspen_server::access::ClientAddr::Peer,
            kill_switch: false,
        }
    }

    /// Checks the settings that would break the snapshot encoding.
    pub fn validate(&self) -> Result<(), String> {
        let r = &self.rules;
        if r.palette.len() < 2 || r.palette.len() > rules::MAX_PALETTE {
            return Err(format!(
                "the palette needs 2 to {} colors",
                rules::MAX_PALETTE
            ));
        }
        for (i, p) in r.palette.iter().enumerate() {
            if !is_color(&p.color) {
                return Err(format!(
                    "palette color {} is not lowercase #rrggbb",
                    p.color
                ));
            }
            if r.palette[..i].iter().any(|q| q.color == p.color) {
                return Err(format!("palette color {} is listed twice", p.color));
            }
        }
        if !(1..=4096).contains(&r.width) || !(1..=4096).contains(&r.height) {
            return Err("width and height must be 1 to 4096".into());
        }
        if r.tiles_per_push == 0 || self.pushes_per_minute == 0 {
            return Err("tiles per push and pushes per minute must be at least 1".into());
        }
        if self.swarm.max_agents == 0 || self.swarm.pushes_per_second_cap == 0 {
            return Err("the swarm needs at least one agent and one push per second".into());
        }
        if self.max_event_streams == 0 || self.max_event_streams_per_address == 0 {
            return Err("event streams in all and per address must be at least 1".into());
        }
        if self.event_stream_max_age < Duration::from_secs(1) {
            return Err("the event stream age limit must be at least 1 s".into());
        }
        Ok(())
    }
}

fn is_color(c: &str) -> bool {
    c.len() == 7
        && c.starts_with('#')
        && c[1..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Why the service could not write.
#[derive(Debug, thiserror::Error)]
pub enum PlaceError {
    /// Receive-pack refused the push (a race lost, a limit, a rule).
    #[error("push refused: {0}")]
    Refused(String),
    /// A store failed.
    #[error("storage: {0}")]
    Store(#[from] StoreError),
    /// The repository holds something the service cannot read.
    #[error("{0}")]
    Corrupt(String),
}

/// The result of a stroke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Painted {
    /// The push's sequence number.
    pub seq: u64,
    /// The new commit.
    pub commit: ObjectId,
    /// Its parent.
    pub parent: ObjectId,
}

/// A caller of receive-pack inside this process.
struct Internal(&'static str);

impl RefAccess for Internal {
    fn may_update(&self, _: &RefName) -> bool {
        true
    }

    fn caller(&self) -> Caller {
        Caller {
            principal: format!("place:{}", self.0),
            key: INTERNAL_KEY.to_owned(),
        }
    }
}

/// A tiny SplitMix64 generator for agent strokes, seeded from the OS.
#[derive(Debug)]
struct Rng(u64);

impl Rng {
    fn new() -> Self {
        let mut seed = [0u8; 8];
        let _ = getrandom::fill(&mut seed);
        Self(u64::from_le_bytes(seed))
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

#[derive(Debug)]
struct Ambient {
    name: String,
    x: u32,
    y: u32,
    color: u8,
    next_ms: u64,
}

/// Checks an `Authorization` header for operator rights (the kill switch).
pub trait Operators: Send + Sync + std::fmt::Debug {
    /// Whether the header names an operator. `address` is the client's
    /// address (`None` when unknown), whose failed authentications are
    /// limited as Git requests' are: an error while it has used them up,
    /// before the credential is checked (HTTP 429).
    fn is_operator<'a>(
        &'a self,
        authorization: Option<&'a [u8]>,
        address: Option<std::net::IpAddr>,
    ) -> aspen_server::receive::hook::HookFuture<'a, Result<bool, aspen_auth::LimitError>>;
}

/// Operators are callers with an `admin` grant on the canvas repository,
/// through Aspen's own tokens or OIDC, checked by the same access policy
/// that guards Git (A9 review obligation 1: real operator credentials,
/// never a static token). `K` is the access layer's SSH key store; `place`
/// itself serves Git over HTTP only, so it builds this with
/// [`aspen_auth::NoSshKeys`].
#[derive(Debug)]
pub struct AdminGrant<M, T, K = aspen_auth::NoSshKeys> {
    /// The access layer Git requests pass.
    pub access: Arc<aspen_server::access::Access<M, T, K>>,
    /// The canvas repository.
    pub repo: RepoName,
}

impl<M, T, K> Operators for AdminGrant<M, T, K>
where
    M: MetaStore + std::fmt::Debug + 'static,
    T: aspen_auth::TokenStore + std::fmt::Debug + 'static,
    K: aspen_auth::SshKeyStore + std::fmt::Debug + 'static,
{
    fn is_operator<'a>(
        &'a self,
        authorization: Option<&'a [u8]>,
        address: Option<std::net::IpAddr>,
    ) -> aspen_server::receive::hook::HookFuture<'a, Result<bool, aspen_auth::LimitError>> {
        Box::pin(async move {
            let Some(header) = authorization else {
                return Ok(false);
            };
            let Ok(credential) = aspen_auth::credential::from_authorization(Some(header)) else {
                return Ok(false);
            };
            // Through the access layer with the key the Git guard uses for
            // the same address, so a stream of bad operator credentials
            // drains the address's failed-authentication bucket shared
            // with Git requests (C7 review obligation 1), and is refused
            // without a check once it is empty.
            let address = address.map_or_else(
                || aspen_server::access::UNKNOWN_ADDRESS.to_owned(),
                aspen_server::access::ip_key,
            );
            match self.access.identify(&credential, &address).await {
                Ok(identity) if !identity.is_anonymous() => Ok(identity
                    .authorize(&self.repo, aspen_auth::Need::Admin)
                    .is_ok()),
                Err(aspen_server::access::Refusal::Limit(limited)) => Err(limited),
                _ => Ok(false),
            }
        })
    }
}

/// A stroke's entries in [`Shared::accepted`] and [`Shared::committed`],
/// removed when it is dropped.
struct Cleanup<'a> {
    shared: &'a Shared,
    name: &'a str,
    commit: ObjectId,
}

impl Cleanup<'_> {
    /// Removes the entries and returns the push's sequence number, if it
    /// committed.
    fn finish(self) -> Option<u64> {
        self.shared
            .committed
            .lock()
            .expect("not poisoned")
            .remove(&(self.name.to_owned(), self.commit))
    }
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        self.shared.accepted.lock().expect("not poisoned").take(
            Writer::Server,
            self.name,
            self.commit,
        );
        self.shared
            .committed
            .lock()
            .expect("not poisoned")
            .remove(&(self.name.to_owned(), self.commit));
    }
}

/// Entries the push hook keeps between an update's check and its commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookBacklog {
    /// Checked updates waiting to commit (bounded, [`Waiting`]).
    pub accepted: usize,
    /// Sequence numbers of the server's own committed pushes not yet
    /// taken by their writer. Zero whenever no stroke is in flight.
    pub committed: usize,
}

/// The service (module docs).
#[derive(Debug)]
pub struct Place<M, B> {
    pub(crate) shared: Arc<Shared>,
    pub(crate) config: PlaceConfig,
    svc: Arc<ReceivePack<M, B>>,
    meta: M,
    repo: Repo,
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    merge_lock: tokio::sync::Mutex<()>,
    permits: Arc<Semaphore>,
    rng: Mutex<Rng>,
    ambient: Mutex<Vec<Ambient>>,
    /// Locked inside `shared.state` by [`Place::start_swarm`]; never lock
    /// `shared.state` while holding it.
    pub(crate) sessions: Mutex<crate::api::Sessions>,
    pub(crate) event_streams: Arc<Slots>,
    pub(crate) operators: Arc<dyn Operators>,
}

/// The canvas repository, created with `main` as trunk if missing.
pub async fn ensure_repo<M: MetaStore>(meta: &M, name: &str) -> Result<Repo, PlaceError> {
    let name = RepoName::new(name).map_err(|e| PlaceError::Corrupt(e.to_string()))?;
    if let Some(r) = meta.repo(&name).await? {
        return Ok(r);
    }
    let trunk = RefName::new(TRUNK_REF).expect("valid");
    let settings = RepoSettings::new(trunk).map_err(|e| PlaceError::Corrupt(e.to_string()))?;
    match meta.create_repo(&name, &settings).await {
        Ok(r) => Ok(r),
        // Another instance created it first.
        Err(StoreError::Conflict { .. }) => meta
            .repo(&name)
            .await?
            .ok_or_else(|| PlaceError::Corrupt("repository vanished".into())),
        Err(e) => Err(e.into()),
    }
}

/// The state the hook and the service share, before `main` is loaded.
/// Give `PlaceHook(shared.clone())` to the receive-pack configuration,
/// then call [`Place::open`].
pub fn shared(repo: &Repo, config: &PlaceConfig) -> Arc<Shared> {
    let rules = Arc::new(config.rules.clone());
    let palette_blobs = rules
        .palette
        .iter()
        .map(|p| crate::git::object_id(ObjectKind::Blob, &rules::tile_bytes(&p.color)))
        .collect();
    let main = Main {
        commit: ObjectId::ZERO,
        pixels: Arc::new(vec![UNSET; rules.len()]),
        extras: Arc::new(Vec::new()),
    };
    let mut state = State::new(rules.clone(), main, config.swarm, config.event_backlog);
    state.kill.engaged = config.kill_switch;
    Arc::new(Shared {
        repo: repo.id,
        rules,
        state: Mutex::new(state),
        limits: CheckLimits {
            max_commits: config.max_commits,
            max_tree_entries: config.max_tree_entries,
            pushes_per_minute: config.pushes_per_minute,
        },
        palette_blobs,
        accepted: Mutex::new(Waiting::default()),
        committed: Mutex::new(HashMap::new()),
        git_quota: Mutex::new(Windows::new(config.pushes_per_minute, 60_000, 100_000)),
    })
}

const README: &str = "# place\n\n\
A shared pixel canvas. Every tile is a file, tiles/<x>_<y>.txt, holding one\n\
palette color as exactly \"#rrggbb\" and a newline. Paint on your own branch:\n\n\
    printf '#d6262b\\n' > tiles/12_40.txt\n\
    git add tiles && git commit -m 'paint'\n\
    git push origin HEAD:refs/heads/paint/<you>\n\n\
A merger bot folds every branch into main every few seconds. Only tile files\n\
may change; see docs/TILES.md in the Aspen repository for every rule.\n";

impl<M, B> Place<M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// Loads `main` from the repository (writing a first commit with a
    /// README if there is none) and the live branches, and returns the
    /// service. `svc` must have been built with `PlaceHook(shared)`.
    pub async fn open(
        config: PlaceConfig,
        shared: Arc<Shared>,
        meta: M,
        svc: Arc<ReceivePack<M, B>>,
        repo: Repo,
        operators: Arc<dyn Operators>,
    ) -> Result<Arc<Self>, PlaceError> {
        config.validate().map_err(PlaceError::Corrupt)?;
        let sessions = crate::api::Sessions::new(&config);
        let event_streams = Slots::new(
            config.max_event_streams_per_address,
            config.max_event_streams,
        );
        let mut rng = Rng::new();
        let words = [
            "quill", "ember", "moss", "larch", "wren", "flint", "sorrel", "tamarack", "heron",
            "cinder",
        ];
        let now = now_ms();
        let ambient = (0..config.ambient_agents)
            .map(|i| Ambient {
                name: format!("agent/{}-{}", words[i % words.len()], 10 + rng.below(89)),
                x: rng.below(u64::from(config.rules.width)) as u32,
                y: rng.below(u64::from(config.rules.height)) as u32,
                color: rng.below(config.rules.palette.len() as u64) as u8,
                next_ms: now + 300 + rng.below(2000),
            })
            .collect();
        let place = Arc::new(Self {
            permits: Arc::new(Semaphore::new(config.agent_concurrency.max(1))),
            shared,
            config,
            svc,
            meta,
            repo,
            locks: Mutex::new(HashMap::new()),
            merge_lock: tokio::sync::Mutex::new(()),
            rng: Mutex::new(rng),
            ambient: Mutex::new(ambient),
            sessions: Mutex::new(sessions),
            event_streams,
            operators,
        });
        place.load_main().await?;
        place.load_branches().await?;
        Ok(place)
    }

    /// Holds the web sessions' lock (session table and per-address
    /// limits) until the guard is dropped. For tests that line up
    /// concurrent requests behind it; never hold it across an `.await` or
    /// while calling into `Place`.
    #[doc(hidden)]
    pub fn lock_sessions(&self) -> std::sync::MutexGuard<'_, crate::api::Sessions> {
        self.sessions.lock().expect("not poisoned")
    }

    /// The canvas repository.
    pub fn repo(&self) -> &Repo {
        &self.repo
    }

    /// What the push hook is holding between checks and commits.
    pub fn hook_backlog(&self) -> HookBacklog {
        HookBacklog {
            accepted: self.shared.accepted.lock().expect("not poisoned").len(),
            committed: self.shared.committed.lock().expect("not poisoned").len(),
        }
    }

    /// The configuration.
    pub fn config(&self) -> &PlaceConfig {
        &self.config
    }

    /// Runs `f` on the canvas state.
    pub fn with_state<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        f(&mut self.shared.state.lock().expect("not poisoned"))
    }

    fn trunk() -> RefName {
        RefName::new(TRUNK_REF).expect("valid")
    }

    async fn load_main(&self) -> Result<(), PlaceError> {
        let trunk = Self::trunk();
        let current = self.meta.current_version(self.repo.id, &trunk).await?;
        let main = match current.as_ref().and_then(|v| v.value.tip().map(|t| (v, t))) {
            Some((v, tip)) => {
                let (pixels, extras) = self.read_canvas(v, tip).await?;
                Main {
                    commit: tip,
                    pixels: Arc::new(pixels),
                    extras: Arc::new(extras),
                }
            }
            None => {
                let mut pb = PackBuilder::default();
                let readme = pb.add(ObjectKind::Blob, README.as_bytes().to_vec());
                let extras = vec![TreeEntry {
                    mode: MODE_FILE,
                    name: b"README.md".to_vec(),
                    id: readme,
                }];
                let pixels = vec![UNSET; self.shared.rules.len()];
                let commit = self.add_commit(&mut pb, &pixels, &extras, &[], "Start the canvas");
                match self
                    .push(
                        &trunk,
                        false,
                        ObjectId::ZERO,
                        commit,
                        Some(pb.finish()),
                        "merger",
                    )
                    .await?
                {
                    Report::Ok => {}
                    // Another instance started it first.
                    Report::Rejected(_) => return Box::pin(self.load_main()).await,
                }
                Main {
                    commit,
                    pixels: Arc::new(pixels),
                    extras: Arc::new(extras),
                }
            }
        };
        self.with_state(|s| s.main = main);
        Ok(())
    }

    async fn load_branches(&self) -> Result<(), PlaceError> {
        let mut after = None;
        loop {
            let page = self
                .meta
                .list_refs(
                    self.repo.id,
                    &RefListQuery {
                        prefixes: vec!["refs/heads/".to_owned()],
                        after: after.clone(),
                        limit: 1000,
                    },
                )
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(last.name.clone());
            let now = now_ms();
            self.with_state(|s| {
                for v in &page {
                    let Some(short) = v.name.as_str().strip_prefix("refs/heads/") else {
                        continue;
                    };
                    let (Some(tip), false) = (v.value.tip(), v.name.as_str() == TRUNK_REF) else {
                        continue;
                    };
                    let actor = actor_for(short);
                    // Restarted branches count as just updated, so a
                    // restart never deletes one early.
                    s.branches.insert(
                        short.to_owned(),
                        crate::state::Branch {
                            actor,
                            head: tip,
                            overlay: Default::default(),
                            updated_ms: now,
                            deleted: false,
                            full: None,
                        },
                    );
                }
            });
            if page.len() < 1000 {
                break;
            }
        }
        Ok(())
    }

    /// Every tile of `tip`'s tree (a commit of `version`), and the root
    /// entries other than `tiles`. Files in `tiles/` that are not valid
    /// tiles are skipped: the rules never let one in.
    async fn read_canvas(
        &self,
        version: &RefVersion,
        tip: ObjectId,
    ) -> Result<(Vec<u8>, Vec<TreeEntry>), PlaceError> {
        let read = |id: ObjectId, want: ObjectKind| async move {
            match self.svc.read_object(self.repo.id, version.id, id).await? {
                Some((k, d)) if k == want => Ok(d),
                _ => Err(PlaceError::Corrupt(format!(
                    "{id} is missing or not a {want:?}"
                ))),
            }
        };
        let (tree, _) =
            parse_commit(&read(tip, ObjectKind::Commit).await?).map_err(PlaceError::Corrupt)?;
        let root = parse_tree(&read(tree, ObjectKind::Tree).await?).map_err(PlaceError::Corrupt)?;
        let rules = &self.shared.rules;
        let mut pixels = vec![UNSET; rules.len()];
        let mut extras = Vec::new();
        for e in root {
            if e.name != b"tiles" || !e.is_tree() {
                extras.push(e);
                continue;
            }
            let tiles =
                parse_tree(&read(e.id, ObjectKind::Tree).await?).map_err(PlaceError::Corrupt)?;
            for t in tiles {
                let path = format!("tiles/{}", String::from_utf8_lossy(&t.name));
                let Ok((x, y)) = rules::parse_tile_path(&path, rules) else {
                    continue;
                };
                let color = match self.shared.palette_blobs.iter().position(|b| *b == t.id) {
                    Some(c) => c as u8,
                    None => match rules::parse_tile_content(
                        &read(t.id, ObjectKind::Blob).await?,
                        rules,
                        &path,
                    ) {
                        Ok(c) => c,
                        Err(_) => continue,
                    },
                };
                pixels[rules.index(x, y)] = color;
            }
        }
        Ok((pixels, extras))
    }

    /// Adds the blobs of `colors`, the trees for `pixels` and a commit to
    /// `pb`, and returns the commit.
    fn add_commit(
        &self,
        pb: &mut PackBuilder,
        pixels: &[u8],
        extras: &[TreeEntry],
        parents: &[ObjectId],
        message: &str,
    ) -> ObjectId {
        let rules = &self.shared.rules;
        let w = rules.width as usize;
        let mut used = [false; 256];
        let mut tiles = Vec::new();
        for (i, &c) in pixels.iter().enumerate() {
            if c == UNSET {
                continue;
            }
            used[usize::from(c)] = true;
            tiles.push(TreeEntry {
                mode: MODE_FILE,
                name: format!("{}_{}.txt", i % w, i / w).into_bytes(),
                id: self.shared.palette_blobs[usize::from(c)],
            });
        }
        // Every color the tree uses goes in the pack; receive-pack stores
        // only the ones the dependencies lack, and the connectivity check
        // then never depends on which branch a color came from.
        for (c, _) in used.iter().enumerate().filter(|(_, u)| **u) {
            pb.add(ObjectKind::Blob, rules::tile_bytes(&rules.palette[c].color));
        }
        let mut root = extras.to_vec();
        if !tiles.is_empty() {
            let id = pb.add(ObjectKind::Tree, write_tree(tiles));
            root.push(TreeEntry {
                mode: MODE_TREE,
                name: b"tiles".to_vec(),
                id,
            });
        }
        let root = pb.add(ObjectKind::Tree, write_tree(root));
        let secs = (now_ms() / 1000) as i64;
        pb.add(
            ObjectKind::Commit,
            write_commit(root, parents, "place <place@aspen.invalid>", secs, message),
        )
    }

    /// One in-process push of `new` to `name`, expecting `old`.
    async fn push(
        &self,
        name: &RefName,
        scoped: bool,
        old: ObjectId,
        new: ObjectId,
        pack: Option<Vec<u8>>,
        role: &'static str,
    ) -> Result<Report, StoreError> {
        let body = receive_request(name.as_str(), old, new, pack);
        let out = self
            .svc
            .push(
                &self.repo,
                scoped.then_some(name),
                &Internal(role),
                None,
                std::io::Cursor::new(body),
            )
            .await
            .map_err(|ServiceError::Store(e)| e)?;
        Ok(crate::git::parse_report(&out.response))
    }

    fn branch_lock(&self, branch: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .expect("not poisoned")
            .entry(branch.to_owned())
            .or_default()
            .clone()
    }

    /// Paints `tiles` (already checked with [`rules::validate_stroke`]) as
    /// one commit on `branch` (a short name), on top of the branch's tip,
    /// or of `main` when the branch does not exist.
    pub async fn paint(
        &self,
        branch: &str,
        actor: Actor,
        tiles: &[Tile],
        message: &str,
    ) -> Result<Painted, PlaceError> {
        let name = RefName::new(format!("refs/heads/{branch}"))
            .map_err(|e| PlaceError::Refused(e.to_string()))?;
        let lock = self.branch_lock(branch);
        let _guard = lock.lock().await;
        let known = self.with_state(|s| {
            s.branches
                .get(branch)
                .filter(|b| !b.deleted)
                .map(|b| (b.head, b.full.clone()))
        });
        let (old, base) = match known {
            Some((head, Some(full))) => (head, full),
            _ => match self.meta.current_version(self.repo.id, &name).await? {
                Some(v) if v.value.tip().is_some() => {
                    let tip = v.value.tip().expect("live");
                    (tip, Arc::new(self.read_canvas(&v, tip).await?.0))
                }
                _ => (ObjectId::ZERO, self.with_state(|s| s.main.pixels.clone())),
            },
        };
        let (parent, extras) = self.with_state(|s| {
            (
                if old.is_zero() { s.main.commit } else { old },
                s.main.extras.clone(),
            )
        });
        let mut pixels = (*base).clone();
        for t in tiles {
            pixels[self.shared.rules.index(t.x, t.y)] = t.color;
        }
        let mut pb = PackBuilder::default();
        let message = if message.trim().is_empty() {
            format!("Paint {} tiles", tiles.len())
        } else {
            message.to_owned()
        };
        let commit = self.add_commit(&mut pb, &pixels, &extras, &[parent], &message);
        // Removes this write's entries however it ends, a dropped request
        // included.
        let cleanup = Cleanup {
            shared: &self.shared,
            name: name.as_str(),
            commit,
        };
        self.shared.accept(
            Writer::Server,
            name.as_str(),
            commit,
            Accepted {
                actor,
                tiles: tiles.to_vec(),
                full: Some(Arc::new(pixels)),
                at_ms: crate::state::now_ms(),
            },
        );
        let report = self
            .push(&name, true, old, commit, Some(pb.finish()), "painter")
            .await;
        let seq = cleanup.finish();
        match (report?, seq) {
            (Report::Ok, Some(seq)) => Ok(Painted {
                seq,
                commit,
                parent,
            }),
            (Report::Ok, None) => Err(PlaceError::Corrupt(
                "the push committed without the hook seeing it".into(),
            )),
            (Report::Rejected(r), _) => Err(PlaceError::Refused(r)),
        }
    }

    /// The merger bot's one run: folds every pending push into `main` as
    /// one fast-forward commit. Returns the merge's sequence number, or
    /// `None` when nothing was pending.
    pub async fn merge_once(&self) -> Result<Option<u64>, PlaceError> {
        let _guard = self.merge_lock.lock().await;
        let (plan, main) = self.with_state(|s| (s.plan_merge(), s.main.clone()));
        let Some(plan) = plan else { return Ok(None) };
        let mut pb = PackBuilder::default();
        let message = format!(
            "Merge {} branches through push {}\n\nMerged by the place merger bot: each push's tiles are\nreplayed onto main in push order.",
            plan.branches.len(),
            plan.through
        );
        let commit = self.add_commit(
            &mut pb,
            &plan.pixels,
            &main.extras,
            &[main.commit],
            &message,
        );
        match self
            .push(
                &Self::trunk(),
                false,
                main.commit,
                commit,
                Some(pb.finish()),
                "merger",
            )
            .await?
        {
            Report::Ok => Ok(Some(
                self.with_state(|s| s.apply_merge(commit, plan, now_ms())),
            )),
            Report::Rejected(r) => {
                // `main` moved under us (an operator, another instance):
                // start again from what is stored.
                self.load_main().await?;
                Err(PlaceError::Refused(r))
            }
        }
    }

    /// Deletes branches whose tiles have all merged and that have been
    /// idle for the branch TTL, or sooner for the server's own painters
    /// ([`crate::state::State::expirable`]), so their strokes stop being
    /// advertised (and fetched by every clone) once they are in `main`;
    /// the next stroke starts the branch again from `main`. Returns how
    /// many expired.
    pub async fn expire_once(&self) -> Result<usize, PlaceError> {
        let ttl = self.config.branch_ttl.as_millis() as u64;
        let due = self.with_state(|s| s.expirable(ttl, now_ms()));
        let mut n = 0;
        for (branch, head, deleted) in due {
            let lock = self.branch_lock(&branch);
            let _guard = lock.lock().await;
            let still = self.with_state(|s| {
                s.expirable(ttl, now_ms())
                    .iter()
                    .any(|(b, h, _)| *b == branch && *h == head)
            });
            if !still {
                continue;
            }
            if !deleted {
                let name = RefName::new(format!("refs/heads/{branch}"))
                    .map_err(|e| PlaceError::Corrupt(e.to_string()))?;
                let report = self
                    .push(&name, true, head, ObjectId::ZERO, None, "expiry")
                    .await?;
                // Refused because it moved since (the new push will be
                // shown), or because it is already gone: expire it then.
                if report != Report::Ok && report.reason() != Some(RejectReason::NoSuchRef) {
                    continue;
                }
            }
            // A delete through receive-pack already expired it (the hook's
            // `committed`); this covers branches deleted in Git earlier.
            self.with_state(|s| s.expire(&branch, now_ms()));
            self.locks.lock().expect("not poisoned").remove(&branch);
            n += 1;
        }
        Ok(n)
    }

    /// Engages or releases the kill switch: engaging ends a running swarm
    /// at once and pauses the ambient agents.
    pub fn set_kill_switch(&self, engaged: bool, reason: Option<String>) -> serde_json::Value {
        self.with_state(|s| {
            let now = now_ms();
            s.kill.engaged = engaged;
            s.kill.reason = if engaged { reason } else { None };
            if engaged && s.swarm.state == crate::state::SwarmState::Running {
                s.end_swarm_run(now);
            }
            s.emit_swarm(now);
            s.swarm_status(now)
        })
    }

    /// Starts a swarm run for the client `starter` (its address key),
    /// unless the kill switch is engaged, a run is running or cooling down,
    /// or `starter` has used its starts for the hour. The run's size and
    /// rate come from the server's limits only.
    ///
    /// The checks, the charge to `starter`'s starts and the start itself
    /// happen under one lock, so concurrent starts from one address spend
    /// one of its starts between them, and only a start that runs is
    /// charged (#124).
    pub fn start_swarm(&self, starter: &str) -> Result<serde_json::Value, (StartRefusal, u64)> {
        self.with_state(|s| {
            let now = now_ms();
            if s.kill.engaged {
                return Err((
                    StartRefusal::KillSwitch {
                        reason: s.kill.reason.clone(),
                    },
                    300,
                ));
            }
            let secs = |t: u64| t.saturating_sub(now).div_ceil(1000).max(1);
            match s.swarm.state {
                crate::state::SwarmState::Running => {
                    return Err((StartRefusal::Cooldown, secs(s.swarm.ends_ms.unwrap_or(now))));
                }
                crate::state::SwarmState::Cooldown => {
                    if let Some(t) = s.swarm.cooldown_until_ms.filter(|t| *t > now) {
                        return Err((StartRefusal::Cooldown, secs(t)));
                    }
                }
                crate::state::SwarmState::Idle => {}
            }
            // Lock order: `shared.state`, then `sessions` (field docs).
            let charged = self
                .sessions
                .lock()
                .expect("not poisoned")
                .swarm_starts
                .admit(starter, now);
            if let Err(ms) = charged {
                return Err((StartRefusal::StartsUsed, ms.div_ceil(1000).max(1)));
            }
            let run = u64::from(s.swarm_limits.run_seconds) * 1000;
            s.swarm = crate::state::Swarm {
                state: crate::state::SwarmState::Running,
                started_ms: Some(now),
                ends_ms: Some(now + run),
                cooldown_until_ms: None,
                pushes: 0,
                credit: 0.0,
                last_tick_ms: now,
                last_broadcast_ms: now,
            };
            s.emit_swarm(now);
            Ok(s.swarm_status(now))
        })
    }

    /// Runs the merger bot, branch expiry, the ambient agents and the
    /// swarm until `shutdown` resolves, then merges what is pending one
    /// last time.
    pub async fn run(self: Arc<Self>, shutdown: impl std::future::Future<Output = ()>) {
        let mut merge = tokio::time::interval(self.config.merge_interval);
        merge.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // When a merge with only ambient pushes may next run.
        let mut ambient_due = tokio::time::Instant::now();
        let mut agents = tokio::time::interval(Duration::from_millis(100));
        agents.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                _ = merge.tick() => {
                    let now = tokio::time::Instant::now();
                    let prompt = self.with_state(|s| {
                        s.pending.iter().any(|p| !p.branch.starts_with(AMBIENT_PREFIX))
                    });
                    if prompt || now >= ambient_due {
                        match self.merge_once().await {
                            Ok(Some(_)) => {
                                ambient_due = now + self.config.ambient_merge_interval;
                            }
                            Ok(None) => {}
                            Err(e) => eprintln!("place: merge failed: {e}"),
                        }
                    }
                    if let Err(e) = self.expire_once().await {
                        eprintln!("place: branch expiry failed: {e}");
                    }
                }
                _ = agents.tick() => self.agents_tick(),
            }
        }
        if let Err(e) = self.merge_once().await {
            eprintln!("place: final merge failed: {e}");
        }
    }

    /// Agent pushes (swarm and ambient) in flight right now.
    pub fn agent_pushes_in_flight(&self) -> usize {
        self.config.agent_concurrency.max(1) - self.permits.available_permits()
    }

    /// One tick of the agents: the swarm's credit, then each ambient agent
    /// that is due. Pushes beyond the free permits are dropped.
    pub fn agents_tick(self: &Arc<Self>) {
        let now = now_ms();
        let (swarm_pushes, agents, killed) = self.with_state(|s| {
            let n = s.swarm_tick(now);
            (n, s.swarm_agents(now), s.kill.engaged)
        });
        let rules = self.shared.rules.clone();
        for _ in 0..swarm_pushes {
            let Ok(permit) = self.permits.clone().try_acquire_owned() else {
                break;
            };
            let (branch, tiles) = {
                let mut rng = self.rng.lock().expect("not poisoned");
                let agent = rng.below(u64::from(agents.max(1)));
                let n = 1 + rng.below(3);
                let mut tiles: Vec<Tile> = Vec::new();
                for _ in 0..n {
                    let (x, y) = (
                        rng.below(u64::from(rules.width)) as u32,
                        rng.below(u64::from(rules.height)) as u32,
                    );
                    if tiles.iter().all(|t| (t.x, t.y) != (x, y)) {
                        let color = rng.below(rules.palette.len() as u64) as u8;
                        tiles.push(Tile { x, y, color });
                    }
                }
                (format!("swarm/agent-{agent:04}"), tiles)
            };
            let place = self.clone();
            tokio::spawn(async move {
                let ok = place
                    .paint(&branch, Actor::agent(&branch), &tiles, "Swarm stroke")
                    .await
                    .is_ok();
                if ok {
                    place.with_state(|s| s.swarm.pushes += 1);
                }
                drop(permit);
            });
        }
        if killed {
            return;
        }
        let due: Vec<(String, Vec<Tile>)> = {
            let mut ambient = self.ambient.lock().expect("not poisoned");
            let mut rng = self.rng.lock().expect("not poisoned");
            let mut out = Vec::new();
            for a in ambient.iter_mut().filter(|a| a.next_ms <= now) {
                let mean = (self.config.ambient_interval.as_millis() as u64).max(2);
                a.next_ms = now + mean / 2 + rng.below(mean);
                if rng.below(10) < 3 {
                    a.color = rng.below(rules.palette.len() as u64) as u8;
                }
                let mut tiles: Vec<Tile> = Vec::new();
                for _ in 0..2 + rng.below(6) {
                    let step = |v: u32, max: u32, r: u64| {
                        (i64::from(v) + r as i64 - 1).clamp(0, i64::from(max) - 1) as u32
                    };
                    a.x = step(a.x, rules.width, rng.below(3));
                    a.y = step(a.y, rules.height, rng.below(3));
                    if tiles.iter().all(|t| (t.x, t.y) != (a.x, a.y)) {
                        tiles.push(Tile {
                            x: a.x,
                            y: a.y,
                            color: a.color,
                        });
                    }
                }
                out.push((a.name.clone(), tiles));
            }
            out
        };
        for (branch, tiles) in due {
            let Ok(permit) = self.permits.clone().try_acquire_owned() else {
                break;
            };
            let place = self.clone();
            tokio::spawn(async move {
                let _ = place
                    .paint(&branch, Actor::agent(&branch), &tiles, "Ambient stroke")
                    .await;
                drop(permit);
            });
        }
    }
}

/// Why a swarm could not start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartRefusal {
    /// The kill switch is engaged, with the operators' reason if they gave
    /// one.
    KillSwitch {
        /// The reason given when it was engaged.
        reason: Option<String>,
    },
    /// A run is in progress or cooling down.
    Cooldown,
    /// The client address has started the swarm too often this hour.
    StartsUsed,
}

/// The ambient agents' branch namespace.
pub const AMBIENT_PREFIX: &str = "agent/";

/// The actor a branch's pushes are shown as, from its namespace.
pub fn actor_for(short: &str) -> Actor {
    if let Some(id) = short.strip_prefix("visitors/") {
        Actor::visitor(id)
    } else if short.starts_with("agent/") || short.starts_with("swarm/") {
        Actor::agent(short)
    } else {
        Actor::git(short)
    }
}

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
    receive_request, write_tree,
};
use crate::hook::{Accepted, CheckLimits, INTERNAL_KEY, MainHistory, Shared, Waiting, Writer};
use crate::limits::{self, BucketProblem, PushCeiling, Slots, Windows};
use crate::rules::{self, AMBIENT_PREFIX, Rules, TRUNK_REF, Tile};
use crate::state::{Actor, History, Main, State, SwarmLimits, UNSET, now_ms};

/// Settings of a `place` deployment.
#[derive(Debug, Clone)]
pub struct PlaceConfig {
    /// The canvas repository's name (`canvas` serves `/canvas.git`).
    pub repo: String,
    /// Canvas size, palette and tiles per push.
    pub rules: Rules,
    /// Pushes per web session, and per Git caller and branch, per minute.
    pub pushes_per_minute: usize,
    /// The server-wide push ceiling: pushes a second from everyone
    /// together, web strokes and Git updates alike (at least
    /// [`limits::MIN_BUCKET_PER_SECOND`]).
    /// Past it, and once [`Self::global_push_burst`] is spent, a push is
    /// refused with a message that the demo is at its limit (`429` with
    /// `Retry-After` on the API, the `ng` reason for Git). It bounds the
    /// pushes the server commits and the merger bot replays, whatever the
    /// number of callers; the server's own agents and merges do not count.
    pub global_pushes_per_second: f64,
    /// Pushes the ceiling lets through at once after a quiet spell (at
    /// least 1, and refilled within [`limits::MAX_BUCKET_REFILL_SECONDS`]
    /// at [`Self::global_pushes_per_second`]).
    pub global_push_burst: u32,
    /// A branch idle this long, with all its tiles merged, is deleted.
    pub branch_ttl: Duration,
    /// How often the merger bot runs while pushes arrive at up to
    /// [`Self::busy_pushes_per_second`].
    pub merge_interval: Duration,
    /// Above this many pushes a second (measured since the last merge) the
    /// merger waits longer between runs, in proportion to the rate, up to
    /// [`Self::merge_interval_max`] ([`merge_wait`]). A merge costs the
    /// same however many pushes it folds, so under load fewer, larger
    /// merges keep the merger's share of the server and the growth of
    /// `main`'s history down; the page shows unmerged pushes as branch
    /// overlays meanwhile.
    pub busy_pushes_per_second: u32,
    /// The longest the merger waits between runs under load (never less
    /// than [`Self::merge_interval`]).
    pub merge_interval_max: Duration,
    /// How often, at most, the merger bot merges when only ambient agents
    /// (`agent/`) have pushes pending; a push from anyone else is merged on
    /// the next run and takes theirs along. Every merge stores a whole
    /// `tiles/` tree in `main`'s history (about 12 KiB on a full 64 x 64
    /// canvas: stored packs have no deltas), so this slows how fast idle
    /// painting uses up [`Self::history_max_bytes`].
    pub ambient_merge_interval: Duration,
    /// Once the commits written on `main` since its root (each with its
    /// whole `tiles/` tree) were stored in packs of this many bytes in all,
    /// the merger starts `main`'s history again from one root commit
    /// holding the canvas as it is ([`Place::bound_history`]), so a full
    /// clone of `main` stays about this size. 0 never starts it again.
    pub history_max_bytes: u64,
    /// Mean time between the pushes of one sprite an ambient agent paints
    /// (each is spread uniformly over half to one and a half times this).
    /// Every push is a commit on the agent's branch, advertised until it
    /// merges.
    pub ambient_interval: Duration,
    /// Mean time an ambient agent rests between two sprites ([`crate::sprites`]).
    pub ambient_sprite_pause: Duration,
    /// Areas `(x, y, width, height)` ambient agents never paint over, for
    /// people's work painted before the server started (it remembers what
    /// people paint only in memory, for six hours).
    pub keep_areas: Vec<(u32, u32, u32, u32)>,
    /// What `GET /config` tells people to `git clone`.
    pub clone_url: String,
    /// The live dashboard, if there is one.
    pub dashboard_url: Option<String>,
    /// Where `main` is mirrored read-only, if anywhere (named in the
    /// canvas's README).
    pub mirror_url: Option<String>,
    /// Background agents that paint a stroke every few seconds each.
    /// Ignored while [`Self::personas`] is on: the personas replace them.
    pub ambient_agents: usize,
    /// Personas ([`crate::personas`]): pretend painters who drop in on
    /// shifts and paint pictures one tile per push, so the canvas always
    /// looks alive. `None` (the default) leaves them out and runs the
    /// ambient sprite agents instead.
    pub personas: Option<crate::personas::PersonaSettings>,
    /// The swarm's limits.
    pub swarm: SwarmLimits,
    /// Swarm runs one client address may start per hour.
    pub swarm_starts_per_hour: usize,
    /// Whether visitors may start the swarm through `POST /swarm`. Off on the
    /// public demo, where one click paints one tile and nothing else.
    pub swarm_enabled: bool,
    /// Tiles one push through the web API may carry. The public page sends
    /// one tile per click; Git pushes keep `rules.tiles_per_push`.
    pub web_tiles_per_push: usize,
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
    /// The event feed's first `seq` is one past this. The binary sets the
    /// start time in ms, so a task started later usually numbers above an
    /// earlier one. Clients that name the feed's `instance` with their
    /// cursor are resynced on any mismatch whatever the numbers
    /// ([`crate::state::State::subscribe`]); this only helps those that do
    /// not.
    pub first_seq: u64,
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
            global_pushes_per_second: 550.0,
            global_push_burst: 2750,
            branch_ttl: Duration::from_secs(600),
            merge_interval: Duration::from_millis(3000),
            busy_pushes_per_second: 5,
            merge_interval_max: Duration::from_secs(30),
            ambient_merge_interval: Duration::from_secs(300),
            history_max_bytes: 8 << 20,
            ambient_interval: Duration::from_secs(30),
            ambient_sprite_pause: Duration::from_secs(1800),
            keep_areas: Vec::new(),
            clone_url: clone_url.into(),
            dashboard_url: None,
            mirror_url: None,
            ambient_agents: 6,
            personas: None,
            swarm: SwarmLimits {
                max_agents: 1000,
                pushes_per_second_cap: 120,
                run_seconds: 20,
                cooldown_seconds: 40,
                ramp_ms: 3000,
            },
            swarm_starts_per_hour: 6,
            swarm_enabled: true,
            web_tiles_per_push: 64,
            sessions_per_minute: 10,
            max_sessions: 100_000,
            event_backlog: 5000,
            max_event_streams: 20_000,
            max_event_streams_per_address: 64,
            event_stream_max_age: Duration::from_secs(600),
            max_commits: 1000,
            max_tree_entries: 200_000,
            agent_concurrency: 16,
            client_addr: aspen_server::access::ClientAddr::Peer,
            kill_switch: false,
            first_seq: 0,
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
        if let Some(problem) =
            limits::bucket_problem(self.global_pushes_per_second, self.global_push_burst)
        {
            let setting = match problem {
                BucketProblem::Rate(_) => "PLACE_GLOBAL_PUSHES_PER_SECOND",
                BucketProblem::Burst => "PLACE_GLOBAL_PUSH_BURST",
                BucketProblem::Refill { .. } => {
                    "PLACE_GLOBAL_PUSH_BURST and PLACE_GLOBAL_PUSHES_PER_SECOND"
                }
            };
            return Err(format!("{setting}: {problem}"));
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
        if self.merge_interval.is_zero() {
            return Err("the merge interval must be above 0".into());
        }
        if self.busy_pushes_per_second == 0 {
            return Err("busy pushes per second must be at least 1".into());
        }
        if let Some(p) = &self.personas {
            p.validate(crate::personas::ROSTER.len())?;
        }
        Ok(())
    }
}

/// How long the merger waits after a run before the next, given `pending`
/// pushes arrived in the `elapsed` time since it: `merge_interval` while
/// that is at most `busy_pushes_per_second`, and longer in proportion to
/// the rate above it, up to `merge_interval_max`. At the defaults (3 s, 5
/// pushes a second, 30 s) the merger runs every 3 s up to 5 pushes a
/// second, every 6 s at 10 and every 30 s from 50.
pub fn merge_wait(config: &PlaceConfig, pending: usize, elapsed: Duration) -> Duration {
    let base = config.merge_interval;
    let elapsed_ms = elapsed.as_millis().max(1);
    // base * (pending / elapsed) / busy, in milliseconds.
    let wanted_ms = base.as_millis() * (pending as u128) * 1000
        / (elapsed_ms * u128::from(config.busy_pushes_per_second.max(1)));
    let wanted = Duration::from_millis(u64::try_from(wanted_ms).unwrap_or(u64::MAX));
    wanted.clamp(base, config.merge_interval_max.max(base))
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
    /// The sprite being painted, if any.
    job: Option<SpriteJob>,
    next_ms: u64,
}

/// A sprite an ambient agent is painting, push by push.
#[derive(Debug)]
struct SpriteJob {
    tiles: Vec<Tile>,
    next: usize,
    rect: Rect,
}

/// A rectangle of tiles: `(x, y, width, height)`.
type Rect = (u32, u32, u32, u32);

fn overlap(a: Rect, b: Rect) -> u32 {
    let w = (a.0 + a.2).min(b.0 + b.2).saturating_sub(a.0.max(b.0));
    let h = (a.1 + a.3).min(b.1 + b.3).saturating_sub(a.1.max(b.1));
    w * h
}

/// Where to put a `w` x `h` sprite on the canvas: the best of a few
/// random spots, never one covering `human` (tiles people painted
/// recently), and preferring spots clear of `busy` (other agents' sprites,
/// recent ones included) so sprites spread out. `None` when every spot
/// tried covers people's work.
fn choose_spot(
    rng: &mut Rng,
    rules: &Rules,
    (w, h): (u32, u32),
    human: &[usize],
    busy: &[Rect],
) -> Option<(u32, u32)> {
    if w > rules.width || h > rules.height {
        return None;
    }
    let mut best: Option<(u32, (u32, u32))> = None;
    for _ in 0..48 {
        let x = rng.below(u64::from(rules.width - w + 1)) as u32;
        let y = rng.below(u64::from(rules.height - h + 1)) as u32;
        let rect = (x, y, w, h);
        let covers_people = human.iter().any(|&i| {
            let (tx, ty) = (i as u32 % rules.width, i as u32 / rules.width);
            tx >= x && tx < x + w && ty >= y && ty < y + h
        });
        if covers_people {
            continue;
        }
        let score: u32 = busy.iter().map(|b| overlap(rect, *b)).sum();
        if best.is_none_or(|(s, _)| score < s) {
            best = Some((score, (x, y)));
        }
        if score == 0 {
            break;
        }
    }
    best.map(|(_, spot)| spot)
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
    /// The personas, when [`PlaceConfig::personas`] is on. Never lock
    /// `shared.state` while holding it.
    personas: Mutex<Option<crate::personas::Director>>,
    /// False while [`Place::run`]'s [`Place::rebuild_memory`] is still
    /// reading recent paint from `main`'s merge commits.
    memory_ready: std::sync::atomic::AtomicBool,
    /// The newest `main` commit whose merges' memory of people's and
    /// sprites' tiles is in the state ([`Self::remember_main_until`] walked
    /// down from it), `None` before the first walk. Separate from the
    /// canvas's `main.commit`: a reload advances the canvas before the walk,
    /// and a walk that fails is retried from here on the next follow.
    remembered: Mutex<Option<ObjectId>>,
    /// Locked inside `shared.state` by [`Place::start_swarm`]; never lock
    /// `shared.state` while holding it.
    pub(crate) sessions: Mutex<crate::api::Sessions>,
    pub(crate) event_streams: Arc<Slots>,
    pub(crate) operators: Arc<dyn Operators>,
    /// `GET /canvas.svg`, rendered once per `main` commit.
    pub(crate) svg: crate::image::SvgCache,
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
/// then call [`Place::open`]. `main_history` is usually
/// [`crate::hook::IndexedMain`] over the metadata store.
///
/// # Panics
///
/// If `config` fails [`PlaceConfig::validate`] on the push ceiling's rate
/// or burst ([`limits::bucket_problem`]); [`crate::build`] validates
/// first.
pub fn shared(
    repo: &Repo,
    config: &PlaceConfig,
    main_history: Arc<dyn MainHistory>,
) -> Arc<Shared> {
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
        history: History::UNKNOWN,
    };
    let mut state = State::new(rules.clone(), main, config.swarm, config.event_backlog);
    state.kill.engaged = config.kill_switch;
    state.start_seq_at(config.first_seq);
    state.instance = format!("{:016x}", Rng::new().next());
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
        main_history,
        pushes: Mutex::new(PushCeiling::new(
            config.global_pushes_per_second,
            config.global_push_burst,
            std::time::Instant::now(),
        )),
    })
}

/// The canvas's `README.md`. Every commit the server writes keeps it, and
/// [`Place::open`] rewrites `main`'s copy when it differs from this one. It
/// shows the live canvas (`GET /api/v1/canvas.svg`) and makes no timing
/// claims, since merge and mirror intervals are settings.
pub fn readme(config: &PlaceConfig) -> String {
    let clone = &config.clone_url;
    let page = clone
        .strip_suffix(&format!("/{}.git", config.repo))
        .unwrap_or(clone);
    let mut lines = vec![
        "# place".to_owned(),
        String::new(),
        format!(
            "[![The canvas, live]({page}{api}/canvas.svg)]({page})",
            api = crate::api::API_BASE
        ),
        String::new(),
        "A shared pixel canvas where every brush stroke is a `git push`, served by".to_owned(),
        "[Aspen](https://github.com/reason-machines/aspen).".to_owned(),
        String::new(),
        "## Paint".to_owned(),
        String::new(),
        format!("    git clone --depth=1 {clone} && cd {}", config.repo),
        "    printf '#d6262b\\n' > tiles/12_40.txt".to_owned(),
        "    git add tiles && git commit -m paint".to_owned(),
        "    git push origin HEAD:refs/heads/paint/<you>".to_owned(),
        String::new(),
        "Each tile is `tiles/<x>_<y>.txt`, holding one palette color as `#rrggbb` and a newline."
            .to_owned(),
        "Pushes may change only tile files. The rules are in".to_owned(),
        "[TILES.md](https://github.com/reason-machines/aspen/blob/main/demo/place/docs/TILES.md)."
            .to_owned(),
        "A bot merges branches into `main`.".to_owned(),
        String::new(),
        "## Notes".to_owned(),
        String::new(),
        "- Pushes are public, including the name and email in your commits.".to_owned(),
    ];
    if config.mirror_url.is_some() {
        lines.push(
            "- This repository is a read-only mirror of `main`. Push to the demo, not here."
                .to_owned(),
        );
    }
    lines.push(String::new());
    lines.join("\n")
}

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
        let personas = match &config.personas {
            Some(settings) => Some(
                crate::personas::Director::new(
                    crate::personas::ROSTER,
                    settings.clone(),
                    rng.next(),
                    now,
                )
                .map_err(PlaceError::Corrupt)?,
            ),
            None => None,
        };
        // The personas replace the ambient sprite agents.
        let ambient_agents = if personas.is_some() {
            0
        } else {
            config.ambient_agents
        };
        let ambient = (0..ambient_agents)
            .map(|i| Ambient {
                name: format!("agent/{}-{}", words[i % words.len()], 10 + rng.below(89)),
                job: None,
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
            personas: Mutex::new(personas),
            memory_ready: std::sync::atomic::AtomicBool::new(true),
            remembered: Mutex::new(None),
            sessions: Mutex::new(sessions),
            event_streams,
            operators,
            svg: crate::image::SvgCache::default(),
        });
        place.load_main().await?;
        place.refresh_readme().await?;
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

    /// Writes a commit on `main` that replaces its `README.md` with
    /// [`readme`], when they differ (the text changed with a new version or
    /// the deployment's settings). The tiles do not change.
    async fn refresh_readme(&self) -> Result<(), PlaceError> {
        let main = self.with_state(|s| s.main.clone());
        let mut pb = PackBuilder::default();
        let id = pb.add(ObjectKind::Blob, readme(&self.config).into_bytes());
        let Some(at) = main.extras.iter().position(|e| e.name == b"README.md") else {
            return Ok(());
        };
        if main.extras[at].id == id {
            return Ok(());
        }
        let mut extras = (*main.extras).clone();
        extras[at].id = id;
        let message = format!(
            "Update the README\n\nWritten by the place server; the canvas is unchanged.{}",
            main.history.trailer(true)
        );
        let commit = self.add_commit(&mut pb, &main.pixels, &extras, &[main.commit], &message);
        let pack = pb.finish();
        let history = main.history.grow(pack.len() as u64);
        match self
            .push(
                &Self::trunk(),
                false,
                main.commit,
                commit,
                Some(pack),
                "merger",
            )
            .await?
        {
            Report::Ok => {
                self.with_state(|s| {
                    s.main.commit = commit;
                    s.main.extras = Arc::new(extras);
                    s.main.history = history;
                });
                Ok(())
            }
            // `main` moved (another writer): the next start tries again.
            Report::Rejected(_) => self.load_main().await,
        }
    }

    /// Catches up with a `main` another writer moved (the other task of a
    /// start-first deploy, an operator): reloads the canvas and remembers
    /// the people's and sprites' tiles of the merges since the `main` this
    /// instance knew, so its personas keep off fresh human paint. The
    /// merger runs it on every tick; a no-op while `main` is where it was.
    pub async fn follow_main(&self) -> Result<(), PlaceError> {
        let _guard = self.merge_lock.lock().await;
        self.follow_main_locked().await
    }

    async fn follow_main_locked(&self) -> Result<(), PlaceError> {
        let known = self.with_state(|s| s.main.commit);
        let current = self
            .meta
            .current_version(self.repo.id, &Self::trunk())
            .await?
            .and_then(|v| v.value.tip());
        if current != Some(known) {
            self.reload_main().await?;
        }
        self.catch_up_memory().await
    }

    /// Reloads `main` after another writer moved it and tells subscribers
    /// which tiles changed ([`State::follow`]). The memory of its merges is
    /// read by [`Self::catch_up_memory`].
    async fn reload_main(&self) -> Result<(), PlaceError> {
        let before = self.with_state(|s| s.main.pixels.clone());
        self.load_main().await?;
        self.with_state(|s| {
            let commit = s.main.commit;
            s.follow(commit, &before, now_ms());
        });
        Ok(())
    }

    /// Remembers the people's and sprites' tiles of every merge on stored
    /// `main` since the last commit remembered (all of
    /// [`MEMORY_WALK`] before the first), and records its tip as
    /// remembered. A no-op when nothing moved.
    async fn catch_up_memory(&self) -> Result<(), PlaceError> {
        let stop = *self.remembered.lock().expect("not poisoned");
        let tip = self.remember_main_until(stop).await?;
        *self.remembered.lock().expect("not poisoned") = tip;
        Ok(())
    }

    async fn load_main(&self) -> Result<(), PlaceError> {
        let trunk = Self::trunk();
        let current = self.meta.current_version(self.repo.id, &trunk).await?;
        let main = match current.as_ref().and_then(|v| v.value.tip().map(|t| (v, t))) {
            Some((v, tip)) => {
                let (pixels, extras) = self.read_canvas(v, tip).await?;
                let history = match self.svc.read_object(self.repo.id, v.id, tip).await? {
                    // The trailer counts the history before the tip; the
                    // tip is one more commit, its pack's size unknown.
                    Some((ObjectKind::Commit, data)) => History::parse(&data).map_or_else(
                        || {
                            match parse_commit(&data) {
                                // A root without a trailer: the history is the root.
                                Ok((_, parents)) if parents.is_empty() => History::root(tip, 0),
                                // Written before the server counted: unknown.
                                _ => History::UNKNOWN,
                            }
                        },
                        |h| h.grow(0),
                    ),
                    _ => return Err(PlaceError::Corrupt(format!("{tip} is not a commit"))),
                };
                Main {
                    commit: tip,
                    pixels: Arc::new(pixels),
                    extras: Arc::new(extras),
                    history,
                }
            }
            None => {
                let mut pb = PackBuilder::default();
                let readme = pb.add(ObjectKind::Blob, readme(&self.config).into_bytes());
                let extras = vec![TreeEntry {
                    mode: MODE_FILE,
                    name: b"README.md".to_vec(),
                    id: readme,
                }];
                let pixels = vec![UNSET; self.shared.rules.len()];
                let commit = self.add_commit(&mut pb, &pixels, &extras, &[], "Start the canvas");
                let pack = pb.finish();
                let history = History::root(commit, pack.len() as u64);
                match self
                    .push(&trunk, false, ObjectId::ZERO, commit, Some(pack), "merger")
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
                    history,
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
                    // restart never deletes one early, and none is expired
                    // for LISTED_BRANCH_GRACE: the old task of a start-first
                    // deploy may still hold its tiles and paint on it.
                    s.branches.insert(
                        short.to_owned(),
                        crate::state::Branch {
                            actor,
                            head: tip,
                            overlay: Default::default(),
                            updated_ms: now,
                            deleted: false,
                            full: None,
                            listed_until_ms: now + LISTED_BRANCH_GRACE_MS,
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
        self.add_commit_by(pb, pixels, extras, parents, message, None)
    }

    /// [`Self::add_commit`] with `author` (`Name <email>`) as the commit's
    /// author; the server stays its committer.
    fn add_commit_by(
        &self,
        pb: &mut PackBuilder,
        pixels: &[u8],
        extras: &[TreeEntry],
        parents: &[ObjectId],
        message: &str,
        author: Option<&str>,
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
        let server = "place <place@aspen.invalid>";
        pb.add(
            ObjectKind::Commit,
            crate::git::write_commit_by(
                root,
                parents,
                author.unwrap_or(server),
                server,
                secs,
                message,
            ),
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
        self.paint_by(branch, actor, tiles, message, None).await
    }

    /// [`Self::paint`] with `author` (`Name <email>`) as the commit's
    /// author instead of the server (a persona's stroke).
    pub async fn paint_by(
        &self,
        branch: &str,
        actor: Actor,
        tiles: &[Tile],
        message: &str,
        author: Option<&str>,
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
        let commit = self.add_commit_by(&mut pb, &pixels, &extras, &[parent], &message, author);
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
            (Report::Rejected(r), _) => {
                // The branch moved under us (another instance painted it,
                // during a start-first deploy): the next paint reads its
                // tip from the store instead of retrying on a stale one.
                self.with_state(|s| s.forget_full(branch));
                Err(PlaceError::Refused(r))
            }
        }
    }

    /// The merger bot's one run: folds every pending push into `main` as
    /// one fast-forward commit. Returns the merge's sequence number, or
    /// `None` when nothing was pending.
    pub async fn merge_once(&self) -> Result<Option<u64>, PlaceError> {
        let _guard = self.merge_lock.lock().await;
        let (plan, main) = self.with_state(|s| (s.plan_merge(now_ms()), s.main.clone()));
        let Some(plan) = plan else { return Ok(None) };
        let mut pb = PackBuilder::default();
        let mut message = format!(
            "Merge {} branches through push {}\n\nMerged by the place merger bot: each push's tiles are\nreplayed onto main in push order.",
            plan.branches.len(),
            plan.through
        );
        let memory = memory_trailers(&self.shared.rules, &plan.people, &plan.sprites);
        message.push_str(&memory);
        message.push_str(&main.history.trailer(memory.is_empty()));
        let commit = self.add_commit(
            &mut pb,
            &plan.pixels,
            &main.extras,
            &[main.commit],
            &message,
        );
        let pack = pb.finish();
        let history = main.history.grow(pack.len() as u64);
        match self
            .push(
                &Self::trunk(),
                false,
                main.commit,
                commit,
                Some(pack),
                "merger",
            )
            .await?
        {
            Report::Ok => {
                // This merge's memory goes into the state below: when the
                // memory was up to date with the `main` it builds on, it
                // still is.
                {
                    let mut r = self.remembered.lock().expect("not poisoned");
                    if *r == Some(main.commit) {
                        *r = Some(commit);
                    }
                }
                Ok(Some(self.with_state(|s| {
                    s.main.history = history;
                    s.apply_merge(commit, plan, now_ms())
                })))
            }
            Report::Rejected(r) => {
                // `main` moved under us (an operator, another instance):
                // start again from what is stored.
                self.follow_main_locked().await?;
                Err(PlaceError::Refused(r))
            }
        }
    }

    /// Starts `main`'s history again from one root commit holding the
    /// canvas as it is now, after merging what is pending, and deletes
    /// every branch whose tiles have all merged (they hold the old
    /// history). The operator's endpoint (`POST /admin/reset-history`);
    /// the merger does the same on its own, without deleting branches, when
    /// the history outgrows [`PlaceConfig::history_max_bytes`]
    /// ([`Place::bound_history`]). Returns the new root commit.
    pub async fn reset_history(&self) -> Result<ObjectId, PlaceError> {
        let commit = self
            .restart_history("The history before this commit was removed by an operator.")
            .await?;
        let due = self.with_state(|s| s.expirable(0, u64::MAX));
        for (branch, head, deleted) in due {
            let lock = self.branch_lock(&branch);
            let _guard = lock.lock().await;
            if !deleted {
                let name = RefName::new(format!("refs/heads/{branch}"))
                    .map_err(|e| PlaceError::Corrupt(e.to_string()))?;
                let report = self
                    .push(&name, true, head, ObjectId::ZERO, None, "expiry")
                    .await?;
                if report != Report::Ok && report.reason() != Some(RejectReason::NoSuchRef) {
                    continue;
                }
            }
            self.with_state(|s| s.expire(&branch, now_ms()));
            self.locks.lock().expect("not poisoned").remove(&branch);
        }
        Ok(commit)
    }

    /// Starts `main`'s history again ([`Self::reset_history`], branches
    /// left alone) when the packs written on it since its root reach
    /// [`PlaceConfig::history_max_bytes`]. Returns the new root commit, or
    /// `None` when the history is within the limit. The merger runs it
    /// after every merge.
    ///
    /// Every merge stores a whole `tiles/` tree (stored packs have no
    /// deltas), so without this a full clone of `main` grows with every
    /// merge. Nothing a client relies on is lost:
    ///
    /// - pending pushes are merged first, and later ones merge onto the
    ///   new root (merges replay tiles, never Git history);
    /// - branches keep their history and their pushes still merge;
    /// - a clone made before the reset still pushes: its new commits are
    ///   checked from the earlier `main` they build on
    ///   ([`crate::hook::MainHistory`]);
    /// - the earlier history stays in the repository (ref versions are
    ///   never deleted) and the root commit names its tip, so
    ///   `git fetch <url> <tip>` still gets all of it.
    ///
    /// Clones made before see a forced update of `main`.
    pub async fn bound_history(&self) -> Result<Option<ObjectId>, PlaceError> {
        let max = self.config.history_max_bytes;
        if max == 0 || self.with_state(|s| s.main.history.bytes) < max {
            return Ok(None);
        }
        let why = format!(
            "main's history since its last root reached {max} bytes of packs\n(PLACE_HISTORY_MAX_BYTES), so the server started it again."
        );
        self.restart_history(&why).await.map(Some)
    }

    /// Merges what is pending, then force-updates `main` to a root commit
    /// with the same canvas, whose message says `why` and names the old
    /// tip, and carries the memory of recent paint
    /// ([`Self::rebuild_memory`] reads it after a restart).
    async fn restart_history(&self, why: &str) -> Result<ObjectId, PlaceError> {
        self.merge_once().await?;
        let _guard = self.merge_lock.lock().await;
        let trunk = Self::trunk();
        let version = self
            .meta
            .current_version(self.repo.id, &trunk)
            .await?
            .ok_or_else(|| PlaceError::Corrupt("main does not exist".into()))?;
        let main = self.with_state(|s| s.main.clone());
        if version.value.tip() != Some(main.commit) {
            // `main` moved under us: start again from what is stored.
            self.follow_main_locked().await?;
            return Err(PlaceError::Refused("main moved; try again".into()));
        }
        // An orphan commit's pack holds everything it reaches: the
        // extras (the README) are read from the current history.
        let mut pb = PackBuilder::default();
        for e in main.extras.iter() {
            match self.svc.read_object(self.repo.id, version.id, e.id).await? {
                Some((ObjectKind::Blob, data)) => {
                    pb.add(ObjectKind::Blob, data);
                }
                _ => {
                    return Err(PlaceError::Corrupt(format!(
                        "{} at the root of main is not a blob",
                        String::from_utf8_lossy(&e.name)
                    )));
                }
            }
        }
        let (people, sprites) = self.with_state(|s| {
            let now = now_ms();
            let fresh = |at: &u64| at.saturating_add(crate::state::HUMAN_MEMORY_MS) > now;
            let tiles = |m: &HashMap<usize, u64>| {
                let mut v: Vec<usize> = m
                    .iter()
                    .filter(|(_, at)| fresh(at))
                    .map(|(i, _)| *i)
                    .collect();
                v.sort_unstable();
                v
            };
            (tiles(&s.human_tiles), tiles(&s.sprite_tiles))
        });
        let message = format!(
            "Start the canvas again from main at {old}\n\n{why} The canvas is unchanged.\nThe earlier history is still in the repository:\n\n    git fetch <this repository's URL> {old}{memory}",
            old = main.commit,
            memory = memory_trailers(&self.shared.rules, &people, &sprites),
        );
        let commit = self.add_commit(&mut pb, &main.pixels, &main.extras, &[], &message);
        let pack = pb.finish();
        let history = History::root(commit, pack.len() as u64);
        let body = crate::git::forced_receive_request(trunk.as_str(), main.commit, commit, pack);
        let repo = Repo {
            settings: self
                .repo
                .settings
                .clone()
                .with_allow_force(aspen_core::repo::ForcePolicy::All),
            ..self.repo.clone()
        };
        let out = self
            .svc
            .push(
                &repo,
                None,
                &Internal("operator"),
                None,
                std::io::Cursor::new(body),
            )
            .await
            .map_err(|ServiceError::Store(e)| e)?;
        match crate::git::parse_report(&out.response) {
            Report::Ok => {}
            Report::Rejected(r) => {
                self.follow_main_locked().await?;
                return Err(PlaceError::Refused(r));
            }
        }
        // The root carries the memory the state holds: still up to date
        // when it was with the `main` it replaced.
        {
            let mut r = self.remembered.lock().expect("not poisoned");
            if *r == Some(main.commit) {
                *r = Some(commit);
            }
        }
        self.with_state(|s| {
            s.main.commit = commit;
            s.main.history = history;
        });
        Ok(commit)
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

    /// Restores the memory of recent paint (people's tiles and sprites'
    /// tiles from the last six hours) from the trailers of `main`'s merge
    /// commits, walking back from the tip, so a restart does not let
    /// ambient agents paint over them. Bounded to [`MEMORY_WALK`] commits.
    pub async fn rebuild_memory(&self) -> Result<(), PlaceError> {
        // Not under the merge lock, so a long walk never holds up merges;
        // remembering twice is harmless.
        self.catch_up_memory().await
    }

    /// [`Self::rebuild_memory`]'s walk, stopping before `stop` (a commit
    /// already remembered) as well. Returns the tip it walked down from.
    async fn remember_main_until(
        &self,
        stop: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, PlaceError> {
        let trunk = Self::trunk();
        let Some(version) = self.meta.current_version(self.repo.id, &trunk).await? else {
            return Ok(None);
        };
        let Some(tip) = version.value.tip() else {
            return Ok(None);
        };
        let mut at = tip;
        let now_s = now_ms() / 1000;
        let window_s = crate::state::HUMAN_MEMORY_MS / 1000;
        let rules = self.shared.rules.clone();
        for _ in 0..MEMORY_WALK {
            if Some(at) == stop {
                break;
            }
            let Some((ObjectKind::Commit, data)) =
                self.svc.read_object(self.repo.id, version.id, at).await?
            else {
                break;
            };
            let (time, people, sprites) = parse_memory(&data, &rules);
            let Some(time) = time.filter(|t| t + window_s > now_s) else {
                break;
            };
            self.with_state(|s| s.remember(&people, &sprites, time * 1000));
            let (_, parents) = parse_commit(&data).map_err(PlaceError::Corrupt)?;
            match parents.first() {
                Some(p) => at = *p,
                None => break,
            }
        }
        Ok(Some(tip))
    }

    /// Runs the merger bot, branch expiry, the ambient agents and the
    /// swarm until `shutdown` resolves, then merges what is pending one
    /// last time.
    pub async fn run(self: Arc<Self>, shutdown: impl std::future::Future<Output = ()>) {
        {
            self.memory_ready
                .store(false, std::sync::atomic::Ordering::Release);
            let place = self.clone();
            tokio::spawn(async move {
                if let Err(e) = place.rebuild_memory().await {
                    eprintln!("place: rebuilding the memory of recent paint failed: {e}");
                }
                place
                    .memory_ready
                    .store(true, std::sync::atomic::Ordering::Release);
            });
        }
        let mut merge = tokio::time::interval(self.config.merge_interval);
        merge.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // When a merge with only ambient pushes may next run.
        let mut ambient_due = tokio::time::Instant::now();
        // The tick of the last merge: the merger waits `merge_wait` after it.
        // None until the first, so the first tick merges at once.
        let mut last_merge: Option<tokio::time::Instant> = None;
        let mut agents = tokio::time::interval(Duration::from_millis(100));
        agents.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                _ = merge.tick() => {
                    if let Err(e) = self.follow_main().await {
                        eprintln!("place: reading main failed: {e}");
                    }
                    let now = tokio::time::Instant::now();
                    let (prompt, pending) = self.with_state(|s| {
                        let prompt = s.pending.iter().any(|p| !p.branch.starts_with(AMBIENT_PREFIX));
                        (prompt, s.pending.len())
                    });
                    let since = last_merge.map_or(Duration::MAX, |t| now.saturating_duration_since(t));
                    let due = since >= merge_wait(&self.config, pending, since);
                    if (prompt && due) || (!prompt && now >= ambient_due) {
                        match self.merge_once().await {
                            Ok(Some(_)) => {
                                last_merge = Some(now);
                                ambient_due = now + self.config.ambient_merge_interval;
                                match self.bound_history().await {
                                    Ok(Some(root)) => eprintln!(
                                        "place: main's history reached its limit; started again at {root}"
                                    ),
                                    Ok(None) => {}
                                    Err(e) => eprintln!("place: starting main's history again failed: {e}"),
                                }
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
        self.final_merge().await;
    }

    /// The merge at shutdown ([`Self::run`] runs it last): merges what is
    /// pending, trying again when another instance moved `main` first.
    pub async fn final_merge(&self) {
        // During a start-first deploy the next instance's merger also moves
        // `main`, so this merge can lose the compare-and-swap. `merge_once`
        // reloads `main` when refused and keeps what is pending, so another
        // try replays the same pushes onto the new `main`. Pushes this
        // instance accepted live only in its memory: the next instance lists
        // their branches with nothing pending and never merges them.
        // A refused try has already reloaded `main` and pushes again at
        // once, so it loses only to a merge landing in that instant; the
        // other task's merger runs every few seconds. A short jittered pause
        // keeps two tasks shutting down together from retrying in step.
        for attempt in 1..=FINAL_MERGE_ATTEMPTS {
            match self.merge_once().await {
                Ok(_) => return,
                Err(PlaceError::Refused(r)) if attempt < FINAL_MERGE_ATTEMPTS => {
                    eprintln!("place: final merge refused ({r}); trying again on the new main");
                    let pause = 50 + self.rng.lock().expect("not poisoned").below(201);
                    tokio::time::sleep(Duration::from_millis(pause)).await;
                }
                Err(e) => {
                    eprintln!("place: final merge failed: {e}");
                    break;
                }
            }
        }
        // Their tiles stay on their branches, which the next instance keeps
        // for LISTED_BRANCH_GRACE but does not merge: name them for an
        // operator (a push of the same tiles merges them).
        let stranded = self.with_state(|s| {
            let mut b: Vec<String> = s.pending.iter().map(|p| p.branch.clone()).collect();
            b.dedup();
            b
        });
        if !stranded.is_empty() {
            eprintln!(
                "place: pushes on {} left unmerged at shutdown: {}",
                stranded.len(),
                stranded.join(", ")
            );
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
        if self.memory_ready.load(std::sync::atomic::Ordering::Acquire) {
            self.personas_tick(now);
        }
        // Tiles people painted recently, read only when an agent is about
        // to pick a new sprite (lock order: `shared.state` is never taken
        // while `ambient` is held).
        // Until the memory of recent paint is rebuilt from `main` after a
        // start, agents pick no new sprites.
        let picking = self.memory_ready.load(std::sync::atomic::Ordering::Acquire)
            && self
                .ambient
                .lock()
                .expect("not poisoned")
                .iter()
                .any(|a| a.next_ms <= now && a.job.is_none());
        let human: Vec<usize> = if picking {
            let mut human: Vec<usize> = self.with_state(|s| s.protected_tiles(now));
            for &(x, y, w, h) in &self.config.keep_areas {
                for ty in y..(y + h).min(rules.height) {
                    for tx in x..(x + w).min(rules.width) {
                        human.push(rules.index(tx, ty));
                    }
                }
            }
            human
        } else {
            Vec::new()
        };
        let chunk = rules.tiles_per_push.max(1);
        let due: Vec<(String, Vec<Tile>)> = {
            let mut ambient = self.ambient.lock().expect("not poisoned");
            let mut rng = self.rng.lock().expect("not poisoned");
            let mut out = Vec::new();
            let busy_of = |ambient: &[Ambient]| {
                ambient
                    .iter()
                    .filter_map(|a| a.job.as_ref().map(|j| j.rect))
                    .collect::<Vec<Rect>>()
            };
            for i in 0..ambient.len() {
                if ambient[i].next_ms > now {
                    continue;
                }
                if ambient[i].job.is_none() {
                    if !picking {
                        continue;
                    }
                    let sprite = &crate::sprites::SPRITES
                        [rng.below(crate::sprites::SPRITES.len() as u64) as usize];
                    let variant = rng.below(sprite.variants.len() as u64) as usize;
                    let busy = busy_of(&ambient);
                    let (w, h) = sprite.size();
                    let picked =
                        choose_spot(&mut rng, &rules, (w, h), &human, &busy).and_then(|(x, y)| {
                            sprite
                                .tiles(variant, x, y, &rules.palette)
                                .map(|tiles| SpriteJob {
                                    tiles,
                                    next: 0,
                                    rect: (x, y, w, h),
                                })
                        });
                    match picked {
                        Some(job) => ambient[i].job = Some(job),
                        None => {
                            // No room clear of people's work and recent
                            // sprites (the canvas is full for now), or a
                            // color missing from the palette: try later.
                            ambient[i].next_ms = now + 60_000;
                            continue;
                        }
                    }
                }
                let a = &mut ambient[i];
                let job = a.job.as_mut().expect("set above");
                let end = (job.next + chunk).min(job.tiles.len());
                let tiles = job.tiles[job.next..end].to_vec();
                job.next = end;
                let mean = if job.next >= job.tiles.len() {
                    a.job = None;
                    (self.config.ambient_sprite_pause.as_millis() as u64).max(2)
                } else {
                    (self.config.ambient_interval.as_millis() as u64).max(2)
                };
                a.next_ms = now + mean / 2 + rng.below(mean);
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
                    .paint(&branch, Actor::agent(&branch), &tiles, "Ambient sprite")
                    .await;
                drop(permit);
            });
        }
    }

    /// Whether the server-wide push ceiling ([`PlaceConfig::global_pushes_per_second`])
    /// is reached or nearly so, so personas back off: true while less than
    /// a tenth of its burst is left. Personas never take a token: they
    /// read the bucket and leave what is there to people, before anyone is
    /// refused.
    pub fn push_ceiling_busy(&self) -> bool {
        let ceiling = self.shared.pushes.lock().expect("not poisoned");
        ceiling.available(std::time::Instant::now()) * 10 < u64::from(ceiling.burst())
    }

    /// The personas' turn ([`crate::personas`]): each stroke due now is
    /// one tile pushed on the persona's `agent/` branch with the persona
    /// as the commit's author. Like ambient strokes, a stroke that finds
    /// no free agent permit is dropped (and tried again on its next turn).
    fn personas_tick(self: &Arc<Self>, now: u64) {
        let due = self
            .personas
            .lock()
            .expect("not poisoned")
            .as_ref()
            .is_some_and(|d| d.due(now));
        if !due {
            return;
        }
        let rules = self.shared.rules.clone();
        // Lock order: `shared.state` is never taken while `personas` is held.
        let (people, agent_paint, pixels) = self.with_state(|s| {
            (
                s.recent_people_tiles(now),
                s.recent_agent_tiles(now),
                s.main.pixels.clone(),
            )
        });
        let mut people: std::collections::HashSet<usize> = people.into_iter().collect();
        for &(x, y, w, h) in &self.config.keep_areas {
            for ty in y..y.saturating_add(h).min(rules.height) {
                for tx in x..x.saturating_add(w).min(rules.width) {
                    people.insert(rules.index(tx, ty));
                }
            }
        }
        let agent_paint = agent_paint.into_iter().collect();
        let view = crate::personas::View {
            rules: &rules,
            people: &people,
            agent_paint: &agent_paint,
            pixels: &pixels,
            ceiling_busy: self.push_ceiling_busy(),
        };
        let strokes = {
            let mut d = self.personas.lock().expect("not poisoned");
            let Some(d) = d.as_mut() else { return };
            d.tick(now, &view)
        };
        for stroke in strokes {
            let Ok(permit) = self.permits.clone().try_acquire_owned() else {
                if let Some(d) = self.personas.lock().expect("not poisoned").as_mut() {
                    d.finished(stroke.persona, false, now);
                }
                continue;
            };
            let place = self.clone();
            tokio::spawn(async move {
                let message = format!("{} paints a {}", stroke.name, stroke.picture);
                let ok = place
                    .paint_by(
                        &stroke.branch,
                        Actor::agent(stroke.name),
                        &[stroke.tile],
                        &message,
                        Some(&stroke.author),
                    )
                    .await
                    .is_ok();
                if let Some(d) = place.personas.lock().expect("not poisoned").as_mut() {
                    d.finished(stroke.persona, ok, now_ms());
                }
                drop(permit);
            });
        }
    }

    /// Personas on shift now: `(name, branch)`.
    pub fn personas_active(&self) -> Vec<(&'static str, String)> {
        self.personas
            .lock()
            .expect("not poisoned")
            .as_ref()
            .map_or_else(Vec::new, |d| {
                d.active()
                    .into_iter()
                    .map(|(i, b)| (crate::personas::ROSTER[i].name, b.to_owned()))
                    .collect()
            })
    }
}

/// Trailers that record which tiles of a merge people painted and which
/// ambient sprites painted, so [`Place::rebuild_memory`] can restore the
/// memory of recent paint from `main` after a restart.
pub fn memory_trailers(rules: &Rules, people: &[usize], sprites: &[usize]) -> String {
    let w = rules.width as usize;
    let list = |tiles: &[usize]| {
        tiles
            .iter()
            .map(|i| format!("{},{}", i % w, i / w))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let mut out = String::new();
    if !people.is_empty() || !sprites.is_empty() {
        out.push('\n');
    }
    if !people.is_empty() {
        out.push_str(&format!("\nPeople-tiles: {}", list(people)));
    }
    if !sprites.is_empty() {
        out.push_str(&format!("\nSprite-tiles: {}", list(sprites)));
    }
    out
}

/// The committer time (Unix seconds) and the people's and sprites' tiles a
/// merge commit's trailers name ([`memory_trailers`]).
pub fn parse_memory(commit: &[u8], rules: &Rules) -> (Option<u64>, Vec<usize>, Vec<usize>) {
    let text = String::from_utf8_lossy(commit);
    let mut time = None;
    let (mut people, mut sprites) = (Vec::new(), Vec::new());
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("committer ") {
            time = rest.rsplit(' ').nth(1).and_then(|s| s.parse().ok());
        }
        let (into, list) = if let Some(l) = line.strip_prefix("People-tiles: ") {
            (&mut people, l)
        } else if let Some(l) = line.strip_prefix("Sprite-tiles: ") {
            (&mut sprites, l)
        } else {
            continue;
        };
        for xy in list.split(' ') {
            if let Some((x, y)) = xy.split_once(',')
                && let (Ok(x), Ok(y)) = (x.parse::<u32>(), y.parse::<u32>())
                && x < rules.width
                && y < rules.height
            {
                into.push(rules.index(x, y));
            }
        }
    }
    (time, people, sprites)
}

/// How many times the merge at shutdown runs when another instance's merge
/// moved `main` first (a start-first deploy). Each refused try reloads
/// `main`, so a second try normally lands.
const FINAL_MERGE_ATTEMPTS: u32 = 5;

/// How long a branch found in the store at start is kept from expiry
/// ([`crate::state::Branch::listed_until_ms`]): longer than the overlap of
/// a start-first deploy, so the old task's branches outlive it.
const LISTED_BRANCH_GRACE_MS: u64 = 10 * 60 * 1000;

/// The most `main` commits [`Place::rebuild_memory`] reads.
pub const MEMORY_WALK: usize = 2000;

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

#[cfg(test)]
mod readme_tests {
    use super::*;

    /// The public demo's README, word for word (it is also the GitHub
    /// mirror's front page).
    #[test]
    fn the_public_demo_readme() {
        let mut c = PlaceConfig::new("https://place.reasonmachines.com/canvas.git");
        c.mirror_url = Some("https://github.com/reason-machines/place-canvas".into());
        let expected = "# place

[![The canvas, live](https://place.reasonmachines.com/api/v1/canvas.svg)](https://place.reasonmachines.com)

A shared pixel canvas where every brush stroke is a `git push`, served by
[Aspen](https://github.com/reason-machines/aspen).

## Paint

    git clone --depth=1 https://place.reasonmachines.com/canvas.git && cd canvas
    printf '#d6262b\\n' > tiles/12_40.txt
    git add tiles && git commit -m paint
    git push origin HEAD:refs/heads/paint/<you>

Each tile is `tiles/<x>_<y>.txt`, holding one palette color as `#rrggbb` and a newline.
Pushes may change only tile files. The rules are in
[TILES.md](https://github.com/reason-machines/aspen/blob/main/demo/place/docs/TILES.md).
A bot merges branches into `main`.

## Notes

- Pushes are public, including the name and email in your commits.
- This repository is a read-only mirror of `main`. Push to the demo, not here.
";
        assert_eq!(readme(&c), expected);
    }
}

#[cfg(test)]
mod sprite_spot_tests {
    use super::*;

    fn rules(w: u32, h: u32) -> Rules {
        Rules {
            width: w,
            height: h,
            palette: rules::default_palette(),
            tiles_per_push: 64,
        }
    }

    /// Sprites never cover tiles people painted, and spread away from
    /// other sprites when there is room.
    #[test]
    fn spots_avoid_people_and_spread_out() {
        let r = rules(32, 32);
        let mut rng = Rng(7);
        // People painted the whole left half.
        let human: Vec<usize> = (0..32u32)
            .flat_map(|y| (0..16u32).map(move |x| (y * 32 + x) as usize))
            .collect();
        let busy = [(16, 0, 8, 8)];
        for _ in 0..50 {
            let (x, y) = choose_spot(&mut rng, &r, (8, 8), &human, &busy).unwrap();
            assert!(x >= 16, "covers people's work at {x},{y}");
            assert_eq!(overlap((x, y, 8, 8), busy[0]), 0, "{x},{y}");
        }
        // No room clear of people: no spot.
        let everything: Vec<usize> = (0..32 * 32).collect();
        assert_eq!(choose_spot(&mut rng, &r, (8, 8), &everything, &[]), None);
        // Larger than the canvas: no spot.
        assert_eq!(choose_spot(&mut rng, &rules(4, 4), (8, 8), &[], &[]), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Invariant: persona settings that cannot work are refused at startup
    /// with the setting to change.
    #[test]
    fn validate_refuses_personas_naming_the_setting_to_change() {
        use crate::personas::PersonaSettings;
        let refused = |p: PersonaSettings| {
            let mut c = PlaceConfig::new("http://localhost/canvas.git");
            c.personas = Some(p);
            c.validate().expect_err("refused")
        };
        let d = PersonaSettings::default();
        for (p, name) in [
            (
                PersonaSettings {
                    min_active: 0,
                    ..d.clone()
                },
                "PLACE_PERSONAS_MIN: ",
            ),
            (
                PersonaSettings {
                    min_active: 7,
                    ..d.clone()
                },
                "PLACE_PERSONAS_MIN: ",
            ),
            (
                PersonaSettings {
                    max_active: 14,
                    ..d.clone()
                },
                "PLACE_PERSONAS_MAX: ",
            ),
            (
                PersonaSettings {
                    pushes_per_second: 0.0,
                    ..d.clone()
                },
                "PLACE_PERSONAS_PUSHES_PER_SECOND: ",
            ),
            (
                PersonaSettings {
                    pushes_per_second: f64::NAN,
                    ..d.clone()
                },
                "PLACE_PERSONAS_PUSHES_PER_SECOND: ",
            ),
        ] {
            let e = refused(p);
            assert!(e.starts_with(name), "{name}: {e}");
        }
        let mut c = PlaceConfig::new("http://localhost/canvas.git");
        c.personas = Some(d);
        assert_eq!(c.validate(), Ok(()));
    }

    /// Invariant: a push ceiling the token bucket cannot hold is refused at
    /// startup with the setting to change, never a panic; the default and
    /// the bounds themselves pass.
    #[test]
    fn validate_refuses_a_push_ceiling_naming_the_setting_to_change() {
        let refused = |rate: f64, burst: u32| {
            let mut c = PlaceConfig::new("http://localhost/canvas.git");
            c.global_pushes_per_second = rate;
            c.global_push_burst = burst;
            c.validate().expect_err("refused")
        };
        for rate in [f64::NAN, f64::INFINITY, -1.0, 0.0, 5e-20, 1e-16, 0.000_999] {
            let e = refused(rate, 1);
            assert!(
                e.starts_with("PLACE_GLOBAL_PUSHES_PER_SECOND: the rate must be"),
                "{rate}: {e}"
            );
        }
        let e = refused(550.0, 0);
        assert_eq!(e, "PLACE_GLOBAL_PUSH_BURST: the burst must be at least 1");
        for (rate, burst) in [(0.001, 1001), (1.0, u32::MAX)] {
            let e = refused(rate, burst);
            assert!(
                e.starts_with(
                    "PLACE_GLOBAL_PUSH_BURST and PLACE_GLOBAL_PUSHES_PER_SECOND: a burst of"
                ) && e.contains("to refill"),
                "{rate} {burst}: {e}"
            );
        }
        for (rate, burst) in [(550.0, 2750), (0.001, 1000), (f64::MAX, u32::MAX)] {
            let mut c = PlaceConfig::new("http://localhost/canvas.git");
            c.global_pushes_per_second = rate;
            c.global_push_burst = burst;
            assert_eq!(c.validate(), Ok(()), "{rate} {burst}");
        }
    }
}

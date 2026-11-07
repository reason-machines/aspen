//! The canvas as the page sees it: `main`, each branch's unmerged tiles,
//! the pushes waiting for the merger bot, the swarm, and the event log.
//!
//! One lock guards all of it, so every event gets its sequence number,
//! enters the backlog and reaches live subscribers in one step: a
//! snapshot's `seq`, the backlog and the live stream always line up.
//! The lock is never held across an `await`.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use aspen_core::id::ObjectId;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::git::TreeEntry;
use crate::rules::{AMBIENT_PREFIX, Rules, Tile};

/// A tile that was never painted.
pub const UNSET: u8 = u8::MAX;

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// RFC 3339 in UTC with milliseconds, e.g. `2026-11-03T12:00:00.000Z`.
pub fn iso(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Howard Hinnant's days-to-civil.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        ms % 1000
    )
}

/// Who painted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Actor {
    /// `visitor`, `agent` or `git`.
    pub kind: &'static str,
    /// A name to show.
    pub label: String,
}

impl Actor {
    /// A visitor painting on the page.
    pub fn visitor(label: impl Into<String>) -> Self {
        Self {
            kind: "visitor",
            label: label.into(),
        }
    }

    /// A simulated agent (ambient or swarm).
    pub fn agent(label: impl Into<String>) -> Self {
        Self {
            kind: "agent",
            label: label.into(),
        }
    }

    /// Someone pushing with a Git client.
    pub fn git(label: impl Into<String>) -> Self {
        Self {
            kind: "git",
            label: label.into(),
        }
    }
}

/// One event of the feed, serialized once.
#[derive(Debug)]
pub struct Event {
    /// Its sequence number.
    pub seq: u64,
    /// The SSE event name, equal to the JSON `type`.
    pub kind: &'static str,
    /// The JSON body.
    pub json: String,
}

impl Event {
    /// The event in `text/event-stream` form.
    pub fn sse(&self) -> String {
        format!(
            "id: {}\nevent: {}\ndata: {}\n\n",
            self.seq, self.kind, self.json
        )
    }
}

/// `main` as the merger bot last wrote it.
#[derive(Debug, Clone)]
pub struct Main {
    /// Its tip.
    pub commit: ObjectId,
    /// One palette index per tile, row-major; [`UNSET`] for no file.
    pub pixels: Arc<Vec<u8>>,
    /// Root tree entries other than `tiles` (a README), kept by every
    /// commit the server writes.
    pub extras: Arc<Vec<TreeEntry>>,
    /// How much history `main` holds since its root commit.
    pub history: History,
}

/// `main`'s history since its root commit (the first commit, or the last
/// history reset): what a full clone of `main` fetches. Each commit the
/// server writes on `main` records the counts before it in a
/// `Place-History:` trailer ([`History::trailer`]), so a restart picks them
/// up from the tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct History {
    /// The root commit.
    pub root: ObjectId,
    /// Commits written on top of it.
    pub commits: u64,
    /// Bytes of the packs those commits and the root were written in
    /// (deflated objects, so about what a clone downloads for them). After
    /// a restart the tip's own pack is missing from the count, so it can
    /// be up to one commit's pack short.
    pub bytes: u64,
}

impl History {
    /// The history of a `main` whose tip has no `Place-History:` trailer
    /// and has parents: written before the server counted, so its size is
    /// unknown and counts as over any limit.
    pub const UNKNOWN: Self = Self {
        root: ObjectId::ZERO,
        commits: u64::MAX,
        bytes: u64::MAX,
    };

    /// A history that is only `root`, written in a pack of `bytes`.
    pub fn root(root: ObjectId, bytes: u64) -> Self {
        Self {
            root,
            commits: 0,
            bytes,
        }
    }

    /// After one more commit, written in a pack of `bytes`.
    pub fn grow(self, bytes: u64) -> Self {
        Self {
            root: self.root,
            commits: self.commits.saturating_add(1),
            bytes: self.bytes.saturating_add(bytes),
        }
    }

    /// The trailer a commit on top of this history carries (with a
    /// leading blank line when `first` is true, to start the trailers).
    pub fn trailer(&self, first: bool) -> String {
        let blank = if first { "\n" } else { "" };
        if *self == Self::UNKNOWN {
            return String::new();
        }
        format!(
            "{blank}\nPlace-History: root={} commits={} bytes={}",
            self.root, self.commits, self.bytes
        )
    }

    /// The history before the commit whose raw object is `commit`: its
    /// `Place-History:` trailer, or `None` without one.
    pub fn parse(commit: &[u8]) -> Option<Self> {
        let text = String::from_utf8_lossy(commit);
        let line = text
            .lines()
            .rev()
            .find_map(|l| l.strip_prefix("Place-History: "))?;
        let mut root = None;
        let (mut commits, mut bytes) = (None, None);
        for field in line.split(' ') {
            match field.split_once('=') {
                Some(("root", v)) => root = v.parse::<ObjectId>().ok(),
                Some(("commits", v)) => commits = v.parse().ok(),
                Some(("bytes", v)) => bytes = v.parse().ok(),
                _ => {}
            }
        }
        Some(Self {
            root: root?,
            commits: commits?,
            bytes: bytes?,
        })
    }
}

/// A branch the page shows.
#[derive(Debug, Clone)]
pub struct Branch {
    /// Who paints on it.
    pub actor: Actor,
    /// Its tip.
    pub head: ObjectId,
    /// Unmerged tiles: index to (color, push seq).
    pub overlay: BTreeMap<usize, (u8, u64)>,
    /// When it last moved, in ms.
    pub updated_ms: u64,
    /// Deleted in Git with tiles still unmerged; it expires once they are.
    pub deleted: bool,
    /// Every tile of `head`'s tree, when the server wrote `head` itself
    /// (it builds the next commit on it without reading Git).
    pub full: Option<Arc<Vec<u8>>>,
}

/// A push the merger bot has not folded into `main` yet.
#[derive(Debug, Clone)]
pub struct Pending {
    /// Its sequence number.
    pub seq: u64,
    /// The branch (short name).
    pub branch: String,
    /// The tiles it painted.
    pub tiles: Vec<Tile>,
}

/// The kill switch.
#[derive(Debug, Clone, Default)]
pub struct KillSwitch {
    /// Engaged by an operator.
    pub engaged: bool,
    /// Why, if given.
    pub reason: Option<String>,
}

/// Swarm limits, enforced by the server whatever a client sends.
#[derive(Debug, Clone, Copy)]
pub struct SwarmLimits {
    /// Agents at full ramp.
    pub max_agents: u32,
    /// Pushes per second across the whole swarm.
    pub pushes_per_second_cap: u32,
    /// Length of one run.
    pub run_seconds: u32,
    /// Pause after a run before the next can start.
    pub cooldown_seconds: u32,
    /// Ramp from 0 to `max_agents`.
    pub ramp_ms: u64,
}

/// The one global swarm run.
#[derive(Debug, Clone, Default)]
pub struct Swarm {
    /// `idle`, `running` or `cooldown`.
    pub state: SwarmState,
    /// When the current or last run started.
    pub started_ms: Option<u64>,
    /// When it ends or ended.
    pub ends_ms: Option<u64>,
    /// When the cooldown ends.
    pub cooldown_until_ms: Option<u64>,
    /// Pushes in the current or last run.
    pub pushes: u64,
    /// Push credit not spent yet.
    pub credit: f64,
    /// When credit was last added.
    pub last_tick_ms: u64,
    /// When the last progress event went out.
    pub last_broadcast_ms: u64,
}

/// A swarm's phase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SwarmState {
    /// Nothing running.
    #[default]
    Idle,
    /// A run is pushing.
    Running,
    /// A run ended; the next may start at `cooldown_until_ms`.
    Cooldown,
}

/// What a new event-stream subscriber gets first.
#[derive(Debug)]
pub enum Subscription {
    /// Every event after the client's `after`, then the live stream.
    Backlog(Vec<Arc<Event>>, broadcast::Receiver<Arc<Event>>),
    /// The client's `after` is gone from the backlog (or from the future,
    /// after a restart): one `resync` event, then the live stream.
    Resync(Arc<Event>, broadcast::Receiver<Arc<Event>>),
}

/// The canvas state (module docs).
#[derive(Debug)]
pub struct State {
    rules: Arc<Rules>,
    /// The last sequence number given out.
    pub seq: u64,
    /// Every push with a sequence number at or below this is in `main`.
    pub merged_through: u64,
    /// `main`.
    pub main: Main,
    /// Branches by short name (`paint/ada`).
    pub branches: BTreeMap<String, Branch>,
    /// Pushes waiting for the merger bot, in sequence order.
    pub pending: Vec<Pending>,
    /// When people (not the server's agents) last painted each tile, in
    /// ms, kept for [`HUMAN_MEMORY_MS`]: ambient agents place their
    /// sprites where they cover none of these. In memory only, like the
    /// rest of the canvas state.
    pub human_tiles: HashMap<usize, u64>,
    /// When ambient agents' sprites last landed on each tile (at merge), in
    /// ms, kept for [`HUMAN_MEMORY_MS`]: a new sprite never covers one.
    pub sprite_tiles: HashMap<usize, u64>,
    /// The kill switch.
    pub kill: KillSwitch,
    /// The swarm.
    pub swarm: Swarm,
    /// Its limits.
    pub swarm_limits: SwarmLimits,
    log: VecDeque<Arc<Event>>,
    backlog: usize,
    tx: broadcast::Sender<Arc<Event>>,
}

/// How long ambient agents keep clear of tiles people painted.
pub const HUMAN_MEMORY_MS: u64 = 6 * 3600 * 1000;

/// How long a swarm agent's branch stays once idle with its tiles merged.
pub const SWARM_BRANCH_TTL_MS: u64 = 30_000;

/// What one merge folds into `main`.
#[derive(Debug, Clone)]
pub struct MergePlan {
    /// The highest sequence number merged.
    pub through: u64,
    /// `main`'s tiles after the merge.
    pub pixels: Vec<u8>,
    /// Tiles whose color in `main` changes, with the new color.
    pub changed: Vec<Tile>,
    /// Branches with pushes in the merge.
    pub branches: Vec<String>,
    /// Tiles people painted in the merged pushes (indexes, ascending).
    pub people: Vec<usize>,
    /// Tiles ambient agents' sprites painted in the merged pushes.
    pub sprites: Vec<usize>,
}

impl State {
    /// A state whose `main` is `main`. Live subscribers lagging by more
    /// than `backlog` events are sent a `resync`.
    pub fn new(rules: Arc<Rules>, main: Main, swarm_limits: SwarmLimits, backlog: usize) -> Self {
        let (tx, _) = broadcast::channel(backlog.max(16));
        Self {
            rules,
            seq: 0,
            merged_through: 0,
            main,
            branches: BTreeMap::new(),
            pending: Vec::new(),
            human_tiles: HashMap::new(),
            sprite_tiles: HashMap::new(),
            kill: KillSwitch::default(),
            swarm: Swarm::default(),
            swarm_limits,
            log: VecDeque::new(),
            backlog,
            tx,
        }
    }

    fn emit(&mut self, kind: &'static str, now: u64, body: impl FnOnce(u64) -> Value) -> u64 {
        self.seq += 1;
        let seq = self.seq;
        let mut v = body(seq);
        v["type"] = json!(kind);
        v["seq"] = json!(seq);
        v["at"] = json!(iso(now));
        let event = Arc::new(Event {
            seq,
            kind,
            json: v.to_string(),
        });
        self.log.push_back(event.clone());
        while self.log.len() > self.backlog {
            self.log.pop_front();
        }
        let _ = self.tx.send(event);
        seq
    }

    /// A subscriber for events after `after` (`None`: from the live edge).
    pub fn subscribe(&self, after: Option<u64>, now: u64) -> Subscription {
        let rx = self.tx.subscribe();
        let Some(after) = after else {
            return Subscription::Backlog(Vec::new(), rx);
        };
        let oldest = self.log.front().map_or(self.seq + 1, |e| e.seq);
        if after > self.seq || after + 1 < oldest {
            let event = Arc::new(Event {
                seq: self.seq,
                kind: "resync",
                json: json!({ "type": "resync", "seq": self.seq, "at": iso(now) }).to_string(),
            });
            return Subscription::Resync(event, rx);
        }
        let backlog = self.log.iter().filter(|e| e.seq > after).cloned().collect();
        Subscription::Backlog(backlog, rx)
    }

    /// Records an accepted push to `branch`: its overlay, the merge queue
    /// and a `push` event. Returns the push's sequence number.
    pub fn record_push(
        &mut self,
        branch: &str,
        actor: Actor,
        head: ObjectId,
        tiles: Vec<Tile>,
        full: Option<Arc<Vec<u8>>>,
        now: u64,
    ) -> u64 {
        let seq = self.seq + 1;
        let rules = self.rules.clone();
        let b = self
            .branches
            .entry(branch.to_owned())
            .or_insert_with(|| Branch {
                actor: actor.clone(),
                head,
                overlay: BTreeMap::new(),
                updated_ms: now,
                deleted: false,
                full: None,
            });
        b.actor = actor.clone();
        b.head = head;
        b.updated_ms = now;
        b.deleted = false;
        b.full = full;
        for t in &tiles {
            b.overlay.insert(rules.index(t.x, t.y), (t.color, seq));
        }
        if !branch.starts_with("agent/") && !branch.starts_with("swarm/") {
            for t in &tiles {
                self.human_tiles.insert(rules.index(t.x, t.y), now);
            }
        }
        self.pending.push(Pending {
            seq,
            branch: branch.to_owned(),
            tiles: tiles.clone(),
        });
        let palette = &rules.palette;
        let got = self.emit("push", now, |_| {
            json!({
                "branch": branch,
                "actor": actor,
                "commit": head.to_string(),
                "tiles": tiles_json(&tiles, palette),
            })
        });
        debug_assert_eq!(got, seq);
        seq
    }

    /// The next merge: every pending push replayed onto `main`'s tiles in
    /// sequence order (the higher sequence number wins a tile). `None`
    /// when nothing is pending.
    pub fn plan_merge(&self) -> Option<MergePlan> {
        let last = self.pending.last()?;
        let mut pixels = (*self.main.pixels).clone();
        let mut branches = Vec::new();
        let (mut people, mut sprites) = (BTreeSet::new(), BTreeSet::new());
        for p in &self.pending {
            for t in &p.tiles {
                let i = self.rules.index(t.x, t.y);
                pixels[i] = t.color;
                if p.branch.starts_with("agent/") {
                    sprites.insert(i);
                } else if !p.branch.starts_with("swarm/") {
                    people.insert(i);
                }
            }
            if !branches.contains(&p.branch) {
                branches.push(p.branch.clone());
            }
        }
        let w = self.rules.width as usize;
        let changed = pixels
            .iter()
            .zip(self.main.pixels.iter())
            .enumerate()
            .filter(|(_, (new, old))| new != old)
            .map(|(i, (&color, _))| Tile {
                x: (i % w) as u32,
                y: (i / w) as u32,
                color,
            })
            .collect();
        Some(MergePlan {
            through: last.seq,
            pixels,
            changed,
            branches,
            people: people.into_iter().collect(),
            sprites: sprites.into_iter().collect(),
        })
    }

    /// `main` moved to `commit` with `plan` merged: drops the merged pushes
    /// and overlay tiles and emits a `merge` event.
    pub fn apply_merge(&mut self, commit: ObjectId, plan: MergePlan, now: u64) -> u64 {
        let through = plan.through;
        self.remember(&plan.people, &plan.sprites, now);
        self.pending.retain(|p| p.seq > through);
        for b in self.branches.values_mut() {
            b.overlay.retain(|_, (_, seq)| *seq > through);
        }
        self.merged_through = through;
        self.main.commit = commit;
        self.main.pixels = Arc::new(plan.pixels);
        let rules = self.rules.clone();
        let palette = &rules.palette;
        self.emit("merge", now, |_| {
            json!({
                "commit": commit.to_string(),
                "throughSeq": through,
                "tiles": tiles_json(&plan.changed, palette),
                "branches": plan.branches,
            })
        })
    }

    /// Records `people`'s and `sprites`' tiles as painted at `at` (ms),
    /// keeping the later time per tile.
    pub fn remember(&mut self, people: &[usize], sprites: &[usize], at: u64) {
        for &i in people {
            let e = self.human_tiles.entry(i).or_insert(at);
            *e = (*e).max(at);
        }
        for &i in sprites {
            let e = self.sprite_tiles.entry(i).or_insert(at);
            *e = (*e).max(at);
        }
    }

    /// Tiles a new sprite must not cover: people's and sprites' from the
    /// last [`HUMAN_MEMORY_MS`] (older entries are dropped).
    pub fn protected_tiles(&mut self, now: u64) -> Vec<usize> {
        let fresh = |at: &mut u64| at.saturating_add(HUMAN_MEMORY_MS) > now;
        self.human_tiles.retain(|_, at| fresh(at));
        self.sprite_tiles.retain(|_, at| fresh(at));
        let rules = self.rules.clone();
        // Sprites pushed but not merged yet count too (ambient-only merges
        // wait up to `ambient_merge_interval`).
        let pending = self
            .pending
            .iter()
            .filter(|p| p.branch.starts_with("agent/"))
            .flat_map(|p| p.tiles.iter().map(|t| rules.index(t.x, t.y)))
            .collect::<Vec<_>>();
        let mut out: Vec<usize> = self
            .human_tiles
            .keys()
            .chain(self.sprite_tiles.keys())
            .copied()
            .chain(pending)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Tiles people (not the server's agents) painted in the last
    /// [`HUMAN_MEMORY_MS`], without the agents' paint
    /// [`Self::protected_tiles`] adds.
    pub fn recent_people_tiles(&self, now: u64) -> Vec<usize> {
        self.human_tiles
            .iter()
            .filter(|(_, at)| at.saturating_add(HUMAN_MEMORY_MS) > now)
            .map(|(i, _)| *i)
            .collect()
    }

    /// Tiles the server's agents (`agent/`) painted in the last
    /// [`HUMAN_MEMORY_MS`], merged or still pending.
    pub fn recent_agent_tiles(&self, now: u64) -> Vec<usize> {
        let rules = &self.rules;
        self.sprite_tiles
            .iter()
            .filter(|(_, at)| at.saturating_add(HUMAN_MEMORY_MS) > now)
            .map(|(i, _)| *i)
            .chain(
                self.pending
                    .iter()
                    .filter(|p| p.branch.starts_with(AMBIENT_PREFIX))
                    .flat_map(|p| p.tiles.iter().map(|t| rules.index(t.x, t.y))),
            )
            .collect()
    }

    /// `branch` was deleted in Git. It leaves the page now if all its
    /// tiles merged, otherwise once they have.
    pub fn branch_deleted(&mut self, branch: &str, now: u64) {
        let Some(b) = self.branches.get_mut(branch) else {
            return;
        };
        if b.overlay.is_empty() {
            self.expire(branch, now);
        } else {
            b.deleted = true;
            b.full = None;
        }
    }

    /// Removes `branch` and emits an `expire` event.
    pub fn expire(&mut self, branch: &str, now: u64) -> Option<u64> {
        self.branches.remove(branch)?;
        Some(self.emit("expire", now, |_| json!({ "branch": branch })))
    }

    /// Branches whose tiles have all merged and that have been idle for
    /// `ttl_ms` (or were deleted in Git): `(name, head, deleted)`. The
    /// server's own painters' branches go sooner, because every clone
    /// fetches every advertised branch's strokes: an ambient agent's
    /// (`agent/`) as soon as its tiles have merged, a swarm agent's
    /// (`swarm/`) after [`SWARM_BRANCH_TTL_MS`] idle (sooner would delete
    /// and recreate hundreds of branches on every merge of a run).
    pub fn expirable(&self, ttl_ms: u64, now: u64) -> Vec<(String, ObjectId, bool)> {
        self.branches
            .iter()
            .filter(|(name, b)| {
                let ttl = if name.starts_with("agent/") {
                    0
                } else if name.starts_with("swarm/") {
                    ttl_ms.min(SWARM_BRANCH_TTL_MS)
                } else {
                    ttl_ms
                };
                b.overlay.is_empty() && (b.deleted || b.updated_ms + ttl <= now)
            })
            .map(|(n, b)| (n.clone(), b.head, b.deleted))
            .collect()
    }

    /// The `GET /canvas` body.
    pub fn snapshot(&self, ttl_ms: u64) -> Value {
        let pixels: String = self
            .main
            .pixels
            .iter()
            .map(|&p| {
                if p == UNSET {
                    '.'
                } else {
                    char::from_digit(u32::from(p), 36).unwrap_or('.')
                }
            })
            .collect();
        let branches: Vec<Value> = self
            .branches
            .iter()
            .filter(|(_, b)| !b.overlay.is_empty())
            .map(|(n, b)| self.overlay_json(n, b, ttl_ms))
            .collect();
        json!({
            "seq": self.seq,
            "mergedThroughSeq": self.merged_through,
            "mainCommit": self.main.commit.to_string(),
            "width": self.rules.width,
            "height": self.rules.height,
            "pixels": pixels,
            "branches": branches,
        })
    }

    fn overlay_json(&self, name: &str, b: &Branch, ttl_ms: u64) -> Value {
        let w = self.rules.width as usize;
        let tiles: Vec<Value> = b
            .overlay
            .iter()
            .map(|(&i, &(color, seq))| {
                json!({
                    "x": i % w,
                    "y": i / w,
                    "color": self.rules.palette[usize::from(color)].color,
                    "seq": seq,
                })
            })
            .collect();
        json!({
            "name": name,
            "actor": b.actor,
            "head": b.head.to_string(),
            "tiles": tiles,
            "updatedAt": iso(b.updated_ms),
            "expiresAt": iso(b.updated_ms + ttl_ms),
        })
    }

    /// The `GET /branches` body.
    pub fn branch_list(&self, ttl_ms: u64) -> Value {
        let branches: Vec<Value> = self
            .branches
            .iter()
            .map(|(n, b)| {
                json!({
                    "name": n,
                    "actor": b.actor,
                    "head": b.head.to_string(),
                    "pendingTiles": b.overlay.len(),
                    "updatedAt": iso(b.updated_ms),
                    "expiresAt": iso(b.updated_ms + ttl_ms),
                })
            })
            .collect();
        json!({ "branches": branches })
    }

    /// Agents pushing right now: ramps to `max_agents` over `ramp_ms`.
    pub fn swarm_agents(&self, now: u64) -> u32 {
        let l = self.swarm_limits;
        match (self.swarm.state, self.swarm.started_ms) {
            (SwarmState::Running, Some(start)) if !self.kill.engaged => {
                let elapsed = now.saturating_sub(start);
                let ramp = l.ramp_ms.max(1);
                ((u64::from(l.max_agents) * elapsed).div_ceil(ramp)).min(u64::from(l.max_agents))
                    as u32
            }
            _ => 0,
        }
    }

    /// The `SwarmStatus` body.
    pub fn swarm_status(&self, now: u64) -> Value {
        let s = &self.swarm;
        let l = self.swarm_limits;
        let state = if self.kill.engaged {
            "disabled"
        } else {
            match s.state {
                SwarmState::Idle => "idle",
                SwarmState::Running => "running",
                SwarmState::Cooldown => "cooldown",
            }
        };
        json!({
            "state": state,
            "agents": self.swarm_agents(now),
            "maxAgents": l.max_agents,
            "pushesPerSecondCap": l.pushes_per_second_cap,
            "runSeconds": l.run_seconds,
            "pushes": s.pushes,
            "startedAt": s.started_ms.map(iso),
            "endsAt": s.ends_ms.map(iso),
            "cooldownUntil": (s.state == SwarmState::Cooldown).then_some(s.cooldown_until_ms).flatten().map(iso),
            "killSwitch": { "engaged": self.kill.engaged, "reason": self.kill.reason },
        })
    }

    /// Emits a `swarm` event with the current status.
    pub fn emit_swarm(&mut self, now: u64) -> u64 {
        let status = self.swarm_status(now);
        self.swarm.last_broadcast_ms = now;
        self.emit("swarm", now, |_| json!({ "status": status }))
    }

    /// Ends a running swarm at `now` and starts its cooldown.
    pub fn end_swarm_run(&mut self, now: u64) {
        let cooldown = u64::from(self.swarm_limits.cooldown_seconds) * 1000;
        self.swarm.state = SwarmState::Cooldown;
        self.swarm.ends_ms = Some(now);
        self.swarm.cooldown_until_ms = Some(now + cooldown);
        self.swarm.credit = 0.0;
    }

    /// Advances the swarm to `now` and returns how many pushes it may make
    /// now: credit accrues at `pushes_per_second_cap` scaled by the ramp,
    /// so a run never exceeds the cap whatever any client sends. Credit
    /// is not banked beyond one second's worth.
    pub fn swarm_tick(&mut self, now: u64) -> u32 {
        let l = self.swarm_limits;
        match self.swarm.state {
            SwarmState::Cooldown => {
                if self.swarm.cooldown_until_ms.is_some_and(|t| now >= t) {
                    self.swarm.state = SwarmState::Idle;
                    self.swarm.cooldown_until_ms = None;
                    self.emit_swarm(now);
                }
                0
            }
            SwarmState::Idle => 0,
            SwarmState::Running => {
                if self.kill.engaged || self.swarm.ends_ms.is_some_and(|e| now >= e) {
                    let end = if self.kill.engaged {
                        now
                    } else {
                        self.swarm.ends_ms.unwrap_or(now)
                    };
                    self.end_swarm_run(end);
                    self.emit_swarm(now);
                    return 0;
                }
                let elapsed = now.saturating_sub(self.swarm.last_tick_ms) as f64 / 1000.0;
                self.swarm.last_tick_ms = now;
                let share = f64::from(self.swarm_agents(now)) / f64::from(l.max_agents.max(1));
                let cap = f64::from(l.pushes_per_second_cap);
                self.swarm.credit = (self.swarm.credit + share * cap * elapsed).min(cap.max(1.0));
                let n = self.swarm.credit.floor();
                self.swarm.credit -= n;
                if now.saturating_sub(self.swarm.last_broadcast_ms) >= 1000 {
                    self.emit_swarm(now);
                }
                n as u32
            }
        }
    }
}

fn tiles_json(tiles: &[Tile], palette: &[crate::rules::PaletteEntry]) -> Vec<Value> {
    tiles
        .iter()
        .map(|t| json!({ "x": t.x, "y": t.y, "color": palette[usize::from(t.color)].color }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::default_palette;

    fn state() -> State {
        let rules = Arc::new(Rules {
            width: 4,
            height: 4,
            palette: default_palette(),
            tiles_per_push: 64,
        });
        let main = Main {
            commit: ObjectId::from_bytes([1; 20]),
            pixels: Arc::new(vec![UNSET; 16]),
            extras: Arc::new(Vec::new()),
            history: History::UNKNOWN,
        };
        let limits = SwarmLimits {
            max_agents: 10,
            pushes_per_second_cap: 5,
            run_seconds: 10,
            cooldown_seconds: 10,
            ramp_ms: 1,
        };
        State::new(rules, main, limits, 3)
    }

    fn tile(x: u32, color: u8) -> Tile {
        Tile { x, y: 0, color }
    }

    #[test]
    fn history_trailers_round_trip_and_unknown_writes_none() {
        let root = ObjectId::from_bytes([7; 20]);
        let h = History::root(root, 100).grow(20).grow(30);
        assert_eq!((h.commits, h.bytes), (2, 150));
        let commit = format!(
            "tree {}\nparent {}\nauthor a <a@b> 1 +0000\ncommitter a <a@b> 1 +0000\n\nMerge{}",
            ObjectId::from_bytes([1; 20]),
            root,
            h.trailer(true)
        );
        assert_eq!(History::parse(commit.as_bytes()), Some(h));
        assert!(h.trailer(true).starts_with("\n\nPlace-History: "));
        assert!(h.trailer(false).starts_with("\nPlace-History: "));
        assert_eq!(History::UNKNOWN.trailer(true), "");
        assert_eq!(History::parse(b"tree x\n\nno trailer"), None);
        assert_eq!(
            History::parse(b"m\n\nPlace-History: root=zz commits=1 bytes=2"),
            None
        );
        // Saturates instead of wrapping: unknown stays over any limit.
        assert_eq!(History::UNKNOWN.grow(5).bytes, u64::MAX);
    }

    #[test]
    fn iso_timestamps_are_utc_rfc3339() {
        assert_eq!(iso(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso(1_762_171_200_123), "2025-11-03T12:00:00.123Z");
        assert_eq!(iso(951_782_400_000), "2000-02-29T00:00:00.000Z");
    }

    /// Every clone fetches every advertised branch, so the server's own
    /// painters' branches leave as soon as their strokes are in `main`
    /// (ambient agents) or shortly after (swarm agents), while people's
    /// branches keep the TTL.
    #[test]
    fn painters_branches_expire_once_merged_and_people_keep_the_ttl() {
        let mut s = state();
        for (i, name) in ["agent/wren-12", "swarm/agent-0001", "paint/ada"]
            .into_iter()
            .enumerate()
        {
            s.record_push(
                name,
                Actor::git(name),
                ObjectId::from_bytes([i as u8 + 2; 20]),
                vec![tile(i as u32, 5)],
                None,
                1_000,
            );
        }
        let names =
            |v: Vec<(String, ObjectId, bool)>| v.into_iter().map(|e| e.0).collect::<Vec<_>>();
        assert!(s.expirable(600_000, 1_000).is_empty(), "nothing merged yet");
        let plan = s.plan_merge().unwrap();
        s.apply_merge(ObjectId::from_bytes([9; 20]), plan, 1_000);
        assert_eq!(names(s.expirable(600_000, 1_000)), ["agent/wren-12"]);
        assert_eq!(
            names(s.expirable(600_000, 1_000 + SWARM_BRANCH_TTL_MS)),
            ["agent/wren-12", "swarm/agent-0001"]
        );
        assert_eq!(names(s.expirable(600_000, 601_000)).len(), 3);
    }

    #[test]
    fn merges_replay_pushes_in_seq_order_and_keep_later_ones_pending() {
        let mut s = state();
        let a = s.record_push(
            "a",
            Actor::git("a"),
            ObjectId::from_bytes([2; 20]),
            vec![tile(0, 5), tile(1, 5)],
            None,
            1,
        );
        let b = s.record_push(
            "b",
            Actor::git("b"),
            ObjectId::from_bytes([3; 20]),
            vec![tile(1, 7)],
            None,
            2,
        );
        assert!(a < b);
        let plan = s.plan_merge().unwrap();
        assert_eq!(plan.through, b);
        // The higher sequence number wins tile 1.
        assert_eq!(&plan.pixels[..2], &[5, 7]);
        assert_eq!(plan.branches, ["a", "b"]);
        // A push recorded after planning stays pending and in its overlay.
        let c = s.record_push(
            "a",
            Actor::git("a"),
            ObjectId::from_bytes([4; 20]),
            vec![tile(1, 3)],
            None,
            3,
        );
        let m = s.apply_merge(ObjectId::from_bytes([9; 20]), plan, 4);
        assert!(m > c);
        assert_eq!(s.merged_through, b);
        assert_eq!(s.pending.len(), 1);
        assert_eq!(s.pending[0].seq, c);
        assert_eq!(s.branches["a"].overlay.len(), 1);
        assert!(s.branches["b"].overlay.is_empty());
        assert_eq!(&s.main.pixels[..2], &[5, 7]);
    }

    #[test]
    fn subscribers_get_exactly_the_events_after_their_seq_or_a_resync() {
        let mut s = state();
        for i in 0..5u8 {
            s.record_push(
                "a",
                Actor::git("a"),
                ObjectId::from_bytes([i; 20]),
                vec![tile(0, 1)],
                None,
                1,
            );
        }
        // Backlog keeps the last 3 (seq 3..=5).
        match s.subscribe(Some(2), 9) {
            Subscription::Backlog(b, _) => {
                assert_eq!(b.iter().map(|e| e.seq).collect::<Vec<_>>(), [3, 4, 5]);
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(s.subscribe(Some(1), 9), Subscription::Resync(..)));
        // A seq from the future (a client of an earlier server run).
        assert!(matches!(s.subscribe(Some(6), 9), Subscription::Resync(..)));
        match s.subscribe(Some(5), 9) {
            Subscription::Backlog(b, mut rx) => {
                assert!(b.is_empty());
                s.record_push(
                    "a",
                    Actor::git("a"),
                    ObjectId::from_bytes([7; 20]),
                    vec![],
                    None,
                    1,
                );
                assert_eq!(rx.try_recv().unwrap().seq, 6);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_swarm_never_exceeds_its_cap_and_the_kill_switch_ends_it() {
        let mut s = state();
        s.swarm = Swarm {
            state: SwarmState::Running,
            started_ms: Some(0),
            ends_ms: Some(100_000),
            last_tick_ms: 0,
            ..Swarm::default()
        };
        // Ticks every 10 ms for 10 s: at most cap * 10 pushes.
        let total: u32 = (1..=1000).map(|i| s.swarm_tick(i * 10)).sum();
        assert!((5 * 10 - 1..=5 * 10).contains(&total), "{total}");
        // A long gap does not bank more than one second of credit.
        assert!(s.swarm_tick(60_000) <= 5);
        s.kill.engaged = true;
        assert_eq!(s.swarm_tick(60_010), 0);
        assert_eq!(s.swarm.state, SwarmState::Cooldown);
        assert_eq!(s.swarm_status(60_010)["state"], "disabled");
    }
}

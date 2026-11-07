/**
 * In-memory simulation of the `place` back end: the canvas repository's main
 * branch, per-visitor branches, the merger bot, ambient agents and the
 * "Unleash 1,000 agents" swarm. It implements the behaviour that
 * `api/openapi.yaml` describes so the front end can be built and tested
 * without a Git server. Nothing here talks to Git; commit ids are fake.
 */
import { createHash, randomBytes } from 'node:crypto';
import type {
  Actor,
  BranchOverlay,
  BranchSummary,
  CanvasSnapshot,
  Config,
  Limits,
  PlaceEvent,
  PushResult,
  Quota,
  Session,
  SwarmStatus,
} from '../src/shared/contract.ts';
import { DEFAULT_PALETTE, type PaletteEntry } from '../src/shared/palette.ts';
import { type CanvasRules, type Rejection, type Tile, validateStroke } from '../src/shared/tiles.ts';

export interface SwarmOptions {
  maxAgents: number;
  pushesPerSecondCap: number;
  runSeconds: number;
  cooldownSeconds: number;
  /** Runs one client IP may start per hour. */
  startsPerIpPerHour: number;
}

export interface MockOptions {
  /** Base domain; the clone URL is `https://place.<domain>/canvas.git`. */
  domain: string;
  /** Overrides the derived clone URL. */
  cloneUrl?: string;
  /** `undefined` derives `https://dashboard.<domain>/`; `null` means none. */
  dashboardUrl?: string | null;
  width: number;
  height: number;
  palette: readonly PaletteEntry[];
  limits: Limits;
  mergeIntervalMs: number;
  /** Background agents that paint a few strokes per second between them. */
  ambientAgents: number;
  /** Sessions one client IP may create per minute. */
  sessionsPerIpPerMinute: number;
  swarm: SwarmOptions;
  /** How many recent events a reconnecting client can catch up from. */
  eventBacklog: number;
  /** Bearer token for the operator-only kill-switch endpoint. */
  operatorToken: string;
  killSwitch: { engaged: boolean; reason: string | null };
  seed: number;
  /** Paint a starter picture on main. */
  seedCanvas: boolean;
  now: () => number;
}

export const DEFAULT_OPTIONS: MockOptions = {
  domain: 'example.com',
  width: 64,
  height: 64,
  palette: DEFAULT_PALETTE,
  limits: { tilesPerPush: 64, pushesPerMinute: 30, branchTtlSeconds: 600 },
  mergeIntervalMs: 3000,
  ambientAgents: 6,
  sessionsPerIpPerMinute: 10,
  swarm: {
    maxAgents: 1000,
    pushesPerSecondCap: 120,
    runSeconds: 20,
    cooldownSeconds: 40,
    startsPerIpPerHour: 6,
  },
  eventBacklog: 5000,
  operatorToken: 'mock-operator-token',
  killSwitch: { engaged: false, reason: null },
  seed: 20261102,
  seedCanvas: true,
  now: () => Date.now(),
};

interface BranchState {
  name: string;
  actor: Actor;
  head: string;
  tiles: Map<number, { color: string; seq: number }>;
  updatedAt: number;
}

interface SessionState {
  token: string;
  branch: string;
  pushTimes: number[];
  createdAt: number;
}

interface PendingPush {
  seq: number;
  branch: string;
  tiles: Tile[];
}

export type PushOutcome =
  | { ok: true; result: PushResult }
  | { ok: false; status: 401; message: string }
  | { ok: false; status: 403; message: string }
  | { ok: false; status: 422; rejections: Rejection[] }
  | { ok: false; status: 429; retryAfterSeconds: number; quota: Quota };

export type StartOutcome =
  | { ok: true; status: SwarmStatus }
  | { ok: false; reason: 'kill-switch'; status: SwarmStatus; retryAfterSeconds: number }
  | { ok: false; reason: 'cooldown' | 'rate-limited'; status: SwarmStatus; retryAfterSeconds: number };

export type Subscription =
  | { ok: true; backlog: PlaceEvent[]; unsubscribe: () => void }
  | { ok: false; resync: PlaceEvent; unsubscribe: () => void };

const UNSET = 255;
const AGENT_NAMES = ['quill', 'ember', 'moss', 'larch', 'wren', 'flint', 'sorrel', 'tamarack', 'heron', 'cinder'];

export class PlaceSim {
  readonly opts: MockOptions;
  readonly rules: CanvasRules;
  private readonly main: Uint8Array;
  private mainCommit: string;
  private seq = 0;
  private mergedThroughSeq = 0;
  /** This mock server's feed (`CanvasSnapshot.instance`). */
  readonly instance = Math.random().toString(16).slice(2, 10);
  private readonly pending: PendingPush[] = [];
  private readonly branches = new Map<string, BranchState>();
  private readonly sessions = new Map<string, SessionState>();
  private readonly ipSessions = new Map<string, number[]>();
  private readonly ipSwarmStarts = new Map<string, number[]>();
  private readonly log: PlaceEvent[] = [];
  private readonly listeners = new Set<(e: PlaceEvent) => void>();
  private readonly rand: () => number;
  private readonly agents: {
    name: string;
    kind: 'agent' | 'git';
    x: number;
    y: number;
    color: number;
    nextAt: number;
  }[] = [];
  private swarm: {
    state: 'idle' | 'running' | 'cooldown';
    startedAt: number | null;
    endsAt: number | null;
    cooldownUntil: number | null;
    pushes: number;
    credit: number;
    lastTickAt: number;
    lastBroadcastAt: number;
    center: { x: number; y: number; r: number };
  };
  private killSwitch: { engaged: boolean; reason: string | null };
  private lastMergeAt: number;
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(options: Partial<MockOptions> = {}) {
    this.opts = {
      ...DEFAULT_OPTIONS,
      ...options,
      limits: { ...DEFAULT_OPTIONS.limits, ...options.limits },
      swarm: { ...DEFAULT_OPTIONS.swarm, ...options.swarm },
    };
    const o = this.opts;
    this.rules = { width: o.width, height: o.height, palette: o.palette, maxTilesPerPush: o.limits.tilesPerPush };
    this.main = new Uint8Array(o.width * o.height).fill(UNSET);
    this.rand = mulberry32(o.seed);
    this.killSwitch = { ...o.killSwitch };
    this.swarm = {
      state: 'idle',
      startedAt: null,
      endsAt: null,
      cooldownUntil: null,
      pushes: 0,
      credit: 0,
      lastTickAt: 0,
      lastBroadcastAt: 0,
      center: { x: 0, y: 0, r: 0 },
    };
    if (o.seedCanvas) paintStarterPicture(this.main, o.width, o.height, o.palette);
    this.mainCommit = fakeOid(`root\n${o.seed}\n${Buffer.from(this.main).toString('hex')}`);
    this.lastMergeAt = o.now();
    const now = o.now();
    for (let i = 0; i < o.ambientAgents; i++) {
      // One ambient painter stands in for a person pushing with stock git.
      const kind = i === 0 ? 'git' : 'agent';
      const word = AGENT_NAMES[i % AGENT_NAMES.length];
      const name = kind === 'git' ? `paint/${word}` : `agent/${word}-${String(10 + Math.floor(this.rand() * 89))}`;
      this.agents.push({
        name,
        kind,
        x: Math.floor(this.rand() * o.width),
        y: Math.floor(this.rand() * o.height),
        color: Math.floor(this.rand() * o.palette.length),
        nextAt: now + 300 + this.rand() * 2000,
      });
    }
  }

  /** Starts the background clock: merger bot, ambient agents and swarm. */
  start(tickMs = 100): void {
    if (this.timer) return;
    this.timer = setInterval(() => this.tick(), tickMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.listeners.clear();
  }

  /** Advances the simulation to `now()`. Called by the clock; tests call it directly. */
  tick(): void {
    const now = this.opts.now();
    for (const a of this.agents) {
      // The kill switch pauses every simulated pusher, not only the swarm.
      if (this.killSwitch.engaged) break;
      if (now >= a.nextAt) {
        this.ambientStroke(a);
        a.nextAt = now + 1200 + this.rand() * 3500;
      }
    }
    this.swarmTick(now);
    if (now - this.lastMergeAt >= this.opts.mergeIntervalMs) {
      this.merge();
      this.expireBranches();
    }
  }

  config(): Config {
    const o = this.opts;
    return {
      cloneUrl: o.cloneUrl ?? `https://place.${o.domain}/canvas.git`,
      dashboardUrl: o.dashboardUrl === undefined ? `https://dashboard.${o.domain}/` : o.dashboardUrl,
      canvas: { width: o.width, height: o.height, palette: o.palette.map((p) => ({ ...p })) },
      limits: { ...o.limits },
      mergeIntervalMs: o.mergeIntervalMs,
    };
  }

  /** `main` as the SVG of `GET /canvas.svg`, with its commit as the ETag. */
  canvasSvg(): { etag: string; svg: string } {
    const { width, height, palette } = this.opts;
    const scale = Math.max(1, Math.floor(720 / Math.max(width, height, 1)));
    const paths: string[] = palette.map(() => '');
    for (let y = 0; y < height; y++) {
      let x = 0;
      while (x < width) {
        const c = this.main[y * width + x];
        const start = x;
        while (x < width && this.main[y * width + x] === c) x++;
        if (c !== UNSET && c < paths.length) paths[c] += `M${start} ${y}h${x - start}v1h-${x - start}z`;
      }
    }
    const svg =
      `<svg xmlns="http://www.w3.org/2000/svg" width="${width * scale}" height="${height * scale}" viewBox="0 0 ${width} ${height}" shape-rendering="crispEdges"><title>place canvas</title>` +
      `<rect width="${width}" height="${height}" fill="#fafafa"/>` +
      palette.map((p, i) => (paths[i] ? `<path fill="${p.color}" d="${paths[i]}"/>` : '')).join('') +
      '</svg>';
    return { etag: `"${this.mainCommit}"`, svg };
  }

  snapshot(): CanvasSnapshot {
    let pixels = '';
    for (const v of this.main) pixels += v === UNSET ? '.' : v.toString(36);
    return {
      seq: this.seq,
      instance: this.instance,
      mergedThroughSeq: this.mergedThroughSeq,
      mainCommit: this.mainCommit,
      width: this.opts.width,
      height: this.opts.height,
      pixels,
      branches: [...this.branches.values()].filter((b) => b.tiles.size > 0).map((b) => this.overlay(b)),
    };
  }

  branchList(): BranchSummary[] {
    return [...this.branches.values()].map((b) => ({
      name: b.name,
      actor: { ...b.actor },
      head: b.head,
      pendingTiles: b.tiles.size,
      updatedAt: iso(b.updatedAt),
      expiresAt: iso(this.expiresAt(b)),
    }));
  }

  createSession(ip: string): { ok: true; session: Session } | { ok: false; retryAfterSeconds: number } {
    const now = this.opts.now();
    const times = window(this.ipSessions.get(ip) ?? [], now, 60_000);
    if (times.length >= this.opts.sessionsPerIpPerMinute) {
      return { ok: false, retryAfterSeconds: retryAfter(times[0] + 60_000, now) };
    }
    times.push(now);
    this.ipSessions.set(ip, times);
    let branch: string;
    do branch = `visitors/${base32(randomBytes(7))}`;
    while (this.branches.has(branch));
    const token = randomBytes(24).toString('base64url');
    const s: SessionState = { token, branch, pushTimes: [], createdAt: now };
    this.sessions.set(token, s);
    return {
      ok: true,
      session: { branch, token, expiresAt: iso(now + this.opts.limits.branchTtlSeconds * 1000), quota: this.quota(s) },
    };
  }

  quotaFor(token: string | null): Quota | null {
    const s = token ? this.sessions.get(token) : undefined;
    return s ? this.quota(s) : null;
  }

  /** A visitor's stroke through the web API. */
  pushAsVisitor(token: string | null, branch: string, tiles: Tile[], message?: string): PushOutcome {
    const s = token ? this.sessions.get(token) : undefined;
    if (!s)
      return { ok: false, status: 401, message: 'Missing or unknown session token. Create one with POST /sessions.' };
    if (s.branch !== branch) {
      return { ok: false, status: 403, message: `This session can push only to ${s.branch}.` };
    }
    const now = this.opts.now();
    s.pushTimes = window(s.pushTimes, now, 60_000);
    if (s.pushTimes.length >= this.opts.limits.pushesPerMinute) {
      return {
        ok: false,
        status: 429,
        retryAfterSeconds: retryAfter(s.pushTimes[0] + 60_000, now),
        quota: this.quota(s),
      };
    }
    const verdict = validateStroke(tiles, this.rules);
    if (!verdict.ok) return { ok: false, status: 422, rejections: verdict.rejections };
    s.pushTimes.push(now);
    const label = branch.slice(branch.indexOf('/') + 1);
    const { seq, commit, parent } = this.recordPush(branch, { kind: 'visitor', label }, verdict.tiles, message);
    return { ok: true, result: { seq, branch, commit, parent, tiles: verdict.tiles, quota: this.quota(s) } };
  }

  /**
   * Records an accepted push from any actor. Validation is the caller's job:
   * agents in this simulation only produce valid strokes.
   */
  recordPush(
    branch: string,
    actor: Actor,
    tiles: Tile[],
    message?: string,
  ): { seq: number; commit: string; parent: string } {
    const now = this.opts.now();
    let b = this.branches.get(branch);
    if (!b) {
      b = { name: branch, actor, head: this.mainCommit, tiles: new Map(), updatedAt: now };
      this.branches.set(branch, b);
    }
    const seq = ++this.seq;
    const parent = b.head;
    const commit = fakeOid(`${parent}\n${branch}\n${seq}\n${message ?? ''}\n${JSON.stringify(tiles)}`);
    b.head = commit;
    b.updatedAt = now;
    for (const t of tiles) b.tiles.set(t.y * this.opts.width + t.x, { color: t.color, seq });
    this.pending.push({ seq, branch, tiles });
    this.emit({ type: 'push', seq, branch, actor: { ...actor }, commit, tiles, at: iso(now) });
    return { seq, commit, parent };
  }

  /** The merger bot: folds every pending push into main, oldest first. */
  merge(): void {
    this.lastMergeAt = this.opts.now();
    if (this.pending.length === 0) return;
    const throughSeq = this.pending[this.pending.length - 1].seq;
    const changed = new Map<number, string>();
    const merged = new Set<string>();
    for (const p of this.pending) {
      merged.add(p.branch);
      for (const t of p.tiles) changed.set(t.y * this.opts.width + t.x, t.color);
    }
    this.pending.length = 0;
    const index = new Map(this.opts.palette.map((p, i) => [p.color, i]));
    for (const [i, color] of changed) this.main[i] = index.get(color) ?? UNSET;
    for (const name of merged) {
      const b = this.branches.get(name);
      if (!b) continue;
      for (const [i, t] of b.tiles) if (t.seq <= throughSeq) b.tiles.delete(i);
    }
    this.mergedThroughSeq = throughSeq;
    this.mainCommit = fakeOid(`${this.mainCommit}\nmerge\n${throughSeq}`);
    const tiles = [...changed].map(([i, color]) => ({
      x: i % this.opts.width,
      y: Math.floor(i / this.opts.width),
      color,
    }));
    this.emit({
      type: 'merge',
      seq: ++this.seq,
      commit: this.mainCommit,
      throughSeq,
      tiles,
      branches: [...merged],
      at: iso(this.lastMergeAt),
    });
  }

  /** Deletes branches idle past their TTL whose tiles have all merged. */
  expireBranches(): void {
    const now = this.opts.now();
    for (const b of this.branches.values()) {
      if (b.tiles.size === 0 && now >= this.expiresAt(b)) {
        this.branches.delete(b.name);
        this.emit({ type: 'expire', seq: ++this.seq, branch: b.name, at: iso(now) });
      }
    }
  }

  swarmStatus(): SwarmStatus {
    const s = this.swarm;
    const o = this.opts.swarm;
    const now = this.opts.now();
    let agents = 0;
    if (s.state === 'running' && s.startedAt !== null) {
      agents = Math.min(o.maxAgents, Math.ceil((o.maxAgents * (now - s.startedAt)) / 3000));
    }
    return {
      state: this.killSwitch.engaged ? 'disabled' : s.state,
      agents,
      maxAgents: o.maxAgents,
      pushesPerSecondCap: o.pushesPerSecondCap,
      runSeconds: o.runSeconds,
      pushes: s.pushes,
      startedAt: s.startedAt === null ? null : iso(s.startedAt),
      endsAt: s.endsAt === null ? null : iso(s.endsAt),
      cooldownUntil: s.state === 'cooldown' && s.cooldownUntil !== null ? iso(s.cooldownUntil) : null,
      killSwitch: { ...this.killSwitch },
    };
  }

  startSwarm(ip: string): StartOutcome {
    const now = this.opts.now();
    if (this.killSwitch.engaged) {
      return { ok: false, reason: 'kill-switch', status: this.swarmStatus(), retryAfterSeconds: 300 };
    }
    if (this.swarm.state === 'running' && this.swarm.endsAt !== null) {
      return {
        ok: false,
        reason: 'cooldown',
        status: this.swarmStatus(),
        retryAfterSeconds: retryAfter(this.swarm.endsAt, now),
      };
    }
    if (this.swarm.state === 'cooldown' && this.swarm.cooldownUntil !== null) {
      return {
        ok: false,
        reason: 'cooldown',
        status: this.swarmStatus(),
        retryAfterSeconds: retryAfter(this.swarm.cooldownUntil, now),
      };
    }
    const starts = window(this.ipSwarmStarts.get(ip) ?? [], now, 3_600_000);
    if (starts.length >= this.opts.swarm.startsPerIpPerHour) {
      return {
        ok: false,
        reason: 'rate-limited',
        status: this.swarmStatus(),
        retryAfterSeconds: retryAfter(starts[0] + 3_600_000, now),
      };
    }
    starts.push(now);
    this.ipSwarmStarts.set(ip, starts);
    const { width: w, height: h } = this.opts;
    const r = Math.floor(Math.min(w, h) * (0.18 + this.rand() * 0.12));
    this.swarm = {
      state: 'running',
      startedAt: now,
      endsAt: now + this.opts.swarm.runSeconds * 1000,
      cooldownUntil: null,
      pushes: 0,
      credit: 0,
      lastTickAt: now,
      lastBroadcastAt: now,
      center: { x: r + Math.floor(this.rand() * (w - 2 * r)), y: r + Math.floor(this.rand() * (h - 2 * r)), r },
    };
    this.emitSwarm();
    return { ok: true, status: this.swarmStatus() };
  }

  /**
   * Operator control. Engaging it stops a running swarm immediately and
   * pauses the ambient agents until it is released.
   */
  setKillSwitch(engaged: boolean, reason: string | null): SwarmStatus {
    this.killSwitch = { engaged, reason: engaged ? reason : null };
    if (engaged && this.swarm.state === 'running') this.endSwarmRun(this.opts.now());
    this.emitSwarm();
    return this.swarmStatus();
  }

  isOperator(token: string | null): boolean {
    return token !== null && token === this.opts.operatorToken;
  }

  /**
   * Subscribes to events after `after`. Returns the missed events to send
   * first, or a resync event when they are no longer in the backlog or
   * `instance` names another server's feed.
   */
  subscribe(after: number | null, listener: (e: PlaceEvent) => void, instance: string | null = null): Subscription {
    this.listeners.add(listener);
    const unsubscribe = () => this.listeners.delete(listener);
    const resync = (): Subscription => ({
      ok: false,
      resync: { type: 'resync', seq: this.seq, instance: this.instance, at: iso(this.opts.now()) },
      unsubscribe,
    });
    if (after !== null && instance !== null && instance !== this.instance) return resync();
    if (after === null || after >= this.seq) return { ok: true, backlog: [], unsubscribe };
    const oldest = this.log.length > 0 ? this.log[0].seq : this.seq + 1;
    if (after < oldest - 1) return resync();
    return { ok: true, backlog: this.log.filter((e) => e.seq > after), unsubscribe };
  }

  private quota(s: SessionState): Quota {
    const now = this.opts.now();
    s.pushTimes = window(s.pushTimes, now, 60_000);
    const l = this.opts.limits;
    return {
      pushesPerMinute: l.pushesPerMinute,
      remaining: Math.max(0, l.pushesPerMinute - s.pushTimes.length),
      resetAt: iso(s.pushTimes.length > 0 ? s.pushTimes[0] + 60_000 : now),
      tilesPerPush: l.tilesPerPush,
    };
  }

  private overlay(b: BranchState): BranchOverlay {
    const w = this.opts.width;
    return {
      name: b.name,
      actor: { ...b.actor },
      head: b.head,
      tiles: [...b.tiles].map(([i, t]) => ({ x: i % w, y: Math.floor(i / w), color: t.color, seq: t.seq })),
      updatedAt: iso(b.updatedAt),
      expiresAt: iso(this.expiresAt(b)),
    };
  }

  private expiresAt(b: BranchState): number {
    return b.updatedAt + this.opts.limits.branchTtlSeconds * 1000;
  }

  private emit(e: PlaceEvent): void {
    this.log.push(e);
    if (this.log.length > this.opts.eventBacklog) this.log.splice(0, this.log.length - this.opts.eventBacklog);
    for (const l of this.listeners) l(e);
  }

  private emitSwarm(): void {
    this.emit({ type: 'swarm', seq: ++this.seq, status: this.swarmStatus(), at: iso(this.opts.now()) });
  }

  private ambientStroke(a: { name: string; kind: 'agent' | 'git'; x: number; y: number; color: number }): void {
    const { width: w, height: h, palette } = this.opts;
    if (this.rand() < 0.3) a.color = Math.floor(this.rand() * palette.length);
    const len = 2 + Math.floor(this.rand() * 6);
    const tiles = new Map<number, Tile>();
    for (let i = 0; i < len; i++) {
      a.x = clamp(a.x + Math.round(this.rand() * 2 - 1), 0, w - 1);
      a.y = clamp(a.y + Math.round(this.rand() * 2 - 1), 0, h - 1);
      tiles.set(a.y * w + a.x, { x: a.x, y: a.y, color: palette[a.color].color });
    }
    this.recordPush(a.name, { kind: a.kind, label: a.name }, [...tiles.values()]);
  }

  private swarmTick(now: number): void {
    const s = this.swarm;
    if (s.state === 'cooldown' && s.cooldownUntil !== null && now >= s.cooldownUntil) {
      s.state = 'idle';
      s.cooldownUntil = null;
      this.emitSwarm();
      return;
    }
    if (s.state !== 'running' || s.endsAt === null || s.startedAt === null) return;
    if (now >= s.endsAt) {
      this.endSwarmRun(s.endsAt);
      this.emitSwarm();
      return;
    }
    const status = this.swarmStatus();
    const elapsed = Math.max(0, now - s.lastTickAt);
    s.credit += (Math.min(1, status.agents / status.maxAgents) * status.pushesPerSecondCap * elapsed) / 1000;
    s.lastTickAt = now;
    const { width: w, height: h, palette } = this.opts;
    const { x: cx, y: cy, r } = s.center;
    const rings = ['#ffd635', '#ff7a1a', '#d6262b', '#6d3a8c'].filter((c) => palette.some((p) => p.color === c));
    while (s.credit >= 1) {
      s.credit -= 1;
      const agent = Math.floor(this.rand() * status.maxAgents);
      const tiles = new Map<number, Tile>();
      const n = 1 + Math.floor(this.rand() * 3);
      for (let i = 0; i < n; i++) {
        const ang = this.rand() * Math.PI * 2;
        const d = Math.sqrt(this.rand()) * r;
        const x = clamp(Math.round(cx + Math.cos(ang) * d), 0, w - 1);
        const y = clamp(Math.round(cy + Math.sin(ang) * d), 0, h - 1);
        const band = Math.min(rings.length - 1, Math.floor((Math.hypot(x - cx, y - cy) / (r + 1)) * rings.length));
        tiles.set(y * w + x, { x, y, color: rings[band] ?? palette[0].color });
      }
      const name = `swarm/agent-${String(agent).padStart(4, '0')}`;
      this.recordPush(name, { kind: 'agent', label: name }, [...tiles.values()]);
      s.pushes++;
    }
    if (now - s.lastBroadcastAt >= 1000) {
      s.lastBroadcastAt = now;
      this.emitSwarm();
    }
  }

  private endSwarmRun(now: number): void {
    this.swarm.state = 'cooldown';
    this.swarm.endsAt = now;
    this.swarm.cooldownUntil = now + this.opts.swarm.cooldownSeconds * 1000;
  }
}

/** A deterministic, recognisable starting picture: aspen trunks against a sky. */
function paintStarterPicture(px: Uint8Array, w: number, h: number, palette: readonly PaletteEntry[]): void {
  const idx = (c: string) => {
    const i = palette.findIndex((p) => p.color === c);
    return Math.max(i, 0);
  };
  const [white, lgrey, black, yellow, orange, green, blue, lime] = [
    '#ffffff',
    '#d4d7d9',
    '#1b1f22',
    '#ffd635',
    '#ff7a1a',
    '#00a368',
    '#3690ea',
    '#7eed56',
  ].map(idx);
  const rand = mulberry32(7);
  const ground = Math.floor(h * 0.82);
  for (let y = ground; y < h; y++) {
    for (let x = 0; x < w; x++) px[y * w + x] = y === ground && rand() < 0.4 ? lime : green;
  }
  const trunks = [0.14, 0.36, 0.63, 0.86].map((f) => Math.floor(w * f));
  for (const [n, tx] of trunks.entries()) {
    const top = Math.floor(h * (0.2 + (n % 2) * 0.08));
    for (let y = top; y < ground; y++) {
      for (let dx = 0; dx < 2; dx++) {
        const x = tx + dx;
        if (x >= w) continue;
        px[y * w + x] = (y + n * 3) % 7 === 0 && dx === 0 ? black : rand() < 0.15 ? lgrey : white;
      }
    }
    const cy = top - 1;
    const rad = Math.floor(w * 0.09);
    for (let y = cy - rad; y <= cy + rad; y++) {
      for (let x = tx - rad; x <= tx + rad + 1; x++) {
        if (x < 0 || y < 0 || x >= w || y >= ground) continue;
        if (Math.hypot(x - tx - 0.5, (y - cy) * 1.2) <= rad && rand() < 0.85) {
          px[y * w + x] = rand() < 0.2 ? orange : yellow;
        }
      }
    }
  }
  for (let y = 0; y < Math.floor(h * 0.06); y++) {
    for (let x = 0; x < w; x++) if (px[y * w + x] === UNSET && rand() < 0.08) px[y * w + x] = blue;
  }
}

/** A Mulberry32 PRNG: tiny and good enough for a simulation. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function fakeOid(input: string): string {
  return createHash('sha1').update(input).digest('hex');
}

function base32(bytes: Uint8Array): string {
  const alphabet = 'abcdefghijkmnpqrstuvwxyz23456789';
  let out = '';
  for (const b of bytes) out += alphabet[b & 31];
  return out;
}

function window(times: number[], now: number, spanMs: number): number[] {
  return times.filter((t) => t > now - spanMs);
}

function retryAfter(at: number, now: number): number {
  return Math.max(1, Math.ceil((at - now) / 1000));
}

function clamp(v: number, lo: number, hi: number): number {
  return v < lo ? lo : Math.min(v, hi);
}

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

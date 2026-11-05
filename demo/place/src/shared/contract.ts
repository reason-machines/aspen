/**
 * TypeScript types for the `place` HTTP API. `api/openapi.yaml` is the
 * normative contract; these types mirror it, and `test/contract.test.ts`
 * checks the mock's responses against the YAML schemas.
 */
import type { PaletteEntry } from './palette.ts';
import type { Rejection, Tile } from './tiles.ts';

export const API_VERSION = 'v1';

export interface Limits {
  /** Most tiles one push (one stroke) may change. */
  tilesPerPush: number;
  /** Pushes allowed per session per rolling minute. */
  pushesPerMinute: number;
  /** A branch with no pushes for this long is deleted after its tiles merge. */
  branchTtlSeconds: number;
}

export interface Config {
  /** What visitors pass to `git clone`, e.g. `https://place.example.com/canvas.git`. */
  cloneUrl: string;
  /** The live dashboard, or null when there is none yet. */
  dashboardUrl: string | null;
  canvas: { width: number; height: number; palette: PaletteEntry[] };
  limits: Limits;
  /** How often the merger bot folds branches into main. */
  mergeIntervalMs: number;
}

export type ActorKind = 'visitor' | 'agent' | 'git';

export interface Actor {
  kind: ActorKind;
  /** Display name: the branch owner, agent name or Git committer name. */
  label: string;
}

export interface OverlayTile extends Tile {
  /** Sequence number of the push that painted it. */
  seq: number;
}

/** Tiles pushed to a branch that the merger bot has not folded into main yet. */
export interface BranchOverlay {
  name: string;
  actor: Actor;
  head: string;
  tiles: OverlayTile[];
  updatedAt: string;
  expiresAt: string;
}

export interface CanvasSnapshot {
  /** Sequence number of the last event this snapshot includes. */
  seq: number;
  /** Every push with `seq <= mergedThroughSeq` is part of main. */
  mergedThroughSeq: number;
  mainCommit: string;
  width: number;
  height: number;
  /**
   * main's pixels, row-major, one character per pixel: `.` for a tile that
   * was never painted, otherwise the palette index in base 36 (`0`-`9`, `a`-`z`).
   */
  pixels: string;
  branches: BranchOverlay[];
}

export interface BranchSummary {
  name: string;
  actor: Actor;
  head: string;
  pendingTiles: number;
  updatedAt: string;
  expiresAt: string;
}

export interface Quota {
  pushesPerMinute: number;
  remaining: number;
  /** When the oldest push in the window stops counting. */
  resetAt: string;
  tilesPerPush: number;
}

export interface Session {
  branch: string;
  /** Bearer token for `POST /pushes` and `GET /quota`. Keep it private. */
  token: string;
  expiresAt: string;
  quota: Quota;
}

export interface PushRequest {
  branch: string;
  tiles: Tile[];
  message?: string;
}

export interface PushResult {
  seq: number;
  branch: string;
  commit: string;
  parent: string;
  tiles: Tile[];
  quota: Quota;
}

export type SwarmState = 'idle' | 'running' | 'cooldown' | 'disabled';

export interface SwarmStatus {
  state: SwarmState;
  /** Agents pushing right now. */
  agents: number;
  maxAgents: number;
  /** The whole swarm never exceeds this many pushes per second. */
  pushesPerSecondCap: number;
  /** Length of one run. */
  runSeconds: number;
  /** Pushes made in the current or last run. */
  pushes: number;
  startedAt: string | null;
  endsAt: string | null;
  /** When the next run may start; set while `state` is `cooldown`. */
  cooldownUntil: string | null;
  killSwitch: { engaged: boolean; reason: string | null };
}

export interface KillSwitchRequest {
  engaged: boolean;
  reason?: string;
}

export type ErrorCode =
  | 'bad-request'
  | 'unauthorized'
  | 'forbidden'
  | 'not-found'
  | 'rejected'
  | 'rate-limited'
  | 'kill-switch'
  | 'cooldown';

/** Every non-2xx response body. */
export interface ApiErrorBody {
  error: ErrorCode;
  message: string;
  /** Tile-rule rejections, for `error: rejected`. */
  rejections?: Rejection[];
  /** Mirrors the `Retry-After` header, for 429 and 503. */
  retryAfterSeconds?: number;
}

// Server-sent events on GET /events. The SSE `event:` field is the `type`,
// `id:` is `seq`, and `data:` is the JSON object.

export interface PushEvent {
  type: 'push';
  seq: number;
  branch: string;
  actor: Actor;
  commit: string;
  tiles: Tile[];
  at: string;
}

export interface MergeEvent {
  type: 'merge';
  seq: number;
  commit: string;
  /** Every push with `seq <= throughSeq` is now in main. */
  throughSeq: number;
  /** main's new color for every tile the merge changed (last write wins). */
  tiles: Tile[];
  branches: string[];
  at: string;
}

export interface ExpireEvent {
  type: 'expire';
  seq: number;
  branch: string;
  at: string;
}

export interface SwarmEvent {
  type: 'swarm';
  seq: number;
  status: SwarmStatus;
  at: string;
}

/** The client fell too far behind; fetch `GET /canvas` and resubscribe. */
export interface ResyncEvent {
  type: 'resync';
  seq: number;
  at: string;
}

export type PlaceEvent = PushEvent | MergeEvent | ExpireEvent | SwarmEvent | ResyncEvent;
export type PlaceEventType = PlaceEvent['type'];
export const EVENT_TYPES: readonly PlaceEventType[] = ['push', 'merge', 'expire', 'swarm', 'resync'];

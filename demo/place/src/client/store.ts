/**
 * Client-side canvas state: main's pixels plus the unmerged tiles on every
 * live branch. Updates are idempotent and tolerate the one reordering the
 * protocol allows: a push response can arrive before or after the stream
 * delivers the same push, and before or after the merge that includes it.
 */
import type { Actor, CanvasSnapshot, PlaceEvent } from '../shared/contract.ts';
import type { PaletteEntry } from '../shared/palette.ts';
import type { Tile } from '../shared/tiles.ts';

export const UNSET = 255;

export interface OverlayEntry {
  branch: string;
  color: string;
  seq: number;
}

export interface BranchInfo {
  actor: Actor;
  head: string;
  updatedAt: string;
}

export class CanvasStore {
  readonly width: number;
  readonly height: number;
  readonly palette: readonly PaletteEntry[];
  /** Palette index per pixel, or UNSET. */
  readonly main: Uint8Array;
  mainCommit = '';
  /** Every push with `seq <= mergedThrough` is in main. */
  mergedThrough = 0;
  /** Last stream event applied. */
  seq = 0;
  readonly branches = new Map<string, BranchInfo>();
  private readonly overlay = new Map<number, Map<string, { color: string; seq: number }>>();
  private readonly colorIndex: Map<string, number>;
  private dirty = new Set<number>();
  private allDirty = true;

  constructor(width: number, height: number, palette: readonly PaletteEntry[]) {
    this.width = width;
    this.height = height;
    this.palette = palette;
    this.main = new Uint8Array(width * height).fill(UNSET);
    this.colorIndex = new Map(palette.map((p, i) => [p.color, i]));
  }

  loadSnapshot(s: CanvasSnapshot): void {
    if (s.width !== this.width || s.height !== this.height || s.pixels.length !== this.width * this.height) {
      throw new Error(
        `Snapshot is ${s.width}x${s.height} with ${s.pixels.length} pixels; expected ${this.width}x${this.height}`,
      );
    }
    for (let i = 0; i < s.pixels.length; i++) {
      const c = s.pixels[i];
      const v = c === '.' ? UNSET : Number.parseInt(c, 36);
      this.main[i] = Number.isNaN(v) || v >= this.palette.length ? UNSET : v;
    }
    this.mainCommit = s.mainCommit;
    this.mergedThrough = s.mergedThroughSeq;
    this.seq = s.seq;
    this.overlay.clear();
    this.branches.clear();
    for (const b of s.branches) {
      this.branches.set(b.name, { actor: b.actor, head: b.head, updatedAt: b.updatedAt });
      for (const t of b.tiles) this.addOverlay(t, b.name, t.seq);
    }
    this.allDirty = true;
  }

  /** Applies one stream event. Events at or before `seq` are ignored. */
  apply(e: PlaceEvent): void {
    if (e.seq <= this.seq) return;
    this.seq = e.seq;
    switch (e.type) {
      case 'push':
        this.branches.set(e.branch, { actor: e.actor, head: e.commit, updatedAt: e.at });
        for (const t of e.tiles) this.addOverlay(t, e.branch, e.seq);
        break;
      case 'merge':
        for (const t of e.tiles) {
          const i = this.index(t);
          if (i < 0) continue;
          this.main[i] = this.colorIndex.get(t.color) ?? UNSET;
          this.dirty.add(i);
        }
        this.mainCommit = e.commit;
        this.dropMergedThrough(e.throughSeq);
        break;
      case 'expire':
        this.branches.delete(e.branch);
        for (const [i, m] of this.overlay) {
          if (m.delete(e.branch)) this.dirty.add(i);
          if (m.size === 0) this.overlay.delete(i);
        }
        break;
      case 'swarm':
      case 'resync':
        break;
    }
  }

  /**
   * Applies this visitor's own accepted push straight from the HTTP response,
   * so the stroke shows without waiting for the stream to echo it.
   */
  applyOwnPush(branch: string, actor: Actor, commit: string, seq: number, tiles: readonly Tile[]): void {
    const prev = this.branches.get(branch);
    if (!prev || this.seq < seq)
      this.branches.set(branch, { actor, head: commit, updatedAt: new Date().toISOString() });
    for (const t of tiles) this.addOverlay(t, branch, seq);
  }

  /** The newest unmerged tile at `i`, if any branch has one. */
  topAt(i: number): OverlayEntry | null {
    const m = this.overlay.get(i);
    if (!m) return null;
    let best: OverlayEntry | null = null;
    for (const [branch, t] of m) if (!best || t.seq > best.seq) best = { branch, color: t.color, seq: t.seq };
    return best;
  }

  /** main's color at `i`, or null for a tile never painted. */
  mainAt(i: number): string | null {
    const v = this.main[i];
    return v === UNSET ? null : (this.palette[v]?.color ?? null);
  }

  /** Number of tiles waiting on some branch to merge. */
  get pendingTiles(): number {
    return this.overlay.size;
  }

  /** Branches with tiles that have not merged yet. */
  liveBranchCount(): number {
    const names = new Set<string>();
    for (const m of this.overlay.values()) for (const b of m.keys()) names.add(b);
    return names.size;
  }

  /** Pixels changed since the last call, or 'all' after a snapshot. */
  drainDirty(): Set<number> | 'all' {
    if (this.allDirty) {
      this.allDirty = false;
      this.dirty.clear();
      return 'all';
    }
    const d = this.dirty;
    this.dirty = new Set();
    return d;
  }

  index(t: { x: number; y: number }): number {
    if (
      !Number.isInteger(t.x) ||
      !Number.isInteger(t.y) ||
      t.x < 0 ||
      t.y < 0 ||
      t.x >= this.width ||
      t.y >= this.height
    ) {
      return -1;
    }
    return t.y * this.width + t.x;
  }

  private addOverlay(t: Tile, branch: string, seq: number): void {
    if (seq <= this.mergedThrough) return;
    const i = this.index(t);
    if (i < 0) return;
    let m = this.overlay.get(i);
    if (!m) {
      m = new Map();
      this.overlay.set(i, m);
    }
    const prev = m.get(branch);
    if (prev && prev.seq >= seq) return;
    m.set(branch, { color: t.color, seq });
    this.dirty.add(i);
  }

  private dropMergedThrough(throughSeq: number): void {
    if (throughSeq <= this.mergedThrough) return;
    this.mergedThrough = throughSeq;
    for (const [i, m] of this.overlay) {
      for (const [b, t] of m) if (t.seq <= throughSeq) m.delete(b);
      if (m.size === 0) this.overlay.delete(i);
      this.dirty.add(i);
    }
  }
}

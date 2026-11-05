/**
 * Draws the canvas: main from an offscreen 1-pixel-per-tile bitmap scaled up
 * without smoothing, then unmerged branch tiles (hatched), the visitor's own
 * branch (gold inset), the stroke being drawn and the keyboard cursor.
 */
import { type CanvasStore, UNSET } from './store.ts';

export interface RenderState {
  showBranches: boolean;
  ownBranch: string | null;
  /** Tiles in the stroke being drawn, by index, with their color. */
  staged: ReadonlyMap<number, string>;
  /** Highlighted tile (pointer hover or keyboard cursor), or null. */
  cursor: { x: number; y: number } | null;
}

export interface Theme {
  paper: string;
  grid: string;
  ink: string;
  gold: string;
}

export class CanvasRenderer {
  private readonly ctx: CanvasRenderingContext2D;
  private readonly bitmap: HTMLCanvasElement;
  private readonly bctx: CanvasRenderingContext2D;
  private readonly image: ImageData;
  private readonly rgb: Uint8Array;
  private paperRgb: [number, number, number] = [255, 255, 255];
  private hatch: CanvasPattern | null = null;
  private theme: Theme;
  private readonly canvas: HTMLCanvasElement;
  private readonly store: CanvasStore;

  constructor(canvas: HTMLCanvasElement, store: CanvasStore, theme: Theme) {
    this.canvas = canvas;
    this.store = store;
    const ctx = canvas.getContext('2d', { alpha: false });
    if (!ctx) throw new Error('2D canvas is not available');
    this.ctx = ctx;
    this.bitmap = document.createElement('canvas');
    this.bitmap.width = store.width;
    this.bitmap.height = store.height;
    const bctx = this.bitmap.getContext('2d');
    if (!bctx) throw new Error('2D canvas is not available');
    this.bctx = bctx;
    this.image = bctx.createImageData(store.width, store.height);
    this.rgb = new Uint8Array(store.palette.length * 3);
    store.palette.forEach((p, i) => {
      const [r, g, b] = hexToRgb(p.color);
      this.rgb.set([r, g, b], i * 3);
    });
    this.theme = theme;
    this.setTheme(theme);
  }

  setTheme(theme: Theme): void {
    this.theme = theme;
    this.paperRgb = hexToRgb(theme.paper);
    this.hatch = this.makeHatch();
    this.refreshBitmap('all');
  }

  /** Sets the backing store to the element's size in device pixels. */
  resize(cssWidth: number, cssHeight: number, dpr: number): void {
    const w = Math.max(1, Math.round(cssWidth * dpr));
    const h = Math.max(1, Math.round(cssHeight * dpr));
    if (this.canvas.width !== w || this.canvas.height !== h) {
      this.canvas.width = w;
      this.canvas.height = h;
      this.hatch = this.makeHatch();
    }
  }

  draw(state: RenderState): void {
    this.refreshBitmap(this.store.drainDirty());
    const { ctx, store } = this;
    const W = this.canvas.width;
    const H = this.canvas.height;
    const cw = W / store.width;
    const ch = H / store.height;
    const x0 = (x: number) => Math.round(x * cw);
    const y0 = (y: number) => Math.round(y * ch);

    ctx.imageSmoothingEnabled = false;
    ctx.drawImage(this.bitmap, 0, 0, W, H);

    if (cw >= 9) {
      ctx.strokeStyle = this.theme.grid;
      ctx.lineWidth = 1;
      ctx.beginPath();
      for (let x = 1; x < store.width; x++) {
        const px = x0(x) + 0.5;
        ctx.moveTo(px, 0);
        ctx.lineTo(px, H);
      }
      for (let y = 1; y < store.height; y++) {
        const py = y0(y) + 0.5;
        ctx.moveTo(0, py);
        ctx.lineTo(W, py);
      }
      ctx.stroke();
    }

    const inset = Math.max(1, Math.round(cw * 0.14));
    if (state.showBranches || state.ownBranch) {
      for (let i = 0; i < store.width * store.height; i++) {
        const top = store.topAt(i);
        if (!top) continue;
        const own = top.branch === state.ownBranch;
        if (!own && !state.showBranches) continue;
        const x = i % store.width;
        const y = (i - x) / store.width;
        const l = x0(x);
        const t = y0(y);
        const w = x0(x + 1) - l;
        const h = y0(y + 1) - t;
        ctx.fillStyle = top.color;
        ctx.fillRect(l, t, w, h);
        if (this.hatch) {
          ctx.fillStyle = this.hatch;
          ctx.fillRect(l, t, w, h);
        }
        if (own && w > 3) {
          ctx.strokeStyle = this.theme.gold;
          ctx.lineWidth = inset;
          ctx.strokeRect(l + inset / 2, t + inset / 2, w - inset, h - inset);
        }
      }
    }

    for (const [i, color] of state.staged) {
      const x = i % store.width;
      const y = (i - x) / store.width;
      const l = x0(x);
      const t = y0(y);
      const w = x0(x + 1) - l;
      const h = y0(y + 1) - t;
      ctx.fillStyle = color;
      ctx.fillRect(l, t, w, h);
      if (w > 3) {
        ctx.strokeStyle = this.theme.ink;
        ctx.lineWidth = Math.max(1, Math.round(inset / 2));
        ctx.setLineDash([Math.max(2, inset), Math.max(2, inset)]);
        ctx.strokeRect(l + 0.5, t + 0.5, w - 1, h - 1);
        ctx.setLineDash([]);
      }
    }

    if (state.cursor) {
      const { x, y } = state.cursor;
      const l = x0(x);
      const t = y0(y);
      const w = x0(x + 1) - l;
      const h = y0(y + 1) - t;
      const lw = Math.max(2, Math.round(Math.min(cw, 24) * 0.12));
      ctx.lineWidth = lw + 2;
      ctx.strokeStyle = '#ffffff';
      ctx.strokeRect(l - lw / 2, t - lw / 2, w + lw, h + lw);
      ctx.lineWidth = lw;
      ctx.strokeStyle = this.theme.ink;
      ctx.strokeRect(l - lw / 2, t - lw / 2, w + lw, h + lw);
    }
  }

  private refreshBitmap(dirty: Set<number> | 'all'): void {
    const { data } = this.image;
    const { main } = this.store;
    const [pr, pg, pb] = this.paperRgb;
    const paint = (i: number) => {
      const v = main[i];
      const o = i * 4;
      if (v === UNSET) {
        // A faint checker marks tiles nobody has painted yet.
        const x = i % this.store.width;
        const y = (i - x) / this.store.width;
        const d = (x + y) % 2 === 0 ? 0 : 7;
        data[o] = pr - d;
        data[o + 1] = pg - d;
        data[o + 2] = pb - d;
      } else {
        data[o] = this.rgb[v * 3];
        data[o + 1] = this.rgb[v * 3 + 1];
        data[o + 2] = this.rgb[v * 3 + 2];
      }
      data[o + 3] = 255;
    };
    if (dirty === 'all') for (let i = 0; i < main.length; i++) paint(i);
    else {
      if (dirty.size === 0) return;
      for (const i of dirty) paint(i);
    }
    this.bctx.putImageData(this.image, 0, 0);
  }

  private makeHatch(): CanvasPattern | null {
    const size = Math.max(6, Math.round(this.canvas.width / this.store.width / 2.5));
    const tile = document.createElement('canvas');
    tile.width = size;
    tile.height = size;
    const t = tile.getContext('2d');
    if (!t) return null;
    t.lineWidth = Math.max(1, size / 5);
    t.strokeStyle = 'rgba(255,255,255,0.55)';
    stripe(t, size, 0);
    t.strokeStyle = 'rgba(0,0,0,0.22)';
    stripe(t, size, size / 2);
    return this.ctx.createPattern(tile, 'repeat');
  }
}

function stripe(t: CanvasRenderingContext2D, size: number, offset: number): void {
  t.beginPath();
  for (const k of [-size, 0, size]) {
    t.moveTo(k + offset, size);
    t.lineTo(k + offset + size, 0);
  }
  t.stroke();
}

function hexToRgb(hex: string): [number, number, number] {
  const m = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex.trim());
  if (!m) return [255, 255, 255];
  return [Number.parseInt(m[1], 16), Number.parseInt(m[2], 16), Number.parseInt(m[3], 16)];
}

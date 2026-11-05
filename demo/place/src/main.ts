/**
 * The `place` page: wires the API client, canvas store, renderer, painting,
 * the event feed and the swarm control together. All untrusted strings
 * (branch names, reasons, messages) are written with textContent.
 */
import { ApiError, createPlaceClient, type PlaceClient, type StreamState } from './client/api.ts';
import { type FeedItem, FeedModel } from './client/feed.ts';
import { cellAt, formatDuration, lineCells, RateMeter, shellQuote, shortSha } from './client/geometry.ts';
import { CanvasRenderer, type Theme } from './client/render.ts';
import { CanvasStore } from './client/store.ts';
import type { Config, PlaceEvent, Quota, Session, SwarmStatus } from './shared/contract.ts';
import { serializeTile, type Tile, tilePath } from './shared/tiles.ts';

const API_BASE = (import.meta.env.VITE_PLACE_API as string | undefined) ?? '/api/v1';
const SESSION_KEY = 'place.session.v1';
const FEED_FLUSH_MS = 250;

const $ = <T extends HTMLElement = HTMLElement>(id: string) => {
  const el = document.getElementById(id);
  if (!el) throw new Error(`#${id} missing from index.html`);
  return el as T;
};

function h<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props: Partial<Record<string, string>> = {},
  ...children: (Node | string | null)[]
): HTMLElementTagNameMap[K] {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) if (v !== undefined) el.setAttribute(k, v);
  for (const c of children) if (c !== null) el.append(c);
  return el;
}

function readTheme(): Theme {
  const s = getComputedStyle(document.documentElement);
  const v = (name: string, fallback: string) => s.getPropertyValue(name).trim() || fallback;
  return {
    paper: v('--paper', '#f6f7f3'),
    grid: v('--grid', 'rgba(0,0,0,0.06)'),
    ink: v('--ink', '#1c2b27'),
    gold: v('--gold', '#e3a21a'),
  };
}

async function boot(): Promise<void> {
  const client = createPlaceClient({ baseUrl: API_BASE });
  let config: Config;
  try {
    config = await client.config();
  } catch (err) {
    $('loading').textContent =
      `The canvas could not load: ${err instanceof Error ? err.message : String(err)}. Reload to try again.`;
    return;
  }
  const app = new App(client, config);
  await app.start();
}

class App {
  private readonly store: CanvasStore;
  private readonly renderer: CanvasRenderer;
  private readonly feed: FeedModel;
  private readonly rate = new RateMeter(5000);
  private readonly canvasEl = $<HTMLCanvasElement>('canvas');
  private session: Session | null = null;
  private quota: Quota | null = null;
  private quotaBlockedUntil = 0;
  private color: string;
  private showBranches = true;
  private zoom = 1;
  private tool: 'brush' | 'move' = 'brush';
  private hover: { x: number; y: number } | null = null;
  private kbd: { x: number; y: number } | null = null;
  private staged = new Map<number, string>();
  private painting: { pointerId: number; last: { x: number; y: number } | null } | null = null;
  private panning: { pointerId: number; sx: number; sy: number; left: number; top: number } | null = null;
  private needsDraw = true;
  private readoutKey = '';
  private feedDirty = false;
  private pushSeqsSinceMerge: number[] = [];
  private lastMergeAt: number | null = null;
  private swarm: SwarmStatus | null = null;
  private swarmNote: { text: string; until: number } | null = null;

  private readonly client: PlaceClient;
  private readonly config: Config;

  constructor(client: PlaceClient, config: Config) {
    this.client = client;
    this.config = config;
    const { width, height, palette } = config.canvas;
    this.store = new CanvasStore(width, height, palette);
    this.renderer = new CanvasRenderer(this.canvasEl, this.store, readTheme());
    this.feed = new FeedModel(80, () => this.session?.branch ?? null);
    this.color = palette.find((p) => p.color === '#d6262b')?.color ?? palette[0].color;
  }

  async start(): Promise<void> {
    this.renderStaticText();
    this.renderPalette();
    this.renderRulers();
    this.bindView();
    this.bindPainting();
    this.bindSwarm();
    this.layout();
    new ResizeObserver(() => this.layout()).observe($('plat'));
    matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
      this.renderer.setTheme(readTheme());
      this.needsDraw = true;
    });

    const snap = await this.client.canvas();
    this.store.loadSnapshot(snap);
    this.feed.reset(snap.mergedThroughSeq);
    $('loading').hidden = true;
    this.needsDraw = true;
    this.updateGitSteps();
    this.frame();
    setInterval(() => this.tickUi(), 500);
    setInterval(() => this.flushFeed(), FEED_FLUSH_MS);

    void this.ensureSession();
    void this.client
      .swarm()
      .then((s) => this.setSwarm(s))
      .catch(() => this.setSwarmText('The load test is unavailable right now.'));
    void this.client.events({
      after: snap.seq,
      signal: new AbortController().signal,
      onEvent: (e) => this.onEvent(e),
      onResync: async () => {
        const s = await this.client.canvas();
        this.store.loadSnapshot(s);
        this.pushSeqsSinceMerge = [];
        this.needsDraw = true;
        return s.seq;
      },
      onState: (s) => this.setConn(s),
    });
  }

  // Events --------------------------------------------------------------

  private onEvent(e: PlaceEvent): void {
    if (e.seq <= this.store.seq && e.type !== 'swarm') return;
    let merged = 0;
    if (e.type === 'push') {
      this.rate.add(Date.now());
      this.pushSeqsSinceMerge.push(e.seq);
    } else if (e.type === 'merge') {
      const keep = this.pushSeqsSinceMerge.filter((s) => s > e.throughSeq);
      merged = this.pushSeqsSinceMerge.length - keep.length;
      this.pushSeqsSinceMerge = keep;
      this.lastMergeAt = Date.now();
    } else if (e.type === 'swarm') {
      this.setSwarm(e.status);
    }
    this.store.apply(e);
    this.feed.add(e, merged);
    this.feedDirty = true;
    this.needsDraw = true;
  }

  private setConn(s: StreamState): void {
    const el = $('conn');
    el.dataset.state = s.state;
    $('conn-text').textContent =
      s.state === 'open'
        ? 'Live'
        : s.state === 'connecting'
          ? 'Connecting'
          : `Reconnecting in ${formatDuration(s.inMs)}`;
  }

  // Session and quota ---------------------------------------------------

  private async ensureSession(): Promise<Session | null> {
    if (this.session) return this.session;
    const saved = loadSession();
    if (saved && Date.parse(saved.expiresAt) > Date.now()) {
      try {
        const q = await this.client.quota(saved.token);
        this.setSession({ ...saved, quota: q });
        return this.session;
      } catch {
        // Unknown token (for example, the server restarted): make a new one.
      }
    }
    try {
      const s = await this.client.createSession();
      this.setSession(s);
      return s;
    } catch (err) {
      $('own-branch').textContent = 'unavailable';
      this.setPaintStatus(errorText(err, 'Could not create your branch'), 'error');
      return null;
    }
  }

  private setSession(s: Session): void {
    this.session = s;
    sessionStorage.setItem(SESSION_KEY, JSON.stringify({ branch: s.branch, token: s.token, expiresAt: s.expiresAt }));
    $('own-branch').textContent = s.branch;
    this.setQuota(s.quota);
    this.updateGitSteps();
  }

  private setQuota(q: Quota): void {
    this.quota = q;
    this.renderQuota();
  }

  private renderQuota(): void {
    const q = this.quota;
    if (!q) return;
    const blocked = q.remaining <= 0 || Date.now() < this.quotaBlockedUntil;
    const resetIn = Math.max(Date.parse(q.resetAt), this.quotaBlockedUntil) - Date.now();
    $('quota-fill').style.width = `${(100 * Math.max(0, q.remaining)) / q.pushesPerMinute}%`;
    $('quota').dataset.blocked = String(blocked);
    $('quota-text').textContent = blocked
      ? `Out of pushes for now. The next one frees up in ${formatDuration(resetIn)}.`
      : `${q.remaining} of ${q.pushesPerMinute} pushes left this minute, up to ${q.tilesPerPush} tiles each.`;
    this.updatePushButton();
  }

  // Painting ------------------------------------------------------------

  private bindPainting(): void {
    const c = this.canvasEl;
    c.addEventListener('pointerdown', (e) => {
      if (e.button !== 0) return;
      if (this.tool === 'move') {
        if (e.pointerType === 'mouse') {
          const plat = $('plat');
          this.panning = {
            pointerId: e.pointerId,
            sx: e.clientX,
            sy: e.clientY,
            left: plat.scrollLeft,
            top: plat.scrollTop,
          };
          c.setPointerCapture(e.pointerId);
        }
        return;
      }
      e.preventDefault();
      c.setPointerCapture(e.pointerId);
      this.painting = { pointerId: e.pointerId, last: null };
      this.kbd = null;
      this.extendStroke(this.cellFromEvent(e));
    });
    c.addEventListener('pointermove', (e) => {
      if (this.panning && e.pointerId === this.panning.pointerId) {
        const plat = $('plat');
        plat.scrollLeft = this.panning.left - (e.clientX - this.panning.sx);
        plat.scrollTop = this.panning.top - (e.clientY - this.panning.sy);
        return;
      }
      const cell = this.cellFromEvent(e);
      if (this.painting && e.pointerId === this.painting.pointerId) {
        for (const ev of e.getCoalescedEvents?.() ?? [e]) this.extendStroke(this.cellFromEvent(ev));
      }
      if (e.pointerType === 'mouse') this.setHover(cell);
    });
    const end = (e: PointerEvent) => {
      if (this.panning && e.pointerId === this.panning.pointerId) this.panning = null;
      if (this.painting && e.pointerId === this.painting.pointerId) {
        this.painting = null;
        if (e.type === 'pointerup') void this.pushStroke();
      }
    };
    c.addEventListener('pointerup', end);
    c.addEventListener('pointercancel', end);
    c.addEventListener('pointerleave', (e) => {
      if (e.pointerType === 'mouse' && !this.painting) this.setHover(null);
    });
    c.addEventListener('keydown', (e) => this.onCanvasKey(e));
    c.addEventListener('focus', () => {
      if (!this.painting && !this.kbd) {
        this.kbd = this.hover ?? { x: Math.floor(this.store.width / 2), y: Math.floor(this.store.height / 2) };
        this.setHover(this.kbd);
      }
    });
    c.addEventListener('blur', () => {
      this.kbd = null;
    });
    $('push-btn').addEventListener('click', () => void this.pushStroke());
  }

  private onCanvasKey(e: KeyboardEvent): void {
    const step = e.shiftKey ? 8 : 1;
    const k = this.kbd ?? { x: 0, y: 0 };
    const move = (dx: number, dy: number) => {
      this.kbd = {
        x: Math.min(this.store.width - 1, Math.max(0, k.x + dx)),
        y: Math.min(this.store.height - 1, Math.max(0, k.y + dy)),
      };
      this.setHover(this.kbd);
      this.scrollCellIntoView(this.kbd);
    };
    switch (e.key) {
      case 'ArrowLeft':
        move(-step, 0);
        break;
      case 'ArrowRight':
        move(step, 0);
        break;
      case 'ArrowUp':
        move(0, -step);
        break;
      case 'ArrowDown':
        move(0, step);
        break;
      case ' ': {
        const i = this.store.index(k);
        if (this.staged.has(i)) this.staged.delete(i);
        else if (this.staged.size < this.config.limits.tilesPerPush) this.staged.set(i, this.color);
        this.onStagedChanged();
        break;
      }
      case 'Enter':
        void this.pushStroke();
        break;
      case 'Escape':
        this.staged.clear();
        this.onStagedChanged();
        break;
      default:
        return;
    }
    e.preventDefault();
  }

  private extendStroke(cell: { x: number; y: number } | null): void {
    if (!this.painting || !cell) return;
    const from = this.painting.last ?? cell;
    for (const [x, y] of lineCells(from.x, from.y, cell.x, cell.y)) {
      if (this.staged.size >= this.config.limits.tilesPerPush) break;
      this.staged.set(this.store.index({ x, y }), this.color);
    }
    this.painting.last = cell;
    this.setHover(cell);
    this.onStagedChanged();
  }

  private onStagedChanged(): void {
    const n = this.staged.size;
    const max = this.config.limits.tilesPerPush;
    $('stroke-count').textContent =
      n === 0
        ? 'Draw on the canvas. Each stroke is pushed when you lift the brush.'
        : n >= max
          ? `${n} tiles staged, the most one push can carry.`
          : `${n} ${n === 1 ? 'tile' : 'tiles'} staged.`;
    this.updatePushButton();
    this.needsDraw = true;
  }

  private updatePushButton(): void {
    const btn = $<HTMLButtonElement>('push-btn');
    btn.disabled = this.staged.size === 0 || !this.session;
  }

  private async pushStroke(retried = false): Promise<void> {
    if (this.staged.size === 0) return;
    const session = await this.ensureSession();
    if (!session) return;
    if (this.quota && (this.quota.remaining <= 0 || Date.now() < this.quotaBlockedUntil)) {
      this.setPaintStatus(
        'You are out of pushes for this minute. Your stroke is still staged; push it when the quota frees up.',
        'error',
      );
      return;
    }
    const staged = this.staged;
    const tiles: Tile[] = [...staged].map(([i, color]) => ({
      x: i % this.store.width,
      y: Math.floor(i / this.store.width),
      color,
    }));
    this.staged = new Map();
    this.onStagedChanged();
    this.setPaintStatus(
      `Pushing ${tiles.length} ${tiles.length === 1 ? 'tile' : 'tiles'} to ${session.branch}…`,
      'busy',
    );
    try {
      const r = await this.client.push(session.token, { branch: session.branch, tiles });
      this.store.applyOwnPush(session.branch, { kind: 'visitor', label: session.branch }, r.commit, r.seq, r.tiles);
      this.setQuota(r.quota);
      this.needsDraw = true;
      this.setPaintStatus(
        `Pushed ${shortSha(r.commit)} with ${r.tiles.length} ${r.tiles.length === 1 ? 'tile' : 'tiles'}. It joins main at the next merge.`,
        'ok',
      );
    } catch (err) {
      if (err instanceof ApiError && err.status === 401 && !retried) {
        this.session = null;
        sessionStorage.removeItem(SESSION_KEY);
        this.staged = staged;
        await this.pushStroke(true);
        return;
      }
      if (err instanceof ApiError && err.status === 429) {
        this.quotaBlockedUntil = Date.now() + (err.retryAfterSeconds ?? 10) * 1000;
        this.renderQuota();
      }
      // Keep the stroke so the visitor can retry after fixing the problem.
      if (!(err instanceof ApiError && err.status === 422)) {
        for (const [i, c] of staged) if (!this.staged.has(i)) this.staged.set(i, c);
        this.onStagedChanged();
      }
      this.setPaintStatus(errorText(err, 'The push failed'), 'error');
    }
  }

  private cellFromEvent(e: PointerEvent): { x: number; y: number } | null {
    const r = this.canvasEl.getBoundingClientRect();
    return cellAt(e.clientX - r.left, e.clientY - r.top, r.width, r.height, this.store.width, this.store.height);
  }

  private setHover(cell: { x: number; y: number } | null): void {
    const same = cell && this.hover && cell.x === this.hover.x && cell.y === this.hover.y;
    if (same || (!cell && !this.hover)) return;
    this.hover = cell;
    this.needsDraw = true;
    this.renderReadout();
    this.renderRulerMarks();
    this.updateGitSteps();
  }

  private renderReadout(): void {
    const el = $('cursor-readout');
    const c = this.hover;
    if (!c) {
      this.readoutKey = '';
      el.textContent = 'Point at a tile to see its file.';
      return;
    }
    const i = this.store.index(c);
    const top = this.store.topAt(i);
    const main = this.store.mainAt(i);
    // Only touch the live region when the sentence changes.
    const key = `${i}|${main}|${top?.branch}|${top?.color}`;
    if (key === this.readoutKey) return;
    this.readoutKey = key;
    const name = (hex: string | null) =>
      hex ? (this.config.canvas.palette.find((p) => p.color === hex)?.name ?? hex) : 'unpainted';
    const parts: (Node | string)[] = [h('code', {}, tilePath(c.x, c.y)), ` is ${name(main).toLowerCase()} on main`];
    if (top) parts.push(`, ${name(top.color).toLowerCase()} on `, h('code', {}, top.branch));
    parts.push('.');
    el.replaceChildren(...parts);
  }

  private scrollCellIntoView(c: { x: number; y: number }): void {
    if (this.zoom === 1) return;
    const plat = $('plat');
    const r = this.canvasEl.getBoundingClientRect();
    const pr = plat.getBoundingClientRect();
    const cx = r.left + ((c.x + 0.5) / this.store.width) * r.width;
    const cy = r.top + ((c.y + 0.5) / this.store.height) * r.height;
    const margin = 40;
    if (cx < pr.left + margin) plat.scrollLeft -= pr.left + margin - cx;
    if (cx > pr.right - margin) plat.scrollLeft += cx - (pr.right - margin);
    if (cy < pr.top + margin) plat.scrollTop -= pr.top + margin - cy;
    if (cy > pr.bottom - margin) plat.scrollTop += cy - (pr.bottom - margin);
  }

  // Static and layout ---------------------------------------------------

  private renderStaticText(): void {
    const { config } = this;
    $('dashboard-slot').replaceChildren(
      config.dashboardUrl
        ? h('a', { class: 'dash-link', href: config.dashboardUrl }, 'Live dashboard')
        : h('span', { class: 'dash-none' }, 'Dashboard not connected yet'),
    );
    $('merge-every').textContent = String(Math.round(config.mergeIntervalMs / 100) / 10);
    $('rule-range').textContent = `x from 0 to ${config.canvas.width - 1} and y from 0 to ${config.canvas.height - 1}`;
    $('rule-tiles').textContent = String(config.limits.tilesPerPush);
    document.documentElement.style.setProperty('--cols', String(config.canvas.width));
    document.documentElement.style.setProperty('--rows', String(config.canvas.height));
  }

  private renderPalette(): void {
    const swatches = this.config.canvas.palette.map((p) => {
      const input = h('input', {
        type: 'radio',
        name: 'color',
        value: p.color,
        'aria-label': p.name,
      });
      input.checked = p.color === this.color;
      input.addEventListener('change', () => {
        if (!input.checked) return;
        this.color = p.color;
        this.updateGitSteps();
      });
      const label = h('label', { class: 'swatch', title: `${p.name} ${p.color}` }, input);
      label.style.setProperty('--swatch', p.color);
      return label;
    });
    $('palette').replaceChildren(...swatches);
  }

  private renderRulers(): void {
    const { width, height } = this.config.canvas;
    const step = (n: number) => (n <= 32 ? 4 : n <= 128 ? 8 : n <= 512 ? 32 : 128);
    const ticks = (n: number, axis: 'x' | 'y') => {
      const out: HTMLElement[] = [];
      for (let v = 0; v < n; v += step(n)) {
        const t = h('span', { class: 'tick' }, String(v));
        t.style.setProperty(axis === 'x' ? 'left' : 'top', `${((v + 0.5) / n) * 100}%`);
        out.push(t);
      }
      const mark = h('span', { class: 'mark', hidden: '' });
      mark.style.setProperty(axis === 'x' ? 'width' : 'height', `${100 / n}%`);
      out.push(mark);
      return out;
    };
    $('ruler-x').replaceChildren(...ticks(width, 'x'));
    $('ruler-y').replaceChildren(...ticks(height, 'y'));
  }

  private renderRulerMarks(): void {
    const c = this.hover;
    const mx = $('ruler-x').querySelector<HTMLElement>('.mark');
    const my = $('ruler-y').querySelector<HTMLElement>('.mark');
    if (!mx || !my) return;
    mx.hidden = my.hidden = !c;
    if (!c) return;
    mx.style.left = `${(c.x / this.store.width) * 100}%`;
    mx.textContent = String(c.x);
    my.style.top = `${(c.y / this.store.height) * 100}%`;
    my.textContent = String(c.y);
  }

  private bindView(): void {
    $<HTMLInputElement>('show-branches').addEventListener('change', (e) => {
      this.showBranches = (e.target as HTMLInputElement).checked;
      this.needsDraw = true;
    });
    const radios = (name: string, onPick: (value: string) => void) => {
      for (const input of document.querySelectorAll<HTMLInputElement>(`input[name="${name}"]`)) {
        input.addEventListener('change', () => {
          if (input.checked) onPick(input.value);
        });
      }
    };
    radios('zoom', (v) => {
      this.zoom = Number(v);
      $('tool-group').hidden = this.zoom === 1;
      if (this.zoom === 1) this.setTool('brush');
      this.layout();
    });
    radios('tool', (v) => this.setTool(v === 'move' ? 'move' : 'brush'));
  }

  private setTool(tool: 'brush' | 'move'): void {
    this.tool = tool;
    this.canvasEl.dataset.tool = tool;
    for (const input of document.querySelectorAll<HTMLInputElement>('input[name="tool"]')) {
      input.checked = input.value === tool;
    }
  }

  private layout(): void {
    const plat = $('plat');
    const gutter = 26;
    const { width: cols, height: rows } = this.config.canvas;
    const wide = matchMedia('(min-width: 900px)').matches;
    const availW = plat.clientWidth - gutter;
    const availH = wide ? window.innerHeight - 230 : Number.POSITIVE_INFINITY;
    const fitTile = Math.max(2, Math.min(availW / cols, availH / rows));
    const tile = fitTile * this.zoom;
    const w = Math.floor(tile * cols);
    const hgt = Math.floor(tile * rows);
    plat.style.setProperty('--frame-w', `${w}px`);
    plat.style.setProperty('--frame-h', `${hgt}px`);
    plat.dataset.zoomed = String(this.zoom > 1);
    plat.style.setProperty('--fit-h', `${Math.floor(fitTile * rows) + gutter}px`);
    this.renderer.resize(w, hgt, window.devicePixelRatio || 1);
    this.needsDraw = true;
  }

  private updateGitSteps(): void {
    const url = this.config.cloneUrl;
    const dir =
      url
        .replace(/\/+$/, '')
        .split('/')
        .pop()
        ?.replace(/\.git$/, '') || 'canvas';
    const c = this.hover ?? { x: Math.floor(this.store.width / 2), y: Math.floor(this.store.height / 2) };
    const path = tilePath(c.x, c.y);
    const content = serializeTile(this.color).replace('\n', '\\n');
    const steps: [string, string][] = [
      ['Clone the canvas', `git clone ${shellQuote(url)}\ncd ${shellQuote(dir)}`],
      ['Start a branch of your own', 'git switch -c paint/your-name'],
      ['Paint a tile with the color you picked', `printf '${content}' > ${path}`],
      [
        'Commit and push it',
        `git add ${path}\ngit commit -m ${shellQuote(`Paint ${c.x},${c.y}`)}\ngit push origin HEAD`,
      ],
    ];
    const list = $('git-steps');
    const existing = list.querySelectorAll('code');
    if (existing.length === steps.length) {
      steps.forEach(([, cmd], i) => {
        if (existing[i].textContent !== cmd) existing[i].textContent = cmd;
      });
      return;
    }
    list.replaceChildren(
      ...steps.map(([label, cmd]) => {
        const code = h('code', {}, cmd);
        const copy = h('button', { type: 'button', class: 'copy', 'aria-label': `Copy: ${label}` }, 'Copy');
        copy.addEventListener('click', async () => {
          try {
            await navigator.clipboard.writeText(code.textContent ?? '');
            copy.textContent = 'Copied';
          } catch {
            copy.textContent = 'Select and copy';
          }
          setTimeout(() => {
            copy.textContent = 'Copy';
          }, 1500);
        });
        return h(
          'li',
          {},
          h('p', { class: 'step-label' }, label),
          h('div', { class: 'cmd' }, h('pre', {}, code), copy),
        );
      }),
    );
  }

  // Swarm ---------------------------------------------------------------

  private bindSwarm(): void {
    $('swarm-btn').addEventListener('click', async () => {
      const btn = $<HTMLButtonElement>('swarm-btn');
      btn.disabled = true;
      try {
        this.setSwarm(await this.client.startSwarm());
      } catch (err) {
        if (
          err instanceof ApiError &&
          (err.code === 'rate-limited' || err.code === 'cooldown' || err.code === 'kill-switch')
        ) {
          this.swarmNote = { text: err.message, until: Date.now() + (err.retryAfterSeconds ?? 30) * 1000 };
          void this.client.swarm().then((s) => this.setSwarm(s));
        } else {
          this.swarmNote = { text: errorText(err, 'Could not start the swarm'), until: Date.now() + 10_000 };
        }
        this.renderSwarm();
      }
    });
  }

  private setSwarm(s: SwarmStatus): void {
    this.swarm = s;
    this.renderSwarm();
  }

  private setSwarmText(t: string): void {
    $('swarm-state').textContent = t;
  }

  private renderSwarm(): void {
    const s = this.swarm;
    if (!s) return;
    const now = Date.now();
    const panel = $('swarm-panel');
    const btn = $<HTMLButtonElement>('swarm-btn');
    const n = (v: number) => v.toLocaleString('en-US');
    const noted = this.swarmNote && now < this.swarmNote.until ? this.swarmNote : null;
    panel.dataset.state = s.state;
    btn.textContent = s.state === 'running' ? 'Swarm running' : `Unleash ${n(s.maxAgents)} agents`;
    btn.disabled = s.state !== 'idle' || noted !== null;
    let text: string;
    switch (s.state) {
      case 'running':
        text = `${n(s.agents)} of ${n(s.maxAgents)} agents pushing. ${n(s.pushes)} pushes so far, ending in ${formatDuration(Date.parse(s.endsAt ?? '') - now)}.`;
        break;
      case 'cooldown':
        text = `The last run made ${n(s.pushes)} pushes. The next run can start in ${formatDuration(Date.parse(s.cooldownUntil ?? '') - now)}.`;
        break;
      case 'disabled':
        text = `Paused by the operators${s.killSwitch.reason ? `: ${s.killSwitch.reason}` : '.'}`;
        break;
      default:
        text = `Each agent pushes to its own branch for ${s.runSeconds} s. Watch the merge keep up.`;
    }
    if (noted && s.state !== 'disabled') text = noted.text;
    this.setSwarmText(text);
    $('swarm-limits').replaceChildren(
      `Capped at ${n(s.pushesPerSecondCap)} pushes/s in total. Kill switch `,
      h('b', { class: s.killSwitch.engaged ? 'ks-on' : 'ks-off' }, s.killSwitch.engaged ? 'engaged' : 'off'),
      '.',
    );
    panel.style.setProperty(
      '--progress',
      s.state === 'running' && s.startedAt && s.endsAt
        ? String(Math.min(1, (now - Date.parse(s.startedAt)) / (Date.parse(s.endsAt) - Date.parse(s.startedAt))))
        : '0',
    );
  }

  // Feed and stats ------------------------------------------------------

  private flushFeed(): void {
    if (!this.feedDirty) return;
    this.feedDirty = false;
    const now = Date.now();
    const list = $('feed');
    list.replaceChildren(...this.feed.list.slice(0, 40).map((it) => this.feedRow(it, now)));
  }

  private feedRow(it: FeedItem, now: number): HTMLElement {
    const ago = h('time', { datetime: it.at }, formatDuration(now - Date.parse(it.at)));
    switch (it.kind) {
      case 'push': {
        const sw = h('span', { class: 'swatches', 'aria-hidden': 'true' });
        for (const c of it.colors) {
          const s = h('i');
          s.style.background = c;
          sw.append(s);
        }
        const who = it.own ? 'you' : it.actor === 'git' ? 'git' : it.actor === 'agent' ? 'agent' : 'visitor';
        return h(
          'li',
          { class: `row row-push actor-${who}` },
          sw,
          h(
            'span',
            { class: 'what' },
            h('code', { class: 'br' }, it.branch),
            ` ${it.tiles} ${it.tiles === 1 ? 'tile' : 'tiles'}`,
          ),
          h('code', { class: 'sha' }, shortSha(it.commit)),
          ago,
        );
      }
      case 'swarm-burst':
        return h(
          'li',
          { class: 'row row-burst' },
          h('span', { class: 'swatches burst', 'aria-hidden': 'true' }),
          h(
            'span',
            { class: 'what' },
            `Swarm: ${it.pushes.toLocaleString('en-US')} pushes from ${it.branches.toLocaleString('en-US')} branches`,
          ),
          h('span', { class: 'sha' }),
          ago,
        );
      case 'merge':
        return h(
          'li',
          { class: 'row row-merge' },
          h('span', { class: 'swatches merge', 'aria-hidden': 'true' }),
          h(
            'span',
            { class: 'what' },
            'Merged into ',
            h('code', {}, 'main'),
            `: ${it.pushes.toLocaleString('en-US')} ${it.pushes === 1 ? 'push' : 'pushes'} from ${it.branches} ${it.branches === 1 ? 'branch' : 'branches'}`,
          ),
          h('code', { class: 'sha' }, shortSha(it.commit)),
          ago,
        );
      case 'swarm-state':
        return h(
          'li',
          { class: 'row row-note' },
          h('span', { class: 'swatches note', 'aria-hidden': 'true' }),
          h('span', { class: 'what' }, it.text),
          h('span', { class: 'sha' }),
          ago,
        );
    }
  }

  private tickUi(): void {
    const now = Date.now();
    $('stat-rate').textContent = String(Math.round(this.rate.perSecond(now)));
    $('stat-branches').textContent = this.store.liveBranchCount().toLocaleString('en-US');
    $('stat-pending').textContent = this.store.pendingTiles.toLocaleString('en-US');
    $('stat-main').textContent = shortSha(this.store.mainCommit);
    $('stat-merge').textContent =
      this.lastMergeAt === null
        ? 'waiting'
        : `${formatDuration(now - this.lastMergeAt)} ago`.replace('now ago', 'just now');
    this.renderQuota();
    this.renderSwarm();
    this.feedDirty = true;
  }

  private frame = (): void => {
    if (this.needsDraw) {
      this.needsDraw = false;
      this.renderer.draw({
        showBranches: this.showBranches,
        ownBranch: this.session?.branch ?? null,
        staged: this.staged,
        cursor: this.hover,
      });
      if (this.hover) this.renderReadout();
    }
    requestAnimationFrame(this.frame);
  };

  private setPaintStatus(text: string, kind: 'ok' | 'error' | 'busy'): void {
    const el = $('paint-status');
    el.dataset.kind = kind;
    el.textContent = text;
  }
}

function loadSession(): Pick<Session, 'branch' | 'token' | 'expiresAt'> | null {
  try {
    const raw = sessionStorage.getItem(SESSION_KEY);
    if (!raw) return null;
    const v = JSON.parse(raw) as Record<string, unknown>;
    return typeof v.branch === 'string' && typeof v.token === 'string' && typeof v.expiresAt === 'string'
      ? { branch: v.branch, token: v.token, expiresAt: v.expiresAt }
      : null;
  } catch {
    return null;
  }
}

function errorText(err: unknown, prefix: string): string {
  if (err instanceof ApiError) return err.message || `${prefix} (HTTP ${err.status}).`;
  return `${prefix}: ${String(err)}`;
}

void boot();

import { describe, expect, it } from 'vitest';
import { createPlaceClient } from '../src/client/api.ts';
import { FeedModel } from '../src/client/feed.ts';
import { cellAt, formatDuration, lineCells, RateMeter, shellQuote } from '../src/client/geometry.ts';
import { createSseParser, type SseMessage } from '../src/client/sse.ts';
import { CanvasStore, UNSET } from '../src/client/store.ts';
import type { CanvasSnapshot, PlaceEvent } from '../src/shared/contract.ts';
import { DEFAULT_PALETTE } from '../src/shared/palette.ts';

const at = '2026-11-02T12:00:00.000Z';
const oid = (c: string) => c.repeat(40);
const red = '#d6262b';
const blue = '#3690ea';
const agent = { kind: 'agent' as const, label: 'a' };

function snapshot(over: Partial<CanvasSnapshot> = {}): CanvasSnapshot {
  return {
    seq: 0,
    mergedThroughSeq: 0,
    mainCommit: oid('0'),
    width: 4,
    height: 3,
    pixels: '.'.repeat(12),
    branches: [],
    ...over,
  };
}

describe('SSE parser', () => {
  it('handles fields split across chunks, CRLF, comments, multi-line data and retry', () => {
    const got: SseMessage[] = [];
    const p = createSseParser((m) => got.push(m));
    p.push('\ufeffretry: 1500\r\n: hello\r\n\r\nid: 7\r');
    p.push('\nevent: push\ndata: {"a":');
    p.push('1}\ndata: x\n\n');
    p.push('data:no-space\n\n');
    p.push('event: ignored-without-data\n\n');
    expect(p.retryMs).toBe(1500);
    expect(got).toEqual([
      { event: 'push', data: '{"a":1}\nx', id: '7' },
      { event: 'message', data: 'no-space', id: '7' },
    ]);
  });
});

describe('canvas store', () => {
  it('decodes base-36 pixels and treats unknown indexes as unpainted', () => {
    const s = new CanvasStore(4, 3, DEFAULT_PALETTE);
    s.loadSnapshot(snapshot({ pixels: '05f.........'.replace('f', 'f') }));
    expect([s.main[0], s.main[1], s.main[2], s.main[3]]).toEqual([0, 5, 15, UNSET]);
    s.loadSnapshot(snapshot({ pixels: 'z...........' }));
    expect(s.main[0]).toBe(UNSET);
    expect(() => s.loadSnapshot(snapshot({ pixels: '...' }))).toThrow();
  });

  it('shows the newest unmerged tile and drops it once merged', () => {
    const s = new CanvasStore(4, 3, DEFAULT_PALETTE);
    s.loadSnapshot(snapshot());
    s.apply({
      type: 'push',
      seq: 1,
      branch: 'a',
      actor: agent,
      commit: oid('1'),
      tiles: [{ x: 1, y: 1, color: red }],
      at,
    });
    s.apply({
      type: 'push',
      seq: 2,
      branch: 'b',
      actor: agent,
      commit: oid('2'),
      tiles: [{ x: 1, y: 1, color: blue }],
      at,
    });
    const i = s.index({ x: 1, y: 1 });
    expect(s.topAt(i)).toEqual({ branch: 'b', color: blue, seq: 2 });
    expect(s.liveBranchCount()).toBe(2);
    s.apply({
      type: 'merge',
      seq: 3,
      commit: oid('3'),
      throughSeq: 1,
      tiles: [{ x: 1, y: 1, color: red }],
      branches: ['a'],
      at,
    });
    expect(s.mainAt(i)).toBe(red);
    expect(s.topAt(i)).toEqual({ branch: 'b', color: blue, seq: 2 });
    s.apply({
      type: 'merge',
      seq: 4,
      commit: oid('4'),
      throughSeq: 2,
      tiles: [{ x: 1, y: 1, color: blue }],
      branches: ['b'],
      at,
    });
    expect(s.mainAt(i)).toBe(blue);
    expect(s.topAt(i)).toBeNull();
    expect(s.pendingTiles).toBe(0);
  });

  it("is idempotent and order-tolerant for the visitor's own push", () => {
    const s = new CanvasStore(4, 3, DEFAULT_PALETTE);
    s.loadSnapshot(snapshot());
    const tiles = [{ x: 0, y: 0, color: red }];
    const push: PlaceEvent = { type: 'push', seq: 1, branch: 'me', actor: agent, commit: oid('1'), tiles, at };
    const merge: PlaceEvent = { type: 'merge', seq: 2, commit: oid('2'), throughSeq: 1, tiles, branches: ['me'], at };

    // Response first, then the stream echo, then the merge.
    s.applyOwnPush('me', agent, oid('1'), 1, tiles);
    s.apply(push);
    s.apply(push);
    expect(s.pendingTiles).toBe(1);
    s.apply(merge);
    expect(s.pendingTiles).toBe(0);

    // Merge already streamed before the HTTP response arrives: nothing lingers.
    const late = new CanvasStore(4, 3, DEFAULT_PALETTE);
    late.loadSnapshot(snapshot());
    late.apply(push);
    late.apply(merge);
    late.applyOwnPush('me', agent, oid('1'), 1, tiles);
    expect(late.pendingTiles).toBe(0);
    expect(late.mainAt(0)).toBe(red);
  });

  it("removes an expired branch's overlay and ignores out-of-range tiles", () => {
    const s = new CanvasStore(4, 3, DEFAULT_PALETTE);
    s.loadSnapshot(snapshot());
    s.apply({
      type: 'push',
      seq: 1,
      branch: 'a',
      actor: agent,
      commit: oid('1'),
      tiles: [
        { x: 2, y: 2, color: red },
        { x: 9, y: 9, color: red },
      ],
      at,
    });
    expect(s.pendingTiles).toBe(1);
    s.apply({ type: 'expire', seq: 2, branch: 'a', at });
    expect(s.pendingTiles).toBe(0);
    expect(s.branches.has('a')).toBe(false);
  });

  it('reports dirty pixels for incremental redraws', () => {
    const s = new CanvasStore(4, 3, DEFAULT_PALETTE);
    s.loadSnapshot(snapshot());
    expect(s.drainDirty()).toBe('all');
    s.apply({
      type: 'push',
      seq: 1,
      branch: 'a',
      actor: agent,
      commit: oid('1'),
      tiles: [{ x: 3, y: 0, color: red }],
      at,
    });
    expect(s.drainDirty()).toEqual(new Set([3]));
    expect(s.drainDirty()).toEqual(new Set());
  });
});

describe('feed', () => {
  it('lists individual pushes, folds swarm pushes into one entry, and counts merges', () => {
    const f = new FeedModel(10, () => 'visitors/me');
    f.reset(0);
    const push = (seq: number, branch: string): PlaceEvent => ({
      type: 'push',
      seq,
      branch,
      actor: agent,
      commit: oid('a'),
      tiles: [{ x: 0, y: 0, color: red }],
      at,
    });
    f.add(push(1, 'visitors/me'), 0);
    for (let i = 2; i < 50; i++) f.add(push(i, `swarm/agent-${i % 7}`), 0);
    f.add({ type: 'merge', seq: 50, commit: oid('m'), throughSeq: 49, tiles: [], branches: ['x', 'y'], at }, 49);
    const [merge, burst, own] = f.list;
    expect(merge).toMatchObject({ kind: 'merge', pushes: 49, branches: 2 });
    expect(burst).toMatchObject({ kind: 'swarm-burst', pushes: 48, branches: 7 });
    expect(own).toMatchObject({ kind: 'push', own: true, branch: 'visitors/me' });
    for (let i = 0; i < 30; i++) f.add(push(100 + i, `agent/x-${i}`), 0);
    expect(f.list).toHaveLength(10);
  });
});

describe('geometry', () => {
  it('rasterises lines without gaps in both directions', () => {
    expect(lineCells(0, 0, 3, 1)).toEqual([
      [0, 0],
      [1, 0],
      [2, 1],
      [3, 1],
    ]);
    expect(lineCells(2, 2, 2, 2)).toEqual([[2, 2]]);
    const back = lineCells(5, 9, 0, 0);
    expect(back[0]).toEqual([5, 9]);
    expect(back.at(-1)).toEqual([0, 0]);
    for (let i = 1; i < back.length; i++) {
      expect(Math.abs(back[i][0] - back[i - 1][0])).toBeLessThanOrEqual(1);
      expect(Math.abs(back[i][1] - back[i - 1][1])).toBeLessThanOrEqual(1);
    }
  });

  it('maps points to cells and clamps the far edge', () => {
    expect(cellAt(0, 0, 640, 640, 64, 64)).toEqual({ x: 0, y: 0 });
    expect(cellAt(639.9, 639.9, 640, 640, 64, 64)).toEqual({ x: 63, y: 63 });
    expect(cellAt(-1, 5, 640, 640, 64, 64)).toBeNull();
    expect(cellAt(640, 5, 640, 640, 64, 64)).toBeNull();
  });

  it('formats durations, quotes shell words and measures rates', () => {
    expect(formatDuration(200)).toBe('now');
    expect(formatDuration(4200)).toBe('4 s');
    expect(formatDuration(180_000)).toBe('3 min');
    expect(shellQuote('https://place.example.com/canvas.git')).toBe('https://place.example.com/canvas.git');
    expect(shellQuote("Paint 1,2 it's")).toBe(`'Paint 1,2 it'\\''s'`);
    const r = new RateMeter(1000);
    r.add(0, 10);
    r.add(600, 5);
    expect(r.perSecond(700)).toBe(15);
    expect(r.perSecond(1300)).toBe(5);
  });
});

describe('event stream', () => {
  // #124: the server refuses a stream with 429 and Retry-After when too many
  // are open (a reconnect after the age limit included). The page keeps
  // retrying, no sooner than the server asked, from the same seq.
  it('reconnects after a refused stream, no sooner than Retry-After', async () => {
    const calls: { url: string; at: number }[] = [];
    const fake = (async (url: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(url), at: Date.now() });
      if (calls.length === 1)
        return new Response(JSON.stringify({ error: 'rate-limited', message: 'Too many event streams.' }), {
          status: 429,
          headers: { 'Content-Type': 'application/json', 'Retry-After': '2' },
        });
      const body = new ReadableStream<Uint8Array>({
        start(c) {
          c.enqueue(
            new TextEncoder().encode(`id: 6\nevent: push\ndata: ${JSON.stringify({ type: 'push', seq: 6 })}\n\n`),
          );
          init?.signal?.addEventListener('abort', () => c.close());
        },
      });
      return new Response(body, { status: 200, headers: { 'Content-Type': 'text/event-stream' } });
    }) as typeof fetch;
    const client = createPlaceClient({ baseUrl: 'http://place.test/api/v1', fetch: fake });
    const seen: PlaceEvent[] = [];
    const waits: number[] = [];
    const ctl = new AbortController();
    const done = client.events({
      after: 5,
      signal: ctl.signal,
      onEvent: (e) => seen.push(e),
      onResync: async () => 5,
      onState: (st) => {
        if (st.state === 'retrying') waits.push(st.inMs);
      },
    });
    const deadline = Date.now() + 5000;
    while (seen.length === 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 20));
    ctl.abort();
    await done;
    expect(seen.map((e) => e.seq)).toEqual([6]);
    expect(calls.map((c) => c.url)).toEqual([
      'http://place.test/api/v1/events?after=5',
      'http://place.test/api/v1/events?after=5',
    ]);
    expect(waits[0]).toBeGreaterThanOrEqual(2000);
    expect(calls[1].at - calls[0].at).toBeGreaterThanOrEqual(1900);
  }, 10_000);
});

describe('agent prompt', () => {
  it('is the public demo prompt, with every fact from the deployment', async () => {
    const { agentPrompt } = await import('../src/client/prompt.ts');
    const text = agentPrompt({
      pageUrl: 'https://place.reasonmachines.com',
      cloneUrl: 'https://place.reasonmachines.com/canvas.git',
      apiUrl: 'https://place.reasonmachines.com/api/v1',
      width: 64,
      height: 64,
      tilesPerPush: 64,
    });
    expect(
      text,
    ).toBe(`Help me paint something on the shared pixel canvas at https://place.reasonmachines.com. Every pixel is a real git push to an Aspen git server. Start by asking me what to paint and where (step 3), and don't paint anything until I say go.

1. git clone --depth=1 https://place.reasonmachines.com/canvas.git && cd canvas
2. Read README.md for the rules: each tile is tiles/<x>_<y>.txt (x from 0 to 63, y from 0 to 63), holding one palette color as lowercase #rrggbb plus a newline. Use only palette colors (GET https://place.reasonmachines.com/api/v1/config lists them) and only tile files. At most 64 tiles per push.
3. Before painting, ask me two things and wait for my answers:
   - What should we paint? (an icon, a word, a pattern, a flag…)
   - Where on the canvas? Offer three choices: a spot I name (x, y from 0 to 63), an area you find without anyone's recognizable picture, or "surprise me".
   To suggest a spot, look at the current picture first (GET https://place.reasonmachines.com/api/v1/canvas returns "pixels", 64 x 64 characters in row-major order, index y * 64 + x, each "." for a tile never painted or the tile's index into the config's palette in base 36), and tell me if the area I chose would paint over someone else's work.
4. Show me a small preview of the plan (a grid of the tiles and colors) and the exact coordinates. Paint only after I say go: write the tile files, for example printf '#d6262b\\n' > tiles/12_40.txt, at most 64 tiles per push.
5. git add tiles && git commit -m "paint: <what you drew>" && git push origin HEAD:refs/heads/paint/<a-short-name>
6. For a bigger picture, repeat in pushes of up to 64 tiles, no more than one push every few seconds.
7. Open https://place.reasonmachines.com and confirm your picture appears.

Your commit's name and email are public. Use a pseudonym if you prefer: git -c user.name=... -c user.email=... commit ...
`);
  });
});

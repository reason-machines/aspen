/**
 * The API client against the mock server over real HTTP, including the
 * event stream, error mapping, quotas, the swarm and its kill switch.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiError, createPlaceClient } from '../src/client/api.ts';
import { CanvasStore } from '../src/client/store.ts';
import type { PlaceEvent } from '../src/shared/contract.ts';
import { type Harness, startMock, waitFor } from './helpers.ts';

let h: Harness;
const red = '#d6262b';
const blue = '#3690ea';

async function expectApiError(p: Promise<unknown>): Promise<ApiError> {
  try {
    await p;
  } catch (err) {
    expect(err).toBeInstanceOf(ApiError);
    return err as ApiError;
  }
  throw new Error('expected the request to fail');
}

describe('place client against the mock', () => {
  beforeEach(async () => {
    h = await startMock({
      domain: 'aspen.test',
      limits: { tilesPerPush: 8, pushesPerMinute: 3, branchTtlSeconds: 60 },
    });
  });
  afterEach(async () => {
    await h.close();
  });

  it('serves config with a clone URL derived from the domain', async () => {
    const c = await h.client.config();
    expect(c.cloneUrl).toBe('https://place.aspen.test/canvas.git');
    expect(c.dashboardUrl).toBe('https://dashboard.aspen.test/');
    expect(c.canvas.palette.length).toBeGreaterThan(1);
    expect(c.limits.tilesPerPush).toBe(8);
  });

  it('honours an explicit clone URL and no dashboard', async () => {
    await h.close();
    h = await startMock({ cloneUrl: 'https://git.example.org/place.git', dashboardUrl: null });
    const c = await h.client.config();
    expect(c.cloneUrl).toBe('https://git.example.org/place.git');
    expect(c.dashboardUrl).toBeNull();
  });

  it('pushes a stroke to the session branch, overlays it, then merges it into main', async () => {
    const s = await h.client.createSession();
    expect(s.branch).toMatch(/^visitors\/[a-z2-9]{7}$/);
    const r = await h.client.push(s.token, {
      branch: s.branch,
      tiles: [
        { x: 1, y: 2, color: red },
        { x: 2, y: 2, color: blue },
      ],
    });
    expect(r.branch).toBe(s.branch);
    expect(r.commit).toMatch(/^[0-9a-f]{40}$/);
    expect(r.quota.remaining).toBe(2);

    let snap = await h.client.canvas();
    expect(snap.branches).toHaveLength(1);
    expect(snap.branches[0].tiles.map((t) => [t.x, t.y, t.color, t.seq])).toEqual([
      [1, 2, red, r.seq],
      [2, 2, blue, r.seq],
    ]);
    expect(snap.pixels[2 * snap.width + 1]).toBe('.');

    h.api.sim.merge();
    snap = await h.client.canvas();
    expect(snap.branches).toHaveLength(0);
    expect(snap.mergedThroughSeq).toBe(r.seq);
    const config = await h.client.config();
    const idx = config.canvas.palette.findIndex((p) => p.color === red);
    expect(snap.pixels[2 * snap.width + 1]).toBe(idx.toString(36));

    const branches = await h.client.branches();
    expect(branches.map((b) => [b.name, b.pendingTiles])).toEqual([[s.branch, 0]]);
  });

  it('maps auth, ownership, rule and quota failures to typed errors', async () => {
    const s = await h.client.createSession();
    const tile = [{ x: 0, y: 0, color: red }];

    let e = await expectApiError(h.client.push('nope', { branch: s.branch, tiles: tile }));
    expect([e.status, e.code]).toEqual([401, 'unauthorized']);

    e = await expectApiError(h.client.push(s.token, { branch: 'visitors/someone-else', tiles: tile }));
    expect([e.status, e.code]).toEqual([403, 'forbidden']);

    e = await expectApiError(
      h.client.push(s.token, {
        branch: s.branch,
        tiles: [
          { x: 0, y: 0, color: '#123456' },
          { x: 9999, y: 0, color: red },
        ],
      }),
    );
    expect([e.status, e.code]).toEqual([422, 'rejected']);
    expect(e.rejections.map((r) => r.code)).toEqual(['color-not-in-palette', 'out-of-bounds']);

    for (let i = 0; i < 3; i++) await h.client.push(s.token, { branch: s.branch, tiles: tile });
    e = await expectApiError(h.client.push(s.token, { branch: s.branch, tiles: tile }));
    expect([e.status, e.code]).toEqual([429, 'rate-limited']);
    expect(e.retryAfterSeconds).toBe(60);
    expect((await h.client.quota(s.token)).remaining).toBe(0);

    h.clock.advance(60_001);
    expect((await h.client.quota(s.token)).remaining).toBe(3);
    await h.client.push(s.token, { branch: s.branch, tiles: tile });
  });

  it('rejects malformed bodies with 400', async () => {
    const s = await h.client.createSession();
    const res = await fetch(`${h.baseUrl}/pushes`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${s.token}`, 'Content-Type': 'application/json' },
      body: '{"branch": 1}',
    });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe('bad-request');
  });

  it('rate limits session creation per address', async () => {
    for (let i = 0; i < 10; i++) await h.client.createSession();
    const e = await expectApiError(h.client.createSession());
    expect([e.status, e.code]).toEqual([429, 'rate-limited']);
    expect(e.retryAfterSeconds).toBeGreaterThan(0);
  });

  it('streams pushes and merges in seq order, and resumes after a given seq', async () => {
    const s = await h.client.createSession();
    const seen: PlaceEvent[] = [];
    const ctl = new AbortController();
    const states: string[] = [];
    const done = h.client.events({
      after: 0,
      signal: ctl.signal,
      onEvent: (e) => seen.push(e),
      onResync: async () => (await h.client.canvas()).seq,
      onState: (st) => states.push(st.state),
    });
    await waitFor(() => states.includes('open'), 3000, 'stream open');
    const r1 = await h.client.push(s.token, { branch: s.branch, tiles: [{ x: 3, y: 3, color: red }] });
    h.api.sim.merge();
    await waitFor(() => seen.length >= 2, 3000, 'two events');
    expect(seen.map((e) => e.type)).toEqual(['push', 'merge']);
    expect(seen[0].seq).toBe(r1.seq);
    const merge = seen[1];
    expect(merge.type === 'merge' && merge.throughSeq).toBe(r1.seq);
    ctl.abort();
    await done;

    // A second client that missed both events catches up from the backlog.
    const replay: PlaceEvent[] = [];
    const ctl2 = new AbortController();
    const done2 = h.client.events({
      after: 0,
      signal: ctl2.signal,
      onEvent: (e) => replay.push(e),
      onResync: async () => 0,
    });
    await waitFor(() => replay.length >= 2, 3000, 'replay');
    expect(replay.map((e) => e.seq)).toEqual(seen.map((e) => e.seq));
    ctl2.abort();
    await done2;
  });

  it('asks a client that fell behind the backlog to resync from a snapshot', async () => {
    await h.close();
    h = await startMock({ eventBacklog: 3 });
    const s = await h.client.createSession();
    for (let i = 0; i < 6; i++)
      h.api.sim.recordPush(s.branch, { kind: 'agent', label: 'a' }, [{ x: i, y: 0, color: red }]);
    let resyncs = 0;
    let streams = 0;
    const seen: PlaceEvent[] = [];
    const ctl = new AbortController();
    const client = createPlaceClient({
      baseUrl: h.baseUrl,
      fetch: (input, init) => {
        if (String(input).includes('/events')) streams++;
        return fetch(input, init);
      },
    });
    const done = client.events({
      after: 1,
      signal: ctl.signal,
      onEvent: (e) => seen.push(e),
      onResync: async () => {
        resyncs++;
        return (await h.client.canvas()).seq;
      },
    });
    await waitFor(() => resyncs === 1, 3000, 'resync');
    // Paint only once the second stream is open, or its event may land
    // before that stream subscribes and be folded into the snapshot.
    await waitFor(() => streams >= 2, 3000, 'second stream');
    await new Promise((r) => setTimeout(r, 200));
    h.api.sim.recordPush(s.branch, { kind: 'agent', label: 'a' }, [{ x: 9, y: 0, color: red }]);
    await waitFor(() => seen.length === 1, 3000, 'live event after resync');
    expect(seen[0].seq).toBe(7);
    ctl.abort();
    await done;
  });

  it('resyncs a client whose cursor belongs to another server instance, then streams with the new one', async () => {
    const s = await h.client.createSession();
    h.api.sim.recordPush(s.branch, { kind: 'agent', label: 'a' }, [{ x: 0, y: 0, color: red }]);
    const snap = await h.client.canvas();
    expect(snap.instance).toBe(h.api.sim.instance);
    const urls: string[] = [];
    const fetchSpy: typeof fetch = (input, init) => {
      urls.push(String(input));
      return fetch(input, init);
    };
    const client = createPlaceClient({ baseUrl: h.baseUrl, fetch: fetchSpy });
    let resyncs = 0;
    const seen: PlaceEvent[] = [];
    const ctl = new AbortController();
    const done = client.events({
      // A cursor inside this feed's range, from another instance's feed.
      after: snap.seq - 1,
      instance: 'another-task',
      signal: ctl.signal,
      onEvent: (e) => seen.push(e),
      onResync: async () => {
        resyncs++;
        const c = await client.canvas();
        return { seq: c.seq, instance: c.instance };
      },
    });
    await waitFor(() => resyncs === 1, 3000, 'resync');
    expect(seen).toEqual([]);
    // Paint only once the second stream is open, or its event may land
    // before that stream subscribes and be folded into the snapshot.
    await waitFor(() => urls.filter((u) => u.includes('/events')).length >= 2, 3000, 'second stream');
    await new Promise((r) => setTimeout(r, 200));
    h.api.sim.recordPush(s.branch, { kind: 'agent', label: 'a' }, [{ x: 1, y: 0, color: red }]);
    await waitFor(() => seen.length === 1, 3000, 'live event after resync');
    expect(seen[0].seq).toBe(snap.seq + 1);
    const streams = urls.filter((u) => u.includes('/events'));
    expect(streams[0]).toContain('instance=another-task');
    expect(streams[1]).toContain(`instance=${snap.instance}`);
    ctl.abort();
    await done;
  });

  it('a resync callback returning only a seq drops the stale instance instead of resyncing forever', async () => {
    const s = await h.client.createSession();
    h.api.sim.recordPush(s.branch, { kind: 'agent', label: 'a' }, [{ x: 0, y: 0, color: red }]);
    const snap = await h.client.canvas();
    const urls: string[] = [];
    const fetchSpy: typeof fetch = (input, init) => {
      urls.push(String(input));
      return fetch(input, init);
    };
    const client = createPlaceClient({ baseUrl: h.baseUrl, fetch: fetchSpy });
    let resyncs = 0;
    const seen: PlaceEvent[] = [];
    const ctl = new AbortController();
    const done = client.events({
      after: snap.seq - 1,
      instance: 'old',
      signal: ctl.signal,
      onEvent: (e) => seen.push(e),
      onResync: async () => {
        resyncs++;
        return (await client.canvas()).seq;
      },
    });
    const streams = () => urls.filter((u) => u.includes('/events'));
    await waitFor(() => streams().length >= 2, 3000, 'second stream');
    await new Promise((r) => setTimeout(r, 200));
    h.api.sim.recordPush(s.branch, { kind: 'agent', label: 'a' }, [{ x: 1, y: 0, color: red }]);
    await waitFor(() => seen.length === 1, 3000, 'live event after resync');
    expect(resyncs).toBe(1);
    expect(streams()).toHaveLength(2);
    expect(streams()[0]).toContain('instance=old');
    expect(streams()[1]).not.toContain('instance=');
    ctl.abort();
    await done;
  });

  it('pauses ambient agents while the kill switch is engaged', async () => {
    await h.close();
    h = await startMock({ ambientAgents: 4 });
    const step = async (ms: number) => {
      for (let t = 0; t < ms; t += 100) {
        h.clock.advance(100);
        h.api.sim.tick();
      }
      return (await h.client.canvas()).seq;
    };
    const before = await step(10_000);
    expect(before).toBeGreaterThan(0);
    h.api.sim.merge(); // fold pushes accepted before the kill, so only new pushes move seq
    await h.client.setKillSwitch('mock-operator-token', { engaged: true, reason: 'budget' });
    const atKill = (await h.client.canvas()).seq;
    expect(await step(10_000)).toBe(atKill);
    await h.client.setKillSwitch('mock-operator-token', { engaged: false });
    expect(await step(10_000)).toBeGreaterThan(atKill);
  });

  it('runs the swarm within its cap and honours cooldown and the kill switch', async () => {
    const opts = h.api.sim.opts.swarm;
    let st = await h.client.swarm();
    expect(st.state).toBe('idle');

    st = await h.client.startSwarm();
    expect(st.state).toBe('running');
    let e = await expectApiError(h.client.startSwarm());
    expect([e.status, e.code]).toEqual([429, 'cooldown']);

    // Drive ten simulated seconds in 100 ms ticks; pushes never exceed the cap.
    for (let i = 0; i < 100; i++) {
      h.clock.advance(100);
      h.api.sim.tick();
    }
    st = await h.client.swarm();
    expect(st.agents).toBe(opts.maxAgents);
    expect(st.pushes).toBeGreaterThan(0);
    expect(st.pushes).toBeLessThanOrEqual(opts.pushesPerSecondCap * 10);
    const branches = await h.client.branches();
    expect(branches.filter((b) => b.name.startsWith('swarm/')).length).toBeGreaterThan(50);

    // Operators only.
    e = await expectApiError(h.client.setKillSwitch('wrong', { engaged: true }));
    expect([e.status, e.code]).toEqual([403, 'forbidden']);

    st = await h.client.setKillSwitch('mock-operator-token', { engaged: true, reason: 'Load test of the load test' });
    expect(st.state).toBe('disabled');
    expect(st.killSwitch).toEqual({ engaged: true, reason: 'Load test of the load test' });
    const pushesAtKill = st.pushes;
    h.clock.advance(1000);
    h.api.sim.tick();
    expect((await h.client.swarm()).pushes).toBe(pushesAtKill);

    e = await expectApiError(h.client.startSwarm());
    expect([e.status, e.code]).toEqual([503, 'kill-switch']);
    expect(e.message).toContain('Load test of the load test');

    st = await h.client.setKillSwitch('mock-operator-token', { engaged: false });
    expect(st.state).toBe('cooldown');
    h.clock.advance(opts.cooldownSeconds * 1000);
    h.api.sim.tick();
    expect((await h.client.swarm()).state).toBe('idle');
  });

  it('limits swarm starts per address per hour', async () => {
    const o = h.api.sim.opts.swarm;
    for (let i = 0; i < o.startsPerIpPerHour; i++) {
      await h.client.startSwarm();
      h.clock.advance((o.runSeconds + o.cooldownSeconds) * 1000);
      h.api.sim.tick();
      h.api.sim.tick();
    }
    const e = await expectApiError(h.client.startSwarm());
    expect([e.status, e.code]).toEqual([429, 'rate-limited']);
  });

  it('keeps many concurrent visitors and agents consistent: the streamed client state equals the server snapshot', async () => {
    await h.close();
    h = await startMock({
      width: 32,
      height: 32,
      limits: { tilesPerPush: 16, pushesPerMinute: 100, branchTtlSeconds: 5 },
    });
    const config = await h.client.config();
    const snap0 = await h.client.canvas();
    const store = new CanvasStore(config.canvas.width, config.canvas.height, config.canvas.palette);
    store.loadSnapshot(snap0);
    const ctl = new AbortController();
    const done = h.client.events({
      after: snap0.seq,
      signal: ctl.signal,
      onEvent: (e) => store.apply(e),
      onResync: async () => {
        const s = await h.client.canvas();
        store.loadSnapshot(s);
        return s.seq;
      },
    });

    // Twenty visitors push in parallel while the merger bot runs between rounds.
    const sessions = [];
    for (let i = 0; i < 10; i++) sessions.push(await h.client.createSession());
    h.clock.advance(61_000);
    for (let i = 0; i < 10; i++) sessions.push(await h.client.createSession());
    const palette = config.canvas.palette;
    for (let round = 0; round < 5; round++) {
      const results = await Promise.all(
        sessions.map((s, i) => {
          const tiles = Array.from({ length: 4 }, (_, k) => ({
            x: (i * 3 + k + round) % 32,
            y: (i + round * 7) % 32,
            color: palette[(i + k + round) % palette.length].color,
          }));
          return h.client.push(s.token, { branch: s.branch, tiles });
        }),
      );
      expect(new Set(results.map((r) => r.seq)).size).toBe(sessions.length);
      if (round % 2 === 1) h.api.sim.merge();
    }
    const startSwarm = await h.client.startSwarm();
    expect(startSwarm.state).toBe('running');
    for (let i = 0; i < 40; i++) {
      h.clock.advance(100);
      h.api.sim.tick();
    }
    h.api.sim.merge();
    h.clock.advance(10_000);
    h.api.sim.tick();

    const final = await h.client.canvas();
    await waitFor(() => store.seq === final.seq, 5000, 'client to catch up');
    ctl.abort();
    await done;

    const check = new CanvasStore(config.canvas.width, config.canvas.height, palette);
    check.loadSnapshot(final);
    expect(Buffer.from(store.main).equals(Buffer.from(check.main))).toBe(true);
    expect(store.mainCommit).toBe(final.mainCommit);
    expect(store.pendingTiles).toBe(check.pendingTiles);
    expect(final.mergedThroughSeq).toBeGreaterThan(100);
  });
});

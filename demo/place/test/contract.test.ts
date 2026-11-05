/**
 * Checks every response the mock produces against `api/openapi.yaml`, so the
 * spec, the TypeScript types and the mock cannot drift apart unnoticed.
 */
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { formatEvent } from '../mock/api.ts';
import { createSseParser, type SseMessage } from '../src/client/sse.ts';
import { EVENT_TYPES, type PlaceEvent } from '../src/shared/contract.ts';
import { type Harness, startMock } from './helpers.ts';
import { loadSpec, validate } from './schema.ts';

const spec = loadSpec(fileURLToPath(new URL('../api/openapi.yaml', import.meta.url)));
const schema = (name: string) => ({ $ref: `#/components/schemas/${name}` });
const red = '#d6262b';

let h: Harness;

async function call(method: string, path: string, body?: unknown, token?: string) {
  const res = await fetch(`${h.baseUrl}${path}`, {
    method,
    headers: {
      ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, headers: res.headers, json: (await res.json()) as unknown };
}

/** The response schema the spec declares for this path, method and status. */
function responseSchema(path: string, method: string, status: number): Record<string, unknown> {
  const op = spec.paths[path]?.[method.toLowerCase()] as { responses: Record<string, unknown> } | undefined;
  if (!op) throw new Error(`spec has no ${method} ${path}`);
  let r = op.responses[String(status)] as Record<string, unknown> | undefined;
  if (!r) throw new Error(`spec has no ${status} response for ${method} ${path}`);
  if (typeof r.$ref === 'string') {
    const name = r.$ref.replace('#/components/responses/', '');
    r = (spec as unknown as { components: { responses: Record<string, Record<string, unknown>> } }).components
      .responses[name];
  }
  const content = r.content as Record<string, { schema: Record<string, unknown> }>;
  return content['application/json'].schema;
}

async function expectConforms(method: string, path: string, body?: unknown, token?: string, status?: number) {
  const r = await call(method, path, body, token);
  if (status !== undefined) expect(r.status).toBe(status);
  const errs = validate(spec, responseSchema(path, method, r.status), r.json);
  expect(errs, `${method} ${path} -> ${r.status}: ${JSON.stringify(r.json)}`).toEqual([]);
  if (r.status === 429 || r.status === 503) expect(r.headers.get('retry-after')).toMatch(/^[1-9][0-9]*$/);
  expect(r.headers.get('cache-control')).toBe('no-store');
  return r;
}

describe('the mock conforms to api/openapi.yaml', () => {
  beforeAll(async () => {
    h = await startMock({
      seedCanvas: true,
      ambientAgents: 3,
      limits: { tilesPerPush: 4, pushesPerMinute: 2, branchTtlSeconds: 1 },
    });
  });
  afterAll(async () => {
    await h.close();
  });

  it('spec and TypeScript agree on the event types', () => {
    const oneOf = (spec.components.schemas.Event.oneOf as { $ref: string }[]).map((r) =>
      r.$ref.replace('#/components/schemas/', ''),
    );
    expect(oneOf.map((n) => n.replace('Event', '').toLowerCase())).toEqual([...EVENT_TYPES]);
  });

  it('every endpoint and error status', async () => {
    await expectConforms('GET', '/config', undefined, undefined, 200);
    await expectConforms('GET', '/canvas', undefined, undefined, 200);
    await expectConforms('GET', '/swarm', undefined, undefined, 200);

    const s = (await expectConforms('POST', '/sessions', undefined, undefined, 201)).json as {
      branch: string;
      token: string;
    };
    await expectConforms('GET', '/quota', undefined, s.token, 200);
    await expectConforms('GET', '/quota', undefined, 'bad-token', 401);

    const tiles = [{ x: 1, y: 1, color: red }];
    await expectConforms('POST', '/pushes', { branch: s.branch, tiles }, s.token, 201);
    await expectConforms('POST', '/pushes', { branch: s.branch, tiles }, undefined, 401);
    await expectConforms('POST', '/pushes', { branch: 'visitors/x', tiles }, s.token, 403);
    await expectConforms('POST', '/pushes', { branch: s.branch, tiles: [] }, s.token, 422);
    await expectConforms('POST', '/pushes', { branch: s.branch, tiles: 'no' }, s.token, 400);
    await expectConforms('POST', '/pushes', { branch: s.branch, tiles }, s.token, 201);
    await expectConforms('POST', '/pushes', { branch: s.branch, tiles }, s.token, 429);

    // Ambient agents and the visitor leave overlays; snapshot and branch list both carry them.
    for (let i = 0; i < 20; i++) {
      h.clock.advance(100);
      h.api.sim.tick();
    }
    const snap = (await expectConforms('GET', '/canvas', undefined, undefined, 200)).json as {
      pixels: string;
      width: number;
      height: number;
      branches: unknown[];
    };
    expect(snap.pixels).toHaveLength(snap.width * snap.height);
    expect(snap.branches.length).toBeGreaterThan(0);
    await expectConforms('GET', '/branches', undefined, undefined, 200);

    await expectConforms('POST', '/swarm', undefined, undefined, 202);
    await expectConforms('POST', '/swarm', undefined, undefined, 429);
    await expectConforms('POST', '/admin/kill-switch', { engaged: true }, 'nope', 403);
    await expectConforms('POST', '/admin/kill-switch', { engaged: 'yes' }, 'mock-operator-token', 400);
    await expectConforms('POST', '/admin/kill-switch', { engaged: true, reason: 'test' }, 'mock-operator-token', 200);
    await expectConforms('POST', '/swarm', undefined, undefined, 503);
    await expectConforms('POST', '/admin/kill-switch', { engaged: false }, 'mock-operator-token', 200);

    for (let i = 0; i < 12; i++) await expectConforms('POST', '/sessions', undefined, undefined);
    const last = await call('POST', '/sessions');
    expect(last.status).toBe(429);
  });

  it('every event type on the stream, with SSE id and event name matching the payload', async () => {
    // Make one of each: push, merge, expire, swarm (above), then read the backlog.
    h.clock.advance(5000);
    h.api.sim.tick();
    h.clock.advance(5000);
    h.api.sim.tick();
    const messages: SseMessage[] = [];
    const parser = createSseParser((m) => messages.push(m));
    const ctl = new AbortController();
    const res = await fetch(`${h.baseUrl}/events?after=0`, { signal: ctl.signal });
    expect(res.headers.get('content-type')).toMatch(/^text\/event-stream/);
    const reader = res.body?.pipeThrough(new TextDecoderStream()).getReader();
    if (!reader) throw new Error('no body');
    const want = new Set(['push', 'merge', 'expire', 'swarm']);
    while ([...want].some((t) => !messages.some((m) => m.event === t))) {
      const { value, done } = await reader.read();
      if (done) break;
      parser.push(value);
    }
    ctl.abort();
    expect(parser.retryMs).toBe(2000);
    for (const m of messages) {
      const e = JSON.parse(m.data) as PlaceEvent;
      expect(m.event).toBe(e.type);
      expect(m.id).toBe(String(e.seq));
      expect(validate(spec, schema('Event'), e), m.data).toEqual([]);
    }
    const seqs = messages.map((m) => Number(m.id));
    expect(seqs).toEqual([...seqs].sort((a, b) => a - b));
    expect(new Set(seqs).size).toBe(seqs.length);
  });

  it('resync events conform too', () => {
    const e: PlaceEvent = { type: 'resync', seq: 4, at: '2026-11-02T12:00:00.000Z' };
    expect(formatEvent(e)).toBe(`id: 4\nevent: resync\ndata: ${JSON.stringify(e)}\n\n`);
    expect(validate(spec, schema('Event'), e)).toEqual([]);
  });

  it('the validator catches real violations', () => {
    expect(validate(spec, schema('Tile'), { x: 1, y: 1, color: '#D6262B' })).not.toEqual([]);
    expect(validate(spec, schema('Tile'), { x: 1, y: 1, color: red, extra: 1 })).not.toEqual([]);
    expect(validate(spec, schema('Tile'), { x: 1.5, y: 1, color: red })).not.toEqual([]);
    expect(validate(spec, schema('Event'), { type: 'push', seq: 1 })).not.toEqual([]);
  });
});

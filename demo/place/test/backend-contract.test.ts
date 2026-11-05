/**
 * Checks a running back end (the real one, `demo/place/server`) against
 * `api/openapi.yaml`: every endpoint, every error status and every event
 * type. Skipped unless `PLACE_CONTRACT_URL` names the API base, e.g.
 *
 *   PLACE_CONTRACT_URL=http://127.0.0.1:7070/api/v1 \
 *   PLACE_CONTRACT_OPERATOR_TOKEN=aspen_... npx vitest run test/backend-contract.test.ts
 *
 * The server must run with 2 pushes per minute, a 1 s branch TTL and a
 * short merge interval (`PLACE_PUSHES_PER_MINUTE=2 PLACE_BRANCH_TTL_SECONDS=1
 * PLACE_MERGE_INTERVAL_MS=300`), on a fresh data directory, with the
 * operator token from `aspen-place operator-token`. The server README has
 * the full command.
 */
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { createSseParser, type SseMessage } from '../src/client/sse.ts';
import type { PlaceEvent } from '../src/shared/contract.ts';
import { loadSpec, validate } from './schema.ts';

const base = process.env.PLACE_CONTRACT_URL;
const operator = process.env.PLACE_CONTRACT_OPERATOR_TOKEN ?? '';
const spec = loadSpec(fileURLToPath(new URL('../api/openapi.yaml', import.meta.url)));
const red = '#d6262b';

async function call(method: string, path: string, body?: unknown, token?: string) {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: {
      ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, headers: res.headers, json: (await res.json()) as unknown };
}

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
  if (status !== undefined) expect(r.status, `${method} ${path}: ${JSON.stringify(r.json)}`).toBe(status);
  const errs = validate(spec, responseSchema(path, method, r.status), r.json);
  expect(errs, `${method} ${path} -> ${r.status}: ${JSON.stringify(r.json)}`).toEqual([]);
  if (r.status === 429 || r.status === 503) expect(r.headers.get('retry-after')).toMatch(/^[1-9][0-9]*$/);
  expect(r.headers.get('cache-control')).toBe('no-store');
  expect(r.headers.get('access-control-allow-origin')).toBe('*');
  return r;
}

describe.skipIf(!base)('a running back end conforms to api/openapi.yaml', () => {
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
    const snap = (await expectConforms('GET', '/canvas', undefined, undefined, 200)).json as {
      pixels: string;
      width: number;
      height: number;
    };
    expect(snap.pixels).toHaveLength(snap.width * snap.height);
    await expectConforms('GET', '/branches', undefined, undefined, 200);

    await expectConforms('POST', '/swarm', undefined, undefined, 202);
    await expectConforms('POST', '/swarm', undefined, undefined, 429);
    await expectConforms('POST', '/admin/kill-switch', { engaged: true }, 'nope', 403);
    await expectConforms('POST', '/admin/kill-switch', { engaged: true }, undefined, 403);
    await expectConforms('POST', '/admin/kill-switch', { engaged: 'yes' }, operator, 400);
    await expectConforms('POST', '/admin/kill-switch', { engaged: true, reason: 'test' }, operator, 200);
    await expectConforms('POST', '/swarm', undefined, undefined, 503);
    await expectConforms('POST', '/admin/kill-switch', { engaged: false }, operator, 200);
    for (let i = 0; i < 12; i++) await expectConforms('POST', '/sessions', undefined, undefined);
    expect((await call('POST', '/sessions')).status).toBe(429);
  });

  it('every event type on the stream, with SSE id and event name matching the payload', async () => {
    const messages: SseMessage[] = [];
    const parser = createSseParser((m) => messages.push(m));
    const ctl = new AbortController();
    const res = await fetch(`${base}/events?after=0`, { signal: ctl.signal });
    expect(res.headers.get('content-type')).toMatch(/^text\/event-stream/);
    const reader = res.body?.pipeThrough(new TextDecoderStream()).getReader();
    if (!reader) throw new Error('no body');
    // Pushes and swarm events came from the test above; merges and expiry
    // follow on the server's own clock.
    const want = ['push', 'merge', 'expire', 'swarm'];
    const deadline = Date.now() + 20_000;
    while (want.some((t) => !messages.some((m) => m.event === t)) && Date.now() < deadline) {
      const { value, done } = await reader.read();
      if (done) break;
      parser.push(value);
    }
    ctl.abort();
    expect(want.filter((t) => !messages.some((m) => m.event === t))).toEqual([]);
    expect(parser.retryMs).toBe(2000);
    for (const m of messages) {
      const e = JSON.parse(m.data) as PlaceEvent;
      expect(m.event).toBe(e.type);
      expect(m.id).toBe(String(e.seq));
      expect(validate(spec, { $ref: '#/components/schemas/Event' }, e), m.data).toEqual([]);
    }
    const seqs = messages.map((m) => Number(m.id));
    expect(seqs).toEqual([...seqs].sort((a, b) => a - b));
    expect(new Set(seqs).size).toBe(seqs.length);
    // A resync for a seq the server does not have.
    const stale = await fetch(`${base}/events?after=999999999`);
    const text = await stale.body?.getReader().read();
    expect(new TextDecoder().decode(text?.value)).toContain('event: resync');
  }, 30_000);
});

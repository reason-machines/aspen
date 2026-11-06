/**
 * HTTP layer of the mock `place` API (see `api/openapi.yaml`). The handler is
 * connect-style, so it runs standalone under `node:http` (`mock/server.ts`)
 * and inside Vite's dev and preview servers (`mock/vite-plugin.ts`).
 */
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { ApiErrorBody, ErrorCode, KillSwitchRequest, PlaceEvent, PushRequest } from '../src/shared/contract.ts';
import type { Tile } from '../src/shared/tiles.ts';
import { type MockOptions, PlaceSim } from './sim.ts';

export const API_BASE = '/api/v1';
const MAX_BODY_BYTES = 64 * 1024;
const HEARTBEAT_MS = 15_000;

export type Handler = (req: IncomingMessage, res: ServerResponse, next?: () => void) => void;

export interface MockApi {
  sim: PlaceSim;
  handler: Handler;
  close: () => void;
}

/** Creates a running mock. Pass `clock: false` to drive `sim.tick()` yourself. */
export function createMockApi(options: Partial<MockOptions> & { clock?: boolean; base?: string } = {}): MockApi {
  const { clock = true, base = API_BASE, ...simOptions } = options;
  const sim = new PlaceSim(simOptions);
  if (clock) sim.start();
  const streams = new Set<ServerResponse>();

  const handler: Handler = (req, res, next) => {
    const url = new URL(req.url ?? '/', 'http://mock');
    if (url.pathname !== base && !url.pathname.startsWith(`${base}/`)) {
      if (next) next();
      else sendError(res, 404, 'not-found', `No route for ${url.pathname}.`);
      return;
    }
    const route = `${req.method ?? 'GET'} ${url.pathname.slice(base.length) || '/'}`;
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Expose-Headers', 'Retry-After');
    void route_(route, req, res, url).catch((err: unknown) => {
      if (!res.headersSent) sendError(res, 500, 'bad-request', `Mock error: ${String(err)}`);
      else res.end();
    });
  };

  async function route_(route: string, req: IncomingMessage, res: ServerResponse, url: URL): Promise<void> {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Authorization, Content-Type, Last-Event-ID',
        'Access-Control-Max-Age': '600',
      });
      res.end();
      return;
    }
    const ip = req.socket.remoteAddress ?? 'unknown';
    switch (route) {
      case 'GET /config':
        return sendJson(res, 200, sim.config());
      case 'GET /canvas':
        return sendJson(res, 200, sim.snapshot());
      case 'GET /canvas.svg': {
        const { etag, svg } = sim.canvasSvg();
        const headers = { 'Content-Type': 'image/svg+xml', 'Cache-Control': 'no-cache, max-age=0', ETag: etag };
        if (req.headers['if-none-match'] === etag) {
          res.writeHead(304, headers);
          res.end();
          return;
        }
        res.writeHead(200, headers);
        res.end(svg);
        return;
      }
      case 'GET /branches':
        return sendJson(res, 200, { branches: sim.branchList() });
      case 'POST /sessions': {
        const r = sim.createSession(ip);
        if (!r.ok)
          return sendError(res, 429, 'rate-limited', 'Too many sessions from this address.', r.retryAfterSeconds);
        return sendJson(res, 201, r.session);
      }
      case 'GET /quota': {
        const q = sim.quotaFor(bearer(req));
        if (!q)
          return sendError(
            res,
            401,
            'unauthorized',
            'Missing or unknown session token. Create one with POST /sessions.',
          );
        return sendJson(res, 200, q);
      }
      case 'POST /pushes': {
        const body = await readJson(req);
        const parsed = parsePushRequest(body);
        if (typeof parsed === 'string') return sendError(res, 400, 'bad-request', parsed);
        const r = sim.pushAsVisitor(bearer(req), parsed.branch, parsed.tiles, parsed.message);
        if (r.ok) return sendJson(res, 201, r.result);
        if (r.status === 401) return sendError(res, 401, 'unauthorized', r.message);
        if (r.status === 403) return sendError(res, 403, 'forbidden', r.message);
        if (r.status === 422) {
          return sendJson(res, 422, {
            error: 'rejected',
            message: r.rejections.map((x) => x.message).join('\n'),
            rejections: r.rejections,
          } satisfies ApiErrorBody);
        }
        return sendError(
          res,
          429,
          'rate-limited',
          `You can push ${r.quota.pushesPerMinute} times a minute. Try again in ${r.retryAfterSeconds} s.`,
          r.retryAfterSeconds,
        );
      }
      case 'GET /swarm':
        return sendJson(res, 200, sim.swarmStatus());
      case 'POST /swarm': {
        const r = sim.startSwarm(ip);
        if (r.ok) return sendJson(res, 202, r.status);
        if (r.reason === 'kill-switch') {
          const reason = r.status.killSwitch.reason;
          return sendError(
            res,
            503,
            'kill-switch',
            `The swarm is paused by the operators${reason ? `: ${reason}` : '.'}`,
            r.retryAfterSeconds,
          );
        }
        const msg =
          r.reason === 'cooldown'
            ? `A swarm ran recently. The next one can start in ${r.retryAfterSeconds} s.`
            : `This address has started the swarm too often. Try again in ${r.retryAfterSeconds} s.`;
        return sendError(res, 429, r.reason, msg, r.retryAfterSeconds);
      }
      case 'POST /admin/kill-switch': {
        if (!sim.isOperator(bearer(req))) return sendError(res, 403, 'forbidden', 'Operator token required.');
        const body = (await readJson(req)) as Partial<KillSwitchRequest> | null;
        if (!body || typeof body.engaged !== 'boolean') {
          return sendError(res, 400, 'bad-request', 'Body must be {"engaged": boolean, "reason"?: string}.');
        }
        return sendJson(
          res,
          200,
          sim.setKillSwitch(body.engaged, typeof body.reason === 'string' ? body.reason.slice(0, 200) : null),
        );
      }
      case 'GET /events':
        return openStream(req, res, url);
      default:
        return sendError(res, 404, 'not-found', `No route for ${route}.`);
    }
  }

  function openStream(req: IncomingMessage, res: ServerResponse, url: URL): void {
    const fromHeader = req.headers['last-event-id'];
    const raw = (typeof fromHeader === 'string' && fromHeader) || url.searchParams.get('after');
    const after = raw !== null && /^[0-9]+$/.test(raw) ? Number(raw) : null;
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-store',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.write('retry: 2000\n\n');
    const write = (e: PlaceEvent) => res.write(formatEvent(e));
    const sub = sim.subscribe(after, write);
    if (sub.ok) for (const e of sub.backlog) write(e);
    else write(sub.resync);
    streams.add(res);
    const heartbeat = setInterval(() => res.write(': keepalive\n\n'), HEARTBEAT_MS);
    heartbeat.unref?.();
    const cleanup = () => {
      clearInterval(heartbeat);
      sub.unsubscribe();
      streams.delete(res);
    };
    req.on('close', cleanup);
    res.on('close', cleanup);
  }

  return {
    sim,
    handler,
    close: () => {
      sim.stop();
      for (const s of streams) s.end();
      streams.clear();
    },
  };
}

/** Serialises one event in `text/event-stream` format. */
export function formatEvent(e: PlaceEvent): string {
  return `id: ${e.seq}\nevent: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`;
}

function parsePushRequest(body: unknown): PushRequest | string {
  if (typeof body !== 'object' || body === null) return 'Body must be a JSON object.';
  const b = body as Record<string, unknown>;
  if (typeof b.branch !== 'string') return '"branch" must be a string.';
  if (!Array.isArray(b.tiles)) return '"tiles" must be an array of {x, y, color}.';
  const tiles: Tile[] = [];
  for (const t of b.tiles) {
    if (typeof t !== 'object' || t === null) return 'Each tile must be an object {x, y, color}.';
    const { x, y, color } = t as Record<string, unknown>;
    if (typeof x !== 'number' || typeof y !== 'number' || typeof color !== 'string') {
      return 'Each tile must be {x: integer, y: integer, color: "#rrggbb"}.';
    }
    tiles.push({ x, y, color });
  }
  if (b.message !== undefined && typeof b.message !== 'string') return '"message" must be a string.';
  return { branch: b.branch, tiles, message: typeof b.message === 'string' ? b.message.slice(0, 200) : undefined };
}

function bearer(req: IncomingMessage): string | null {
  const h = req.headers.authorization;
  return typeof h === 'string' && h.startsWith('Bearer ') ? h.slice(7).trim() : null;
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  let size = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(chunk as Buffer);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    return null;
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(body));
}

function sendError(
  res: ServerResponse,
  status: number,
  error: ErrorCode,
  message: string,
  retryAfterSeconds?: number,
): void {
  if (retryAfterSeconds !== undefined) res.setHeader('Retry-After', String(retryAfterSeconds));
  const body: ApiErrorBody = { error, message };
  if (retryAfterSeconds !== undefined) body.retryAfterSeconds = retryAfterSeconds;
  sendJson(res, status, body);
}

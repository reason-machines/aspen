/**
 * Typed client for the `place` API (`api/openapi.yaml`). Works in browsers
 * and in Node 22+, which both provide `fetch` and web streams.
 */
import type {
  ApiErrorBody,
  BranchSummary,
  CanvasSnapshot,
  Config,
  ErrorCode,
  KillSwitchRequest,
  PlaceEvent,
  PushRequest,
  PushResult,
  Quota,
  Session,
  SwarmStatus,
} from '../shared/contract.ts';
import type { Rejection } from '../shared/tiles.ts';
import { createSseParser } from './sse.ts';

export class ApiError extends Error {
  readonly status: number;
  readonly code: ErrorCode | 'network';
  readonly retryAfterSeconds: number | null;
  readonly rejections: Rejection[];
  /** False when place itself did not answer (no response, or a body that is not place's JSON). */
  readonly fromPlace: boolean;

  constructor(
    status: number,
    body: Partial<ApiErrorBody>,
    retryAfterHeader: string | null = null,
    fromPlace = status !== 0,
  ) {
    super(body.message ?? `HTTP ${status}`);
    this.fromPlace = fromPlace;
    this.name = 'ApiError';
    this.status = status;
    this.code = body.error ?? (status === 0 ? 'network' : 'bad-request');
    const header = retryAfterHeader !== null && /^[0-9]+$/.test(retryAfterHeader) ? Number(retryAfterHeader) : null;
    this.retryAfterSeconds = body.retryAfterSeconds ?? header;
    this.rejections = body.rejections ?? [];
  }
}

/**
 * True when the server is restarting or overloaded (the load balancer has
 * no healthy place to send to, or place is shutting down): the request did
 * not go through, and the same request may succeed shortly.
 */
export function isUnavailable(err: unknown): boolean {
  return err instanceof ApiError && (err.status === 502 || err.status === 503 || err.status === 504);
}

/**
 * True when the request provably never reached place, so sending it again
 * cannot commit twice: the network failed before a response, or the load
 * balancer answered 503 itself because it had no healthy place task (its
 * HTML page, not place's JSON). A 502 or 504 may have reached place, so it
 * does not count.
 */
export function neverReachedPlace(err: unknown): boolean {
  return err instanceof ApiError && !err.fromPlace && (err.status === 0 || err.status === 503);
}

/** A short, readable message for an error response whose body is not JSON. */
export function nonJsonErrorMessage(status: number): string {
  if (status === 502 || status === 503 || status === 504) {
    return `The canvas server is restarting (HTTP ${status}). Try again in a minute.`;
  }
  return `The server answered HTTP ${status}. Try again in a minute.`;
}

export type StreamState =
  | { state: 'connecting' }
  | { state: 'open' }
  | { state: 'retrying'; inMs: number; error: unknown };

export interface EventStreamOptions {
  /** Resume after this sequence number; null starts at the live edge. */
  after: number | null;
  onEvent: (e: PlaceEvent) => void;
  /**
   * Called on a `resync` event. Reload the canvas snapshot and resolve with
   * its `seq`; the stream resumes after it.
   */
  onResync: () => Promise<number>;
  onState?: (s: StreamState) => void;
  signal: AbortSignal;
}

export interface PlaceClient {
  config(): Promise<Config>;
  canvas(): Promise<CanvasSnapshot>;
  branches(): Promise<BranchSummary[]>;
  createSession(): Promise<Session>;
  quota(token: string): Promise<Quota>;
  push(token: string, req: PushRequest): Promise<PushResult>;
  swarm(): Promise<SwarmStatus>;
  startSwarm(): Promise<SwarmStatus>;
  setKillSwitch(operatorToken: string, req: KillSwitchRequest): Promise<SwarmStatus>;
  /** Streams events until `signal` aborts, reconnecting with backoff. */
  events(opts: EventStreamOptions): Promise<void>;
}

export interface ClientOptions {
  /** e.g. `/api/v1` or `https://place.example.com/api/v1`. */
  baseUrl: string;
  fetch?: typeof fetch;
}

export function createPlaceClient({
  baseUrl,
  fetch: fetchImpl = globalThis.fetch.bind(globalThis),
}: ClientOptions): PlaceClient {
  const base = baseUrl.replace(/\/+$/, '');

  async function request<T>(method: 'GET' | 'POST', path: string, body?: unknown, token?: string): Promise<T> {
    const headers: Record<string, string> = { Accept: 'application/json' };
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    if (token) headers.Authorization = `Bearer ${token}`;
    let res: Response;
    try {
      res = await fetchImpl(`${base}${path}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch (err) {
      throw new ApiError(0, { message: `Could not reach the server: ${String(err)}` });
    }
    const text = await res.text();
    let json: unknown = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      // Fall through: a non-JSON body becomes a generic error below.
    }
    if (!res.ok) {
      // A body that is not JSON comes from something in front of place (the
      // load balancer's HTML 503 while place restarts) or from place's own
      // shutdown: never show it verbatim.
      const fromPlace = json !== null && typeof json === 'object';
      const errBody = (fromPlace ? json : { message: nonJsonErrorMessage(res.status) }) as Partial<ApiErrorBody>;
      throw new ApiError(res.status, errBody, res.headers.get('Retry-After'), fromPlace);
    }
    return json as T;
  }

  async function events(opts: EventStreamOptions): Promise<void> {
    const { signal, onEvent, onResync, onState } = opts;
    let after = opts.after;
    let attempt = 0;
    let serverRetry: number | null = null;
    while (!signal.aborted) {
      let resync = false;
      try {
        onState?.({ state: 'connecting' });
        const query = after === null ? '' : `?after=${after}`;
        const res = await fetchImpl(`${base}/events${query}`, { headers: { Accept: 'text/event-stream' }, signal });
        if (!res.ok || !res.body) {
          // A refused stream (429 when too many are open) says when to retry.
          await res.body?.cancel().catch(() => {});
          throw new ApiError(
            res.status,
            { message: `Event stream failed with HTTP ${res.status}` },
            res.headers.get('Retry-After'),
          );
        }
        onState?.({ state: 'open' });
        attempt = 0;
        const parser = createSseParser((m) => {
          if (resync) return;
          const e = JSON.parse(m.data) as PlaceEvent;
          if (e.type === 'resync') {
            resync = true;
            return;
          }
          after = e.seq;
          onEvent(e);
        });
        const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
        try {
          while (!resync) {
            const { value, done } = await reader.read();
            if (done) break;
            parser.push(value);
            serverRetry = parser.retryMs;
          }
        } finally {
          await reader.cancel().catch(() => {});
        }
        if (resync) {
          after = await onResync();
          continue;
        }
        throw new Error('Event stream ended');
      } catch (error) {
        if (signal.aborted) return;
        attempt++;
        const backoff = Math.min(30_000, (serverRetry ?? 1000) * 2 ** Math.min(attempt - 1, 5));
        const asked =
          error instanceof ApiError && error.retryAfterSeconds !== null ? error.retryAfterSeconds * 1000 : 0;
        const inMs = Math.max(backoff, asked) + Math.floor(Math.random() * 250);
        onState?.({ state: 'retrying', inMs, error });
        await sleep(inMs, signal);
      }
    }
  }

  return {
    config: () => request<Config>('GET', '/config'),
    canvas: () => request<CanvasSnapshot>('GET', '/canvas'),
    branches: async () => (await request<{ branches: BranchSummary[] }>('GET', '/branches')).branches,
    createSession: () => request<Session>('POST', '/sessions'),
    quota: (token) => request<Quota>('GET', '/quota', undefined, token),
    push: (token, req) => request<PushResult>('POST', '/pushes', req, token),
    swarm: () => request<SwarmStatus>('GET', '/swarm'),
    startSwarm: () => request<SwarmStatus>('POST', '/swarm'),
    setKillSwitch: (operatorToken, req) => request<SwarmStatus>('POST', '/admin/kill-switch', req, operatorToken),
    events,
  };
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const t = setTimeout(done, ms);
    function done() {
      clearTimeout(t);
      signal.removeEventListener('abort', done);
      resolve();
    }
    signal.addEventListener('abort', done, { once: true });
  });
}

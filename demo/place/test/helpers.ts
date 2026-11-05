import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createMockApi, type MockApi } from '../mock/api.ts';
import type { MockOptions } from '../mock/sim.ts';
import { createPlaceClient, type PlaceClient } from '../src/client/api.ts';

export interface Harness {
  api: MockApi;
  client: PlaceClient;
  baseUrl: string;
  clock: { now: number; advance(ms: number): void };
  close(): Promise<void>;
}

/**
 * Starts the mock on a random local port with a manual clock and no
 * background timer: tests advance time and call `api.sim.tick()` themselves.
 */
export async function startMock(options: Partial<MockOptions> = {}): Promise<Harness> {
  const clock = {
    now: Date.parse('2026-11-02T12:00:00.000Z'),
    advance(ms: number) {
      this.now += ms;
    },
  };
  const api = createMockApi({ ambientAgents: 0, seedCanvas: false, ...options, now: () => clock.now, clock: false });
  const server: Server = createServer((req, res) => api.handler(req, res));
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  const baseUrl = `http://127.0.0.1:${port}/api/v1`;
  return {
    api,
    client: createPlaceClient({ baseUrl }),
    baseUrl,
    clock,
    close: async () => {
      api.close();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

export async function waitFor(cond: () => boolean, timeoutMs = 3000, what = 'condition'): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error(`Timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 10));
  }
}

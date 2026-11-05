import type { Plugin } from 'vite';
import { createMockApi, type MockApi } from './api.ts';
import { optionsFromEnv } from './env.ts';

/**
 * Serves the mock API under /api/v1 from `vite` and `vite preview`, so one
 * process runs the whole demo. Disabled when PLACE_API_PROXY points the dev
 * server at a real back end instead.
 */
export function placeMock(): Plugin {
  let api: MockApi | null = null;
  const get = () => {
    api ??= createMockApi(optionsFromEnv(process.env));
    return api;
  };
  return {
    name: 'place-mock-api',
    apply: () => !process.env.PLACE_API_PROXY,
    configureServer(server) {
      server.middlewares.use((req, res, next) => get().handler(req, res, next));
      server.httpServer?.on('close', () => api?.close());
    },
    configurePreviewServer(server) {
      server.middlewares.use((req, res, next) => get().handler(req, res, next));
      server.httpServer?.on('close', () => api?.close());
    },
  };
}

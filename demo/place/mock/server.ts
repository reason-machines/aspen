/**
 * Runs the mock `place` API on its own:
 *
 *   npm run mock                 # http://localhost:8787/api/v1
 *   PORT=9000 PLACE_DOMAIN=aspen.dev npm run mock
 *
 * Environment: PORT, HOST, PLACE_DOMAIN, PLACE_DASHBOARD_URL ("none" for no
 * dashboard), PLACE_AGENTS (ambient agents), PLACE_KILL_SWITCH ("1" engages
 * it at start), PLACE_OPERATOR_TOKEN.
 */
import { createServer } from 'node:http';
import { API_BASE, createMockApi } from './api.ts';
import { optionsFromEnv } from './env.ts';

const port = Number(process.env.PORT ?? 8787);
const host = process.env.HOST ?? '127.0.0.1';
const api = createMockApi(optionsFromEnv(process.env));
const server = createServer((req, res) => api.handler(req, res));
server.listen(port, host, () => {
  console.log(`place mock API on http://${host}:${port}${API_BASE}`);
});
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, () => {
    api.close();
    server.close(() => process.exit(0));
  });
}

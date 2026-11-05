import type { MockOptions } from './sim.ts';

/** Reads mock settings from environment variables (see mock/server.ts). */
export function optionsFromEnv(env: Record<string, string | undefined>): Partial<MockOptions> {
  const o: Partial<MockOptions> = {};
  if (env.PLACE_DOMAIN) o.domain = env.PLACE_DOMAIN;
  if (env.PLACE_CLONE_URL) o.cloneUrl = env.PLACE_CLONE_URL;
  if (env.PLACE_DASHBOARD_URL) o.dashboardUrl = env.PLACE_DASHBOARD_URL === 'none' ? null : env.PLACE_DASHBOARD_URL;
  if (env.PLACE_AGENTS && /^[0-9]+$/.test(env.PLACE_AGENTS)) o.ambientAgents = Number(env.PLACE_AGENTS);
  if (env.PLACE_OPERATOR_TOKEN) o.operatorToken = env.PLACE_OPERATOR_TOKEN;
  if (env.PLACE_KILL_SWITCH === '1') o.killSwitch = { engaged: true, reason: 'Engaged at start (PLACE_KILL_SWITCH=1)' };
  return o;
}

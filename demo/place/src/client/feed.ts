/**
 * Turns the event stream into a readable feed. Individual pushes from people
 * and ambient agents are listed one by one; swarm pushes arrive at up to a
 * hundred or more per second, so they are folded into one running entry.
 */
import type { ActorKind, PlaceEvent } from '../shared/contract.ts';

export type FeedItem =
  | {
      kind: 'push';
      key: string;
      seq: number;
      actor: ActorKind;
      /** The actor's label: a persona's name for its `agent/` branch. */
      who: string;
      branch: string;
      commit: string;
      colors: string[];
      tiles: number;
      at: string;
      own: boolean;
    }
  | { kind: 'swarm-burst'; key: string; seq: number; pushes: number; branches: number; tiles: number; at: string }
  | {
      kind: 'merge';
      key: string;
      seq: number;
      commit: string;
      pushes: number;
      branches: number;
      tiles: number;
      at: string;
    }
  | { kind: 'swarm-state'; key: string; seq: number; text: string; at: string };

const SWARM_PREFIX = 'swarm/';

export class FeedModel {
  private items: FeedItem[] = [];
  private burstBranches = new Set<string>();
  private lastMergeThrough = 0;
  private lastSwarmState = '';

  private readonly limit: number;
  private readonly ownBranch: () => string | null;

  constructor(limit = 80, ownBranch: () => string | null = () => null) {
    this.limit = limit;
    this.ownBranch = ownBranch;
  }

  /** Newest first. */
  get list(): readonly FeedItem[] {
    return this.items;
  }

  /** Sets the merge baseline from a snapshot, so the first merge counts correctly. */
  reset(mergedThroughSeq: number): void {
    this.items = [];
    this.burstBranches.clear();
    this.lastMergeThrough = mergedThroughSeq;
    this.lastSwarmState = '';
  }

  add(e: PlaceEvent, pushesSinceLastMerge: number): void {
    switch (e.type) {
      case 'push': {
        if (e.branch.startsWith(SWARM_PREFIX)) {
          const head = this.items[0];
          if (head?.kind === 'swarm-burst') {
            this.burstBranches.add(e.branch);
            head.pushes++;
            head.tiles += e.tiles.length;
            head.branches = this.burstBranches.size;
            head.seq = e.seq;
            head.at = e.at;
            return;
          }
          this.burstBranches = new Set([e.branch]);
          this.unshift({
            kind: 'swarm-burst',
            key: `b${e.seq}`,
            seq: e.seq,
            pushes: 1,
            branches: 1,
            tiles: e.tiles.length,
            at: e.at,
          });
          return;
        }
        this.unshift({
          kind: 'push',
          key: `p${e.seq}`,
          seq: e.seq,
          actor: e.actor.kind,
          who: e.actor.label,
          branch: e.branch,
          commit: e.commit,
          colors: [...new Set(e.tiles.map((t) => t.color))].slice(0, 6),
          tiles: e.tiles.length,
          at: e.at,
          own: e.branch === this.ownBranch(),
        });
        return;
      }
      case 'merge':
        this.lastMergeThrough = e.throughSeq;
        this.unshift({
          kind: 'merge',
          key: `m${e.seq}`,
          seq: e.seq,
          commit: e.commit,
          pushes: pushesSinceLastMerge,
          branches: e.branches.length,
          tiles: e.tiles.length,
          at: e.at,
        });
        return;
      case 'swarm': {
        const s = e.status;
        const state = s.killSwitch.engaged ? 'disabled' : s.state;
        if (state === this.lastSwarmState) return;
        const prev = this.lastSwarmState;
        this.lastSwarmState = state;
        if (prev === '' && state === 'idle') return;
        const text =
          prev === 'disabled' && state !== 'running'
            ? 'Kill switch released'
            : state === 'running'
              ? `Swarm started: up to ${s.maxAgents.toLocaleString('en-US')} agents for ${s.runSeconds} s`
              : state === 'cooldown'
                ? `Swarm finished after ${s.pushes.toLocaleString('en-US')} pushes`
                : state === 'disabled'
                  ? `Swarm stopped by the kill switch${s.killSwitch.reason ? `: ${s.killSwitch.reason}` : ''}`
                  : 'Swarm ready again';
        this.unshift({ kind: 'swarm-state', key: `s${e.seq}`, seq: e.seq, text, at: e.at });
        return;
      }
      case 'expire':
      case 'resync':
        return;
    }
  }

  get mergedThrough(): number {
    return this.lastMergeThrough;
  }

  private unshift(item: FeedItem): void {
    this.items.unshift(item);
    if (this.items.length > this.limit) this.items.length = this.limit;
  }
}

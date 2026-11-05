/**
 * The `place` tile format and push rules.
 *
 * This module is the executable version of `docs/TILES.md`. The mock API uses
 * it to validate pushes, and the real pre-receive rules (plan node C7) must
 * accept and reject exactly the same inputs. Keep the two in sync: a change
 * here is a change to the public contract.
 */
import type { PaletteEntry } from './palette.ts';

/** A painted pixel. `color` is lowercase `#rrggbb` from the palette. */
export interface Tile {
  x: number;
  y: number;
  color: string;
}

export interface CanvasRules {
  width: number;
  height: number;
  palette: readonly PaletteEntry[];
  /** The most tiles one push (one commit range) may change. */
  maxTilesPerPush: number;
}

/** Stable machine-readable reasons a push or tile is rejected. */
export type RejectionCode =
  | 'outside-tiles'
  | 'bad-tile-path'
  | 'out-of-bounds'
  | 'bad-mode'
  | 'delete-not-allowed'
  | 'bad-format'
  | 'color-not-in-palette'
  | 'too-many-tiles'
  | 'duplicate-tile'
  | 'empty-stroke'
  | 'merge-commit'
  | 'protected-ref'
  | 'bad-ref';

export interface Rejection {
  code: RejectionCode;
  /** The file or ref the rejection is about, when there is one. */
  path?: string;
  /** One line, written for a person reading `git push` output. */
  message: string;
}

export type Result<T> = { ok: true; value: T } | { ok: false; rejection: Rejection };

/** The only file mode a tile may have: a regular, non-executable file. */
export const TILE_MODE = '100644';
/** A tile file is exactly this many bytes: `#rrggbb\n`. */
export const TILE_FILE_BYTES = 8;
/** The trunk branch. Only the merger bot writes it. */
export const TRUNK_REF = 'refs/heads/main';

const PATH_RE = /^tiles\/(0|[1-9][0-9]{0,4})_(0|[1-9][0-9]{0,4})\.txt$/;
const CONTENT_RE = /^#[0-9a-f]{6}\n$/;

/** The repository path of the tile at (x, y), e.g. `tiles/12_40.txt`. */
export function tilePath(x: number, y: number): string {
  return `tiles/${x}_${y}.txt`;
}

/** The exact bytes of a tile file for `color`, as text. */
export function serializeTile(color: string): string {
  return `${color}\n`;
}

/**
 * Parses a repository path into tile coordinates. Paths outside `tiles/` are
 * `outside-tiles`; paths inside it that are not `tiles/<x>_<y>.txt` with
 * canonical decimal numbers are `bad-tile-path`.
 */
export function parseTilePath(
  path: string,
  rules: Pick<CanvasRules, 'width' | 'height'>,
): Result<{ x: number; y: number }> {
  if (!path.startsWith('tiles/')) {
    return reject('outside-tiles', path, `${path}: only files under tiles/ may change`);
  }
  const m = PATH_RE.exec(path);
  if (!m) {
    return reject(
      'bad-tile-path',
      path,
      `${path}: tile files are named tiles/<x>_<y>.txt with plain decimal coordinates, e.g. tiles/12_40.txt`,
    );
  }
  const x = Number(m[1]);
  const y = Number(m[2]);
  if (x >= rules.width || y >= rules.height) {
    return reject(
      'out-of-bounds',
      path,
      `${path}: (${x}, ${y}) is outside the ${rules.width}x${rules.height} canvas; x is 0-${rules.width - 1}, y is 0-${rules.height - 1}`,
    );
  }
  return { ok: true, value: { x, y } };
}

/**
 * Parses the contents of a tile file. The only accepted form is eight bytes:
 * `#`, six lowercase hex digits naming a palette color, and one LF.
 */
export function parseTileContent(
  content: string | Uint8Array,
  palette: readonly PaletteEntry[],
  path = 'tile',
): Result<string> {
  const text = typeof content === 'string' ? content : decodeLatin1(content);
  if (!CONTENT_RE.test(text)) {
    return reject('bad-format', path, `${path}: ${describeFormatProblem(text)}`);
  }
  const color = text.slice(0, 7);
  if (!palette.some((p) => p.color === color)) {
    return reject(
      'color-not-in-palette',
      path,
      `${path}: ${color} is not in the palette; pick one of ${palette.map((p) => p.color).join(' ')}`,
    );
  }
  return { ok: true, value: color };
}

/** One file change in a commit, as the pre-receive hook sees it. */
export interface FileChange {
  path: string;
  /** The new file mode, e.g. `100644`. Absent for deletions. */
  mode?: string;
  kind: 'add' | 'modify' | 'delete';
  /** New contents. Absent for deletions. */
  content?: string | Uint8Array;
}

/** One commit in a push, with its changes against its first parent. */
export interface PushedCommit {
  oid: string;
  parentCount: number;
  changes: readonly FileChange[];
}

export type PushVerdict = { ok: true; tiles: Tile[] } | { ok: false; rejections: Rejection[] };

/**
 * Branch namespaces the server writes for its own painters (web visitors,
 * ambient agents, the swarm). A Git client may not push into them, so a
 * visitor's branch only ever moves through that visitor's session.
 */
export const RESERVED_PREFIXES = ['visitors/', 'agent/', 'swarm/'] as const;

/** Who is pushing: a Git client, or the server itself (web, agents, merger). */
export type Pusher = 'git' | 'server';

/**
 * Checks a ref update. Git clients may create, update or delete any branch
 * except the trunk and the server's own namespaces; tags and other refs are
 * refused.
 */
export function validateRef(ref: string, pusher: Pusher = 'git'): Rejection | null {
  if (ref === TRUNK_REF) {
    return {
      code: 'protected-ref',
      path: ref,
      message: `${ref}: main is written by the merger bot; push to your own branch, e.g. git push origin HEAD:refs/heads/paint/<you>`,
    };
  }
  if (!ref.startsWith('refs/heads/') || ref.length > 'refs/heads/'.length + 100) {
    return {
      code: 'bad-ref',
      path: ref,
      message: `${ref}: only branches (refs/heads/*, at most 100 characters) can be pushed`,
    };
  }
  const short = ref.slice('refs/heads/'.length);
  if (pusher === 'git' && RESERVED_PREFIXES.some((p) => short.startsWith(p))) {
    return {
      code: 'protected-ref',
      path: ref,
      message: `${ref}: branches under ${RESERVED_PREFIXES.join(', ')} belong to the web page and its agents; push to your own branch, e.g. refs/heads/paint/<you>`,
    };
  }
  return null;
}

/** A commit as the range computation sees it: its id and parent ids. */
export interface CommitNode {
  oid: string;
  parents: readonly string[];
}

/**
 * The commits a branch update is checked over, oldest first (parents before
 * children): those reachable from `newTip` and from none of `exclude`, i.e.
 * `git rev-list --topo-order --reverse <newTip> ^<trunk tip> ^<stored tip>`.
 *
 * The server passes its own trunk tip and the branch's currently stored tip,
 * never the client's old-oid: Aspen receives updates to unadvertised branches
 * with a zero old-oid, so trusting it would recount a branch's earlier,
 * already-accepted commits toward the tile cap. Pass `null` for a branch that
 * does not exist yet. Force-pushes and rewritten branches use the same rule.
 */
export function pushRange(
  graph: ReadonlyMap<string, CommitNode>,
  newTip: string,
  exclude: readonly (string | null)[],
): CommitNode[] {
  const parentsOf = (oid: string): readonly string[] => {
    const node = graph.get(oid);
    if (!node) throw new Error(`unknown commit ${oid}`);
    return node.parents;
  };
  const excluded = new Set<string>();
  const stack = exclude.filter((oid): oid is string => oid !== null);
  while (stack.length > 0) {
    const oid = stack.pop() as string;
    if (excluded.has(oid)) continue;
    excluded.add(oid);
    stack.push(...parentsOf(oid));
  }
  // Iterative post-order walk, so parents come out before their children.
  const out: CommitNode[] = [];
  const done = new Set<string>(excluded);
  const work: { oid: string; expanded: boolean }[] = [{ oid: newTip, expanded: false }];
  while (work.length > 0) {
    const item = work.pop() as { oid: string; expanded: boolean };
    if (done.has(item.oid)) continue;
    if (item.expanded) {
      done.add(item.oid);
      out.push(graph.get(item.oid) as CommitNode);
      continue;
    }
    work.push({ oid: item.oid, expanded: true });
    for (const parent of [...parentsOf(item.oid)].reverse()) {
      if (!done.has(parent)) work.push({ oid: parent, expanded: false });
    }
  }
  return out;
}

/**
 * The pre-receive rules for the commits a push adds to one branch, oldest
 * first, as chosen by `pushRange`. Every commit must be a non-merge commit whose changes are tile
 * additions or modifications in the fixed format; the push as a whole may
 * touch at most `maxTilesPerPush` distinct tiles. Returns the resulting tile
 * colors (last write wins) or every rejection found.
 */
export function validatePush(commits: readonly PushedCommit[], rules: CanvasRules): PushVerdict {
  const rejections: Rejection[] = [];
  const tiles = new Map<string, Tile>();
  for (const commit of commits) {
    if (commit.parentCount > 1) {
      rejections.push({
        code: 'merge-commit',
        path: commit.oid,
        message: `${commit.oid.slice(0, 12)}: merge commits are not accepted; rebase onto origin/main instead`,
      });
      continue;
    }
    for (const change of commit.changes) {
      const r = validateChange(change, rules);
      if (!r.ok) {
        rejections.push(r.rejection);
        continue;
      }
      tiles.set(`${r.value.x},${r.value.y}`, r.value);
    }
  }
  if (tiles.size > rules.maxTilesPerPush) {
    rejections.push({
      code: 'too-many-tiles',
      message: `this push changes ${tiles.size} tiles; the limit is ${rules.maxTilesPerPush} per push`,
    });
  }
  return rejections.length > 0 ? { ok: false, rejections } : { ok: true, tiles: [...tiles.values()] };
}

/**
 * The rules for a stroke painted through the web API. A stroke is the same
 * thing as a one-commit push, so the same limits apply; it additionally may
 * not name one tile twice.
 */
export function validateStroke(tiles: readonly Tile[], rules: CanvasRules): PushVerdict {
  const rejections: Rejection[] = [];
  const seen = new Set<string>();
  const ok: Tile[] = [];
  for (const t of tiles) {
    const path =
      Number.isSafeInteger(t.x) && Number.isSafeInteger(t.y) && t.x >= 0 && t.y >= 0
        ? tilePath(t.x, t.y)
        : `tiles/${String(t.x)}_${String(t.y)}.txt`;
    const p = parseTilePath(path, rules);
    if (!p.ok) {
      rejections.push(p.rejection);
      continue;
    }
    const c = parseTileContent(typeof t.color === 'string' ? serializeTile(t.color) : '', rules.palette, path);
    if (!c.ok) {
      rejections.push(c.rejection);
      continue;
    }
    if (seen.has(path)) {
      rejections.push({ code: 'duplicate-tile', path, message: `${path}: painted twice in one stroke` });
      continue;
    }
    seen.add(path);
    ok.push({ x: p.value.x, y: p.value.y, color: c.value });
  }
  if (tiles.length === 0) {
    rejections.push({ code: 'empty-stroke', message: 'a stroke must paint at least one tile' });
  }
  if (tiles.length > rules.maxTilesPerPush) {
    rejections.push({
      code: 'too-many-tiles',
      message: `this stroke paints ${tiles.length} tiles; the limit is ${rules.maxTilesPerPush} per push`,
    });
  }
  return rejections.length > 0 ? { ok: false, rejections } : { ok: true, tiles: ok };
}

function validateChange(change: FileChange, rules: CanvasRules): Result<Tile> {
  const p = parseTilePath(change.path, rules);
  if (!p.ok) return p;
  if (change.kind === 'delete') {
    return reject(
      'delete-not-allowed',
      change.path,
      `${change.path}: tiles can be repainted but not deleted; paint it #ffffff instead`,
    );
  }
  if (change.mode !== TILE_MODE) {
    return reject(
      'bad-mode',
      change.path,
      `${change.path}: tiles must be regular files (mode ${TILE_MODE}), not mode ${change.mode ?? 'unknown'}`,
    );
  }
  const c = parseTileContent(change.content ?? '', rules.palette, change.path);
  if (!c.ok) return c;
  return { ok: true, value: { x: p.value.x, y: p.value.y, color: c.value } };
}

function describeFormatProblem(text: string): string {
  const expected = 'expected exactly "#rrggbb" and one newline, e.g. "#d6262b\\n"';
  if (text.length === 0) return `the file is empty; ${expected}`;
  if (text.startsWith('\u00ef\u00bb\u00bf') || text.startsWith('\ufeff'))
    return `remove the byte-order mark; ${expected}`;
  if (text.includes('\r')) return `use LF line endings, not CRLF; ${expected}`;
  if (/^#[0-9a-fA-F]{6}$/.test(text)) return `end the file with one newline; ${expected}`;
  if (/^#[0-9a-fA-F]{6}\n$/.test(text)) return `use lowercase hex digits; ${expected}`;
  if (/^[0-9a-fA-F]{6}\n?$/.test(text)) return `start the color with "#"; ${expected}`;
  if (text.length > TILE_FILE_BYTES) return `the file has extra content (${text.length} bytes); ${expected}`;
  return expected;
}

function decodeLatin1(bytes: Uint8Array): string {
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return s;
}

function reject<T>(code: RejectionCode, path: string, message: string): Result<T> {
  return { ok: false, rejection: { code, path, message } };
}

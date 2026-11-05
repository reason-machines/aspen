import { describe, expect, it } from 'vitest';
import { DEFAULT_PALETTE } from '../src/shared/palette.ts';
import {
  type CanvasRules,
  type FileChange,
  parseTileContent,
  parseTilePath,
  pushRange,
  serializeTile,
  TILE_FILE_BYTES,
  tilePath,
  validatePush,
  validateRef,
  validateStroke,
} from '../src/shared/tiles.ts';

const rules: CanvasRules = { width: 64, height: 48, palette: DEFAULT_PALETTE, maxTilesPerPush: 4 };
const red = '#d6262b';

const add = (path: string, content: string | Uint8Array, mode = '100644'): FileChange => ({
  path,
  mode,
  kind: 'add',
  content,
});

describe('tile paths', () => {
  it('round-trips every corner of the canvas', () => {
    for (const [x, y] of [
      [0, 0],
      [63, 0],
      [0, 47],
      [63, 47],
      [12, 40],
    ]) {
      const p = parseTilePath(tilePath(x, y), rules);
      expect(p).toEqual({ ok: true, value: { x, y } });
    }
  });

  it.each([
    ['README.md', 'outside-tiles'],
    ['tile/1_2.txt', 'outside-tiles'],
    ['./tiles/1_2.txt', 'outside-tiles'],
    ['tiles/01_2.txt', 'bad-tile-path'],
    ['tiles/1_02.txt', 'bad-tile-path'],
    ['tiles/-1_2.txt', 'bad-tile-path'],
    ['tiles/1_2.TXT', 'bad-tile-path'],
    ['tiles/1_2.txt.bak', 'bad-tile-path'],
    ['tiles/1-2.txt', 'bad-tile-path'],
    ['tiles/sub/1_2.txt', 'bad-tile-path'],
    ['tiles/1_2', 'bad-tile-path'],
    ['tiles/ 1_2.txt', 'bad-tile-path'],
    ['tiles/1e1_2.txt', 'bad-tile-path'],
    ['tiles/123456_2.txt', 'bad-tile-path'],
    ['tiles/64_0.txt', 'out-of-bounds'],
    ['tiles/0_48.txt', 'out-of-bounds'],
  ])('rejects %s as %s', (path, code) => {
    const p = parseTilePath(path, rules);
    expect(p.ok).toBe(false);
    if (!p.ok) {
      expect(p.rejection.code).toBe(code);
      expect(p.rejection.path).toBe(path);
    }
  });
});

describe('tile contents', () => {
  it('accepts exactly "#rrggbb\\n" with a palette color', () => {
    expect(serializeTile(red)).toBe('#d6262b\n');
    expect(serializeTile(red)).toHaveLength(TILE_FILE_BYTES);
    expect(parseTileContent(serializeTile(red), DEFAULT_PALETTE)).toEqual({ ok: true, value: red });
  });

  it('accepts raw bytes', () => {
    const bytes = new TextEncoder().encode('#3690ea\n');
    expect(parseTileContent(bytes, DEFAULT_PALETTE)).toEqual({ ok: true, value: '#3690ea' });
  });

  it('accepts every palette color', () => {
    for (const p of DEFAULT_PALETTE) {
      expect(parseTileContent(serializeTile(p.color), DEFAULT_PALETTE).ok).toBe(true);
    }
  });

  it.each([
    ['', 'empty'],
    ['#d6262b', 'newline'],
    ['#D6262B\n', 'lowercase'],
    ['d6262b\n', '"#"'],
    ['#d6262b\r\n', 'CRLF'],
    ['\ufeff#d6262b\n', 'byte-order mark'],
    ['#d6262b\n\n', 'extra content'],
    ['#d6262b \n', 'expected exactly'],
    [' #d6262b\n', 'expected exactly'],
    ['#d626\n', 'expected exactly'],
    ['#d6262bff\n', 'extra content'],
    ['red\n', 'expected exactly'],
  ])('rejects %j as bad-format with a hint about %s', (content, hint) => {
    const r = parseTileContent(content, DEFAULT_PALETTE, 'tiles/1_1.txt');
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.rejection.code).toBe('bad-format');
      expect(r.rejection.message).toContain(hint);
      expect(r.rejection.message.startsWith('tiles/1_1.txt: ')).toBe(true);
    }
  });

  it('rejects a well-formed color that is not in the palette', () => {
    const r = parseTileContent('#123456\n', DEFAULT_PALETTE);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.rejection.code).toBe('color-not-in-palette');
  });

  it('rejects a UTF-8 BOM given as bytes', () => {
    const bytes = new Uint8Array([0xef, 0xbb, 0xbf, ...new TextEncoder().encode('#d6262b\n')]);
    const r = parseTileContent(bytes, DEFAULT_PALETTE);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.rejection.message).toContain('byte-order mark');
  });
});

describe('push validation (pre-receive rules)', () => {
  it('accepts tile additions and modifications, last write wins', () => {
    const v = validatePush(
      [
        {
          oid: 'a'.repeat(40),
          parentCount: 1,
          changes: [add('tiles/1_1.txt', '#d6262b\n'), add('tiles/2_1.txt', '#3690ea\n')],
        },
        {
          oid: 'b'.repeat(40),
          parentCount: 1,
          changes: [{ path: 'tiles/1_1.txt', mode: '100644', kind: 'modify', content: '#ffd635\n' }],
        },
      ],
      rules,
    );
    expect(v.ok).toBe(true);
    if (v.ok) {
      expect(v.tiles).toEqual([
        { x: 1, y: 1, color: '#ffd635' },
        { x: 2, y: 1, color: '#3690ea' },
      ]);
    }
  });

  it('collects every rejection, not just the first', () => {
    const v = validatePush(
      [
        {
          oid: 'c'.repeat(40),
          parentCount: 1,
          changes: [
            add('README.md', 'hi\n'),
            add('tiles/1_1.txt', '#d6262b\n', '100755'),
            add('tiles/1_2.txt', '#d6262b\n', '120000'),
            { path: 'tiles/3_3.txt', kind: 'delete' },
            add('tiles/4_4.txt', '#D6262B\n'),
          ],
        },
        { oid: 'd'.repeat(40), parentCount: 2, changes: [] },
      ],
      rules,
    );
    expect(v.ok).toBe(false);
    if (!v.ok) {
      expect(v.rejections.map((r) => r.code)).toEqual([
        'outside-tiles',
        'bad-mode',
        'bad-mode',
        'delete-not-allowed',
        'bad-format',
        'merge-commit',
      ]);
    }
  });

  it('limits distinct tiles per push across all commits', () => {
    const commits = [0, 1, 2, 3, 4].map((i) => ({
      oid: String(i).repeat(40),
      parentCount: 1,
      changes: [add(tilePath(i, 0), '#d6262b\n')],
    }));
    const v = validatePush(commits, rules);
    expect(v.ok).toBe(false);
    if (!v.ok) expect(v.rejections.map((r) => r.code)).toEqual(['too-many-tiles']);
    // Repainting the same tile many times counts once.
    const same = [0, 1, 2, 3, 4].map((i) => ({
      oid: String(i).repeat(40),
      parentCount: 1,
      changes: [add(tilePath(0, 0), '#d6262b\n')],
    }));
    expect(validatePush(same, rules).ok).toBe(true);
  });

  it('reports a rename as a delete plus an add, so it is refused', () => {
    const v = validatePush(
      [
        {
          oid: 'e'.repeat(40),
          parentCount: 1,
          changes: [{ path: 'tiles/1_1.txt', kind: 'delete' }, add('tiles/2_2.txt', '#d6262b\n')],
        },
      ],
      rules,
    );
    expect(v.ok).toBe(false);
    if (!v.ok) expect(v.rejections.map((r) => r.code)).toEqual(['delete-not-allowed']);
  });

  it('protects main and refuses non-branch refs', () => {
    expect(validateRef('refs/heads/main')?.code).toBe('protected-ref');
    expect(validateRef('refs/tags/v1')?.code).toBe('bad-ref');
    expect(validateRef(`refs/heads/${'x'.repeat(101)}`)?.code).toBe('bad-ref');
    expect(validateRef('refs/heads/paint/ada')).toBeNull();
  });

  it("keeps Git clients out of the server's own branch namespaces", () => {
    for (const ref of ['refs/heads/visitors/k3x9', 'refs/heads/agent/moss-12', 'refs/heads/swarm/agent-0001']) {
      expect(validateRef(ref)?.code).toBe('protected-ref');
      expect(validateRef(ref, 'server')).toBeNull();
    }
    expect(validateRef('refs/heads/main', 'server')?.code).toBe('protected-ref');
    expect(validateRef('refs/heads/visitorsx/a')).toBeNull();
  });
});

describe('push range (which commits a branch update is checked over)', () => {
  // An in-memory repository: commit id -> parents and the tiles it paints.
  const graph = new Map<string, { oid: string; parents: string[] }>();
  const paints = new Map<string, number[]>();
  const commit = (oid: string, parents: string[], xs: number[] = []) => {
    graph.set(oid, { oid, parents });
    paints.set(oid, xs);
  };
  const check = (oids: readonly { oid: string }[]) =>
    validatePush(
      oids.map(({ oid }) => ({
        oid,
        parentCount: graph.get(oid)?.parents.length ?? 0,
        changes: (paints.get(oid) ?? []).map((x) => add(tilePath(x, 0), '#d6262b\n')),
      })),
      rules, // maxTilesPerPush: 4
    );

  commit('t0', []); // trunk when the visitor cloned
  commit('a1', ['t0'], [0, 1, 2]); // first push to refs/heads/paint/ada
  commit('t1', ['t0']); // merger bot fast-forwards trunk meanwhile
  commit('a2', ['a1'], [3, 4]); // second push to the same branch
  commit('b1', ['t0'], [5, 6, 7, 8]); // rewritten branch, force-pushed

  it('counts only the commits new to the branch, not its earlier pushes', () => {
    // First push: the branch does not exist yet.
    const first = pushRange(graph, 'a1', ['t0', null]);
    expect(first.map((c) => c.oid)).toEqual(['a1']);
    expect(check(first).ok).toBe(true);

    // Second push arrives with a zero old-oid (the branch is unadvertised),
    // so the server excludes its own stored tip, a1. 2 tiles: accepted.
    const second = pushRange(graph, 'a2', ['t1', 'a1']);
    expect(second.map((c) => c.oid)).toEqual(['a2']);
    expect(check(second).ok).toBe(true);

    // Trusting the zero old-oid would recount a1: 5 tiles, wrongly refused.
    const naive = pushRange(graph, 'a2', ['t1', null]);
    expect(naive.map((c) => c.oid)).toEqual(['a1', 'a2']);
    const v = check(naive);
    expect(v.ok).toBe(false);
    if (!v.ok) expect(v.rejections.map((r) => r.code)).toEqual(['too-many-tiles']);
  });

  it('applies the same rule to a force-push', () => {
    const forced = pushRange(graph, 'b1', ['t1', 'a2']);
    expect(forced.map((c) => c.oid)).toEqual(['b1']);
    expect(check(forced).ok).toBe(true);
  });

  it('orders parents before children, including across a merge', () => {
    const g = new Map([
      ['r', { oid: 'r', parents: [] }],
      ['x', { oid: 'x', parents: ['r'] }],
      ['y', { oid: 'y', parents: ['r'] }],
      ['m', { oid: 'm', parents: ['x', 'y'] }],
    ]);
    expect(pushRange(g, 'm', ['r']).map((c) => c.oid)).toEqual(['x', 'y', 'm']);
    expect(pushRange(g, 'm', ['y']).map((c) => c.oid)).toEqual(['x', 'm']);
    expect(pushRange(g, 'r', ['m'])).toEqual([]);
  });
});

describe('stroke validation (web API)', () => {
  it('accepts a valid stroke', () => {
    expect(validateStroke([{ x: 1, y: 2, color: red }], rules)).toEqual({
      ok: true,
      tiles: [{ x: 1, y: 2, color: red }],
    });
  });

  it('rejects empty, oversized, duplicate, fractional and off-palette strokes', () => {
    const codes = (tiles: Parameters<typeof validateStroke>[0]) => {
      const v = validateStroke(tiles, rules);
      return v.ok ? [] : v.rejections.map((r) => r.code);
    };
    expect(codes([])).toEqual(['empty-stroke']);
    expect(codes([0, 1, 2, 3, 4].map((x) => ({ x, y: 0, color: red })))).toEqual(['too-many-tiles']);
    expect(
      codes([
        { x: 1, y: 1, color: red },
        { x: 1, y: 1, color: red },
      ]),
    ).toEqual(['duplicate-tile']);
    expect(codes([{ x: 1.5, y: 1, color: red }])).toEqual(['bad-tile-path']);
    expect(codes([{ x: -1, y: 1, color: red }])).toEqual(['bad-tile-path']);
    expect(codes([{ x: 99, y: 1, color: red }])).toEqual(['out-of-bounds']);
    expect(codes([{ x: 1, y: 1, color: '#D6262B' }])).toEqual(['bad-format']);
    expect(codes([{ x: 1, y: 1, color: '#123456' }])).toEqual(['color-not-in-palette']);
  });
});

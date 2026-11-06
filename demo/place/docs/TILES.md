# place tile format and push rules

The `place` canvas is an ordinary Git repository. This document is the
normative description of what a push to it may contain. The back end's
pre-receive check (plan node C7) must accept and reject exactly what is
described here. [`src/shared/tiles.ts`](../src/shared/tiles.ts) is an
executable copy of these rules, and
[`test/tiles.test.ts`](../test/tiles.test.ts) pins the edge cases; C7 can port
those cases as its own tests.

## Repository layout

```
canvas/
  tiles/
    0_0.txt
    12_40.txt
    ...
```

- The canvas is `width` x `height` tiles; both come from `GET /api/v1/config`
  (64 x 64 in the default deployment). `x` grows to the right, `y` grows
  downwards, and (0, 0) is the top-left tile.
- A tile that has never been painted has no file.
- `main` may contain other files written by the operators, such as a README.
  Pushes may not change them.

## Tile files

A tile file is at `tiles/<x>_<y>.txt`, where:

- `x` and `y` are plain decimal integers: no sign, no leading zeros (`0` is
  fine, `07` is not), at most five digits;
- `0 <= x < width` and `0 <= y < height`;
- the file is directly in `tiles/`, not in a subdirectory, and the extension
  is lowercase `.txt`.

Its contents are exactly 8 bytes:

```
#rrggbb\n
```

- `#`, then six **lowercase** hexadecimal digits, then one line feed (0x0A);
- the color must be one of the palette colors in `GET /api/v1/config`;
- no byte-order mark, no carriage return, no trailing spaces, no second line.

For example, to paint tile (12, 40) red:

```bash
printf '#d6262b\n' > tiles/12_40.txt
```

Avoid `echo` on systems where it may not write a single `\n`, and editors that
add a byte-order mark or CRLF line endings.

The file mode must be `100644` (a regular, non-executable file). Symlinks
(`120000`), executables (`100755`) and submodules (`160000`) are rejected.

## Pushes

A push is checked per updated ref, over the commits it adds to that ref.

### Which commits are checked

"The commits it adds" means the commits reachable from the pushed new tip
and from neither the trunk tip nor the branch's currently stored tip:

```bash
git rev-list --topo-order --reverse <new tip> ^<trunk tip> ^<stored branch tip>
```

The server reads both excluded tips from its own ref store at check time.
It never uses the old-oid the client sent: Aspen receives updates to
branches it did not advertise with a zero old-oid (spike S2), and a client
may also send a stale one. Without the stored tip, a branch's second push
would recount the tiles of its first push toward the cap below and be
wrongly refused. When the branch does not exist yet there is no stored tip,
and only the trunk is excluded. [`pushRange`](../src/shared/tiles.ts)
implements this rule over an in-memory commit graph.

Worked example, with the default cap of 64 tiles:

1. Ada clones at trunk `T0`, commits `A1` painting 40 tiles and pushes to
   `refs/heads/paint/ada`. The branch is new, so the range is
   `A1 ^T0` = {`A1`}: 40 tiles, accepted. The stored tip is now `A1`.
2. The merger bot moves trunk to `T1`. Ada commits `A2` on top of `A1`,
   painting 30 more tiles, and pushes. Her client sends a zero old-oid. The
   range is `A2 ^T1 ^A1` = {`A2`}: 30 tiles, accepted. Counting from the
   zero old-oid (`A2 ^T1`) would include `A1` and refuse 70 tiles.
3. Ada rewrites her branch as `B1` on `T0` (60 tiles) and force-pushes. The
   same rule applies: `B1 ^T1 ^A2` = {`B1`}: 60 tiles, accepted.

A rename of a tile file shows up as a delete plus an add, so it is refused
with `delete-not-allowed`.

### Rules

| Rule | Rejection code |
|---|---|
| Only branches (`refs/heads/*`) can be pushed; tags and other refs are refused. Branch names are at most 100 characters after `refs/heads/`. | `bad-ref` |
| `refs/heads/main` belongs to the merger bot. Push to any other branch. | `protected-ref` |
| Branches under `refs/heads/visitors/`, `refs/heads/agent/` and `refs/heads/swarm/` belong to the web page's visitors and the server's agents; Git clients may not push to them. | `protected-ref` |
| Deleting a branch (new tip all zeros) is allowed; no commit rules apply. Whether a given client may delete or force-push a branch is Aspen's ref-update policy (spike S2), not a tile rule. | (none) |
| Every changed path is under `tiles/`. | `outside-tiles` |
| Every path under `tiles/` is a valid tile name. | `bad-tile-path` |
| Every tile is inside the canvas. | `out-of-bounds` |
| Tile files are added or modified, never deleted. To clear a tile, paint it `#ffffff`. | `delete-not-allowed` |
| Tile files have mode `100644`. | `bad-mode` |
| Tile files have the exact format above. | `bad-format` |
| The color is in the palette. | `color-not-in-palette` |
| Merge commits are refused; rebase onto `origin/main` instead. | `merge-commit` |
| One push changes at most `limits.tilesPerPush` distinct tiles (64 by default), counted across all its commits. Repainting one tile several times counts once. | `too-many-tiles` |

Each commit's changes are taken against its first parent. The check reports
every problem it finds, not just the first, one line per problem, in the form
`<path>: <what is wrong and how to fix it>`, so the `git push` output tells
people what to change.

Strokes painted through the web API (`POST /api/v1/pushes`) are one commit
and follow the same rules. A stroke additionally may not name one tile twice
(`duplicate-tile`) and must paint at least one tile (`empty-stroke`).

## Quotas and expiry

These are enforced by the server, not by the pre-receive rules:

- pushes per session (web) or per client address and branch (Git) per
  rolling minute: `limits.pushesPerMinute`. A signed-in Git client counts
  by its identity instead of its address, and an IPv6 address counts by
  its /64;
- the back end refuses a push that adds more than 1,000 commits to a branch
  or whose diffs visit more than 200,000 tree entries ("push fewer at a
  time"); these are limits of the check, not tile rules, so they have no
  rejection code. The tree-entry budget is the one you reach first: each
  commit that changes tiles reads its own `tiles/` tree and its parent's
  in full (one entry per painted tile, up to 4,096 each on the default
  64 x 64 canvas), so once the canvas is full about 24 such commits fit in
  one push. Squash a long history before pushing it;
- a branch with no pushes for `limits.branchTtlSeconds`, whose tiles have all
  merged, is deleted.

## Merging

The merger bot never does a Git merge of branches, and C7 does not need
`git merge`. It runs every `mergeIntervalMs` (less often under load, up to a
server-set maximum), takes the tile set of every
push accepted since its last run (the result `validatePush` returned for
it), and replays those tile sets onto the trunk tree in `seq` order. It then
writes one new commit whose only parent is the current trunk tip and
fast-forwards `main` to it. When two pushes paint the same tile, the one
with the higher `seq` wins. Trunk history is therefore linear and
fast-forwards, except when the server starts it again: once `main`'s
history outgrows a server-set size, the merger replaces it with one root
commit holding the same canvas (a forced update for earlier clones), after
merging what is pending. Pushes built on an earlier `main` are still
checked from the `main` commit they build on, so clones made before keep
pushing. Branches are left as they are until they expire. The canvas
on the page is `main` plus each branch's tiles that have not merged yet.

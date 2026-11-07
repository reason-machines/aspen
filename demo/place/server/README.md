# place back end (`aspen-place`)

The back end of the `place` demo (plan node C7): a shared pixel canvas
stored in a real Git repository on Aspen. One process serves Git over HTTP
(`/canvas.git`), the page's API (`/api/v1`, as specified in
[`../api/openapi.yaml`](../api/openapi.yaml)), the merger bot and the
simulated agents.

## How it works

- **Tile rules run inside receive-pack.** `aspen-place` gives Aspen's
  receive-pack a push hook (`aspen_server::receive::hook`) that checks
  every branch update against [`../docs/TILES.md`](../docs/TILES.md). The
  hook sees the commits `git rev-list <new> ^<trunk tip> ^<stored tip>`
  lists, with both excluded tips read by the server, never the client's old
  value (Aspen receives updates to unadvertised branches with a zero old
  value). A refused push gets one `remote: aspen: ...` line per problem.
- **Every write is a push.** A web stroke becomes one commit on the
  visitor's branch, pushed in process through the branch-scoped URL with
  the branch's real tip as the old value. The merger bot replays every
  pending push's tiles onto `main`'s tiles, in push order, and pushes one
  commit whose only parent is `main`'s tip. Expired branches are deleted
  with a compare-and-swap delete. Nothing writes refs or packs directly, so
  everything the server writes is stored exactly like a client's push.
- **One sequence for the page.** Every push, merge, branch expiry and swarm
  update gets a number from one counter, and enters the event backlog and
  the live stream in one step, so `GET /canvas`, `GET /events?after=<seq>`
  and push responses always line up.
- **Agents.** Ambient agents paint small pixel sprites (a heart, a star, a
  rocket…, `src/sprites.rs`), one at a time, where they cover nothing people
  or other sprites painted in the last six hours, and stop when the canvas
  has no such room. Merge commits record which tiles people and sprites
  painted (`People-tiles:` and `Sprite-tiles:` trailers), and a restarted
  server rebuilds that memory from `main`. With `PLACE_PERSONAS` on (the
  default), personas replace them (`src/personas.rs`): 3 to 6 of a
  roster of 13 pretend painters are always on a shift, each painting a
  picture it loves one tile per push, every few seconds, on its own
  `agent/<name>-<n>` branch with its own commit author, so the feed shows
  them as agents. They never paint a tile people painted in the last six
  hours or a keep area, move a picture when a person paints into it, leave
  a just-finished picture alone for ten minutes, take no token of the
  server-wide push ceiling and stop while less than a tenth of its burst
  is left, so people get it; together they push about once a
  second, at most `PLACE_PERSONAS_PUSHES_PER_SECOND`. Personas run
  permanently, starting automatically on every boot and deployment. They
  have no end date, run duration, total push allowance, token allowance,
  or visitor session budget. Shifts rotate painters, not the service.
  Rate credit refills continuously; ceiling, no-room and quiet pauses retry
  automatically. Failed strokes retry the same tile with exponential
  backoff from one second to one minute, with no attempt limit; success
  resets the backoff. Only the kill switch or explicit configuration
  disables them. Restarting starts fresh shifts and restores the canvas
  and memory of people's recent paint from Git, not a saved visitor session.
  The
  "Unleash 1,000 agents" swarm is one global run with server-side limits:
  `maxAgents`, `pushesPerSecondCap`, run length, cooldown and starts per
  client address apply whatever a client sends. A start checks the kill
  switch, the run state and the address's starts and starts the run in one
  step, so concurrent starts spend one start between them, and a refused
  start spends none. Agent pushes that find no free slot
  (`PLACE_AGENT_CONCURRENCY`) are dropped, not queued.
- **Aspen's background worker** runs in the same process, as in `aspen
  serve`: it compacts the packs every push stores into covers, indexes
  commits, builds exact shallow packs for `--depth` clones, and collects
  garbage. Without it every stroke stays its own pack and a clone stitches
  one pack per push the canvas ever took.
- **Event streams** are capped per client address and in all, and each
  ends after a maximum age; clients resume with `after=<seq>` (#124).
- **Kill switch.** `POST /api/v1/admin/kill-switch` needs an operator
  credential: an Aspen API token or OIDC token with an `admin` grant on the
  canvas repository. Engaging it ends a running swarm at once, refuses new
  starts, and pauses the ambient agents and the personas. It lives in the
  task's memory: a deploy starts a new task with it released (unless
  `PLACE_KILL_SWITCH=1`), and while two tasks overlap each has its own. Do
  not deploy during an incident that needs it engaged; if you must, set
  `PLACE_KILL_SWITCH=1` for that deploy.

- **Bounded history.** Every merge stores a whole `tiles/` tree (9 to 25
  KiB on a painted 64 x 64 canvas: stored packs have no deltas), so a full
  clone of `main` would grow with every merge. Once the packs written on
  `main` since its root reach `PLACE_HISTORY_MAX_BYTES` (8 MiB), the
  merger merges what is pending and force-updates `main` to one root
  commit holding the canvas as it is: a full clone stays about that size.
  The picture does not change and no push is lost. Branches are kept, and
  a clone made before the reset still pushes: the tile check stops at
  commits of any earlier `main` (found through Aspen's commit index), as it
  stops at the current one. The root commit names the old tip, and the old
  history stays in the repository (`git fetch <url> <tip>`); earlier clones
  see a forced update of `main`. Each commit on `main` records the size so
  far in a `Place-History:` trailer, so a restart keeps counting; a `main`
  written before the server counted is started again after its next merge.
  A branch built on an earlier history still reaches it, so a full clone
  fetches that history too until the branch expires; `--depth=1` clones
  never do.
- **Merge cadence.** The merger runs every `PLACE_MERGE_INTERVAL_MS`
  while pushes arrive at up to `PLACE_MERGE_BUSY_PUSHES_PER_SECOND`, and
  waits longer in proportion above that, up to
  `PLACE_MERGE_INTERVAL_MAX_MS`: at the defaults every 3 s up to 5 pushes a
  second, every 6 s at 10, every 30 s from 50. A merge costs the same
  however many pushes it folds, so under load the merger's work and
  `main`'s growth stay flat; the page shows pushes not merged yet as branch
  overlays.
- **Reset history now.** `POST /api/v1/admin/reset-history` (operator
  credential, no body) does the same as the automatic reset, and also
  deletes every branch whose tiles have all merged, so a full clone
  fetches one commit. It is an operator tool outside the page's API
  contract.

Branch namespaces `visitors/`, `agent/` and `swarm/` belong to the server's
own painters; `agent/` branches are deleted as soon as their tiles have
merged and `swarm/` branches 30 s after (the next stroke starts them again
from `main`), so clones stop fetching their merged strokes; Git clients push anywhere else under `refs/heads/` except
`main`.

## Running it

The `place` image (`deploy/docker/Dockerfile`, target `place`) ships the
binary and the built page; [deploy/terraform/aws/public-demo](../../../deploy/terraform/aws/public-demo)
runs it on AWS. From a checkout:

```bash
cargo run -p aspen-place -- serve          # http://127.0.0.1:7070
git clone http://127.0.0.1:7070/canvas.git
cd canvas && printf '#d6262b\n' > tiles/3_4.txt
git add tiles && git commit -m paint && git push origin HEAD:refs/heads/paint/me
```

Point the page at it from `demo/place`:

```bash
PLACE_API_PROXY=http://127.0.0.1:7070 npm run dev
```

Issue an operator token (printed once; only its digest is stored):

```bash
aspen-place operator-token ops@example.com
curl -X POST http://127.0.0.1:7070/api/v1/admin/kill-switch \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"engaged": true, "reason": "Maintenance"}'
```

### Settings

| Variable | Default | Effect |
|---|---|---|
| `PLACE_LISTEN` | `127.0.0.1:7070` | Address to serve on. |
| `PLACE_DATA_DIR` | `.place` | SQLite database, local blobs and the push spool, unless set below. |
| `PLACE_DATABASE_URL` | SQLite in the data directory | `postgres://...` (password in `PGPASSWORD`) or `sqlite://...`. |
| `PLACE_DB_MAX_CONNECTIONS` | `10` | Connections in the metadata database's pool. Size it to the database: the public demo's Terraform sets 64 on an 8-vCPU PostgreSQL. |
| `PLACE_BLOB_URL` | `file://` in the data directory | `s3://bucket/prefix` or `file:///path`. |
| `PLACE_SPOOL_DIR` | `<data dir>/spool` | Push spool; a real disk, not tmpfs. |
| `PLACE_DOMAIN` | `example.com` | Clone URL `https://place.<domain>/canvas.git`, dashboard `https://dashboard.<domain>/`. |
| `PLACE_CLONE_URL`, `PLACE_DASHBOARD_URL` | | Override those; `PLACE_DASHBOARD_URL=none` hides the dashboard link. |
| `PLACE_WIDTH`, `PLACE_HEIGHT` | `64` | Canvas size, 1 to 4096. |
| `PLACE_TILES_PER_PUSH` | `64` | Distinct tiles one push may change. |
| `PLACE_PUSHES_PER_MINUTE` | `30` | Per web session, and per Git caller and branch. |
| `PLACE_GLOBAL_PUSHES_PER_SECOND`, `PLACE_GLOBAL_PUSH_BURST` | `550`, `2750` | The server-wide push ceiling: pushes a second from everyone together, web strokes and Git updates alike, and how many it lets through at once after a quiet spell. Past it a push is refused with a message that the demo is at its limit: `429` with `Retry-After` on the API, the update's `remote rejected` reason for Git. Nothing of a refused push is stored. The server's own agents and the merger bot do not count. The rate may be fractional, down to `0.001`; the burst must refill within 10^6 s at the rate (a sanity cap against mistaken settings), or startup refuses the setting. |
| `PLACE_BRANCH_TTL_SECONDS` | `600` | Idle branches whose tiles have merged are deleted after this. |
| `PLACE_MERGE_INTERVAL_MS` | `3000` | How often the merger bot runs while pushes arrive at up to `PLACE_MERGE_BUSY_PUSHES_PER_SECOND`. |
| `PLACE_MERGE_BUSY_PUSHES_PER_SECOND` | `5` | Above this push rate (since the last merge) the merger waits longer between runs, in proportion to the rate. |
| `PLACE_MERGE_INTERVAL_MAX_MS` | `30000` | The longest the merger waits between runs under load. |
| `PLACE_HISTORY_MAX_BYTES` | `8388608` | Once the packs written on `main` since its root reach this size, the merger starts `main`'s history again from one root commit with the same canvas, so a full clone stays about this size. `0` never does. |
| `PLACE_AGENTS` | `6` | Ambient agents (sprite painters). Ignored while `PLACE_PERSONAS` is on: the personas replace them. |
| `PLACE_PERSONAS` | `true` | Personas: a roster of 13 pretend painters who drop in on shifts of 3 to 20 minutes and paint pictures they love (flags, a frog, a house, a cat...) one tile per push, every 1 to 5 s each, on `agent/<name>-<n>` branches with their own commit author (`Name <name@personas.place.invalid>`). They never paint a tile people painted in the last six hours or a keep area, and move a picture when a person paints into it. Both the binary and public-demo Terraform default on. Set `false` or `0` to disable them explicitly. |
| `PLACE_PERSONAS_MIN`, `PLACE_PERSONAS_MAX` | `3`, `6` | Personas on shift at least and at most; who is on changes over time. |
| `PLACE_PERSONAS_PUSHES_PER_SECOND` | `3` | Pushes per second all personas together may make (they average about 1). |
| `PLACE_AMBIENT_INTERVAL_MS` | `30000` | Mean time between the pushes of one sprite an ambient agent paints. |
| `PLACE_AMBIENT_SPRITE_PAUSE_SECONDS` | `1800` | Mean time an ambient agent rests between two sprites. |
| `PLACE_KEEP_AREAS` | | `x,y,width,height;...`: areas ambient agents never paint over (people's work from before the server started; it remembers what people paint for six hours, in memory). |
| `PLACE_AMBIENT_MERGE_INTERVAL_SECONDS` | `300` | How often, at most, the merger runs when only ambient agents have pushes pending; any other push is merged on the next run. Slows how fast idle painting uses up `PLACE_HISTORY_MAX_BYTES` (each merge stores a whole `tiles/` tree). |
| `PLACE_SWARM_MAX_AGENTS`, `PLACE_SWARM_PUSHES_PER_SECOND`, `PLACE_SWARM_RUN_SECONDS`, `PLACE_SWARM_COOLDOWN_SECONDS`, `PLACE_SWARM_STARTS_PER_HOUR` | `1000`, `120`, `20`, `40`, `6` | Swarm limits. |
| `PLACE_SWARM_ENABLED` | `false` | Whether visitors may start the swarm (`POST /api/v1/swarm`); off answers 404. |
| `PLACE_WEB_TILES_PER_PUSH` | `1` | Tiles one push from the page may carry: one click, one commit, one push. Git pushes keep `PLACE_TILES_PER_PUSH`. |
| `PLACE_AGENT_CONCURRENCY` | `16` | Agent pushes in flight at once. |
| `PLACE_EVENT_STREAMS`, `PLACE_EVENT_STREAMS_PER_ADDRESS` | `20000`, `64` | Event streams (`GET /api/v1/events`) open at once, in all and per client address; more are refused with 429. An open stream costs about 22 KB of server memory (20,000 streams about 425 MiB) (measured by `an_event_stream_costs_kilobytes_so_twenty_thousand_fit_the_memory_mark`), and each holds a file descriptor (Fargate allows 65,535). |
| `PLACE_EVENT_STREAM_MAX_SECONDS` | `600` | An event stream ends after at most this long (at a random point in its last tenth, so streams do not all reconnect at once); the page reconnects with the `seq` of the last event it saw and misses nothing. |
| `PLACE_TRUSTED_PROXY_HOPS` | `0` | Proxies in front that append to `X-Forwarded-For`; 0 uses the TCP peer. |
| `PLACE_TRUSTED_PROXIES` | private ranges | Comma-separated addresses or CIDR blocks the nearest proxy connects from; `X-Forwarded-For` is read only from these peers, and only with hops above 0. Same rules as Aspen's `ASPEN_TRUSTED_PROXIES`. |
| `PLACE_KILL_SWITCH` | | `1` starts with the kill switch engaged. |
| `PLACE_POLICY_FILE` | anyone may read and push branches | An `aspen-auth` `PolicyConfig` (JSON), for example to add OIDC operators. |
| `PLACE_LIMITS_FILE` | per client address: 200 pushes/s (2,000 at once) and 200 fetches/s (2,000 at once), 192 pushes and 64 fetches running at once, 1 MiB packs | An `aspen-auth` `LimitsConfig` (JSON). Rates count HTTP requests: a push is 2, a fetch 3 (`aspen_place::defaults`). |
| `PLACE_SESSIONS_PER_MINUTE` | `120` | Page sessions (`POST /api/v1/sessions`) one client address may open per minute. |
| `PLACE_MEMORY_HIGH_WATER_BYTES` | 80% of the task's memory | Resident memory over which new Git requests are refused with `503` (the page, the API and the server's own pushes keep running). `0` sets no mark. |
| `PLACE_WEB_DIR` | | A built page (`npm run build` in `demo/place` writes `dist/`) to serve at `/` beside the API and Git. Read into memory at startup; the start is refused if it has no `index.html`, or if a file would be served at or under a path the server keeps: `/api/v1`, `/-`, `/canvas` or `/canvas.git` (the canvas repository's name). The `place` image sets it. |
| `PLACE_SHUTDOWN_DRAIN_SECONDS` | `20` | On SIGTERM or Ctrl-C, how long requests in flight may run before the server ends them, closes their connections and runs its last merge. Event streams last up to `PLACE_EVENT_STREAM_MAX_SECONDS`, so this bounds every shutdown. |

Two limits follow from the canvas and are not settings: the largest object
a push may carry, and so the longest commit message, is four times the
canvas's full `tiles/` tree (601,088 bytes at 64 x 64; resolving the tree
as a delta needs room for two copies) and at least 64 KiB; the largest
pack one push may send is 1 MiB unless `PLACE_LIMITS_FILE` says otherwise;
and the instance runs at most 256 pushes and 128 fetches at once, with up
to 1,024 of each waiting at most 10 s for a slot before a `503`
(`aspen_place::defaults`).

Anonymous callers are counted by client address, an IPv6 address by its
/64.

## Checks

```bash
cargo test -p aspen-place                       # rules, state, and end to end with stock git
cargo test --release -p aspen-place --test history -- --ignored --nocapture   # sustained load, history resets
cargo test -p aspen-server --test push_hook     # the push hook in receive-pack
python3 demo/place/server/scripts/check-mutations.py   # each guard's test fails without it
```

The page's contract test can also run against this server. Start it on a
fresh data directory with `PLACE_PUSHES_PER_MINUTE=2
PLACE_BRANCH_TTL_SECONDS=1 PLACE_MERGE_INTERVAL_MS=300`, issue an operator
token, then from `demo/place`:

```bash
PLACE_CONTRACT_URL=http://127.0.0.1:7070/api/v1 PLACE_CONTRACT_OPERATOR_TOKEN=$TOKEN \
  npx vitest run test/backend-contract.test.ts
```

## Limits of this version

- **One instance.** The canvas overlays, the merge queue, sessions and the
  event log live in memory. A restart reloads `main` from Git and lists the
  branches again; event sequence numbers start over, and clients resync.
  Pushes accepted but not merged when the process dies are not merged
  later (they stay on their branches); a graceful shutdown (SIGTERM or
  Ctrl-C) merges first, after at most `PLACE_SHUTDOWN_DRAIN_SECONDS`
  (#55 tracks
  surviving a crash). When the drain ends, requests still running are cut off
  and their connections closed before that last merge; a push whose
  ingest is already running may still land on its branch during or after
  the merge, and then stays there the same way. Two instances may overlap
  for the minute of a start-first deploy: every write to `main` and to a
  branch is a compare-and-swap in the shared store, a merge that loses one
  reloads `main` and tries again (the last merge at shutdown up to five
  times, a short jittered pause apart; branches still unmerged after that
  are logged by name), and a refused server paint reads the branch's tip
  again next time. Each instance follows `main` on every merger tick, so
  its canvas and its memory of people's tiles include the other's merges,
  and branches found at start are not expired for ten minutes. While they
  overlap each has its own event feed (numbered from its start time, so a
  client that moves between them resyncs), sessions, swarm, kill switch and
  personas, and pushes it accepted merge only through its own merger. Do
  not run two for longer.
- **Storage.** Each commit the server writes carries a whole `tiles/` tree
  (about 30 bytes per painted tile, compressed), with no deltas.

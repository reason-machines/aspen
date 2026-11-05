# place

A shared pixel canvas where every brush stroke is a `git push`. Each visitor
paints on their own branch, so pushes run in parallel without contending; a
merger bot folds every branch into `main` every few seconds. The page shows
`main` plus each branch's unmerged tiles (hatched), live.

This directory holds the web front end, the API contract it consumes, a
mock server that implements the contract with simulated agents, and the real
back end in [`server/`](server/README.md) (plan node C7, see
[docs/PLAN.md](../../docs/PLAN.md), sections 3 and 6): a Rust service on
Aspen where every stroke is a real `git push`.

| Path | What |
|---|---|
| [`api/openapi.yaml`](api/openapi.yaml) | The HTTP + server-sent events contract. C7 implements this. |
| [`docs/TILES.md`](docs/TILES.md) | Tile file format and push rules, for the pre-receive check. |
| `src/shared/` | Contract types, palette and the tile rules as code (used by both the page and the mock). |
| `src/client/` | API client, SSE parser, canvas state, renderer, feed. |
| `src/main.ts`, `index.html`, `src/styles.css` | The page. |
| `mock/` | Mock API: an in-memory simulation (`sim.ts`) behind an HTTP layer (`api.ts`). |
| `test/` | Tile rules, client units, the client against the mock over HTTP, and a contract test of the mock against the OpenAPI file. |
| `scripts/screenshots.mjs` | Desktop and mobile screenshots with headless Chromium, for PR evidence. |
| [`server/`](server/README.md) | The real back end (`aspen-place`): Git over HTTP, the tile rules inside receive-pack, the merger bot, the API and the event stream. |

## Running it

You need Node.js 22.18 or later (the version used in CI is in `.nvmrc`).

```bash
cd demo/place
npm ci
npm run dev        # page + mock API on http://localhost:5173
```

`npm run dev` and `npm run preview` (after `npm run build`) serve the mock API
from the same process under `/api/v1`. To run the mock on its own:

```bash
npm run mock       # http://127.0.0.1:8787/api/v1
```

To point the dev server at the real back end instead of the mock (start it
with `cargo run -p aspen-place` from the repository root):

```bash
PLACE_API_PROXY=http://localhost:7070 npm run dev
```

For a static build served from a different origin than the API, set
`VITE_PLACE_API=https://place.example.com/api/v1` at build time.

### Mock settings

Environment variables for `npm run dev`, `npm run preview` and `npm run mock`:

| Variable | Default | Effect |
|---|---|---|
| `PLACE_DOMAIN` | `example.com` | Clone URL becomes `https://place.<domain>/canvas.git`, dashboard `https://dashboard.<domain>/`. |
| `PLACE_CLONE_URL` | | Overrides the clone URL entirely. |
| `PLACE_DASHBOARD_URL` | | Overrides the dashboard link; `none` hides it. |
| `PLACE_AGENTS` | `6` | Ambient agents painting in the background. |
| `PLACE_KILL_SWITCH` | | `1` starts with the swarm kill switch engaged. |
| `PLACE_OPERATOR_TOKEN` | `mock-operator-token` | Bearer token for `POST /api/v1/admin/kill-switch`. |

Engage the kill switch on a running mock:

```bash
curl -X POST http://localhost:5173/api/v1/admin/kill-switch \
  -H 'Authorization: Bearer mock-operator-token' \
  -H 'Content-Type: application/json' \
  -d '{"engaged": true, "reason": "Maintenance"}'
```

## Checks

```bash
npm run lint        # Biome: lint + formatting
npm run typecheck   # tsc --noEmit
npm test            # Vitest
npm run build       # production bundle in dist/
npm run check       # all of the above
```

CI runs the same commands in the `place` job of `.github/workflows/ci.yml`,
in parallel with the Rust jobs; `ci-ok` requires it.

## How the page works

- On load it reads `GET /config`, then the `GET /canvas` snapshot, then
  streams `GET /events?after=<snapshot seq>`. Every push and merge has a
  sequence number from one counter, so the snapshot and the stream line up
  exactly, and a reconnecting client resumes where it stopped (or reloads the
  snapshot when the server asks it to `resync`).
- `POST /sessions` gives the visitor a branch (`visitors/<id>`) and a bearer
  token, kept in session storage. Lifting the brush sends the stroke as one
  `POST /pushes`; the response and the stream echo are deduplicated by `seq`.
- The canvas is drawn on a `<canvas>`: `main` from a one-pixel-per-tile bitmap
  scaled without smoothing, then unmerged branch tiles with a hatch, your own
  branch with a gold inset, and the stroke you are drawing with a dashed edge.
- Keyboard: focus the canvas, move with the arrow keys (Shift moves 8 tiles),
  Space adds or removes the tile under the cursor, Enter pushes, Escape
  discards. The color picker is a native radio group.
- The swarm button calls `POST /swarm`. The panel shows its state from
  `GET /swarm` and `swarm` events: running with agent count and pushes,
  cooling down, rate-limited for this address, or paused by the operators'
  kill switch.

## Dependencies

Development only, pinned exactly in `package.json` and locked in
`package-lock.json`: Vite (dev server and bundler), TypeScript, Vitest, Biome
(lint and format) and `yaml` (the contract test reads the OpenAPI file). The
shipped page has no runtime dependencies and loads no web fonts or third-party
assets. `cargo deny` does not cover this directory; the nightly workflow runs
`npm audit --audit-level=high` over the dev tooling instead.

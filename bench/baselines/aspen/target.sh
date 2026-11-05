# Target: Aspen, as `aspen dev` (SQLite and a local blob store under one
# data directory, anonymous read and write, loopback only) built from this
# checkout in release mode. No Docker. Sourced by ../run.sh.
#
# Each workload gets a fresh repository from `aspen repo create`, run
# against the same SQLite database while the server runs.
#
# Aspen's push advertisement shows only trunk (spike S2), so a push to the
# plain repository URL would be built against trunk and resend every commit
# the benchmark branch has gained. Push through each branch's scoped URL,
# <repo>/-/refs/heads/<branch>, instead. Readers fetch the unadvertised
# benchmark branches through protocol v2 `ls-refs` with `ref-prefix`, so
# keep the default --protocol 2: with --protocol 0 or 1 they cannot see
# the benchmark branches on Aspen.
TARGET_PUSH_ARGS=(--branch-scoped-push)

BENCH_PORT="${BENCH_PORT:-8086}"
BASE_URL="http://127.0.0.1:${BENCH_PORT}"
ASPEN_DATA="$BASELINES/.state/${TARGET}-data"
ASPEN_BIN="$REPO_ROOT/target/release/aspen"
# `aspen dev` serves /-/metrics beside Git; the stale-branch workload reads
# the planner's counters there and waits for compaction to catch up.
TARGET_STALE_ARGS=(--metrics-url "$BASE_URL/-/metrics")
# Extra `aspen dev` flags (the aspen-no-compaction target sets one).
ASPEN_DEV_ARGS=(${ASPEN_DEV_ARGS[@]+"${ASPEN_DEV_ARGS[@]}"})

target_up() {
  log "building aspen"
  (cd "$REPO_ROOT" && cargo build --release --locked -p aspen-cli)
  rm -rf "$ASPEN_DATA"
  mkdir -p "$ASPEN_DATA"
  "$ASPEN_BIN" dev --listen "127.0.0.1:${BENCH_PORT}" --data-dir "$ASPEN_DATA" \
    ${ASPEN_DEV_ARGS[@]+"${ASPEN_DEV_ARGS[@]}"} >"$ASPEN_DATA/aspen.log" 2>&1 &
  save_state ASPEN_PID "$!"
  wait_http "$BASE_URL/healthz" 60
}

target_create_repo() {
  DATABASE_URL="sqlite://$ASPEN_DATA/meta.db" "$ASPEN_BIN" repo create "$1" >&2
  echo "$BASE_URL/$1.git"
}

target_down() {
  load_state
  if [[ -n "${ASPEN_PID:-}" ]]; then
    kill "$ASPEN_PID" 2>/dev/null || true
    wait "$ASPEN_PID" 2>/dev/null || true
  fi
  rm -rf "$ASPEN_DATA"
}

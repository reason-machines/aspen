# Target: Gitea + Postgres. Sourced by ../run.sh.
BENCH_PORT="${BENCH_PORT:-3000}"
BASE_URL="http://127.0.0.1:${BENCH_PORT}"
# Forgejo reuses this file with its own service and CLI name.
FORGE_SERVICE="${FORGE_SERVICE:-gitea}"

target_up() {
  compose up -d --wait
  wait_http "$BASE_URL/api/v1/version" 180
  local password
  password="$(random_secret)"
  # The CLI must run as the unprivileged `git` user inside the container.
  compose exec -T -u git "$FORGE_SERVICE" "$FORGE_SERVICE" admin user create --admin \
    --username bench --password "$password" --email bench@aspen.invalid \
    --must-change-password=false >/dev/null
  save_state BENCH_USER bench
  save_state BENCH_PASSWORD "$password"
}

target_create_repo() {
  load_state
  curl -fsS -o /dev/null -u "bench:${BENCH_PASSWORD}" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$1\",\"private\":false,\"default_branch\":\"main\"}" \
    "$BASE_URL/api/v1/user/repos"
  echo "http://bench:${BENCH_PASSWORD}@127.0.0.1:${BENCH_PORT}/bench/$1.git"
}

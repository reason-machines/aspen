# Target: GitLab CE (Gitaly behind Workhorse). Sourced by ../run.sh.
BENCH_PORT="${BENCH_PORT:-8929}"
BASE_URL="http://127.0.0.1:${BENCH_PORT}"

# compose.yaml requires GITLAB_ROOT_PASSWORD for every compose command, but
# GitLab only reads it on first boot; later commands get a placeholder.
compose() {
  GITLAB_ROOT_PASSWORD="${GITLAB_ROOT_PASSWORD:-unused-after-first-boot}" \
    docker compose -f "$BASELINES/$TARGET/compose.yaml" "$@"
}

target_up() {
  local root_password token
  root_password="$(random_secret)"
  token="bench-$(random_secret)"
  GITLAB_ROOT_PASSWORD="$root_password" compose up -d
  # Omnibus reconfigures on first boot; this takes minutes.
  wait_http "$BASE_URL/users/sign_in" 900
  # A personal access token for root, created through the Rails console.
  # Same steps as GitLab's "create a personal access token programmatically".
  compose exec -T gitlab gitlab-rails runner "
    t = User.find_by_username('root').personal_access_tokens.create(
      name: 'aspen-bench', scopes: %w[api read_repository write_repository],
      expires_at: 7.days.from_now)
    t.set_token('$token')
    t.save!
  " >/dev/null
  save_state BENCH_TOKEN "$token"
}

target_create_repo() {
  load_state
  curl -fsS -o /dev/null -H "PRIVATE-TOKEN: ${BENCH_TOKEN}" -X POST \
    --data-urlencode "name=$1" --data-urlencode "path=$1" \
    --data-urlencode visibility=public --data-urlencode initialize_with_readme=false \
    "$BASE_URL/api/v4/projects"
  echo "http://root:${BENCH_TOKEN}@127.0.0.1:${BENCH_PORT}/root/$1.git"
}

# Target: Forgejo + Postgres. Forgejo keeps Gitea's CLI and API, so this
# reuses the Gitea target with Forgejo's port, service and CLI name.
# Sourced by ../run.sh.
BENCH_PORT="${BENCH_PORT:-3001}"
FORGE_SERVICE=forgejo
# shellcheck source=../gitea/target.sh
. "$BASELINES/gitea/target.sh"

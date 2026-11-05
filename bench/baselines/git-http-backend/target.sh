# Target: stock `git http-backend` behind Apache. Sourced by ../run.sh.
BENCH_PORT="${BENCH_PORT:-8081}"
BASE_URL="http://127.0.0.1:${BENCH_PORT}"

target_up() {
  compose up -d --build --wait
  # `git http-backend` answers 404 for a path with no repository; any HTTP
  # answer means Apache is up.
  wait_http "$BASE_URL/" 60 any
}

target_create_repo() {
  compose exec -T git new-repo "$1"
  echo "$BASE_URL/$1.git"
}

#!/bin/sh
# Runs create-database.sh against a throwaway local PostgreSQL set up the
# way RDS sets up its master user: a login role with CREATEROLE and
# CREATEDB that is not a superuser, owning the database `aspen`.
#
#   PG_BIN=/usr/lib/postgresql/17/bin \
#     deploy/terraform/aws/public-demo/environment/tests/create-database.test.sh
#
# Needs initdb, pg_ctl and psql (PG_BIN, or on PATH). Checks that the script
# works twice in a row (every task start runs it), that `place` reaches its
# own database and owns its schema, that it cannot reach Aspen's database,
# that a second run with another password rotates it, and that the password
# never appears in psql's arguments (which `ps` and /proc/<pid>/cmdline show
# to every user on the host).
set -eu

here=$(cd "$(dirname "$0")" && pwd)
script="$here/../create-database.sh"
if [ -n "${PG_BIN:-}" ]; then PATH="$PG_BIN:$PATH"; fi

dir=$(mktemp -d)
port=${PG_TEST_PORT:-54329}
cleanup() {
  pg_ctl -D "$dir/data" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$dir"
}
trap cleanup EXIT

initdb -D "$dir/data" -U postgres -A scram-sha-256 --pwfile=/dev/stdin >/dev/null <<EOF
superpass
EOF
# TCP on loopback, not a socket under TMPDIR, whose path may be too long.
pg_ctl -D "$dir/data" -o "-p $port -k '' -c listen_addresses=127.0.0.1" -l "$dir/log" -w start >/dev/null ||
  { cat "$dir/log" >&2; exit 1; }

export PGHOST=127.0.0.1 PGPORT="$port"

PGUSER=postgres PGPASSWORD=superpass PGDATABASE=postgres psql -X -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE aspen LOGIN CREATEROLE CREATEDB PASSWORD 'masterpass';
CREATE DATABASE aspen OWNER aspen;
SQL

master() {
  PGUSER=aspen PGPASSWORD=masterpass PGDATABASE=aspen "$@"
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# The script runs with a psql on PATH that records its arguments, one per
# line, and then runs the real psql.
real_psql=$(command -v psql)
mkdir "$dir/shim"
printf '#!/bin/sh\nprintf "%%s\\n" "$@" >>"%s"\nexec "%s" "$@"\n' \
  "$dir/psql-argv" "$real_psql" >"$dir/shim/psql"
chmod +x "$dir/shim/psql"

# run_script <password>: runs create-database.sh as the master user with
# PLACE_PASSWORD=<password>, and fails the test if psql's arguments contained
# the password.
run_script() {
  : >"$dir/psql-argv"
  master env PATH="$dir/shim:$PATH" PLACE_PASSWORD="$1" ASPEN_DATABASE=aspen sh "$script" || return 1
  [ -s "$dir/psql-argv" ] || fail "the script did not run psql through the test's PATH"
  if grep -qF -e "$1" "$dir/psql-argv"; then
    fail "the place password is on psql's command line"
  fi
}

run_script first || fail "first run"
run_script first || fail "second run (idempotent)"

as_place() {
  password=$1
  shift
  PGUSER=place PGPASSWORD="$password" psql -X -qtA -v ON_ERROR_STOP=1 "$@"
}

[ "$(as_place first -d place -c 'SELECT current_user')" = place ] || fail "place cannot reach its database"
as_place first -d place -c 'CREATE TABLE t (x int); DROP TABLE t' || fail "place cannot create tables in its database"
[ "$(master psql -X -qtA -c "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = 'place'")" = place ] ||
  fail "the place database is not owned by place"
[ "$(master psql -X -qtA -c "SELECT rolsuper OR rolcreaterole OR rolcreatedb FROM pg_roles WHERE rolname = 'place'")" = f ] ||
  fail "place has more than LOGIN"

if as_place first -d aspen -c 'SELECT 1' >/dev/null 2>&1; then
  fail "place can connect to Aspen's database"
fi
[ "$(master psql -X -qtA -c 'SELECT current_user')" = aspen ] || fail "the master user lost access to Aspen's database"

# Some other role must not reach place's database either.
PGUSER=postgres PGPASSWORD=superpass PGDATABASE=postgres psql -X -q -c "CREATE ROLE other LOGIN PASSWORD 'otherpass'"
if PGUSER=other PGPASSWORD=otherpass psql -X -q -d place -c 'SELECT 1' >/dev/null 2>&1; then
  fail "a role other than place can connect to the place database"
fi

# A quote, a backslash and a space must reach the server unchanged.
second="sec'ond \\pass"
run_script "$second" || fail "rotation run"
if as_place first -d place -c 'SELECT 1' >/dev/null 2>&1; then
  fail "the old password still works after rotation"
fi
as_place "$second" -d place -c 'SELECT 1' >/dev/null || fail "the new password does not work"

if master env ASPEN_DATABASE=aspen sh "$script" >/dev/null 2>&1; then
  fail "the script ran without PLACE_PASSWORD"
fi

echo "create-database.sh: all checks passed"

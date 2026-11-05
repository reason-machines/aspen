#!/usr/bin/env bash
# Starts a throwaway PostgreSQL cluster for the differential oracle's
# PostgreSQL run, listening only on a Unix socket in DIR, with trust
# authentication (no password) and fsync off. Prints the database URL for
# `aspen-testkit diff --candidate aspen --aspen-database-url`.
#
# usage: testkit/scripts/throwaway-postgres.sh DIR       start; prints the URL
#        testkit/scripts/throwaway-postgres.sh --stop DIR
#
# The binaries come from $ASPEN_TEST_PG_BIN (a directory holding initdb and
# pg_ctl, as for aspen-meta's tests) or the newest /usr/lib/postgresql/*/bin,
# which GitHub's Ubuntu runners have preinstalled. DIR must be short (a Unix
# socket path is limited to about 100 bytes), for example /tmp/aspen-pg.
set -euo pipefail

stop=0
if [[ ${1:-} == --stop ]]; then stop=1; shift; fi
dir=${1:?usage: throwaway-postgres.sh [--stop] DIR}

bin=${ASPEN_TEST_PG_BIN:-}
if [[ -z $bin ]]; then
  bin=$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1 || true)
fi
[[ -x $bin/initdb && -x $bin/pg_ctl ]] \
  || { echo "no initdb/pg_ctl found; set ASPEN_TEST_PG_BIN" >&2; exit 1; }

if ((stop)); then
  "$bin/pg_ctl" -D "$dir/data" -m immediate stop >/dev/null
  exit 0
fi

mkdir -p "$dir"
"$bin/initdb" -D "$dir/data" -U aspen -A trust -E UTF8 --no-locale --no-sync >"$dir/initdb.log"
"$bin/pg_ctl" -D "$dir/data" -l "$dir/log" -w \
  -o "-k $dir -c listen_addresses='' -p 5432 -c fsync=off -c max_connections=200" start >/dev/null
echo "postgres://aspen@localhost:5432/postgres?host=$dir"

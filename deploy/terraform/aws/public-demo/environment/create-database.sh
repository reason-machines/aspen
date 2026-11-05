#!/bin/sh
# Run by the place task's `create-database` init container (place.tf) before
# aspen-place starts, as the RDS master user, so that aspen-place itself
# never holds the master credentials.
#
# It makes sure that:
# - a login role `place` exists, with no other attributes, and its password
#   is PLACE_PASSWORD (so bumping place_db_password_version and restarting
#   the task rotates it);
# - a database `place` exists and is owned by that role;
# - only `place` (and the master user) may connect to `place`, and `place`
#   may not connect to Aspen's database (ASPEN_DATABASE).
#
# Every step is idempotent: the container runs on every task start.
#
# Environment: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE and PGSSLMODE
# for the master connection (libpq reads them), PLACE_PASSWORD and
# ASPEN_DATABASE. psql reads PLACE_PASSWORD from its own environment with
# `\getenv` (psql 15 and later; place_database_init_image is PostgreSQL 17),
# so the password is never on a command line, where `ps` and
# /proc/<pid>/cmdline would show it to every user on the host.
#
# Test against a local PostgreSQL:
#   deploy/terraform/aws/public-demo/environment/tests/create-database.test.sh
set -eu

: "${PLACE_PASSWORD:?PLACE_PASSWORD must be set}"
: "${ASPEN_DATABASE:?ASPEN_DATABASE must be set}"

psql -X -q -v ON_ERROR_STOP=1 \
  -v aspen_database="$ASPEN_DATABASE" <<'SQL'
SET client_min_messages = warning;
\getenv place_password PLACE_PASSWORD
SELECT NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'place') AS create_role \gset
\if :create_role
CREATE ROLE place;
\endif
ALTER ROLE place WITH LOGIN NOCREATEDB NOCREATEROLE PASSWORD :'place_password';

-- PostgreSQL 16 and later: creating or giving away a database owned by
-- another role needs the right to SET ROLE to it ("must be able to SET ROLE
-- place" without this). The master user may act as place, not the reverse.
GRANT place TO CURRENT_USER;

SELECT NOT EXISTS (SELECT FROM pg_database WHERE datname = 'place') AS create_database \gset
\if :create_database
CREATE DATABASE place OWNER place;
\endif
ALTER DATABASE place OWNER TO place;

REVOKE ALL ON DATABASE place FROM PUBLIC;
REVOKE CONNECT, TEMPORARY ON DATABASE :"aspen_database" FROM PUBLIC;
SQL

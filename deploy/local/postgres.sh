#!/usr/bin/env bash
# What the official postgres image's entrypoint does, for the settle-local image: initialise the
# data directory once, run deploy/initdb.sql, then run PostgreSQL in the foreground.
set -euo pipefail
PG=/usr/lib/postgresql/16/bin
DATA=/var/lib/postgresql/data
mkdir -p "$DATA" /run/postgresql
chown -R postgres:postgres /var/lib/postgresql /run/postgresql
as_pg() { /usr/bin/setpriv --reuid=postgres --regid=postgres --init-groups "$@"; }
if [ ! -s "$DATA/PG_VERSION" ]; then
  as_pg "$PG/initdb" -D "$DATA" -U postgres --auth-local=trust -E UTF8 --locale=C.UTF-8 >/dev/null
  echo "host all all 0.0.0.0/0 scram-sha-256" >> "$DATA/pg_hba.conf"  # over TCP: settle's password
  as_pg "$PG/pg_ctl" -D "$DATA" -l /tmp/initdb.log -o "-c listen_addresses=" -w start >/dev/null
  as_pg /usr/bin/psql -v ON_ERROR_STOP=1 -U postgres -q -f /app/deploy/initdb.sql
  as_pg "$PG/pg_ctl" -D "$DATA" -m fast -w stop >/dev/null
fi
[ "${1:-}" = postgres ] && shift  # the compose command starts with the program name, like the official image
exec /usr/bin/setpriv --reuid=postgres --regid=postgres --init-groups "$PG/postgres" -D "$DATA" -c listen_addresses='*' "$@"

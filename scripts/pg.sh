#!/usr/bin/env bash
# Local Postgres control: scripts/pg.sh start|stop|status
set -euo pipefail
source "$(dirname "$0")/env.sh"
case "${1:-status}" in
  start)
    if [ ! -d "$PGDATA" ]; then
      initdb -D "$PGDATA" -U claims --auth=trust --encoding=UTF8 --locale=C >/dev/null
      # Tuned for an 8 GB laptop running Spark alongside.
      cat >> "$PGDATA/postgresql.conf" <<CONF
port = $PGPORT
listen_addresses = '127.0.0.1'
shared_buffers = 256MB
max_connections = 40
fsync = off
synchronous_commit = off
CONF
    fi
    pg_ctl -D "$PGDATA" status >/dev/null 2>&1 || pg_ctl -D "$PGDATA" -l "$TOOLS_HOME/postgres.log" -w start >/dev/null
    psql -d postgres -tc "select 1 from pg_database where datname='claims'" | grep -q 1 || createdb -O claims claims
    echo "postgres up on 127.0.0.1:$PGPORT" ;;
  stop) pg_ctl -D "$PGDATA" -m fast stop ;;
  status) pg_ctl -D "$PGDATA" status ;;
  *) echo "usage: $0 start|stop|status"; exit 2 ;;
esac

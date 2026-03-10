#!/bin/bash
set -e

# --- Config file selection ---
CFG=/etc/postgresql/config
PG_CONF=$CFG/postgresql.conf
[ -f $CFG/postgresql.local.conf ] && PG_CONF=$CFG/postgresql.local.conf && \
  echo '[CONFIG] Using local override: postgresql.local.conf'
HBA_CONF=$CFG/pg_hba.conf
[ -f $CFG/pg_hba.local.conf ] && HBA_CONF=$CFG/pg_hba.local.conf && \
  echo '[CONFIG] Using local override: pg_hba.local.conf'

# --- Tablespace directory permissions ---
if [ -d /data/tablespace ]; then
    for dir in /data/tablespace/*/; do
        [ -d "$dir" ] && chown postgres:postgres "$dir"
    done
fi

# --- Start PostgreSQL ---
chown -R postgres:postgres /var/lib/postgresql
exec docker-entrypoint.sh postgres \
    -c config_file=$PG_CONF \
    -c hba_file=$HBA_CONF \
    -c port=${POSTGRES_PORT:-5432}

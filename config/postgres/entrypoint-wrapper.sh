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

# --- Auth migration for existing databases ---
if [ "${POSTGRES_AUTH_ENABLED:-}" = "true" ]; then
    # Check if this is an existing database (not first init)
    if [ -f "/var/lib/postgresql/data/PG_VERSION" ]; then
        echo '[CONFIG] Auth enabled on existing database — setting password'
        # Start postgres temporarily with trust auth to set password
        su postgres -c "pg_ctl start -D /var/lib/postgresql/data \
            -o \"-c hba_file=$CFG/pg_hba.conf -c port=${POSTGRES_PORT:-5432}\" \
            -w -l /tmp/pg_auth_init.log"
        su postgres -c "psql -p ${POSTGRES_PORT:-5432} -c \
            \"ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD}'\""
        su postgres -c "pg_ctl stop -D /var/lib/postgresql/data -w"
        echo '[CONFIG] Password set successfully'
    fi
fi

# --- Start PostgreSQL ---
chown -R postgres:postgres /var/lib/postgresql
exec docker-entrypoint.sh postgres \
    -c config_file=$PG_CONF \
    -c hba_file=$HBA_CONF \
    -c port=${POSTGRES_PORT:-5432}

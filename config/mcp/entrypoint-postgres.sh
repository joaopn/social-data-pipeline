#!/bin/sh
# Entrypoint for PostgreSQL MCP server (crystaldba/postgres-mcp).
# Reads MCP credentials from the data volume when auth is enabled,
# then delegates to the default entrypoint.
#
# Environment:
#   POSTGRES_MCP_USER  - MCP user (triggers credential file lookup)
#   DATABASE_URI       - Fallback connection string (when no auth)
#   POSTGRES_PORT      - PostgreSQL port (default: 5432)
#   DB_NAME            - Database name (default: datasets)

set -e

if [ -n "${POSTGRES_MCP_USER:-}" ]; then
    # Read password from credentials file in mounted data volume.
    # File format: single-line {password}\n (chmod 600). Username is
    # authoritative in config/db/postgres.yaml, mirrored to POSTGRES_MCP_USER.
    CRED_FILE="/data/database/.ro_credentials"
    if [ -f "$CRED_FILE" ]; then
        MCP_PASSWORD=$(cat "$CRED_FILE")
    fi

    if [ -n "${MCP_PASSWORD:-}" ]; then
        # Percent-encode credentials for the connection-string userinfo: the
        # RO password can contain reserved chars (e.g. ':') that make the URI
        # invalid. Encode via env (not argv) so the password never appears in
        # the process table; this is a Python MCP image so python is present.
        # Nothing here logs the password.
        PY=$(command -v python3 || command -v python)
        if [ -z "$PY" ]; then
            echo "[ERROR] no python interpreter found to encode the connection URI"
            exit 1
        fi
        ENC_USER=$(_ENC="${POSTGRES_MCP_USER}" "$PY" -c 'import os,urllib.parse;print(urllib.parse.quote(os.environ["_ENC"],safe=""),end="")')
        ENC_PASS=$(_ENC="${MCP_PASSWORD}" "$PY" -c 'import os,urllib.parse;print(urllib.parse.quote(os.environ["_ENC"],safe=""),end="")')
        export DATABASE_URI="postgresql://${ENC_USER}:${ENC_PASS}@postgres:${POSTGRES_PORT:-5432}/${DB_NAME:-datasets}"
    else
        echo "[ERROR] POSTGRES_MCP_USER set but no password found (checked $CRED_FILE)"
        exit 1
    fi
fi

# Delegate to the default image entrypoint
exec /app/docker-entrypoint.sh postgres-mcp "$@"

#!/bin/sh
# Entrypoint for StarRocks MCP server (mcp-server-starrocks).
# Builds the command from environment variables:
#   STARROCKS_MCP_USER      - MCP user (when auth enabled, reads password from credentials file)
#   STARROCKS_HOST           - StarRocks host (default: starrocks)
#   STARROCKS_PORT           - StarRocks MySQL port (default: 9030)
#   MCP_PORT                 - HTTP port (default: 9000)
#   MCP_QUERY_TIMEOUT_SECONDS - optional: StarRocks stops each query after this many seconds (query_timeout, 1-3600)
#
# Read-only enforcement: database-level only, via the RO user's sdp_readonly role
# (SELECT-only grants). The official mcp-server-starrocks has no application-level
# read-only flag — write operations will fail with SQL permission errors.

set -e

PORT="${MCP_PORT:-9000}"

# Optional query time limit (the chat's copy of this server sets it): whole seconds from 1 to 3600; unset or empty means none of this server's own.
QUERY_TIMEOUT="${MCP_QUERY_TIMEOUT_SECONDS:-}"
if [ -n "$QUERY_TIMEOUT" ]; then
    case "$QUERY_TIMEOUT" in
        *[!0-9]*) QUERY_TIMEOUT_BAD=1 ;;
        *) if [ "${#QUERY_TIMEOUT}" -gt 4 ] || [ "$QUERY_TIMEOUT" -lt 1 ] || [ "$QUERY_TIMEOUT" -gt 3600 ]; then QUERY_TIMEOUT_BAD=1; fi ;;
    esac
    if [ -n "${QUERY_TIMEOUT_BAD:-}" ]; then
        echo "[ERROR] MCP_QUERY_TIMEOUT_SECONDS must be a whole number of seconds from 1 to 3600 (for the chat's copy it comes from CHAT_QUERY_TIMEOUT_SECONDS in .env)"
        exit 1
    fi
fi

# Build credentials: use credentials file if MCP user is set, otherwise root with no password
if [ -n "${STARROCKS_MCP_USER:-}" ]; then
    # Read password from credentials file in mounted data volume.
    # File format: single-line {password}\n (chmod 600). Username is
    # authoritative in config/db/starrocks.yaml, mirrored to STARROCKS_MCP_USER.
    CRED_FILE="/data/starrocks/.ro_credentials"
    if [ -f "$CRED_FILE" ]; then
        STARROCKS_MCP_PASSWORD=$(cat "$CRED_FILE")
    fi

    if [ -n "${STARROCKS_MCP_PASSWORD:-}" ]; then
        export STARROCKS_USER="${STARROCKS_MCP_USER}"
        export STARROCKS_PASSWORD="${STARROCKS_MCP_PASSWORD}"
    else
        echo "[ERROR] STARROCKS_MCP_USER set but no password found (checked $CRED_FILE)"
        exit 1
    fi
else
    export STARROCKS_USER="root"
    export STARROCKS_PASSWORD=""
fi

exec mcp-server-starrocks --mode streamable-http --host 0.0.0.0 --port "${PORT}"

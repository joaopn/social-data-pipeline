#!/bin/sh
# Entrypoint for MongoDB MCP server (kiliczsh/mcp-mongo-server).
# Builds the command from environment variables:
#   MCP_MONGODB_URI       - MongoDB connection string (fallback when no auth)
#   MCP_MONGODB_USER      - MCP user (when auth enabled, reads password from credentials file)
#   MCP_MONGODB_PASSWORD  - MCP password (set externally or read from credentials file)
#   MCP_MONGODB_READONLY  - "true" to enable read-only mode (default: true)
#   MCP_PORT              - HTTP port (default: 3000)

set -e

PORT="${MCP_PORT:-3000}"
READONLY="${MCP_MONGODB_READONLY:-true}"

# Build URI: use credentials if MCP user is set, otherwise fall back to MCP_MONGODB_URI
if [ -n "${MCP_MONGODB_USER:-}" ]; then
    # Read password from credentials file in mounted data volume.
    # File format: single-line {password}\n (chmod 600). Username is
    # authoritative in config/db/mongo.yaml, mirrored to MCP_MONGODB_USER.
    CRED_FILE="/data/mongo/.ro_credentials"
    if [ -f "$CRED_FILE" ]; then
        MCP_MONGODB_PASSWORD=$(cat "$CRED_FILE")
    fi

    if [ -n "${MCP_MONGODB_PASSWORD:-}" ]; then
        # Percent-encode credentials for the connection-string userinfo: the
        # RO password can contain reserved chars (e.g. ':') that make the
        # Mongo driver reject a raw URI. Encode via env (not argv) so the
        # password never appears in the process table; node is always present
        # in this image. Nothing here logs the password.
        ENC_USER=$(_ENC="${MCP_MONGODB_USER}" node -e 'process.stdout.write(encodeURIComponent(process.env._ENC))')
        ENC_PASS=$(_ENC="${MCP_MONGODB_PASSWORD}" node -e 'process.stdout.write(encodeURIComponent(process.env._ENC))')
        URI="mongodb://${ENC_USER}:${ENC_PASS}@mongo:27017/?authSource=admin"
    else
        echo "[ERROR] MCP_MONGODB_USER set but no password found (checked $CRED_FILE)"
        exit 1
    fi
else
    URI="${MCP_MONGODB_URI:?MCP_MONGODB_URI is required}"
fi

ARGS="build/index.js ${URI} --transport http --port ${PORT}"

if [ "${READONLY}" = "true" ]; then
    ARGS="${ARGS} --read-only"
fi

# shellcheck disable=SC2086  # Intentional word splitting: ARGS contains multiple arguments
exec node ${ARGS}

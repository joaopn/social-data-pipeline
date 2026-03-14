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
    # Read password from credentials file in mounted data volume
    CRED_FILE="/data/mongo/.mcp_credentials"
    if [ -f "$CRED_FILE" ]; then
        MCP_MONGODB_PASSWORD=$(cut -d: -f2- "$CRED_FILE")
    fi

    if [ -n "${MCP_MONGODB_PASSWORD:-}" ]; then
        URI="mongodb://${MCP_MONGODB_USER}:${MCP_MONGODB_PASSWORD}@mongo:27017/?authSource=admin"
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

exec node ${ARGS}

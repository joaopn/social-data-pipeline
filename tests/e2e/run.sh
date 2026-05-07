#!/usr/bin/env bash
# E2E test runner — builds the sysbox container and runs pytest inside it.
#
# Usage:
#   ./tests/e2e/run.sh                  # Run enabled E2E tests
#   ./tests/e2e/run.sh -k parse         # Run tests matching "parse"
#   ./tests/e2e/run.sh -x               # Stop on first failure
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
IMAGE_NAME="sdp-e2e"
CONTAINER_NAME="sdp-e2e-runner"

# Enabled tests — add/remove files here to control what runs.
# Paths relative to repo root.
ENABLED_TESTS=(
    tests/e2e/tests/test_mongo_flow.py
    tests/e2e/tests/test_parse_reddit.py
    tests/e2e/tests/test_postgres_flow.py
    tests/e2e/tests/test_sr_flow.py
    tests/e2e/tests/test_sr_ml_flow.py
    tests/e2e/tests/test_auth_postgres.py
    tests/e2e/tests/test_auth_mongo.py
    tests/e2e/tests/test_auth_starrocks.py
    tests/e2e/tests/test_idempotency.py
    tests/e2e/tests/test_filter.py
    tests/e2e/tests/test_recovery_postgres.py
    tests/e2e/tests/test_postgres_dedup_null_safe.py
    tests/e2e/tests/test_unsetup_gating.py
    tests/e2e/tests/test_mount_drift.py
    tests/e2e/tests/test_unsetup_symmetry.py
    tests/e2e/tests/test_auth_full_cycle.py
    # tests/e2e/tests/test_mcp_lifecycle.py   # disabled: hangs inside sysbox during MCP image build
    # tests/e2e/tests/test_parse_custom.py    # deferred: custom parser fields config
)

cleanup() {
    echo "Stopping E2E container..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}
trap cleanup EXIT

# Build the E2E image (cached after first run)
echo "Building E2E image..."
docker build -t "$IMAGE_NAME" -f "$REPO_ROOT/tests/e2e/Dockerfile.e2e" "$REPO_ROOT"

# Remove stale container if it exists
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

# Start the sysbox container detached — systemd boots as PID 1 and starts dockerd.
# Overriding CMD would replace /sbin/init and prevent systemd from starting.
echo "Starting E2E container..."
docker run -d \
    --runtime=sysbox-runc \
    --name "$CONTAINER_NAME" \
    -v "$REPO_ROOT:/repo:ro" \
    "$IMAGE_NAME"

# Wait for inner dockerd to be ready
echo "Waiting for dockerd..."
for _i in $(seq 1 30); do
    if docker exec "$CONTAINER_NAME" docker info >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
if ! docker exec "$CONTAINER_NAME" docker info >/dev/null 2>&1; then
    echo "ERROR: dockerd did not start within 30s"
    exit 1
fi
echo "dockerd ready."

# Run E2E tests inside the container
docker exec -w /repo "$CONTAINER_NAME" \
    python -m pytest "${ENABLED_TESTS[@]}" -v --tb=short "$@"

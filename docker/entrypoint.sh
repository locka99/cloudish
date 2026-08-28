#!/usr/bin/env bash
# entrypoint.sh — start PostgreSQL then cloudish
#
# Strategy:
#   1. Run the official postgres entrypoint in the background so that it
#      initialises the data directory and starts the server.
#   2. Poll with pg_isready until the server accepts connections.
#   3. exec cloudish (replaces this shell so signals propagate correctly).

set -euo pipefail

# ── Start PostgreSQL ──────────────────────────────────────────────────────────

echo "[entrypoint] Starting PostgreSQL..."

# The official postgres image ships docker-entrypoint.sh which handles first-
# run initialisation, POSTGRES_DB creation, etc.  We call it in the background
# so we can wait for it to become ready before launching cloudish.
docker-entrypoint.sh postgres &
PG_PID=$!

# ── Wait for PostgreSQL to be ready ──────────────────────────────────────────

echo "[entrypoint] Waiting for PostgreSQL to accept connections..."

# pg_isready polls the server; -q suppresses output. Retry for up to 60 s.
RETRIES=60
until pg_isready -q -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-cloudish}" 2>/dev/null; do
    RETRIES=$((RETRIES - 1))
    if [ "$RETRIES" -le 0 ]; then
        echo "[entrypoint] ERROR: PostgreSQL did not become ready in time." >&2
        kill "$PG_PID" 2>/dev/null || true
        exit 1
    fi
    sleep 1
done

echo "[entrypoint] PostgreSQL is ready."

# ── Handle termination ────────────────────────────────────────────────────────

# If cloudish exits (or the container receives SIGTERM/SIGINT), stop postgres.
_term() {
    echo "[entrypoint] Caught signal, shutting down..."
    kill "$CHILD_PID" 2>/dev/null || true
    kill "$PG_PID"   2>/dev/null || true
    wait "$PG_PID"   2>/dev/null || true
}
trap _term SIGTERM SIGINT

# ── Start cloudish ────────────────────────────────────────────────────────────

echo "[entrypoint] Starting cloudish..."

# cloudish looks for cloudish.yaml in the current working directory first.
# Changing into /etc/cloudish makes it pick up the config baked into the image
# (or a bind-mounted replacement).
cd /etc/cloudish

cloudish &
CHILD_PID=$!

# Wait for cloudish to exit.
wait "$CHILD_PID"
EXIT_CODE=$?

# Stop postgres cleanly.
kill "$PG_PID" 2>/dev/null || true
wait "$PG_PID" 2>/dev/null || true

exit $EXIT_CODE

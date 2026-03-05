#!/usr/bin/env bash
# =============================================================================
# docker-entrypoint.sh — Container startup orchestration
# =============================================================================
#
# This script runs before the main application process (uvicorn) starts.
# It handles:
#   1. Waiting for PostgreSQL to accept connections
#   2. Running database migrations (placeholder for Alembic)
#   3. Starting uvicorn with proper signal handling
#
# Why a shell entrypoint instead of just CMD?
#   - Docker's depends_on with service_healthy only waits for the postgres
#     container's healthcheck to pass. But there's a race condition: the
#     healthcheck might pass before PostgreSQL is ready to accept NEW
#     connections (it could be in recovery mode). This script adds a
#     belt-and-suspenders application-level check.
#   - Alembic migrations must run BEFORE the app starts, not during
#     the FastAPI lifespan (where create_all currently lives).
#   - exec replaces the shell with uvicorn, so PID 1 is uvicorn,
#     which properly receives SIGTERM from Docker for graceful shutdown.
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Extract host/port from DATABASE_URL for pg_isready.
# DATABASE_URL format: postgresql+asyncpg://user:pass@host:port/dbname
# We need the host and port components.
DB_HOST="${DB_HOST:-postgres}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${POSTGRES_USER:-postgres}"
MAX_RETRIES="${DB_CONNECT_RETRIES:-30}"
RETRY_INTERVAL="${DB_CONNECT_RETRY_INTERVAL:-2}"

# ---------------------------------------------------------------------------
# Step 1: Wait for PostgreSQL
# ---------------------------------------------------------------------------
echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."

retries=0
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -q; do
    retries=$((retries + 1))
    if [ "$retries" -ge "$MAX_RETRIES" ]; then
        echo "ERROR: PostgreSQL not ready after ${MAX_RETRIES} attempts. Exiting."
        exit 1
    fi
    echo "  PostgreSQL not ready (attempt ${retries}/${MAX_RETRIES}), retrying in ${RETRY_INTERVAL}s..."
    sleep "$RETRY_INTERVAL"
done

echo "PostgreSQL is ready."

# ---------------------------------------------------------------------------
# Step 2: Run database migrations
# ---------------------------------------------------------------------------
# PLACEHOLDER: When you adopt Alembic for schema migrations, uncomment:
#
#   echo "Running database migrations..."
#   alembic upgrade head
#   echo "Migrations complete."
#
# Currently, table creation is handled by SQLAlchemy's Base.metadata.create_all()
# in the FastAPI lifespan (main.py). This is acceptable for coursework but
# does NOT handle schema changes — only initial table creation.
#
# To migrate to Alembic:
#   1. pip install alembic
#   2. alembic init alembic
#   3. Configure alembic.ini with DATABASE_URL
#   4. alembic revision --autogenerate -m "initial"
#   5. Uncomment the lines above
#   6. Remove create_all() from main.py lifespan (or keep as fallback)
echo "Skipping migrations (using create_all in lifespan — see entrypoint for Alembic instructions)."

# ---------------------------------------------------------------------------
# Step 3: Launch the application
# ---------------------------------------------------------------------------
# exec replaces this shell process with uvicorn, so:
#   - uvicorn becomes PID 1 in the container
#   - SIGTERM from `docker stop` goes directly to uvicorn
#   - uvicorn's graceful shutdown handler runs (closes connections, stops scheduler)
#   - No zombie shell process lingering
#
# "$@" passes through the CMD from the Dockerfile (or docker-compose override).
# This allows: docker run urban-env-api uvicorn main:app --workers 2
# to override the default CMD while still running the entrypoint checks.
echo "Starting application: $@"
exec "$@"

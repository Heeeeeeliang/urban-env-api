# =============================================================================
# Urban Environmental Intelligence API — Production Dockerfile
# =============================================================================
#
# Multi-stage build optimised for:
#   - Small image size (<200MB) via slim base + no build tools in runtime
#   - Security: non-root user, no shell in final image (optional)
#   - Reproducibility: pinned Python version, venv isolation
#   - APScheduler compatibility: single worker only (see CMD)
#
# Build:   docker build -t urban-env-api .
# Run:     docker run -p 8000:8000 --env-file .env urban-env-api
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: BUILDER — install dependencies into a virtual environment
# ---------------------------------------------------------------------------
# Why a venv inside Docker? It gives us a single directory (/opt/venv)
# to COPY into the runtime stage, cleanly separating app dependencies
# from system packages. Without this, we'd need to copy all of
# /usr/local/lib/python3.11 and risk pulling in build artifacts.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

# Build-time dependencies required by asyncpg (needs libpq-dev + gcc)
# and other C-extension wheels. These stay in the builder stage only.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment in a predictable location
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies.
# COPY requirements.txt first (before app code) to leverage Docker's
# layer caching — dependencies only re-install when requirements.txt
# changes, not on every code edit.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Install MCP server dependency (fastmcp) separately.
# This is not in requirements.txt because mcp_server.py is an optional
# component — some deployments only need the HTTP API.
RUN pip install --no-cache-dir "mcp[cli]>=1.0.0,<2.0.0"


# ---------------------------------------------------------------------------
# Stage 2: RUNTIME — minimal image with only what's needed to run
# ---------------------------------------------------------------------------
# Why a separate runtime stage?
#   - No gcc, no build-essential, no libpq-dev headers → smaller image
#   - Reduced attack surface (fewer binaries = fewer CVEs to patch)
#   - ~150MB final image vs ~450MB if we kept build tools
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

# OCI image labels (replaces deprecated MAINTAINER directive)
LABEL maintainer="Hari <hari@urban-env.dev>" \
      version="1.0.0" \
      description="Urban Environmental Intelligence API — air quality monitoring with AI insights" \
      org.opencontainers.image.source="https://github.com/hari/urban-env-api" \
      org.opencontainers.image.licenses="MIT"

# Runtime-only system dependencies:
#   - libpq5: PostgreSQL client library (required by asyncpg at runtime)
#   - curl: used by HEALTHCHECK and entrypoint pg_isready alternative
#   - postgresql-client: provides pg_isready for entrypoint health checks
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Security: create a non-root user.
# Running as root inside a container is a common misconfiguration that
# allows container-escape exploits to gain host-level root access.
# UID 1000 is conventional for the first non-system user.
RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

# Copy the virtual environment from the builder stage.
# This is the key multi-stage benefit: we get all compiled wheels
# without any of the build toolchain.
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    # Prevent Python from writing .pyc files (reduces image layer noise)
    PYTHONDONTWRITEBYTECODE=1 \
    # Force unbuffered stdout/stderr so logs appear in real-time
    # in `docker logs` and `docker compose logs`. Without this,
    # Python buffers output and logs appear delayed or not at all.
    PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy entrypoint script first (changes less frequently than app code)
COPY scripts/docker-entrypoint.sh /app/scripts/docker-entrypoint.sh
RUN chmod +x /app/scripts/docker-entrypoint.sh

# Copy application code.
# This layer changes most frequently, so it's last to maximise cache hits
# on the layers above (venv, system packages, entrypoint).
COPY . /app

# Switch to non-root user AFTER copying files (COPY creates files as root)
# chown the app directory to appuser first
RUN chown -R appuser:appuser /app
USER appuser

# Document the port (informational — doesn't actually publish it)
EXPOSE 8000

# Health check: verify the API is responding.
# --fail makes curl return exit code 22 on HTTP errors (4xx/5xx).
# --silent suppresses progress output.
# The /docs endpoint is always available (no auth required) and returns
# the Swagger UI HTML, confirming both the app and OpenAPI spec are working.
# Interval: check every 30s. Timeout: fail if no response in 10s.
# Retries: 3 failures before marking unhealthy. Start period: 40s grace
# for the app to start (table creation + scheduler init can take a moment).
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=40s \
    CMD curl --fail --silent http://localhost:8000/docs || exit 1

# Start the application via the entrypoint script.
# The entrypoint handles: waiting for postgres, running migrations,
# and launching uvicorn.
ENTRYPOINT ["/app/scripts/docker-entrypoint.sh"]

# Default CMD: uvicorn with a SINGLE worker.
#
# WHY --workers 1?
# APScheduler's AsyncIOScheduler runs inside the uvicorn worker process.
# If we set --workers 4, uvicorn forks 4 processes, each running its own
# scheduler instance. This means:
#   - 4x duplicate ingestion cycles hitting OpenAQ/Open-Meteo APIs
#   - 4x duplicate database writes (race conditions, constraint violations)
#   - 4x the API rate limit consumption
#
# For horizontal scaling, use multiple container replicas behind a load
# balancer, with only ONE replica running the scheduler (leader election
# or a dedicated scheduler container).
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

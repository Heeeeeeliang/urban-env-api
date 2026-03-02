"""
Urban Environmental Intelligence API — Application Entrypoint
=============================================================

Architectural decisions documented here:

1. LIFESPAN CONTEXT MANAGER (not @app.on_event)
   FastAPI's @app.on_event("startup") is deprecated as of v0.109.
   The lifespan context manager is the modern replacement. It provides
   a single function that handles both startup (before yield) and
   shutdown (after yield), making resource lifecycle management explicit
   and guaranteeing cleanup even on crashes. This matters because we
   manage two stateful resources: the database connection pool and the
   background ingestion scheduler.

2. CORS MIDDLEWARE
   Environmental data APIs are consumed by frontend dashboards, Jupyter
   notebooks, and third-party tools. CORS must be configured explicitly.
   We default to restrictive settings (no wildcard origins in production)
   but allow broad access in development. The allow_origins list should
   be tightened to specific domains before any real deployment.

3. ROUTER REGISTRATION WITH /api/v1 PREFIX
   URL-prefix versioning from day one, even though we'll likely never
   ship v2 in a coursework project. This is a near-zero-cost decision
   now that would be extremely expensive to retrofit later. It also
   signals to API consumers that the contract is stable within a version.

4. SEPARATION OF CONCERNS
   main.py does exactly three things: configure the app, wire up
   middleware, and register routers. It contains zero business logic.
   If you find yourself importing models or writing queries here,
   something has gone wrong architecturally.
"""

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.database import engine
from app.models.models import Base

# ---------------------------------------------------------------------------
# Logging — configured early so all modules inherit the same format.
# In production you'd use structured logging (e.g. structlog) to make
# logs machine-parseable. For coursework, standard logging is sufficient.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan: manages the full lifecycle of stateful resources.
#
# Why async? Our database engine (asyncpg) and future HTTP clients
# (httpx.AsyncClient for OpenAQ/Open-Meteo) are all async. Using a
# sync lifespan would force us into awkward event-loop bridging.
#
# Resource lifecycle:
#   STARTUP  → create tables, start scheduler, warm up connections
#   SHUTDOWN → dispose engine (closes all pooled connections), stop scheduler
#
# The `yield` separates startup from shutdown. FastAPI guarantees the
# code after yield runs even if the server is killed with SIGTERM.
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Urban Environmental Intelligence API")
    logger.info(f"Environment: {settings.ENVIRONMENT}")

    # --- STARTUP -----------------------------------------------------------

    # Create all tables if they don't exist.
    # In production, you'd use Alembic migrations instead of create_all().
    # create_all() is acceptable here because:
    #   (a) We're in a coursework context with a single developer.
    #   (b) It's idempotent — safe to run on every restart.
    #   (c) It doesn't handle schema *changes*, only initial creation.
    # The technical report should note that Alembic would be required for
    # any schema evolution in a production system.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables verified/created")

    # TODO: Initialise APScheduler here for background data ingestion.
    # The scheduler should be attached to app.state so it's accessible
    # from route handlers (e.g. for a /admin/trigger-ingestion endpoint).
    from app.services.ingestion_service import setup_scheduler

    scheduler = setup_scheduler()
    scheduler.start()
    app.state.scheduler = scheduler
    logger.info("Background ingestion scheduler started")

    yield  # ← Application is running and serving requests

    # --- SHUTDOWN ----------------------------------------------------------
    logger.info("Shutting down: stopping scheduler and disposing connection pool")

    if hasattr(app.state, "scheduler"):
        app.state.scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")

    await engine.dispose()
    logger.info("Shutdown complete")


# ---------------------------------------------------------------------------
# App factory
#
# We configure the app with OpenAPI metadata that makes the auto-generated
# /docs page useful as actual API documentation. FastAPI generates OpenAPI
# 3.1 specs automatically — this is one of the key reasons we chose FastAPI
# over Flask/Django for this project. The generated docs serve double duty:
# they're the interactive API documentation AND the spec that tools like
# Postman can import.
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Urban Environmental Intelligence API",
    description=(
        "A data-driven API integrating real-time air quality (OpenAQ) and "
        "weather data (Open-Meteo) for multiple cities. Provides CRUD "
        "operations, time-series analytics, anomaly detection, cross-metric "
        "correlation, and AI-powered environmental insight generation."
    ),
    version="1.0.0",
    lifespan=lifespan,
    # Serve docs at /docs (Swagger UI) and /redoc (ReDoc).
    # In production, you might disable these or put them behind auth.
    docs_url="/docs",
    redoc_url="/redoc",
)


# ---------------------------------------------------------------------------
# CORS Middleware
#
# Why not a wildcard ("*") for allow_origins?
# A wildcard disables credential-based requests (cookies, Authorization
# headers). Since our API uses API key authentication via headers, a
# wildcard would silently break authenticated requests from browsers.
# We explicitly list allowed origins instead.
#
# For development/coursework, we allow localhost on common ports.
# The CORS_ORIGINS setting can be overridden via environment variable
# for deployment flexibility.
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Router Registration
#
# Each router is a self-contained module responsible for one domain.
# The /api/v1 prefix is applied here, not in individual routers, so
# routers remain version-agnostic and reusable if we ever create /api/v2.
#
# Registration order doesn't affect functionality, but we list them in
# dependency order for readability: cities (reference data) before
# readings (depends on cities) before analytics (depends on readings).
# ---------------------------------------------------------------------------
from app.routers import cities, analytics, ai

app.include_router(cities.router, prefix="/api/v1", tags=["Cities"])
app.include_router(analytics.router, prefix="/api/v1", tags=["Analytics"])
app.include_router(ai.router, prefix="/api/v1", tags=["AI Insights"])

# TODO: Uncomment as routers are implemented:
# from app.routers import readings
# app.include_router(readings.router,   prefix="/api/v1", tags=["Readings"])


# ---------------------------------------------------------------------------
# Health Check Endpoint
#
# This lives in main.py rather than a router because it's infrastructure,
# not business logic. It serves three purposes:
#   1. Load balancer health probes (is the service alive?)
#   2. Monitoring data freshness (when did we last ingest data?)
#   3. Debugging deployment issues (can we reach the database?)
#
# A production health check would also report:
#   - External API reachability (OpenAQ, Open-Meteo, Anthropic)
#   - Scheduler status (is the ingestion job running?)
#   - Memory/CPU usage
# We keep it simple but extensible.
# ---------------------------------------------------------------------------
@app.get(
    "/health",
    tags=["Infrastructure"],
    summary="Service health and data freshness check",
)
async def health_check():
    """
    Returns service status, database connectivity, and data staleness.
    Used by monitoring systems and as a quick diagnostic endpoint.
    """
    from datetime import datetime, timezone
    from sqlalchemy import text
    from app.core.database import async_session_factory

    health = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": app.version,
        "environment": settings.ENVIRONMENT,
        "database": {"connected": False},
        "data_freshness": {
            "last_ingestion_at": None,
            "staleness_minutes": None,
        },
    }

    try:
        async with async_session_factory() as session:
            # Check 1: Can we reach the database?
            await session.execute(text("SELECT 1"))
            health["database"]["connected"] = True

            # Check 2: When was the most recent data ingestion?
            # This query is cheap: it hits the index on (timestamp DESC)
            # and returns a single row.
            result = await session.execute(
                text("SELECT MAX(created_at) AS last_ingestion FROM readings")
            )
            row = result.first()

            if row and row.last_ingestion:
                last_ingestion = row.last_ingestion
                health["data_freshness"]["last_ingestion_at"] = (
                    last_ingestion.isoformat()
                )
                staleness = datetime.now(timezone.utc) - last_ingestion
                health["data_freshness"]["staleness_minutes"] = round(
                    staleness.total_seconds() / 60, 1
                )

                # Flag as degraded if data is older than 2x the ingestion interval.
                # This means one missed cycle is tolerated, but two is a problem.
                if staleness.total_seconds() > (
                    settings.INGESTION_INTERVAL_MINUTES * 2 * 60
                ):
                    health["status"] = "degraded"
                    health["data_freshness"]["warning"] = (
                        "Data is stale — ingestion may have failed"
                    )

    except Exception as e:
        health["status"] = "unhealthy"
        health["database"]["connected"] = False
        health["database"]["error"] = str(e)

    return health

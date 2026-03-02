"""
Database Engine & Session Management
=====================================

Architectural decisions documented here:

1. ASYNC SQLAlchemy (not sync SQLAlchemy + run_in_executor)
   FastAPI is built on Starlette's async foundation. Using synchronous
   database calls would block the event loop, serialising all DB queries
   through a thread pool. With asyncpg (PostgreSQL's async driver), DB
   queries yield control back to the event loop while waiting for results,
   allowing concurrent request handling on a single worker process.

   This matters specifically for our API because:
     - The ingestion pipeline makes external HTTP calls AND DB writes.
       With sync I/O, an ingestion task would block all API responses.
     - The AI insight endpoint calls the Anthropic API (3-10s latency).
       Without async, this would freeze the entire server for each request.

2. SESSION-PER-REQUEST PATTERN (via get_db dependency)
   Each FastAPI request gets its own database session, injected via
   Depends(get_db). The session is:
     - Created at the start of request handling.
     - Committed on success (explicitly by the route handler).
     - Rolled back on exception (by the finally block in the generator).
     - Closed at the end of the request (returning the connection to the pool).

   This prevents a dangerous anti-pattern: sharing a session across
   requests, which causes one request's uncommitted transaction to leak
   into another request's queries. This is particularly insidious with
   async code where interleaving is non-deterministic.

3. CONNECTION POOLING
   asyncpg maintains a pool of persistent TCP connections to PostgreSQL.
   Creating a new connection per request would add ~5-20ms of TCP/TLS
   handshake overhead. The pool recycles connections, amortising that cost.

   Pool sizing (from config):
     - pool_size=5: Persistent connections, always open.
     - max_overflow=10: Burst connections, opened on demand, closed after use.
   Total max connections: pool_size + max_overflow = 15.
   This must be less than PostgreSQL's max_connections (default: 100).

4. WHY NOT RAW asyncpg?
   asyncpg is faster than SQLAlchemy+asyncpg for raw queries (~20% lower
   latency). But SQLAlchemy gives us:
     - ORM models that serve as living documentation of the schema.
     - The Unit of Work pattern (session.add/commit/rollback) that prevents
       partial writes.
     - Declarative table definitions that generate CREATE TABLE statements
       (used in our lifespan startup).
     - Portability: if we ever switch to SQLite for testing, we change one
       connection string instead of rewriting all queries.
   For a coursework project, developer productivity matters more than
   the marginal performance of raw asyncpg.
"""

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool
from typing import AsyncGenerator

from app.core.config import settings

# ---------------------------------------------------------------------------
# Engine — the core connection manager.
#
# `echo=False`: Set to True during development to log every SQL statement.
#   Useful for catching N+1 queries, but extremely noisy in production.
#   Consider using SQLAlchemy's event system for selective query logging
#   instead of blanket echo=True.
#
# `pool_pre_ping=True`: Before handing a connection to a request, the pool
#   sends a lightweight "SELECT 1" to verify the connection is alive.
#   This handles the case where PostgreSQL has killed an idle connection
#   (common with cloud-hosted databases that enforce idle timeouts).
#   The cost is one extra round-trip per connection checkout, which is
#   negligible compared to the cost of a failed request due to a dead connection.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Dialect-aware engine configuration.
#
# SQLite (used in testing) does not support pool_size, max_overflow,
# or pool_recycle. These are QueuePool parameters and SQLite uses
# StaticPool. We detect the dialect from the URL and configure accordingly.
#
# This is the exact portability benefit documented above — changing one
# connection string switches from PostgreSQL to SQLite for testing.
# ---------------------------------------------------------------------------
_is_sqlite = settings.DATABASE_URL.startswith("sqlite")

if _is_sqlite:
    from sqlalchemy.pool import StaticPool

    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
else:
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_size=settings.DB_POOL_SIZE,
        max_overflow=settings.DB_MAX_OVERFLOW,
        pool_pre_ping=True,
        # pool_recycle=3600: Recycle connections after 1 hour to prevent
        # issues with firewalls/load balancers that silently drop long-lived
        # TCP connections. This is a defensive measure — you may never hit it,
        # but when you do, the failure mode is extremely hard to diagnose.
        pool_recycle=3600,
    )


# ---------------------------------------------------------------------------
# Session Factory
#
# async_sessionmaker creates a factory (not an instance). Each call to
# async_session_factory() produces a new AsyncSession bound to the engine.
#
# expire_on_commit=False:
#   By default, SQLAlchemy expires all loaded attributes after commit(),
#   forcing a re-query on next access. This is a safety feature for sync
#   code (ensures you see the latest DB state), but in async code it causes
#   unexpected implicit I/O — accessing an attribute after commit triggers
#   a lazy load, which may fail if the session is already closed.
#
#   Setting expire_on_commit=False means objects retain their in-memory
#   state after commit. This is safe in our request-scoped pattern because
#   we never reuse objects across requests.
# ---------------------------------------------------------------------------
async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


# ---------------------------------------------------------------------------
# Dependency: get_db
#
# This is a FastAPI dependency generator. It's used as:
#   @router.get("/cities")
#   async def list_cities(db: AsyncSession = Depends(get_db)):
#       ...
#
# The generator pattern guarantees cleanup:
#   - yield gives the route handler a live session.
#   - The finally block always closes the session, even if the handler
#     throws an exception.
#   - We don't commit here — that's the handler's responsibility.
#     Auto-committing in the dependency would make it impossible to
#     perform multi-step operations that should be atomic.
#
# Why a generator and not a context manager?
#   FastAPI's Depends() system natively supports async generators.
#   Using a generator keeps the dependency injection clean and avoids
#   nesting `async with` blocks in every route handler.
# ---------------------------------------------------------------------------
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Provide a transactional database session for the duration of a request.

    Usage:
        @router.get("/example")
        async def example(db: AsyncSession = Depends(get_db)):
            result = await db.execute(select(City))
            return result.scalars().all()
    """
    async with async_session_factory() as session:
        try:
            yield session
        except Exception:
            # Roll back any uncommitted changes on error.
            # This prevents partial writes from leaking into the database.
            await session.rollback()
            raise
        finally:
            # Always close the session, returning the connection to the pool.
            await session.close()

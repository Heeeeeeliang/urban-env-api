"""
Test Configuration — conftest.py
==================================

WHY SQLITE IN-MEMORY FOR TESTS?

The production stack uses PostgreSQL, but tests use SQLite in-memory.
This is a deliberate trade-off:

  Advantages:
    - Zero infrastructure: no Docker container, no pg_isready wait.
    - Speed: in-memory SQLite is ~50x faster than network PostgreSQL.
    - Isolation: each test function gets a fresh database (no teardown bugs).
    - CI-friendly: works on GitHub Actions runners with no extra services.

  What we lose:
    - PostgreSQL-specific functions (gen_random_uuid, corr(), date_trunc).
    - Window function edge cases where SQLite and PG semantics diverge.
    - PostGIS spatial queries.

  How we mitigate:
    - Python-side UUID generation (uuid.uuid4()) bypasses gen_random_uuid().
    - Analytics SQL that uses corr() or date_trunc() is tested in integration
      tests against a real PostgreSQL instance (not in this conftest).
    - This conftest covers CRUD operations, HTTP status codes, caching logic,
      and request/response contracts — none of which need PG-specific features.

FIXTURE SCOPING:

  All fixtures use scope="function" (the default). This means every test
  function gets:
    - A fresh SQLite database (all tables recreated)
    - A fresh FastAPI app instance (dependency overrides isolated)
    - A fresh httpx client (no shared cookies or headers)
    - Fresh seed data (known state, no test ordering dependencies)

  scope="session" would be faster but creates test coupling — if test A
  modifies data, test B might see it. For a 30-test suite, the extra
  ~200ms of per-test setup is worth the isolation guarantee.
"""

import uuid
import random
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.models.models import Base
from app.models.models import City
from app.models.models import Reading


# ---------------------------------------------------------------------------
# DATABASE ENGINE (SQLite in-memory, dialect-aware)
# ---------------------------------------------------------------------------
#
# Key differences from production engine (core/database.py):
#   - "sqlite+aiosqlite://" instead of "postgresql+asyncpg://..."
#   - No pool_size, max_overflow, pool_recycle — SQLite doesn't use
#     connection pooling. Passing these would raise a warning.
#   - echo=False to keep test output clean (set True to debug SQL).
#
# The engine is created at module level (not per-test) because engine
# creation is expensive and stateless — the per-test isolation comes
# from create_all/drop_all in the `app` fixture, not from separate engines.
# ---------------------------------------------------------------------------

TEST_ENGINE = create_async_engine(
    "sqlite+aiosqlite://",
    echo=False,
)

TestSessionFactory = async_sessionmaker(
    TEST_ENGINE,
    class_=AsyncSession,
    expire_on_commit=False,
)


# ---------------------------------------------------------------------------
# FIXTURE 1: app — FastAPI test application with overridden DB
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="function")
async def app():
    """
    Yield a FastAPI app wired to the in-memory SQLite test database.

    Lifecycle per test:
      1. Create all tables (Base.metadata.create_all)
      2. Override the get_db dependency to use TestSessionFactory
      3. Yield the app for the test to use
      4. Drop all tables (clean slate for next test)

    The dependency override is the critical piece. In production,
    get_db() yields sessions from the PostgreSQL async_session_factory.
    Here we swap it to yield from TestSessionFactory instead. FastAPI's
    dependency injection system makes this a one-liner — no monkey-patching,
    no import hacks. This is why we use Depends() instead of global imports.
    """
    # Create tables fresh for this test
    async with TEST_ENGINE.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Import here (not at module top) so the override is applied before
    # any router code runs. This also avoids circular import issues
    # if conftest is loaded before the app modules.
    from main import app as fastapi_app
    from app.core.database import get_db

    async def _override_get_db():
        async with TestSessionFactory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    fastapi_app.dependency_overrides[get_db] = _override_get_db

    yield fastapi_app

    # Cleanup: remove override and drop all tables
    fastapi_app.dependency_overrides.clear()
    async with TEST_ENGINE.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


# ---------------------------------------------------------------------------
# FIXTURE 2: client — httpx AsyncClient bound to the test app
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="function")
async def client(app):
    """
    Yield an httpx AsyncClient that sends requests to the test app.

    Why httpx instead of TestClient (from starlette)?
      - TestClient is synchronous, wrapping async code in a thread.
        This works but hides async bugs (e.g. forgetting to await).
      - AsyncClient with ASGITransport runs in the same event loop
        as the app, giving true async-to-async testing.
      - httpx's API matches requests (familiar), and the same client
        class works for both ASGI testing and real HTTP calls.

    The base_url is arbitrary (no network is used) but must be a valid
    URL for httpx to construct request objects.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-Api-Key": "test-key"},
    ) as ac:
        yield ac


# ---------------------------------------------------------------------------
# FIXTURE 3: seed_data — deterministic test dataset
# ---------------------------------------------------------------------------
#
# Seed data design principles:
#   - DETERMINISTIC: seeded random (seed=42) so failures are reproducible.
#   - MINIMAL BUT SUFFICIENT: 2 cities, 50 readings — enough to exercise
#     all CRUD and basic analytics without slowing tests.
#   - ONE KNOWN ANOMALY: value=45.0 at a specific timestamp, so anomaly
#     detection tests can assert on a known spike without fragile
#     statistical threshold tuning.
#   - REALISTIC DISTRIBUTION: values 10-15 µg/m³ with gaussian noise
#     (mean=12.5, stddev=1.5) approximates real urban PM2.5 levels.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="function")
async def seed_data(app):
    """
    Insert 2 cities and 50 readings into the test database.

    Returns a dict with references for test assertions:
      {
        "cities": [City, City],
        "readings": [Reading, ...],       # all 50
        "spike_reading": Reading,          # the value=45.0 anomaly
        "spike_timestamp": datetime,       # 2026-02-20T19:00:00Z
      }

    The spike at index 43 (hour 43 from base) lands on 2026-02-20T19:00Z.
    This placement ensures anomaly detection has enough preceding data
    for a rolling baseline, and enough following data to confirm the
    spike is isolated (not a trend shift).
    """
    rng = random.Random(42)

    async with TestSessionFactory() as session:
        # --- Cities ---
        london = City(
            id="london",
            name="London",
            country="United Kingdom",
            country_code="GB",
            latitude=51.5074,
            longitude=-0.1278,
            timezone="Europe/London",
            is_active=True,
        )
        manchester = City(
            id="manchester",
            name="Manchester",
            country="United Kingdom",
            country_code="GB",
            latitude=53.4808,
            longitude=-2.2426,
            timezone="Europe/London",
            is_active=True,
        )
        session.add_all([london, manchester])
        await session.flush()

        # --- Readings for London ---
        # 50 hourly readings starting 2026-02-19T00:00:00Z
        # All pm25, mostly 10-15 µg/m³, one spike at hour 43 (Feb 20 19:00)
        base_time = datetime(2026, 2, 19, 0, 0, 0, tzinfo=timezone.utc)
        spike_index = 43  # 2026-02-20T19:00:00Z = base + 43 hours
        spike_timestamp = base_time + timedelta(hours=spike_index)

        readings = []
        spike_reading = None

        for i in range(50):
            ts = base_time + timedelta(hours=i)

            if i == spike_index:
                value = 45.0
            else:
                # Gaussian noise around 12.5 µg/m³, clamped to [8, 18]
                value = round(max(8.0, min(18.0, rng.gauss(12.5, 1.5))), 2)

            reading = Reading(
                id=uuid.uuid4(),
                city_id="london",
                parameter="pm25",
                value=value,
                unit="µg/m³",
                quality_flag="valid",
                source="test_seed",
                timestamp=ts,
            )
            readings.append(reading)

            if i == spike_index:
                spike_reading = reading

        session.add_all(readings)
        await session.commit()

        # Refresh to populate server-side defaults (created_at, updated_at)
        for obj in [london, manchester] + readings:
            await session.refresh(obj)

        yield {
            "cities": [london, manchester],
            "readings": readings,
            "spike_reading": spike_reading,
            "spike_timestamp": spike_timestamp,
        }

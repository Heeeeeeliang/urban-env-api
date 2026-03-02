"""
Integration Tests — Analytics Endpoints
==========================================

These tests verify the HTTP contract (routing, parameter validation,
status codes, response serialisation) for the analytics endpoints.

WHY MOCK THE SERVICE LAYER?

  The analytics SQL uses PostgreSQL-specific syntax that SQLite cannot
  execute:
    - `::numeric` casts     (trend, anomaly, compare, correlation)
    - `date_trunc()`        (trend)
    - `ANY(:array_param)`   (compare)
    - `STDDEV() OVER w`     (anomaly — SQLite lacks population stddev)
    - `corr()`              (correlation)

  Rather than registering fragile SQLite shims for every PG function,
  we mock the AnalyticsService methods. This is the standard test
  architecture for dialect-dependent SQL:

    Unit tests    → pure functions (test_validation.py) — no DB
    HTTP contract → mock service, real HTTP layer (this file)
    SQL correctness → requires real PostgreSQL (separate CI job)

  The mock return values use the seed_data fixture's known spike (value=45.0 at
  2026-02-20T19:00:00Z) so assertions remain semantically meaningful
  rather than testing arbitrary dummy data.

TEST STRUCTURE:

  1. Trend (happy path)     → 200, non-empty data list
  2. Anomaly detection      → 200, spike timestamp present in results
  3. Trend (no data city)   → 200, empty data list (graceful, not 500)
  4. Multi-city comparison  → 200, London ranked, Manchester ranked
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from app.services.city_service import CityNotFoundError


# -------------------------------------------------------------------
# MOCK RETURN VALUES
# -------------------------------------------------------------------
# These match the dict structures returned by AnalyticsService methods
# (verified against services/analytics_service.py return statements).
# Values are derived from the seed_data fixture's known distribution: 49 readings
# ~12.5 µg/m³ + 1 spike at 45.0 → avg ≈ 13.2, max = 45.0.
# -------------------------------------------------------------------


TREND_LONDON = {
    "city_id": "london",
    "city_name": "London",
    "parameter": "pm25",
    "unit": "µg/m³",
    "interval": "daily",
    "days": 30,
    "data": [
        {
            "period": datetime(2026, 2, 19, 0, 0, tzinfo=timezone.utc),
            "avg": 12.48,
            "min": 9.21,
            "max": 15.73,
            "reading_count": 24,
        },
        {
            "period": datetime(2026, 2, 20, 0, 0, tzinfo=timezone.utc),
            "avg": 14.91,
            "min": 8.84,
            "max": 45.0,
            "reading_count": 24,
        },
    ],
    "total_readings": 48,
}

TREND_MANCHESTER_EMPTY = {
    "city_id": "manchester",
    "city_name": "Manchester",
    "parameter": "pm25",
    "unit": "µg/m³",
    "interval": "daily",
    "days": 30,
    "data": [],
    "total_readings": 0,
}

ANOMALIES_LONDON = {
    "city_id": "london",
    "city_name": "London",
    "parameter": "pm25",
    "unit": "µg/m³",
    "sensitivity": 2.0,
    "anomalies": [
        {
            "timestamp": datetime(2026, 2, 20, 19, 0, tzinfo=timezone.utc),
            "value": 45.0,
            "rolling_avg": 12.53,
            "rolling_stddev": 1.87,
            "z_score": 17.36,
            "severity": "high",
        },
    ],
    "total_readings_analysed": 50,
}

COMPARE_RESULT = {
    "parameter": "pm25",
    "unit": "µg/m³",
    "days": 7,
    "cities": [
        {
            "city_id": "manchester",
            "city_name": "Manchester",
            "avg": 0.0,
            "min": 0.0,
            "max": 0.0,
            "reading_count": 0,
            "rank": 1,
            "pct_change_vs_prev_period": None,
        },
        {
            "city_id": "london",
            "city_name": "London",
            "avg": 13.16,
            "min": 8.84,
            "max": 45.0,
            "reading_count": 50,
            "rank": 2,
            "pct_change_vs_prev_period": None,
        },
    ],
}


# ===================================================================
# 1. TREND — happy path
# ===================================================================


@pytest.mark.asyncio
@patch(
    "app.routers.analytics.AnalyticsService",
    autospec=True,
)
async def test_trend_returns_non_empty_data(MockService, client, seed_data):
    """
    GET /analytics/trend/london?parameter=pm25&days=30 returns 200
    with a non-empty list of aggregated data points.

    The mock simulates what PostgreSQL would produce: two daily
    buckets covering the 50 hourly readings in seed_data.
    """
    instance = MockService.return_value
    instance.get_trend = AsyncMock(return_value=TREND_LONDON)

    resp = await client.get(
        "/api/v1/analytics/trend/london",
        params={"parameter": "pm25", "days": 30, "interval": "day"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["city_id"] == "london"
    assert body["parameter"] == "pm25"
    assert len(body["data"]) > 0
    assert body["total_readings"] > 0

    # Verify data points have required fields
    point = body["data"][0]
    assert "period" in point
    assert "avg" in point
    assert "min" in point
    assert "max" in point
    assert "reading_count" in point


# ===================================================================
# 2. ANOMALY — spike detected
# ===================================================================


@pytest.mark.asyncio
@patch(
    "app.routers.analytics.AnalyticsService",
    autospec=True,
)
async def test_anomaly_detects_spike(MockService, client, seed_data):
    """
    GET /analytics/anomalies/london?parameter=pm25&sensitivity=2.0
    returns 200 with the known spike at 2026-02-20T19:00:00Z.

    The spike (value=45.0 vs baseline ~12.5) produces a z-score
    of ~17.4 — well above the sensitivity=2.0 threshold. This tests
    that the endpoint correctly includes the anomaly timestamp in
    its response.
    """
    instance = MockService.return_value
    instance.detect_anomalies = AsyncMock(return_value=ANOMALIES_LONDON)

    resp = await client.get(
        "/api/v1/analytics/anomalies/london",
        params={"parameter": "pm25", "sensitivity": 2.0},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["city_id"] == "london"
    assert body["sensitivity"] == 2.0
    assert len(body["anomalies"]) >= 1

    # The spike at 2026-02-20T19:00:00Z must be present
    timestamps = [a["timestamp"] for a in body["anomalies"]]
    assert any("2026-02-20T19:00:00" in ts for ts in timestamps)

    # Verify the spike's z-score is extreme (well above threshold)
    spike = body["anomalies"][0]
    assert spike["value"] == 45.0
    assert spike["z_score"] > 2.0
    assert spike["severity"] == "high"


# ===================================================================
# 3. TREND — city with no readings (graceful empty)
# ===================================================================


@pytest.mark.asyncio
@patch(
    "app.routers.analytics.AnalyticsService",
    autospec=True,
)
async def test_trend_empty_city_returns_200_with_empty_data(
    MockService, client, seed_data
):
    """
    GET /analytics/trend/manchester?parameter=pm25 returns 200 with
    an empty data list — not 500.

    Manchester exists in seed_data but has zero readings. The service
    should return a valid response with data=[] and total_readings=0,
    not crash or return a server error. This is important because
    newly added cities will have no data until the first ingestion.
    """
    instance = MockService.return_value
    instance.get_trend = AsyncMock(return_value=TREND_MANCHESTER_EMPTY)

    resp = await client.get(
        "/api/v1/analytics/trend/manchester",
        params={"parameter": "pm25"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["city_id"] == "manchester"
    assert body["data"] == []
    assert body["total_readings"] == 0


# ===================================================================
# 4. COMPARE — multi-city ranking
# ===================================================================


@pytest.mark.asyncio
@patch(
    "app.routers.analytics.AnalyticsService",
    autospec=True,
)
async def test_compare_ranks_cities(MockService, client, seed_data):
    """
    GET /analytics/compare?cities=london&cities=manchester&parameter=pm25
    returns 200 with both cities ranked.

    Ranking is ASC (lower pollution = rank 1). Manchester has no
    readings (avg=0), so it ranks first. London's avg ≈ 13.2 µg/m³
    from the seed data gives it a higher (worse) rank.

    The key assertion: both cities appear in the response with valid
    rank values, and the response structure matches CompareResponse.
    """
    instance = MockService.return_value
    instance.compare_cities = AsyncMock(return_value=COMPARE_RESULT)

    resp = await client.get(
        "/api/v1/analytics/compare",
        params={
            "cities": ["london", "manchester"],
            "parameter": "pm25",
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["parameter"] == "pm25"
    assert len(body["cities"]) == 2

    # Extract by city_id for deterministic assertions
    by_id = {c["city_id"]: c for c in body["cities"]}
    assert "london" in by_id
    assert "manchester" in by_id

    # Both must have a rank
    assert by_id["london"]["rank"] >= 1
    assert by_id["manchester"]["rank"] >= 1

    # London has higher avg → higher (worse) rank number
    assert by_id["london"]["rank"] > by_id["manchester"]["rank"]

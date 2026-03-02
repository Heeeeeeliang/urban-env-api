"""
Analytics Router — HTTP Interface for Analytical Endpoints
============================================================

Five endpoints covering all analytical query patterns:

  1. GET /analytics/trend/{city_id}       — Time-aggregated trend
  2. GET /analytics/compare               — Multi-city comparison
  3. GET /analytics/anomalies/{city_id}   — Anomaly detection
  4. GET /analytics/correlation/{city_id} — Cross-metric correlation
  5. GET /analytics/ranking               — City ranking by parameter

Each endpoint translates HTTP query parameters into service method
calls and maps domain exceptions to HTTP status codes. The service
layer owns all SQL logic; the router owns all HTTP logic.

QUERY PARAMETER VALIDATION:
  FastAPI validates query parameter types and constraints (ge, le, etc.)
  automatically via the function signature. Invalid values produce a
  422 with a detailed error message — no manual validation needed.

RESPONSE MODEL ENFORCEMENT:
  Every endpoint declares its response_model, which:
    - Generates accurate OpenAPI documentation.
    - Strips any fields not in the schema (defence against data leaks).
    - Validates the response data at runtime (catches service layer bugs).
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.deps import DbSession, verify_api_key
from app.schemas.schemas import (
    TimeInterval,
    TrendResponse,
    CompareResponse,
    AnomalyResponse,
    CorrelationResponse,
    RankingResponse,
)
from app.schemas.schemas import ErrorResponse
from app.services.analytics_service import AnalyticsService
from app.services.city_service import CityNotFoundError

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/analytics",
    tags=["Analytics"],
    dependencies=[Depends(verify_api_key)],
)


def get_analytics_service() -> AnalyticsService:
    return AnalyticsService()


# ===========================================================================
# 1. GET /analytics/trend/{city_id} — Time-Aggregated Trend
# ===========================================================================


@router.get(
    "/trend/{city_id}",
    response_model=TrendResponse,
    summary="Time-aggregated trend analysis",
    description=(
        "Compute aggregated statistics (avg, min, max, count) for a single "
        "parameter over time, grouped by the specified interval. "
        "Supports hourly, daily, and weekly aggregation. "
        "Uses `date_trunc()` with the composite index on "
        "`(city_id, parameter, timestamp)` for efficient GROUP BY."
    ),
    responses={
        200: {"model": TrendResponse},
        404: {"description": "City not found", "model": ErrorResponse},
    },
)
async def get_trend(
    city_id: str,
    db: DbSession,
    parameter: str = Query(
        ...,
        description="Environmental parameter to analyse (e.g. 'pm25', 'temperature').",
        examples=["pm25"],
    ),
    days: int = Query(
        30,
        ge=1,
        le=365,
        description="Number of days of historical data to include.",
    ),
    interval: TimeInterval = Query(
        TimeInterval.daily,
        description="Aggregation interval: hourly, daily, or weekly.",
    ),
    service: AnalyticsService = Depends(get_analytics_service),
) -> TrendResponse:
    """
    Example: `GET /analytics/trend/london?parameter=pm25&days=30&interval=daily`

    Returns daily average PM2.5 for London over the last 30 days.
    """
    try:
        result = await service.get_trend(
            db,
            city_id=city_id,
            parameter=parameter,
            days=days,
            interval=interval.value,  # Convert enum to PostgreSQL date_trunc arg
        )
        return TrendResponse(**result)

    except CityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"City '{city_id}' not found"},
        )


# ===========================================================================
# 2. GET /analytics/compare — Multi-City Comparison
# ===========================================================================


@router.get(
    "/compare",
    response_model=CompareResponse,
    summary="Compare a parameter across multiple cities",
    description=(
        "Rank multiple cities by their average value for a given parameter. "
        "Includes period-over-period percentage change: if days=7, compares "
        "the last 7 days against the preceding 7 days. "
        "Uses a CTE with RANK() window function and LEFT JOIN for the "
        "previous period comparison."
    ),
    responses={
        200: {"model": CompareResponse},
        422: {"description": "Fewer than 2 cities provided", "model": ErrorResponse},
    },
)
async def compare_cities(
    db: DbSession,
    cities: list[str] = Query(
        ...,
        description=(
            "City IDs to compare. Provide multiple values: "
            "`?cities=london&cities=manchester&cities=birmingham`"
        ),
        examples=["london"],
        min_length=1,
    ),
    parameter: str = Query(
        ...,
        description="Parameter to compare across cities.",
        examples=["pm25"],
    ),
    days: int = Query(
        7,
        ge=1,
        le=365,
        description="Number of days for the comparison period.",
    ),
    service: AnalyticsService = Depends(get_analytics_service),
) -> CompareResponse:
    """
    Example: `GET /analytics/compare?cities=london&cities=manchester&parameter=pm25&days=7`
    """
    if len(cities) < 2:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "insufficient_cities",
                "message": "At least 2 city IDs are required for comparison.",
            },
        )

    result = await service.compare_cities(
        db,
        city_ids=cities,
        parameter=parameter,
        days=days,
    )
    return CompareResponse(**result)


# ===========================================================================
# 3. GET /analytics/anomalies/{city_id} — Anomaly Detection
# ===========================================================================


@router.get(
    "/anomalies/{city_id}",
    response_model=AnomalyResponse,
    summary="Detect anomalies using z-score analysis",
    description=(
        "Identify readings that deviate significantly from a 7-day rolling "
        "baseline. Uses a window function over 168 preceding rows (7 days "
        "of hourly data) to compute rolling mean and standard deviation. "
        "Readings exceeding the sensitivity threshold (in standard deviations) "
        "are classified as anomalies with severity levels: "
        "low (|z| < 2.5), medium (|z| < 3.5), high (|z| ≥ 3.5). "
        "Results are ordered by z-score magnitude (most extreme first)."
    ),
    responses={
        200: {"model": AnomalyResponse},
        404: {"description": "City not found", "model": ErrorResponse},
    },
)
async def detect_anomalies(
    city_id: str,
    db: DbSession,
    parameter: str = Query(
        ...,
        description="Parameter to analyse for anomalies.",
        examples=["pm25"],
    ),
    sensitivity: float = Query(
        2.0,
        ge=1.0,
        le=5.0,
        description=(
            "Z-score threshold for flagging anomalies. Lower values "
            "detect more anomalies (more sensitive). Default: 2.0. "
            "Recommended range: 1.5 (very sensitive) to 3.5 (conservative)."
        ),
    ),
    days: int = Query(
        30,
        ge=7,
        le=365,
        description=(
            "Analysis window in days. Minimum 7 to ensure the rolling "
            "baseline has enough data after the warmup period."
        ),
    ),
    service: AnalyticsService = Depends(get_analytics_service),
) -> AnomalyResponse:
    """
    Example: `GET /analytics/anomalies/london?parameter=pm25&sensitivity=2.0&days=30`

    Returns anomalous PM2.5 readings in London over the last 30 days.
    """
    try:
        result = await service.detect_anomalies(
            db,
            city_id=city_id,
            parameter=parameter,
            sensitivity=sensitivity,
            days=days,
        )
        return AnomalyResponse(**result)

    except CityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"City '{city_id}' not found"},
        )


# ===========================================================================
# 4. GET /analytics/correlation/{city_id} — Cross-Metric Correlation
# ===========================================================================


@router.get(
    "/correlation/{city_id}",
    response_model=CorrelationResponse,
    summary="Compute correlation between two parameters",
    description=(
        "Calculate the Pearson correlation coefficient between two "
        "environmental parameters for a single city. Uses a self-join "
        "on the readings table, pairing values by matching timestamp. "
        "The composite index `(city_id, parameter, timestamp)` enables "
        "an efficient merge join on both sides. "
        "Returns the coefficient, sample size, and a human-readable "
        "interpretation of the correlation strength."
    ),
    responses={
        200: {"model": CorrelationResponse},
        404: {"description": "City not found", "model": ErrorResponse},
        422: {"description": "Same parameter for both", "model": ErrorResponse},
    },
)
async def compute_correlation(
    city_id: str,
    db: DbSession,
    param1: str = Query(
        ...,
        description="First parameter (e.g. 'pm25').",
        examples=["pm25"],
    ),
    param2: str = Query(
        ...,
        description="Second parameter (e.g. 'temperature').",
        examples=["temperature"],
    ),
    days: int = Query(
        30,
        ge=1,
        le=365,
        description="Number of days of paired data to include.",
    ),
    service: AnalyticsService = Depends(get_analytics_service),
) -> CorrelationResponse:
    """
    Example: `GET /analytics/correlation/london?param1=pm25&param2=temperature&days=30`

    Computes the correlation between PM2.5 and temperature in London.
    """
    if param1 == param2:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "identical_parameters",
                "message": (
                    f"param1 and param2 must be different. "
                    f"Both are '{param1}'. A parameter always correlates "
                    f"perfectly with itself (r=1.0)."
                ),
            },
        )

    try:
        result = await service.compute_correlation(
            db,
            city_id=city_id,
            param1=param1,
            param2=param2,
            days=days,
        )
        return CorrelationResponse(**result)

    except CityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"City '{city_id}' not found"},
        )


# ===========================================================================
# 5. GET /analytics/ranking — City Ranking by Parameter
# ===========================================================================


@router.get(
    "/ranking",
    response_model=RankingResponse,
    summary="Rank cities by average parameter value",
    description=(
        "Rank all monitored cities (or cities in a specific country) by "
        "their average value for a given parameter. "
        "Cities with fewer than 10 readings in the period are excluded "
        "to prevent noise from dominating the ranking. "
        "Includes data freshness (last reading timestamp) per city "
        "so consumers can identify cities with stale data."
    ),
    responses={
        200: {"model": RankingResponse},
    },
)
async def rank_cities(
    db: DbSession,
    parameter: str = Query(
        ...,
        description="Parameter to rank by (e.g. 'pm25').",
        examples=["pm25"],
    ),
    days: int = Query(
        7,
        ge=1,
        le=365,
        description="Number of recent days to include in the ranking.",
    ),
    country: Optional[str] = Query(
        None,
        min_length=2,
        max_length=2,
        description=(
            "ISO 3166-1 alpha-2 country code to filter by (e.g. 'GB'). "
            "If omitted, all countries are included."
        ),
        examples=["GB"],
    ),
    limit: int = Query(
        10,
        ge=1,
        le=100,
        description="Maximum number of cities to return.",
    ),
    service: AnalyticsService = Depends(get_analytics_service),
) -> RankingResponse:
    """
    Example: `GET /analytics/ranking?parameter=pm25&country=GB&days=7&limit=10`

    Returns the top 10 UK cities ranked by average PM2.5 over the last 7 days.
    """
    result = await service.rank_cities(
        db,
        parameter=parameter,
        days=days,
        country=country.upper() if country else None,
        limit=limit,
    )
    return RankingResponse(**result)

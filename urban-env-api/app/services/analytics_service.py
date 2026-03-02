"""
Analytics Service — Analytical Query Patterns
===============================================

This service implements the five analytical query patterns identified
during architectural design. Each method maps to a specific SQL pattern
that was stress-tested against the narrow readings table schema.

WHY text() SQL INSTEAD OF ORM CONSTRUCTS?

SQLAlchemy's ORM is excellent for CRUD, but analytical queries with
window functions, CTEs, corr(), and date_trunc() are more readable
and maintainable as raw SQL. The ORM would require deeply nested
func() calls that obscure the query logic — the opposite of what
we want for a project where the SQL IS the deliverable.

Using text() with bound parameters (:param) still provides:
  - SQL injection protection (parameterised queries).
  - Async execution through the AsyncSession.
  - Connection pooling via the engine.

Every method includes the raw SQL as a docstring comment so the
technical report can reference the exact queries.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import City
from app.services.city_service import CityService, CityNotFoundError

logger = logging.getLogger(__name__)

# Canonical units lookup (duplicated from ingestion for independence)
CANONICAL_UNITS = {
    "pm25": "µg/m³",
    "pm10": "µg/m³",
    "no2": "µg/m³",
    "o3": "µg/m³",
    "co": "µg/m³",
    "so2": "µg/m³",
    "temperature": "°C",
    "humidity": "%",
    "wind_speed": "m/s",
    "precipitation": "mm",
}


def _get_unit(parameter: str) -> str:
    return CANONICAL_UNITS.get(parameter, "unknown")


class AnalyticsService:
    """
    Analytical query engine for environmental time-series data.

    Each public method corresponds to one of the five query patterns
    validated during schema design. Methods take an AsyncSession and
    return structured dictionaries that the router converts to Pydantic
    response models.
    """

    def __init__(self):
        self._city_service = CityService()

    # ===================================================================
    # PATTERN 3: TIME-AGGREGATED TREND
    # ===================================================================

    async def get_trend(
        self,
        db: AsyncSession,
        city_id: str,
        parameter: str,
        days: int = 30,
        interval: str = "day",
    ) -> dict:
        """
        Compute time-aggregated trend for a single city and parameter.

        Raw SQL equivalent:
        -------------------
        SELECT date_trunc(:interval, timestamp) AS period,
               AVG(value)   AS avg,
               MIN(value)   AS min,
               MAX(value)   AS max,
               COUNT(*)     AS reading_count
        FROM readings
        WHERE city_id = :city_id
          AND parameter = :parameter
          AND quality_flag = 'valid'
          AND timestamp >= now() - interval ':days days'
        GROUP BY period
        ORDER BY period;

        Index usage:
          The composite index (city_id, parameter, timestamp) handles
          the WHERE clause efficiently. The GROUP BY on date_trunc()
          is a sequential scan within the index range — PostgreSQL
          reads the matching rows in timestamp order (already sorted
          by the index) and groups them. No sort step needed.
        """
        city = await self._city_service.get_city(db, city_id)

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        result = await db.execute(
            text("""
                SELECT date_trunc(:interval, timestamp) AS period,
                       ROUND(AVG(value)::numeric, 4)    AS avg,
                       ROUND(MIN(value)::numeric, 4)    AS min,
                       ROUND(MAX(value)::numeric, 4)    AS max,
                       COUNT(*)                         AS reading_count
                FROM readings
                WHERE city_id = :city_id
                  AND parameter = :parameter
                  AND quality_flag = 'valid'
                  AND timestamp >= :cutoff
                GROUP BY period
                ORDER BY period
            """),
            {
                "interval": interval,
                "city_id": city_id,
                "parameter": parameter,
                "cutoff": cutoff,
            },
        )

        rows = result.fetchall()

        data = [
            {
                "period": row.period,
                "avg": float(row.avg),
                "min": float(row.min),
                "max": float(row.max),
                "reading_count": row.reading_count,
            }
            for row in rows
        ]

        total_readings = sum(d["reading_count"] for d in data)

        return {
            "city_id": city.id,
            "city_name": city.name,
            "parameter": parameter,
            "unit": _get_unit(parameter),
            "interval": {v: k for k, v in {"hourly": "hour", "daily": "day", "weekly": "week"}.items()}.get(interval, interval),
            "days": days,
            "data": data,
            "total_readings": total_readings,
        }

    # ===================================================================
    # PATTERN 2: MULTI-CITY COMPARISON
    # ===================================================================

    async def compare_cities(
        self,
        db: AsyncSession,
        city_ids: list[str],
        parameter: str,
        days: int = 7,
    ) -> dict:
        """
        Compare a parameter across multiple cities with ranking and
        period-over-period percentage change.

        Raw SQL equivalent:
        -------------------
        WITH current_period AS (
            SELECT r.city_id,
                   c.name AS city_name,
                   ROUND(AVG(r.value)::numeric, 4) AS avg,
                   ROUND(MIN(r.value)::numeric, 4) AS min,
                   ROUND(MAX(r.value)::numeric, 4) AS max,
                   COUNT(*) AS reading_count
            FROM readings r
            JOIN cities c ON r.city_id = c.id
            WHERE r.city_id = ANY(:city_ids)
              AND r.parameter = :parameter
              AND r.quality_flag = 'valid'
              AND r.timestamp >= :current_cutoff
            GROUP BY r.city_id, c.name
        ),
        previous_period AS (
            SELECT r.city_id,
                   ROUND(AVG(r.value)::numeric, 4) AS avg
            FROM readings r
            WHERE r.city_id = ANY(:city_ids)
              AND r.parameter = :parameter
              AND r.quality_flag = 'valid'
              AND r.timestamp >= :prev_cutoff
              AND r.timestamp < :current_cutoff
            GROUP BY r.city_id
        )
        SELECT cp.*,
               RANK() OVER (ORDER BY cp.avg ASC) AS rank,
               CASE WHEN pp.avg > 0
                    THEN ROUND(((cp.avg - pp.avg) / pp.avg * 100)::numeric, 2)
                    ELSE NULL
               END AS pct_change_vs_prev_period
        FROM current_period cp
        LEFT JOIN previous_period pp ON cp.city_id = pp.city_id
        ORDER BY rank;

        Why LEFT JOIN for previous_period?
          A city might be newly added and have no data for the previous
          period. LEFT JOIN ensures it still appears in results with
          pct_change = NULL rather than being silently dropped.

        Why RANK() not ROW_NUMBER()?
          RANK() assigns the same rank to cities with identical averages
          (tie handling). ROW_NUMBER() would arbitrarily break ties.
        """
        now = datetime.now(timezone.utc)
        current_cutoff = now - timedelta(days=days)
        prev_cutoff = now - timedelta(days=days * 2)

        result = await db.execute(
            text("""
                WITH current_period AS (
                    SELECT r.city_id,
                           c.name                          AS city_name,
                           ROUND(AVG(r.value)::numeric, 4) AS avg,
                           ROUND(MIN(r.value)::numeric, 4) AS min,
                           ROUND(MAX(r.value)::numeric, 4) AS max,
                           COUNT(*)                        AS reading_count
                    FROM readings r
                    JOIN cities c ON r.city_id = c.id
                    WHERE r.city_id = ANY(:city_ids)
                      AND r.parameter = :parameter
                      AND r.quality_flag = 'valid'
                      AND r.timestamp >= :current_cutoff
                    GROUP BY r.city_id, c.name
                ),
                previous_period AS (
                    SELECT r.city_id,
                           ROUND(AVG(r.value)::numeric, 4) AS avg
                    FROM readings r
                    WHERE r.city_id = ANY(:city_ids)
                      AND r.parameter = :parameter
                      AND r.quality_flag = 'valid'
                      AND r.timestamp >= :prev_cutoff
                      AND r.timestamp < :current_cutoff
                    GROUP BY r.city_id
                )
                SELECT cp.city_id,
                       cp.city_name,
                       cp.avg,
                       cp.min,
                       cp.max,
                       cp.reading_count,
                       RANK() OVER (ORDER BY cp.avg ASC) AS rank,
                       CASE WHEN pp.avg IS NOT NULL AND pp.avg > 0
                            THEN ROUND(((cp.avg - pp.avg) / pp.avg * 100)::numeric, 2)
                            ELSE NULL
                       END AS pct_change_vs_prev_period
                FROM current_period cp
                LEFT JOIN previous_period pp ON cp.city_id = pp.city_id
                ORDER BY rank
            """),
            {
                "city_ids": city_ids,
                "parameter": parameter,
                "current_cutoff": current_cutoff,
                "prev_cutoff": prev_cutoff,
            },
        )

        rows = result.fetchall()

        cities = [
            {
                "city_id": row.city_id,
                "city_name": row.city_name,
                "avg": float(row.avg),
                "min": float(row.min),
                "max": float(row.max),
                "reading_count": row.reading_count,
                "rank": row.rank,
                "pct_change_vs_prev_period": (
                    float(row.pct_change_vs_prev_period)
                    if row.pct_change_vs_prev_period is not None
                    else None
                ),
            }
            for row in rows
        ]

        return {
            "parameter": parameter,
            "unit": _get_unit(parameter),
            "days": days,
            "cities": cities,
        }

    # ===================================================================
    # PATTERN 4: ANOMALY DETECTION
    # ===================================================================

    async def detect_anomalies(
        self,
        db: AsyncSession,
        city_id: str,
        parameter: str,
        sensitivity: float = 2.0,
        days: int = 30,
    ) -> dict:
        """
        Detect anomalies using z-score against a 7-day rolling baseline.

        Raw SQL equivalent:
        -------------------
        WITH rolling AS (
            SELECT timestamp,
                   value,
                   AVG(value) OVER w  AS rolling_avg,
                   STDDEV(value) OVER w AS rolling_stddev
            FROM readings
            WHERE city_id = :city_id
              AND parameter = :parameter
              AND quality_flag = 'valid'
              AND timestamp >= :cutoff
            WINDOW w AS (
                ORDER BY timestamp
                ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
            )
        )
        SELECT timestamp, value,
               ROUND(rolling_avg::numeric, 4) AS rolling_avg,
               ROUND(rolling_stddev::numeric, 4) AS rolling_stddev,
               ROUND(((value - rolling_avg)
                    / NULLIF(rolling_stddev, 0))::numeric, 4) AS z_score
        FROM rolling
        WHERE rolling_stddev > 0
          AND ABS((value - rolling_avg) / rolling_stddev) >= :sensitivity
        ORDER BY ABS((value - rolling_avg) / rolling_stddev) DESC;

        Window frame: ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
          168 rows = 7 days × 24 hours/day for hourly data.
          '1 PRECEDING' excludes the current row from its own baseline,
          preventing a reading from suppressing its own anomaly signal.

        NULLIF(rolling_stddev, 0):
          Prevents division by zero when all values in the window are
          identical (stddev = 0). This happens during sensor maintenance
          periods when the sensor reports a constant value.

        Index usage:
          The composite index (city_id, parameter, timestamp) provides
          the rows in timestamp order, which is exactly the ORDER BY
          in the window function. PostgreSQL can scan the index and
          compute the window without a separate sort step.
        """
        city = await self._city_service.get_city(db, city_id)

        # Extend cutoff to include 7 extra days for the rolling window warmup.
        # Without this, the first 7 days of readings would have incomplete
        # baselines (fewer than 168 preceding rows), producing unreliable
        # z-scores that might flag normal readings as anomalies.
        analysis_cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        warmup_cutoff = analysis_cutoff - timedelta(days=7)

        result = await db.execute(
            text("""
                WITH rolling AS (
                    SELECT timestamp,
                           value,
                           AVG(value) OVER w    AS rolling_avg,
                           STDDEV(value) OVER w AS rolling_stddev,
                           COUNT(*) OVER w      AS window_size
                    FROM readings
                    WHERE city_id = :city_id
                      AND parameter = :parameter
                      AND quality_flag = 'valid'
                      AND timestamp >= :warmup_cutoff
                    WINDOW w AS (
                        ORDER BY timestamp
                        ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
                    )
                )
                SELECT timestamp,
                       value,
                       ROUND(rolling_avg::numeric, 4)    AS rolling_avg,
                       ROUND(rolling_stddev::numeric, 4) AS rolling_stddev,
                       ROUND(((value - rolling_avg)
                            / NULLIF(rolling_stddev, 0))::numeric, 4) AS z_score
                FROM rolling
                WHERE timestamp >= :analysis_cutoff
                  AND window_size >= 24
                  AND rolling_stddev > 0
                  AND ABS((value - rolling_avg) / rolling_stddev) >= :sensitivity
                ORDER BY ABS((value - rolling_avg) / rolling_stddev) DESC
            """),
            {
                "city_id": city_id,
                "parameter": parameter,
                "warmup_cutoff": warmup_cutoff,
                "analysis_cutoff": analysis_cutoff,
                "sensitivity": sensitivity,
            },
        )

        rows = result.fetchall()

        anomalies = []
        for row in rows:
            z = float(row.z_score)
            abs_z = abs(z)

            if abs_z >= 3.5:
                severity = "high"
            elif abs_z >= 2.5:
                severity = "medium"
            else:
                severity = "low"

            anomalies.append(
                {
                    "timestamp": row.timestamp,
                    "value": float(row.value),
                    "rolling_avg": float(row.rolling_avg),
                    "rolling_stddev": float(row.rolling_stddev),
                    "z_score": z,
                    "severity": severity,
                }
            )

        # Count total readings in the analysis window for context
        count_result = await db.execute(
            text("""
                SELECT COUNT(*) AS total
                FROM readings
                WHERE city_id = :city_id
                  AND parameter = :parameter
                  AND quality_flag = 'valid'
                  AND timestamp >= :analysis_cutoff
            """),
            {"city_id": city_id, "parameter": parameter, "analysis_cutoff": analysis_cutoff},
        )
        total_readings = count_result.scalar_one()

        return {
            "city_id": city.id,
            "city_name": city.name,
            "parameter": parameter,
            "unit": _get_unit(parameter),
            "sensitivity": sensitivity,
            "anomalies": anomalies,
            "total_readings_analysed": total_readings,
        }

    # ===================================================================
    # PATTERN 5: CROSS-METRIC CORRELATION
    # ===================================================================

    async def compute_correlation(
        self,
        db: AsyncSession,
        city_id: str,
        param1: str,
        param2: str,
        days: int = 30,
    ) -> dict:
        """
        Compute Pearson correlation between two parameters via self-join.

        Raw SQL equivalent:
        -------------------
        SELECT ROUND(corr(a.value, b.value)::numeric, 6) AS pearson_r,
               COUNT(*) AS sample_size
        FROM readings a
        JOIN readings b
          ON a.city_id = b.city_id
         AND a.timestamp = b.timestamp
        WHERE a.parameter = :param1
          AND b.parameter = :param2
          AND a.city_id = :city_id
          AND a.quality_flag = 'valid'
          AND b.quality_flag = 'valid'
          AND a.timestamp >= :cutoff;

        Self-join mechanics:
          This joins the readings table to itself on (city_id, timestamp),
          pairing measurements that were taken at the same time.
          For a.parameter = 'pm25' and b.parameter = 'temperature',
          each result row contains a (pm25, temperature) pair.

        Index usage:
          Both sides of the join use the composite index
          (city_id, parameter, timestamp). PostgreSQL typically
          chooses a merge join strategy because both sides are
          already sorted by timestamp within the index.

        corr() returns NULL if:
          - Fewer than 2 data points exist.
          - All values in either column are identical (zero variance).
        """
        city = await self._city_service.get_city(db, city_id)

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        result = await db.execute(
            text("""
                SELECT ROUND(corr(a.value, b.value)::numeric, 6) AS pearson_r,
                       COUNT(*) AS sample_size
                FROM readings a
                JOIN readings b
                  ON a.city_id = b.city_id
                 AND a.timestamp = b.timestamp
                WHERE a.parameter = :param1
                  AND b.parameter = :param2
                  AND a.city_id = :city_id
                  AND a.quality_flag = 'valid'
                  AND b.quality_flag = 'valid'
                  AND a.timestamp >= :cutoff
            """),
            {
                "param1": param1,
                "param2": param2,
                "city_id": city_id,
                "cutoff": cutoff,
            },
        )

        row = result.fetchone()
        pearson_r = float(row.pearson_r) if row.pearson_r is not None else None
        sample_size = row.sample_size if row else 0

        interpretation, direction = _interpret_correlation(pearson_r)

        return {
            "city_id": city.id,
            "city_name": city.name,
            "param1": param1,
            "param2": param2,
            "days": days,
            "pearson_r": pearson_r,
            "sample_size": sample_size,
            "interpretation": interpretation,
            "direction": direction,
        }

    # ===================================================================
    # PATTERN 5b: CITY RANKING BY PARAMETER
    # ===================================================================

    async def rank_cities(
        self,
        db: AsyncSession,
        parameter: str,
        days: int = 7,
        country: Optional[str] = None,
        limit: int = 10,
    ) -> dict:
        """
        Rank cities by average value of a parameter.

        Raw SQL equivalent:
        -------------------
        SELECT c.id         AS city_id,
               c.name       AS city_name,
               ROUND(AVG(r.value)::numeric, 4) AS avg_value,
               RANK() OVER (ORDER BY AVG(r.value) ASC) AS rank,
               MAX(r.timestamp) AS data_freshness,
               COUNT(*)     AS reading_count
        FROM readings r
        JOIN cities c ON r.city_id = c.id
        WHERE r.parameter = :parameter
          AND r.quality_flag = 'valid'
          AND r.timestamp >= :cutoff
          AND c.is_active = true
          AND (:country IS NULL OR c.country_code = :country)
        GROUP BY c.id, c.name
        HAVING COUNT(*) >= 10
        ORDER BY rank
        LIMIT :limit;

        HAVING COUNT(*) >= 10:
          Excludes cities with too few readings to produce a meaningful
          average. A city with 2 readings could top the ranking due to
          noise rather than genuinely better air quality. 10 is a pragmatic
          minimum (roughly half a day of hourly data).

        Why RANK() ORDER BY ASC for pollutants?
          Lower pollutant values are better. Rank 1 = cleanest city.
          For temperature or wind_speed, the consumer may want different
          ordering — this is documented as a simplification.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        result = await db.execute(
            text("""
                SELECT c.id                               AS city_id,
                       c.name                             AS city_name,
                       ROUND(AVG(r.value)::numeric, 4)    AS avg_value,
                       RANK() OVER (ORDER BY AVG(r.value) ASC) AS rank,
                       MAX(r.timestamp)                   AS data_freshness,
                       COUNT(*)                           AS reading_count
                FROM readings r
                JOIN cities c ON r.city_id = c.id
                WHERE r.parameter = :parameter
                  AND r.quality_flag = 'valid'
                  AND r.timestamp >= :cutoff
                  AND c.is_active = true
                  AND (:country IS NULL OR c.country_code = :country)
                GROUP BY c.id, c.name
                HAVING COUNT(*) >= 10
                ORDER BY rank
                LIMIT :limit
            """),
            {
                "parameter": parameter,
                "cutoff": cutoff,
                "country": country,
                "limit": limit,
            },
        )

        rows = result.fetchall()

        rankings = [
            {
                "city_id": row.city_id,
                "city_name": row.city_name,
                "avg_value": float(row.avg_value),
                "rank": row.rank,
                "data_freshness": row.data_freshness,
                "reading_count": row.reading_count,
            }
            for row in rows
        ]

        return {
            "parameter": parameter,
            "unit": _get_unit(parameter),
            "country": country,
            "days": days,
            "rankings": rankings,
        }


# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================


def _interpret_correlation(r: Optional[float]) -> tuple[str, Optional[str]]:
    """
    Interpret a Pearson correlation coefficient into human-readable
    strength and direction labels.

    Returns (strength, direction) tuple.
    """
    if r is None:
        return "negligible", None

    abs_r = abs(r)

    if abs_r < 0.1:
        strength = "negligible"
    elif abs_r < 0.3:
        strength = "weak"
    elif abs_r < 0.7:
        strength = "moderate"
    elif abs_r < 0.9:
        strength = "strong"
    else:
        strength = "very_strong"

    if abs_r < 0.1:
        direction = None
    elif r > 0:
        direction = "positive"
    else:
        direction = "negative"

    return strength, direction

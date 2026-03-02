"""
Data Ingestion Service — Background Pipeline
==============================================

This is the most operationally critical module in the project. It runs as a
background task (via APScheduler) every 60 minutes, fetching environmental
data from two external APIs and persisting it to the readings table.

ARCHITECTURAL DECISIONS:

1. DECOUPLED INGESTION (not proxy-on-request)
   The API never makes synchronous calls to external APIs during a consumer
   request. Instead, this service runs on a schedule, populating the local
   database. Consumer requests always hit the local DB. This means:
     - API latency is determined by our DB, not by OpenAQ's response time.
     - If OpenAQ goes down, our API continues serving cached data.
     - We control our own rate limits independently of external APIs.

2. CITY-LEVEL ISOLATION
   If London's data fetch fails, Manchester's fetch still proceeds.
   Each city is ingested independently with its own error handling.
   The orchestrator logs per-city results and continues on failure.

3. EXPONENTIAL BACKOFF WITH JITTER
   External APIs rate-limit and occasionally fail. Naive retry (sleep 1s)
   causes thundering herd problems when multiple workers retry simultaneously.
   Exponential backoff (1s, 2s, 4s, 8s) with random jitter (±25%) spreads
   retries across time, reducing pressure on the external API.

4. BULK UPSERT (not individual INSERTs)
   Environmental data may overlap between ingestion cycles (e.g. OpenAQ
   returns the last 24 hours, and we ingest hourly). The upsert pattern
   (INSERT ... ON CONFLICT DO UPDATE) handles duplicates idempotently:
     - New readings are inserted.
     - Existing readings (same city/parameter/timestamp) are updated
       if the value has changed (e.g. a correction from the data provider).
     - No duplicate constraint violations.

5. RAW VALUE PRESERVATION
   Every reading stores both the normalised value (in canonical units)
   and the raw value + unit from the source API. This makes the pipeline
   fully auditable and reversible — if we discover a unit conversion bug
   six months later, we can reprocess from raw values without re-fetching.

DATA FLOW:
  APScheduler (every 60 min)
    → for each active city:
        ├── fetch_openaq_data(city) → list[RawReading]
        ├── fetch_open_meteo_data(city) → list[RawReading]
        ├── validate_readings(readings) → list[ValidatedReading]
        └── bulk_upsert(readings) → InsertionStats
    → log cycle summary
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import async_session_factory
from app.models.models import City

logger = logging.getLogger(__name__)


# ===========================================================================
# DATA STRUCTURES
# ===========================================================================


@dataclass
class RawReading:
    """
    Intermediate representation of a reading before DB insertion.

    This exists as a bridge between the external API response format
    and our ORM model. It's a plain dataclass (not an ORM object)
    because:
      - We may discard it during validation (no point creating an ORM
        object for data we'll throw away).
      - Batch operations are more efficient with dicts than ORM objects
        (we use raw SQL for the bulk upsert).
    """

    city_id: str
    parameter: str
    value: Optional[float]
    unit: str
    raw_value: Optional[float]
    raw_unit: str
    timestamp: datetime
    source: str
    quality_flag: str = "valid"


@dataclass
class IngestionStats:
    """Per-city ingestion statistics for structured logging."""

    city_id: str
    city_name: str
    readings_fetched: int = 0
    readings_inserted: int = 0
    readings_updated: int = 0
    readings_skipped: int = 0
    quality_valid: int = 0
    quality_suspect: int = 0
    quality_missing: int = 0
    errors: list[str] = field(default_factory=list)
    duration_ms: float = 0.0


@dataclass
class CycleReport:
    """Summary of a complete ingestion cycle across all cities."""

    started_at: datetime
    finished_at: Optional[datetime] = None
    cities_attempted: int = 0
    cities_succeeded: int = 0
    cities_failed: int = 0
    total_readings: int = 0
    city_stats: list[IngestionStats] = field(default_factory=list)


# ===========================================================================
# PARAMETER CONFIGURATION
# ===========================================================================

# Canonical units for normalisation.
# All values are converted to these units before storage.
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

# Physically plausible ranges for quality flag classification.
# Values outside these ranges are flagged as 'suspect'.
# Source: WHO guidelines, OpenAQ documentation, meteorological records.
PLAUSIBLE_RANGES: dict[str, tuple[float, float]] = {
    "pm25": (0.0, 500.0),
    "pm10": (0.0, 600.0),
    "no2": (0.0, 400.0),
    "o3": (0.0, 300.0),
    "co": (0.0, 50000.0),
    "so2": (0.0, 500.0),
    "temperature": (-30.0, 50.0),
    "humidity": (0.0, 100.0),
    "wind_speed": (0.0, 100.0),
    "precipitation": (0.0, 500.0),
}

# OpenAQ parameter name mapping.
# OpenAQ v3 uses these parameter names; we normalise to our canonical names.
OPENAQ_PARAM_MAP = {
    "pm25": "pm25",
    "pm10": "pm10",
    "no2": "no2",
    "o3": "o3",
    "co": "co",
    "so2": "so2",
}

# Open-Meteo variable → our parameter name mapping.
METEO_PARAM_MAP = {
    "temperature_2m": "temperature",
    "relative_humidity_2m": "humidity",
    "wind_speed_10m": "wind_speed",
    "precipitation": "precipitation",
}


# ===========================================================================
# HTTP CLIENT WITH RETRY LOGIC
# ===========================================================================


async def _request_with_backoff(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> Optional[dict]:
    """
    Make an HTTP request with exponential backoff and jitter.

    Why exponential backoff?
      If the external API is rate-limiting us (HTTP 429) or temporarily
      overloaded (HTTP 503), retrying immediately makes the problem worse.
      Exponential backoff (1s → 2s → 4s) gives the API time to recover.

    Why jitter?
      If 10 cities all hit the rate limit at the same time and all wait
      exactly 1s before retrying, they'll all retry simultaneously —
      causing another rate limit. Adding random jitter (±25%) spreads
      retries across time, breaking the synchronisation.

    Returns None on exhausted retries (the caller handles this gracefully
    by logging the failure and continuing with the next city).
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = await client.request(
                method,
                url,
                params=params,
                headers=headers,
            )

            # Rate limited: honour Retry-After if present, else backoff
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after and attempt < max_retries:
                    delay = float(retry_after)
                    logger.warning(
                        "Rate limited by %s, waiting %.1fs (Retry-After header)",
                        url,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue

            # Server error: retry with backoff
            if response.status_code >= 500 and attempt < max_retries:
                delay = _backoff_delay(attempt, base_delay)
                logger.warning(
                    "Server error %d from %s, retrying in %.1fs (attempt %d/%d)",
                    response.status_code,
                    url,
                    delay,
                    attempt + 1,
                    max_retries,
                )
                await asyncio.sleep(delay)
                continue

            # Any non-2xx response that isn't retryable
            response.raise_for_status()
            return response.json()

        except httpx.TimeoutException as e:
            last_exception = e
            if attempt < max_retries:
                delay = _backoff_delay(attempt, base_delay)
                logger.warning(
                    "Timeout fetching %s, retrying in %.1fs (attempt %d/%d)",
                    url,
                    delay,
                    attempt + 1,
                    max_retries,
                )
                await asyncio.sleep(delay)
            continue

        except httpx.HTTPStatusError as e:
            last_exception = e
            # Client errors (4xx except 429) are not retryable
            if 400 <= e.response.status_code < 500:
                logger.error(
                    "Client error %d from %s: %s",
                    e.response.status_code,
                    url,
                    e.response.text[:200],
                )
                return None
            # Server errors already handled above
            if attempt >= max_retries:
                break
            delay = _backoff_delay(attempt, base_delay)
            await asyncio.sleep(delay)

        except httpx.RequestError as e:
            last_exception = e
            if attempt < max_retries:
                delay = _backoff_delay(attempt, base_delay)
                logger.warning(
                    "Connection error to %s: %s, retrying in %.1fs",
                    url,
                    str(e),
                    delay,
                )
                await asyncio.sleep(delay)

    logger.error(
        "All %d retries exhausted for %s: %s",
        max_retries,
        url,
        str(last_exception),
    )
    return None


def _backoff_delay(attempt: int, base: float) -> float:
    """
    Calculate exponential backoff with jitter.

    attempt=0 → ~1.0s, attempt=1 → ~2.0s, attempt=2 → ~4.0s
    Jitter adds ±25% randomness to prevent thundering herd.
    """
    delay = base * (2**attempt)
    jitter = delay * 0.25 * (2 * random.random() - 1)  # ±25%
    return max(0.1, delay + jitter)


# ===========================================================================
# OPENAQ v3 DATA FETCHER
# ===========================================================================


async def fetch_openaq_data(
    client: httpx.AsyncClient,
    city: City,
) -> list[RawReading]:
    """
    Fetch air quality data from OpenAQ API v3 for a single city.

    OpenAQ v3 data model:
      Location → has many Sensors → each Sensor measures one Parameter
      We query by coordinates (lat/lng + radius) to find nearby stations,
      then fetch measurements for each relevant sensor.

    API flow:
      1. GET /v3/locations?coordinates={lat},{lng}&radius=25000&limit=5
         → Find monitoring stations near this city.
      2. For each location, extract sensor IDs for parameters we care about.
      3. GET /v3/sensors/{sensor_id}/measurements?date_from=...&date_to=...
         → Fetch hourly measurements for the last 24 hours.

    Why coordinates instead of city name?
      OpenAQ's city names are inconsistent across data providers.
      "London" might be listed as "London", "Greater London", or
      "City of London" depending on the source. Coordinates with a
      radius are deterministic.
    """
    readings: list[RawReading] = []

    # Build headers — OpenAQ v3 requires API key
    headers = {}
    if settings.OPENAQ_API_KEY:
        headers["X-API-Key"] = settings.OPENAQ_API_KEY

    # Step 1: Find monitoring locations near this city
    locations_data = await _request_with_backoff(
        client,
        "GET",
        "https://api.openaq.org/v3/locations",
        params={
            "coordinates": f"{city.latitude},{city.longitude}",
            "radius": 25000,  # 25km radius
            "limit": 5,  # Top 5 nearest stations
        },
        headers=headers,
    )

    if not locations_data or "results" not in locations_data:
        logger.warning(
            "No OpenAQ locations found near %s (%.4f, %.4f)",
            city.name,
            city.latitude,
            city.longitude,
        )
        return readings

    # Step 2: Extract sensors for parameters we care about
    sensors_to_fetch: list[dict] = []
    # Track which parameters we've already found to avoid duplicates
    # from multiple nearby stations measuring the same thing
    seen_params: set[str] = set()

    for location in locations_data["results"]:
        for sensor in location.get("sensors", []):
            param_info = sensor.get("parameter", {})
            param_name = param_info.get("name", "").lower()

            if param_name in OPENAQ_PARAM_MAP and param_name not in seen_params:
                sensors_to_fetch.append(
                    {
                        "sensor_id": sensor["id"],
                        "parameter": OPENAQ_PARAM_MAP[param_name],
                        "unit": param_info.get("units", "µg/m³"),
                        "location_name": location.get("name", "unknown"),
                    }
                )
                seen_params.add(param_name)

    if not sensors_to_fetch:
        logger.warning(
            "No relevant sensors found for %s across %d locations",
            city.name,
            len(locations_data["results"]),
        )
        return readings

    # Step 3: Fetch measurements for each sensor
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    date_to = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    for sensor_info in sensors_to_fetch:
        sensor_id = sensor_info["sensor_id"]

        measurements_data = await _request_with_backoff(
            client,
            "GET",
            f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements",
            params={
                "date_from": date_from,
                "date_to": date_to,
                "limit": 100,
            },
            headers=headers,
        )

        if not measurements_data or "results" not in measurements_data:
            logger.warning(
                "No measurements from sensor %d (%s) for %s",
                sensor_id,
                sensor_info["parameter"],
                city.name,
            )
            continue

        for measurement in measurements_data["results"]:
            # Extract value — OpenAQ v3 uses "value" at the top level
            raw_value = measurement.get("value")

            # Extract timestamp — OpenAQ v3 nests under "datetime"
            dt_obj = measurement.get("datetime", {})
            ts_str = dt_obj.get("utc") if isinstance(dt_obj, dict) else None

            # Some responses put the period's date range instead
            period = measurement.get("period", {})
            if not ts_str and isinstance(period, dict):
                dt_from = period.get("datetimeFrom", {})
                ts_str = (
                    dt_from.get("utc") if isinstance(dt_from, dict) else None
                )

            if ts_str is None:
                continue

            # Parse timestamp — handle multiple formats
            try:
                ts = _parse_timestamp(ts_str)
            except (ValueError, TypeError):
                logger.debug(
                    "Unparseable timestamp '%s' from sensor %d",
                    ts_str,
                    sensor_id,
                )
                continue

            readings.append(
                RawReading(
                    city_id=city.id,
                    parameter=sensor_info["parameter"],
                    value=float(raw_value) if raw_value is not None else None,
                    unit=CANONICAL_UNITS.get(
                        sensor_info["parameter"], sensor_info["unit"]
                    ),
                    raw_value=float(raw_value) if raw_value is not None else None,
                    raw_unit=sensor_info["unit"],
                    timestamp=ts,
                    source="openaq",
                )
            )

    logger.info(
        "OpenAQ: fetched %d readings for %s from %d sensors",
        len(readings),
        city.name,
        len(sensors_to_fetch),
    )
    return readings


# ===========================================================================
# OPEN-METEO DATA FETCHER
# ===========================================================================


async def fetch_open_meteo_data(
    client: httpx.AsyncClient,
    city: City,
) -> list[RawReading]:
    """
    Fetch weather data from Open-Meteo Archive API for a single city.

    Open-Meteo response format:
      {
        "hourly": {
          "time": ["2025-02-26T00:00", "2025-02-26T01:00", ...],
          "temperature_2m": [5.2, 4.8, ...],
          "relative_humidity_2m": [82, 85, ...],
          ...
        },
        "hourly_units": {
          "temperature_2m": "°C",
          "relative_humidity_2m": "%",
          ...
        }
      }

    The response uses parallel arrays — time[i] corresponds to
    temperature_2m[i], etc. We zip them together into RawReading objects.

    Why the archive API instead of the forecast API?
      The archive API serves historical data (including yesterday),
      while the forecast API serves predictions. For an environmental
      monitoring system, we want observed data, not forecasts.
      However, the archive API has a ~5 day delay for the most recent
      data. For the most recent 5 days, we fall back to the forecast
      API with past_days parameter.
    """
    readings: list[RawReading] = []

    now = datetime.now(timezone.utc)

    # Open-Meteo archive has a lag, so use the forecast API with
    # past_days for recent data (covers last 1-2 days reliably).
    # For older data, switch to the archive API.
    # For coursework simplicity, we use the forecast API with past_days=1
    # which returns yesterday's observed data plus today's forecast.
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    today = now.strftime("%Y-%m-%d")

    weather_data = await _request_with_backoff(
        client,
        "GET",
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": city.latitude,
            "longitude": city.longitude,
            "hourly": (
                "temperature_2m,"
                "relative_humidity_2m,"
                "wind_speed_10m,"
                "precipitation"
            ),
            "start_date": yesterday,
            "end_date": today,
            "timezone": "UTC",
        },
    )

    if not weather_data or "hourly" not in weather_data:
        logger.warning("No Open-Meteo data returned for %s", city.name)
        return readings

    hourly = weather_data["hourly"]
    hourly_units = weather_data.get("hourly_units", {})
    timestamps = hourly.get("time", [])

    if not timestamps:
        logger.warning("Open-Meteo returned empty time array for %s", city.name)
        return readings

    # Iterate over each weather variable we requested
    for meteo_var, our_param in METEO_PARAM_MAP.items():
        values = hourly.get(meteo_var, [])
        raw_unit = hourly_units.get(meteo_var, "unknown")

        for i, ts_str in enumerate(timestamps):
            if i >= len(values):
                break

            raw_val = values[i]

            # Parse timestamp — Open-Meteo uses "YYYY-MM-DDTHH:MM" (no seconds, no Z)
            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M").replace(
                    tzinfo=timezone.utc
                )
            except (ValueError, TypeError):
                continue

            # Only include data from the last 24 hours
            if ts < (now - timedelta(hours=25)):
                continue

            # Normalise units if needed
            normalised_value = _normalise_weather_value(
                our_param, raw_val, raw_unit
            )

            readings.append(
                RawReading(
                    city_id=city.id,
                    parameter=our_param,
                    value=normalised_value,
                    unit=CANONICAL_UNITS.get(our_param, raw_unit),
                    raw_value=float(raw_val) if raw_val is not None else None,
                    raw_unit=raw_unit,
                    timestamp=ts,
                    source="open-meteo",
                )
            )

    logger.info(
        "Open-Meteo: fetched %d readings for %s",
        len(readings),
        city.name,
    )
    return readings


def _normalise_weather_value(
    parameter: str,
    value: Optional[float],
    unit: str,
) -> Optional[float]:
    """
    Convert weather values to canonical units if necessary.

    Open-Meteo already returns temperature in °C, humidity in %,
    wind speed in km/h, and precipitation in mm. We convert wind
    speed from km/h to m/s for consistency.

    This function is the single place where unit conversion happens.
    If a new data source uses different units, add the conversion here.
    """
    if value is None:
        return None

    # Wind speed: Open-Meteo returns km/h, we store m/s
    if parameter == "wind_speed" and unit == "km/h":
        return round(value / 3.6, 2)

    return float(value)


# ===========================================================================
# DATA VALIDATION
# ===========================================================================


def validate_readings(readings: list[RawReading]) -> list[RawReading]:
    """
    Apply quality flags to a batch of readings.

    Quality classification:
      'missing':  value is None (API returned null).
      'suspect':  value is outside physically plausible range.
      'valid':    value is within plausible range.

    Why not reject suspect readings outright?
      Extreme but real events (bushfires, sandstorms, industrial accidents)
      can produce readings that look implausible. PM2.5 of 450 µg/m³ is
      "suspect" by our thresholds but was observed during the 2020
      Australian bushfires. By flagging rather than discarding, we let
      the API consumer decide whether to include suspect data.

    Z-score based outlier detection (vs 7-day rolling average) is
    deferred to query time (Pattern 4) rather than ingestion time,
    because it requires historical context that we may not have during
    the first ingestion cycle.
    """
    for reading in readings:
        if reading.value is None:
            reading.quality_flag = "missing"
            continue

        bounds = PLAUSIBLE_RANGES.get(reading.parameter)
        if bounds:
            min_val, max_val = bounds
            if reading.value < min_val or reading.value > max_val:
                reading.quality_flag = "suspect"
                logger.debug(
                    "Suspect reading: %s=%s for %s (bounds: %s-%s)",
                    reading.parameter,
                    reading.value,
                    reading.city_id,
                    min_val,
                    max_val,
                )
            else:
                reading.quality_flag = "valid"
        else:
            # Unknown parameter — accept but log
            reading.quality_flag = "valid"

    return readings


# ===========================================================================
# BULK UPSERT
# ===========================================================================


async def bulk_upsert_readings(
    db: AsyncSession,
    readings: list[RawReading],
) -> tuple[int, int]:
    """
    Bulk insert readings with ON CONFLICT upsert.

    Uses raw SQL for performance. SQLAlchemy's ORM merge() would work
    but issues individual SELECT + INSERT/UPDATE per row. For 100+
    readings per city, raw SQL with executemany is dramatically faster.

    The ON CONFLICT clause targets the unique constraint on
    (city_id, parameter, timestamp). When a duplicate is detected:
      - If the value has changed, UPDATE it (data correction from provider).
      - If the value is the same, the UPDATE is a no-op (idempotent).

    Returns (inserted_count, updated_count). PostgreSQL doesn't natively
    distinguish inserts from updates in ON CONFLICT, so we approximate:
    total affected = inserts + updates.
    """
    if not readings:
        return 0, 0

    upsert_sql = text("""
        INSERT INTO readings (
            city_id, parameter, value, unit,
            raw_value, raw_unit, timestamp,
            source, quality_flag
        ) VALUES (
            :city_id, :parameter, :value, :unit,
            :raw_value, :raw_unit, :timestamp,
            :source, :quality_flag
        )
        ON CONFLICT (city_id, parameter, timestamp)
        DO UPDATE SET
            value = EXCLUDED.value,
            unit = EXCLUDED.unit,
            raw_value = EXCLUDED.raw_value,
            raw_unit = EXCLUDED.raw_unit,
            source = EXCLUDED.source,
            quality_flag = EXCLUDED.quality_flag,
            updated_at = now()
        WHERE readings.value IS DISTINCT FROM EXCLUDED.value
           OR readings.quality_flag IS DISTINCT FROM EXCLUDED.quality_flag
    """)

    # Convert RawReading dataclasses to dicts for executemany
    params = [
        {
            "city_id": r.city_id,
            "parameter": r.parameter,
            "value": r.value,
            "unit": r.unit,
            "raw_value": r.raw_value,
            "raw_unit": r.raw_unit,
            "timestamp": r.timestamp,
            "source": r.source,
            "quality_flag": r.quality_flag,
        }
        for r in readings
    ]

    # Execute in batches to avoid overwhelming the connection
    batch_size = 500
    total_affected = 0

    for i in range(0, len(params), batch_size):
        batch = params[i : i + batch_size]
        result = await db.execute(upsert_sql, batch)
        total_affected += result.rowcount

    await db.commit()

    # Approximate: rows affected = inserts + updates.
    # Rows that matched but had identical values are NOT counted
    # (due to the WHERE ... IS DISTINCT FROM clause).
    inserted = total_affected  # Close enough for logging
    updated = 0  # We can't distinguish without xmax tricks

    return inserted, updated


# ===========================================================================
# TIMESTAMP PARSING UTILITY
# ===========================================================================


def _parse_timestamp(ts_str: str) -> datetime:
    """
    Parse timestamps from various external API formats into
    timezone-aware UTC datetimes.

    OpenAQ v3 uses ISO 8601 with various flavours:
      "2025-02-27T10:00:00Z"
      "2025-02-27T10:00:00+00:00"
      "2025-02-27T10:00:00"

    Open-Meteo uses:
      "2025-02-27T10:00"
    """
    # Strip trailing Z and replace with +00:00 for fromisoformat
    cleaned = ts_str.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        # Fallback: try without timezone info
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
            try:
                dt = datetime.strptime(cleaned, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Cannot parse timestamp: {ts_str}")

    # Ensure timezone-aware (UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


# ===========================================================================
# CITY INGESTION ORCHESTRATOR
# ===========================================================================


async def ingest_city(
    client: httpx.AsyncClient,
    city: City,
) -> IngestionStats:
    """
    Run the full ingestion pipeline for a single city.

    This is the unit of work: one city, one database session,
    one commit. If this fails, only this city's data is lost
    for this cycle — other cities are unaffected.
    """
    stats = IngestionStats(city_id=city.id, city_name=city.name)
    start_time = time.monotonic()

    try:
        # Fetch from both sources concurrently
        # asyncio.gather runs both fetchers in parallel, cutting
        # wall-clock time roughly in half vs sequential.
        openaq_readings, meteo_readings = await asyncio.gather(
            fetch_openaq_data(client, city),
            fetch_open_meteo_data(client, city),
            return_exceptions=True,
        )

        # Handle exceptions from gather (return_exceptions=True
        # returns exceptions as values instead of raising them)
        all_readings: list[RawReading] = []

        if isinstance(openaq_readings, Exception):
            stats.errors.append(f"OpenAQ fetch failed: {openaq_readings}")
            logger.error(
                "OpenAQ fetch failed for %s: %s", city.name, openaq_readings
            )
        else:
            all_readings.extend(openaq_readings)

        if isinstance(meteo_readings, Exception):
            stats.errors.append(f"Open-Meteo fetch failed: {meteo_readings}")
            logger.error(
                "Open-Meteo fetch failed for %s: %s", city.name, meteo_readings
            )
        else:
            all_readings.extend(meteo_readings)

        stats.readings_fetched = len(all_readings)

        if not all_readings:
            logger.warning("No readings fetched for %s", city.name)
            return stats

        # Validate and assign quality flags
        validated = validate_readings(all_readings)

        # Count quality distribution
        for r in validated:
            if r.quality_flag == "valid":
                stats.quality_valid += 1
            elif r.quality_flag == "suspect":
                stats.quality_suspect += 1
            elif r.quality_flag == "missing":
                stats.quality_missing += 1

        # Filter out readings with no value AND no useful information
        # (keep 'missing' flags as they represent data gaps — these are
        # informational rows with value=NULL)
        insertable = [
            r for r in validated if r.value is not None or r.quality_flag == "missing"
        ]
        stats.readings_skipped = len(validated) - len(insertable)

        # Bulk upsert to database
        async with async_session_factory() as db:
            inserted, updated = await bulk_upsert_readings(db, insertable)
            stats.readings_inserted = inserted
            stats.readings_updated = updated

    except Exception as e:
        stats.errors.append(f"Unexpected error: {str(e)}")
        logger.exception("Ingestion failed for %s", city.name)

    stats.duration_ms = round((time.monotonic() - start_time) * 1000, 1)
    return stats


# ===========================================================================
# FULL INGESTION CYCLE
# ===========================================================================


async def run_ingestion_cycle() -> CycleReport:
    """
    Execute a complete ingestion cycle across all active cities.

    This is the top-level function called by APScheduler every
    INGESTION_INTERVAL_MINUTES minutes. It:
      1. Loads all active cities from the database.
      2. Ingests each city independently (failure-isolated).
      3. Logs a structured summary of the cycle.

    City ingestion is sequential (not parallel) to avoid
    overwhelming external APIs. OpenAQ has rate limits that
    would be hit if we fetched 10 cities simultaneously.
    For a production system with higher limits, you'd use
    asyncio.Semaphore to control concurrency.
    """
    report = CycleReport(started_at=datetime.now(timezone.utc))

    logger.info("=" * 60)
    logger.info("INGESTION CYCLE STARTED at %s", report.started_at.isoformat())
    logger.info("=" * 60)

    # Load active cities
    async with async_session_factory() as db:
        from sqlalchemy import select

        result = await db.execute(
            select(City).where(City.is_active == True).order_by(City.name)  # noqa: E712
        )
        cities = result.scalars().all()

    if not cities:
        logger.warning("No active cities found — skipping ingestion cycle")
        report.finished_at = datetime.now(timezone.utc)
        return report

    report.cities_attempted = len(cities)
    logger.info("Found %d active cities to ingest", len(cities))

    # Create a shared HTTP client with sensible timeouts.
    # The client is shared across all city fetches to reuse
    # TCP connections (connection pooling).
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(
            connect=10.0,  # TCP connect timeout
            read=30.0,  # Read timeout (some APIs are slow)
            write=10.0,  # Write timeout
            pool=10.0,  # Wait for connection from pool
        ),
        follow_redirects=True,
        # Limit concurrent connections to avoid overwhelming APIs
        limits=httpx.Limits(
            max_connections=10,
            max_keepalive_connections=5,
        ),
    ) as client:
        for city in cities:
            logger.info("--- Ingesting: %s (%s) ---", city.name, city.id)

            stats = await ingest_city(client, city)
            report.city_stats.append(stats)

            if stats.errors:
                report.cities_failed += 1
                for error in stats.errors:
                    logger.error(
                        "  [%s] ERROR: %s", city.name, error
                    )
            else:
                report.cities_succeeded += 1

            report.total_readings += stats.readings_inserted

            # Structured log line for each city — easy to parse with log aggregators
            logger.info(
                "  [%s] fetched=%d inserted=%d skipped=%d "
                "valid=%d suspect=%d missing=%d duration=%.0fms errors=%d",
                city.name,
                stats.readings_fetched,
                stats.readings_inserted,
                stats.readings_skipped,
                stats.quality_valid,
                stats.quality_suspect,
                stats.quality_missing,
                stats.duration_ms,
                len(stats.errors),
            )

            # Brief delay between cities to be respectful of rate limits
            await asyncio.sleep(0.5)

    report.finished_at = datetime.now(timezone.utc)
    duration = (report.finished_at - report.started_at).total_seconds()

    logger.info("=" * 60)
    logger.info(
        "INGESTION CYCLE COMPLETE: %d/%d cities succeeded, "
        "%d total readings, %.1fs total duration",
        report.cities_succeeded,
        report.cities_attempted,
        report.total_readings,
        duration,
    )
    logger.info("=" * 60)

    return report


# ===========================================================================
# SCHEDULER SETUP
# ===========================================================================


def setup_scheduler():
    """
    Configure and return an APScheduler instance for periodic ingestion.

    APScheduler is used over FastAPI's BackgroundTasks because:
      - BackgroundTasks are request-scoped (triggered by an HTTP request).
        Our ingestion needs to run on a schedule regardless of traffic.
      - APScheduler supports cron-like scheduling, misfire handling,
        and job persistence (if backed by a job store).
      - It runs in the same process as FastAPI (no separate worker needed),
        keeping the deployment simple for coursework.

    The scheduler should be started in the lifespan context manager
    (main.py) and shut down on app exit.

    Usage in main.py:
        from app.services.ingestion_service import setup_scheduler

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            scheduler = setup_scheduler()
            scheduler.start()
            app.state.scheduler = scheduler
            yield
            scheduler.shutdown(wait=True)
    """
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        run_ingestion_cycle,
        trigger=IntervalTrigger(minutes=settings.INGESTION_INTERVAL_MINUTES),
        id="data_ingestion",
        name="Environmental Data Ingestion",
        # replace_existing prevents duplicate jobs if the app restarts
        # and the job ID already exists in a persistent job store.
        replace_existing=True,
        # max_instances=1 prevents overlapping cycles. If a cycle takes
        # longer than the interval (unlikely but possible with API
        # slowdowns), the next cycle is skipped rather than running
        # concurrently. This prevents double-ingestion race conditions.
        max_instances=1,
        # misfire_grace_time: if the scheduler missed a scheduled run
        # (e.g. the app was down), run it immediately when the app
        # comes back up, but only if less than 5 minutes have passed.
        # After 5 minutes, skip the missed run and wait for the next one.
        misfire_grace_time=300,
    )

    logger.info(
        "Scheduler configured: ingestion every %d minutes",
        settings.INGESTION_INTERVAL_MINUTES,
    )

    return scheduler

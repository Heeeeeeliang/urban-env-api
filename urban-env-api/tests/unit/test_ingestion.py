"""
Unit Tests — Ingestion Service Pure Functions
================================================

Tests for validate_readings (batch mode), _parse_timestamp, and
_backoff_delay from the ingestion service module. These are pure
function tests: no database, no HTTP, no async fixtures.

These complement the existing test_validation.py by testing:
  - Batch classification distribution (valid/suspect/missing counts)
  - Timestamp parsing across all API formats encountered in the wild
  - Exponential backoff with jitter bounds (deterministic bounds check)

WHY SEPARATE FROM test_validation.py?
  test_validation.py covers the three-path quality flag logic with
  parametrised per-value tests. This file tests:
  - Batch-level statistics (how many of each flag in a mixed batch)
  - Timestamp parsing (different module, different concerns)
  - Retry logic (backoff delay calculation)
  Keeping them separate follows the convention of one file per concern.
"""

import random
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.ingestion_service import (
    RawReading,
    validate_readings,
    _parse_timestamp,
    _backoff_delay,
)

pytestmark = pytest.mark.unit


# -------------------------------------------------------------------
# HELPER: build a RawReading with controlled values
# -------------------------------------------------------------------

def _reading(
    parameter: str = "pm25",
    value: float | None = 12.5,
    city_id: str = "london",
) -> RawReading:
    """Create a RawReading with minimal boilerplate for batch tests."""
    return RawReading(
        city_id=city_id,
        parameter=parameter,
        value=value,
        unit="µg/m³",
        raw_value=value,
        raw_unit="µg/m³",
        timestamp=datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc),
        source="test",
    )


# ===================================================================
# validate_readings — batch distribution
# ===================================================================


class TestValidateReadingsBatch:
    """
    Test batch-level behaviour of validate_readings: given a mixed input
    of valid/suspect/missing values, verify the distribution of quality
    flags matches expectations.

    This supplements test_validation.py's per-value tests with a batch-
    level view that's closer to how ingest_city actually calls the function.
    """

    def test_mixed_batch_counts(self):
        """
        A batch with 3 valid, 2 suspect, 1 missing readings should
        produce exactly those flag counts after validation.

        This is the batch equivalent of seed_data's spike: we know
        the input distribution and verify the output matches.
        """
        readings = [
            # Valid: within PM2.5 range (0-500)
            _reading(value=10.0),
            _reading(value=25.0),
            _reading(value=100.0),
            # Suspect: outside PM2.5 range
            _reading(value=-5.0),
            _reading(value=600.0),
            # Missing: None value
            _reading(value=None),
        ]

        result = validate_readings(readings)

        counts = {"valid": 0, "suspect": 0, "missing": 0}
        for r in result:
            counts[r.quality_flag] += 1

        assert counts["valid"] == 3
        assert counts["suspect"] == 2
        assert counts["missing"] == 1

    def test_all_valid_batch(self):
        """A batch of entirely valid readings produces no suspect/missing flags."""
        readings = [_reading(value=v) for v in [10.0, 20.0, 30.0, 40.0]]
        validate_readings(readings)
        assert all(r.quality_flag == "valid" for r in readings)

    def test_multi_parameter_batch(self):
        """
        validate_readings handles mixed parameters in the same batch.

        In production, a single ingest_city call produces readings for
        pm25, temperature, humidity, etc. The validation function must
        apply the correct range for each parameter, not just pm25.
        """
        readings = [
            _reading(parameter="pm25", value=12.0),        # valid (0-500)
            _reading(parameter="temperature", value=-35.0), # suspect (range: -30 to 50)
            _reading(parameter="humidity", value=50.0),     # valid (0-100)
            _reading(parameter="wind_speed", value=150.0),  # suspect (range: 0-100)
        ]

        validate_readings(readings)

        assert readings[0].quality_flag == "valid"
        assert readings[1].quality_flag == "suspect"
        assert readings[2].quality_flag == "valid"
        assert readings[3].quality_flag == "suspect"


# ===================================================================
# _parse_timestamp — format coverage
# ===================================================================


class TestParseTimestamp:
    """
    Test _parse_timestamp with every format encountered from external APIs.

    OpenAQ v3 returns ISO 8601 in several flavours depending on the data
    provider. Open-Meteo uses truncated ISO without timezone. Our parser
    must handle all of them and always return a timezone-aware UTC datetime.
    """

    def test_iso_with_z_suffix(self):
        """
        "2026-02-27T10:00:00Z" — OpenAQ's most common format.
        The trailing Z means UTC. Python's fromisoformat() doesn't handle
        Z natively before 3.11, so our parser replaces it with +00:00.
        """
        result = _parse_timestamp("2026-02-27T10:00:00Z")
        assert result == datetime(2026, 2, 27, 10, 0, 0, tzinfo=timezone.utc)
        assert result.tzinfo is not None

    def test_iso_with_utc_offset(self):
        """
        "2026-02-27T10:00:00+00:00" — explicit UTC offset.
        Equivalent to Z but more verbose. Some OpenAQ providers use this.
        """
        result = _parse_timestamp("2026-02-27T10:00:00+00:00")
        assert result == datetime(2026, 2, 27, 10, 0, 0, tzinfo=timezone.utc)

    def test_iso_without_timezone(self):
        """
        "2026-02-27T10:00:00" — no timezone info.
        Some OpenAQ providers omit the timezone. Our parser assumes UTC
        (because all our stored timestamps are in UTC).
        """
        result = _parse_timestamp("2026-02-27T10:00:00")
        assert result == datetime(2026, 2, 27, 10, 0, 0, tzinfo=timezone.utc)
        # Must be timezone-aware, not naive
        assert result.tzinfo is not None

    def test_truncated_iso_open_meteo(self):
        """
        "2026-02-27T10:00" — Open-Meteo's format (no seconds).
        Open-Meteo omits seconds because hourly data doesn't need them.
        """
        result = _parse_timestamp("2026-02-27T10:00")
        assert result.year == 2026
        assert result.month == 2
        assert result.day == 27
        assert result.hour == 10
        assert result.minute == 0
        assert result.tzinfo is not None

    def test_invalid_string_raises_valueerror(self):
        """
        Garbage input raises ValueError, not a silent None or wrong date.

        The caller (fetch_openaq_data) catches ValueError and logs it
        as an unparseable timestamp. Silently returning None would cause
        a downstream NoneType error that's harder to debug.
        """
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            _parse_timestamp("not-a-date")

    def test_empty_string_raises_valueerror(self):
        """Empty string should fail loudly, not return epoch or None."""
        with pytest.raises((ValueError, TypeError)):
            _parse_timestamp("")

    def test_whitespace_is_stripped(self):
        """
        Leading/trailing whitespace in API responses should not cause
        parsing failures. Some APIs return " 2026-02-27T10:00:00Z "
        due to serialisation bugs.
        """
        result = _parse_timestamp("  2026-02-27T10:00:00Z  ")
        assert result == datetime(2026, 2, 27, 10, 0, 0, tzinfo=timezone.utc)


# ===================================================================
# _backoff_delay — exponential growth with jitter
# ===================================================================


class TestBackoffDelay:
    """
    Test _backoff_delay(attempt, base) for correct exponential growth
    and bounded jitter.

    The function implements: delay = base * 2^attempt ± 25% jitter.
    We verify:
      - The expected value doubles with each attempt.
      - The actual value stays within the ±25% jitter envelope.
      - The minimum is at least 0.1s (hard floor in the implementation).

    WHY NOT TEST EXACT VALUES?
      _backoff_delay uses random.random() for jitter, making exact
      values non-deterministic. Instead we verify the bounds:
        expected = base * 2^attempt
        lower = expected * 0.75
        upper = expected * 1.25
    """

    def test_attempt_zero_base_delay(self):
        """
        attempt=0, base=1.0 → expected delay ~1.0s (±25%).
        This is the first retry delay after an initial failure.
        """
        # Run multiple times to catch jitter range
        for _ in range(20):
            delay = _backoff_delay(0, 1.0)
            assert 0.75 <= delay <= 1.25, f"Delay {delay} out of bounds for attempt 0"

    def test_attempt_one_doubles(self):
        """
        attempt=1, base=1.0 → expected delay ~2.0s (±25%).
        Exponential doubling: 2^1 = 2.
        """
        for _ in range(20):
            delay = _backoff_delay(1, 1.0)
            assert 1.5 <= delay <= 2.5, f"Delay {delay} out of bounds for attempt 1"

    def test_attempt_two_quadruples(self):
        """
        attempt=2, base=1.0 → expected delay ~4.0s (±25%).
        Exponential doubling: 2^2 = 4.
        """
        for _ in range(20):
            delay = _backoff_delay(2, 1.0)
            assert 3.0 <= delay <= 5.0, f"Delay {delay} out of bounds for attempt 2"

    def test_custom_base_delay(self):
        """
        Custom base=0.5 → attempt 0 should be ~0.5s (±25%).
        Verifies that the base parameter is actually used (not hardcoded to 1.0).
        """
        for _ in range(20):
            delay = _backoff_delay(0, 0.5)
            assert 0.375 <= delay <= 0.625, f"Delay {delay} out of bounds for base=0.5"

    def test_minimum_floor(self):
        """
        The implementation has a max(0.1, ...) floor to prevent zero
        or negative delays from extreme jitter at very small base values.
        """
        for _ in range(50):
            delay = _backoff_delay(0, 0.01)
            assert delay >= 0.1, f"Delay {delay} below minimum floor of 0.1"

    def test_jitter_varies(self):
        """
        Jitter must actually vary — if all 20 calls return the exact same
        value, the jitter is broken (random.random() not being called).

        This catches a regression where someone replaces random.random()
        with a constant or removes the jitter entirely.
        """
        delays = [_backoff_delay(1, 1.0) for _ in range(20)]
        unique_values = set(round(d, 6) for d in delays)
        assert len(unique_values) > 1, (
            "All backoff delays were identical — jitter is not working"
        )


# ===================================================================
# _request_with_backoff — mock httpx error handling
# ===================================================================


class TestRequestWithBackoffRetries:
    """
    Test the retry logic in _request_with_backoff by mocking httpx responses.

    We mock asyncio.sleep to avoid actual delays in tests, and mock
    the httpx.AsyncClient.request method to simulate various failure
    modes from external APIs.

    NOTE: _request_with_backoff is an async function, so these tests
    use @pytest.mark.asyncio.
    """

    @pytest.mark.asyncio
    @patch("app.services.ingestion_service.asyncio.sleep", new_callable=AsyncMock)
    async def test_429_retries_then_succeeds(self, mock_sleep):
        """
        HTTP 429 (rate limited) should trigger a retry. On the second
        attempt, the API returns 200 — the function should return the
        parsed JSON without raising.

        Real-world scenario: OpenAQ rate limits us on the first request,
        but the retry after Retry-After succeeds.
        """
        import httpx
        from app.services.ingestion_service import _request_with_backoff

        # First call: 429 with Retry-After header
        response_429 = MagicMock()
        response_429.status_code = 429
        response_429.headers = {"Retry-After": "1"}

        # Second call: 200 success
        response_200 = MagicMock()
        response_200.status_code = 200
        response_200.json.return_value = {"results": []}
        response_200.raise_for_status = MagicMock()

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(side_effect=[response_429, response_200])

        result = await _request_with_backoff(
            mock_client, "GET", "https://api.openaq.org/v3/locations",
            max_retries=3, base_delay=0.01,
        )

        assert result == {"results": []}
        assert mock_client.request.call_count == 2

    @pytest.mark.asyncio
    @patch("app.services.ingestion_service.asyncio.sleep", new_callable=AsyncMock)
    async def test_500_retries_then_gives_up(self, mock_sleep):
        """
        Persistent HTTP 500 (server error) should exhaust all retries
        and return None. The function must not raise — the caller
        (ingest_city) handles None by logging the failure and continuing.

        Real-world scenario: OpenAQ is experiencing an outage. We retry
        3 times with backoff, then gracefully give up for this city.
        """
        import httpx
        from app.services.ingestion_service import _request_with_backoff

        response_500 = MagicMock()
        response_500.status_code = 500
        response_500.text = "Internal Server Error"
        response_500.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "500 Server Error",
                request=MagicMock(),
                response=response_500,
            )
        )

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(return_value=response_500)

        result = await _request_with_backoff(
            mock_client, "GET", "https://api.openaq.org/v3/locations",
            max_retries=2, base_delay=0.01,
        )

        assert result is None
        # Initial attempt + 2 retries = 3 calls
        assert mock_client.request.call_count == 3

    @pytest.mark.asyncio
    @patch("app.services.ingestion_service.asyncio.sleep", new_callable=AsyncMock)
    async def test_timeout_retries_with_backoff(self, mock_sleep):
        """
        httpx.TimeoutException should trigger retries with exponential
        backoff. After exhausting retries, return None.

        Real-world scenario: the API server is slow under load. Our
        10-second read timeout fires, and we retry with increasing
        delays to give the server time to recover.
        """
        import httpx
        from app.services.ingestion_service import _request_with_backoff

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(
            side_effect=httpx.TimeoutException("Read timed out")
        )

        result = await _request_with_backoff(
            mock_client, "GET", "https://api.openaq.org/v3/locations",
            max_retries=2, base_delay=0.01,
        )

        assert result is None
        # Initial attempt + 2 retries = 3 calls
        assert mock_client.request.call_count == 3
        # sleep should have been called for the retries (not the initial attempt)
        assert mock_sleep.call_count == 2

    @pytest.mark.asyncio
    @patch("app.services.ingestion_service.asyncio.sleep", new_callable=AsyncMock)
    async def test_client_error_not_retried(self, mock_sleep):
        """
        HTTP 4xx errors (except 429) are client errors that won't be
        fixed by retrying. The function should return None immediately
        without exhausting retries.

        Real-world scenario: our API key is invalid (401) or the
        endpoint doesn't exist (404). Retrying wastes time.
        """
        import httpx
        from app.services.ingestion_service import _request_with_backoff

        response_401 = MagicMock()
        response_401.status_code = 401
        response_401.text = "Unauthorized"
        response_401.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "401 Unauthorized",
                request=MagicMock(),
                response=response_401,
            )
        )

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request = AsyncMock(return_value=response_401)

        result = await _request_with_backoff(
            mock_client, "GET", "https://api.openaq.org/v3/locations",
            max_retries=3, base_delay=0.01,
        )

        assert result is None
        # Should NOT retry — only 1 call
        assert mock_client.request.call_count == 1

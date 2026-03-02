"""
Unit Tests — validate_readings quality flag assignment
========================================================

These are pure function tests: no database, no HTTP, no async fixtures.
Each test creates RawReading dataclass instances (cheap, in-memory) and
passes them through validate_readings(), then asserts the quality_flag
field was set correctly.

The test cases cover the three return paths in the validation logic,
plus boundary values and unknown-parameter edge cases:

  value is None          → 'missing'
  value outside range    → 'suspect'
  value inside range     → 'valid'
  unknown parameter      → 'valid' (no range to violate)
  boundary values        → 'valid' (inclusive bounds: lo <= value <= hi)

WHY PARAMETRIZE?

  pytest.mark.parametrize generates one test item per (parameter, value)
  tuple, so a failure message reads:

    FAILED test_validation.py::test_quality_flag[pm25-negative]
           - AssertionError: assert 'valid' == 'suspect'

  instead of a generic "test_quality_flag failed" with no indication
  of which case broke. This matters when a refactor accidentally
  changes a boundary condition — the test name tells you exactly
  which parameter/value combination regressed.
"""

from datetime import datetime, timezone

import pytest

from app.services.ingestion_service import RawReading, validate_readings


# -------------------------------------------------------------------
# HELPER: build a RawReading with minimal boilerplate
# -------------------------------------------------------------------
# The function under test only inspects .parameter, .value, and writes
# .quality_flag. All other fields are mandatory for the dataclass but
# irrelevant to validation, so we fix them here.
# -------------------------------------------------------------------


def _make_reading(parameter: str, value: float | None) -> RawReading:
    """Create a RawReading with only the fields that matter for validation."""
    return RawReading(
        city_id="london",
        parameter=parameter,
        value=value,
        unit="µg/m³",
        raw_value=value,
        raw_unit="µg/m³",
        timestamp=datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc),
        source="test",
    )


def _flag(parameter: str, value: float | None) -> str:
    """Shorthand: validate a single reading and return its quality_flag."""
    reading = _make_reading(parameter, value)
    validate_readings([reading])
    return reading.quality_flag


# -------------------------------------------------------------------
# Core three-path coverage: missing, suspect, valid
# -------------------------------------------------------------------


class TestMissingPath:
    """value=None should always return 'missing', regardless of parameter."""

    def test_pm25_none(self):
        assert _flag("pm25", None) == "missing"

    def test_temperature_none(self):
        assert _flag("temperature", None) == "missing"

    def test_unknown_param_none(self):
        """Even parameters without defined ranges get 'missing' for None."""
        assert _flag("cosmic_rays", None) == "missing"


class TestSuspectPath:
    """Values outside the plausible range should return 'suspect'."""

    def test_pm25_negative(self):
        assert _flag("pm25", -5) == "suspect"

    def test_pm25_exceeds_max(self):
        assert _flag("pm25", 501) == "suspect"

    def test_temperature_above_range(self):
        assert _flag("temperature", 60) == "suspect"

    def test_temperature_below_range(self):
        assert _flag("temperature", -31) == "suspect"

    def test_no2_negative(self):
        assert _flag("no2", -0.1) == "suspect"

    def test_pm10_exceeds_max(self):
        assert _flag("pm10", 601) == "suspect"


class TestValidPath:
    """Values within range, and unknown parameters, should return 'valid'."""

    def test_pm25_typical(self):
        assert _flag("pm25", 12) == "valid"

    def test_temperature_typical(self):
        assert _flag("temperature", 22.5) == "valid"

    def test_unknown_parameter_any_value(self):
        """Parameters not in PLAUSIBLE_RANGES have no bounds to violate."""
        assert _flag("cosmic_rays", 999.9) == "valid"

    def test_unknown_parameter_negative_value(self):
        assert _flag("cosmic_rays", -50) == "valid"


# -------------------------------------------------------------------
# Boundary values: inclusive range check (lo <= value <= hi)
# -------------------------------------------------------------------


class TestBoundaryValues:
    """Verify that range boundaries are inclusive (<=, not <)."""

    def test_pm25_at_zero(self):
        assert _flag("pm25", 0) == "valid"

    def test_pm25_at_max(self):
        assert _flag("pm25", 500) == "valid"

    def test_temperature_at_low_bound(self):
        assert _flag("temperature", -30) == "valid"

    def test_temperature_at_high_bound(self):
        assert _flag("temperature", 50) == "valid"

    def test_just_inside_lower(self):
        """Epsilon above lower bound should be valid."""
        assert _flag("pm25", 0.001) == "valid"

    def test_just_outside_upper(self):
        """Epsilon above upper bound should be suspect."""
        assert _flag("pm25", 500.001) == "suspect"


# -------------------------------------------------------------------
# Batch behaviour: validate_readings processes a list in place
# -------------------------------------------------------------------


class TestBatchProcessing:
    """validate_readings operates on a list, mutating each reading."""

    def test_mixed_batch(self):
        """A batch with all three flag types is classified correctly."""
        readings = [
            _make_reading("pm25", 12),     # valid
            _make_reading("pm25", -5),     # suspect
            _make_reading("pm25", None),   # missing
        ]
        result = validate_readings(readings)

        assert result[0].quality_flag == "valid"
        assert result[1].quality_flag == "suspect"
        assert result[2].quality_flag == "missing"

    def test_returns_same_list(self):
        """validate_readings mutates in place and returns the same list."""
        readings = [_make_reading("pm25", 10)]
        result = validate_readings(readings)
        assert result is readings

    def test_empty_list(self):
        """Empty input returns empty output without error."""
        assert validate_readings([]) == []


# -------------------------------------------------------------------
# Parametrized sweep: compact coverage across all known parameters
# -------------------------------------------------------------------


@pytest.mark.parametrize(
    "parameter, value, expected",
    [
        # fmt: off
        ("pm25",        -5,    "suspect"),
        ("pm25",        12,    "valid"),
        ("pm25",        None,  "missing"),
        ("pm25",        0,     "valid"),
        ("pm25",        500,   "valid"),
        ("pm25",        501,   "suspect"),
        ("pm10",        300,   "valid"),
        ("pm10",        601,   "suspect"),
        ("no2",         200,   "valid"),
        ("no2",         -1,    "suspect"),
        ("temperature", -30,   "valid"),
        ("temperature", 50,    "valid"),
        ("temperature", 60,    "suspect"),
        ("temperature", -31,   "suspect"),
        ("cosmic_rays", 999,   "valid"),
        ("unknown",     None,  "missing"),
        # fmt: on
    ],
    ids=[
        "pm25-negative",
        "pm25-typical",
        "pm25-none",
        "pm25-at-zero",
        "pm25-at-max",
        "pm25-over-max",
        "pm10-typical",
        "pm10-over-max",
        "no2-typical",
        "no2-negative",
        "temp-at-low",
        "temp-at-high",
        "temp-above-range",
        "temp-below-range",
        "unknown-param",
        "unknown-none",
    ],
)
def test_quality_flag(parameter, value, expected):
    assert _flag(parameter, value) == expected

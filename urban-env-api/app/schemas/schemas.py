"""
Consolidated Pydantic Schemas
===============================

All request/response schemas in a single file for simplified imports::

    from app.schemas.schemas import CityCreate, CityResponse, TrendResponse, ...

Sections:
  1. City schemas (CRUD request/response contracts)
  2. Analytics schemas (trend, compare, anomaly, correlation, ranking)
  3. AI Insight schemas
  4. Reading schemas
"""

import re
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ======================================================================
# CITY SCHEMAS
# ======================================================================

class CityCreate(BaseModel):
    """
    Schema for creating a new city.

    The consumer provides human-readable fields; the API generates
    the URL-safe slug ID from the city name.
    """

    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Human-readable city name. Must be unique across all cities.",
        examples=["London"],
    )

    country: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Full country name for display purposes.",
        examples=["United Kingdom"],
    )

    country_code: str = Field(
        ...,
        min_length=2,
        max_length=2,
        description="ISO 3166-1 alpha-2 country code. Must be exactly 2 uppercase letters.",
        examples=["GB"],
    )

    latitude: float = Field(
        ...,
        ge=-90.0,
        le=90.0,
        description="WGS84 latitude in decimal degrees.",
        examples=[51.5074],
    )

    longitude: float = Field(
        ...,
        ge=-180.0,
        le=180.0,
        description="WGS84 longitude in decimal degrees.",
        examples=[-0.1278],
    )

    timezone: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="IANA timezone identifier (e.g. 'Europe/London'). Used for localising timestamps in API responses.",
        examples=["Europe/London"],
    )

    # -------------------------------------------------------------------
    # Validators
    #
    # Pydantic v2 validators use @field_validator with mode="before" or
    # mode="after". "before" runs on raw input (before type coercion),
    # "after" runs on the already-typed value.
    #
    # We validate country_code format here rather than in the service
    # layer because it's a data format constraint, not a business rule.
    # The service layer validates business rules like "no duplicate names."
    # -------------------------------------------------------------------

    @field_validator("country_code")
    @classmethod
    def validate_country_code_format(cls, v: str) -> str:
        """Enforce ISO 3166-1 alpha-2: exactly 2 uppercase ASCII letters."""
        if not re.match(r"^[A-Z]{2}$", v):
            raise ValueError(
                "country_code must be exactly 2 uppercase letters (ISO 3166-1 alpha-2)"
            )
        return v

    @field_validator("timezone")
    @classmethod
    def validate_timezone_format(cls, v: str) -> str:
        """Basic sanity check: IANA timezones contain a '/' (e.g. Europe/London)."""
        if "/" not in v and v != "UTC":
            raise ValueError(
                "timezone must be a valid IANA identifier (e.g. 'Europe/London')"
            )
        return v

    def generate_slug(self) -> str:
        """
        Generate a URL-safe slug from the city name.

        Examples:
            "London"       → "london"
            "New York"     → "new-york"
            "São Paulo"    → "so-paulo"
            "Hong Kong"    → "hong-kong"

        This is intentionally simple. A production system would use a
        library like python-slugify for full Unicode transliteration.
        """
        slug = self.name.lower().strip()
        slug = re.sub(r"[^a-z0-9\s-]", "", slug)  # Remove non-alphanumeric
        slug = re.sub(r"[\s_]+", "-", slug)  # Spaces/underscores → hyphens
        slug = re.sub(r"-+", "-", slug)  # Collapse multiple hyphens
        return slug.strip("-")


class CityUpdate(BaseModel):
    """
    Schema for updating an existing city.

    All fields are optional — consumers send only the fields they want
    to change. This is the "partial update" pattern (sometimes called
    PATCH semantics, though we use PUT for simplicity).

    Why PUT with optional fields instead of PATCH?
      PATCH is technically more correct for partial updates (RFC 5789),
      but it adds complexity: you need to distinguish between "field not
      sent" (don't change) and "field sent as null" (clear the value).
      For a coursework API with a small schema, PUT with optional fields
      is pragmatic and avoids confusing consumers.
    """

    name: Optional[str] = Field(
        None,
        min_length=1,
        max_length=100,
        description="Updated city name.",
        examples=["Greater London"],
    )

    country: Optional[str] = Field(
        None,
        min_length=1,
        max_length=100,
        description="Updated country name.",
        examples=["United Kingdom"],
    )

    country_code: Optional[str] = Field(
        None,
        min_length=2,
        max_length=2,
        description="Updated ISO 3166-1 alpha-2 country code.",
        examples=["GB"],
    )

    latitude: Optional[float] = Field(
        None,
        ge=-90.0,
        le=90.0,
        description="Updated WGS84 latitude.",
        examples=[51.5074],
    )

    longitude: Optional[float] = Field(
        None,
        ge=-180.0,
        le=180.0,
        description="Updated WGS84 longitude.",
        examples=[-0.1278],
    )

    timezone: Optional[str] = Field(
        None,
        min_length=1,
        max_length=50,
        description="Updated IANA timezone identifier.",
        examples=["Europe/London"],
    )

    @field_validator("country_code")
    @classmethod
    def validate_country_code_format(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.match(r"^[A-Z]{2}$", v):
            raise ValueError(
                "country_code must be exactly 2 uppercase letters (ISO 3166-1 alpha-2)"
            )
        return v

    @field_validator("timezone")
    @classmethod
    def validate_timezone_format(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and "/" not in v and v != "UTC":
            raise ValueError(
                "timezone must be a valid IANA identifier (e.g. 'Europe/London')"
            )
        return v


class CityResponse(BaseModel):
    """
    Schema for city data returned by the API.

    Includes all public fields plus server-generated metadata
    (created_at, updated_at, is_active). Excludes internal-only
    fields if any are added in the future.

    model_config from_attributes=True tells Pydantic to read data
    from ORM model attributes (city.name) instead of dict keys
    (city["name"]). This is what allows:
        CityResponse.model_validate(city_orm_instance)
    """

    id: str = Field(
        ...,
        description="URL-safe slug identifier.",
        examples=["london"],
    )

    name: str = Field(
        ...,
        description="Human-readable city name.",
        examples=["London"],
    )

    country: str = Field(
        ...,
        description="Country name.",
        examples=["United Kingdom"],
    )

    country_code: str = Field(
        ...,
        description="ISO 3166-1 alpha-2 country code.",
        examples=["GB"],
    )

    latitude: float = Field(
        ...,
        description="WGS84 latitude in decimal degrees.",
        examples=[51.5074],
    )

    longitude: float = Field(
        ...,
        description="WGS84 longitude in decimal degrees.",
        examples=[-0.1278],
    )

    timezone: str = Field(
        ...,
        description="IANA timezone identifier.",
        examples=["Europe/London"],
    )

    is_active: bool = Field(
        ...,
        description="Whether this city is actively monitored.",
        examples=[True],
    )

    created_at: datetime = Field(
        ...,
        description="Timestamp when this city was added (UTC).",
    )

    updated_at: datetime = Field(
        ...,
        description="Timestamp of the last modification (UTC).",
    )

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": "london",
                "name": "London",
                "country": "United Kingdom",
                "country_code": "GB",
                "latitude": 51.5074,
                "longitude": -0.1278,
                "timezone": "Europe/London",
                "is_active": True,
                "created_at": "2025-02-27T10:00:00Z",
                "updated_at": "2025-02-27T10:00:00Z",
            }
        },
    )


class PaginatedCityResponse(BaseModel):
    """
    Paginated wrapper for city list responses.

    Why pagination metadata instead of just returning a list?
      A bare list gives the consumer no way to know:
        - How many total cities exist (for rendering "Page 1 of 5").
        - Whether there are more results (for "Load more" buttons).
        - What offset/limit were applied (for constructing the next page URL).

    This metadata is cheap to compute (a single COUNT(*) query) and
    dramatically improves the consumer experience.
    """

    items: list[CityResponse] = Field(
        ...,
        description="List of cities for the current page.",
    )

    total: int = Field(
        ...,
        ge=0,
        description="Total number of cities matching the query (across all pages).",
        examples=[42],
    )

    skip: int = Field(
        ...,
        ge=0,
        description="Number of records skipped (offset applied).",
        examples=[0],
    )

    limit: int = Field(
        ...,
        ge=1,
        description="Maximum number of records per page.",
        examples=[100],
    )

    @property
    def has_more(self) -> bool:
        """Whether there are more results beyond this page."""
        return (self.skip + self.limit) < self.total


class ErrorResponse(BaseModel):
    """
    Consistent error response schema used across all endpoints.

    Every HTTP error from this API follows this structure, making it
    predictable for consumers to parse. This is a simple but important
    design decision — inconsistent error formats are one of the most
    common complaints about APIs.
    """

    error: str = Field(
        ...,
        description="Machine-readable error code (e.g. 'not_found', 'duplicate_city').",
        examples=["not_found"],
    )

    message: str = Field(
        ...,
        description="Human-readable error description.",
        examples=["City with id 'atlantis' not found"],
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "error": "not_found",
                "message": "City with id 'atlantis' not found",
            }
        },
    )


# ======================================================================
# ANALYTICS SCHEMAS
# ======================================================================

# ===========================================================================
# SHARED ENUMS
# ===========================================================================


class TimeInterval(str, Enum):
    """
    Aggregation interval for trend analysis.

    Maps directly to PostgreSQL's date_trunc() argument.
    'hourly' returns raw-resolution data (no aggregation beyond
    what already exists in the readings table).
    """

    hourly = "hour"
    daily = "day"
    weekly = "week"


class AnomalySeverity(str, Enum):
    """
    Severity classification based on z-score magnitude.

    Thresholds follow standard statistical convention:
      low:    2.0 ≤ |z| < 2.5  (~1.2% of normal data)
      medium: 2.5 ≤ |z| < 3.5  (~0.6% of normal data)
      high:   |z| ≥ 3.5        (~0.05% of normal data)
    """

    low = "low"
    medium = "medium"
    high = "high"


class CorrelationStrength(str, Enum):
    """
    Interpretation of Pearson correlation coefficient.

    Based on standard interpretation guidelines:
      negligible: |r| < 0.1
      weak:       0.1 ≤ |r| < 0.3
      moderate:   0.3 ≤ |r| < 0.7
      strong:     0.7 ≤ |r| < 0.9
      very_strong:|r| ≥ 0.9
    """

    negligible = "negligible"
    weak = "weak"
    moderate = "moderate"
    strong = "strong"
    very_strong = "very_strong"


# ===========================================================================
# PATTERN 3: TREND ANALYSIS
# ===========================================================================


class TrendDataPoint(BaseModel):
    """Single aggregated data point in a trend series."""

    period: datetime = Field(
        ...,
        description="Start of the aggregation period (UTC).",
        examples=["2025-02-01T00:00:00Z"],
    )
    avg: float = Field(
        ...,
        description="Mean value for this period.",
        examples=[12.34],
    )
    min: float = Field(
        ...,
        description="Minimum value observed in this period.",
        examples=[3.2],
    )
    max: float = Field(
        ...,
        description="Maximum value observed in this period.",
        examples=[45.1],
    )
    reading_count: int = Field(
        ...,
        ge=0,
        description="Number of valid readings aggregated into this period.",
        examples=[24],
    )

    model_config = ConfigDict(from_attributes=True)


class TrendResponse(BaseModel):
    """
    Response for GET /analytics/trend/{city_id}.

    Wraps the trend data points with metadata about the query
    so the consumer knows exactly what was computed.
    """

    city_id: str = Field(..., examples=["london"])
    city_name: str = Field(..., examples=["London"])
    parameter: str = Field(..., examples=["pm25"])
    unit: str = Field(..., examples=["µg/m³"])
    interval: str = Field(..., examples=["daily"])
    days: int = Field(..., examples=[30])
    data: list[TrendDataPoint] = Field(
        ..., description="Aggregated trend data ordered chronologically."
    )
    total_readings: int = Field(
        ...,
        description="Total valid readings across all periods.",
        examples=[720],
    )


# ===========================================================================
# PATTERN 2: MULTI-CITY COMPARISON
# ===========================================================================


class CityComparisonItem(BaseModel):
    """Single city's statistics in a multi-city comparison."""

    city_id: str = Field(..., examples=["london"])
    city_name: str = Field(..., examples=["London"])
    avg: float = Field(
        ...,
        description="Mean value for this city over the time period.",
        examples=[12.34],
    )
    min: float = Field(..., examples=[3.2])
    max: float = Field(..., examples=[45.1])
    reading_count: int = Field(..., examples=[720])
    rank: int = Field(
        ...,
        description="Rank among compared cities (1 = lowest/best for pollutants).",
        examples=[1],
    )
    pct_change_vs_prev_period: Optional[float] = Field(
        None,
        description=(
            "Percentage change compared to the equivalent preceding period. "
            "E.g. if days=7, this compares the last 7 days to the 7 days before that. "
            "Null if insufficient historical data."
        ),
        examples=[-12.5],
    )

    model_config = ConfigDict(from_attributes=True)


class CompareResponse(BaseModel):
    """Response for GET /analytics/compare."""

    parameter: str = Field(..., examples=["pm25"])
    unit: str = Field(..., examples=["µg/m³"])
    days: int = Field(..., examples=[7])
    cities: list[CityComparisonItem]


# ===========================================================================
# PATTERN 4: ANOMALY DETECTION
# ===========================================================================


class AnomalyItem(BaseModel):
    """A single detected anomaly."""

    timestamp: datetime = Field(
        ...,
        description="When the anomalous reading occurred (UTC).",
    )
    value: float = Field(
        ...,
        description="The actual measured value.",
        examples=[45.1],
    )
    rolling_avg: float = Field(
        ...,
        description="Rolling 7-day average at the time of this reading.",
        examples=[12.3],
    )
    rolling_stddev: float = Field(
        ...,
        description="Rolling 7-day standard deviation.",
        examples=[5.2],
    )
    z_score: float = Field(
        ...,
        description="Number of standard deviations from the rolling mean.",
        examples=[3.21],
    )
    severity: AnomalySeverity = Field(
        ...,
        description="Classified severity based on z-score magnitude.",
        examples=["medium"],
    )

    model_config = ConfigDict(from_attributes=True)


class AnomalyResponse(BaseModel):
    """Response for GET /analytics/anomalies/{city_id}."""

    city_id: str = Field(..., examples=["london"])
    city_name: str = Field(..., examples=["London"])
    parameter: str = Field(..., examples=["pm25"])
    unit: str = Field(..., examples=["µg/m³"])
    sensitivity: float = Field(
        ...,
        description="Z-score threshold used for detection.",
        examples=[2.0],
    )
    anomalies: list[AnomalyItem]
    total_readings_analysed: int = Field(
        ...,
        description="Total readings in the analysis window.",
        examples=[720],
    )


# ===========================================================================
# PATTERN 5: CROSS-METRIC CORRELATION
# ===========================================================================


class CorrelationResponse(BaseModel):
    """Response for GET /analytics/correlation/{city_id}."""

    city_id: str = Field(..., examples=["london"])
    city_name: str = Field(..., examples=["London"])
    param1: str = Field(..., examples=["pm25"])
    param2: str = Field(..., examples=["temperature"])
    days: int = Field(..., examples=[30])
    pearson_r: Optional[float] = Field(
        None,
        description=(
            "Pearson correlation coefficient (-1 to +1). "
            "Null if insufficient paired data points."
        ),
        examples=[-0.34],
    )
    sample_size: int = Field(
        ...,
        description="Number of timestamp-aligned data point pairs used.",
        examples=[680],
    )
    interpretation: CorrelationStrength = Field(
        ...,
        description="Human-readable interpretation of correlation strength.",
        examples=["moderate"],
    )
    direction: Optional[str] = Field(
        None,
        description="'positive' or 'negative' (null if negligible).",
        examples=["negative"],
    )


# ===========================================================================
# PATTERN 5b: CITY RANKING
# ===========================================================================


class RankingItem(BaseModel):
    """Single city in a parameter ranking."""

    city_id: str = Field(..., examples=["london"])
    city_name: str = Field(..., examples=["London"])
    avg_value: float = Field(
        ...,
        description="Average value over the ranking period.",
        examples=[12.34],
    )
    rank: int = Field(
        ...,
        description="Rank (1 = lowest value, best air quality for pollutants).",
        examples=[1],
    )
    data_freshness: Optional[datetime] = Field(
        None,
        description="Timestamp of the most recent reading for this city.",
    )
    reading_count: int = Field(..., examples=[168])

    model_config = ConfigDict(from_attributes=True)


class RankingResponse(BaseModel):
    """Response for GET /analytics/ranking."""

    parameter: str = Field(..., examples=["pm25"])
    unit: str = Field(..., examples=["µg/m³"])
    country: Optional[str] = Field(None, examples=["GB"])
    days: int = Field(..., examples=[7])
    rankings: list[RankingItem]


# ======================================================================
# AI INSIGHT SCHEMAS
# ======================================================================

class AIInsightResponse(BaseModel):
    """Response for GET /ai/insight/{city_id}."""

    city_id: str = Field(..., examples=["london"])
    city_name: str = Field(..., examples=["London"])
    insight: str = Field(
        ...,
        description=(
            "AI-generated natural language summary of the city's "
            "environmental conditions over the last 7 days."
        ),
        examples=[
            "London's air quality has deteriorated over the past week, with "
            "PM2.5 levels rising 18% to an average of 14.2 µg/m³. A sharp "
            "spike on Tuesday evening coincided with a drop in wind speeds "
            "and temperature, suggesting a thermal inversion trapped pollutants "
            "near ground level. If this upward trend continues, the council "
            "should consider issuing an advisory for vulnerable residents."
        ],
    )
    generated_at: datetime = Field(
        ...,
        description="When this insight was generated (UTC).",
    )
    expires_at: datetime = Field(
        ...,
        description="When this cached insight expires (UTC).",
    )
    cached: bool = Field(
        ...,
        description=(
            "Whether this response was served from cache. "
            "True = previously generated, False = freshly generated."
        ),
    )
    model_used: str = Field(
        ...,
        description="Anthropic model used for generation.",
        examples=["claude-haiku-4-5-20251001"],
    )
    data_summary: Optional[dict] = Field(
        None,
        description=(
            "The pre-aggregated context payload that was sent to the AI model. "
            "Included for transparency and debugging. Can be suppressed with "
            "?include_context=false."
        ),
    )

    model_config = ConfigDict(from_attributes=True)


# ======================================================================
# READING SCHEMAS
# ======================================================================

class ReadingResponse(BaseModel):
    """
    Response schema for a single reading.

    Used by GET /readings endpoints and referenced in analytics
    context windows. Serialises the Reading ORM model into a
    JSON-safe format with human-readable field descriptions.
    """

    id: str = Field(
        ...,
        description="UUID of the reading.",
        examples=["a1b2c3d4-e5f6-7890-abcd-ef1234567890"],
    )
    city_id: str = Field(
        ...,
        description="City this reading belongs to.",
        examples=["london"],
    )
    parameter: str = Field(
        ...,
        description="Environmental parameter measured.",
        examples=["pm25"],
    )
    value: Optional[float] = Field(
        None,
        description="Normalised measurement value. NULL for missing readings.",
        examples=[12.5],
    )
    unit: str = Field(
        ...,
        description="Unit of the normalised value.",
        examples=["µg/m³"],
    )
    raw_value: Optional[float] = Field(
        None,
        description="Original value from the source API before normalisation.",
        examples=[12.5],
    )
    raw_unit: str = Field(
        ...,
        description="Original unit from the source API.",
        examples=["µg/m³"],
    )
    quality_flag: str = Field(
        ...,
        description="Data quality classification: 'valid', 'suspect', or 'missing'.",
        examples=["valid"],
    )
    source: str = Field(
        ...,
        description="Data source identifier.",
        examples=["openaq"],
    )
    timestamp: datetime = Field(
        ...,
        description="When the measurement was taken (UTC).",
    )
    created_at: datetime = Field(
        ...,
        description="When this record was inserted into the database.",
    )

    model_config = ConfigDict(from_attributes=True)

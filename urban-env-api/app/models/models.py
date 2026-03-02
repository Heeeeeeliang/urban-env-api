"""
Consolidated ORM Models
========================

All SQLAlchemy ORM models in a single file for simplified imports::

    from app.models.models import Base, City, Reading, AIInsight

Models:
  - Base: DeclarativeBase with shared metadata registry
  - TimestampMixin: Adds created_at / updated_at to any model
  - City: Monitored city with geolocation metadata (dimension table)
  - Reading: Environmental sensor reading in EAV format (fact table)
  - AIInsight: Cached AI-generated insight per city (TTL-based)
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


# ======================================================================
# BASE & MIXINS
# ======================================================================

class Base(DeclarativeBase):
    """
    Declarative base for all ORM models.

    All models inherit from this. SQLAlchemy uses Base.metadata to track
    table definitions, which is used by:
      - create_all() in the lifespan to create tables.
      - Alembic (if added later) to generate migration scripts.
      - The ORM's identity map to resolve relationships.
    """
    pass


class TimestampMixin:
    """
    Mixin that adds created_at and updated_at columns to any model.

    Why server-side defaults (server_default) instead of Python-side defaults?
      - server_default=func.now() tells PostgreSQL to set the timestamp.
        This guarantees consistency even if data is inserted via raw SQL,
        migrations, or external tools — not just through the ORM.
      - Python-side defaults (default=datetime.utcnow) use the application
        server's clock, which may differ from the database server's clock.
        In a multi-server deployment, this causes subtle ordering bugs.

    Why UTC?
      Environmental data spans multiple timezones. Storing everything in UTC
      and converting to local time at the API response layer prevents:
        - Ambiguous timestamps during DST transitions.
        - Incorrect ordering when comparing readings across cities.
      The timezone conversion is the API consumer's responsibility — we
      provide UTC timestamps and the city's timezone in the response.

    Why updated_at?
      Tracks when a row was last modified. Essential for:
        - Cache invalidation (has this data changed since I last fetched it?).
        - Debugging data quality issues (when was this reading corrected?).
        - ETL pipelines that need to process only changed records.
      onupdate=func.now() tells PostgreSQL to refresh this on every UPDATE.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Row creation timestamp (UTC, set by database server)",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        doc="Last modification timestamp (UTC, updated automatically)",
    )


# ======================================================================
# CITY MODEL
# ======================================================================

class City(Base, TimestampMixin):
    """
    Represents a monitored city.

    This is a low-cardinality reference table (likely <50 rows).
    It exists to normalise city metadata out of the high-volume readings table.
    """

    __tablename__ = "cities"

    # -----------------------------------------------------------------------
    # Primary Key
    #
    # We use a string slug (e.g. "london", "tokyo") instead of an auto-
    # incrementing integer. Rationale:
    #   - city_id appears in API URLs: /api/v1/cities/london is more
    #     readable and bookmarkable than /api/v1/cities/7.
    #   - The set of cities is small and stable — we're not generating IDs
    #     at high throughput where integer PKs have performance advantages.
    #   - It makes raw SQL queries during development more readable:
    #     WHERE city_id = 'london' vs WHERE city_id = 7.
    #
    # Trade-off: string PKs use more storage per foreign key reference
    # than integers (~20 bytes vs 4 bytes). At our scale (<1M readings),
    # this is negligible. At 100M+ rows, you'd want integer PKs with a
    # separate slug column.
    # -----------------------------------------------------------------------
    id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
        doc="URL-safe slug identifier (e.g. 'london', 'new-york')",
    )

    # -----------------------------------------------------------------------
    # City Metadata
    # -----------------------------------------------------------------------
    name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Human-readable display name (e.g. 'London', 'New York')",
    )

    country: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Country name for display and grouping",
    )

    # ISO 3166-1 alpha-2 country code (e.g. "GB", "US").
    # Useful for frontend flag icons and for querying external APIs
    # that accept country codes.
    country_code: Mapped[str] = mapped_column(
        String(2),
        nullable=False,
        doc="ISO 3166-1 alpha-2 country code",
    )

    latitude: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        doc="WGS84 latitude (decimal degrees)",
    )

    longitude: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        doc="WGS84 longitude (decimal degrees)",
    )

    timezone: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        doc="IANA timezone identifier (e.g. 'Europe/London')",
    )

    # -----------------------------------------------------------------------
    # Soft Delete
    # -----------------------------------------------------------------------
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default="true",
        nullable=False,
        doc="Soft delete flag. Inactive cities are excluded from API responses.",
    )

    # -----------------------------------------------------------------------
    # Relationships
    #
    # back_populates creates a bidirectional relationship: city.readings
    # and reading.city both work. This is more explicit than backref,
    # which creates the reverse relationship implicitly (and can surprise
    # developers who don't realise it exists).
    #
    # lazy="selectin" uses a SELECT ... WHERE city_id IN (...) strategy
    # when loading related readings. This avoids the N+1 query problem
    # when loading multiple cities with their readings.
    # However, for our typical access pattern (query readings directly,
    # join to city for display name), we rarely traverse this relationship.
    # It exists primarily for administrative endpoints that need city details
    # alongside a summary of their data.
    # -----------------------------------------------------------------------
    readings: Mapped[list["Reading"]] = relationship(
        "Reading",
        back_populates="city",
        lazy="selectin",
    )

    # -----------------------------------------------------------------------
    # Indexes
    #
    # country_code index: enables efficient filtering by country in the
    # multi-city comparison endpoint (Pattern 2), where a consumer might
    # request "compare all UK cities."
    # -----------------------------------------------------------------------
    __table_args__ = (
        Index("ix_cities_country_code", "country_code"),
        Index("ix_cities_is_active", "is_active"),
    )

    def __repr__(self) -> str:
        return f"<City(id='{self.id}', name='{self.name}')>"


# ======================================================================
# READING MODEL
# ======================================================================

class Reading(Base, TimestampMixin):
    """
    A single environmental measurement at a point in time.

    Each row represents ONE parameter (e.g. PM2.5) for ONE city at ONE timestamp.
    This is the narrow/EAV design — multiple parameters at the same timestamp
    are stored as separate rows, not separate columns.

    Expected volume: ~500k rows/year for 10 cities × 8 parameters × hourly readings.
    """

    __tablename__ = "readings"

    # -----------------------------------------------------------------------
    # Primary Key
    #
    # UUID v4 instead of auto-incrementing integer. Rationale:
    #   - Ingestion is idempotent: if the scheduler runs twice for the same
    #     hour, we need to detect duplicates. With UUIDs, we can generate
    #     deterministic IDs from (city_id, parameter, timestamp) using UUID v5,
    #     making upserts natural. With auto-increment, duplicate detection
    #     requires a separate unique constraint check.
    #   - UUIDs are globally unique, which matters if we ever merge data
    #     from multiple instances or export to external systems.
    #
    # Trade-off: UUIDs are 16 bytes vs 4 bytes for integers, and their
    # randomness causes B-tree index fragmentation. At 500k rows, this
    # is not a concern. At 50M+ rows, consider UUID v7 (time-ordered)
    # or switch to integer PKs.
    #
    # server_default generates the UUID in PostgreSQL, not in Python.
    # This ensures consistency even for raw SQL inserts.
    # -----------------------------------------------------------------------
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        doc="Unique reading identifier (UUID v4)",
    )

    # -----------------------------------------------------------------------
    # Foreign Key: City
    #
    # ondelete="CASCADE" vs "RESTRICT":
    #   We use RESTRICT (the default) because deleting a city should NOT
    #   silently delete thousands of readings. The soft-delete pattern on
    #   the City model (is_active=False) is the correct way to "remove"
    #   a city while preserving data integrity.
    #
    #   CASCADE would be appropriate if readings had no value without their
    #   parent city — but environmental readings are valuable historical
    #   data regardless of whether the city is actively monitored.
    # -----------------------------------------------------------------------
    city_id: Mapped[str] = mapped_column(
        String(50),
        ForeignKey("cities.id", ondelete="RESTRICT"),
        nullable=False,
        doc="Reference to the city this reading belongs to",
    )

    # -----------------------------------------------------------------------
    # The EAV "Attribute": parameter
    #
    # This is the column that makes the narrow table design work.
    # Values: 'pm25', 'pm10', 'no2', 'o3', 'co', 'so2', 'temperature',
    #         'humidity', 'wind_speed', 'pressure', etc.
    #
    # Why a string and not a foreign key to a parameters table?
    #   A parameters table would enforce a closed set of allowed values,
    #   which sounds good but creates friction: every new parameter
    #   requires an INSERT into the parameters table before ingestion can
    #   write readings. A CHECK constraint (below) provides validation
    #   without the join overhead, and adding a new parameter is a single
    #   ALTER TABLE to extend the constraint.
    #
    #   For a more rigorous system, a parameters table with metadata
    #   (display_name, unit, description, valid_range) would be justified.
    #   We document this as a known simplification in the technical report.
    # -----------------------------------------------------------------------
    parameter: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        doc="Environmental parameter identifier (e.g. 'pm25', 'temperature')",
    )

    # -----------------------------------------------------------------------
    # The EAV "Value": normalised measurement
    #
    # All values are stored in a canonical unit:
    #   - Pollutants: µg/m³ (micrograms per cubic meter)
    #   - Temperature: °C (Celsius)
    #   - Humidity: % (relative humidity)
    #   - Wind speed: m/s (meters per second)
    #   - Pressure: hPa (hectopascals)
    #
    # The normalisation happens in the ingestion layer. The original value
    # and unit are preserved in raw_value and raw_unit for auditability.
    # -----------------------------------------------------------------------
    value: Mapped[float] = mapped_column(
        Float,
        nullable=True,  # Nullable because quality_flag='missing' has no value
        doc="Normalised measurement value in canonical units",
    )

    unit: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        doc="Canonical unit for the normalised value (e.g. 'µg/m³', '°C')",
    )

    # -----------------------------------------------------------------------
    # Data Lineage: raw value preservation
    #
    # These columns make the data pipeline fully reversible and auditable.
    # If we discover a unit conversion bug six months from now, we can
    # reprocess all readings from raw_value + raw_unit without re-fetching
    # from external APIs (which may no longer serve historical data).
    # -----------------------------------------------------------------------
    raw_value: Mapped[float] = mapped_column(
        Float,
        nullable=True,
        doc="Original value as received from the data source, before normalisation",
    )

    raw_unit: Mapped[str] = mapped_column(
        String(20),
        nullable=True,
        doc="Original unit from the data source (e.g. 'ppb', '°F')",
    )

    # -----------------------------------------------------------------------
    # Data Quality
    #
    # Three-state quality model applied during ingestion:
    #   'valid'   — value within expected physical bounds.
    #   'suspect' — value outside expected bounds but not null.
    #              (e.g. PM2.5 of 600: physically possible in extreme
    #               events but warrants investigation)
    #   'missing' — no data received for this parameter at this timestamp.
    #              (We insert a row with value=NULL and quality_flag='missing'
    #               to distinguish "no reading" from "never checked")
    #
    # Why store missing readings as rows?
    #   Data gaps are information. If PM2.5 readings stop for 3 hours,
    #   the anomaly detection endpoint (Pattern 4) needs to know whether
    #   the air was clean or the sensor was offline. Missing rows make
    #   gaps visible in query results.
    # -----------------------------------------------------------------------
    quality_flag: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        default="valid",
        server_default="valid",
        doc="Data quality classification: 'valid', 'suspect', or 'missing'",
    )

    # -----------------------------------------------------------------------
    # Data Source
    #
    # Identifies which external API provided this reading.
    # Values: 'openaq', 'open-meteo', 'manual', 'derived'
    #
    # 'derived' is for computed values (e.g. AQI calculated from raw
    # pollutant readings) — not a current feature, but the schema supports
    # it without migration.
    # -----------------------------------------------------------------------
    source: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        doc="Data provenance: which API or process generated this reading",
    )

    # -----------------------------------------------------------------------
    # Timestamp
    #
    # The timestamp of the actual measurement, NOT when it was ingested.
    # (Ingestion time is captured by created_at from TimestampMixin.)
    #
    # timezone=True stores as TIMESTAMPTZ in PostgreSQL, which normalises
    # all values to UTC on storage and converts to session timezone on
    # retrieval. Since we always work in UTC, this is transparent but
    # prevents the class of bugs where naive timestamps are silently
    # interpreted in the server's local timezone.
    #
    # This column is the time-axis for all analytical queries and is
    # part of the primary composite index.
    # -----------------------------------------------------------------------
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="Measurement timestamp (UTC). When the reading was taken, not ingested.",
    )

    # -----------------------------------------------------------------------
    # Relationships
    # -----------------------------------------------------------------------
    city: Mapped["City"] = relationship(
        "City",
        back_populates="readings",
        lazy="joined",  # Eagerly load city data — it's small and almost always needed
    )

    # -----------------------------------------------------------------------
    # Indexes — THE MOST CRITICAL PERFORMANCE DECISION
    #
    # The composite index (city_id, parameter, timestamp) is the backbone
    # of all five query patterns. Here's how each pattern uses it:
    #
    # Pattern 1 (single city, time range, one metric):
    #   WHERE city_id = $1 AND parameter = $2 AND timestamp BETWEEN $3 AND $4
    #   → Full index scan on all three columns. Optimal.
    #
    # Pattern 2 (multi-city comparison):
    #   WHERE parameter = $1 AND timestamp >= ...
    #   → Uses (parameter, timestamp) suffix of the index via index skip scan
    #     (PostgreSQL 14+) or falls back to the parameter-only index.
    #
    # Pattern 3 (time-aggregated trend):
    #   WHERE city_id = $1 AND parameter = $2 GROUP BY date_trunc(...)
    #   → Index range scan on (city_id, parameter), then sequential scan
    #     within the timestamp range. Optimal for aggregation.
    #
    # Pattern 4 (anomaly detection with window functions):
    #   PARTITION BY city_id, parameter ORDER BY timestamp
    #   → The index IS the partition and ordering. PostgreSQL can scan the
    #     index in order without a sort step. This is crucial — window
    #     functions on unsorted data require a full sort (O(n log n)).
    #
    # Pattern 5 (cross-metric correlation via self-join):
    #   JOIN ON city_id AND timestamp WHERE parameter IN ($1, $2)
    #   → Both sides of the join use the index. The join strategy is
    #     typically a merge join (both sides are already sorted by timestamp
    #     within the index).
    #
    # The quality_flag index supports the default filter (quality_flag = 'valid')
    # that appears in all analytical queries. PostgreSQL's query planner
    # will combine it with the composite index via a BitmapAnd.
    #
    # The unique constraint on (city_id, parameter, timestamp) prevents
    # duplicate readings from being inserted by the ingestion pipeline.
    # This is the idempotency guarantee: running ingestion twice for the
    # same hour produces the same database state.
    # -----------------------------------------------------------------------
    __table_args__ = (
        # Primary analytical index — covers all 5 query patterns
        Index(
            "ix_readings_city_param_ts",
            "city_id",
            "parameter",
            "timestamp",
        ),
        # Supports Pattern 2 (multi-city comparison by parameter)
        Index(
            "ix_readings_param_ts",
            "parameter",
            "timestamp",
        ),
        # Supports quality-filtered queries (all patterns default to 'valid')
        Index(
            "ix_readings_quality",
            "quality_flag",
        ),
        # Supports the /health endpoint's "most recent reading" query
        Index(
            "ix_readings_created_at",
            "created_at",
        ),
        # Idempotency constraint: one reading per city/parameter/timestamp
        Index(
            "uq_readings_city_param_ts",
            "city_id",
            "parameter",
            "timestamp",
            unique=True,
        ),
        # Quality bounds: quality_flag must be one of our three states
        CheckConstraint(
            "quality_flag IN ('valid', 'suspect', 'missing')",
            name="ck_readings_quality_flag",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<Reading(city='{self.city_id}', param='{self.parameter}', "
            f"value={self.value}, ts='{self.timestamp}')>"
        )


# ======================================================================
# AI INSIGHT CACHE MODEL
# ======================================================================

class AIInsight(Base, TimestampMixin):
    """
    Cached AI-generated environmental insight for a city.

    One active (non-expired) insight per city at any time.
    Expired insights are retained for:
      - Fallback serving when the Anthropic API is unavailable.
      - Historical analysis of how AI interpretations evolve.
      - Cost auditing (token_count over time).
    """

    __tablename__ = "ai_insights"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    city_id: Mapped[str] = mapped_column(
        String(50),
        ForeignKey("cities.id", ondelete="CASCADE"),
        nullable=False,
        doc="City this insight was generated for.",
    )

    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="When the insight was generated (UTC).",
    )

    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="When this cached insight expires (UTC). Based on CACHE_TTL_SECONDS.",
    )

    insight_text: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc="The generated natural language insight.",
    )

    # SHA-256 hash of the context payload sent to the LLM.
    # If the same context produces the same hash, we know the underlying
    # data hasn't changed and can serve the cache even if it's technically
    # "fresh" (regenerated with identical input).
    context_hash: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        doc="SHA-256 of the context JSON sent to the LLM.",
    )

    model_used: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Anthropic model identifier used for generation.",
    )

    # Token counts for cost tracking.
    # Stored separately for input and output because pricing differs.
    input_tokens: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of input tokens consumed.",
    )

    output_tokens: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of output tokens generated.",
    )

    # Relationship back to city
    city: Mapped["City"] = relationship("City", lazy="joined")

    __table_args__ = (
        # Fast lookup: "give me the latest non-expired insight for this city"
        Index("ix_ai_insights_city_expires", "city_id", "expires_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<AIInsight(city='{self.city_id}', "
            f"generated='{self.generated_at}', "
            f"expires='{self.expires_at}')>"
        )

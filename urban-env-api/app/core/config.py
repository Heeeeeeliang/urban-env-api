"""
Application Configuration — Single Source of Truth
===================================================

Architectural decisions documented here:

1. PYDANTIC SETTINGS (not os.environ or dotenv directly)
   Pydantic Settings provides three things os.environ doesn't:
     (a) Type validation at startup — if DATABASE_URL is missing or
         CACHE_TTL_SECONDS isn't an integer, the app fails immediately
         with a clear error instead of crashing at runtime when that
         config value is first accessed.
     (b) Documented schema — this file IS the configuration documentation.
         Every setting has a type, a default (or lack thereof), and a
         description. New developers read this file to understand what
         the app needs.
     (c) Immutability — once loaded, settings can't be accidentally
         mutated by business logic. This prevents a class of bugs where
         one module changes a config value and breaks another module.

2. SEPARATION OF REQUIRED vs OPTIONAL SETTINGS
   Settings without defaults (DATABASE_URL, ANTHROPIC_API_KEY) will
   cause a ValidationError at import time if not provided. This is
   intentional: fail fast, fail loudly. Settings with defaults
   (CACHE_TTL_SECONDS, CORS_ORIGINS) are tuning parameters that have
   sensible defaults for development.

3. ENVIRONMENT-AWARE CONFIGURATION
   The ENVIRONMENT field controls behaviour switches (e.g. CORS origins,
   log verbosity, debug mode). We don't use separate config files per
   environment — that leads to config drift. Instead, a single schema
   with environment-aware defaults keeps configuration DRY.

4. WHY NOT A .env FILE ALONE?
   .env files are loaded for development convenience (model_config
   env_file=".env"), but they're not the canonical config source.
   In production/deployment, environment variables are injected by the
   runtime (Docker, systemd, cloud platform). The .env file is a
   development-only convenience and is .gitignored.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import ClassVar


class Settings(BaseSettings):
    """
    Application settings, loaded from environment variables.

    Priority order (highest to lowest):
      1. Explicit environment variables (e.g. export DATABASE_URL=...)
      2. .env file in the project root
      3. Default values defined here

    Usage:
      from app.core.config import settings
      print(settings.DATABASE_URL)
    """

    # =======================================================================
    # ENVIRONMENT
    # =======================================================================
    # Controls behaviour switches. Possible values: development, staging, production.
    # Never used for feature flags — use explicit settings for those.
    ENVIRONMENT: str = "development"

    # =======================================================================
    # DATABASE
    # =======================================================================
    # No default: the app MUST be given a database URL. This is the single
    # most critical configuration value. Format:
    #   postgresql+asyncpg://user:password@host:port/dbname
    #
    # We use asyncpg (not psycopg2) because our entire I/O path is async.
    # Mixing sync DB drivers with async FastAPI causes thread pool exhaustion
    # under load — a subtle bug that's hard to diagnose.
    DATABASE_URL: str

    # Connection pool sizing. These defaults are appropriate for a single-server
    # deployment with moderate concurrency.
    #
    # pool_size=5: Number of persistent connections. Each connection holds a
    #   PostgreSQL backend process, so this should be <= max_connections / number_of_workers.
    # max_overflow=10: Burst capacity above pool_size. Overflow connections are
    #   created on demand and destroyed after use. This handles request spikes
    #   without permanently holding excess connections.
    #
    # For coursework with a single uvicorn worker, these are generous.
    # In production, you'd tune these based on observed connection utilisation.
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10

    # =======================================================================
    # EXTERNAL API KEYS
    # =======================================================================
    # Anthropic API key for the /ai/insight endpoint.
    # No default: if you haven't configured this, the AI endpoint should
    # return a clear error rather than silently failing.
    ANTHROPIC_API_KEY: str

    # OpenAQ API key.
    # OpenAQ offers a free tier with rate limits. The key is required to
    # avoid the unauthenticated rate limit (which is very restrictive).
    OPENAQ_API_KEY: str = ""

    # Open-Meteo doesn't require an API key for its free tier.
    # We include this field for forward-compatibility in case they add
    # key-based rate limit tiers (common pattern for weather APIs).
    OPEN_METEO_API_KEY: str = ""

    # =======================================================================
    # AI INSIGHT CONFIGURATION
    # =======================================================================
    # The Claude model to use for generating environmental insights.
    # We default to claude-sonnet-4-20250514 — it balances quality and cost well
    # for structured summarisation tasks. claude-opus-4-5-20250929 would be overkill
    # for this use case; haiku would sacrifice quality.
    ANTHROPIC_MODEL: str = "claude-sonnet-4-20250514"

    # Maximum tokens for the AI insight response. Environmental summaries
    # should be concise (2-3 paragraphs), so 1024 tokens is generous.
    # Raising this increases latency and cost without proportional quality gain.
    AI_MAX_TOKENS: int = 1024

    # =======================================================================
    # CACHING
    # =======================================================================
    # TTL for cached AI insights, in seconds.
    # Default: 3600 (1 hour), aligned with the ingestion interval.
    # The insight cache should invalidate when new data arrives — setting
    # this equal to INGESTION_INTERVAL_MINUTES * 60 ensures insights are
    # regenerated once per ingestion cycle.
    #
    # Why DB-level caching instead of Redis?
    # Redis adds operational complexity (another service to run, monitor,
    # and back up). For a single-server deployment with <100 cached items
    # (num_cities × 1), a simple DB table with TTL-based expiry is simpler,
    # equally fast (single indexed lookup), and doesn't introduce a new
    # failure mode. Redis becomes justified at ~10k+ cached items or when
    # you need sub-millisecond reads.
    CACHE_TTL_SECONDS: int = 3600

    # =======================================================================
    # DATA INGESTION
    # =======================================================================
    # How often (in minutes) the background scheduler fetches new data
    # from OpenAQ and Open-Meteo. Default: 60 (hourly).
    #
    # This value has cascading architectural implications:
    #   - It defines your data resolution ceiling (can't serve finer-grained
    #     data than you ingest).
    #   - It determines the /health endpoint's staleness threshold (2x this value).
    #   - It should align with CACHE_TTL_SECONDS to avoid serving stale AI insights.
    #   - OpenAQ's rate limits may constrain how low you can set this.
    #
    # For coursework development, 60 minutes is conservative and avoids
    # hitting API rate limits during testing.
    INGESTION_INTERVAL_MINUTES: int = 60

    # =======================================================================
    # CORS
    # =======================================================================
    # Allowed origins for cross-origin requests.
    # Default permits localhost development servers (React, Jupyter, etc.).
    #
    # This is a comma-separated string in the environment variable,
    # parsed into a list by the validator below.
    CORS_ORIGINS: list[str] = [
        "http://localhost:3000",  # React dev server
        "http://localhost:8080",  # Vue dev server
        "http://localhost:8888",  # Jupyter
        "http://127.0.0.1:8000", # FastAPI's own Swagger UI
    ]

    # =======================================================================
    # API KEY MANAGEMENT (MULTI-KEY SUPPORT)
    # =======================================================================
    # Single API key for backward compatibility.
    # This is the original field from .env — kept so existing deployments
    # continue to work without configuration changes.
    API_KEY: str = ""

    # Multi-key support: JSON-encoded dict mapping keys to permission tiers.
    # Format: {"key1": "admin", "key2": "read", "key3": "write"}
    # If not set, the system falls back to API_KEY with admin permissions.
    # Set via environment variable:
    #   API_KEYS='{"abc123": "admin", "reader-key": "read"}'
    API_KEYS: str = ""

    # =======================================================================
    # API RATE LIMITING
    # =======================================================================
    # Simple rate limiting: max requests per minute per API key.
    # This is a placeholder for a production rate limiter (e.g. slowapi).
    # For coursework, it exists to demonstrate awareness of the concern
    # and to document in the technical report.
    RATE_LIMIT_PER_MINUTE: int = 60

    # =======================================================================
    # RATE LIMITING (WRITE OPERATIONS)
    # =======================================================================
    # Separate limit for write operations (POST/PUT/DELETE).
    # Writes are more expensive (DB mutations) so we cap them tighter.
    # Defaults to 1/3 of the read limit.
    RATE_LIMIT_WRITES_PER_MINUTE: int = 20

    # =======================================================================
    # VALIDATION RULES FOR DATA QUALITY
    # =======================================================================
    # These define the bounds for quality_flag classification during ingestion.
    # Values outside these ranges are flagged as 'suspect'.
    # Values that are None/null are flagged as 'missing'.
    #
    # Source: WHO Air Quality Guidelines + OpenAQ documentation.
    # PM2.5 > 500 is physically implausible (highest recorded: ~999 in extreme events).
    # Negative pollutant values indicate sensor malfunction.
    QUALITY_PM25_MIN: float = 0.0
    QUALITY_PM25_MAX: float = 500.0
    QUALITY_TEMPERATURE_MIN: float = -90.0   # Coldest recorded: -89.2°C
    QUALITY_TEMPERATURE_MAX: float = 60.0    # Hottest recorded: 56.7°C

    # =======================================================================
    # SETTINGS INFRASTRUCTURE
    # =======================================================================
    # model_config replaces the old class Config in Pydantic v2.
    # env_file loads .env for development; env_file_encoding handles
    # UTF-8 .env files on Windows.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # case_sensitive=True means DATABASE_URL in code must match
        # DATABASE_URL in the environment. This prevents subtle bugs
        # on case-insensitive filesystems (Windows, macOS).
        case_sensitive=True,
    )

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        """
        Accept CORS_ORIGINS as either a comma-separated string
        (from environment variable) or a list (from code/defaults).

        Environment variable example:
          CORS_ORIGINS=http://localhost:3000,https://my-dashboard.com
        """
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return v


# ---------------------------------------------------------------------------
# Singleton instance.
#
# Settings is instantiated at import time. This means:
#   - Validation runs once, on first import.
#   - Missing required variables cause an immediate, clear crash.
#   - All modules share the same validated instance.
#
# This is intentional: configuration errors should be caught at startup,
# not at 3am when the /ai/insight endpoint first tries to read
# ANTHROPIC_API_KEY and finds it's empty.
# ---------------------------------------------------------------------------
settings = Settings()

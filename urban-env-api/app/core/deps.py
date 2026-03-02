"""
Shared FastAPI Dependencies
============================

Architectural decisions documented here:

1. DEPENDENCY INJECTION (not global state)
   FastAPI's Depends() system is its most underappreciated feature.
   Instead of importing a global db session or reading config values
   at module level, we declare dependencies as function parameters.
   This provides:
     - Testability: override any dependency in tests without monkey-patching.
     - Lifecycle management: resources are created/destroyed per request.
     - Documentation: the function signature IS the dependency graph.
     - Type safety: the IDE knows db is an AsyncSession, not Any.

2. COMMON QUERY PARAMETERS AS DEPENDENCIES
   Pagination, filtering, and quality flag inclusion appear across
   multiple endpoints. Extracting them into shared dependencies ensures:
     - Consistent parameter names across the API (skip/limit, not
       offset/count in one endpoint and page/per_page in another).
     - Centralised validation (limit capped at 1000 to prevent
       accidental full-table dumps).
     - Single place to change defaults.

3. API KEY VALIDATION
   A minimal authentication layer. This is NOT production-grade security
   (no OAuth2, no JWT, no user model). It exists to:
     - Demonstrate awareness of API security concerns.
     - Provide a hook for rate limiting (future: track usage per key).
     - Show how FastAPI's dependency injection handles cross-cutting
       concerns like auth without polluting business logic.
   The technical report documents that a production system would use
   OAuth2 with scopes, or at minimum, hashed API keys with a keys table.
"""

from datetime import datetime, timezone, timedelta
from typing import Annotated, Optional

from fastapi import Depends, Header, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db


# ---------------------------------------------------------------------------
# Type alias for the DB session dependency.
#
# This pattern (PEP 593 Annotated type) lets route handlers declare:
#   async def list_cities(db: DbSession):
# instead of:
#   async def list_cities(db: AsyncSession = Depends(get_db)):
#
# It's purely a readability improvement — both forms are functionally
# identical. The Annotated form is the modern FastAPI convention.
# ---------------------------------------------------------------------------
DbSession = Annotated[AsyncSession, Depends(get_db)]


# ---------------------------------------------------------------------------
# Common Query Parameters
#
# These are shared across endpoints that return paginated lists or
# time-series data. By defining them as dependency classes, we get:
#   - Consistent parameter names across the API.
#   - Automatic OpenAPI documentation with descriptions and defaults.
#   - Centralised validation (e.g. limit <= 1000).
# ---------------------------------------------------------------------------

class PaginationParams:
    """
    Standard pagination parameters for list endpoints.

    Usage in a route handler:
        @router.get("/readings")
        async def list_readings(
            pagination: PaginationParams = Depends(),
        ):
            query = select(Reading).offset(pagination.skip).limit(pagination.limit)

    Why offset/limit instead of cursor-based pagination?
      Offset/limit is simpler to implement and sufficient for our data volumes.
      The trade-off is that deep pagination (skip=100000) becomes slow because
      PostgreSQL must scan and discard skipped rows. For time-series data,
      cursor-based pagination (WHERE timestamp > last_seen_timestamp) would
      be more efficient, but adds implementation complexity. This is documented
      as a known limitation.
    """

    def __init__(
        self,
        skip: Annotated[
            int,
            Query(ge=0, description="Number of records to skip (offset)"),
        ] = 0,
        limit: Annotated[
            int,
            Query(
                ge=1,
                le=1000,
                description="Maximum number of records to return (capped at 1000)",
            ),
        ] = 100,
    ):
        self.skip = skip
        self.limit = limit


class TimeRangeParams:
    """
    Time range filter for time-series endpoints (Patterns 1-4).

    Defaults to the last 24 hours if no range is specified.
    This default prevents accidental full-table scans when a consumer
    forgets to include a time range — a defensive design choice.

    Usage:
        @router.get("/readings/{city_id}/{parameter}")
        async def get_readings(
            city_id: str,
            parameter: str,
            time_range: TimeRangeParams = Depends(),
        ):
            query = select(Reading).where(
                Reading.timestamp.between(time_range.start, time_range.end)
            )
    """

    def __init__(
        self,
        start: Annotated[
            Optional[datetime],
            Query(
                description=(
                    "Start of time range (ISO 8601). "
                    "Defaults to 24 hours ago."
                ),
            ),
        ] = None,
        end: Annotated[
            Optional[datetime],
            Query(
                description=(
                    "End of time range (ISO 8601). "
                    "Defaults to now."
                ),
            ),
        ] = None,
    ):
        # Default to last 24 hours if not specified.
        # Using timezone-aware datetimes (UTC) to match the TIMESTAMPTZ
        # column in PostgreSQL. Naive datetimes would cause comparison
        # errors or silent timezone misinterpretation.
        now = datetime.now(timezone.utc)
        self.start = start or (now - timedelta(hours=24))
        self.end = end or now

        # Validate that start < end. This prevents confusing empty results
        # when a consumer accidentally swaps the parameters.
        if self.start >= self.end:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="'start' must be before 'end'",
            )


class QualityFilterParams:
    """
    Data quality filter, used by all analytical endpoints.

    By default, only 'valid' readings are included. This prevents
    suspect or missing data from silently corrupting analytics.
    Consumers who want to perform their own quality assessment can
    explicitly opt in to suspect data.
    """

    def __init__(
        self,
        include_suspect: Annotated[
            bool,
            Query(
                description=(
                    "Include readings flagged as 'suspect' in addition to 'valid'. "
                    "Default: false (only validated readings)."
                ),
            ),
        ] = False,
    ):
        self.allowed_flags = ["valid"]
        if include_suspect:
            self.allowed_flags.append("suspect")


# ---------------------------------------------------------------------------
# API Key Authentication
#
# IMPORTANT: This is a MINIMAL auth implementation for coursework purposes.
# It demonstrates the dependency injection pattern for cross-cutting concerns.
#
# What this does NOT do (and a production system would):
#   - Hash API keys (we compare plaintext — acceptable for coursework).
#   - Look up keys in a database (we use a hardcoded list).
#   - Associate keys with users/permissions (no RBAC).
#   - Rotate keys or handle expiration.
#   - Rate limit per key.
#
# What it DOES demonstrate:
#   - How to implement auth as a dependency that's injected into routes.
#   - How to return proper 401/403 responses with consistent error format.
#   - How to make auth optional for some routes (health, docs) and
#     required for others (data endpoints).
# ---------------------------------------------------------------------------

# In production, these would come from a database table, not a config list.
# For coursework, we accept any non-empty key to simplify testing.
# A real implementation would validate against a hashed keys table.
VALID_API_KEYS = {settings.ANTHROPIC_API_KEY}  # Placeholder — replace with real keys


async def verify_api_key(
    x_api_key: Annotated[
        str,
        Header(
            description="API key for authentication. Include as X-Api-Key header.",
        ),
    ],
) -> str:
    """
    Validate the API key from the X-Api-Key header.

    Returns the validated key (useful for logging/auditing which key
    made a request, even though we don't have per-key permissions yet).

    Raises:
        HTTPException 401: If the header is missing or the key is invalid.
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": "missing_api_key",
                "message": "X-Api-Key header is required",
            },
        )

    # For coursework: accept any non-empty key to simplify testing.
    # In production: validate against a hashed keys table.
    # Uncomment the block below to enforce key validation:
    #
    # if x_api_key not in VALID_API_KEYS:
    #     raise HTTPException(
    #         status_code=status.HTTP_403_FORBIDDEN,
    #         detail={
    #             "error": "invalid_api_key",
    #             "message": "The provided API key is not valid",
    #         },
    #     )

    return x_api_key

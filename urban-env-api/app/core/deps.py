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

3. API KEY VALIDATION WITH PERMISSION TIERS
   Enhanced from the original minimal implementation to support:
     - Multiple API keys with permission levels: "read", "write", "admin".
     - Keys loaded from the API_KEYS env var as JSON (e.g.
       {"key1": "admin", "key2": "read"}).
     - Backward compatible: if API_KEYS is not set, falls back to
       the single API_KEY env var with "admin" permission.
     - 401 for missing/invalid keys, 403 for insufficient permissions.
   The technical report documents that a production system would use
   OAuth2 with scopes, or at minimum, hashed API keys with a keys table.
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Annotated, Optional

from fastapi import Depends, Header, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db

logger = logging.getLogger(__name__)


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
# API Key Authentication — Permission Tier System
#
# UPGRADE FROM ORIGINAL:
#   The original implementation accepted any non-empty key. This enhanced
#   version supports multiple keys with permission tiers while maintaining
#   full backward compatibility.
#
# PERMISSION HIERARCHY:
#   "read"  — can call GET endpoints only.
#   "write" — can call GET + POST/PUT/DELETE endpoints.
#   "admin" — full access including future admin endpoints.
#
# KEY LOADING STRATEGY:
#   1. If API_KEYS env var is set (JSON dict), parse it.
#   2. If only API_KEY is set (single string), treat it as an admin key.
#   3. Both can coexist — they are merged.
#
# SECURITY NOTE:
#   Keys are still stored in plaintext (in env vars, compared in memory).
#   A production system would hash keys with bcrypt/argon2 and store
#   hashes in a database. This is documented as a known limitation.
# ---------------------------------------------------------------------------

# Permission hierarchy: higher tiers include all lower permissions.
PERMISSION_HIERARCHY: dict[str, int] = {
    "read": 1,
    "write": 2,
    "admin": 3,
}


def _load_api_keys() -> dict[str, str]:
    """
    Load API keys and their permission tiers from configuration.

    Returns:
        Dict mapping API key strings to permission tier names.
        Example: {"abc123": "admin", "reader-key": "read"}
    """
    keys: dict[str, str] = {}

    # Strategy 1: Load multi-key JSON from API_KEYS env var.
    # Format: {"key1": "admin", "key2": "read", "key3": "write"}
    if settings.API_KEYS:
        try:
            parsed = json.loads(settings.API_KEYS)
            if isinstance(parsed, dict):
                for key, tier in parsed.items():
                    tier_lower = str(tier).lower()
                    if tier_lower in PERMISSION_HIERARCHY:
                        keys[key] = tier_lower
                    else:
                        logger.warning(
                            "Ignoring API key with unknown tier: %s", tier
                        )
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error("Failed to parse API_KEYS JSON: %s", exc)

    # Strategy 2: Backward compatibility — single API_KEY as admin.
    # This ensures existing deployments that only set API_KEY continue
    # to work without any configuration changes.
    if settings.API_KEY and settings.API_KEY not in keys:
        keys[settings.API_KEY] = "admin"

    return keys


# Load keys once at module import time (same pattern as settings singleton).
# This avoids re-parsing JSON on every request.
_API_KEYS: dict[str, str] = _load_api_keys()


def _has_permission(key_tier: str, required_tier: str) -> bool:
    """
    Check if a key's tier meets or exceeds the required tier.

    Uses the PERMISSION_HIERARCHY numeric levels:
      admin (3) >= write (2) >= read (1)

    Args:
        key_tier: The tier assigned to the API key (e.g. "read").
        required_tier: The minimum tier required by the endpoint.

    Returns:
        True if key_tier >= required_tier in the hierarchy.
    """
    key_level = PERMISSION_HIERARCHY.get(key_tier, 0)
    required_level = PERMISSION_HIERARCHY.get(required_tier, 0)
    return key_level >= required_level


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

    # If no keys are configured at all, accept any non-empty key.
    # This preserves the original coursework behaviour where key
    # validation was intentionally relaxed for simplified testing.
    if not _API_KEYS:
        return x_api_key

    if x_api_key not in _API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": "invalid_api_key",
                "message": "The provided API key is not valid",
            },
        )

    return x_api_key


def require_permission(required_tier: str):
    """
    Factory for permission-checking dependencies.

    Usage in routers:
        from app.core.deps import require_permission

        @router.post(
            "",
            dependencies=[Depends(require_permission("write"))],
        )
        async def create_city(...): ...

    This is a higher-order dependency: it returns a dependency function
    that checks whether the authenticated key's tier meets the required
    level. It must be used *after* verify_api_key in the dependency chain.

    Args:
        required_tier: Minimum permission tier ("read", "write", "admin").

    Returns:
        An async dependency function that raises 403 if the key lacks
        sufficient permissions.
    """

    async def _check_permission(
        x_api_key: Annotated[
            str,
            Header(
                description="API key for authentication. Include as X-Api-Key header.",
            ),
        ],
    ) -> str:
        """
        Verify the API key exists AND has sufficient permissions.

        Raises:
            HTTPException 401: Missing or invalid key.
            HTTPException 403: Valid key but insufficient permissions.
        """
        # First, validate the key exists (reuses verify_api_key logic).
        if not x_api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={
                    "error": "missing_api_key",
                    "message": "X-Api-Key header is required",
                },
            )

        # If no keys configured, accept any key (backward compat).
        if not _API_KEYS:
            return x_api_key

        if x_api_key not in _API_KEYS:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={
                    "error": "invalid_api_key",
                    "message": "The provided API key is not valid",
                },
            )

        # Key is valid — now check permissions.
        key_tier = _API_KEYS[x_api_key]
        if not _has_permission(key_tier, required_tier):
            logger.warning(
                "Permission denied | key_tier=%s required=%s path=<request>",
                key_tier,
                required_tier,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "error": "insufficient_permissions",
                    "message": (
                        f"This API key has '{key_tier}' permission but "
                        f"'{required_tier}' is required for this endpoint."
                    ),
                },
            )

        return x_api_key

    return _check_permission

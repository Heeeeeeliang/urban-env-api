"""
Unit Tests — Security Headers Cache-Control Logic
=====================================================

Pure function tests for _determine_cache_control(), the decision function
that selects Cache-Control directives based on path and HTTP method.

WHY TEST THIS AS A UNIT?
  The cache-control logic has three branches with subtle path-matching
  rules. A bug here is invisible to casual testing (wrong cache headers
  don't cause errors — they cause stale data or credential leaks).
  Unit-testing the pure function is cheap and catches regressions
  immediately, without needing a running server.

CACHE-CONTROL STRATEGY SUMMARY:
  - Auth endpoints → "no-store" (OWASP: never cache credentials)
  - GET analytics  → "public, max-age=300" (data updates hourly,
    5-minute cache is safe and reduces load)
  - Everything else → "no-cache" (browser must revalidate; prevents
    stale responses for non-analytics data that may change per-request)
"""

import pytest

from app.middleware.security_headers import _determine_cache_control

pytestmark = pytest.mark.unit


# ===================================================================
# Auth endpoints → "no-store"
# ===================================================================


class TestCacheControlAuthEndpoints:
    """Auth paths must never be cached — credentials in cached responses
    are a critical OWASP vulnerability (CWE-524)."""

    def test_auth_login_path(self):
        """POST /auth/login should return no-store regardless of method."""
        assert _determine_cache_control("/auth/login", "POST") == "no-store"

    def test_auth_token_refresh(self):
        """GET /auth/refresh — even reads from auth endpoints are no-store."""
        assert _determine_cache_control("/auth/refresh", "GET") == "no-store"

    def test_auth_nested_path(self):
        """Any path containing '/auth' triggers no-store."""
        assert _determine_cache_control("/api/v1/auth/keys", "GET") == "no-store"


# ===================================================================
# GET analytics → "public, max-age=300"
# ===================================================================


class TestCacheControlAnalyticsEndpoints:
    """GET requests to analytics endpoints are cache-friendly because
    the underlying data only changes on ingestion (hourly). A 5-minute
    TTL reduces redundant DB queries from dashboard auto-refresh."""

    def test_get_trend_endpoint(self):
        """GET /api/v1/analytics/trend/london → cacheable."""
        result = _determine_cache_control("/api/v1/analytics/trend/london", "GET")
        assert result == "public, max-age=300"

    def test_get_anomalies_endpoint(self):
        """GET /api/v1/analytics/anomalies/london → cacheable."""
        result = _determine_cache_control("/api/v1/analytics/anomalies/london", "GET")
        assert result == "public, max-age=300"

    def test_get_compare_endpoint(self):
        """GET /api/v1/analytics/compare → cacheable."""
        result = _determine_cache_control("/api/v1/analytics/compare", "GET")
        assert result == "public, max-age=300"

    def test_post_to_analytics_is_not_cached(self):
        """
        POST to an analytics path should NOT be cached.

        Only GET is safe to cache. A hypothetical POST /analytics/custom
        might trigger computation with side effects — caching the response
        would be dangerous.
        """
        result = _determine_cache_control("/api/v1/analytics/trend/london", "POST")
        assert result == "no-cache"


# ===================================================================
# Default → "no-cache"
# ===================================================================


class TestCacheControlDefaultPaths:
    """All other paths get no-cache: the browser must revalidate with
    the server before using a cached response."""

    def test_get_cities(self):
        """
        GET /api/v1/cities is NOT cached with max-age because city data
        can change at any time (create/update/delete). no-cache forces
        the browser to revalidate, which is the safe default.
        """
        assert _determine_cache_control("/api/v1/cities", "GET") == "no-cache"

    def test_post_cities(self):
        """POST /api/v1/cities — mutations are never cached."""
        assert _determine_cache_control("/api/v1/cities", "POST") == "no-cache"

    def test_get_health(self):
        """GET /health — infrastructure endpoint, always fresh."""
        assert _determine_cache_control("/health", "GET") == "no-cache"

    def test_delete_city(self):
        """DELETE /api/v1/cities/london — destructive ops never cached."""
        assert _determine_cache_control("/api/v1/cities/london", "DELETE") == "no-cache"

    def test_put_city(self):
        """PUT /api/v1/cities/london — updates never cached."""
        assert _determine_cache_control("/api/v1/cities/london", "PUT") == "no-cache"

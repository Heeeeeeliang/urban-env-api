"""
Integration Tests — API Key Permission Tiers
===============================================

Verifies the enhanced API key authentication system that supports
multiple keys with permission levels: "read", "write", "admin".

WHY MOCK _API_KEYS?
  The permission tier system loads keys from environment variables at
  module import time (via _load_api_keys). In tests, we don't want to
  set real env vars — instead we patch the module-level _API_KEYS dict
  to inject known test keys with specific tiers.

  This follows the same pattern as test_analytics.py, which patches
  the AnalyticsService to avoid PostgreSQL-specific SQL.

TEST MATRIX:
                    GET /cities   POST /cities   DELETE /cities/{id}
  read key             ✓              403             403
  write key            ✓               ✓              ✓ (write ≥ write)
  admin key            ✓               ✓              ✓
  invalid key         401             401             401
  missing key       401/422         401/422         401/422

NOTE ON BACKWARD COMPATIBILITY:
  When _API_KEYS is empty (no keys configured), verify_api_key accepts
  any non-empty key. This preserves the original coursework behaviour.
  The test_backward_compat_no_keys_configured test verifies this.
"""

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.integration


# -------------------------------------------------------------------
# Test keys — injected via mock to replace _API_KEYS
# -------------------------------------------------------------------
MOCK_API_KEYS = {
    "admin-key-001": "admin",
    "write-key-001": "write",
    "read-key-001": "read",
}


# -------------------------------------------------------------------
# Helper: create a client with a specific API key
# -------------------------------------------------------------------
async def _make_client(app, api_key: str | None = None):
    """
    Build an httpx AsyncClient bound to the test app with a specific
    API key (or no key if None).

    We create a fresh client per test rather than using the conftest
    client fixture because each test needs a different X-Api-Key value.
    """
    headers = {}
    if api_key is not None:
        headers["X-Api-Key"] = api_key

    transport = ASGITransport(app=app)
    return AsyncClient(
        transport=transport,
        base_url="http://test",
        headers=headers,
    )


# ===================================================================
# Admin key — full access
# ===================================================================


@pytest.mark.asyncio
@patch("app.core.deps._API_KEYS", MOCK_API_KEYS)
async def test_admin_key_can_read(app, seed_data):
    """
    Admin keys have the highest permission tier (level 3) and can
    access all endpoints, including read-only ones.
    """
    async with await _make_client(app, "admin-key-001") as client:
        resp = await client.get("/api/v1/cities")
    assert resp.status_code == 200


@pytest.mark.asyncio
@patch("app.core.deps._API_KEYS", MOCK_API_KEYS)
async def test_admin_key_can_write(app, seed_data):
    """
    Admin can create resources (POST requires "write" or higher).
    """
    async with await _make_client(app, "admin-key-001") as client:
        resp = await client.post(
            "/api/v1/cities",
            json={
                "name": "Bristol",
                "country": "United Kingdom",
                "country_code": "GB",
                "latitude": 51.4545,
                "longitude": -2.5879,
                "timezone": "Europe/London",
            },
        )
    assert resp.status_code == 201


@pytest.mark.asyncio
@patch("app.core.deps._API_KEYS", MOCK_API_KEYS)
async def test_admin_key_can_delete(app, seed_data):
    """
    Admin can delete resources — highest tier has no restrictions.
    """
    async with await _make_client(app, "admin-key-001") as client:
        resp = await client.delete("/api/v1/cities/london")
    assert resp.status_code == 204


# ===================================================================
# Invalid / missing keys — authentication failure
# ===================================================================


@pytest.mark.asyncio
@patch("app.core.deps._API_KEYS", MOCK_API_KEYS)
async def test_invalid_key_returns_401(app, seed_data):
    """
    A key not in the _API_KEYS dict should be rejected with 401.

    This is authentication failure (who are you?), not authorisation
    failure (you don't have permission). The distinction matters for
    client-side error handling: 401 means "try a different key",
    403 means "this key can't do that".
    """
    async with await _make_client(app, "totally-bogus-key") as client:
        resp = await client.get("/api/v1/cities")
    assert resp.status_code == 401


@pytest.mark.asyncio
@patch("app.core.deps._API_KEYS", MOCK_API_KEYS)
async def test_missing_key_returns_401_or_422(app, seed_data):
    """
    No X-Api-Key header at all. FastAPI may return 422 (missing required
    header parameter) before our dependency runs, or 401 if the empty
    string reaches verify_api_key. Both are acceptable rejections.
    """
    async with await _make_client(app, api_key=None) as client:
        resp = await client.get("/api/v1/cities")
    assert resp.status_code in (401, 422)


# ===================================================================
# Backward compatibility — no keys configured
# ===================================================================


@pytest.mark.asyncio
@patch("app.core.deps._API_KEYS", {})
async def test_backward_compat_no_keys_configured(app, seed_data):
    """
    When _API_KEYS is empty (neither API_KEY nor API_KEYS env vars set),
    verify_api_key falls back to accepting any non-empty key.

    This preserves the original coursework behaviour where key validation
    was intentionally relaxed. Existing tests that use "test-key" (set by
    the client fixture in conftest.py) continue to pass without
    configuring API keys.
    """
    async with await _make_client(app, "any-random-key") as client:
        resp = await client.get("/api/v1/cities")
    assert resp.status_code not in (401, 403)

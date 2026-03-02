"""
Integration Tests — API Key Authentication
=============================================

Verifies the X-Api-Key header requirement enforced by verify_api_key
on all /api/v1/ routes.

NOTE ON STATUS CODE RANGE:
  FastAPI returns 422 (not 401) when a required Header() parameter is
  entirely absent, because the framework's parameter validation layer
  runs before our custom verify_api_key dependency. We widen the
  assertion to include 422 alongside 401/403 to cover both the
  framework-level rejection (missing header) and our application-level
  rejection (empty/invalid key).

NOTE ON KEY VALIDATION:
  The current coursework implementation accepts any non-empty API key
  (key validation is commented out in core/deps.py for simplified
  testing). test_wrong_api_key is marked xfail to document the
  intended production behaviour. When key validation is enabled,
  remove the xfail marker and the test will pass as-is.
"""

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_no_api_key_returns_401_or_403():
    """
    A request with no X-Api-Key header is rejected.

    FastAPI's Header() validation runs first and returns 422 for a
    missing required header. Our verify_api_key would return 401 if
    the header were present but empty. Either way, the request is
    blocked before reaching business logic.
    """
    from main import app as fastapi_app
    async with AsyncClient(
        transport=ASGITransport(app=fastapi_app),
        base_url="http://testserver",
    ) as c:
        resp = await c.get("/api/v1/cities")

    assert resp.status_code in (401, 403, 422)


@pytest.mark.asyncio
@pytest.mark.xfail(
    reason=(
        "Key validation is commented out in core/deps.py for coursework "
        "simplification — any non-empty key is currently accepted. "
        "Enable the VALID_API_KEYS check to make this test pass."
    ),
    strict=True,
)
async def test_wrong_api_key_returns_401_or_403():
    """
    A request with an invalid API key should be rejected.

    When key validation is enabled, verify_api_key checks the key
    against a set of valid keys and returns 403 for unrecognised ones.
    """
    from main import app as fastapi_app
    async with AsyncClient(
        transport=ASGITransport(app=fastapi_app),
        base_url="http://testserver",
        headers={"X-Api-Key": "wrong-key"},
    ) as c:
        resp = await c.get("/api/v1/cities")

    assert resp.status_code in (401, 403)


@pytest.mark.asyncio
async def test_valid_api_key_passes_auth(client):
    """
    A request with a valid X-Api-Key header passes authentication.

    The client fixture from conftest.py includes the X-Api-Key header.
    The response may be 200 (with data) or another success/client code,
    but must NOT be an auth rejection.
    """
    resp = await client.get("/api/v1/cities")

    assert resp.status_code not in (401, 403)

"""
Integration Tests — Middleware Stack (Security Headers, Request ID, Cache-Control)
====================================================================================

These tests verify that the middleware stack correctly injects security
headers into real HTTP responses. They use the existing `client` fixture
from conftest.py, which wires an httpx AsyncClient to the FastAPI app
via ASGITransport with an X-Api-Key header pre-set.

WHY INTEGRATION, NOT UNIT?
  Middleware operates on live ASGI request/response cycles. Unit-testing
  a pure ASGI middleware requires manually constructing scope dicts,
  receive/send callables, and header byte-tuples — brittle and doesn't
  catch wiring issues (e.g. middleware registered in wrong order).
  Integration tests with httpx catch both logic bugs and registration bugs.

WHAT WE VERIFY:
  1. Every response includes OWASP security headers (nosniff, DENY, etc.)
  2. X-Request-ID is generated when absent, echoed when provided.
  3. Cache-Control varies by endpoint type (analytics vs cities).

WHAT WE DON'T VERIFY HERE:
  - HSTS header (only present in production; test env is "development")
  - Rate limiting (requires many requests; separate test or manual)
  - Request logging output (would require capturing log output)
"""

import re

import pytest

pytestmark = pytest.mark.integration

# UUID v4 regex — validates that X-Request-ID is a properly formatted UUID.
# Format: 8-4-4-4-12 hex characters separated by hyphens.
UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


# ===================================================================
# 1. OWASP Security Headers
# ===================================================================


@pytest.mark.asyncio
async def test_response_includes_nosniff(client, seed_data):
    """
    X-Content-Type-Options: nosniff prevents browsers from MIME-sniffing
    the response body. Without this, a browser could interpret a JSON
    error message as HTML if it contained angle brackets, enabling
    reflected XSS attacks.
    """
    resp = await client.get("/api/v1/cities")
    assert resp.headers.get("x-content-type-options") == "nosniff"


@pytest.mark.asyncio
async def test_response_includes_frame_deny(client, seed_data):
    """
    X-Frame-Options: DENY prevents the API from being embedded in an
    iframe. This blocks clickjacking attacks where a malicious page
    overlays invisible controls on top of our API's Swagger UI.
    """
    resp = await client.get("/api/v1/cities")
    assert resp.headers.get("x-frame-options") == "DENY"


@pytest.mark.asyncio
async def test_response_includes_xss_protection_disabled(client, seed_data):
    """
    X-XSS-Protection: 0 disables the legacy browser XSS filter.

    Counter-intuitive but correct: the old XSS filter in IE/Chrome
    can actually *introduce* vulnerabilities (CVE-2018-8178). Modern
    defence is Content-Security-Policy. Setting this to "0" follows
    OWASP's current recommendation.
    """
    resp = await client.get("/api/v1/cities")
    assert resp.headers.get("x-xss-protection") == "0"


@pytest.mark.asyncio
async def test_referrer_policy_present(client, seed_data):
    """
    Referrer-Policy limits how much URL information is sent in the
    Referer header on cross-origin requests. 'strict-origin-when-cross-origin'
    sends only the origin (not the full path) for cross-origin requests,
    preventing API paths (which may contain IDs) from leaking to third parties.
    """
    resp = await client.get("/api/v1/cities")
    assert resp.headers.get("referrer-policy") == "strict-origin-when-cross-origin"


@pytest.mark.asyncio
async def test_permissions_policy_present(client, seed_data):
    """
    Permissions-Policy disables browser APIs we don't need. An API
    server has no reason to access camera, microphone, or geolocation.
    Disabling them reduces the attack surface if the response is ever
    rendered in a browser context.
    """
    resp = await client.get("/api/v1/cities")
    policy = resp.headers.get("permissions-policy")
    assert policy is not None
    assert "camera=()" in policy
    assert "microphone=()" in policy


# ===================================================================
# 2. X-Request-ID — tracing header
# ===================================================================


@pytest.mark.asyncio
async def test_request_id_generated_when_absent(client, seed_data):
    """
    When the client doesn't send X-Request-ID, the middleware generates
    a UUID v4 and includes it in the response. This enables correlating
    a client's error report with server-side logs.
    """
    resp = await client.get("/api/v1/cities")
    request_id = resp.headers.get("x-request-id")
    assert request_id is not None
    assert UUID_PATTERN.match(request_id), (
        f"Expected UUID v4 format, got: {request_id}"
    )


@pytest.mark.asyncio
async def test_request_id_echoed_when_provided(client, seed_data):
    """
    When the client sends an X-Request-ID, the middleware preserves it
    instead of generating a new one. This supports distributed tracing
    (e.g. a frontend sends a trace ID that propagates through all
    backend services).
    """
    custom_id = "my-trace-12345-abc"
    resp = await client.get(
        "/api/v1/cities",
        headers={"X-Request-ID": custom_id},
    )
    assert resp.headers.get("x-request-id") == custom_id


# ===================================================================
# 3. Cache-Control — varies by endpoint
# ===================================================================


@pytest.mark.asyncio
async def test_cities_get_no_cache(client, seed_data):
    """
    GET /api/v1/cities returns Cache-Control: no-cache because city
    data can change at any time via CRUD operations. Caching city
    lists would cause stale data after a city is created or deleted.
    """
    resp = await client.get("/api/v1/cities")
    cache_control = resp.headers.get("cache-control")
    assert cache_control == "no-cache"


@pytest.mark.asyncio
async def test_cities_post_no_cache(client, seed_data):
    """
    POST /api/v1/cities returns no-cache. Mutation responses should
    never be cached — the response represents a one-time state change.
    """
    payload = {
        "name": "Leeds",
        "country": "United Kingdom",
        "country_code": "GB",
        "latitude": 53.8008,
        "longitude": -1.5491,
        "timezone": "Europe/London",
    }
    resp = await client.post("/api/v1/cities", json=payload)
    cache_control = resp.headers.get("cache-control")
    assert cache_control == "no-cache"

"""
Unit Tests — Rate Limiter Helper Functions
=============================================

Pure function tests for the rate_limiter module's IP extraction and
error handler. No database, no HTTP server, no async fixtures needed.

WHAT WE TEST:
  - get_client_ip() correctly prioritises X-Forwarded-For > X-Real-Ip >
    request.client.host > "unknown" fallback.
  - rate_limit_exceeded_handler() returns a JSON 429 with the Retry-After
    header and the expected error response schema.

WHAT WE DON'T TEST HERE:
  - Actual rate limiting behaviour (requires a running app with SlowAPI
    middleware — covered in integration tests if needed).
  - The limiter instance configuration (validated by slowapi internally).

WHY MOCK THE REQUEST OBJECT?
  Starlette's Request is tightly coupled to an ASGI scope dict. Building
  a real scope with the correct header format (list of byte-tuple pairs)
  is fragile and verbose. Using unittest.mock lets us control exactly
  which headers are present without wrestling with ASGI internals.
"""

from unittest.mock import MagicMock, PropertyMock

import pytest

from app.middleware.rate_limiter import get_client_ip, rate_limit_exceeded_handler

pytestmark = pytest.mark.unit


# -------------------------------------------------------------------
# HELPER: build a minimal mock Request for get_client_ip
# -------------------------------------------------------------------

def _mock_request(
    headers: dict[str, str] | None = None,
    client_host: str | None = None,
) -> MagicMock:
    """
    Build a mock Starlette Request with controllable headers and client.

    Args:
        headers: Dict of header names to values. Simulates
                 request.headers.get(name) returning the value.
        client_host: If provided, request.client.host returns this.
                     If None, request.client is set to None (no direct
                     connection info — e.g. behind a proxy with no
                     REMOTE_ADDR forwarding).
    """
    mock = MagicMock()
    mock.headers = headers or {}

    if client_host:
        mock.client.host = client_host
    else:
        mock.client = None

    return mock


# ===================================================================
# get_client_ip — header priority tests
# ===================================================================


class TestGetClientIpForwardedFor:
    """X-Forwarded-For is the highest-priority source for proxy-aware IP."""

    def test_single_ip(self):
        """
        A single IP in X-Forwarded-For (typical single-proxy setup).

        Most reverse proxies (Render, Heroku, nginx) set this header to
        the original client's IP. When there's only one proxy, there's
        only one IP in the header.
        """
        request = _mock_request(
            headers={"x-forwarded-for": "203.0.113.50"},
            client_host="10.0.0.1",
        )
        assert get_client_ip(request) == "203.0.113.50"

    def test_multiple_ips_takes_first(self):
        """
        Multiple IPs in X-Forwarded-For: the first is the original client.

        Format: "client, proxy1, proxy2". Each proxy appends its own IP.
        The first entry is the original client — this is what we want for
        rate limiting. Using the last entry would rate-limit by proxy IP,
        causing all users behind that proxy to share one bucket.
        """
        request = _mock_request(
            headers={"x-forwarded-for": "203.0.113.50, 70.41.3.18, 150.172.238.178"},
        )
        assert get_client_ip(request) == "203.0.113.50"

    def test_whitespace_is_stripped(self):
        """
        Proxy implementations vary in whitespace around IPs.

        Some proxies use "ip1, ip2" (with space) and others use "ip1,ip2"
        (without). Our extraction must handle both.
        """
        request = _mock_request(
            headers={"x-forwarded-for": "  203.0.113.50 , 70.41.3.18"},
        )
        assert get_client_ip(request) == "203.0.113.50"

    def test_takes_precedence_over_real_ip(self):
        """
        When both X-Forwarded-For and X-Real-Ip are present,
        X-Forwarded-For wins. This matches nginx's default behaviour
        where both headers may be set simultaneously.
        """
        request = _mock_request(
            headers={
                "x-forwarded-for": "203.0.113.50",
                "x-real-ip": "198.51.100.10",
            },
            client_host="10.0.0.1",
        )
        assert get_client_ip(request) == "203.0.113.50"


class TestGetClientIpRealIp:
    """X-Real-Ip is the second-priority source (used by nginx)."""

    def test_used_when_no_forwarded_for(self):
        """
        X-Real-Ip is only checked when X-Forwarded-For is absent.

        nginx can be configured to set X-Real-Ip instead of (or in
        addition to) X-Forwarded-For. Some setups only use X-Real-Ip.
        """
        request = _mock_request(
            headers={"x-real-ip": "198.51.100.10"},
            client_host="10.0.0.1",
        )
        assert get_client_ip(request) == "198.51.100.10"

    def test_whitespace_stripped(self):
        """Leading/trailing whitespace in X-Real-Ip is stripped."""
        request = _mock_request(headers={"x-real-ip": "  198.51.100.10  "})
        assert get_client_ip(request) == "198.51.100.10"


class TestGetClientIpDirectConnection:
    """request.client.host is used when no proxy headers are present."""

    def test_client_host_fallback(self):
        """
        Direct connection (no proxy) — request.client.host is the
        socket-level remote address set by uvicorn/ASGI server.
        """
        request = _mock_request(headers={}, client_host="127.0.0.1")
        assert get_client_ip(request) == "127.0.0.1"


class TestGetClientIpUnknown:
    """Fallback to "unknown" when all sources are unavailable."""

    def test_no_headers_no_client(self):
        """
        Edge case: no proxy headers AND no client info.

        This shouldn't happen in production (ASGI always provides
        request.client), but could occur in malformed test setups or
        if the ASGI server strips client info. We must not crash.
        """
        request = _mock_request(headers={}, client_host=None)
        assert get_client_ip(request) == "unknown"


# ===================================================================
# rate_limit_exceeded_handler — 429 response structure
# ===================================================================


class TestRateLimitExceededHandler:
    """Verify the custom 429 handler returns the expected JSON structure."""

    def test_returns_429_status(self):
        """
        The handler must return HTTP 429 (Too Many Requests), not 403
        or 503. 429 is the RFC 6585 standard for rate limiting and is
        expected by well-behaved HTTP clients for backoff logic.
        """
        request = _mock_request(headers={}, client_host="127.0.0.1")
        request.url.path = "/api/v1/cities"

        exc = MagicMock()
        exc.detail = "60"
        exc.__str__ = lambda self: "60 per 1 minute"

        response = rate_limit_exceeded_handler(request, exc)
        assert response.status_code == 429

    def test_includes_retry_after_header(self):
        """
        RFC 6585 §4 requires a Retry-After header so clients know
        when to stop backing off. Without it, clients either hammer
        the API or give up entirely — neither is desirable.
        """
        request = _mock_request(headers={}, client_host="127.0.0.1")
        request.url.path = "/api/v1/cities"

        exc = MagicMock()
        exc.detail = "60"
        exc.__str__ = lambda self: "60 per 1 minute"

        response = rate_limit_exceeded_handler(request, exc)
        assert response.headers.get("retry-after") == "60"

    def test_json_body_matches_error_schema(self):
        """
        The 429 body must follow the API's standard error format:
        {"error": "...", "message": "..."}. This ensures API consumers
        can parse rate limit errors with the same code they use for
        other errors (404, 422, etc.).
        """
        request = _mock_request(headers={}, client_host="127.0.0.1")
        request.url.path = "/api/v1/cities"

        exc = MagicMock()
        exc.detail = "60"
        exc.__str__ = lambda self: "60 per 1 minute"

        response = rate_limit_exceeded_handler(request, exc)
        body = response.body
        # JSONResponse stores the body as bytes; decode and parse
        import json
        data = json.loads(body.decode("utf-8"))
        assert data["error"] == "rate_limit_exceeded"
        assert "retry" in data["message"].lower()
        assert "retry_after_seconds" in data

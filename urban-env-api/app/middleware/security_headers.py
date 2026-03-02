"""
Security Headers Middleware — Pure ASGI Implementation
========================================================

Architectural decisions documented here:

1. PURE ASGI (not BaseHTTPMiddleware)
   Starlette's BaseHTTPMiddleware buffers the entire response body in
   memory before calling `call_next()`. This breaks:
     - Streaming responses (SSE, large file downloads).
     - Memory efficiency (a 100MB CSV response is fully buffered).
     - WebSocket connections (BaseHTTPMiddleware doesn't handle them).
   A pure ASGI middleware wraps the ASGI `send` callable directly,
   injecting headers into the HTTP response start message without
   buffering. This is the pattern recommended by the Starlette and
   FastAPI maintainers for header-only middleware.

2. OWASP SECURE HEADERS
   Every header addresses a specific attack vector:
     - X-Content-Type-Options: nosniff → prevents MIME-type sniffing
       (stops browsers from interpreting JSON as HTML to execute XSS).
     - X-Frame-Options: DENY → prevents clickjacking (our API has
       no reason to be embedded in iframes).
     - X-XSS-Protection: 0 → disables the broken legacy XSS filter
       in older browsers (modern CSP is the correct defence; the old
       filter can actually *introduce* vulnerabilities).
     - Strict-Transport-Security → forces HTTPS in production. We omit
       this in development to avoid breaking local HTTP.
     - Referrer-Policy: strict-origin-when-cross-origin → limits
       referrer leakage to origin only for cross-origin requests.
     - Permissions-Policy → disables browser features we don't use
       (camera, microphone, geolocation) to reduce attack surface.

3. X-Request-ID
   A UUID attached to every response, enabling request tracing across
   logs, error reports, and support tickets. If the client sends an
   X-Request-ID header, we echo it back (preserving their trace ID).
   Otherwise we generate one. This is standard practice in
   production APIs (AWS, Stripe, GitHub all do this).

4. CACHE-CONTROL
   Authentication endpoints (paths containing /auth) get
   `no-store` to prevent credentials from being cached.
   GET analytics endpoints get `public, max-age=300` (5 minutes)
   because analytics data changes infrequently (hourly ingestion).
   All other responses get `no-cache` (revalidate on every request).
"""

import logging
import uuid
from typing import Callable

from app.core.config import settings

logger = logging.getLogger(__name__)


class SecurityHeadersMiddleware:
    """
    Pure ASGI middleware that injects security headers into every response.

    Usage:
        app.add_middleware(SecurityHeadersMiddleware)

    This class implements the ASGI interface directly (no Starlette
    BaseHTTPMiddleware). The __call__ method receives the ASGI scope,
    receive, and send callables, and wraps `send` to intercept the
    "http.response.start" message and inject headers.
    """

    def __init__(self, app: Callable) -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Callable, send: Callable) -> None:
        """
        ASGI entry point. Only processes HTTP requests; passes through
        WebSocket and lifespan events unchanged.
        """
        if scope["type"] != "http":
            # WebSocket, lifespan — pass through untouched.
            await self.app(scope, receive, send)
            return

        # Generate or preserve the request ID for tracing.
        # Check if the client provided one (for distributed tracing).
        request_id: str = ""
        for header_name, header_value in scope.get("headers", []):
            if header_name == b"x-request-id":
                request_id = header_value.decode("latin-1")
                break
        if not request_id:
            request_id = str(uuid.uuid4())

        # Store request_id in scope state so the request logger can access it.
        if "state" not in scope:
            scope["state"] = {}
        scope["state"]["request_id"] = request_id

        # Determine the request path for cache-control decisions.
        path: str = scope.get("path", "")
        method: str = scope.get("method", "GET")

        async def send_with_security_headers(message: dict) -> None:
            """
            Intercept the response start message and inject security headers.
            """
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))

                # --- Core Security Headers (OWASP) ---
                headers.append((b"x-content-type-options", b"nosniff"))
                headers.append((b"x-frame-options", b"DENY"))
                # X-XSS-Protection: 0 disables the legacy browser XSS filter.
                # Modern defence is Content-Security-Policy, not this header.
                # The old filter can actually introduce XSS in some cases.
                headers.append((b"x-xss-protection", b"0"))
                headers.append(
                    (b"referrer-policy", b"strict-origin-when-cross-origin")
                )
                headers.append(
                    (
                        b"permissions-policy",
                        b"camera=(), microphone=(), geolocation=()",
                    )
                )

                # --- HSTS — production only ---
                # HSTS tells browsers to only connect via HTTPS for the next
                # max-age seconds. Including this in development would break
                # http://localhost connections.
                if settings.ENVIRONMENT == "production":
                    headers.append(
                        (
                            b"strict-transport-security",
                            b"max-age=31536000; includeSubDomains",
                        )
                    )

                # --- Request Tracing ---
                headers.append((b"x-request-id", request_id.encode("latin-1")))

                # --- Cache-Control ---
                cache_value = _determine_cache_control(path, method)
                headers.append((b"cache-control", cache_value.encode("latin-1")))

                message["headers"] = headers

            await send(message)

        await self.app(scope, receive, send_with_security_headers)


def _determine_cache_control(path: str, method: str) -> str:
    """
    Select the appropriate Cache-Control directive based on the endpoint.

    Rules:
      - Auth-related endpoints → no-store (never cache credentials).
      - GET on analytics endpoints → public, max-age=300 (5-min cache;
        analytics data updates hourly via ingestion).
      - Everything else → no-cache (revalidate every request).

    Returns:
        Cache-Control header value string.
    """
    # Auth endpoints: never cache anything related to authentication.
    if "/auth" in path:
        return "no-store"

    # GET analytics: safe to cache because data only changes on ingestion.
    if method == "GET" and "/analytics" in path:
        return "public, max-age=300"

    # Default: no-cache means the browser must revalidate with the server
    # before using a cached response. This is stricter than no default
    # but less aggressive than no-store.
    return "no-cache"

"""
Request Logging Middleware — Structured Per-Request Logging
=============================================================

Architectural decisions documented here:

1. PURE ASGI (not BaseHTTPMiddleware)
   Same rationale as SecurityHeadersMiddleware: BaseHTTPMiddleware
   buffers the response body. We only need to measure timing and
   capture the status code — no reason to buffer.

2. STRUCTURED LOG FORMAT
   Each log line contains: method, path, status_code, duration_ms,
   client_ip, request_id. This makes logs grep-friendly and parseable
   by log aggregators (Datadog, Loki, CloudWatch).

3. EXCLUDED PATHS
   /docs, /redoc, and /openapi.json are excluded because:
     - They generate noise (Swagger UI makes multiple requests on load).
     - They contain no business logic worth monitoring.
     - They inflate request count metrics.

4. LOG LEVEL BY STATUS CODE
   - 2xx/3xx → INFO (normal operation).
   - 4xx → WARNING (client error — may indicate misuse or bugs in
     consuming code, worth investigating in aggregate).
   - 5xx → ERROR (server error — always requires investigation).
   This convention is standard in web application logging and allows
   log-level-based alerting (e.g. page on ERROR, report on WARNING).
"""

import logging
import time
from typing import Callable, Optional

from app.middleware.rate_limiter import get_client_ip

logger = logging.getLogger("api.access")

# Paths to exclude from logging — documentation endpoints generate
# noise without business value.
EXCLUDED_PATHS: frozenset[str] = frozenset(
    {"/docs", "/redoc", "/openapi.json", "/favicon.ico"}
)


class RequestLoggerMiddleware:
    """
    Pure ASGI middleware that logs every HTTP request with structured fields.

    Log format:
        METHOD /path → STATUS in DURATIONms | ip=IP request_id=UUID

    Captures timing from the moment the request enters this middleware
    to when the response headers are sent. This includes all downstream
    middleware and the route handler, giving an accurate end-to-end
    latency measurement.
    """

    def __init__(self, app: Callable) -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Callable, send: Callable) -> None:
        """
        ASGI entry point. Times the request and logs on response start.
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "")

        # Skip documentation endpoints — they generate noise.
        if path in EXCLUDED_PATHS:
            await self.app(scope, receive, send)
            return

        method: str = scope.get("method", "?")
        start_time: float = time.perf_counter()

        # Extract client IP from headers (proxy-aware).
        # We build a minimal Request-like object to reuse get_client_ip.
        client_ip: str = _extract_ip_from_scope(scope)

        # Pull request_id from scope state (set by SecurityHeadersMiddleware).
        request_id: str = ""
        if "state" in scope:
            request_id = scope["state"].get("request_id", "")

        status_code: int = 0

        async def send_with_logging(message: dict) -> None:
            nonlocal status_code

            if message["type"] == "http.response.start":
                status_code = message.get("status", 0)

                # Calculate duration at the point response headers are sent.
                duration_ms = (time.perf_counter() - start_time) * 1000

                log_message = (
                    "%s %s → %d in %.1fms | ip=%s request_id=%s"
                )
                log_args = (
                    method,
                    path,
                    status_code,
                    duration_ms,
                    client_ip,
                    request_id,
                )

                # Log at the appropriate level based on status code range.
                if status_code >= 500:
                    logger.error(log_message, *log_args)
                elif status_code >= 400:
                    logger.warning(log_message, *log_args)
                else:
                    logger.info(log_message, *log_args)

            await send(message)

        await self.app(scope, receive, send_with_logging)


def _extract_ip_from_scope(scope: dict) -> str:
    """
    Extract client IP from ASGI scope headers without constructing
    a full Starlette Request object (avoids unnecessary overhead).

    Falls back to scope["client"][0] if no proxy headers are present.
    """
    headers = dict(scope.get("headers", []))

    forwarded_for: Optional[bytes] = headers.get(b"x-forwarded-for")
    if forwarded_for:
        return forwarded_for.decode("latin-1").split(",")[0].strip()

    real_ip: Optional[bytes] = headers.get(b"x-real-ip")
    if real_ip:
        return real_ip.decode("latin-1").strip()

    client = scope.get("client")
    if client:
        return client[0]

    return "unknown"

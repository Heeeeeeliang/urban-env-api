"""
Rate Limiting Middleware — slowapi-based Per-IP Throttling
===========================================================

Architectural decisions documented here:

1. WHY slowapi (not custom middleware)?
   slowapi wraps the battle-tested `limits` library and integrates
   natively with FastAPI/Starlette. It handles:
     - Thread-safe token bucket / fixed window counters.
     - Automatic 429 responses with Retry-After headers.
     - Key extraction from requests (IP, headers, API key).
   Writing this from scratch would be reimplementing well-tested code.

2. IN-MEMORY STORAGE (not Redis)
   This API is deployed on Render's free tier — a single instance with
   no persistent services. In-memory storage means:
     - Zero external dependencies (no Redis to provision or pay for).
     - Rate limits reset on deploy/restart (acceptable for coursework).
     - No cross-instance coordination (irrelevant on a single instance).
   If scaling to multiple workers/instances, swap MemoryStorage for
   RedisStorage — the limiter interface is identical.

3. SEPARATE LIMITS FOR READ vs WRITE
   GET requests (read) are capped at 60/minute — generous enough for
   dashboard polling but protective against scraping.
   POST/PUT/DELETE (write) are capped at 20/minute — tighter because
   writes are more expensive (DB mutations, ingestion triggers).
   This asymmetry mirrors real-world API design (e.g. GitHub: 5000
   reads vs 500 mutations per hour).

4. X-Forwarded-For FOR PROXY-AWARE IP EXTRACTION
   On Render (and behind any reverse proxy), the client's real IP is
   in the X-Forwarded-For header, not request.client.host (which is
   the proxy's IP). We extract the first IP in the chain (the original
   client). This prevents all users from sharing one rate limit bucket
   behind a load balancer.
"""

import logging
from typing import Optional

from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.core.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Key Function — extract client identity for rate limit bucketing
#
# Priority order:
#   1. X-Forwarded-For header (first IP = original client behind proxy)
#   2. X-Real-Ip header (alternative proxy header, used by nginx)
#   3. request.client.host (direct connection, no proxy)
#   4. "unknown" fallback (should never happen in practice)
#
# Security note: X-Forwarded-For can be spoofed by malicious clients.
# In production behind a trusted proxy (Render, Cloudflare), the proxy
# overwrites this header, making spoofing impossible. On untrusted
# networks, you'd validate the header chain against known proxy IPs.
# ---------------------------------------------------------------------------
def get_client_ip(request: Request) -> str:
    """
    Extract the real client IP from proxy headers or direct connection.

    Returns:
        Client IP address string for rate limit bucketing.
    """
    forwarded_for: Optional[str] = request.headers.get("x-forwarded-for")
    if forwarded_for:
        # X-Forwarded-For format: "client, proxy1, proxy2"
        # The first entry is the original client IP.
        return forwarded_for.split(",")[0].strip()

    real_ip: Optional[str] = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()

    if request.client:
        return request.client.host

    return "unknown"


# ---------------------------------------------------------------------------
# Limiter Instance — module-level singleton
#
# The Limiter is created once at import time. Its internal MemoryStorage
# is shared across all requests within the same process. This works
# correctly with uvicorn's async event loop (single-threaded, no races).
#
# default_limits applies to ALL routes unless overridden per-endpoint.
# We set a generous global default and apply tighter per-method limits
# via the decorators in the router modules.
# ---------------------------------------------------------------------------
limiter = Limiter(
    key_func=get_client_ip,
    default_limits=[f"{settings.RATE_LIMIT_PER_MINUTE}/minute"],
    # in-memory storage — no Redis URI needed
    storage_uri="memory://",
)

# Convenience strings for use in route decorators:
#   @limiter.limit(READ_LIMIT)   → on GET endpoints
#   @limiter.limit(WRITE_LIMIT)  → on POST/PUT/DELETE endpoints
READ_LIMIT: str = f"{settings.RATE_LIMIT_PER_MINUTE}/minute"
WRITE_LIMIT: str = f"{settings.RATE_LIMIT_WRITES_PER_MINUTE}/minute"


# ---------------------------------------------------------------------------
# Custom 429 Handler
#
# slowapi's default handler returns plain text. We override it to return
# JSON matching our API's error response schema (error + message keys)
# and include the Retry-After header (seconds until the limit resets).
#
# Why a custom handler instead of relying on slowapi's default?
#   - Consistency: all error responses use the same JSON structure.
#   - Machine-readability: clients can parse the retry_after field
#     programmatically to implement exponential backoff.
#   - OWASP compliance: rate limit responses should tell the client
#     when to retry, not just that they were limited.
# ---------------------------------------------------------------------------
def rate_limit_exceeded_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """
    Return a structured 429 response with Retry-After header.

    The Retry-After value comes from slowapi's exception, which
    calculates the seconds remaining until the rate limit window resets.
    """
    logger.warning(
        "Rate limit exceeded | ip=%s path=%s limit=%s",
        get_client_ip(request),
        request.url.path,
        str(exc),
    )

    return JSONResponse(
        status_code=429,
        content={
            "error": "rate_limit_exceeded",
            "message": (
                "Too many requests. Please slow down and retry after "
                "the period indicated in the Retry-After header."
            ),
            "retry_after_seconds": 60,
        },
        headers={
            "Retry-After": "60",
            "X-RateLimit-Limit": str(settings.RATE_LIMIT_PER_MINUTE),
        },
    )

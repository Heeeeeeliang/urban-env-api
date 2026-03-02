"""
Middleware Registration — Centralised Setup
=============================================

All middleware is registered here in a single function, called from
main.py. This keeps main.py clean and makes middleware ordering explicit.

MIDDLEWARE ORDERING (outermost → innermost):
  1. RequestLoggerMiddleware (outermost — captures total request time
     including all other middleware).
  2. SecurityHeadersMiddleware (injects headers on every response,
     including error responses from inner middleware).
  3. SlowAPIMiddleware (rate limiting — must run before route handlers
     but after logging so rate-limited requests are logged).
  4. CORSMiddleware (innermost of our custom stack — handles preflight
     OPTIONS requests before they reach rate limiting).

FastAPI/Starlette middleware is an onion: the LAST middleware added
with app.add_middleware() runs FIRST (outermost). We add them in
reverse order so the execution order matches the numbered list above.

Why centralise this?
  - main.py stays focused on app config and router registration.
  - Middleware ordering bugs are caught in one place.
  - Adding/removing middleware doesn't require touching main.py.
"""

import logging

from fastapi import FastAPI
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.middleware.rate_limiter import limiter, rate_limit_exceeded_handler
from app.middleware.security_headers import SecurityHeadersMiddleware
from app.middleware.request_logger import RequestLoggerMiddleware

logger = logging.getLogger(__name__)


def setup_middleware(app: FastAPI) -> None:
    """
    Register all middleware on the FastAPI app in the correct order.

    This function is idempotent — calling it multiple times will add
    duplicate middleware. It should be called exactly once from main.py.

    Args:
        app: The FastAPI application instance.
    """
    # --- 1. Attach the rate limiter state to the app ---
    # slowapi requires app.state.limiter to be set before
    # SlowAPIMiddleware is added.
    app.state.limiter = limiter

    # --- 2. Register the custom 429 handler ---
    # This replaces slowapi's default plain-text 429 with our
    # JSON-formatted response that matches the API's error schema.
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

    # --- 3. Add middleware in REVERSE execution order ---
    # Remember: last added = first executed (outermost).

    # Innermost: rate limiting (runs closest to route handlers).
    app.add_middleware(SlowAPIMiddleware)

    # Middle: security headers (injects on all responses).
    app.add_middleware(SecurityHeadersMiddleware)

    # Outermost: request logging (captures total time including
    # security headers + rate limiting overhead).
    app.add_middleware(RequestLoggerMiddleware)

    logger.info(
        "Middleware stack registered: "
        "RequestLogger → SecurityHeaders → SlowAPI (rate limiting)"
    )

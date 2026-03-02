"""
AI Insight Router — HTTP Interface
====================================

Single endpoint that demonstrates the highest-tier GenAI integration:
an LLM as a runtime component of the API, generating natural language
environmental briefings from pre-computed analytics.

The endpoint follows a strict caching discipline:
  - Cache HIT:  ~5ms response (DB lookup only)
  - Cache MISS: ~3-8s response (full pipeline + LLM call)
  - API FAIL:   Serves expired cache with a warning header

This means the first request after cache expiry is slow, but all
subsequent requests within the TTL are fast. The cost scales with
(cities × ingestion frequency), not with request volume.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.deps import DbSession
from app.schemas.schemas import AIInsightResponse
from app.schemas.schemas import ErrorResponse
from app.services.ai_service import AIInsightService, AIInsightUnavailableError
from app.services.city_service import CityNotFoundError

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ai",
    tags=["AI Insights"],
)


def get_ai_service() -> AIInsightService:
    return AIInsightService()


@router.get(
    "/insight/{city_id}",
    response_model=AIInsightResponse,
    summary="Generate AI environmental insight for a city",
    description=(
        "Produces a natural language briefing of a city's environmental "
        "conditions over the last 7 days, powered by Claude (Anthropic). "
        "\n\n"
        "**Pipeline:**\n"
        "1. Pre-aggregates statistics from PostgreSQL (deterministic)\n"
        "2. Detects anomalies via z-score analysis\n"
        "3. Extracts raw data context around each anomaly\n"
        "4. Computes cross-metric correlations\n"
        "5. Sends structured context to Claude for narration\n"
        "6. Caches the result (TTL = ingestion interval)\n"
        "\n\n"
        "**Caching:** Results are cached per city. Cached responses "
        "return in ~5ms. Fresh generation takes 3-8 seconds. "
        "If the AI service is unavailable, an expired cached insight "
        "is served as fallback.\n\n"
        "**Cost:** Uses Claude Haiku for cost efficiency. "
        "Token usage is tracked per-insight."
    ),
    responses={
        200: {
            "description": "AI-generated environmental insight",
            "model": AIInsightResponse,
        },
        404: {
            "description": "City not found",
            "model": ErrorResponse,
        },
        503: {
            "description": (
                "AI service unavailable and no cached insight exists"
            ),
            "model": ErrorResponse,
        },
    },
)
async def get_insight(
    city_id: str,
    db: DbSession,
    include_context: bool = Query(
        True,
        description=(
            "Include the pre-aggregated data payload in the response. "
            "Set to false to reduce response size. Default: true."
        ),
    ),
    force_refresh: bool = Query(
        False,
        description=(
            "Bypass cache and regenerate the insight. "
            "Useful for testing. Default: false."
        ),
    ),
    service: AIInsightService = Depends(get_ai_service),
) -> AIInsightResponse:
    """
    Example: `GET /ai/insight/london`

    Returns an AI-generated environmental briefing for London.

    Example response:
    ```json
    {
      "city_id": "london",
      "city_name": "London",
      "insight": "London's air quality has deteriorated over the past week...",
      "generated_at": "2025-02-27T10:00:00Z",
      "expires_at": "2025-02-27T11:00:00Z",
      "cached": false,
      "model_used": "claude-haiku-4-5-20251001",
      "data_summary": { ... }
    }
    ```
    """
    try:
        result = await service.generate_insight(
            db,
            city_id=city_id,
            force_refresh=force_refresh,
        )

        # Strip context payload if not requested
        if not include_context:
            result["data_summary"] = None

        return AIInsightResponse(**result)

    except CityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "not_found",
                "message": f"City '{city_id}' not found",
            },
        )

    except AIInsightUnavailableError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "ai_unavailable",
                "message": str(e),
            },
        )

"""
AI Insight Service — Hybrid Pre-Aggregation + LLM Narration
=============================================================

This is the capstone service that demonstrates the highest-tier GenAI
integration: using an LLM not as a code generator, but as a runtime
component of the API itself.

ARCHITECTURAL PRINCIPLE: DETERMINISTIC ANALYTICS, PROBABILISTIC NARRATION

The pipeline splits intelligence into two layers:
  1. PostgreSQL computes all statistics deterministically (averages,
     trends, anomalies, correlations). These numbers are CORRECT.
  2. The LLM interprets and narrates those statistics into a briefing.
     The LLM never does arithmetic — it explains patterns.

This means:
  - If the LLM hallucinates, the numbers in the context payload are
    still correct (consumers can verify).
  - If the LLM is unavailable, we can still serve the raw statistics.
  - The prompt template can be iterated without touching any SQL.

PIPELINE STEPS (matching the architectural diagram from our design):
  1. Pre-aggregate stats per parameter (SQL: AVG, MIN, MAX, trend)
  2. Run anomaly detection (reuses Pattern 4 from analytics service)
  3. Extract raw context window around each anomaly (±3 hours)
  4. Compute cross-metric correlations (pm25 vs temperature, humidity)
  5. Check cache for valid (non-expired) insight
  6. If cache miss: assemble context → call Anthropic API → cache result
  7. Return insight with metadata

COST CONTROL:
  - Cache TTL aligns with ingestion interval (1 hour default).
  - LLM calls scale with cities × ingestion frequency, NOT request volume.
  - Token usage is logged per-insight for cost auditing.
  - Haiku is used instead of Sonnet/Opus for cost efficiency.
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import anthropic
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.models import AIInsight
from app.models.models import City
from app.services.city_service import CityService, CityNotFoundError

logger = logging.getLogger(__name__)


# ===========================================================================
# PROMPT TEMPLATE
# ===========================================================================
# This is an architectural decision, not just a string. The prompt defines
# the contract between our deterministic analytics and the LLM's output.
#
# Design principles:
#   - Role framing ("environmental data analyst briefing a city council
#     member") constrains the tone and vocabulary.
#   - Explicit rules prevent common LLM failure modes:
#     - "Interpret trends, do not restate raw numbers" prevents the LLM
#       from just reading the JSON back.
#     - Conditional rules (z_score >= 3.0, pearson_r < -0.4) encode
#       domain knowledge that the LLM might not reliably apply on its own.
#   - "Write only the briefing, no headers or preamble" prevents
#     the LLM from adding markdown headers or "Here is your briefing:"
#     prefixes that would look wrong in an API response.
#
# This template should be version-controlled and A/B tested in production.
# Changes to the template directly affect the API's output quality.
# ===========================================================================

INSIGHT_PROMPT_TEMPLATE = """You are an environmental data analyst briefing a city council member.
Given pre-computed statistics and anomaly data, write a 2-3 paragraph summary in plain English.

Rules:
- Interpret trends, do not restate raw numbers
- If any anomaly z_score >= 3.0, mention a potential air quality concern
- If pearson_r for pm25/temperature < -0.4, reference possible thermal inversion effect
- End with one actionable observation if pm25 trend is rising > 10%
- Write only the briefing, no headers or preamble

Data:
{context_json}"""


# ===========================================================================
# CONTEXT ASSEMBLY (Steps 1-4)
# ===========================================================================


async def _build_context_payload(
    db: AsyncSession,
    city: City,
) -> dict:
    """
    Assemble the structured context payload for the LLM.

    This is the heart of the hybrid architecture. Each step produces
    deterministic, pre-computed data that the LLM will narrate.

    The payload is designed to be:
      - Compact: ~200-400 tokens of JSON (90% smaller than raw data).
      - Self-contained: the LLM doesn't need external context.
      - Verifiable: consumers can inspect data_summary to fact-check.
    """
    now = datetime.now(timezone.utc)
    seven_days_ago = now - timedelta(days=7)
    fourteen_days_ago = now - timedelta(days=14)

    # ----- Step 1: Pre-aggregated statistics per parameter -----
    stats_result = await db.execute(
        text("""
            SELECT parameter,
                   ROUND(AVG(value)::numeric, 2) AS avg,
                   ROUND(MIN(value)::numeric, 2) AS min,
                   ROUND(MAX(value)::numeric, 2) AS max,
                   COUNT(*) AS reading_count
            FROM readings
            WHERE city_id = :city_id
              AND quality_flag = 'valid'
              AND timestamp >= :cutoff
            GROUP BY parameter
            ORDER BY parameter
        """),
        {"city_id": city.id, "cutoff": seven_days_ago},
    )
    stats_rows = stats_result.fetchall()

    # Compute trend (% change vs previous 7 days) per parameter
    prev_stats_result = await db.execute(
        text("""
            SELECT parameter,
                   ROUND(AVG(value)::numeric, 2) AS avg
            FROM readings
            WHERE city_id = :city_id
              AND quality_flag = 'valid'
              AND timestamp >= :prev_cutoff
              AND timestamp < :current_cutoff
            GROUP BY parameter
        """),
        {
            "city_id": city.id,
            "prev_cutoff": fourteen_days_ago,
            "current_cutoff": seven_days_ago,
        },
    )
    prev_avgs = {row.parameter: float(row.avg) for row in prev_stats_result.fetchall()}

    statistics = {}
    for row in stats_rows:
        param = row.parameter
        current_avg = float(row.avg)
        prev_avg = prev_avgs.get(param)

        if prev_avg and prev_avg > 0:
            pct_change = round((current_avg - prev_avg) / prev_avg * 100, 1)
            trend_direction = (
                "rising" if pct_change > 5
                else "falling" if pct_change < -5
                else "stable"
            )
        else:
            pct_change = None
            trend_direction = "insufficient_data"

        statistics[param] = {
            "avg": current_avg,
            "min": float(row.min),
            "max": float(row.max),
            "reading_count": row.reading_count,
            "trend_direction": trend_direction,
            "trend_pct_change": pct_change,
        }

    # ----- Step 2: Anomaly detection (reuses Pattern 4 logic) -----
    # We inline the anomaly query rather than calling AnalyticsService
    # to avoid circular dependencies and to keep the context assembly
    # self-contained within a single DB session.
    warmup_cutoff = seven_days_ago - timedelta(days=7)

    anomaly_result = await db.execute(
        text("""
            WITH rolling AS (
                SELECT timestamp, parameter, value,
                       AVG(value) OVER w    AS rolling_avg,
                       STDDEV(value) OVER w AS rolling_stddev,
                       COUNT(*) OVER w      AS window_size
                FROM readings
                WHERE city_id = :city_id
                  AND quality_flag = 'valid'
                  AND timestamp >= :warmup_cutoff
                WINDOW w AS (
                    PARTITION BY parameter
                    ORDER BY timestamp
                    ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
                )
            )
            SELECT timestamp, parameter, value,
                   ROUND(rolling_avg::numeric, 2) AS rolling_avg,
                   ROUND(rolling_stddev::numeric, 2) AS rolling_stddev,
                   ROUND(((value - rolling_avg)
                        / NULLIF(rolling_stddev, 0))::numeric, 2) AS z_score
            FROM rolling
            WHERE timestamp >= :analysis_cutoff
              AND window_size >= 24
              AND rolling_stddev > 0
              AND ABS((value - rolling_avg) / rolling_stddev) >= 2.0
            ORDER BY ABS((value - rolling_avg) / rolling_stddev) DESC
            LIMIT 10
        """),
        {
            "city_id": city.id,
            "warmup_cutoff": warmup_cutoff,
            "analysis_cutoff": seven_days_ago,
        },
    )
    anomaly_rows = anomaly_result.fetchall()

    anomalies = []
    for row in anomaly_rows:
        anomaly_entry = {
            "timestamp": row.timestamp.isoformat(),
            "parameter": row.parameter,
            "value": float(row.value),
            "rolling_avg": float(row.rolling_avg),
            "z_score": float(row.z_score),
        }

        # ----- Step 3: Extract ±3 hour raw context window -----
        context_result = await db.execute(
            text("""
                SELECT timestamp, parameter, value
                FROM readings
                WHERE city_id = :city_id
                  AND quality_flag = 'valid'
                  AND timestamp BETWEEN :window_start AND :window_end
                ORDER BY timestamp
            """),
            {
                "city_id": city.id,
                "window_start": row.timestamp - timedelta(hours=3),
                "window_end": row.timestamp + timedelta(hours=3),
            },
        )
        context_rows = context_result.fetchall()

        # Pivot the context window into a compact format:
        # [{timestamp, param1: val, param2: val}, ...]
        time_buckets: dict[str, dict] = {}
        for cr in context_rows:
            ts_key = cr.timestamp.isoformat()
            if ts_key not in time_buckets:
                time_buckets[ts_key] = {"timestamp": ts_key}
            time_buckets[ts_key][cr.parameter] = float(cr.value)

        anomaly_entry["context_window"] = list(time_buckets.values())[-7:]  # Cap at 7 entries
        anomalies.append(anomaly_entry)

    # ----- Step 4: Cross-metric correlations -----
    correlations = []
    correlation_pairs = [
        ("pm25", "temperature"),
        ("pm25", "humidity"),
    ]

    for param_a, param_b in correlation_pairs:
        corr_result = await db.execute(
            text("""
                SELECT ROUND(corr(a.value, b.value)::numeric, 4) AS pearson_r,
                       COUNT(*) AS sample_size
                FROM readings a
                JOIN readings b
                  ON a.city_id = b.city_id
                 AND a.timestamp = b.timestamp
                WHERE a.parameter = :param_a
                  AND b.parameter = :param_b
                  AND a.city_id = :city_id
                  AND a.quality_flag = 'valid'
                  AND b.quality_flag = 'valid'
                  AND a.timestamp >= :cutoff
            """),
            {
                "param_a": param_a,
                "param_b": param_b,
                "city_id": city.id,
                "cutoff": seven_days_ago,
            },
        )
        corr_row = corr_result.fetchone()

        if corr_row and corr_row.pearson_r is not None:
            correlations.append({
                "params": [param_a, param_b],
                "pearson_r": float(corr_row.pearson_r),
                "sample_size": corr_row.sample_size,
            })

    # ----- Step 5: Assemble the payload -----
    #
    # Data quality metadata: tells the LLM (and the consumer) how
    # much data underlies the analysis. An insight based on 50 readings
    # is less reliable than one based on 1,000 — the LLM should know this.
    quality_result = await db.execute(
        text("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE quality_flag = 'valid') AS valid,
                   COUNT(*) FILTER (WHERE quality_flag = 'suspect') AS suspect,
                   COUNT(*) FILTER (WHERE quality_flag = 'missing') AS missing
            FROM readings
            WHERE city_id = :city_id
              AND timestamp >= :cutoff
        """),
        {"city_id": city.id, "cutoff": seven_days_ago},
    )
    quality_row = quality_result.fetchone()

    total = quality_row.total if quality_row else 0
    valid = quality_row.valid if quality_row else 0

    payload = {
        "city": city.name,
        "country": city.country,
        "period": f"{seven_days_ago.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}",
        "statistics": statistics,
        "anomalies": anomalies[:5],  # Top 5 most extreme
        "correlations": correlations,
        "data_quality": {
            "total_readings": total,
            "valid_pct": round(valid / total * 100, 1) if total > 0 else 0,
            "suspect": quality_row.suspect if quality_row else 0,
            "missing": quality_row.missing if quality_row else 0,
        },
    }

    return payload


# ===========================================================================
# CACHE MANAGEMENT (Steps 5-6)
# ===========================================================================


def _compute_context_hash(payload: dict) -> str:
    """
    SHA-256 hash of the context payload.

    Used to detect when the underlying data has changed. If two
    consecutive ingestion cycles produce identical statistics
    (same averages, same anomalies), the hash will match and
    we can skip regeneration even if the cache has technically expired.
    """
    # Sort keys for deterministic serialisation
    serialised = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialised.encode()).hexdigest()


async def _get_cached_insight(
    db: AsyncSession,
    city_id: str,
    *,
    allow_expired: bool = False,
) -> Optional[AIInsight]:
    """
    Retrieve a cached insight for the given city.

    By default, only returns non-expired insights. When allow_expired=True,
    returns the most recent insight regardless of expiry — used as a
    fallback when the Anthropic API is unavailable.
    """
    now = datetime.now(timezone.utc)

    stmt = (
        select(AIInsight)
        .where(AIInsight.city_id == city_id)
        .order_by(AIInsight.generated_at.desc())
        .limit(1)
    )

    if not allow_expired:
        stmt = stmt.where(AIInsight.expires_at > now)

    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def _store_insight(
    db: AsyncSession,
    city_id: str,
    insight_text: str,
    context_hash: str,
    model_used: str,
    input_tokens: int,
    output_tokens: int,
) -> AIInsight:
    """
    Store a generated insight in the cache table.

    TTL is set from settings.CACHE_TTL_SECONDS, which should align
    with the ingestion interval so insights are regenerated once
    per data refresh cycle.
    """
    now = datetime.now(timezone.utc)

    insight = AIInsight(
        city_id=city_id,
        generated_at=now,
        expires_at=now + timedelta(seconds=settings.CACHE_TTL_SECONDS),
        insight_text=insight_text,
        context_hash=context_hash,
        model_used=model_used,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )

    db.add(insight)
    await db.flush()
    await db.refresh(insight)
    return insight


# ===========================================================================
# LLM CALL (Step 7)
# ===========================================================================


async def _call_anthropic(
    context_payload: dict,
) -> dict:
    """
    Call the Anthropic API to generate an environmental insight.

    Returns a dict with:
      - text: the generated insight
      - input_tokens: tokens consumed for input
      - output_tokens: tokens generated
      - model: model identifier used

    Uses the synchronous Anthropic client in a way that's compatible
    with async code. The anthropic SDK handles HTTP internally.

    Why claude-haiku-4-5-20251001?
      This endpoint is called frequently (once per city per ingestion
      cycle). Haiku provides sufficient quality for structured
      summarisation at ~10x lower cost than Sonnet. The prompt is
      highly structured with explicit rules, which compensates for
      Haiku's lower general reasoning ability compared to Sonnet.

    Timeout: 10 seconds.
      Environmental briefings are 2-3 paragraphs (~150-250 tokens output).
      Haiku generates this in 1-3 seconds. A 10-second timeout is generous
      and only triggers during API outages or extreme load.
    """
    context_json = json.dumps(context_payload, indent=2, default=str)
    prompt = INSIGHT_PROMPT_TEMPLATE.format(context_json=context_json)

    model = "claude-haiku-4-5-20251001"

    client = anthropic.Anthropic(
        api_key=settings.ANTHROPIC_API_KEY,
        timeout=10.0,
    )

    response = client.messages.create(
        model=model,
        max_tokens=settings.AI_MAX_TOKENS,
        messages=[
            {"role": "user", "content": prompt},
        ],
    )

    # Extract text from the response content blocks
    insight_text = ""
    for block in response.content:
        if block.type == "text":
            insight_text += block.text

    return {
        "text": insight_text.strip(),
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
        "model": model,
    }


# ===========================================================================
# PUBLIC API: GENERATE INSIGHT
# ===========================================================================


class AIInsightService:
    """
    Orchestrates the hybrid AI insight pipeline.

    Pipeline:
      1. Validate city exists
      2. Build context payload (SQL analytics)
      3. Check cache
      4. If cache hit → return cached
      5. If cache miss → call LLM → cache → return
      6. If LLM fails → return expired cache as fallback
    """

    def __init__(self):
        self._city_service = CityService()

    async def generate_insight(
        self,
        db: AsyncSession,
        city_id: str,
        *,
        force_refresh: bool = False,
    ) -> dict:
        """
        Generate or retrieve a cached AI insight for a city.

        Parameters:
            city_id: City slug identifier.
            force_refresh: If True, bypass cache and regenerate.
                          Used by admin/testing endpoints.

        Returns a dict suitable for AIInsightResponse serialisation.

        Error handling strategy:
          - City not found → raise CityNotFoundError (→ 404)
          - Context assembly fails → raise (→ 500)
          - Cache hit → return immediately (fast path)
          - LLM call fails → serve expired cache with warning
          - LLM call fails AND no cache → return error dict
        """
        start_time = time.monotonic()

        # Step 1: Validate city
        city = await self._city_service.get_city(db, city_id)

        # Step 5 (checked early): Look for valid cache
        if not force_refresh:
            cached = await _get_cached_insight(db, city_id)
            if cached:
                logger.info(
                    "Cache HIT for %s (generated %s, expires %s)",
                    city_id,
                    cached.generated_at.isoformat(),
                    cached.expires_at.isoformat(),
                )
                return _format_response(city, cached, cached=True)

        # Steps 1-4: Build context payload
        logger.info("Building context payload for %s", city_id)
        context_payload = await _build_context_payload(db, city)
        context_hash = _compute_context_hash(context_payload)

        # Check if we have a cached insight with the same context hash
        # (data hasn't changed even though cache expired)
        if not force_refresh:
            expired_cache = await _get_cached_insight(
                db, city_id, allow_expired=True
            )
            if expired_cache and expired_cache.context_hash == context_hash:
                # Data hasn't changed — extend the cache instead of
                # spending tokens on an identical insight.
                logger.info(
                    "Context unchanged for %s — extending expired cache",
                    city_id,
                )
                now = datetime.now(timezone.utc)
                expired_cache.expires_at = now + timedelta(
                    seconds=settings.CACHE_TTL_SECONDS
                )
                await db.flush()
                await db.refresh(expired_cache)
                await db.commit()
                return _format_response(
                    city, expired_cache, cached=True,
                    context_payload=context_payload,
                )

        # Step 7: Call Anthropic API
        logger.info("Calling Anthropic API for %s", city_id)
        try:
            llm_result = await _call_anthropic(context_payload)

            duration_ms = round((time.monotonic() - start_time) * 1000, 1)

            logger.info(
                "LLM response for %s: %d input tokens, %d output tokens, "
                "%.0fms total pipeline duration",
                city_id,
                llm_result["input_tokens"],
                llm_result["output_tokens"],
                duration_ms,
            )

            # Store in cache
            insight = await _store_insight(
                db,
                city_id=city_id,
                insight_text=llm_result["text"],
                context_hash=context_hash,
                model_used=llm_result["model"],
                input_tokens=llm_result["input_tokens"],
                output_tokens=llm_result["output_tokens"],
            )
            await db.commit()

            return _format_response(
                city, insight, cached=False,
                context_payload=context_payload,
            )

        except (
            anthropic.APITimeoutError,
            anthropic.APIConnectionError,
            anthropic.RateLimitError,
            anthropic.APIStatusError,
        ) as e:
            # Step 8 fallback: serve expired cache if available
            logger.error(
                "Anthropic API error for %s: %s — checking for fallback cache",
                city_id,
                str(e),
            )

            fallback = await _get_cached_insight(
                db, city_id, allow_expired=True
            )
            if fallback:
                logger.warning(
                    "Serving expired cache for %s (generated %s)",
                    city_id,
                    fallback.generated_at.isoformat(),
                )
                return _format_response(
                    city, fallback, cached=True,
                    context_payload=context_payload,
                    warning="Served from expired cache due to AI service unavailability",
                )

            # No cache at all — return a structured error
            logger.error("No fallback cache available for %s", city_id)
            raise AIInsightUnavailableError(
                f"AI insight generation failed and no cached insight "
                f"exists for city '{city_id}'"
            )


# ===========================================================================
# RESPONSE FORMATTING
# ===========================================================================


def _format_response(
    city: City,
    insight: AIInsight,
    *,
    cached: bool,
    context_payload: Optional[dict] = None,
    warning: Optional[str] = None,
) -> dict:
    """Format an AIInsight ORM object into a response dict."""
    result = {
        "city_id": city.id,
        "city_name": city.name,
        "insight": insight.insight_text,
        "generated_at": insight.generated_at,
        "expires_at": insight.expires_at,
        "cached": cached,
        "model_used": insight.model_used,
        "data_summary": context_payload,
    }

    if warning:
        result["warning"] = warning

    return result


# ===========================================================================
# DOMAIN EXCEPTIONS
# ===========================================================================


class AIInsightUnavailableError(Exception):
    """Raised when AI insight generation fails and no cache exists."""

    def __init__(self, message: str):
        super().__init__(message)

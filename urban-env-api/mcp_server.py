"""
Urban Environmental Intelligence — MCP Server
================================================

This module exposes the Urban Environmental Intelligence API as a
Model Context Protocol (MCP) server. Instead of going through HTTP,
MCP tools call the service layer directly via the database, which
makes them faster and usable as Claude Desktop integrations.

WHY MCP?

MCP (Model Context Protocol) is an open standard that lets LLMs
invoke tools, read resources, and follow prompts in a structured way.
By wrapping our API as an MCP server, any MCP-compatible client —
Claude Desktop, Cursor, VS Code with Copilot — can use our
environmental data tools natively, without the LLM needing to
construct HTTP requests or parse JSON responses.

ARCHITECTURE:

  ┌────────────────────┐
  │  Claude Desktop    │   (MCP Client)
  │  or other MCP host │
  └────────┬───────────┘
           │ stdio / streamable-http
  ┌────────▼───────────┐
  │   mcp_server.py    │   (This file — MCP Server)
  │   FastMCP tools    │
  └────────┬───────────┘
           │ Direct Python imports
  ┌────────▼───────────┐
  │   Service Layer    │   (CityService, AnalyticsService, AIInsightService)
  └────────┬───────────┘
           │ async SQLAlchemy
  ┌────────▼───────────┐
  │   PostgreSQL       │
  └────────────────────┘

The MCP server bypasses the FastAPI router layer entirely and calls
services directly. This means:
  - No HTTP overhead (no serialisation/deserialisation round-trip).
  - No authentication layer (MCP handles trust at the transport level).
  - Same business logic, different interface.

TOOL DESIGN PRINCIPLES:

1. Tools accept city NAMES (not slugs/IDs). Humans say "London",
   not "london". The server resolves names to IDs internally.

2. Tools return FORMATTED STRINGS, not JSON. MCP tools feed into
   an LLM's context window, and LLMs work better with readable
   text than raw JSON. The LLM can then synthesise, compare, or
   summarise the data for the user.

3. Errors return informative strings (never raise exceptions).
   If a city isn't found, the tool returns "City 'Londn' not found.
   Available cities: London, Manchester, ..." — the LLM can then
   suggest a correction to the user.

USAGE:
  # stdio transport (Claude Desktop)
  python mcp_server.py

  # streamable-http transport (web clients, MCP Inspector)
  python mcp_server.py --transport http

  # Test with MCP Inspector
  mcp dev mcp_server.py
"""

import asyncio
import logging
import re
import sys
from typing import Optional

from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Bootstrap: configure logging to stderr (stdout is reserved for MCP protocol
# messages in stdio transport — writing to stdout would corrupt the JSON-RPC
# stream and break the MCP connection).
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("mcp_server")


# ---------------------------------------------------------------------------
# MCP Server instance
# ---------------------------------------------------------------------------
mcp = FastMCP(
    "Urban Environmental Intelligence",
    instructions=(
        "This server provides real-time environmental monitoring data "
        "for cities. Use it to check air quality, compare pollution "
        "levels across cities, detect anomalies in environmental data, "
        "and generate AI-powered environmental briefings."
    ),
)


# ---------------------------------------------------------------------------
# Database session helper
# ---------------------------------------------------------------------------
# We import lazily inside tools to avoid import-time side effects
# (the settings module validates env vars on import, which would fail
# if DATABASE_URL isn't set when the module is first parsed).


async def _get_db_session():
    """
    Create an async database session for a tool invocation.

    Each tool call gets its own session, committed and closed
    within the tool function. This mirrors the session-per-request
    pattern from the FastAPI dependency injection.
    """
    from app.core.database import async_session_factory

    return async_session_factory()


async def _resolve_city(db, city_name: str):
    """
    Resolve a human-friendly city name to a City ORM object.

    Tries two strategies:
      1. Case-insensitive name match via repository.
      2. Slug-based lookup (in case the user passes a slug).

    Returns (city, None) on success or (None, error_string) on failure.
    The error string includes available cities to help the LLM suggest
    corrections.
    """
    from app.repositories.city_repository import CityRepository
    from sqlalchemy import select, func
    from app.models.models import City

    repo = CityRepository()

    # Strategy 1: exact name match (case-insensitive)
    city = await repo.get_by_name(db, city_name)
    if city:
        return city, None

    # Strategy 2: try as slug
    city = await repo.get_by_id(db, city_name.lower().strip())
    if city:
        return city, None

    # Strategy 3: try slug-ifying the input ("New York" → "new-york")
    slug = re.sub(r"[^a-z0-9]+", "-", city_name.lower()).strip("-")
    city = await repo.get_by_id(db, slug)
    if city:
        return city, None

    # Not found — build helpful error with available cities
    result = await db.execute(
        select(City.name).where(City.is_active == True).order_by(City.name)  # noqa: E712
    )
    available = [row[0] for row in result.fetchall()]

    if available:
        return None, (
            f"City '{city_name}' not found. "
            f"Available cities: {', '.join(available)}"
        )
    else:
        return None, (
            f"City '{city_name}' not found and no cities are registered. "
            f"Cities must be created via the API before querying."
        )


async def _resolve_multiple_cities(db, city_names: list[str]):
    """
    Resolve a list of city names. Returns (city_list, error_string).
    If any city is not found, returns error for all missing cities.
    """
    cities = []
    missing = []

    for name in city_names:
        city, err = await _resolve_city(db, name)
        if city:
            cities.append(city)
        else:
            missing.append(name)

    if missing:
        from sqlalchemy import select
        from app.models.models import City

        result = await db.execute(
            select(City.name).where(City.is_active == True).order_by(City.name)  # noqa: E712
        )
        available = [row[0] for row in result.fetchall()]
        return None, (
            f"Cities not found: {', '.join(missing)}. "
            f"Available cities: {', '.join(available) if available else '(none)'}"
        )

    return cities, None


# ===========================================================================
# TOOL 1: GET CITY AIR QUALITY
# ===========================================================================


@mcp.tool()
async def get_city_air_quality(city_name: str, days: int = 7) -> str:
    """
    Get current air quality status and trend for a city.

    Returns a summary of air quality parameters (PM2.5, PM10, NO2, O3)
    and weather conditions (temperature, humidity, wind speed) over the
    specified number of days, including average values, trends, and
    whether conditions are improving or worsening.

    Use this when a user asks about air quality in a specific city,
    current pollution levels, or whether conditions are getting better
    or worse.

    Args:
        city_name: Name of the city (e.g. "London", "Manchester")
        days: Number of days to analyse (1-365, default 7)
    """
    try:
        async with await _get_db_session() as db:
            city, err = await _resolve_city(db, city_name)
            if err:
                return err

            from app.services.analytics_service import AnalyticsService

            service = AnalyticsService()

            # Fetch trends for all key parameters
            parameters = ["pm25", "pm10", "no2", "o3", "temperature", "humidity", "wind_speed"]
            sections = []

            sections.append(f"=== Air Quality Report: {city.name} ({days}-day summary) ===\n")

            for param in parameters:
                try:
                    result = await service.get_trend(
                        db,
                        city_id=city.id,
                        parameter=param,
                        days=days,
                        interval="day",
                    )

                    if not result["data"]:
                        continue

                    data = result["data"]
                    overall_avg = sum(d["avg"] for d in data) / len(data)
                    latest_avg = data[-1]["avg"] if data else 0
                    earliest_avg = data[0]["avg"] if data else 0

                    # Compute trend
                    if earliest_avg > 0:
                        pct_change = ((latest_avg - earliest_avg) / earliest_avg) * 100
                        if pct_change > 5:
                            trend = f"↑ RISING ({pct_change:+.1f}%)"
                        elif pct_change < -5:
                            trend = f"↓ FALLING ({pct_change:+.1f}%)"
                        else:
                            trend = f"→ STABLE ({pct_change:+.1f}%)"
                    else:
                        trend = "→ insufficient baseline"

                    unit = result["unit"]

                    # AQI interpretation for PM2.5
                    aqi_note = ""
                    if param == "pm25":
                        if overall_avg <= 12:
                            aqi_note = " [GOOD — meets WHO guideline]"
                        elif overall_avg <= 35.4:
                            aqi_note = " [MODERATE]"
                        elif overall_avg <= 55.4:
                            aqi_note = " [UNHEALTHY for sensitive groups]"
                        elif overall_avg <= 150.4:
                            aqi_note = " [UNHEALTHY]"
                        else:
                            aqi_note = " [VERY UNHEALTHY]"

                    sections.append(
                        f"  {param.upper():>14s}: avg={overall_avg:.1f} {unit}, "
                        f"range=[{min(d['min'] for d in data):.1f}–"
                        f"{max(d['max'] for d in data):.1f}], "
                        f"trend: {trend}{aqi_note}"
                    )

                except Exception:
                    # Parameter might not have data — skip silently
                    continue

            if len(sections) <= 1:
                return f"No environmental data available for {city.name} in the last {days} days."

            sections.append(f"\n  Data period: last {days} days")
            sections.append(f"  Total parameters tracked: {len(sections) - 2}")

            return "\n".join(sections)

    except Exception as e:
        logger.exception("Error in get_city_air_quality")
        return f"Error retrieving air quality for '{city_name}': {str(e)}"


# ===========================================================================
# TOOL 2: COMPARE CITIES POLLUTION
# ===========================================================================


@mcp.tool()
async def compare_cities_pollution(
    city_names: list[str],
    parameter: str = "pm25",
) -> str:
    """
    Compare pollution levels across multiple cities with ranking.

    Returns a ranked comparison table showing which cities have the
    best and worst air quality for the specified parameter, including
    average values and whether conditions are improving or worsening
    compared to the previous period.

    Use this when a user asks to compare air quality between cities,
    wants to know which city has better/worse pollution, or asks
    for a ranking.

    Args:
        city_names: List of city names to compare (at least 2)
        parameter: Environmental parameter to compare (default "pm25").
                   Options: pm25, pm10, no2, o3, temperature, humidity, wind_speed
    """
    try:
        if len(city_names) < 2:
            return "Please provide at least 2 cities to compare."

        async with await _get_db_session() as db:
            cities, err = await _resolve_multiple_cities(db, city_names)
            if err:
                return err

            from app.services.analytics_service import AnalyticsService

            service = AnalyticsService()
            city_ids = [c.id for c in cities]

            result = await service.compare_cities(
                db,
                city_ids=city_ids,
                parameter=parameter,
                days=7,
            )

            if not result["cities"]:
                return (
                    f"No data for parameter '{parameter}' across the "
                    f"requested cities in the last 7 days."
                )

            unit = result["unit"]
            lines = [
                f"=== City Comparison: {parameter.upper()} (last 7 days) ===\n",
                f"  {'Rank':<6}{'City':<20}{'Average':>10} {unit:<8}"
                f"{'Trend':>12}",
                f"  {'─' * 58}",
            ]

            for city_data in result["cities"]:
                pct = city_data.get("pct_change_vs_prev_period")
                if pct is not None:
                    if pct > 5:
                        trend = f"↑ {pct:+.1f}%"
                    elif pct < -5:
                        trend = f"↓ {pct:+.1f}%"
                    else:
                        trend = f"→ {pct:+.1f}%"
                else:
                    trend = "n/a"

                medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(city_data["rank"], "  ")

                lines.append(
                    f"  {medal} {city_data['rank']:<4}"
                    f"{city_data['city_name']:<20}"
                    f"{city_data['avg']:>10.2f} {unit:<8}"
                    f"{trend:>12}"
                )

            lines.append(f"\n  Ranking: 1 = lowest (cleanest) for pollutants")
            return "\n".join(lines)

    except Exception as e:
        logger.exception("Error in compare_cities_pollution")
        return f"Error comparing cities: {str(e)}"


# ===========================================================================
# TOOL 3: DETECT POLLUTION ANOMALIES
# ===========================================================================


@mcp.tool()
async def detect_pollution_anomalies(
    city_name: str,
    sensitivity: float = 2.0,
) -> str:
    """
    Detect unusual pollution events in a city's recent data.

    Analyses the last 30 days of environmental data to find readings
    that deviate significantly from the 7-day rolling average. Returns
    a list of anomalous events with timestamps, severity ratings, and
    context about what was happening at the time.

    Severity levels:
    - LOW: Notable deviation (z-score 2.0-2.5)
    - MEDIUM: Significant deviation (z-score 2.5-3.5)
    - HIGH: Extreme deviation (z-score 3.5+), possible pollution event

    Use this when a user asks about pollution spikes, unusual readings,
    air quality events, or whether anything abnormal has happened recently.

    Args:
        city_name: Name of the city to analyse
        sensitivity: Z-score threshold (lower = more sensitive, default 2.0)
    """
    try:
        async with await _get_db_session() as db:
            city, err = await _resolve_city(db, city_name)
            if err:
                return err

            from app.services.analytics_service import AnalyticsService

            service = AnalyticsService()

            # Check anomalies across all air quality parameters
            all_anomalies = []
            for param in ["pm25", "pm10", "no2", "o3"]:
                try:
                    result = await service.detect_anomalies(
                        db,
                        city_id=city.id,
                        parameter=param,
                        sensitivity=sensitivity,
                        days=30,
                    )
                    for a in result["anomalies"]:
                        a["parameter"] = param
                        a["unit"] = result["unit"]
                    all_anomalies.extend(result["anomalies"])
                except Exception:
                    continue

            if not all_anomalies:
                return (
                    f"No anomalies detected in {city.name} over the last "
                    f"30 days at sensitivity {sensitivity}. This suggests "
                    f"environmental conditions have been within normal ranges."
                )

            # Sort by absolute z-score (most extreme first)
            all_anomalies.sort(key=lambda a: abs(a["z_score"]), reverse=True)

            lines = [
                f"=== Anomaly Report: {city.name} (last 30 days) ===",
                f"  Sensitivity: z-score ≥ {sensitivity}",
                f"  Total anomalies detected: {len(all_anomalies)}\n",
            ]

            severity_counts = {"high": 0, "medium": 0, "low": 0}
            for a in all_anomalies:
                severity_counts[a["severity"]] += 1

            lines.append(
                f"  Summary: {severity_counts['high']} HIGH, "
                f"{severity_counts['medium']} MEDIUM, "
                f"{severity_counts['low']} LOW\n"
            )

            # Show top 10 most extreme
            for i, a in enumerate(all_anomalies[:10], 1):
                ts = a["timestamp"]
                if hasattr(ts, "strftime"):
                    ts_str = ts.strftime("%Y-%m-%d %H:%M UTC")
                else:
                    ts_str = str(ts)

                severity_icon = {
                    "high": "🔴 HIGH",
                    "medium": "🟡 MEDIUM",
                    "low": "🟢 LOW",
                }[a["severity"]]

                lines.append(
                    f"  {i:>2}. [{severity_icon}] {a['parameter'].upper()} "
                    f"at {ts_str}"
                )
                lines.append(
                    f"      Value: {a['value']:.1f} {a.get('unit', '')} "
                    f"(rolling avg: {a['rolling_avg']:.1f}, "
                    f"z-score: {a['z_score']:+.2f})"
                )

            if len(all_anomalies) > 10:
                lines.append(f"\n  ... and {len(all_anomalies) - 10} more anomalies")

            return "\n".join(lines)

    except Exception as e:
        logger.exception("Error in detect_pollution_anomalies")
        return f"Error detecting anomalies for '{city_name}': {str(e)}"


# ===========================================================================
# TOOL 4: GET AI ENVIRONMENTAL INSIGHT
# ===========================================================================


@mcp.tool()
async def get_ai_environmental_insight(city_name: str) -> str:
    """
    Get an AI-generated environmental briefing for a city.

    Produces a natural language summary of a city's environmental
    conditions over the last 7 days, written as if briefing a
    city council member. The briefing is powered by Claude and
    interprets trends, anomalies, and cross-metric correlations
    into plain English.

    The briefing covers:
    - Overall air quality trends (improving/worsening)
    - Notable pollution events or anomalies
    - Correlations between weather and pollution (e.g. thermal inversions)
    - Actionable observations if conditions are concerning

    Results are cached for 1 hour per city. First request may take
    3-8 seconds; subsequent requests return instantly from cache.

    Use this when a user asks for an environmental summary, overview,
    report, or briefing about a city's conditions.

    Args:
        city_name: Name of the city to generate a briefing for
    """
    try:
        async with await _get_db_session() as db:
            city, err = await _resolve_city(db, city_name)
            if err:
                return err

            from app.services.ai_service import AIInsightService, AIInsightUnavailableError

            service = AIInsightService()

            try:
                result = await service.generate_insight(db, city_id=city.id)

                cached_label = " (from cache)" if result.get("cached") else " (freshly generated)"
                warning = result.get("warning", "")
                if warning:
                    warning = f"\n⚠️  {warning}\n"

                return (
                    f"=== Environmental Briefing: {city.name}{cached_label} ===\n"
                    f"{warning}\n"
                    f"{result['insight']}\n\n"
                    f"---\n"
                    f"Generated: {result['generated_at'].strftime('%Y-%m-%d %H:%M UTC')}\n"
                    f"Expires: {result['expires_at'].strftime('%Y-%m-%d %H:%M UTC')}\n"
                    f"Model: {result['model_used']}"
                )

            except AIInsightUnavailableError:
                return (
                    f"AI insight service is currently unavailable for {city.name}. "
                    f"No cached insight exists. Try again later, or use the "
                    f"'get_city_air_quality' tool for raw data."
                )

    except Exception as e:
        logger.exception("Error in get_ai_environmental_insight")
        return f"Error generating insight for '{city_name}': {str(e)}"


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    transport = "stdio"
    if "--transport" in sys.argv:
        idx = sys.argv.index("--transport")
        if idx + 1 < len(sys.argv):
            transport = sys.argv[idx + 1]
    if "--http" in sys.argv:
        transport = "streamable-http"

    logger.info(
        "Starting Urban Environmental Intelligence MCP server "
        "(transport=%s)",
        transport,
    )
    mcp.run(transport=transport)

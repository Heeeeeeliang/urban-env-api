# Urban Environmental Intelligence API

![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?logo=fastapi&logoColor=white)
![PostgreSQL 15](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Deployed on Render](https://img.shields.io/badge/Deployed-Render-46E3B7?logo=render&logoColor=white)

## Overview

The Urban Environmental Intelligence API is a production-grade data platform that integrates real-time air quality measurements, weather observations, and AI-powered analysis into a single RESTful interface. It continuously ingests data from [OpenAQ](https://openaq.org/) (PM2.5, PM10, NO2, O3) and [Open-Meteo](https://open-meteo.com/) (temperature, humidity, wind speed, pressure), validates every reading with automated quality flags, and stores the results in PostgreSQL for time-series analytics.

On top of this data foundation, the API provides five analytical query patterns — trend analysis, multi-city comparison, anomaly detection, cross-metric correlation, and city ranking — all backed by PostgreSQL-optimised SQL with composite indexes. An AI insight endpoint sends pre-aggregated statistics to [Anthropic Claude](https://www.anthropic.com/), which generates natural language environmental briefings that interpret trends and anomalies in plain English.

The system also exposes an MCP (Model Context Protocol) server, enabling Claude Desktop and other MCP-compatible clients to query environmental data through native tool integrations — no HTTP required.

## Live Demo

- **Swagger UI:** [https://urban-env-api.onrender.com/docs](https://urban-env-api.onrender.com/docs)
- **ReDoc:** [https://urban-env-api.onrender.com/redoc](https://urban-env-api.onrender.com/redoc)
- **Health Check:** [https://urban-env-api.onrender.com/health](https://urban-env-api.onrender.com/health)

> The API is hosted on Render's free tier. The first request may take ~30 seconds while the service cold-starts.

## Architecture

```mermaid
graph TB
    Client([Client / Browser]) -->|HTTP + API Key| FW[FastAPI + Middleware]

    subgraph API["FastAPI Application"]
        FW --> Cities[Cities Router]
        FW --> Analytics[Analytics Router]
        FW --> AI[AI Insight Router]
        FW --> Health[Health Endpoint]

        Cities --> CS[City Service]
        Analytics --> AS[Analytics Service]
        AI --> AIS[AI Insight Service]

        CS --> CR[City Repository]
        AS -->|PostgreSQL-specific SQL| DB[(PostgreSQL 15)]
        AIS -->|Pre-aggregated stats| Claude[Anthropic Claude API]
        AIS -->|Cache read/write| DB
        CR --> DB
    end

    subgraph Background["Background Pipeline"]
        Sched[APScheduler] -->|Every 60 min| Ingest[Ingestion Service]
        Ingest -->|Air quality| OpenAQ[OpenAQ API]
        Ingest -->|Weather data| Meteo[Open-Meteo API]
        Ingest -->|Validate + upsert| DB
    end

    subgraph MCP["MCP Server"]
        MCPClient([Claude Desktop / Cursor]) -->|stdio / HTTP| MCPServer[FastMCP Server]
        MCPServer -->|Direct Python imports| CS
        MCPServer --> AS
        MCPServer --> AIS
    end

    style DB fill:#336791,color:#fff
    style Claude fill:#D97757,color:#fff
    style OpenAQ fill:#00B4D8,color:#fff
    style Meteo fill:#2EC4B6,color:#fff
```

## Key Features

**Data Integration** — An hourly background pipeline fetches air quality data from OpenAQ and weather observations from Open-Meteo for all monitored cities. Readings are validated, quality-flagged (valid/suspect/missing), and bulk-upserted with idempotency guarantees.

**Analytics Engine** — Five PostgreSQL-optimised query patterns: time-aggregated trends with configurable intervals (hourly/daily/weekly), multi-city comparison with percentage change tracking, z-score anomaly detection with rolling baselines, Pearson cross-metric correlation (e.g. PM2.5 vs temperature), and city rankings with minimum-sample thresholds.

**AI-Powered Insights** — Claude generates environmental briefings from pre-computed statistics. The pipeline separates deterministic analytics (PostgreSQL) from probabilistic narration (LLM) — the AI never does arithmetic, it interprets patterns. Results are cached with TTL-aligned to the ingestion interval.

**MCP Integration** — Four tools for Claude Desktop: query air quality, compare cities, detect anomalies, and generate AI briefings. Tools accept human-friendly city names, resolve them internally, and return formatted text optimised for LLM context windows.

**Security** — Rate limiting via slowapi (60 req/min GET, 20 req/min POST), OWASP security headers (CSP, X-Frame-Options, Referrer-Policy), tiered API key authentication with read/write/admin permissions, and per-request X-Request-ID tracing.

**Containerised Deployment** — Multi-stage Docker build (<200MB final image), docker-compose orchestration with PostgreSQL 15, health checks, non-root container user, and a graceful-shutdown entrypoint script.

## Tech Stack

| Category | Technology | Purpose |
|----------|-----------|---------|
| Framework | FastAPI | Async web framework with auto-generated OpenAPI 3.1 docs |
| Database | PostgreSQL 15 | Time-series storage with composite indexes and window functions |
| ORM | SQLAlchemy 2.0 (async) | Async ORM with asyncpg driver for non-blocking DB I/O |
| HTTP Client | httpx | Async client for OpenAQ and Open-Meteo API calls |
| Scheduler | APScheduler | In-process cron-like scheduling for data ingestion |
| Rate Limiting | slowapi | IP-based rate limiting with configurable thresholds |
| AI | Anthropic SDK | Claude API for generating environmental briefings |
| MCP | FastMCP | Model Context Protocol server for LLM tool integration |
| Containerisation | Docker | Multi-stage build with docker-compose orchestration |
| Testing | pytest + httpx | Async test suite with SQLite in-memory for speed |
| Deployment | Render | Cloud hosting with managed PostgreSQL |

## Quick Start

### Option 1: Docker (recommended)

```bash
git clone https://github.com/hari/urban-env-api.git
cd urban-env-api

cp .env.example .env
# Edit .env with your POSTGRES_PASSWORD, ANTHROPIC_API_KEY, etc.

docker compose up -d
# API available at http://localhost:8000/docs
```

### Option 2: Local Development

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env — ensure DATABASE_URL points to a running PostgreSQL instance

uvicorn main:app --reload
```

### Option 3: Live API

```bash
curl -H "X-Api-Key: your-key" \
  https://urban-env-api.onrender.com/api/v1/cities
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string (`postgresql+asyncpg://...`) |
| `ANTHROPIC_API_KEY` | Yes | — | Anthropic API key for Claude-powered insights |
| `API_KEY` | No | — | API key for `X-Api-Key` header authentication |
| `OPENAQ_API_KEY` | No | `""` | OpenAQ v3 API key (recommended to avoid rate limits) |
| `ENVIRONMENT` | No | `development` | `development`, `staging`, or `production` |
| `INGESTION_INTERVAL_MINUTES` | No | `60` | Background pipeline frequency |
| `RATE_LIMIT_PER_MINUTE` | No | `60` | Max requests per minute per client |
| `CACHE_TTL_SECONDS` | No | `3600` | AI insight cache lifetime (align with ingestion interval) |
| `AI_MAX_TOKENS` | No | `1024` | Max output tokens for AI insight generation |
| `DB_POOL_SIZE` | No | `5` | Persistent database connections |
| `DB_MAX_OVERFLOW` | No | `10` | Burst database connections above pool size |
| `CORS_ORIGINS` | No | `localhost:3000,...` | Comma-separated allowed origins |
| `HOST` | No | `0.0.0.0` | Uvicorn bind address |
| `PORT` | No | `8000` | Uvicorn port |

## API Endpoints

### Cities (CRUD)

| Method | Path | Description | Auth |
|--------|------|-------------|------|
| `GET` | `/api/v1/cities` | List all active cities (paginated) | Yes |
| `POST` | `/api/v1/cities` | Register a new city for monitoring | Yes |
| `GET` | `/api/v1/cities/{city_id}` | Get city details by slug | Yes |
| `PUT` | `/api/v1/cities/{city_id}` | Update city metadata (partial) | Yes |
| `DELETE` | `/api/v1/cities/{city_id}` | Soft-delete a city | Yes |

### Analytics

| Method | Path | Description | Auth |
|--------|------|-------------|------|
| `GET` | `/api/v1/analytics/trend/{city_id}` | Time-aggregated trend (hourly/daily/weekly) | Yes |
| `GET` | `/api/v1/analytics/compare` | Compare multiple cities by parameter | Yes |
| `GET` | `/api/v1/analytics/anomalies/{city_id}` | Z-score anomaly detection | Yes |
| `GET` | `/api/v1/analytics/correlation/{city_id}` | Pearson correlation between two parameters | Yes |
| `GET` | `/api/v1/analytics/ranking` | Rank cities by average parameter value | Yes |

### AI Insights

| Method | Path | Description | Auth |
|--------|------|-------------|------|
| `GET` | `/api/v1/ai/insight/{city_id}` | AI-generated environmental briefing | No |

### Infrastructure

| Method | Path | Description | Auth |
|--------|------|-------------|------|
| `GET` | `/health` | Service health, DB connectivity, data freshness | No |
| `GET` | `/docs` | Swagger UI (interactive documentation) | No |
| `GET` | `/redoc` | ReDoc (alternative documentation) | No |

## Example Requests

**Create a city:**

```bash
curl -X POST https://urban-env-api.onrender.com/api/v1/cities \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-key" \
  -d '{
    "name": "London",
    "country": "United Kingdom",
    "country_code": "GB",
    "latitude": 51.5074,
    "longitude": -0.1278,
    "timezone": "Europe/London"
  }'
```

```json
{
  "id": "london",
  "name": "London",
  "country": "United Kingdom",
  "country_code": "GB",
  "latitude": 51.5074,
  "longitude": -0.1278,
  "timezone": "Europe/London",
  "is_active": true,
  "created_at": "2026-02-27T10:00:00Z"
}
```

**Get air quality trend:**

```bash
curl -H "X-Api-Key: your-key" \
  "https://urban-env-api.onrender.com/api/v1/analytics/trend/london?parameter=pm25&days=7&interval=day"
```

```json
{
  "city_id": "london",
  "city_name": "London",
  "parameter": "pm25",
  "unit": "µg/m³",
  "interval": "day",
  "data": [
    {"period": "2026-02-20T00:00:00Z", "avg": 12.3, "min": 8.1, "max": 18.7, "reading_count": 24},
    {"period": "2026-02-21T00:00:00Z", "avg": 14.1, "min": 9.2, "max": 45.0, "reading_count": 24}
  ],
  "total_readings": 168
}
```

**Detect anomalies:**

```bash
curl -H "X-Api-Key: your-key" \
  "https://urban-env-api.onrender.com/api/v1/analytics/anomalies/london?parameter=pm25&sensitivity=2.0"
```

```json
{
  "city_id": "london",
  "city_name": "London",
  "parameter": "pm25",
  "unit": "µg/m³",
  "sensitivity": 2.0,
  "anomalies": [
    {
      "timestamp": "2026-02-20T19:00:00Z",
      "value": 45.0,
      "rolling_avg": 12.5,
      "z_score": 17.4,
      "severity": "high"
    }
  ],
  "total_readings_analysed": 720
}
```

**Get AI environmental insight:**

```bash
curl "https://urban-env-api.onrender.com/api/v1/ai/insight/london"
```

```json
{
  "city_id": "london",
  "city_name": "London",
  "insight": "London's air quality has deteriorated over the past week, with PM2.5 levels rising 18% to an average of 14.2 µg/m³...",
  "generated_at": "2026-02-27T10:00:00Z",
  "expires_at": "2026-02-27T11:00:00Z",
  "cached": false,
  "model_used": "claude-haiku-4-5-20251001",
  "data_summary": { "..." : "..." }
}
```

## MCP Integration

Add the Urban Environmental Intelligence server to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "urban-env": {
      "command": "python",
      "args": ["mcp_server.py"],
      "env": {
        "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost:5432/urban_env_db",
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

For streamable-http transport (web clients, MCP Inspector):

```bash
python mcp_server.py --transport http
```

### Available MCP Tools

| Tool | Description |
|------|-------------|
| `get_city_air_quality` | Air quality status and 7-day trend for a city (PM2.5, PM10, NO2, O3 + weather) |
| `compare_cities_pollution` | Side-by-side comparison of pollution levels across multiple cities with rankings |
| `detect_pollution_anomalies` | Z-score analysis to find unusual pollution events in the last 30 days |
| `get_ai_environmental_insight` | Claude-generated natural language briefing of environmental conditions |

## Testing

```bash
# Run all tests
pytest

# Unit tests only (pure logic, no DB)
pytest tests/unit/

# Integration tests (real HTTP via httpx → FastAPI → SQLite)
pytest tests/integration/

# With coverage report
pytest --cov=app --cov-report=html --cov-report=term-missing
```

The test suite uses **51 tests** across a unit/integration split. Integration tests use an in-memory SQLite database for speed (~50x faster than network PostgreSQL) while still exercising the full HTTP request/response cycle through httpx's `ASGITransport`. PostgreSQL-specific analytics SQL is tested via mocked service layers to avoid SQLite dialect incompatibilities.

## Project Structure

```
urban-env-api/
├── main.py                      # App entrypoint, lifespan, health check, router registration
├── mcp_server.py                # MCP server with 4 tools for Claude Desktop
├── requirements.txt             # Pinned dependencies
├── Dockerfile                   # Multi-stage production build
├── docker-compose.yml           # API + PostgreSQL orchestration
├── docker-compose.dev.yml       # Development override (live reload, DB port)
├── .env.example                 # Environment variable template
├── pytest.ini                   # Test configuration and markers
│
├── app/
│   ├── core/
│   │   ├── config.py            # Pydantic Settings (all env vars, validation)
│   │   ├── database.py          # Async SQLAlchemy engine + session factory
│   │   └── deps.py              # FastAPI dependencies (auth, pagination, time range)
│   │
│   ├── models/
│   │   └── models.py            # ORM models: City, Reading, AIInsight
│   │
│   ├── schemas/
│   │   └── schemas.py           # Pydantic request/response schemas
│   │
│   ├── repositories/
│   │   └── city_repository.py   # Data access layer (SQLAlchemy queries)
│   │
│   ├── routers/
│   │   ├── cities.py            # CRUD endpoints for city management
│   │   ├── analytics.py         # 5 analytical query endpoints
│   │   └── ai.py                # AI insight generation endpoint
│   │
│   ├── services/
│   │   ├── city_service.py      # Business logic for city operations
│   │   ├── analytics_service.py # PostgreSQL analytics (trend, anomaly, correlation)
│   │   ├── ai_service.py        # LLM pipeline: pre-aggregate → prompt → cache
│   │   └── ingestion_service.py # Background data pipeline (OpenAQ + Open-Meteo)
│   │
│   └── middleware/
│       ├── rate_limiter.py      # slowapi rate limiting (IP extraction, 429 handler)
│       ├── security_headers.py  # OWASP headers (CSP, HSTS, X-Frame-Options)
│       └── request_logger.py    # Structured logging with X-Request-ID tracing
│
├── scripts/
│   └── docker-entrypoint.sh     # Postgres readiness check + graceful startup
│
└── tests/
    ├── conftest.py              # Fixtures: SQLite engine, app, client, seed data
    ├── unit/                    # Pure function tests (validation, parsing, retry logic)
    └── integration/             # HTTP contract tests (cities CRUD, analytics, middleware)
```

## Data Pipeline

The ingestion pipeline runs on a configurable schedule (default: every 60 minutes) via APScheduler's `AsyncIOScheduler`, which operates in-process alongside the FastAPI server.

For each active city, the pipeline: fetches air quality readings from OpenAQ v3 and weather observations from Open-Meteo, normalises units to canonical forms (µg/m³ for pollutants, °C for temperature), validates each reading against physical bounds (e.g. PM2.5: 0–500 µg/m³, temperature: -90–60°C), assigns quality flags (`valid`, `suspect`, or `missing`), and performs a bulk upsert with a unique constraint on `(city_id, parameter, timestamp)` to guarantee idempotency.

Both raw and normalised values are preserved for auditability. The pipeline includes exponential backoff with jitter for external API failures (HTTP 429/5xx) and structured logging of per-city ingestion statistics.

> **Single-worker constraint:** Because APScheduler runs in the same process as uvicorn, the server must use `--workers 1`. Multiple workers would spawn duplicate schedulers. For horizontal scaling, deploy multiple replicas behind a load balancer with only one running the scheduler.

## Docker Deployment

```bash
# Quick start
cp .env.example .env && docker compose up -d

# Development mode (live reload + DB port exposed)
docker compose -f docker-compose.yml -f docker-compose.dev.yml up

# View logs
docker compose logs -f api

# Stop (data persists)
docker compose down

# Stop and delete all data
docker compose down -v
```

## License

MIT

## Acknowledgements

- [OpenAQ](https://openaq.org/) — Open air quality data from thousands of monitoring stations worldwide
- [Open-Meteo](https://open-meteo.com/) — Free weather API with global coverage and no key required
- [Anthropic](https://www.anthropic.com/) — Claude API for AI-powered environmental insights and the MCP protocol
- University of Leeds — School of Computing, COMP3011 Web Services and Web Data

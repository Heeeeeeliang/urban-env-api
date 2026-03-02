"""
Integration Tests — Cities CRUD Endpoints
============================================

These tests exercise the full HTTP request/response cycle:
  httpx client → FastAPI router → service layer → SQLAlchemy → SQLite

Each test uses the `client` and `seed_data` fixtures from conftest.py,
giving it:
  - A fresh in-memory database with known state (2 cities, 50 readings).
  - An httpx AsyncClient wired to the FastAPI app via ASGITransport.
  - Complete isolation from other tests (function-scoped fixtures).

NOTE ON CITY IDs:
  The API uses URL-safe slug identifiers (e.g. "london"), not numeric IDs.
  The slug is auto-generated from the city name during creation. Test
  assertions use these string slugs, not integers.

NOTE ON SOFT DELETE:
  DELETE /cities/{id} sets is_active=False. Subsequent GET requests
  return 404 because get_city filters out inactive cities. The data
  remains in the database for historical integrity.
"""

import pytest


# ===================================================================
# 1. CREATE — POST /api/v1/cities
# ===================================================================


@pytest.mark.asyncio
async def test_create_city_valid(client, seed_data):
    """
    POST with a complete, valid body returns 201 with the created city.

    The response must contain the server-generated slug ID (derived
    from the name) and echo back all provided fields. This verifies
    the full pipeline: schema validation → service logic → DB insert
    → response serialisation.
    """
    payload = {
        "name": "Birmingham",
        "country": "United Kingdom",
        "country_code": "GB",
        "latitude": 52.4862,
        "longitude": -1.8904,
        "timezone": "Europe/London",
    }

    resp = await client.post("/api/v1/cities", json=payload)

    assert resp.status_code == 201
    body = resp.json()
    assert body["id"] == "birmingham"
    assert body["name"] == "Birmingham"
    assert body["country"] == "United Kingdom"
    assert body["country_code"] == "GB"
    assert body["is_active"] is True


@pytest.mark.asyncio
async def test_create_city_duplicate_name_returns_409(client, seed_data):
    """
    POST with a name that already exists returns 409 Conflict.

    "London" is in seed_data. Attempting to create another "London"
    should fail with a clear error, not a 500 from a DB unique constraint
    violation. The service layer catches this before it reaches the DB.
    """
    payload = {
        "name": "London",
        "country": "United Kingdom",
        "country_code": "GB",
        "latitude": 51.5074,
        "longitude": -0.1278,
        "timezone": "Europe/London",
    }

    resp = await client.post("/api/v1/cities", json=payload)

    assert resp.status_code == 409
    assert "error" in resp.json()["detail"]


# ===================================================================
# 2. READ — GET /api/v1/cities/{city_id}
# ===================================================================


@pytest.mark.asyncio
async def test_get_city_exists(client, seed_data):
    """
    GET with a valid city slug returns 200 and the full city object.

    Verifies that seed_data's London is accessible and that the
    response schema includes all expected fields.
    """
    resp = await client.get("/api/v1/cities/london")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == "london"
    assert body["name"] == "London"
    assert body["country_code"] == "GB"
    assert "latitude" in body
    assert "longitude" in body


@pytest.mark.asyncio
async def test_get_city_not_found_returns_404(client, seed_data):
    """
    GET with a nonexistent slug returns 404, not 500.

    This confirms the service layer's CityNotFoundError is correctly
    mapped to an HTTP 404 by the router, with a structured error body.
    """
    resp = await client.get("/api/v1/cities/atlantis")

    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "not_found"


# ===================================================================
# 3. UPDATE — PUT /api/v1/cities/{city_id}
# ===================================================================


@pytest.mark.asyncio
async def test_update_city_name(client, seed_data):
    """
    PUT with a new name returns 200 and the updated city.

    The API uses PUT with optional fields (partial update semantics).
    Only the name is sent; all other fields should remain unchanged.
    """
    resp = await client.put(
        "/api/v1/cities/london",
        json={"name": "Greater London"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "Greater London"
    # Other fields unchanged
    assert body["country_code"] == "GB"


# ===================================================================
# 4. DELETE — DELETE /api/v1/cities/{city_id}
# ===================================================================


@pytest.mark.asyncio
async def test_delete_city_returns_204(client, seed_data):
    """
    DELETE on an existing city returns 204 No Content.

    This is a soft delete — the city is deactivated, not removed.
    The response body is empty per REST convention (RFC 7231).

    We delete London to test the soft delete contract. Although London
    has FK-linked readings in seed_data, soft delete only sets
    is_active=False — it does not remove the row, so no FK violation
    occurs.
    """
    resp = await client.delete("/api/v1/cities/london")

    assert resp.status_code == 204
    assert resp.content == b""


@pytest.mark.asyncio
async def test_get_city_after_delete_returns_404(client, seed_data):
    """
    GET after DELETE returns 404 — soft-deleted cities are invisible.

    This is the critical behavioural test for the soft delete pattern:
    the city still exists in the database (is_active=False) but the
    API treats it as nonexistent. This two-step test (DELETE then GET)
    verifies the end-to-end contract.
    """
    # Step 1: delete
    delete_resp = await client.delete("/api/v1/cities/london")
    assert delete_resp.status_code == 204

    # Step 2: verify gone
    get_resp = await client.get("/api/v1/cities/london")
    assert get_resp.status_code == 404

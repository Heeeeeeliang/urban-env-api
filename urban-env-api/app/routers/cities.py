"""
Cities Router — Layer 3: HTTP Interface
=========================================

Architectural decisions documented here:

1. ROUTER AS TRANSLATION LAYER
   The router has exactly one job: translate between HTTP and the service
   layer. It translates:
     - HTTP requests → Pydantic schemas → service method calls.
     - Service return values → Pydantic response schemas → JSON.
     - Domain exceptions → HTTP status codes with consistent error bodies.

   If you find business logic creeping into a route handler (e.g. checking
   for duplicate names), it belongs in the service layer. If you find SQL
   in a route handler, it belongs in the repository.

2. EXCEPTION MAPPING PATTERN
   Each route handler catches domain-specific exceptions from the service
   layer and translates them to HTTP responses:
     - CityNotFoundError    → 404 Not Found
     - DuplicateCityError   → 409 Conflict
     - InvalidCityDataError → 422 Unprocessable Entity

   This mapping lives exclusively in the router. The service layer never
   imports HTTPException and never knows about HTTP status codes.

3. RESPONSE MODEL ENFORCEMENT
   Every endpoint declares response_model=CityResponse or
   response_model=PaginatedCityResponse. This does two things:
     (a) FastAPI auto-generates accurate OpenAPI response schemas.
     (b) FastAPI filters the response through the Pydantic model,
         stripping any fields that shouldn't be exposed. If someone
         adds a `password_hash` field to the City ORM model, it won't
         leak into API responses unless explicitly added to CityResponse.

4. STATUS CODES AS EXPLICIT DECLARATIONS
   Each endpoint declares its status_code in the decorator, not by
   returning a Response object. This ensures the OpenAPI spec accurately
   documents what each endpoint returns, even before the implementation
   is tested.

5. CONSISTENT ERROR RESPONSE FORMAT
   All error responses follow the ErrorResponse schema:
     {"error": "machine_readable_code", "message": "Human-readable description"}
   This is documented via the `responses` parameter in each decorator,
   which adds these error schemas to the OpenAPI spec.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.deps import DbSession, PaginationParams, verify_api_key
from app.schemas.schemas import (
    CityCreate,
    CityUpdate,
    CityResponse,
    PaginatedCityResponse,
    ErrorResponse,
)
from app.services.city_service import (
    CityService,
    CityNotFoundError,
    DuplicateCityError,
    InvalidCityDataError,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Router Configuration
#
# prefix="" because the /api/v1 prefix is applied in main.py when the
# router is registered. This keeps the router version-agnostic — it can
# be mounted at /api/v1 or /api/v2 without changes.
#
# tags=["Cities"] groups these endpoints in the Swagger UI sidebar,
# making the auto-generated docs navigable for API consumers.
# ---------------------------------------------------------------------------
router = APIRouter(
    prefix="/cities",
    tags=["Cities"],
    dependencies=[Depends(verify_api_key)],
)


def get_city_service() -> CityService:
    """
    Factory for CityService.

    Why a factory function instead of a module-level singleton?
      - Testability: tests can override this dependency to inject a mock.
      - Lifecycle clarity: the service is created per-request, not at
        import time. This matters if the service ever holds state.
      - Consistency: follows the same Depends() pattern as get_db.

    For a stateless service like CityService, a singleton would also work.
    We use the factory pattern for consistency with how we'd handle
    stateful services (e.g. one that holds an httpx.AsyncClient).
    """
    return CityService()


# ---------------------------------------------------------------------------
# POST /cities — Create a new city
# ---------------------------------------------------------------------------
@router.post(
    "",
    response_model=CityResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new monitored city",
    description=(
        "Register a new city for environmental monitoring. "
        "The city name must be unique (case-insensitive). "
        "A URL-safe slug ID is auto-generated from the city name. "
        "The city will immediately become eligible for data ingestion "
        "in the next scheduled cycle."
    ),
    responses={
        201: {
            "description": "City created successfully",
            "model": CityResponse,
        },
        409: {
            "description": "A city with this name already exists",
            "model": ErrorResponse,
        },
        422: {
            "description": "Invalid input data (validation error)",
            "model": ErrorResponse,
        },
    },
)
async def create_city(
    data: CityCreate,
    db: DbSession,
    service: CityService = Depends(get_city_service),
) -> CityResponse:
    """
    Create a new city for environmental monitoring.

    The slug ID is generated from the city name:
    - "London" → "london"
    - "New York" → "new-york"

    The city name must be unique (case-insensitive comparison).
    """
    try:
        city = await service.create_city(db, data)
        return CityResponse.model_validate(city)

    except DuplicateCityError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "duplicate_city", "message": str(e)},
        )
    except InvalidCityDataError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"error": "invalid_data", "message": str(e)},
        )


# ---------------------------------------------------------------------------
# GET /cities — List all cities with pagination
# ---------------------------------------------------------------------------
@router.get(
    "",
    response_model=PaginatedCityResponse,
    status_code=status.HTTP_200_OK,
    summary="List all monitored cities",
    description=(
        "Retrieve a paginated list of all active cities being monitored. "
        "Includes pagination metadata (total count, skip, limit) to support "
        "client-side pagination controls. Results are ordered alphabetically "
        "by city name for deterministic pagination."
    ),
    responses={
        200: {
            "description": "Paginated list of cities",
            "model": PaginatedCityResponse,
        },
    },
)
async def list_cities(
    db: DbSession,
    pagination: PaginationParams = Depends(),
    service: CityService = Depends(get_city_service),
) -> PaginatedCityResponse:
    """
    List all active cities with pagination.

    Use `skip` and `limit` query parameters to paginate:
    - `GET /cities?skip=0&limit=10` → first 10 cities
    - `GET /cities?skip=10&limit=10` → next 10 cities
    """
    cities, total = await service.list_cities(
        db, skip=pagination.skip, limit=pagination.limit
    )

    return PaginatedCityResponse(
        items=[CityResponse.model_validate(c) for c in cities],
        total=total,
        skip=pagination.skip,
        limit=pagination.limit,
    )


# ---------------------------------------------------------------------------
# GET /cities/{city_id} — Retrieve a single city
# ---------------------------------------------------------------------------
@router.get(
    "/{city_id}",
    response_model=CityResponse,
    status_code=status.HTTP_200_OK,
    summary="Get a city by ID",
    description=(
        "Retrieve detailed information for a single city by its slug ID. "
        "Returns 404 if the city does not exist or has been deactivated."
    ),
    responses={
        200: {
            "description": "City details",
            "model": CityResponse,
        },
        404: {
            "description": "City not found",
            "model": ErrorResponse,
        },
    },
)
async def get_city(
    city_id: str,
    db: DbSession,
    service: CityService = Depends(get_city_service),
) -> CityResponse:
    """
    Get city details by slug ID.

    Example: `GET /cities/london` returns details for London.
    """
    try:
        city = await service.get_city(db, city_id)
        return CityResponse.model_validate(city)

    except CityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "not_found",
                "message": f"City with id '{city_id}' not found",
            },
        )


# ---------------------------------------------------------------------------
# PUT /cities/{city_id} — Update a city
# ---------------------------------------------------------------------------
@router.put(
    "/{city_id}",
    response_model=CityResponse,
    status_code=status.HTTP_200_OK,
    summary="Update a city",
    description=(
        "Update one or more fields of an existing city. Only include fields "
        "you want to change — unspecified fields remain unchanged. "
        "If the name is changed, uniqueness is re-validated. "
        "Returns the full updated city object."
    ),
    responses={
        200: {
            "description": "City updated successfully",
            "model": CityResponse,
        },
        404: {
            "description": "City not found",
            "model": ErrorResponse,
        },
        409: {
            "description": "Updated name conflicts with an existing city",
            "model": ErrorResponse,
        },
        422: {
            "description": "Invalid update data",
            "model": ErrorResponse,
        },
    },
)
async def update_city(
    city_id: str,
    data: CityUpdate,
    db: DbSession,
    service: CityService = Depends(get_city_service),
) -> CityResponse:
    """
    Partially update a city.

    Send only the fields you want to change:
    ```json
    {"name": "Greater London"}
    ```
    Fields not included in the request body are left unchanged.
    """
    try:
        city = await service.update_city(db, city_id, data)
        return CityResponse.model_validate(city)

    except CityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "not_found",
                "message": f"City with id '{city_id}' not found",
            },
        )
    except DuplicateCityError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "duplicate_city", "message": str(e)},
        )
    except InvalidCityDataError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"error": "invalid_data", "message": str(e)},
        )


# ---------------------------------------------------------------------------
# DELETE /cities/{city_id} — Soft-delete a city
# ---------------------------------------------------------------------------
@router.delete(
    "/{city_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a city",
    description=(
        "Soft-delete a city by deactivating it. The city and its historical "
        "readings are preserved in the database but excluded from all API "
        "responses and future ingestion cycles. This operation is reversible "
        "at the database level (set is_active=true) but not via the API."
    ),
    responses={
        204: {
            "description": "City deleted successfully (no response body)",
        },
        404: {
            "description": "City not found",
            "model": ErrorResponse,
        },
    },
)
async def delete_city(
    city_id: str,
    db: DbSession,
    service: CityService = Depends(get_city_service),
) -> None:
    """
    Soft-delete a city.

    The city is deactivated, not permanently removed. Historical data
    is preserved for integrity. Returns 204 with no response body on success.

    Why 204 and not 200 with the deleted entity?
      RFC 7231 recommends 204 for DELETE operations where no response body
      is needed. Returning the deleted entity in a 200 is also valid but
      less conventional. We follow the common REST convention.
    """
    try:
        await service.delete_city(db, city_id)
        # 204 No Content — FastAPI handles the empty response automatically
        # when the function returns None with status_code=204.

    except CityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "not_found",
                "message": f"City with id '{city_id}' not found",
            },
        )

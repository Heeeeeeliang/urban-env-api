"""
City Service — Layer 2: Business Logic
========================================

Architectural decisions documented here:

1. SERVICE LAYER RESPONSIBILITY
   The service layer sits between routers and repositories. It owns
   business rules — the logic that would survive a framework migration.
   If we moved from FastAPI to Django or even a CLI tool, the service
   layer would be reusable unchanged.

   What lives HERE (business rules):
     - "No two active cities can have the same name."
     - "Country code must be 2 uppercase letters."
     - "Slug generation follows a specific algorithm."

   What does NOT live here:
     - HTTP status codes or response formatting (router layer).
     - SQL queries or ORM operations (repository layer).
     - Input validation like "latitude must be between -90 and 90" (schema layer).

2. CUSTOM EXCEPTIONS (not HTTPException)
   The service layer raises domain-specific exceptions (DuplicateCityError,
   CityNotFoundError), NOT FastAPI's HTTPException. This is critical:
     - HTTPException is a framework concept. The service layer shouldn't
       know it's being called from an HTTP API — it might be called from
       a CLI tool, a background task, or a test.
     - The router catches domain exceptions and translates them to HTTP
       responses. This keeps the mapping between business errors and HTTP
       status codes in one place (the router).

3. TRANSACTION MANAGEMENT
   The service layer calls await db.commit() after successful operations.
   This is a deliberate choice:
     - The repository uses flush() (writes to DB but doesn't commit).
     - The service composes multiple repository calls if needed, then
       commits once everything succeeds.
     - If any step fails, the session's exception handler (in get_db)
       rolls back everything.
   This gives us atomic multi-step operations without explicit
   transaction management.
"""

import logging
import re
from typing import Optional, Sequence

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import City
from app.repositories.city_repository import CityRepository
from app.schemas.schemas import CityCreate, CityUpdate

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain Exceptions
#
# These are business logic errors, not HTTP errors. The router layer
# translates them into appropriate HTTP responses.
#
# Why not just use ValueError?
#   Catching ValueError is too broad — it would also catch unrelated
#   errors from third-party libraries. Custom exceptions make the
#   error handling in routers precise and self-documenting.
# ---------------------------------------------------------------------------


class CityNotFoundError(Exception):
    """Raised when a requested city does not exist or is inactive."""

    def __init__(self, city_id: str):
        self.city_id = city_id
        super().__init__(f"City with id '{city_id}' not found")


class DuplicateCityError(Exception):
    """Raised when attempting to create a city with a name that already exists."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(f"A city named '{name}' already exists")


class InvalidCityDataError(Exception):
    """Raised when city data fails business validation rules."""

    def __init__(self, message: str):
        super().__init__(message)


class CityService:
    """
    Business logic for city management.

    Orchestrates repository operations with validation rules.
    All public methods take an AsyncSession as parameter, following
    the same pattern as the repository for consistency and testability.
    """

    def __init__(self):
        self._repo = CityRepository()

    # -------------------------------------------------------------------
    # CREATE
    # -------------------------------------------------------------------

    async def create_city(
        self,
        db: AsyncSession,
        data: CityCreate,
    ) -> City:
        """
        Create a new city after validating business rules.

        Business rules enforced:
          1. No duplicate names (case-insensitive).
          2. Country code is 2 uppercase letters (also in schema, but
             defence in depth — the schema might be bypassed by internal
             callers).
          3. Generated slug must not collide with an existing city ID.

        Why check slug collision separately from name duplication?
          "São Paulo" and "Sao Paulo" generate the same slug ("sao-paulo")
          but have different names. The name check catches exact duplicates;
          the slug check catches transliteration collisions.
        """
        # Rule 1: Check for duplicate name
        existing = await self._repo.get_by_name(db, data.name)
        if existing:
            logger.warning(
                "Duplicate city creation attempted: '%s' (existing id: '%s')",
                data.name,
                existing.id,
            )
            raise DuplicateCityError(data.name)

        # Rule 2: Validate country code (defence in depth)
        self._validate_country_code(data.country_code)

        # Rule 3: Generate slug and check for collision
        slug = data.generate_slug()
        if not slug:
            raise InvalidCityDataError(
                f"Cannot generate a valid URL slug from city name '{data.name}'"
            )

        existing_by_id = await self._repo.get_by_id(
            db, slug, include_inactive=True
        )
        if existing_by_id:
            raise DuplicateCityError(
                f"A city with slug '{slug}' already exists "
                f"(from city name '{existing_by_id.name}')"
            )

        # Build the ORM entity
        city = City(
            id=slug,
            name=data.name.strip(),
            country=data.country.strip(),
            country_code=data.country_code.upper(),
            latitude=data.latitude,
            longitude=data.longitude,
            timezone=data.timezone,
        )

        city = await self._repo.create(db, city)
        await db.commit()

        logger.info("Created city: id='%s', name='%s'", city.id, city.name)
        return city

    # -------------------------------------------------------------------
    # READ
    # -------------------------------------------------------------------

    async def get_city(self, db: AsyncSession, city_id: str) -> City:
        """
        Retrieve a single city by ID.

        Raises CityNotFoundError if the city doesn't exist or is inactive.
        The router catches this and returns a 404.
        """
        city = await self._repo.get_by_id(db, city_id)
        if not city:
            raise CityNotFoundError(city_id)
        return city

    async def list_cities(
        self,
        db: AsyncSession,
        *,
        skip: int = 0,
        limit: int = 100,
    ) -> tuple[Sequence[City], int]:
        """
        Retrieve a paginated list of active cities.

        Returns (cities, total_count) for pagination metadata.
        No business rules to enforce here — just a pass-through to
        the repository with default filtering (active only).
        """
        return await self._repo.get_all(db, skip=skip, limit=limit)

    # -------------------------------------------------------------------
    # UPDATE
    # -------------------------------------------------------------------

    async def update_city(
        self,
        db: AsyncSession,
        city_id: str,
        data: CityUpdate,
    ) -> City:
        """
        Update an existing city with partial data.

        Business rules enforced:
          1. City must exist and be active.
          2. If name is changing, the new name must not duplicate an
             existing city.
          3. If country_code is changing, it must be valid.

        model_dump(exclude_unset=True) is the key Pydantic v2 method here:
        it returns only the fields the consumer explicitly set, distinguishing
        between "field not sent" and "field sent as None". This prevents
        accidentally overwriting fields with None.
        """
        # Fetch existing city (raises CityNotFoundError if missing)
        city = await self.get_city(db, city_id)

        # Extract only the fields that were explicitly provided
        update_data = data.model_dump(exclude_unset=True)

        if not update_data:
            # Nothing to update — return the city as-is.
            # This is not an error; it's idempotent.
            return city

        # Rule 2: If renaming, check for duplicates
        if "name" in update_data and update_data["name"] != city.name:
            existing = await self._repo.get_by_name(db, update_data["name"])
            if existing and existing.id != city_id:
                raise DuplicateCityError(update_data["name"])

        # Rule 3: If changing country_code, validate format
        if "country_code" in update_data:
            self._validate_country_code(update_data["country_code"])

        city = await self._repo.update(db, city, update_data)
        await db.commit()

        logger.info("Updated city: id='%s', fields=%s", city_id, list(update_data.keys()))
        return city

    # -------------------------------------------------------------------
    # DELETE
    # -------------------------------------------------------------------

    async def delete_city(self, db: AsyncSession, city_id: str) -> City:
        """
        Soft-delete a city by setting is_active=False.

        The city and its readings remain in the database for historical
        data integrity. The city is simply excluded from future API
        responses and ingestion cycles.

        Returns the deactivated city for the router to confirm success.
        """
        city = await self.get_city(db, city_id)

        city = await self._repo.soft_delete(db, city)
        await db.commit()

        logger.info("Soft-deleted city: id='%s', name='%s'", city.id, city.name)
        return city

    # -------------------------------------------------------------------
    # Private validation helpers
    # -------------------------------------------------------------------

    @staticmethod
    def _validate_country_code(code: str) -> None:
        """
        Validate ISO 3166-1 alpha-2 country code format.

        This is duplicated from the Pydantic schema validator intentionally
        (defence in depth). The schema validates consumer input; this method
        validates data from any caller, including internal services and
        background tasks that might bypass the schema layer.
        """
        if not re.match(r"^[A-Z]{2}$", code):
            raise InvalidCityDataError(
                f"Invalid country code '{code}': "
                "must be exactly 2 uppercase letters (ISO 3166-1 alpha-2)"
            )

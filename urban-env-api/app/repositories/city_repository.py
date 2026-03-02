"""
City Repository — Layer 1: Data Access
========================================

Architectural decisions documented here:

1. REPOSITORY PATTERN (not raw queries in route handlers)
   The repository is the ONLY layer that knows about SQLAlchemy. It
   translates between ORM objects and database operations. This means:
     - Route handlers never import sqlalchemy.
     - Service layer never writes SQL.
     - If we swapped PostgreSQL for MongoDB (unlikely but illustrative),
       only this file changes.

   The repository is intentionally "dumb" — it performs CRUD operations
   without enforcing business rules. It doesn't know that duplicate city
   names are forbidden, or that country codes must be uppercase. Those
   rules live in the service layer.

2. SESSION AS PARAMETER (not injected via constructor)
   Each repository method takes an AsyncSession as its first argument
   rather than storing a session as an instance variable. This is
   deliberate:
     - It makes the transaction boundary explicit. The caller controls
       when to commit/rollback, not the repository.
     - It prevents the "session leak" antipattern where a repository
       holds a reference to a session that outlives the request.
     - It enables composing multiple repository calls within a single
       transaction (e.g. create a city, then create its initial readings).

3. RETURNING ORM OBJECTS (not dicts or schemas)
   Repository methods return SQLAlchemy ORM instances, not Pydantic
   models or dicts. The conversion to response schemas happens in the
   router layer. This keeps each layer's responsibility clean:
     - Repository: ORM ↔ Database
     - Service: ORM ↔ Business rules
     - Router: ORM → Pydantic → JSON

4. SELECT THEN ACT (not blind writes)
   Update and delete methods fetch the entity first, then modify it.
   This is slightly less efficient than a blind UPDATE ... WHERE id = $1
   (two queries instead of one), but it:
     - Returns the updated/deleted entity for the response.
     - Enables the service layer to inspect the entity before modification.
     - Produces a clean 404 when the entity doesn't exist (vs. silently
       affecting zero rows).
"""

from typing import Optional, Sequence

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import City


class CityRepository:
    """
    Data access layer for City entities.

    All methods are stateless and take an AsyncSession as parameter.
    No business logic — just CRUD operations.
    """

    # -------------------------------------------------------------------
    # CREATE
    # -------------------------------------------------------------------

    async def create(self, db: AsyncSession, city: City) -> City:
        """
        Persist a new City entity to the database.

        The caller is responsible for constructing the City ORM object
        (including generating the slug ID). The repository just persists it.

        Note: We call flush() instead of commit(). flush() sends the INSERT
        to the database (making it visible within this transaction) but
        doesn't commit. The actual commit happens in the route handler's
        finally block or explicitly after all operations succeed.

        Why? Because the service layer might need to perform additional
        operations after creating the city (e.g. scheduling an initial
        data ingestion). If we committed here and the subsequent operation
        failed, we'd have an orphaned city with no readings.

        refresh() reloads the object from the database, populating
        server-generated fields (created_at, updated_at, server_default values).
        """
        db.add(city)
        await db.flush()
        await db.refresh(city)
        return city

    # -------------------------------------------------------------------
    # READ — single entity
    # -------------------------------------------------------------------

    async def get_by_id(
        self,
        db: AsyncSession,
        city_id: str,
        *,
        include_inactive: bool = False,
    ) -> Optional[City]:
        """
        Retrieve a city by its slug ID.

        The include_inactive flag defaults to False, meaning soft-deleted
        cities are invisible by default. Admin endpoints can pass
        include_inactive=True to see everything.

        Why `Optional[City]` instead of raising an exception?
          The repository doesn't know whether a missing city is an error.
          In a GET /cities/{id} context, it's a 404. In a "check if exists
          before creating" context, None is the expected happy path.
          The service layer makes that determination.
        """
        stmt = select(City).where(City.id == city_id)

        if not include_inactive:
            stmt = stmt.where(City.is_active == True)  # noqa: E712

        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_name(
        self,
        db: AsyncSession,
        name: str,
        *,
        include_inactive: bool = False,
    ) -> Optional[City]:
        """
        Retrieve a city by its display name (case-insensitive).

        Used by the service layer to check for duplicate names before
        creating a new city. Case-insensitive comparison prevents
        "London" and "london" from coexisting.

        func.lower() pushes the lowercase comparison to PostgreSQL,
        which is more efficient than loading all cities and comparing
        in Python. For large datasets, a functional index on lower(name)
        would optimise this further.
        """
        stmt = select(City).where(func.lower(City.name) == name.lower())

        if not include_inactive:
            stmt = stmt.where(City.is_active == True)  # noqa: E712

        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    # -------------------------------------------------------------------
    # READ — collection
    # -------------------------------------------------------------------

    async def get_all(
        self,
        db: AsyncSession,
        *,
        skip: int = 0,
        limit: int = 100,
        include_inactive: bool = False,
    ) -> tuple[Sequence[City], int]:
        """
        Retrieve a paginated list of cities with total count.

        Returns a tuple of (cities, total_count) to support pagination
        metadata in the API response. The total count is fetched in a
        separate query because SQL doesn't provide both the rows and
        a count of all matching rows in a single SELECT efficiently.

        Why two queries instead of window functions (COUNT(*) OVER())?
          Window functions compute the count for every row, which means
          PostgreSQL must scan the entire result set even when you only
          want 10 rows. For our low-cardinality cities table (<50 rows),
          the difference is negligible. For the readings table (500k+ rows),
          separate queries would be the correct choice. We use the same
          pattern here for consistency.

        Ordering by name provides deterministic pagination. Without an
        ORDER BY, PostgreSQL returns rows in an undefined order that can
        change between queries, causing items to appear on multiple pages
        or be skipped entirely.
        """
        # Count query
        count_stmt = select(func.count(City.id))
        if not include_inactive:
            count_stmt = count_stmt.where(City.is_active == True)  # noqa: E712
        total = (await db.execute(count_stmt)).scalar_one()

        # Data query
        data_stmt = select(City).order_by(City.name).offset(skip).limit(limit)
        if not include_inactive:
            data_stmt = data_stmt.where(City.is_active == True)  # noqa: E712

        result = await db.execute(data_stmt)
        cities = result.scalars().all()

        return cities, total

    # -------------------------------------------------------------------
    # UPDATE
    # -------------------------------------------------------------------

    async def update(
        self,
        db: AsyncSession,
        city: City,
        update_data: dict,
    ) -> City:
        """
        Apply a partial update to an existing City entity.

        Takes a dict of {field_name: new_value} and sets each attribute
        on the ORM instance. Only fields present in update_data are
        modified — this enables partial updates.

        The caller (service layer) is responsible for:
          1. Fetching the city via get_by_id (returns None → 404).
          2. Validating the update_data (e.g. no duplicate names).
          3. Passing only validated fields to this method.

        Why setattr() instead of a SQL UPDATE statement?
          setattr on the ORM instance lets SQLAlchemy's unit-of-work
          pattern track exactly which columns changed. The generated UPDATE
          statement includes only modified columns, which is more efficient
          and produces cleaner audit logs.
        """
        for field, value in update_data.items():
            setattr(city, field, value)

        await db.flush()
        await db.refresh(city)
        return city

    # -------------------------------------------------------------------
    # DELETE (soft delete)
    # -------------------------------------------------------------------

    async def soft_delete(self, db: AsyncSession, city: City) -> City:
        """
        Soft-delete a city by setting is_active=False.

        Why soft delete instead of DELETE FROM?
          The readings table has a RESTRICT foreign key to cities.
          A hard delete would either:
            (a) Fail with a FK violation (if RESTRICT), losing the
                consumer's intent.
            (b) Cascade-delete thousands of readings (if CASCADE),
                causing irreversible data loss.
          Soft delete preserves data integrity while removing the city
          from active API responses.

        The returned city (with is_active=False) is used by the router
        to confirm the operation succeeded.
        """
        city.is_active = False
        await db.flush()
        await db.refresh(city)
        return city

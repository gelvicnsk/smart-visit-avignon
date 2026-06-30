from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class BaseRepository(ABC, Generic[T]):
    """Contract that every domain repository must implement.

    Concrete implementations (Phase 4) will inject the relevant
    database client (MongoDB collection, Neo4j session, Redis hash)
    via FastAPI's dependency injection system.
    """

    @abstractmethod
    async def find_one(self, id: str) -> T | None:
        """Return the entity matching *id*, or None if not found."""

    @abstractmethod
    async def find_many(self, filters: dict) -> list[T]:
        """Return all entities matching *filters*."""

    @abstractmethod
    async def insert(self, entity: T) -> str:
        """Persist *entity* and return its generated string ID."""

    @abstractmethod
    async def update(self, id: str, data: dict) -> bool:
        """Apply *data* patch to the entity. Returns True if found."""

    @abstractmethod
    async def delete(self, id: str) -> bool:
        """Remove the entity. Returns True if it existed."""

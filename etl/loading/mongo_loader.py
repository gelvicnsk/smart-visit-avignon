"""MongoDB loader — upserts curated data into the smartvisit database.

Collections written:
  events, venues, parkings, bike_stations, bus_stops

Each document is upserted on its ``id`` field using pymongo bulk_write.
"""

from __future__ import annotations

from typing import Any

from pymongo import MongoClient, ReplaceOne

from etl.config import ETLConfig
from etl.loading.base_loader import BaseLoader

# Ordered list of (collection_name, data_key) for the run() convenience method
_COLLECTIONS = [
    "events",
    "venues",
    "parkings",
    "bike_stations",
    "bus_stops",
]


class MongoDBLoader(BaseLoader):
    """Upserts curated records into MongoDB.

    Connection is lazy — established on the first call to :meth:`upsert`.

    Usage::

        with MongoDBLoader() as loader:
            loader.run(events, venues, parkings, bike_stations, bus_stops)
    """

    def __init__(self, config: ETLConfig | None = None) -> None:
        super().__init__(config)
        self._client: MongoClient | None = None

    # ── Connection management ───────────────────────────────────────────────────

    @property
    def _db(self):
        if self._client is None:
            self._client = MongoClient(
                self.config.mongo_uri,
                serverSelectionTimeoutMS=5_000,
            )
        return self._client[self.config.mongo_db]

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # ── Public API ──────────────────────────────────────────────────────────────

    @staticmethod
    def _build_operations(data: list[dict[str, Any]]) -> list[ReplaceOne]:
        """Build a list of pymongo ReplaceOne upsert operations."""
        return [ReplaceOne({"id": doc["id"]}, doc, upsert=True) for doc in data]

    def upsert(self, collection_name: str, data: list[dict[str, Any]]) -> int:
        """Upsert *data* into *collection_name*, keyed by the ``id`` field.

        Returns:
            Total number of documents inserted or modified.
        """
        if not data:
            self.logger.info(
                "mongo.upsert_skipped", collection=collection_name, reason="empty"
            )
            return 0

        operations = self._build_operations(data)
        result = self._db[collection_name].bulk_write(operations, ordered=False)
        count = result.upserted_count + result.modified_count
        self.logger.info(
            "mongo.upserted",
            collection=collection_name,
            inserted=result.upserted_count,
            modified=result.modified_count,
        )
        return count

    def run(
        self,
        events: list[dict[str, Any]],
        venues: list[dict[str, Any]],
        parkings: list[dict[str, Any]],
        bike_stations: list[dict[str, Any]],
        bus_stops: list[dict[str, Any]],
    ) -> int:
        """Upsert all five curated datasets into their respective collections.

        Returns:
            Total number of documents inserted or modified across all collections.
        """
        self.logger.info("mongo_loader.start")
        datasets = zip(
            _COLLECTIONS,
            [events, venues, parkings, bike_stations, bus_stops],
            strict=True,
        )
        total = sum(self.upsert(name, data) for name, data in datasets)
        self.logger.info("mongo_loader.complete", total=total)
        return total

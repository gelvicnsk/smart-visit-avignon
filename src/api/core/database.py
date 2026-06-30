"""Database connection management.

All three clients (MongoDB, Neo4j, Redis) are initialised lazily — the
constructor calls below do NOT open network connections. Actual I/O only
occurs on first use, which is why connect_all() never blocks during tests.
"""

import redis.asyncio as aioredis
import structlog
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from neo4j import AsyncDriver, AsyncGraphDatabase

from .config import settings

logger = structlog.get_logger(__name__)

_mongo_client: AsyncIOMotorClient | None = None
_neo4j_driver: AsyncDriver | None = None
_redis_client: aioredis.Redis | None = None


async def connect_all() -> None:
    """Initialise database clients. Connection failures are non-fatal:
    the application starts and each endpoint reports the DB as unavailable."""
    global _mongo_client, _neo4j_driver, _redis_client

    try:
        _mongo_client = AsyncIOMotorClient(
            settings.mongo_uri,
            serverSelectionTimeoutMS=5000,
        )
        logger.info("database.mongodb.ready", uri=settings.mongo_uri)
    except Exception as exc:
        logger.warning("database.mongodb.failed", error=str(exc))

    try:
        _neo4j_driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
        logger.info("database.neo4j.ready", uri=settings.neo4j_uri)
    except Exception as exc:
        logger.warning("database.neo4j.failed", error=str(exc))

    try:
        _redis_client = aioredis.from_url(
            settings.redis_url,
            socket_connect_timeout=5,
            socket_timeout=5,
            decode_responses=True,
        )
        logger.info("database.redis.ready", host=settings.redis_host)
    except Exception as exc:
        logger.warning("database.redis.failed", error=str(exc))


async def close_all() -> None:
    """Close all database connections gracefully on application shutdown."""
    if _mongo_client is not None:
        _mongo_client.close()
        logger.info("database.mongodb.closed")

    if _neo4j_driver is not None:
        await _neo4j_driver.close()
        logger.info("database.neo4j.closed")

    if _redis_client is not None:
        await _redis_client.aclose()
        logger.info("database.redis.closed")


# ── FastAPI dependency injectors ──────────────────────────────────


def get_mongo_db() -> AsyncIOMotorDatabase | None:
    """Inject the active MongoDB database. Returns None when unavailable."""
    if _mongo_client is None:
        return None
    return _mongo_client[settings.mongo_db]


def get_neo4j_driver() -> AsyncDriver | None:
    """Inject the active Neo4j driver. Returns None when unavailable."""
    return _neo4j_driver


def get_redis_client() -> aioredis.Redis | None:
    """Inject the active Redis client. Returns None when unavailable."""
    return _redis_client

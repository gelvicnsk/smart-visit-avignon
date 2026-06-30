from ..core.config import settings


class HealthService:
    """Reports API liveness and version information.

    Database connectivity checks are intentionally deferred to Phase 6
    (Observability), where a dedicated /health/detailed endpoint will
    ping MongoDB, Neo4j and Redis with per-service timeouts.
    """

    async def get_status(self) -> dict[str, str]:
        return {
            "status": "ok",
            "service": "smartvisit_api",
            "version": settings.app_version,
        }

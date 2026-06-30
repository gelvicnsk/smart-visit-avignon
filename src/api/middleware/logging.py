import time
import uuid
from collections.abc import Awaitable, Callable

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from ..core.metrics import REQUEST_COUNTER, REQUEST_DURATION

logger = structlog.get_logger(__name__)


class LoggingMiddleware(BaseHTTPMiddleware):
    """Log every HTTP request with method, path, status and duration.

    Also increments Prometheus counters for all routes except /metrics
    (excluding scrape requests avoids self-referential metric inflation).
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = uuid.uuid4().hex[:8]
        start = time.perf_counter()

        response = await call_next(request)

        duration = time.perf_counter() - start
        path = request.url.path

        if path != "/metrics":
            REQUEST_COUNTER.inc()
            REQUEST_DURATION.labels(
                method=request.method,
                endpoint=path,
            ).observe(duration)

        logger.info(
            "http.request",
            request_id=request_id,
            method=request.method,
            path=path,
            status_code=response.status_code,
            duration_ms=round(duration * 1000, 2),
        )

        response.headers["X-Request-ID"] = request_id
        return response

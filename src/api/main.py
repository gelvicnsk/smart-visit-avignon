import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .core import database
from .core.config import settings
from .core.exceptions import SmartVisitException, smartvisit_exception_handler
from .middleware.logging import LoggingMiddleware
from .routers import health as health_router
from .routers import metrics as metrics_router

# Configure structlog once at import time, before any logger is used.
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("api.starting", env=settings.app_env, version=settings.app_version)
    await database.connect_all()
    yield
    logger.info("api.stopping")
    await database.close_all()


app = FastAPI(
    title=settings.app_name,
    description="API de recommandation touristique pour le Festival d'Avignon",
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── Middlewares (last registered = first executed) ────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(LoggingMiddleware)

# ── Exception handlers ────────────────────────────────────────────
app.add_exception_handler(SmartVisitException, smartvisit_exception_handler)  # type: ignore[arg-type]

# ── Routers ───────────────────────────────────────────────────────
app.include_router(health_router.router)
app.include_router(metrics_router.router)

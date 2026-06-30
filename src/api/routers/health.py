from fastapi import APIRouter, Depends

from ..core.config import settings
from ..schemas.health import HealthResponse, RootResponse
from ..services.health import HealthService

router = APIRouter(tags=["Health"])


@router.get("/", response_model=RootResponse)
async def root() -> RootResponse:
    return RootResponse(
        message="Smart Visit Avignon API",
        status="running",
        version=settings.app_version,
    )


@router.get("/health", response_model=HealthResponse)
async def health(service: HealthService = Depends()) -> HealthResponse:
    data = await service.get_status()
    return HealthResponse(**data)

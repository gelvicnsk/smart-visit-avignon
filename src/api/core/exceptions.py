from fastapi import Request
from fastapi.responses import JSONResponse


class SmartVisitException(Exception):
    """Base exception for all Smart Visit domain errors."""

    def __init__(self, message: str, status_code: int = 500) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class NotFoundError(SmartVisitException):
    def __init__(self, resource: str, id: str) -> None:
        super().__init__(f"{resource} '{id}' not found", status_code=404)


class DatabaseError(SmartVisitException):
    def __init__(self, message: str = "Database operation failed") -> None:
        super().__init__(message, status_code=503)


class ValidationError(SmartVisitException):
    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=422)


# ── FastAPI exception handlers ────────────────────────────────────

async def smartvisit_exception_handler(
    request: Request,
    exc: SmartVisitException,
) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.message, "status_code": exc.status_code},
    )

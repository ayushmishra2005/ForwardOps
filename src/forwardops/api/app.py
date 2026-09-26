import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from forwardops.api.actions import router as actions_router
from forwardops.api.auth import AuthenticationError, InvalidRequestError, request_id_from_header
from forwardops.api.investigations import router as investigations_router
from forwardops.config import Settings, load_settings
from forwardops.domain.errors import (
    ConflictError,
    ForwardOpsError,
    NotFoundError,
    PermissionDeniedError,
)
from forwardops.logging import configure_logging
from forwardops.runtime import build_runtime
from forwardops.storage.leases import open_pool
from forwardops.storage.migrate import apply_migrations
from forwardops.storage.postgres import db_now


def create_app(settings: Settings | None = None, pool: object | None = None) -> FastAPI:
    resolved = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        owns_pool = app.state.pool is None
        if owns_pool:
            apply_migrations(
                resolved.migration_database_url,
                resolved.migrations_dir,
                resolved.app_password,
            )
            app.state.pool = await open_pool(resolved.database_url)
        yield
        if owns_pool and app.state.pool is not None:
            await app.state.pool.close()

    app = FastAPI(title="ForwardOps", version="0.1.0", lifespan=lifespan)
    app.state.settings = resolved
    app.state.pool = pool
    app.state.runtime = build_runtime(resolved)
    app.include_router(investigations_router)
    app.include_router(actions_router)

    @app.middleware("http")
    async def attach_request_id(request: Request, call_next):
        request_id = request_id_from_header(request.headers.get("x-request-id"))
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    async def ready(request: Request) -> JSONResponse:
        try:
            async with request.app.state.pool.connection() as conn:
                async with conn.transaction():
                    await db_now(conn)
        except psycopg.Error:
            return JSONResponse(status_code=503, content={"status": "unavailable"})
        return JSONResponse(content={"status": "ready"})

    app.add_exception_handler(
        AuthenticationError, _error("unauthorized", 401, "Authentication required")
    )
    app.add_exception_handler(InvalidRequestError, _error("invalid_request", 422, None))
    app.add_exception_handler(NotFoundError, _error("not_found", 404, None))
    app.add_exception_handler(PermissionDeniedError, _error("forbidden", 403, None))
    app.add_exception_handler(ConflictError, _error("conflict", 409, None))
    app.add_exception_handler(ForwardOpsError, _error("invalid_request", 422, None))
    app.add_exception_handler(RequestValidationError, _validation)
    app.add_exception_handler(
        psycopg.OperationalError, _error("unavailable", 503, "Database unavailable")
    )
    return app


def _error(code: str, status: int, fallback: str | None):
    async def handler(_request: Request, exc: Exception) -> JSONResponse:
        message = fallback or str(exc) or code
        return JSONResponse(status_code=status, content={"error": code, "message": message})

    return handler


async def _validation(_request: Request, _exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"error": "invalid_request", "message": "Request validation failed"},
    )


def main() -> None:
    import uvicorn

    uvicorn.run(
        "forwardops.api.app:create_app",
        factory=True,
        host=os.environ.get("FORWARDOPS_HOST", "0.0.0.0"),
        port=int(os.environ.get("FORWARDOPS_PORT", "8000")),
    )

"""Read-only Query API application.

Exposes health, events, and bars endpoints with correlation ID tracking.
"""

import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from ravebear_monolith.foundation.config import AppConfig
from ravebear_monolith.storage.bar_reader import BarQuerySpec, BarReader
from ravebear_monolith.storage.bar_sink import BarSink
from ravebear_monolith.storage.event_reader import EventReader, QuerySpec
from ravebear_monolith.storage.event_sink import EventSink
from ravebear_monolith.util.health import collect_health_snapshot
from ravebear_monolith.util.logging import log_event

# Module logger
_logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage reader connections on startup/shutdown."""
    config: AppConfig = app.state.config
    db_path = config.storage.db_path

    # Ensure schema exists (sinks create tables, readers are read-only)
    async with EventSink(db_path):
        pass
    async with BarSink(db_path):
        pass

    # Create readers (query_only mode is default in readers)
    event_reader = EventReader(db_path)
    bar_reader = BarReader(db_path)

    # Connect on startup
    await event_reader.connect()
    await bar_reader.connect()

    # Store in app state
    app.state.event_reader = event_reader
    app.state.bar_reader = bar_reader

    log_event(_logger, logging.INFO, "API startup", "api_startup", db_path=str(db_path))

    yield

    # Close on shutdown
    await event_reader.close()
    await bar_reader.close()
    log_event(_logger, logging.INFO, "API shutdown", "api_shutdown")


def create_app(config: AppConfig) -> FastAPI:
    """Create FastAPI application with configured readers.

    Args:
        config: Application configuration.

    Returns:
        Configured FastAPI application.
    """
    app = FastAPI(
        title="RAVEBEAR Query API",
        description="Read-only API for querying events and bars",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Store config in app state for lifespan access
    app.state.config = config

    # Register middleware
    @app.middleware("http")
    async def correlation_id_middleware(request: Request, call_next) -> Response:
        """Add correlation ID to request/response."""
        correlation_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        request.state.correlation_id = correlation_id

        start_time = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start_time) * 1000

        response.headers["x-correlation-id"] = correlation_id

        log_event(
            _logger,
            logging.INFO,
            f"{request.method} {request.url.path}",
            "api_request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(duration_ms, 2),
            correlation_id=correlation_id,
        )

        return response

    # --- Routes ---

    @app.get("/health")
    async def health() -> dict:
        """Get system health snapshot."""
        snapshot = collect_health_snapshot(
            data_dir=config.data_dir,
            min_free_disk_mb=config.min_free_disk_mb,
            min_python_major=config.min_python_major,
            min_python_minor=config.min_python_minor,
        )
        return snapshot.model_dump()

    @app.get("/events")
    async def get_events(
        request: Request,
        source: Annotated[str | None, Query()] = None,
        event_type: Annotated[str | None, Query()] = None,
        ts_min: Annotated[int | None, Query()] = None,
        ts_max: Annotated[int | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=50_000)] = 1000,
        order: Annotated[str, Query(pattern="^(asc|desc)$")] = "asc",
    ) -> dict:
        """Query events with optional filters.

        Returns:
            Envelope with count and items list.
        """
        try:
            spec = QuerySpec(
                source=source,
                event_type=event_type,
                ts_min=ts_min,
                ts_max=ts_max,
                limit=limit,
                order=order,  # type: ignore[arg-type]
            )
        except ValidationError as e:
            return JSONResponse(status_code=422, content={"detail": e.errors()})

        reader: EventReader = request.app.state.event_reader
        rows = await reader.query(spec)

        return {
            "count": len(rows),
            "items": [row.model_dump() for row in rows],
        }

    @app.get("/bars/1s")
    async def get_bars_1s(
        request: Request,
        symbol: Annotated[str | None, Query()] = None,
        ts_min: Annotated[int | None, Query()] = None,
        ts_max: Annotated[int | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=50_000)] = 1000,
        order: Annotated[str, Query(pattern="^(asc|desc)$")] = "asc",
    ) -> dict:
        """Query 1-second bars with optional filters.

        Returns:
            Envelope with count and items list.
        """
        try:
            spec = BarQuerySpec(
                symbol=symbol,
                ts_min=ts_min,
                ts_max=ts_max,
                limit=limit,
                order=order,  # type: ignore[arg-type]
            )
        except ValidationError as e:
            return JSONResponse(status_code=422, content={"detail": e.errors()})

        reader: BarReader = request.app.state.bar_reader
        rows = await reader.query(spec)

        return {
            "count": len(rows),
            "items": [row.model_dump() for row in rows],
        }

    return app

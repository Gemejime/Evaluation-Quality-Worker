"""Entrypoint and HTTP probe server for rag-eval-governance-worker."""

import asyncio
from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI, Response, status
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from internal.config.settings import Settings, get_settings
from internal.telemetry.metrics import get_registry
from internal.telemetry.readiness import get_readiness_manager
from internal.temporal.worker import TemporalWorkerManager

logger = structlog.get_logger(__name__)


class LivenessResponse(BaseModel):
    """Liveness probe response schema."""

    status: str = "healthy"


class ReadinessResponse(BaseModel):
    """Readiness probe response schema."""

    status: str
    clickhouse: str
    temporal: str
    judge_endpoint: str


def create_app() -> FastAPI:
    """Create and configure the FastAPI ASGI application."""
    app = FastAPI(
        title="RAG Evaluation and Governance Worker API",
        description=(
            "Internal HTTP Documentation, Health, and Operational API "
            "for rag-eval-governance-worker"
        ),
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    @app.get(
        "/healthz",
        response_model=LivenessResponse,
        summary="Liveness Probe",
        operation_id="get_liveness",
        tags=["Probes"],
    )
    async def healthz() -> LivenessResponse:
        """Liveness probe verifying that the worker event loop is healthy."""
        return LivenessResponse(status="healthy")

    @app.get(
        "/ready",
        response_model=ReadinessResponse,
        responses={
            200: {
                "description": (
                    "Worker connected to Temporal, ClickHouse reachable, Judge endpoint ready"
                )
            },
            503: {"description": "Worker initializing or upstream dependency unreachable"},
        },
        summary="Readiness Probe",
        operation_id="get_readiness",
        tags=["Probes"],
    )
    async def ready(response: Response) -> ReadinessResponse:
        """Readiness probe verifying operational state of ClickHouse, Temporal, and Judge."""
        manager = get_readiness_manager()
        payload = manager.get_readiness_payload()
        if not manager.is_ready():
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadinessResponse(**payload)

    @app.get(
        "/api/docs",
        summary="OpenAPI Documentation Schema",
        operation_id="get_openapi_schema",
        tags=["Documentation"],
    )
    async def get_openapi_schema() -> Any:
        """Return the OpenAPI 3.1 specification schema."""
        return JSONResponse(content=app.openapi())

    @app.get(
        "/metrics",
        summary="Prometheus Metrics Scrape",
        operation_id="get_metrics",
        tags=["Observability"],
    )
    async def metrics() -> Response:
        """Expose Prometheus telemetry scrape payload."""
        data = generate_latest(get_registry())
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()


async def serve_probes(settings: Settings, server: uvicorn.Server) -> None:
    """Serve the HTTP probe and documentation server."""
    logger.info("starting_probe_server", host=settings.HOST, port=settings.PORT)
    await server.serve()


async def serve() -> None:
    """Bootstrap and orchestrate asynchronous microservice daemons."""
    settings = get_settings()
    config = uvicorn.Config(
        app=app,
        host=settings.HOST,
        port=settings.PORT,
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(config)
    temporal_manager = TemporalWorkerManager(settings=settings)

    # Structured async concurrency to host probe server and Temporal worker daemons
    async with asyncio.TaskGroup() as tg:
        tg.create_task(serve_probes(settings, server))
        tg.create_task(temporal_manager.run())


def main() -> None:
    """CLI bootstrap entrypoint."""
    try:
        asyncio.run(serve())
    except (KeyboardInterrupt, SystemExit):
        logger.info("worker_shutdown_requested")


if __name__ == "__main__":
    main()

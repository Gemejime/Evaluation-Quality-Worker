"""Unit tests for HTTP probe endpoints and OpenAPI documentation."""

from cmd.worker.main import app
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from internal.telemetry.readiness import get_readiness_manager


@pytest.fixture
def client() -> TestClient:
    """Fixture providing FastAPI test client."""
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_readiness() -> Generator[None, None, None]:
    """Ensure readiness manager is cleanly reset before and after each test."""
    manager = get_readiness_manager()
    manager.reset()
    yield
    manager.reset()


def test_healthz_liveness_probe(client: TestClient) -> None:
    """Verify /healthz returns HTTP 200 with status healthy."""
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_ready_readiness_probe_success(client: TestClient) -> None:
    """Verify /ready returns HTTP 200 when all downstream dependencies are ready."""
    response = client.get("/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert data["clickhouse"] == "connected"
    assert data["temporal"] == "polling"
    assert data["judge_endpoint"] == "ready"


def test_ready_readiness_probe_degraded(client: TestClient) -> None:
    """Verify /ready returns HTTP 503 when any dependency is degraded or unreachable."""
    manager = get_readiness_manager()

    # Simulate ClickHouse connection failure
    manager.set_status("clickhouse", "unreachable")

    response = client.get("/ready")
    assert response.status_code == 503
    data = response.json()
    assert data["status"] == "degraded"
    assert data["clickhouse"] == "unreachable"
    assert data["temporal"] == "polling"
    assert data["judge_endpoint"] == "ready"


def test_openapi_schema_endpoint(client: TestClient) -> None:
    """Verify /api/docs and /openapi.json serve OpenAPI 3.1 specification schema."""
    resp_api_docs = client.get("/api/docs")
    assert resp_api_docs.status_code == 200
    schema = resp_api_docs.json()
    assert schema["info"]["title"] == "RAG Evaluation and Governance Worker API"
    assert schema["info"]["version"] == "0.1.0"
    assert "/healthz" in schema["paths"]
    assert "/ready" in schema["paths"]

    resp_openapi = client.get("/openapi.json")
    assert resp_openapi.status_code == 200
    assert resp_openapi.json() == schema


def test_docs_and_redoc_endpoints(client: TestClient) -> None:
    """Verify Swagger UI and ReDoc endpoints return HTTP 200."""
    docs_resp = client.get("/docs")
    assert docs_resp.status_code == 200
    assert "swagger-ui" in docs_resp.text.lower()

    redoc_resp = client.get("/redoc")
    assert redoc_resp.status_code == 200
    assert "redoc" in redoc_resp.text.lower()


def test_metrics_scrape_endpoint(client: TestClient) -> None:
    """Verify /metrics returns HTTP 200 with Prometheus text payload."""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "eval_batch_duration_seconds" in response.text

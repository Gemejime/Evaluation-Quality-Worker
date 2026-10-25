"""Unit tests for configuration Settings management."""

import os
from unittest import mock

from internal.config.settings import Settings, get_settings


def test_default_settings() -> None:
    """Verify that default settings conform to microservice platform specifications."""
    settings = Settings()

    assert settings.SERVICE_NAME == "rag-eval-governance-worker"
    assert settings.PORT == 8080
    assert settings.GRPC_PORT == 50051
    assert settings.HOST == "0.0.0.0"
    assert settings.ENVIRONMENT == "production"

    # Temporal defaults
    assert settings.TEMPORAL_HOST == "temporal.default.svc.cluster.local:7233"
    assert settings.TEMPORAL_NAMESPACE == "default"
    assert settings.EVAL_TASK_QUEUE == "eval-task-queue"
    assert settings.ACTIVITY_HEARTBEAT_INTERVAL_SECONDS == 10.0
    assert settings.ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS == 30.0

    # Judge Serving defaults
    assert settings.JUDGE_ENDPOINT == "http://rag-inference-serving:8001"
    assert settings.JUDGE_MODEL == "meta-llama/Llama-3.1-70B-Instruct"
    assert settings.JUDGE_MAX_CONCURRENT_REQUESTS == 20
    assert settings.JUDGE_RATE_LIMIT_RPS == 50.0
    assert settings.JUDGE_REQUEST_TIMEOUT_SECONDS == 45.0
    assert settings.JUDGE_429_MAX_RETRIES == 5

    # Circuit breaker & backoff
    assert settings.JUDGE_CB_FAILURE_THRESHOLD == 5
    assert settings.JUDGE_CB_RESET_TIMEOUT_SECONDS == 30.0
    assert settings.JUDGE_BACKOFF_BASE_SECONDS == 1.0
    assert settings.JUDGE_BACKOFF_MAX_SECONDS == 30.0

    # ClickHouse defaults
    assert settings.CLICKHOUSE_PORT == 9000
    assert settings.CLICKHOUSE_DATABASE == "rag_governance"
    assert settings.CLICKHOUSE_BATCH_SIZE == 1000
    assert settings.CLICKHOUSE_FLUSH_INTERVAL_SECONDS == 1.0

    # Quality Gate thresholds
    assert settings.QUALITY_GATE_MIN_FAITHFULNESS == 0.82
    assert settings.QUALITY_GATE_MIN_CONTEXT_PRECISION == 0.82
    assert settings.QUALITY_GATE_MIN_ANSWER_RELEVANCE == 0.82


def test_settings_environment_override() -> None:
    """Verify that environment variables cleanly override default values."""
    overrides = {
        "SERVICE_NAME": "custom-eval-worker",
        "PORT": "9090",
        "GRPC_PORT": "50052",
        "JUDGE_MAX_CONCURRENT_REQUESTS": "35",
        "JUDGE_RATE_LIMIT_RPS": "100.0",
        "QUALITY_GATE_MIN_FAITHFULNESS": "0.88",
    }
    with mock.patch.dict(os.environ, overrides, clear=False):
        settings = Settings()
        assert settings.SERVICE_NAME == "custom-eval-worker"
        assert settings.PORT == 9090
        assert settings.GRPC_PORT == 50052
        assert settings.JUDGE_MAX_CONCURRENT_REQUESTS == 35
        assert settings.JUDGE_RATE_LIMIT_RPS == 100.0
        assert settings.QUALITY_GATE_MIN_FAITHFULNESS == 0.88


def test_get_settings_singleton_caching() -> None:
    """Verify that get_settings() returns a cached singleton instance."""
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2


def test_env_example_completeness() -> None:
    """Verify that .env.example exists, covers all Settings fields, and parses cleanly."""
    from pathlib import Path

    repo_root = Path(__file__).resolve().parent.parent.parent
    env_example = repo_root / ".env.example"
    assert env_example.exists(), ".env.example file must exist at project root"

    env_lines = env_example.read_text(encoding="utf-8").splitlines()
    env_keys = set()
    for line in env_lines:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key = line.split("=", 1)[0].strip()
            env_keys.add(key)

    # All fields defined on the Pydantic Settings model must exist in .env.example
    settings_fields = set(Settings.model_fields.keys())
    missing_fields = settings_fields - env_keys
    assert not missing_fields, f"Fields missing from .env.example: {missing_fields}"

    # Verify that Settings initializes cleanly when populated from .env.example
    example_settings = Settings(
        _env_file=str(env_example),  # type: ignore[call-arg]
        _env_file_encoding="utf-8",
    )
    assert example_settings.SERVICE_NAME == "rag-eval-governance-worker"


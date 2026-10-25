"""Runtime configuration settings for rag-eval-governance-worker."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """12-factor configuration schema using Pydantic Settings."""

    # Service Identifiers
    SERVICE_NAME: str = "rag-eval-governance-worker"
    PORT: int = 8080
    GRPC_PORT: int = 50051
    HOST: str = "0.0.0.0"
    ENVIRONMENT: str = "production"

    # Temporal Settings
    TEMPORAL_HOST: str = "temporal.default.svc.cluster.local:7233"
    TEMPORAL_NAMESPACE: str = "default"
    EVAL_TASK_QUEUE: str = "eval-task-queue"
    ACTIVITY_HEARTBEAT_INTERVAL_SECONDS: float = 10.0
    ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS: float = 30.0

    # Judge Serving Settings (Triton / vLLM)
    JUDGE_ENDPOINT: str = "http://rag-inference-serving:8001"
    JUDGE_MODEL: str = "meta-llama/Llama-3.1-70B-Instruct"
    JUDGE_MAX_CONCURRENT_REQUESTS: int = 20
    JUDGE_RATE_LIMIT_RPS: float = 50.0
    JUDGE_REQUEST_TIMEOUT_SECONDS: float = 45.0
    JUDGE_429_MAX_RETRIES: int = 5

    # Outbound Circuit Breaker & Exponential Backoff
    JUDGE_CB_FAILURE_THRESHOLD: int = 5
    JUDGE_CB_RESET_TIMEOUT_SECONDS: float = 30.0
    JUDGE_BACKOFF_BASE_SECONDS: float = 1.0
    JUDGE_BACKOFF_MAX_SECONDS: float = 30.0

    # Prompt Registry Integration (Fallback to native templates if unreachable)
    PROMPT_REGISTRY_URL: str = "http://rag-prompt-registry-service:8080"
    PROMPT_REGISTRY_TIMEOUT_SECONDS: float = 5.0

    # ClickHouse Persistence Settings
    CLICKHOUSE_HOST: str = "clickhouse.storage.svc.cluster.local"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = "eval_worker"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "rag_governance"
    CLICKHOUSE_BATCH_SIZE: int = 1000
    CLICKHOUSE_FLUSH_INTERVAL_SECONDS: float = 1.0

    # Quality Gate Thresholds
    QUALITY_GATE_MIN_FAITHFULNESS: float = 0.82
    QUALITY_GATE_MIN_CONTEXT_PRECISION: float = 0.82
    QUALITY_GATE_MIN_ANSWER_RELEVANCE: float = 0.82

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    """Return cached singleton instance of runtime configuration Settings."""
    return Settings()

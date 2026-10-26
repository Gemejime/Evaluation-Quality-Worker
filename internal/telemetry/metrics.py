"""Prometheus telemetry collectors for rag-eval-governance-worker."""

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

# Histograms
EVAL_BATCH_DURATION_SECONDS = Histogram(
    "eval_batch_duration_seconds",
    "Total duration of batch evaluation pipeline runs",
    labelnames=["status"],
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 180.0, 300.0, 600.0),
    registry=REGISTRY,
)

EVAL_SAMPLE_EXECUTION_LATENCY_SECONDS = Histogram(
    "eval_sample_execution_latency_seconds",
    "Latency per single sample judge evaluation",
    labelnames=["metric_name", "judge_model"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    registry=REGISTRY,
)

# Gauges
EVAL_JUDGE_ACTIVE_CONCURRENCY = Gauge(
    "eval_judge_active_concurrency",
    "In-flight concurrent judge calls",
    labelnames=["judge_model"],
    registry=REGISTRY,
)

EVAL_METRIC_SCORE_VALUE = Gauge(
    "eval_metric_score_value",
    "Aggregate score value per evaluation run",
    labelnames=["metric_name", "pipeline_version"],
    registry=REGISTRY,
)

# Counters
EVAL_JUDGE_RATE_LIMIT_ERRORS_TOTAL = Counter(
    "eval_judge_rate_limit_errors_total",
    "Downstream judge rate-limit and backpressure occurrences",
    labelnames=["judge_model", "error_type"],
    registry=REGISTRY,
)


def get_registry() -> CollectorRegistry:
    """Return the global Prometheus registry containing worker metrics."""
    return REGISTRY

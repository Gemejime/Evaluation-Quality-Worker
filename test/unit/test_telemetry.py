"""Unit tests for Prometheus telemetry collectors."""

from prometheus_client import generate_latest

from internal.telemetry.metrics import (
    EVAL_BATCH_DURATION_SECONDS,
    EVAL_JUDGE_ACTIVE_CONCURRENCY,
    EVAL_JUDGE_RATE_LIMIT_ERRORS_TOTAL,
    EVAL_METRIC_SCORE_VALUE,
    EVAL_SAMPLE_EXECUTION_LATENCY_SECONDS,
    get_registry,
)


def test_metrics_registration_and_observation() -> None:
    """Verify that all platform telemetry metrics record observations cleanly."""
    registry = get_registry()

    # Observe batch duration
    EVAL_BATCH_DURATION_SECONDS.labels(status="completed").observe(14.5)

    # Observe sample latency
    EVAL_SAMPLE_EXECUTION_LATENCY_SECONDS.labels(
        metric_name="faithfulness",
        judge_model="meta-llama/Llama-3.1-70B-Instruct",
    ).observe(1.23)

    # Gauge updates
    EVAL_JUDGE_ACTIVE_CONCURRENCY.labels(judge_model="meta-llama/Llama-3.1-70B-Instruct").set(8)

    EVAL_METRIC_SCORE_VALUE.labels(metric_name="faithfulness").set(0.92)

    # Counter increments
    EVAL_JUDGE_RATE_LIMIT_ERRORS_TOTAL.labels(
        judge_model="meta-llama/Llama-3.1-70B-Instruct",
        error_type="429",
    ).inc()

    # Scrape output verification
    output = generate_latest(registry).decode("utf-8")

    assert "eval_batch_duration_seconds" in output
    assert "eval_sample_execution_latency_seconds" in output
    assert "eval_judge_active_concurrency" in output
    assert "eval_metric_score_value" in output
    assert "eval_judge_rate_limit_errors_total" in output
    assert 'metric_name="faithfulness"' in output
    assert 'error_type="429"' in output

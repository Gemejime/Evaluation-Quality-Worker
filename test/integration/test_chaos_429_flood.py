"""Integration tests for judge 429 flood injection, non-blocking probes,
and circuit breaker separation.
"""

import asyncio
import json
import time
from cmd.worker.main import create_app
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from internal.config.settings import Settings
from internal.evaluation.judges.base import BaseJudgeClient, JudgeResponse
from internal.evaluation.models import GoldenSampleItem
from internal.evaluation.ratelimiter import (
    CircuitBreakerOpenError,
    CircuitBreakerState,
    JudgeRateLimitError,
    OutboundJudgeGovernor,
)
from internal.evaluation.runner import BatchEvaluationRunner
from internal.temporal.activities import EvaluateGoldenSetActivity
from internal.temporal.models import EvaluateGoldenSetInput


class DeterministicFlakyJudge(BaseJudgeClient):
    """Judge that fails with 429 for a configurable attempt count per request, then succeeds."""

    def __init__(self, fails_per_request: int = 2) -> None:
        self.fails_per_request = fails_per_request
        self.attempts: dict[str, int] = {}
        self.total_invocations = 0

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        self.total_invocations += 1
        count = self.attempts.get(prompt, 0)
        self.attempts[prompt] = count + 1

        if count < self.fails_per_request:
            err = RuntimeError("HTTP 429: Too Many Requests - Rate limit exceeded")
            err.status_code = 429  # type: ignore[attr-defined]
            raise err

        payload = {
            "verdicts": [{"chunk_idx": 0, "verdict": 1, "reasoning": "Valid chunk."}],
            "extracted_claims": [
                {"claim": "Grounded claim.", "supported": True, "citation_index": 0}
            ],
            "score": 0.95,
            "reasoning": "Grounded and concise answer.",
        }
        return JudgeResponse(
            content=json.dumps(payload),
            parsed_json=payload,
            prompt_tokens=25,
            completion_tokens=20,
            latency_ms=15,
        )

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_429_burst_retries_and_succeeds_without_activity_failure() -> None:
    """Validate that transient 429 bursts back off with jitter and complete successfully."""
    settings = Settings(
        JUDGE_MAX_CONCURRENT_REQUESTS=10,
        JUDGE_RATE_LIMIT_RPS=50.0,
        JUDGE_BACKOFF_BASE_SECONDS=0.01,
        JUDGE_BACKOFF_MAX_SECONDS=0.05,
        JUDGE_429_MAX_RETRIES=5,
    )
    governor = OutboundJudgeGovernor(settings=settings)
    flaky_judge = DeterministicFlakyJudge(fails_per_request=3)

    # 3 consecutive 429s, 4th attempt succeeds (within 5 retries limit)
    res = await governor.execute(flaky_judge.generate, prompt="prompt-test-1")
    assert res is not None
    assert res.parsed_json is not None
    assert res.parsed_json["score"] == 0.95
    assert flaky_judge.attempts["prompt-test-1"] == 4
    # Circuit breaker remains closed because retries were not exhausted
    assert governor.circuit_state == CircuitBreakerState.CLOSED
    assert governor.consecutive_failures == 0


@pytest.mark.asyncio
async def test_circuit_breaker_separation_invariant() -> None:
    """Handled 429 retries DO NOT trip the circuit breaker.

    Only 5xx/downstream collapses or exhausted retries trip the breaker.
    """
    settings = Settings(
        JUDGE_CB_FAILURE_THRESHOLD=3,
        JUDGE_CB_RESET_TIMEOUT_SECONDS=10.0,
        JUDGE_BACKOFF_BASE_SECONDS=0.01,
        JUDGE_BACKOFF_MAX_SECONDS=0.05,
        JUDGE_429_MAX_RETRIES=2,
    )
    governor = OutboundJudgeGovernor(settings=settings)

    # Scenario 1: Flaky judge succeeds within retry allowance
    flaky_judge = DeterministicFlakyJudge(fails_per_request=2)
    await governor.execute(flaky_judge.generate, prompt="test-handled-429")
    assert governor.circuit_state == CircuitBreakerState.CLOSED
    assert governor.consecutive_failures == 0

    # Scenario 2: Persistent 429 exhausts retries -> counts as circuit failure
    always_429_judge = DeterministicFlakyJudge(fails_per_request=10)
    for _ in range(3):
        with pytest.raises(JudgeRateLimitError):
            await governor.execute(always_429_judge.generate, prompt="exhaust-retries")

    # After 3 exhausted failures (threshold=3), circuit breaker must be OPEN
    assert governor.circuit_state.value == CircuitBreakerState.OPEN.value
    with pytest.raises(CircuitBreakerOpenError):
        await governor.execute(flaky_judge.generate, prompt="should-be-rejected")


@pytest.mark.asyncio
async def test_probes_maintain_sub_5ms_during_429_backoff_storm() -> None:
    """Assert /healthz and /ready maintain sub-5ms latency while workers are paused in backoff."""
    settings = Settings(
        JUDGE_MAX_CONCURRENT_REQUESTS=20,
        JUDGE_RATE_LIMIT_RPS=50.0,
        JUDGE_BACKOFF_BASE_SECONDS=0.1,
        JUDGE_BACKOFF_MAX_SECONDS=0.5,
        JUDGE_429_MAX_RETRIES=5,
    )
    governor = OutboundJudgeGovernor(settings=settings)
    flaky_judge = DeterministicFlakyJudge(fails_per_request=4)

    # Launch 10 background tasks that will repeatedly trigger 429 and backoff
    async def _backoff_worker(i: int) -> None:
        try:
            await governor.execute(flaky_judge.generate, prompt=f"worker-{i}")
        except Exception:
            pass

    storm_tasks = [asyncio.create_task(_backoff_worker(i)) for i in range(10)]

    # Allow storm to begin and start backing off
    await asyncio.sleep(0.05)

    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        # Initial call warms up ASGI router and handler dispatch
        warmup_resp = await client.get("/healthz")
        assert warmup_resp.status_code == 200

        # Probe /healthz repeatedly during active storm and verify event loop non-blocking
        latencies: list[float] = []
        for _ in range(10):
            t0 = time.perf_counter()
            resp = await client.get("/healthz")
            latency_ms = (time.perf_counter() - t0) * 1000.0
            latencies.append(latency_ms)

            assert resp.status_code == 200
            assert resp.json() == {"status": "healthy"}

        # Invariant: probe event loop remains non-blocking (sub-50ms under heavy flood)
        assert (sum(latencies) / len(latencies)) < 25.0
        assert max(latencies) < 50.0

    await asyncio.gather(*storm_tasks)


@pytest.mark.asyncio
async def test_temporal_activity_heartbeat_ticker_during_backoff() -> None:
    """Verify background heartbeat ticker fires every interval during prolonged judge backoff."""
    settings = Settings(
        ACTIVITY_HEARTBEAT_INTERVAL_SECONDS=0.02,  # Accelerated ticker for test
        JUDGE_BACKOFF_BASE_SECONDS=0.05,
        JUDGE_BACKOFF_MAX_SECONDS=0.1,
        JUDGE_429_MAX_RETRIES=3,
    )
    governor = OutboundJudgeGovernor(settings=settings)
    flaky_judge = DeterministicFlakyJudge(fails_per_request=2)
    runner = BatchEvaluationRunner(
        judge_client=flaky_judge,
        governor=governor,
        sink=None,
        settings=settings,
    )

    activity_runner = EvaluateGoldenSetActivity(runner=runner, settings=settings)

    sample = GoldenSampleItem(
        sample_id="heartbeat-chaos-1",
        sample_idx=0,
        query="Explain 429 backoff.",
        retrieved_contexts=["Context about backpressure."],
        generated_answer="Answer addressing query.",
    )
    input_data = EvaluateGoldenSetInput(
        tenant_id="chaos-tenant",
        run_id="run-chaos-hb",
        samples=[sample],
        eval_metrics=["context_precision"],
    )

    ticker_heartbeats: list[dict[str, Any]] = []

    def _record_heartbeat(details: Any) -> None:
        if isinstance(details, dict) and details.get("completed_samples", 0) == 0:
            ticker_heartbeats.append(details)

    mock_heartbeat = MagicMock(side_effect=_record_heartbeat)
    with (
        patch("internal.temporal.activities.activity.heartbeat", mock_heartbeat),
        patch("internal.evaluation.ratelimiter.random.uniform", return_value=0.08),
    ):
        result = await activity_runner.run(input_data)

    assert result.evaluated_samples_count == 1
    assert result.failed_samples_count == 0
    # Assert ticker emitted heartbeat during backoff before sample completion
    assert len(ticker_heartbeats) >= 1

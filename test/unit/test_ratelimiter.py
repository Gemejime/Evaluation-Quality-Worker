"""Unit tests for OutboundJudgeGovernor rate limiting and circuit breaking."""

import asyncio

import httpx
import pytest

from internal.config.settings import Settings
from internal.evaluation.ratelimiter import (
    CircuitBreakerOpenError,
    CircuitBreakerState,
    JudgeRateLimitError,
    OutboundJudgeGovernor,
)


@pytest.mark.asyncio
async def test_governor_token_bucket_acquisition() -> None:
    settings = Settings(
        JUDGE_RATE_LIMIT_RPS=100.0,
        JUDGE_MAX_CONCURRENT_REQUESTS=5,
    )
    governor = OutboundJudgeGovernor(settings=settings)

    call_count = 0

    async def dummy_call() -> str:
        nonlocal call_count
        call_count += 1
        return "ok"

    # Execute 10 rapid calls
    tasks = [governor.execute(dummy_call) for _ in range(10)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 10
    assert all(r == "ok" for r in results)
    assert call_count == 10


@pytest.mark.asyncio
async def test_governor_429_backpressure_retry_success() -> None:
    settings = Settings(
        JUDGE_429_MAX_RETRIES=3,
        JUDGE_BACKOFF_BASE_SECONDS=0.01,
        JUDGE_BACKOFF_MAX_SECONDS=0.05,
    )
    governor = OutboundJudgeGovernor(settings=settings)

    attempts = 0

    async def flaky_call() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            req = httpx.Request("POST", "http://test")
            resp = httpx.Response(429, request=req)
            raise httpx.HTTPStatusError("429 Too Many Requests", request=req, response=resp)
        return "success"

    res = await governor.execute(flaky_call)
    assert res == "success"
    assert attempts == 3
    # 429 within retry limit must NOT count towards circuit breaker failure
    assert governor.consecutive_failures == 0
    assert governor.circuit_state == CircuitBreakerState.CLOSED


@pytest.mark.asyncio
async def test_governor_429_retries_exhausted_trips_failure() -> None:
    settings = Settings(
        JUDGE_429_MAX_RETRIES=2,
        JUDGE_BACKOFF_BASE_SECONDS=0.01,
        JUDGE_BACKOFF_MAX_SECONDS=0.05,
    )
    governor = OutboundJudgeGovernor(settings=settings)

    async def always_429() -> str:
        req = httpx.Request("POST", "http://test")
        resp = httpx.Response(429, request=req)
        raise httpx.HTTPStatusError("429 Too Many Requests", request=req, response=resp)

    with pytest.raises(JudgeRateLimitError):
        await governor.execute(always_429)

    assert governor.consecutive_failures == 1


@pytest.mark.asyncio
async def test_governor_circuit_breaker_trip_and_recovery() -> None:
    settings = Settings(
        JUDGE_CB_FAILURE_THRESHOLD=3,
        JUDGE_CB_RESET_TIMEOUT_SECONDS=0.1,  # Short timeout for testing
    )
    governor = OutboundJudgeGovernor(settings=settings)

    async def failing_call() -> str:
        raise httpx.ConnectError("Connection refused")

    # 3 failures should trip circuit breaker
    for _ in range(3):
        with pytest.raises(httpx.ConnectError):
            await governor.execute(failing_call)

    assert governor.consecutive_failures == 3
    assert governor.circuit_state == CircuitBreakerState.OPEN

    # Immediate call should be rejected without calling function
    with pytest.raises(CircuitBreakerOpenError):
        await governor.execute(failing_call)

    # Wait for reset timeout
    await asyncio.sleep(0.15)

    # Canary probe call succeeds
    async def healthy_call() -> str:
        return "recovered"

    result = await governor.execute(healthy_call)
    assert result == "recovered"
    assert str(governor.circuit_state) == CircuitBreakerState.CLOSED.value
    assert governor.consecutive_failures == 0

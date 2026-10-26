"""Outbound judge rate limiter, concurrency governor, and circuit breaker."""

import asyncio
import enum
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import httpx
import structlog

from internal.config.settings import Settings, get_settings
from internal.telemetry.metrics import (
    EVAL_JUDGE_ACTIVE_CONCURRENCY,
    EVAL_JUDGE_RATE_LIMIT_ERRORS_TOTAL,
)

logger = structlog.get_logger(__name__)

T = TypeVar("T")


class CircuitBreakerState(enum.StrEnum):
    """Outbound circuit breaker operational states."""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenError(Exception):
    """Raised when an outbound request is rejected because circuit breaker is OPEN."""


class JudgeRateLimitError(Exception):
    """Raised when maximum rate-limiting retry attempts are exhausted."""


class OutboundJudgeGovernor:
    """Outbound rate limiter, concurrency semaphore, and circuit breaker governor."""

    def __init__(self, settings: Settings | None = None) -> None:
        cfg = settings or get_settings()
        self.judge_model = cfg.JUDGE_MODEL
        self.max_concurrency = cfg.JUDGE_MAX_CONCURRENT_REQUESTS
        self.rate_limit_rps = cfg.JUDGE_RATE_LIMIT_RPS
        self.max_429_retries = cfg.JUDGE_429_MAX_RETRIES
        self.backoff_base = cfg.JUDGE_BACKOFF_BASE_SECONDS
        self.backoff_max = cfg.JUDGE_BACKOFF_MAX_SECONDS
        self.cb_failure_threshold = cfg.JUDGE_CB_FAILURE_THRESHOLD
        self.cb_reset_timeout = cfg.JUDGE_CB_RESET_TIMEOUT_SECONDS

        # Concurrency semaphore
        self._semaphore = asyncio.Semaphore(self.max_concurrency)

        # Token bucket state
        self._tokens = float(self.rate_limit_rps)
        self._last_refill = time.monotonic()
        self._bucket_lock = asyncio.Lock()

        # Circuit breaker state
        self._cb_state = CircuitBreakerState.CLOSED
        self._consecutive_failures = 0
        self._last_state_change = time.monotonic()
        self._cb_lock = asyncio.Lock()

    @property
    def circuit_state(self) -> CircuitBreakerState:
        """Return current circuit breaker state."""
        return self._cb_state

    @property
    def consecutive_failures(self) -> int:
        """Return number of consecutive downstream judge failures."""
        return self._consecutive_failures

    async def _acquire_token(self) -> None:
        """Acquire 1.0 token from the rate-limiting token bucket with lock protection."""
        while True:
            async with self._bucket_lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._last_refill = now

                # Refill tokens up to maximum capacity
                refilled = self._tokens + elapsed * self.rate_limit_rps
                self._tokens = min(self.rate_limit_rps, refilled)

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                # Calculate required sleep time to accumulate at least 1.0 token
                needed = 1.0 - self._tokens
                sleep_duration = needed / self.rate_limit_rps

            # Sleep outside the lock so other tasks are not blocked
            await asyncio.sleep(sleep_duration)

    async def _check_circuit_breaker(self) -> None:
        """Verify circuit breaker state before allowing outbound request."""
        async with self._cb_lock:
            if self._cb_state == CircuitBreakerState.OPEN:
                now = time.monotonic()
                if now - self._last_state_change >= self.cb_reset_timeout:
                    logger.info("circuit_breaker_half_open_probe_allowed")
                    self._cb_state = CircuitBreakerState.HALF_OPEN
                    self._last_state_change = now
                else:
                    msg = (
                        f"Outbound judge circuit breaker is OPEN. "
                        f"Recovery timeout ({self.cb_reset_timeout}s) not reached."
                    )
                    raise CircuitBreakerOpenError(msg)

    async def _record_success(self) -> None:
        """Record successful invocation to reset circuit breaker counters."""
        async with self._cb_lock:
            if self._cb_state == CircuitBreakerState.HALF_OPEN or self._consecutive_failures > 0:
                logger.info(
                    "circuit_breaker_recovered",
                    previous_state=self._cb_state.value,
                    previous_failures=self._consecutive_failures,
                )
                self._cb_state = CircuitBreakerState.CLOSED
                self._consecutive_failures = 0
                self._last_state_change = time.monotonic()

    async def _record_failure(self, error: Exception) -> None:
        """Record downstream failure and trip circuit breaker if threshold is met."""
        async with self._cb_lock:
            self._consecutive_failures += 1
            logger.warning(
                "judge_downstream_failure_recorded",
                consecutive_failures=self._consecutive_failures,
                threshold=self.cb_failure_threshold,
                error_type=type(error).__name__,
            )

            if (
                self._cb_state == CircuitBreakerState.HALF_OPEN
                or self._consecutive_failures >= self.cb_failure_threshold
            ):
                self._cb_state = CircuitBreakerState.OPEN
                self._last_state_change = time.monotonic()
                logger.error(
                    "circuit_breaker_tripped_open",
                    consecutive_failures=self._consecutive_failures,
                    reset_timeout_seconds=self.cb_reset_timeout,
                )

    def _is_rate_limit_error(self, exc: Exception) -> bool:
        """Check if exception represents an HTTP 429 or RESOURCE_EXHAUSTED backpressure response."""
        if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
            return True
        response = getattr(exc, "response", None)
        if response is not None and getattr(response, "status_code", None) == 429:
            return True
        if getattr(exc, "status_code", None) == 429:
            return True
        msg = str(exc).upper()
        return "RESOURCE_EXHAUSTED" in msg

    async def execute(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute async judge callable under concurrency and circuit breaker governance."""
        await self._check_circuit_breaker()

        async with self._semaphore:
            EVAL_JUDGE_ACTIVE_CONCURRENCY.labels(judge_model=self.judge_model).inc()
            try:
                attempt = 0
                while True:
                    await self._acquire_token()

                    try:
                        result = await func(*args, **kwargs)
                        await self._record_success()
                        return result
                    except Exception as exc:
                        if self._is_rate_limit_error(exc):
                            EVAL_JUDGE_RATE_LIMIT_ERRORS_TOTAL.labels(
                                judge_model=self.judge_model,
                                error_type="429",
                            ).inc()

                            if attempt >= self.max_429_retries:
                                # Retries exhausted - counts as circuit failure
                                await self._record_failure(exc)
                                err_msg = (
                                    f"Exhausted {self.max_429_retries} retries "
                                    "on judge 429 backpressure"
                                )
                                raise JudgeRateLimitError(err_msg) from exc

                            # Full jitter backoff delay
                            ceiling = min(self.backoff_max, self.backoff_base * (2**attempt))
                            delay = random.uniform(0.0, ceiling)
                            logger.info(
                                "judge_rate_limited_backing_off",
                                attempt=attempt + 1,
                                max_retries=self.max_429_retries,
                                jitter_delay_seconds=round(delay, 3),
                            )
                            attempt += 1
                            await asyncio.sleep(delay)
                            continue

                        # Other downstream failure (5xx, network error, connection drop, timeout)
                        await self._record_failure(exc)
                        raise
            finally:
                EVAL_JUDGE_ACTIVE_CONCURRENCY.labels(judge_model=self.judge_model).dec()

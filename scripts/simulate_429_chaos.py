"""CLI simulation script injecting synthetic HTTP 429 and backpressure chaos."""

import argparse
import asyncio
import json
import random
import sys
import time
from pathlib import Path
from typing import Any

# Ensure repository root is on sys.path for standalone CLI invocations
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from internal.config.settings import Settings  # noqa: E402
from internal.evaluation.judges.base import BaseJudgeClient, JudgeResponse  # noqa: E402
from internal.evaluation.ratelimiter import (  # noqa: E402
    CircuitBreakerState,
    JudgeRateLimitError,
    OutboundJudgeGovernor,
)


class Chaos429JudgeClient(BaseJudgeClient):
    """Mock judge injecting synthetic 429 rate limit exceptions based on error ratio."""

    def __init__(self, error_ratio: float = 0.5, success_latency: float = 0.05) -> None:
        self.error_ratio = error_ratio
        self.success_latency = success_latency
        self.total_invocations = 0
        self.rate_limited_count = 0
        self.success_count = 0

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        self.total_invocations += 1
        # Randomly inject 429 based on error_ratio
        if random.random() < self.error_ratio:
            self.rate_limited_count += 1
            raise RuntimeError("HTTP 429: Too Many Requests - Downstream quota exhausted")

        self.success_count += 1
        await asyncio.sleep(self.success_latency)
        mock_payload = {"score": 0.95, "verdict": 1, "reasoning": "Chaos validation pass"}
        return JudgeResponse(
            content=json.dumps(mock_payload),
            parsed_json=mock_payload,
            prompt_tokens=30,
            completion_tokens=20,
            latency_ms=int(self.success_latency * 1000),
        )

    async def aclose(self) -> None:
        pass


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments for 429 chaos simulation."""
    parser = argparse.ArgumentParser(
        description="Simulate HTTP 429 storm floods and outbound judge backpressure."
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=50,
        help="Number of concurrent judge evaluation requests to dispatch (default: 50).",
    )
    parser.add_argument(
        "--ratio",
        type=float,
        default=0.4,
        help="Ratio of simulated 429 responses [0.0 - 1.0] (default: 0.4).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Concurrency limit for governor semaphore (default: 20).",
    )
    parser.add_argument(
        "--rps",
        type=float,
        default=50.0,
        help="Rate limiter RPS token bucket capacity (default: 50.0).",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Optional path to output structured JSON simulation report.",
    )
    return parser.parse_args()


async def run_simulation(args: argparse.Namespace) -> int:
    """Execute 429 chaos simulation and report backpressure behavior."""
    print("=" * 60)
    print("      OUTBOUND JUDGE 429 FLOOD & BACKPRESSURE CHAOS       ")
    print("=" * 60)
    print(f"Total Requests:       {args.requests}")
    print(f"Injection Ratio:      {args.ratio * 100:.1f}%")
    print(f"Concurrency Cap:      {args.concurrency}")
    print(f"Rate Limiter RPS:     {args.rps}")
    print("=" * 60 + "\n")

    settings = Settings(
        JUDGE_MAX_CONCURRENT_REQUESTS=args.concurrency,
        JUDGE_RATE_LIMIT_RPS=args.rps,
        JUDGE_BACKOFF_BASE_SECONDS=0.1,  # Accelerated for CLI simulation
        JUDGE_BACKOFF_MAX_SECONDS=1.0,
        JUDGE_429_MAX_RETRIES=5,
    )

    governor = OutboundJudgeGovernor(settings=settings)
    chaos_client = Chaos429JudgeClient(error_ratio=args.ratio, success_latency=0.02)

    start_time = time.perf_counter()
    successful = 0
    failed_exhausted = 0
    circuit_breaker_rejections = 0

    async def _worker(req_id: int) -> None:
        nonlocal successful, failed_exhausted, circuit_breaker_rejections
        try:
            await governor.execute(
                chaos_client.generate,
                prompt=f"Evaluate query for req-{req_id}",
            )
            successful += 1
        except JudgeRateLimitError:
            failed_exhausted += 1
        except Exception:
            if governor.circuit_state == CircuitBreakerState.OPEN:
                circuit_breaker_rejections += 1
            else:
                failed_exhausted += 1

    tasks = [_worker(i) for i in range(args.requests)]
    await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - start_time

    report: dict[str, Any] = {
        "total_requests": args.requests,
        "injection_ratio": args.ratio,
        "concurrency_cap": args.concurrency,
        "rate_limit_rps": args.rps,
        "elapsed_seconds": round(elapsed, 3),
        "successful_requests": successful,
        "failed_exhausted_retries": failed_exhausted,
        "circuit_breaker_rejections": circuit_breaker_rejections,
        "circuit_breaker_state": governor.circuit_state.value,
        "total_driver_invocations": chaos_client.total_invocations,
        "simulated_429_count": chaos_client.rate_limited_count,
    }

    print("Chaos Simulation Completed:")
    print(f"Elapsed Time:                {report['elapsed_seconds']}s")
    print(f"Successful Invocations:      {report['successful_requests']}")
    print(f"Exhausted 429 Retries:       {report['failed_exhausted_retries']}")
    print(f"Total Underlying Attempts:   {report['total_driver_invocations']}")
    print(f"Total 429s Injected:         {report['simulated_429_count']}")
    print(f"Circuit Breaker Final State: {report['circuit_breaker_state']}")

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        def _save_json() -> None:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)

        await asyncio.to_thread(_save_json)
        print(f"\n[INFO] Wrote chaos simulation report to: {out_path}")

    # Chaos invariant check: for moderate 429 ratio (<= 0.6), requests must succeed
    if args.ratio <= 0.6 and successful == 0:
        print("\n[ERROR] All requests failed under moderate 429 ratio!", file=sys.stderr)
        return 1

    print("\n[SUCCESS] Backpressure and jittered retry simulation verified.\n")
    return 0


def main() -> None:
    args = parse_arguments()
    exit_code = asyncio.run(run_simulation(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

"""High-throughput golden-set evaluation benchmarking suite."""

import argparse
import asyncio
import copy
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
from internal.evaluation.judges.triton_client import TritonJudgeClient  # noqa: E402
from internal.evaluation.models import BatchEvaluationInput, GoldenSampleItem  # noqa: E402
from internal.evaluation.ratelimiter import OutboundJudgeGovernor  # noqa: E402
from internal.evaluation.runner import BatchEvaluationRunner  # noqa: E402
from internal.storage.clickhouse_sink import ClickHouseSink  # noqa: E402


class BenchmarkMockJudgeClient(BaseJudgeClient):
    """Deterministic mock judge client simulating realistic network I/O latencies."""

    def __init__(self, mean_latency_seconds: float = 0.2) -> None:
        self.mean_latency = mean_latency_seconds
        self.recorded_network_latencies_ms: list[float] = []

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        # Simulate network delay with slight jitter
        delay = max(0.005, random.gauss(self.mean_latency, self.mean_latency * 0.15))
        await asyncio.sleep(delay)
        latency_ms = delay * 1000.0
        self.recorded_network_latencies_ms.append(latency_ms)

        sys_str = system_prompt or ""
        if "retrieval quality" in sys_str or "chunk_idx" in prompt or "verdicts" in sys_str:
            payload: dict[str, Any] = {
                "verdicts": [
                    {"chunk_idx": 0, "verdict": 1, "reasoning": "Relevant context chunk."},
                    {"chunk_idx": 1, "verdict": 1, "reasoning": "Corroborating context."},
                ]
            }
        elif "faithfulness" in sys_str or "atomic" in sys_str or "extracted_claims" in sys_str:
            payload = {
                "extracted_claims": [
                    {
                        "claim": "Answer is grounded directly in context passages.",
                        "supported": True,
                        "citation_index": 0,
                    }
                ],
                "reasoning": "All statements are backed by retrieved documents.",
            }
        else:
            payload = {
                "score": 0.95,
                "reasoning": "Answer addresses query concisely.",
            }

        return JudgeResponse(
            content=json.dumps(payload),
            parsed_json=payload,
            prompt_tokens=45,
            completion_tokens=30,
            latency_ms=int(latency_ms),
        )

    async def aclose(self) -> None:
        pass


class MockBenchmarkClickHouseClient:
    """High-speed in-memory ClickHouse mock client for benchmark persistence profiling."""

    def __init__(self) -> None:
        self.inserts: list[tuple[str, int]] = []
        self.insert_latencies_ms: list[float] = []

    def command(self, cmd: str) -> None:
        pass

    def insert(self, table: str, data: list[list[Any]], column_names: list[str]) -> None:
        t0 = time.perf_counter()
        time.sleep(0.001)  # Simulate fast micro-batch write I/O (1ms)
        self.inserts.append((table, len(data)))
        self.insert_latencies_ms.append((time.perf_counter() - t0) * 1000.0)

    def close(self) -> None:
        pass


def calculate_quantiles(data: list[float]) -> dict[str, float]:
    """Calculate P50, P90, P95, and P99 quantiles from a list of numerical values."""
    if not data:
        return {"p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0, "min": 0.0, "max": 0.0}

    s = sorted(data)
    n = len(s)

    def _q(p: float) -> float:
        idx = min(n - 1, max(0, int(p * n)))
        return s[idx]

    return {
        "p50": round(_q(0.50), 3),
        "p90": round(_q(0.90), 3),
        "p95": round(_q(0.95), 3),
        "p99": round(_q(0.99), 3),
        "min": round(s[0], 3),
        "max": round(s[-1], 3),
        "mean": round(sum(s) / n, 3),
    }


def load_and_scale_samples(
    fixture_path: str,
    target_count: int,
) -> list[GoldenSampleItem]:
    """Load base fixture and dynamically scale to target sample count."""
    p = Path(fixture_path)
    if not p.is_file():
        raise FileNotFoundError(f"Fixture file not found: {p}")

    with open(p, encoding="utf-8") as f:
        raw_items = json.load(f)

    base_samples = [GoldenSampleItem.model_validate(item) for item in raw_items]
    if not base_samples:
        raise ValueError(f"No samples found in fixture: {fixture_path}")

    scaled_samples: list[GoldenSampleItem] = []
    base_len = len(base_samples)
    for idx in range(target_count):
        source = base_samples[idx % base_len]
        cloned = copy.deepcopy(source)
        cloned.sample_idx = idx
        cloned.sample_id = f"bench-{idx:04d}-{source.sample_id}"
        scaled_samples.append(cloned)

    return scaled_samples


def parse_arguments() -> argparse.Namespace:
    """Parse benchmark suite command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Benchmark golden-set evaluation throughput and latency quantiles."
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=100,
        help="Target number of evaluation samples (default: 100).",
    )
    parser.add_argument(
        "--replicate-to",
        type=int,
        default=None,
        help="Replicate fixture up to specified sample count (defaults to --samples).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Outbound judge concurrency cap (default: 20).",
    )
    parser.add_argument(
        "--golden-set",
        type=str,
        default="test/fixtures/golden_set.json",
        help="Path to base golden-set fixture JSON.",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default="http://rag-inference-serving:8001",
        help="Judge inference endpoint URL.",
    )
    parser.add_argument(
        "--mock-judge",
        action="store_true",
        help="Enable deterministic offline mock judge with configurable latency.",
    )
    parser.add_argument(
        "--mock-latency-mean",
        type=float,
        default=0.2,
        help="Mean mock judge latency in seconds (default: 0.2s).",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Optional path to write structured JSON benchmark results.",
    )
    return parser.parse_args()


async def run_benchmark(args: argparse.Namespace) -> int:
    """Execute evaluation benchmark and validate platform SLOs."""
    target_count = args.replicate_to if args.replicate_to is not None else args.samples
    samples = load_and_scale_samples(args.golden_set, target_count)

    print("=" * 65)
    print("      RAG EVALUATION BATCH THROUGHPUT BENCHMARK SUITE     ")
    print("=" * 65)
    print(f"Total Scaled Samples:     {len(samples)}")
    print(f"Concurrency Limit:        {args.concurrency}")
    print(f"Base Fixture:             {args.golden_set}")
    print(f"Mock Judge Mode:          {args.mock_judge}")
    if args.mock_judge:
        print(f"Mock Judge Mean Latency:  {args.mock_latency_mean}s")
    else:
        print(f"Judge Endpoint:           {args.endpoint}")
    print("=" * 65 + "\n")

    settings = Settings(
        JUDGE_ENDPOINT=args.endpoint,
        JUDGE_MAX_CONCURRENT_REQUESTS=args.concurrency,
        JUDGE_RATE_LIMIT_RPS=50.0,
        CLICKHOUSE_BATCH_SIZE=1000,
        CLICKHOUSE_FLUSH_INTERVAL_SECONDS=1.0,
    )

    mock_judge: BenchmarkMockJudgeClient | None = None
    if args.mock_judge:
        mock_judge = BenchmarkMockJudgeClient(mean_latency_seconds=args.mock_latency_mean)
        judge_client: BaseJudgeClient = mock_judge
    else:
        judge_client = TritonJudgeClient(settings=settings)

    mock_ch_client = MockBenchmarkClickHouseClient()
    sink = ClickHouseSink(settings=settings, client=mock_ch_client, start_flusher=False)
    governor = OutboundJudgeGovernor(settings=settings)
    runner = BatchEvaluationRunner(
        judge_client=judge_client,
        governor=governor,
        sink=sink,
        settings=settings,
    )

    batch_input = BatchEvaluationInput(
        tenant_id="benchmark-tenant",
        run_id=f"bench-{int(time.time())}",
        dataset_id="golden-set-benchmark",
        samples=samples,
        eval_metrics=["context_precision", "faithfulness", "answer_relevance"],
        concurrency_limit=args.concurrency,
    )

    # Per-sample latency tracking
    sample_latencies: dict[int, float] = {}
    sample_start_times: dict[int, float] = {}
    total_start = time.perf_counter()

    for s in samples:
        sample_start_times[s.sample_idx] = time.perf_counter()

    async def _on_sample_done(idx: int, _item: Any, _results: Any) -> None:
        if idx in sample_start_times:
            sample_latencies[idx] = (time.perf_counter() - sample_start_times[idx]) * 1000.0

    try:
        output = await runner.run_batch(batch_input, on_sample_complete=_on_sample_done)
        await sink.close()
    finally:
        await judge_client.aclose()

    total_wall_clock_seconds = time.perf_counter() - total_start
    per_sample_latencies_ms = list(sample_latencies.values())
    latency_quantiles_ms = calculate_quantiles(per_sample_latencies_ms)
    throughput_samples_per_sec = (
        round(output.evaluated_samples / total_wall_clock_seconds, 2)
        if total_wall_clock_seconds > 0
        else 0.0
    )

    judge_network_quantiles_ms = (
        calculate_quantiles(mock_judge.recorded_network_latencies_ms) if mock_judge else {}
    )

    # Validate Platform SLO:
    # SLO: Total batch turnaround for 100 samples <= 180s under 20 concurrent connections
    slo_turnaround_max_seconds = 180.0
    turnaround_slo_passed = (
        total_wall_clock_seconds <= slo_turnaround_max_seconds if len(samples) >= 100 else True
    )

    report: dict[str, Any] = {
        "benchmark_summary": {
            "total_samples": output.total_samples,
            "evaluated_samples": output.evaluated_samples,
            "failed_samples": output.failed_samples_count,
            "concurrency_limit": args.concurrency,
            "total_turnaround_seconds": round(total_wall_clock_seconds, 3),
            "throughput_samples_per_sec": throughput_samples_per_sec,
            "slo_target_seconds": slo_turnaround_max_seconds,
            "slo_passed": turnaround_slo_passed,
        },
        "quality_metrics": {
            "avg_context_precision": round(output.avg_context_precision, 4),
            "avg_faithfulness": round(output.avg_faithfulness, 4),
            "avg_answer_relevance": round(output.avg_answer_relevance, 4),
            "passed_quality_gates": output.passed_quality_gates,
        },
        "per_sample_latency_ms": latency_quantiles_ms,
        "latency_breakdown": {
            "judge_network_io_ms": judge_network_quantiles_ms,
            "clickhouse_insert_count": len(mock_ch_client.inserts),
            "clickhouse_insert_latency_ms": calculate_quantiles(mock_ch_client.insert_latencies_ms),
        },
    }

    print("=" * 65)
    print("                     BENCHMARK RESULTS                        ")
    print("=" * 65)
    turnaround_val = report["benchmark_summary"]["total_turnaround_seconds"]
    print(f"Total Wall-Clock Time:       {turnaround_val}s")
    print(f"Evaluated Samples:           {report['benchmark_summary']['evaluated_samples']}")
    print(f"Throughput:                  {throughput_samples_per_sec} samples/sec")
    print(f"Per-Sample Latency P50:      {latency_quantiles_ms['p50']} ms")
    print(f"Per-Sample Latency P90:      {latency_quantiles_ms['p90']} ms")
    print(f"Per-Sample Latency P95:      {latency_quantiles_ms['p95']} ms")
    print(f"Per-Sample Latency P99:      {latency_quantiles_ms['p99']} ms")
    print(f"SLO Target (<= 180s):        {'PASSED' if turnaround_slo_passed else 'FAILED'}")
    print("=" * 65)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        def _save_json() -> None:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)

        await asyncio.to_thread(_save_json)
        print(f"\n[INFO] Wrote benchmark report to: {out_path}")

    if not turnaround_slo_passed:
        print(
            f"\n[ERROR] Platform SLO breached: turnaround {total_wall_clock_seconds:.2f}s "
            f"exceeds {slo_turnaround_max_seconds}s for 100 samples!",
            file=sys.stderr,
        )
        return 1

    print("\n[SUCCESS] Throughput benchmark completed within platform SLO budgets.\n")
    return 0


def main() -> None:
    args = parse_arguments()
    exit_code = asyncio.run(run_benchmark(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

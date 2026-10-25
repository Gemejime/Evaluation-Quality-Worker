"""Continuous soak testing and cross-platform memory stability profiler."""

import argparse
import asyncio
import ctypes
import gc
import json
import os
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
from internal.evaluation.models import BatchEvaluationInput, GoldenSampleItem  # noqa: E402
from internal.evaluation.ratelimiter import OutboundJudgeGovernor  # noqa: E402
from internal.evaluation.runner import BatchEvaluationRunner  # noqa: E402
from internal.storage.clickhouse_sink import ClickHouseSink  # noqa: E402


def get_process_rss_bytes() -> int:
    """Return current process Resident Set Size (RSS) in bytes using OS primitives."""
    if sys.platform == "win32":

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(ProcessMemoryCounters)
        handle = ctypes.windll.kernel32.GetCurrentProcess()
        if ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
            return int(counters.WorkingSetSize)
        return 0
    else:
        try:
            with open("/proc/self/statm", encoding="utf-8") as f:
                parts = f.read().split()
                pages = int(parts[1])
                page_size = os.sysconf("SC_PAGE_SIZE")
                return pages * page_size
        except Exception:
            import resource

            usage = resource.getrusage(resource.RUSAGE_SELF)
            # Linux returns ru_maxrss in kilobytes, macOS in bytes
            multiplier = 1024 if sys.platform != "darwin" else 1
            return int(usage.ru_maxrss * multiplier)


class SoakMockJudgeClient(BaseJudgeClient):
    """Zero-overhead mock judge for high-iteration soak testing."""

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        await asyncio.sleep(0.001)
        payload = {
            "verdicts": [{"chunk_idx": 0, "verdict": 1, "reasoning": "Valid."}],
            "extracted_claims": [
                {"claim": "Valid statement.", "supported": True, "citation_index": 0}
            ],
            "score": 0.95,
            "reasoning": "Concise grounded answer.",
        }
        return JudgeResponse(
            content=json.dumps(payload),
            parsed_json=payload,
            prompt_tokens=30,
            completion_tokens=20,
            latency_ms=1,
        )

    async def aclose(self) -> None:
        pass


class SoakMockClickHouseClient:
    """Mock ClickHouse sink capturing records without unbounded heap accumulation."""

    def __init__(self) -> None:
        self.total_inserted_rows = 0

    def command(self, cmd: str) -> None:
        pass

    def insert(self, table: str, data: list[list[Any]], column_names: list[str]) -> None:
        self.total_inserted_rows += len(data)

    def close(self) -> None:
        pass


def parse_arguments() -> argparse.Namespace:
    """Parse soak testing CLI parameters."""
    parser = argparse.ArgumentParser(
        description="Execute long-running soak test and RSS stability validation."
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=0.0,
        help="Maximum duration in hours to run soak test (0 for iteration-based).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="Number of batch evaluation iterations to execute (default: 100).",
    )
    parser.add_argument(
        "--samples-per-batch",
        type=int,
        default=10,
        help="Samples evaluated per batch iteration (default: 10).",
    )
    parser.add_argument(
        "--warmup-iterations",
        type=int,
        default=5,
        help="Warmup iterations before baseline RSS is established (default: 5).",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Optional path to output structured JSON soak test profile.",
    )
    return parser.parse_args()


async def run_soak_test(args: argparse.Namespace) -> int:
    """Execute continuous soak test and assert RSS flat profile <= 2.0 GiB."""
    print("=" * 65)
    print("      LONG-RUNNING SOAK TEST & MEMORY STABILITY HARNESS       ")
    print("=" * 65)
    print(f"Target Iterations:    {args.iterations}")
    print(f"Samples per Batch:    {args.samples_per_batch}")
    print(f"Warmup Iterations:    {args.warmup_iterations}")
    if args.duration_hours > 0:
        print(f"Max Duration:         {args.duration_hours} hours")
    print("=" * 65 + "\n")

    settings = Settings(
        JUDGE_MAX_CONCURRENT_REQUESTS=20,
        JUDGE_RATE_LIMIT_RPS=1000.0,  # Uncapped for soak throughput
        CLICKHOUSE_BATCH_SIZE=500,
    )

    judge_client = SoakMockJudgeClient()
    mock_ch_client = SoakMockClickHouseClient()
    sink = ClickHouseSink(settings=settings, client=mock_ch_client, start_flusher=False)
    governor = OutboundJudgeGovernor(settings=settings)
    runner = BatchEvaluationRunner(
        judge_client=judge_client,
        governor=governor,
        sink=sink,
        settings=settings,
    )

    # Base samples fixture
    samples = [
        GoldenSampleItem(
            sample_id=f"soak-sample-{i}",
            sample_idx=i,
            query=f"Soak query {i}",
            retrieved_contexts=["Context passage 1", "Context passage 2"],
            generated_answer=f"Generated answer {i}",
        )
        for i in range(args.samples_per_batch)
    ]

    initial_rss_mb = get_process_rss_bytes() / (1024 * 1024)
    print(f"Initial Process RSS: {initial_rss_mb:.2f} MB")

    baseline_rss_mb: float | None = None
    max_rss_mb = initial_rss_mb
    rss_history_mb: list[float] = []

    start_time = time.perf_counter()
    max_duration_seconds = args.duration_hours * 3600.0 if args.duration_hours > 0 else float("inf")

    iteration = 0
    while iteration < args.iterations:
        if time.perf_counter() - start_time >= max_duration_seconds:
            print("\n[INFO] Maximum duration reached, concluding soak execution.")
            break

        iteration += 1
        batch_input = BatchEvaluationInput(
            tenant_id="soak-tenant",
            run_id=f"soak-run-{iteration}",
            dataset_id="soak-dataset",
            samples=samples,
            eval_metrics=["context_precision", "faithfulness", "answer_relevance"],
            concurrency_limit=20,
        )

        await runner.run_batch(batch_input)

        if iteration % 10 == 0 or iteration == args.warmup_iterations:
            gc.collect()
            current_rss_mb = get_process_rss_bytes() / (1024 * 1024)
            rss_history_mb.append(round(current_rss_mb, 2))
            max_rss_mb = max(max_rss_mb, current_rss_mb)

            if iteration == args.warmup_iterations:
                baseline_rss_mb = current_rss_mb
                print(f"[WARMUP COMPLETE] Baseline RSS established: {baseline_rss_mb:.2f} MB")
            else:
                delta_str = (
                    f"(Δ: {current_rss_mb - baseline_rss_mb:+.2f} MB)"
                    if baseline_rss_mb is not None
                    else ""
                )
                print(
                    f"Iteration {iteration:4d}/{args.iterations}: "
                    f"RSS = {current_rss_mb:6.2f} MB {delta_str}"
                )

    await sink.close()
    elapsed_seconds = time.perf_counter() - start_time
    final_rss_mb = get_process_rss_bytes() / (1024 * 1024)

    if baseline_rss_mb is None:
        baseline_rss_mb = initial_rss_mb

    delta_rss_mb = final_rss_mb - baseline_rss_mb

    # Invariants:
    # 1. Total RSS <= 2.0 GiB (2048 MB)
    # 2. Delta RSS after warmup <= 100 MB under soak load
    rss_bound_passed = final_rss_mb <= 2048.0
    delta_bound_passed = delta_rss_mb <= 100.0

    report: dict[str, Any] = {
        "soak_summary": {
            "completed_iterations": iteration,
            "total_samples_evaluated": iteration * args.samples_per_batch,
            "elapsed_seconds": round(elapsed_seconds, 2),
            "initial_rss_mb": round(initial_rss_mb, 2),
            "baseline_rss_mb": round(baseline_rss_mb, 2),
            "final_rss_mb": round(final_rss_mb, 2),
            "peak_rss_mb": round(max_rss_mb, 2),
            "delta_rss_mb": round(delta_rss_mb, 2),
            "rss_under_2gib_passed": rss_bound_passed,
            "delta_rss_under_100mb_passed": delta_bound_passed,
        },
        "rss_samples_mb": rss_history_mb,
    }

    print("\n" + "=" * 65)
    print("                     SOAK TEST SUMMARY                        ")
    print("=" * 65)
    print(f"Iterations Completed:   {iteration}")
    print(f"Elapsed Time:           {elapsed_seconds:.2f}s")
    print(f"Baseline RSS:           {baseline_rss_mb:.2f} MB")
    print(f"Final RSS:              {final_rss_mb:.2f} MB")
    print(f"Delta RSS:              {delta_rss_mb:+.2f} MB (Target: <= +100 MB)")
    print(f"Memory Bound (< 2.0GiB):{'PASSED' if rss_bound_passed else 'FAILED'}")
    print("=" * 65)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        def _save_json() -> None:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)

        await asyncio.to_thread(_save_json)
        print(f"\n[INFO] Wrote soak test report to: {out_path}")

    if not rss_bound_passed or not delta_bound_passed:
        print("\n[ERROR] Memory stability invariant breached!", file=sys.stderr)
        return 1

    print("\n[SUCCESS] Soak test verified flat RSS profile under memory limits.\n")
    return 0


def main() -> None:
    args = parse_arguments()
    exit_code = asyncio.run(run_soak_test(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

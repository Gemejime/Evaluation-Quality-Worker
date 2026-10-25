"""CLI script triggering CI/CD golden-set regression evaluation quality gates."""

import argparse
import asyncio
import json
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


class MockDeterministicJudgeClient(BaseJudgeClient):
    """Deterministic mock judge client producing high-quality verdicts for offline CI validation."""

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        await asyncio.sleep(0.005)

        sys_str = system_prompt or ""
        # Context Precision prompt
        if "retrieval quality" in sys_str or "verdicts" in sys_str or "chunk_idx" in prompt:
            payload: dict[str, Any] = {
                "verdicts": [
                    {"chunk_idx": 0, "verdict": 1, "reasoning": "Relevant passage."},
                    {"chunk_idx": 1, "verdict": 1, "reasoning": "Additional facts."},
                ]
            }
        # Faithfulness prompt
        elif "faithfulness" in sys_str or "atomic" in sys_str or "extracted_claims" in sys_str:
            payload = {
                "extracted_claims": [
                    {
                        "claim": "Answer is grounded directly in retrieved contexts.",
                        "supported": True,
                        "citation_index": 0,
                    }
                ],
                "reasoning": "All claims are strictly grounded in reference passages.",
            }
        # Answer Relevance prompt
        else:
            payload = {
                "score": 0.95,
                "reasoning": "Answer directly and concisely addresses the user query.",
            }

        text = json.dumps(payload)
        return JudgeResponse(
            content=text,
            parsed_json=payload,
            prompt_tokens=40,
            completion_tokens=25,
            latency_ms=10,
        )

    async def aclose(self) -> None:
        pass


def load_golden_set(file_path_str: str) -> list[GoldenSampleItem]:
    """Load and deserialize golden sample items from filesystem synchronously."""
    golden_set_path = Path(file_path_str)
    if not golden_set_path.is_file():
        raise FileNotFoundError(f"Golden-set file not found at: {golden_set_path}")

    with open(golden_set_path, encoding="utf-8") as f:
        samples_raw = json.load(f)

    return [GoldenSampleItem.model_validate(item) for item in samples_raw]


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments for golden-set regression gate execution."""
    parser = argparse.ArgumentParser(
        description="Trigger golden-set evaluation regression quality gate."
    )
    parser.add_argument(
        "--golden-set",
        type=str,
        default="test/fixtures/golden_set.json",
        help="Path to golden-set JSON fixture.",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default="http://rag-inference-serving:8001",
        help="Downstream judge inference endpoint URL.",
    )
    parser.add_argument(
        "--mock-judge",
        action="store_true",
        help="Use deterministic mock judge for offline CI evaluation.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Max concurrency limit for judge evaluations.",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.82,
        help="Minimum required average score for quality gates (default 0.82).",
    )
    parser.add_argument(
        "--tenant-id",
        type=str,
        default="ci-regression",
        help="Tenant identifier for evaluation run.",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=f"run-{int(time.time())}",
        help="Unique evaluation run identifier.",
    )
    return parser.parse_args()


async def run_regression_gate(args: argparse.Namespace) -> int:
    """Execute the regression gate evaluation and assert release thresholds."""
    try:
        samples = await asyncio.to_thread(load_golden_set, args.golden_set)
    except Exception as exc:
        print(f"[ERROR] Failed to load golden set: {exc}", file=sys.stderr)
        return 1

    total_samples = len(samples)
    print(f"Loaded {total_samples} samples from {args.golden_set}")

    settings = Settings(
        JUDGE_ENDPOINT=args.endpoint,
        JUDGE_MAX_CONCURRENT_REQUESTS=args.concurrency,
        QUALITY_GATE_MIN_FAITHFULNESS=args.min_score,
        QUALITY_GATE_MIN_CONTEXT_PRECISION=args.min_score,
        QUALITY_GATE_MIN_ANSWER_RELEVANCE=args.min_score,
    )

    if args.mock_judge:
        print("[INFO] Running in --mock-judge mode (deterministic offline judge).")
        judge_client: BaseJudgeClient = MockDeterministicJudgeClient()
    else:
        print(f"[INFO] Running against live judge endpoint: {args.endpoint}")
        judge_client = TritonJudgeClient(settings=settings)

    governor = OutboundJudgeGovernor(settings)
    runner = BatchEvaluationRunner(
        judge_client=judge_client,
        governor=governor,
        sink=None,
        settings=settings,
    )

    batch_input = BatchEvaluationInput(
        tenant_id=args.tenant_id,
        run_id=args.run_id,
        dataset_id="golden-set-regression",
        samples=samples,
        eval_metrics=["context_precision", "faithfulness", "answer_relevance"],
        concurrency_limit=args.concurrency,
    )

    try:
        output = await runner.run_batch(batch_input)
    finally:
        await judge_client.aclose()

    print("\n" + "=" * 50)
    print("       GOLDEN-SET EVALUATION REGRESSION RESULTS       ")
    print("=" * 50)
    print(f"Tenant ID:              {output.tenant_id}")
    print(f"Run ID:                 {output.run_id}")
    print(f"Total Samples:          {output.total_samples}")
    print(f"Evaluated Samples:      {output.evaluated_samples}")
    print(f"Failed Samples (DLQ):   {output.failed_samples_count}")
    print(
        f"Context Precision:      {output.avg_context_precision:.4f} "
        f"(Threshold: >= {args.min_score})"
    )
    print(f"Faithfulness:           {output.avg_faithfulness:.4f} (Threshold: >= {args.min_score})")
    print(
        f"Answer Relevance:       {output.avg_answer_relevance:.4f} "
        f"(Threshold: >= {args.min_score})"
    )
    print(f"Duration:               {output.duration_seconds:.2f}s")
    print("=" * 50)

    passed = (
        output.avg_faithfulness >= args.min_score
        and output.avg_context_precision >= args.min_score
        and output.avg_answer_relevance >= args.min_score
        and output.failed_samples_count == 0
    )

    if passed:
        print("\n[SUCCESS] All evaluation quality gates passed (tau >= 0.82, DLQ = 0).\n")
        return 0

    print(
        "\n[FAILURE] Regression quality gates breached! Pull request or deployment blocked.\n",
        file=sys.stderr,
    )
    return 1


def main() -> None:
    """CLI entrypoint."""
    args = parse_arguments()
    exit_code = asyncio.run(run_regression_gate(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

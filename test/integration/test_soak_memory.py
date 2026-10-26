"""Integration test verifying flat process memory profile and zero leak accumulation."""

import asyncio
import gc
import json
from typing import Any

import pytest

from internal.config.settings import Settings
from internal.evaluation.judges.base import BaseJudgeClient, JudgeResponse
from internal.evaluation.models import BatchEvaluationInput, GoldenSampleItem
from internal.evaluation.ratelimiter import OutboundJudgeGovernor
from internal.evaluation.runner import BatchEvaluationRunner
from scripts.soak_test_memory import get_process_rss_bytes


class LightweightMockJudge(BaseJudgeClient):
    """Minimal judge producing instant mock responses for fast leak profiling."""

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        await asyncio.sleep(0.0001)
        payload = {
            "verdicts": [{"chunk_idx": 0, "verdict": 1, "reasoning": "Valid."}],
            "extracted_claims": [
                {"claim": "Valid factual statement.", "supported": True, "citation_index": 0}
            ],
            "score": 0.95,
            "reasoning": "Concise verified answer.",
        }
        return JudgeResponse(
            content=json.dumps(payload),
            parsed_json=payload,
            prompt_tokens=20,
            completion_tokens=15,
            latency_ms=1,
        )

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_memory_stability_across_repetitive_evaluations() -> None:
    """Execute 35 evaluation batches in CI and assert delta RSS <= 50 MB and total RSS < 2.0 GiB."""
    settings = Settings(
        JUDGE_MAX_CONCURRENT_REQUESTS=10,
        JUDGE_RATE_LIMIT_RPS=500.0,
    )
    judge_client = LightweightMockJudge()
    governor = OutboundJudgeGovernor(settings=settings)
    runner = BatchEvaluationRunner(
        judge_client=judge_client,
        governor=governor,
        sink=None,
        settings=settings,
    )

    samples = [
        GoldenSampleItem(
            sample_id=f"leak-sample-{i}",
            sample_idx=i,
            query=f"Query {i}",
            retrieved_contexts=["Context 1", "Context 2"],
            generated_answer=f"Answer {i}",
        )
        for i in range(3)
    ]

    # Warmup iterations to let Python VM and imported modules stabilize
    for w in range(3):
        batch_input = BatchEvaluationInput(
            tenant_id="warmup-tenant",
            run_id=f"warmup-run-{w}",
            dataset_id="warmup-dataset",
            samples=samples,
            eval_metrics=["context_precision", "faithfulness", "answer_relevance"],
            concurrency_limit=10,
        )
        await runner.run_batch(batch_input)

    gc.collect()
    baseline_rss_mb = get_process_rss_bytes() / (1024 * 1024)

    # Execute 25 iterations of batch evaluation
    for it in range(25):
        batch_input = BatchEvaluationInput(
            tenant_id="soak-tenant",
            run_id=f"soak-run-{it}",
            dataset_id="soak-dataset",
            samples=samples,
            eval_metrics=["context_precision", "faithfulness", "answer_relevance"],
            concurrency_limit=10,
        )
        output = await runner.run_batch(batch_input)
        assert output.evaluated_samples == 3
        assert output.failed_samples_count == 0

    gc.collect()
    final_rss_mb = get_process_rss_bytes() / (1024 * 1024)
    delta_rss_mb = final_rss_mb - baseline_rss_mb

    # Invariants:
    # 1. Total RSS < 2.0 GiB (2048 MB)
    assert final_rss_mb < 2048.0, f"Total RSS {final_rss_mb:.2f} MB exceeds 2.0 GiB limit"

    # 2. Delta RSS <= 50 MB
    assert delta_rss_mb <= 50.0, f"Memory growth {delta_rss_mb:.2f} MB exceeds 50 MB threshold"

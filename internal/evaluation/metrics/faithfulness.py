"""Faithfulness (Groundedness) metric evaluator executing single-prompt claim verification."""

import time
from typing import Any

import structlog

from internal.evaluation.judges.base import BaseJudgeClient
from internal.evaluation.metrics.base import (
    BaseMetricEvaluator,
    MetricResult,
    compute_reasoning_hash,
)
from internal.evaluation.models import GoldenSampleItem

logger = structlog.get_logger(__name__)

SYSTEM_PROMPT = """You are an expert evaluator assessing the faithfulness and groundedness
of an AI generated answer against retrieved reference contexts.
Your task is to:
1. Break down the generated answer into atomic, verifiable factual claims.
2. For each extracted claim, verify whether it is directly supported by the retrieved contexts.
3. Identify citation_index (0-indexed integer of the supporting chunk, or -1 if unsupported).

Output must be valid JSON in the exact format:
{
  "extracted_claims": [
    {
      "claim": "...",
      "supported": true,
      "citation_index": 0
    }
  ],
  "reasoning": "Summary of verification"
}"""


class FaithfulnessEvaluator(BaseMetricEvaluator):
    """Evaluates whether factual claims in generated answer are supported by contexts."""

    def __init__(self, judge_client: BaseJudgeClient) -> None:
        self.judge = judge_client

    @property
    def metric_name(self) -> str:
        return "faithfulness"

    async def evaluate(self, sample: GoldenSampleItem) -> MetricResult:
        start_time = time.perf_counter()

        contexts_text = "\n\n".join(
            f"--- Context [{idx}] ---\n{chunk}"
            for idx, chunk in enumerate(sample.retrieved_contexts)
        )
        user_prompt = (
            f"User Query:\n{sample.query}\n\n"
            f"Retrieved Reference Contexts:\n{contexts_text}\n\n"
            f"Generated Answer:\n{sample.generated_answer}"
        )

        judge_response = await self.judge.generate(
            prompt=user_prompt,
            system_prompt=SYSTEM_PROMPT,
        )

        parsed = judge_response.parsed_json or {}
        extracted_claims: list[dict[str, Any]] = parsed.get("extracted_claims", [])
        judge_reasoning = str(parsed.get("reasoning", "")).strip()

        total_claims = len(extracted_claims)
        if total_claims == 0:
            # Zero Extracted Claims Contract: 1.0 (no unsupported claims asserted)
            score = 1.0
            reasoning = (
                judge_reasoning or "No factual claims asserted in generated answer (score: 1.0)."
            )
        else:
            supported_count = 0
            for item in extracted_claims:
                if isinstance(item, dict):
                    supported = item.get("supported")
                    if supported in (True, "true", 1, "1"):
                        supported_count += 1

            score = supported_count / total_claims
            score = max(0.0, min(1.0, float(score)))
            reasoning = (
                judge_reasoning
                or f"Faithfulness: {score:.4f} ({supported_count}/{total_claims} claims supported)."
            )

        latency_ms = int((time.perf_counter() - start_time) * 1000)
        return MetricResult(
            metric_name=self.metric_name,
            score=round(score, 4),
            reasoning=reasoning,
            reasoning_hash=compute_reasoning_hash(reasoning),
            execution_latency_ms=latency_ms,
        )

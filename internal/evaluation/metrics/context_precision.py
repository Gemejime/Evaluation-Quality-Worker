"""Context Precision metric evaluator executing single-prompt multi-chunk evaluation."""

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

SYSTEM_PROMPT = """You are an expert evaluator assessing retrieval quality in a RAG system.
Given a user query and a list of retrieved context chunks, evaluate whether each
chunk is relevant and useful for answering the query.
For each chunk, provide:
1. chunk_idx (0-indexed integer matching the input)
2. verdict (integer: 1 if chunk contains directly relevant information, 0 otherwise)
3. reasoning (concise explanation)

Output must be valid JSON in the exact format:
{
  "verdicts": [
    {"chunk_idx": 0, "verdict": 1, "reasoning": "..."},
    ...
  ]
}"""


class ContextPrecisionEvaluator(BaseMetricEvaluator):
    """Evaluates whether retrieved context chunks relevant to the query are ranked higher."""

    def __init__(self, judge_client: BaseJudgeClient) -> None:
        self.judge = judge_client

    @property
    def metric_name(self) -> str:
        return "context_precision"

    async def evaluate(self, sample: GoldenSampleItem) -> MetricResult:
        start_time = time.perf_counter()

        if not sample.retrieved_contexts:
            reasoning = "No retrieved context chunks provided."
            return MetricResult(
                metric_name=self.metric_name,
                score=0.0,
                reasoning=reasoning,
                reasoning_hash=compute_reasoning_hash(reasoning),
                execution_latency_ms=int((time.perf_counter() - start_time) * 1000),
            )

        # Build prompt with all context chunks
        chunks_text = "\n\n".join(
            f"--- Context Chunk {idx} ---\n{chunk}"
            for idx, chunk in enumerate(sample.retrieved_contexts)
        )
        user_prompt = f"User Query: {sample.query}\n\nRetrieved Context Chunks:\n{chunks_text}"

        judge_response = await self.judge.generate(
            prompt=user_prompt,
            system_prompt=SYSTEM_PROMPT,
        )

        parsed = judge_response.parsed_json or {}
        verdicts_list: list[dict[str, Any]] = parsed.get("verdicts", [])

        # Fallback if judge returned single dict or alternative key
        if not verdicts_list and "verdict" in parsed:
            verdicts_list = [
                {
                    "chunk_idx": 0,
                    "verdict": parsed.get("verdict", 0),
                    "reasoning": parsed.get("reasoning", ""),
                }
            ]

        # Map verdicts to indices
        k = len(sample.retrieved_contexts)
        binary_verdicts = [0] * k

        for item in verdicts_list:
            if isinstance(item, dict):
                idx = item.get("chunk_idx")
                v = item.get("verdict")
                if isinstance(idx, int) and 0 <= idx < k:
                    binary_verdicts[idx] = 1 if v in (1, "1", True) else 0

        total_relevant = sum(binary_verdicts)
        if total_relevant == 0:
            score = 0.0
            reasoning = f"Zero relevant chunks identified out of {k} retrieved chunks."
        else:
            # Precision@k calculation
            cumulative_relevant = 0
            precision_sum = 0.0
            for rank, v in enumerate(binary_verdicts, start=1):
                if v == 1:
                    cumulative_relevant += 1
                    precision_at_k = cumulative_relevant / rank
                    precision_sum += precision_at_k

            score = precision_sum / total_relevant
            score = max(0.0, min(1.0, float(score)))
            reasoning = (
                f"Context Precision@K: {score:.4f} ({total_relevant}/{k} relevant chunks. "
                f"Binary verdicts: {binary_verdicts})"
            )

        latency_ms = int((time.perf_counter() - start_time) * 1000)
        return MetricResult(
            metric_name=self.metric_name,
            score=round(score, 4),
            reasoning=reasoning,
            reasoning_hash=compute_reasoning_hash(reasoning),
            execution_latency_ms=latency_ms,
        )

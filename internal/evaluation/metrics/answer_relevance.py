"""Answer Relevance metric evaluator scoring query-to-answer semantic relevance."""

import math
import time

import structlog

from internal.evaluation.judges.base import BaseJudgeClient
from internal.evaluation.metrics.base import (
    BaseMetricEvaluator,
    MetricResult,
    compute_reasoning_hash,
)
from internal.evaluation.models import GoldenSampleItem

logger = structlog.get_logger(__name__)

SYSTEM_PROMPT = """You are an expert evaluator assessing the relevance of an AI generated
answer to the original user query. Assess whether the answer directly addresses
the query without irrelevant tangents, superfluous rambling, or omitting key aspects.
Score the semantic relevance on a continuous scale from 0.0 (completely irrelevant
or divergent) to 1.0 (perfectly relevant and concise).

Output must be valid JSON in the exact format:
{
  "score": 0.95,
  "reasoning": "Concise justification for the score"
}"""


class AnswerRelevanceEvaluator(BaseMetricEvaluator):
    """Evaluates whether the generated answer directly addresses the user query."""

    def __init__(self, judge_client: BaseJudgeClient) -> None:
        self.judge = judge_client

    @property
    def metric_name(self) -> str:
        return "answer_relevance"

    async def evaluate(self, sample: GoldenSampleItem) -> MetricResult:
        start_time = time.perf_counter()

        user_prompt = f"User Query:\n{sample.query}\n\nGenerated Answer:\n{sample.generated_answer}"

        judge_response = await self.judge.generate(
            prompt=user_prompt,
            system_prompt=SYSTEM_PROMPT,
        )

        if judge_response.parsed_json is None:
            raise ValueError("Missing parsed JSON in judge response")

        parsed = judge_response.parsed_json
        raw_score = parsed.get("score")
        reasoning = str(parsed.get("reasoning", "")).strip()

        if raw_score is None:
            # Fallback check for verdict or rating
            raw_score = parsed.get("verdict", parsed.get("rating"))

        if raw_score is None:
            raise ValueError("No numeric score, verdict, or rating present in judge response")

        try:
            score = float(raw_score)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid non-numeric relevance score: {raw_score}") from exc

        if not math.isfinite(score):
            raise ValueError(f"Non-finite relevance score returned by judge: {score}")

        score = max(0.0, min(1.0, score))
        if not reasoning:
            reasoning = f"Answer relevance evaluated at {score:.4f}."

        latency_ms = int((time.perf_counter() - start_time) * 1000)
        return MetricResult(
            metric_name=self.metric_name,
            score=round(score, 4),
            reasoning=reasoning,
            reasoning_hash=compute_reasoning_hash(reasoning),
            execution_latency_ms=latency_ms,
        )

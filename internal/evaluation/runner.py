"""Batch evaluation runner orchestrating metrics, rate limiting, persistence, and DLQ."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from internal.config.settings import Settings, get_settings
from internal.evaluation.judges.base import BaseJudgeClient, JudgeResponse
from internal.evaluation.metrics.answer_relevance import AnswerRelevanceEvaluator
from internal.evaluation.metrics.base import BaseMetricEvaluator, MetricResult
from internal.evaluation.metrics.context_precision import ContextPrecisionEvaluator
from internal.evaluation.metrics.faithfulness import FaithfulnessEvaluator
from internal.evaluation.models import (
    BatchEvaluationInput,
    BatchEvaluationOutput,
    GoldenSampleItem,
)
from internal.evaluation.ratelimiter import OutboundJudgeGovernor
from internal.storage.clickhouse_sink import ClickHouseSink
from internal.telemetry.metrics import (
    EVAL_BATCH_DURATION_SECONDS,
    EVAL_METRIC_SCORE_VALUE,
    EVAL_SAMPLE_EXECUTION_LATENCY_SECONDS,
)

logger = structlog.get_logger(__name__)


class GovernedJudgeClient(BaseJudgeClient):
    """Judge client wrapper applying rate limiting, concurrency caps, and circuit breaking."""

    def __init__(self, client: BaseJudgeClient, governor: OutboundJudgeGovernor) -> None:
        self._client = client
        self._governor = governor

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        return await self._governor.execute(
            self._client.generate,
            prompt,
            system_prompt,
            response_schema,
        )

    async def aclose(self) -> None:
        await self._client.aclose()


class BatchEvaluationRunner:
    """Orchestrates golden-set batch evaluation pipelines with DLQ isolation and ClickHouse sink."""

    def __init__(
        self,
        judge_client: BaseJudgeClient,
        governor: OutboundJudgeGovernor | None = None,
        sink: ClickHouseSink | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.governor = governor or OutboundJudgeGovernor(self.settings)
        self.governed_judge = GovernedJudgeClient(judge_client, self.governor)
        self.sink = sink

        # Register native metric evaluators
        self._evaluators: dict[str, BaseMetricEvaluator] = {
            "context_precision": ContextPrecisionEvaluator(self.governed_judge),
            "faithfulness": FaithfulnessEvaluator(self.governed_judge),
            "answer_relevance": AnswerRelevanceEvaluator(self.governed_judge),
        }

    async def run_batch(
        self,
        batch_input: BatchEvaluationInput,
        on_sample_complete: Callable[[int, GoldenSampleItem, list[MetricResult]], Awaitable[None]]
        | None = None,
    ) -> BatchEvaluationOutput:
        """Execute batch evaluation over input samples with per-sample DLQ fault isolation."""
        start_time = time.perf_counter()
        total_samples = len(batch_input.samples)

        logger.info(
            "starting_batch_evaluation",
            tenant_id=batch_input.tenant_id,
            run_id=batch_input.run_id,
            dataset_id=batch_input.dataset_id,
            total_samples=total_samples,
            metrics=batch_input.eval_metrics,
        )

        active_evaluators = [
            self._evaluators[m] for m in batch_input.eval_metrics if m in self._evaluators
        ]

        # Tracking accumulators
        evaluated_samples = 0
        failed_samples_count = 0
        dlq_samples: list[dict[str, Any]] = []

        scores: dict[str, list[float]] = {m: [] for m in batch_input.eval_metrics}

        semaphore = asyncio.Semaphore(batch_input.concurrency_limit)

        async def _process_sample(
            sample: GoldenSampleItem,
        ) -> tuple[int, list[MetricResult] | None, Exception | None]:
            async with semaphore:
                try:
                    results: list[MetricResult] = []
                    for evaluator in active_evaluators:
                        res = await evaluator.evaluate(sample)
                        results.append(res)
                    return sample.sample_idx, results, None
                except Exception as exc:
                    return sample.sample_idx, None, exc

        tasks = [_process_sample(sample) for sample in batch_input.samples]
        sample_map = {s.sample_idx: s for s in batch_input.samples}

        # Process sample results as they complete
        for coro in asyncio.as_completed(tasks):
            sample_idx, metric_results, err = await coro
            sample_item = sample_map[sample_idx]

            if err is not None or metric_results is None:
                failed_samples_count += 1
                dlq_record = {
                    "tenant_id": batch_input.tenant_id,
                    "run_id": batch_input.run_id,
                    "sample_idx": sample_idx,
                    "sample_id": sample_item.sample_id,
                    "error_type": type(err).__name__ if err else "UnknownError",
                    "error_message": str(err),
                }
                dlq_samples.append(dlq_record)
                logger.warning(
                    "sample_evaluation_quarantined_to_dlq",
                    tenant_id=batch_input.tenant_id,
                    run_id=batch_input.run_id,
                    sample_idx=sample_idx,
                    sample_id=sample_item.sample_id,
                    error_type=dlq_record["error_type"],
                )
                continue

            evaluated_samples += 1
            for result in metric_results:
                scores[result.metric_name].append(result.score)

                # Record Prometheus latency
                EVAL_SAMPLE_EXECUTION_LATENCY_SECONDS.labels(
                    metric_name=result.metric_name,
                    judge_model=self.governor.judge_model,
                ).observe(result.execution_latency_ms / 1000.0)

                # Buffer into ClickHouse sink if available
                if self.sink is not None:
                    await self.sink.buffer_metric_point(
                        {
                            "tenant_id": batch_input.tenant_id,
                            "run_id": batch_input.run_id,
                            "sample_idx": sample_idx,
                            "sample_id": sample_item.sample_id,
                            "metric_name": result.metric_name,
                            "score": result.score,
                            "reasoning_hash": result.reasoning_hash,
                            "execution_latency_ms": result.execution_latency_ms,
                            "judge_model": self.governor.judge_model,
                        }
                    )

                # Secured structured logging: strict redaction of raw text,
                # truncated 16-character reasoning hash
                logger.info(
                    "sample_metric_evaluated",
                    tenant_id=batch_input.tenant_id,
                    run_id=batch_input.run_id,
                    sample_idx=sample_idx,
                    metric_name=result.metric_name,
                    score=result.score,
                    reasoning_hash=result.reasoning_hash[:16],
                    execution_latency_ms=result.execution_latency_ms,
                )

            # Resumption hook for Temporal heartbeat
            if on_sample_complete is not None:
                await on_sample_complete(sample_idx, sample_item, metric_results)

        duration_seconds = time.perf_counter() - start_time

        # Calculate averages
        avg_faithfulness = (
            sum(scores.get("faithfulness", [])) / len(scores["faithfulness"])
            if scores.get("faithfulness")
            else 0.0
        )
        avg_context_precision = (
            sum(scores.get("context_precision", [])) / len(scores["context_precision"])
            if scores.get("context_precision")
            else 0.0
        )
        avg_answer_relevance = (
            sum(scores.get("answer_relevance", [])) / len(scores["answer_relevance"])
            if scores.get("answer_relevance")
            else 0.0
        )

        metric_averages = {
            "faithfulness": (avg_faithfulness, self.settings.QUALITY_GATE_MIN_FAITHFULNESS),
            "context_precision": (
                avg_context_precision,
                self.settings.QUALITY_GATE_MIN_CONTEXT_PRECISION,
            ),
            "answer_relevance": (
                avg_answer_relevance,
                self.settings.QUALITY_GATE_MIN_ANSWER_RELEVANCE,
            ),
        }

        active_metrics = [
            m for m in batch_input.eval_metrics
            if m in scores and len(scores[m]) > 0
        ]

        if not active_metrics or failed_samples_count > 0:
            passed_quality_gates = False
        else:
            passed_quality_gates = all(
                metric_averages[m][0] >= metric_averages[m][1]
                for m in active_metrics
                if m in metric_averages
            )

        # Update Prometheus metrics
        if "faithfulness" in scores:
            EVAL_METRIC_SCORE_VALUE.labels(metric_name="faithfulness").set(avg_faithfulness)
        if "context_precision" in scores:
            EVAL_METRIC_SCORE_VALUE.labels(metric_name="context_precision").set(
                avg_context_precision
            )
        if "answer_relevance" in scores:
            EVAL_METRIC_SCORE_VALUE.labels(metric_name="answer_relevance").set(avg_answer_relevance)

        status_label = "completed" if failed_samples_count == 0 else "completed_with_errors"
        EVAL_BATCH_DURATION_SECONDS.labels(status=status_label).observe(duration_seconds)

        # Flush sink and insert run header
        if self.sink is not None:
            await self.sink.flush()
            await self.sink.insert_run(
                {
                    "tenant_id": batch_input.tenant_id,
                    "run_id": batch_input.run_id,
                    "pipeline_version": batch_input.pipeline_version,
                    "dataset_id": batch_input.dataset_id,
                    "sample_count": total_samples,
                    "avg_faithfulness": avg_faithfulness,
                    "avg_context_precision": avg_context_precision,
                    "avg_answer_relevance": avg_answer_relevance,
                    "duration_seconds": duration_seconds,
                    "status": "COMPLETED" if failed_samples_count == 0 else "FAILED_SAMPLES",
                }
            )

        logger.info(
            "batch_evaluation_finished",
            tenant_id=batch_input.tenant_id,
            run_id=batch_input.run_id,
            total_samples=total_samples,
            evaluated_samples=evaluated_samples,
            failed_samples_count=failed_samples_count,
            avg_faithfulness=round(avg_faithfulness, 4),
            avg_context_precision=round(avg_context_precision, 4),
            avg_answer_relevance=round(avg_answer_relevance, 4),
            passed_quality_gates=passed_quality_gates,
            duration_seconds=round(duration_seconds, 3),
        )

        return BatchEvaluationOutput(
            tenant_id=batch_input.tenant_id,
            run_id=batch_input.run_id,
            dataset_id=batch_input.dataset_id,
            total_samples=total_samples,
            evaluated_samples=evaluated_samples,
            failed_samples_count=failed_samples_count,
            avg_faithfulness=round(avg_faithfulness, 4),
            avg_context_precision=round(avg_context_precision, 4),
            avg_answer_relevance=round(avg_answer_relevance, 4),
            passed_quality_gates=passed_quality_gates,
            duration_seconds=round(duration_seconds, 3),
            dlq_samples=dlq_samples,
        )

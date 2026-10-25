"""Temporal activities for batch evaluation with resumption and dual heartbeating."""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
from temporalio import activity
from temporalio.exceptions import ApplicationError, CancelledError

from internal.config.settings import Settings, get_settings
from internal.evaluation.metrics.base import MetricResult
from internal.evaluation.models import (
    BatchEvaluationInput,
    EvaluationHeartbeatDetails,
    GoldenSampleItem,
)
from internal.evaluation.runner import BatchEvaluationRunner
from internal.temporal.models import EvaluateGoldenSetInput, EvaluateGoldenSetResult

logger = structlog.get_logger(__name__)


def get_heartbeat_details() -> Any:
    """Retrieve heartbeat details from active Temporal activity context or test harness."""
    try:
        if activity.in_activity():
            details = activity.info().heartbeat_details
            if details:
                return details[0] if len(details) == 1 else details
    except Exception:
        pass
    return None


class EvaluateGoldenSetActivity:
    """Temporal activity executing golden-set evaluation with out-of-order resumption."""

    def __init__(
        self,
        runner: BatchEvaluationRunner,
        settings: Settings | None = None,
    ) -> None:
        self.runner = runner
        self.settings = settings or get_settings()

    def _load_samples_from_uri(self, uri: str) -> list[GoldenSampleItem]:
        """Load and parse golden sample items from a filesystem path or URI."""
        file_path = uri[7:] if uri.startswith("file://") else uri
        target = Path(file_path)
        if not target.is_file():
            raise FileNotFoundError(f"Golden-set file not found at: {file_path}")

        with open(target, encoding="utf-8") as f:
            raw_data = json.load(f)

        if not isinstance(raw_data, list):
            raise ValueError(f"Golden-set payload must be a JSON array, got: {type(raw_data)}")

        return [GoldenSampleItem.model_validate(item) for item in raw_data]

    @activity.defn(name="EvaluateGoldenSetActivity")
    async def run(self, input_data: EvaluateGoldenSetInput) -> EvaluateGoldenSetResult:
        """Execute golden set evaluation activity with zero-recompute resumption."""
        logger.info(
            "evaluate_golden_set_activity_started",
            tenant_id=input_data.tenant_id,
            run_id=input_data.run_id,
            dataset_id=input_data.dataset_id,
            metrics=input_data.eval_metrics,
        )

        # 1. Resolve samples dataset
        all_samples: list[GoldenSampleItem]
        if input_data.golden_set_uri:
            all_samples = self._load_samples_from_uri(input_data.golden_set_uri)
        elif input_data.samples:
            all_samples = list(input_data.samples)
        else:
            all_samples = []

        total_samples = len(all_samples)
        if total_samples == 0:
            logger.warning(
                "evaluate_golden_set_empty_samples",
                tenant_id=input_data.tenant_id,
                run_id=input_data.run_id,
            )
            return EvaluateGoldenSetResult(
                tenant_id=input_data.tenant_id,
                run_id=input_data.run_id,
                dataset_id=input_data.dataset_id,
                total_samples=0,
                evaluated_samples_count=0,
                failed_samples_count=0,
                avg_faithfulness=0.0,
                avg_context_precision=0.0,
                avg_answer_relevance=0.0,
                passed_quality_gates=True,
                duration_seconds=0.0,
                dlq_samples=[],
            )

        # 2. Checkpoint inspection & Zero-Recompute Resumption (Out-of-Order Concurrency Safe)
        completed_indices: set[int] = set()
        try:
            raw_details = get_heartbeat_details()
            if raw_details:
                detail_dict: dict[str, Any] = {}
                if isinstance(raw_details, dict):
                    detail_dict = raw_details
                elif isinstance(raw_details, (list, tuple)) and len(raw_details) > 0:
                    first = raw_details[0]
                    if isinstance(first, dict):
                        detail_dict = first
                    elif hasattr(first, "model_dump"):
                        detail_dict = first.model_dump()
                elif hasattr(raw_details, "model_dump"):
                    detail_dict = raw_details.model_dump()

                if detail_dict:
                    checkpoint = EvaluationHeartbeatDetails.model_validate(detail_dict)
                    completed_indices = set(checkpoint.completed_sample_indices)
                    logger.info(
                        "resuming_activity_from_checkpoint",
                        tenant_id=input_data.tenant_id,
                        run_id=input_data.run_id,
                        completed_samples_count=len(completed_indices),
                    )
        except Exception as exc:
            logger.debug("no_active_checkpoint_found", error=str(exc))

        # Filter out already completed samples using set difference
        remaining_samples = [s for s in all_samples if s.sample_idx not in completed_indices]

        last_sample_id = ""
        last_sample_idx = -1

        def _make_heartbeat_payload() -> dict[str, Any]:
            return EvaluationHeartbeatDetails(
                tenant_id=input_data.tenant_id,
                run_id=input_data.run_id,
                current_sample_idx=last_sample_idx,
                total_samples=total_samples,
                current_sample_id=last_sample_id,
                completed_samples=len(completed_indices),
                completed_sample_indices=sorted(completed_indices),
                last_checkpoint_timestamp_utc=datetime.now(UTC).isoformat(),
            ).model_dump()

        # 3. Dual-Trigger Heartbeating
        # Periodic background ticker emits heartbeat every 10s to prevent
        # activity timeouts during downstream judge queuing and backoff delays
        async def _heartbeat_ticker() -> None:
            interval = self.settings.ACTIVITY_HEARTBEAT_INTERVAL_SECONDS
            try:
                while True:
                    await asyncio.sleep(interval)
                    try:
                        activity.heartbeat(_make_heartbeat_payload())
                    except Exception as err:
                        logger.debug("heartbeat_ticker_emission_error", error=str(err))
            except asyncio.CancelledError:
                pass

        # Per-sample completion callback updating checkpoint and emitting immediate heartbeat
        async def _on_sample_complete(
            sample_idx: int,
            sample_item: GoldenSampleItem,
            _metric_results: list[MetricResult],
        ) -> None:
            nonlocal last_sample_id, last_sample_idx
            completed_indices.add(sample_idx)
            last_sample_id = sample_item.sample_id
            last_sample_idx = sample_idx
            try:
                activity.heartbeat(_make_heartbeat_payload())
            except Exception as err:
                logger.debug("sample_heartbeat_emission_error", error=str(err))

        ticker_task = asyncio.create_task(_heartbeat_ticker())

        try:
            if remaining_samples:
                batch_input = BatchEvaluationInput(
                    tenant_id=input_data.tenant_id,
                    run_id=input_data.run_id,
                    dataset_id=input_data.dataset_id,
                    samples=remaining_samples,
                    eval_metrics=input_data.eval_metrics,
                    concurrency_limit=input_data.concurrency_limit,
                    pipeline_version=input_data.pipeline_version,
                )
                batch_output = await self.runner.run_batch(
                    batch_input=batch_input,
                    on_sample_complete=_on_sample_complete,
                )
            else:
                # All samples were already completed in previous attempt
                logger.info(
                    "all_samples_already_evaluated_in_prior_checkpoint",
                    tenant_id=input_data.tenant_id,
                    run_id=input_data.run_id,
                )
                return EvaluateGoldenSetResult(
                    tenant_id=input_data.tenant_id,
                    run_id=input_data.run_id,
                    dataset_id=input_data.dataset_id,
                    total_samples=total_samples,
                    evaluated_samples_count=len(completed_indices),
                    failed_samples_count=0,
                    avg_faithfulness=1.0,
                    avg_context_precision=1.0,
                    avg_answer_relevance=1.0,
                    passed_quality_gates=True,
                    duration_seconds=0.0,
                    dlq_samples=[],
                )
        except (CancelledError, asyncio.CancelledError):
            logger.warning(
                "activity_cancelled_flushing_storage_buffer",
                tenant_id=input_data.tenant_id,
                run_id=input_data.run_id,
            )
            if self.runner.sink is not None:
                await self.runner.sink.flush()
            raise
        except Exception as exc:
            logger.error(
                "activity_execution_failed_with_platform_error",
                tenant_id=input_data.tenant_id,
                run_id=input_data.run_id,
                error=str(exc),
            )
            if self.runner.sink is not None:
                await self.runner.sink.flush()
            raise ApplicationError(f"Platform failure: {exc}", non_retryable=True) from exc
        finally:
            ticker_task.cancel()
            try:
                await ticker_task
            except (asyncio.CancelledError, Exception):
                pass

        # Evaluate release quality gates scoped to active metrics in input_data
        passed_quality_gates = True
        if "faithfulness" in input_data.eval_metrics:
            if batch_output.avg_faithfulness < self.settings.QUALITY_GATE_MIN_FAITHFULNESS:
                passed_quality_gates = False
        if "context_precision" in input_data.eval_metrics:
            min_prec = self.settings.QUALITY_GATE_MIN_CONTEXT_PRECISION
            if batch_output.avg_context_precision < min_prec:
                passed_quality_gates = False
        if "answer_relevance" in input_data.eval_metrics:
            if batch_output.avg_answer_relevance < self.settings.QUALITY_GATE_MIN_ANSWER_RELEVANCE:
                passed_quality_gates = False
        if batch_output.failed_samples_count > 0:
            passed_quality_gates = False

        logger.info(
            "evaluate_golden_set_activity_completed",
            tenant_id=input_data.tenant_id,
            run_id=input_data.run_id,
            evaluated_count=len(completed_indices),
            failed_count=batch_output.failed_samples_count,
            passed_quality_gates=passed_quality_gates,
        )

        return EvaluateGoldenSetResult(
            tenant_id=input_data.tenant_id,
            run_id=input_data.run_id,
            dataset_id=input_data.dataset_id,
            total_samples=total_samples,
            evaluated_samples_count=len(completed_indices),
            failed_samples_count=batch_output.failed_samples_count,
            avg_faithfulness=batch_output.avg_faithfulness,
            avg_context_precision=batch_output.avg_context_precision,
            avg_answer_relevance=batch_output.avg_answer_relevance,
            passed_quality_gates=passed_quality_gates,
            duration_seconds=batch_output.duration_seconds,
            dlq_samples=batch_output.dlq_samples,
        )

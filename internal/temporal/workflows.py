"""Temporal workflow orchestrating batch golden-set evaluation and release quality gates."""

from datetime import timedelta

import structlog
from temporalio import workflow
from temporalio.common import RetryPolicy

from internal.temporal.models import (
    EvaluateGoldenSetInput,
    EvaluateGoldenSetResult,
    RunEvaluationWorkflowInput,
    RunEvaluationWorkflowOutput,
)

logger = structlog.get_logger(__name__)


@workflow.defn(name="BatchEvaluationWorkflow")
class BatchEvaluationWorkflow:
    """Orchestrates golden-set batch evaluation pipelines and enforces quality gates."""

    @workflow.run
    async def run(self, workflow_input: RunEvaluationWorkflowInput) -> RunEvaluationWorkflowOutput:
        """Execute batch evaluation workflow with deterministic activity invocation."""
        logger.info(
            "batch_evaluation_workflow_started",
            tenant_id=workflow_input.tenant_id,
            run_id=workflow_input.run_id,
            dataset_id=workflow_input.dataset_id,
            metrics=workflow_input.eval_metrics,
        )

        activity_input = EvaluateGoldenSetInput(
            tenant_id=workflow_input.tenant_id,
            run_id=workflow_input.run_id,
            dataset_id=workflow_input.dataset_id,
            golden_set_uri=workflow_input.golden_set_s3_uri,
            concurrency_limit=workflow_input.concurrency_limit,
            eval_metrics=workflow_input.eval_metrics,
        )

        result: EvaluateGoldenSetResult = await workflow.execute_activity(
            "EvaluateGoldenSetActivity",
            activity_input,
            start_to_close_timeout=timedelta(seconds=600),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=2),
                maximum_attempts=3,
                non_retryable_error_types=["ApplicationError"],
            ),
        )

        # Scoped quality gate evaluation based on active eval_metrics
        passed_quality_gates = True
        min_threshold = 0.82

        if "faithfulness" in workflow_input.eval_metrics:
            if result.avg_faithfulness < min_threshold:
                passed_quality_gates = False

        if "context_precision" in workflow_input.eval_metrics:
            if result.avg_context_precision < min_threshold:
                passed_quality_gates = False

        if "answer_relevance" in workflow_input.eval_metrics:
            if result.avg_answer_relevance < min_threshold:
                passed_quality_gates = False

        if result.failed_samples_count > 0:
            passed_quality_gates = False

        logger.info(
            "batch_evaluation_workflow_finished",
            tenant_id=workflow_input.tenant_id,
            run_id=workflow_input.run_id,
            passed_quality_gates=passed_quality_gates,
        )

        return RunEvaluationWorkflowOutput(
            tenant_id=result.tenant_id,
            run_id=result.run_id,
            total_samples=result.total_samples,
            evaluated_samples=result.evaluated_samples_count,
            failed_samples_count=result.failed_samples_count,
            avg_faithfulness=result.avg_faithfulness,
            avg_context_precision=result.avg_context_precision,
            avg_answer_relevance=result.avg_answer_relevance,
            duration_seconds=result.duration_seconds,
            passed_quality_gates=passed_quality_gates,
        )

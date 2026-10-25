"""Unit tests for BatchEvaluationWorkflow execution and scoped quality gates."""

from unittest.mock import AsyncMock

import pytest
from temporalio import workflow

from internal.temporal.models import (
    EvaluateGoldenSetResult,
    RunEvaluationWorkflowInput,
    RunEvaluationWorkflowOutput,
)
from internal.temporal.workflows import BatchEvaluationWorkflow


@pytest.mark.asyncio
async def test_workflow_successful_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify workflow executes activity and passes quality gates when scores >= 0.82."""
    mock_result = EvaluateGoldenSetResult(
        tenant_id="tenant-wf",
        run_id="run-wf",
        dataset_id="golden-set",
        total_samples=10,
        evaluated_samples_count=10,
        failed_samples_count=0,
        avg_faithfulness=0.92,
        avg_context_precision=0.88,
        avg_answer_relevance=0.90,
        passed_quality_gates=True,
        duration_seconds=5.0,
        dlq_samples=[],
    )

    mock_execute = AsyncMock(return_value=mock_result)
    monkeypatch.setattr(workflow, "execute_activity", mock_execute)

    wf = BatchEvaluationWorkflow()
    wf_input = RunEvaluationWorkflowInput(
        tenant_id="tenant-wf",
        run_id="run-wf",
        dataset_id="golden-set",
        golden_set_s3_uri="s3://eval/golden.json",
        eval_metrics=["context_precision", "faithfulness", "answer_relevance"],
    )

    out: RunEvaluationWorkflowOutput = await wf.run(wf_input)
    assert out.passed_quality_gates is True
    assert out.failed_samples_count == 0
    assert out.avg_faithfulness == 0.92
    mock_execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_workflow_scoped_metric_subset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify quality gate only checks active eval_metrics in the input."""
    # Context precision is 0.50 (failing), but eval_metrics only asks for faithfulness!
    mock_result = EvaluateGoldenSetResult(
        tenant_id="tenant-scoped",
        run_id="run-scoped",
        dataset_id="golden-set",
        total_samples=10,
        evaluated_samples_count=10,
        failed_samples_count=0,
        avg_faithfulness=0.95,
        avg_context_precision=0.50,
        avg_answer_relevance=0.50,
        passed_quality_gates=False,
        duration_seconds=3.0,
        dlq_samples=[],
    )

    mock_execute = AsyncMock(return_value=mock_result)
    monkeypatch.setattr(workflow, "execute_activity", mock_execute)

    wf = BatchEvaluationWorkflow()
    wf_input = RunEvaluationWorkflowInput(
        tenant_id="tenant-scoped",
        run_id="run-scoped",
        dataset_id="golden-set",
        golden_set_s3_uri="s3://eval/golden.json",
        eval_metrics=["faithfulness"],  # Only faithfulness active
    )

    out = await wf.run(wf_input)
    assert out.passed_quality_gates is True


@pytest.mark.asyncio
async def test_workflow_fails_gate_on_dlq_samples(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify workflow rejects quality gates if any sample failed into DLQ."""
    mock_result = EvaluateGoldenSetResult(
        tenant_id="tenant-fail",
        run_id="run-fail",
        dataset_id="golden-set",
        total_samples=10,
        evaluated_samples_count=9,
        failed_samples_count=1,
        avg_faithfulness=0.95,
        avg_context_precision=0.95,
        avg_answer_relevance=0.95,
        passed_quality_gates=False,
        duration_seconds=4.0,
        dlq_samples=[{"sample_id": "bad", "error_type": "ValueError"}],
    )

    mock_execute = AsyncMock(return_value=mock_result)
    monkeypatch.setattr(workflow, "execute_activity", mock_execute)

    wf = BatchEvaluationWorkflow()
    wf_input = RunEvaluationWorkflowInput(
        tenant_id="tenant-fail",
        run_id="run-fail",
        dataset_id="golden-set",
        golden_set_s3_uri="s3://eval/golden.json",
    )

    out = await wf.run(wf_input)
    assert out.passed_quality_gates is False
    assert out.failed_samples_count == 1

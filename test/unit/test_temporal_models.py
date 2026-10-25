"""Unit tests for Temporal workflow and activity data models."""

from internal.evaluation.models import GoldenSampleItem
from internal.temporal.models import (
    EvaluateGoldenSetInput,
    EvaluateGoldenSetResult,
    EvaluationHeartbeatDetails,
    RunEvaluationWorkflowInput,
    RunEvaluationWorkflowOutput,
)


def test_workflow_input_defaults_and_validation() -> None:
    """Verify RunEvaluationWorkflowInput default values and field validation."""
    inp = RunEvaluationWorkflowInput(
        tenant_id="tenant-123",
        run_id="run-456",
        dataset_id="golden-v1",
        golden_set_s3_uri="s3://eval-bucket/golden_set.json",
    )
    assert inp.tenant_id == "tenant-123"
    assert inp.concurrency_limit == 20
    assert inp.eval_metrics == ["context_precision", "faithfulness", "answer_relevance"]
    assert inp.judge_model == "meta-llama/Llama-3.1-70B-Instruct"


def test_workflow_output_serialization() -> None:
    """Verify RunEvaluationWorkflowOutput serialization and gate flags."""
    out = RunEvaluationWorkflowOutput(
        tenant_id="tenant-123",
        run_id="run-456",
        total_samples=10,
        evaluated_samples=10,
        failed_samples_count=0,
        avg_faithfulness=0.89,
        avg_context_precision=0.91,
        avg_answer_relevance=0.88,
        duration_seconds=12.45,
        passed_quality_gates=True,
    )
    assert out.passed_quality_gates is True
    data = out.model_dump()
    assert data["failed_samples_count"] == 0
    assert data["avg_faithfulness"] == 0.89


def test_evaluate_golden_set_input_variants() -> None:
    """Verify EvaluateGoldenSetInput with inline samples and URI."""
    sample = GoldenSampleItem(
        sample_id="s1",
        sample_idx=0,
        query="What is RAG?",
        retrieved_contexts=["Retrieval Augmented Generation combines retrieval with LLMs."],
        generated_answer="RAG combines search retrieval and generative models.",
    )
    # Inline samples variant
    inp1 = EvaluateGoldenSetInput(
        tenant_id="t1",
        run_id="r1",
        samples=[sample],
    )
    assert len(inp1.samples) == 1
    assert inp1.golden_set_uri is None

    # URI variant
    inp2 = EvaluateGoldenSetInput(
        tenant_id="t1",
        run_id="r1",
        golden_set_uri="file:///tmp/golden.json",
    )
    assert inp2.golden_set_uri == "file:///tmp/golden.json"
    assert len(inp2.samples) == 0


def test_heartbeat_details_out_of_order_indices() -> None:
    """Verify EvaluationHeartbeatDetails supports completed_sample_indices set operations."""
    details = EvaluationHeartbeatDetails(
        tenant_id="t1",
        run_id="r1",
        current_sample_idx=4,
        total_samples=10,
        current_sample_id="sample-004",
        completed_samples=3,
        completed_sample_indices=[0, 2, 4],
        last_checkpoint_timestamp_utc="2026-10-25T02:00:00Z",
    )
    data = details.model_dump()
    assert data["completed_sample_indices"] == [0, 2, 4]

    reconstructed = EvaluationHeartbeatDetails.model_validate(data)
    completed_set = set(reconstructed.completed_sample_indices)
    assert 0 in completed_set
    assert 1 not in completed_set
    assert 2 in completed_set
    assert 4 in completed_set


def test_evaluate_golden_set_result_dlq_preservation() -> None:
    """Verify EvaluateGoldenSetResult preserves dead-letter quarantined sample metadata."""
    dlq_entry = {
        "tenant_id": "t1",
        "run_id": "r1",
        "sample_idx": 3,
        "sample_id": "poison-pill",
        "error_type": "JSONDecodeError",
        "error_message": "Invalid control character",
    }
    result = EvaluateGoldenSetResult(
        tenant_id="t1",
        run_id="r1",
        dataset_id="golden-set",
        total_samples=5,
        evaluated_samples_count=4,
        failed_samples_count=1,
        avg_faithfulness=0.85,
        avg_context_precision=0.85,
        avg_answer_relevance=0.85,
        passed_quality_gates=False,
        duration_seconds=5.2,
        dlq_samples=[dlq_entry],
    )
    assert result.failed_samples_count == 1
    assert len(result.dlq_samples) == 1
    assert result.dlq_samples[0]["error_type"] == "JSONDecodeError"

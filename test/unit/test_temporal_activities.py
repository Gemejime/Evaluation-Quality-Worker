"""Unit tests for EvaluateGoldenSetActivity resumption, heartbeating, and DLQ isolation."""

from typing import Any
from unittest.mock import AsyncMock

import pytest
from temporalio import activity
from temporalio.exceptions import CancelledError

from internal.config.settings import Settings
from internal.evaluation.judges.base import BaseJudgeClient, JudgeResponse
from internal.evaluation.models import GoldenSampleItem
from internal.evaluation.ratelimiter import OutboundJudgeGovernor
from internal.evaluation.runner import BatchEvaluationRunner
from internal.temporal import activities
from internal.temporal.activities import EvaluateGoldenSetActivity
from internal.temporal.models import EvaluateGoldenSetInput


class MockSimpleJudge(BaseJudgeClient):
    """Simple mock judge client returning valid verdicts."""

    def __init__(self, should_fail_on_sample_id: str | None = None) -> None:
        self.should_fail_on_sample_id = should_fail_on_sample_id

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        if self.should_fail_on_sample_id and self.should_fail_on_sample_id in prompt:
            raise RuntimeError(f"Downstream judge crash on: {self.should_fail_on_sample_id}")

        sys_str = system_prompt or ""
        payload: dict[str, Any]
        if "retrieval quality" in sys_str:
            payload = {
                "verdicts": [
                    {"chunk_idx": 0, "verdict": 1, "reasoning": "Relevant"},
                ]
            }
        elif "faithfulness" in sys_str:
            payload = {
                "extracted_claims": [
                    {"claim": "Grounded claim", "supported": True, "citation_index": 0}
                ],
                "reasoning": "Faithful",
            }
        else:
            payload = {"score": 0.9, "reasoning": "Direct"}

        return JudgeResponse(
            content="{}",
            parsed_json=payload,
            prompt_tokens=20,
            completion_tokens=10,
            latency_ms=5,
        )

    async def aclose(self) -> None:
        pass


@pytest.fixture
def sample_triplets() -> list[GoldenSampleItem]:
    """Return 4 golden sample items for resumption testing."""
    return [
        GoldenSampleItem(
            sample_id=f"sample-{i}",
            sample_idx=i,
            query=f"Query {i}",
            retrieved_contexts=[f"Context passage for query {i}."],
            generated_answer=f"Answer for query {i}.",
        )
        for i in range(4)
    ]


@pytest.mark.asyncio
async def test_activity_out_of_order_resumption(
    sample_triplets: list[GoldenSampleItem],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that samples in completed_sample_indices are skipped on activity restart."""
    settings = Settings(
        ACTIVITY_HEARTBEAT_INTERVAL_SECONDS=0.05,
        JUDGE_MAX_CONCURRENT_REQUESTS=10,
    )
    judge = MockSimpleJudge()
    governor = OutboundJudgeGovernor(settings)
    runner = BatchEvaluationRunner(judge_client=judge, governor=governor, settings=settings)
    activity_instance = EvaluateGoldenSetActivity(runner=runner, settings=settings)

    # Simulate checkpoint with non-sequential completed samples [0, 2]
    heartbeat_calls: list[dict[str, Any]] = []
    checkpoint_state = {
        "tenant_id": "tenant-test",
        "run_id": "run-resumption",
        "current_sample_idx": 2,
        "total_samples": 4,
        "current_sample_id": "sample-2",
        "completed_samples": 2,
        "completed_sample_indices": [0, 2],
        "last_checkpoint_timestamp_utc": "2026-10-25T00:00:00Z",
        "metric_score_sums": {"context_precision": 2.0},
        "metric_score_counts": {"context_precision": 2},
    }

    def record_heartbeat(details: Any) -> None:
        heartbeat_calls.append(details)

    monkeypatch.setattr(activities, "get_heartbeat_details", lambda: checkpoint_state)
    monkeypatch.setattr(activity, "heartbeat", record_heartbeat)

    activity_input = EvaluateGoldenSetInput(
        tenant_id="tenant-test",
        run_id="run-resumption",
        samples=sample_triplets,
        eval_metrics=["context_precision"],
    )

    result = await activity_instance.run(activity_input)

    # Only sample-1 and sample-3 needed evaluation (2 remaining + 2 already completed = 4 total)
    assert result.total_samples == 4
    assert result.evaluated_samples_count == 4
    assert result.failed_samples_count == 0
    assert result.passed_quality_gates is True
    assert len(heartbeat_calls) > 0


@pytest.mark.asyncio
async def test_activity_dual_trigger_heartbeating(
    sample_triplets: list[GoldenSampleItem],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify periodic ticker and per-sample completion both trigger activity.heartbeat."""
    settings = Settings(
        ACTIVITY_HEARTBEAT_INTERVAL_SECONDS=0.02,
        JUDGE_MAX_CONCURRENT_REQUESTS=2,
    )
    judge = MockSimpleJudge()
    governor = OutboundJudgeGovernor(settings)
    runner = BatchEvaluationRunner(judge_client=judge, governor=governor, settings=settings)
    activity_instance = EvaluateGoldenSetActivity(runner=runner, settings=settings)

    heartbeats: list[dict[str, Any]] = []

    def record_hb_dual(details: Any) -> None:
        heartbeats.append(details)

    monkeypatch.setattr(activities, "get_heartbeat_details", lambda: None)
    monkeypatch.setattr(activity, "heartbeat", record_hb_dual)

    activity_input = EvaluateGoldenSetInput(
        tenant_id="tenant-hb",
        run_id="run-hb",
        samples=sample_triplets[:2],
        eval_metrics=["context_precision"],
    )

    result = await activity_instance.run(activity_input)
    assert result.evaluated_samples_count == 2
    assert len(heartbeats) >= 2


@pytest.mark.asyncio
async def test_activity_sample_dlq_fault_isolation(
    sample_triplets: list[GoldenSampleItem],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify individual poisoned sample error is quarantined to DLQ without crashing activity."""
    settings = Settings(
        ACTIVITY_HEARTBEAT_INTERVAL_SECONDS=0.1,
        JUDGE_MAX_CONCURRENT_REQUESTS=5,
    )
    # Fail on Query 1 (sample-1)
    judge = MockSimpleJudge(should_fail_on_sample_id="Query 1")
    governor = OutboundJudgeGovernor(settings)
    runner = BatchEvaluationRunner(judge_client=judge, governor=governor, settings=settings)
    activity_instance = EvaluateGoldenSetActivity(runner=runner, settings=settings)

    def noop_hb(*args: Any) -> None:
        pass

    monkeypatch.setattr(activities, "get_heartbeat_details", lambda: None)
    monkeypatch.setattr(activity, "heartbeat", noop_hb)

    activity_input = EvaluateGoldenSetInput(
        tenant_id="tenant-dlq",
        run_id="run-dlq",
        samples=sample_triplets,
        eval_metrics=["context_precision"],
    )

    result = await activity_instance.run(activity_input)
    assert result.total_samples == 4
    assert result.evaluated_samples_count == 3
    assert result.failed_samples_count == 1
    assert result.passed_quality_gates is False
    assert len(result.dlq_samples) == 1
    assert result.dlq_samples[0]["sample_id"] == "sample-1"


@pytest.mark.asyncio
async def test_activity_cancellation_flushes_sink(
    sample_triplets: list[GoldenSampleItem],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify activity cancellation invokes sink buffer flush and re-raises CancelledError."""
    settings = Settings()
    judge = MockSimpleJudge()
    governor = OutboundJudgeGovernor(settings)

    mock_sink = AsyncMock()
    mock_sink.flush = AsyncMock()

    runner = BatchEvaluationRunner(
        judge_client=judge,
        governor=governor,
        sink=mock_sink,
        settings=settings,
    )
    activity_instance = EvaluateGoldenSetActivity(runner=runner, settings=settings)

    # Simulate CancelledError inside run_batch
    async def mock_run_batch_cancelled(*args: Any, **kwargs: Any) -> Any:
        raise CancelledError("Activity cancelled by Temporal test")

    def noop_cancel(*args: Any) -> None:
        pass

    monkeypatch.setattr(runner, "run_batch", mock_run_batch_cancelled)
    monkeypatch.setattr(activities, "get_heartbeat_details", lambda: None)
    monkeypatch.setattr(activity, "heartbeat", noop_cancel)

    activity_input = EvaluateGoldenSetInput(
        tenant_id="tenant-cancel",
        run_id="run-cancel",
        samples=sample_triplets,
    )

    with pytest.raises(CancelledError):
        await activity_instance.run(activity_input)

    mock_sink.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_activity_all_samples_already_completed(
    sample_triplets: list[GoldenSampleItem],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify activity returns immediately when all samples were completed previously."""
    settings = Settings()
    judge = MockSimpleJudge()
    governor = OutboundJudgeGovernor(settings)
    runner = BatchEvaluationRunner(judge_client=judge, governor=governor, settings=settings)
    activity_instance = EvaluateGoldenSetActivity(runner=runner, settings=settings)

    checkpoint_state = {
        "tenant_id": "tenant-done",
        "run_id": "run-done",
        "current_sample_idx": 3,
        "total_samples": 4,
        "current_sample_id": "sample-3",
        "completed_samples": 4,
        "completed_sample_indices": [0, 1, 2, 3],
        "last_checkpoint_timestamp_utc": "2026-10-25T00:00:00Z",
        "metric_score_sums": {
            "faithfulness": 3.6,
            "context_precision": 3.6,
            "answer_relevance": 3.6,
        },
        "metric_score_counts": {
            "faithfulness": 4,
            "context_precision": 4,
            "answer_relevance": 4,
        },
    }

    monkeypatch.setattr(activities, "get_heartbeat_details", lambda: checkpoint_state)

    activity_input = EvaluateGoldenSetInput(
        tenant_id="tenant-done",
        run_id="run-done",
        samples=sample_triplets,
    )

    result = await activity_instance.run(activity_input)
    assert result.total_samples == 4
    assert result.evaluated_samples_count == 4
    assert result.failed_samples_count == 0
    assert result.avg_faithfulness == 0.9
    assert result.avg_context_precision == 0.9
    assert result.avg_answer_relevance == 0.9
    assert result.passed_quality_gates is True


@pytest.mark.asyncio
async def test_activity_unrecoverable_scores_recomputes_run(
    sample_triplets: list[GoldenSampleItem],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify activity recomputes all samples if checkpoint scores cannot be recovered."""
    settings = Settings(
        ACTIVITY_HEARTBEAT_INTERVAL_SECONDS=0.1,
        JUDGE_MAX_CONCURRENT_REQUESTS=5,
    )
    judge = MockSimpleJudge()
    governor = OutboundJudgeGovernor(settings)
    runner = BatchEvaluationRunner(judge_client=judge, governor=governor, settings=settings)
    activity_instance = EvaluateGoldenSetActivity(runner=runner, settings=settings)

    # Checkpoint with completed indices but NO metric_score_counts
    checkpoint_state = {
        "tenant_id": "tenant-recompute",
        "run_id": "run-recompute",
        "current_sample_idx": 3,
        "total_samples": 4,
        "current_sample_id": "sample-3",
        "completed_samples": 4,
        "completed_sample_indices": [0, 1, 2, 3],
        "last_checkpoint_timestamp_utc": "2026-10-25T00:00:00Z",
        "metric_score_sums": dict[str, float](),
        "metric_score_counts": dict[str, int](),
    }

    def noop_hb(*args: Any) -> None:
        pass

    monkeypatch.setattr(activities, "get_heartbeat_details", lambda: checkpoint_state)
    monkeypatch.setattr(activity, "heartbeat", noop_hb)

    activity_input = EvaluateGoldenSetInput(
        tenant_id="tenant-recompute",
        run_id="run-recompute",
        samples=sample_triplets,
        eval_metrics=["context_precision"],
    )

    result = await activity_instance.run(activity_input)
    # Because scores were unrecoverable, all 4 were recomputed
    assert result.total_samples == 4
    assert result.evaluated_samples_count == 4
    assert result.failed_samples_count == 0
    assert result.avg_context_precision > 0.0


@pytest.mark.asyncio
async def test_activity_load_samples_from_s3_object_store() -> None:
    """Verify _load_samples_from_uri loads s3:// URIs using the provided object_store_reader."""
    settings = Settings()
    judge = MockSimpleJudge()
    governor = OutboundJudgeGovernor(settings)
    runner = BatchEvaluationRunner(judge_client=judge, governor=governor, settings=settings)

    sample_payload = [
        {
            "sample_id": "s3-sample-1",
            "sample_idx": 0,
            "query": "What is RAG?",
            "retrieved_contexts": ["RAG is retrieval augmented generation."],
            "generated_answer": "Retrieval Augmented Generation.",
        }
    ]

    import json

    class MockS3Reader:
        async def read_uri(self, uri: str) -> str:
            assert uri == "s3://eval-bucket/golden_set.json"
            return json.dumps(sample_payload)

    activity_instance = EvaluateGoldenSetActivity(
        runner=runner,
        settings=settings,
        object_store_reader=MockS3Reader(),
    )

    loaded = await activity_instance._load_samples_from_uri("s3://eval-bucket/golden_set.json")
    assert len(loaded) == 1
    assert loaded[0].sample_id == "s3-sample-1"


@pytest.mark.asyncio
async def test_activity_load_samples_forbidden_directory() -> None:
    """Verify _load_samples_from_uri raises PermissionError for forbidden base directories."""
    settings = Settings()
    judge = MockSimpleJudge()
    governor = OutboundJudgeGovernor(settings)
    runner = BatchEvaluationRunner(judge_client=judge, governor=governor, settings=settings)

    from pathlib import Path

    allowed = [Path("/isolated/secure/dir")]
    activity_instance = EvaluateGoldenSetActivity(
        runner=runner,
        settings=settings,
        allowed_base_dirs=allowed,
    )

    with pytest.raises(PermissionError):
        await activity_instance._load_samples_from_uri("file:///etc/shadow")


@pytest.mark.asyncio
async def test_activity_on_sample_complete_flushes_sink(
    sample_triplets: list[GoldenSampleItem],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify runner.sink is flushed upon each completed sample before heartbeat."""
    settings = Settings(
        ACTIVITY_HEARTBEAT_INTERVAL_SECONDS=0.1,
        JUDGE_MAX_CONCURRENT_REQUESTS=5,
    )
    judge = MockSimpleJudge()
    governor = OutboundJudgeGovernor(settings)
    mock_sink = AsyncMock()
    runner = BatchEvaluationRunner(
        judge_client=judge,
        governor=governor,
        sink=mock_sink,
        settings=settings,
    )
    activity_instance = EvaluateGoldenSetActivity(runner=runner, settings=settings)

    heartbeats: list[dict[str, Any]] = []
    call_order: list[str] = []

    async def mock_flush() -> None:
        call_order.append("flush")

    mock_sink.flush.side_effect = mock_flush

    def record_hb(details: Any) -> None:
        call_order.append("heartbeat")
        heartbeats.append(details)

    monkeypatch.setattr(activities, "get_heartbeat_details", lambda: None)
    monkeypatch.setattr(activity, "heartbeat", record_hb)

    activity_input = EvaluateGoldenSetInput(
        tenant_id="tenant-flush",
        run_id="run-flush",
        samples=sample_triplets[:2],
        eval_metrics=["context_precision"],
    )

    result = await activity_instance.run(activity_input)
    assert result.evaluated_samples_count == 2
    # Flush must occur before heartbeat for each sample
    assert call_order[0] == "flush"
    assert call_order[1] == "heartbeat"
    assert call_order[2] == "flush"
    assert call_order[3] == "heartbeat"
    assert mock_sink.flush.await_count >= 3
    assert mock_sink.insert_run.await_count >= 1

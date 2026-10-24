"""Unit tests for BatchEvaluationRunner, DLQ fault isolation, and secured logging."""

from typing import Any
from unittest.mock import MagicMock

import pytest

from internal.config.settings import Settings
from internal.evaluation.judges.base import BaseJudgeClient, JudgeResponse
from internal.evaluation.metrics.base import MetricResult
from internal.evaluation.models import (
    BatchEvaluationInput,
    GoldenSampleItem,
)
from internal.evaluation.ratelimiter import OutboundJudgeGovernor
from internal.evaluation.runner import BatchEvaluationRunner
from internal.storage.clickhouse_sink import ClickHouseSink


class MockJudgeForRunner(BaseJudgeClient):
    """Mock judge returning valid metric responses or raising errors on specific query."""

    def __init__(self, fail_query: str | None = None) -> None:
        self.fail_query = fail_query

    async def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        response_schema: type[Any] | None = None,
    ) -> JudgeResponse:
        if self.fail_query and self.fail_query in prompt:
            raise RuntimeError("Poison pill sample triggered unrecoverable judge error")

        # Determine which metric is being evaluated based on system prompt / prompt
        parsed: dict[str, Any]
        if "retrieval quality" in (system_prompt or ""):
            parsed = {
                "verdicts": [
                    {"chunk_idx": 0, "verdict": 1, "reasoning": "c0 is relevant"},
                    {"chunk_idx": 1, "verdict": 1, "reasoning": "c1 is relevant"},
                ]
            }
        elif "faithfulness" in (system_prompt or ""):
            parsed = {
                "extracted_claims": [
                    {"claim": "Fact A", "supported": True, "citation_index": 0},
                    {"claim": "Fact B", "supported": True, "citation_index": 1},
                ],
                "reasoning": "All claims supported",
            }
        else:
            parsed = {
                "score": 0.90,
                "reasoning": "Direct and relevant answer",
            }

        return JudgeResponse(
            content=str(parsed),
            parsed_json=parsed,
            prompt_tokens=100,
            completion_tokens=40,
            latency_ms=15,
        )

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_runner_successful_batch_evaluation() -> None:
    settings = Settings(
        JUDGE_RATE_LIMIT_RPS=100.0,
        JUDGE_MAX_CONCURRENT_REQUESTS=10,
        QUALITY_GATE_MIN_FAITHFULNESS=0.82,
        QUALITY_GATE_MIN_CONTEXT_PRECISION=0.82,
        QUALITY_GATE_MIN_ANSWER_RELEVANCE=0.82,
    )
    judge = MockJudgeForRunner()
    governor = OutboundJudgeGovernor(settings=settings)

    mock_client = MagicMock()
    sink = ClickHouseSink(settings=settings, client=mock_client, start_flusher=False)

    runner = BatchEvaluationRunner(
        judge_client=judge,
        governor=governor,
        sink=sink,
        settings=settings,
    )

    samples = [
        GoldenSampleItem(
            sample_id=f"sample-{i}",
            sample_idx=i,
            query=f"Query {i}",
            retrieved_contexts=["Context 0", "Context 1"],
            generated_answer=f"Answer {i}",
        )
        for i in range(3)
    ]

    completed_hooks: list[int] = []

    async def on_complete_hook(idx: int, s: GoldenSampleItem, results: list[MetricResult]) -> None:
        completed_hooks.append(idx)

    batch_input = BatchEvaluationInput(
        tenant_id="tenant-alpha",
        run_id="run-001",
        dataset_id="golden-v1",
        samples=samples,
    )

    output = await runner.run_batch(batch_input, on_sample_complete=on_complete_hook)

    assert output.total_samples == 3
    assert output.evaluated_samples == 3
    assert output.failed_samples_count == 0
    assert len(completed_hooks) == 3
    assert output.avg_faithfulness >= 0.82
    assert output.avg_context_precision >= 0.82
    assert output.avg_answer_relevance >= 0.82
    assert output.passed_quality_gates is True
    assert len(output.dlq_samples) == 0

    # Ensure sink flushed
    assert mock_client.insert.called
    await sink.close()


@pytest.mark.asyncio
async def test_runner_sample_dlq_fault_isolation() -> None:
    settings = Settings(
        JUDGE_RATE_LIMIT_RPS=100.0,
        JUDGE_MAX_CONCURRENT_REQUESTS=10,
    )
    # Configure mock judge to fail on "POISON"
    judge = MockJudgeForRunner(fail_query="POISON")
    governor = OutboundJudgeGovernor(settings=settings)
    runner = BatchEvaluationRunner(
        judge_client=judge,
        governor=governor,
        sink=None,
        settings=settings,
    )

    samples = [
        GoldenSampleItem(
            sample_id="sample-good-1",
            sample_idx=0,
            query="Valid query 1",
            retrieved_contexts=["c0"],
            generated_answer="Answer 1",
        ),
        GoldenSampleItem(
            sample_id="sample-poison",
            sample_idx=1,
            query="POISON prompt error",
            retrieved_contexts=["c0"],
            generated_answer="Answer bad",
        ),
        GoldenSampleItem(
            sample_id="sample-good-2",
            sample_idx=2,
            query="Valid query 2",
            retrieved_contexts=["c0"],
            generated_answer="Answer 2",
        ),
    ]

    batch_input = BatchEvaluationInput(
        tenant_id="tenant-beta",
        run_id="run-002",
        samples=samples,
    )

    output = await runner.run_batch(batch_input)

    # Poison pill sample was quarantined to DLQ without crashing the entire batch
    assert output.total_samples == 3
    assert output.evaluated_samples == 2
    assert output.failed_samples_count == 1
    assert output.passed_quality_gates is False
    assert len(output.dlq_samples) == 1
    assert output.dlq_samples[0]["sample_id"] == "sample-poison"
    assert output.dlq_samples[0]["error_type"] == "RuntimeError"


@pytest.mark.asyncio
async def test_runner_secured_logging_audit(caplog: pytest.LogCaptureFixture) -> None:
    settings = Settings(
        JUDGE_RATE_LIMIT_RPS=100.0,
        JUDGE_MAX_CONCURRENT_REQUESTS=5,
    )
    judge = MockJudgeForRunner()
    governor = OutboundJudgeGovernor(settings=settings)
    runner = BatchEvaluationRunner(
        judge_client=judge,
        governor=governor,
        sink=None,
        settings=settings,
    )

    secret_query = "SECRET_PATIENT_SSN_12345"
    secret_context = "SECRET_MEDICAL_DIAGNOSIS_DATA"
    secret_answer = "SECRET_PRESCRIPTION_DETAILS"

    sample = GoldenSampleItem(
        sample_id="sec-sample",
        sample_idx=0,
        query=secret_query,
        retrieved_contexts=[secret_context],
        generated_answer=secret_answer,
    )

    batch_input = BatchEvaluationInput(
        tenant_id="tenant-secure",
        run_id="run-sec",
        samples=[sample],
    )

    with caplog.at_level("INFO"):
        await runner.run_batch(batch_input)

    captured_text = caplog.text

    # Verify that raw secret queries, contexts, and answers NEVER leaked to logs
    assert secret_query not in captured_text
    assert secret_context not in captured_text
    assert secret_answer not in captured_text

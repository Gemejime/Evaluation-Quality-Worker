"""Temporal workflow and activity data models."""

from typing import Any

from pydantic import BaseModel, Field

from internal.evaluation.models import EvaluationHeartbeatDetails, GoldenSampleItem


class RunEvaluationWorkflowInput(BaseModel):
    """Input parameters initiating the BatchEvaluationWorkflow."""

    tenant_id: str
    run_id: str
    dataset_id: str
    golden_set_s3_uri: str
    concurrency_limit: int = 20
    eval_metrics: list[str] = Field(
        default_factory=lambda: ["context_precision", "faithfulness", "answer_relevance"]
    )
    judge_model: str = "meta-llama/Llama-3.1-70B-Instruct"


class RunEvaluationWorkflowOutput(BaseModel):
    """Aggregate output returned by the BatchEvaluationWorkflow."""

    tenant_id: str
    run_id: str
    total_samples: int
    evaluated_samples: int
    failed_samples_count: int
    avg_faithfulness: float
    avg_context_precision: float
    avg_answer_relevance: float
    duration_seconds: float
    passed_quality_gates: bool


class EvaluateGoldenSetInput(BaseModel):
    """Input payload for EvaluateGoldenSetActivity."""

    tenant_id: str
    run_id: str
    dataset_id: str = "golden-set"
    samples: list[GoldenSampleItem] = Field(default_factory=list)
    golden_set_uri: str | None = None
    concurrency_limit: int = 20
    eval_metrics: list[str] = Field(
        default_factory=lambda: ["context_precision", "faithfulness", "answer_relevance"]
    )
    pipeline_version: str = "v1.0.0"


class EvaluateGoldenSetResult(BaseModel):
    """Result summary produced by EvaluateGoldenSetActivity."""

    tenant_id: str
    run_id: str
    dataset_id: str
    total_samples: int
    evaluated_samples_count: int
    failed_samples_count: int
    avg_faithfulness: float
    avg_context_precision: float
    avg_answer_relevance: float
    passed_quality_gates: bool
    duration_seconds: float
    dlq_samples: list[dict[str, Any]] = Field(default_factory=list)


__all__ = [
    "EvaluateGoldenSetInput",
    "EvaluateGoldenSetResult",
    "EvaluationHeartbeatDetails",
    "GoldenSampleItem",
    "RunEvaluationWorkflowInput",
    "RunEvaluationWorkflowOutput",
]

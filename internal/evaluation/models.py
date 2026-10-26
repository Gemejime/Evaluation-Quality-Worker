"""Data models for evaluation batch execution and checkpoints."""

from typing import Any

from pydantic import BaseModel, Field


class GoldenSampleItem(BaseModel):
    """Single golden-set evaluation item representing a query-context-answer triplet."""

    sample_id: str
    sample_idx: int
    query: str
    retrieved_contexts: list[str]
    generated_answer: str
    ground_truth: str | None = None


class EvaluationHeartbeatDetails(BaseModel):
    """Heartbeat details payload for Temporal activity state resumption."""

    tenant_id: str
    run_id: str
    current_sample_idx: int
    total_samples: int
    current_sample_id: str
    completed_samples: int
    completed_sample_indices: list[int] = Field(default_factory=list)
    last_checkpoint_timestamp_utc: str
    metric_score_sums: dict[str, float] = Field(default_factory=dict)
    metric_score_counts: dict[str, int] = Field(default_factory=dict)


class BatchEvaluationInput(BaseModel):
    """Input payload orchestrating batch golden-set evaluation."""

    tenant_id: str
    run_id: str
    dataset_id: str = "golden-set"
    samples: list[GoldenSampleItem]
    eval_metrics: list[str] = Field(
        default_factory=lambda: ["context_precision", "faithfulness", "answer_relevance"]
    )
    concurrency_limit: int = Field(default=20, gt=0)
    pipeline_version: str = "v1.0.0"


class BatchEvaluationOutput(BaseModel):
    """Aggregate result payload returned upon batch evaluation completion."""

    tenant_id: str
    run_id: str
    dataset_id: str
    total_samples: int
    evaluated_samples: int
    failed_samples_count: int
    avg_faithfulness: float
    avg_context_precision: float
    avg_answer_relevance: float
    passed_quality_gates: bool
    duration_seconds: float
    dlq_samples: list[dict[str, Any]] = Field(default_factory=list)

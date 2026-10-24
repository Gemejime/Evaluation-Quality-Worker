"""Base classes and utility functions for RAG metric evaluators."""

import hashlib
from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from internal.evaluation.models import GoldenSampleItem


def compute_reasoning_hash(reasoning: str | None) -> str:
    """Compute a guaranteed 64-character hexadecimal SHA-256 hash for forensic storage."""
    payload = (reasoning or "").strip().encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class MetricResult(BaseModel):
    """Result of evaluating a single sample against a specific metric."""

    metric_name: str
    score: float = Field(ge=0.0, le=1.0)
    reasoning: str
    reasoning_hash: str = Field(min_length=64, max_length=64)
    execution_latency_ms: int = 0


class BaseMetricEvaluator(ABC):
    """Abstract evaluator interface for RAG quality metrics."""

    @property
    @abstractmethod
    def metric_name(self) -> str:
        """Name identifier of the metric."""
        raise NotImplementedError

    @abstractmethod
    async def evaluate(self, sample: GoldenSampleItem) -> MetricResult:
        """Evaluate a single golden sample item and return a validated MetricResult."""
        raise NotImplementedError

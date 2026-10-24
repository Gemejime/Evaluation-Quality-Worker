from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EvaluateBatchRequest(_message.Message):
    __slots__ = ("tenant_id", "run_id", "dataset_id", "samples", "metrics", "concurrency_limit")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    DATASET_ID_FIELD_NUMBER: _ClassVar[int]
    SAMPLES_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    CONCURRENCY_LIMIT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    run_id: str
    dataset_id: str
    samples: _containers.RepeatedCompositeFieldContainer[EvaluationSample]
    metrics: _containers.RepeatedScalarFieldContainer[str]
    concurrency_limit: int
    def __init__(self, tenant_id: _Optional[str] = ..., run_id: _Optional[str] = ..., dataset_id: _Optional[str] = ..., samples: _Optional[_Iterable[_Union[EvaluationSample, _Mapping]]] = ..., metrics: _Optional[_Iterable[str]] = ..., concurrency_limit: _Optional[int] = ...) -> None: ...

class EvaluationSample(_message.Message):
    __slots__ = ("sample_id", "sample_idx", "query", "contexts", "generated_answer", "ground_truth")
    SAMPLE_ID_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_IDX_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    CONTEXTS_FIELD_NUMBER: _ClassVar[int]
    GENERATED_ANSWER_FIELD_NUMBER: _ClassVar[int]
    GROUND_TRUTH_FIELD_NUMBER: _ClassVar[int]
    sample_id: str
    sample_idx: int
    query: str
    contexts: _containers.RepeatedScalarFieldContainer[str]
    generated_answer: str
    ground_truth: str
    def __init__(self, sample_id: _Optional[str] = ..., sample_idx: _Optional[int] = ..., query: _Optional[str] = ..., contexts: _Optional[_Iterable[str]] = ..., generated_answer: _Optional[str] = ..., ground_truth: _Optional[str] = ...) -> None: ...

class EvaluateBatchResponse(_message.Message):
    __slots__ = ("tenant_id", "run_id", "total_samples", "completed_samples", "avg_faithfulness", "avg_context_precision", "avg_answer_relevance", "passed_release_gate", "duration_seconds")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SAMPLES_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_SAMPLES_FIELD_NUMBER: _ClassVar[int]
    AVG_FAITHFULNESS_FIELD_NUMBER: _ClassVar[int]
    AVG_CONTEXT_PRECISION_FIELD_NUMBER: _ClassVar[int]
    AVG_ANSWER_RELEVANCE_FIELD_NUMBER: _ClassVar[int]
    PASSED_RELEASE_GATE_FIELD_NUMBER: _ClassVar[int]
    DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    run_id: str
    total_samples: int
    completed_samples: int
    avg_faithfulness: float
    avg_context_precision: float
    avg_answer_relevance: float
    passed_release_gate: bool
    duration_seconds: float
    def __init__(self, tenant_id: _Optional[str] = ..., run_id: _Optional[str] = ..., total_samples: _Optional[int] = ..., completed_samples: _Optional[int] = ..., avg_faithfulness: _Optional[float] = ..., avg_context_precision: _Optional[float] = ..., avg_answer_relevance: _Optional[float] = ..., passed_release_gate: _Optional[bool] = ..., duration_seconds: _Optional[float] = ...) -> None: ...

class GetRunStatusRequest(_message.Message):
    __slots__ = ("tenant_id", "run_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    run_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., run_id: _Optional[str] = ...) -> None: ...

class GetRunStatusResponse(_message.Message):
    __slots__ = ("status", "progress_percentage", "current_sample_idx")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_PERCENTAGE_FIELD_NUMBER: _ClassVar[int]
    CURRENT_SAMPLE_IDX_FIELD_NUMBER: _ClassVar[int]
    status: str
    progress_percentage: int
    current_sample_idx: int
    def __init__(self, status: _Optional[str] = ..., progress_percentage: _Optional[int] = ..., current_sample_idx: _Optional[int] = ...) -> None: ...

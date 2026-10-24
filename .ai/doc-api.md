# Microservice Documented API ("Doc API")

> **MANDATORY INVARIANT**: This document specifies both the internal system contracts and the **HTTP Doc API & OpenAPI Endpoint** exposed by the microservice.
> **API ACCESSIBILITY REQUIREMENT**: This documentation specification MUST be accessible directly via HTTP endpoints hosted on the worker's internal server (port `8080`):
>
> - `GET /docs` — Interactive OpenAPI 3.1 Documentation via **Swagger UI**.
> - `GET /redoc` — Interactive OpenAPI 3.1 Documentation via **ReDoc**.
> - `GET /openapi.json` & `GET /api/docs` — Machine-readable **OpenAPI 3.1 JSON Specification**.
>
> **CONTINUOUS DOC API UPDATE INVARIANT**: This file and the corresponding OpenAPI specification schemas MUST be updated on **every code change or addition**, regardless of how small the modification is.

---

## 1. Documentation & Probe API Endpoints (OpenAPI 3.1)

The worker process (`rag-eval-worker`) runs an internal FastAPI ASGI server (default port `8080`) serving operational probes and OpenAPI contracts:

| Endpoint | Method | Response Format | Description |
| :--- | :--- | :--- | :--- |
| `/docs` | `GET` | `text/html` | Interactive Swagger UI powered by OpenAPI 3.1 |
| `/redoc` | `GET` | `text/html` | Interactive ReDoc UI documentation |
| `/api/docs` | `GET` | `application/json` | Raw OpenAPI 3.1 JSON specification schema |
| `/openapi.json` | `GET` | `application/json` | Standard machine-readable OpenAPI 3.1 JSON specification |
| `/healthz` | `GET` | `application/json` | Liveness probe returning `{"status": "healthy"}` |
| `/ready` | `GET` | `application/json` | Readiness probe verifying ClickHouse, Judge endpoints, and Temporal queue |
| `/metrics` | `GET` | `text/plain` | Prometheus metrics scrape endpoint |

### Prometheus Observability & Metrics Contract

The `/metrics` endpoint exposes runtime telemetry scraped by Prometheus collectors:

| Metric Identifier | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `eval_batch_duration_seconds` | Histogram | `status` | Total duration of batch evaluation pipeline runs (high-cardinality `tenant_id`/`dataset_id` retained in logs/exemplars) |
| `eval_sample_execution_latency_seconds` | Histogram | `metric_name`, `judge_model` | Latency per single sample judge evaluation (tenant context tracked via exemplars/logs) |
| `eval_judge_active_concurrency` | Gauge | `judge_model` | In-flight concurrent judge calls (capped by semaphore) |
| `eval_judge_rate_limit_errors_total` | Counter | `judge_model`, `error_type` (429, exhausted) | Downstream judge rate-limit and backpressure occurrences |
| `eval_metric_score_value` | Gauge | `metric_name` | Aggregate score value per evaluation run (`tenant_id`/`dataset_id` tracked in logs/exemplars) |

#### Operational Alert Rules
- **Judge Quota Saturation (Severity: Warning)**: `rate(eval_judge_rate_limit_errors_total[5m]) > 0` (Triggers dynamic concurrency cap reduction).
- **Activity Heartbeat Stalling (Severity: Page)**: Activity heartbeat latency exceeds 25 seconds (indicates blocked event loop or socket freeze).
- **Evaluation Quality Drop (Severity: Warning)**: Aggregate faithfulness or context precision falls below 0.82 (violating the $\tau \ge 0.82$ platform release gate).

```json
{
  "openapi": "3.1.0",
  "info": {
    "title": "RAG Evaluation and Governance Worker API",
    "description": "Internal HTTP Documentation, Health, and Operational API for rag-eval-governance-worker",
    "version": "0.1.0"
  },
  "paths": {
    "/healthz": {
      "get": {
        "summary": "Liveness Probe",
        "operationId": "get_liveness",
        "responses": {
          "200": {
            "description": "Worker event loop is healthy",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "status": { "type": "string", "example": "healthy" }
                  },
                  "required": ["status"]
                }
              }
            }
          }
        }
      }
    },
    "/ready": {
      "get": {
        "summary": "Readiness Probe",
        "operationId": "get_readiness",
        "responses": {
          "200": {
            "description": "Worker connected to Temporal, ClickHouse reachable, Judge endpoint ready",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "status": { "type": "string", "example": "ready" },
                    "clickhouse": { "type": "string", "example": "connected" },
                    "temporal": { "type": "string", "example": "polling" },
                    "judge_endpoint": { "type": "string", "example": "ready" }
                  },
                  "required": ["status", "clickhouse", "temporal", "judge_endpoint"]
                }
              }
            }
          },
          "503": {
            "description": "Worker initializing or upstream dependency unreachable"
          }
        }
      }
    },
    "/api/docs": {
      "get": {
        "summary": "OpenAPI Documentation Schema",
        "operationId": "get_openapi_schema",
        "responses": {
          "200": { "description": "OpenAPI 3.1 JSON specification" }
        }
      }
    }
  }
}
```

---

## 2. Temporal Workflows & Activity Interfaces

### Task Queue: `eval-task-queue`

#### Workflow: `BatchEvaluationWorkflow`
- **Module**: `internal.temporal.workflows`
- **Workflow ID Pattern**: `eval-run-{tenant_id}-{run_id}`
- **Execution Target**: Orchestrates batch golden-set evaluation, sample execution checkpoints, rate-limited judge calls, and ClickHouse micro-batch ingestion.

```python
class RunEvaluationWorkflowInput(BaseModel):
    tenant_id: str
    run_id: str
    dataset_id: str
    golden_set_s3_uri: str
    concurrency_limit: int = 20
    eval_metrics: list[str] = ["context_precision", "faithfulness", "answer_relevance"]
    judge_model: str = "meta-llama/Llama-3.1-70B-Instruct"


class RunEvaluationWorkflowOutput(BaseModel):
    tenant_id: str
    run_id: str
    total_samples: int
    evaluated_samples: int
    avg_faithfulness: float
    avg_context_precision: float
    avg_answer_relevance: float
    duration_seconds: float
    passed_quality_gates: bool
```

#### Activity: `EvaluateGoldenSetActivity`
- **Module**: `internal.temporal.activities`
- **Queue**: `eval-task-queue`
- **Heartbeat Timeout**: `30s` (Emits heartbeat every `10s` per evaluated sample)
- **Zero-Recompute Checkpointing**: Checks `activity.get_heartbeat_details()` on startup. Resumes execution at the last acknowledged checkpoint. If a crash occurs before heartbeat emission, sample $N$ may re-execute (at-least-once), while ClickHouse deduplication ensures zero duplicate persisted rows.
- **Fault Isolation & Dead Letter Queue (DLQ)**:
  - Poison-pill samples (schema parsing failures, unrecoverable judge timeouts after 5 retries, corrupted payloads) are captured into an evaluation dead-letter queue (DLQ) and increment `failed_samples_count`.
  - An individual sample failure NEVER fails the overall activity; processing continues seamlessly across remaining golden samples.
  - Only catastrophic infrastructure failures (ClickHouse connection drop, Temporal activity cancellation) raise non-retryable `ApplicationError`.

```python
class GoldenSampleItem(BaseModel):
    sample_id: str
    sample_idx: int  # Preserved from gRPC EvaluationSample to maintain stable deduplication key
    query: str
    retrieved_contexts: list[str]
    generated_answer: str
    ground_truth: str | None = None


class EvaluateGoldenSetInput(BaseModel):
    tenant_id: str
    run_id: str
    samples: list[GoldenSampleItem]
    concurrency_limit: int = 20


class EvaluationHeartbeatDetails(BaseModel):
    tenant_id: str
    run_id: str
    current_sample_idx: int
    total_samples: int
    current_sample_id: str
    completed_samples: int
    last_checkpoint_timestamp_utc: str


class EvaluateGoldenSetResult(BaseModel):
    tenant_id: str
    run_id: str
    evaluated_samples_count: int
    failed_samples_count: int
    duration_seconds: float
```

---

## 3. Native LLM-as-a-Judge Metrics & Prompt Contracts

(ADR-002: Native Prompts over Ragas/TruLens Dependencies)

### A. Context Precision (`context_precision.py`)
- **Objective**: Evaluates whether retrieved context chunks relevant to the query are ranked higher than irrelevant chunks.
- **Judge Response Schema**:
  ```json
  {
    "verdict": 1,
    "reasoning": "Context chunk 1 contains exact factual answer to user question."
  }
  ```
- **Formula**:
  $$\text{Context Precision@K} = \frac{\sum_{k=1}^K (\text{Precision@}k \times v_k)}{\text{Total Relevant Chunks in Top } K}$$
  where $v_k \in \{0, 1\}$ is the judge binary relevance verdict.
  *Zero Relevant Chunks Contract*: When no retrieved chunks in the top-$K$ are relevant ($\text{Total Relevant Chunks in Top } K = 0$), $\text{Context Precision@K}$ is defined as `0.0`, failing the $\tau \ge 0.82$ quality gate.

### B. Faithfulness / Groundedness (`faithfulness.py`)
- **Objective**: Verifies that all factual claims made in the generated answer are strictly grounded in the retrieved contexts without hallucination.
- **Judge Response Schema**:
  ```json
  {
    "extracted_claims": [
      {
        "claim": "The server uses jemalloc preloading.",
        "supported": true,
        "citation_index": 0
      }
    ],
    "verdict": 1.0,
    "reasoning": "All 3 claims are directly supported by retrieved context snippets."
  }
  ```
- **Formula**:
  $$\text{Faithfulness} = \frac{|\text{Supported Claims}|}{|\text{Total Extracted Claims}|}$$
  *Zero Extracted Claims Contract*: When Total Extracted Claims is zero ($|\text{Total Extracted Claims}| = 0$, e.g., the answer contains no factual assertions), Faithfulness is defined as `1.0` (absence of unsupported claims); for nonzero claim counts, the standard ratio applies.

### C. Answer Relevance (`answer_relevance.py`)
- **Objective**: Measures whether the generated answer directly addresses the original query without superfluous or divergent information.
- **Judge Response Schema**:
  ```json
  {
    "score": 0.95,
    "reasoning": "Answer directly answers query with high conciseness and zero tangential rambling."
  }
  ```

---

## 4. ClickHouse Data Persistence Contracts (ADR-003)

Unified ClickHouse MergeTree tables partitioned by month (`toYYYYMM(run_date)`):

### Table: `evaluation_runs`
```sql
CREATE TABLE IF NOT EXISTS evaluation_runs (
    tenant_id String,
    run_id String,
    run_date Date,
    pipeline_version LowCardinality(String),
    dataset_id String,
    sample_count UInt32,
    avg_faithfulness Float32,
    avg_context_precision Float32,
    avg_answer_relevance Float32,
    duration_seconds Float32,
    status LowCardinality(String),
    attempt UInt32 DEFAULT 1,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(attempt)
PARTITION BY toYYYYMM(run_date)
ORDER BY (tenant_id, run_id);
```

### Table: `eval_metric_points`
```sql
CREATE TABLE IF NOT EXISTS eval_metric_points (
    tenant_id String,
    run_id String,
    run_date Date,
    sample_idx UInt32,
    sample_id String,
    metric_name LowCardinality(String),
    score Float32,
    reasoning_hash FixedString(64),
    execution_latency_ms UInt32,
    judge_model LowCardinality(String),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(run_date)
ORDER BY (tenant_id, run_id, metric_name, sample_idx);
```

> **Idempotency & Read Deduplication Contract**:
> - `ENGINE = ReplacingMergeTree` tables (`evaluation_runs` keyed by `(tenant_id, run_id)` with monotonic `attempt`, and `eval_metric_points` keyed by `(tenant_id, run_id, metric_name, sample_idx)` with `created_at`) perform deduplication asynchronously during background merges; they do **NOT** provide immediate write-time idempotency.
> - Readers querying current state (including metric evaluators and quality-gate consumers) **MUST** query with `FINAL` (e.g. `SELECT ... FROM eval_metric_points FINAL WHERE tenant_id = ... AND run_id = ...`) or an equivalent version-aware deduplication aggregation (e.g., `argMax(...) GROUP BY (tenant_id, run_id, metric_name, sample_idx)`) to select the latest row and prevent seeing duplicate pre-merge attempts.
> - `reasoning_hash FixedString(64)` preserves the full 64-character SHA-256 hexadecimal digest for forensic auditability and deduplication in storage. Application structured logs output only the truncated 16-character prefix (`reasoning_hash = sha256(reasoning)[:16]`) to prevent log bloat.

---

## 5. Protobuf v3 gRPC Contract (`api/proto/v1/eval_worker.proto`)

```protobuf
syntax = "proto3";

package rag.eval.v1;

option go_package = "github.com/gemejime/rag/proto/eval/v1;evalv1";

service EvaluationWorkerService {
  rpc EvaluateBatch (EvaluateBatchRequest) returns (EvaluateBatchResponse);
  rpc GetRunStatus (GetRunStatusRequest) returns (GetRunStatusResponse);
}

message EvaluateBatchRequest {
  string tenant_id = 1;
  string run_id = 2;
  string dataset_id = 3;
  repeated EvaluationSample samples = 4;
  repeated string metrics = 5;
  uint32 concurrency_limit = 6;
}

message EvaluationSample {
  string sample_id = 1;
  uint32 sample_idx = 2;
  string query = 3;
  repeated string contexts = 4;
  string generated_answer = 5;
  string ground_truth = 6;
}

message EvaluateBatchResponse {
  string tenant_id = 1;
  string run_id = 2;
  uint32 total_samples = 3;
  uint32 completed_samples = 4;
  float avg_faithfulness = 5;
  float avg_context_precision = 6;
  float avg_answer_relevance = 7;
  bool passed_release_gate = 8;
  float duration_seconds = 9;
}

message GetRunStatusRequest {
  string tenant_id = 1;
  string run_id = 2;
}

message GetRunStatusResponse {
  string status = 1;
  uint32 progress_percentage = 2;
  uint32 current_sample_idx = 3;
}
```

---

## 6. Runtime Configuration & Environment Schema

```python
class Settings(BaseSettings):
    # Service Identifiers
    SERVICE_NAME: str = "rag-eval-governance-worker"
    PORT: int = 8080  # Internal FastAPI ASGI Doc API & Probes
    GRPC_PORT: int = 50051  # Internal gRPC EvaluationWorkerService
    HOST: str = "0.0.0.0"
    ENVIRONMENT: str = "production"

    # Temporal Settings
    TEMPORAL_HOST: str = "temporal.default.svc.cluster.local:7233"
    TEMPORAL_NAMESPACE: str = "default"
    EVAL_TASK_QUEUE: str = "eval-task-queue"
    ACTIVITY_HEARTBEAT_INTERVAL_SECONDS: float = 10.0
    ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS: float = 30.0

    # Judge Serving Settings (Triton / vLLM)
    JUDGE_ENDPOINT: str = "http://rag-inference-serving:8001"
    JUDGE_MODEL: str = "meta-llama/Llama-3.1-70B-Instruct"
    JUDGE_MAX_CONCURRENT_REQUESTS: int = 20
    JUDGE_RATE_LIMIT_RPS: float = 50.0
    JUDGE_REQUEST_TIMEOUT_SECONDS: float = 45.0
    JUDGE_429_MAX_RETRIES: int = 5

    # Outbound Circuit Breaker & Exponential Backoff
    JUDGE_CB_FAILURE_THRESHOLD: int = 5
    JUDGE_CB_RESET_TIMEOUT_SECONDS: float = 30.0
    JUDGE_BACKOFF_BASE_SECONDS: float = 1.0
    JUDGE_BACKOFF_MAX_SECONDS: float = 30.0

    # Prompt Registry Integration (Fallback to native templates if unreachable)
    PROMPT_REGISTRY_URL: str = "http://rag-prompt-registry-service:8080"
    PROMPT_REGISTRY_TIMEOUT_SECONDS: float = 5.0

    # ClickHouse Persistence Settings
    CLICKHOUSE_HOST: str = "clickhouse.storage.svc.cluster.local"
    CLICKHOUSE_PORT: int = 9000
    CLICKHOUSE_USER: str = "eval_worker"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "rag_governance"
    CLICKHOUSE_BATCH_SIZE: int = 1000
    CLICKHOUSE_FLUSH_INTERVAL_SECONDS: float = 1.0

    # Quality Gate Thresholds
    QUALITY_GATE_MIN_FAITHFULNESS: float = 0.82
    QUALITY_GATE_MIN_CONTEXT_PRECISION: float = 0.82
    QUALITY_GATE_MIN_ANSWER_RELEVANCE: float = 0.82
```

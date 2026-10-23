# Architecture Context - RAG Evaluation & Governance Worker

## System Stack & Specifications

| Layer | Technology | Role & Operational Characteristics |
| :--- | :--- | :--- |
| **Language Runtime** | Python 3.12+ (64-bit) | Modern compute runtime with strict type annotations |
| **Memory Allocator** | `jemalloc` (`LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`) | Eliminates glibc heap fragmentation during string manipulation; maintains RSS $\le 2.0$ GiB |
| **Orchestration** | Temporal Python SDK (`temporalio`) | Activity workers with 10s heartbeat checkpointing on `eval-task-queue` |
| **Judge Harness** | Native async HTTP/gRPC client to Triton/vLLM | Custom, lean LLM-as-a-judge client using structured JSON schemas (No Ragas/TruLens bloat) |
| **Metrics Engine** | Native prompt-based evaluators | Faithfulness, Context Precision, and Answer Relevance |
| **Rate Limiter** | Token-Bucket & Concurrency Semaphores | Governs outbound judge calls (max 20 concurrent), exponential jitter backoff on HTTP 429 |
| **Persistence Engine** | ClickHouse MergeTree Engine | Direct micro-batched ingestion for `evaluation_runs` and `eval_metric_points` |
| **API & Probes** | FastAPI + Uvicorn (ASGI) | OpenAPI 3.1 endpoints (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`), `/healthz`, `/ready`, `/metrics` |
| **Autoscaling** | KEDA (Kubernetes Event-driven Autoscaling) | Queue-backlog autoscaling driven by Temporal `eval-task-queue` depth (Min: 1, Max: 8) |

---

## Production-Grade 5-Layer Backend Architecture

Aligned with [Production Grade Backend Concepts.pdf](file:///c:/Users/ANIK/Desktop/Gemejime/Evaluation%20&%20Quality%20Worker/tasks/Production%20Grade%20Backend%20Concepts.pdf):

### Layer 1: API & Routing
- **Entry Points (`cmd/worker/main.py`)**:
  - Initializes runtime environment, validates configuration via Pydantic Settings, establishes ClickHouse connection pools, and bootstraps the Temporal activity worker daemon (`eval-task-queue`).
  - Concurrently launches the lightweight FastAPI HTTP probe server on port `8080` (`/healthz`, `/ready`, `/docs`, `/metrics`).
  - Concurrently binds the gRPC server on port `50051` exposing `rag.eval.v1.EvaluationWorkerService` (`EvaluateBatch`, `GetRunStatus`).
- **Endpoints & Routing**:
  - `GET /docs`: Interactive Swagger UI powered by OpenAPI 3.1.
  - `GET /redoc`: Interactive ReDoc UI.
  - `GET /api/docs` & `GET /openapi.json`: Machine-readable OpenAPI 3.1 JSON specification.
  - `GET /healthz`: Liveness probe verifying worker event loop health.
  - `GET /ready`: Readiness probe verifying ClickHouse connectivity, judge endpoint reachability, and Temporal queue polling.
  - `GET /metrics`: Prometheus telemetry scrape endpoint exposing histograms, gauges, and counters.
- **Middleware & Interception**:
  - Request ID tracing middleware assigning ULIDs to every operational probe and RPC.
  - Structured logging middleware capturing request duration, tenant ID, and response codes.
- **Input Validation & Sanitization**:
  - Strict Pydantic v2 schemas validating incoming evaluation jobs, sample records, query/context/ground-truth triplets, and judge model JSON responses.
- **Rate Limiting, Throttling & Circuit Breaking**:
  - Client-side token-bucket rate limiter (50 RPS) and concurrency semaphores (`JUDGE_MAX_CONCURRENT_REQUESTS=20`) to prevent overwhelming Triton/vLLM serving clusters.
  - Automatic backpressure with exponential full jitter on downstream HTTP 429 / `RESOURCE_EXHAUSTED`.
  - Circuit breaker trips after 5 consecutive downstream judge failures with a 30s reset timeout to prevent cascaded thread exhaustion.
- **Idempotency**:
  - Batch evaluation runs and sample evaluations are keyed by unique `(tenant_id, run_id, sample_idx)`. Checkpoint resumption skips previously evaluated samples without duplicate judge calls.

### Layer 2: Data Persistence & State
- **ClickHouse Unified Persistence & Idempotency (ADR-003)**:
  - Eliminates dual-writes to relational databases (PostgreSQL removed).
  - Table `evaluation_runs`: Run-level execution metadata, status, sample count, aggregate scores, and duration (`ENGINE = ReplacingMergeTree()`).
  - Table `eval_metric_points`: Sample-level point-in-time metric evaluations (`ENGINE = ReplacingMergeTree(created_at)`) partitioned by `toYYYYMM(run_date)` and ordered by `(tenant_id, run_id, metric_name, sample_idx)`. Repeated evaluations or retried activity attempts overwrite cleanly without duplicate row accumulation.
  - Forensic Hash Integrity: ClickHouse persists the full 64-character SHA-256 digest (`FixedString(64)`), while application logs redact it to 16 characters (`[:16]`).
- **Direct Micro-Batching & Buffering**:
  - Client-side async buffer micro-batches metric insertions (1,000 records or 1.0s flush interval), eliminating small part fragmentation in ClickHouse.
- **Connection Pooling**:
  - Managed ClickHouse TCP/Native client pool with active health ping and bounded maximum connections.
- **Resource Identification Strategies**:
  - High-entropy ULIDs/UUIDv4 for `run_id`, `sample_id`, and `evaluation_id` preventing enumeration attacks.

### Layer 3: Architecture & Scale
- **Asynchronous Task Queues & Workers**:
  - Temporal task queue: `eval-task-queue`.
  - Batch evaluation DAG workflow decomposes golden sets into thread-isolated activity executions.
- **High Availability & Autoscaling**:
  - Stateless worker pods deployed on compute-optimized nodes (`c6i.2xlarge`).
  - Autoscaled by KEDA based on Temporal queue backlog: Min 1 replica, Max 8 replicas.
- **Creational Design Patterns**:
  - Singletons for judge client connection manager, ClickHouse writer sink, and telemetry registries.
- **Decoupled Architecture (ADR-001)**:
  - Real-time ingestion masking (`rag-masking-sanitizer`) is decoupled into a dedicated stream repository, isolating batch evaluation compute from streaming ingestion.

### Layer 4: Security & Identity
- **Tenant Isolation & Access Control (AuthN vs AuthZ)**:
  - Tenant boundary enforcement: Every evaluation task requires explicit `tenant_id`. All ClickHouse queries and storage partitions are tenant-isolated.
- **Secrets & Configuration Management**:
  - 12-Factor App design via `internal/config/settings.py` reading environment variables.
  - Zero hardcoded secrets: HashiCorp Vault / AWS KMS keys for HMAC-SHA256 token hashing reside purely in volatile heap memory.
- **Secured Logging Invariant**:
  - All logging is structured JSON.
  - Automated redaction of customer PII, golden-set raw prompt/context text spans, bearer tokens, presigned URLs, and encryption salts before emission.
- **Container Hardening**:
  - Non-root UID 1000 (`runAsNonRoot: true`), read-only root filesystem (`readOnlyRootFilesystem: true`), ephemeral scratch mounts on `/tmp` and emptyDir `/dev/shm`.

### Layer 5: Observability & Quality
- **Structured Logging**: Machine-parseable JSON logs containing `tenant_id`, `run_id`, `sample_idx`, `metric_name`, and latency.
- **Prometheus Observability Matrix**:
  - `eval_batch_duration_seconds`: Histogram of total batch evaluation pipeline runs.
  - `eval_sample_execution_latency_seconds`: Latency per single sample judge evaluation.
  - `eval_judge_active_concurrency`: Gauge tracking active in-flight judge calls against configured cap (20).
  - `eval_judge_rate_limit_errors_total`: Counter tracking HTTP 429 / `RESOURCE_EXHAUSTED` responses.
  - `eval_metric_score_value`: Gauge tracking calculated scores across evaluation dimensions.
- **Alert Rules**:
  - *Judge Quota Saturation (Warning)*: `rate(eval_judge_rate_limit_errors_total[5m]) > 0`.
  - *Activity Heartbeat Stalling (Page)*: Activity heartbeat latency exceeds 25 seconds.
  - *Evaluation Quality Drop (Warning)*: Aggregate faithfulness or context_precision falls below $\tau < 0.82$.
- **Per-Sample Heartbeat Resumption**:
  - Activities emit Temporal heartbeats every 10 seconds containing current sample index checkpoint.
  - If a worker is terminated (e.g. SIGKILL at sample 47), the restarted worker resumes at sample 48 with zero duplicate judge queries.
- **Fault Isolation & Sample-Level DLQ**:
  - Corrupted or unparseable samples and samples that persistently fail downstream judge queries (exceeding max retries) are isolated to a Dead-Letter Queue (DLQ) and logged in `failed_samples_count`.
  - The evaluation activity continues processing the remaining valid samples to ensure complete golden-set evaluation runs without batch abortion.

---

## Monorepo Directory Boundaries

```
rag-eval-worker/
├── cmd/
│   └── worker/
│       └── main.py              # Temporal worker daemon & FastAPI probe bootstrap
├── api/
│   └── proto/v1/
│       └── eval_worker.proto    # Batch evaluation request and metric schemas
├── internal/
│   ├── config/
│   │   └── settings.py          # Judge endpoints, rate limits, ClickHouse creds
│   ├── evaluation/
│   │   ├── runner.py            # Async batch runner with per-sample heartbeating
│   │   ├── ratelimiter.py       # Outbound judge token-bucket concurrency governor
│   │   ├── judges/
│   │   │   ├── base.py          # Abstract judge client interface
│   │   │   └── triton_client.py # Native gRPC/HTTP client to rag-inference-serving
│   │   └── metrics/
│   │       ├── context_precision.py # Native prompt-based retrieval precision
│   │       ├── faithfulness.py      # Native prompt-based claim verification
│   │       └── answer_relevance.py  # Native prompt-based query relevance
│   ├── storage/
│   │   └── clickhouse_sink.py   # Direct micro-batched client for runs and scores
│   ├── temporal/
│   │   ├── activities.py        # Temporal activity wrapper with heartbeating
│   │   └── workflows.py         # Batch evaluation DAG definitions
│   └── telemetry/
│       └── metrics.py           # Prometheus counters, score distributions, judge gauges
├── deployments/
│   ├── docker/
│   │   └── Dockerfile           # Lightweight image with jemalloc (no spacy/trulens bloat)
│   └── helm/
│       ├── values.yaml          # Pod CPU/Memory limits and KEDA queue scaling
│       └── templates/
│           └── scaledobject.yaml# KEDA scaler driven by Temporal backlog
├── test/
│   ├── unit/                    # Metric formula and prompt validation tests
│   ├── integration/             # Mock judge and ClickHouse sink pipeline tests
│   └── fixtures/                # Golden-set evaluation suites
├── pyproject.toml
└── Makefile
```

---

## Architectural Decision Records (ADRs)

- **ADR-001: Separation of Ingestion Sanitization from Offline Evaluation**: Eliminates monolithic resource starvation by creating independent microservices with dedicated scaling profiles (`rag-masking-sanitizer` on stream vs. `rag-eval-worker` on batch).
- **ADR-002: Native Metric Prompts over Ragas/TruLens Dependencies**: Replaces third-party evaluation library runtimes with in-house, version-controlled prompt templates executed directly against inference gateways to maintain environment predictability and low pod overhead.
- **ADR-003: ClickHouse Unified Persistence**: Replaces PostgreSQL/ClickHouse dual writes by storing both run execution headers and point-in-time evaluation records in ClickHouse tables.
- **ADR-004: Tiered PII/PHI Processing**: Applies fast-path regex/automaton checks first, reserving heavy NLP/NER models purely for spans marked with high informational entropy.

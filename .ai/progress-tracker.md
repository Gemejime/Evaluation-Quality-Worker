# Progress Tracker - RAG Evaluation & Governance Worker

## Current Phase

- **Phase 3: Temporal Task Queues, Rate Limiting & DLQ** (🟢 Completed)

## Current Goal

- Prepare for Phase 4: Chaos hardening, 429 flood injection, soak testing, and throughput benchmarking on c6i.2xlarge.

---

## Calibrated 4-Phase Roadmap & Milestones

| Phase | Timeline | Focus Area | Deliverables & Exit Criteria | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 1** | Weeks 1–2 | Decoupling & Batch Worker Foundation | • `.gitignore` and `.github/workflows/ci.yml` established.<br>• Full `.ai` context and `GEMINI.MD` aligned with backend concepts & ADR-001.<br>• Protobuf v3 contract (`api/proto/v1/eval_worker.proto`) defined and compiled.<br>• Packaging and dependency environment configured (`pyproject.toml`, Dockerfile with `jemalloc`).<br>• FastAPI probe server (`/healthz`, `/ready`, `/docs`, `/redoc`, `/api/docs`, `/metrics`) and async runner daemon scaffold.<br>• Comprehensive unit test suite (`test/unit/`) validating settings, telemetry, and probes. | 🟢 Completed |
| **Phase 2** | Weeks 3–4 | Lean Eval Runner & Direct ClickHouse Sink | • Native prompt-based judge adapter targeting Triton/vLLM endpoints (with fallback to `rag-prompt-registry-service`).<br>• Implementation of core RAG triad metrics with structured JSON schemas (Faithfulness, Context Precision, Answer Relevance).<br>• Direct micro-batched ClickHouse writer with buffer tables (`evaluation_runs`, `eval_metric_points` with `ReplacingMergeTree`).<br>• Token-bucket rate limiter, concurrency governor (20 cap), full jitter backoff on 429, and decoupled circuit breaker.<br>• Batch runner with sample-level DLQ fault isolation and secured structured logging audit.<br>• Comprehensive unit tests (33 tests) passing cleanly under strict typing. | 🟢 Completed |
| **Phase 3** | Weeks 5–6 | Temporal Task Queues, Rate Limiting & DLQ | • Temporal activity runner (`eval-task-queue`) with per-sample 10s heartbeating and DLQ fault isolation.<br>• Token-bucket rate limiter enforcing outbound judge concurrency caps (max 20) with exponential backoff and circuit breaker.<br>• Zero-recompute resumption with out-of-order concurrency set difference filtering.<br>• CI/CD golden-set regression quality gates ($\tau \ge 0.82$) blocking pull requests on regressions.<br>• Kubernetes Helm blueprint with c6i.2xlarge sizing and KEDA queue backlog autoscaler.<br>• Comprehensive unit test suite (49 tests) passing under strict typing. | 🟢 Completed |
| **Phase 4** | Weeks 7–8 | Chaos Hardening & Benchmarking | • Evaluation throughput benchmarks: 100-sample golden set $P95 \le 180$s on `c6i.2xlarge`.<br>• Simulated judge 429 flood testing verifying circuit breaker trip, non-blocking queue pause, and jittered resumption.<br>• 12-hour soak testing under `jemalloc` validating RSS stability below 2.0 GiB. | ⚪ Queued |

---

## Completed Units

- **Phase 1 Foundation Setup**:
  - **Git Ignore Configuration** (`.gitignore`):
    - Configured comprehensive ignore rules ignoring `tasks/` reference folder, virtual environments, caches, OS artifacts, and secrets.
  - **GitHub Actions CI/CD Pipeline** (`.github/workflows/ci.yml`):
    - Established CI workflow with jobs for code quality & type safety (Ruff lint, format, mypy), test suite (pytest), golden-set evaluation regression quality gate simulation, and Docker container build verification.
  - **Context & Architecture System Overhaul** (`.ai/`, `.ai.md`):
    - Synchronized all `.ai` context files (`project-overview.md`, `architecture.md`, `doc-api.md`, `code-standards.md`, `security.md`, `ai-workflow-rules.md`, `agents.md`, `progress-tracker.md`) to the `rag-eval-governance-worker` microservice specification.
    - Integrated core 5-layer concepts from `Production Grade Backend Concepts.pdf` into architecture and code standards.
  - **Master Agent Directive** (`GEMINI.MD`):
    - Created comprehensive system prompt referencing `.ai/agents.md` and all `.ai/` context files, enforcing non-negotiable invariants, secured logging, continuous Doc API / OpenAPI updates, and Git commit standards.
  - **Protobuf v3 Contract & Generation Tooling** (`api/proto/v1/eval_worker.proto`, `scripts/compile_proto.py`):
    - Defined `EvaluationWorkerService` gRPC interface with `EvaluateBatch` and `GetRunStatus`.
    - Generated Python stubs and typings (`eval_worker_pb2.py`, `eval_worker_pb2_grpc.py`, `eval_worker_pb2.pyi`).
  - **Modern Project Packaging** (`pyproject.toml`):
    - Configured PEP 518/621 packaging with explicit runtime, dev, and test dependencies and configurations for Ruff, mypy strict typing, and pytest.
  - **Hardened Container Blueprint** (`deployments/docker/Dockerfile`):
    - Docker blueprint with `python:3.12-slim-bookworm`, `jemalloc` memory allocator preloading (`LD_PRELOAD`), non-root `appuser` (UID 1000), healthcheck probes, and ports 8080/50051.
  - **12-Factor Settings Management** (`internal/config/settings.py`):
    - Pydantic Settings implementation for service, Temporal, judge inference, prompt registry, ClickHouse, and quality gates with `@lru_cache` singleton accessor.
  - **Prometheus Telemetry & Decoupled Readiness** (`internal/telemetry/metrics.py`, `internal/telemetry/readiness.py`):
    - Prometheus metrics collectors (`eval_batch_duration_seconds`, `eval_sample_execution_latency_seconds`, `eval_judge_active_concurrency`, `eval_judge_rate_limit_errors_total`, `eval_metric_score_value`).
    - Decoupled `ReadinessManager` supporting simulated dependency states for deterministic testing.
  - **Async Worker Daemon Entrypoint & Probe Server** (`cmd/worker/main.py`):
    - FastAPI ASGI application exposing `/healthz`, `/ready` (200/503), `/docs`, `/redoc`, `/api/docs`, and `/metrics`.
    - Asynchronous `TaskGroup` daemon runner for probe server, gRPC server, and Temporal activities.
  - **Automated Unit Test Suite** (`test/unit/`):
    - Comprehensive unit tests verifying settings, Prometheus metrics, and all HTTP probe endpoints.
- **Phase 2 Lean Eval Runner & Direct ClickHouse Persistence**:
  - **Native Judge Client Adapter** (`internal/evaluation/judges/`):
    - Abstract `BaseJudgeClient` protocol and Pydantic `JudgeResponse`.
    - Native `TritonJudgeClient` targeting Triton/vLLM endpoints with connection pooling (`httpx.Limits(max_connections=20)`), JSON object enforcement (`response_format={"type": "json_object"}`), defensive markdown code block stripping, and latency measurements.
  - **Outbound Judge Rate Limiter & Concurrency Governor** (`internal/evaluation/ratelimiter.py`):
    - Thread-safe token bucket rate limiter (50 RPS) guarded by `asyncio.Lock` eliminating concurrency stampedes.
    - Concurrency semaphore (20 cap) tracking active gauge in Prometheus.
    - Full jitter exponential backoff on HTTP 429 / `RESOURCE_EXHAUSTED` (5 retries).
    - Decoupled circuit breaker tripping after 5 consecutive downstream judge failures (30s recovery window) without counting handled 429s.
  - **Native RAG Triad Metric Evaluators** (`internal/evaluation/metrics/`):
    - Abstract `BaseMetricEvaluator` with guaranteed 64-char hexadecimal SHA-256 `reasoning_hash` computation.
    - `ContextPrecisionEvaluator`: single-prompt multi-chunk evaluation returning binary verdicts $v_k \in \{0, 1\}$; precision@k formula with explicit zero-relevant-chunks contract (`0.0`).
    - `FaithfulnessEvaluator`: single-prompt atomic claim extraction and factual verification; ratio formula with zero-extracted-claims contract (`1.0`).
    - `AnswerRelevanceEvaluator`: continuous semantic relevance scoring ($0.0$ to $1.0$).
  - **Direct Micro-Batched ClickHouse Storage Sink** (`internal/storage/clickhouse_sink.py`):
    - Direct persistence to `evaluation_runs` and `eval_metric_points` using `ReplacingMergeTree` tables without PostgreSQL dual writes (ADR-003).
    - Non-blocking driver operations dispatched via `asyncio.to_thread` to protect ASGI event loop and health probes.
    - Client-side asynchronous micro-batch buffering (1,000 records or 1.0s interval).
    - Read deduplication helper `get_run_metrics(...)` enforcing `FINAL` query semantics.
  - **Batch Evaluation Runner** (`internal/evaluation/runner.py`):
    - Async coordinator orchestrating evaluation batches across selected metrics.
    - Sample-level Dead-Letter Queue (DLQ) fault isolation: malformed triplets or unrecoverable judge errors increment `failed_samples_count` without crashing the batch.
    - Forward-compatible progress hook `on_sample_complete` for seamless Phase 3 Temporal heartbeat resumption.
    - Secured Logging: Strict redaction of raw prompt texts, context passages, and generated answers; emitting only truncated 16-character reasoning hashes (`sha256(reasoning)[:16]`), token counts, and execution latencies.
  - **Comprehensive Unit Test Suite** (`test/unit/`):
    - 33 automated tests covering judge client parsing, rate limiter state transitions, triad metric formulas, ClickHouse micro-batch buffering, DLQ fault isolation, and secured logging redaction.
- **Phase 3 Temporal Task Queues, Rate Limiting & DLQ**:
  - **Temporal Workflow & Activity Data Models** (`internal/temporal/models.py`):
    - `RunEvaluationWorkflowInput`, `RunEvaluationWorkflowOutput`, `EvaluateGoldenSetInput`, `EvaluateGoldenSetResult`, and `EvaluationHeartbeatDetails`.
    - Checkpoint payload supporting `completed_sample_indices: list[int]` for set-difference resumption.
  - **Temporal Activity Runner with Out-of-Order Resumption & Dual Heartbeating** (`internal/temporal/activities.py`):
    - `@activity.defn(name="EvaluateGoldenSetActivity")` on `eval-task-queue`.
    - Zero-recompute resumption via set difference: `remaining_samples = [s for s in samples if s.sample_idx not in completed_sample_indices]`, guaranteeing no skipped samples under out-of-order execution.
    - Dual-trigger heartbeating: periodic background ticker (every 10s) preventing timeouts on judge backoff delays + immediate per-sample completion callback updating checkpoint.
    - Graceful cancellation handling: flushes buffered ClickHouse records on `CancelledError` before re-raising.
    - Sample-level DLQ quarantine: poison-pill samples isolate errors without crashing batch execution.
  - **Batch Evaluation Workflow Orchestration** (`internal/temporal/workflows.py`):
    - `@workflow.defn(name="BatchEvaluationWorkflow")` with activity timeouts (`start_to_close=600s`, `heartbeat=30s`) and retry policy.
    - Scoped release quality gate enforcement ($\tau \ge 0.82$, `failed_samples_count == 0`) across active metric list.
  - **Resilient Temporal Worker Daemon & Lifecycle Integration** (`internal/temporal/worker.py` & `cmd/worker/main.py`):
    - `TemporalWorkerManager` with automatic non-fatal reconnection loop updating `ReadinessManager` status (`temporal="polling"` vs `"unreachable"`).
    - Integrated into `asyncio.TaskGroup` in `cmd/worker/main.py`.
  - **Golden-Set Regression Gate & CI/CD Pipeline Integration** (`test/fixtures/golden_set.json`, `scripts/trigger_regression_gate.py`, `.github/workflows/ci.yml`):
    - Realistic query-context-answer golden-set fixture.
    - Standalone CLI runner with `--mock-judge` flag for offline validation.
    - CI/CD workflow step asserting release quality gates ($\tau \ge 0.82$, DLQ = 0).
  - **Kubernetes Helm Deployment Blueprint** (`deployments/helm/values.yaml`, `deployments/helm/templates/scaledobject.yaml`):
    - Calibrated resource sizing for AWS `c6i.2xlarge` compute-optimized nodes (`2000m / 4000m` CPU, `2.0Gi / 3.5Gi` RAM, `LD_PRELOAD` jemalloc).
    - KEDA `ScaledObject` driven by Prometheus metric monitoring Temporal queue backlog.
  - **Comprehensive Automated Test Suite** (`test/unit/`):
    - 49 unit tests all passing with 100% success rate under strict typing.

---

## Architectural Decision Records (ADRs)

- **ADR-001: Separation of Ingestion Sanitization from Offline Evaluation**: Eliminates monolithic resource starvation by creating independent microservices with dedicated scaling profiles (`rag-masking-sanitizer` on stream vs. `rag-eval-worker` on batch).
- **ADR-002: Native Metric Prompts over Ragas/TruLens Dependencies**: Replaces third-party evaluation library runtimes with in-house, version-controlled prompt templates executed directly against inference gateways to maintain environment predictability and low pod overhead.
- **ADR-003: ClickHouse Unified Persistence**: Replaces PostgreSQL/ClickHouse dual writes by storing both run execution headers and point-in-time evaluation records in ClickHouse tables.
- **ADR-004: Tiered PII/PHI Processing**: Applies fast-path regex/automaton checks first, reserving heavy NLP/NER models purely for spans marked with high informational entropy.

---

## Operational Runbooks

### Runbook 1: Downstream Judge Saturation Remediation
1. Inspect `eval_judge_rate_limit_errors_total`.
2. If 429 rates rise, reduce the concurrency cap dynamically in the worker configuration map (`JUDGE_MAX_CONCURRENT_REQUESTS`).
3. Verify that Temporal activity timeouts are sized appropriately to allow queues to drain without failing execution activities.

### Runbook 2: Quality Gate CI/CD Rejection Troubleshooting
1. Inspect failed pipeline execution via ClickHouse (using `FINAL` for deduplicated reads):
   ```sql
   SELECT * FROM eval_metric_points FINAL WHERE run_id = '{run_id}' AND score < 0.82;
   ```
2. If faithfulness degraded: check for regressions in prompt template system instructions in `rag-prompt-registry-service`.
3. If context_precision degraded: check candidate retrieval and reranking parameters in `rag-search-orchestrator`.

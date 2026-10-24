# Implementation Plan: Phase 1 - Batch Evaluation Worker Foundation (Hardened)

**Microservice**: `rag-eval-governance-worker` (`rag-eval-worker`)  
**Specification Reference**: `tasks/RAG Evaluation and Governance Worker Plan.pdf` & `GEMINI.MD`  
**Authoritative Context**: `.ai/architecture.md`, `.ai/doc-api.md`, `.ai/code-standards.md`, `.ai/security.md`, `.ai/ai-workflow-rules.md`, `.ai/progress-tracker.md`

---

## 1. Executive Summary & Objective

In accordance with **ADR-001** (Decoupled Compute Architecture), real-time stream masking is isolated to `rag-masking-sanitizer`, and this repository focuses entirely on the batch evaluation and governance worker (`rag-eval-worker`). The calibrated 4-phase milestone progression strictly follows `.ai/progress-tracker.md` (superseding the pre-decoupling combined milestone table in early planning documents).

The objective of Phase 1 is to construct the hardened foundational microservice infrastructure for `rag-eval-worker`:
1. Establish Protobuf v3 gRPC definitions (`api/proto/v1/eval_worker.proto`) and stub generation tooling.
2. Configure complete packaging and runtime/dev dependency definitions (`pyproject.toml`) including `clickhouse-connect` and `grpcio-tools`.
3. Build hardened Docker container blueprint with verified `jemalloc` library paths and non-root execution (`deployments/docker/Dockerfile`).
4. Implement 12-factor runtime configuration schema (`internal/config/settings.py`) using Pydantic Settings.
5. Implement runtime Prometheus telemetry collectors (`internal/telemetry/metrics.py`) matching platform observability specifications.
6. Build asynchronous entrypoint and FastAPI ASGI probe server (`cmd/worker/main.py`) exposing `/healthz`, `/ready`, `/metrics`, and OpenAPI 3.1 endpoints (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`), architected with an asynchronous task manager to host gRPC and Temporal daemons in subsequent phases.
7. Implement unit test suite verifying settings, telemetry, and HTTP probes (`test/unit/`), featuring decoupled readiness state providers.
8. Align CI workflow (`.github/workflows/ci.yml`) to ensure Phase 1 passes cleanly without premature evaluator dependency failures.
9. Synchronize `.ai/doc-api.md` and `.ai/progress-tracker.md`.

---

## 2. Invariants & Architectural Alignment

- **Secured Logging Invariant**: Structured JSON logging. No raw golden-set prompt texts, retrieved contexts, generated answers, KMS salts, or PII are logged. Diagnostic logs emit only truncated reasoning hashes (`sha256(reasoning)[:16]`) and score values.
- **Continuous Doc API Invariant**: `.ai/doc-api.md` and OpenAPI 3.1 endpoints (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`) are updated and strictly aligned with all routes and schemas.
- **Memory Allocation**: Dockerfile configures `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2` (with symlink fallback) and non-root execution (`USER 1000`) to enforce flat RSS $\le 2.0$ GiB.
- **Code Quality**: Ruff linting, formatting, and mypy strict static typing pass cleanly with zero errors.
- **Git Commit Standards**: Conventional commits (`feat:`, `chore:`, `tests:`, `docs:`) with at least two distinct commits.

---

## 3. Detailed Deliverables & Files to Create / Modify

### A. Protobuf Contract & Generation Tooling (`api/proto/v1/eval_worker.proto`)
- `syntax = "proto3";`
- Package: `rag.eval.v1`
- Option: `option go_package = "github.com/gemejime/rag/proto/eval/v1;evalv1";`
- RPCs:
  - `EvaluateBatch(EvaluateBatchRequest) returns (EvaluateBatchResponse)`
  - `GetRunStatus(GetRunStatusRequest) returns (GetRunStatusResponse)`
- Messages:
  - `EvaluateBatchRequest` (`string tenant_id`, `string run_id`, `string dataset_id`, `repeated EvaluationSample samples`, `repeated string metrics`, `uint32 concurrency_limit`)
  - `EvaluationSample` (`string sample_id`, `uint32 sample_idx`, `string query`, `repeated string contexts`, `string generated_answer`, `string ground_truth`)
  - `EvaluateBatchResponse` (`string tenant_id`, `string run_id`, `uint32 total_samples`, `uint32 completed_samples`, `float avg_faithfulness`, `float avg_context_precision`, `float avg_answer_relevance`, `bool passed_release_gate`, `float duration_seconds`)
  - `GetRunStatusRequest` (`string tenant_id`, `string run_id`)
  - `GetRunStatusResponse` (`string status`, `uint32 progress_percentage`, `uint32 current_sample_idx`)
- Compilation tooling: Provide a utility script (`scripts/compile_proto.py` or Makefile) using `grpc_tools.protoc` to compile `.proto` to `_pb2.py` and `_pb2_grpc.py`.

### B. Project Packaging (`pyproject.toml`)
- Modern PEP 518/621 configuration with `[build-system]` and `[project]`.
- Runtime dependencies:
  - `fastapi>=0.115.0`
  - `uvicorn[standard]>=0.30.0`
  - `pydantic>=2.8.0`
  - `pydantic-settings>=2.4.0`
  - `prometheus-client>=0.20.0`
  - `temporalio>=1.6.0`
  - `structlog>=24.4.0`
  - `grpcio>=1.65.0`
  - `protobuf>=5.27.0`
  - `httpx>=0.27.0`
  - `clickhouse-connect>=0.7.0` (Core persistence client for ADR-003)
- Optional/Test dependencies (`[project.optional-dependencies]`):
  - `test = ["pytest>=8.3.0", "pytest-asyncio>=0.24.0", "pytest-cov>=5.0.0"]`
  - `dev = ["ruff>=0.6.0", "mypy>=1.11.0", "types-protobuf>=5.27.0", "grpcio-tools>=1.65.0"]`
- Tool configurations:
  - `[tool.ruff]`: line-length = 100, target-version = "py312"
  - `[tool.ruff.lint]`: select = ["E", "F", "I", "N", "W", "ASYNC", "B", "C4", "UP"]
  - `[tool.mypy]`: python_version = "3.12", strict = true, disallow_untyped_defs = true
  - `[tool.pytest.ini_options]`: asyncio_mode = "auto", testpaths = ["test"]

### C. Hardened Container Blueprint (`deployments/docker/Dockerfile`)
- Base image: `python:3.12-slim-bookworm`
- Install system dependencies: `libjemalloc2`, `curl`, `ca-certificates`
- Safe symlink & preload:
  ```dockerfile
  RUN apt-get update && apt-get install -y --no-install-recommends libjemalloc2 curl ca-certificates \
      && ln -sf /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 /usr/lib/x86_64-linux-gnu/libjemalloc.so \
      && rm -rf /var/lib/apt/lists/*
  ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
  ```
- Create unprivileged user `appuser` (UID 1000)
- Copy requirements, install wheels, copy application source
- Expose ports `8080` (HTTP probes/OpenAPI) and `50051` (gRPC)
- Healthcheck targeting `http://localhost:8080/healthz`
- Non-root user `USER 1000`
- Entrypoint executing `python -m cmd.worker.main`

### D. Settings Management (`internal/config/settings.py`)
- Pydantic `BaseSettings` reading environment variables with case-sensitive matching:
  - Service parameters (`SERVICE_NAME="rag-eval-governance-worker"`, `PORT=8080`, `GRPC_PORT=50051`, `HOST="0.0.0.0"`, `ENVIRONMENT="production"`)
  - Temporal parameters (`TEMPORAL_HOST`, `TEMPORAL_NAMESPACE`, `EVAL_TASK_QUEUE="eval-task-queue"`, `ACTIVITY_HEARTBEAT_INTERVAL_SECONDS=10.0`, `ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS=30.0`)
  - Judge Serving (`JUDGE_ENDPOINT`, `JUDGE_MODEL="meta-llama/Llama-3.1-70B-Instruct"`, `JUDGE_MAX_CONCURRENT_REQUESTS=20`, `JUDGE_RATE_LIMIT_RPS=50.0`, `JUDGE_REQUEST_TIMEOUT_SECONDS=45.0`, `JUDGE_429_MAX_RETRIES=5`, `JUDGE_CB_FAILURE_THRESHOLD=5`, `JUDGE_CB_RESET_TIMEOUT_SECONDS=30.0`, `JUDGE_BACKOFF_BASE_SECONDS=1.0`, `JUDGE_BACKOFF_MAX_SECONDS=30.0`)
  - Prompt Registry (`PROMPT_REGISTRY_URL`, `PROMPT_REGISTRY_TIMEOUT_SECONDS=5.0`)
  - ClickHouse (`CLICKHOUSE_HOST`, `CLICKHOUSE_PORT=9000`, `CLICKHOUSE_USER="eval_worker"`, `CLICKHOUSE_PASSWORD=""`, `CLICKHOUSE_DATABASE="rag_governance"`, `CLICKHOUSE_BATCH_SIZE=1000`, `CLICKHOUSE_FLUSH_INTERVAL_SECONDS=1.0`)
  - Quality Gates (`QUALITY_GATE_MIN_FAITHFULNESS=0.82`, `QUALITY_GATE_MIN_CONTEXT_PRECISION=0.82`, `QUALITY_GATE_MIN_ANSWER_RELEVANCE=0.82`)
- Singleton accessor `get_settings()` with `@lru_cache`.

### E. Telemetry Collectors (`internal/telemetry/metrics.py`)
- Prometheus collectors adhering to `.ai/doc-api.md`:
  - `eval_batch_duration_seconds` (Histogram, labels: `["status"]`)
  - `eval_sample_execution_latency_seconds` (Histogram, labels: `["metric_name", "judge_model"]`)
  - `eval_judge_active_concurrency` (Gauge, labels: `["judge_model"]`)
  - `eval_judge_rate_limit_errors_total` (Counter, labels: `["judge_model", "error_type"]`) — tracks `error_type="429"` and `"exhausted"`
  - `eval_metric_score_value` (Gauge, labels: `["metric_name"]`)

### F. Asynchronous Daemon Entrypoint & HTTP Probe Server (`cmd/worker/main.py`)
- FastAPI ASGI application configured with OpenAPI 3.1 metadata (`title`, `version`, `description`, `docs_url="/docs"`, `redoc_url="/redoc"`, `openapi_url="/openapi.json"`).
- Decoupled Readiness Health Registry (`internal/telemetry/readiness.py` or state manager):
  - Provides queryable flags for `clickhouse`, `temporal`, `judge_endpoint`.
  - Enables clean 200 vs 503 testing without hard external socket dependencies in unit tests.
- Endpoints:
  - `GET /healthz`: Returns `{"status": "healthy"}`.
  - `GET /ready`: Returns readiness JSON with dependency statuses (status 200 if all ready, 503 if any degraded).
  - `GET /api/docs`: Explicit alias returning OpenAPI 3.1 JSON schema.
  - `GET /metrics`: Returns Prometheus scrape payload (`text/plain; version=0.0.4`).
- CLI bootstrap function `main()`:
  - Runs an asynchronous application scaffold (`asyncio.run(serve())`) that starts `uvicorn.Server.serve()` concurrently, structured to cleanly incorporate gRPC server and Temporal worker daemons in Phases 2 and 3.

### G. CI Pipeline Alignment (`.github/workflows/ci.yml`)
- Update `ci.yml` `golden-set-eval-gate` to check if `evaluator_file` exists before executing. If absent, output:
  `Evaluator internal/evaluation/runner.py queued for Phase 2; skipping golden-set eval gate.`
  This prevents breaking CI before Phase 2 implementation while keeping strict checks active once implemented.

### H. Unit Tests (`test/unit/`)
- `test/unit/test_settings.py`: Validates default settings, environment variable overrides, and quality gate boundaries.
- `test/unit/test_telemetry.py`: Validates metric registration, observation, and Prometheus scrape emission.
- `test/unit/test_probes.py`: Validates `/healthz`, `/ready` (both ready and degraded scenarios via state mocking), `/api/docs`, `/docs`, `/metrics` using FastAPI `TestClient`.

---

## 4. Verification & Quality Gates

1. **Linting & Formatting**:
   ```powershell
   python -m ruff check --fix .
   python -m ruff format .
   ```
2. **Type Checking**:
   ```powershell
   python -m mypy --ignore-missing-imports cmd internal test
   ```
3. **Automated Unit Tests**:
   ```powershell
   python -m pytest test/unit -v
   ```
4. **Container Build Verification**:
   ```powershell
   docker build -f deployments/docker/Dockerfile -t rag-eval-governance-worker:phase1 .
   ```
5. **Doc API & Progress Tracker Sync**:
   Verify `.ai/doc-api.md` matches endpoints and update `.ai/progress-tracker.md`.
6. **Git Commit Plan**:
   - Commit 1: `chore(build): configure packaging, Dockerfile with jemalloc, proto contract, and CI alignment`
   - Commit 2: `feat(api): implement settings, Prometheus telemetry, and async probe server`
   - Commit 3: `tests: add unit tests for settings, telemetry, and health probe endpoints`

---

## 5. Confirmation

Upon confirmation, execution of Phase 1 will proceed strictly according to this hardened implementation plan.

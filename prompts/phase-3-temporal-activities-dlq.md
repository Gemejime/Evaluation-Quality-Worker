# Implementation Plan: Phase 3 - Temporal Task Queues, Rate Limiting & DLQ (Pressure-Tested)

**Microservice**: `rag-eval-governance-worker` (`rag-eval-worker`)  
**Specification References**: `tasks/RAG Evaluation and Governance Worker Plan.pdf` & `GEMINI.MD`  
**Authoritative Context**: `.ai/architecture.md`, `.ai/doc-api.md`, `.ai/code-standards.md`, `.ai/security.md`, `.ai/ai-workflow-rules.md`, `.ai/progress-tracker.md`

---

## 1. Executive Summary & Objective

Phase 1 established the microservice foundational infrastructure, CI/CD pipeline, Protobuf v3 contract, Dockerfile with `jemalloc`, 12-factor configuration, Prometheus telemetry collectors, and FastAPI probe server.
Phase 2 implemented the native prompt-based judge adapter (`TritonJudgeClient`), outbound judge rate limiter (50 RPS token bucket) & concurrency governor (max 20), circuit breaker (5 failures, 30s reset), native RAG triad metric evaluators (Context Precision, Faithfulness, Answer Relevance), direct micro-batched ClickHouse sink (`ReplacingMergeTree`), and batch evaluation runner with sample-level DLQ fault isolation.

The objective of **Phase 3 (Weeks 5–6)** is to implement end-to-end distributed workflow orchestration, per-sample Temporal heartbeating with zero-recompute resumption, rate-limited egress integration, CI/CD regression gates, and production autoscaling specifications:
1. **Temporal Data Models (`internal/temporal/models.py`)**:
   - `RunEvaluationWorkflowInput`: Tenant ID, run ID, dataset ID, golden set URI, concurrency limit, metric list, judge model.
   - `RunEvaluationWorkflowOutput`: Aggregate scores, sample counts, duration, and boolean `passed_quality_gates`.
   - `EvaluateGoldenSetInput`: Input payload supporting either `samples: list[GoldenSampleItem]` or `golden_set_uri: str | None` (preserving workflow determinism and preventing Temporal history bloat).
   - `EvaluateGoldenSetResult`: Activity summary including evaluated and failed sample counts, score distributions, and duration.
   - `EvaluationHeartbeatDetails`: Checkpoint payload serialized to Temporal heartbeats (`tenant_id`, `run_id`, `current_sample_idx`, `total_samples`, `current_sample_id`, `completed_samples`, `completed_sample_indices: list[int]`, `last_checkpoint_timestamp_utc`).
2. **Temporal Activity Runner with Out-of-Order Resumption & Dual Heartbeating (`internal/temporal/activities.py`)**:
   - Activity definition `@activity.defn(name="EvaluateGoldenSetActivity")` polling `eval-task-queue`.
   - **Zero-Recompute Resumption (Out-of-Order Concurrency Safe)**:
     - On startup, queries `activity.get_heartbeat_details()`.
     - Deserializes `EvaluationHeartbeatDetails`.
     - Filters samples using set-difference: `remaining_samples = [s for s in samples if s.sample_idx not in set(checkpoint.completed_sample_indices)]`. This guarantees zero skipped samples when concurrent evaluations complete non-sequentially.
   - **Dual-Trigger Heartbeating**:
     - Background ticker emits `activity.heartbeat(...)` periodically every `ACTIVITY_HEARTBEAT_INTERVAL_SECONDS = 10.0` to prevent activity timeouts during long downstream judge backoff/queuing.
     - Per-sample progress callback updates the checkpoint state and emits immediate heartbeats upon sample completion.
     - Activity timeout: `heartbeat_timeout = 30.0s`.
   - **Sample-Level DLQ Fault Isolation**:
     - Poison pills (malformed queries, context truncation, persistent downstream timeouts) are quarantined into the DLQ, incrementing `failed_samples_count`.
     - The activity NEVER fails on individual sample failures. Only non-retryable platform faults (unrecoverable datastore drop, activity cancellation) raise `ApplicationError`.
   - **Cancellation & Micro-Batch Flush**:
     - Gracefully catches `activity.CancelledError`, cancels pending sample tasks, flushes buffered ClickHouse metric points, and re-raises.
3. **Temporal Evaluation Workflow Orchestration (`internal/temporal/workflows.py`)**:
   - Workflow definition `@workflow.defn(name="BatchEvaluationWorkflow")`.
   - Deterministic workflow execution:
     - Schedules `EvaluateGoldenSetActivity` with `start_to_close_timeout = 600s` and `heartbeat_timeout = 30s`.
     - Applies retry policy for transient worker restarts (`initial_interval=2s, maximum_attempts=3, non_retryable_error_types=["ApplicationError"]`).
     - Enforces quality gate evaluations ($\tau \ge 0.82$) on active metrics (`eval_metrics`) and `failed_samples_count == 0`.
     - Emits final workflow summary output (`RunEvaluationWorkflowOutput`).
4. **Resilient Temporal Worker Daemon & Lifecycle Integration (`internal/temporal/worker.py` & `cmd/worker/main.py`)**:
   - Asynchronous Temporal worker listening on `eval-task-queue`.
   - Non-fatal connection retry loop preventing worker crash loops when Temporal cluster is temporarily unreachable during startup.
   - Dynamic synchronization with `ReadinessManager` updating `/ready` probe (`temporal="polling"` when active, `"unreachable"` while reconnecting), preserving `/healthz` liveness.
5. **CI/CD Golden-Set Regression Gate (`scripts/trigger_regression_gate.py`, `test/fixtures/golden_set.json` & `.github/workflows/ci.yml`)**:
   - Golden-set fixture `test/fixtures/golden_set.json` with realistic query-context-answer triplets.
   - Standalone CLI/CI trigger script initiating golden-set evaluations, asserting quality gates ($\tau \ge 0.82$), with `--mock-judge` flag for offline CI environments.
   - Non-zero exit code on quality gate failure for automated pull-request blocking.
   - Update `.github/workflows/ci.yml` `golden-set-eval-gate` step to run `python scripts/trigger_regression_gate.py --golden-set test/fixtures/golden_set.json --mock-judge`.
6. **Kubernetes Deployment & KEDA Autoscaler (`deployments/helm/`)**:
   - `deployments/helm/values.yaml`: CPU/Memory sizing aligned with c6i.2xlarge compute-optimized nodes (`2,000m / 4,000m` CPU, `2.0 GiB / 3.5 GiB` RAM, `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`).
   - `deployments/helm/templates/scaledobject.yaml`: Standard KEDA `ScaledObject` with `type: prometheus` scaler driven by Temporal activity queue backlog or poller metrics (min 1, max 8 replicas).
7. **Automated Unit & Integration Test Suites (`test/unit/`)**:
   - Activity checkpoint restoration and out-of-order zero-recompute resumption tests.
   - Heartbeat interval and timeout compliance tests under simulated judge latency.
   - Sample-level DLQ quarantine validation under synthetic judge failures.
   - Workflow execution and quality gate assertion tests (including partial metric runs).
   - Regression gate CLI execution tests with mock judge fixtures.
8. **Continuous Doc API & Progress Tracker Maintenance**:
   - Continuous synchronization of `.ai/doc-api.md` and `.ai/progress-tracker.md`.

---

## 2. Invariants & Architectural Alignment

- **ADR-001 (Decoupled Microservice Compute)**: Stream sanitization is isolated to `rag-masking-sanitizer`. `rag-eval-worker` operates purely on batch evaluation.
- **ADR-002 (Native Lean Evaluation)**: No Ragas/TruLens runtimes. Direct prompt evaluations against Triton/vLLM endpoints.
- **ADR-003 (ClickHouse Unified Persistence)**: Micro-batched inserts directly into `evaluation_runs` and `eval_metric_points` `ReplacingMergeTree` tables. Read-time deduplication via `FINAL`.
- **Temporal Invariants**:
  - Heartbeat every 10 seconds per evaluated sample + periodic ticker to prevent timeouts during downstream latency.
  - Zero duplicate rows in ClickHouse despite at-least-once activity retry, guaranteed by `ReplacingMergeTree` primary keys `(tenant_id, run_id, metric_name, sample_idx)`.
- **Secured Structured Logging**:
  - Redaction of raw prompt texts, retrieved contexts, and generated answers.
  - Truncated 16-character SHA-256 reasoning hashes (`sha256(reasoning)[:16]`) in logs.
- **Git Commit Standards**:
  - Conventional commit messages (`feat:`, `fix:`, `tests:`, `docs:`, `chore:`).
  - At least two git commits for the phase.

---

## 3. Detailed Component Implementation Plan

### A. Temporal Data Models (`internal/temporal/models.py`)
- Define Pydantic models aligning strictly with `.ai/doc-api.md`:
  - `RunEvaluationWorkflowInput`: `tenant_id`, `run_id`, `dataset_id`, `golden_set_s3_uri`, `concurrency_limit=20`, `eval_metrics`, `judge_model`.
  - `RunEvaluationWorkflowOutput`: `tenant_id`, `run_id`, `total_samples`, `evaluated_samples`, `failed_samples_count`, `avg_faithfulness`, `avg_context_precision`, `avg_answer_relevance`, `duration_seconds`, `passed_quality_gates`.
  - `EvaluateGoldenSetInput`: `tenant_id`, `run_id`, `dataset_id = "golden-set"`, `samples: list[GoldenSampleItem] = []`, `golden_set_uri: str | None = None`, `concurrency_limit=20`, `eval_metrics`, `pipeline_version = "v1.0.0"`.
  - `EvaluateGoldenSetResult`: `tenant_id`, `run_id`, `dataset_id`, `total_samples`, `evaluated_samples_count`, `failed_samples_count`, `avg_faithfulness`, `avg_context_precision`, `avg_answer_relevance`, `passed_quality_gates`, `duration_seconds`, `dlq_samples`.
  - `EvaluationHeartbeatDetails`: `tenant_id`, `run_id`, `current_sample_idx`, `total_samples`, `current_sample_id`, `completed_samples`, `completed_sample_indices: list[int] = []`, `last_checkpoint_timestamp_utc`.
  - Re-export and validate against `internal.evaluation.models`.

### B. Temporal Activity Runner (`internal/temporal/activities.py`)
- `EvaluateGoldenSetActivity`:
  - Injected with dependencies: `BatchEvaluationRunner`, `Settings`.
  - Dataset resolution:
    - If `activity_input.golden_set_uri` is supplied, load and parse golden set JSON file inside the activity.
    - Otherwise, use `activity_input.samples`.
  - Resumption logic (concurrency safe):
    ```python
    details = activity.get_heartbeat_details()
    completed_indices: set[int] = set()
    if details:
        checkpoint = EvaluationHeartbeatDetails(**details)
        completed_indices = set(checkpoint.completed_sample_indices)
    remaining_samples = [s for s in all_samples if s.sample_idx not in completed_indices]
    ```
  - Dual-Trigger Heartbeating:
    - Maintain background heartbeat ticker task emitting `activity.heartbeat(...)` every 10 seconds.
    - `on_sample_complete` callback updates `completed_indices` and triggers checkpoint heartbeat.
  - Catches `activity.CancelledError` gracefully, cancels sample tasks, flushes ClickHouse sink buffer, and re-raises.
  - Returns `EvaluateGoldenSetResult`.

### C. Temporal Workflow (`internal/temporal/workflows.py`)
- `BatchEvaluationWorkflow`:
  - Decorated with `@workflow.defn(name="BatchEvaluationWorkflow")`.
  - Deterministic execution without non-deterministic I/O:
    - Maps `workflow_input` to `EvaluateGoldenSetInput(golden_set_uri=workflow_input.golden_set_s3_uri, ...)` so the activity handles file retrieval.
    - Executes activity `EvaluateGoldenSetActivity` with:
      - `start_to_close_timeout = timedelta(seconds=600)`
      - `heartbeat_timeout = timedelta(seconds=30)`
      - `retry_policy = RetryPolicy(initial_interval=timedelta(seconds=2), maximum_attempts=3, non_retryable_error_types=["ApplicationError"])`
    - Evaluates release gates scoped strictly to active metrics:
      - Validates each metric in `workflow_input.eval_metrics` meets its threshold ($\tau \ge 0.82$) and `failed_samples_count == 0`.
    - Returns `RunEvaluationWorkflowOutput`.

### D. Resilient Temporal Worker Daemon (`internal/temporal/worker.py`)
- `TemporalWorkerManager`:
  - Manages connection lifecycle to `TEMPORAL_HOST` in namespace `TEMPORAL_NAMESPACE`.
  - Registers `BatchEvaluationWorkflow` and `EvaluateGoldenSetActivity`.
  - Listens on `settings.EVAL_TASK_QUEUE`.
  - Reconnection loop: If connection fails at startup, logs warning, marks `ReadinessManager.set_status("temporal", "unreachable")`, and retries with backoff rather than crashing the process.
  - Once connected, updates `ReadinessManager.set_status("temporal", "polling")`.
  - Exposes `async def run()` and `async def shutdown()`.

### E. Daemon Integration in `cmd/worker/main.py`
- Wire `TemporalWorkerManager` into the `asyncio.TaskGroup` in `serve()`:
  - Task 1: HTTP Probe Server (`uvicorn`).
  - Task 2: Temporal Worker Runner.
- Update `/ready` probe to report real-time status of Temporal worker polling.

### F. Golden Set Fixture & CI/CD Regression Gate (`test/fixtures/golden_set.json` & `scripts/trigger_regression_gate.py`)
- Create `test/fixtures/golden_set.json` with realistic golden evaluation triplets.
- Implement `scripts/trigger_regression_gate.py`:
  - CLI arguments: `--golden-set`, `--endpoint`, `--mock-judge`, `--concurrency`.
  - In `--mock-judge` mode, uses simulated high-accuracy deterministic responses to validate the pipeline without live external LLM services.
  - In standard mode, runs through `BatchEvaluationRunner` with live judge client.
  - Asserts quality thresholds ($\tau \ge 0.82$) and returns non-zero exit code on failure.
- Update `.github/workflows/ci.yml` `golden-set-eval-gate` step to execute:
  `python scripts/trigger_regression_gate.py --golden-set test/fixtures/golden_set.json --mock-judge`

### G. Helm Deployment Blueprint (`deployments/helm/`)
- `deployments/helm/values.yaml`:
  - Pod resource limits: `requests: {cpu: "2000m", memory: "2Gi"}`, `limits: {cpu: "4000m", memory: "3.5Gi"}`.
  - Environment variables: `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`, `TEMPORAL_HOST`, `JUDGE_ENDPOINT`, `CLICKHOUSE_HOST`.
- `deployments/helm/templates/scaledobject.yaml`:
  - KEDA ScaledObject using standard `type: prometheus` trigger querying Temporal activity schedule-to-start latency or task queue backlog metrics.
  - Min replicas: 1, Max replicas: 8.

### H. Comprehensive Test Suite (`test/unit/`)
- `test/unit/test_temporal_models.py`:
  - Serializing and deserializing workflow inputs, activity inputs, and heartbeat details (including `completed_sample_indices`).
- `test/unit/test_temporal_activities.py`:
  - Out-of-order resumption: verifies that completed samples are skipped regardless of completion order upon activity restart.
  - Heartbeat ticker: verifies background heartbeat is emitted during prolonged execution.
  - DLQ fault isolation: verifies poisoned samples are quarantined and activity returns valid result.
- `test/unit/test_temporal_workflows.py`:
  - Workflow execution test with mock activity execution.
  - Scoped quality gate verification: passing when active metrics meet $\ge 0.82$, handling partial metric lists.
- `test/unit/test_regression_gate.py`:
  - Unit test for CLI regression gate script in both passing and breaching threshold scenarios.

---

## 4. Verification & Quality Gates

1. **Linting & Formatting**:
   ```powershell
   python -m ruff check --fix .
   python -m ruff format .
   ```
2. **Type Safety**:
   ```powershell
   python -m mypy cmd internal test
   ```
3. **Automated Unit Tests**:
   ```powershell
   python -m pytest test/unit -v
   ```
4. **Secured Logging Audit**:
   Verify zero PII, golden-set queries, or retrieved contexts appear in test/runner logs.
5. **Continuous Doc API & Progress Tracker Sync**:
   Verify `.ai/doc-api.md` matches all models and update `.ai/progress-tracker.md` to reflect Phase 3 progress.
6. **Git Commit Plan**:
   - Commit 1: `feat(temporal): implement Temporal data models, activities with heartbeating, and workflow orchestration`
   - Commit 2: `feat(worker): integrate Temporal worker daemon into main async TaskGroup and readiness manager`
   - Commit 3: `feat(deployments): add Helm values and KEDA ScaledObject template for Temporal queue backlog`
   - Commit 4: `feat(ci): implement CI/CD golden-set regression gate script, golden set fixture, and workflow integration`
   - Commit 5: `tests(temporal): add comprehensive unit test suite for activities, workflows, resumption, and regression gate`
   - Commit 6: `docs: synchronize doc-api schemas and update progress tracker for Phase 3 completion`

---

## 5. Human-in-the-Loop Confirmation Request

Per `.ai/ai-workflow-rules.md`, please confirm the implementation plan above to begin Phase 3 execution.

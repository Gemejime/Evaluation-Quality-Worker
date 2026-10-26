# Implementation Plan: Phase 4 - Chaos Hardening, 429 Flood Injection, Benchmarking & GitOps ArgoCD

**Microservice**: `rag-eval-governance-worker` (`rag-eval-worker`)  
**Specification References**: `tasks/RAG Evaluation and Governance Worker Plan.pdf` & `GEMINI.MD`  
**Authoritative Context**: `.ai/architecture.md`, `.ai/doc-api.md`, `.ai/code-standards.md`, `.ai/security.md`, `.ai/ai-workflow-rules.md`, `.ai/progress-tracker.md`  
**GitOps Status**: ArgoCD delivery blueprint approved for cluster management.

---

## 1. Executive Summary & Objective

Phase 1 established the microservice foundational infrastructure, CI/CD pipeline, Protobuf v3 contract, Dockerfile with `jemalloc`, 12-factor configuration, Prometheus telemetry collectors, and FastAPI probe server.  
Phase 2 implemented the native prompt-based judge adapter (`TritonJudgeClient`), outbound judge rate limiter (50 RPS token bucket) & concurrency governor (max 20), circuit breaker (5 failures, 30s reset), native RAG triad metric evaluators (Context Precision, Faithfulness, Answer Relevance), direct micro-batched ClickHouse sink (`ReplacingMergeTree`), and batch evaluation runner with sample-level DLQ fault isolation.  
Phase 3 implemented distributed Temporal workflow orchestration (`BatchEvaluationWorkflow`, `EvaluateGoldenSetActivity`), per-sample 10s heartbeating with out-of-order zero-recompute resumption, CI/CD golden-set regression quality gates ($\tau \ge 0.82$), and Kubernetes Helm blueprint with KEDA autoscaler.

The objective of **Phase 4 (Weeks 7–8)** is to execute comprehensive resilience validation, chaos injection, soak profiling, throughput benchmarking, and declarative GitOps delivery:
1. **Batch Evaluation Throughput Benchmark Suite (`scripts/benchmark_throughput.py`)**:
   - Automated benchmark orchestrating golden-set evaluations up to 100 samples with concurrent judge calls capped at 20.
   - Built-in fixture scaling (`--replicate-to 100`) dynamically scaling the 5-sample golden-set fixture to 100 samples without manual duplication.
   - Measures and validates latency quantiles ($P50, P90, P95, P99$) and asserts the critical platform SLO: **total batch turnaround $P95 \le 180$s for 100 samples** under 20 concurrent connections.
   - Outputs structured latency breakdown: judge network round-trip, metric scoring, and ClickHouse micro-batch persistence, serializing to a machine-readable JSON artifact for CI verification.
   - Supports both offline deterministic mock mode (`--mock-judge`, `--mock-latency-mean`) and live Triton/vLLM endpoints.
2. **Judge 429 Flood & Backpressure Chaos Suite (`scripts/simulate_429_chaos.py` & `test/integration/test_chaos_429_flood.py`)**:
   - Injects synthetic HTTP 429 (`Too Many Requests`) and `RESOURCE_EXHAUSTED` storm floods at varying injection ratios (e.g. 20%, 50%, 80%, 100%).
   - Validates that the token-bucket rate limiter and exponential backoff with full jitter throttle in-process without crashing or dropping requests.
   - Asserts the circuit breaker separation invariant: handled 429 retries DO NOT trip the circuit breaker; the circuit breaker trips cleanly strictly upon downstream 5xx/network collapses or when max retries (5) are exhausted.
   - Validates event-loop non-blocking: verifies `/healthz` and `/ready` probes maintain sub-5ms responses during severe 429 floods.
   - Verifies that Temporal activity heartbeats continue firing on the 10-second ticker even during prolonged judge backoff pauses.
3. **12-Hour Soak Testing & Memory Bound Validation Harness (`scripts/soak_test_memory.py` & `test/integration/test_soak_memory.py`)**:
   - Long-running execution harness simulating prolonged production batch evaluation runs (supporting 12-hour continuous soak test or accelerated leak detection).
   - Cross-platform RSS memory profiling using standard library counters (Windows `ctypes.windll.psapi.GetProcessMemoryInfo` and Linux `/proc/self/statm` / `resource.getrusage`) without introducing external dependencies.
   - Formally asserts memory bounds: **RSS $\le 2.0$ GiB** flat memory profile under prolonged high-concurrency loads, validating `jemalloc` preloading efficacy and zero leak accumulation.
   - Differentiates between a long-running daemon (`scripts/soak_test_memory.py`) and a fast, deterministic CI leak regression test (`test/integration/test_soak_memory.py`, 30–50 batch iterations asserting $\Delta \text{RSS} \le 50$ MB).
4. **ClickHouse High-Volume Micro-Batching Chaos & Buffer Resilience (`internal/storage/clickhouse_sink.py` & `test/integration/test_clickhouse_chaos.py`)**:
   - Hardens `ClickHouseSink.flush()` to prevent data loss on transient network disconnects: uncommitted buffer records are re-enqueued if `client.insert()` fails.
   - Floods the storage sink with 10,000 metric records to verify client-side buffering (1,000 records or 1.0s interval) sustains high throughput without detached parts.
   - Verifies `ReplacingMergeTree` deduplication when retrying duplicate batches using `FINAL`.
   - Validates transient network disconnect resilience and graceful buffer flush on worker shutdown or activity cancellation.
5. **Decoupled Masking / Entropy Latency Profiler (`scripts/profile_masking_latency.py`)**:
   - Calibrated strictly to **ADR-001** (Microservice Isolation): avoids bundling heavy NLP runtime dependencies (`spacy`, `presidio-analyzer`) into `rag-eval-worker`, preserving the lightweight container invariant.
   - Profiles Tier 1 compiled regex/pattern matching ($P99 \le 5$ms) and Shannon entropy calculation ($P99 \le 120$ms) across 1,000-token synthetic chunks to benchmark CPU envelope limits, or verifies the client-side transport latency against `rag-masking-sanitizer`.
6. **Declarative GitOps & ArgoCD Progressive Delivery Blueprint (`deployments/helm/` & `deployments/argocd/`)**:
   - Completes the Helm chart foundation in `deployments/helm/`: adds `Chart.yaml`, `templates/service.yaml`, `templates/configmap.yaml`, and `templates/rollout.yaml` so ArgoCD can render and synchronize cleanly.
   - **ArgoCD `Application` Manifest (`deployments/argocd/application.yaml`)**:
     - Declaratively binds the `deployments/helm/` chart to Kubernetes clusters in namespace `rag-evaluation`.
     - Automated synchronization (`automated.prune=true`, `automated.selfHeal=true`), namespace auto-creation, and health status tracking.
   - **Argo Rollouts & Quality Gate Analysis (`deployments/argocd/analysis-template.yaml` & `deployments/argocd/rollout.yaml`)**:
     - Progressive canary deployment blueprint (e.g. 20% canary step).
     - Automated `AnalysisTemplate` hooked directly into the platform release gate ($\tau \ge 0.82$): queries Prometheus `eval_metric_score_value` and verifies `eval_judge_rate_limit_errors_total` stays within budget.
     - Automatically halts and rolls back canary deployments if aggregate evaluation quality drops below threshold ($\tau < 0.82$) or downstream 429 rates spike.
     - Coordinates KEDA `ScaledObject` to scale the `Rollout` resource.
7. **Continuous Doc API & Progress Tracker Maintenance**:
   - Keep `.ai/doc-api.md` synchronized with all new CLI scripts, telemetry alerts, ArgoCD manifests, and test contracts.
   - Update `.ai/progress-tracker.md` to reflect Phase 4 completion and system readiness.
8. **Git Commit Standards**:
   - At least two conventional commits (`feat:`, `tests:`, `docs:`, `chore:`).
   - 100% clean Ruff linting, formatting, mypy typing, and automated test suite.

---

## 2. Invariants & Architectural Alignment

- **SLO Enforcement**: Total 100-sample golden set turnaround $P95 \le 180$s under 20 concurrent judge connections.
- **Memory Invariant**: Flat Resident Set Size (RSS) $\le 2.0$ GiB under prolonged execution using `jemalloc` preloading on Linux.
- **Outbound Backpressure**: Token-bucket 50 RPS, concurrency cap 20, exponential backoff with full jitter, circuit breaker (5 failures, 30s recovery). Handled 429s must never trip circuit breaker.
- **Zero Data Loss**: Micro-batched inserts with idempotent `ReplacingMergeTree` deduplication keyed by `(tenant_id, run_id, metric_name, sample_idx)`. ClickHouse sink buffer must re-enqueue uncommitted rows on network disconnects.
- **ADR-001 Boundary Integrity**: `rag-eval-worker` maintains zero `spacy`/`trulens`/`presidio` runtime bloat; streaming sanitization remains isolated to `rag-masking-sanitizer`.
- **GitOps Invariant**: Declarative cluster state management via ArgoCD consuming `deployments/helm/` without introducing runtime dependencies or circular build-time dependencies into worker code.
- **Quality Gate Invariant**: Platform release gate $\tau \ge 0.82$ enforced in CI/CD and canary promotions (Argo Rollouts Analysis).
- **Secured Logging**: Complete JSON structured logging with redaction of raw queries, contexts, and answers. Truncated 16-character SHA-256 reasoning hashes in logs.

---

## 3. Implementation Units & Steps

### Unit 1: Throughput Benchmark Suite (`scripts/benchmark_throughput.py`)
- CLI options:
  - `--samples`: Target number of samples to evaluate (default: 100).
  - `--replicate-to`: Auto-replicates smaller fixtures (e.g., `test/fixtures/golden_set.json`) to reach the target sample count.
  - `--concurrency`: Outbound judge concurrency cap (default: 20).
  - `--golden-set`: Path to source golden set fixture.
  - `--mock-judge`: Enables deterministic offline mock judge.
  - `--mock-latency-mean`: Configurable mock network latency in seconds (default: 0.2s).
  - `--output-json`: File path to output structured JSON metrics report.
- Measures & Reports:
  - Total elapsed wall-clock turnaround time (asserting $\le 180$s for 100 samples).
  - Per-sample evaluation latency quantiles ($P50, P90, P95, P99$).
  - Evaluated samples per second throughput.
  - Latency breakdown: judge network I/O, metric scoring, and ClickHouse micro-batch buffering.
  - Enforces process exit code `1` if batch turnaround $P95 > 180$s for 100 samples.

### Unit 2: Judge 429 Flood & Backpressure Chaos Suite
- `scripts/simulate_429_chaos.py`:
  - Standalone chaos simulation injecting configurable 429 error ratios (e.g. 20%, 50%, 80%, 100%).
  - Tracks backoff progression, jittered wait times, retry attempts, and active gauges.
- `test/integration/test_chaos_429_flood.py`:
  - Unit/integration pytest suite validating:
    - 429 burst does not fail the activity (retries succeed within 5 attempts).
    - Rate limiter throttles concurrency without deadlocks or token leakage.
    - Probe server `/healthz` and `/ready` respond in $< 5$ms in background tasks during active 429 backoff storms.
    - Circuit breaker remains CLOSED during 429s and only trips on consecutive downstream 500/503/timeout bursts or exhausted retries.
    - Temporal activity background ticker continues emitting heartbeats every 10s even while judge calls are paused in exponential backoff.

### Unit 3: Soak Testing & Cross-Platform Memory Stability Harness
- `scripts/soak_test_memory.py`:
  - Continuous evaluation harness with configurable iterations or duration (`--duration-hours 12`, `--iterations 500`).
  - Standard library memory sampler:
    - Windows: `ctypes.windll.psapi.GetProcessMemoryInfo`
    - Linux: `/proc/self/statm` and `resource.getrusage`
  - Asserts delta RSS after warm-up $\le 100$ MB and total RSS $< 2.0$ GiB across multiple consecutive batch executions.
- `test/integration/test_soak_memory.py`:
  - Pytest validating memory stability across repetitive sample evaluations (30–50 batch iterations in CI, asserting $\Delta \text{RSS} \le 50$ MB).

### Unit 4: ClickHouse High-Volume Sink Resilience & Buffer Hardening
- Hardening `internal/storage/clickhouse_sink.py`:
  - Wrap `_insert_batch` in `try...except`; on failure, safely prepend uncommitted rows back to `self._buffer` before raising, preventing data loss on transient network disconnects.
- `test/integration/test_clickhouse_chaos.py`:
  - 10,000 record flood test verifying micro-batching flush interval (1.0s / 1,000 records).
  - Simulates socket disconnect during insert and verifies retry and error propagation without lost records.
  - Verifies read deduplication with `FINAL` under repeated batch retries.
  - Verifies graceful buffer flush on worker shutdown or activity cancellation.

### Unit 5: Decoupled Masking / Entropy Latency Profiler
- `scripts/profile_masking_latency.py`:
  - Respects ADR-001 by avoiding heavy NLP package imports (`spacy`/`presidio-analyzer`).
  - Profiles Tier 1 regex/pattern scanning ($P99 \le 5$ms) and Shannon entropy calculation ($P99 \le 120$ms) across 1,000-token synthetic chunks to validate CPU baseline execution budgets.
  - Exposes CLI `--tokens`, `--chunks`, and `--output-json`.

### Unit 6: Helm Chart Foundation & GitOps ArgoCD Delivery Blueprint
- Complete Helm Chart in `deployments/helm/`:
  - `deployments/helm/Chart.yaml`: Helm chart metadata (`name: rag-eval-governance-worker`, `version: 0.1.0`).
  - `deployments/helm/templates/service.yaml`: ClusterIP service exposing ports 8080 (HTTP probe/metrics) and 50051 (gRPC).
  - `deployments/helm/templates/configmap.yaml`: ConfigMap for environment variables.
  - `deployments/helm/templates/deployment.yaml` / `rollout.yaml`: Deployable workload definition supporting Argo Rollout canary steps.
  - Update `deployments/helm/templates/scaledobject.yaml`: KEDA autoscaler configured to scale the target workload (`Rollout` or `Deployment`).
- Declarative ArgoCD Manifests in `deployments/argocd/`:
  - `deployments/argocd/application.yaml`:
    - ArgoCD `Application` specification mapping Git repo path `deployments/helm` to cluster namespace `rag-evaluation`.
    - Automated synchronization policy (`prune: true`, `selfHeal: true`, `CreateNamespace=true`).
  - `deployments/argocd/analysis-template.yaml`:
    - Argo Rollouts `AnalysisTemplate` querying Prometheus metric `eval_metric_score_value` to assert quality release gate ($\tau \ge 0.82$) and verifying `rate(eval_judge_rate_limit_errors_total[5m]) == 0`.
  - `deployments/argocd/rollout.yaml`:
    - Argo Rollout definition for `rag-eval-governance-worker` with canary deployment steps (e.g. 20% step, 5m pause) and automated rollback guard referencing `AnalysisTemplate`.

### Unit 7: Documentation, Progress Tracker & Verification
- Update `.ai/doc-api.md` with:
  - Throughput benchmark CLI and JSON schema.
  - Chaos scripts and integration test contracts.
  - Soak testing parameters and memory sampling endpoints.
  - ArgoCD and Argo Rollouts manifests and parameters.
- Update `.ai/progress-tracker.md` to mark Phase 4 complete.
- Run full verification checklist:
  - `python -m ruff check --fix .`
  - `python -m ruff format .`
  - `python -m mypy cmd internal test scripts`
  - `python -m pytest test/`
- Execute multiple conventional Git commits (`feat:`, `tests:`, `docs:`, `chore:`).

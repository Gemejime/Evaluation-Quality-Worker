# GEMINI Master Directive - RAG Evaluation & Governance Worker

You are working on **`rag-eval-governance-worker`** (Evaluation & Quality Worker), a high-reliability Python compute microservice that implements the batch evaluation worker (`rag-eval-worker`). It executes offline and asynchronous golden-set evaluation pipelines (Context Precision, Faithfulness, Answer Relevance), governs judge concurrency with token-bucket governors, micro-batches evaluation records directly into ClickHouse, and isolates ingestion-time masking to the discrete `rag-masking-sanitizer` microservice repository (ADR-001).

---

## 1. Authoritative Context Hierarchy

Before implementing any code or making architectural changes, you **MUST** consult and follow the documentation files in the [`.ai/`](.ai) directory. To eliminate ambiguity across onboarding guides, the authoritative sequence (governed by [`.ai.md`](.ai.md)) is:

1. [`.ai/project-overview.md`](.ai/project-overview.md) — Strategic goals, decoupled compute architecture (`rag-masking-sanitizer` stream vs. `rag-eval-worker` batch), and calibrated SLIs/SLOs.
2. [`.ai/architecture.md`](.ai/architecture.md) — 5-layer backend architecture (aligned with Production Grade Backend Concepts), system stack, directory structure, memory management (`jemalloc`), and Architectural Decision Records (ADR-001 through ADR-004).
3. [`.ai/doc-api.md`](.ai/doc-api.md) — **Microservice Documented API ("Doc API")**. Specifies OpenAPI 3.1 endpoints (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`), health probes (`/healthz`, `/ready`), Temporal activity/workflow contracts (`eval-task-queue`), ClickHouse schemas (`evaluation_runs`, `eval_metric_points`), and Protobuf v3 contracts.
4. [`.ai/code-standards.md`](.ai/code-standards.md) — Python 3.12+ conventions, Ruff linting/formatting/isort, mypy strict typing, rate limiting, and ClickHouse micro-batching rules.
5. [`.ai/security.md`](.ai/security.md) — Threat model, trust boundaries, KMS-salted HMAC token hashing, container hardening, and Secured Logging invariants.
6. [`.ai/ai-workflow-rules.md`](.ai/ai-workflow-rules.md) — Spec-driven workflow, pre-commit checklist, continuous Doc API sync, and scoping guards.
7. [`.ai/agents.md`](.ai/agents.md) — Core AI Agent Persona & Execution Protocol. Defines engineering identity, responsibilities, and procedural standards.
8. [`.ai/progress-tracker.md`](.ai/progress-tracker.md) — 4-phase milestone tracker, completed units, ADRs, and operational runbooks.

---

## 2. Non-Negotiable Operational Invariants

### A. Continuous Doc API Updates via OpenAPI
- **MANDATORY**: The [`.ai/doc-api.md`](.ai/doc-api.md) file and OpenAPI 3.1 endpoints (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`) MUST be kept updated on **every code change or addition**, no matter how little the created or modified code is.
- Any change to models, activity signatures, gRPC proto contracts, endpoints, or ClickHouse table schemas must be immediately reflected in [`.ai/doc-api.md`](.ai/doc-api.md).

### B. Secured Logging
- **MANDATORY**: All logs emitted must be structured JSON.
- **STRICT REDACTION**: Customer Personally Identifiable Information (PII), Protected Health Information (PHI), raw golden-set prompt texts, retrieved document context passages, generated answers, authorization headers, presigned URLs, and KMS salts must be strictly redacted prior to log emission.
- Diagnostic logs must only output truncated hashes (`reasoning_hash = sha256(reasoning)[:16]`), token counts, scores, and execution latencies (full 64-character SHA-256 hash is preserved in ClickHouse for audit trails).

### C. Standardized Lean Evaluation Engine (ADR-002)
- Execute native, version-controlled prompt templates for Context Precision, Faithfulness/Groundedness, and Answer Relevance directly against Triton/vLLM endpoints (`rag-inference-serving`), with optional dynamic template retrieval from `rag-prompt-registry-service`.
- Heavy external evaluation frameworks (Ragas, TruLens) are strictly excluded from runtime dependencies to eliminate package conflicts and unbounded heap growth.

### D. Direct ClickHouse Micro-Batching & Idempotent State (ADR-003)
- Persist evaluation run metadata (`evaluation_runs`) and point-in-time sample metrics (`eval_metric_points`) directly into ClickHouse using `ReplacingMergeTree` tables and client-side async buffering (1,000 records or 1.0s flush intervals).
- **Read-Time Deduplication Contract**: `ReplacingMergeTree` performs deduplication asynchronously in background merges. All metric evaluators, quality gates, and downstream consumers MUST query using `FINAL` or an explicit version-aware query keyed by `(tenant_id, run_id, metric_name, sample_idx)` selecting the latest row by `created_at` (and `(tenant_id, run_id)` selecting latest `attempt`). All consumers are required to follow this contract to prevent processing unmerged duplicates.
- Zero transactional dual-writes to relational databases.

### E. Temporal Heartbeat Reliability & Resumption
- Temporal activity tasks polling `eval-task-queue` must emit heartbeats with the current `sample_idx` every 10 seconds.
- On worker pod restart or preemption, the worker must inspect `activity.get_heartbeat_details()` and resume from the last acknowledged checkpoint. Because a crash before heartbeat delivery may repeat sample $N$, judge execution follows at-least-once semantics; deduplication and idempotency are enforced at the ClickHouse persistence boundary via `(tenant_id, run_id, metric_name, sample_idx)` to guarantee zero duplicate persisted records.

### F. Outbound Judge Rate Limiting, Backpressure & Circuit Breaking
- Outbound judge calls must pass through a token-bucket rate limiter (50 RPS) and concurrency semaphores (capped at 20 concurrent requests).
- Apply exponential backoff with full jitter on HTTP 429 / `RESOURCE_EXHAUSTED` responses.
- Implement an outbound circuit breaker (tripping after 5 consecutive downstream judge failures, 30s recovery window) to fail gracefully without crashing active workers.

### G. Fault Isolation & Sample-Level DLQ
- Per-sample parsing errors or unrecoverable judge timeouts must be quarantined into a dead-letter queue (DLQ) and increment `failed_samples_count`.
- Poison-pill samples must NEVER crash the entire batch evaluation activity; the activity continues processing remaining valid samples.

### H. Memory Bounds
- Flat Resident Set Size (RSS) $\le 2.0$ GiB per eval worker under prolonged execution using `jemalloc` preloading (`LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`).

---

## 3. Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup

---

## 4. Pre-Commit Verification Checklist

Before committing code, submitting changes, or completing any phase:

1. **Lint & Format**: Run `python -m ruff check --fix .` and `python -m ruff format .`.
2. **Type Checking**: Verify with `mypy cmd internal test`.
3. **Tests**: Ensure tests pass in `test/unit/` and `test/integration/`.
4. **Secured Logging Audit**: Confirm no sensitive text, prompt spans, or credentials leak in logs.
5. **Doc API & OpenAPI Check**: Confirm [`.ai/doc-api.md`](.ai/doc-api.md) is fully synchronized with any code or schema adjustments.
6. **Progress Tracker**: Update [`.ai/progress-tracker.md`](.ai/progress-tracker.md).

# Code Standards - RAG Evaluation & Governance Worker

## General Principles

- **Single Responsibility**: Decouple batch evaluation orchestration (Temporal) from LLM judge invocation and ClickHouse micro-batching.
- **Fail-Fast on Corrupt Input**: Distinguish batch-level from per-sample failures: only invalid batch schemas or configuration escape the activity as non-retryable exceptions (`ApplicationError`), while malformed individual samples must enter the Dead-Letter Queue (DLQ) path and allow the activity to continue processing the remaining valid samples.
- **Root-Cause Focus**: Never patch race conditions or judge 429 throttling with arbitrary sleep statements; use disciplined token-bucket governors with exponential jitter backoff.

---

## Python & Code Quality Standards

- **Runtime Target**: Python 3.12+ (64-bit).
- **Linter & Formatter**: **Ruff** is the authoritative linter, formatter, and import sorter:
  - Line length: `100` characters.
  - Formatter: `python -m ruff format .`
  - Linter & Import Sorter: `python -m ruff check --fix .`
  - Imports: Grouped into standard library, third-party, and first-party (`internal`, `cmd`, `api`).
- **Static Typing**: Strict typing with **mypy**:
  - No untyped `def` functions (`disallow_untyped_defs = true`).
  - Avoid `typing.Any` wherever possible; use explicit Pydantic models or typed Protobuf structures.
  - Model incoming evaluation jobs, sample records, and judge outputs via Pydantic `BaseModel`.

---

## Concurrency, Rate Limiting & Heartbeat Invariants

1. **Judge Concurrency Governor & Circuit Breaker**:
   - Outbound judge requests to Triton/vLLM must pass through client-side concurrency semaphores (`JUDGE_MAX_CONCURRENT_REQUESTS=20`).
   - Token bucket algorithm controls requests per second (`JUDGE_RATE_LIMIT_RPS=50.0`).
   - On HTTP 429 / `RESOURCE_EXHAUSTED`, apply exponential backoff with full jitter (`JUDGE_BACKOFF_BASE_SECONDS=1.0`, `JUDGE_BACKOFF_MAX_SECONDS=30.0`).
   - Outbound circuit breaker trips after 5 consecutive downstream failures, halting requests for 30s to allow downstream serving recovery without thrashing worker threads.
2. **Heartbeat Continuity & Checkpointing**:
   - Temporal activity workers must emit heartbeats with current `sample_idx` every 10 seconds.
   - On worker preemption or pod restart, `activity.get_heartbeat_details()` is inspected to resume at the last checkpoint; judge calls follow at-least-once semantics, while ClickHouse composite key deduplication guarantees 0 duplicate persisted rows.
3. **Memory Management**:
   - `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so` must be enabled in container deployments to prevent heap fragmentation during high-volume string manipulations.

---

## ClickHouse Micro-Batching & Idempotency Invariants

1. **Client-Side Buffering**:
   - Write metric records to ClickHouse via client-side async buffer flushing at 1,000 records or 1.0s flush intervals.
   - Raw single-row inserts are strictly prohibited to avoid creating excessive detached parts in ClickHouse.
2. **Unified Persistence & Deduplication (ADR-003)**:
   - Relational PostgreSQL run tracking is removed. All run metadata (`evaluation_runs`) and sample-level metric points (`eval_metric_points`) reside in ClickHouse `ReplacingMergeTree` tables.
   - `eval_metric_points` uses `ReplacingMergeTree(created_at)` ordered by `(tenant_id, run_id, metric_name, sample_idx)` to guarantee idempotent re-runs; read-time consumers must query with `FINAL` or equivalent version-aware selection.

---

## Secured Logging Invariants

- **Structured JSON Logging**: All logs must be emitted as machine-parseable JSON lines (using `structlog` or standard logging configured with JSON formatters).
- **Mandatory Correlation IDs**: Every log message must include:
  - `tenant_id`
  - `run_id`
  - `sample_idx`
  - `metric_name`
  - `judge_model`
- **Security Invariant (Secured Logging)**:
  - **Strict Redaction**: Never log raw golden-set prompt texts, retrieved document context spans, generated answers, PII/PHI, KMS salts, API tokens, or bearer headers.
  - Emit only lengths, truncated hashes (`reasoning_hash = sha256(reasoning)[:16]`), or scores (the full 64-character SHA-256 hex digest is retained strictly in ClickHouse storage for forensic auditing).
  - Error logs must sanitize internal system paths and database connection credentials.

---

## Doc API Mandatory Maintenance

- **Continuous Synchronization**: Whenever any model, activity signature, gRPC proto, or storage path is created or altered, [`.ai/doc-api.md`](doc-api.md) and OpenAPI 3.1 endpoints **MUST** be updated immediately in the same change step, no matter how small the code modification is.

---

## Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup

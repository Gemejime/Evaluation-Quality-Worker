# Implementation Plan: Phase 2 - Lean Eval Runner & Direct ClickHouse Sink

**Microservice**: `rag-eval-governance-worker` (`rag-eval-worker`)  
**Specification References**: `tasks/RAG Evaluation and Governance Worker Plan.pdf` & `GEMINI.MD`  
**Authoritative Context**: `.ai/architecture.md`, `.ai/doc-api.md`, `.ai/code-standards.md`, `.ai/security.md`, `.ai/ai-workflow-rules.md`, `.ai/progress-tracker.md`

---

## 1. Executive Summary & Objective

Phase 1 established the microservice foundational infrastructure, CI/CD pipeline, Protobuf v3 contract, Dockerfile with `jemalloc`, 12-factor configuration, Prometheus telemetry collectors, and FastAPI probe server.

The objective of **Phase 2 (Weeks 3–4)** is to implement the core evaluation compute engine and storage sink with calibrated production safeguards:
1. **Native Prompt-Based Judge Adapter (`internal/evaluation/judges/`)**:
   - Abstract `BaseJudgeClient` defining the judge protocol.
   - Native `TritonJudgeClient` using `httpx.AsyncClient` interacting with Triton/vLLM endpoints (`rag-inference-serving`), with structured JSON schema outputs (`response_format={"type": "json_object"}`) and connection pooling (`max_connections=20`).
2. **Outbound Judge Rate Limiter & Concurrency Governor (`internal/evaluation/ratelimiter.py`)**:
   - Client-side token-bucket rate limiter enforcing 50 RPS (`JUDGE_RATE_LIMIT_RPS=50.0`) guarded by `asyncio.Lock` to eliminate token stampedes.
   - Concurrency semaphores capping parallel in-flight judge calls to 20 (`JUDGE_MAX_CONCURRENT_REQUESTS=20`).
   - Exponential backoff with full jitter on HTTP 429 / `RESOURCE_EXHAUSTED` (`JUDGE_429_MAX_RETRIES=5`, `1.0s` to `30.0s`).
   - Outbound Circuit Breaker tripping after 5 consecutive downstream judge failures (exhausted retries or 5xx/connection errors) with a 30s recovery window. Throttled 429s are handled via jittered backoff and decoupled from circuit trip counters.
3. **Core RAG Triad Metric Evaluators (`internal/evaluation/metrics/`)**:
   - Native prompt-based evaluators without third-party evaluation library bloat (ADR-002: Ragas/TruLens strictly excluded).
   - **Context Precision**: Single-prompt multi-chunk evaluation returning binary relevance verdicts $v_k \in \{0, 1\}$ over all retrieved chunks in one JSON response; precision@k formula with explicit zero-relevant-chunks contract ($0.0$).
   - **Faithfulness / Groundedness**: Single-prompt atomic claim extraction and factual verification against retrieved contexts; ratio formula with zero-extracted-claims contract ($1.0$).
   - **Answer Relevance**: Query-to-answer semantic relevance scoring ($0.0$ to $1.0$).
4. **Direct Micro-Batched ClickHouse Persistence Sink (`internal/storage/clickhouse_sink.py`)**:
   - Direct persistence to `ReplacingMergeTree` tables (`evaluation_runs` and `eval_metric_points`) without relational database dual-writes (ADR-003).
   - Non-blocking I/O: synchronous `clickhouse-connect` driver operations dispatched to thread pools via `asyncio.to_thread` to protect the ASGI event loop and health probes.
   - Client-side asynchronous micro-batch buffering (1,000 records or 1.0s flush interval).
   - Defensive 64-character SHA-256 hexadecimal digest generation (`FixedString(64)`) ensuring zero database type validation failures.
   - Read deduplication helper `get_run_metrics(...)` enforcing `FINAL` query semantics.
5. **Batch Evaluation Runner (`internal/evaluation/runner.py`)**:
   - Async coordinator executing evaluation batches across selected metrics.
   - Sample-level Dead-Letter Queue (DLQ) fault isolation: malformed triplets or unrecoverable sample errors increment `failed_samples_count` without crashing the batch.
   - Forward-compatible progress hook `on_sample_complete` for seamless Phase 3 Temporal heartbeat resumption.
   - Secured Logging: Strict redaction of raw prompt texts, context passages, and generated answers; emitting only truncated 16-character reasoning hashes (`sha256(reasoning)[:16]`), token counts, and execution latencies.
6. **Automated Unit & Integration Test Suites**:
   - Comprehensive tests validating metric formulas, edge cases, rate limiter/circuit breaker state transitions, micro-batch buffer flushing, and runner DLQ quarantine.
7. **Continuous Doc API & OpenAPI Maintenance**:
   - Continuous synchronization of `.ai/doc-api.md` and `.ai/progress-tracker.md`.

---

## 2. Invariants & Architectural Alignment

- **ADR-001 (Decoupled Compute)**: Zero stream masking or tokenization logic in this repository (isolated to `rag-masking-sanitizer`).
- **ADR-002 (Lean Evaluation Engine)**: Native, version-controlled prompt templates directly targeting Triton/vLLM endpoints. Zero external evaluation runtime dependencies (Ragas, TruLens).
- **ADR-003 (ClickHouse Unified Persistence)**: Direct micro-batch insertion into `evaluation_runs` and `eval_metric_points` `ReplacingMergeTree` tables. Zero PostgreSQL dual-writes. Readers query with `FINAL` or explicit version-aware filters.
- **Secured Logging Invariant**: Structured JSON logging. Raw prompt texts, context passages, generated answers, PII/PHI, and secrets are strictly redacted. Only 16-char truncated reasoning hashes (`sha256(reasoning)[:16]`), scores, and latencies are logged.
- **Outbound Judge Governance**: Concurrency semaphore (max 20), lock-protected token bucket (50 RPS), exponential backoff with full jitter on 429, circuit breaker (5 failures, 30s reset).
- **Sample-Level DLQ**: Poison-pill samples never crash the batch activity.
- **Git Commit Standards**: Conventional commits (`feat:`, `fix:`, `tests:`, `docs:`, `chore:`) with at least two distinct commits.

---

## 3. Detailed Component Specifications

### A. Judge Client Interface & Implementation (`internal/evaluation/judges/`)
- `base.py`:
  - `BaseJudgeClient` abstract base class defining `async def generate(self, prompt: str, system_prompt: str | None = None, response_schema: type[BaseModel] | None = None) -> JudgeResponse`.
  - `JudgeResponse`: Pydantic model containing `content: str`, `parsed_json: dict[str, Any] | None`, `prompt_tokens: int`, `completion_tokens: int`, `latency_ms: int`.
- `triton_client.py`:
  - Native `TritonJudgeClient` using `httpx.AsyncClient` with connection pooling (`limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)`) targeting Triton / vLLM OpenAI-compatible endpoints (`/v1/chat/completions`).
  - Integration with `Settings.JUDGE_ENDPOINT` and `Settings.JUDGE_MODEL`.
  - Structured output enforcement: requests JSON format (`response_format={"type": "json_object"}`).
  - Robust JSON parsing: handles escaped JSON strings, markdown code blocks (````json ... ````), and direct dictionaries.

### B. Outbound Rate Limiter & Concurrency Governor (`internal/evaluation/ratelimiter.py`)
- `OutboundJudgeGovernor`:
  - **Thread-Safe Token Bucket**: Asynchronous token bucket tracking available tokens up to capacity `JUDGE_RATE_LIMIT_RPS` (50.0). Protected by `asyncio.Lock` to eliminate token over-consumption stampedes during concurrent wakeups.
  - **Concurrency Semaphore**: `asyncio.Semaphore(JUDGE_MAX_CONCURRENT_REQUESTS)` (20). Updates `eval_judge_active_concurrency` Prometheus gauge.
  - **Exponential Backoff with Full Jitter**:
    - On HTTP 429 or `RESOURCE_EXHAUSTED`, increments `eval_judge_rate_limit_errors_total(judge_model, error_type="429")`.
    - Backoff delay: $t = \text{random.uniform}(0, \min(t_{\max}, t_{\text{base}} \times 2^{\text{attempt}}))$.
    - Retries up to `JUDGE_429_MAX_RETRIES` (5).
  - **Circuit Breaker with Throttling Decoupling**:
    - States: `CLOSED` (normal), `OPEN` (broken), `HALF_OPEN` (testing recovery).
    - Throttled HTTP 429s handled within the retry loop do NOT count as circuit breaker failures.
    - If downstream failures (exhausted retries, HTTP 5xx, or network socket timeouts) reach `JUDGE_CB_FAILURE_THRESHOLD` (5), transitions to `OPEN`.
    - Holds in `OPEN` for `JUDGE_CB_RESET_TIMEOUT_SECONDS` (30.0s), rejecting outbound requests with `CircuitBreakerOpenError`.
    - After timeout, transitions to `HALF_OPEN` allowing a canary probe; resets to `CLOSED` on success, or reverts to `OPEN` on failure.

### C. Native RAG Triad Metric Evaluators (`internal/evaluation/metrics/`)
- `base.py`:
  - `MetricResult`: `metric_name: str`, `score: float`, `reasoning: str`, `reasoning_hash: str` (64-char hex), `execution_latency_ms: int`.
  - Helper `compute_reasoning_hash(reasoning: str | None) -> str`: Produces a guaranteed 64-char hexadecimal SHA-256 digest (`FixedString(64)` safe).
  - `BaseMetricEvaluator`: Abstract base defining `async def evaluate(...) -> MetricResult`.
- `context_precision.py`:
  - Single-prompt multi-chunk evaluation: sends query and all $K$ context chunks in a single prompt; judge returns binary relevance verdicts $v_k \in \{0, 1\}$ and reasoning for each chunk.
  - Precision@k calculation: $\text{Context Precision@K} = \frac{\sum_{k=1}^K (\text{Precision@}k \times v_k)}{\text{Total Relevant Chunks in Top } K}$.
  - Edge case contract: If total relevant chunks is 0, score is strictly `0.0`.
- `faithfulness.py`:
  - Single-prompt claim extraction and factual verification: judge extracts atomic factual claims from the generated answer and evaluates whether each is directly supported by context snippets in one generation turn.
  - Structured list of claims with boolean `supported` flag and citation index.
  - Formula: $\text{Faithfulness} = \frac{|\text{Supported Claims}|}{|\text{Total Extracted Claims}|}$.
  - Edge case contract: If total extracted claims is 0, score is strictly `1.0`.
- `answer_relevance.py`:
  - Evaluates whether generated answer directly addresses the query without superfluous rambling.
  - Judge scores semantic relevance ($0.0$ to $1.0$) with justification; bounded and clamped to $[0.0, 1.0]$.

### D. Direct Micro-Batched ClickHouse Persistence (`internal/storage/clickhouse_sink.py`)
- `ClickHouseSink`:
  - Uses `clickhouse-connect` driver pool.
  - **Non-Blocking Thread Dispatch**: All synchronous driver commands (`client.command`, `client.insert`) execute via `asyncio.to_thread` to ensure zero event loop blocking.
  - Schema initialization: creates `evaluation_runs` and `eval_metric_points` `ReplacingMergeTree` tables if not existing.
  - Automatic `run_date` derivation from `created_at.date()` or `now(timezone.utc).date()`.
  - **Micro-Batch Buffering**:
    - Flushes when buffer reaches `CLICKHOUSE_BATCH_SIZE` (1,000 records) or `CLICKHOUSE_FLUSH_INTERVAL_SECONDS` (1.0s).
    - `insert_run(...)`: Inserts run metadata into `evaluation_runs`.
    - `buffer_metric_point(...)`: Buffers sample-level metric row into `eval_metric_points`.
    - `flush()`: Flushes all pending buffer rows.
    - `close()`: Flushes buffer and gracefully terminates background timers and client connections.
  - Read query helper `get_run_metrics(...)` enforcing `FINAL` keyword or version-aware deduplication semantics as specified in ADR-003.

### E. Batch Evaluation Runner (`internal/evaluation/runner.py`)
- `BatchEvaluationRunner`:
  - Orchestrates evaluation for a list of samples against requested metrics.
  - Manages judge calls through `OutboundJudgeGovernor`.
  - Coordinates metric evaluation tasks with concurrency control.
  - Stores results via `ClickHouseSink` and executes final `await sink.flush()`.
  - **Sample-Level DLQ**:
    - Wraps sample processing in per-sample exception handler.
    - Malformed samples, schema validation errors, or persistent judge timeouts are logged into a DLQ collection, incrementing `failed_samples_count`.
    - Evaluates remaining valid samples to completion.
  - **Resumption Hook**: Optional callback `on_sample_complete: Callable[[int, GoldenSampleItem, list[MetricResult]], Awaitable[None]] | None = None` for seamless Phase 3 Temporal heartbeat integration.
  - Computes batch aggregate statistics: `avg_faithfulness`, `avg_context_precision`, `avg_answer_relevance`, `passed_quality_gates` ($\ge 0.82$).
  - Updates Prometheus metrics (`eval_batch_duration_seconds`, `eval_sample_execution_latency_seconds`, `eval_metric_score_value`).
  - **Secured Logging**: Strictly outputs JSON logs with correlation IDs (`tenant_id`, `run_id`, `sample_idx`, `metric_name`, `score`), 16-char truncated reasoning hashes (`sha256(reasoning)[:16]`), and latencies. Zero raw prompt or context emission.

### F. Unit & Integration Test Suites (`test/unit/` & `test/integration/`)
- `test/unit/test_metrics.py`:
  - Tests formulas for Context Precision (standard, 0 relevant -> 0.0, all relevant -> 1.0, single-prompt chunk list parsing).
  - Tests formulas for Faithfulness (standard, 0 claims -> 1.0, 0 supported -> 0.0, unified claim verification).
  - Tests Answer Relevance parsing and score bounds $[0.0, 1.0]$.
  - Tests `compute_reasoning_hash` length (strictly 64 hex characters) and determinism.
- `test/unit/test_ratelimiter.py`:
  - Tests token-bucket replenishment, lock synchronization, and consumption.
  - Tests concurrency semaphore capping at 20.
  - Tests 429 exponential backoff with full jitter and verifies it does NOT trip the circuit breaker.
  - Tests circuit breaker trip after 5 consecutive exhausted/5xx failures and half-open reset.
- `test/unit/test_clickhouse_sink.py`:
  - Tests non-blocking thread dispatch via `asyncio.to_thread`.
  - Tests batch size trigger (flushing at 1,000 items).
  - Tests time-based trigger (flushing at 1.0s).
  - Tests payload serialization and 64-char `reasoning_hash` generation.
  - Tests read query with `FINAL` deduplication semantics.
- `test/unit/test_runner.py`:
  - Tests end-to-end batch execution with mock judge.
  - Tests DLQ sample error isolation (bad sample does not crash batch).
  - Tests progress callback hook invocation.
  - Tests secured logging compliance (verifies no raw prompt texts or contexts in captured log output).

---

## 4. Verification & Quality Gates

1. **Linting & Formatting**:
   ```powershell
   python -m ruff check --fix .
   python -m ruff format .
   ```
2. **Type Safety**:
   ```powershell
   python -m mypy --ignore-missing-imports cmd internal test
   ```
3. **Automated Unit Tests**:
   ```powershell
   python -m pytest test/unit -v
   ```
4. **Secured Logging Audit**:
   Verify zero PII, golden-set queries, or retrieved contexts appear in test/runner logs.
5. **Continuous Doc API & Progress Tracker Sync**:
   Verify `.ai/doc-api.md` matches all models and update `.ai/progress-tracker.md` to reflect Phase 2 progress.
6. **Git Commit Plan**:
   - Commit 1: `feat(eval): implement judge client, rate limiter governor, and circuit breaker`
   - Commit 2: `feat(metrics): implement native RAG triad prompt evaluators with precision formulas`
   - Commit 3: `feat(storage): implement non-blocking micro-batched ClickHouse sink with buffer management`
   - Commit 4: `feat(runner): implement async batch eval runner with sample DLQ, progress hooks, and secured logging`
   - Commit 5: `tests: add comprehensive unit test suite for judge, metrics, rate limiter, and sink`
   - Commit 6: `docs: synchronize doc-api schemas and progress tracker for Phase 2 completion`

---

## 5. Human-in-the-Loop Confirmation Request

Per `.ai/ai-workflow-rules.md`, please confirm the implementation plan above to begin Phase 2 code execution.

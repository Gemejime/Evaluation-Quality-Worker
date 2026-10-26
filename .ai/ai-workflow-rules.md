# AI Workflow Rules - RAG Evaluation & Governance Worker

## Development Approach

Build this microservice incrementally following a spec-driven workflow:
- The specification documents in [`tasks/`](tasks) and context files in [`.ai/`](.ai) define system invariants, data structures, and operational boundaries.
- Adhere strictly to the decoupled worker design (`rag-eval-worker` batch evaluation and `rag-masking-sanitizer` stream ingestion) and the 5 backend architectural layers documented in [`.ai/architecture.md`](.ai/architecture.md).
- Implement against these specs without inferring undocumented dependencies or deviating from architectural decision records (ADR-001 through ADR-004).

---

## Human-in-the-Loop Rules

- **Plan First**: Propose an implementation plan or prompt in `prompts/<task-name>.md` before making substantial code changes, and await human confirmation.
- **Ambiguity Escalation**: If judge model endpoints, rate limits, ClickHouse partition strategies, or metric formulas are ambiguous, prompt for clarification before proceeding.
- **Destructive Action Guards**: Always request human confirmation before running destructive operations, table drops, or major schema mutations.
- **Demonstrate & Verify**: Provide clear verification steps, unit test output, and linting checks after completing a unit.

---

## Non-Negotiable Invariants

1. **Continuous Doc API & OpenAPI Maintenance**: The [`.ai/doc-api.md`](doc-api.md) file and OpenAPI 3.1 endpoints (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`) MUST be updated on **every code change or addition**, regardless of how small the modification is.
2. **Secured Logging**: All logs emitted by workers or utilities must be structured JSON. Strictly redact golden-set raw texts, context passages, generated answers, PII/PHI, KMS salts, and authorization tokens before emission.
3. **Decoupled Architecture**: Maintain clean separation between stream sanitization and batch evaluation pipelines.
4. **Standardized Lean Evaluation Engine**: Use native prompt-based evaluation adapters (Context Precision, Faithfulness, Answer Relevance) directly targeting Triton/vLLM. Third-party evaluation frameworks (Ragas, TruLens) are strictly excluded from runtime dependencies.
5. **ClickHouse Unified Persistence**: Write evaluation headers and sample metric points directly to ClickHouse MergeTree tables with client-side micro-batching. Do not introduce relational database dual-writes.
6. **Temporal Heartbeat Reliability**: Batch evaluation activities must emit heartbeats with current sample indices every 10 seconds to ensure seamless resumption upon pod preemption.
7. **Judge Concurrency Governor**: Cap concurrent outbound judge requests at 20, enforcing token-bucket rate limits and exponential backoff with full jitter on HTTP 429.
8. **Git Commit Standards**:
   - **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
   - **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
     - `feat:` for new features 
     - `fix:` for bug fixes
     - `refactor:` for code refactoring
     - `docs:` for documentation updates
     - `tests:` for tests addition and modification
     - `chore:` for maintenance tasks and environment setup
9. **Quality Gates**: Ruff linting, formatting, and import sorting must pass cleanly before any commit.

---

## Pre-Commit Verification Checklist

Before committing code, submitting changes, or marking a task complete:

1. **Lint & Import Sorting**: Run `python -m ruff check --fix .` (zero errors or warnings).
2. **Code Formatting**: Run `python -m ruff format .` (clean formatting verified).
3. **Type Safety**: Verify static types via `mypy cmd internal test`.
4. **Automated Tests**: Ensure unit and integration tests are passing in `test/unit/` and `test/integration/`.
5. **Secured Logging Audit**: Verify no raw PII, golden-set queries, or secret tokens are leaked in logging statements.
6. **Doc API & OpenAPI Sync**: Confirm [`.ai/doc-api.md`](doc-api.md) and OpenAPI schemas reflect any new or modified classes, parameters, endpoints, or storage tables.
7. **Progress Tracker**: Update [`.ai/progress-tracker.md`](progress-tracker.md) with session notes and completed items.

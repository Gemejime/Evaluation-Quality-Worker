# Progress Tracker - RAG Evaluation & Governance Worker

## Current Phase

- **Phase 1: Repository Decoupling & Batch Eval Worker Foundation** (In Progress)

## Current Goal

- Establish microservice foundation for `rag-eval-governance-worker` (implementing `rag-eval-worker`), configure CI/CD workflows, build context invariants, and prepare for lean eval runner and ClickHouse sink implementation (ingestion masking is isolated to `rag-masking-sanitizer` repository per ADR-001).

---

## Calibrated 4-Phase Roadmap & Milestones

| Phase | Timeline | Focus Area | Deliverables & Exit Criteria | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 1** | Weeks 1–2 | Decoupling & Batch Worker Foundation | • `.gitignore` and `.github/workflows/ci.yml` established.<br>• Full `.ai` context and `GEMINI.MD` aligned with backend concepts & ADR-001.<br>• Protobuf v3 contract (`api/proto/v1/eval_worker.proto`) defined.<br>• Packaging and dependency environment configured (`pyproject.toml`, Dockerfile with `jemalloc`).<br>• FastAPI probe server skeleton (`/healthz`, `/ready`, `/docs`, `/metrics`). | 🟡 Active |
| **Phase 2** | Weeks 3–4 | Lean Eval Runner & Direct ClickHouse Sink | • Native prompt-based judge adapter targeting Triton/vLLM endpoints (with fallback to `rag-prompt-registry-service`).<br>• Implementation of core RAG triad metrics with structured JSON schemas (Faithfulness, Context Precision, Answer Relevance).<br>• Direct micro-batched ClickHouse writer with buffer tables (`evaluation_runs`, `eval_metric_points` with `ReplacingMergeTree`). | ⚪ Queued |
| **Phase 3** | Weeks 5–6 | Temporal Task Queues, Rate Limiting & DLQ | • Temporal activity runner (`eval-task-queue`) with per-sample 10s heartbeating and DLQ fault isolation.<br>• Token-bucket rate limiter enforcing outbound judge concurrency caps (max 20) with exponential backoff and circuit breaker.<br>• CI/CD webhook trigger integrating golden-set regression gates ($\tau \ge 0.82$). | ⚪ Queued |
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

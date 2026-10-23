# RAG Evaluation & Governance Worker - Project Overview

## Overview

The **`rag-eval-governance-worker`** (Evaluation & Quality Worker) is a stateless, horizontally autoscaling Python compute microservice operating within the horizontal execution tier of the Multimodal RAG platform.

The system decouples ingestion-time token sanitization from offline evaluation pipelines:
1. **`rag-masking-sanitizer` (Stream Component)**: Real-time chunk PII/PHI tokenization and masking prior to vectorization, using a two-tier scanning engine (Tier 1 compiled Aho-Corasick/Regex fast-path, Tier 2 Presidio/spaCy CPU NLP fallback triggered by entropy $H > \tau$) and KMS/Vault-salted HMAC deterministic token hashing. *(Maintained in a discrete repository with dedicated stream pipeline bindings).*
2. **`rag-eval-worker` (Batch Component — Core of this codebase)**: Asynchronous Temporal Python activity worker consuming evaluation jobs from `eval-task-queue`. It executes lean, native LLM-as-a-judge pipelines for RAG triad metrics (Context Precision, Faithfulness/Groundedness, Answer Relevance) directly against Triton/vLLM endpoints, governs judge concurrency via token-bucket rate limiters, and micro-batches results directly into ClickHouse.

```
[ Ingestion Path ]
Raw Chunks ──► [ rag-masking-sanitizer ] (CPU / Stream) ──► [ rag-inference-serving ] (Triton)
                   │
                   ▼ (Tier 1 Regex/Aho-Corasick ──► Tier 2 High-Entropy NLP ──► KMS-Salted HMAC)

[ Offline Eval Path ]
CI/CD Triggers / Traces ──► [ Temporal Queue: eval-task-queue ] ──► [ rag-eval-worker ]
                                                                          │
                            ┌─────────────────────────────────────────────┴─────────────────────────────────────────────┐
                            ▼                                                                                           ▼
                 Egress Judge Rate Limiter                                                                       ClickHouse Sink
          (Token-Bucket / Concurrency Semaphores)                                                         (Direct Micro-Batched Insert)
                            │                                                                                           │
                            ▼                                                                                           ▼
          [ rag-inference-serving ] / External LLM                                                     evaluation_runs & metric_spans
```

---

## Strategic Objectives

1. **Strict Decoupling of Ingestion from Offline Evaluation**: Isolate real-time chunk tokenization from batch LLM-as-a-judge evaluation pipelines to prevent queue cross-talk, memory contention, and scaling interference.
2. **Deterministic, Reversible Token Sanitization**: Provide tenant-salted HMAC masking for identified sensitive entities prior to vectorization, maintaining embedding semantic integrity with zero salt leaks.
3. **Standardized, Lean Evaluation Engine**: Replace bloated third-party evaluation dependencies (Ragas, TruLens) with a unified, lightweight LLM judge harness executing evaluated prompt templates (Faithfulness, Context Precision, Answer Relevance) directly against Triton/vLLM endpoints.
4. **Zero-Allocation Egress & Direct Micro-Batching**: Persist evaluation run metadata and metric score distributions directly into ClickHouse without transactional dual-writes to relational databases.
5. **Continuous Documentation & Contract Synchronization**: Ensure internal OpenAPI 3.1 specifications (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`) and `.ai/doc-api.md` are continuously synchronized with every code change.

---

## Calibrated Quantitative SLOs & SLIs

- **Batch Evaluation Turnaround (`rag-eval-worker`)**: $P95 \le 180$s for a 100-sample golden-set run with concurrent judge requests capped at 20 parallel requests to protect serving capacity.
- **Temporal Heartbeat Reliability**: 100% of batch evaluation tasks heartbeat every 10 seconds per evaluated sample, enabling zero-recompute resumption upon pod preemption (e.g., resuming at sample 48 after SIGKILL at sample 47).
- **Masking Ingestion Throughput (`rag-masking-sanitizer`)**:
  - Tier 1 (Regex / Aho-Corasick Fast-Path): $P99 \le 5$ms per 1,000-token chunk.
  - Tier 2 (Presidio / spaCy CPU Fallback): $P99 \le 120$ms per 1,000-token chunk on standard `c6i.2xlarge` instances.
- **Memory Bounds**: Flat Resident Set Size (RSS) $\le 2.0$ GiB per eval worker under prolonged execution using `jemalloc` preloading (`LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`).
- **ClickHouse Insert Latency**: Client-side micro-batching maintains ClickHouse insert latencies $\le 1.0$s under 10,000 metric data points flood without creating excessive detached parts.

---

## Scope & Exclusions

### In-Scope
- `rag-eval-worker` Temporal activity and workflow implementations (`eval-task-queue`).
- Single standardized LLM-as-a-judge runner executing native, low-overhead prompt templates (Context Precision, Faithfulness, Answer Relevance).
- Outbound token-bucket rate limiter and concurrency semaphores (capped at 20 parallel requests) with full jitter exponential backoff on HTTP 429.
- Triton/vLLM gRPC client adapter (`rag-inference-serving`).
- Direct micro-batched ClickHouse writer for `evaluation_runs` and `eval_metric_points` MergeTree tables.
- Internal FastAPI HTTP documentation server exposing OpenAPI 3.1 endpoints (`/docs`, `/redoc`, `/api/docs`, `/openapi.json`), `/healthz`, `/ready`, and `/metrics`.
- Prometheus observability metrics, alerting rules, and KEDA queue-backlog autoscaling.
- Dockerfile with Python 3.12+ and `jemalloc` preloading.

### Exclusions (Explicitly Cut or Deferred)
- **OCR Spatial Bounding-Box Lineage**: Deferred entirely to `rag-document-ai-worker` layout verification; eval worker evaluates purely textual span attribution.
- **Multi-Framework Runtimes (Ragas & TruLens)**: Both libraries are removed from runtime dependencies to eliminate package conflicts and unconstrained heap growth.
- **Dual Persistence Layer**: Relational PostgreSQL run tracking is removed; ClickHouse acts as the unified datastore for both run-level metadata and time-series score distributions.
- **Real-time Stream Bindings**: `rag-masking-sanitizer` is maintained in a discrete repository with dedicated stream pipeline bindings.

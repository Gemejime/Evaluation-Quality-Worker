# Security Context - RAG Evaluation & Governance Worker

## Threat Model & Trust Boundaries

- **Untrusted Evaluation Payloads**: Golden-set queries, retrieved contexts, and generated answers originate from diverse tenant pipelines and may contain adversarial prompts, injection vectors, or malformed characters.
- **Judge Response Validation**: Model inference outputs from Triton/vLLM endpoints are untrusted until parsed and validated against strict Pydantic JSON schemas.
- **Secrets & Salt Isolation**: HashiCorp Vault / AWS KMS integration pulls tenant master keys for HMAC-SHA256 token hashing; keys reside purely in volatile heap memory with zero plaintext leakage in logs or persistent storage.
- **Compute Worker Runtime**: Isolated compute environment. Workers operate with non-root Linux permissions (`runAsNonRoot: true`, non-root UID 1000) inside container images with dropped Linux capabilities (`capabilities: drop: ["ALL"]`). Filesystem write controls enforce a read-only root filesystem (`readOnlyRootFilesystem: true`) with ephemeral mounts scoped to temporary scratch storage (`/tmp`).
- **Internal RPC & Egress**: gRPC transport to `rag-inference-serving` and native TCP connections to ClickHouse must run with mTLS or VPC-internal network isolation.

---

## Secured Logging Invariants

1. **Confidential Content Redaction**:
   - Golden-set evaluation samples contain proprietary tenant queries, retrieved context passages, and generated answers that may include Personally Identifiable Information (PII) or Protected Health Information (PHI).
   - **Raw prompt texts, context passages, and model answers must NEVER be logged to stdout or logging backends**.
2. **Secrets & Credentials Masking**:
   - Database credentials, KMS salts, Temporal authentication tokens, and Triton gRPC bearer tokens must be intercepted and masked before any log emission.
3. **Structured Machine-Readable Output**:
   - Log entries must be structured JSON logs containing only metadata IDs (`tenant_id`, `run_id`, `sample_idx`, `metric_name`, `score`, `duration_ms`), never raw binary or sensitive text payloads.
   - For reasoning tracking, log only truncated SHA-256 hashes (`reasoning_hash = sha256(reasoning.encode())[:16]`).

---

## Input Quarantine & Memory Protection

1. **Malformed Input Quarantine**:
   - Batches or samples failing JSON schema validation or containing unsupported metric definitions must trigger non-retryable Temporal `ApplicationError` exceptions with error reason `INVALID_EVALUATION_INPUT`.
   - Never allow corrupted inputs to trigger infinite retry loops that saturate judge inference quotas.
2. **Memory Safety & Resource Caps**:
   - `jemalloc` (`LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`) must be active in production to prevent glibc heap fragmentation during string manipulation.
   - Enforce pod memory limits (Request: 2.0 GiB, Limit: 3.5 GiB) ensuring RSS stays bounded $\le 2.0$ GiB under prolonged execution.

---

## Secrets & Configuration Management

- Environment variables and credentials must never be committed to source control.
- All secrets are loaded through environment variables or Kubernetes Secret volume mounts.
- Local development utilizes `.env.example` templates; actual `.env` files are ignored via `.gitignore`.

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

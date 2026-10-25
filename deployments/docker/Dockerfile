# Build and runtime stage for rag-eval-governance-worker
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8080 \
    GRPC_PORT=50051

# Install jemalloc for memory fragmentation reduction (ADR-002, RSS <= 2.0 GiB) and curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    libjemalloc2 \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV LD_PRELOAD=libjemalloc.so.2

WORKDIR /app

# Create unprivileged system user
RUN groupadd -g 1000 appgroup && \
    useradd -u 1000 -g appgroup -s /bin/bash -m appuser

# Copy application source code and project metadata
COPY --chown=appuser:appgroup . /app

# Install dependencies using pip
RUN pip install --no-cache-dir .

# Ensure proper permissions
RUN chown -R appuser:appgroup /app

USER 1000

EXPOSE 8080 50051

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

ENTRYPOINT ["python", "-m", "cmd.worker.main"]

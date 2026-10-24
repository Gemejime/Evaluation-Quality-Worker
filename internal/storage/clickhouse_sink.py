"""Direct micro-batched ClickHouse sink with ReplacingMergeTree persistence."""

import asyncio
from datetime import UTC, date, datetime
from typing import Any

import clickhouse_connect
import structlog
from clickhouse_connect.driver.client import Client

from internal.config.settings import Settings, get_settings

logger = structlog.get_logger(__name__)

CREATE_EVALUATION_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS evaluation_runs (
    tenant_id String,
    run_id String,
    run_date Date,
    pipeline_version LowCardinality(String),
    dataset_id String,
    sample_count UInt32,
    avg_faithfulness Float32,
    avg_context_precision Float32,
    avg_answer_relevance Float32,
    duration_seconds Float32,
    status LowCardinality(String),
    attempt UInt32 DEFAULT 1,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(attempt)
PARTITION BY toYYYYMM(run_date)
ORDER BY (tenant_id, run_id);
"""

CREATE_EVAL_METRIC_POINTS_TABLE = """
CREATE TABLE IF NOT EXISTS eval_metric_points (
    tenant_id String,
    run_id String,
    run_date Date,
    sample_idx UInt32,
    sample_id String,
    metric_name LowCardinality(String),
    score Float32,
    reasoning_hash FixedString(64),
    execution_latency_ms UInt32,
    judge_model LowCardinality(String),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(run_date)
ORDER BY (tenant_id, run_id, metric_name, sample_idx);
"""


class ClickHouseSink:
    """Micro-batched ClickHouse storage sink executing non-blocking driver operations."""

    def __init__(
        self,
        settings: Settings | None = None,
        client: Any | None = None,
        start_flusher: bool = True,
    ) -> None:
        self.settings = settings or get_settings()
        self.batch_size = self.settings.CLICKHOUSE_BATCH_SIZE
        self.flush_interval = self.settings.CLICKHOUSE_FLUSH_INTERVAL_SECONDS
        self._client: Client | None = client
        self._owns_client = client is None

        self._buffer: list[dict[str, Any]] = []
        self._buffer_lock = asyncio.Lock()
        self._flusher_task: asyncio.Task[None] | None = None
        self._is_closed = False

        if start_flusher:
            self._flusher_task = asyncio.create_task(self._periodic_flusher())

    def _get_client(self) -> Client:
        """Lazily initialize or return ClickHouse driver client."""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.settings.CLICKHOUSE_HOST,
                port=self.settings.CLICKHOUSE_PORT,
                username=self.settings.CLICKHOUSE_USER,
                password=self.settings.CLICKHOUSE_PASSWORD,
                database=self.settings.CLICKHOUSE_DATABASE,
            )
        return self._client

    async def initialize_schema(self) -> None:
        """Initialize ClickHouse tables with ReplacingMergeTree engine if non-existent."""
        client = self._get_client()

        def _init() -> None:
            client.command(CREATE_EVALUATION_RUNS_TABLE)
            client.command(CREATE_EVAL_METRIC_POINTS_TABLE)

        await asyncio.to_thread(_init)
        logger.info("clickhouse_schema_initialized")

    async def _periodic_flusher(self) -> None:
        """Background coroutine flushing micro-batch buffer at fixed intervals."""
        try:
            while not self._is_closed:
                await asyncio.sleep(self.flush_interval)
                await self.flush()
        except asyncio.CancelledError:
            pass

    async def buffer_metric_point(self, point: dict[str, Any]) -> None:
        """Buffer a sample-level metric row into the micro-batch queue."""
        should_flush = False
        # Normalize and validate fields defensively
        run_date_val = point.get("run_date")
        if not run_date_val:
            created_at = point.get("created_at")
            if isinstance(created_at, datetime):
                run_date_val = created_at.date()
            else:
                run_date_val = datetime.now(UTC).date()
        elif isinstance(run_date_val, datetime):
            run_date_val = run_date_val.date()
        elif isinstance(run_date_val, str):
            run_date_val = date.fromisoformat(run_date_val)

        # Ensure reasoning_hash is strictly 64 chars
        r_hash = str(point.get("reasoning_hash", "")).strip()
        if len(r_hash) != 64:
            r_hash = r_hash.ljust(64, "0")[:64]

        record = {
            "tenant_id": str(point["tenant_id"]),
            "run_id": str(point["run_id"]),
            "run_date": run_date_val,
            "sample_idx": int(point["sample_idx"]),
            "sample_id": str(point["sample_id"]),
            "metric_name": str(point["metric_name"]),
            "score": float(point["score"]),
            "reasoning_hash": r_hash,
            "execution_latency_ms": int(point.get("execution_latency_ms", 0)),
            "judge_model": str(point.get("judge_model", self.settings.JUDGE_MODEL)),
            "created_at": point.get("created_at", datetime.now(UTC)),
        }

        async with self._buffer_lock:
            self._buffer.append(record)
            if len(self._buffer) >= self.batch_size:
                should_flush = True

        if should_flush:
            await self.flush()

    async def insert_run(self, run: dict[str, Any]) -> None:
        """Insert evaluation run summary metadata into evaluation_runs table."""
        client = self._get_client()

        run_date_val = run.get("run_date")
        if not run_date_val:
            created_at = run.get("created_at")
            if isinstance(created_at, datetime):
                run_date_val = created_at.date()
            else:
                run_date_val = datetime.now(UTC).date()
        elif isinstance(run_date_val, datetime):
            run_date_val = run_date_val.date()
        elif isinstance(run_date_val, str):
            run_date_val = date.fromisoformat(run_date_val)

        columns = [
            "tenant_id",
            "run_id",
            "run_date",
            "pipeline_version",
            "dataset_id",
            "sample_count",
            "avg_faithfulness",
            "avg_context_precision",
            "avg_answer_relevance",
            "duration_seconds",
            "status",
            "attempt",
            "created_at",
        ]
        row = [
            str(run["tenant_id"]),
            str(run["run_id"]),
            run_date_val,
            str(run.get("pipeline_version", "v1.0.0")),
            str(run.get("dataset_id", "default")),
            int(run["sample_count"]),
            float(run.get("avg_faithfulness", 0.0)),
            float(run.get("avg_context_precision", 0.0)),
            float(run.get("avg_answer_relevance", 0.0)),
            float(run.get("duration_seconds", 0.0)),
            str(run.get("status", "COMPLETED")),
            int(run.get("attempt", 1)),
            run.get("created_at", datetime.now(UTC)),
        ]

        def _insert() -> None:
            client.insert(
                "evaluation_runs",
                [row],
                column_names=columns,
            )

        await asyncio.to_thread(_insert)
        logger.info(
            "evaluation_run_persisted",
            tenant_id=run["tenant_id"],
            run_id=run["run_id"],
            status=run.get("status", "COMPLETED"),
        )

    async def flush(self) -> None:
        """Flush all pending metric points in buffer to ClickHouse."""
        async with self._buffer_lock:
            if not self._buffer:
                return
            to_flush = list(self._buffer)
            self._buffer.clear()

        client = self._get_client()
        columns = [
            "tenant_id",
            "run_id",
            "run_date",
            "sample_idx",
            "sample_id",
            "metric_name",
            "score",
            "reasoning_hash",
            "execution_latency_ms",
            "judge_model",
            "created_at",
        ]
        rows = [
            [
                item["tenant_id"],
                item["run_id"],
                item["run_date"],
                item["sample_idx"],
                item["sample_id"],
                item["metric_name"],
                item["score"],
                item["reasoning_hash"],
                item["execution_latency_ms"],
                item["judge_model"],
                item["created_at"],
            ]
            for item in to_flush
        ]

        def _insert_batch() -> None:
            client.insert(
                "eval_metric_points",
                rows,
                column_names=columns,
            )

        await asyncio.to_thread(_insert_batch)
        logger.info(
            "clickhouse_buffer_flushed",
            record_count=len(rows),
        )

    async def get_run_metrics(self, tenant_id: str, run_id: str) -> list[dict[str, Any]]:
        """Read deduplicated metric points for a run enforcing FINAL semantics."""
        client = self._get_client()
        query = (
            "SELECT tenant_id, run_id, run_date, sample_idx, sample_id, metric_name, "
            "score, reasoning_hash, execution_latency_ms, judge_model, created_at "
            "FROM eval_metric_points FINAL "
            "WHERE tenant_id = %(tenant_id)s AND run_id = %(run_id)s "
            "ORDER BY sample_idx ASC, metric_name ASC"
        )
        params = {"tenant_id": tenant_id, "run_id": run_id}

        def _query() -> list[dict[str, Any]]:
            result = client.query(query, parameters=params)
            return list(result.named_results())

        return await asyncio.to_thread(_query)

    async def close(self) -> None:
        """Gracefully terminate flusher task, flush remaining records, and close client."""
        if self._is_closed:
            return
        self._is_closed = True

        if self._flusher_task is not None:
            self._flusher_task.cancel()
            try:
                await self._flusher_task
            except asyncio.CancelledError:
                pass

        await self.flush()

        client_ref = self._client
        if self._owns_client and client_ref is not None:

            def _close() -> None:
                if hasattr(client_ref, "close"):
                    client_ref.close()

            await asyncio.to_thread(_close)
            self._client = None

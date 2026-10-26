"""Chaos and resilience tests for ClickHouse micro-batched storage sink."""

from typing import Any
from unittest.mock import MagicMock

import pytest

from internal.config.settings import Settings
from internal.storage.clickhouse_sink import ClickHouseSink


class ChaosClickHouseClient:
    """Mock ClickHouse client with chaos injection for transient network disconnects."""

    def __init__(self, fail_first_n_inserts: int = 0) -> None:
        self.inserts: list[tuple[str, list[list[Any]], list[str]]] = []
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.fail_countdown = fail_first_n_inserts
        self.closed = False

    def command(self, cmd: str) -> None:
        pass

    def insert(self, table: str, data: list[list[Any]], column_names: list[str]) -> None:
        if self.fail_countdown > 0:
            self.fail_countdown -= 1
            raise ConnectionResetError("Simulated ClickHouse TCP socket disconnect during write")
        self.inserts.append((table, data, column_names))

    def query(self, q: str, parameters: dict[str, Any] | None = None) -> Any:
        self.queries.append((q, parameters or {}))
        mock_result = MagicMock()
        # Simulate FINAL returning deduplicated records
        mock_result.named_results.return_value = [
            {
                "tenant_id": "chaos-tenant",
                "run_id": "run-chaos-1",
                "sample_idx": 0,
                "metric_name": "faithfulness",
                "score": 0.95,
                "reasoning_hash": "e" * 64,
                "attempt": 2,
            }
        ]
        return mock_result

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_clickhouse_flood_10000_records() -> None:
    """Flood storage sink with 10,000 metric points to verify micro-batching and zero loss."""
    settings = Settings(CLICKHOUSE_BATCH_SIZE=1000, CLICKHOUSE_FLUSH_INTERVAL_SECONDS=1.0)
    client = ChaosClickHouseClient()
    sink = ClickHouseSink(settings=settings, client=client, start_flusher=False)

    for i in range(10000):
        await sink.buffer_metric_point(
            {
                "tenant_id": "tenant-flood",
                "run_id": "run-flood-1",
                "sample_idx": i,
                "sample_id": f"s-{i}",
                "metric_name": "context_precision",
                "score": 0.90,
                "reasoning_hash": "f" * 64,
                "execution_latency_ms": 25,
            }
        )

    # 10,000 items with batch size 1,000 should trigger exactly 10 flushes of 1,000 rows
    assert len(client.inserts) == 10
    total_rows = sum(len(rows) for _, rows, _ in client.inserts)
    assert total_rows == 10000

    # Ensure buffer is empty
    assert len(sink._buffer) == 0
    await sink.close()


@pytest.mark.asyncio
async def test_clickhouse_transient_socket_disconnect_requeues_buffer() -> None:
    """Verify that uncommitted records are re-enqueued on socket disconnect and retried."""
    client = ChaosClickHouseClient(fail_first_n_inserts=1)
    sink = ClickHouseSink(client=client, start_flusher=False)

    for i in range(5):
        await sink.buffer_metric_point(
            {
                "tenant_id": "tenant-requeue",
                "run_id": "run-requeue-1",
                "sample_idx": i,
                "sample_id": f"s-{i}",
                "metric_name": "faithfulness",
                "score": 0.85,
                "reasoning_hash": "c" * 64,
            }
        )

    # First flush attempt fails due to simulated socket disconnect
    with pytest.raises(ConnectionResetError, match="TCP socket disconnect"):
        await sink.flush()

    # Invariant: buffer must not have dropped the 5 records
    assert len(sink._buffer) == 5

    # Second flush attempt succeeds (fail_countdown is now 0)
    await sink.flush()
    assert len(client.inserts) == 1
    assert len(client.inserts[0][1]) == 5
    assert len(sink._buffer) == 0
    await sink.close()


@pytest.mark.asyncio
async def test_clickhouse_read_deduplication_contract_uses_final() -> None:
    """Assert ReplacingMergeTree read queries strictly use FINAL modifier."""
    client = ChaosClickHouseClient()
    sink = ClickHouseSink(client=client, start_flusher=False)

    records = await sink.get_run_metrics(tenant_id="chaos-tenant", run_id="run-chaos-1")
    assert len(records) == 1
    assert len(client.queries) == 1

    query_str, params = client.queries[0]
    assert "FROM eval_metric_points FINAL" in query_str
    assert params["tenant_id"] == "chaos-tenant"
    assert params["run_id"] == "run-chaos-1"
    await sink.close()


@pytest.mark.asyncio
async def test_clickhouse_graceful_buffer_flush_on_shutdown() -> None:
    """Verify uncommitted buffered items below batch threshold are flushed upon close()."""
    settings = Settings(CLICKHOUSE_BATCH_SIZE=1000)
    client = ChaosClickHouseClient()
    sink = ClickHouseSink(settings=settings, client=client, start_flusher=False)

    # Buffer 450 items (below 1000 batch trigger)
    for i in range(450):
        await sink.buffer_metric_point(
            {
                "tenant_id": "tenant-shutdown",
                "run_id": "run-shutdown-1",
                "sample_idx": i,
                "sample_id": f"s-{i}",
                "metric_name": "answer_relevance",
                "score": 0.92,
                "reasoning_hash": "d" * 64,
            }
        )

    assert len(client.inserts) == 0
    assert len(sink._buffer) == 450

    # Closing the sink flushes remaining items
    await sink.close()
    assert len(client.inserts) == 1
    assert len(client.inserts[0][1]) == 450
    assert len(sink._buffer) == 0

"""Unit tests for ClickHouseSink buffering, schema init, and FINAL read queries."""

from typing import Any
from unittest.mock import MagicMock

import pytest

from internal.config.settings import Settings
from internal.storage.clickhouse_sink import ClickHouseSink


class MockClickHouseClient:
    """Mock ClickHouse Connect client capturing commands, inserts, and queries."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list[list[Any]], list[str]]] = []
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    def command(self, cmd: str) -> None:
        self.commands.append(cmd)

    def insert(self, table: str, data: list[list[Any]], column_names: list[str]) -> None:
        self.inserts.append((table, data, column_names))

    def query(self, q: str, parameters: dict[str, Any] | None = None) -> Any:
        self.queries.append((q, parameters or {}))
        mock_result = MagicMock()
        mock_result.named_results.return_value = [
            {
                "tenant_id": parameters.get("tenant_id") if parameters else "t1",
                "run_id": parameters.get("run_id") if parameters else "r1",
                "sample_idx": 0,
                "metric_name": "faithfulness",
                "score": 0.9,
                "reasoning_hash": "a" * 64,
            }
        ]
        return mock_result

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_clickhouse_sink_initialize_schema() -> None:
    client = MockClickHouseClient()
    sink = ClickHouseSink(client=client, start_flusher=False)

    await sink.initialize_schema()
    assert len(client.commands) == 2
    assert "CREATE TABLE IF NOT EXISTS evaluation_runs" in client.commands[0]
    assert "CREATE TABLE IF NOT EXISTS eval_metric_points" in client.commands[1]
    await sink.close()


@pytest.mark.asyncio
async def test_clickhouse_sink_batch_size_trigger() -> None:
    settings = Settings(CLICKHOUSE_BATCH_SIZE=3)
    client = MockClickHouseClient()
    sink = ClickHouseSink(settings=settings, client=client, start_flusher=False)

    # Buffer 2 items -> no flush yet
    for i in range(2):
        await sink.buffer_metric_point(
            {
                "tenant_id": "tenant-1",
                "run_id": "run-1",
                "sample_idx": i,
                "sample_id": f"s-{i}",
                "metric_name": "faithfulness",
                "score": 0.95,
                "reasoning_hash": "a" * 64,
            }
        )
    assert len(client.inserts) == 0

    # 3rd item triggers batch flush
    await sink.buffer_metric_point(
        {
            "tenant_id": "tenant-1",
            "run_id": "run-1",
            "sample_idx": 2,
            "sample_id": "s-2",
            "metric_name": "faithfulness",
            "score": 0.88,
            "reasoning_hash": "b" * 64,
        }
    )
    assert len(client.inserts) == 1
    table_name, rows, columns = client.inserts[0]
    assert table_name == "eval_metric_points"
    assert len(rows) == 3
    assert "reasoning_hash" in columns
    assert len(rows[0][7]) == 64  # reasoning_hash index
    await sink.close()


@pytest.mark.asyncio
async def test_clickhouse_sink_insert_run() -> None:
    client = MockClickHouseClient()
    sink = ClickHouseSink(client=client, start_flusher=False)

    run_payload = {
        "tenant_id": "tenant-prod",
        "run_id": "run-42",
        "dataset_id": "golden-set-v1",
        "sample_count": 100,
        "avg_faithfulness": 0.91,
        "avg_context_precision": 0.89,
        "avg_answer_relevance": 0.94,
        "duration_seconds": 12.5,
        "status": "COMPLETED",
    }
    await sink.insert_run(run_payload)
    assert len(client.inserts) == 1
    table, rows, cols = client.inserts[0]
    assert table == "evaluation_runs"
    assert len(rows) == 1
    assert rows[0][0] == "tenant-prod"
    assert rows[0][1] == "run-42"
    assert rows[0][5] == 100  # sample_count
    await sink.close()


@pytest.mark.asyncio
async def test_clickhouse_sink_get_run_metrics_uses_final() -> None:
    client = MockClickHouseClient()
    sink = ClickHouseSink(client=client, start_flusher=False)

    results = await sink.get_run_metrics(tenant_id="t1", run_id="r1")
    assert len(results) == 1
    assert len(client.queries) == 1
    query_str, params = client.queries[0]
    assert "FROM eval_metric_points FINAL" in query_str
    assert params == {"tenant_id": "t1", "run_id": "r1"}
    await sink.close()
    assert client.closed is False  # Injected client not owned

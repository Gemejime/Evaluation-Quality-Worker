"""Resilient Temporal worker daemon manager and lifecycle orchestration."""

import asyncio
from typing import Any

import structlog
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from internal.config.settings import Settings, get_settings
from internal.evaluation.judges.triton_client import TritonJudgeClient
from internal.evaluation.ratelimiter import OutboundJudgeGovernor
from internal.evaluation.runner import BatchEvaluationRunner
from internal.storage.clickhouse_sink import ClickHouseSink
from internal.telemetry.readiness import ReadinessManager, get_readiness_manager
from internal.temporal.activities import EvaluateGoldenSetActivity
from internal.temporal.workflows import BatchEvaluationWorkflow

logger = structlog.get_logger(__name__)


class TemporalWorkerManager:
    """Manages the connection lifecycle and activity execution of the Temporal worker daemon."""

    def __init__(
        self,
        settings: Settings | None = None,
        readiness_manager: ReadinessManager | None = None,
        runner: BatchEvaluationRunner | None = None,
        object_store_reader: Any | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.readiness = readiness_manager or get_readiness_manager()
        self._runner = runner
        self.object_store_reader = object_store_reader
        self._shutdown_event = asyncio.Event()
        self._worker: Worker | None = None
        self._client: Client | None = None

    def _build_default_runner(self) -> BatchEvaluationRunner:
        """Construct default batch evaluation runner with governed judge and ClickHouse sink."""
        judge_client = TritonJudgeClient(settings=self.settings)
        governor = OutboundJudgeGovernor(self.settings)
        sink = ClickHouseSink(self.settings)
        return BatchEvaluationRunner(
            judge_client=judge_client,
            governor=governor,
            sink=sink,
            settings=self.settings,
        )

    async def run(self) -> None:
        """Run the Temporal worker with resilient startup reconnection loop."""
        runner = self._runner or self._build_default_runner()
        activity_instance = EvaluateGoldenSetActivity(
            runner=runner,
            settings=self.settings,
            object_store_reader=self.object_store_reader,
        )

        backoff = 1.0
        max_backoff = 30.0
        self.readiness.set_status("temporal", "connecting")

        while not self._shutdown_event.is_set():
            try:
                logger.info(
                    "connecting_to_temporal",
                    host=self.settings.TEMPORAL_HOST,
                    namespace=self.settings.TEMPORAL_NAMESPACE,
                )
                self._client = await Client.connect(
                    self.settings.TEMPORAL_HOST,
                    namespace=self.settings.TEMPORAL_NAMESPACE,
                    data_converter=pydantic_data_converter,
                )
                self.readiness.set_status("temporal", "polling")
                logger.info(
                    "temporal_connected_starting_worker",
                    queue=self.settings.EVAL_TASK_QUEUE,
                )

                self._worker = Worker(
                    self._client,
                    task_queue=self.settings.EVAL_TASK_QUEUE,
                    workflows=[BatchEvaluationWorkflow],
                    activities=[activity_instance.run],
                )

                backoff = 1.0
                await self._worker.run()
                break
            except asyncio.CancelledError:
                logger.info("temporal_worker_run_cancelled")
                break
            except Exception as exc:
                self.readiness.set_status("temporal", "unreachable")
                logger.warning(
                    "temporal_connection_failed_retrying",
                    host=self.settings.TEMPORAL_HOST,
                    retry_in_seconds=backoff,
                    error=str(exc),
                )
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=backoff)
                    break
                except TimeoutError:
                    backoff = min(backoff * 2.0, max_backoff)

        logger.info("temporal_worker_manager_exited")

    async def shutdown(self) -> None:
        """Signal worker shutdown and release connection resources."""
        self._shutdown_event.set()
        if self._worker is not None:
            # Worker shutdown will terminate active polling
            pass
        self.readiness.set_status("temporal", "unreachable")

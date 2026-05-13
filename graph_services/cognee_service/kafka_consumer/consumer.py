"""Kafka consumer for Cogni data enrichment events.

Consumes DataEnrichmentEvent messages from the data_enrichment topic
and processes files through Cognee for knowledge graph enrichment.
Consumer group: cogni-processor (independent from code-changes-consumer).
"""

import asyncio
import json
import signal
from typing import Any

from aiokafka import AIOKafkaConsumer
import structlog
from shared.kafka_schemas import DataEnrichmentEvent

from .config import CogniConsumerConfig
from .processor import CogneeProcessor
from .stats_tracker import CogniStatsTracker

logger = structlog.get_logger(__name__)


class CogniKafkaConsumer:
    """Kafka consumer that reads DataEnrichmentEvent and processes via Cognee."""

    def __init__(
        self,
        processor: CogneeProcessor,
        stats_tracker: CogniStatsTracker | None = None,
        config: type[CogniConsumerConfig] = CogniConsumerConfig,
    ):
        self.processor = processor
        self.stats_tracker = stats_tracker
        self.config = config
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Initialize and start Kafka consumer."""
        if self.consumer is not None:
            logger.warning("consumer.already_started")
            return

        logger.info(
            "consumer.starting",
            topic=self.config.KAFKA_TOPIC,
            group=self.config.KAFKA_CONSUMER_GROUP_ID,
            bootstrap=self.config.KAFKA_BOOTSTRAP_SERVERS,
            batch_size=self.config.BATCH_SIZE,
        )

        self.consumer = AIOKafkaConsumer(
            self.config.KAFKA_TOPIC,
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.config.KAFKA_CONSUMER_GROUP_ID,
            enable_auto_commit=self.config.KAFKA_AUTO_COMMIT,
            auto_offset_reset=self.config.KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=self._deserialize_message,
        )

        await self.consumer.start()
        logger.info("consumer.started")

    async def stop(self) -> None:
        """Stop Kafka consumer gracefully."""
        if self.consumer is None:
            return

        logger.info("consumer.stopping")
        self._running = False
        self._shutdown_event.set()

        try:
            await self.consumer.stop()
            logger.info("consumer.stopped")
        except Exception as e:
            logger.error("consumer.stop_error", error=str(e))
        finally:
            self.consumer = None

    async def consume(self) -> None:
        """Main consumption loop with sliding-window concurrency."""
        if self.consumer is None:
            raise RuntimeError("Consumer not started. Call start() first.")

        self._running = True
        max_concurrent = self.config.BATCH_SIZE  # 20
        sem = asyncio.Semaphore(max_concurrent)

        # Pending adds grouped by (company_id, dataset_name)
        pending: dict[tuple[str, str], list[DataEnrichmentEvent]] = {}
        pending_lock = asyncio.Lock()
        in_flight: set[asyncio.Task] = set()

        async def _process_one(event: DataEnrichmentEvent) -> None:
            """Process a single file under semaphore control."""
            async with sem:
                if self.stats_tracker:
                    await self.stats_tracker.record_consumed(event)

                result = await self._add_with_retries(event)

                if result["status"] == "added":
                    ds_name = self.processor.build_dataset_name(event)
                    key = (event.company_id, ds_name)
                    async with pending_lock:
                        pending.setdefault(key, []).append(event)

        try:
            while self._running:
                # Fetch messages (non-blocking, short timeout)
                messages = await self.consumer.getmany(
                    timeout_ms=self.config.KAFKA_FETCH_TIMEOUT_MS,
                    max_records=max_concurrent,
                )

                for partition_msgs in messages.values():
                    for msg in partition_msgs:
                        event = msg.value
                        if not isinstance(event, DataEnrichmentEvent):
                            logger.error(
                                "consumer.invalid_message_type",
                                type=str(type(event)),
                            )
                            continue

                        if not event.project_id or not event.company_id:
                            logger.warning(
                                "consumer.missing_tenant_ids",
                                file_path=event.file_path,
                                event_id=event.event_id,
                            )
                            continue

                        # Launch concurrent task (semaphore limits to N in-flight)
                        task = asyncio.create_task(_process_one(event))
                        in_flight.add(task)
                        task.add_done_callback(in_flight.discard)

                # Clean up completed tasks and check for errors
                done = {t for t in in_flight if t.done()}
                for t in done:
                    in_flight.discard(t)
                    if t.exception():
                        logger.error("consumer.task_error", error=str(t.exception()))

                # Cognify datasets that have accumulated enough files
                async with pending_lock:
                    for key in list(pending.keys()):
                        if len(pending[key]) >= max_concurrent:
                            company_id, dataset_name = key
                            batch = pending.pop(key)
                            await self._cognify_batch(company_id, dataset_name, batch)

                # Idle flush: no messages fetched and no in-flight → flush partial batches
                if not messages and not in_flight:
                    async with pending_lock:
                        for key in list(pending.keys()):
                            company_id, dataset_name = key
                            batch = pending.pop(key)
                            if batch:
                                logger.info(
                                    "consumer.flush_partial_batch",
                                    dataset_name=dataset_name,
                                    files=len(batch),
                                )
                                await self._cognify_batch(company_id, dataset_name, batch)
                    if self._shutdown_event.is_set():
                        break

        except asyncio.CancelledError:
            logger.info("consumer.cancelled")
        except Exception as e:
            logger.error("consumer.fatal_error", error=str(e), exc_info=True)
            raise
        finally:
            # Wait for in-flight tasks
            if in_flight:
                logger.info("consumer.draining_in_flight", count=len(in_flight))
                await asyncio.gather(*in_flight, return_exceptions=True)
            # Flush remaining on exit
            for key in list(pending.keys()):
                company_id, dataset_name = key
                batch = pending.pop(key)
                if batch:
                    await self._cognify_batch(company_id, dataset_name, batch)
            logger.info("consumer.loop_exited")

    async def _process_message(self, event: DataEnrichmentEvent) -> None:
        """Process a single event with retries."""
        try:
            result = await self._process_with_retries(event)

            if result["status"] == "success":
                logger.info(
                    "consumer.processed",
                    file_path=event.file_path,
                    change_type=str(event.change_type),
                    duration_s=result.get("duration_s", 0),
                )
                if self.stats_tracker:
                    await self.stats_tracker.record_processed(event)
            elif result["status"] == "skipped":
                logger.warning(
                    "consumer.skipped",
                    file_path=event.file_path,
                    reason=result.get("reason"),
                    change_type=result.get("change_type"),
                )
            else:
                logger.error(
                    "consumer.failed",
                    file_path=event.file_path,
                    error=result.get("error"),
                )
                if self.stats_tracker:
                    await self.stats_tracker.record_failed(
                        event=event,
                        error_message=result.get("error", "unknown"),
                        error_type=result.get("error_type", "unknown"),
                        failure_stage="processing",
                    )

        except Exception as e:
            logger.error(
                "consumer.process_error",
                file_path=event.file_path,
                error=str(e),
                exc_info=True,
            )
            if self.stats_tracker:
                await self.stats_tracker.record_failed(
                    event=event,
                    error_message=str(e),
                    error_type=type(e).__name__,
                    failure_stage="processing",
                )

    async def _process_with_retries(self, event: DataEnrichmentEvent) -> dict[str, Any]:
        """Process event with exponential backoff retry."""
        last_error = None

        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                result = await self.processor.process(event)
                return result
            except Exception as e:
                last_error = e
                logger.warning(
                    "consumer.retry",
                    attempt=attempt,
                    max_retries=self.config.MAX_RETRIES,
                    file_path=event.file_path,
                    error=str(e),
                )
                if attempt < self.config.MAX_RETRIES:
                    delay = self.config.RETRY_DELAY * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)

        return {
            "status": "error",
            "error": f"Failed after {self.config.MAX_RETRIES} retries: {last_error}",
            "error_type": type(last_error).__name__ if last_error else "unknown",
            "event_id": event.event_id,
            "file_path": event.file_path,
        }

    async def _add_with_retries(self, event: DataEnrichmentEvent) -> dict[str, Any]:
        """Call processor.add_only() with retries. Returns result dict."""
        last_error = None
        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                result = await self.processor.add_only(event)
                if result["status"] == "skipped":
                    logger.warning(
                        "consumer.add_skipped",
                        file_path=event.file_path,
                        reason=result.get("reason"),
                    )
                return result
            except Exception as e:
                last_error = e
                logger.warning(
                    "consumer.add_retry",
                    attempt=attempt,
                    file_path=event.file_path,
                    error=str(e),
                )
                if attempt < self.config.MAX_RETRIES:
                    await asyncio.sleep(self.config.RETRY_DELAY * (2 ** (attempt - 1)))

        logger.error("consumer.add_failed", file_path=event.file_path, error=str(last_error))
        if self.stats_tracker:
            await self.stats_tracker.record_failed(
                event=event,
                error_message=str(last_error),
                error_type=type(last_error).__name__ if last_error else "unknown",
                failure_stage="add",
            )
        return {"status": "error", "error": str(last_error)}

    async def _cognify_batch(
        self, company_id: str, dataset_name: str, batch: list[DataEnrichmentEvent]
    ) -> None:
        """Run cognify for a dataset after a batch of add_only() calls."""
        logger.info(
            "consumer.cognify_batch",
            dataset_name=dataset_name,
            company_id=company_id,
            files_in_batch=len(batch),
        )
        try:
            result = await self.processor.cognify_dataset(company_id, dataset_name)
            logger.info(
                "consumer.cognify_batch_done",
                dataset_name=dataset_name,
                duration_s=result.get("duration_s", 0),
                files_in_batch=len(batch),
            )
            # Mark all files in batch as processed
            if self.stats_tracker:
                for event in batch:
                    await self.stats_tracker.record_processed(event)
        except Exception as e:
            logger.error(
                "consumer.cognify_batch_error",
                dataset_name=dataset_name,
                error=str(e),
                exc_info=True,
            )
            if self.stats_tracker:
                for event in batch:
                    await self.stats_tracker.record_failed(
                        event=event,
                        error_message=str(e),
                        error_type=type(e).__name__,
                        failure_stage="cognify",
                    )

    def _deserialize_message(self, raw_value: bytes) -> DataEnrichmentEvent:
        """Deserialize Kafka message bytes to DataEnrichmentEvent."""
        try:
            data = json.loads(raw_value.decode("utf-8"))
            return DataEnrichmentEvent(**data)
        except Exception as e:
            logger.error("consumer.deserialize_error", error=str(e))
            raise ValueError(f"Invalid message format: {e}") from e

    def setup_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM for graceful shutdown."""

        def handler(signum, frame):
            logger.info("consumer.signal_received", signal=signum)
            self._running = False
            self._shutdown_event.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

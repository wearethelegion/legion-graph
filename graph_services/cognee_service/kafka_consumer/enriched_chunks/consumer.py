"""Kafka consumer for enriched code chunks.

Consumes EnrichedChunkMessage from enriched-code-chunks topic and processes
batches of 50 through the Cognee pipeline (stages 3-6 only).
Consumer group: cognee-enriched-chunks-processor (independent).
"""

import asyncio
import json
import signal
from typing import Any

from aiokafka import AIOKafkaConsumer
import structlog

from .config import EnrichedChunksConsumerConfig
from .models import EnrichedChunkMessage
from .processor import EnrichedChunksProcessor
from ..stats_tracker import CogniStatsTracker

logger = structlog.get_logger(__name__)


class EnrichedChunksKafkaConsumer:
    """Kafka consumer that reads EnrichedChunkMessage and processes via Cognee."""

    def __init__(
        self,
        processor: EnrichedChunksProcessor,
        stats_tracker: CogniStatsTracker | None = None,
        config: type[EnrichedChunksConsumerConfig] = EnrichedChunksConsumerConfig,
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
        """Main consumption loop with batching.

        Collects messages into batches of BATCH_SIZE (50) grouped by
        (company_id, dataset_name, action), then processes each batch.
        Separate batches for process vs delete actions.
        """
        if self.consumer is None:
            raise RuntimeError("Consumer not started. Call start() first.")

        self._running = True
        batch_size = self.config.BATCH_SIZE  # 50

        # Pending batches grouped by (company_id, dataset_name, action)
        pending: dict[tuple[str, str, str], list[EnrichedChunkMessage]] = {}

        try:
            while self._running:
                # Fetch messages (non-blocking, short timeout)
                messages = await self.consumer.getmany(
                    timeout_ms=self.config.KAFKA_FETCH_TIMEOUT_MS,
                    max_records=batch_size * 2,  # Fetch up to 2 batches worth
                )

                for partition_msgs in messages.values():
                    for msg in partition_msgs:
                        event = msg.value
                        if not isinstance(event, EnrichedChunkMessage):
                            logger.error(
                                "consumer.invalid_message_type",
                                type=str(type(event)),
                            )
                            continue

                        if not event.company_id or (
                            event.content_type == "code" and not event.project_id
                        ):
                            logger.warning(
                                "consumer.missing_tenant_ids",
                                file_path=event.file_path,
                                chunk_id=getattr(event, "chunk_id", None),
                            )
                            # Record skipped file
                            if self.stats_tracker and event.ingestion_id:
                                try:
                                    await self.stats_tracker.record_skipped_file(
                                        ingestion_id=event.ingestion_id,
                                        company_id=event.company_id or "unknown",
                                        project_id=event.project_id
                                        or event.company_id
                                        or "unknown",
                                        repository=event.repository or "",
                                        branch=event.branch or "main",
                                        file_path=event.file_path,
                                        service="consumer",
                                        skip_type="missing_tenant_ids",
                                        reason="Missing company_id or code project_id",
                                    )
                                except Exception as e:
                                    logger.warning("consumer.stats_skip_error", error=str(e))
                            continue

                        # Group by (company_id, dataset_name, action)
                        branch = event.branch.replace("/", "_").replace("-", "_")
                        dataset_name = (
                            f"{event.company_id}_knowledge"
                            if event.content_type == "document"
                            else f"{event.project_id}_{branch}_code"
                        )
                        action = event.action or "process"
                        key = (event.company_id, dataset_name, action)

                        pending.setdefault(key, []).append(event)

                        # Process full batches immediately
                        if len(pending[key]) >= batch_size:
                            batch = pending.pop(key)
                            await self._process_batch(batch)

                # Idle flush: no messages fetched → flush partial batches
                if not messages and pending:
                    for key in list(pending.keys()):
                        batch = pending.pop(key)
                        if batch:
                            logger.info(
                                "consumer.flush_partial_batch",
                                dataset_name=key[1],
                                action=key[2],
                                chunks=len(batch),
                            )
                            await self._process_batch(batch)
                    if self._shutdown_event.is_set():
                        break

        except asyncio.CancelledError:
            logger.info("consumer.cancelled")
        except Exception as e:
            logger.error("consumer.fatal_error", error=str(e), exc_info=True)
            raise
        finally:
            # Flush remaining on exit
            for key in list(pending.keys()):
                batch = pending.pop(key)
                if batch:
                    await self._process_batch(batch)
            logger.info("consumer.loop_exited")

    async def _process_batch(self, batch: list[EnrichedChunkMessage]) -> None:
        """Process a batch of enriched chunks with retries.

        Routes to either process_batch or delete_batch based on action field.
        """
        if not batch:
            return

        company_id = batch[0].company_id
        dataset_name = self.processor.build_dataset_name(batch[0])
        action = batch[0].action or "process"

        logger.info(
            "consumer.batch_processing",
            company_id=company_id,
            dataset_name=dataset_name,
            action=action,
            batch_size=len(batch),
        )

        # Route based on action
        if action == "delete":
            await self._process_delete_batch(batch, company_id, dataset_name)
        else:
            await self._process_normal_batch(batch, company_id, dataset_name)

    async def _process_delete_batch(
        self,
        batch: list[EnrichedChunkMessage],
        company_id: str,
        dataset_name: str,
    ) -> None:
        """Process a batch of delete messages."""
        try:
            result = await self._process_with_retries(batch)
            if result["status"] == "success":
                logger.info(
                    "consumer.delete_batch_processed",
                    dataset_name=dataset_name,
                    files_deleted=result.get("files_deleted", 0),
                    duration_s=result.get("duration_s", 0),
                )
                if self.stats_tracker:
                    try:
                        await self.stats_tracker.record_chunk_batch_deleted(batch)
                    except Exception as e:
                        logger.warning("consumer.stats_deleted_error", error=str(e))
            else:
                logger.error(
                    "consumer.delete_batch_failed",
                    dataset_name=dataset_name,
                    error=result.get("error"),
                )
                # Record pipeline errors on delete failure
                if self.stats_tracker:
                    try:
                        await self.stats_tracker.record_pipeline_errors(
                            batch,
                            pipeline_stage="system",
                            error_type=result.get("error_type", "unknown"),
                            error_message=result.get("error", "unknown"),
                        )
                    except Exception as e:
                        logger.warning("consumer.stats_delete_error", error=str(e))
        except Exception as e:
            logger.error(
                "consumer.delete_batch_error",
                dataset_name=dataset_name,
                error=str(e),
                exc_info=True,
            )
            # Record pipeline errors on exception
            if self.stats_tracker:
                try:
                    await self.stats_tracker.record_pipeline_errors(
                        batch,
                        pipeline_stage="system",
                        error_type=type(e).__name__,
                        error_message=str(e)[:2000],
                    )
                except Exception:
                    pass

    async def _process_normal_batch(
        self,
        batch: list[EnrichedChunkMessage],
        company_id: str,
        dataset_name: str,
    ) -> None:
        """Process a batch of normal process messages."""
        # Record consumed in Postgres
        if self.stats_tracker:
            try:
                await self.stats_tracker.record_chunk_batch_consumed(batch, dataset_name)
            except Exception as e:
                logger.warning("consumer.stats_consumed_error", error=str(e))

        try:
            result = await self._process_with_retries(batch)
            if result["status"] == "success":
                logger.info(
                    "consumer.batch_processed",
                    dataset_name=dataset_name,
                    chunks=result.get("chunks_processed", 0),
                    duration_s=result.get("duration_s", 0),
                )
                if self.stats_tracker:
                    try:
                        await self.stats_tracker.record_chunk_batch_processed(batch)
                    except Exception as e:
                        logger.warning("consumer.stats_processed_error", error=str(e))
            else:
                logger.error(
                    "consumer.batch_failed",
                    dataset_name=dataset_name,
                    error=result.get("error"),
                )
                if self.stats_tracker:
                    try:
                        await self.stats_tracker.record_chunk_batch_failed(
                            batch,
                            result.get("error", "unknown"),
                            result.get("error_type", "unknown"),
                        )
                        # Record pipeline errors for each file in the batch
                        await self.stats_tracker.record_pipeline_errors(
                            batch,
                            pipeline_stage="preprocessing",
                            error_type=result.get("error_type", "unknown"),
                            error_message=result.get("error", "unknown"),
                        )
                    except Exception as e:
                        logger.warning("consumer.stats_failed_error", error=str(e))
        except Exception as e:
            logger.error(
                "consumer.batch_error",
                dataset_name=dataset_name,
                error=str(e),
                exc_info=True,
            )
            if self.stats_tracker:
                try:
                    await self.stats_tracker.record_chunk_batch_failed(
                        batch, str(e)[:2000], type(e).__name__
                    )
                    # Record pipeline errors
                    await self.stats_tracker.record_pipeline_errors(
                        batch,
                        pipeline_stage="preprocessing",
                        error_type=type(e).__name__,
                        error_message=str(e)[:2000],
                    )
                except Exception:
                    pass

    async def _process_with_retries(self, batch: list[EnrichedChunkMessage]) -> dict[str, Any]:
        """Process batch with exponential backoff retry.

        Routes to process_batch or delete_files based on the action field.
        """
        last_error = None
        action = batch[0].action if batch else "process"

        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                if action == "delete":
                    result = await self.processor.delete_files(batch)
                else:
                    result = await self.processor.process_batch(batch)
                return result
            except Exception as e:
                last_error = e
                logger.warning(
                    "consumer.retry",
                    attempt=attempt,
                    max_retries=self.config.MAX_RETRIES,
                    error=str(e),
                )
                if attempt < self.config.MAX_RETRIES:
                    delay = self.config.RETRY_DELAY * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)

        return {
            "status": "error",
            "error": f"Failed after {self.config.MAX_RETRIES} retries: {last_error}",
            "error_type": type(last_error).__name__ if last_error else "unknown",
        }

    def _deserialize_message(self, raw_value: bytes) -> EnrichedChunkMessage:
        """Deserialize Kafka message bytes to EnrichedChunkMessage."""
        try:
            data = json.loads(raw_value.decode("utf-8"))
            return EnrichedChunkMessage(**data)
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

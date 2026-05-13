"""Kafka consumer for Entity Extraction Service (Service 2).

Consumes EnrichedChunkMessage from enriched-code-chunks topic,
processes via streaming worker pool, publishes ExtractedEntitiesEvent
to extracted-entities topic, and emits extraction_complete to
pipeline-events when all chunks are done.

Architecture: Kafka → asyncio.Queue → N persistent workers → Postgres + Kafka
Consumer group: entity-extraction-processor-v2
"""

import asyncio
import json
import signal
from typing import Any, Dict, List, Optional, Set

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import structlog

from cognee_service.kafka_consumer.enriched_chunks.models import EnrichedChunkMessage
from cognee_service.multi_tenancy import ensure_neo4j_database, set_company_context

from .config import EntityExtractionConfig
from .models import ExtractedEntitiesEvent, PipelineEvent
from .processor import EntityExtractionProcessor

logger = structlog.get_logger(__name__)

# Sentinel value for queue shutdown
_SHUTDOWN_SENTINEL = object()


class EntityExtractionConsumer:
    """Kafka consumer with streaming worker pool architecture.

    Lifecycle: start() -> consume() -> stop()

    Flow:
    1. Kafka consumer feeds chunks into asyncio.Queue (maxsize = MAX_PARALLEL_WORKERS)
    2. N persistent worker coroutines pull from queue continuously
    3. Each worker: dequeue → extract → upsert → publish → loop
    4. No batch boundaries — immediate per-chunk publication
    """

    def __init__(
        self,
        processor: EntityExtractionProcessor,
        config: type[EntityExtractionConfig] = EntityExtractionConfig,
    ):
        self._processor = processor
        self._config = config
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        # Track ingestions that already emitted extraction_complete
        self._completed_ingestions: Set[str] = set()
        self._state_lock = asyncio.Lock()

        # Streaming worker pool
        self._work_queue: Optional[asyncio.Queue] = None
        self._workers: List[asyncio.Task] = []

    async def start(self) -> None:
        """Initialize Kafka consumer, producer, and worker pool."""
        if self._consumer is not None:
            logger.warning("consumer.already_started")
            return

        logger.info(
            "consumer.starting",
            input_topic=self._config.KAFKA_INPUT_TOPIC,
            output_topic=self._config.KAFKA_OUTPUT_TOPIC,
            group=self._config.KAFKA_CONSUMER_GROUP_ID,
            bootstrap=self._config.KAFKA_BOOTSTRAP_SERVERS,
            max_workers=self._config.MAX_PARALLEL_WORKERS,
        )

        # Consumer
        self._consumer = AIOKafkaConsumer(
            self._config.KAFKA_INPUT_TOPIC,
            bootstrap_servers=self._config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self._config.KAFKA_CONSUMER_GROUP_ID,
            enable_auto_commit=self._config.KAFKA_AUTO_COMMIT,
            auto_offset_reset=self._config.KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=self._deserialize_message,
        )

        # Producer for output events
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=self._serialize_event,
        )

        await self._consumer.start()
        await self._producer.start()

        # Initialize work queue and spawn workers
        self._work_queue = asyncio.Queue(maxsize=self._config.MAX_PARALLEL_WORKERS)
        self._workers = [
            asyncio.create_task(self._worker_loop(worker_id))
            for worker_id in range(self._config.MAX_PARALLEL_WORKERS)
        ]

        logger.info(
            "consumer.started",
            workers=len(self._workers),
            queue_size=self._config.MAX_PARALLEL_WORKERS,
        )

    async def stop(self) -> None:
        """Stop consumer, workers, and producer gracefully."""
        logger.info("consumer.stopping")
        self._running = False
        self._shutdown_event.set()

        # Signal workers to shut down via sentinel
        if self._work_queue:
            for _ in range(len(self._workers)):
                await self._work_queue.put(_SHUTDOWN_SENTINEL)

        # Wait for workers to finish
        if self._workers:
            logger.info("consumer.waiting_for_workers", count=len(self._workers))
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()

        if self._producer:
            try:
                await self._producer.stop()
            except Exception as e:
                logger.error("consumer.producer_stop_error", error=str(e))
            finally:
                self._producer = None

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.error("consumer.stop_error", error=str(e))
            finally:
                self._consumer = None

        logger.info("consumer.stopped")

    async def consume(self) -> None:
        """Main consumption loop — feeds work queue with validated chunks.

        Kafka consumer pulls messages and enqueues them for workers.
        Workers handle extraction + publishing.
        """
        if self._consumer is None or self._work_queue is None:
            raise RuntimeError("Consumer not started. Call start() first.")

        self._running = True

        try:
            while self._running:
                messages = await self._consumer.getmany(
                    timeout_ms=self._config.KAFKA_FETCH_TIMEOUT_MS,
                    max_records=100,
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

                        # Skip non-process actions
                        if event.action != "process":
                            logger.debug(
                                "consumer.skip_non_process",
                                action=event.action,
                                chunk_id=event.chunk_id,
                            )
                            continue

                        # Validate required fields
                        # Note: project_id may be empty for company-level documents
                        # (lessons, knowledge, etc.), so we only require it for code chunks
                        required_fields_valid = all(
                            [
                                event.company_id,
                                event.chunk_id,
                                event.content,
                            ]
                        )
                        # For code chunks, project_id is required
                        content_type = getattr(event, "content_type", "code")
                        if content_type == "code" and not event.project_id:
                            required_fields_valid = False

                        if not required_fields_valid:
                            logger.warning(
                                "consumer.missing_required_fields",
                                chunk_id=event.chunk_id,
                                content_type=content_type,
                                has_content=bool(event.content),
                                has_company=bool(event.company_id),
                                has_project=bool(event.project_id),
                            )
                            continue

                        # Enqueue for worker pool (blocks when queue full = backpressure)
                        await self._work_queue.put(event)

                # Check shutdown signal
                if self._shutdown_event.is_set():
                    break

        except asyncio.CancelledError:
            logger.info("consumer.cancelled")
        except Exception as e:
            logger.error("consumer.fatal_error", error=str(e), exc_info=True)
            raise
        finally:
            logger.info("consumer.loop_exited")

    async def _worker_loop(self, worker_id: int) -> None:
        """Persistent worker: dequeue → extract → publish → loop.

        Each worker pulls chunks from queue continuously until sentinel received.
        """
        logger.info("worker.started", worker_id=worker_id)

        try:
            while True:
                # Dequeue chunk (blocks when empty)
                chunk = await self._work_queue.get()

                # Shutdown sentinel
                if chunk is _SHUTDOWN_SENTINEL:
                    logger.info("worker.shutdown", worker_id=worker_id)
                    break

                # Process chunk
                try:
                    await self._process_chunk(chunk)
                except Exception as e:
                    logger.error(
                        "worker.chunk_error",
                        worker_id=worker_id,
                        chunk_id=chunk.chunk_id,
                        error=str(e),
                        exc_info=True,
                    )
                finally:
                    self._work_queue.task_done()

        except asyncio.CancelledError:
            logger.info("worker.cancelled", worker_id=worker_id)
        except Exception as e:
            logger.error("worker.fatal_error", worker_id=worker_id, error=str(e), exc_info=True)
        finally:
            logger.info("worker.stopped", worker_id=worker_id)

    async def _process_chunk(self, chunk: EnrichedChunkMessage) -> None:
        """Process single chunk: set context → extract → upsert → publish → check completion."""
        # Set multi-tenancy context
        try:
            await ensure_neo4j_database(chunk.company_id)
            set_company_context(chunk.company_id)
        except Exception as e:
            logger.warning(
                "worker.tenant_context_error",
                chunk_id=chunk.chunk_id,
                company_id=chunk.company_id,
                error=str(e),
            )

        # Increment chunks_received counter
        await self._processor._store.increment_counter(
            chunk.ingestion_id,
            "chunks_received",
            1,
        )

        # Extract entities (calls processor._do_extract with retries)
        event = await self._processor._extract_with_retries(chunk, 0, 1)

        if event is None:
            logger.error(
                "worker.extraction_failed",
                chunk_id=chunk.chunk_id,
                ingestion_id=chunk.ingestion_id,
            )
            return

        # Increment entity/edge counters
        if event.entities:
            await self._processor._store.increment_counter(
                chunk.ingestion_id,
                "entities_extracted",
                len(event.entities),
            )
        if event.edges:
            await self._processor._store.increment_counter(
                chunk.ingestion_id,
                "edges_extracted",
                len(event.edges),
            )

        # Publish immediately
        await self._publish_event(self._config.KAFKA_OUTPUT_TOPIC, event)

        logger.debug(
            "worker.chunk_published",
            chunk_id=chunk.chunk_id,
            entities=len(event.entities),
            edges=len(event.edges),
        )

        # Check completion after each chunk
        async with self._state_lock:
            already_done = chunk.ingestion_id in self._completed_ingestions
        if not already_done:
            await self._check_and_emit_completion(
                chunk.ingestion_id,
                chunk.company_id,
                chunk.project_id,
            )

    async def _check_and_emit_completion(
        self,
        ingestion_id: str,
        company_id: str,
        project_id: str,
    ) -> None:
        """Check if all chunks for an ingestion are processed; emit completion if so."""
        try:
            is_complete, received, total = await self._processor.check_ingestion_complete(
                ingestion_id
            )

            if is_complete and total is not None:
                async with self._state_lock:
                    self._completed_ingestions.add(ingestion_id)

                # Get entity/edge counts for the completion event
                counters = await self._processor._store.get_all_counters(ingestion_id)

                completion_event = PipelineEvent(
                    event_type="extraction_complete",
                    ingestion_id=ingestion_id,
                    company_id=company_id,
                    project_id=project_id,
                    chunks_processed=received,
                    total_entities=counters.get("entities_extracted", 0),
                    total_edges=counters.get("edges_extracted", 0),
                )

                await self._publish_event(self._config.KAFKA_EVENTS_TOPIC, completion_event)

                # Finalize counters
                await self._processor._store.finalize_counters(ingestion_id)

                logger.info(
                    "consumer.extraction_complete",
                    ingestion_id=ingestion_id,
                    chunks_processed=received,
                    total_chunks=total,
                    total_entities=counters.get("entities_extracted", 0),
                    total_edges=counters.get("edges_extracted", 0),
                )
            else:
                logger.debug(
                    "consumer.ingestion_in_progress",
                    ingestion_id=ingestion_id,
                    chunks_received=received,
                    total_chunks=total,
                )
        except Exception as e:
            logger.warning(
                "consumer.completion_check_error",
                ingestion_id=ingestion_id,
                error=str(e),
            )

    async def _publish_event(self, topic: str, event: Any) -> None:
        """Publish a Pydantic model as JSON to a Kafka topic."""
        if self._producer is None:
            logger.error("consumer.producer_not_initialized")
            return

        try:
            await self._producer.send_and_wait(topic, event)
        except Exception as e:
            logger.error(
                "consumer.publish_error",
                topic=topic,
                error=str(e),
            )

    def _deserialize_message(self, raw_value: bytes) -> EnrichedChunkMessage:
        """Deserialize Kafka message bytes to EnrichedChunkMessage."""
        try:
            data = json.loads(raw_value.decode("utf-8"))
            return EnrichedChunkMessage(**data)
        except Exception as e:
            logger.error("consumer.deserialize_error", error=str(e))
            raise ValueError(f"Invalid message format: {e}") from e

    def _serialize_event(self, event: Any) -> bytes:
        """Serialize a Pydantic model to JSON bytes for Kafka."""
        if hasattr(event, "model_dump"):
            return json.dumps(event.model_dump()).encode("utf-8")
        if hasattr(event, "dict"):
            return json.dumps(event.dict()).encode("utf-8")
        return json.dumps(event).encode("utf-8")

    def setup_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM for graceful shutdown."""

        def handler(signum, frame):
            logger.info("consumer.signal_received", signal=signum)
            self._running = False
            self._shutdown_event.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

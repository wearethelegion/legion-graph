"""Entrypoint for Qdrant Storage Service (Service 5) — STREAMING VERSION.

Consumes embedding-ready data from Kafka and writes to Qdrant in real-time.
NO gate-based batching — pure streaming with micro-batches.

Consumes:
- embeddings-ready: entity + summary embeddings from embedding service
- enriched-code-chunks: pre-embedded chunks from preprocessor

Usage:
    python -m qdrant_storage_service.main
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from qdrant_client import AsyncQdrantClient

from .config import QdrantStorageConfig
from .models import ChunkMessage, EmbeddingReadyEvent
from .pipeline_store import QdrantPipelineStore
from .writer import QdrantBatchWriter
from shared.project_name_resolver import ProjectNameResolver

load_dotenv()

# ── Logging ────────────────────────────────────────────────────
_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=getattr(logging, _log_level, logging.INFO),
)
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, _log_level, logging.INFO)),
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

_SHUTDOWN_SENTINEL = object()


class QdrantStorageService:
    """Streaming Qdrant storage service.

    Two-stage micro-batching:
    1. Collector: builds batches from work queue
    2. Workers: process batches and write to Qdrant
    """

    def __init__(
        self,
        store: QdrantPipelineStore,
        writer: QdrantBatchWriter,
        config: type[QdrantStorageConfig] = QdrantStorageConfig,
        project_resolver: Optional[ProjectNameResolver] = None,
    ) -> None:
        self._store = store
        self._writer = writer
        self._config = config
        self._project_resolver: Optional[ProjectNameResolver] = project_resolver
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Two-stage queues
        self._work_queue: Optional[asyncio.Queue] = None
        self._batch_queue: Optional[asyncio.Queue] = None
        self._collector_task: Optional[asyncio.Task] = None
        self._workers: List[asyncio.Task] = []

    async def start(self) -> None:
        """Initialize Kafka consumer, producer, collector, and workers."""
        logger.info(
            "service.starting",
            embeddings_topic=self._config.KAFKA_EMBEDDINGS_TOPIC,
            chunks_topic=self._config.KAFKA_CHUNKS_TOPIC,
            workers=self._config.STREAMING_WORKERS,
            batch_size=self._config.QDRANT_BATCH_SIZE,
        )

        # Subscribe to both embedding topics
        self._consumer = AIOKafkaConsumer(
            self._config.KAFKA_EMBEDDINGS_TOPIC,
            self._config.KAFKA_CHUNKS_TOPIC,
            bootstrap_servers=self._config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self._config.KAFKA_CONSUMER_GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset=self._config.KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=self._deserialize_message,
        )

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=self._serialize_event,
        )

        await self._consumer.start()
        await self._producer.start()

        # Initialize two-stage queues
        self._work_queue = asyncio.Queue(maxsize=self._config.STREAMING_WORKERS * 2)
        self._batch_queue = asyncio.Queue(maxsize=self._config.STREAMING_WORKERS)

        # Start collector and workers
        self._collector_task = asyncio.create_task(self._collector_loop())
        self._workers = [
            asyncio.create_task(self._batch_worker_loop(i))
            for i in range(self._config.STREAMING_WORKERS)
        ]

        logger.info("service.started", collector=1, workers=len(self._workers))

    async def stop(self) -> None:
        """Stop consumer, collector, workers, and producer gracefully."""
        logger.info("service.stopping")
        self._running = False
        self._shutdown_event.set()

        # Signal shutdown to collector
        if self._work_queue:
            await self._work_queue.put(_SHUTDOWN_SENTINEL)

        # Wait for collector to drain work queue
        if self._collector_task:
            logger.info("service.waiting_for_collector")
            await self._collector_task

        # Wait for workers to finish
        if self._workers:
            logger.info("service.waiting_for_workers", count=len(self._workers))
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()

        if self._producer:
            try:
                await self._producer.stop()
            except Exception as e:
                logger.error("service.producer_stop_error", error=str(e))
            finally:
                self._producer = None

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.error("service.consumer_stop_error", error=str(e))
            finally:
                self._consumer = None

        logger.info("service.stopped")

    async def consume(self) -> None:
        """Main consumption loop — feeds work queue from Kafka."""
        if self._consumer is None or self._work_queue is None:
            raise RuntimeError("Service not started. Call start() first.")

        self._running = True

        try:
            while self._running:
                messages = await self._consumer.getmany(
                    timeout_ms=self._config.KAFKA_FETCH_TIMEOUT_MS,
                    max_records=100,
                )

                for tp, partition_msgs in messages.items():
                    topic = tp.topic

                    for msg in partition_msgs:
                        data = msg.value
                        if not isinstance(data, dict):
                            logger.error("consumer.invalid_message_type", type=str(type(data)))
                            continue

                        # Handle delete messages immediately (bypass batching)
                        if data.get("action") == "delete":
                            await self._handle_delete(data)
                            continue

                        # Determine source type from topic
                        if topic == self._config.KAFKA_CHUNKS_TOPIC:
                            await self._work_queue.put(("chunk", data))
                        elif topic == self._config.KAFKA_EMBEDDINGS_TOPIC:
                            await self._work_queue.put(("embedding", data))
                        else:
                            logger.warning("consumer.unknown_topic", topic=topic)

                if messages:
                    await self._consumer.commit()

                if self._shutdown_event.is_set():
                    break

        except asyncio.CancelledError:
            logger.info("consumer.cancelled")
        except Exception as e:
            logger.error("consumer.fatal_error", error=str(e), exc_info=True)
            raise
        finally:
            logger.info("consumer.loop_exited")

    async def _handle_delete(self, data: Dict[str, Any]) -> None:
        """Handle a delete message by removing all Qdrant points for the file_version_id.

        Delete messages arrive on enriched-code-chunks with action=delete.
        Executed immediately outside the batching pipeline.

        ``company_id`` is extracted from the message and forwarded to
        ``delete_by_file_version_id`` so it can also purge the per-company
        ``{company_id}_knowledge`` collection where document chunks land.
        """
        file_version_id = data.get("file_version_id")
        if not file_version_id:
            logger.error(
                "consumer.delete_missing_file_version_id",
                data_keys=list(data.keys()),
            )
            return

        # company_id is present for document deletes (emit_delete publishes it).
        # It may be absent for legacy code-pipeline delete messages — None is safe.
        company_id: Optional[str] = data.get("company_id") or None

        try:
            results = await self._writer.delete_by_file_version_id(
                file_version_id,
                company_id=company_id,
            )
            total_deleted = sum(results.values())
            logger.info(
                "consumer.delete_handled",
                file_version_id=file_version_id,
                company_id=company_id,
                total_deleted=total_deleted,
                results=results,
            )
        except Exception as e:
            logger.error(
                "consumer.delete_failed",
                file_version_id=file_version_id,
                error=str(e),
                exc_info=True,
            )

    async def _collector_loop(self) -> None:
        """Collector: build maximally-full batches from work queue."""
        logger.info("collector.started")

        try:
            while True:
                first_item = await self._work_queue.get()

                if first_item is _SHUTDOWN_SENTINEL:
                    logger.info("collector.shutdown_signal")
                    # Drain remaining work
                    batch = []
                    while True:
                        try:
                            work_item = self._work_queue.get_nowait()
                            if work_item is not _SHUTDOWN_SENTINEL:
                                batch.append(work_item)
                        except asyncio.QueueEmpty:
                            break

                    if batch:
                        logger.info("collector.final_batch", size=len(batch))
                        await self._batch_queue.put(batch)

                    # Send shutdown signal to all workers
                    for _ in range(self._config.STREAMING_WORKERS):
                        await self._batch_queue.put(_SHUTDOWN_SENTINEL)

                    break

                batch = [first_item]

                # Build batch up to QDRANT_BATCH_SIZE or timeout
                while len(batch) < self._config.QDRANT_BATCH_SIZE:
                    try:
                        work_item = self._work_queue.get_nowait()
                        if work_item is _SHUTDOWN_SENTINEL:
                            await self._work_queue.put(_SHUTDOWN_SENTINEL)
                            break
                        batch.append(work_item)
                    except asyncio.QueueEmpty:
                        try:
                            work_item = await asyncio.wait_for(
                                self._work_queue.get(),
                                timeout=self._config.COLLECT_TIMEOUT,
                            )
                            if work_item is _SHUTDOWN_SENTINEL:
                                await self._work_queue.put(_SHUTDOWN_SENTINEL)
                                break
                            batch.append(work_item)
                        except asyncio.TimeoutError:
                            break

                await self._batch_queue.put(batch)
                logger.debug("collector.batch_pushed", size=len(batch))

        except asyncio.CancelledError:
            logger.info("collector.cancelled")
        except Exception as e:
            logger.error("collector.fatal_error", error=str(e), exc_info=True)
        finally:
            logger.info("collector.stopped")

    async def _batch_worker_loop(self, worker_id: int) -> None:
        """Batch worker: process batch and write to Qdrant."""
        logger.info("worker.started", worker_id=worker_id)

        try:
            while True:
                batch = await self._batch_queue.get()

                if batch is _SHUTDOWN_SENTINEL:
                    logger.info("worker.shutdown", worker_id=worker_id)
                    self._batch_queue.task_done()
                    break

                try:
                    await self._process_batch(batch, worker_id)
                except Exception as e:
                    logger.error(
                        "worker.batch_error",
                        worker_id=worker_id,
                        batch_size=len(batch),
                        error=str(e),
                        exc_info=True,
                    )
                finally:
                    self._batch_queue.task_done()

        except asyncio.CancelledError:
            logger.info("worker.cancelled", worker_id=worker_id)
        except Exception as e:
            logger.error("worker.fatal_error", worker_id=worker_id, error=str(e), exc_info=True)
        finally:
            logger.info("worker.stopped", worker_id=worker_id)

    async def _process_batch(self, batch: List[tuple], worker_id: int) -> None:
        """Process a batch of work items and write to Qdrant.

        Routes by source type:
        - "chunk" → upsert_chunks()
        - "embedding" → parse EmbeddingReadyEvent → route by source_type
        """
        if not batch:
            return

        t0 = time.time()

        # Pre-resolve project names for all unique project_ids in this batch
        project_names: Dict[str, str] = {}
        if self._project_resolver:
            unique_pids = {
                data.get("project_id", "") for _, data in batch if data.get("project_id")
            }
            for pid in unique_pids:
                project_names[pid] = await self._project_resolver.resolve(pid)

        # Group by type
        chunks = []
        entities = []
        summaries = []
        triplets = []
        edge_types = []
        entity_types = []

        for source_type, data in batch:
            if source_type == "chunk":
                # Parse chunk message
                chunk_data = self._parse_chunk_message(data, project_names)
                if chunk_data:
                    chunks.append(chunk_data)

            elif source_type == "embedding":
                # Parse EmbeddingReadyEvent
                event_data = self._parse_embedding_event(data, project_names)
                if event_data:
                    for item_type, item in event_data:
                        if item_type == "entity":
                            entities.append(item)
                        elif item_type == "summary":
                            summaries.append(item)
                        elif item_type == "triplet":
                            triplets.append(item)
                        elif item_type == "edge_type":
                            edge_types.append(item)
                        elif item_type == "entity_type":
                            entity_types.append(item)

        # Write to Qdrant
        chunk_count = 0
        entity_count = 0
        summary_count = 0
        triplet_count = 0
        edge_type_count = 0
        entity_type_count = 0

        if chunks:
            # Filter out garbage chunks (< 50 chars of actual text)
            MIN_CHUNK_SIZE = 50
            pre_filter = len(chunks)
            chunks = [
                c for c in chunks if len((c.get("content", "") or "").strip()) >= MIN_CHUNK_SIZE
            ]
            if len(chunks) < pre_filter:
                logger.info("qdrant.filtered_garbage_chunks", filtered=pre_filter - len(chunks))
            chunk_count = await self._writer.upsert_chunks(chunks)

        if entities:
            entity_count = await self._writer.upsert_entities(entities)

        if summaries:
            summary_count = await self._writer.upsert_summaries(summaries)

        if triplets:
            triplet_count = await self._writer.upsert_triplets(triplets)

        if edge_types:
            edge_type_count = await self._writer.upsert_edge_types(edge_types)

        if entity_types:
            entity_type_count = await self._writer.upsert_entity_types(entity_types)

        duration = round(time.time() - t0, 3)

        logger.info(
            "worker.batch_processed",
            worker_id=worker_id,
            batch_size=len(batch),
            chunks=chunk_count,
            entities=entity_count,
            summaries=summary_count,
            triplets=triplet_count,
            edge_types=edge_type_count,
            entity_types=entity_type_count,
            duration_s=duration,
        )

    def _parse_chunk_message(
        self,
        data: Dict[str, Any],
        project_names: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Parse enriched chunk message to dict for writer.

        Expected fields:
        - chunk_id, embedding, chunk_text, company_id, project_id
        - file_path, language, repository, branch
        - content_type (optional, defaults to "code")
        """
        try:
            chunk_id = data.get("chunk_id")
            embedding = data.get("embedding")
            content = data.get("content", "") or data.get("chunk_text", "")
            project_id = data.get("project_id") or None
            company_id = data.get("company_id", "")

            # Detect content_type for routing (T4: Knowledge graph storage)
            content_type = data.get("content_type", "") or "code"

            if not chunk_id or not embedding or (content_type == "code" and not project_id):
                logger.warning("chunk.missing_required_fields", chunk_id=chunk_id)
                return None
            if content_type == "document" and project_id is not None:
                logger.warning("chunk.invalid_document_scope", chunk_id=chunk_id)
                return None

            # Resolve project_name for node set naming
            _pnames = project_names or {}
            project_name = _pnames.get(project_id, project_id) if project_id else None

            return {
                "chunk_id": chunk_id,
                "embedding": embedding,
                "content": content,
                "header": data.get("header", ""),
                "file_path": data.get("file_path", ""),
                "language": data.get("language", ""),
                "repository": data.get("repository", ""),
                "branch": data.get("branch", "main"),
                "company_id": company_id,
                "project_id": project_id,
                "project_name": project_name,
                "file_version_id": data.get("file_version_id", ""),
                "chunk_index": data.get("chunk_index", 0),
                "ingestion_id": data.get("ingestion_id", ""),
                "start_line": data.get("start_line", 0),
                "end_line": data.get("end_line", 0),
                "content_type": content_type,
            }
        except Exception as e:
            logger.error("chunk.parse_error", error=str(e), data=data)
            return None

    def _parse_embedding_event(
        self,
        data: Dict[str, Any],
        project_names: Optional[Dict[str, str]] = None,
    ) -> List[tuple]:
        """Parse EmbeddingReadyEvent to list of (type, dict) tuples.

        Returns list of ("entity"|"summary"|"triplet"|"edge_type", record_dict) for writer.
        """
        try:
            company_id = data.get("company_id", "")
            project_id = data.get("project_id") or None
            ingestion_id = data.get("ingestion_id", "")
            file_version_id = data.get("file_version_id", "")
            repository = data.get("repository", "")
            branch = data.get("branch", "main")

            # Resolve project_name for node set naming
            _pnames = project_names or {}
            project_name = _pnames.get(project_id, project_id) if project_id else None
            content_type = data.get("content_type", "") or (
                "document" if not project_id else "code"
            )

            embeddings = data.get("embeddings", [])
            if not embeddings:
                return []

            results = []
            for payload in embeddings:
                source_id = payload.get("source_id")
                source_type = payload.get("source_type")
                text = payload.get("text", "")
                embedding = payload.get("embedding")

                if not source_id or not source_type or not embedding:
                    continue

                if content_type == "document" and project_id is not None:
                    logger.warning("embedding.invalid_document_scope", source_id=source_id)
                    return []

                record = {
                    "company_id": company_id,
                    "project_id": project_id,
                    "project_name": project_name,
                    "file_version_id": file_version_id,
                    "repository": repository,
                    "branch": branch,
                    "embedding": embedding,
                    "content_type": data.get("content_type", "")
                    or ("document" if not project_id else "code"),
                }

                if source_type == "entity":
                    record.update(
                        {
                            "entity_id": source_id,
                            "name": text,
                            "entity_type": payload.get("entity_type", ""),
                            "description": payload.get("description", ""),
                        }
                    )
                    results.append(("entity", record))

                elif source_type == "summary":
                    record.update(
                        {
                            "summary_id": source_id,
                            "summary_text": text,
                            "chunk_id": "",  # Not in EmbeddingPayload
                        }
                    )
                    results.append(("summary", record))

                elif source_type == "triplet":
                    # Parse triplet text: "{source_name}-›{relationship}-›{target_name}"
                    parts = text.split("-›")
                    source_name = parts[0] if len(parts) > 0 else ""
                    relationship_type = parts[1] if len(parts) > 1 else ""
                    target_name = parts[2] if len(parts) > 2 else ""

                    record.update(
                        {
                            "triplet_id": source_id,
                            "source_name": source_name,
                            "relationship_type": relationship_type,
                            "target_name": target_name,
                            "source_entity_id": payload.get("from_node_id", ""),
                            "target_entity_id": payload.get("to_node_id", ""),
                        }
                    )
                    results.append(("triplet", record))

                elif source_type == "edge_type":
                    record.update(
                        {
                            "edge_type_id": source_id,
                            "relationship_name": text,
                            "number_of_edges": payload.get("number_of_edges", 0),
                        }
                    )
                    results.append(("edge_type", record))

                elif source_type == "entity_type":
                    record.update(
                        {
                            "entity_type_id": source_id,
                            "name": text,
                            "number_of_entities": payload.get("number_of_entities", 0),
                        }
                    )
                    results.append(("entity_type", record))

            return results

        except Exception as e:
            logger.error("embedding.parse_error", error=str(e), data=data)
            return []

    def _deserialize_message(self, raw_value: bytes) -> dict:
        """Deserialize Kafka message bytes to dict."""
        try:
            return json.loads(raw_value.decode("utf-8"))
        except Exception as e:
            logger.error("service.deserialize_error", error=str(e))
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
            logger.info("service.signal_received", signal=signum)
            self._running = False
            self._shutdown_event.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)


async def main() -> None:
    """Start the Qdrant Storage Service (Streaming)."""
    config = QdrantStorageConfig
    config.validate()

    logger.info(
        "main.starting",
        qdrant_url=config.QDRANT_URL,
        batch_size=config.QDRANT_BATCH_SIZE,
        workers=config.STREAMING_WORKERS,
    )

    # Initialize Cognee DB and run migrations
    from cognee_service.config import configure_cognee
    from cognee.infrastructure.databases.relational.create_db_and_tables import create_db_and_tables
    from cognee.run_migrations import run_migrations

    configure_cognee()
    await create_db_and_tables()
    await run_migrations()
    logger.info("main.cognee_migrations_complete")

    # Initialize Postgres connection pool
    pool = await asyncpg.create_pool(
        config.POSTGRES_DSN,
        min_size=config.POSTGRES_MIN_POOL,
        max_size=config.POSTGRES_MAX_POOL,
    )
    logger.info("main.postgres_connected")

    # Initialize store and ensure tables exist
    store = QdrantPipelineStore(pool)
    await store.ensure_tables()
    logger.info("main.tables_ensured")

    # Initialize Qdrant client
    qdrant_client = AsyncQdrantClient(
        url=config.QDRANT_URL,
        api_key=config.QDRANT_API_KEY or None,
    )
    logger.info("main.qdrant_connected")

    # Initialize components
    writer = QdrantBatchWriter(client=qdrant_client, config=config)
    await writer.ensure_collections()
    logger.info("main.collections_ensured")

    project_resolver = ProjectNameResolver(pool)
    service = QdrantStorageService(
        store=store,
        writer=writer,
        config=config,
        project_resolver=project_resolver,
    )
    service.setup_signal_handlers()

    try:
        await service.start()
        logger.info("main.service_ready")
        await service.consume()
    except KeyboardInterrupt:
        logger.info("main.keyboard_interrupt")
    except Exception as e:
        logger.error("main.fatal_error", error=str(e), exc_info=True)
        raise
    finally:
        try:
            await service.stop()
        except Exception as e:
            logger.error("cleanup.service_stop_failed", error=str(e))
        try:
            await pool.close()
        except Exception as e:
            logger.error("cleanup.pool_close_failed", error=str(e))
        logger.info("main.shutdown_complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        logger.error("main.exit_error", error=str(e))
        sys.exit(1)

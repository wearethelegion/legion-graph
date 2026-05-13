"""Kafka consumer for Embedding Service (Service 4).

Two-stage pipeline: Collector → Batch Workers → Postgres + Kafka
- Collector: builds maximally-full batches from work_queue
- Workers: embed batch → store → publish

Consumes: extracted-entities, text-summaries
Group: embedding-processor-v2
"""

import asyncio
import json
import signal
import time
from collections import Counter
from typing import Any, Dict, List, Optional, Set
from uuid import uuid5, NAMESPACE_OID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import structlog


def _cognee_id(text: str) -> str:
    """Generate UUID5 matching Cognee's generate_node_id / generate_edge_id normalisation."""
    return str(uuid5(NAMESPACE_OID, text.lower().replace(" ", "_").replace("'", "")))


from .config import EmbeddingConfig
from .models import EmbeddingPayload, EmbeddingReadyEvent, PipelineEvent
from .processor import EmbeddingProcessor

logger = structlog.get_logger(__name__)

_SHUTDOWN_SENTINEL = object()


class EmbeddingConsumer:
    """Two-stage batch pipeline for embedding service.

    Lifecycle: start() -> consume() -> stop()
    """

    def __init__(
        self,
        processor: EmbeddingProcessor,
        config: type[EmbeddingConfig] = EmbeddingConfig,
    ):
        self._processor = processor
        self._config = config
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._completed_ingestions: Set[str] = set()
        self._state_lock = asyncio.Lock()

        self._work_queue: Optional[asyncio.Queue] = None
        self._batch_queue: Optional[asyncio.Queue] = None
        self._collector_task: Optional[asyncio.Task] = None
        self._workers: List[asyncio.Task] = []

    async def start(self) -> None:
        """Initialize Kafka consumer, producer, collector, and batch workers."""
        if self._consumer is not None:
            logger.warning("consumer.already_started")
            return

        logger.info(
            "consumer.starting",
            input_topics=self._config.KAFKA_INPUT_TOPICS,
            output_topic=self._config.KAFKA_OUTPUT_TOPIC,
            workers=self._config.EMBEDDING_WORKERS,
            batch_size=self._config.EMBEDDING_BATCH_SIZE,
        )

        self._consumer = AIOKafkaConsumer(
            *self._config.KAFKA_INPUT_TOPICS,
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

        self._work_queue = asyncio.Queue(maxsize=self._config.EMBEDDING_WORKERS * 2)
        self._batch_queue = asyncio.Queue(maxsize=self._config.EMBEDDING_WORKERS)

        self._collector_task = asyncio.create_task(self._collector_loop())
        self._workers = [
            asyncio.create_task(self._batch_worker_loop(i))
            for i in range(self._config.EMBEDDING_WORKERS)
        ]

        logger.info("consumer.started", collector=1, workers=len(self._workers))

    async def stop(self) -> None:
        """Stop consumer, collector, workers, and producer gracefully."""
        logger.info("consumer.stopping")
        self._running = False
        self._shutdown_event.set()

        if self._work_queue:
            await self._work_queue.put(_SHUTDOWN_SENTINEL)

        if self._collector_task:
            logger.info("consumer.waiting_for_collector")
            await self._collector_task

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
        """Main consumption loop — feeds work queue."""
        if self._consumer is None or self._work_queue is None:
            raise RuntimeError("Consumer not started. Call start() first.")

        self._running = True

        try:
            while self._running:
                messages = await self._consumer.getmany(
                    timeout_ms=self._config.KAFKA_FETCH_TIMEOUT_MS,
                    max_records=100,
                )

                for tp, partition_msgs in messages.items():
                    topic = tp.topic
                    source_type = self._topic_to_source_type(topic)

                    for msg in partition_msgs:
                        data = msg.value
                        if not isinstance(data, dict):
                            logger.error("consumer.invalid_message_type", type=str(type(data)))
                            continue

                        ingestion_id = data.get("ingestion_id", "")
                        if not ingestion_id:
                            logger.warning("consumer.missing_ingestion_id")
                            continue

                        items = self._extract_items(data, source_type)
                        if not items:
                            continue

                        for item in items:
                            await self._work_queue.put((item, item.get("source_type", source_type)))

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

    async def _collector_loop(self) -> None:
        """Collector: build maximally-full batches from work_queue."""
        logger.info("collector.started")

        try:
            while True:
                first_item = await self._work_queue.get()

                if first_item is _SHUTDOWN_SENTINEL:
                    logger.info("collector.shutdown_signal")
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

                    for _ in range(self._config.EMBEDDING_WORKERS):
                        await self._batch_queue.put(_SHUTDOWN_SENTINEL)

                    break

                batch = [first_item]

                while len(batch) < self._config.EMBEDDING_BATCH_SIZE:
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
        """Batch worker: embed batch → store → publish."""
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

    def _topic_to_source_type(self, topic: str) -> str:
        """Map Kafka topic name to source_type."""
        if "entities" in topic or "extraction" in topic:
            return "entity"
        if "summar" in topic:
            return "summary"
        return "unknown"

    def _extract_items(self, data: Dict[str, Any], source_type: str) -> List[Dict[str, Any]]:
        """Extract embeddable items from a Kafka message."""
        ingestion_id = data.get("ingestion_id", "")
        company_id = data.get("company_id", "")
        project_id = data.get("project_id") or None
        content_type = data.get("content_type", "code")
        repository = data.get("repository", "")
        branch = data.get("branch", "")
        file_version_id = data.get("file_version_id", "")
        items: List[Dict[str, Any]] = []

        if source_type == "entity":
            entity_map: Dict[str, str] = {}
            for ent in data.get("entities", []):
                name = ent.get("name", "")
                entity_id = ent.get("entity_id", "")
                if name and entity_id:
                    entity_map[entity_id] = name
                    items.append(
                        {
                            "source_id": entity_id,
                            "text": name,
                            "source_type": "entity",
                            "entity_type": ent.get("entity_type", ""),
                            "description": ent.get("description", ""),
                            "ingestion_id": ingestion_id,
                            "company_id": company_id,
                            "project_id": project_id,
                            "content_type": content_type,
                            "repository": repository,
                            "branch": branch,
                            "file_version_id": file_version_id,
                        }
                    )

            for edge in data.get("edges", []):
                src_id = edge.get("source_id", "")
                tgt_id = edge.get("target_id", "")
                rel_type = edge.get("relationship_type", "")
                src_name = entity_map.get(src_id, "")
                tgt_name = entity_map.get(tgt_id, "")
                if src_name and tgt_name and rel_type:
                    triplet_text = f"{src_name}-\u203a{rel_type}-\u203a{tgt_name}"
                    triplet_seed = f"{src_id}{rel_type}{tgt_id}"
                    triplet_id = _cognee_id(triplet_seed)
                    items.append(
                        {
                            "source_id": triplet_id,
                            "text": triplet_text,
                            "source_type": "triplet",
                            "from_node_id": src_id,
                            "to_node_id": tgt_id,
                            "ingestion_id": ingestion_id,
                            "company_id": company_id,
                            "project_id": project_id,
                            "content_type": content_type,
                            "repository": repository,
                            "branch": branch,
                            "file_version_id": file_version_id,
                        }
                    )

            rel_types = [
                e.get("relationship_type", "")
                for e in data.get("edges", [])
                if e.get("relationship_type")
            ]
            for rel_type, count in Counter(rel_types).items():
                edge_type_id = _cognee_id(rel_type)
                items.append(
                    {
                        "source_id": edge_type_id,
                        "text": rel_type,
                        "source_type": "edge_type",
                        "number_of_edges": count,
                        "ingestion_id": ingestion_id,
                        "company_id": company_id,
                        "project_id": project_id,
                        "content_type": content_type,
                        "repository": repository,
                        "branch": branch,
                        "file_version_id": file_version_id,
                    }
                )

            entity_types = [
                e.get("entity_type", "") for e in data.get("entities", []) if e.get("entity_type")
            ]
            for entity_type, count in Counter(entity_types).items():
                entity_type_id = _cognee_id(entity_type)
                items.append(
                    {
                        "source_id": entity_type_id,
                        "text": entity_type,
                        "source_type": "entity_type",
                        "number_of_entities": count,
                        "ingestion_id": ingestion_id,
                        "company_id": company_id,
                        "project_id": project_id,
                        "content_type": content_type,
                        "repository": repository,
                        "branch": branch,
                        "file_version_id": file_version_id,
                    }
                )
        elif source_type == "summary":
            summary_text = data.get("summary_text", "")
            summary_id = data.get("summary_id", data.get("chunk_id", ""))
            if summary_text and summary_id:
                items.append(
                    {
                        "source_id": summary_id,
                        "text": summary_text,
                        "source_type": "summary",
                        "ingestion_id": ingestion_id,
                        "company_id": company_id,
                        "project_id": project_id,
                        "content_type": content_type,
                        "repository": repository,
                        "branch": branch,
                        "file_version_id": file_version_id,
                    }
                )

        return items

    async def _process_batch(self, batch: List[tuple], worker_id: int) -> None:
        """Process batch: embed all texts → upsert → publish → check completion."""
        if not batch:
            return

        t0 = time.time()
        items = []
        source_types = []
        for work_item in batch:
            item, source_type = work_item
            items.append(item)
            source_types.append(source_type)

        valid_items = []
        valid_source_types = []
        for item, source_type in zip(items, source_types):
            if item.get("text") and item.get("source_id"):
                valid_items.append(item)
                valid_source_types.append(source_type)
            else:
                logger.warning(
                    "worker.invalid_item_skipped",
                    worker_id=worker_id,
                    source_id=item.get("source_id", "unknown"),
                )

        if not valid_items:
            return

        texts = [item["text"] for item in valid_items]
        ingestion_ids = {item["ingestion_id"] for item in valid_items}

        for ingestion_id in ingestion_ids:
            entity_count = sum(
                1
                for item, st in zip(valid_items, valid_source_types)
                if item["ingestion_id"] == ingestion_id and st == "entity"
            )
            summary_count = sum(
                1
                for item, st in zip(valid_items, valid_source_types)
                if item["ingestion_id"] == ingestion_id and st == "summary"
            )
            triplet_count = sum(
                1
                for item, st in zip(valid_items, valid_source_types)
                if item["ingestion_id"] == ingestion_id and st == "triplet"
            )
            edge_type_count = sum(
                1
                for item, st in zip(valid_items, valid_source_types)
                if item["ingestion_id"] == ingestion_id and st == "edge_type"
            )
            entity_type_count = sum(
                1
                for item, st in zip(valid_items, valid_source_types)
                if item["ingestion_id"] == ingestion_id and st == "entity_type"
            )
            if entity_count > 0:
                await self._processor._store.increment_counter(
                    ingestion_id, "entities_received", entity_count
                )
            if summary_count > 0:
                await self._processor._store.increment_counter(
                    ingestion_id, "summaries_received", summary_count
                )
            if triplet_count > 0:
                await self._processor._store.increment_counter(
                    ingestion_id, "triplets_received", triplet_count
                )
            if edge_type_count > 0:
                await self._processor._store.increment_counter(
                    ingestion_id, "edge_types_received", edge_type_count
                )
            if entity_type_count > 0:
                await self._processor._store.increment_counter(
                    ingestion_id, "entity_types_received", entity_type_count
                )

        try:
            embeddings = await self._processor._embed_texts_batched(texts)

            if len(embeddings) != len(valid_items):
                logger.error(
                    "worker.embedding_count_mismatch",
                    worker_id=worker_id,
                    expected=len(valid_items),
                    received=len(embeddings),
                )
                return

            for ingestion_id in ingestion_ids:
                count = sum(1 for item in valid_items if item["ingestion_id"] == ingestion_id)
                await self._processor._store.increment_counter(
                    ingestion_id, "embeddings_computed", count
                )

            duration = round(time.time() - t0, 3)

            for item, source_type, embedding in zip(valid_items, valid_source_types, embeddings):
                payload = EmbeddingPayload(
                    source_id=item["source_id"],
                    source_type=source_type,
                    text=item["text"],
                    embedding=embedding,
                    entity_type=item.get("entity_type", ""),
                    description=item.get("description", ""),
                )

                event = EmbeddingReadyEvent(
                    ingestion_id=item["ingestion_id"],
                    company_id=item["company_id"],
                    project_id=item["project_id"] or None,
                    content_type=item.get("content_type", "code"),
                    file_version_id=item.get("file_version_id", ""),
                    repository=item.get("repository", ""),
                    branch=item.get("branch", ""),
                    embeddings=[payload],
                    embedding_duration_s=duration,
                )

                await self._publish_event(self._config.KAFKA_OUTPUT_TOPIC, event)

            logger.debug(
                "worker.batch_published",
                worker_id=worker_id,
                batch_size=len(valid_items),
                duration_s=duration,
            )

            for item in valid_items:
                ingestion_id = item["ingestion_id"]
                async with self._state_lock:
                    already_done = ingestion_id in self._completed_ingestions
                if not already_done:
                    await self._check_and_emit_completion(
                        ingestion_id, item["company_id"], item["project_id"]
                    )

        except Exception as e:
            logger.error(
                "worker.batch_processing_failed",
                worker_id=worker_id,
                batch_size=len(valid_items),
                error=str(e),
                exc_info=True,
            )

    async def _check_and_emit_completion(
        self,
        ingestion_id: str,
        company_id: str,
        project_id: str,
    ) -> None:
        """Check if all items for an ingestion are embedded; emit completion."""
        try:
            is_complete, received, total = await self._processor.check_ingestion_complete(
                ingestion_id
            )

            if is_complete and total is not None:
                async with self._state_lock:
                    self._completed_ingestions.add(ingestion_id)

                counters = await self._processor._store.get_all_counters(ingestion_id)

                completion_event = PipelineEvent(
                    event_type="embedding_complete",
                    ingestion_id=ingestion_id,
                    company_id=company_id,
                    project_id=project_id,
                    entities_received=counters.get("entities_received", 0),
                    summaries_received=counters.get("summaries_received", 0),
                    embeddings_computed=counters.get("embeddings_computed", 0),
                )

                await self._publish_event(self._config.KAFKA_EVENTS_TOPIC, completion_event)
                await self._processor._store.finalize_counters(ingestion_id)

                logger.info(
                    "consumer.embedding_complete",
                    ingestion_id=ingestion_id,
                    items_processed=received,
                    total_embeddings=counters.get("embeddings_computed", 0),
                )
            else:
                logger.debug(
                    "consumer.ingestion_in_progress",
                    ingestion_id=ingestion_id,
                    items_received=received,
                )
        except Exception as e:
            logger.warning(
                "consumer.completion_check_error", ingestion_id=ingestion_id, error=str(e)
            )

    async def _publish_event(self, topic: str, event: Any) -> None:
        """Publish a Pydantic model as JSON to a Kafka topic."""
        if self._producer is None:
            logger.error("consumer.producer_not_initialized")
            return

        try:
            await self._producer.send_and_wait(topic, event)
        except Exception as e:
            logger.error("consumer.publish_error", topic=topic, error=str(e))

    def _deserialize_message(self, raw_value: bytes) -> dict:
        """Deserialize Kafka message bytes to dict."""
        try:
            return json.loads(raw_value.decode("utf-8"))
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

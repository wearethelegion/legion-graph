"""Entrypoint for Neo4j Storage Service (Service 6) — Streaming Version.

Consumes from extracted-entities Kafka topic and writes entities/edges to Neo4j
as they arrive, in micro-batches.

Two-stage pipeline:
- Collector: builds maximally-full batches from work_queue
- Workers: write batch to Neo4j → update counters → publish completion

Usage:
    python -m neo4j_storage_service.main
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Set, Union
from uuid import UUID, uuid5, NAMESPACE_OID

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase

from .multi_tenancy import ensure_neo4j_database

from .config import Neo4jStorageConfig
from .models import DeleteEvent, ExtractedEntitiesEvent, PipelineEvent, TextSummaryEvent
from .pipeline_store import Neo4jPipelineStore
from .canonicalisation import (
    CanonicalisationOutcome,
    canonicalise_document_entities,
    rewrite_document_batch_references,
)
from shared.canonicaliser import (
    canonicalise_entity as _canonicalise_entity,
    build_business_domain_whitelist as _build_business_domain_whitelist,
)
from .writer import Neo4jBatchWriter, _build_node_set
from .writer_hierarchy import Neo4jHierarchyWriter
from shared.project_name_resolver import ProjectNameResolver
from .cognee_context import (
    build_cognee_context,
    convert_entities_to_datapoints,
    convert_chunks_to_datapoints,
    convert_entity_types_to_datapoints,
    convert_edges_to_tuples,
    convert_documents_to_datapoints,
    convert_summaries_to_datapoints,
    _cognee_id,
)

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


class Neo4jStorageService:
    """Streaming Neo4j storage service.

    Consumes from extracted-entities topic, writes to Neo4j in micro-batches.
    """

    def __init__(
        self,
        store: Neo4jPipelineStore,
        writer: Neo4jBatchWriter,
        config: type[Neo4jStorageConfig] = Neo4jStorageConfig,
        project_resolver: Optional[ProjectNameResolver] = None,
        hierarchy_writer: Optional[Neo4jHierarchyWriter] = None,
    ) -> None:
        self._store = store
        self._writer = writer
        self._hierarchy_writer: Optional[Neo4jHierarchyWriter] = hierarchy_writer
        self._config = config
        self._project_resolver: Optional[ProjectNameResolver] = project_resolver
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Two-stage pipeline queues
        self._work_queue: Optional[asyncio.Queue] = None
        self._batch_queue: Optional[asyncio.Queue] = None
        self._collector_task: Optional[asyncio.Task] = None
        self._workers: List[asyncio.Task] = []

        # Ingestion tracking for completion signals
        self._ingestion_chunks: Dict[str, Set[str]] = {}  # ingestion_id -> set(chunk_ids)
        self._state_lock = asyncio.Lock()

        # BusinessDomain whitelist cache, keyed by company_id.  Each value is a
        # dict[normalised_key -> canonical_name] suitable for direct use by
        # ``shared.canonicaliser.canonicalise_entity``.  Populated lazily from
        # ``code_processing.company_business_domains`` and reused across batches.
        # Cache is process-lifetime — analyse_project re-runs are rare and
        # warrant a service restart anyway.
        self._business_domain_whitelist_cache: Dict[str, Dict[str, str]] = {}

    async def _load_business_domain_whitelist(self, company_id: str) -> Dict[str, str]:
        """Return the whitelist dict for *company_id*, loading on cache miss.

        Returns an empty dict if no rows exist for the company — callers
        treat that as "no enforcement", same as the canonicaliser's
        ``business_domain_whitelist=None`` legacy path.
        """
        if not company_id:
            return {}
        cached = self._business_domain_whitelist_cache.get(company_id)
        if cached is not None:
            return cached

        try:
            rows = await self._store._pool.fetch(
                """
                SELECT canonical_name
                  FROM code_processing.company_business_domains
                 WHERE company_id = $1
                """,
                company_id,
            )
        except Exception as exc:
            logger.warning(
                "canonicaliser.whitelist_load_failed",
                company_id=company_id,
                error=str(exc),
            )
            self._business_domain_whitelist_cache[company_id] = {}
            return {}

        whitelist = _build_business_domain_whitelist([dict(r) for r in rows])
        self._business_domain_whitelist_cache[company_id] = whitelist
        logger.info(
            "canonicaliser.whitelist_loaded",
            company_id=company_id,
            size=len(whitelist),
        )
        return whitelist

    async def start(self) -> None:
        """Initialize Kafka consumer, producer, collector, and batch workers."""
        if self._consumer is not None:
            logger.warning("service.already_started")
            return

        logger.info(
            "service.starting",
            data_topic=self._config.KAFKA_DATA_TOPIC,
            summaries_topic=self._config.KAFKA_SUMMARIES_TOPIC,
            enriched_chunks_topic=self._config.KAFKA_ENRICHED_CHUNKS_TOPIC,
            workers=self._config.STREAMING_WORKERS,
            batch_size=self._config.BATCH_SIZE,
        )

        self._consumer = AIOKafkaConsumer(
            self._config.KAFKA_DATA_TOPIC,
            self._config.KAFKA_SUMMARIES_TOPIC,
            self._config.KAFKA_ENRICHED_CHUNKS_TOPIC,
            bootstrap_servers=self._config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self._config.KAFKA_CONSUMER_GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset=self._config.KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=self._deserialize_event,
        )

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=self._serialize_event,
        )

        await self._consumer.start()
        await self._producer.start()

        # Initialize two-stage pipeline
        self._work_queue = asyncio.Queue(maxsize=self._config.STREAMING_WORKERS * 2)
        self._batch_queue = asyncio.Queue(maxsize=self._config.STREAMING_WORKERS)

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

        # Signal collector to stop
        if self._work_queue:
            await self._work_queue.put(_SHUTDOWN_SENTINEL)

        # Wait for collector to finish
        if self._collector_task:
            logger.info("service.waiting_for_collector")
            await self._collector_task

        # Wait for all workers to finish
        if self._workers:
            logger.info("service.waiting_for_workers", count=len(self._workers))
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()

        # Stop Kafka components
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
        """Main consumption loop — feeds work queue with entity and summary events."""
        if self._consumer is None or self._work_queue is None:
            raise RuntimeError("Service not started. Call start() first.")

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
                        if event is None:
                            continue

                        # Delete events are handled immediately (not batched)
                        if isinstance(event, DeleteEvent):
                            await self._handle_delete_event(event)
                            continue

                        if not isinstance(event, (ExtractedEntitiesEvent, TextSummaryEvent)):
                            logger.error("consumer.invalid_event_type", type=str(type(event)))
                            continue

                        # Feed event to work queue (both types handled by workers)
                        await self._work_queue.put(event)

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
                    # Drain remaining items
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

                    # Send shutdown signals to all workers
                    for _ in range(self._config.STREAMING_WORKERS):
                        await self._batch_queue.put(_SHUTDOWN_SENTINEL)

                    break

                batch = [first_item]

                # Try to fill batch up to BATCH_SIZE
                while len(batch) < self._config.BATCH_SIZE:
                    try:
                        work_item = self._work_queue.get_nowait()
                        if work_item is _SHUTDOWN_SENTINEL:
                            await self._work_queue.put(_SHUTDOWN_SENTINEL)
                            break
                        batch.append(work_item)
                    except asyncio.QueueEmpty:
                        # Wait up to COLLECT_TIMEOUT for more items
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
        """Batch worker: write batch to Neo4j → update counters."""
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

    async def _process_batch(self, batch: List, worker_id: int) -> None:
        """Process a batch of entity and/or summary events: write to Neo4j."""
        if not batch:
            return

        t0 = time.time()

        # Split batch by event type
        entity_events = [e for e in batch if isinstance(e, ExtractedEntitiesEvent)]
        summary_events = [e for e in batch if isinstance(e, TextSummaryEvent)]

        # Process summary events
        if summary_events:
            await self._process_summary_batch(summary_events, worker_id)

        # Process entity events (original logic)
        if not entity_events:
            duration = round(time.time() - t0, 2)
            logger.info(
                "worker.batch_complete",
                worker_id=worker_id,
                batch_size=len(batch),
                total_nodes=0,
                total_edges=0,
                duration_s=duration,
            )
            return

        # Group by company_id for multi-tenant database routing
        by_company: Dict[str, List[ExtractedEntitiesEvent]] = {}
        for event in entity_events:
            by_company.setdefault(event.company_id, []).append(event)

        total_nodes = 0
        total_edges = 0

        # Process each company's events separately
        for company_id, company_events in by_company.items():
            # Ensure Neo4j database exists for this company
            await ensure_neo4j_database(company_id)
            db_name = f"cognee-{company_id}"

            # Initialize constraints (idempotent)
            try:
                await self._writer.ensure_constraints(database=db_name)
            except Exception as e:
                logger.warning(
                    "worker.constraint_init_warning",
                    worker_id=worker_id,
                    company_id=company_id,
                    error=str(e),
                )

            # Aggregate all entities, edges, chunks from this batch
            all_entities = []
            all_edges = []
            all_chunks = []
            entity_types_by_set: Dict[str, set[str]] = {}
            entity_chunk_mappings = []
            entity_document_mappings = []  # Entity → Document (via file_path)

            # Pre-resolve project names for this batch (cached after first lookup)
            project_names: Dict[str, str] = {}
            if self._project_resolver:
                unique_pids = {e.project_id for e in company_events if e.project_id}
                for pid in unique_pids:
                    project_names[pid] = await self._project_resolver.resolve(pid)

            document_events = [
                event
                for event in company_events
                if getattr(event, "content_type", "") == "document"
            ]

            for event in company_events:
                # Detect content_type for document routing (T4: Knowledge graph storage)
                content_type = getattr(event, "content_type", "") or "code"

                start_line = getattr(event, "start_line", 0)
                end_line = getattr(event, "end_line", 0)

                chunk_info = {
                    "chunk_id": event.chunk_id,
                    "company_id": event.company_id,
                    "project_id": event.project_id,
                    "project_name": project_names.get(event.project_id, event.project_id),
                    "file_version_id": event.file_version_id,
                    "file_path": getattr(event, "file_path", ""),
                    "repository": getattr(event, "repository", ""),
                    "branch": getattr(event, "branch", ""),
                    "language": getattr(event, "language", ""),
                    "document_title": getattr(event, "document_title", None),
                    "document_slug": getattr(event, "document_slug", None),
                    "chunk_index": getattr(event, "chunk_index", 0),
                    "description": getattr(event, "description", None)
                    or f"Chunk {getattr(event, 'chunk_index', 0)} from {getattr(event, 'file_path', '')}",
                    "text": getattr(event, "chunk_text", ""),
                    "start_line": start_line,
                    "end_line": end_line,
                    "content_type": content_type,  # Pass through for later routing
                }
                all_chunks.append(chunk_info)

                for entity in event.entities:
                    entity_node_set = _build_node_set(
                        {
                            "content_type": content_type,
                            "company_id": event.company_id,
                            "project_id": event.project_id,
                            "project_name": project_names.get(event.project_id, event.project_id),
                        }
                    )
                    entity_dict = {
                        "entity_id": entity.entity_id,
                        "name": entity.name,
                        "entity_type": entity.entity_type,
                        "description": entity.description,
                        "company_id": event.company_id,
                        "project_id": event.project_id,
                        "project_name": project_names.get(event.project_id, event.project_id),
                        "file_version_id": event.file_version_id,
                        "branch": getattr(event, "branch", ""),
                        "file_path": getattr(event, "file_path", ""),
                        "content_type": content_type,  # propagate for node_set routing
                        "node_set": entity_node_set,
                        "properties": entity.properties,
                    }
                    all_entities.append(entity_dict)
                    entity_types_by_set.setdefault(entity_node_set, set()).add(entity.entity_type)

                    # Create entity-chunk mapping for 'contains' edge
                    entity_chunk_mappings.append(
                        {
                            "chunk_id": event.chunk_id,
                            "entity_id": entity.entity_id,
                        }
                    )

                    # Create entity-document mapping for 'is_part_of' edge
                    file_path = getattr(event, "file_path", "")
                    if file_path:
                        source_node_set = _build_node_set(
                            {
                                "content_type": content_type,
                                "company_id": event.company_id,
                                "project_id": event.project_id,
                                "project_name": project_names.get(
                                    event.project_id, event.project_id
                                ),
                            }
                        )

                        entity_document_mappings.append(
                            {
                                "entity_id": entity.entity_id,
                                "file_path": file_path,
                                "source_node_set": source_node_set,
                                "project_id": event.project_id,
                            }
                        )

                for edge in event.edges:
                    # Filter self-referencing edges early (e.g. Client→Client)
                    if str(edge.source_id) == str(edge.target_id):
                        continue
                    edge_dict = {
                        "source_id": edge.source_id,
                        "target_id": edge.target_id,
                        "relationship_type": edge.relationship_type,
                        "source_name": edge.source_name,
                        "target_name": edge.target_name,
                        "properties": edge.properties,
                    }
                    all_edges.append(edge_dict)

            # Deduplicate entities and edges across events in this batch
            seen_entities = {}
            for e in all_entities:
                seen_entities[e["entity_id"]] = e  # last-write wins (same ID = same entity)
            all_entities = [
                e
                for e in seen_entities.values()
                if e.get("entity_type", "").lower() not in ("repository", "file")
            ]

            seen_edges = {}
            for e in all_edges:
                src = e.get("source_entity_id") or e.get("source_id", "")
                tgt = e.get("target_entity_id") or e.get("target_id", "")
                key = f"{src}:{e.get('relationship_type', '')}:{tgt}"
                seen_edges[key] = e
            all_edges = list(seen_edges.values())

            # Rewrite edges targeting LLM-hallucinated Repository entities
            # → redirect them to the real Repository node from pipeline metadata
            filtered_repo_ids = {
                e["entity_id"]
                for e in seen_entities.values()
                if e.get("entity_type", "").lower() == "repository"
            }

            # Collect filtered File entity IDs and map them to real Document node IDs
            filtered_file_ids = {}
            for e in seen_entities.values():
                if e.get("entity_type", "").lower() == "file":
                    # File entity name is typically a file path — map to Document node ID
                    file_name = e.get("name", "")
                    first_event = company_events[0]
                    doc_id = str(_cognee_id(f"{first_event.project_id or company_id}:{file_name}"))
                    filtered_file_ids[e["entity_id"]] = doc_id

            if filtered_repo_ids or filtered_file_ids:
                first_event = company_events[0]
                real_repo_id = str(_cognee_id(f"repo:{first_event.project_id or company_id}"))
                valid_entity_ids = {e["entity_id"] for e in all_entities}
                cleaned_edges = []
                for e in all_edges:
                    src = e.get("source_entity_id") or e.get("source_id", "")
                    tgt = e.get("target_entity_id") or e.get("target_id", "")
                    # Drop edges FROM fake repos (entity shouldn't be a source)
                    if src in filtered_repo_ids:
                        continue
                    # Drop edges FROM fake files (entity shouldn't be a source)
                    if src in filtered_file_ids:
                        continue
                    # Rewrite edges TO fake repos → point to real repo
                    if tgt in filtered_repo_ids:
                        tgt_key = "target_entity_id" if "target_entity_id" in e else "target_id"
                        e[tgt_key] = real_repo_id
                    # Rewrite edges TO fake files → point to real Document node
                    if tgt in filtered_file_ids:
                        tgt_key = "target_entity_id" if "target_entity_id" in e else "target_id"
                        e[tgt_key] = filtered_file_ids[tgt]
                    cleaned_edges.append(e)
                all_edges = cleaned_edges

            # ── Canonicaliser pass (business-tag types only) ──────────────────
            # Run before any Neo4j writes so rejected entities never reach the DB.
            # Builds a rejected_ids set so orphaned edges can be dropped cleanly.
            #
            # Whitelist enforcement: load the company's BusinessDomain whitelist
            # from Postgres (cached per-company).  When present, BusinessDomain
            # values outside the whitelist are rejected and casing/punctuation
            # drift is force-corrected to the whitelist canonical form.
            _bd_whitelist = await self._load_business_domain_whitelist(company_id)
            rejected_ids: set[str] = set()
            canonicalised_entities: list[dict] = []
            for _entity in all_entities:
                _canon = _canonicalise_entity(
                    _entity,
                    business_domain_whitelist=_bd_whitelist or None,
                )
                if _canon is None:
                    _eid = str(_entity.get("entity_id", ""))
                    rejected_ids.add(_eid)
                    logger.info(
                        "canonicaliser.rejected",
                        entity_type=_entity.get("entity_type"),
                        name=_entity.get("name"),
                        entity_id=_eid,
                        company_id=company_id,
                    )
                else:
                    if _canon is not _entity:
                        logger.info(
                            "canonicaliser.normalised",
                            entity_type=_entity.get("entity_type"),
                            old_name=_entity.get("name"),
                            new_name=_canon.get("name"),
                            company_id=company_id,
                        )
                    canonicalised_entities.append(_canon)

            if rejected_ids:
                # Drop edges where source or target was rejected
                pre_edge_count = len(all_edges)
                all_edges = [
                    _e
                    for _e in all_edges
                    if (
                        str(_e.get("source_entity_id") or _e.get("source_id", ""))
                        not in rejected_ids
                        and str(_e.get("target_entity_id") or _e.get("target_id", ""))
                        not in rejected_ids
                    )
                ]
                dropped_edges = pre_edge_count - len(all_edges)
                if dropped_edges:
                    logger.info(
                        "canonicaliser.orphan_edges_dropped",
                        count=dropped_edges,
                        company_id=company_id,
                    )
                # Also drop chunk/document mappings for rejected entities
                entity_chunk_mappings = [
                    m
                    for m in entity_chunk_mappings
                    if str(m.get("entity_id", "")) not in rejected_ids
                ]
                entity_document_mappings = [
                    m
                    for m in entity_document_mappings
                    if str(m.get("entity_id", "")) not in rejected_ids
                ]

            all_entities = canonicalised_entities

            # Write to Neo4j in phases
            try:
                # Phase 1: Write EntityType nodes (list of strings, not dicts)
                entity_types = [
                    {"name": t, "node_set": node_set}
                    for node_set, types in entity_types_by_set.items()
                    for t in types
                    if t.lower() not in ("repository", "file")
                ]
                entity_type_count = await self._writer.write_entity_type_nodes(
                    entity_types, database=db_name
                )

                # Phase 2: Write Entity nodes (deduplicated, post-canonicalisation)
                entity_count = await self._writer.write_entity_nodes(all_entities, database=db_name)

                # ── Canonicalisation gated by ENABLE_DOCUMENT_CANONICALISATION env flag ──
                #
                # TODO(canonicalisation): The current canonicaliser is disabled by default
                # because:
                #   1. It runs synchronously on the write path and gates every batch on a
                #      full Entity scan + LLM merge calls. With ~7k+ entities this can
                #      block longer than the Kafka session timeout, kicking the consumer
                #      out of its group and stalling the entire neo4j-storage pipeline
                #      (both code AND document writes).
                #   2. Its merge rules damage identifier-grade names by splitting CamelCase
                #      ("PostgreSQL" -> "Postgre SQL", "MultiEdit" -> "Multi Edit",
                #      "pg_repack" -> "Pg Repack") and over-eager Levenshtein-<=-2 merges
                #      conflate genuinely-different concepts ("LLM Error Handling" got
                #      merged into "MCP Tool Error Handling").
                #
                # When we revisit this we want a replacement that:
                #   - runs OFF the hot write path (background job or in-memory only),
                #   - uses a key like `" ".join(name.lower().split())` so CamelCase stays
                #     intact and `OpenAI` / `Open AI` / `OPENAI` still collapse to one,
                #   - uses RELATIVE Levenshtein with a min-length floor (no absolute <=2
                #     thresholds on short names like "Allen"/"Alen"),
                #   - has a vendor/product whitelist for canonical product names.
                #
                # Set ENABLE_DOCUMENT_CANONICALISATION=true to re-enable the legacy path
                # if you want to compare behaviour.
                if os.getenv("ENABLE_DOCUMENT_CANONICALISATION", "false").lower() == "true":
                    canonicalisation_outcome = await canonicalise_document_entities(
                        self._writer,
                        company_id=company_id,
                        database=db_name,
                        document_entities=[
                            entity
                            for entity in all_entities
                            if entity.get("content_type") == "document"
                        ],
                        content_type="document" if document_events else "code",
                    )
                else:
                    # No-op outcome: empty mappings → rewrite_document_batch_references
                    # returns inputs unchanged.
                    canonicalisation_outcome = CanonicalisationOutcome()

                all_entities, all_edges, entity_chunk_mappings, entity_document_mappings = (
                    rewrite_document_batch_references(
                        all_entities,
                        all_edges,
                        entity_chunk_mappings,
                        entity_document_mappings,
                        canonicalisation_outcome,
                    )
                )

                # Phase 3: Write DocumentChunk nodes
                chunk_count = await self._writer.write_chunk_nodes(all_chunks, database=db_name)

                # Phase 3-backfill: Backfill summary edges for newly created chunks
                # Summary events may arrive before entity events, creating orphaned
                # TextSummary nodes with no edges. Now that chunks exist, create missing edges.
                chunk_ids = [str(c["chunk_id"]) for c in all_chunks]
                backfill_count = await self._writer.backfill_summary_edges(
                    chunk_ids, database=db_name
                )
                if backfill_count > 0:
                    logger.info(
                        "worker.summary_edges_backfilled",
                        count=backfill_count,
                        worker_id=worker_id,
                        company_id=company_id,
                    )

                # Phase 3a: Write Repository node (one per project)
                repo_count = await self._writer.write_repository_node(all_chunks, database=db_name)

                # Phase 3b: Write Document nodes (one per file_path) + is_part_of edges
                document_count = await self._writer.write_document_nodes(
                    all_chunks, database=db_name
                )

                # Phase 4: Write LLM-extracted edges (deduplicated)
                llm_edge_count = await self._writer.write_llm_edges(all_edges, database=db_name)

                # Build entity_id -> name lookup for edge_text generation
                entity_names = {e["entity_id"]: e["name"] for e in all_entities}

                # Phase 5: Write 'contains' edges (DocumentChunk -> Entity)
                contains_count = await self._writer.write_contains_edges(
                    entity_chunk_mappings, database=db_name, entity_names=entity_names
                )

                # Phase 6: Write 'made_from' edges (Entity -> DocumentChunk)
                made_from_count = await self._writer.write_made_from_edges(
                    entity_chunk_mappings, database=db_name, entity_names=entity_names
                )

                # Phase 7: Write 'is_a' edges (Entity -> EntityType)
                is_a_count = await self._writer.write_is_a_edges(all_entities, database=db_name)

                # Phase 7b: Write NodeSet nodes + belongs_to_set edges
                node_set_count = await self._writer.write_node_sets(all_chunks, database=db_name)

                # Phase 8: Write 'is_part_of' edges (Entity -> Document)
                entity_doc_count = await self._writer.write_entity_document_edges(
                    entity_document_mappings, database=db_name
                )

                # ── Phase 4.1: Hierarchy nodes (Company/Project/Branch) ──────
                hierarchy_nodes = 0
                if self._hierarchy_writer:
                    try:
                        first_event = company_events[0]
                        _branch = getattr(first_event, "branch", "main")
                        if first_event.project_id:
                            _pname = project_names.get(
                                first_event.project_id, first_event.project_id
                            )

                            # Write Company → Project → Branch hierarchy
                            hierarchy_nodes += await self._hierarchy_writer.write_hierarchy(
                                company_id=company_id,
                                project_id=first_event.project_id,
                                branch_name=_branch,
                                project_name=_pname,
                                database=db_name,
                            )

                            # Write Entity -[:exists_on]-> Branch edges
                            entity_branch_mappings = [
                                {
                                    "entity_id": e["entity_id"],
                                    "project_id": e.get("project_id", first_event.project_id),
                                    "project_name": e.get("project_name", _pname),
                                    "branch_name": _branch,
                                }
                                for e in all_entities
                            ]
                            await self._hierarchy_writer.write_entity_branch_edges(
                                entity_branch_mappings, database=db_name
                            )

                            # Write business domain nodes if present in events
                            all_business_domains = []
                            all_technical_tags: set = set()

                            for event in company_events:
                                if hasattr(event, "business_domains") and event.business_domains:
                                    all_business_domains.extend(event.business_domains)
                                if hasattr(event, "technical_tags") and event.technical_tags:
                                    for tag in event.technical_tags:
                                        all_technical_tags.add(tag)

                            if all_business_domains:
                                # Deduplicate by normalised_key
                                seen_keys = {}
                                for d in all_business_domains:
                                    key = d.get("key", d.get("canonical_name", "").lower())
                                    if key and key not in seen_keys:
                                        seen_keys[key] = d
                                unique_domains = list(seen_keys.values())
                                hierarchy_nodes += (
                                    await self._hierarchy_writer.write_business_domain_nodes(
                                        unique_domains, company_id=company_id, database=db_name
                                    )
                                )

                            if all_technical_tags:
                                hierarchy_nodes += (
                                    await self._hierarchy_writer.write_technical_domain_nodes(
                                        list(all_technical_tags),
                                        project_id=first_event.project_id,
                                        project_name=_pname,
                                        database=db_name,
                                    )
                                )

                            # Write CodeBlock nodes from chunk text (one per entity).
                            # Knowledge events (content_type='document') have project_id=None;
                            # propagate content_type + company_id so write_code_block_nodes can
                            # build the correct knowledge node_set instead of falling back to
                            # the malformed "unknown_unknown_code" sentinel.
                            code_blocks = []
                            for event in company_events:
                                chunk_text = getattr(event, "chunk_text", "")
                                start_line = getattr(event, "start_line", 0)
                                end_line = getattr(event, "end_line", 0)
                                event_content_type = (
                                    getattr(event, "content_type", "code") or "code"
                                )
                                event_company_id = getattr(event, "company_id", "") or ""
                                for entity in event.entities:
                                    code_blocks.append(
                                        {
                                            "entity_id": entity.entity_id,
                                            "text": chunk_text,
                                            "start_line": start_line,
                                            "end_line": end_line,
                                            "file_path": getattr(event, "file_path", ""),
                                            "language": getattr(event, "language", ""),
                                            "file_version_id": event.file_version_id,
                                            "company_id": event_company_id,
                                            "content_type": event_content_type,
                                            "project_id": event.project_id,
                                            "project_name": project_names.get(
                                                event.project_id, event.project_id
                                            ),
                                            "branch": _branch,
                                        }
                                    )
                            if code_blocks:
                                hierarchy_nodes += (
                                    await self._hierarchy_writer.write_code_block_nodes(
                                        code_blocks, database=db_name
                                    )
                                )

                    except Exception as e:
                        # Hierarchy writes are non-critical — log and continue
                        logger.error(
                            "worker.hierarchy_write_failed",
                            worker_id=worker_id,
                            company_id=company_id,
                            error=str(e),
                            exc_info=True,
                        )

                batch_nodes = (
                    entity_count
                    + entity_type_count
                    + chunk_count
                    + document_count
                    + node_set_count
                    + hierarchy_nodes
                )
                batch_edges = (
                    llm_edge_count
                    + contains_count
                    + made_from_count
                    + is_a_count
                    + entity_doc_count
                )
                total_nodes += batch_nodes
                total_edges += batch_edges

                logger.info(
                    "worker.batch_written",
                    worker_id=worker_id,
                    company_id=company_id,
                    events=len(company_events),
                    nodes=batch_nodes,
                    edges=batch_edges,
                    entities=entity_count,
                    entity_types=entity_type_count,
                    chunks=chunk_count,
                    documents=document_count,
                    node_sets=node_set_count,
                )

                # Phase 8: Write to Cognee Postgres (nodes and edges tables)
                try:
                    # Build Cognee context (User, Dataset, Data)
                    # Use first event to get project_id
                    first_event = company_events[0]
                    context = await build_cognee_context(
                        company_id=company_id,
                        project_id=first_event.project_id,
                        branch=getattr(first_event, "branch", "main"),
                        content_type=getattr(first_event, "content_type", "code"),
                    )

                    # Import Cognee upsert methods
                    from cognee.modules.graph.methods.upsert_nodes import upsert_nodes
                    from cognee.modules.graph.methods.upsert_edges import upsert_edges
                    from cognee.infrastructure.databases.relational import get_relational_engine

                    # Convert and upsert nodes
                    entity_datapoints = convert_entities_to_datapoints(all_entities)
                    chunk_datapoints = convert_chunks_to_datapoints(all_chunks)
                    # entity_types is already a list of dicts {"name", "node_set"}
                    # since the scope-aware EntityType refactor (Tommy, 2026-05-01).
                    entity_type_datapoints = convert_entity_types_to_datapoints(entity_types)
                    document_datapoints = convert_documents_to_datapoints(all_chunks)

                    all_datapoints = (
                        entity_datapoints
                        + chunk_datapoints
                        + entity_type_datapoints
                        + document_datapoints
                    )

                    # Get a Cognee DB session for Postgres writes
                    db_engine = get_relational_engine()
                    async with db_engine.get_async_session() as session:
                        if all_datapoints:
                            await upsert_nodes(
                                all_datapoints,
                                context["user"].tenant_id,
                                context["user"].id,
                                context["dataset"].id,
                                context["data"].id,
                                session,
                            )

                        # Convert and upsert edges
                        # 1. LLM-extracted edges
                        llm_edge_tuples = convert_edges_to_tuples(all_edges)

                        # 2. Contains edges (chunk -> entity)
                        contains_edges = [
                            (
                                self._uuid_from_str(m["chunk_id"]),
                                self._uuid_from_str(m["entity_id"]),
                                "contains",
                                {},
                            )
                            for m in entity_chunk_mappings
                        ]

                        # 3. Made_from edges (entity -> chunk) — reverse of contains
                        made_from_edges = [
                            (
                                self._uuid_from_str(m["entity_id"]),
                                self._uuid_from_str(m["chunk_id"]),
                                "made_from",
                                {},
                            )
                            for m in entity_chunk_mappings
                        ]

                        # 4. Is_a edges (entity -> entity_type)
                        is_a_edges = [
                            (
                                self._uuid_from_str(e["entity_id"]),
                                uuid5(NAMESPACE_OID, f"entity_type:{e['entity_type']}"),
                                "is_a",
                                {},
                            )
                            for e in all_entities
                        ]

                        # 5. Is_part_of edges (chunk -> document)
                        is_part_of_edges = []
                        for c in all_chunks:
                            fp = c.get("file_path", "")
                            if fp:
                                project_id = c.get("project_id") or c.get("company_id", "unknown")
                                doc_id = _cognee_id(f"{project_id}:{fp}")
                                is_part_of_edges.append(
                                    (
                                        self._uuid_from_str(c["chunk_id"]),
                                        UUID(doc_id),
                                        "is_part_of",
                                        {},
                                    )
                                )

                        # 6. Belongs_to_set edges (chunk -> NodeSet)
                        belongs_to_set_edges = []
                        for c in all_chunks:
                            node_set_name = _build_node_set(c)
                            node_set_id = _cognee_id(node_set_name)

                            belongs_to_set_edges.append(
                                (
                                    self._uuid_from_str(c["chunk_id"]),
                                    UUID(node_set_id),
                                    "belongs_to_set",
                                    {},
                                )
                            )

                        # 7. Defined_in edges (entity -> document)
                        defined_in_edges = []
                        for m in entity_document_mappings:
                            fp = m.get("file_path", "")
                            if fp:
                                project_id = m.get("project_id") or company_id
                                doc_id = _cognee_id(f"{project_id}:{fp}")
                                defined_in_edges.append(
                                    (
                                        self._uuid_from_str(m["entity_id"]),
                                        UUID(doc_id),
                                        "defined_in",
                                        {},
                                    )
                                )

                        all_edge_tuples = (
                            llm_edge_tuples
                            + contains_edges
                            + made_from_edges
                            + is_a_edges
                            + is_part_of_edges
                            + belongs_to_set_edges
                            + defined_in_edges
                        )

                        if all_edge_tuples:
                            await upsert_edges(
                                all_edge_tuples,
                                context["user"].tenant_id,
                                context["user"].id,
                                context["data"].id,
                                context["dataset"].id,
                                session,
                            )

                        await session.commit()

                    logger.info(
                        "worker.cognee_postgres_written",
                        worker_id=worker_id,
                        company_id=company_id,
                        nodes=len(all_datapoints),
                        edges=len(all_edge_tuples),
                    )

                except Exception as e:
                    # Log but don't fail the batch — Neo4j writes already succeeded
                    logger.error(
                        "worker.cognee_postgres_write_failed",
                        worker_id=worker_id,
                        company_id=company_id,
                        error=str(e),
                        exc_info=True,
                    )

                # Update counters in Postgres
                for event in company_events:
                    await self._store.increment_counter(event.ingestion_id, "chunks_processed", 1)
                    await self._store.increment_counter(
                        event.ingestion_id, "nodes_created", batch_nodes
                    )
                    await self._store.increment_counter(
                        event.ingestion_id, "edges_created", batch_edges
                    )

            except Exception as e:
                logger.error(
                    "worker.neo4j_write_failed",
                    worker_id=worker_id,
                    company_id=company_id,
                    error=str(e),
                    exc_info=True,
                )
                # Mark ingestion as failed
                for event in company_events:
                    await self._store.set_counter(event.ingestion_id, "neo4j_status", 0, "failed")

        duration = round(time.time() - t0, 2)
        logger.info(
            "worker.batch_complete",
            worker_id=worker_id,
            batch_size=len(batch),
            total_nodes=total_nodes,
            total_edges=total_edges,
            duration_s=duration,
        )

    async def _process_summary_batch(
        self, summary_events: List[TextSummaryEvent], worker_id: int
    ) -> None:
        """Process a batch of summary events: write TextSummary nodes and summarizes edges."""
        # Group by company_id for multi-tenant database routing
        by_company: Dict[str, List[TextSummaryEvent]] = {}
        for event in summary_events:
            by_company.setdefault(event.company_id, []).append(event)

        for company_id, company_summaries in by_company.items():
            await ensure_neo4j_database(company_id)
            db_name = f"cognee-{company_id}"

            try:
                await self._writer.ensure_constraints(database=db_name)
            except Exception as e:
                logger.warning(
                    "worker.summary_constraint_warning",
                    worker_id=worker_id,
                    company_id=company_id,
                    error=str(e),
                )

            # Pre-resolve project names for this summary batch
            summary_project_names: Dict[str, str] = {}
            if self._project_resolver:
                unique_pids = {e.project_id for e in company_summaries if e.project_id}
                for pid in unique_pids:
                    summary_project_names[pid] = await self._project_resolver.resolve(pid)

            # Build summary dicts for writer
            summary_dicts = []
            for event in company_summaries:
                content_type = getattr(event, "content_type", "") or "code"
                project_name = summary_project_names.get(event.project_id, event.project_id)
                source_node_set = _build_node_set(
                    {
                        "content_type": content_type,
                        "company_id": event.company_id,
                        "project_id": event.project_id,
                        "project_name": project_name,
                    }
                )

                summary_dicts.append(
                    {
                        "summary_id": event.summary_id,
                        "chunk_id": event.chunk_id,
                        "chunk_index": getattr(event, "chunk_index", 0),
                        "summary_text": event.summary_text,
                        "company_id": event.company_id,
                        "project_id": event.project_id,
                        "project_name": project_name,
                        "file_version_id": event.file_version_id,
                        "branch": getattr(event, "branch", ""),
                        "file_path": getattr(event, "file_path", ""),
                        "content_type": content_type,
                        "source_node_set": source_node_set,
                    }
                )

            try:
                # Write TextSummary nodes
                summary_count = await self._writer.write_summary_nodes(
                    summary_dicts, database=db_name
                )

                # Write summarizes edges (TextSummary -> DocumentChunk)
                summarizes_count = await self._writer.write_summarizes_edges(
                    summary_dicts, database=db_name
                )

                # Write made_from edges (TextSummary -> DocumentChunk)
                made_from_count = await self._writer.write_summary_made_from_edges(
                    summary_dicts, database=db_name
                )

                # Write is_part_of edges (TextSummary -> Document)
                summary_doc_mappings = [
                    {
                        "entity_id": s["summary_id"],
                        "file_path": s["file_path"],
                        "source_node_set": s["source_node_set"],
                    }
                    for s in summary_dicts
                    if s.get("file_path")
                ]
                summary_doc_count = await self._writer.write_entity_document_edges(
                    summary_doc_mappings, database=db_name
                )

                logger.info(
                    "worker.summaries_written",
                    worker_id=worker_id,
                    company_id=company_id,
                    events=len(company_summaries),
                    summary_nodes=summary_count,
                    summarizes_edges=summarizes_count,
                    made_from_edges=made_from_count,
                    summary_doc_edges=summary_doc_count,
                )

                # Phase: Write to Cognee Postgres
                try:
                    first_event = company_summaries[0]
                    context = await build_cognee_context(
                        company_id=company_id,
                        project_id=first_event.project_id,
                        branch=getattr(first_event, "branch", "main"),
                        content_type=getattr(first_event, "content_type", "code"),
                    )

                    from cognee.modules.graph.methods.upsert_nodes import upsert_nodes
                    from cognee.modules.graph.methods.upsert_edges import upsert_edges
                    from cognee.infrastructure.databases.relational import (
                        get_relational_engine,
                    )

                    # Convert summaries to datapoints
                    summary_datapoints = convert_summaries_to_datapoints(company_summaries)

                    db_engine = get_relational_engine()
                    async with db_engine.get_async_session() as session:
                        if summary_datapoints:
                            await upsert_nodes(
                                summary_datapoints,
                                context["user"].tenant_id,
                                context["user"].id,
                                context["dataset"].id,
                                context["data"].id,
                                session,
                            )

                        # Build summarizes edges (summary -> chunk)
                        summarizes_edges = [
                            (
                                UUID(s["summary_id"]),
                                self._uuid_from_str(s["chunk_id"]),
                                "summarizes",
                                {},
                            )
                            for s in summary_dicts
                        ]

                        if summarizes_edges:
                            await upsert_edges(
                                summarizes_edges,
                                context["user"].tenant_id,
                                context["user"].id,
                                context["data"].id,
                                context["dataset"].id,
                                session,
                            )

                        await session.commit()

                    logger.info(
                        "worker.summary_cognee_postgres_written",
                        worker_id=worker_id,
                        company_id=company_id,
                        nodes=len(summary_datapoints),
                        edges=len(summarizes_edges),
                    )
                except Exception as e:
                    logger.error(
                        "worker.summary_cognee_postgres_write_failed",
                        worker_id=worker_id,
                        company_id=company_id,
                        error=str(e),
                        exc_info=True,
                    )

            except Exception as e:
                logger.error(
                    "worker.summary_write_failed",
                    worker_id=worker_id,
                    company_id=company_id,
                    error=str(e),
                    exc_info=True,
                )

    # ── Delete Handling ──────────────────────────────────────────────

    async def _handle_delete_event(self, event: DeleteEvent) -> None:
        """Handle a delete event: remove all Neo4j data for the deleted file.

        Steps:
        1. Resolve company_id → database name
        2. Resolve file_version_id (from event or Neo4j lookup)
        3. Call writer.delete_by_file_version_id()
        4. Delete from Cognee Postgres (nodes/edges tables)
        """
        logger.info(
            "consumer.delete_received",
            file_path=event.file_path,
            company_id=event.company_id,
            project_id=event.project_id,
            repository=event.repository,
            branch=event.branch,
        )

        try:
            # Ensure database exists
            await ensure_neo4j_database(event.company_id)
            db_name = f"cognee-{event.company_id}"

            # Resolve file_version_id
            file_version_id = event.file_version_id
            if not file_version_id:
                file_version_id = await self._writer.resolve_file_version_id(
                    file_path=event.file_path,
                    project_id=event.project_id or event.company_id,
                    database=db_name,
                )

            if not file_version_id:
                logger.warning(
                    "consumer.delete_no_file_version_id",
                    file_path=event.file_path,
                    project_id=event.project_id,
                    msg="No nodes found in Neo4j for this file — nothing to delete",
                )
                return

            # Delete from Neo4j
            result = await self._writer.delete_by_file_version_id(
                file_version_id=file_version_id,
                database=db_name,
            )

            # Delete from Cognee Postgres (nodes/edges tables)
            try:
                await self._delete_from_cognee_postgres(
                    file_version_id=file_version_id,
                    company_id=event.company_id,
                    project_id=event.project_id,
                    branch=event.branch,
                )
            except Exception as e:
                # Log but don't fail — Neo4j delete already succeeded
                logger.error(
                    "consumer.delete_cognee_postgres_failed",
                    file_version_id=file_version_id,
                    error=str(e),
                    exc_info=True,
                )

            logger.info(
                "consumer.delete_complete",
                file_path=event.file_path,
                **result,
            )

        except Exception as e:
            logger.error(
                "consumer.delete_failed",
                file_path=event.file_path,
                company_id=event.company_id,
                error=str(e),
                exc_info=True,
            )

    async def _delete_from_cognee_postgres(
        self,
        file_version_id: str,
        company_id: str,
        project_id: str,
        branch: str,
    ) -> None:
        """Cognee Postgres cleanup — intentionally a no-op.

        Cognee's Node model does NOT store file_version_id in its columns.
        The V2 pipeline manages all data via Neo4j (direct graph operations)
        and Qdrant (file_version_id in payload). Cognee's relational tables
        (nodes/edges) are managed by Cognee's own add/delete APIs, not by us.

        The Neo4j delete (delete_by_file_version_id) and Qdrant delete
        (delete_by_file_version_id) handle all cleanup.
        """
        logger.debug(
            "consumer.cognee_postgres_skip",
            file_version_id=file_version_id,
            reason="cognee_node_model_has_no_file_version_id",
        )

    # ── Kafka Helpers ─────────────────────────────────────────────────

    def _deserialize_event(
        self, raw_value: bytes
    ) -> ExtractedEntitiesEvent | TextSummaryEvent | DeleteEvent | None:
        """Deserialize Kafka message bytes to the appropriate event type.

        Routes based on discriminating fields:
        - action="delete" → DeleteEvent
        - action="process" (or "embedding"/"header" in enriched-code-chunks) → None (ignored)
        - "summary_text" present → TextSummaryEvent
        - "embedding" present (enriched-code-chunks without action field) → None (legacy skip)
        - Otherwise → ExtractedEntitiesEvent
        """
        try:
            data = json.loads(raw_value.decode("utf-8"))

            # Check for enriched-code-chunks messages (have "action" field)
            action = data.get("action")
            if action is not None:
                if action == "delete":
                    return DeleteEvent(**data)
                # Non-delete enriched-code-chunks messages — skip silently
                return None

            # Legacy enriched-code-chunks messages (pre-action field) — skip.
            # These have "embedding" and/or "content" fields but no "entities".
            if "embedding" in data and "entities" not in data:
                return None

            # Route to correct model based on presence of discriminating fields
            if "summary_text" in data:
                return self._parse_summary_event(data)
            return ExtractedEntitiesEvent(**data)
        except Exception as e:
            logger.error("service.deserialize_error", error=str(e))
            return None

    def _parse_summary_event(self, data: Dict[str, Any]) -> TextSummaryEvent:
        """Parse a raw dict into a TextSummaryEvent."""
        return TextSummaryEvent(**data)

    def _serialize_event(self, event: Any) -> bytes:
        """Serialize a Pydantic model to JSON bytes for Kafka."""
        if hasattr(event, "model_dump"):
            return json.dumps(event.model_dump()).encode("utf-8")
        if hasattr(event, "dict"):
            return json.dumps(event.dict()).encode("utf-8")
        return json.dumps(event).encode("utf-8")

    def _uuid_from_str(self, uuid_str: str) -> UUID:
        """Convert string UUID to UUID object."""
        return UUID(uuid_str)

    def setup_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM for graceful shutdown."""

        def handler(signum, frame):
            logger.info("service.signal_received", signal=signum)
            self._running = False
            self._shutdown_event.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)


async def main() -> None:
    """Start the Neo4j Storage Service."""
    config = Neo4jStorageConfig
    config.validate()

    logger.info(
        "main.starting",
        neo4j_uri=config.NEO4J_URI,
        data_topic=config.KAFKA_DATA_TOPIC,
        workers=config.STREAMING_WORKERS,
        batch_size=config.BATCH_SIZE,
    )

    # Initialize Cognee DB and run migrations.
    # cognee_patches MUST be imported BEFORE any cognee imports — it sets env vars
    # (GRAPH_DATABASE_PROVIDER, etc.), registers the Qdrant adapter, and patches
    # setup() to skip pgvector table creation for non-pgvector providers.
    import cognee_service.cognee_patches  # noqa: F401 — must be first

    from cognee_service.config import configure_cognee
    from cognee.infrastructure.databases.relational.create_db_and_tables import create_db_and_tables

    configure_cognee()

    # Force-import ALL SQLAlchemy models so Base.metadata.create_all sees them.
    # Missing imports here cause DatabaseNotCreatedError at runtime because the
    # dependent tables (principals, roles, tenants, etc.) don't get created.
    from cognee.modules.users.models import (  # noqa: F401
        User,
        Role,
        Tenant,
        Permission,
        ACL,
        UserRole,
        UserTenant,
        DatasetDatabase,
        RoleDefaultPermissions,
        UserDefaultPermissions,
        TenantDefaultPermissions,
        PrincipalConfiguration,
    )
    from cognee.modules.data.models import Data, Dataset  # noqa: F401
    from cognee.modules.graph.models.Node import Node  # noqa: F401
    from cognee.modules.graph.models.Edge import Edge  # noqa: F401

    await create_db_and_tables()
    logger.info("main.cognee_tables_created")

    # Initialize Postgres connection pool
    pool = await asyncpg.create_pool(
        config.POSTGRES_DSN,
        min_size=config.POSTGRES_MIN_POOL,
        max_size=config.POSTGRES_MAX_POOL,
    )
    logger.info("main.postgres_connected")

    # Initialize store and ensure tables exist
    store = Neo4jPipelineStore(pool)
    await store.ensure_tables()
    logger.info("main.tables_ensured")

    # Initialize Neo4j driver
    neo4j_driver = AsyncGraphDatabase.driver(
        config.NEO4J_URI,
        auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
    )
    logger.info("main.neo4j_connected")

    # Initialize components
    writer = Neo4jBatchWriter(driver=neo4j_driver, config=config)
    hierarchy_writer = Neo4jHierarchyWriter(driver=neo4j_driver, config=config)
    project_resolver = ProjectNameResolver(pool)
    service = Neo4jStorageService(
        store=store,
        writer=writer,
        config=config,
        project_resolver=project_resolver,
        hierarchy_writer=hierarchy_writer,
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
            await neo4j_driver.close()
        except Exception as e:
            logger.error("cleanup.neo4j_driver_close_failed", error=str(e))
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

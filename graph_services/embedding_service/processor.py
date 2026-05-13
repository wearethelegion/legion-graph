"""Embedding processor — embeds entity names and summary texts via Gemini.

Consumes EntityInputEvent and SummaryInputEvent, calls the embedding engine
in batches of up to EMBEDDING_BATCH_SIZE (100) texts, checks content-hash
checkpoints for deduplication, and returns EmbeddingReadyEvent for Kafka publishing.
"""

import asyncio
import hashlib
import time
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid5, NAMESPACE_OID

import structlog


def _cognee_id(text: str) -> str:
    """Generate UUID5 matching Cognee's generate_node_id / generate_edge_id normalisation."""
    return str(uuid5(NAMESPACE_OID, text.lower().replace(" ", "_").replace("'", "")))


from cognee.infrastructure.databases.vector.embeddings.LiteLLMEmbeddingEngine import (
    LiteLLMEmbeddingEngine,
)

from .config import EmbeddingConfig
from .models import (
    EmbeddingPayload,
    EmbeddingReadyEvent,
    EntityInputEvent,
    SummaryInputEvent,
)
from .pipeline_store import EmbeddingStore

logger = structlog.get_logger(__name__)


class EmbeddingProcessor:
    """Embeds entity names and summary texts via Gemini 768d.

    Per batch:
    1. Collect all texts to embed (entity names or summary texts)
    2. Check content-hash checkpoints for deduplication (skip already-processed)
    3. Batch into groups of EMBEDDING_BATCH_SIZE (100)
    4. Call embedding engine with retry on rate limit (429)
    5. Return EmbeddingReadyEvent for Kafka publishing
    """

    def __init__(
        self,
        store: EmbeddingStore,
        config: type[EmbeddingConfig] = EmbeddingConfig,
        embedding_engine: Optional[LiteLLMEmbeddingEngine] = None,
    ):
        self._store = store
        self._config = config
        self._semaphore = asyncio.Semaphore(config.EMBEDDING_API_CONCURRENCY)
        self._embedding_engine = embedding_engine

    def _get_engine(self) -> LiteLLMEmbeddingEngine:
        """Lazy-init embedding engine (allows patched Cognee engine)."""
        if self._embedding_engine is None:
            from cognee.infrastructure.databases.vector.embeddings.get_embedding_engine import (
                get_embedding_engine,
            )

            self._embedding_engine = get_embedding_engine()
        return self._embedding_engine

    # -- Entity batch processing -----------------------------------------------

    async def process_entity_batch(
        self,
        events: List[EntityInputEvent],
    ) -> List[EmbeddingReadyEvent]:
        """Process a batch of entity events: embed entity names, triplets, and edge types."""
        if not events:
            return []

        t0 = time.time()
        ingestion_id = events[0].ingestion_id
        first_event = events[0]

        all_items: List[Dict[str, Any]] = []
        event_boundaries: List[int] = []

        # Collect all edge relationship_types across the whole batch for EdgeType aggregation
        all_relationship_types: List[str] = []

        for event in events:
            count = 0

            # Process entities for embedding
            for ent in event.entities:
                name = ent.get("name", "")
                entity_id = ent.get("entity_id", "")
                if name and entity_id:
                    all_items.append(
                        {
                            "source_id": entity_id,
                            "source_type": "entity",
                            "text": name,
                            "ingestion_id": event.ingestion_id,
                            "company_id": event.company_id,
                            "project_id": event.project_id,
                            "content_type": getattr(event, "content_type", "code"),
                        }
                    )
                    count += 1

            # Generate triplet texts from edges + collect relationship_types
            edges = event.edges or []
            if edges:
                # Build entity_id → name mapping for this event
                entity_map = {e.get("entity_id", ""): e.get("name", "") for e in event.entities}

                for edge in edges:
                    source_id = edge.get("source_id", "")
                    target_id = edge.get("target_id", "")
                    rel_type = edge.get("relationship_type", "")

                    # Collect for EdgeType aggregation (all edges, not just those with names)
                    if rel_type:
                        all_relationship_types.append(rel_type)

                    source_name = entity_map.get(source_id, "")
                    target_name = entity_map.get(target_id, "")

                    if source_name and target_name and rel_type:
                        # Generate triplet text using Cognee format: "{source}-›{relationship}-›{target}"
                        triplet_text = f"{source_name}-›{rel_type}-›{target_name}"

                        # Generate deterministic triplet_id (Cognee normalisation)
                        triplet_seed = f"{source_id}{rel_type}{target_id}"
                        triplet_id = _cognee_id(triplet_seed)

                        all_items.append(
                            {
                                "source_id": triplet_id,
                                "source_type": "triplet",
                                "text": triplet_text,
                                "from_node_id": source_id,
                                "to_node_id": target_id,
                                "ingestion_id": event.ingestion_id,
                                "company_id": event.company_id,
                                "project_id": event.project_id,
                                "content_type": getattr(event, "content_type", "code"),
                            }
                        )
                        count += 1

            event_boundaries.append(count)

        # Aggregate edges by relationship_type (matching Cognee's index_graph_edges.py logic)
        # One EdgeType item per distinct relationship_type across the whole batch
        edge_type_items: List[Dict[str, Any]] = []
        if all_relationship_types:
            from uuid import uuid5, NAMESPACE_OID

            edge_type_counts = Counter(all_relationship_types)
            for rel_type, edge_count in edge_type_counts.items():
                # Deterministic ID: uuid5(NAMESPACE_OID, relationship_type) — same as Cognee
                edge_type_id = _cognee_id(rel_type)
                edge_type_items.append(
                    {
                        "source_id": edge_type_id,
                        "source_type": "edge_type",
                        "text": rel_type,
                        "number_of_edges": edge_count,
                        "ingestion_id": first_event.ingestion_id,
                        "company_id": first_event.company_id,
                        "project_id": first_event.project_id,
                        "content_type": getattr(first_event, "content_type", "code"),
                    }
                )

        total_items = len(all_items)
        if total_items > 0:
            # Count entities and triplets separately
            entities_count = sum(1 for item in all_items if item["source_type"] == "entity")
            triplets_count = sum(1 for item in all_items if item["source_type"] == "triplet")

            await self._store.increment_counter(ingestion_id, "entities_received", entities_count)
            if triplets_count > 0:
                await self._store.increment_counter(
                    ingestion_id, "triplets_received", triplets_count
                )

        # Checkpoint deduplication: filter items that are already processed with same content
        items_to_embed: List[Dict[str, Any]] = []
        checkpoint_checks = []
        for item in all_items:
            content_hash = hashlib.sha256(item["text"].encode("utf-8")).hexdigest()
            item["content_hash"] = content_hash
            checkpoint_checks.append(
                self._store.check_checkpoint(item["source_id"], "embedding", content_hash)
            )

        skip_flags = await asyncio.gather(*checkpoint_checks)
        for item, skip in zip(all_items, skip_flags):
            if not skip:
                items_to_embed.append(item)

        # Checkpoint deduplication for edge_type items
        edge_type_items_to_embed: List[Dict[str, Any]] = []
        if edge_type_items:
            edge_type_checkpoint_checks = []
            for item in edge_type_items:
                content_hash = hashlib.sha256(item["text"].encode("utf-8")).hexdigest()
                item["content_hash"] = content_hash
                edge_type_checkpoint_checks.append(
                    self._store.has_checkpoint(item["source_id"], "embedding", content_hash)
                )
            edge_type_skip_flags = await asyncio.gather(*edge_type_checkpoint_checks)
            for item, skip in zip(edge_type_items, edge_type_skip_flags):
                if not skip:
                    edge_type_items_to_embed.append(item)

        if not items_to_embed and not edge_type_items_to_embed:
            # All items already processed — return empty events but preserve metadata
            logger.info(
                "processor.entity_batch_dedup_skip",
                events=len(events),
                total_items=total_items,
                skipped=total_items,
            )
            return []

        # Embed entity/triplet items
        texts = [item["text"] for item in items_to_embed]
        embeddings = await self._embed_texts_batched(texts) if texts else []
        for item, embedding in zip(items_to_embed, embeddings):
            item["embedding"] = embedding

        # Save checkpoints after successful embedding (entity/triplet items)
        if items_to_embed:
            checkpoint_saves = [
                self._store.save_checkpoint(
                    item["source_id"], "embedding", item["content_hash"], ingestion_id
                )
                for item in items_to_embed
            ]
            await asyncio.gather(*checkpoint_saves)

        # Embed edge_type items
        if edge_type_items_to_embed:
            edge_type_texts = [item["text"] for item in edge_type_items_to_embed]
            edge_type_embeddings = await self._embed_texts_batched(edge_type_texts)
            for item, embedding in zip(edge_type_items_to_embed, edge_type_embeddings):
                item["embedding"] = embedding

            # Save checkpoints after successful embedding (edge_type items)
            edge_type_checkpoint_saves = [
                self._store.save_checkpoint(
                    item["source_id"], "embedding", item["content_hash"], ingestion_id
                )
                for item in edge_type_items_to_embed
            ]
            await asyncio.gather(*edge_type_checkpoint_saves)

        # No bulk Postgres writes — embeddings go directly to Kafka
        total_embedded = len(items_to_embed) + len(edge_type_items_to_embed)
        if total_embedded > 0:
            await self._store.increment_counter(ingestion_id, "embeddings_computed", total_embedded)

        result_events: List[EmbeddingReadyEvent] = []
        offset = 0
        for idx, event in enumerate(events):
            count = event_boundaries[idx]
            # Get all original items for this event (including skipped ones)
            event_items_all = all_items[offset : offset + count]
            offset += count

            # Filter to only those that were embedded (not skipped)
            event_items_embedded = [item for item in event_items_all if "embedding" in item]

            payloads = [
                EmbeddingPayload(
                    source_id=item["source_id"],
                    source_type=item["source_type"],  # "entity" or "triplet"
                    text=item["text"],
                    embedding=item["embedding"],
                    from_node_id=item.get("from_node_id", ""),
                    to_node_id=item.get("to_node_id", ""),
                )
                for item in event_items_embedded
            ]

            if payloads:
                result_events.append(
                    EmbeddingReadyEvent(
                        ingestion_id=event.ingestion_id,
                        company_id=event.company_id,
                        project_id=event.project_id,
                        content_type=getattr(event, "content_type", "code"),
                        file_version_id=event.file_version_id,
                        repository=event.repository,
                        branch=event.branch,
                        embeddings=payloads,
                        embedding_duration_s=round(time.time() - t0, 3),
                    )
                )

        # Emit a separate EmbeddingReadyEvent for edge_type items (batch-level, not per-event)
        if edge_type_items_to_embed:
            edge_type_payloads = [
                EmbeddingPayload(
                    source_id=item["source_id"],
                    source_type="edge_type",
                    text=item["text"],
                    embedding=item["embedding"],
                    number_of_edges=item.get("number_of_edges", 0),
                )
                for item in edge_type_items_to_embed
                if "embedding" in item
            ]
            if edge_type_payloads:
                result_events.append(
                    EmbeddingReadyEvent(
                        ingestion_id=first_event.ingestion_id,
                        company_id=first_event.company_id,
                        project_id=first_event.project_id,
                        content_type=getattr(first_event, "content_type", "code"),
                        file_version_id=first_event.file_version_id,
                        repository=first_event.repository,
                        branch=first_event.branch,
                        embeddings=edge_type_payloads,
                        embedding_duration_s=round(time.time() - t0, 3),
                    )
                )

        # Log completion with entity + triplet + edge_type counts (embedded, not skipped)
        entities_embedded = sum(1 for item in items_to_embed if item["source_type"] == "entity")
        triplets_embedded = sum(1 for item in items_to_embed if item["source_type"] == "triplet")
        entities_total = sum(1 for item in all_items if item["source_type"] == "entity")
        triplets_total = sum(1 for item in all_items if item["source_type"] == "triplet")

        logger.info(
            "processor.entity_batch_complete",
            events=len(events),
            total_entities=entities_total,
            total_triplets=triplets_total,
            total_edge_types=len(edge_type_items),
            embedded_entities=entities_embedded,
            embedded_triplets=triplets_embedded,
            embedded_edge_types=len(edge_type_items_to_embed),
            skipped=len(all_items) - len(items_to_embed),
            duration_s=round(time.time() - t0, 3),
        )
        return result_events

    # -- Summary batch processing ----------------------------------------------

    async def process_summary_batch(
        self,
        events: List[SummaryInputEvent],
    ) -> List[EmbeddingReadyEvent]:
        """Process a batch of summary events: embed summary texts."""
        if not events:
            return []

        t0 = time.time()
        ingestion_id = events[0].ingestion_id

        all_items: List[Dict[str, Any]] = []
        for event in events:
            if event.summary_text:
                all_items.append(
                    {
                        "source_id": event.summary_id or event.chunk_id,
                        "source_type": "summary",
                        "text": event.summary_text,
                        "ingestion_id": event.ingestion_id,
                        "company_id": event.company_id,
                        "project_id": event.project_id,
                        "content_type": getattr(event, "content_type", "code"),
                    }
                )

        total_summaries = len(all_items)
        if total_summaries > 0:
            await self._store.increment_counter(ingestion_id, "summaries_received", total_summaries)

        # Checkpoint deduplication: filter items that are already processed with same content
        items_to_embed: List[Dict[str, Any]] = []
        checkpoint_checks = []
        for item in all_items:
            content_hash = hashlib.sha256(item["text"].encode("utf-8")).hexdigest()
            item["content_hash"] = content_hash
            checkpoint_checks.append(
                self._store.has_checkpoint(item["source_id"], "embedding", content_hash)
            )

        skip_flags = await asyncio.gather(*checkpoint_checks)
        for item, skip in zip(all_items, skip_flags):
            if not skip:
                items_to_embed.append(item)

        if not items_to_embed:
            # All items already processed — return empty events
            logger.info(
                "processor.summary_batch_dedup_skip",
                events=len(events),
                total_summaries=total_summaries,
                skipped=total_summaries,
            )
            return []

        texts = [item["text"] for item in items_to_embed]
        embeddings = await self._embed_texts_batched(texts)

        # Update items with embeddings
        for item, embedding in zip(items_to_embed, embeddings):
            item["embedding"] = embedding

        # Save checkpoints after successful embedding
        if items_to_embed:
            checkpoint_saves = [
                self._store.save_checkpoint(
                    item["source_id"], "embedding", item["content_hash"], ingestion_id
                )
                for item in items_to_embed
            ]
            await asyncio.gather(*checkpoint_saves)

        # No bulk Postgres writes — embeddings go directly to Kafka
        if items_to_embed:
            await self._store.increment_counter(
                ingestion_id, "embeddings_computed", len(items_to_embed)
            )

        result_events: List[EmbeddingReadyEvent] = []
        for idx, event in enumerate(events):
            if idx < len(all_items):
                item = all_items[idx]
                # Only create event if this item was embedded (not skipped)
                if "embedding" in item:
                    payload = EmbeddingPayload(
                        source_id=item["source_id"],
                        source_type="summary",
                        text=item["text"],
                        embedding=item["embedding"],
                    )
                    result_events.append(
                        EmbeddingReadyEvent(
                            ingestion_id=event.ingestion_id,
                            company_id=event.company_id,
                            project_id=event.project_id,
                            content_type=getattr(event, "content_type", "code"),
                            file_version_id=event.file_version_id,
                            repository=event.repository,
                            branch=event.branch,
                            embeddings=[payload],
                            embedding_duration_s=round(time.time() - t0, 3),
                        )
                    )

        logger.info(
            "processor.summary_batch_complete",
            events=len(events),
            total_summaries=total_summaries,
            embedded=len(items_to_embed),
            skipped=len(all_items) - len(items_to_embed),
            duration_s=round(time.time() - t0, 3),
        )
        return result_events

    # -- Embedding engine call with batching + retry ---------------------------

    async def _embed_texts_batched(self, texts: List[str]) -> List[List[float]]:
        """Embed texts in batches of EMBEDDING_BATCH_SIZE with parallel sub-batches."""
        if not texts:
            return []
        batch_size = self._config.EMBEDDING_BATCH_SIZE

        # Split into sub-batches
        sub_batches: List[List[str]] = []
        for i in range(0, len(texts), batch_size):
            sub_batches.append(texts[i : i + batch_size])

        # Process sub-batches in parallel with semaphore
        tasks = [self._embed_with_retry(batch) for batch in sub_batches]
        results = await asyncio.gather(*tasks)

        # Flatten results
        all_embeddings: List[List[float]] = []
        for batch_embeddings in results:
            all_embeddings.extend(batch_embeddings)
        return all_embeddings

    async def _embed_with_retry(self, texts: List[str]) -> List[List[float]]:
        """Call embedding engine with exponential backoff retry and semaphore."""
        # Acquire semaphore to limit concurrent embedding calls
        async with self._semaphore:
            last_error: Optional[Exception] = None
            for attempt in range(1, self._config.MAX_RETRIES + 1):
                try:
                    engine = self._get_engine()
                    result = await asyncio.wait_for(engine.embed_text(texts), timeout=60.0)
                    logger.debug(
                        "processor.embedding_call",
                        batch_size=len(texts),
                    )
                    return result
                except asyncio.TimeoutError:
                    last_error = asyncio.TimeoutError(
                        f"Embedding call timed out after 60s (attempt {attempt})"
                    )
                    logger.warning(
                        "processor.embedding_timeout",
                        attempt=attempt,
                        max_retries=self._config.MAX_RETRIES,
                        batch_size=len(texts),
                    )
                    if attempt < self._config.MAX_RETRIES:
                        delay = self._config.RETRY_BASE_DELAY * (2 ** (attempt - 1))
                        await asyncio.sleep(delay)
                    continue
                except Exception as e:
                    last_error = e
                    logger.warning(
                        "processor.embedding_retry",
                        attempt=attempt,
                        max_retries=self._config.MAX_RETRIES,
                        error=str(e),
                    )
                    if attempt < self._config.MAX_RETRIES:
                        delay = self._config.RETRY_BASE_DELAY * (2 ** (attempt - 1))
                        await asyncio.sleep(delay)
            raise RuntimeError(
                f"Embedding failed after {self._config.MAX_RETRIES} retries: {last_error}"
            ) from last_error

    # -- Completion check ------------------------------------------------------

    async def check_ingestion_complete(self, ingestion_id: str) -> Tuple[bool, int, Optional[int]]:
        """Check if all entities and summaries have been embedded.

        Returns (is_complete, received_count, total_count) matching the
        pattern used in entity_extraction and summarization services.
        """
        counters = await self._store.get_all_counters(ingestion_id)
        extraction_complete = await self._store.is_upstream_complete(
            ingestion_id, "entity_extraction"
        )
        summarization_complete = await self._store.is_upstream_complete(
            ingestion_id, "summarization"
        )

        our_entities = counters.get("entities_received", 0)
        our_summaries = counters.get("summaries_received", 0)
        received_count = our_entities + our_summaries

        if not extraction_complete or not summarization_complete:
            return False, received_count, None

        upstream_entities = await self._store.get_upstream_total(
            ingestion_id, "entity_extraction", "entities_extracted"
        )
        upstream_summaries = await self._store.get_upstream_total(
            ingestion_id, "summarization", "summaries_produced"
        )

        total_count: Optional[int] = None
        if upstream_entities is not None and upstream_summaries is not None:
            total_count = upstream_entities + upstream_summaries

        entities_done = upstream_entities is not None and our_entities >= upstream_entities
        summaries_done = upstream_summaries is not None and our_summaries >= upstream_summaries
        return entities_done and summaries_done, received_count, total_count

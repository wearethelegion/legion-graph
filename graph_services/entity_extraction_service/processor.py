"""Entity extraction processor — runs LLM extraction per chunk.

Consumes EnrichedChunkMessage, calls extract_content_graph per chunk,
repairs the output with Pydantic (repair-and-accept policy), converts
KnowledgeGraph results into EntityPayload/EdgePayload, and returns
ExtractedEntitiesEvent for Kafka publishing.

Content-hash deduplication prevents re-processing identical chunks.

## Phase 3.1 — Prompt routing
The extraction prompt is read from the incoming message's `extraction_prompt`
field. If the field is present but empty/None, the chunk is REJECTED (no silent
fallback). If the field is absent entirely (old-format messages), a warning is
logged and the file-based prompt is used as a transitional fallback.

## Phase 3.2 — Output validation (repair-and-accept)
After the LLM returns a KnowledgeGraph the output is converted and repaired via
repair_extraction_result(). Quality issues (short descriptions, bad edge endpoints)
are REPAIRED, never rejected. Retries only happen for LLM call failures (network
errors, rate limits, completely unparseable responses). A chunk is only skipped
if all LLM call attempts fail — never due to validation issues.
"""

import asyncio
import hashlib
import json
import time
from pathlib import Path
from typing import List, Optional, Tuple

import structlog
from cognee.infrastructure.llm.extraction import extract_content_graph
from cognee.shared.data_models import KnowledgeGraph
from cognee_service.kafka_consumer.enriched_chunks.models import (
    EnrichedChunkMessage,
)
from shared.slugify import slugify

from .config import EntityExtractionConfig
from .models import (
    EdgePayload,
    EntityPayload,
    ExtractedEntitiesEvent,
    entity_name_to_uuid,
)
from .pipeline_store import EntityExtractionStore
from .validation import (
    ExtractionResult,
    knowledge_graph_to_extraction_result,
    repair_extraction_result,
)

logger = structlog.get_logger(__name__)


class EntityExtractionProcessor:
    """Extracts entities from enriched code chunks via LLM.

    Per chunk:
    1. Resolve extraction prompt (from message field, with fallback for old messages)
    2. Check content-hash checkpoint (skip if unchanged)
    3. Build LLM input (header + content)
    4. Call extract_content_graph with resolved prompt (retry on LLM call failure)
    5. Convert KnowledgeGraph to ExtractionResult and repair (repair-and-accept)
    6. Convert repaired ExtractionResult to EntityPayload/EdgePayload
    7. Return ExtractedEntitiesEvent for Kafka publishing
    """

    def __init__(
        self,
        store: EntityExtractionStore,
        config: type[EntityExtractionConfig] = EntityExtractionConfig,
        custom_prompt_path: Optional[str] = None,
    ):
        self._store = store
        self._config = config
        self._semaphore = asyncio.Semaphore(config.MAX_PARALLEL_WORKERS)

        # ── File-based prompts (transitional fallback for old-format messages) ──
        # TODO(v4): Remove file-based prompt fallback once all producers send extraction_prompt.
        self._custom_prompt: Optional[str] = None
        prompt_path = custom_prompt_path or config.CUSTOM_PROMPT_PATH
        if prompt_path and Path(prompt_path).is_file():
            self._custom_prompt = Path(prompt_path).read_text(encoding="utf-8").strip()
            logger.info(
                "processor.custom_prompt_loaded",
                path=prompt_path,
                length=len(self._custom_prompt),
            )
        elif prompt_path:
            logger.warning("processor.custom_prompt_not_found", path=prompt_path)

        self._document_prompt: Optional[str] = None
        doc_prompt_path = config.DOCUMENT_PROMPT_PATH
        if doc_prompt_path and Path(doc_prompt_path).is_file():
            self._document_prompt = Path(doc_prompt_path).read_text(encoding="utf-8").strip()
            logger.info(
                "processor.document_prompt_loaded",
                path=doc_prompt_path,
                length=len(self._document_prompt),
            )
        elif doc_prompt_path:
            logger.warning("processor.document_prompt_not_found", path=doc_prompt_path)

    # ── Public API ────────────────────────────────────────────────────

    async def process_batch(
        self,
        messages: List[EnrichedChunkMessage],
    ) -> List[ExtractedEntitiesEvent]:
        """Process a batch of enriched chunks in parallel.

        Uses asyncio.gather with semaphore to bound concurrency.
        Returns list of ExtractedEntitiesEvent (one per chunk).
        Failed and rejected chunks are logged and skipped.
        """
        if not messages:
            return []

        t0 = time.time()
        company_id = messages[0].company_id
        ingestion_id = messages[0].ingestion_id

        logger.info(
            "processor.batch_start",
            batch_size=len(messages),
            company_id=company_id,
            ingestion_id=ingestion_id,
        )

        await self._store.increment_counter(ingestion_id, "chunks_received", len(messages))

        results = await asyncio.gather(
            *[self._process_one_chunk(msg, idx, len(messages)) for idx, msg in enumerate(messages)],
            return_exceptions=True,
        )

        events: List[ExtractedEntitiesEvent] = []
        total_entities = 0
        total_edges = 0

        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(
                    "processor.chunk_failed",
                    idx=idx,
                    chunk_id=messages[idx].chunk_id,
                    error=str(result),
                    error_type=type(result).__name__,
                )
                continue

            if result is not None:
                events.append(result)
                total_entities += len(result.entities)
                total_edges += len(result.edges)

        if total_entities > 0:
            await self._store.increment_counter(ingestion_id, "entities_extracted", total_entities)
        if total_edges > 0:
            await self._store.increment_counter(ingestion_id, "edges_extracted", total_edges)

        duration = round(time.time() - t0, 2)
        logger.info(
            "processor.batch_complete",
            batch_size=len(messages),
            successful=len(events),
            failed=len(messages) - len(events),
            total_entities=total_entities,
            total_edges=total_edges,
            duration_s=duration,
        )

        return events

    async def check_ingestion_complete(
        self,
        ingestion_id: str,
    ) -> Tuple[bool, int, Optional[int]]:
        """Check if all chunks for an ingestion have been processed.

        Returns (is_complete, chunks_received, total_chunks).
        total_chunks is None if preprocessor hasn't reported yet.
        """
        chunks_received = await self._store.get_counter(ingestion_id, "chunks_received")
        total_chunks = await self._store.get_preprocessor_total_chunks(ingestion_id)

        if total_chunks is not None and chunks_received >= total_chunks:
            return True, chunks_received, total_chunks

        return False, chunks_received, total_chunks

    # ── Private: orchestration ────────────────────────────────────────

    async def _process_one_chunk(
        self,
        msg: EnrichedChunkMessage,
        idx: int,
        total: int,
    ) -> Optional[ExtractedEntitiesEvent]:
        """Extract entities from a single chunk (bounded by semaphore)."""
        async with self._semaphore:
            return await self._extract_with_retries(msg, idx, total)

    async def _extract_with_retries(
        self,
        msg: EnrichedChunkMessage,
        idx: int,
        total: int,
    ) -> Optional[ExtractedEntitiesEvent]:
        """LLM extraction with exponential backoff + jitter on LLM errors."""
        import random

        last_error: Optional[Exception] = None

        for attempt in range(1, self._config.MAX_RETRIES + 1):
            try:
                return await self._do_extract(msg, idx, total)
            except Exception as e:
                last_error = e
                logger.warning(
                    "processor.extraction_retry",
                    chunk_id=msg.chunk_id,
                    attempt=attempt,
                    max_retries=self._config.MAX_RETRIES,
                    error=str(e),
                )
                if attempt < self._config.MAX_RETRIES:
                    base = self._config.RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    jitter = random.uniform(0, base)
                    await asyncio.sleep(base + jitter)

        logger.error(
            "processor.extraction_exhausted",
            chunk_id=msg.chunk_id,
            error=str(last_error),
        )
        return None

    # ── Private: prompt resolution (Phase 3.1) ───────────────────────

    def _resolve_prompt(self, msg: EnrichedChunkMessage) -> Optional[str]:
        """Resolve the extraction prompt for a message.

        Routes to document or code prompt resolution based on content_type.
        Returns the prompt string to use, or None to signal rejection.
        """
        content_type = getattr(msg, "content_type", "") or "code"

        if content_type == "document":
            return self._resolve_document_prompt(msg)
        return self._resolve_code_prompt(msg)

    def _resolve_document_prompt(self, msg: EnrichedChunkMessage) -> Optional[str]:
        """Resolve extraction prompt for document messages.

        Resolution priority:
        1. DB prompt via msg.extraction_prompt (set by document-preprocessor) → use with substitutions
        2. Legacy fallback: no extraction_prompt field → use file-based with warning
        3. Hard reject: extraction_prompt present but null/empty → return None
        """
        if hasattr(msg, "extraction_prompt"):
            # New-format message: field is present
            prompt = getattr(msg, "extraction_prompt", None)
            if prompt:
                # Priority 1: Use DB prompt with placeholder substitution
                return self._substitute_document_placeholders(prompt, msg)

            # Priority 3: Field present but empty/None → hard reject (no silent fallback)
            logger.error(
                "processor.document_rejected_empty_prompt",
                chunk_id=msg.chunk_id,
                entity_type=getattr(msg, "entity_type", "unknown"),
                detail="extraction_prompt field present but null/empty — chunk rejected",
            )
            return None

        # Priority 2: Old-format message without extraction_prompt field → legacy fallback
        if self._document_prompt:
            logger.warning(
                "processor.document_legacy_fallback",
                chunk_id=msg.chunk_id,
                detail="document message has no extraction_prompt field — using file-based prompt",
            )
            return self._substitute_document_placeholders(self._document_prompt, msg)

        logger.error(
            "processor.no_document_prompt_available",
            chunk_id=msg.chunk_id,
            detail="no extraction_prompt and no file-based document prompt configured",
        )
        return None

    def _resolve_code_prompt(self, msg: EnrichedChunkMessage) -> Optional[str]:
        """Resolve extraction prompt for code messages (original logic).

        Resolution order:
        1. msg.extraction_prompt present and non-empty → use it directly
        2. msg.extraction_prompt present but None/empty → REJECT
        3. msg.extraction_prompt attribute absent → warn + fall back to file-based
        """
        if hasattr(msg, "extraction_prompt"):
            # New-format message: field is present
            extraction_prompt = msg.extraction_prompt
            if extraction_prompt:
                return extraction_prompt
            # Field present but empty/None → reject
            return None
        else:
            # Old-format message: field absent entirely
            # TODO(v4): Remove file-based prompt fallback once all producers send extraction_prompt.
            logger.warning(
                "processor.old_format_message_detected",
                chunk_id=msg.chunk_id,
                detail=(
                    "Message has no extraction_prompt field. "
                    "Using file-based prompt as transitional fallback. "
                    "This support will be removed in v4."
                ),
            )
            return self._file_prompt_for(msg)

    def _substitute_document_placeholders(self, prompt: str, msg: EnrichedChunkMessage) -> str:
        """Substitute {{ENTITY_TYPE}} and {{DOCUMENT_TITLE}} placeholders in document prompts."""
        entity_type = getattr(msg, "entity_type", None) or "knowledge"
        document_title = getattr(msg, "document_title", None) or "Untitled"
        prompt = prompt.replace("{{ENTITY_TYPE}}", entity_type)
        prompt = prompt.replace("{{DOCUMENT_TITLE}}", document_title)
        return prompt

    def _file_prompt_for(self, msg: EnrichedChunkMessage) -> Optional[str]:
        """Return the appropriate file-based prompt for a message (old-format fallback).

        Note: This is now primarily for code messages. Document messages should go through
        _resolve_document_prompt() which handles both DB and file-based prompts.
        """
        content_type = getattr(msg, "content_type", "code") or "code"

        if content_type == "document" and self._document_prompt:
            # Use shared placeholder substitution for consistency
            return self._substitute_document_placeholders(self._document_prompt, msg)

        # Code prompt with template variable substitution
        prompt = self._custom_prompt
        if prompt:
            if hasattr(msg, "business_domains") and msg.business_domains:
                domain_lines = ", ".join(
                    f"{d['name']} (concepts: {', '.join(d.get('key_concepts', []))})"
                    for d in msg.business_domains
                )
                prompt = prompt.replace("{{BUSINESS_DOMAINS}}", domain_lines)
            if hasattr(msg, "technical_tags") and msg.technical_tags:
                prompt = prompt.replace("{{TECHNICAL_TAGS}}", ", ".join(msg.technical_tags))
            prompt = prompt.replace("{{BUSINESS_DOMAINS}}", "not available")
            prompt = prompt.replace("{{TECHNICAL_TAGS}}", "not available")
        return prompt

    # ── Private: extraction + validation (Phase 3.2) ─────────────────

    async def _do_extract(
        self,
        msg: EnrichedChunkMessage,
        idx: int,
        total: int,
    ) -> Optional[ExtractedEntitiesEvent]:
        """Execute LLM entity extraction for a single chunk.

        1. Resolve extraction prompt (Phase 3.1 routing)
        2. Check content-hash checkpoint (skip if already processed)
        3. Build LLM text (header + content)
        4. Call extract_content_graph (raised exceptions propagate to retry loop)
        5. Convert KnowledgeGraph → ExtractionResult and repair (repair-and-accept)
        6. Convert repaired result to EntityPayload/EdgePayload
        7. Save checkpoint and return ExtractedEntitiesEvent
        """
        t0 = time.time()

        # ── Phase 3.1: Resolve prompt ──────────────────────────────
        prompt = self._resolve_prompt(msg)

        if prompt is None:
            # extraction_prompt field present but empty/None → hard reject
            logger.error(
                "processor.chunk_rejected_no_prompt",
                chunk_id=msg.chunk_id,
                file_path=msg.file_path,
                ingestion_id=msg.ingestion_id,
                detail=(
                    "Message has extraction_prompt field but it is null/empty. "
                    "Chunk rejected — no silent fallback to file-based prompt."
                ),
            )
            return None

        # ── Deduplication ──────────────────────────────────────────
        content_hash = hashlib.sha256(
            json.dumps({"chunk_id": msg.chunk_id, "content": msg.content}, sort_keys=True).encode()
        ).hexdigest()

        already_done = await self._store.has_checkpoint(
            item_id=msg.chunk_id,
            stage="extraction",
            content_hash=content_hash,
        )

        if already_done:
            logger.info(
                "processor.chunk_skipped_duplicate",
                chunk_id=msg.chunk_id,
                idx=idx,
                total=total,
            )
            return None

        # ── LLM input ──────────────────────────────────────────────
        llm_text = f"{msg.header}\n{msg.content}" if msg.header else msg.content

        # ── Phase 3.2: LLM call (exceptions propagate to retry wrapper) ──
        kg: KnowledgeGraph = await extract_content_graph(
            llm_text,
            KnowledgeGraph,
            custom_prompt=prompt,
        )

        # ── Convert + repair (never raises for quality issues) ─────
        raw_result = knowledge_graph_to_extraction_result(kg)
        repaired = repair_extraction_result(raw_result)

        # ── Convert repaired ExtractionResult → wire payloads ─────
        node_set = _build_node_set(msg)
        entity_payloads = _to_entity_payloads(repaired, node_set)
        edge_payloads = _to_edge_payloads(repaired, node_set)

        duration = round(time.time() - t0, 3)

        logger.info(
            "processor.chunk_extracted",
            idx=idx,
            total=total,
            chunk_id=msg.chunk_id,
            entities=len(entity_payloads),
            edges=len(edge_payloads),
            duration_s=duration,
        )

        # Save checkpoint AFTER successful processing — never before.
        await self._store.save_checkpoint(
            item_id=msg.chunk_id,
            stage="extraction",
            content_hash=content_hash,
            ingestion_id=msg.ingestion_id,
        )

        raw_document_title = getattr(msg, "document_title", None)
        document_slug = getattr(msg, "document_slug", None)
        if document_slug is None and getattr(msg, "content_type", "code") == "document":
            document_slug = slugify(raw_document_title or "") or getattr(msg, "file_version_id", "")

        return ExtractedEntitiesEvent(
            ingestion_id=msg.ingestion_id,
            chunk_id=msg.chunk_id,
            company_id=msg.company_id,
            project_id=msg.project_id,
            file_version_id=msg.file_version_id,
            file_path=msg.file_path,
            repository=msg.repository,
            branch=msg.branch,
            language=msg.language or "",
            content_type=getattr(msg, "content_type", "code"),
            document_title=raw_document_title,
            document_slug=document_slug,
            start_line=getattr(msg, "start_line", 0) or 0,
            end_line=getattr(msg, "end_line", 0) or 0,
            chunk_index=getattr(msg, "chunk_index", 0) or 0,
            chunk_text=msg.content or "",
            entities=entity_payloads,
            edges=edge_payloads,
            extraction_duration_s=duration,
        )


# ── Module-level helpers ──────────────────────────────────────────────


def _build_node_set(msg) -> str:
    if getattr(msg, "content_type", "code") == "document" or not getattr(msg, "project_id", None):
        return f"{msg.company_id}_knowledge"
    project_name = getattr(msg, "project_name", None) or msg.project_id
    return f"{msg.project_id}_{project_name}_code"


def _to_entity_payloads(result: ExtractionResult, node_set: str) -> List[EntityPayload]:
    """Convert validated ExtractionResult entities to wire EntityPayload list."""
    payloads: List[EntityPayload] = []
    for node in result.entities:
        eid = str(entity_name_to_uuid(node.name, node_set))
        payloads.append(
            EntityPayload(
                entity_id=eid,
                name=node.name,
                entity_type=node.type,
                description=node.description,
            )
        )
    return payloads


def _to_edge_payloads(result: ExtractionResult, node_set: str) -> List[EdgePayload]:
    """Convert validated ExtractionResult relationships to wire EdgePayload list."""
    payloads: List[EdgePayload] = []
    for edge in result.relationships:
        source_eid = str(entity_name_to_uuid(edge.source, node_set))
        target_eid = str(entity_name_to_uuid(edge.target, node_set))
        payloads.append(
            EdgePayload(
                source_id=source_eid,
                target_id=target_eid,
                relationship_type=edge.type,
                source_name=edge.source,
                target_name=edge.target,
            )
        )
    return payloads

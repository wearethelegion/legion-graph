"""Summarization processor — runs LLM summarization per chunk.

Consumes EnrichedChunkMessage, calls summarize_text per chunk,
and returns TextSummaryEvent for Kafka publishing.

Uses content-hash checkpoint deduplication to skip unchanged chunks.
"""

import asyncio
import hashlib
import time

from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4, uuid5, NAMESPACE_OID

import structlog

from cognee.tasks.summarization import summarize_text
from cognee_service.kafka_consumer.enriched_chunks.models import (
    EnrichedChunkMessage,
)

from .config import SummarizationConfig
from .models import TextSummaryEvent
from .pipeline_store import SummarizationStore

logger = structlog.get_logger(__name__)


class SummarizationProcessor:
    """Summarizes enriched code chunks via LLM.

    Per chunk:
    1. Build LLM input (header + content)
    2. Call summarize_text
    3. Store summary in Postgres staging table
    4. Return TextSummaryEvent for Kafka publishing
    """

    def __init__(
        self,
        store: SummarizationStore,
        config: type[SummarizationConfig] = SummarizationConfig,
    ):
        self._store = store
        self._config = config
        self._semaphore = asyncio.Semaphore(config.MAX_PARALLEL_WORKERS)

    async def process_batch(
        self,
        messages: List[EnrichedChunkMessage],
    ) -> List[TextSummaryEvent]:
        """Process a batch of enriched chunks in parallel.

        Uses asyncio.gather with semaphore to bound concurrency.
        Returns list of TextSummaryEvent (one per chunk).
        Failed chunks are logged and skipped (not re-raised).
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

        # Increment chunks_received counter
        await self._store.increment_counter(ingestion_id, "chunks_received", len(messages))

        # Process all chunks in parallel with semaphore
        results = await asyncio.gather(
            *[self._process_one_chunk(msg, idx, len(messages)) for idx, msg in enumerate(messages)],
            return_exceptions=True,
        )

        # Collect successful results, log failures
        events: List[TextSummaryEvent] = []
        total_summaries = 0

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
                total_summaries += 1

        # Update counters
        if total_summaries > 0:
            await self._store.increment_counter(ingestion_id, "summaries_produced", total_summaries)

        duration = round(time.time() - t0, 2)
        logger.info(
            "processor.batch_complete",
            batch_size=len(messages),
            successful=len(events),
            failed=len(messages) - len(events),
            total_summaries=total_summaries,
            duration_s=duration,
        )

        return events

    async def _process_one_chunk(
        self,
        msg: EnrichedChunkMessage,
        idx: int,
        total: int,
    ) -> Optional[TextSummaryEvent]:
        """Summarize a single chunk.

        Bounded by semaphore. Retries up to MAX_RETRIES on LLM failure.
        Returns TextSummaryEvent or None on permanent failure.
        """
        async with self._semaphore:
            return await self._summarize_with_retries(msg, idx, total)

    async def _summarize_with_retries(
        self,
        msg: EnrichedChunkMessage,
        idx: int,
        total: int,
    ) -> Optional[TextSummaryEvent]:
        """Call LLM summarization with exponential backoff retry."""
        last_error: Optional[Exception] = None

        for attempt in range(1, self._config.MAX_RETRIES + 1):
            try:
                return await self._do_summarize(msg, idx, total)
            except Exception as e:
                last_error = e
                logger.warning(
                    "processor.summarization_retry",
                    chunk_id=msg.chunk_id,
                    attempt=attempt,
                    max_retries=self._config.MAX_RETRIES,
                    error=str(e),
                )
                if attempt < self._config.MAX_RETRIES:
                    delay = self._config.RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)

        logger.error(
            "processor.summarization_exhausted",
            chunk_id=msg.chunk_id,
            error=str(last_error),
        )
        return None

    async def _do_summarize(
        self,
        msg: EnrichedChunkMessage,
        idx: int,
        total: int,
    ) -> Optional[TextSummaryEvent]:
        """Execute LLM summarization for a single chunk.

        1. Build LLM text (header + content)
        2. Check content-hash checkpoint
        3. If unchanged, skip processing
        4. If new/changed: call summarize_text
        5. Return TextSummaryEvent
        """
        t0 = time.time()

        # Build LLM input: header is context only, chunk content is what gets summarized
        content_type = getattr(msg, "content_type", "code") or "code"
        chunk_label = (
            "CONTENT TO SUMMARIZE" if content_type == "document" else "CODE CHUNK TO SUMMARIZE"
        )

        if msg.header:
            llm_text = (
                f"--- CONTEXT (for reference only, do not summarize) ---\n"
                f"{msg.header}\n\n"
                f"--- {chunk_label} ---\n"
                f"{msg.content}"
            )
        else:
            llm_text = msg.content

        # Compute content hash for checkpoint dedup
        content_hash = hashlib.sha256(llm_text.encode("utf-8")).hexdigest()

        # Check if already processed with same content
        already_processed = await self._store.has_checkpoint(
            item_id=msg.chunk_id,
            stage="summarization",
            content_hash=content_hash,
        )

        if already_processed:
            logger.info(
                "processor.chunk_skipped",
                idx=idx,
                total=total,
                chunk_id=msg.chunk_id,
                reason="unchanged_content",
            )
            return None

        # summarize_text expects list[DocumentChunk] with Pydantic validation.
        # Construct a minimal valid DocumentChunk.
        from cognee.modules.chunking.models.DocumentChunk import DocumentChunk
        from cognee.modules.data.processing.document_types.Document import Document

        try:
            chunk_uuid = UUID(msg.chunk_id)
        except (ValueError, AttributeError):
            chunk_uuid = uuid5(NAMESPACE_OID, msg.chunk_id)

        stub_doc = Document(
            id=chunk_uuid,
            name=msg.file_path or "unknown",
            raw_data_location=msg.file_path or "",
            external_metadata=None,
            mime_type="text/plain",
        )
        chunk_obj = DocumentChunk(
            id=chunk_uuid,
            text=llm_text,
            chunk_size=len(llm_text),
            chunk_index=msg.chunk_index or 0,
            cut_type="v2_pipeline",
            is_part_of=stub_doc,
        )

        # Call Cognee's summarize_text with timeout protection.
        try:
            summaries = await asyncio.wait_for(summarize_text([chunk_obj]), timeout=600.0)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(
                f"Summarization timed out after 120s for chunk {msg.chunk_id}"
            )

        # summarize_text returns list[TextSummary]; extract the summary text
        summary_text = summaries[0].text if summaries else ""

        # Generate summary ID
        summary_id = str(uuid4())

        duration = round(time.time() - t0, 3)

        logger.info(
            "processor.chunk_summarized",
            idx=idx,
            total=total,
            chunk_id=msg.chunk_id,
            summary_length=len(summary_text),
            duration_s=duration,
        )

        # Save checkpoint AFTER successful processing
        # Critical: This must happen AFTER summarization succeeds, not before.
        # If we save checkpoint before processing and then processing fails,
        # the chunk is permanently lost on retry (marked as duplicate).
        await self._store.save_checkpoint(
            item_id=msg.chunk_id,
            stage="summarization",
            content_hash=content_hash,
            ingestion_id=msg.ingestion_id,
        )

        return TextSummaryEvent(
            ingestion_id=msg.ingestion_id,
            chunk_id=msg.chunk_id,
            company_id=msg.company_id,
            project_id=msg.project_id,
            content_type=getattr(msg, "content_type", "code"),
            chunk_index=getattr(msg, "chunk_index", 0) or 0,
            file_version_id=msg.file_version_id,
            file_path=msg.file_path,
            repository=msg.repository,
            branch=msg.branch,
            language=msg.language or "",
            summary_text=summary_text,
            summary_id=summary_id,
            summarization_duration_s=duration,
        )

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

"""DocumentProcessor — routes BrainEvent by action, orchestrates chunking + publishing.

For each brain event:
  1. Check idempotency cache — skip if event_id already processed
  2. Content-hash dedup — skip if text_content hash unchanged
  3. Route by action: create / update / delete
  4. Chunk document text (markdown-aware)
  5. Store version + chunks in Postgres (document_processing schema)
  6. Produce EnrichedChunkMessage to enriched-code-chunks topic
"""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from collections import OrderedDict
from typing import Any, Optional

import asyncpg

from shared.kafka_schemas import BrainEvent

from document_preprocessor.chunker import chunk_document
from document_preprocessor.config import DocumentPreprocessorSettings
from document_preprocessor.event_emitter import DocumentEventEmitter

logger = logging.getLogger(__name__)


class _LRUSet:
    """Bounded LRU set for idempotency tracking.

    Same implementation as BrainEventProcessor._LRUSet — keeps the last N
    event_ids in memory for dedup on Kafka replay.
    """

    def __init__(self, max_size: int = 10_000) -> None:
        self._cache: OrderedDict[str, None] = OrderedDict()
        self._max_size = max_size

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def add(self, key: str) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)
            return
        self._cache[key] = None
        if len(self._cache) > self._max_size:
            self._cache.popitem(last=False)


class DocumentProcessor:
    """Processes BrainEvent messages: chunk documents and publish to Kafka."""

    def __init__(
        self,
        settings: DocumentPreprocessorSettings,
        emitter: DocumentEventEmitter,
        db_pool: Optional[asyncpg.Pool] = None,
    ) -> None:
        self._settings = settings
        self._emitter = emitter
        self._db_pool = db_pool
        self._processed_ids = _LRUSet(settings.idempotency_cache_size)

    async def process(self, event: BrainEvent) -> dict[str, Any]:
        """Route a BrainEvent by its action field.

        Returns dict with processing result metadata.
        """
        # Idempotency guard
        if event.event_id in self._processed_ids:
            logger.info(
                "doc_processor.duplicate_skipped",
                extra={
                    "event_id": event.event_id,
                    "entity_id": event.entity_id,
                    "action": event.action,
                },
            )
            return {
                "status": "success",
                "action": event.action,
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "skipped": True,
                "reason": "duplicate_event_id",
            }

        action = event.action.lower()
        if action == "create":
            result = await self._process_create(event)
        elif action == "update":
            result = await self._process_update(event)
        elif action == "delete":
            result = await self._process_delete(event)
        else:
            logger.error(
                "doc_processor.unknown_action: %s for entity %s",
                event.action,
                event.entity_id,
            )
            return {
                "status": "error",
                "error": f"Unknown action: {event.action}",
                "entity_id": event.entity_id,
            }

        # Only record as processed on success
        if result.get("status") == "success":
            self._processed_ids.add(event.event_id)

        return result

    # ── CREATE ────────────────────────────────────────────────────────

    async def _process_create(self, event: BrainEvent) -> dict[str, Any]:
        t0 = time.time()
        ingestion_id = str(uuid.uuid4())
        content_hash = _hash_content(event.text_content)

        logger.info(
            "doc_processor.create.start: entity_type=%s entity_id=%s",
            event.entity_type,
            event.entity_id,
        )

        # Load extraction prompt for entity_type — FAIL LOUDLY if missing
        prompt = await self._load_prompt_for_entity_type(event.entity_type)
        if prompt is None:
            logger.error(
                "doc_processor.create.missing_prompt: entity_type=%s entity_id=%s",
                event.entity_type,
                event.entity_id,
            )
            return {
                "status": "error",
                "action": "create",
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "reason": "missing_prompt",
            }

        # Content-hash dedup: skip if identical content already processed
        if self._db_pool and await self._content_hash_exists(event.entity_id, content_hash):
            logger.info(
                "doc_processor.create.skipped_unchanged: entity_id=%s",
                event.entity_id,
            )
            return {
                "status": "success",
                "action": "create",
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "skipped": True,
                "reason": "content_unchanged",
            }

        # Chunk the document
        chunks = chunk_document(
            event.text_content,
            max_chars=self._settings.chunk_max_chars,
            min_chars=self._settings.chunk_min_chars,
        )

        if not chunks:
            logger.warning(
                "doc_processor.create.no_chunks: entity_id=%s (empty text?)",
                event.entity_id,
            )
            return {
                "status": "success",
                "action": "create",
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "chunks": 0,
            }

        # Store version + chunks in Postgres
        if self._db_pool:
            version_id = await self._store_document_version(
                event, content_hash, len(chunks), ingestion_id
            )
            if version_id:
                await self._store_document_chunks(version_id, chunks)

        # Embed and publish all chunks as a single batched call
        await self._emitter.emit_process_chunks_batch(
            chunks=chunks,
            entity_id=event.entity_id,
            entity_type=event.entity_type,
            title=event.title,
            company_id=event.company_id,
            project_id=event.project_id,
            ingestion_id=ingestion_id,
            extraction_prompt=prompt,
        )

        await self._emitter.flush()

        duration = round(time.time() - t0, 2)
        logger.info(
            "doc_processor.create.done: entity_id=%s chunks=%d duration=%.2fs",
            event.entity_id,
            len(chunks),
            duration,
        )
        return {
            "status": "success",
            "action": "create",
            "entity_type": event.entity_type,
            "entity_id": event.entity_id,
            "chunks": len(chunks),
            "duration_s": duration,
        }

    # ── UPDATE ────────────────────────────────────────────────────────

    async def _process_update(self, event: BrainEvent) -> dict[str, Any]:
        """Update = delete old chunks + create new ones (same pattern as code pipeline)."""
        t0 = time.time()
        ingestion_id = str(uuid.uuid4())
        content_hash = _hash_content(event.text_content)

        logger.info(
            "doc_processor.update.start: entity_type=%s entity_id=%s",
            event.entity_type,
            event.entity_id,
        )

        # Load extraction prompt for entity_type — FAIL LOUDLY if missing
        prompt = await self._load_prompt_for_entity_type(event.entity_type)
        if prompt is None:
            logger.error(
                "doc_processor.update.missing_prompt: entity_type=%s entity_id=%s",
                event.entity_type,
                event.entity_id,
            )
            return {
                "status": "error",
                "action": "update",
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "reason": "missing_prompt",
            }

        # Content-hash dedup
        if self._db_pool and await self._content_hash_exists(event.entity_id, content_hash):
            logger.info(
                "doc_processor.update.skipped_unchanged: entity_id=%s",
                event.entity_id,
            )
            return {
                "status": "success",
                "action": "update",
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "skipped": True,
                "reason": "content_unchanged",
            }

        # Step 1: Delete old chunks
        await self._emitter.emit_delete(
            entity_id=event.entity_id,
            entity_type=event.entity_type,
            company_id=event.company_id,
            project_id=event.project_id,
            ingestion_id=ingestion_id,
        )

        # Hard-delete old chunks + version row from Postgres before creating the new version.
        # This matches code-pipeline semantics: update = delete-then-create, no soft rows left.
        if self._db_pool:
            await self._hard_delete_versions_and_chunks(event.entity_id)

        # Step 2: Chunk new content
        chunks = chunk_document(
            event.text_content,
            max_chars=self._settings.chunk_max_chars,
            min_chars=self._settings.chunk_min_chars,
        )

        if not chunks:
            await self._emitter.flush()
            return {
                "status": "success",
                "action": "update",
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "chunks": 0,
            }

        # Step 3: Store new version + chunks in Postgres, then embed and publish
        if self._db_pool:
            version_id = await self._store_document_version(
                event, content_hash, len(chunks), ingestion_id
            )
            if version_id:
                await self._store_document_chunks(version_id, chunks)

        # Embed and publish all chunks as a single batched call
        await self._emitter.emit_process_chunks_batch(
            chunks=chunks,
            entity_id=event.entity_id,
            entity_type=event.entity_type,
            title=event.title,
            company_id=event.company_id,
            project_id=event.project_id,
            ingestion_id=ingestion_id,
            extraction_prompt=prompt,
        )

        await self._emitter.flush()

        duration = round(time.time() - t0, 2)
        logger.info(
            "doc_processor.update.done: entity_id=%s chunks=%d duration=%.2fs",
            event.entity_id,
            len(chunks),
            duration,
        )
        return {
            "status": "success",
            "action": "update",
            "entity_type": event.entity_type,
            "entity_id": event.entity_id,
            "chunks": len(chunks),
            "duration_s": duration,
        }

    # ── DELETE ────────────────────────────────────────────────────────

    async def _process_delete(self, event: BrainEvent) -> dict[str, Any]:
        t0 = time.time()
        ingestion_id = str(uuid.uuid4())

        logger.info(
            "doc_processor.delete.start: entity_type=%s entity_id=%s",
            event.entity_type,
            event.entity_id,
        )

        # Emit delete to downstream pipeline
        await self._emitter.emit_delete(
            entity_id=event.entity_id,
            entity_type=event.entity_type,
            company_id=event.company_id,
            project_id=event.project_id,
            ingestion_id=ingestion_id,
        )
        await self._emitter.flush()

        # Hard-delete all Postgres rows for this entity (chunks first, then version).
        # Matches code-pipeline semantics: delete removes data, not just marks it.
        if self._db_pool:
            await self._hard_delete_versions_and_chunks(event.entity_id)

        duration = round(time.time() - t0, 2)
        logger.info(
            "doc_processor.delete.done: entity_id=%s duration=%.2fs",
            event.entity_id,
            duration,
        )
        return {
            "status": "success",
            "action": "delete",
            "entity_type": event.entity_type,
            "entity_id": event.entity_id,
            "duration_s": duration,
        }

    # ── Postgres helpers ──────────────────────────────────────────────

    async def _content_hash_exists(self, entity_id: str, content_hash: str) -> bool:
        """Check if the latest non-deleted version has the same content hash."""
        try:
            row = await self._db_pool.fetchrow(
                "SELECT content_hash FROM document_processing.document_versions "
                "WHERE entity_id = $1 AND NOT deleted "
                "ORDER BY created_at DESC LIMIT 1",
                entity_id,
            )
            return row is not None and row["content_hash"] == content_hash
        except Exception as exc:
            logger.warning(
                "doc_processor.hash_check_failed: entity_id=%s error=%s",
                entity_id,
                exc,
            )
            return False

    async def _store_document_version(
        self,
        event: BrainEvent,
        content_hash: str,
        chunk_count: int,
        ingestion_id: str,
    ) -> Optional[str]:
        """Insert a document version record in Postgres.

        Returns the inserted row's UUID (from RETURNING id) so that
        ``_store_document_chunks`` can reference it via FK.
        Returns None on failure — caller skips chunk insert when None.
        """
        try:
            version_id = await self._db_pool.fetchval(
                "INSERT INTO document_processing.document_versions "
                "(entity_id, entity_type, company_id, project_id, "
                " content_hash, title, chunk_count, ingestion_id) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                "RETURNING id",
                event.entity_id,
                event.entity_type,
                event.company_id,
                event.project_id or "",
                content_hash,
                event.title,
                chunk_count,
                ingestion_id,
            )
            return str(version_id) if version_id else None
        except Exception as exc:
            logger.warning(
                "doc_processor.store_version_failed: entity_id=%s error=%s",
                event.entity_id,
                exc,
            )
            return None

    async def _mark_version_deleted(self, entity_id: str) -> None:
        """Soft-delete all versions for an entity.

        Kept for any external callers that want soft-delete semantics.
        The update + delete paths use :meth:`_hard_delete_versions_and_chunks`
        instead to achieve full Postgres cleanup consistent with the code path.
        """
        try:
            await self._db_pool.execute(
                "UPDATE document_processing.document_versions "
                "SET deleted = TRUE WHERE entity_id = $1",
                entity_id,
            )
        except Exception as exc:
            logger.warning(
                "doc_processor.mark_deleted_failed: entity_id=%s error=%s",
                entity_id,
                exc,
            )

    async def _hard_delete_versions_and_chunks(self, entity_id: str) -> None:
        """Hard-delete all Postgres rows for an entity.

        Deletes document_chunks first (FK child), then document_versions (FK parent),
        both inside a single transaction so a mid-flight failure leaves no orphans.

        This mirrors the code-pipeline semantics: when a document is updated or
        deleted, old rows are physically removed — not just soft-marked.
        """
        try:
            async with self._db_pool.acquire() as conn:
                async with conn.transaction():
                    deleted_chunks = await conn.fetchval(
                        "WITH deleted AS ("
                        "  DELETE FROM document_processing.document_chunks "
                        "  WHERE version_id IN ("
                        "    SELECT id FROM document_processing.document_versions "
                        "    WHERE entity_id = $1"
                        "  ) RETURNING 1"
                        ") SELECT COUNT(*) FROM deleted",
                        entity_id,
                    )
                    deleted_versions = await conn.fetchval(
                        "WITH deleted AS ("
                        "  DELETE FROM document_processing.document_versions "
                        "  WHERE entity_id = $1 RETURNING 1"
                        ") SELECT COUNT(*) FROM deleted",
                        entity_id,
                    )
            logger.info(
                "doc_processor.hard_deleted: entity_id=%s chunks=%s versions=%s",
                entity_id,
                deleted_chunks,
                deleted_versions,
            )
        except Exception as exc:
            logger.warning(
                "doc_processor.hard_delete_failed: entity_id=%s error=%s",
                entity_id,
                exc,
            )

    async def _store_document_chunks(
        self,
        version_id: str,
        chunks: list,
    ) -> None:
        """Insert chunk text records into document_processing.document_chunks.

        This is the canonical Postgres store for chunk text (enables re-export,
        version diff, and regeneration without Neo4j).  The table is FK-linked to
        document_versions via ``version_id``.

        Non-fatal: logs a warning on failure so the Kafka publish is not blocked.
        """
        try:
            await self._db_pool.executemany(
                "INSERT INTO document_processing.document_chunks "
                "(version_id, chunk_index, total_chunks, chunk_text, chunk_hash, section_heading) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                [
                    (
                        version_id,
                        c.chunk_index,
                        c.total_chunks,
                        c.text,
                        hashlib.md5(c.text.encode()).hexdigest(),
                        c.section_heading,
                    )
                    for c in chunks
                ],
            )
            logger.info(
                "doc_processor.stored_chunks: version_id=%s count=%d",
                version_id,
                len(chunks),
            )
        except Exception as exc:
            logger.warning(
                "doc_processor.store_chunks_failed: version_id=%s error=%s",
                version_id,
                exc,
            )

    async def _load_prompt_for_entity_type(self, entity_type: str) -> Optional[str]:
        """Load extraction prompt from Postgres for a given KGRAG entity_type.

        Queries code_processing.document_extraction_prompts table.
        Returns None if not found — caller MUST fail loudly (no silent fallback).
        """
        if not self._db_pool:
            logger.error(
                "doc_processor.no_db_pool — cannot load prompt for %s",
                entity_type,
            )
            return None
        try:
            row = await self._db_pool.fetchrow(
                """
                SELECT template_text
                  FROM code_processing.document_extraction_prompts
                 WHERE entity_type = $1
                 ORDER BY version DESC
                 LIMIT 1
                """,
                entity_type,
            )
            if row:
                return row["template_text"]
            logger.error(
                "doc_processor.no_prompt_for_entity_type: %s — add to "
                "code_processing.document_extraction_prompts",
                entity_type,
            )
            return None
        except Exception as exc:
            logger.error(
                "doc_processor.prompt_load_failed: %s error=%s",
                entity_type,
                exc,
            )
            return None


def _hash_content(text: str) -> str:
    """SHA-256 hex digest of text content for dedup."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

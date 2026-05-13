"""Kafka event emission for the document preprocessor.

Produces EnrichedChunkMessage (as JSON) to the enriched-code-chunks topic.
Handles both process and delete actions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import List, Optional

from aiokafka import AIOKafkaProducer

from document_preprocessor.config import DocumentPreprocessorSettings
from document_preprocessor.models import DocumentChunkResult
from shared.slugify import slugify

logger = logging.getLogger(__name__)


class DocumentEventEmitter:
    """Publishes document chunk events to Kafka."""

    def __init__(
        self,
        producer: AIOKafkaProducer,
        settings: DocumentPreprocessorSettings,
    ) -> None:
        self._producer = producer
        self._settings = settings

    async def emit_process_chunk(
        self,
        *,
        chunk: DocumentChunkResult,
        entity_id: str,
        entity_type: str,
        title: str,
        company_id: str,
        project_id: Optional[str],
        ingestion_id: str,
        extraction_prompt: str,
    ) -> None:
        """Publish a single document chunk as an EnrichedChunkMessage.

        .. deprecated::
            Use :meth:`emit_process_chunks_batch` instead. This method publishes
            chunks with ``embedding=None``, which causes ``v2-qdrant-storage`` to
            reject them. It is preserved for backward-compatibility with tests and
            any callers that cannot be migrated immediately.
        """
        chunk_id = str(uuid.uuid4())
        file_path = f"document://{entity_type}/{entity_id}"
        document_slug = slugify(title) or entity_id.lower()

        # Build context header (same structured format as code pipeline)
        header = _build_document_header(
            entity_type=entity_type,
            title=title,
            project_id=project_id,
            chunk_index=chunk.chunk_index,
            total_chunks=chunk.total_chunks,
            section_heading=chunk.section_heading,
        )

        message = {
            "action": "process",
            "company_id": company_id,
            "project_id": project_id or None,
            "repository": "kgrag-documents",
            "branch": "main",
            "file_path": file_path,
            "ingestion_id": ingestion_id,
            "file_version_id": entity_id,
            # Content routing fields
            "content_type": "document",
            "entity_type": entity_type,
            "document_title": title,
            "document_slug": document_slug,
            # Chunk data
            "chunk_id": chunk_id,
            "parent_id": entity_id,
            "language": "markdown",
            "chunk_index": chunk.chunk_index,
            "total_chunks": chunk.total_chunks,
            "content": chunk.text,
            "header": header,
            "embedding": None,
            "file_skeleton": "",
            # Extraction prompt
            "extraction_prompt": extraction_prompt,
        }

        key_bytes = chunk_id.encode("utf-8")
        value_bytes = json.dumps(message).encode("utf-8")

        try:
            await self._producer.send(
                self._settings.kafka_output_topic,
                value=value_bytes,
                key=key_bytes,
            )
        except Exception as exc:
            logger.error(
                "Failed to emit document chunk %s for %s/%s: %s",
                chunk_id,
                entity_type,
                entity_id,
                exc,
            )
            raise

    async def _embed_chunks(self, texts: List[str]) -> List[List[float]]:
        """Embed document chunk texts via LiteLLM Gemini API.

        Mirrors the ``code_preprocessor.enrichment.embed_and_publish_batch``
        pattern.  Uses ``GEMINI_API_KEY`` — no Vertex SA credentials required.

        Returns a list of embedding vectors, one per input text.
        Raises on unrecoverable failure — caller must NOT publish null-embedding messages.
        """
        import litellm  # imported lazily to avoid cost when embedding is not used

        api_key = self._settings.gemini_api_key or os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY not configured for document embedding")

        model = self._settings.embedding_model
        dimensions = self._settings.embedding_dimensions
        batch_size = self._settings.embedding_batch_size
        semaphore = asyncio.Semaphore(self._settings.embedding_concurrency)

        async def _embed_batch(batch_texts: List[str]) -> List[List[float]]:
            async with semaphore:
                for attempt in range(3):
                    try:
                        resp = await litellm.aembedding(
                            model=model,
                            input=batch_texts,
                            api_key=api_key,
                            dimensions=dimensions,
                        )
                        vectors = [item["embedding"] for item in resp.data]
                        if len(vectors) != len(batch_texts):
                            raise ValueError(
                                f"Embedding count mismatch: expected {len(batch_texts)}, got {len(vectors)}"
                            )
                        return vectors
                    except Exception as exc:
                        if "429" in str(exc) and attempt < 2:
                            await asyncio.sleep(30 * (attempt + 1))
                        else:
                            raise
            return []  # unreachable — satisfies type checker

        tasks = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            tasks.append(_embed_batch(batch))

        results = await asyncio.gather(*tasks)
        all_vectors: List[List[float]] = []
        for batch_vectors in results:
            all_vectors.extend(batch_vectors)
        return all_vectors

    async def emit_process_chunks_batch(
        self,
        *,
        chunks: List[DocumentChunkResult],
        entity_id: str,
        entity_type: str,
        title: str,
        company_id: str,
        project_id: Optional[str],
        ingestion_id: str,
        extraction_prompt: str,
    ) -> None:
        """Embed all chunks in one batched call, then publish each to Kafka.

        This is the preferred method over the deprecated ``emit_process_chunk``.
        Each Kafka message carries a populated ``embedding`` vector so that
        ``v2-qdrant-storage`` accepts it without a ``chunk.missing_required_fields``
        rejection.

        Raises on embedding failure — processor must NOT continue silently on error.
        """
        if not chunks:
            return

        texts = [c.text for c in chunks]

        try:
            embeddings = await self._embed_chunks(texts)
        except Exception as exc:
            logger.error(
                "doc_emitter.embedding_failed: entity_id=%s error=%s — chunks NOT published",
                entity_id,
                exc,
            )
            raise  # propagate to processor — fail loudly, never publish null embeddings

        document_slug = slugify(title) or entity_id.lower()
        file_path = f"document://{entity_type}/{entity_id}"

        for chunk, embedding in zip(chunks, embeddings):
            chunk_id = str(uuid.uuid4())
            header = _build_document_header(
                entity_type=entity_type,
                title=title,
                project_id=project_id,
                chunk_index=chunk.chunk_index,
                total_chunks=chunk.total_chunks,
                section_heading=chunk.section_heading,
            )
            message = {
                "action": "process",
                "company_id": company_id,
                "project_id": project_id or None,
                "repository": "kgrag-documents",
                "branch": "main",
                "file_path": file_path,
                "ingestion_id": ingestion_id,
                "file_version_id": entity_id,
                "content_type": "document",
                "entity_type": entity_type,
                "document_title": title,
                "document_slug": document_slug,
                "chunk_id": chunk_id,
                "parent_id": entity_id,
                "language": "markdown",
                "chunk_index": chunk.chunk_index,
                "total_chunks": chunk.total_chunks,
                "content": chunk.text,
                "header": header,
                "embedding": list(embedding),  # populated — not None
                "file_skeleton": "",
                "extraction_prompt": extraction_prompt,
            }
            key_bytes = chunk_id.encode("utf-8")
            value_bytes = json.dumps(message).encode("utf-8")
            try:
                await self._producer.send(
                    self._settings.kafka_output_topic,
                    value=value_bytes,
                    key=key_bytes,
                )
            except Exception as exc:
                logger.error(
                    "Failed to send embedded chunk %s for %s/%s: %s",
                    chunk_id,
                    entity_type,
                    entity_id,
                    exc,
                )
                raise

        logger.info(
            "doc_emitter.chunks_published count=%d embedded=True entity_id=%s",
            len(chunks),
            entity_id,
        )

    async def emit_delete(
        self,
        *,
        entity_id: str,
        entity_type: str,
        company_id: str,
        project_id: Optional[str],
        ingestion_id: str,
    ) -> None:
        """Emit a delete message to remove all chunks for an entity."""
        file_path = f"document://{entity_type}/{entity_id}"

        message = {
            "action": "delete",
            "company_id": company_id,
            "project_id": project_id or None,
            "repository": "kgrag-documents",
            "branch": "main",
            "file_path": file_path,
            "ingestion_id": ingestion_id,
            "file_version_id": entity_id,
            "content_type": "document",
            "entity_type": entity_type,
        }

        key_bytes = file_path.encode("utf-8")
        value_bytes = json.dumps(message).encode("utf-8")

        try:
            await self._producer.send(
                self._settings.kafka_output_topic,
                value=value_bytes,
                key=key_bytes,
            )
            logger.info(
                "Emitted document delete for %s/%s",
                entity_type,
                entity_id,
            )
        except Exception as exc:
            logger.warning(
                "Failed to emit document delete for %s/%s: %s",
                entity_type,
                entity_id,
                exc,
            )

    async def flush(self) -> None:
        """Flush the producer to ensure all messages are sent."""
        await self._producer.flush()


def _build_document_header(
    *,
    entity_type: str,
    title: str,
    project_id: Optional[str],
    chunk_index: int,
    total_chunks: int,
    section_heading: Optional[str],
) -> str:
    """Build a structured context header for a document chunk.

    Mirrors the code pipeline header format (PROJECT/FILE/SIGNATURE/CODE sections)
    but adapted for documents (DOCUMENT/CONTENT sections).
    """
    parts: list[str] = []

    # Document metadata section
    scope = project_id if project_id else "company-level"
    doc_section = f"=== DOCUMENT ===\nType: {entity_type}\nTitle: {title}\nProject: {scope}"
    parts.append(doc_section)

    # Section heading (if from a heading-split chunk)
    if section_heading:
        parts.append(f"=== SECTION ===\n{section_heading}")

    # Content position
    parts.append(f"=== CONTENT (chunk {chunk_index + 1}/{total_chunks}) ===")

    return "\n\n".join(parts)

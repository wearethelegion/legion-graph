"""Enrichment pipeline: skeleton extraction, chunking, embedding, Kafka publish.

Wires into consumer.py to populate code_processing.file_chunks with embeddings
and publish enriched chunks to Kafka for downstream consumers (e.g. Cognee).
"""

import asyncio
import hashlib
import json
import logging
import os
from typing import Any, Optional

import asyncpg
from aiokafka import AIOKafkaProducer

from code_preprocessor.skeleton_extractor import extract_skeleton
from code_preprocessor.chunker import chunk_file

logger = logging.getLogger(__name__)


def simple_chunk(text: str, max_chars: int = 1000) -> list[str]:
    """Split text into chunks by accumulating lines until ~max_chars."""
    chunks = []
    current = []
    current_len = 0
    for line in text.split("\n"):
        if current_len + len(line) + 1 > max_chars and current:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += len(line) + 1
    if current:
        chunks.append("\n".join(current))
    return chunks


async def get_file_version_id(pool: asyncpg.Pool, document_id: str) -> Optional[str]:
    """Get the UUID id for a file version by its document_id."""
    row = await pool.fetchrow(
        """
        SELECT id FROM code_processing.repository_file_versions
         WHERE document_id = $1
        """,
        document_id,
    )
    return str(row["id"]) if row else None


async def enrich_and_store_file(
    pool: asyncpg.Pool,
    document_id: str,
    file_path: str,
    content: str,
    ingestion_id: str,
    project_id: str,
    company_id: str,
) -> int:
    """Extract skeleton, chunk content, store to Postgres.

    Args:
        document_id: The document_id field from the version record (not the UUID id).

    Returns number of chunks created.
    """
    # Get the actual UUID id
    file_version_id = await get_file_version_id(pool, document_id)
    if not file_version_id:
        logger.warning("No file version found for document_id %s", document_id)
        return 0

    # Extract skeleton
    skeleton_data = extract_skeleton(file_path, content)
    language = None
    file_skeleton = None

    if skeleton_data:
        language = skeleton_data.get("language")
        file_skeleton = json.dumps(skeleton_data.get("declarations", []))

    # Update file version with skeleton
    await pool.execute(
        """
        UPDATE code_processing.repository_file_versions
           SET language = $2, file_skeleton = $3::jsonb
         WHERE id = $1
        """,
        file_version_id,
        language,
        file_skeleton,
    )

    # Chunk content using smart chunker (TreeSitter for code, recursive text for others)
    # Returns list of (chunk_text, start_line, end_line) tuples
    chunks = chunk_file(file_path, content)
    total_chunks = len(chunks)

    # Insert chunks
    for idx, (chunk_text, start_line, end_line) in enumerate(chunks):
        chunk_hash = hashlib.md5(chunk_text.encode("utf-8")).hexdigest()
        await pool.execute(
            """
            INSERT INTO code_processing.file_chunks
                (chunk_text, chunk_index, total_chunks, chunk_hash,
                 file_version_id, ingestion_id, project_id, company_id, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
            """,
            chunk_text,
            idx,
            total_chunks,
            chunk_hash,
            file_version_id,
            ingestion_id,
            project_id,
            company_id,
        )

    return total_chunks


async def batch_embed_chunks(pool: asyncpg.Pool, ingestion_id: str, batch_size: int = 100) -> int:
    """Batch embed all pending chunks for an ingestion via Gemini API (litellm).
    Pipeline order: enrich_chunks() → batch_embed_chunks().
    """
    import litellm

    api_key = os.environ.get("GEMINI_API_KEY")
    vertex_project = os.environ.get("VERTEXAI_PROJECT")
    vertex_location = os.environ.get("VERTEXAI_LOCATION", "us-central1")
    if not api_key and not vertex_project:
        logger.warning("Neither GEMINI_API_KEY nor VERTEXAI_PROJECT set — skipping embedding")
        return 0
    embed_model = os.environ.get("EMBEDDING_MODEL", "vertex_ai/gemini-embedding-001")

    rows = await pool.fetch(
        "SELECT id, chunk_text FROM code_processing.file_chunks "
        "WHERE ingestion_id = $1 AND embedding IS NULL ORDER BY id",
        ingestion_id,
    )
    if not rows:
        return 0

    max_concurrent = int(os.environ.get("EMBEDDING_CONCURRENCY", "5"))
    semaphore = asyncio.Semaphore(max_concurrent)
    total_batches = (len(rows) + batch_size - 1) // batch_size
    max_retries = 3

    # Round-robin across regions to multiply effective token quota
    regions = os.environ.get(
        "VERTEXAI_EMBEDDING_REGIONS",
        os.environ.get("VERTEXAI_REGIONS", vertex_location or "us-central1"),
    ).split(",")

    async def _embed_batch(batch_idx: int, batch_rows: list) -> int:
        chunk_ids = [row["id"] for row in batch_rows]
        texts = [row["chunk_text"] for row in batch_rows]
        region = regions[batch_idx % len(regions)]

        async with semaphore:
            for attempt in range(max_retries):
                try:
                    resp = await litellm.aembedding(
                        model=embed_model,
                        input=texts,
                        api_key=api_key,
                        vertex_project=vertex_project,
                        vertex_location=region,
                        dimensions=int(os.environ.get("EMBEDDING_DIMENSIONS", "3072")),
                    )
                    vectors = [item["embedding"] for item in resp.data]
                    for chunk_id, vector in zip(chunk_ids, vectors):
                        await pool.execute(
                            "UPDATE code_processing.file_chunks SET embedding = $2 WHERE id = $1",
                            chunk_id,
                            vector,
                        )
                    logger.info(
                        "Embedded batch %d/%d (%d chunks, region=%s)",
                        batch_idx + 1,
                        total_batches,
                        len(chunk_ids),
                        region,
                    )
                    return len(chunk_ids)
                except Exception as exc:
                    if "429" in str(exc) and attempt < max_retries - 1:
                        wait = 30 * (attempt + 1)
                        logger.warning(
                            "Batch %d rate limited (region=%s), retry in %ds",
                            batch_idx + 1,
                            region,
                            wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.error("Embedding batch %d failed: %s", batch_idx + 1, exc)
                        return 0
        return 0

    tasks = [
        _embed_batch(i // batch_size, rows[i : i + batch_size])
        for i in range(0, len(rows), batch_size)
    ]
    return sum(await asyncio.gather(*tasks))


def _format_skeleton(file_skeleton) -> str:
    """Format JSONB skeleton array into indented declaration lines."""
    if not file_skeleton:
        return ""
    if isinstance(file_skeleton, str):
        file_skeleton = json.loads(file_skeleton)
    return "\n".join(f"  {decl}" for decl in file_skeleton)


def _build_project_section(analysis: dict) -> str:
    """Build the === PROJECT === header from project_analysis JSONB."""
    if not analysis:
        return ""
    domains = ", ".join(
        d["name"]
        for d in analysis.get("business_domains", [])
        if isinstance(d, dict) and "name" in d
    )
    patterns = ", ".join(
        p["name"]
        for p in analysis.get("design_patterns", [])
        if isinstance(p, dict) and "name" in p
    )
    arch = analysis.get("architecture", {})
    arch_style = arch.get("style", "") if isinstance(arch, dict) else ""
    lines = ["=== PROJECT ===", analysis.get("description", "")]
    if domains:
        lines.append(f"Business domains: {domains}")
    if patterns:
        lines.append(f"Design patterns: {patterns}")
    if arch_style:
        lines.append(f"Architecture: {arch_style}")
    return "\n".join(lines)


def build_chunk_header(
    project_analysis: Optional[dict],
    file_path: str,
    language: Optional[str],
    file_skeleton: Optional[str],
    chunk_index: int,
    total_chunks: int,
) -> str:
    """Build enrichment header for a single chunk (pure function).

    Used by file workers in the two-stage streaming pipeline.
    Combines project context, file metadata, and skeleton into a header
    that is stored alongside the raw chunk text.

    Args:
        project_analysis: Parsed project_analysis dict (from analyze_project).
        file_path: Relative file path in the repository.
        language: Detected programming language (or None).
        file_skeleton: JSON string of skeleton declarations (or None).
        chunk_index: Zero-based index of this chunk within the file.
        total_chunks: Total number of chunks for this file.

    Returns:
        Multi-section header string.
    """
    project_section = _build_project_section(project_analysis) if project_analysis else ""
    skel = _format_skeleton(file_skeleton)

    parts: list[str] = []
    if project_section:
        parts.append(project_section)
    parts.append(f"=== FILE ===\nPath: {file_path}\nLanguage: {language or 'unknown'}")
    if skel:
        parts.append(f"=== SIGNATURE ===\n{skel}")
    parts.append(f"=== CODE (chunk {chunk_index + 1}/{total_chunks}) ===")
    return "\n\n".join(parts)


async def embed_and_publish_batch(
    pool: asyncpg.Pool,
    producer: AIOKafkaProducer,
    chunks: list[dict],
    embed_model: str,
    topic: str = "enriched-code-chunks",
    pipeline_store: "Optional[Any]" = None,
) -> int:
    """Embed a batch of chunks, update Postgres, publish to Kafka.

    Used by embed workers in the two-stage streaming pipeline.

    Each item in *chunks* must contain:
        chunk_id, chunk_text, header, file_path, language, file_skeleton,
        repository, branch, ingestion_id, project_id, company_id,
        chunk_index, total_chunks, file_version_id

    Returns number of chunks successfully embedded and published.
    """
    import litellm

    if not chunks:
        return 0

    api_key = os.environ.get("GEMINI_API_KEY")
    vertex_project = os.environ.get("VERTEXAI_PROJECT")
    vertex_location = os.environ.get("VERTEXAI_LOCATION", "us-central1")
    regions = os.environ.get(
        "VERTEXAI_EMBEDDING_REGIONS", os.environ.get("VERTEXAI_REGIONS", vertex_location)
    ).split(",")

    texts = [c["chunk_text"] for c in chunks]
    region = regions[0]  # embed workers rotate via batch index externally

    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = await litellm.aembedding(
                model=embed_model,
                input=texts,
                api_key=api_key,
                vertex_project=vertex_project,
                vertex_location=region,
                dimensions=int(os.environ.get("EMBEDDING_DIMENSIONS", "3072")),
            )
            break
        except Exception as exc:
            if "429" in str(exc) and attempt < max_retries - 1:
                wait = 30 * (attempt + 1)
                logger.warning("embed batch rate limited (region=%s), retry in %ds", region, wait)
                await asyncio.sleep(wait)
            else:
                logger.error("embed batch failed after %d attempts: %s", attempt + 1, exc)
                return 0
    else:
        return 0

    vectors = [item["embedding"] for item in resp.data]
    if len(vectors) != len(chunks):
        logger.error("Embedding count mismatch: expected %d, got %d", len(chunks), len(vectors))
        return 0

    # Update embeddings in Postgres
    await pool.executemany(
        "UPDATE code_processing.file_chunks SET embedding = $2 WHERE id = $1",
        [(c["chunk_id"], v) for c, v in zip(chunks, vectors)],
    )

    # Publish each chunk to Kafka + pipeline_chunks staging
    pipeline_batch: list[dict] = []
    for chunk, vector in zip(chunks, vectors):
        skel_text = _format_skeleton(chunk.get("file_skeleton"))
        message = {
            "chunk_id": str(chunk["chunk_id"]),
            "parent_id": str(chunk["file_version_id"]),
            "file_version_id": str(chunk["file_version_id"]),
            "ingestion_id": chunk["ingestion_id"],
            "company_id": chunk["company_id"],
            "project_id": chunk["project_id"],
            "repository": chunk["repository"],
            "branch": chunk["branch"],
            "file_path": chunk["file_path"],
            "language": chunk["language"],
            "chunk_index": chunk["chunk_index"],
            "total_chunks": chunk["total_chunks"],
            "content": chunk["chunk_text"],
            "header": chunk["header"],
            "embedding": list(vector),
            "file_skeleton": skel_text,
            "start_line": chunk.get("start_line", 0),
            "end_line": chunk.get("end_line", 0),
            # V3: project-specific extraction prompt; null for legacy messages
            "extraction_prompt": chunk.get("extraction_prompt"),
            # Phase 2: per-file content_type (e.g. 'ruby_spec') for routing traceability
            # Falls back to 'code' (EnrichedChunkMessage default) when not set.
            "content_type": chunk.get("content_type", "code"),
        }
        key_bytes = str(chunk["chunk_id"]).encode("utf-8")
        value_bytes = json.dumps(message).encode("utf-8")
        await producer.send(topic, value=value_bytes, key=key_bytes)

        if pipeline_store:
            pipeline_batch.append(
                {
                    "chunk_id": str(chunk["chunk_id"]),
                    "ingestion_id": chunk["ingestion_id"],
                    "company_id": chunk["company_id"],
                    "project_id": chunk["project_id"],
                    "file_path": chunk["file_path"],
                    "repository": chunk["repository"],
                    "branch": chunk["branch"],
                    "language": chunk["language"],
                    "chunk_index": chunk["chunk_index"],
                    "total_chunks": chunk["total_chunks"],
                    "content": chunk["chunk_text"],
                    "header": chunk["header"],
                    "embedding": list(vector),
                    "file_skeleton": skel_text,
                    "start_line": chunk.get("start_line", 0),
                    "end_line": chunk.get("end_line", 0),
                    "extraction_prompt": chunk.get("extraction_prompt"),
                }
            )

    await producer.flush()

    if pipeline_store and pipeline_batch:
        try:
            await pipeline_store.store_chunks_batch(pipeline_batch)
        except Exception as exc:
            logger.warning("Failed to store %d pipeline chunks: %s", len(pipeline_batch), exc)

    return len(chunks)


async def store_project_tree(pool: asyncpg.Pool, ingestion_id: str, project_tree: str):
    """Store project tree in ingestion_batches."""
    await pool.execute(
        """
        UPDATE code_processing.ingestion_batches
           SET project_tree = $2
         WHERE ingestion_id = $1
        """,
        ingestion_id,
        project_tree,
    )


async def emit_ingestion_complete(
    producer: AIOKafkaProducer,
    ingestion_id: str,
    company_id: str,
    project_id: str,
    total_files: int,
    total_chunks: int,
    topic: str = "pipeline-events",
) -> None:
    """Emit ingestion_complete signal to the pipeline-events Kafka topic.

    This signals downstream services (entity extraction, summarization)
    that the preprocessor has finished producing all chunks for this ingestion.
    """
    from datetime import datetime, timezone

    event = {
        "event_type": "ingestion_complete",
        "ingestion_id": ingestion_id,
        "company_id": company_id,
        "project_id": project_id,
        "total_files": total_files,
        "total_chunks": total_chunks,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        key_bytes = ingestion_id.encode("utf-8")
        value_bytes = json.dumps(event).encode("utf-8")
        await producer.send(topic, value=value_bytes, key=key_bytes)
        await producer.flush()
        logger.info(
            "Emitted ingestion_complete for %s: %d files, %d chunks",
            ingestion_id,
            total_files,
            total_chunks,
        )
    except Exception as exc:
        logger.error(
            "Failed to emit ingestion_complete for %s: %s",
            ingestion_id,
            exc,
        )


# ── Deprecated re-exports (moved to _legacy_enrichment.py) ──────────
# Kept here so existing test/script imports don't break.

from code_preprocessor._legacy_enrichment import (  # noqa: E402, F401
    enrich_chunks,
    batch_embed_chunks,
    publish_enriched_chunks,
)

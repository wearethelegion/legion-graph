"""Legacy batch enrichment functions — deprecated by the two-stage streaming pipeline.

These functions are kept for backward compatibility with tests and scripts.
New code should NOT import from this module. Use the streaming pipeline
(build_chunk_header + embed_and_publish_batch) in enrichment.py instead.
"""

import asyncio
import json
import logging
import os
from typing import Any, Optional

import asyncpg
from aiokafka import AIOKafkaProducer

from code_preprocessor.enrichment import (
    _build_project_section,
    _format_skeleton,
)

logger = logging.getLogger(__name__)


async def enrich_chunks(pool: asyncpg.Pool, ingestion_id: str) -> int:
    """Build context header for each chunk and store it separately.

    .. deprecated:: Use build_chunk_header() in file workers instead.
    """
    meta = await pool.fetchrow(
        "SELECT repository, branch FROM code_processing.ingestion_batches WHERE ingestion_id = $1",
        ingestion_id,
    )
    if not meta:
        logger.error("No ingestion batch found for %s", ingestion_id)
        return 0
    repository, branch = meta["repository"], meta["branch"]

    analysis_row = await pool.fetchrow(
        "SELECT project_analysis FROM code_processing.ingestion_batches "
        "WHERE repository = $1 AND branch = $2 AND project_analysis IS NOT NULL "
        "ORDER BY created_at DESC LIMIT 1",
        repository,
        branch,
    )
    project_analysis = None
    if analysis_row and analysis_row["project_analysis"]:
        pa = analysis_row["project_analysis"]
        project_analysis = json.loads(pa) if isinstance(pa, str) else pa
    project_section = _build_project_section(project_analysis)
    if not project_section:
        logger.warning(
            "No project_analysis for %s/%s — skipping project context", repository, branch
        )

    rows = await pool.fetch(
        "SELECT c.id, c.chunk_text, c.chunk_index, c.total_chunks, "
        "  f.file_path, f.language, f.file_skeleton "
        "FROM code_processing.file_chunks c "
        "JOIN code_processing.repository_file_versions f ON c.file_version_id = f.id "
        "WHERE c.ingestion_id = $1 ORDER BY f.file_path, c.chunk_index",
        ingestion_id,
    )
    if not rows:
        logger.warning("No chunks found for ingestion %s", ingestion_id)
        return 0
    logger.info("Enriching %d chunks for ingestion %s", len(rows), ingestion_id)

    updates = []
    for row in rows:
        skel = _format_skeleton(row["file_skeleton"])
        parts = []
        if project_section:
            parts.append(project_section)
        parts.append(
            f"=== FILE ===\nPath: {row['file_path']}\nLanguage: {row['language'] or 'unknown'}"
        )
        if skel:
            parts.append(f"=== SIGNATURE ===\n{skel}")
        parts.append(f"=== CODE (chunk {row['chunk_index'] + 1}/{row['total_chunks']}) ===")
        header = "\n\n".join(parts)
        updates.append((row["id"], header))

    await pool.executemany(
        "UPDATE code_processing.file_chunks SET header = $2 WHERE id = $1",
        updates,
    )
    enriched_count = len(updates)

    first_row = await pool.fetchrow(
        "SELECT header, chunk_text FROM code_processing.file_chunks WHERE id = $1",
        rows[0]["id"],
    )
    if first_row:
        header_preview = (first_row["header"][:300] + "...") if first_row["header"] else ""
        code_preview = (first_row["chunk_text"][:200] + "...") if first_row["chunk_text"] else ""
        logger.info(
            "Enriched %d chunks. First chunk header:\n%s\nCode preview:\n%s",
            enriched_count,
            header_preview,
            code_preview,
        )
    return enriched_count


async def batch_embed_chunks(pool: asyncpg.Pool, ingestion_id: str, batch_size: int = 100) -> int:
    """Batch embed all pending chunks for an ingestion.

    .. deprecated:: Use embed_and_publish_batch() in embed workers instead.
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
    regions = os.environ.get("VERTEXAI_REGIONS", vertex_location or "us-central1").split(",")

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
                        dimensions=768,
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


async def publish_enriched_chunks(
    pool: asyncpg.Pool,
    producer: AIOKafkaProducer,
    repository: str,
    branch: str,
    company_id: str = "",
    project_id: str = "",
    topic: str = "enriched-code-chunks",
    ingestion_id: str = "",
    pipeline_store: "Optional[Any]" = None,
) -> int:
    """Publish embedded chunks to Kafka AND store in pipeline_chunks.

    .. deprecated:: Use embed_and_publish_batch() in embed workers instead.
    """
    rows = await pool.fetch(
        """
        SELECT c.id        AS chunk_id,
               c.file_version_id AS parent_id,
               c.ingestion_id,
               c.chunk_text AS content,
               c.header,
               c.chunk_index,
               c.total_chunks,
               c.embedding,
               f.file_path,
               f.language,
               f.repository,
               f.branch,
               f.file_skeleton
          FROM code_processing.file_chunks c
          JOIN code_processing.repository_file_versions f
            ON c.file_version_id = f.id
         WHERE f.repository = $1
           AND f.branch = $2
           AND c.embedding IS NOT NULL
         ORDER BY f.file_path, c.chunk_index
        """,
        repository,
        branch,
    )

    if not rows:
        logger.info("No embedded chunks to publish for %s@%s", repository, branch)
        return 0

    flush_interval = 100
    published = 0
    pipeline_chunk_batch: list[dict] = []
    batch_size = 50

    for row in rows:
        file_skeleton_text = _format_skeleton(row.get("file_skeleton"))
        message = {
            "chunk_id": str(row["chunk_id"]),
            "parent_id": str(row["parent_id"]),
            "file_version_id": str(row["parent_id"]),
            "ingestion_id": row["ingestion_id"],
            "company_id": company_id,
            "project_id": project_id,
            "repository": row["repository"],
            "branch": row["branch"],
            "file_path": row["file_path"],
            "language": row["language"],
            "chunk_index": row["chunk_index"],
            "total_chunks": row["total_chunks"],
            "content": row["content"],
            "header": row["header"] or "",
            "embedding": list(row["embedding"]) if row["embedding"] else [],
            "file_skeleton": file_skeleton_text,
        }

        key_bytes = str(row["chunk_id"]).encode("utf-8")
        value_bytes = json.dumps(message).encode("utf-8")
        await producer.send(topic, value=value_bytes, key=key_bytes)
        published += 1

        if pipeline_store:
            pipeline_chunk_batch.append(
                {
                    "chunk_id": str(row["chunk_id"]),
                    "ingestion_id": row["ingestion_id"] or ingestion_id,
                    "company_id": company_id,
                    "project_id": project_id,
                    "file_path": row["file_path"],
                    "repository": row["repository"],
                    "branch": row["branch"],
                    "language": row["language"],
                    "chunk_index": row["chunk_index"],
                    "total_chunks": row["total_chunks"],
                    "content": row["content"],
                    "header": row["header"] or "",
                    "embedding": list(row["embedding"]) if row["embedding"] else None,
                    "file_skeleton": file_skeleton_text,
                }
            )

            if len(pipeline_chunk_batch) >= batch_size:
                try:
                    await pipeline_store.store_chunks_batch(pipeline_chunk_batch)
                except Exception as exc:
                    logger.warning(
                        "Failed to batch-store %d pipeline chunks: %s",
                        len(pipeline_chunk_batch),
                        exc,
                    )
                pipeline_chunk_batch = []

        if published % flush_interval == 0:
            await producer.flush()

    await producer.flush()

    if pipeline_store and pipeline_chunk_batch:
        try:
            await pipeline_store.store_chunks_batch(pipeline_chunk_batch)
        except Exception as exc:
            logger.warning(
                "Failed to store final %d pipeline chunks: %s",
                len(pipeline_chunk_batch),
                exc,
            )

    logger.info(
        "Published %d enriched chunks to topic '%s' for %s@%s",
        published,
        topic,
        repository,
        branch,
    )

    if ingestion_id:
        try:
            files_produced_count = await pool.fetchval(
                """
                SELECT COUNT(DISTINCT f.file_path)
                  FROM code_processing.file_chunks c
                  JOIN code_processing.repository_file_versions f
                    ON c.file_version_id = f.id
                 WHERE f.repository = $1
                   AND f.branch = $2
                   AND c.ingestion_id = $3
                   AND c.embedding IS NOT NULL
                """,
                repository,
                branch,
                ingestion_id,
            )
            if files_produced_count:
                await pool.execute(
                    """
                    UPDATE code_processing.cogni_ingestion_stats
                       SET files_produced = $2
                     WHERE ingestion_id = $1
                    """,
                    ingestion_id,
                    files_produced_count,
                )
                logger.info(
                    "Updated files_produced=%d for ingestion %s",
                    files_produced_count,
                    ingestion_id,
                )
        except Exception as exc:
            logger.warning("Failed to update files_produced for %s: %s", ingestion_id, exc)

    return published

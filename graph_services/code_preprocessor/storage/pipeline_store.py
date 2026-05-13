"""Postgres-backed storage for v2 pipeline counters and chunk staging.

Provides:
- pipeline_counters: per-service, per-ingestion counter tracking
- pipeline_chunks: denormalized chunk storage for downstream services

All methods are async. The asyncpg pool is injected.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg

logger = logging.getLogger(__name__)


class PipelineStore:
    """Track pipeline counters and store chunks in Postgres staging tables.

    The asyncpg pool is injected — this class never creates or closes it.
    """

    SERVICE_NAME = "preprocessor"

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── Pipeline Counters ─────────────────────────────────────────────

    async def set_counter(
        self,
        ingestion_id: str,
        counter_name: str,
        counter_value: int,
        status: str = "running",
    ) -> None:
        """Upsert a pipeline counter for this service.

        Uses ON CONFLICT to be idempotent — updates value if already exists.
        """
        await self._pool.execute(
            """
            INSERT INTO code_processing.pipeline_counters
                (ingestion_id, service_name, counter_name, counter_value, status)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (ingestion_id, service_name, counter_name)
            DO UPDATE SET
                counter_value = EXCLUDED.counter_value,
                status = EXCLUDED.status,
                updated_at = NOW()
            """,
            ingestion_id,
            self.SERVICE_NAME,
            counter_name,
            counter_value,
            status,
        )

    async def increment_counter(
        self,
        ingestion_id: str,
        counter_name: str,
        delta: int = 1,
    ) -> None:
        """Atomically increment a pipeline counter."""
        await self._pool.execute(
            """
            INSERT INTO code_processing.pipeline_counters
                (ingestion_id, service_name, counter_name, counter_value, status)
            VALUES ($1, $2, $3, $4, 'running')
            ON CONFLICT (ingestion_id, service_name, counter_name)
            DO UPDATE SET
                counter_value = code_processing.pipeline_counters.counter_value + EXCLUDED.counter_value,
                updated_at = NOW()
            """,
            ingestion_id,
            self.SERVICE_NAME,
            counter_name,
            delta,
        )

    async def finalize_counters(self, ingestion_id: str) -> None:
        """Mark all counters for this ingestion+service as complete."""
        await self._pool.execute(
            """
            UPDATE code_processing.pipeline_counters
               SET status = 'complete', updated_at = NOW()
             WHERE ingestion_id = $1
               AND service_name = $2
            """,
            ingestion_id,
            self.SERVICE_NAME,
        )

    async def get_counters(self, ingestion_id: str) -> Dict[str, int]:
        """Get all counter values for this ingestion+service."""
        rows = await self._pool.fetch(
            """
            SELECT counter_name, counter_value
              FROM code_processing.pipeline_counters
             WHERE ingestion_id = $1
               AND service_name = $2
            """,
            ingestion_id,
            self.SERVICE_NAME,
        )
        return {row["counter_name"]: row["counter_value"] for row in rows}

    # ── Pipeline Chunks ───────────────────────────────────────────────

    async def store_chunk(
        self,
        *,
        chunk_id: str,
        ingestion_id: str,
        company_id: str,
        project_id: str,
        file_path: str,
        repository: str,
        branch: str,
        language: Optional[str],
        chunk_index: int,
        total_chunks: int,
        content: str,
        header: str = "",
        embedding: Optional[List[float]] = None,
        file_skeleton: str = "",
    ) -> None:
        """Store a single chunk in the pipeline_chunks staging table."""
        await self._pool.execute(
            """
            INSERT INTO code_processing.pipeline_chunks
                (chunk_id, ingestion_id, company_id, project_id,
                 file_path, repository, branch, language,
                 chunk_index, total_chunks, content, header,
                 embedding, file_skeleton)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13, $14)
            ON CONFLICT (chunk_id) DO UPDATE SET
                content = EXCLUDED.content,
                header = EXCLUDED.header,
                embedding = EXCLUDED.embedding,
                file_skeleton = EXCLUDED.file_skeleton
            """,
            chunk_id,
            ingestion_id,
            company_id,
            project_id,
            file_path,
            repository,
            branch,
            language,
            chunk_index,
            total_chunks,
            content,
            header,
            embedding,
            file_skeleton,
        )

    async def store_chunks_batch(
        self,
        chunks: List[Dict[str, Any]],
    ) -> int:
        """Batch-store multiple chunks using executemany for efficiency.

        Each dict must have keys matching store_chunk parameters.
        Returns number of chunks stored.
        """
        if not chunks:
            return 0

        records = [
            (
                c["chunk_id"],
                c["ingestion_id"],
                c["company_id"],
                c["project_id"],
                c["file_path"],
                c["repository"],
                c["branch"],
                c.get("language"),
                c["chunk_index"],
                c["total_chunks"],
                c["content"],
                c.get("header", ""),
                c.get("embedding"),
                c.get("file_skeleton", ""),
            )
            for c in chunks
        ]

        await self._pool.executemany(
            """
            INSERT INTO code_processing.pipeline_chunks
                (chunk_id, ingestion_id, company_id, project_id,
                 file_path, repository, branch, language,
                 chunk_index, total_chunks, content, header,
                 embedding, file_skeleton)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13, $14)
            ON CONFLICT (chunk_id) DO UPDATE SET
                content = EXCLUDED.content,
                header = EXCLUDED.header,
                embedding = EXCLUDED.embedding,
                file_skeleton = EXCLUDED.file_skeleton
            """,
            records,
        )

        return len(records)

    async def get_chunk_count(self, ingestion_id: str) -> int:
        """Count total chunks stored for an ingestion."""
        return await self._pool.fetchval(
            "SELECT COUNT(*) FROM code_processing.pipeline_chunks WHERE ingestion_id = $1",
            ingestion_id,
        )

    async def close(self) -> None:
        """No-op — the pool is managed externally."""

"""Postgres store for Qdrant Storage Service (Service 5).

All entity, edge, summary, and embedding data flows through Kafka topics.
The service consumes directly from Kafka in its consumer loop.

Postgres tables used:
- pipeline_chunks: chunk text + pre-computed embeddings (preprocessor staging)
- pipeline_counters: counter tracking (shared table)

All methods are async. The asyncpg pool is injected.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


class QdrantPipelineStore:
    """Postgres store for reading pipeline data and writing counters.

    The asyncpg pool is injected — this class never creates or closes it.
    """

    SERVICE_NAME = "qdrant_storage"

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── Table Initialization ──────────────────────────────────────────

    async def ensure_tables(self) -> None:
        """Create pipeline_counters if it doesn't exist.

        pipeline_chunks is created by the preprocessor upstream.
        All entity/edge/summary/embedding data flows via Kafka, not Postgres.
        """
        async with self._pool.acquire() as conn:
            await conn.execute("CREATE SCHEMA IF NOT EXISTS code_processing;")

            # ── pipeline_counters (defensive) ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS code_processing.pipeline_counters (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    ingestion_id UUID NOT NULL,
                    service_name VARCHAR(50) NOT NULL,
                    counter_name VARCHAR(50) NOT NULL,
                    counter_value INTEGER NOT NULL DEFAULT 0,
                    status VARCHAR(20) NOT NULL DEFAULT 'running',
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE(ingestion_id, service_name, counter_name)
                );
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pipeline_counters_ingestion
                ON code_processing.pipeline_counters(ingestion_id);
            """)

        logger.info("QdrantPipelineStore: tables ensured")

    # ── Read: Chunks ──────────────────────────────────────────────────

    async def fetch_chunks(
        self,
        ingestion_id: str,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Fetch chunks with embeddings for a given ingestion.

        Returns list of dicts with chunk_id, content, header, embedding,
        company_id, project_id, file_path, language, repository, branch.
        Only returns chunks that have a non-null embedding.
        """
        rows = await self._pool.fetch(
            """
            SELECT chunk_id, ingestion_id, company_id, project_id,
                   file_path, repository, branch, language,
                   chunk_index, total_chunks, content, header, embedding
              FROM code_processing.pipeline_chunks
             WHERE ingestion_id = $1
               AND embedding IS NOT NULL
             ORDER BY chunk_index
             LIMIT $2 OFFSET $3
            """,
            ingestion_id,
            limit,
            offset,
        )
        return [dict(row) for row in rows]

    async def count_chunks(self, ingestion_id: str) -> int:
        """Count chunks with embeddings for an ingestion."""
        return (
            await self._pool.fetchval(
                """
            SELECT COUNT(*) FROM code_processing.pipeline_chunks
             WHERE ingestion_id = $1
               AND embedding IS NOT NULL
            """,
                ingestion_id,
            )
            or 0
        )

    # ── Pipeline Counters ─────────────────────────────────────────────

    async def increment_counter(
        self,
        ingestion_id: str,
        counter_name: str,
        delta: int = 1,
    ) -> None:
        """Atomically increment a pipeline counter for this service."""
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

    async def set_counter(
        self,
        ingestion_id: str,
        counter_name: str,
        counter_value: int,
        status: str = "running",
    ) -> None:
        """Upsert a pipeline counter (absolute value)."""
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

    async def get_all_counters(self, ingestion_id: str) -> Dict[str, int]:
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

    # ── Dedup Gate Check ──────────────────────────────────────────────

    async def check_phase_a_complete(self, ingestion_id: str) -> Dict[str, str]:
        """Check whether all Phase A services have reported complete.

        Returns dict of {service_name: status} for the three Phase A services.
        """
        rows = await self._pool.fetch(
            """
            SELECT DISTINCT service_name, status
              FROM code_processing.pipeline_counters
             WHERE ingestion_id = $1
               AND service_name IN ('entity_extraction', 'summarization', 'embedding')
               AND counter_name = 'status_final'
            """,
            ingestion_id,
        )
        return {row["service_name"]: row["status"] for row in rows}

"""Postgres store for Neo4j Storage Service (Service 6).

All entity, edge, and summary data flows through Kafka topics.
The service consumes directly from Kafka in its consumer loop.

Postgres tables used:
- pipeline_chunks: chunk metadata for DocumentChunk nodes (preprocessor staging)
- pipeline_counters: counter tracking (shared table)

All methods are async. The asyncpg pool is injected.
"""

from __future__ import annotations

from typing import Any, Dict, List

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


class Neo4jPipelineStore:
    """Postgres store for reading pipeline data and writing counters.

    The asyncpg pool is injected — this class never creates or closes it.
    """

    SERVICE_NAME = "neo4j_storage"

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── Table Initialization ──────────────────────────────────────────

    async def ensure_tables(self) -> None:
        """Ensure pipeline_counters table exists (defensive).

        pipeline_chunks is created by the preprocessor upstream.
        All entity/edge/summary data flows via Kafka, not Postgres.
        """
        async with self._pool.acquire() as conn:
            await conn.execute("CREATE SCHEMA IF NOT EXISTS code_processing;")

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

        logger.info("Neo4jPipelineStore: tables ensured")

    # ── Read: Chunks (for DocumentChunk nodes) ────────────────────────

    async def fetch_chunks(
        self,
        ingestion_id: str,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Fetch chunk metadata for DocumentChunk nodes.

        Returns list of dicts with chunk_id, file_path, repository, branch,
        language, chunk_index, company_id, project_id.
        """
        rows = await self._pool.fetch(
            """
            SELECT chunk_id, file_path, repository, branch,
                   language, chunk_index, company_id, project_id
              FROM code_processing.pipeline_chunks
             WHERE ingestion_id = $1
             ORDER BY chunk_index
             LIMIT $2 OFFSET $3
            """,
            ingestion_id,
            limit,
            offset,
        )
        return [dict(row) for row in rows]

    async def count_chunks(self, ingestion_id: str) -> int:
        """Count chunks for an ingestion."""
        return (
            await self._pool.fetchval(
                """
            SELECT COUNT(*) FROM code_processing.pipeline_chunks
             WHERE ingestion_id = $1
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

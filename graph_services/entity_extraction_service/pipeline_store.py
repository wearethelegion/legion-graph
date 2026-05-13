"""Postgres storage for Entity Extraction Service (Service 2).

Manages:
- processing_checkpoints: content-hash based deduplication
- pipeline_counters: per-service counter tracking (shared table with other services)

All methods are async. The asyncpg pool is injected.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


class EntityExtractionStore:
    """Postgres store for entity extraction pipeline data.

    The asyncpg pool is injected — this class never creates or closes it.
    """

    SERVICE_NAME = "entity_extraction"

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── Table Initialization ──────────────────────────────────────────

    async def ensure_tables(self) -> None:
        """Create processing_checkpoints and pipeline_counters tables.

        pipeline_counters is assumed to already exist (created by preprocessor's db_init).
        We add it defensively here too.
        """
        async with self._pool.acquire() as conn:
            await conn.execute("CREATE SCHEMA IF NOT EXISTS code_processing;")

            # ── processing_checkpoints ────────────────────────────
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS code_processing.processing_checkpoints (
                    item_id      TEXT NOT NULL,
                    stage        TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    ingestion_id TEXT NOT NULL,
                    processed_at TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (item_id, stage)
                );
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_processing_checkpoints_ingestion
                ON code_processing.processing_checkpoints(ingestion_id);
            """)

            # ── pipeline_counters (defensive — preprocessor should create) ──
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

        logger.info("EntityExtractionStore: tables ensured")

    # ── Content-Hash Checkpointing ───────────────────────────────────

    async def has_checkpoint(
        self,
        item_id: str,
        stage: str,
        content_hash: str,
    ) -> bool:
        """Check if item was already successfully processed with this content hash.

        READ-ONLY operation — does not modify database state.

        Returns:
            True if already processed (skip)
            False if needs processing
        """
        existing_hash = await self._pool.fetchval(
            """
            SELECT content_hash
              FROM code_processing.processing_checkpoints
             WHERE item_id = $1
               AND stage = $2
            """,
            item_id,
            stage,
        )

        return existing_hash == content_hash

    async def save_checkpoint(
        self,
        item_id: str,
        stage: str,
        content_hash: str,
        ingestion_id: str,
    ) -> None:
        """Record successful processing completion.

        WRITE operation — call ONLY after processing succeeds.

        Critical: This must be called AFTER entities are extracted and published,
        not before. Premature checkpoint saves cause permanent chunk loss on retry.
        """
        await self._pool.execute(
            """
            INSERT INTO code_processing.processing_checkpoints
                (item_id, stage, content_hash, ingestion_id, processed_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (item_id, stage)
            DO UPDATE SET
                content_hash = EXCLUDED.content_hash,
                ingestion_id = EXCLUDED.ingestion_id,
                processed_at = NOW()
            """,
            item_id,
            stage,
            content_hash,
            ingestion_id,
        )

    async def check_checkpoint(
        self,
        item_id: str,
        stage: str,
        content_hash: str,
        ingestion_id: str,
    ) -> bool:
        """DEPRECATED: Use has_checkpoint + save_checkpoint separately.

        This method combines read and write operations, which causes permanent
        chunk loss when processing fails after checkpoint is written.

        Legacy behavior preserved for backward compatibility:
        - Checks if processing is needed
        - If yes, writes checkpoint BEFORE processing (BUG!)
        - Returns True if processing is needed

        Returns:
            True if processing is needed (new or changed content)
            False if already processed with same content hash (skip)
        """
        existing_hash = await self._pool.fetchval(
            """
            SELECT content_hash
              FROM code_processing.processing_checkpoints
             WHERE item_id = $1
               AND stage = $2
            """,
            item_id,
            stage,
        )

        # If hash matches, skip processing
        if existing_hash == content_hash:
            return False

        # New or changed — update checkpoint and signal processing needed
        await self._pool.execute(
            """
            INSERT INTO code_processing.processing_checkpoints
                (item_id, stage, content_hash, ingestion_id, processed_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (item_id, stage)
            DO UPDATE SET
                content_hash = EXCLUDED.content_hash,
                ingestion_id = EXCLUDED.ingestion_id,
                processed_at = NOW()
            """,
            item_id,
            stage,
            content_hash,
            ingestion_id,
        )

        return True

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
        """Upsert a pipeline counter (absolute value, not increment)."""
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

    async def get_counter(
        self,
        ingestion_id: str,
        counter_name: str,
    ) -> int:
        """Get a single counter value. Returns 0 if not found."""
        val = await self._pool.fetchval(
            """
            SELECT counter_value
              FROM code_processing.pipeline_counters
             WHERE ingestion_id = $1
               AND service_name = $2
               AND counter_name = $3
            """,
            ingestion_id,
            self.SERVICE_NAME,
            counter_name,
        )
        return val or 0

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

    async def get_preprocessor_total_chunks(self, ingestion_id: str) -> Optional[int]:
        """Read total_chunks counter set by the preprocessor.

        Returns None if not yet available (preprocessor hasn't finished).
        """
        val = await self._pool.fetchval(
            """
            SELECT counter_value
              FROM code_processing.pipeline_counters
             WHERE ingestion_id = $1
               AND service_name = 'preprocessor'
               AND counter_name = 'chunks_produced'
            """,
            ingestion_id,
        )
        return val

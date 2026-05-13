"""CogniStatsTracker — writes Cogni processing stats to Postgres code_processing schema.

Tracks per-ingestion counters and per-file processing details.
Uses asyncpg for async Postgres operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import asyncpg
import structlog
from shared.kafka_schemas import DataEnrichmentEvent

if TYPE_CHECKING:
    from .enriched_chunks.models import EnrichedChunkMessage

from .config import CogniConsumerConfig

logger = structlog.get_logger(__name__)


class CogniStatsTracker:
    """Tracks Cogni processing stats in Postgres."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @classmethod
    async def create(
        cls, config: type[CogniConsumerConfig] = CogniConsumerConfig
    ) -> "CogniStatsTracker":
        """Factory: create tracker with connection pool."""
        pool = await asyncpg.create_pool(
            dsn=config.POSTGRES_DSN,
            min_size=1,
            max_size=5,
        )
        tracker = cls(pool)
        await tracker._ensure_tables()
        return tracker

    async def close(self) -> None:
        """Close connection pool."""
        if self._pool:
            await self._pool.close()

    async def _ensure_tables(self) -> None:
        """Create tables if they don't exist (idempotent)."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                CREATE SCHEMA IF NOT EXISTS code_processing;
            """)
            # Per-ingestion counters
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS code_processing.cogni_ingestion_stats (
                    ingestion_id        TEXT PRIMARY KEY,
                    company_id          TEXT NOT NULL,
                    project_id          TEXT NOT NULL,
                    files_consumed      INTEGER NOT NULL DEFAULT 0,
                    files_processed     INTEGER NOT NULL DEFAULT 0,
                    files_failed        INTEGER NOT NULL DEFAULT 0,
                    first_consumed_at   TIMESTAMPTZ,
                    last_processed_at   TIMESTAMPTZ,
                    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            # Add files_produced and files_skipped columns (migration)
            await conn.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'code_processing'
                          AND table_name = 'cogni_ingestion_stats'
                          AND column_name = 'files_produced'
                    ) THEN
                        ALTER TABLE code_processing.cogni_ingestion_stats
                            ADD COLUMN files_produced INTEGER NOT NULL DEFAULT 0;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'code_processing'
                          AND table_name = 'cogni_ingestion_stats'
                          AND column_name = 'files_skipped'
                    ) THEN
                        ALTER TABLE code_processing.cogni_ingestion_stats
                            ADD COLUMN files_skipped INTEGER NOT NULL DEFAULT 0;
                    END IF;
                END $$;
            """)
            # Per-file tracking
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS code_processing.cogni_processed_files (
                    id                  BIGSERIAL PRIMARY KEY,
                    ingestion_id        TEXT NOT NULL,
                    file_path           TEXT NOT NULL,
                    change_type         TEXT NOT NULL,
                    dataset_name        TEXT NOT NULL,
                    company_id          TEXT NOT NULL,
                    project_id          TEXT NOT NULL,
                    repository          TEXT,
                    branch              TEXT,
                    status              TEXT NOT NULL DEFAULT 'consumed',
                    consumed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    processed_at        TIMESTAMPTZ,
                    error_message       TEXT,
                    error_type          TEXT,
                    failure_stage       TEXT,
                    event_id            TEXT,
                    content_hash        TEXT,
                    file_index          INTEGER DEFAULT 0,
                    total_files         INTEGER DEFAULT 0,
                    UNIQUE (company_id, project_id, file_path, ingestion_id)
                );
            """)
            # Deduplicate existing rows and add UNIQUE constraint
            await conn.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'cogni_processed_files_unique'
                    ) THEN
                        -- Remove duplicates: keep the row with the highest id
                        DELETE FROM code_processing.cogni_processed_files a
                        USING code_processing.cogni_processed_files b
                        WHERE a.id < b.id
                          AND a.company_id = b.company_id
                          AND a.project_id = b.project_id
                          AND a.file_path  = b.file_path
                          AND a.ingestion_id = b.ingestion_id;

                        ALTER TABLE code_processing.cogni_processed_files
                            ADD CONSTRAINT cogni_processed_files_unique
                            UNIQUE (company_id, project_id, file_path, ingestion_id);
                    END IF;
                END $$;
            """)
            # Add repository and branch columns if they don't exist (migration)
            await conn.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'code_processing'
                          AND table_name = 'cogni_processed_files'
                          AND column_name = 'repository'
                    ) THEN
                        ALTER TABLE code_processing.cogni_processed_files
                            ADD COLUMN repository TEXT;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'code_processing'
                          AND table_name = 'cogni_processed_files'
                          AND column_name = 'branch'
                    ) THEN
                        ALTER TABLE code_processing.cogni_processed_files
                            ADD COLUMN branch TEXT;
                    END IF;
                END $$;
            """)
            # Indexes for common queries
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cogni_processed_files_ingestion
                ON code_processing.cogni_processed_files(ingestion_id);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cogni_processed_files_status
                ON code_processing.cogni_processed_files(ingestion_id, status);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cogni_processed_files_failed
                ON code_processing.cogni_processed_files(status)
                WHERE status = 'failed';
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cogni_ingestion_stats_company
                ON code_processing.cogni_ingestion_stats(company_id, project_id);
            """)
            # Create skipped_files table (fresh-deploy path)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS code_processing.skipped_files (
                    id              BIGSERIAL PRIMARY KEY,
                    ingestion_id    TEXT NOT NULL,
                    company_id      TEXT,
                    project_id      TEXT,
                    repository      TEXT,
                    branch          TEXT,
                    file_path       TEXT NOT NULL,
                    service         TEXT,
                    skip_type       TEXT,
                    reason          TEXT,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            # Alembic-first path: table exists with different columns
            await conn.execute("""
                ALTER TABLE code_processing.skipped_files
                    ADD COLUMN IF NOT EXISTS company_id   TEXT,
                    ADD COLUMN IF NOT EXISTS project_id   TEXT,
                    ADD COLUMN IF NOT EXISTS repository   TEXT,
                    ADD COLUMN IF NOT EXISTS branch       TEXT,
                    ADD COLUMN IF NOT EXISTS service      TEXT,
                    ADD COLUMN IF NOT EXISTS skip_type    TEXT,
                    ADD COLUMN IF NOT EXISTS reason       TEXT;
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_skipped_files_ingestion
                ON code_processing.skipped_files(ingestion_id);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_skipped_files_service
                ON code_processing.skipped_files(service, skip_type);
            """)
            # Create pipeline_errors table (fresh-deploy path)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS code_processing.pipeline_errors (
                    id              BIGSERIAL PRIMARY KEY,
                    ingestion_id    TEXT,
                    company_id      TEXT,
                    project_id      TEXT,
                    repository      TEXT,
                    branch          TEXT,
                    file_path       TEXT,
                    service         TEXT,
                    stage           TEXT,
                    error_type      TEXT NOT NULL,
                    error_message   TEXT,
                    error_severity  TEXT DEFAULT 'error',
                    pipeline_stage  TEXT DEFAULT 'preprocessing',
                    stack_trace     TEXT,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            # Alembic-first path: table exists with pipeline_stage, not service/stage
            await conn.execute("""
                ALTER TABLE code_processing.pipeline_errors
                    ADD COLUMN IF NOT EXISTS service        TEXT,
                    ADD COLUMN IF NOT EXISTS stage          TEXT,
                    ADD COLUMN IF NOT EXISTS error_severity TEXT DEFAULT 'error',
                    ADD COLUMN IF NOT EXISTS pipeline_stage TEXT DEFAULT 'preprocessing',
                    ADD COLUMN IF NOT EXISTS stack_trace    TEXT;
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pipeline_errors_ingestion
                ON code_processing.pipeline_errors(ingestion_id);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pipeline_errors_service
                ON code_processing.pipeline_errors(service, stage);
            """)
        logger.info("stats_tracker.tables_ensured")

    async def record_consumed(self, event: DataEnrichmentEvent) -> None:
        """Record that a message was consumed from Kafka."""
        if not event.ingestion_id:
            return
        async with self._pool.acquire() as conn:
            # Upsert ingestion stats
            await conn.execute(
                """
                INSERT INTO code_processing.cogni_ingestion_stats
                    (ingestion_id, company_id, project_id,
                     files_consumed, first_consumed_at)
                VALUES ($1, $2, $3, 1, NOW())
                ON CONFLICT (ingestion_id) DO UPDATE SET
                    files_consumed =
                        code_processing.cogni_ingestion_stats.files_consumed + 1,
                    updated_at = NOW()
            """,
                event.ingestion_id,
                event.company_id,
                event.project_id,
            )

            # Insert per-file record
            branch = (event.branch or "main").replace("/", "_").replace("-", "_")
            dataset_name = f"{event.project_id}_{branch}_code"
            await conn.execute(
                """
                INSERT INTO code_processing.cogni_processed_files
                    (ingestion_id, file_path, change_type, dataset_name,
                     company_id, project_id, status, event_id, content_hash,
                     file_index, total_files)
                VALUES ($1, $2, $3, $4, $5, $6, 'consumed', $7, $8, $9, $10)
                ON CONFLICT (company_id, project_id, file_path, ingestion_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    consumed_at = EXCLUDED.consumed_at,
                    processed_at = EXCLUDED.processed_at,
                    error_message = EXCLUDED.error_message
            """,
                event.ingestion_id,
                event.file_path,
                str(event.change_type),
                dataset_name,
                event.company_id,
                event.project_id,
                event.event_id,
                event.content_hash,
                event.file_index,
                event.total_files,
            )

    async def record_processed(self, event: DataEnrichmentEvent) -> None:
        """Record successful processing."""
        if not event.ingestion_id:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE code_processing.cogni_ingestion_stats
                SET files_processed = files_processed + 1,
                    last_processed_at = NOW(),
                    updated_at = NOW()
                WHERE ingestion_id = $1
            """,
                event.ingestion_id,
            )

            await conn.execute(
                """
                UPDATE code_processing.cogni_processed_files
                SET status = 'processed', processed_at = NOW()
                WHERE ingestion_id = $1
                  AND file_path = $2
                  AND status = 'consumed'
            """,
                event.ingestion_id,
                event.file_path,
            )

    async def record_failed(
        self,
        event: DataEnrichmentEvent,
        error_message: str,
        error_type: str,
        failure_stage: str,
    ) -> None:
        """Record a processing failure."""
        if not event.ingestion_id:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE code_processing.cogni_ingestion_stats
                SET files_failed = files_failed + 1,
                    updated_at = NOW()
                WHERE ingestion_id = $1
            """,
                event.ingestion_id,
            )

            await conn.execute(
                """
                UPDATE code_processing.cogni_processed_files
                SET status = 'failed', processed_at = NOW(),
                    error_message = $3, error_type = $4, failure_stage = $5
                WHERE ingestion_id = $1
                  AND file_path = $2
                  AND status = 'consumed'
            """,
                event.ingestion_id,
                event.file_path,
                error_message[:2000],
                error_type,
                failure_stage,
            )

    # ── Enriched Chunks batch methods ──────────────────────────

    async def record_chunk_batch_consumed(
        self,
        messages: list[EnrichedChunkMessage],
        dataset_name: str,
    ) -> None:
        """Record that a batch of enriched chunks was consumed from Kafka.

        Groups chunks by (ingestion_id, file_path) and upserts one row per file.
        """
        if not messages:
            return

        # Deduplicate: group chunks by (ingestion_id, file_path)
        files: dict[tuple[str, str], EnrichedChunkMessage] = {}
        for msg in messages:
            key = (msg.ingestion_id, msg.file_path)
            if key not in files:
                files[key] = msg

        async with self._pool.acquire() as conn:
            for (ingestion_id, file_path), msg in files.items():
                # Upsert ingestion stats
                await conn.execute(
                    """
                    INSERT INTO code_processing.cogni_ingestion_stats
                        (ingestion_id, company_id, project_id,
                         files_consumed, first_consumed_at)
                    VALUES ($1, $2, $3, 1, NOW())
                    ON CONFLICT (ingestion_id) DO UPDATE SET
                        files_consumed =
                            code_processing.cogni_ingestion_stats.files_consumed + 1,
                        updated_at = NOW()
                    """,
                    ingestion_id,
                    msg.company_id,
                    msg.project_id,
                )

                # Insert per-file record
                await conn.execute(
                    """
                    INSERT INTO code_processing.cogni_processed_files
                        (ingestion_id, file_path, change_type, dataset_name,
                         company_id, project_id, repository, branch,
                         status, event_id, content_hash,
                         file_index, total_files)
                    VALUES ($1, $2, 'enriched_chunk', $3, $4, $5, $6, $7, 'consumed',
                            $8, NULL, $9, $10)
                    ON CONFLICT (company_id, project_id, file_path, ingestion_id)
                    DO UPDATE SET
                        status = 'consumed',
                        consumed_at = NOW(),
                        processed_at = NULL,
                        error_message = NULL,
                        repository = EXCLUDED.repository,
                        branch = EXCLUDED.branch
                    """,
                    ingestion_id,
                    file_path,
                    dataset_name,
                    msg.company_id,
                    msg.project_id,
                    msg.repository,
                    msg.branch,
                    msg.chunk_id,  # use first chunk_id as event_id
                    msg.chunk_index or 0,
                    msg.total_chunks or 0,
                )

        logger.info(
            "stats_tracker.chunk_batch_consumed",
            files=len(files),
            chunks=len(messages),
        )

    async def record_chunk_batch_processed(
        self,
        messages: list[EnrichedChunkMessage],
    ) -> None:
        """Record successful processing of a chunk batch."""
        if not messages:
            return

        files: dict[tuple[str, str], EnrichedChunkMessage] = {}
        for msg in messages:
            key = (msg.ingestion_id, msg.file_path)
            if key not in files:
                files[key] = msg

        async with self._pool.acquire() as conn:
            for (ingestion_id, file_path), msg in files.items():
                await conn.execute(
                    """
                    UPDATE code_processing.cogni_ingestion_stats
                    SET files_processed = files_processed + 1,
                        last_processed_at = NOW(),
                        updated_at = NOW()
                    WHERE ingestion_id = $1
                    """,
                    ingestion_id,
                )

                await conn.execute(
                    """
                    UPDATE code_processing.cogni_processed_files
                    SET status = 'processed', processed_at = NOW()
                    WHERE ingestion_id = $1
                      AND file_path = $2
                      AND company_id = $3
                      AND project_id = $4
                    """,
                    ingestion_id,
                    file_path,
                    msg.company_id,
                    msg.project_id,
                )

        logger.info(
            "stats_tracker.chunk_batch_processed",
            files=len(files),
        )

    async def record_chunk_batch_failed(
        self,
        messages: list[EnrichedChunkMessage],
        error_message: str,
        error_type: str,
    ) -> None:
        """Record failure of a chunk batch."""
        if not messages:
            return

        files: dict[tuple[str, str], EnrichedChunkMessage] = {}
        for msg in messages:
            key = (msg.ingestion_id, msg.file_path)
            if key not in files:
                files[key] = msg

        async with self._pool.acquire() as conn:
            for (ingestion_id, file_path), msg in files.items():
                await conn.execute(
                    """
                    UPDATE code_processing.cogni_ingestion_stats
                    SET files_failed = files_failed + 1,
                        updated_at = NOW()
                    WHERE ingestion_id = $1
                    """,
                    ingestion_id,
                )

                await conn.execute(
                    """
                    UPDATE code_processing.cogni_processed_files
                    SET status = 'failed', processed_at = NOW(),
                        error_message = $3, error_type = $4,
                        failure_stage = 'enriched_chunks_consumer'
                    WHERE ingestion_id = $1
                      AND file_path = $2
                      AND company_id = $5
                      AND project_id = $6
                    """,
                    ingestion_id,
                    file_path,
                    error_message[:2000],
                    error_type,
                    msg.company_id,
                    msg.project_id,
                )

        logger.info(
            "stats_tracker.chunk_batch_failed",
            files=len(files),
            error_type=error_type,
        )

    async def record_chunk_batch_deleted(
        self,
        messages: list[EnrichedChunkMessage],
    ) -> None:
        """Record successful deletion of a chunk batch.

        Updates cogni_processed_files status to 'deleted'.
        """
        if not messages:
            return

        # Deduplicate by file_path
        files: dict[tuple[str, str], EnrichedChunkMessage] = {}
        for msg in messages:
            key = (msg.ingestion_id, msg.file_path)
            if key not in files:
                files[key] = msg

        async with self._pool.acquire() as conn:
            for (ingestion_id, file_path), msg in files.items():
                # Update status to 'deleted'
                # Table has: company_id, project_id, file_path, ingestion_id (no repository/branch cols)
                await conn.execute(
                    """
                    UPDATE code_processing.cogni_processed_files
                    SET status = 'deleted', processed_at = NOW()
                    WHERE file_path = $1
                      AND company_id = $2
                      AND project_id = $3
                    """,
                    file_path,
                    msg.company_id,
                    msg.project_id,
                )

        logger.info(
            "stats_tracker.chunk_batch_deleted",
            files=len(files),
        )

    async def record_skipped_file(
        self,
        ingestion_id: str,
        company_id: str,
        project_id: str,
        repository: str,
        branch: str,
        file_path: str,
        service: str,
        skip_type: str,
        reason: str | None = None,
    ) -> None:
        """Record a skipped file to the skipped_files table."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO code_processing.skipped_files
                    (ingestion_id, company_id, project_id, repository, branch,
                     file_path, service, skip_type, reason)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                ingestion_id,
                company_id,
                project_id,
                repository,
                branch,
                file_path,
                service,
                skip_type,
                reason,
            )

    async def record_pipeline_errors(
        self,
        messages: list[EnrichedChunkMessage],
        pipeline_stage: str,
        error_type: str,
        error_message: str,
    ) -> None:
        """Record pipeline errors for a batch of messages.

        Writes one row per unique file in the batch.

        Args:
            pipeline_stage: Must match the ``pipeline_stage`` CHECK constraint
                in the Alembic schema (e.g. 'preprocessing', 'system').
        """
        if not messages:
            return

        # Deduplicate by file_path
        files: dict[tuple[str, str], EnrichedChunkMessage] = {}
        for msg in messages:
            key = (msg.ingestion_id, msg.file_path)
            if key not in files:
                files[key] = msg

        async with self._pool.acquire() as conn:
            for (ingestion_id, file_path), msg in files.items():
                try:
                    await conn.execute(
                        """
                        INSERT INTO code_processing.pipeline_errors
                            (ingestion_id, company_id, project_id, repository, branch,
                             file_path, pipeline_stage, error_type, error_message)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        """,
                        ingestion_id,
                        msg.company_id,
                        msg.project_id,
                        msg.repository,
                        msg.branch,
                        file_path,
                        pipeline_stage,
                        error_type,
                        error_message[:2000] if error_message else None,
                    )
                except asyncpg.ForeignKeyViolationError:
                    # FK on ingestion_id — ingestion_batches row may not exist.
                    # Retry with NULL ingestion_id so the error is still recorded.
                    logger.warning(
                        "stats_tracker.pipeline_error_fk_retry",
                        ingestion_id=ingestion_id,
                        file_path=file_path,
                    )
                    await conn.execute(
                        """
                        INSERT INTO code_processing.pipeline_errors
                            (ingestion_id, company_id, project_id, repository, branch,
                             file_path, pipeline_stage, error_type, error_message)
                        VALUES (NULL, $1, $2, $3, $4, $5, $6, $7, $8)
                        """,
                        msg.company_id,
                        msg.project_id,
                        msg.repository,
                        msg.branch,
                        file_path,
                        pipeline_stage,
                        error_type,
                        error_message[:2000] if error_message else None,
                    )

        logger.info(
            "stats_tracker.pipeline_errors_recorded",
            files=len(files),
            pipeline_stage=pipeline_stage,
        )

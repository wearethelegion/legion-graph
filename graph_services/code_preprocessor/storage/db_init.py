"""Database initialization for ingestion tracking tables.

Creates tracking tables for skipped files, pipeline errors, and stats.
If the tables already exist (e.g. created by alembic migration 059),
ALTER TABLE adds any missing columns so both creation paths converge
on the same schema.
"""

import logging

import asyncpg

logger = logging.getLogger(__name__)


async def ensure_tracking_tables(pool: asyncpg.Pool) -> None:
    """Create ingestion tracking tables if they don't exist.

    Creates:
    - code_processing.skipped_files: tracks filtered and unchanged files
    - code_processing.pipeline_errors: tracks processing failures
    - Adds columns to code_processing.cogni_ingestion_stats

    Handles two scenarios:
    1. Fresh deploy (no alembic): CREATE TABLE creates with the full schema.
    2. Alembic-first: table already exists with a subset of columns;
       ALTER TABLE ADD COLUMN IF NOT EXISTS fills in the gaps.

    Args:
        pool: asyncpg connection pool
    """
    async with pool.acquire() as conn:
        # Ensure schema exists
        await conn.execute("CREATE SCHEMA IF NOT EXISTS code_processing;")

        # ── skipped_files ──────────────────────────────────────────────
        # CREATE covers the fresh-deploy path.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS code_processing.skipped_files (
                id BIGSERIAL PRIMARY KEY,
                ingestion_id TEXT NOT NULL,
                company_id TEXT,
                project_id TEXT,
                repository TEXT,
                branch TEXT,
                file_path TEXT NOT NULL,
                service TEXT,
                skip_type TEXT,
                reason TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        # ALTER covers the alembic-first path: migration 059 created the
        # table with (ingestion_id, file_path, file_size_bytes, skip_reason,
        # skip_detail) but consumer.py INSERTs use these columns.
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

        # ── pipeline_errors ────────────────────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS code_processing.pipeline_errors (
                id BIGSERIAL PRIMARY KEY,
                ingestion_id TEXT,
                company_id TEXT,
                project_id TEXT,
                repository TEXT,
                branch TEXT,
                file_path TEXT,
                service TEXT,
                stage TEXT,
                error_type TEXT NOT NULL,
                error_message TEXT,
                error_severity TEXT DEFAULT 'error',
                pipeline_stage TEXT DEFAULT 'preprocessing',
                stack_trace TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        # ALTER covers the alembic-first path: migration 059 created the
        # table with pipeline_stage but consumer.py uses service + stage.
        # Also covers the fresh-deploy path for columns only in alembic.
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

        # ── file_chunks: add header column ─────────────────────────────
        await conn.execute("""
            ALTER TABLE code_processing.file_chunks
            ADD COLUMN IF NOT EXISTS header TEXT;
        """)

        # ── cogni_ingestion_stats: add counter columns ─────────────────
        await conn.execute("""
            ALTER TABLE code_processing.cogni_ingestion_stats 
            ADD COLUMN IF NOT EXISTS files_produced INTEGER NOT NULL DEFAULT 0;
        """)

        await conn.execute("""
            ALTER TABLE code_processing.cogni_ingestion_stats 
            ADD COLUMN IF NOT EXISTS files_skipped INTEGER NOT NULL DEFAULT 0;
        """)

        # ── pipeline_counters (v2 pipeline observability) ──────────────
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

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pipeline_counters_service
            ON code_processing.pipeline_counters(ingestion_id, service_name);
        """)

        # ── pipeline_chunks (v2 Postgres staging layer) ────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS code_processing.pipeline_chunks (
                chunk_id UUID PRIMARY KEY,
                ingestion_id UUID NOT NULL,
                company_id VARCHAR(50) NOT NULL,
                project_id VARCHAR(50) NOT NULL,
                file_path TEXT NOT NULL,
                repository TEXT NOT NULL,
                branch TEXT NOT NULL,
                language VARCHAR(20),
                chunk_index INTEGER NOT NULL,
                total_chunks INTEGER NOT NULL,
                content TEXT NOT NULL,
                header TEXT DEFAULT '',
                embedding FLOAT8[] DEFAULT NULL,
                file_skeleton TEXT DEFAULT '',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pipeline_chunks_ingestion
            ON code_processing.pipeline_chunks(ingestion_id);
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pipeline_chunks_file
            ON code_processing.pipeline_chunks(ingestion_id, file_path);
        """)

        logger.info("Ingestion tracking tables initialized successfully (incl. v2 pipeline tables)")

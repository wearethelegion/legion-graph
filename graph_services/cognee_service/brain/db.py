"""
Brain v2 — Asyncpg Connection Pool

Lazy-initialised connection pool for the KGRAG Postgres database.
The cognee_service container connects to the same Postgres instance
as the main API but through its own pool with independent sizing.

Environment variables (checked in order):
  KGRAG_DATABASE_URL  - full DSN: postgresql://user:pass@host:port/db
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD - individual params
"""

import os
from typing import Optional

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

# ── Module-level singleton ───────────────────────────────────────────────────

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Return the initialised pool, creating it lazily if needed."""
    global _pool
    if _pool is None:
        await _init_pool()
    return _pool


async def _init_pool() -> None:
    """Create the asyncpg connection pool from environment config."""
    global _pool
    if _pool is not None:
        return

    dsn = os.getenv("KGRAG_DATABASE_URL") or os.getenv("DATABASE_URL")

    if dsn:
        _pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=2,
            max_size=20,
        )
    else:
        host = os.getenv("PG_HOST", "localhost")
        port = int(os.getenv("PG_PORT", "5432"))
        database = os.getenv("PG_DB", "kgrag_auth")
        user = os.getenv("PG_USER", "kgrag")
        password = os.getenv("PG_PASSWORD", "")

        _pool = await asyncpg.create_pool(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            min_size=2,
            max_size=20,
        )

    logger.info("brain.db_pool_initialised", pool_size="2-20")


async def shutdown_pool() -> None:
    """Gracefully close the connection pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("brain.db_pool_closed")


async def health_check() -> bool:
    """Quick connectivity check — returns True if Postgres is reachable."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as exc:
        logger.error("brain.db_health_check_failed", error=str(exc))
        return False


# ── Schema bootstrap ─────────────────────────────────────────────────────────

# Brain content tables live in the same Postgres DB as auth (`kgrag_auth` by
# default, configurable via KGRAG_DATABASE_URL). They are not managed by
# cognee's own create_db_and_tables() — cognee uses a separate `cognee` DB.
#
# DDL columns are derived directly from the INSERT/SELECT statements in
# cognee_service/brain_content/repositories.py. Keep this in sync if those
# queries change. Idempotent (CREATE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
_BRAIN_DDL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS brain_knowledge (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id          VARCHAR NOT NULL,
    title               TEXT NOT NULL,
    content             TEXT NOT NULL,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash        VARCHAR,
    created_by_user_id  VARCHAR,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_brain_knowledge_company ON brain_knowledge(company_id);
CREATE INDEX IF NOT EXISTS idx_brain_knowledge_hash    ON brain_knowledge(content_hash);

CREATE TABLE IF NOT EXISTS brain_expertise (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id          VARCHAR NOT NULL,
    title               TEXT NOT NULL,
    content             TEXT NOT NULL,
    when_to_use         TEXT,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash        VARCHAR,
    created_by_user_id  VARCHAR,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_brain_expertise_company ON brain_expertise(company_id);
CREATE INDEX IF NOT EXISTS idx_brain_expertise_hash    ON brain_expertise(content_hash);

CREATE TABLE IF NOT EXISTS brain_lessons (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id          VARCHAR NOT NULL,
    title               TEXT NOT NULL,
    content             TEXT NOT NULL,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash        VARCHAR,
    created_by_user_id  VARCHAR,
    symptom             TEXT,
    root_cause          TEXT,
    solution            TEXT,
    prevention          TEXT,
    severity            VARCHAR,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_brain_lessons_company ON brain_lessons(company_id);
CREATE INDEX IF NOT EXISTS idx_brain_lessons_hash    ON brain_lessons(content_hash);
"""


async def init_brain_tables() -> None:
    """Create the brain_knowledge / brain_expertise / brain_lessons tables.

    Idempotent — uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.
    Called once during cognee_service startup (server.py:serve) after the pool
    is initialised. Safe to re-run on every boot; safe to run concurrently
    against the same DB (per-statement IF NOT EXISTS guards).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(_BRAIN_DDL)
    logger.info(
        "brain.tables_ensured", tables=["brain_knowledge", "brain_expertise", "brain_lessons"]
    )

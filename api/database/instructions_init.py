"""Idempotent schema bootstrap for `company_instructions` and `project_instructions`.

The auth service's SQLAlchemy ORM (`auth/database.py`) does not declare these
tables; they are queried by the REST API via raw asyncpg in
`api/repositories/instructions_repository.py`.

This module is called once from `api.main.lifespan` so the tables always exist
on fresh deploys without manual DDL.

DDL is derived from the SELECT/INSERT statements in
`api/repositories/instructions_repository.py`. Keep in sync if those change.
"""

from __future__ import annotations

from loguru import logger

from api.database.connection import get_db_pool


_DDL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS company_instructions (
    id                    VARCHAR PRIMARY KEY,
    company_id            VARCHAR NOT NULL,
    ground_rules          TEXT,
    coding_standards      TEXT,
    communication_style   TEXT,
    forbidden_actions     TEXT,
    custom_instructions   TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_company_instructions_company ON company_instructions(company_id);

CREATE TABLE IF NOT EXISTS project_instructions (
    id                    VARCHAR PRIMARY KEY,
    project_id            VARCHAR NOT NULL,
    description           TEXT,
    languages             TEXT[],
    frameworks            TEXT[],
    tools                 TEXT[],
    architecture_notes    TEXT,
    conventions           TEXT,
    custom_instructions   TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_project_instructions_project ON project_instructions(project_id);
"""


async def init_instructions_tables() -> None:
    """Create `company_instructions` + `project_instructions` if missing.

    Idempotent — uses CREATE TABLE IF NOT EXISTS / CREATE UNIQUE INDEX IF NOT EXISTS.
    Called once during REST API startup (api.main.lifespan).
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute(_DDL)
    logger.info("✓ Instructions tables ensured (company_instructions, project_instructions)")

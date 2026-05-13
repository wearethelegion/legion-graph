"""Idempotent schema bootstrap for `code_processing.document_extraction_prompts`.

The document-preprocessor loads LLM extraction prompts at runtime from this
table, keyed by entity_type (`knowledge`, `expertise`, `lesson`). Without the
table seeded, every BrainEvent ends with `prompt_load_failed → missing_prompt`
and the document never reaches the downstream pipeline → search returns
`NoDataError: No valid chunks loaded`.

This module creates the table AND seeds the three default prompts shipped with
the service (vendored under `document_preprocessor/prompts/`). Called once on
service startup by `document_preprocessor/main.py:main`.

Seeding is idempotent: each prompt is inserted with `ON CONFLICT DO NOTHING`
keyed on (entity_type, version=1). To roll out a new prompt version, bump the
version column and insert a new row — the loader picks the highest version.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Mapping

import asyncpg

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

_ENTITY_TYPES: Mapping[str, str] = {
    "knowledge": "knowledge.txt",
    "expertise": "expertise.txt",
    "lesson": "lesson.txt",
}

_DDL = """
CREATE SCHEMA IF NOT EXISTS code_processing;

CREATE TABLE IF NOT EXISTS code_processing.document_extraction_prompts (
    id            BIGSERIAL PRIMARY KEY,
    entity_type   VARCHAR NOT NULL,
    version       INT     NOT NULL DEFAULT 1,
    template_text TEXT    NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_type, version)
);

CREATE INDEX IF NOT EXISTS idx_doc_extraction_prompts_entity_type
    ON code_processing.document_extraction_prompts(entity_type);
"""


def _load_prompt(filename: str) -> str:
    """Read a prompt file from the vendored prompts directory."""
    path = PROMPTS_DIR / filename
    return path.read_text(encoding="utf-8")


async def init_document_extraction_prompts(pool: asyncpg.Pool) -> None:
    """Create the prompts table and seed default prompt rows if missing.

    Idempotent on both DDL and the seed inserts (ON CONFLICT DO NOTHING).
    Safe to call on every container start.
    """
    if pool is None:
        logger.warning(
            "document_extraction_prompts.init_skipped: no db pool — "
            "prompt loading at runtime will fail"
        )
        return

    async with pool.acquire() as conn:
        await conn.execute(_DDL)

        for entity_type, filename in _ENTITY_TYPES.items():
            path = PROMPTS_DIR / filename
            if not path.exists():
                logger.error(
                    "document_extraction_prompts.seed_missing_file: "
                    "entity_type=%s file=%s — skipping seed",
                    entity_type,
                    path,
                )
                continue

            template_text = _load_prompt(filename)
            await conn.execute(
                """
                INSERT INTO code_processing.document_extraction_prompts
                    (entity_type, version, template_text)
                VALUES ($1, 1, $2)
                ON CONFLICT (entity_type, version) DO NOTHING
                """,
                entity_type,
                template_text,
            )

    logger.info(
        "document_extraction_prompts.ensured: entity_types=%s",
        list(_ENTITY_TYPES.keys()),
    )

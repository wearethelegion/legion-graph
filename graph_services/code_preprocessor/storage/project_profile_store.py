"""Postgres-backed storage for Code Intelligence Pipeline V3 entities.

Manages three tables in the code_processing schema:
  - extraction_prompt_templates: versioned LLM prompt templates
  - project_profiles:            per-project analysis results
  - company_business_domains:    company-level business domain taxonomy

All methods are async. The asyncpg pool is injected.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg

logger = logging.getLogger(__name__)


class ProjectProfileStore:
    """Read and write Code Intelligence V3 entities in Postgres.

    The asyncpg pool is injected — this class never creates or closes it.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── extraction_prompt_templates ───────────────────────────────────────────

    async def get_prompt_template(self, version: int = 1) -> Optional[Dict[str, Any]]:
        """Fetch a prompt template by version number.

        Returns None when no template with that version exists.
        """
        row = await self._pool.fetchrow(
            """
            SELECT id, template_text, version, created_at
              FROM code_processing.extraction_prompt_templates
             WHERE version = $1
            """,
            version,
        )
        return self._row_to_dict(row) if row else None

    async def get_latest_prompt_template(self) -> Optional[Dict[str, Any]]:
        """Fetch the prompt template with the highest version number."""
        row = await self._pool.fetchrow(
            """
            SELECT id, template_text, version, created_at
              FROM code_processing.extraction_prompt_templates
             ORDER BY version DESC
             LIMIT 1
            """
        )
        return self._row_to_dict(row) if row else None

    async def create_prompt_template(
        self,
        template_text: str,
        version: int,
    ) -> Dict[str, Any]:
        """Insert a new prompt template version.

        Raises asyncpg.UniqueViolationError if the version already exists.
        """
        row = await self._pool.fetchrow(
            """
            INSERT INTO code_processing.extraction_prompt_templates
                (template_text, version)
            VALUES ($1, $2)
            RETURNING id, template_text, version, created_at
            """,
            template_text,
            version,
        )
        logger.info("Created extraction_prompt_template version=%d", version)
        return self._row_to_dict(row)

    # ── project_profiles ──────────────────────────────────────────────────────

    async def get_project_profile(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Fetch the analysis profile for a project.

        Returns None when no profile has been recorded yet.
        """
        row = await self._pool.fetchrow(
            """
            SELECT id, project_id, language, framework,
                   chunker_config, extraction_prompt, technical_domains,
                   analysed_at, created_at, updated_at
              FROM code_processing.project_profiles
             WHERE project_id = $1
            """,
            project_id,
        )
        return self._row_to_dict(row) if row else None

    async def upsert_project_profile(
        self,
        *,
        project_id: str,
        language: Optional[str] = None,
        framework: Optional[str] = None,
        chunker_config: Optional[Dict[str, Any]] = None,
        extraction_prompt: Optional[str] = None,
        technical_domains: Optional[List[Any]] = None,
        analysed_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Insert or update the analysis profile for a project.

        Uses ON CONFLICT ON CONSTRAINT to guarantee idempotency — safe to
        call multiple times for the same project_id.
        """
        if analysed_at is None:
            analysed_at = datetime.now(timezone.utc)

        import json as _json

        chunker_config_json = _json.dumps(chunker_config) if chunker_config is not None else None
        technical_domains_json = (
            _json.dumps(technical_domains) if technical_domains is not None else None
        )

        row = await self._pool.fetchrow(
            """
            INSERT INTO code_processing.project_profiles
                (project_id, language, framework,
                 chunker_config, extraction_prompt, technical_domains,
                 analysed_at, updated_at)
            VALUES ($1, $2, $3,
                    $4::jsonb, $5, $6::jsonb,
                    $7, NOW())
            ON CONFLICT ON CONSTRAINT uq_pp_project_id DO UPDATE SET
                language          = EXCLUDED.language,
                framework         = EXCLUDED.framework,
                chunker_config    = EXCLUDED.chunker_config,
                extraction_prompt = EXCLUDED.extraction_prompt,
                technical_domains = EXCLUDED.technical_domains,
                analysed_at       = EXCLUDED.analysed_at,
                updated_at        = NOW()
            RETURNING id, project_id, language, framework,
                      chunker_config, extraction_prompt, technical_domains,
                      analysed_at, created_at, updated_at
            """,
            project_id,
            language,
            framework,
            chunker_config_json,
            extraction_prompt,
            technical_domains_json,
            analysed_at,
        )
        logger.info("Upserted project_profile for project_id=%s", project_id)
        return self._row_to_dict(row)

    async def delete_project_profile(self, project_id: str) -> bool:
        """Delete the profile for a project. Returns True if a row was removed."""
        tag = await self._pool.execute(
            "DELETE FROM code_processing.project_profiles WHERE project_id = $1",
            project_id,
        )
        return self._affected(tag) > 0

    # ── company_business_domains ──────────────────────────────────────────────

    async def list_company_domains(
        self,
        company_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return all business domains for a company, ordered by canonical_name."""
        rows = await self._pool.fetch(
            """
            SELECT id, company_id, canonical_name, normalised_key,
                   description, aliases, created_at, updated_at
              FROM code_processing.company_business_domains
             WHERE company_id = $1
             ORDER BY canonical_name
             LIMIT $2 OFFSET $3
            """,
            company_id,
            limit,
            offset,
        )
        return [self._row_to_dict(r) for r in rows]

    async def get_company_domain(
        self,
        company_id: str,
        normalised_key: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single domain by company + normalised_key.

        Returns None when the domain does not exist.
        """
        row = await self._pool.fetchrow(
            """
            SELECT id, company_id, canonical_name, normalised_key,
                   description, aliases, created_at, updated_at
              FROM code_processing.company_business_domains
             WHERE company_id = $1
               AND normalised_key = $2
            """,
            company_id,
            normalised_key,
        )
        return self._row_to_dict(row) if row else None

    async def upsert_company_domain(
        self,
        *,
        company_id: str,
        canonical_name: str,
        normalised_key: str,
        description: Optional[str] = None,
        aliases: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Insert or update a company business domain.

        The UNIQUE constraint on (company_id, normalised_key) guarantees
        deduplication — repeated calls with the same key update the record.
        """
        import json as _json

        aliases_json = _json.dumps(aliases or [])

        row = await self._pool.fetchrow(
            """
            INSERT INTO code_processing.company_business_domains
                (company_id, canonical_name, normalised_key,
                 description, aliases, updated_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
            ON CONFLICT ON CONSTRAINT uq_cbd_company_key DO UPDATE SET
                canonical_name = EXCLUDED.canonical_name,
                description    = EXCLUDED.description,
                aliases        = EXCLUDED.aliases,
                updated_at     = NOW()
            RETURNING id, company_id, canonical_name, normalised_key,
                      description, aliases, created_at, updated_at
            """,
            company_id,
            canonical_name,
            normalised_key,
            description,
            aliases_json,
        )
        logger.info(
            "Upserted company_business_domain company_id=%s key=%s",
            company_id,
            normalised_key,
        )
        return self._row_to_dict(row)

    async def delete_company_domain(
        self,
        company_id: str,
        normalised_key: str,
    ) -> bool:
        """Delete a business domain. Returns True if a row was removed."""
        tag = await self._pool.execute(
            """
            DELETE FROM code_processing.company_business_domains
             WHERE company_id = $1 AND normalised_key = $2
            """,
            company_id,
            normalised_key,
        )
        return self._affected(tag) > 0

    async def bulk_upsert_company_domains(
        self,
        company_id: str,
        domains: List[Dict[str, Any]],
    ) -> int:
        """Upsert multiple business domains for a company.

        Each dict in `domains` must contain at minimum:
          - canonical_name (str)
          - normalised_key (str)
        Optional keys: description (str), aliases (list[str]).

        Returns the number of records processed.
        """
        if not domains:
            return 0

        import json as _json

        records = [
            (
                company_id,
                d["canonical_name"],
                d["normalised_key"],
                d.get("description"),
                _json.dumps(d.get("aliases") or []),
            )
            for d in domains
        ]

        await self._pool.executemany(
            """
            INSERT INTO code_processing.company_business_domains
                (company_id, canonical_name, normalised_key,
                 description, aliases, updated_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
            ON CONFLICT ON CONSTRAINT uq_cbd_company_key DO UPDATE SET
                canonical_name = EXCLUDED.canonical_name,
                description    = EXCLUDED.description,
                aliases        = EXCLUDED.aliases,
                updated_at     = NOW()
            """,
            records,
        )
        logger.info(
            "Bulk-upserted %d company_business_domains for company_id=%s",
            len(records),
            company_id,
        )
        return len(records)

    async def close(self) -> None:
        """No-op — the pool is managed externally."""

    # ── private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _row_to_dict(row: asyncpg.Record) -> Dict[str, Any]:
        """Convert an asyncpg Record to a plain dict with ISO timestamps."""
        result: Dict[str, Any] = dict(row)
        for field in ("created_at", "updated_at", "analysed_at"):
            val = result.get(field)
            if val is not None:
                result[field] = val.isoformat()
        return result

    @staticmethod
    def _affected(command_tag: str) -> int:
        """Extract row count from an asyncpg command tag like 'DELETE 1'."""
        parts = command_tag.split()
        if len(parts) >= 2 and parts[-1].isdigit():
            return int(parts[-1])
        return 0

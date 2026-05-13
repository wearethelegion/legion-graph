"""Postgres-backed storage for repository file versions.

Uses asyncpg with the code_processing schema.
All methods are async.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import asyncpg

logger = logging.getLogger(__name__)


class RepositoryVersionStore:
    """Persist versioned snapshots of repository files into Postgres.

    The asyncpg pool is injected — this class never creates or closes it.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def record_change(
        self,
        *,
        repository: str,
        branch: str,
        framework: str,
        file_path: str,
        change_type: str,
        commit_sha: Optional[str],
        content_bytes: Optional[bytes] = None,
        previous_path: Optional[str] = None,
        parser_data: Optional[dict] = None,
        force_full_refresh: bool = False,
    ) -> dict:
        """Insert a new version for the given file and return the stored record.

        Skips insertion when the content hash is identical to the latest
        version already stored for this (repo, branch, file_path).
        Parser data is stored across three JSONB columns.
        """
        # ── prepare content fields ────────────────────────────────────
        content_text: Optional[str] = None
        content_b64: Optional[str] = None
        file_size: Optional[int] = None
        content_hash: Optional[str] = None

        if content_bytes is not None:
            file_size = len(content_bytes)
            content_hash = hashlib.sha256(content_bytes).hexdigest()
            try:
                content_text = content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                content_text = None

            # Prepend file path header so Cognee never misinterprets
            # content starting with '/' (e.g. JSDoc `/**`) as a file path.
            header = f"Source: {file_path}\n\n"
            tagged_bytes = header.encode("utf-8") + content_bytes
            content_b64 = base64.b64encode(tagged_bytes).decode("ascii")

        # ── duplicate detection ───────────────────────────────────────
        if content_hash and not force_full_refresh:
            existing = await self._pool.fetchrow(
                """
                SELECT id, document_id, repository, branch, framework,
                       file_path, file_type, version, change_type,
                       commit_sha, deleted, previous_path,
                       file_size, content_hash, content_b64,
                       created_at, updated_at
                  FROM code_processing.repository_file_versions
                 WHERE repository = $1
                   AND branch     = $2
                   AND file_path  = $3
                   AND content_hash = $4
                 ORDER BY version DESC
                 LIMIT 1
                """,
                repository,
                branch,
                file_path,
                content_hash,
            )
            if existing:
                logger.debug(
                    "Skipping %s@%s:%s — identical content_hash %s",
                    repository,
                    branch,
                    file_path,
                    content_hash[:8],
                )
                return self._row_to_dict(existing)

        # ── next version number ───────────────────────────────────────
        next_version = await self._next_version(repository, branch, file_path)

        # ── derived fields ────────────────────────────────────────────
        file_type = Path(file_path).suffix.lower() if Path(file_path).suffix else None
        doc_id = (
            f"gitingest:{repository}:{branch}:{next_version}"
            f":{hashlib.sha256(file_path.encode('utf-8')).hexdigest()[:8]}"
        )
        is_deleted = change_type.upper().startswith("D")

        # ── parser data → three JSONB columns ─────────────────────────
        parser_nodes_json: Optional[str] = None
        parser_rels_json: Optional[str] = None
        parser_meta_json: Optional[str] = None
        if parser_data:
            nodes = parser_data.get("nodes")
            rels = parser_data.get("relationships")
            meta = parser_data.get("metadata")
            if nodes:
                parser_nodes_json = json.dumps(nodes)
            if rels:
                parser_rels_json = json.dumps(rels)
            if meta:
                parser_meta_json = json.dumps(meta)
            logger.debug(
                "Storing parser data for %s: %d nodes, %d relationships",
                file_path,
                len(nodes or []),
                len(rels or []),
            )

        # ── insert ────────────────────────────────────────────────────
        logger.debug(
            "record_change INSERT for %s:%s — content_b64 present: %s, length: %d",
            repository,
            file_path,
            content_b64 is not None,
            len(content_b64) if content_b64 else 0,
        )
        row = await self._pool.fetchrow(
            """
            INSERT INTO code_processing.repository_file_versions
                (document_id, repository, branch, framework,
                 file_path, file_type, version,
                 change_type, commit_sha, deleted, previous_path,
                 file_content, content_b64, file_size, content_hash,
                 parser_nodes, parser_relationships, parser_metadata)
            VALUES ($1, $2, $3, $4,
                    $5, $6, $7,
                    $8, $9, $10, $11,
                    $12, $13, $14, $15,
                    $16::jsonb, $17::jsonb, $18::jsonb)
            RETURNING id, document_id, repository, branch, framework,
                      file_path, file_type, version,
                      change_type, commit_sha, deleted, previous_path,
                      file_size, content_hash, content_b64,
                      created_at, updated_at
            """,
            doc_id,
            repository,
            branch,
            framework,
            file_path,
            file_type,
            next_version,
            change_type,
            commit_sha,
            is_deleted,
            previous_path,
            content_text,
            content_b64,
            file_size,
            content_hash,
            parser_nodes_json,
            parser_rels_json,
            parser_meta_json,
        )
        result = self._row_to_dict(row)
        logger.debug(
            "record_change result for %s:%s — content_b64 in dict: %s, length: %d",
            repository,
            file_path,
            "content_b64" in result,
            len(result.get("content_b64") or "") if result.get("content_b64") else 0,
        )
        return result

    async def close(self) -> None:
        """No-op — the pool is managed externally."""

    # ── private helpers ───────────────────────────────────────────────

    async def _next_version(self, repository: str, branch: str, file_path: str) -> int:
        """Return the next version number for a (repo, branch, file_path) triple."""
        val = await self._pool.fetchval(
            """
            SELECT COALESCE(MAX(version), 0) + 1
              FROM code_processing.repository_file_versions
             WHERE repository = $1
               AND branch     = $2
               AND file_path  = $3
            """,
            repository,
            branch,
            file_path,
        )
        return int(val)

    @staticmethod
    def _row_to_dict(row: asyncpg.Record) -> Dict[str, Any]:
        """Convert an asyncpg Record to a dict with ISO timestamps.

        Preserves both 'repository' and 'project' keys so downstream
        consumers that read either name get the correct value.
        """
        result: Dict[str, Any] = dict(row)
        # Map surrogate PK id out, keep document_id as _id for consumer compat
        result.pop("id", None)
        result["_id"] = result.pop("document_id", None)
        # Keep 'repository' AND add 'project' alias for downstream compat.
        # The consumer's _extract_document_metadata reads "repository",
        # while older MongoDB code expected "project".
        result["project"] = result.get("repository")
        for field in ("created_at", "updated_at"):
            val = result.get(field)
            if val is not None:
                result[field] = val.isoformat()
        return result

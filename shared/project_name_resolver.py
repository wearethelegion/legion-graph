"""Shared utility for resolving project names from Postgres with in-memory cache.

Used by pipeline services to convert project_id → slugified project_name for
node set naming: {project_id}_{project_name}_code.

Branch is no longer part of node set names — it lives in the graph as a
Company → Project → Branch hierarchy.
"""

from __future__ import annotations

import re
from typing import Optional

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


class ProjectNameResolver:
    """Resolves project_id → slugified project_name with in-memory cache.

    Queries: SELECT name FROM projects WHERE id = $1
    Falls back to project_id on lookup failure (DB unavailable, project not found).

    Usage:
        resolver = ProjectNameResolver(pool)
        project_name = await resolver.resolve("3cad0776-c73c-486c-8a43-3ddc82f7fa19")
        # → "my-project" or the project_id as fallback
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._cache: dict[str, str] = {}

    async def resolve(self, project_id: str) -> str:
        """Return slugified project name, falling back to project_id.

        Args:
            project_id: Project UUID string.

        Returns:
            URL-safe slugified project name, or project_id if lookup fails.
        """
        if not project_id:
            return project_id
        if project_id in self._cache:
            return self._cache[project_id]
        try:
            row = await self._pool.fetchrow(
                "SELECT name FROM projects WHERE id = $1",
                project_id,
            )
            name = self._slugify(row["name"]) if row else project_id
        except Exception as e:
            logger.warning(
                "project_name_resolver.lookup_failed",
                project_id=project_id,
                error=str(e),
            )
            name = project_id
        self._cache[project_id] = name
        return name

    @staticmethod
    def _slugify(name: str) -> str:
        """Convert human-readable name to URL-safe slug.

        Examples:
            "My Project" → "my-project"
            "Kgrag Backend" → "kgrag-backend"
            "ACME Corp!" → "acme-corp"
        """
        name = name.lower()
        name = re.sub(r"[^\w\s-]", "", name)
        name = re.sub(r"[\s_]+", "-", name)
        return re.sub(r"-+", "-", name).strip("-")

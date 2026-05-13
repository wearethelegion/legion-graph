"""
Code Search Composite — full_search multi-level orchestrator.

Combines the six primitives from CodeSearchPrimitives into a single
depth-gated search call. Imported and re-exported by CodeSearchService.
"""

from typing import Any, Dict, List, Optional

from loguru import logger

from api.services.code_search.primitives import CodeSearchPrimitives


class CodeSearchComposite(CodeSearchPrimitives):
    """
    Extends CodeSearchPrimitives with the full_search composite method.

    depth=0: domains only
    depth=1: domains + entities
    depth=2: domains + entities + summaries
    depth=3: domains + entities + summaries + code for top entity
    """

    async def full_search(
        self,
        query: str,
        company_id: str,
        depth: int = 1,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        domain: Optional[str] = None,
        entity_type: Optional[str] = None,
        limit: int = 10,
        exclude_tests: bool = True,
    ) -> Dict[str, Any]:
        """
        Multi-level search combining all levels up to `depth`.

        Args:
            query: Natural language search query.
            company_id: Company UUID (REQUIRED).
            depth: Search depth 0–3 (default 1).
            project_id: Optionally filter to a specific project.
            project_name: Canonical project name required with project_id.
            domain: Optionally filter entities by BusinessDomain name.
            entity_type: Optionally filter entities by type.
            limit: Max results per level.
            exclude_tests: Exclude test/spec files and entities.

        Returns:
            Dict with keys present based on depth:
            domains (≥0), entities (≥1), summaries (≥2), code (=3)
        """
        result: Dict[str, Any] = {}

        if depth >= 0:
            result["domains"] = await self.get_domains(
                company_id=company_id,
                project_id=project_id,
            )

        if depth >= 1:
            result["entities"] = await self.search_entities(
                query=query,
                company_id=company_id,
                project_id=project_id,
                project_name=project_name,
                domain=domain,
                entity_type=entity_type,
                limit=limit,
                exclude_tests=exclude_tests,
            )

        if depth >= 2:
            result["summaries"] = await self.search_summaries(
                query=query,
                company_id=company_id,
                project_id=project_id,
                project_name=project_name,
                limit=limit,
                exclude_tests=exclude_tests,
            )

        if depth >= 3:
            entities: List[Dict[str, Any]] = result.get("entities", [])
            top_entity = entities[0]["name"] if entities else query
            result["code"] = await self.get_code_for_entity(
                entity_name=top_entity,
                company_id=company_id,
                exclude_tests=exclude_tests,
            )

        logger.debug(
            "full_search: query=%r company=%s depth=%d → keys=%s",
            query[:60],
            company_id,
            depth,
            list(result.keys()),
        )
        return result

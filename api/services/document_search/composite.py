"""
Document Search Composite — full_document_search multi-level orchestrator.

Combines the five primitives from DocumentSearchPrimitives into a single
broad sweep search call. Mirrors api/services/code_search/composite.py.
"""

from typing import Any, Dict, List, Optional

from loguru import logger

from api.services.document_search.primitives import DocumentSearchPrimitives


class DocumentSearchComposite(DocumentSearchPrimitives):
    """
    Extends DocumentSearchPrimitives with the full_document_search composite.

    The composite executes all four search dimensions in parallel-style
    sequential calls and returns a unified result dict. This mirrors
    CodeSearchComposite.full_search but adapted to knowledge documents.
    """

    async def full_document_search(
        self,
        query: str,
        company_id: str,
        limit: int = 10,
    ) -> Dict[str, Any]:
        """
        Broad sweep search across all knowledge document dimensions.

        Executes:
          1. get_collections       → which knowledge node_sets exist
          2. search_documents      → top chunk matches (Qdrant)
          3. search_document_summaries → top summary matches (Qdrant)
          4. search_document_entities  → top entity matches (Qdrant)

        Args:
            query: Natural language search query.
            company_id: Company UUID (REQUIRED).
            limit: Max results per dimension (default 10).

        Returns:
            Dict with keys: collections, chunks, summaries, entities
        """
        collections = await self.get_collections(company_id=company_id)

        chunks = await self.search_documents(
            query=query,
            company_id=company_id,
            limit=limit,
        )

        summaries = await self.search_document_summaries(
            query=query,
            company_id=company_id,
            limit=limit,
        )

        entities = await self.search_document_entities(
            query=query,
            company_id=company_id,
            limit=limit,
        )

        result = {
            "collections": collections,
            "chunks": chunks,
            "summaries": summaries,
            "entities": entities,
        }

        logger.debug(
            "full_document_search: query=%r company=%s → "
            "colls=%d chunks=%d summaries=%d entities=%d",
            query[:60],
            company_id,
            len(collections),
            len(chunks),
            len(summaries),
            len(entities),
        )
        return result

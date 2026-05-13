"""
Document Search Primitives — six atomic search methods for V2-pipeline knowledge docs.

Storage Contract (discovered 2026-04-29 by probing cognee Neo4j + Qdrant):
  V2 ingestion writes documents to Neo4j database ``cognee-{company_id}`` and
  Qdrant with the following source_node_set scheme:

    - NodeSet name in Neo4j  : ``{company_id}_knowledge``
    - Qdrant collection (chunks): ``{company_id}_knowledge``
                                   (separate per-company named collection,
                                   NOT the shared DocumentChunk_text collection)
    - Entity_name Qdrant         : source_node_set = ``{company_id}_knowledge``
    - TextSummary_text Qdrant    : source_node_set = ``{company_id}_knowledge``

  Contrast with code (T10 / code_search.py):
    - Code chunks     : DocumentChunk_text, source_node_set = code_{project_id}
    - Code entities   : Entity_name, source_node_set = entities_{project_id}
    - Code summaries  : TextSummary_text, source_node_set = summaries_{project_id}

  Knowledge is company-scoped (not project-scoped) — there is no project_id field
  in the knowledge source_node_set pattern, only company_id.

Mirrors api/services/code_search/primitives.py exactly in structure.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Set

from loguru import logger
from qdrant_client.models import FieldCondition, Filter, MatchValue

from kgrag.embeddings import GeminiEmbedder
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository

# ── Configuration constants ───────────────────────────────────────────────────

NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")

# Shared Qdrant collections (Entity and TextSummary live here for all pipelines)
COLLECTION_ENTITIES: str = "Entity_name"
COLLECTION_SUMMARIES: str = "TextSummary_text"

# Knowledge chunks live in a per-company collection, not DocumentChunk_text.
# Name is constructed dynamically: _knowledge_collection_name(company_id).


def _neo4j_db_name(company_id: str) -> str:
    """Compute the per-company Neo4j database name."""
    return f"cognee-{company_id}"


def _knowledge_source_node_set(company_id: str) -> str:
    """
    Return the source_node_set value that identifies knowledge docs for a company.

    Scheme confirmed by probing cognee Neo4j (2026-04-29):
        ``{company_id}_knowledge``
    e.g. ``cc6bdaf4-311f-4b04-8ee3-07cc85b76142_knowledge``
    """
    return f"{company_id}_knowledge"


def _knowledge_collection_name(company_id: str) -> str:
    """
    Return the Qdrant collection name for knowledge document chunks.

    V2 pipeline writes chunks to a per-company collection named after the
    source_node_set (confirmed by Qdrant inspection 2026-04-29):
        ``{company_id}_knowledge``
    """
    return f"{company_id}_knowledge"


class DocumentSearchPrimitives:
    """
    Six atomic search primitives backed by Qdrant (vector) and Neo4j (graph).

    Mirrors CodeSearchPrimitives but targets the knowledge (document) node_set
    instead of the code node_set. Intended to be subclassed/composed into
    DocumentSearchService.
    """

    def __init__(
        self,
        neo4j_repository: Neo4jRepository,
        qdrant_repository: QdrantRepository,
        embedder: Optional[GeminiEmbedder] = None,
    ) -> None:
        self._neo4j = neo4j_repository
        self._qdrant = qdrant_repository
        self._embedder = embedder or GeminiEmbedder()

    def _embed(self, query: str) -> List[float]:
        """Synchronous embedding via GeminiEmbedder.embed_query()."""
        return self._embedder.embed_query(query)

    async def _neo4j_session(self, company_id: str):
        """Return an async Neo4j session scoped to the company's cognee database."""
        await self._neo4j.connect()
        return self._neo4j.driver.session(database=_neo4j_db_name(company_id))

    # ── Primitive 1: get_collections ─────────────────────────────────────────

    async def get_collections(
        self,
        company_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Return the list of knowledge NodeSets (collections) for a company.

        Queries Neo4j for NodeSet nodes whose name ends with '_knowledge' and
        whose company_id matches the authenticated company.

        Returns:
            List of dicts: [{name, id, description}]
        """
        sns_pattern = _knowledge_source_node_set(company_id)

        async with await self._neo4j_session(company_id) as session:
            r = await session.run(
                """
                MATCH (ns:NodeSet)
                WHERE ns.name ENDS WITH '_knowledge'
                  AND ns.name STARTS WITH $company_id
                RETURN ns.name AS name, ns.id AS id,
                       ns.description AS description
                ORDER BY ns.name
                """,
                {"company_id": company_id},
            )
            results = [
                {
                    "name": rec["name"],
                    "id": rec["id"] or "",
                    "description": rec["description"] or "",
                }
                async for rec in r
            ]

        logger.debug("get_collections: company=%s → %d collections", company_id, len(results))
        return results

    # ── Primitive 2: search_documents ────────────────────────────────────────

    async def search_documents(
        self,
        query: str,
        company_id: str,
        collection: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Semantic search for document chunks in the company's knowledge collection.

        V2 knowledge chunks live in a per-company Qdrant collection named
        ``{company_id}_knowledge`` (not the shared DocumentChunk_text).

        Args:
            query: Natural language search query.
            company_id: Company UUID (REQUIRED — drives Qdrant collection + Neo4j DB).
            collection: Optional specific source_node_set to filter to (e.g. a
                        named sub-collection). Pass None to search all knowledge
                        for this company.
            limit: Max results (default 10).

        Returns:
            List of dicts: [{chunk_id, text, file_path, language, repository,
                             chunk_index, file_version_id, source_node_set, score}]
        """
        query_vector = self._embed(query)
        qdrant_collection = _knowledge_collection_name(company_id)

        # Optional sub-collection filter
        must: List[Any] = []
        if collection:
            must.append(
                FieldCondition(
                    key="source_node_set",
                    match=MatchValue(value=collection),
                )
            )

        search_filter = Filter(must=must) if must else None

        try:
            response = self._qdrant.client.query_points(
                collection_name=qdrant_collection,
                query=query_vector,
                using="text",
                query_filter=search_filter,
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            points = response.points
        except Exception as e:
            err_str = str(e)
            if (
                "doesn't exist" in err_str
                or "Not found" in err_str
                or "Vector dimension error" in err_str
                or "Unexpected Response" in err_str
                or "Collection" in err_str
            ):
                logger.warning("search_documents: Qdrant collection unavailable: {}", err_str)
                return []
            raise

        results = [
            {
                "chunk_id": str((p.payload or {}).get("id", p.id) or p.id),
                "text": (p.payload or {}).get("text", "") or "",
                "file_path": (p.payload or {}).get("file_path", "") or "",
                "language": (p.payload or {}).get("language", "") or "",
                "repository": (p.payload or {}).get("repository", "") or "",
                "chunk_index": int((p.payload or {}).get("chunk_index", 0) or 0),
                "file_version_id": (p.payload or {}).get("file_version_id", "") or "",
                "source_node_set": (p.payload or {}).get("source_node_set", "") or "",
                "score": float(p.score),
            }
            for p in points
        ]

        logger.debug(
            "search_documents: query=%r company=%s → %d", query[:60], company_id, len(results)
        )
        return results

    # ── Primitive 3: get_document_chunk ──────────────────────────────────────

    async def get_document_chunk(
        self,
        chunk_id: str,
        company_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch full content for a single DocumentChunk by its ID from cognee Neo4j.

        Multi-tenancy: queries only the company's cognee DB (cognee-{company_id}).

        Returns:
            Dict with full chunk fields, or None if not found.
        """
        sns = _knowledge_source_node_set(company_id)

        async with await self._neo4j_session(company_id) as session:
            r = await session.run(
                """
                MATCH (dc:DocumentChunk {id: $chunk_id})
                WHERE dc.source_node_set = $sns
                RETURN dc.id AS id, dc.text AS text,
                       dc.file_path AS file_path, dc.language AS language,
                       dc.repository AS repository, dc.branch AS branch,
                       dc.chunk_index AS chunk_index, dc.chunk_size AS chunk_size,
                       dc.file_version_id AS file_version_id,
                       dc.source_node_set AS source_node_set,
                       dc.company_id AS company_id,
                       dc.description AS description
                LIMIT 1
                """,
                {"chunk_id": chunk_id, "sns": sns},
            )
            rec = await r.single()

        if not rec:
            logger.debug(
                "get_document_chunk: chunk_id=%r company=%s → not found",
                chunk_id,
                company_id,
            )
            return None

        return {
            "chunk_id": rec.get("id", "") or "",
            "text": rec.get("text", "") or "",
            "file_path": rec.get("file_path", "") or "",
            "language": rec.get("language", "") or "",
            "repository": rec.get("repository", "") or "",
            "branch": rec.get("branch", "") or "",
            "chunk_index": int(rec.get("chunk_index", 0) or 0),
            "chunk_size": int(rec.get("chunk_size", 0) or 0),
            "file_version_id": rec.get("file_version_id", "") or "",
            "source_node_set": rec.get("source_node_set", "") or "",
            "company_id": rec.get("company_id", "") or "",
            "description": rec.get("description", "") or "",
        }

    # ── Primitive 4: search_document_summaries ───────────────────────────────

    async def search_document_summaries(
        self,
        query: str,
        company_id: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Semantic search for document summaries in TextSummary_text Qdrant collection.

        Filters by source_node_set = ``{company_id}_knowledge`` to scope results
        to knowledge documents (not code).

        Returns:
            List of dicts: [{text, file_version_id, source_node_set, score}]
        """
        query_vector = self._embed(query)
        sns = _knowledge_source_node_set(company_id)

        must: List[Any] = [
            FieldCondition(
                key="source_node_set",
                match=MatchValue(value=sns),
            )
        ]
        search_filter = Filter(must=must)

        try:
            response = self._qdrant.client.query_points(
                collection_name=COLLECTION_SUMMARIES,
                query=query_vector,
                using="text",
                query_filter=search_filter,
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            points = response.points
        except Exception as e:
            err_str = str(e)
            if (
                "doesn't exist" in err_str
                or "Not found" in err_str
                or "Vector dimension error" in err_str
                or "Unexpected Response" in err_str
            ):
                logger.warning("search_document_summaries: Qdrant unavailable: {}", err_str)
                return []
            raise

        results = [
            {
                "text": (p.payload or {}).get("text", "")
                or (p.payload or {}).get("summary_text", ""),
                "file_version_id": (p.payload or {}).get("file_version_id", "") or "",
                "source_node_set": (p.payload or {}).get("source_node_set", "") or "",
                "score": float(p.score),
            }
            for p in points
        ]

        logger.debug(
            "search_document_summaries: query=%r company=%s → %d",
            query[:60],
            company_id,
            len(results),
        )
        return results

    # ── Primitive 5: traverse_document_graph ─────────────────────────────────

    async def traverse_document_graph(
        self,
        entity_name: str,
        company_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Traverse 1-hop relationships out of a document knowledge entity.

        Filters to entities whose source_node_set = ``{company_id}_knowledge``
        to prevent cross-contamination with code entities.

        Returns:
            List of dicts: [{source, relationship, target, target_type, source_node_set}]
        """
        sns = _knowledge_source_node_set(company_id)

        async with await self._neo4j_session(company_id) as session:
            result = await session.run(
                """
                MATCH (e:Entity {name: $name, source_node_set: $sns})-[r]-(related:Entity)
                RETURN e.name AS source, type(r) AS relationship,
                       related.name AS target,
                       related.entity_type AS target_type,
                       related.source_node_set AS source_node_set
                LIMIT 50
                """,
                {"name": entity_name, "sns": sns},
            )
            records = await result.data()

        edges = [
            {
                "source": rec.get("source", "") or "",
                "relationship": rec.get("relationship", "") or "",
                "target": rec.get("target", "") or "",
                "target_type": rec.get("target_type", "") or "",
                "source_node_set": rec.get("source_node_set", "") or "",
            }
            for rec in records
        ]

        logger.debug(
            "traverse_document_graph: entity=%r company=%s → %d edges",
            entity_name,
            company_id,
            len(edges),
        )
        return edges

    # ── Primitive 6 (partial): search_document_entities ──────────────────────

    async def search_document_entities(
        self,
        query: str,
        company_id: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Semantic search for entities extracted from knowledge documents.

        Filters Entity_name Qdrant collection by
        source_node_set = ``{company_id}_knowledge``.

        Returns:
            List of dicts: [{entity_id, name, entity_type, description, score}]
        """
        query_vector = self._embed(query)
        sns = _knowledge_source_node_set(company_id)

        must: List[Any] = [
            FieldCondition(
                key="source_node_set",
                match=MatchValue(value=sns),
            )
        ]
        search_filter = Filter(must=must)

        try:
            response = self._qdrant.client.query_points(
                collection_name=COLLECTION_ENTITIES,
                query=query_vector,
                using="text",
                query_filter=search_filter,
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            points = response.points
        except Exception as e:
            err_str = str(e)
            if (
                "doesn't exist" in err_str
                or "Not found" in err_str
                or "Vector dimension error" in err_str
                or "Unexpected Response" in err_str
            ):
                logger.warning("search_document_entities: Qdrant unavailable: {}", err_str)
                return []
            raise

        results = [
            {
                "entity_id": str((p.payload or {}).get("id", "") or ""),
                "name": (p.payload or {}).get("name", "") or "",
                "entity_type": (p.payload or {}).get("entity_type", "") or "",
                "description": ((p.payload or {}).get("description", "") or "")[:500],
                "source_node_set": (p.payload or {}).get("source_node_set", "") or "",
                "score": float(p.score),
            }
            for p in points
        ]

        logger.debug(
            "search_document_entities: query=%r company=%s → %d",
            query[:60],
            company_id,
            len(results),
        )
        return results

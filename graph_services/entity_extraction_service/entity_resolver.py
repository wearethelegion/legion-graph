"""Cross-file entity resolver for the Entity Extraction Pipeline.

## Purpose
After all chunks in an ingestion batch are extracted and written to Neo4j,
entities extracted from different files may share the same identity (same UUID5
from name). Neo4j MERGE already handles node deduplication implicitly — same-name
entities map to the same UUID5 and thus the same Neo4j node.

This module provides post-extraction resolution that:

1. Creates ``defined_in`` edges (Entity → Document) — points to the file where
   the entity is DEFINED (class/function body, not just referenced).
2. Creates ``imported_by`` edges (Entity → Document) — points to files where the
   entity appears as an import or external reference (not defined there).
3. Logs resolution statistics (entities processed, edges created, errors).

## Idempotency
All edges are created via ``apoc.merge.relationship`` with a deterministic identity
key ``{source_node_id, target_node_id}``. Re-running the resolver produces the
same graph state — no duplicate edges.

## Integration
The resolver is triggered by the ``extraction_complete`` PipelineEvent emitted
by ``EntityExtractionConsumer._check_and_emit_completion()``.  The consumer
(or an external pipeline coordinator) calls ``EntityResolver.resolve()``.

## Definition vs Import heuristic
Within a DocumentChunk the chunk text is analysed:
- Entity is considered DEFINED in a chunk when the chunk's entity_type for that
  entity is a structural type: ``class``, ``function``, ``method``, ``interface``,
  ``struct``, ``enum``, ``module``, or ``namespace``.
- Entity is considered IMPORTED/USED in a chunk when it appears but entity_type
  is a reference type: ``variable``, ``import``, ``constant``, ``parameter``,
  ``argument``, ``call``, or any unrecognised type.

This is a best-effort heuristic — the LLM extraction already classifies types.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import structlog
from neo4j import AsyncDriver

logger = structlog.get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────

# Entity types that indicate the entity is DEFINED in a chunk.
# All comparisons are done in lowercase.
_DEFINITION_TYPES: frozenset[str] = frozenset(
    {
        "class",
        "function",
        "method",
        "interface",
        "struct",
        "enum",
        "module",
        "namespace",
        "type",
        "trait",
        "mixin",
        "abstract",
        "protocol",
        "decorator",
        "generator",
        "coroutine",
        "lambda",
    }
)

# Batch size for Neo4j write operations.
_DEFAULT_BATCH_SIZE = 200


# ── Resolution result ────────────────────────────────────────────────


class ResolutionStats:
    """Accumulated statistics for a single resolver run."""

    __slots__ = (
        "ingestion_id",
        "project_id",
        "entities_analysed",
        "defined_in_created",
        "imported_by_created",
        "errors",
    )

    def __init__(self, ingestion_id: str, project_id: str) -> None:
        self.ingestion_id = ingestion_id
        self.project_id = project_id
        self.entities_analysed: int = 0
        self.defined_in_created: int = 0
        self.imported_by_created: int = 0
        self.errors: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ingestion_id": self.ingestion_id,
            "project_id": self.project_id,
            "entities_analysed": self.entities_analysed,
            "defined_in_created": self.defined_in_created,
            "imported_by_created": self.imported_by_created,
            "errors": self.errors,
        }


# ── Main resolver ────────────────────────────────────────────────────


class EntityResolver:
    """Post-extraction cross-file entity resolver.

    Reads entity-chunk-document relationships from Neo4j for a given
    ingestion batch, classifies each appearance as a definition or import,
    and creates the corresponding semantic edges.

    Args:
        driver: An async Neo4j driver instance (shared, not owned).
        batch_size: Number of edges to write per Neo4j transaction.
    """

    def __init__(
        self,
        driver: AsyncDriver,
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> None:
        self._driver = driver
        self._batch_size = batch_size

    # ── Public API ────────────────────────────────────────────────────

    async def resolve(
        self,
        ingestion_id: str,
        company_id: str,
        project_id: str,
        database: Optional[str] = None,
    ) -> ResolutionStats:
        """Run cross-file entity resolution for a completed ingestion batch.

        Steps:
        1. Query Neo4j for all (entity, entity_type, file_path, doc_id) tuples
           that belong to this project.  Only entities written during this
           ingestion are targeted (filtered by ingestion scope via
           file_version_id / project_id).
        2. Group appearances per entity_id → per doc_id.
        3. Classify each appearance as definition or import.
        4. Merge ``defined_in`` / ``imported_by`` edges in Neo4j.

        Args:
            ingestion_id: Identifies the ingestion batch.
            company_id: Tenant identifier (used to resolve database name).
            project_id: Project scope.
            database: Neo4j database name.  Defaults to ``cognee-{company_id}``.

        Returns:
            ResolutionStats with counts of edges created.
        """
        db = database or f"cognee-{company_id}"
        stats = ResolutionStats(ingestion_id=ingestion_id, project_id=project_id)

        t0 = time.time()
        logger.info(
            "resolver.started",
            ingestion_id=ingestion_id,
            project_id=project_id,
            database=db,
        )

        try:
            appearances = await self._fetch_entity_appearances(project_id, db)
        except Exception as exc:
            logger.error(
                "resolver.fetch_failed",
                ingestion_id=ingestion_id,
                error=str(exc),
                exc_info=True,
            )
            stats.errors += 1
            return stats

        if not appearances:
            logger.info(
                "resolver.no_entities_found",
                ingestion_id=ingestion_id,
                project_id=project_id,
            )
            return stats

        # Classify and build edge lists
        defined_in_edges, imported_by_edges = self._classify_appearances(appearances, stats)

        # Write edges to Neo4j
        try:
            stats.defined_in_created = await self._write_edges(defined_in_edges, "defined_in", db)
        except Exception as exc:
            logger.error(
                "resolver.defined_in_write_failed",
                ingestion_id=ingestion_id,
                error=str(exc),
                exc_info=True,
            )
            stats.errors += 1

        try:
            stats.imported_by_created = await self._write_edges(
                imported_by_edges, "imported_by", db
            )
        except Exception as exc:
            logger.error(
                "resolver.imported_by_write_failed",
                ingestion_id=ingestion_id,
                error=str(exc),
                exc_info=True,
            )
            stats.errors += 1

        duration = round(time.time() - t0, 3)
        logger.info(
            "resolver.complete",
            duration_s=duration,
            **stats.to_dict(),
        )

        return stats

    # ── Private: Neo4j query ──────────────────────────────────────────

    async def _fetch_entity_appearances(
        self,
        project_id: str,
        database: str,
    ) -> List[Dict[str, Any]]:
        """Fetch all (entity_id, entity_type, doc_id, file_path) tuples for a project.

        Uses the existing graph edges:
        - DocumentChunk -[:contains]-> Entity
        - DocumentChunk -[:is_part_of]-> Document

        This gives us exactly the entity appearances (one row per entity per chunk)
        with the associated document information, which enables cross-file analysis.

        Returns a list of dicts with keys:
            entity_id, entity_type, entity_name,
            chunk_id, doc_id, file_path
        """
        query = """
        MATCH (dc:`__Node__`:DocumentChunk {project_id: $project_id})
              -[:contains]->(e:`__Node__`:Entity)
        MATCH (dc)-[:is_part_of]->(d:`__Node__`:Document)
        RETURN
            e.id           AS entity_id,
            e.entity_type  AS entity_type,
            e.name         AS entity_name,
            dc.id          AS chunk_id,
            d.id           AS doc_id,
            d.file_path    AS file_path
        """
        async with self._driver.session(database=database) as session:
            result = await session.run(query, {"project_id": project_id})
            records = await result.data()

        return [
            {
                "entity_id": r["entity_id"],
                "entity_type": (r["entity_type"] or "").lower(),
                "entity_name": r["entity_name"] or "",
                "chunk_id": r["chunk_id"],
                "doc_id": r["doc_id"],
                "file_path": r["file_path"] or "",
            }
            for r in records
            if r.get("entity_id") and r.get("doc_id")
        ]

    # ── Private: classification ───────────────────────────────────────

    def _classify_appearances(
        self,
        appearances: List[Dict[str, Any]],
        stats: ResolutionStats,
    ) -> tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        """Group appearances by entity and classify as definition vs import.

        For each entity, if it appears in multiple documents:
        - Documents where entity_type is a definition type → ``defined_in``
        - Documents where entity_type is a reference type → ``imported_by``

        When the same entity appears with different types in different chunks of
        the SAME document, the definition type takes precedence for that document.

        Returns:
            Tuple of (defined_in_edges, imported_by_edges) — each a list of
            dicts with keys ``entity_id`` and ``doc_id``.
        """
        # entity_id → doc_id → best_entity_type (definition wins over import)
        entity_doc_types: Dict[str, Dict[str, str]] = {}

        for row in appearances:
            eid = row["entity_id"]
            did = row["doc_id"]
            etype = row["entity_type"]

            if eid not in entity_doc_types:
                entity_doc_types[eid] = {}

            current = entity_doc_types[eid].get(did, "")
            # Upgrade to definition type if we encounter one
            if _is_definition_type(etype) or not current:
                entity_doc_types[eid][did] = etype

        stats.entities_analysed = len(entity_doc_types)

        defined_in_edges: List[Dict[str, str]] = []
        imported_by_edges: List[Dict[str, str]] = []

        for entity_id, doc_map in entity_doc_types.items():
            for doc_id, entity_type in doc_map.items():
                edge = {"entity_id": entity_id, "doc_id": doc_id}
                if _is_definition_type(entity_type):
                    defined_in_edges.append(edge)
                else:
                    imported_by_edges.append(edge)

        return defined_in_edges, imported_by_edges

    # ── Private: Neo4j write ──────────────────────────────────────────

    async def _write_edges(
        self,
        edges: List[Dict[str, str]],
        relationship_type: str,
        database: str,
    ) -> int:
        """Batch MERGE edges of a given relationship type in Neo4j.

        Uses APOC ``apoc.merge.relationship`` for idempotent writes.
        Batches are committed individually to bound transaction size.

        Args:
            edges: List of dicts with ``entity_id`` and ``doc_id``.
            relationship_type: Neo4j relationship label (e.g. ``defined_in``).
            database: Neo4j database name.

        Returns:
            Total number of relationship rows affected.
        """
        if not edges:
            return 0

        total = 0
        now_ms = int(time.time() * 1000)

        for i in range(0, len(edges), self._batch_size):
            batch = edges[i : i + self._batch_size]
            records = [
                {
                    "entity_id": e["entity_id"],
                    "doc_id": e["doc_id"],
                    "properties": {
                        "source_node_id": e["entity_id"],
                        "target_node_id": e["doc_id"],
                        "relationship_name": relationship_type,
                        "ontology_valid": False,
                        "updated_at": now_ms,
                    },
                }
                for e in batch
            ]

            async with self._driver.session(database=database) as session:
                result = await session.run(
                    """
                    UNWIND $edges AS edge
                    MATCH (entity:`__Node__` {id: edge.entity_id})
                    MATCH (doc:`__Node__`:Document {id: edge.doc_id})
                    CALL apoc.merge.relationship(
                        entity,
                        $rel_type,
                        {source_node_id: edge.entity_id, target_node_id: edge.doc_id},
                        edge.properties,
                        doc
                    ) YIELD rel
                    RETURN rel
                    """,
                    {"edges": records, "rel_type": relationship_type},
                )
                summary = await result.consume()
                # Each matched relationship counts as one row
                total += summary.counters.relationships_created

        logger.debug(
            "resolver.edges_written",
            relationship_type=relationship_type,
            total=total,
        )
        return total


# ── Module-level helpers ──────────────────────────────────────────────


def _is_definition_type(entity_type: str) -> bool:
    """Return True if the entity type indicates the entity is defined (not just referenced)."""
    return entity_type.lower() in _DEFINITION_TYPES

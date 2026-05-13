"""Neo4j batch write logic for Neo4j Storage Service (Service 6).

Handles:
- Phase 0: Initialize constraints and APOC verification
- Phase 1: MERGE entity nodes, EntityType nodes, DocumentChunk nodes
- Phase 2: MERGE edges (LLM relationships, contains, is_a)
- All writes use UNWIND for batching efficiency
- All nodes use __Node__ base label + APOC dynamic labels (Cognee-compatible)
- All nodes include DataPoint base properties
- All edges use APOC dynamic relationship types
- Retry with exponential backoff on transient failures
"""

from __future__ import annotations

import asyncio
import re
import os
import time
import uuid
from typing import Any, Dict, List, Optional

from shared.slugify import slugify

from .multi_tenancy import ensure_neo4j_database, invalidate_database_cache


def _cognee_id(text: str) -> str:
    """Generate UUID5 matching Cognee's generate_node_id / generate_edge_id normalisation."""
    return str(uuid.uuid5(uuid.NAMESPACE_OID, text.lower().replace(" ", "_").replace("'", "")))


def _entity_id(name: str, node_set: str) -> str:
    """Generate the scope-aware Entity / EntityType UUID5."""
    return _cognee_id(f"{name}|{node_set}")


def _build_node_set(item: Dict[str, Any]) -> str:
    """Build the canonical node_set / source_node_set value.

    - Documents and summaries: "{company_id}_knowledge"
    - Code: "{project_id}_{project_name}_code"

    ``project_name`` falls back to ``project_id`` for legacy callers.
    """
    content_type = item.get("content_type", "") or "code"
    if content_type == "document":
        return f"{item.get('company_id', 'unknown')}_knowledge"
    project_id = item.get("project_id") or item.get("company_id", "unknown")
    project_name = item.get("project_name") or project_id
    return f"{project_id}_{project_name}_code"


def _scope_key(company_id: str, project_id: Optional[str]) -> str:
    """Return the active scope key.

    Documents fall back to company_id; code remains project-scoped.
    """
    return project_id or company_id


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _sorted_records(records: List[Dict[str, Any]], *keys: str) -> List[Dict[str, Any]]:
    return sorted(records, key=lambda record: tuple(str(record.get(key, "")) for key in keys))


import structlog
from neo4j import AsyncDriver

from .config import Neo4jStorageConfig

logger = structlog.get_logger(__name__)

# Map file extensions to MIME types for Document nodes
_MIME_MAP = {
    ".py": "text/x-python",
    ".js": "text/javascript",
    ".ts": "text/typescript",
    ".tsx": "text/typescript-jsx",
    ".jsx": "text/javascript-jsx",
    ".json": "application/json",
    ".html": "text/html",
    ".css": "text/css",
    ".md": "text/markdown",
    ".yaml": "text/yaml",
    ".yml": "text/yaml",
    ".xml": "text/xml",
    ".sql": "text/x-sql",
    ".sh": "text/x-shellscript",
    ".rb": "text/x-ruby",
    ".java": "text/x-java",
    ".go": "text/x-go",
    ".rs": "text/x-rust",
    ".swift": "text/x-swift",
    ".kt": "text/x-kotlin",
    ".c": "text/x-c",
    ".cpp": "text/x-c++",
    ".h": "text/x-c-header",
    ".vue": "text/x-vue",
    ".svelte": "text/x-svelte",
}


def _guess_mime_type(file_path: str) -> str:
    """Guess MIME type from file extension."""
    for ext, mime in _MIME_MAP.items():
        if file_path.endswith(ext):
            return mime
    return "text/plain"


class Neo4jBatchWriter:
    """Batch writer for Neo4j graph database.

    Uses UNWIND MERGE pattern for efficient batch writes.
    Three-phase write: constraints initialization, nodes, then edges.
    All nodes use __Node__ base label + APOC dynamic labels (Cognee-compatible).
    """

    def __init__(
        self,
        driver: AsyncDriver,
        config: type[Neo4jStorageConfig] = Neo4jStorageConfig,
    ) -> None:
        self._driver = driver
        self._config = config

    # ── Phase 0: Initialization ───────────────────────────────────────

    async def ensure_constraints(
        self,
        database: Optional[str] = None,
    ) -> None:
        """Create required constraints for __Node__ base label.

        Must be called once before any writes.
        Idempotent — safe to call multiple times.
        """
        await self._execute_with_retry(
            """
            CREATE CONSTRAINT IF NOT EXISTS FOR (n:`__Node__`) REQUIRE n.id IS UNIQUE
            """,
            {},
            database=database,
        )
        logger.info("writer.constraints_ensured")

    # ── Phase 1: Node Writes ──────────────────────────────────────────

    async def write_entity_nodes(
        self,
        entities: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Batch MERGE entity nodes using UNWIND + APOC dynamic labels.

        Each entity dict must have: entity_id, name, entity_type,
        description, company_id, project_id.

        All nodes get __Node__ base label + Entity dynamic label via APOC.
        All nodes include DataPoint base properties.

        Returns number of nodes merged.
        """
        if not entities:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE

        for i in range(0, len(entities), batch_size):
            batch = entities[i : i + batch_size]
            now_ms = int(time.time() * 1000)
            records = _sorted_records(
                [
                    {
                        "scope_key": _scope_key(
                            e.get("company_id", ""), e.get("project_id") or None
                        ),
                        "id": str(e["entity_id"]),
                        "label": "Entity",
                        "properties": {
                            "id": str(e["entity_id"]),
                            "name": e.get("name", ""),
                            "entity_type": e.get("entity_type", ""),
                            "description": e.get("description", ""),
                            "company_id": e.get("company_id", ""),
                            **(
                                {"project_id": e.get("project_id", "")}
                                if e.get("content_type", "code") != "document"
                                else {}
                            ),
                            **(
                                {
                                    "project_name": e.get("project_name")
                                    or e.get("project_id")
                                    or e.get("company_id", "")
                                }
                                if e.get("content_type", "code") != "document"
                                else {}
                            ),
                            "file_version_id": e.get("file_version_id", ""),
                            "branch": e.get("branch", ""),
                            "file_path": e.get("file_path", ""),
                            "node_set": _build_node_set(
                                {**e, "project_id": e.get("project_id") or e.get("company_id", "")}
                            ),
                            # DataPoint base properties
                            "created_at": now_ms,
                            "updated_at": now_ms,
                            "ontology_valid": False,
                            "version": 1,
                            "topological_rank": 0,
                            "type": "Entity",
                            "source_pipeline": "v2_ingestion",
                            "source_task": "entity_storage",
                            "source_node_set": _build_node_set(
                                {**e, "project_id": e.get("project_id") or e.get("company_id", "")}
                            ),
                            "source_user": "default_user@example.com",
                        },
                    }
                    for e in batch
                ],
                "id",
            )

            counters = await self._execute_with_retry(
                """
                UNWIND $nodes AS node
                MERGE (n:`__Node__` {id: node.id})
                ON CREATE SET n += node.properties, n.updated_at = timestamp()
                ON MATCH SET
                    n.name = node.properties.name,
                    n.text = node.properties.text,
                    n.chunk_index = node.properties.chunk_index,
                    n.chunk_id = node.properties.chunk_id,
                    n.summary_text = node.properties.summary_text,
                    n.description = node.properties.description,
                    n.company_id = node.properties.company_id,
                    n.project_id = node.properties.project_id,
                    n.file_version_id = node.properties.file_version_id,
                    n.branch = node.properties.branch,
                    n.updated_at = timestamp(),
                    n.ontology_valid = node.properties.ontology_valid,
                    n.version = node.properties.version,
                    n.topological_rank = node.properties.topological_rank,
                    n.type = node.properties.type,
                    n.source_pipeline = node.properties.source_pipeline,
                    n.source_task = node.properties.source_task,
                    n.source_node_set = node.properties.source_node_set,
                    n.source_user = node.properties.source_user
                WITH n, node.label AS label
                CALL apoc.create.addLabels(n, [label]) YIELD node AS labeledNode
                RETURN ID(labeledNode) AS internal_id
                """,
                {"nodes": records},
                database=database,
            )
            total += counters["nodes_created"]

        logger.info(
            "writer.entity_nodes_written",
            total=total,
        )
        return total

    async def write_entity_type_nodes(
        self,
        entity_types: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Batch MERGE EntityType nodes using UNWIND + APOC dynamic labels.

        Each record must include at least ``name`` and ``node_set``.

        All nodes get __Node__ base label + EntityType dynamic label via APOC.
        All nodes include DataPoint base properties.

        Returns number of nodes merged.
        """
        if not entity_types:
            return 0

        now_ms = int(time.time() * 1000)
        records = _sorted_records(
            [
                {
                    "id": _entity_id(
                        t.get("name", t.get("entity_type", "")), t.get("node_set", "")
                    ),
                    "label": "EntityType",
                    "properties": {
                        "id": _entity_id(
                            t.get("name", t.get("entity_type", "")), t.get("node_set", "")
                        ),
                        "name": t.get("name", t.get("entity_type", "")),
                        "description": f"Entity type: {t.get('name', t.get('entity_type', ''))}",
                        "node_set": t.get("node_set", ""),
                        # DataPoint base properties
                        "created_at": now_ms,
                        "updated_at": now_ms,
                        "ontology_valid": False,
                        "version": 1,
                        "topological_rank": 0,
                        "type": "EntityType",
                        "source_pipeline": "v2_ingestion",
                        "source_task": "entity_storage",
                        "source_node_set": t.get("node_set", ""),
                        "source_user": "default_user@example.com",
                    },
                }
                for t in entity_types
                if t.get("name", t.get("entity_type", "")) and t.get("node_set")
            ],
            "id",
        )

        counters = await self._execute_with_retry(
            """
            UNWIND $nodes AS node
            MERGE (n:`__Node__` {id: node.id})
            ON CREATE SET n += node.properties, n.updated_at = timestamp()
            ON MATCH SET n += node.properties, n.updated_at = timestamp()
            WITH n, node.label AS label
            CALL apoc.create.addLabels(n, [label]) YIELD node AS labeledNode
            RETURN ID(labeledNode) AS internal_id
            """,
            {"nodes": records},
            database=database,
        )
        created = counters["nodes_created"]

        logger.info(
            "writer.entity_type_nodes_written",
            total=created,
        )
        return created

    async def write_chunk_nodes(
        self,
        chunks: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Batch MERGE DocumentChunk nodes using UNWIND + APOC dynamic labels.

        Each chunk dict must have: chunk_id, text, file_path, repository, branch,
        language, chunk_index, company_id, project_id.

        All nodes get __Node__ base label + DocumentChunk dynamic label via APOC.
        All nodes include DataPoint base properties.
        The ``text`` property stores raw chunk content for graph retrieval.

        Returns number of nodes merged.
        """
        if not chunks:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE

        for i in range(0, len(chunks), batch_size):
            batch = chunks[i : i + batch_size]
            now_ms = int(time.time() * 1000)
            records = _sorted_records(
                [
                    {
                        "chunk_index": c.get("chunk_index", 0),
                        "scope_key": _scope_key(
                            c.get("company_id", ""), c.get("project_id") or None
                        ),
                        "id": str(c["chunk_id"]),
                        "label": "DocumentChunk",
                        "content_type": c.get("content_type", "code"),
                        "properties": {
                            "id": str(c["chunk_id"]),
                            "name": f"{c.get('file_path', '')}#{c.get('chunk_index', 0)}",
                            "text": c.get("text", ""),
                            "file_path": c.get("file_path", ""),
                            "repository": c.get("repository", ""),
                            "branch": c.get("branch", ""),
                            "language": c.get("language", ""),
                            "chunk_index": c.get("chunk_index", 0),
                            "chunk_size": len(c.get("text", "")),
                            "start_line": c.get("start_line", 0),
                            "end_line": c.get("end_line", 0),
                            "cut_type": "TreeSitter",
                            "company_id": c.get("company_id", ""),
                            **(
                                {"project_id": c.get("project_id", "")}
                                if c.get("content_type", "code") != "document"
                                else {}
                            ),
                            **(
                                {
                                    "project_name": c.get("project_name")
                                    or c.get("project_id")
                                    or c.get("company_id", "")
                                }
                                if c.get("content_type", "code") != "document"
                                else {}
                            ),
                            "file_version_id": c.get("file_version_id", ""),
                            "description": c.get("description")
                            or f"Chunk {c.get('chunk_index', 0)} from {c.get('file_path', '')}",
                            "node_set": _build_node_set(
                                {**c, "project_id": c.get("project_id") or c.get("company_id", "")}
                            ),
                            # DataPoint base properties
                            "created_at": now_ms,
                            "updated_at": now_ms,
                            "ontology_valid": False,
                            "version": 1,
                            "topological_rank": 0,
                            "type": "DocumentChunk",
                            "source_pipeline": "v2_ingestion",
                            "source_task": "chunk_storage",
                            "source_node_set": _build_node_set(
                                {**c, "project_id": c.get("project_id") or c.get("company_id", "")}
                            ),
                            "source_user": "default_user@example.com",
                        },
                    }
                    for c in batch
                ],
                "id",
            )

            counters = await self._execute_with_retry(
                """
                UNWIND $nodes AS node
                MERGE (n:`__Node__` {id: node.id})
                ON CREATE SET n += node.properties, n.updated_at = timestamp()
                ON MATCH SET 
                    n += node.properties,
                    n.description = node.properties.description,
                    n.entity_type = node.properties.entity_type,
                    n.company_id = node.properties.company_id,
                    n.project_id = node.properties.project_id,
                    n.file_version_id = node.properties.file_version_id,
                    n.branch = node.properties.branch,
                    n.chunk_index = node.properties.chunk_index,
                    n.file_path = CASE
                        WHEN node.properties.file_path CONTAINS node.properties.name THEN node.properties.file_path
                        WHEN n.file_path CONTAINS node.properties.name THEN n.file_path
                        ELSE node.properties.file_path
                    END,
                    n.updated_at = timestamp(),
                    n.ontology_valid = node.properties.ontology_valid,
                    n.version = node.properties.version,
                    n.topological_rank = node.properties.topological_rank,
                    n.type = node.properties.type,
                    n.source_pipeline = node.properties.source_pipeline,
                    n.source_task = node.properties.source_task,
                    n.source_node_set = node.properties.source_node_set,
                    n.source_user = node.properties.source_user
                WITH n, node.label AS label
                CALL apoc.create.addLabels(n, [label]) YIELD node AS labeledNode
                RETURN ID(labeledNode) AS internal_id
                """,
                {"nodes": records},
                database=database,
            )
            total += counters["nodes_created"]

        logger.info(
            "writer.chunk_nodes_written",
            total=total,
        )
        return total

    async def write_summary_nodes(
        self,
        summaries: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Batch MERGE TextSummary nodes using UNWIND + APOC dynamic labels.

        Each summary dict must have: summary_id, chunk_id, summary_text,
        company_id, project_id.

        All nodes get __Node__ base label + TextSummary dynamic label via APOC.
        All nodes include DataPoint base properties.

        Returns number of nodes merged.
        """
        if not summaries:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE

        for i in range(0, len(summaries), batch_size):
            batch = summaries[i : i + batch_size]
            now_ms = int(time.time() * 1000)
            records = _sorted_records(
                [
                    {
                        "scope_key": _scope_key(
                            s.get("company_id", ""), s.get("project_id") or None
                        ),
                        "id": str(s["summary_id"]),
                        "label": "TextSummary",
                        "content_type": s.get("content_type", "code"),
                        "properties": {
                            "id": str(s["summary_id"]),
                            "name": f"summary_{s.get('chunk_id', '')}",
                            "chunk_id": str(s.get("chunk_id", "")),
                            "chunk_index": s.get("chunk_index", 0),
                            "text": s.get("summary_text", ""),
                            "summary_text": s.get("summary_text", ""),
                            "description": s.get("summary_text", "")[:200],
                            "company_id": s.get("company_id", ""),
                            **(
                                {"project_id": s.get("project_id", "")}
                                if s.get("content_type", "code") != "document"
                                else {}
                            ),
                            **(
                                {
                                    "project_name": s.get("project_name")
                                    or s.get("project_id")
                                    or s.get("company_id", "")
                                }
                                if s.get("content_type", "code") != "document"
                                else {}
                            ),
                            "file_version_id": s.get("file_version_id", ""),
                            "branch": s.get("branch", ""),
                            "node_set": _build_node_set(
                                {**s, "project_id": s.get("project_id") or s.get("company_id", "")}
                            ),
                            # DataPoint base properties
                            "created_at": now_ms,
                            "updated_at": now_ms,
                            "ontology_valid": False,
                            "version": 1,
                            "topological_rank": 0,
                            "type": "TextSummary",
                            "source_pipeline": "v2_ingestion",
                            "source_task": "summarization",
                            "source_node_set": _build_node_set(
                                {**s, "project_id": s.get("project_id") or s.get("company_id", "")}
                            ),
                            "source_user": "default_user@example.com",
                        },
                    }
                    for s in batch
                ],
                "id",
            )

            counters = await self._execute_with_retry(
                """
                UNWIND $nodes AS node
                MERGE (n:`__Node__` {id: node.id})
                ON CREATE SET n += node.properties, n.updated_at = timestamp()
                ON MATCH SET
                    n += node.properties,
                    n.name = node.properties.name,
                    n.text = node.properties.text,
                    n.chunk_index = node.properties.chunk_index,
                    n.chunk_id = node.properties.chunk_id,
                    n.summary_text = node.properties.summary_text,
                    n.description = node.properties.description,
                    n.company_id = node.properties.company_id,
                    n.project_id = node.properties.project_id,
                    n.file_version_id = node.properties.file_version_id,
                    n.branch = node.properties.branch,
                    n.updated_at = timestamp(),
                    n.ontology_valid = node.properties.ontology_valid,
                    n.version = node.properties.version,
                    n.topological_rank = node.properties.topological_rank,
                    n.type = node.properties.type,
                    n.source_pipeline = node.properties.source_pipeline,
                    n.source_task = node.properties.source_task,
                    n.source_node_set = node.properties.source_node_set,
                    n.source_user = node.properties.source_user
                WITH n, node.label AS label
                CALL apoc.create.addLabels(n, [label]) YIELD node AS labeledNode
                RETURN ID(labeledNode) AS internal_id
                """,
                {"nodes": records},
                database=database,
            )
            total += counters["nodes_created"]

        logger.info(
            "writer.summary_nodes_written",
            total=total,
        )
        return total

    async def write_summarizes_edges(
        self,
        summaries: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Batch MERGE 'summarizes' edges: TextSummary -> DocumentChunk.

        Each summary dict must have: summary_id, chunk_id.

        Uses lowercase 'summarizes' relationship type.
        Uses APOC for dynamic relationship creation.
        All edges include standard Cognee edge properties.

        Returns number of edges merged.
        """
        if not summaries:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE

        for i in range(0, len(summaries), batch_size):
            batch = summaries[i : i + batch_size]
            now_ms = int(time.time() * 1000)
            records = _sorted_records(
                [
                    {
                        "summary_id": str(s["summary_id"]),
                        "chunk_id": str(s["chunk_id"]),
                        "properties": {
                            "source_node_id": str(s["summary_id"]),
                            "target_node_id": str(s["chunk_id"]),
                            "relationship_name": "summarizes",
                            "ontology_valid": False,
                            "updated_at": now_ms,
                        },
                    }
                    for s in batch
                ],
                "summary_id",
                "chunk_id",
            )

            counters = await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (from_node:`__Node__` {id: edge.summary_id})
                MATCH (to_node:`__Node__` {id: edge.chunk_id})
                CALL apoc.merge.relationship(
                    from_node,
                    'summarizes',
                    {source_node_id: edge.summary_id, target_node_id: edge.chunk_id},
                    edge.properties,
                    to_node
                ) YIELD rel
                RETURN rel
                """,
                {"edges": records},
                database=database,
            )
            total += counters["rows_affected"]

        logger.info(
            "writer.summarizes_edges_written",
            total=total,
        )
        return total

    async def write_summary_made_from_edges(
        self,
        summaries: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Batch MERGE 'made_from' edges: TextSummary -> DocumentChunk.

        Links each TextSummary back to the DocumentChunk it was generated from.
        Each summary dict must have: summary_id, chunk_id.

        Uses lowercase 'made_from' relationship type (Cognee convention).
        Uses APOC for dynamic relationship creation.
        All edges include standard Cognee edge properties.

        Returns number of edges merged.
        """
        if not summaries:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE

        for i in range(0, len(summaries), batch_size):
            batch = summaries[i : i + batch_size]
            now_ms = int(time.time() * 1000)
            records = _sorted_records(
                [
                    {
                        "summary_id": str(s["summary_id"]),
                        "chunk_id": str(s["chunk_id"]),
                        "properties": {
                            "source_node_id": str(s["summary_id"]),
                            "target_node_id": str(s["chunk_id"]),
                            "relationship_name": "made_from",
                            "ontology_valid": False,
                            "updated_at": now_ms,
                        },
                    }
                    for s in batch
                ],
                "summary_id",
                "chunk_id",
            )

            counters = await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (from_node:`__Node__` {id: edge.summary_id})
                MATCH (to_node:`__Node__` {id: edge.chunk_id})
                CALL apoc.merge.relationship(
                    from_node,
                    'made_from',
                    {source_node_id: edge.summary_id, target_node_id: edge.chunk_id},
                    edge.properties,
                    to_node
                ) YIELD rel
                RETURN rel
                """,
                {"edges": records},
                database=database,
            )
            total += counters["rows_affected"]

        logger.info(
            "writer.summary_made_from_edges_written",
            total=total,
        )
        return total

    async def backfill_summary_edges(
        self,
        chunk_ids: List[str],
        database: Optional[str] = None,
    ) -> int:
        """Backfill missing summary edges for newly created DocumentChunk nodes.

        When summary events arrive before entity extraction events, TextSummary
        nodes are created but their edges (summarizes, made_from) fail to create
        because the target DocumentChunk nodes don't exist yet.

        This method finds all TextSummary nodes referencing the given chunk_ids,
        then creates any missing 'summarizes' and 'made_from' edges to the now-
        existing DocumentChunk nodes.

        Args:
            chunk_ids: List of chunk_id strings for newly created DocumentChunks.
            database: Neo4j database name (defaults to config).

        Returns:
            Total count of edges created (summarizes + made_from).
        """
        if not chunk_ids:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE
        now_ms = int(time.time() * 1000)

        for i in range(0, len(chunk_ids), batch_size):
            batch = chunk_ids[i : i + batch_size]
            batch = sorted(batch)

            # Create summarizes edges (TextSummary -> DocumentChunk)
            summarizes_counters = await self._execute_with_retry(
                """
                UNWIND $chunk_ids AS chunk_id
                MATCH (ts:`__Node__`:TextSummary {chunk_id: chunk_id})
                MATCH (dc:`__Node__`:DocumentChunk {id: chunk_id})
                CALL apoc.merge.relationship(
                    ts,
                    'summarizes',
                    {source_node_id: ts.id, target_node_id: dc.id},
                    {
                        source_node_id: ts.id,
                        target_node_id: dc.id,
                        relationship_name: 'summarizes',
                        ontology_valid: false,
                        updated_at: $now_ms
                    },
                    dc
                ) YIELD rel
                RETURN rel
                """,
                {"chunk_ids": batch, "now_ms": now_ms},
                database=database,
            )

            # Create made_from edges (TextSummary -> DocumentChunk)
            made_from_counters = await self._execute_with_retry(
                """
                UNWIND $chunk_ids AS chunk_id
                MATCH (ts:`__Node__`:TextSummary {chunk_id: chunk_id})
                MATCH (dc:`__Node__`:DocumentChunk {id: chunk_id})
                CALL apoc.merge.relationship(
                    ts,
                    'made_from',
                    {source_node_id: ts.id, target_node_id: dc.id},
                    {
                        source_node_id: ts.id,
                        target_node_id: dc.id,
                        relationship_name: 'made_from',
                        ontology_valid: false,
                        updated_at: $now_ms
                    },
                    dc
                ) YIELD rel
                RETURN rel
                """,
                {"chunk_ids": batch, "now_ms": now_ms},
                database=database,
            )

            total += summarizes_counters["rows_affected"] + made_from_counters["rows_affected"]

        if total > 0:
            logger.info(
                "writer.summary_edges_backfilled",
                total=total,
            )

        return total

    # ── Phase 1b: NodeSet Nodes + belongs_to_set Edges ───────────────

    async def write_node_sets(
        self,
        chunks: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Create NodeSet nodes and belongs_to_set edges for cognee.search() scoping.

        Extracts unique ``source_node_set`` values from the chunks, MERGEs a
        ``(__Node__:NodeSet)`` node for each one, and MERGEs ``belongs_to_set``
        edges from every matching DocumentChunk to its NodeSet.

        Uses deterministic UUIDs via ``uuid5(NAMESPACE_OID, normalised_name)`` so repeated
        runs are idempotent.

        Returns number of NodeSet nodes merged.
        """
        if not chunks:
            return 0

        # Collect unique source_node_set values (same formula as write_chunk_nodes)
        unique_sets: Dict[str, str] = {}
        for c in chunks:
            name = _build_node_set(c)
            if name not in unique_sets:
                ns_id = _cognee_id(name)
                unique_sets[name] = ns_id

        if not unique_sets:
            return 0

        now_ms = int(time.time() * 1000)
        records = _sorted_records(
            [
                {
                    "id": ns_id,
                    "label": "NodeSet",
                    "properties": {
                        "id": ns_id,
                        "name": name,
                        "description": f"NodeSet: {name}",
                        # DataPoint base properties
                        "created_at": now_ms,
                        "updated_at": now_ms,
                        "ontology_valid": False,
                        "version": 1,
                        "topological_rank": 0,
                        "type": "NodeSet",
                        "source_pipeline": "v2_ingestion",
                        "source_task": "node_set_creation",
                        "source_node_set": name,
                        "source_user": "default_user@example.com",
                    },
                }
                for name, ns_id in unique_sets.items()
            ],
            "id",
        )

        # Step 1: MERGE NodeSet nodes
        counters = await self._execute_with_retry(
            """
            UNWIND $nodes AS node
            MERGE (n:`__Node__` {id: node.id})
            ON CREATE SET n += node.properties, n.updated_at = timestamp()
            ON MATCH SET n += node.properties, n.updated_at = timestamp()
            WITH n, node.label AS label
            CALL apoc.create.addLabels(n, [label]) YIELD node AS labeledNode
            RETURN ID(labeledNode) AS internal_id
            """,
            {"nodes": records},
            database=database,
        )
        node_count = counters["nodes_created"]

        # Step 2: MERGE belongs_to_set edges (DocumentChunk -> NodeSet)
        edge_records = _sorted_records(
            [
                {
                    "node_set_name": name,
                    "node_set_id": ns_id,
                }
                for name, ns_id in unique_sets.items()
            ],
            "node_set_id",
            "node_set_name",
        )

        now_ms = int(time.time() * 1000)
        await self._execute_with_retry(
            """
            UNWIND $sets AS s
            MATCH (c:`__Node__`:DocumentChunk {source_node_set: s.node_set_name})
            MATCH (ns:`__Node__` {id: s.node_set_id})
            CALL apoc.merge.relationship(
                c,
                'belongs_to_set',
                {source_node_id: c.id, target_node_id: s.node_set_id},
                {
                    source_node_id: c.id,
                    target_node_id: s.node_set_id,
                    relationship_name: 'belongs_to_set',
                    ontology_valid: false,
                    updated_at: $now_ms
                },
                ns
            ) YIELD rel
            RETURN rel
            """,
            {"sets": edge_records, "now_ms": now_ms},
            database=database,
        )

        # Step 3: MERGE belongs_to_set edges (Entity -> NodeSet)
        await self._execute_with_retry(
            """
            UNWIND $sets AS s
            MATCH (e:`__Node__`:Entity {source_node_set: s.node_set_name})
            MATCH (ns:`__Node__` {id: s.node_set_id})
            CALL apoc.merge.relationship(
                e,
                'belongs_to_set',
                {source_node_id: e.id, target_node_id: s.node_set_id},
                {
                    source_node_id: e.id,
                    target_node_id: s.node_set_id,
                    relationship_name: 'belongs_to_set',
                    ontology_valid: false,
                    updated_at: $now_ms
                },
                ns
            ) YIELD rel
            RETURN rel
            """,
            {"sets": edge_records, "now_ms": now_ms},
            database=database,
        )

        # Step 4: MERGE belongs_to_set edges (TextSummary -> NodeSet)
        await self._execute_with_retry(
            """
            UNWIND $sets AS s
            MATCH (ts:`__Node__`:TextSummary {source_node_set: s.node_set_name})
            MATCH (ns:`__Node__` {id: s.node_set_id})
            CALL apoc.merge.relationship(
                ts,
                'belongs_to_set',
                {source_node_id: ts.id, target_node_id: s.node_set_id},
                {
                    source_node_id: ts.id,
                    target_node_id: s.node_set_id,
                    relationship_name: 'belongs_to_set',
                    ontology_valid: false,
                    updated_at: $now_ms
                },
                ns
            ) YIELD rel
            RETURN rel
            """,
            {"sets": edge_records, "now_ms": now_ms},
            database=database,
        )

        logger.info(
            "writer.node_sets_written",
            node_sets=len(unique_sets),
            nodes_created=node_count,
        )
        return len(unique_sets)

    # ── Phase 1c0: Repository Node ──────────────────────────────────────

    async def write_repository_node(
        self,
        chunks: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Create a single Repository node per unique repository name.

        MERGEs ``(__Node__:Repository)`` with deterministic UUID from the active scope key.
        Then MERGEs ``is_part_of`` edges from Document nodes to Repository.

        Returns number of Repository nodes merged.
        """
        if not chunks:
            return 0

        # Collect unique repositories: keyed by scope key
        repos: Dict[str, Dict[str, str]] = {}
        for c in chunks:
            if c.get("content_type") == "document" or c.get("repository") == "kgrag-documents":
                continue
            pid = c.get("project_id") or c.get("company_id", "")
            repo_name = c.get("repository", "")
            if pid and repo_name and pid not in repos:
                repos[pid] = {
                    "project_id": pid,
                    "project_name": c.get("project_name") or pid,
                    "repository": repo_name,
                    "branch": c["branch"],
                    "company_id": c.get("company_id", ""),
                }

        if not repos:
            return 0

        now_ms = int(time.time() * 1000)
        records = []
        for pid, info in repos.items():
            repo_id = _cognee_id(f"repo:{pid}")
            node_set = _build_node_set(
                {
                    "content_type": "code",
                    "project_id": pid,
                    "project_name": info.get("project_name") or pid,
                }
            )
            records.append(
                {
                    "id": repo_id,
                    "label": "Repository",
                    "properties": {
                        "id": repo_id,
                        "name": info["repository"],
                        "repository": info["repository"],
                        "project_id": pid,
                        "company_id": info["company_id"],
                        "branch": info["branch"],
                        "node_set": node_set,
                        "created_at": now_ms,
                        "updated_at": now_ms,
                        "ontology_valid": False,
                        "version": 1,
                        "topological_rank": 0,
                        "type": "Repository",
                        "source_pipeline": "v2_ingestion",
                        "source_task": "repository_creation",
                        "source_node_set": node_set,
                        "source_user": "default_user@example.com",
                    },
                }
            )

        # MERGE Repository nodes
        records = _sorted_records(records, "id")

        counters = await self._execute_with_retry(
            """
            UNWIND $nodes AS node
            MERGE (n:`__Node__` {id: node.id})
            ON CREATE SET n += node.properties, n.updated_at = timestamp()
            ON MATCH SET n += node.properties, n.updated_at = timestamp()
            WITH n, node.label AS label
            CALL apoc.create.addLabels(n, [label]) YIELD node AS labeledNode
            RETURN ID(labeledNode) AS internal_id
            """,
            {"nodes": records},
            database=database,
        )

        logger.info(
            "writer.repository_nodes_written",
            count=len(records),
        )
        return len(records)

    # ── Phase 1c: Document Nodes + is_part_of Edges ────────────────────

    async def write_document_nodes(
        self,
        chunks: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Create Document nodes (one per unique file_path) and is_part_of edges.

        Groups chunks by canonical file identity, MERGEs a ``(__Node__:Document)`` node for
        each unique ``file_version_id`` when available (falling back to legacy
        slug/file-path identity only when needed), then MERGEs ``is_part_of`` edges from every matching
        DocumentChunk to its parent Document.

        Chunks with empty ``file_path`` are silently skipped.

        Uses deterministic UUIDs via ``uuid5(NAMESPACE_OID, normalised_id)``
        so repeated runs are idempotent.

        Returns number of Document nodes merged.
        """
        if not chunks:
            return 0

        def _document_identity(chunk: Dict[str, Any]) -> tuple[str, str, Optional[str], str, str]:
            company_id = chunk.get("company_id", "unknown")
            fp = chunk.get("file_path", "")
            document_title = (chunk.get("document_title") or chunk.get("title") or "").strip()
            document_slug = (
                (chunk.get("document_slug") or "").strip() or slugify(document_title) or None
            )
            file_version_id = str(chunk.get("file_version_id", "") or "").strip()
            # `content_type` carries the extraction-prompt routing key produced by
            # `shared/content_type_classifier.py` (e.g. "tsx", "typescript",
            # "ruby_rails", "ruby_spec", "java"). This value is selected
            # intentionally so the entity-extraction service can pick the
            # right prompt template, and it must be preserved verbatim on the
            # Document node for traceability.
            #
            # The only normalisation we apply is the Kafka producer's
            # "document" → "knowledge" rename, so the graph property aligns
            # with the node_set suffix (`_knowledge`) instead of duplicating
            # the node label.
            raw_content_type = str(chunk.get("content_type", "code") or "code")
            content_type = "knowledge" if raw_content_type == "document" else raw_content_type

            if file_version_id:
                return (
                    file_version_id,
                    document_title,
                    document_slug,
                    fp,
                    content_type,
                )

            if content_type == "document" and document_slug:
                return (
                    _cognee_id(f"{company_id}:{document_slug}"),
                    document_title,
                    document_slug,
                    fp,
                    content_type,
                )
            return (
                _cognee_id(f"{company_id}:{fp}"),
                document_title,
                document_slug,
                fp,
                content_type,
            )

        # Group by canonical document identity — slug for knowledge documents,
        # file_path for code/doc fallback.
        document_map: Dict[str, Dict[str, Any]] = {}
        for c in chunks:
            fp = c.get("file_path", "")
            if not fp:
                continue
            doc_id, _, _, _, _ = _document_identity(c)
            if doc_id not in document_map:
                document_map[doc_id] = c

        if not document_map:
            return 0

        now_ms = int(time.time() * 1000)
        records = []
        for doc_id, c in document_map.items():
            _, document_title, document_slug, fp, content_type = _document_identity(c)
            source_node_set = _build_node_set(c)
            node_name = document_title or fp
            records.append(
                {
                    "id": doc_id,
                    "label": "Document",
                    "properties": {
                        "id": doc_id,
                        "name": node_name,
                        "title": document_title or None,
                        "slug": document_slug,
                        "file_path": fp,
                        "raw_data_location": f"{c.get('repository', '')}/{fp}",
                        "mime_type": _guess_mime_type(fp),
                        "content_type": content_type,
                        "repository": c.get("repository", ""),
                        "branch": c.get("branch", ""),
                        "language": c.get("language", ""),
                        "company_id": c.get("company_id", ""),
                        "file_version_id": c.get("file_version_id", ""),
                        "node_set": source_node_set,
                        "source_node_set": source_node_set,
                        # DataPoint base properties
                        "created_at": now_ms,
                        "updated_at": now_ms,
                        "ontology_valid": False,
                        "version": 1,
                        "topological_rank": 0,
                        "type": "Document",
                        "source_pipeline": "v2_ingestion",
                        "source_task": "document_creation",
                        "source_user": "default_user@example.com",
                    },
                }
            )

        records = _sorted_records(records, "id")

        batch_size = self._config.NEO4J_BATCH_SIZE

        # Step 1: MERGE Document nodes
        node_count = 0
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            counters = await self._execute_with_retry(
                """
                UNWIND $nodes AS node
                MERGE (n:`__Node__` {id: node.id})
                ON CREATE SET n += node.properties, n.updated_at = timestamp()
                ON MATCH SET n += node.properties, n.updated_at = timestamp()
                SET n.title = coalesce(node.properties.title, n.title)
                SET n.name = coalesce(node.properties.name, node.properties.file_path, n.name)
                WITH n, node.label AS label
                CALL apoc.create.addLabels(n, [label]) YIELD node AS labeledNode
                RETURN ID(labeledNode) AS internal_id
                """,
                {"nodes": batch},
                database=database,
            )
            node_count += counters["nodes_created"]

        # Step 2: MERGE is_part_of edges (DocumentChunk -> Document)
        # Match chunks by source_node_set + file_path, match Document by canonical id.
        edge_records = []
        for c in chunks:
            fp = c.get("file_path", "")
            if not fp:
                continue
            doc_id, _, _, _, _ = _document_identity(c)
            source_node_set = _build_node_set(c)
            edge_records.append(
                {
                    "file_path": fp,
                    "doc_id": doc_id,
                    "source_node_set": source_node_set,
                }
            )

        edge_records = _sorted_records(edge_records, "doc_id", "file_path", "source_node_set")

        now_ms = int(time.time() * 1000)
        for i in range(0, len(edge_records), batch_size):
            batch = edge_records[i : i + batch_size]
            await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (c:`__Node__`:DocumentChunk {
                    source_node_set: edge.source_node_set,
                    file_path: edge.file_path
                })
                MATCH (d:`__Node__` {id: edge.doc_id})
                CALL apoc.merge.relationship(
                    c,
                    'is_part_of',
                    {source_node_id: c.id, target_node_id: edge.doc_id},
                    {
                        source_node_id: c.id,
                        target_node_id: edge.doc_id,
                        relationship_name: 'is_part_of',
                        ontology_valid: false,
                        updated_at: $now_ms
                    },
                    d
                ) YIELD rel
                RETURN rel
                """,
                {"edges": batch, "now_ms": now_ms},
                database=database,
            )

        # Step 3: MERGE is_a edges between Document and File Entity
        # File entities (created by LLM) have name matching file_path
        # Document is_a File Entity, File Entity is_a Document
        now_ms = int(time.time() * 1000)
        for i in range(0, len(edge_records), batch_size):
            batch = edge_records[i : i + batch_size]
            await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (d:`__Node__`:Document {id: edge.doc_id})
                MATCH (fe:`__Node__`:Entity {name: edge.file_path})
                WHERE (fe)-[:is_a]->(:EntityType {name: 'File'})
                CALL apoc.merge.relationship(
                    fe,
                    'is_a',
                    {source_node_id: fe.id, target_node_id: edge.doc_id},
                    {
                        source_node_id: fe.id,
                        target_node_id: edge.doc_id,
                        relationship_name: 'is_a',
                        ontology_valid: false,
                        updated_at: $now_ms
                    },
                    d
                ) YIELD rel
                RETURN rel
                """,
                {"edges": batch, "now_ms": now_ms},
                database=database,
            )

        # Step 3b: MERGE is_part_of edges (File Entity -> Repository)
        file_repo_edges = []
        for c in chunks:
            if c.get("content_type") == "document" or c.get("repository") == "kgrag-documents":
                continue
            fp = c.get("file_path", "")
            if not fp:
                continue
            cid = c.get("company_id", "")
            if not cid:
                continue
            repo_id = _cognee_id(f"repo:{cid}")
            file_repo_edges.append({"file_path": fp, "repo_id": repo_id})

        file_repo_edges = _sorted_records(file_repo_edges, "file_path", "repo_id")

        now_ms = int(time.time() * 1000)
        for i in range(0, len(file_repo_edges), batch_size):
            batch = file_repo_edges[i : i + batch_size]
            await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (fe:`__Node__`:Entity {name: edge.file_path})
                WHERE (fe)-[:is_a]->(:EntityType {name: 'File'})
                MATCH (r:`__Node__`:Repository {id: edge.repo_id})
                CALL apoc.merge.relationship(
                    fe,
                    'is_part_of',
                    {source_node_id: fe.id, target_node_id: edge.repo_id},
                    {
                        source_node_id: fe.id,
                        target_node_id: edge.repo_id,
                        relationship_name: 'is_part_of',
                        ontology_valid: false,
                        updated_at: $now_ms
                    },
                    r
                ) YIELD rel
                RETURN rel
                """,
                {"edges": batch, "now_ms": now_ms},
                database=database,
            )

        # Step 4: MERGE is_part_of edges (Document -> Repository)
        # Knowledge documents are intentionally excluded from Repository linkage.
        repo_edges = []
        for c in chunks:
            if c.get("content_type") == "document" or c.get("repository") == "kgrag-documents":
                continue
            fp = c.get("file_path", "")
            if not fp:
                continue
            cid = c.get("company_id", "")
            if not cid:
                continue
            doc_id = _cognee_id(f"{cid}:{fp}")
            repo_id = _cognee_id(f"repo:{cid}")
            repo_edges.append({"doc_id": doc_id, "repo_id": repo_id})

        repo_edges = _sorted_records(repo_edges, "doc_id", "repo_id")

        now_ms = int(time.time() * 1000)
        for i in range(0, len(repo_edges), batch_size):
            batch = repo_edges[i : i + batch_size]
            await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (d:`__Node__` {id: edge.doc_id})
                MATCH (r:`__Node__`:Repository {id: edge.repo_id})
                CALL apoc.merge.relationship(
                    d,
                    'is_part_of',
                    {source_node_id: edge.doc_id, target_node_id: edge.repo_id},
                    {
                        source_node_id: edge.doc_id,
                        target_node_id: edge.repo_id,
                        relationship_name: 'is_part_of',
                        ontology_valid: false,
                        updated_at: $now_ms
                    },
                    r
                ) YIELD rel
                RETURN rel
                """,
                {"edges": batch, "now_ms": now_ms},
                database=database,
            )

        logger.info(
            "writer.document_nodes_written",
            documents=len(document_map),
            nodes_created=node_count,
        )

        await self.backfill_document_identity_contract(list(document_map.keys()), database=database)

        return len(document_map)

    async def backfill_document_identity_contract(
        self,
        file_version_ids: List[str],
        database: Optional[str] = None,
    ) -> int:
        """Backfill legacy Document nodes to the canonical identity contract."""
        clean_ids = [str(v).strip() for v in file_version_ids if str(v or "").strip()]
        if not clean_ids:
            return 0

        clean_ids = sorted(clean_ids)

        counters = await self._execute_with_retry(
            """
            UNWIND $file_version_ids AS fvid
            MATCH (d:`__Node__`:Document {file_version_id: fvid})
            SET d.id = coalesce(d.file_version_id, d.id),
                d.content_type = coalesce(d.content_type, 'document'),
                d.updated_at = timestamp()
            RETURN count(d) AS updated
            """,
            {"file_version_ids": clean_ids},
            database=database,
        )
        logger.info(
            "writer.document_identity_backfill",
            updated=counters.get("nodes_created", 0),
            file_version_ids=len(clean_ids),
        )
        return counters.get("nodes_created", 0)

    # ── Phase 1d: Entity → Document (is_part_of) Edges ──────────────

    async def write_entity_document_edges(
        self,
        mappings: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Batch MERGE 'is_part_of' edges: Entity -> Document.

        Links each extracted entity to the Document (file) it was extracted from.
        This enables cascading deletion when a file is modified or removed.

        Each mapping has: entity_id, file_path, source_node_set.
        Returns number of edges created.
        """
        if not mappings:
            return 0

        # Deduplicate: same entity can appear in multiple chunks of same file
        seen = set()
        unique = []
        for m in mappings:
            key = (m["entity_id"], m["file_path"])
            if key not in seen:
                seen.add(key)
                unique.append(m)

        unique = sorted(unique, key=lambda m: (str(m["entity_id"]), str(m["file_path"])))

        now_ms = int(time.time() * 1000)
        counters = await self._execute_with_retry(
            """
            UNWIND $mappings AS m
            MATCH (e:`__Node__` {id: m.entity_id})
            MATCH (d:`__Node__`:Document {
                file_path: m.file_path,
                source_node_set: m.source_node_set
            })
            CALL apoc.merge.relationship(
                e,
                'is_part_of',
                {source_node_id: m.entity_id, target_node_id: d.id},
                {
                    source_node_id: m.entity_id,
                    target_node_id: d.id,
                    relationship_name: 'is_part_of',
                    ontology_valid: false,
                    updated_at: $now_ms
                },
                d
            ) YIELD rel
            RETURN rel
            """,
            {"mappings": unique, "now_ms": now_ms},
            database=database,
        )

        logger.info(
            "writer.entity_document_edges_written",
            edges=len(unique),
        )
        return len(unique)

    # ── Phase 2: Edge Writes ──────────────────────────────────────────

    async def write_llm_edges(
        self,
        edges: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Batch MERGE LLM-extracted relationship edges using APOC dynamic types.

        Each edge dict must have: source_entity_id, target_entity_id,
        relationship_type.

        Uses APOC to create dynamic relationship types.
        All edges include standard Cognee edge properties.

        Returns number of edges merged.
        """
        if not edges:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE

        # Filter out self-referencing edges (e.g. Client→Client) — noisy and useless
        edges = [
            e
            for e in edges
            if str(e.get("source_entity_id") or e.get("source_id", ""))
            != str(e.get("target_entity_id") or e.get("target_id", ""))
        ]

        if not edges:
            logger.debug("writer.llm_edges_all_self_refs_filtered")
            return 0

        for i in range(0, len(edges), batch_size):
            batch = edges[i : i + batch_size]
            now_ms = int(time.time() * 1000)
            records = _sorted_records(
                [
                    {
                        "source_id": str(e.get("source_entity_id") or e["source_id"]),
                        "target_id": str(e.get("target_entity_id") or e["target_id"]),
                        "rel_type": e.get("relationship_type", "RELATED_TO"),
                        "properties": {
                            "source_node_id": str(e.get("source_entity_id") or e["source_id"]),
                            "target_node_id": str(e.get("target_entity_id") or e["target_id"]),
                            "relationship_name": e.get("relationship_type", "RELATED_TO"),
                            "edge_text": f"{e.get('source_name', '')} {e.get('relationship_type', 'RELATED_TO')} {e.get('target_name', '')}".strip(),
                            "ontology_valid": False,
                            "updated_at": now_ms,
                        },
                    }
                    for e in batch
                ],
                "source_id",
                "target_id",
            )

            counters = await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (from_node:`__Node__` {id: edge.source_id})
                MATCH (to_node:`__Node__` {id: edge.target_id})
                CALL apoc.merge.relationship(
                    from_node,
                    edge.rel_type,
                    {source_node_id: edge.source_id, target_node_id: edge.target_id},
                    edge.properties,
                    to_node
                ) YIELD rel
                RETURN rel
                """,
                {"edges": records},
                database=database,
            )
            total += counters["rows_affected"]

        logger.info(
            "writer.llm_edges_written",
            total=total,
        )
        return total

    async def write_contains_edges(
        self,
        mappings: List[Dict[str, str]],
        database: Optional[str] = None,
        entity_names: Optional[Dict[str, str]] = None,
    ) -> int:
        """Batch MERGE 'contains' edges: DocumentChunk -> Entity.

        Each mapping dict has: chunk_id, entity_id.

        Uses lowercase 'contains' relationship type (Cognee convention).
        Uses APOC for dynamic relationship creation.
        All edges include standard Cognee edge properties.

        Args:
            mappings: List of dicts with chunk_id and entity_id
            database: Optional Neo4j database name
            entity_names: Optional dict mapping entity_id to entity name for edge_text

        Returns number of edges merged.
        """
        if not mappings:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE
        entity_names = entity_names or {}

        for i in range(0, len(mappings), batch_size):
            batch = mappings[i : i + batch_size]
            now_ms = int(time.time() * 1000)
            records = _sorted_records(
                [
                    {
                        "chunk_id": str(m["chunk_id"]),
                        "entity_id": str(m["entity_id"]),
                        "properties": {
                            "source_node_id": str(m["chunk_id"]),
                            "target_node_id": str(m["entity_id"]),
                            "relationship_name": "contains",
                            "edge_text": f"{m['chunk_id']} contains {entity_names.get(str(m['entity_id']), '')}".strip(),
                            "ontology_valid": False,
                            "updated_at": now_ms,
                        },
                    }
                    for m in batch
                ],
                "chunk_id",
                "entity_id",
            )

            counters = await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (from_node:`__Node__` {id: edge.chunk_id})
                MATCH (to_node:`__Node__` {id: edge.entity_id})
                CALL apoc.merge.relationship(
                    from_node,
                    'contains',
                    {source_node_id: edge.chunk_id, target_node_id: edge.entity_id},
                    edge.properties,
                    to_node
                ) YIELD rel
                RETURN rel
                """,
                {"edges": records},
                database=database,
            )
            total += counters["rows_affected"]

        logger.info(
            "writer.contains_edges_written",
            total=total,
        )
        return total

    async def write_is_a_edges(
        self,
        entities: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Batch MERGE 'is_a' edges: Entity -> EntityType.

        Uses entity_id and entity_type from the entity records.

        Uses lowercase 'is_a' relationship type (Cognee convention).
        Uses APOC for dynamic relationship creation.
        All edges include standard Cognee edge properties.

        Returns number of edges merged.
        """
        if not entities:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE

        for i in range(0, len(entities), batch_size):
            batch = entities[i : i + batch_size]
            now_ms = int(time.time() * 1000)
            records = _sorted_records(
                [
                    {
                        "entity_id": str(e["entity_id"]),
                        "entity_type": e.get("entity_type", ""),
                        "node_set": _build_node_set(e),
                        "type_id": _entity_id(e.get("entity_type", ""), _build_node_set(e)),
                        "properties": {
                            "source_node_id": str(e["entity_id"]),
                            "target_node_id": _entity_id(
                                e.get("entity_type", ""), _build_node_set(e)
                            ),
                            "relationship_name": "is_a",
                            "edge_text": f"{e.get('name', '')} is_a {e.get('entity_type', '')}".strip(),
                            "ontology_valid": False,
                            "updated_at": now_ms,
                        },
                    }
                    for e in batch
                    if e.get("entity_type")
                ],
                "entity_id",
                "type_id",
            )

            if not records:
                continue

            counters = await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (from_node:`__Node__` {id: edge.entity_id})
                MATCH (to_node:`__Node__` {id: edge.type_id})
                CALL apoc.merge.relationship(
                    from_node,
                    'is_a',
                    {source_node_id: edge.entity_id, target_node_id: edge.type_id},
                    edge.properties,
                    to_node
                ) YIELD rel
                RETURN rel
                """,
                {"edges": records},
                database=database,
            )
            total += counters["rows_affected"]

        logger.info(
            "writer.is_a_edges_written",
            total=total,
        )
        return total

    async def write_made_from_edges(
        self,
        mappings: List[Dict[str, str]],
        database: Optional[str] = None,
        entity_names: Optional[Dict[str, str]] = None,
    ) -> int:
        """Batch MERGE 'made_from' edges: Entity -> DocumentChunk.

        Reverse of 'contains' — shows which chunk an entity was extracted from.

        Each mapping dict has: chunk_id, entity_id.

        Uses lowercase 'made_from' relationship type (Cognee convention).
        Uses APOC for dynamic relationship creation.
        All edges include standard Cognee edge properties.

        Args:
            mappings: List of dicts with chunk_id and entity_id
            database: Optional Neo4j database name
            entity_names: Optional dict mapping entity_id to entity name for edge_text

        Returns number of edges merged.
        """
        if not mappings:
            return 0

        total = 0
        batch_size = self._config.NEO4J_BATCH_SIZE
        entity_names = entity_names or {}

        for i in range(0, len(mappings), batch_size):
            batch = mappings[i : i + batch_size]
            now_ms = int(time.time() * 1000)
            records = _sorted_records(
                [
                    {
                        "entity_id": str(m["entity_id"]),
                        "chunk_id": str(m["chunk_id"]),
                        "properties": {
                            "source_node_id": str(m["entity_id"]),
                            "target_node_id": str(m["chunk_id"]),
                            "relationship_name": "made_from",
                            "edge_text": f"{entity_names.get(str(m['entity_id']), '')} made_from {m['chunk_id']}".strip(),
                            "ontology_valid": False,
                            "updated_at": now_ms,
                        },
                    }
                    for m in batch
                ],
                "entity_id",
                "chunk_id",
            )

            counters = await self._execute_with_retry(
                """
                UNWIND $edges AS edge
                MATCH (from_node:`__Node__` {id: edge.entity_id})
                MATCH (to_node:`__Node__` {id: edge.chunk_id})
                CALL apoc.merge.relationship(
                    from_node,
                    'made_from',
                    {source_node_id: edge.entity_id, target_node_id: edge.chunk_id},
                    edge.properties,
                    to_node
                ) YIELD rel
                RETURN rel
                """,
                {"edges": records},
                database=database,
            )
            total += counters["rows_affected"]

        logger.info(
            "writer.made_from_edges_written",
            total=total,
        )
        return total

    # ── Delete Operations ────────────────────────────────────────────

    async def resolve_file_version_id(
        self,
        file_path: str,
        project_id: str,
        database: Optional[str] = None,
    ) -> Optional[str]:
        """Look up file_version_id from Neo4j by file_path + project_id.

        Used when a delete message arrives without file_version_id.
        Queries nodes to find the file_version_id associated with a file.

        Returns the file_version_id string or None if not found.
        """
        db = database or self._config.NEO4J_DATABASE
        try:
            async with self._driver.session(database=db) as session:
                result = await session.run(
                    """
                    MATCH (n:`__Node__`)
                    WHERE n.file_path = $file_path AND n.project_id = $project_id
                      AND n.file_version_id IS NOT NULL AND n.file_version_id <> ''
                    RETURN DISTINCT n.file_version_id AS fvid
                    LIMIT 1
                    """,
                    file_path=file_path,
                    project_id=project_id,
                )
                record = await result.single()
                if record:
                    return record["fvid"]
        except Exception as e:
            logger.error(
                "writer.resolve_file_version_id_failed",
                file_path=file_path,
                project_id=project_id,
                error=str(e),
            )
        return None

    async def delete_by_file_version_id(
        self,
        file_version_id: str,
        database: str,
    ) -> dict:
        """Delete all Neo4j data associated with a file_version_id.

        Cascade rules (from architecture decision):
        - Document, DocumentChunk, TextSummary: ALWAYS delete (1:1 with file)
        - Entity: conditional — delete only if no other file extracted the
          same entity (by name + entity_type). Otherwise the entity survives
          because another file still defines it.

        DETACH DELETE removes the node AND all its relationships.

        Args:
            file_version_id: The file version ID whose nodes to purge.
            database: Neo4j database name (e.g. ``cognee-<company_id>``).

        Returns:
            Dict with counts: documents_deleted, chunks_deleted,
            summaries_deleted, orphan_entities_deleted, surviving_entities.
        """
        logger.info(
            "writer.delete_start",
            file_version_id=file_version_id,
            database=database,
        )

        # ── Step A: Delete Document nodes ─────────────────────────────
        doc_counters = await self._execute_with_retry(
            """
            MATCH (n:`__Node__`)
            WHERE n.file_version_id = $fvid AND n.type = 'Document'
            DETACH DELETE n
            RETURN count(n) AS deleted_count
            """,
            {"fvid": file_version_id},
            database=database,
        )
        documents_deleted = doc_counters["nodes_deleted"]

        # ── Step B: Delete DocumentChunk nodes ────────────────────────
        chunk_counters = await self._execute_with_retry(
            """
            MATCH (n:`__Node__`)
            WHERE n.file_version_id = $fvid AND n.type = 'DocumentChunk'
            DETACH DELETE n
            RETURN count(n) AS deleted_count
            """,
            {"fvid": file_version_id},
            database=database,
        )
        chunks_deleted = chunk_counters["nodes_deleted"]

        # ── Step C: Delete TextSummary nodes ──────────────────────────
        summary_counters = await self._execute_with_retry(
            """
            MATCH (n:`__Node__`)
            WHERE n.file_version_id = $fvid AND n.type = 'TextSummary'
            DETACH DELETE n
            RETURN count(n) AS deleted_count
            """,
            {"fvid": file_version_id},
            database=database,
        )
        summaries_deleted = summary_counters["nodes_deleted"]

        # ── Step D: Conditional Entity deletion ───────────────────────
        # For each Entity with this file_version_id, check if the same
        # entity (by name + entity_type) exists from a DIFFERENT file.
        # If no other file defines it → orphaned → DETACH DELETE.
        # If another file defines it → entity survives (edges to deleted
        # chunks are already gone from Steps A-C via DETACH DELETE).
        orphan_counters = await self._execute_with_retry(
            """
            MATCH (e:`__Node__`)
            WHERE e.file_version_id = $fvid AND e.entity_type IS NOT NULL
            WITH e, e.name AS ename, e.entity_type AS etype
            OPTIONAL MATCH (other:`__Node__`)
            WHERE other.name = ename
              AND other.entity_type = etype
              AND other.file_version_id <> $fvid
            WITH e, other
            WHERE other IS NULL
            DETACH DELETE e
            RETURN count(e) AS orphan_entities_deleted
            """,
            {"fvid": file_version_id},
            database=database,
        )
        orphan_entities_deleted = orphan_counters["nodes_deleted"]

        # Count surviving entities (those NOT deleted because another file defines them)
        surviving_counters = await self._execute_with_retry(
            """
            MATCH (e:`__Node__`)
            WHERE e.file_version_id = $fvid AND e.entity_type IS NOT NULL
            RETURN count(e) AS surviving_count
            """,
            {"fvid": file_version_id},
            database=database,
        )
        surviving_entities = surviving_counters["rows_affected"]

        result = {
            "file_version_id": file_version_id,
            "documents_deleted": documents_deleted,
            "chunks_deleted": chunks_deleted,
            "summaries_deleted": summaries_deleted,
            "orphan_entities_deleted": orphan_entities_deleted,
            "surviving_entities": surviving_entities,
        }

        logger.info(
            "writer.delete_complete",
            **result,
        )

        return result

    # ── Internal: Execute with Retry ──────────────────────────────────

    async def _execute_with_retry(
        self,
        query: str,
        parameters: Dict[str, Any],
        database: Optional[str] = None,
    ) -> Dict[str, int]:
        """Execute a Cypher query with exponential backoff retry.

        Uses an auto-commit transaction via session.run().
        Neo4j transactions are atomic — on failure, nothing is committed.

        Returns:
            Dict with Neo4j summary counters (nodes_created, relationships_created, etc.)
            plus ``rows_affected`` — the number of result records returned by the query.

            ``rows_affected`` is critical for APOC-based writes (e.g.
            ``apoc.merge.relationship``) because APOC procedures do NOT increment
            Neo4j's native ``relationships_created`` counter.  The returned row
            count is the only reliable indicator of how many edges were actually
            created/merged.
        """
        db = database or self._config.NEO4J_DATABASE
        last_error: Optional[Exception] = None

        deadlock_max_retries = _env_int("NEO4J_MAX_RETRIES", 8)
        deadlock_base_delay = _env_int("NEO4J_DEADLOCK_BACKOFF_MS_BASE", 50) / 1000.0
        default_max_retries = self._config.MAX_RETRIES
        default_base_delay = self._config.RETRY_BASE_DELAY

        attempt = 1
        while True:
            try:
                async with self._driver.session(
                    database=db,
                ) as session:
                    result = await session.run(query, parameters)
                    # Collect all result records BEFORE consuming the summary.
                    # This is required to get an accurate row count — once
                    # consume() is called, unread records are discarded.
                    records = [record async for record in result]
                    summary = await result.consume()
                    counters = summary.counters
                    return {
                        "nodes_created": counters.nodes_created,
                        "nodes_deleted": counters.nodes_deleted,
                        "relationships_created": counters.relationships_created,
                        "relationships_deleted": counters.relationships_deleted,
                        "properties_set": counters.properties_set,
                        "rows_affected": len(records),
                    }
            except Exception as e:
                last_error = e
                err_str = str(e)
                is_deadlock = "DeadlockDetected" in err_str
                max_retries = deadlock_max_retries if is_deadlock else default_max_retries
                base_delay = deadlock_base_delay if is_deadlock else default_base_delay
                logger.warning(
                    "writer.query_retry",
                    attempt=attempt,
                    max_retries=max_retries,
                    deadlock=is_deadlock,
                    error=err_str,
                )

                # If the target database was dropped out from under us
                # (e.g. by clean_cogni_data.sh), evict the in-process
                # "already exists" cache and recreate it before retrying.
                if "DatabaseNotFound" in err_str or "Graph not found" in err_str:
                    invalidate_database_cache(db)
                    m = re.match(r"^cognee-([a-zA-Z0-9_-]+)$", db)
                    if m:
                        try:
                            await ensure_neo4j_database(m.group(1), force=True)
                            logger.info("writer.database_recreated", database=db)
                        except Exception as recreate_err:  # pragma: no cover
                            logger.error(
                                "writer.database_recreate_failed",
                                database=db,
                                error=str(recreate_err),
                            )

                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)
                    attempt += 1
                    continue

                break

        raise RuntimeError(f"Neo4j query failed after {max_retries} retries: {last_error}")

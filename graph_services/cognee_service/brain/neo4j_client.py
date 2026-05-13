"""
Brain v2 — Direct Neo4j fallback client for graph node cleanup.

Cognee's delete_data() cleans Qdrant + relational store but has a known bug
where Neo4j graph nodes are not removed. This client connects directly to Neo4j
and deletes graph nodes by source_node_set + entity context.

Environment variables:
  NEO4J_URI      — bolt URI, e.g. bolt://kgrag-neo4j:7687
  NEO4J_USERNAME — defaults to "neo4j"
  NEO4J_PASSWORD — required (also accepted via NEO4J_AUTH as "neo4j/password")
"""

import os
from typing import Optional

import structlog
from neo4j import AsyncGraphDatabase

logger = structlog.get_logger(__name__)


def _get_neo4j_password() -> str:
    """Resolve Neo4j password from environment.

    Checks NEO4J_PASSWORD first; falls back to parsing NEO4J_AUTH
    which has the format "neo4j/password123".
    """
    password = os.environ.get("NEO4J_PASSWORD", "")
    if password:
        return password
    auth_str = os.environ.get("NEO4J_AUTH", "")
    if auth_str and "/" in auth_str:
        return auth_str.split("/", 1)[1]
    return ""


class BrainNeo4jClient:
    """Direct Neo4j client for cleaning up Cognee graph nodes after delete.

    Scoped to a company-specific Neo4j database (cognee-{company_id}).
    All errors are logged but not re-raised — failures here are non-fatal
    since Cognee already cleaned Qdrant + relational stores.

    Usage::

        client = BrainNeo4jClient()
        await client.delete_entity_graph(
            company_id="0677750d-...",
            entity_type="expertise",
            entity_id="7a23a05b-...",
            project_id="proj-uuid",
        )
        await client.close()
    """

    def __init__(self) -> None:
        uri = os.environ.get("NEO4J_URI", "bolt://kgrag-neo4j:7687")
        username = os.environ.get("NEO4J_USERNAME", "neo4j")
        password = _get_neo4j_password()
        self._driver = AsyncGraphDatabase.driver(uri, auth=(username, password))
        logger.debug("brain_neo4j_client.initialised", uri=uri)

    @staticmethod
    def _db_name(company_id: str) -> str:
        """Return the company-scoped Neo4j database name."""
        return f"cognee-{company_id}"

    @staticmethod
    def _node_set_prefix(event_entity_type: str, scope_id: str) -> str:
        """Return the source_node_set prefix used when data was ingested.

        Matches BrainEventProcessor.build_node_set: "{dataset_name}_{scope_id}".
        """
        # Map entity_type → dataset_name (mirrors _ENTITY_TYPE_TO_DATASET)
        _ENTITY_TYPE_TO_DATASET: dict[str, str] = {
            "knowledge": "knowledge",
            "expertise": "expertise",
            "expertise_chunk": "expertise",
            "lesson": "lesson",
            "entry": "entry",
            "engagement": "engagement",
        }
        dataset_name = _ENTITY_TYPE_TO_DATASET.get(event_entity_type, "brain")
        return f"{dataset_name}_{scope_id}"

    async def delete_entity_graph(
        self,
        company_id: str,
        entity_type: str,
        entity_id: str,
        project_id: Optional[str],
    ) -> None:
        """Delete Neo4j graph nodes for the given entity.

        Finds TextDocument nodes where source_node_set matches the entity's
        dataset scope and name contains the entity_id, then traverses 1-3 hops
        to find related DocumentChunk, TextSummary, and Entity nodes, deleting
        everything in one DETACH DELETE.

        Args:
            company_id: Company UUID — determines which Neo4j database to use.
            entity_type: Entity type string (e.g. "knowledge", "expertise").
            entity_id: The entity UUID used as the DataItem label on ingest.
            project_id: Project UUID for source_node_set scoping; falls back
                        to company_id when None.
        """
        scope_id = project_id or company_id
        node_set_prefix = self._node_set_prefix(entity_type, scope_id)
        db_name = self._db_name(company_id)

        logger.info(
            "brain_neo4j_client.delete.start",
            company_id=company_id,
            entity_type=entity_type,
            entity_id=entity_id,
            db_name=db_name,
            node_set_prefix=node_set_prefix,
        )

        # Cypher: find the TextDocument by source_node_set + entity_id in name,
        # traverse related nodes that share the same source_node_set context,
        # then DETACH DELETE doc + all related nodes.
        cypher = """
            MATCH (doc:TextDocument)
            WHERE doc.source_node_set CONTAINS $node_set_prefix
              AND doc.name CONTAINS $entity_id
            OPTIONAL MATCH (doc)-[*1..3]-(related)
            WHERE related.source_node_set CONTAINS $node_set_prefix
            DETACH DELETE doc, related
        """

        try:
            async with self._driver.session(database=db_name) as session:
                result = await session.run(
                    cypher,
                    node_set_prefix=node_set_prefix,
                    entity_id=entity_id,
                )
                summary = await result.consume()
                deleted_nodes = summary.counters.nodes_deleted
                deleted_rels = summary.counters.relationships_deleted
                logger.info(
                    "brain_neo4j_client.delete.done",
                    company_id=company_id,
                    entity_id=entity_id,
                    nodes_deleted=deleted_nodes,
                    relationships_deleted=deleted_rels,
                )
        except Exception as exc:
            logger.error(
                "brain_neo4j_client.delete.failed",
                company_id=company_id,
                entity_type=entity_type,
                entity_id=entity_id,
                db_name=db_name,
                error=str(exc),
            )
            # Non-fatal: Cognee already cleaned Qdrant + relational stores.
            # Log the failure and let the consumer continue.

    async def close(self) -> None:
        """Close the underlying Neo4j driver."""
        try:
            await self._driver.close()
            logger.debug("brain_neo4j_client.closed")
        except Exception as exc:
            logger.error("brain_neo4j_client.close_failed", error=str(exc))

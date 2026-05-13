"""Neo4j hierarchy write logic for Phase 4.1 Graph Model Overhaul.

Handles:
- Company → Project → Branch hierarchy nodes
- BusinessDomain nodes (company-level, connected to Company)
- TechnicalDomain nodes (project-level, connected to Project)
- CodeBlock nodes (entity code text, connected to Entity and Branch)
- Hierarchy edges: has_project, has_branch, has_business_domain,
  has_technical_domain, has_code, exists_on, belongs_to_domain,
  belongs_to_technical_domain

All writes use UNWIND MERGE pattern for batch efficiency.
All nodes use __Node__ base label + APOC dynamic labels (Cognee-compatible).
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

import structlog
from neo4j import AsyncDriver

from .config import Neo4jStorageConfig
from .writer import _build_node_set

logger = structlog.get_logger(__name__)


def _hierarchy_id(text: str) -> str:
    """Generate UUID5 for hierarchy nodes."""
    return str(uuid.uuid5(uuid.NAMESPACE_OID, text.lower().replace(" ", "_")))


def _code_node_set(project_id: str, project_name: str = "") -> str:
    return _build_node_set(
        {
            "content_type": "code",
            "project_id": project_id,
            "project_name": project_name or project_id,
        }
    )


def _knowledge_node_set(company_id: str) -> str:
    return _build_node_set({"content_type": "document", "company_id": company_id})


class Neo4jHierarchyWriter:
    """Writes Company/Project/Branch/Domain/CodeBlock hierarchy to Neo4j.

    All methods are async and use UNWIND MERGE for batch efficiency.
    Designed to be composed into Neo4jBatchWriter.
    """

    def __init__(
        self,
        driver: AsyncDriver,
        config: type[Neo4jStorageConfig] = Neo4jStorageConfig,
    ) -> None:
        self._driver = driver
        self._config = config

    async def _execute_with_retry(
        self,
        query: str,
        params: Dict[str, Any],
        database: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute Cypher with retry on transient failures."""
        import asyncio

        max_retries = self._config.MAX_RETRIES
        backoff = self._config.RETRY_BASE_DELAY

        for attempt in range(max_retries):
            try:
                async with self._driver.session(
                    database=database or self._config.NEO4J_DATABASE
                ) as session:
                    result = await session.run(query, params)
                    summary = await result.consume()
                    counters = summary.counters
                    return {
                        "nodes_created": counters.nodes_created,
                        "relationships_created": counters.relationships_created,
                        "rows_affected": max(
                            counters.nodes_created,
                            counters.relationships_created,
                        ),
                    }
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(
                        "hierarchy_writer.query_failed",
                        attempt=attempt,
                        error=str(e),
                        query=query[:100],
                    )
                    raise
                await asyncio.sleep(backoff * (2**attempt))

        return {"nodes_created": 0, "relationships_created": 0, "rows_affected": 0}

    # ── Company Nodes ─────────────────────────────────────────────

    async def write_company_node(
        self,
        company_id: str,
        company_name: str = "",
        database: Optional[str] = None,
    ) -> int:
        """MERGE a Company node.

        Args:
            company_id: Company UUID.
            company_name: Human-readable name (optional).
            database: Neo4j database name.

        Returns:
            Number of nodes created (0 if already existed).
        """
        node_id = _hierarchy_id(f"company:{company_id}")
        now_ms = int(time.time() * 1000)

        counters = await self._execute_with_retry(
            """
            MERGE (n:__Node__ {id: $node_id})
            ON CREATE SET
                n.company_id = $company_id,
                n.name = $company_name,
                n.created_at = $now,
                n.updated_at = $now
            ON MATCH SET
                n.updated_at = $now
            WITH n
            CALL apoc.create.addLabels(n, ['Company']) YIELD node
            RETURN node
            """,
            {
                "node_id": node_id,
                "company_id": company_id,
                "company_name": company_name,
                "now": now_ms,
            },
            database=database,
        )

        logger.debug(
            "hierarchy_writer.company_node",
            company_id=company_id,
            created=counters["nodes_created"],
        )
        return counters["nodes_created"]

    # ── Project Nodes ─────────────────────────────────────────────

    async def write_project_node(
        self,
        project_id: str,
        company_id: str,
        project_name: str = "",
        language: str = "",
        framework: str = "",
        database: Optional[str] = None,
    ) -> int:
        """MERGE a Project node and link it to its Company.

        Creates Company -[:has_project]-> Project edge.

        Returns:
            Number of nodes created.
        """
        node_id = _hierarchy_id(f"project:{project_id}")
        company_node_id = _hierarchy_id(f"company:{company_id}")
        now_ms = int(time.time() * 1000)

        counters = await self._execute_with_retry(
            """
            MERGE (proj:__Node__ {id: $node_id})
            ON CREATE SET
                proj.project_id = $project_id,
                proj.name = $project_name,
                proj.language = $language,
                proj.framework = $framework,
                proj.created_at = $now,
                proj.updated_at = $now
            ON MATCH SET
                proj.updated_at = $now
            WITH proj
            CALL apoc.create.addLabels(proj, ['Project']) YIELD node AS proj2
            WITH proj2
            MATCH (co:__Node__ {id: $company_node_id})
            CALL apoc.merge.relationship(
                co, 'has_project',
                {source_node_id: $company_node_id, target_node_id: $node_id},
                {updated_at: $now},
                proj2
            ) YIELD rel
            RETURN proj2
            """,
            {
                "node_id": node_id,
                "project_id": project_id,
                "project_name": project_name,
                "language": language,
                "framework": framework,
                "company_node_id": company_node_id,
                "now": now_ms,
            },
            database=database,
        )

        logger.debug(
            "hierarchy_writer.project_node",
            project_id=project_id,
            company_id=company_id,
            created=counters["nodes_created"],
        )
        return counters["nodes_created"]

    # ── Branch Nodes ──────────────────────────────────────────────

    async def write_branch_node(
        self,
        project_id: str,
        branch_name: str,
        project_name: str = "",
        database: Optional[str] = None,
    ) -> int:
        """MERGE a Branch node and link it to its Project.

        Creates Project -[:has_branch]-> Branch edge.

        Returns:
            Number of nodes created.
        """
        node_set = _code_node_set(project_id, project_name)
        branch_id = _hierarchy_id(f"branch:{project_id}:{branch_name}:{node_set}")
        project_node_id = _hierarchy_id(f"project:{project_id}")
        now_ms = int(time.time() * 1000)

        counters = await self._execute_with_retry(
            """
            MERGE (br:__Node__ {id: $branch_id})
            ON CREATE SET
                br.name = $branch_name,
                br.project_id = $project_id,
                br.node_set = $node_set,
                br.created_at = $now,
                br.updated_at = $now
            ON MATCH SET
                br.node_set = $node_set,
                br.updated_at = $now
            WITH br
            CALL apoc.create.addLabels(br, ['Branch']) YIELD node AS br2
            WITH br2
            MATCH (proj:__Node__ {id: $project_node_id})
            CALL apoc.merge.relationship(
                proj, 'has_branch',
                {source_node_id: $project_node_id, target_node_id: $branch_id},
                {updated_at: $now},
                br2
            ) YIELD rel
            RETURN br2
            """,
            {
                "branch_id": branch_id,
                "branch_name": branch_name,
                "project_id": project_id,
                "node_set": node_set,
                "project_node_id": project_node_id,
                "now": now_ms,
            },
            database=database,
        )

        logger.debug(
            "hierarchy_writer.branch_node",
            project_id=project_id,
            branch_name=branch_name,
            created=counters["nodes_created"],
        )
        return counters["nodes_created"]

    # ── Full Hierarchy (batch) ─────────────────────────────────────

    async def write_hierarchy(
        self,
        company_id: str,
        project_id: str,
        branch_name: str,
        project_name: str = "",
        company_name: str = "",
        language: str = "",
        framework: str = "",
        database: Optional[str] = None,
    ) -> int:
        """Write full Company → Project → Branch hierarchy in one pass.

        MERGE all three nodes and their edges atomically.
        Idempotent — safe to call for every batch.

        Returns:
            Total nodes created.
        """
        company_node_id = _hierarchy_id(f"company:{company_id}")
        project_node_id = _hierarchy_id(f"project:{project_id}")
        node_set = _code_node_set(project_id, project_name)
        branch_id = _hierarchy_id(f"branch:{project_id}:{branch_name}:{node_set}")
        now_ms = int(time.time() * 1000)

        counters = await self._execute_with_retry(
            """
            // Company
            MERGE (co:__Node__ {id: $company_node_id})
            ON CREATE SET co.company_id = $company_id, co.name = $company_name,
                          co.created_at = $now, co.updated_at = $now
            ON MATCH SET co.updated_at = $now

            // Project
            MERGE (proj:__Node__ {id: $project_node_id})
            ON CREATE SET proj.project_id = $project_id, proj.name = $project_name,
                          proj.language = $language, proj.framework = $framework,
                          proj.created_at = $now, proj.updated_at = $now
            ON MATCH SET proj.updated_at = $now

            // Branch
            MERGE (br:__Node__ {id: $branch_id})
            ON CREATE SET br.name = $branch_name, br.project_id = $project_id,
                          br.node_set = $node_set, br.created_at = $now, br.updated_at = $now
            ON MATCH SET br.node_set = $node_set, br.updated_at = $now

            WITH co, proj, br
            CALL apoc.create.addLabels(co, ['Company']) YIELD node AS co2
            CALL apoc.create.addLabels(proj, ['Project']) YIELD node AS proj2
            CALL apoc.create.addLabels(br, ['Branch']) YIELD node AS br2

            // Company → Project edge
            CALL apoc.merge.relationship(
                co2, 'has_project',
                {source_node_id: $company_node_id, target_node_id: $project_node_id},
                {updated_at: $now},
                proj2
            ) YIELD rel AS r1

            // Project → Branch edge
            CALL apoc.merge.relationship(
                proj2, 'has_branch',
                {source_node_id: $project_node_id, target_node_id: $branch_id},
                {updated_at: $now},
                br2
            ) YIELD rel AS r2

            RETURN co2, proj2, br2
            """,
            {
                "company_node_id": company_node_id,
                "company_id": company_id,
                "company_name": company_name,
                "project_node_id": project_node_id,
                "project_id": project_id,
                "project_name": project_name,
                "language": language,
                "framework": framework,
                "branch_id": branch_id,
                "branch_name": branch_name,
                "node_set": node_set,
                "now": now_ms,
            },
            database=database,
        )

        logger.info(
            "hierarchy_writer.hierarchy_written",
            company_id=company_id,
            project_id=project_id,
            branch=branch_name,
            nodes_created=counters["nodes_created"],
        )
        return counters["nodes_created"]

    # ── Entity → Branch (exists_on) ───────────────────────────────

    async def write_entity_branch_edges(
        self,
        entity_branch_mappings: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Write Entity -[:exists_on]-> Branch edges.

        Args:
            entity_branch_mappings: List of {entity_id, project_id, branch_name}.

        Returns:
            Number of edges created.
        """
        if not entity_branch_mappings:
            return 0

        records = [
            {
                "entity_id": m["entity_id"],
                "branch_id": _hierarchy_id(
                    f"branch:{m['project_id']}:{m['branch_name']}:{_code_node_set(m['project_id'], m.get('project_name') or m['project_id'])}"
                ),
            }
            for m in entity_branch_mappings
        ]
        now_ms = int(time.time() * 1000)

        counters = await self._execute_with_retry(
            """
            UNWIND $edges AS edge
            MATCH (entity:__Node__ {id: edge.entity_id})
            MATCH (branch:__Node__ {id: edge.branch_id})
            CALL apoc.merge.relationship(
                entity, 'exists_on',
                {source_node_id: edge.entity_id, target_node_id: edge.branch_id},
                {updated_at: $now},
                branch
            ) YIELD rel
            RETURN rel
            """,
            {"edges": records, "now": now_ms},
            database=database,
        )

        logger.debug(
            "hierarchy_writer.entity_branch_edges",
            count=counters["relationships_created"],
        )
        return counters["relationships_created"]

    # ── Business Domain Nodes ─────────────────────────────────────

    async def write_business_domain_nodes(
        self,
        domains: List[Dict[str, Any]],
        company_id: str,
        database: Optional[str] = None,
    ) -> int:
        """MERGE BusinessDomain nodes and link to Company.

        Each domain dict must have: canonical_name, normalised_key.
        Optional: description.

        Creates Company -[:has_business_domain]-> BusinessDomain edges.

        Returns:
            Number of nodes created.
        """
        if not domains:
            return 0

        company_node_id = _hierarchy_id(f"company:{company_id}")
        now_ms = int(time.time() * 1000)

        records = [
            {
                "domain_id": _hierarchy_id(
                    f"business_domain:{company_id}:{d.get('normalised_key', d.get('key', d.get('canonical_name', '')).lower())}:{_knowledge_node_set(company_id)}"
                ),
                "canonical_name": d.get("canonical_name", d.get("name", "")),
                "normalised_key": d.get(
                    "normalised_key", d.get("key", d.get("canonical_name", "").lower())
                ),
                "description": d.get("description", ""),
            }
            for d in domains
            if d.get("canonical_name") or d.get("name")
        ]

        if not records:
            return 0

        counters = await self._execute_with_retry(
            """
            UNWIND $domains AS dom
            MERGE (bd:__Node__ {id: dom.domain_id})
            ON CREATE SET
                bd.canonical_name = dom.canonical_name,
                bd.normalised_key = dom.normalised_key,
                bd.description = dom.description,
                bd.node_set = $node_set,
                bd.created_at = $now,
                bd.updated_at = $now
            ON MATCH SET
                bd.node_set = $node_set,
                bd.updated_at = $now
            WITH bd, dom
            CALL apoc.create.addLabels(bd, ['BusinessDomain']) YIELD node AS bd2
            WITH bd2, dom
            MATCH (co:__Node__ {id: $company_node_id})
            CALL apoc.merge.relationship(
                co, 'has_business_domain',
                {source_node_id: $company_node_id, target_node_id: dom.domain_id},
                {updated_at: $now},
                bd2
            ) YIELD rel
            RETURN bd2
            """,
            {
                "domains": records,
                "company_node_id": company_node_id,
                "node_set": _knowledge_node_set(company_id),
                "now": now_ms,
            },
            database=database,
        )

        logger.debug(
            "hierarchy_writer.business_domains",
            count=len(records),
            created=counters["nodes_created"],
        )
        return counters["nodes_created"]

    async def write_entity_domain_edges(
        self,
        entity_domain_mappings: List[Dict[str, Any]],
        company_id: str,
        database: Optional[str] = None,
    ) -> int:
        """Write Entity -[:belongs_to_domain]-> BusinessDomain edges.

        Args:
            entity_domain_mappings: List of {entity_id, domain_key}.

        Returns:
            Number of edges created.
        """
        if not entity_domain_mappings:
            return 0

        records = [
            {
                "entity_id": m["entity_id"],
                "domain_id": _hierarchy_id(
                    f"business_domain:{company_id}:{m['domain_key']}:{_knowledge_node_set(company_id)}"
                ),
            }
            for m in entity_domain_mappings
            if m.get("domain_key")
        ]

        if not records:
            return 0

        now_ms = int(time.time() * 1000)

        counters = await self._execute_with_retry(
            """
            UNWIND $edges AS edge
            MATCH (entity:__Node__ {id: edge.entity_id})
            MATCH (domain:__Node__ {id: edge.domain_id})
            CALL apoc.merge.relationship(
                entity, 'belongs_to_domain',
                {source_node_id: edge.entity_id, target_node_id: edge.domain_id},
                {updated_at: $now},
                domain
            ) YIELD rel
            RETURN rel
            """,
            {"edges": records, "now": now_ms},
            database=database,
        )

        return counters["relationships_created"]

    # ── Technical Domain Nodes ────────────────────────────────────

    async def write_technical_domain_nodes(
        self,
        technical_tags: List[str],
        project_id: str,
        project_name: str = "",
        database: Optional[str] = None,
    ) -> int:
        """MERGE TechnicalDomain nodes from technical_tags and link to Project.

        Args:
            technical_tags: List of tag strings from project analysis.
            project_id: Project UUID.

        Returns:
            Number of nodes created.
        """
        if not technical_tags:
            return 0

        project_node_id = _hierarchy_id(f"project:{project_id}")
        node_set = _code_node_set(project_id, project_name)
        now_ms = int(time.time() * 1000)

        records = [
            {
                "domain_id": _hierarchy_id(
                    f"technical_domain:{project_id}:{tag.lower()}:{node_set}"
                ),
                "name": tag,
            }
            for tag in technical_tags
            if tag
        ]

        if not records:
            return 0

        counters = await self._execute_with_retry(
            """
            UNWIND $domains AS dom
            MERGE (td:__Node__ {id: dom.domain_id})
            ON CREATE SET
                td.name = dom.name,
                td.project_id = $project_id,
                td.node_set = $node_set,
                td.created_at = $now,
                td.updated_at = $now
            ON MATCH SET
                td.node_set = $node_set,
                td.updated_at = $now
            WITH td, dom
            CALL apoc.create.addLabels(td, ['TechnicalDomain']) YIELD node AS td2
            WITH td2, dom
            MATCH (proj:__Node__ {id: $project_node_id})
            CALL apoc.merge.relationship(
                proj, 'has_technical_domain',
                {source_node_id: $project_node_id, target_node_id: dom.domain_id},
                {updated_at: $now},
                td2
            ) YIELD rel
            RETURN td2
            """,
            {
                "domains": records,
                "project_id": project_id,
                "node_set": node_set,
                "project_node_id": project_node_id,
                "now": now_ms,
            },
            database=database,
        )

        logger.debug(
            "hierarchy_writer.technical_domains",
            project_id=project_id,
            count=len(records),
            created=counters["nodes_created"],
        )
        return counters["nodes_created"]

    # ── CodeBlock Nodes ───────────────────────────────────────────

    async def write_code_block_nodes(
        self,
        code_blocks: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """MERGE CodeBlock nodes and link to Entity and Branch.

        Each code_block dict must have: entity_id, text, file_path,
        file_version_id, project_id, branch.
        Optional: start_line, end_line, language.

        Creates:
        - Entity -[:has_code]-> CodeBlock
        - Branch -[:has_code_block]-> CodeBlock

        Returns:
            Number of nodes created.
        """
        if not code_blocks:
            return 0

        batch_size = self._config.NEO4J_BATCH_SIZE
        total_created = 0
        now_ms = int(time.time() * 1000)

        for i in range(0, len(code_blocks), batch_size):
            batch = code_blocks[i : i + batch_size]

            records = [
                {
                    "code_block_id": _hierarchy_id(
                        f"code_block:{cb['entity_id']}:{cb['file_version_id']}"
                    ),
                    "entity_id": cb["entity_id"],
                    "text": cb.get("text", ""),
                    "start_line": cb.get("start_line", 0),
                    "end_line": cb.get("end_line", 0),
                    "file_path": cb.get("file_path", ""),
                    "language": cb.get("language", ""),
                    "file_version_id": cb.get("file_version_id", ""),
                    # Content-type-aware scope. For knowledge events (content_type='document')
                    # _build_node_set returns "{company_id}_knowledge"; for code events it returns
                    # "{project_id}_{project_name}_code". Falls back gracefully when project_id is None.
                    "node_set": cb.get("node_set") or _build_node_set(cb),
                    "source_node_set": cb.get("source_node_set")
                    or cb.get("node_set")
                    or _build_node_set(cb),
                    "branch_id": _hierarchy_id(
                        f"branch:{cb.get('project_id') or cb.get('company_id', '')}:{cb.get('branch', '')}:{cb.get('source_node_set') or cb.get('node_set') or _build_node_set(cb)}"
                    ),
                }
                for cb in batch
                if cb.get("entity_id") and cb.get("file_version_id")
            ]

            if not records:
                continue

            counters = await self._execute_with_retry(
                """
                UNWIND $blocks AS cb
                MERGE (block:__Node__ {id: cb.code_block_id})
                ON CREATE SET
                    block.text = cb.text,
                    block.start_line = cb.start_line,
                    block.end_line = cb.end_line,
                    block.file_path = cb.file_path,
                    block.language = cb.language,
                    block.file_version_id = cb.file_version_id,
                    block.node_set = cb.node_set,
                    block.source_node_set = cb.source_node_set,
                    block.created_at = $now,
                    block.updated_at = $now
                ON MATCH SET
                    block.node_set = cb.node_set,
                    block.source_node_set = cb.source_node_set,
                    block.updated_at = $now

                WITH block, cb
                CALL apoc.create.addLabels(block, ['CodeBlock']) YIELD node AS block2

                // Entity -[:has_code]-> CodeBlock
                WITH block2, cb
                MATCH (entity:__Node__ {id: cb.entity_id})
                CALL apoc.merge.relationship(
                    entity, 'has_code',
                    {source_node_id: cb.entity_id, target_node_id: cb.code_block_id},
                    {updated_at: $now},
                    block2
                ) YIELD rel AS r1

                // Branch -[:has_code_block]-> CodeBlock
                WITH block2, cb
                MATCH (branch:__Node__ {id: cb.branch_id})
                CALL apoc.merge.relationship(
                    branch, 'has_code_block',
                    {source_node_id: cb.branch_id, target_node_id: cb.code_block_id},
                    {updated_at: $now},
                    block2
                ) YIELD rel AS r2

                RETURN block2
                """,
                {"blocks": records, "now": now_ms},
                database=database,
            )

            total_created += counters["nodes_created"]

        logger.info(
            "hierarchy_writer.code_block_nodes",
            total_created=total_created,
            total_processed=len(code_blocks),
        )
        return total_created

    async def backfill_code_block_source_node_set(
        self,
        code_blocks: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Backfill legacy CodeBlock nodes with the mirrored source_node_set."""
        records = []
        for cb in code_blocks:
            entity_id = cb.get("entity_id")
            file_version_id = cb.get("file_version_id")
            if not entity_id or not file_version_id:
                continue
            records.append(
                {
                    "code_block_id": _hierarchy_id(f"code_block:{entity_id}:{file_version_id}"),
                    "source_node_set": cb.get("source_node_set")
                    or cb.get("node_set")
                    or _build_node_set(cb),
                }
            )

        if not records:
            return 0

        counters = await self._execute_with_retry(
            """
            UNWIND $blocks AS cb
            MATCH (block:__Node__ {id: cb.code_block_id})
            SET block.source_node_set = cb.source_node_set,
                block.updated_at = $now
            RETURN count(block) AS updated
            """,
            {"blocks": records, "now": int(time.time() * 1000)},
            database=database,
        )
        logger.info(
            "hierarchy_writer.code_block_source_node_set_backfill",
            updated=counters.get("nodes_created", 0),
            total_processed=len(records),
        )
        return counters.get("nodes_created", 0)

    # ── TextSummary -[:has_summary]-> Entity ──────────────────────

    async def write_summary_entity_edges(
        self,
        summary_entity_mappings: List[Dict[str, Any]],
        database: Optional[str] = None,
    ) -> int:
        """Write TextSummary -[:has_summary]-> Entity edges.

        Links each TextSummary to the Entity it summarizes, enabling
        'show me all summaries for entity X' queries.

        Args:
            summary_entity_mappings: List of {summary_id, entity_id}.

        Returns:
            Number of edges created.
        """
        if not summary_entity_mappings:
            return 0

        records = [
            {
                "summary_id": m["summary_id"],
                "entity_id": m["entity_id"],
            }
            for m in summary_entity_mappings
            if m.get("summary_id") and m.get("entity_id")
        ]

        if not records:
            return 0

        now_ms = int(time.time() * 1000)

        counters = await self._execute_with_retry(
            """
            UNWIND $edges AS edge
            MATCH (summary:__Node__ {id: edge.summary_id})
            MATCH (entity:__Node__ {id: edge.entity_id})
            CALL apoc.merge.relationship(
                summary, 'has_summary',
                {source_node_id: edge.summary_id, target_node_id: edge.entity_id},
                {updated_at: $now},
                entity
            ) YIELD rel
            RETURN rel
            """,
            {"edges": records, "now": now_ms},
            database=database,
        )

        logger.debug(
            "hierarchy_writer.summary_entity_edges",
            count=counters["relationships_created"],
        )
        return counters["relationships_created"]

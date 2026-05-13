"""
Neo4j Graph Repository
Handles graph database operations for organizational hierarchy.

Multi-tenant architecture: Single 'kgrag' database with company_id/project_id properties.
"""

from typing import Dict, Any, Optional, List
from neo4j import AsyncGraphDatabase, AsyncDriver, NotificationMinimumSeverity
import os
import json
from loguru import logger
from kgrag.config import config


class Neo4jRepository:
    """Repository for Neo4j graph operations."""

    def __init__(self):
        """Initialize Neo4j connection."""
        self.uri = config.NEO4J_URI
        self.user = config.NEO4J_USER
        self.password = config.NEO4J_PASSWORD
        self._database = config.NEO4J_DATABASE  # Immutable - single 'kgrag' database
        self.driver: Optional[AsyncDriver] = None

    @property
    def database(self) -> str:
        """Database name (immutable - always returns 'kgrag')."""
        return self._database

    async def connect(self):
        """Establish Neo4j connection."""
        if not self.driver:
            self.driver = AsyncGraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                notifications_min_severity=NotificationMinimumSeverity.OFF,
            )
            logger.info(f"Connected to Neo4j at {self.uri}")

    async def close(self):
        """Close Neo4j connection."""
        if self.driver:
            await self.driver.close()
            self.driver = None
            logger.info("Closed Neo4j connection")

    async def create_company_node(
        self, company_id: str, name: str, description: Optional[str] = None, is_active: bool = True
    ) -> None:
        """
        Create Company node in Neo4j.

        Args:
            company_id: Company UUID
            name: Company name
            description: Company description
            is_active: Whether company is active
        """
        await self.connect()

        query = """
        MERGE (c:Company {id: $company_id})
        SET c.name = $name,
            c.description = $description,
            c.is_active = $is_active,
            c.company_id = $company_id,
            c.updated_at = datetime()
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(
                query,
                company_id=company_id,
                name=name,
                description=description,
                is_active=is_active,
            )

        logger.info(f"Created/updated Company node: {company_id}")

    async def create_project_node(
        self, project_id: str, company_id: str, name: str, description: Optional[str] = None
    ) -> None:
        """
        Create Project node and link to Company.

        Args:
            project_id: Project UUID
            company_id: Company UUID
            name: Project name
            description: Project description

        Raises:
            Exception: If Company node not found or project creation fails
        """
        await self.connect()

        # Self-healing: MERGE the Company node so we don't require a separate
        # mirror step from the auth service. Companies live in Postgres (kgrag_auth)
        # and there is no out-of-band sync. Using MERGE here is idempotent and
        # mirrors the pattern used by create_agent_node (line ~600).
        query = """
        MERGE (c:Company {id: $company_id})
        ON CREATE SET c.company_id = $company_id,
                      c.created_at = datetime()
        SET c.company_id = $company_id,
            c.updated_at = datetime()
        MERGE (p:Project {id: $project_id})
        SET p.name = $name,
            p.description = $description,
            p.company_id = $company_id,
            p.project_id = $project_id,
            p.updated_at = datetime()
        MERGE (c)-[:HAS_PROJECT]->(p)
        RETURN p.id AS project_id
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(
                query,
                project_id=project_id,
                company_id=company_id,
                name=name,
                description=description,
            )

            record = await result.single()
            if not record:
                raise Exception(
                    f"Failed to create Project node {project_id} in database '{self.database}'"
                )

        logger.info(
            f"Created/updated Project node: {project_id} (Company {company_id} merged if missing)"
        )

    async def create_repository_node(
        self,
        repository_id: str,
        project_id: str,
        name: str,
        url: Optional[str] = None,
        company_id: Optional[str] = None,
    ) -> None:
        """
        Create Repository node and link to Project.

        Args:
            repository_id: Repository UUID
            project_id: Project UUID
            name: Repository name
            url: Repository URL
            company_id: Company UUID (for multi-tenant scoping)
        """
        await self.connect()

        query = """
        MATCH (p:Project {id: $project_id, project_id: $project_id})
        MERGE (r:Repository {id: $repository_id})
        SET r.name = $name,
            r.url = $url,
            r.project_id = $project_id,
            r.company_id = $company_id,
            r.updated_at = datetime()
        MERGE (p)-[:HAS_REPOSITORY]->(r)
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(
                query,
                repository_id=repository_id,
                project_id=project_id,
                name=name,
                url=url,
                company_id=company_id,
            )

        logger.info(f"Created/updated Repository node: {repository_id}")

    async def transfer_project_scope(
        self,
        project_id: str,
        source_company_id: str,
        target_company_id: str,
    ) -> Dict[str, int]:
        """
        Move a project's graph scope from one company to another.

        Updates company_id for all nodes with matching project_id and
        rewires Company-[:HAS_PROJECT]->Project ownership edge.

        Args:
            project_id: Project UUID
            source_company_id: Source company UUID
            target_company_id: Target company UUID

        Returns:
            Dict with migration counters
        """
        await self.connect()

        if source_company_id == target_company_id:
            return {
                "project_nodes_total": 0,
                "project_nodes_updated": 0,
                "ownership_edges_rewired": 0,
            }

        async with self.driver.session(database=self.database) as session:
            # Count nodes for observability
            count_query = """
            MATCH (n)
            WHERE n.project_id = $project_id
            RETURN count(n) AS total_nodes
            """
            count_result = await session.run(count_query, project_id=project_id)
            count_row = await count_result.single()
            total_nodes = count_row["total_nodes"] if count_row else 0

            # Rewire ownership and update company scope
            transfer_query = """
            MERGE (target:Company {id: $target_company_id})
            SET target.company_id = $target_company_id,
                target.updated_at = datetime()
            WITH target
            OPTIONAL MATCH (source:Company {id: $source_company_id})-[old_rel:HAS_PROJECT]->(p:Project {id: $project_id})
            WITH target, p, collect(old_rel) AS old_rels
            FOREACH (rel IN old_rels | DELETE rel)
            FOREACH (_ IN CASE WHEN p IS NULL THEN [] ELSE [1] END |
                MERGE (target)-[:HAS_PROJECT]->(p)
            )
            FOREACH (_ IN CASE WHEN p IS NULL THEN [] ELSE [1] END |
                SET p.company_id = $target_company_id,
                    p.updated_at = datetime()
            )
            WITH size(old_rels) AS old_rel_count
            MATCH (n)
            WHERE n.project_id = $project_id
            SET n.company_id = $target_company_id,
                n.updated_at = datetime()
            RETURN old_rel_count AS ownership_edges_removed, count(n) AS updated_nodes
            """

            result = await session.run(
                transfer_query,
                project_id=project_id,
                source_company_id=source_company_id,
                target_company_id=target_company_id,
            )
            row = await result.single()

        updated_nodes = row["updated_nodes"] if row else 0
        ownership_edges_removed = row["ownership_edges_removed"] if row else 0

        logger.info(
            f"Transferred Neo4j project scope for {project_id}: "
            f"nodes={updated_nodes}, ownership_edges_removed={ownership_edges_removed}"
        )

        return {
            "project_nodes_total": total_nodes,
            "project_nodes_updated": updated_nodes,
            "ownership_edges_rewired": ownership_edges_removed,
        }

    async def create_branch_node(
        self,
        branch_id: str,
        repository_id: str,
        name: str,
        commit_sha: Optional[str] = None,
        company_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> None:
        """
        Create Branch node and link to Repository.

        Args:
            branch_id: Branch UUID
            repository_id: Repository UUID
            name: Branch name
            commit_sha: Commit SHA
            company_id: Company UUID (for multi-tenant scoping)
            project_id: Project UUID (for multi-tenant scoping)
        """
        await self.connect()

        query = """
        MATCH (r:Repository {id: $repository_id})
        MERGE (b:Branch {id: $branch_id})
        SET b.name = $name,
            b.commit_sha = $commit_sha,
            b.repository_id = $repository_id,
            b.company_id = $company_id,
            b.project_id = $project_id,
            b.updated_at = datetime()
        MERGE (r)-[:HAS_BRANCH]->(b)
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(
                query,
                branch_id=branch_id,
                repository_id=repository_id,
                name=name,
                commit_sha=commit_sha,
                company_id=company_id,
                project_id=project_id,
            )

        logger.info(f"Created/updated Branch node: {branch_id}")

    async def create_document_node(
        self,
        document_id: str,
        company_id: str,
        project_id: str,
        filename: str,
        file_type: str,
        file_size: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create Document node and link to Company and Project.

        Args:
            document_id: Document UUID
            company_id: Company UUID
            project_id: Project UUID
            filename: Document filename
            file_type: Document file type
            file_size: Document file size
            metadata: Additional metadata
        """
        await self.connect()

        query = """
        MATCH (c:Company {id: $company_id, company_id: $company_id})
        MATCH (p:Project {id: $project_id, project_id: $project_id})
        MERGE (d:Document {id: $document_id})
        SET d.filename = $filename,
            d.file_type = $file_type,
            d.file_size = $file_size,
            d.company_id = $company_id,
            d.project_id = $project_id,
            d.metadata = $metadata,
            d.updated_at = datetime()
        MERGE (c)-[:HAS_DOCUMENT]->(d)
        MERGE (p)-[:HAS_DOCUMENT]->(d)
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(
                query,
                document_id=document_id,
                company_id=company_id,
                project_id=project_id,
                filename=filename,
                file_type=file_type,
                file_size=file_size,
                metadata=metadata or {},
            )

        logger.info(f"Created/updated Document node: {document_id}")

    async def delete_company_node(self, company_id: str) -> None:
        """Delete Company node and all related nodes (multi-tenant safe)."""
        await self.connect()

        query = """
        MATCH (c:Company {id: $company_id, company_id: $company_id})
        OPTIONAL MATCH (c)-[:HAS_PROJECT]->(p:Project {company_id: $company_id})
        OPTIONAL MATCH (p)-[:HAS_REPOSITORY]->(r:Repository {company_id: $company_id})
        OPTIONAL MATCH (r)-[:HAS_BRANCH]->(b:Branch {company_id: $company_id})
        OPTIONAL MATCH (c)-[:HAS_DOCUMENT]->(d:Document {company_id: $company_id})
        DETACH DELETE c, p, r, b, d
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(query, company_id=company_id)

        logger.info(f"Deleted Company node and related nodes: {company_id}")

    async def delete_project_node(
        self, project_id: str, company_id: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Delete Project node and ALL related nodes (comprehensive cleanup).

        Deletes all nodes with project_id property including:
        - Project, Repository, Branch, Document
        - Code, Entity (code intelligence)
        - Knowledge, Expertise, ExpertiseChunk (knowledge base)
        - Fact (derived facts)

        Args:
            project_id: Project UUID to delete
            company_id: Optional company_id for additional safety check

        Returns:
            Dict with counts of deleted nodes by label
        """
        await self.connect()

        # First, count nodes to be deleted for reporting
        count_query = """
        MATCH (n)
        WHERE n.project_id = $project_id
        RETURN labels(n)[0] AS label, count(n) AS count
        """

        async with self.driver.session(database=self.database) as session:
            # Get counts before deletion
            count_result = await session.run(count_query, project_id=project_id)
            counts = {record["label"]: record["count"] async for record in count_result}

            if not counts:
                logger.info(f"No nodes found for project {project_id}")
                return {}

            logger.info(f"Found nodes to delete for project {project_id}: {counts}")

            # Delete ALL nodes with this project_id (handles all node types)
            # DETACH DELETE removes all relationships automatically
            delete_query = """
            MATCH (n)
            WHERE n.project_id = $project_id
            DETACH DELETE n
            """

            await session.run(delete_query, project_id=project_id)

        total_deleted = sum(counts.values())
        logger.info(f"Deleted {total_deleted} nodes for project {project_id}: {counts}")
        return counts

    async def delete_repository_node(self, repository_id: str) -> None:
        """Delete Repository node and all related nodes."""
        await self.connect()

        query = """
        MATCH (r:Repository {id: $repository_id})
        OPTIONAL MATCH (r)-[:HAS_BRANCH]->(b:Branch)
        DETACH DELETE r, b
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(query, repository_id=repository_id)

        logger.info(f"Deleted Repository node and related nodes: {repository_id}")

    async def delete_branch_node(self, branch_id: str) -> None:
        """Delete Branch node."""
        await self.connect()

        query = """
        MATCH (b:Branch {id: $branch_id})
        DETACH DELETE b
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(query, branch_id=branch_id)

        logger.info(f"Deleted Branch node: {branch_id}")

    async def delete_document_node(self, document_id: str) -> None:
        """Delete Document node."""
        await self.connect()

        query = """
        MATCH (d:Document {id: $document_id})
        DETACH DELETE d
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(query, document_id=document_id)

        logger.info(f"Deleted Document node: {document_id}")

    # Feature graph operations

    async def create_feature_node(
        self,
        feature_id: str,
        project_id: str,
        company_id: str,
        name: str,
        description: str,
        status: str = "ready for refinement",
        priority: str = "medium",
        next_prompt: Optional[str] = None,
    ) -> None:
        """
        Create Feature node and link to Project.

        Args:
            feature_id: Feature UUID
            project_id: Project UUID
            company_id: Company UUID
            name: Feature name
            description: Feature description
            status: Feature status
            priority: Feature priority
            next_prompt: Next step or prompt
        """
        await self.connect()

        query = """
        MATCH (p:Project {id: $project_id})
        MERGE (f:Feature {id: $feature_id})
        SET f.name = $name,
            f.description = $description,
            f.status = $status,
            f.priority = $priority,
            f.next_prompt = $next_prompt,
            f.project_id = $project_id,
            f.company_id = $company_id,
            f.updated_at = datetime()
        MERGE (p)-[:HAS_FEATURE]->(f)
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(
                query,
                feature_id=feature_id,
                project_id=project_id,
                company_id=company_id,
                name=name,
                description=description,
                status=status,
                priority=priority,
                next_prompt=next_prompt,
            )

        logger.info(f"Created/updated Feature node: {feature_id}")

    async def create_feature_chunk_nodes(self, chunks: List[Dict[str, Any]]) -> None:
        """
        Create FeatureChunk nodes and link to Feature.

        Args:
            chunks: List of chunk dicts with feature_id, chunk_index, content, metadata
        """
        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for chunk in chunks:
                query = """
                MATCH (f:Feature {id: $feature_id})
                MERGE (c:FeatureChunk {id: $chunk_id})
                SET c.chunk_index = $chunk_index,
                    c.chunk_type = $chunk_type,
                    c.summary = $summary,
                    c.content = $content,
                    c.token_count = $token_count,
                    c.qdrant_point_id = $qdrant_point_id,
                    c.updated_at = datetime()
                MERGE (f)-[:HAS_CHUNK]->(c)
                """

                await session.run(
                    query,
                    feature_id=chunk["feature_id"],
                    chunk_id=str(chunk["id"]),
                    chunk_index=chunk["chunk_index"],
                    chunk_type=chunk.get("chunk_type"),
                    summary=chunk.get("summary"),
                    content=chunk.get("content", ""),
                    token_count=chunk.get("token_count"),
                    qdrant_point_id=chunk.get("qdrant_point_id"),
                )

        logger.info(f"Created {len(chunks)} FeatureChunk nodes")

    async def delete_feature_graph(self, feature_id: str) -> None:
        """Delete all feature-related nodes and relationships."""
        await self.connect()

        query = """
        MATCH (f:Feature {id: $feature_id})
        OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:FeatureChunk)
        OPTIONAL MATCH (c)-[:COVERS]->(concept:Concept)
        OPTIONAL MATCH (c)-[:REQUIRES]->(dep:Dependency)
        DETACH DELETE f, c, concept, dep
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(query, feature_id=feature_id)

        logger.info(f"Deleted feature graph: {feature_id}")

    # Agent Graph Operations

    async def create_agent_node(
        self,
        agent_id: str,
        company_id: str,
        name: str,
        personality: str,
        main_responsibilities: str,
        when_to_use: Optional[str] = None,
    ) -> None:
        """
        Create Agent node in Neo4j with self-healing Company node creation.

        Uses MERGE for Company to auto-create if missing (idempotent).
        This fixes silent failures when Company nodes don't exist.

        Args:
            agent_id: Agent UUID
            company_id: Company UUID
            name: Agent name
            personality: Agent personality description
            main_responsibilities: Main responsibilities
            when_to_use: When to delegate to this agent
        """
        await self.connect()

        query = """
        MERGE (c:Company {id: $company_id})
        ON CREATE SET c.created_at = datetime()
        MERGE (a:Agent {id: $agent_id})
        SET a.name = $name,
            a.personality = $personality,
            a.main_responsibilities = $main_responsibilities,
            a.company_id = $company_id,
            a.when_to_use = $when_to_use,
            a.updated_at = datetime()
        MERGE (c)-[:HAS_AGENT]->(a)
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(
                query,
                agent_id=agent_id,
                company_id=company_id,
                name=name,
                personality=personality,
                main_responsibilities=main_responsibilities,
                when_to_use=when_to_use,
            )

            # Verify node creation succeeded
            summary = await result.consume()
            nodes_created = summary.counters.nodes_created

            if nodes_created == 0 and summary.counters.properties_set == 0:
                raise Exception(
                    f"Failed to create Agent node {agent_id} - no nodes or properties modified. "
                    f"This may indicate a database connectivity issue."
                )

            logger.info(
                f"Created Agent node: {agent_id} "
                f"(nodes_created={nodes_created}, relationships_created={summary.counters.relationships_created})"
            )

            # Warn if Company node was auto-created (indicates missing company setup)
            if nodes_created >= 2:  # Both Company and Agent created
                logger.warning(
                    f"Auto-created Company node {company_id} during agent sync. "
                    f"Company should have been created during company registration."
                )

    async def create_skill_file_nodes(
        self, agent_id: str, file_metadata: List[Dict[str, Any]]
    ) -> None:
        """
        Create file nodes (Skill, Reference, Script, Asset) and link to Agent.

        Args:
            agent_id: Agent UUID
            file_metadata: List of dicts with {file_path, file_type, chunk_count}
        """
        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for file_meta in file_metadata:
                file_path = file_meta["file_path"]
                file_type = file_meta["file_type"]
                chunk_count = file_meta.get("chunk_count", 0)

                # Determine node label based on file path
                if file_path == "SKILL.md":
                    node_label = "Skill"
                    relationship = "HAS_SKILL"
                elif file_path.startswith("references/"):
                    node_label = "Reference"
                    relationship = "HAS_REFERENCE"
                elif file_path.startswith("scripts/"):
                    node_label = "Script"
                    relationship = "HAS_SCRIPT"
                elif file_path.startswith("assets/"):
                    node_label = "Asset"
                    relationship = "HAS_ASSET"
                else:
                    node_label = "SkillFile"
                    relationship = "HAS_FILE"

                query = f"""
                MATCH (a:Agent {{id: $agent_id}})
                MERGE (f:{node_label} {{file_path: $file_path, agent_id: $agent_id}})
                SET f.file_type = $file_type,
                    f.chunk_count = $chunk_count,
                    f.updated_at = datetime()
                MERGE (a)-[:{relationship}]->(f)
                """

                await session.run(
                    query,
                    agent_id=agent_id,
                    file_path=file_path,
                    file_type=file_type,
                    chunk_count=chunk_count,
                )

        logger.info(f"Created {len(file_metadata)} file nodes for agent {agent_id}")

    async def create_skill_chunk_nodes(self, chunks: List[Dict[str, Any]]) -> None:
        """
        Create SkillChunk nodes and link to files.

        Args:
            chunks: List of chunk dicts with file_path, chunk_index, content, metadata
        """
        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for chunk in chunks:
                # Find parent file node (could be Skill, Reference, Script, etc.)
                query = """
                MATCH (f {file_path: $file_path, agent_id: $agent_id})
                MERGE (c:SkillChunk {id: $chunk_id})
                SET c.chunk_index = $chunk_index,
                    c.section_title = $section_title,
                    c.chunk_type = $chunk_type,
                    c.summary = $summary,
                    c.content = $content,
                    c.token_count = $token_count,
                    c.qdrant_point_id = $qdrant_point_id,
                    c.updated_at = datetime()
                MERGE (f)-[:HAS_CHUNK]->(c)
                """

                await session.run(
                    query,
                    agent_id=chunk["agent_id"],
                    file_path=chunk["file_path"],
                    chunk_id=str(chunk["id"]),
                    chunk_index=chunk["chunk_index"],
                    section_title=chunk.get("section_title"),
                    chunk_type=chunk.get("chunk_type"),
                    summary=chunk.get("summary"),
                    content=chunk.get("content", ""),
                    token_count=chunk.get("token_count"),
                    qdrant_point_id=chunk.get("qdrant_point_id"),
                )

        logger.info(f"Created {len(chunks)} SkillChunk nodes")

    async def create_concept_nodes(
        self, agent_id: str, file_path: str, concepts: List[str]
    ) -> None:
        """
        Create Concept nodes and COVERS relationships.

        Args:
            agent_id: Agent UUID
            file_path: Source file path
            concepts: List of concept strings
        """
        if not concepts:
            return

        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for concept in concepts:
                # Match any file node type (Skill, Reference, Script, Asset, SkillFile)
                query = """
                MATCH (f)
                WHERE f.file_path = $file_path AND f.agent_id = $agent_id
                MERGE (c:Concept {name: $concept})
                MERGE (f)-[:COVERS]->(c)
                """

                result = await session.run(
                    query, agent_id=agent_id, file_path=file_path, concept=concept
                )

                # Verify relationship creation
                summary = await result.consume()
                if (
                    summary.counters.relationships_created == 0
                    and summary.counters.nodes_created == 0
                ):
                    logger.warning(
                        f"No COVERS relationship created for concept '{concept}' - "
                        f"File with file_path='{file_path}' may not exist"
                    )

        logger.info(f"Created {len(concepts)} concept nodes for {file_path}")

    async def create_dependency_nodes(
        self, agent_id: str, file_path: str, dependencies: List[str]
    ) -> None:
        """Create Dependency nodes and REQUIRES relationships."""
        if not dependencies:
            return

        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for dep in dependencies:
                # Match any file node type (Skill, Reference, Script, Asset, SkillFile)
                query = """
                MATCH (f)
                WHERE f.file_path = $file_path AND f.agent_id = $agent_id
                MERGE (d:Dependency {name: $dependency})
                MERGE (f)-[:REQUIRES]->(d)
                """

                result = await session.run(
                    query, agent_id=agent_id, file_path=file_path, dependency=dep
                )

                # Verify relationship creation
                summary = await result.consume()
                if (
                    summary.counters.relationships_created == 0
                    and summary.counters.nodes_created == 0
                ):
                    logger.warning(
                        f"No REQUIRES relationship created for dependency '{dep}' - "
                        f"File with file_path='{file_path}' may not exist"
                    )

        logger.info(f"Created {len(dependencies)} dependency nodes for {file_path}")

    async def create_file_references(
        self, agent_id: str, source_file: str, target_files: List[str]
    ) -> None:
        """Create REFERENCES_FILE relationships between files."""
        if not target_files:
            return

        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for target in target_files:
                query = """
                MATCH (source {file_path: $source_file, agent_id: $agent_id})
                MATCH (target {file_path: $target_file, agent_id: $agent_id})
                MERGE (source)-[:REFERENCES_FILE]->(target)
                """

                await session.run(
                    query, agent_id=agent_id, source_file=source_file, target_file=target
                )

        logger.info(f"{source_file} references {len(target_files)} files")

    async def delete_agent_graph(self, agent_id: str) -> None:
        """Delete all agent-related nodes and relationships."""
        await self.connect()

        query = """
        MATCH (a:Agent {id: $agent_id})
        OPTIONAL MATCH (a)-[:HAS_SKILL|HAS_REFERENCE|HAS_SCRIPT|HAS_ASSET|HAS_FILE]->(f)
        OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c:SkillChunk)
        OPTIONAL MATCH (f)-[:COVERS]->(concept:Concept)
        OPTIONAL MATCH (f)-[:REQUIRES]->(dep:Dependency)
        DETACH DELETE a, f, c, concept, dep
        """

        async with self.driver.session(database=self.database) as session:
            await session.run(query, agent_id=agent_id)

        logger.info(f"Deleted agent graph: {agent_id}")

    # Agent Skill Operations (Agent → Expertise linking)

    async def link_agent_skill(self, agent_id: str, expertise_id: str) -> bool:
        """
        Create HAS_SKILL relationship between Agent and Expertise.

        Args:
            agent_id: Agent UUID
            expertise_id: Expertise UUID

        Returns:
            True if relationship created, False if already exists
        """
        await self.connect()

        query = """
        MATCH (a:Agent {id: $agent_id})
        MATCH (e:Expertise {id: $expertise_id})
        MERGE (a)-[r:HAS_SKILL]->(e)
        ON CREATE SET r.linked_at = datetime()
        RETURN r.linked_at AS linked_at
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, agent_id=agent_id, expertise_id=expertise_id)
            record = await result.single()

        if record:
            logger.info(f"Linked Agent {agent_id} to Expertise {expertise_id}")
            return True
        return False

    async def unlink_agent_skill(self, agent_id: str, expertise_id: str) -> bool:
        """
        Remove HAS_SKILL relationship between Agent and Expertise.

        Args:
            agent_id: Agent UUID
            expertise_id: Expertise UUID

        Returns:
            True if relationship deleted, False if not found
        """
        await self.connect()

        query = """
        MATCH (a:Agent {id: $agent_id})-[r:HAS_SKILL]->(e:Expertise {id: $expertise_id})
        DELETE r
        RETURN count(r) AS deleted
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, agent_id=agent_id, expertise_id=expertise_id)
            record = await result.single()

        deleted = record["deleted"] if record else 0
        if deleted > 0:
            logger.info(f"Unlinked Agent {agent_id} from Expertise {expertise_id}")
            return True
        return False

    async def get_agent_skills(self, agent_id: str) -> List[Dict[str, Any]]:
        """
        Get all Expertise linked to Agent with their chunks for LLM navigation.

        Returns skill tree structure:
        [
            {
                "expertise_id": "...",
                "title": "Python Backend",
                "summary": "FastAPI, SQLAlchemy...",
                "sections": [
                    {"chunk_id": "...", "title": "API Design", "summary": "...", "has_code": true},
                    ...
                ]
            }
        ]

        Args:
            agent_id: Agent UUID

        Returns:
            List of skill dictionaries with nested sections
        """
        await self.connect()

        # Get expertise with their root chunks
        query = """
        MATCH (a:Agent {id: $agent_id})-[:HAS_SKILL]->(e:Expertise)
        OPTIONAL MATCH (e)-[:HAS_ROOT_CHUNK]->(root:ExpertiseChunk)
        OPTIONAL MATCH (root)-[:HAS_CHILD*0..]->(c:ExpertiseChunk)
        WITH e, collect(DISTINCT {
            chunk_id: c.id,
            title: coalesce(c.summary, 'Section'),
            summary: left(c.content, 200),
            level: c.level,
            position: c.position,
            has_code: coalesce(
                CASE WHEN c.metadata CONTAINS 'has_code' THEN true ELSE false END,
                false
            ),
            metadata: c.metadata
        }) AS chunks
        RETURN e.id AS expertise_id,
               e.title AS title,
               coalesce(left(e.content, 200), e.title) AS summary,
               e.when_to_use AS when_to_use,
               chunks
        ORDER BY e.title
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, agent_id=agent_id)
            records = await result.data()

        skills = []
        for record in records:
            # Filter out null chunks and build sections
            chunks = record.get("chunks", [])
            sections = []

            for chunk in chunks:
                if chunk.get("chunk_id"):
                    # Parse metadata JSON to extract has_code
                    metadata_str = chunk.get("metadata", "{}")
                    try:
                        metadata = json.loads(metadata_str) if metadata_str else {}
                        has_code = metadata.get("has_code", False)
                    except (json.JSONDecodeError, TypeError):
                        has_code = False

                    sections.append(
                        {
                            "chunk_id": chunk["chunk_id"],
                            "title": chunk.get("title", "Section"),
                            "summary": chunk.get("summary", "")[:200]
                            if chunk.get("summary")
                            else "",
                            "has_code": has_code,
                            "level": chunk.get("level", 0),
                            "position": chunk.get("position", 0),
                        }
                    )

            # Sort sections by level then position
            sections.sort(key=lambda x: (x.get("level", 0), x.get("position", 0)))

            skills.append(
                {
                    "expertise_id": record["expertise_id"],
                    "title": record.get("title", ""),
                    "summary": record.get("summary", ""),
                    "when_to_use": record.get("when_to_use", ""),
                    "sections": sections,
                }
            )

        logger.info(f"Retrieved {len(skills)} skills for agent {agent_id}")
        return skills

    async def get_expertise_sections(self, expertise_id: str) -> List[Dict[str, Any]]:
        """
        Get all sections (chunks) for a specific Expertise document.

        This is the second tier of progressive disclosure:
        1. get_agent_skills() -> lightweight skill titles (no sections)
        2. get_expertise_sections(expertise_id) -> sections for ONE skill
        3. get_expertise_chunk(chunk_id) -> full content for ONE section

        Args:
            expertise_id: Expertise UUID

        Returns:
            List of section dictionaries with chunk_id, title, summary, has_code, level, position
        """
        await self.connect()

        query = """
        MATCH (e:Expertise {id: $expertise_id})-[:HAS_ROOT_CHUNK]->(root:ExpertiseChunk)
        OPTIONAL MATCH (root)-[:HAS_CHILD*0..]->(c:ExpertiseChunk)
        WITH collect(DISTINCT c) AS all_chunks
        UNWIND all_chunks AS chunk
        WITH chunk WHERE chunk IS NOT NULL
        RETURN chunk.id AS chunk_id,
               coalesce(chunk.summary, 'Section') AS title,
               left(chunk.content, 200) AS summary,
               chunk.level AS level,
               chunk.position AS position,
               chunk.metadata AS metadata
        ORDER BY chunk.level, chunk.position
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, expertise_id=expertise_id)
            records = await result.data()

        sections = []
        for record in records:
            # Parse metadata JSON to extract has_code
            metadata_str = record.get("metadata", "{}")
            try:
                metadata = json.loads(metadata_str) if metadata_str else {}
                has_code = metadata.get("has_code", False)
            except (json.JSONDecodeError, TypeError):
                has_code = False

            sections.append(
                {
                    "chunk_id": record["chunk_id"],
                    "title": record.get("title", "Section"),
                    "summary": record.get("summary", "")[:200] if record.get("summary") else "",
                    "has_code": has_code,
                    "level": record.get("level", 0),
                    "position": record.get("position", 0),
                }
            )

        logger.info(f"Retrieved {len(sections)} sections for expertise {expertise_id}")
        return sections

    async def get_agent_info(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """
        Get basic Agent info for skill tree response.

        Args:
            agent_id: Agent UUID

        Returns:
            Agent info dict or None if not found
        """
        await self.connect()

        query = """
        MATCH (a:Agent {id: $agent_id})
        RETURN a.id AS id,
               a.name AS name,
               a.company_id AS company_id
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, agent_id=agent_id)
            record = await result.single()

        if record:
            return {
                "id": record["id"],
                "name": record.get("name", ""),
                "company_id": record.get("company_id"),
            }
        return None

    # Knowledge Graph Operations

    async def check_duplicate_knowledge(
        self, project_id: str, content_hash: str
    ) -> Optional[Dict[str, str]]:
        """
        Check if knowledge with same content hash exists in project.

        Args:
            project_id: Project UUID
            content_hash: SHA256 hash of content

        Returns:
            Dict with knowledge_id and title if duplicate exists, None otherwise
        """
        await self.connect()

        query = """
        MATCH (k:Knowledge {project_id: $project_id})
        WHERE k.content_hash = $content_hash
        RETURN k.id AS knowledge_id, k.title AS title
        LIMIT 1
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, project_id=project_id, content_hash=content_hash)
            record = await result.single()

            if record:
                return {"knowledge_id": record["knowledge_id"], "title": record["title"]}
            return None

    async def create_knowledge_node(
        self,
        knowledge_id: str,
        company_id: str,
        project_id: str,
        title: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
        content_hash: Optional[str] = None,
        when_to_use: Optional[str] = None,
    ) -> None:
        """
        Create Knowledge node and link to Project.

        Args:
            knowledge_id: Knowledge UUID
            company_id: Company UUID (stored for multi-tenant scoping)
            project_id: Project UUID
            title: Knowledge title/summary
            content: Raw content text
            metadata: Additional metadata (type, tags, etc.)
            content_hash: SHA256 hash of content for deduplication
            when_to_use: When to use this knowledge
        """
        await self.connect()

        query = """
        MATCH (p:Project {id: $project_id, project_id: $project_id, company_id: $company_id})
        MERGE (k:Knowledge {id: $knowledge_id})
        SET k.title = $title,
            k.content = $content,
            k.content_hash = $content_hash,
            k.company_id = $company_id,
            k.project_id = $project_id,
            k.metadata = $metadata,
            k.when_to_use = $when_to_use,
            k.created_at = coalesce(k.created_at, datetime()),
            k.updated_at = datetime()
        MERGE (p)-[:HAS_KNOWLEDGE]->(k)
        """

        # Convert metadata dict to JSON string (Neo4j doesn't support nested maps as properties)
        metadata_json = json.dumps(metadata or {})

        async with self.driver.session(database=self.database) as session:
            result = await session.run(
                query,
                knowledge_id=knowledge_id,
                company_id=company_id,
                project_id=project_id,
                title=title,
                content=content,
                metadata=metadata_json,
                content_hash=content_hash,
                when_to_use=when_to_use,
            )
            await result.consume()  # Consume result to catch any errors

        logger.info(f"Created/updated Knowledge node: {knowledge_id}")

    async def create_knowledge_chunk_nodes(self, chunks: List[Dict[str, Any]]) -> None:
        """
        Create KnowledgeChunk nodes and link to Knowledge.

        Args:
            chunks: List of chunk dicts with knowledge_id, chunk_index, content, metadata
        """
        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for chunk in chunks:
                # Convert keywords list to JSON string for Neo4j
                keywords_json = json.dumps(chunk.get("keywords", []))

                query = """
                MATCH (k:Knowledge {id: $knowledge_id})
                MERGE (c:KnowledgeChunk {id: $chunk_id})
                SET c.chunk_index = $chunk_index,
                    c.content = $content,
                    c.summary = $summary,
                    c.token_count = $token_count,
                    c.qdrant_point_id = $qdrant_point_id,
                    c.section_title = $section_title,
                    c.section_level = $section_level,
                    c.chunk_type = $chunk_type,
                    c.has_code = $has_code,
                    c.keywords = $keywords,
                    c.parent_section = $parent_section,
                    c.updated_at = datetime()
                MERGE (k)-[:HAS_CHUNK]->(c)
                """

                result = await session.run(
                    query,
                    knowledge_id=chunk["knowledge_id"],
                    chunk_id=str(chunk["id"]),
                    chunk_index=chunk["chunk_index"],
                    content=chunk.get("content", ""),
                    summary=chunk.get("summary"),
                    token_count=chunk.get("token_count"),
                    qdrant_point_id=chunk.get("qdrant_point_id"),
                    section_title=chunk.get("section_title"),
                    section_level=chunk.get("section_level"),
                    chunk_type=chunk.get("chunk_type", "prose"),
                    has_code=chunk.get("has_code", False),
                    keywords=keywords_json,
                    parent_section=chunk.get("parent_section"),
                )
                await result.consume()

        logger.info(f"Created {len(chunks)} KnowledgeChunk nodes")

    async def create_knowledge_entities(
        self, knowledge_id: str, entities: List[Dict[str, Any]]
    ) -> None:
        """
        Create Entity nodes extracted from knowledge and link to Knowledge.

        Args:
            knowledge_id: Knowledge UUID
            entities: List of entity dicts with name, type, attributes
        """
        if not entities:
            return

        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for entity in entities:
                query = """
                MATCH (k:Knowledge {id: $knowledge_id})
                MERGE (e:Entity {name: $name, type: $type})
                SET e.attributes = $attributes,
                    e.description = $description,
                    e.confidence = $confidence,
                    e.updated_at = datetime()
                MERGE (k)-[:CONTAINS]->(e)
                """

                # Convert attributes dict to JSON string
                attributes_json = json.dumps(entity.get("attributes", {}))

                result = await session.run(
                    query,
                    knowledge_id=knowledge_id,
                    name=entity["name"],
                    type=entity["type"],
                    attributes=attributes_json,
                    description=entity.get("description", ""),
                    confidence=entity.get("confidence", 1.0),
                )
                await result.consume()

        logger.info(f"Created {len(entities)} Entity nodes for knowledge {knowledge_id}")

    async def create_knowledge_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Create relationships between entities extracted from knowledge.

        Args:
            relationships: List of relationship dicts with source, target, type
        """
        if not relationships:
            return

        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for rel in relationships:
                # Use dynamic relationship type (sanitized by caller)
                query = f"""
                MATCH (source:Entity {{name: $source_name}})
                MATCH (target:Entity {{name: $target_name}})
                MERGE (source)-[r:{rel["type"]}]->(target)
                SET r.context = $context,
                    r.confidence = $confidence,
                    r.updated_at = datetime()
                """

                result = await session.run(
                    query,
                    source_name=rel["source"],
                    target_name=rel["target"],
                    context=rel.get("context", ""),
                    confidence=rel.get("confidence", 1.0),
                )
                await result.consume()

        logger.info(f"Created {len(relationships)} knowledge relationships")

    async def delete_knowledge_node(
        self, knowledge_id: str, company_id: Optional[str] = None
    ) -> None:
        """Delete Knowledge node and all related chunks/entities (multi-tenant safe)."""
        await self.connect()

        # If company_id provided, use it for safety
        if company_id:
            query = """
            MATCH (k:Knowledge {id: $knowledge_id, company_id: $company_id})
            OPTIONAL MATCH (k)-[:HAS_CHUNK]->(c:KnowledgeChunk)
            OPTIONAL MATCH (k)-[:CONTAINS]->(e:Entity)
            DETACH DELETE k, c, e
            """
            params = {"knowledge_id": knowledge_id, "company_id": company_id}
        else:
            query = """
            MATCH (k:Knowledge {id: $knowledge_id})
            OPTIONAL MATCH (k)-[:HAS_CHUNK]->(c:KnowledgeChunk)
            OPTIONAL MATCH (k)-[:CONTAINS]->(e:Entity)
            DETACH DELETE k, c, e
            """
            params = {"knowledge_id": knowledge_id}

        async with self.driver.session(database=self.database) as session:
            await session.run(query, **params)

        logger.info(f"Deleted Knowledge node and related nodes: {knowledge_id}")

    async def get_knowledge_node(self, knowledge_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a single Knowledge node by ID with full content.

        Args:
            knowledge_id: Knowledge UUID

        Returns:
            Dict with knowledge details or None if not found
        """
        await self.connect()

        query = """
        MATCH (k:Knowledge {id: $knowledge_id})
        OPTIONAL MATCH (k)-[:HAS_CHUNK]->(c:KnowledgeChunk)
        WITH k, COUNT(c) AS chunks_count
        RETURN k.id AS knowledge_id,
               k.title AS title,
               k.content AS content,
               k.content_hash AS content_hash,
               k.when_to_use AS when_to_use,
               k.company_id AS company_id,
               k.project_id AS project_id,
               k.metadata AS metadata,
               k.created_at AS created_at,
               k.updated_at AS updated_at,
               chunks_count
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, knowledge_id=knowledge_id)
            record = await result.single()

            if not record:
                return None

            # Parse metadata from JSON string
            metadata_str = record.get("metadata")
            metadata = json.loads(metadata_str) if metadata_str else {}

            # Format datetime for response
            created_at = record.get("created_at")
            updated_at = record.get("updated_at")

            return {
                "knowledge_id": record["knowledge_id"],
                "title": record.get("title", ""),
                "content": record.get("content", ""),
                "content_hash": record.get("content_hash"),
                "when_to_use": record.get("when_to_use"),
                "company_id": record.get("company_id", ""),
                "project_id": record.get("project_id", ""),
                "metadata": metadata,
                "created_at": str(created_at) if created_at else None,
                "updated_at": str(updated_at) if updated_at else None,
                "chunks_count": record.get("chunks_count", 0),
            }

    async def list_knowledge_nodes(
        self, project_id: str, limit: int = 50, offset: int = 0
    ) -> Dict[str, Any]:
        """
        List Knowledge nodes for a project with pagination.

        Args:
            project_id: Project UUID
            limit: Maximum results (default: 50, max: 100)
            offset: Skip first N results

        Returns:
            Dict with total_count and knowledge_list
        """
        await self.connect()

        # Enforce max limit
        limit = min(limit, 100)

        # Count query
        count_query = """
        MATCH (k:Knowledge {project_id: $project_id})
        RETURN COUNT(k) AS total
        """

        # List query with pagination
        list_query = """
        MATCH (k:Knowledge {project_id: $project_id})
        OPTIONAL MATCH (k)-[:HAS_CHUNK]->(c:KnowledgeChunk)
        WITH k, COUNT(c) AS chunks_count
        RETURN k.id AS knowledge_id,
               k.title AS title,
               LEFT(k.content, 200) AS summary,
               k.project_id AS project_id,
               k.metadata AS metadata,
               k.created_at AS created_at,
               k.updated_at AS updated_at,
               chunks_count
        ORDER BY k.updated_at DESC
        SKIP $offset LIMIT $limit
        """

        async with self.driver.session(database=self.database) as session:
            # Get total count
            count_result = await session.run(count_query, project_id=project_id)
            count_record = await count_result.single()
            total_count = count_record["total"] if count_record else 0

            # Get paginated list
            list_result = await session.run(
                list_query, project_id=project_id, limit=limit, offset=offset
            )
            records = await list_result.data()

            knowledge_list = []
            for record in records:
                # Parse metadata from JSON string
                metadata_str = record.get("metadata")
                metadata = json.loads(metadata_str) if metadata_str else {}

                # Format datetime for response
                created_at = record.get("created_at")
                updated_at = record.get("updated_at")

                knowledge_list.append(
                    {
                        "knowledge_id": record["knowledge_id"],
                        "title": record.get("title", ""),
                        "summary": record.get("summary", ""),
                        "project_id": record.get("project_id", ""),
                        "metadata": metadata,
                        "created_at": str(created_at) if created_at else None,
                        "updated_at": str(updated_at) if updated_at else None,
                        "chunks_count": record.get("chunks_count", 0),
                    }
                )

            return {"total_count": total_count, "knowledge_list": knowledge_list}

    async def count_knowledge(self, project_id: str) -> int:
        """
        Count Knowledge nodes for a project.

        Args:
            project_id: Project UUID

        Returns:
            Total count of knowledge documents
        """
        await self.connect()

        query = """
        MATCH (k:Knowledge {project_id: $project_id})
        RETURN COUNT(k) AS total
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, project_id=project_id)
            record = await result.single()
            return record["total"] if record else 0

    # Code Graph Operations

    async def check_duplicate_code(
        self, project_id: str, content_hash: str
    ) -> Optional[Dict[str, str]]:
        """
        Check if code with same content hash exists in project.

        Args:
            project_id: Project UUID
            content_hash: SHA256 hash of content

        Returns:
            Dict with code_id and title if duplicate exists, None otherwise
        """
        await self.connect()

        query = """
        MATCH (c:Code {project_id: $project_id})
        WHERE c.content_hash = $content_hash
        RETURN c.id AS code_id, c.title AS title
        LIMIT 1
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, project_id=project_id, content_hash=content_hash)
            record = await result.single()

            if record:
                return {"code_id": record["code_id"], "title": record["title"]}
            return None

    async def create_code_node(
        self,
        code_id: str,
        company_id: str,
        project_id: str,
        filename: str,
        language: str,
        title: str,
        content: str,
        content_hash: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create :Code node and link to Project.

        Args:
            code_id: Code UUID
            company_id: Company UUID (stored for multi-tenant scoping)
            project_id: Project UUID
            filename: Name of the code file
            language: Programming language
            title: Code title/summary
            content: Raw code content
            content_hash: SHA256 hash of content for deduplication
            metadata: Additional metadata (tags, etc.)

        Returns:
            Created node properties
        """
        await self.connect()

        query = """
        MATCH (p:Project {id: $project_id, project_id: $project_id, company_id: $company_id})
        MERGE (c:Code {id: $code_id})
        SET c.filename = $filename,
            c.language = $language,
            c.title = $title,
            c.content = $content,
            c.content_hash = $content_hash,
            c.company_id = $company_id,
            c.project_id = $project_id,
            c.metadata = $metadata,
            c.created_at = coalesce(c.created_at, datetime()),
            c.updated_at = datetime()
        MERGE (p)-[:HAS_CODE]->(c)
        RETURN c
        """

        # Convert metadata dict to JSON string (Neo4j doesn't support nested maps as properties)
        metadata_json = json.dumps(metadata or {})

        async with self.driver.session(database=self.database) as session:
            result = await session.run(
                query,
                code_id=code_id,
                company_id=company_id,
                project_id=project_id,
                filename=filename,
                language=language,
                title=title,
                content=content,
                content_hash=content_hash,
                metadata=metadata_json,
            )
            record = await result.single()
            await result.consume()  # Consume result to catch any errors

        logger.info(f"Created/updated Code node: {code_id}")

        return {"id": code_id, "filename": filename, "language": language, "title": title}

    async def create_code_chunk_nodes(self, chunks: List[Dict[str, Any]]) -> int:
        """
        Create :CodeChunk nodes and link to :Code.

        Args:
            chunks: List of chunk dicts with code_id, chunk_index, content, metadata

        Returns:
            Number of chunks created
        """
        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for chunk in chunks:
                # Convert list fields to JSON strings for Neo4j
                keywords_json = json.dumps(chunk.get("keywords", []))
                decorators_json = json.dumps(chunk.get("decorators", []))

                query = """
                MATCH (c:Code {id: $code_id})
                MERGE (cc:CodeChunk {id: $chunk_id})
                SET cc.code_id = $code_id,
                    cc.chunk_index = $chunk_index,
                    cc.content = $content,
                    cc.summary = $summary,
                    cc.token_count = $token_count,
                    cc.qdrant_point_id = $qdrant_point_id,
                    cc.chunk_type = $chunk_type,
                    cc.complexity = $complexity,
                    cc.entry_point = $entry_point,
                    cc.keywords = $keywords,
                    cc.function_signature = $function_signature,
                    cc.class_name = $class_name,
                    cc.decorators = $decorators,
                    cc.updated_at = datetime()
                MERGE (c)-[:HAS_CHUNK]->(cc)
                """

                result = await session.run(
                    query,
                    code_id=chunk["code_id"],
                    chunk_id=str(chunk["id"]),
                    chunk_index=chunk["chunk_index"],
                    content=chunk.get("content", ""),
                    summary=chunk.get("summary"),
                    token_count=chunk.get("token_count"),
                    qdrant_point_id=chunk.get("qdrant_point_id"),
                    chunk_type=chunk.get("chunk_type", "utility"),
                    complexity=chunk.get("complexity", "medium"),
                    entry_point=chunk.get("entry_point", False),
                    keywords=keywords_json,
                    function_signature=chunk.get("function_signature"),
                    class_name=chunk.get("class_name"),
                    decorators=decorators_json,
                )
                await result.consume()

        logger.info(f"Created {len(chunks)} CodeChunk nodes")
        return len(chunks)

    async def create_code_entities(self, code_id: str, entities: List[Dict[str, Any]]) -> None:
        """
        Create Entity nodes extracted from code and link to Code.
        Reuses shared Entity infrastructure - entities can be linked to both Code and Knowledge.

        Args:
            code_id: Code UUID
            entities: List of entity dicts with name, type, attributes
        """
        if not entities:
            return

        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for entity in entities:
                query = """
                MATCH (c:Code {id: $code_id})
                MERGE (e:Entity {name: $name, type: $type})
                SET e.attributes = $attributes,
                    e.description = $description,
                    e.confidence = $confidence,
                    e.updated_at = datetime()
                MERGE (c)-[:CONTAINS]->(e)
                """

                # Convert attributes dict to JSON string
                attributes_json = json.dumps(entity.get("attributes", {}))

                result = await session.run(
                    query,
                    code_id=code_id,
                    name=entity["name"],
                    type=entity["type"],
                    attributes=attributes_json,
                    description=entity.get("description", ""),
                    confidence=entity.get("confidence", 1.0),
                )
                await result.consume()

        logger.info(f"Created {len(entities)} Entity nodes for code {code_id}")

    async def create_code_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Create relationships between entities extracted from code.
        Reuses shared Entity infrastructure - same relationship logic as knowledge.

        Args:
            relationships: List of relationship dicts with source, target, type
        """
        if not relationships:
            return

        await self.connect()

        async with self.driver.session(database=self.database) as session:
            for rel in relationships:
                # Use dynamic relationship type (sanitized by caller)
                query = f"""
                MATCH (source:Entity {{name: $source_name}})
                MATCH (target:Entity {{name: $target_name}})
                MERGE (source)-[r:{rel["type"]}]->(target)
                SET r.context = $context,
                    r.confidence = $confidence,
                    r.updated_at = datetime()
                """

                result = await session.run(
                    query,
                    source_name=rel["source"],
                    target_name=rel["target"],
                    context=rel.get("context", ""),
                    confidence=rel.get("confidence", 1.0),
                )
                await result.consume()

        logger.info(f"Created {len(relationships)} code relationships")

    async def create_code_indexes(self) -> None:
        """
        Create indexes for :Code, :CodeChunk, and :Directory nodes.

        Indexes:
        - code_company_id: Index on company_id for filtering
        - code_project_id: Index on project_id for filtering
        - code_content_hash: Index on content_hash for duplicate detection
        - code_id_unique: Unique constraint on id
        - code_name: Index on name for filename lookups
        - code_directory_path: Index on directory_path for folder queries
        - code_chunk_code_id: Index on code_id for chunk lookup
        - directory_path: Index on path for folder lookups
        - directory_project_id: Index on project_id for filtering
        - directory_id_unique: Unique constraint on id
        """
        await self.connect()

        indexes = [
            # Code node indexes
            "CREATE INDEX code_company_id IF NOT EXISTS FOR (c:Code) ON (c.company_id)",
            "CREATE INDEX code_project_id IF NOT EXISTS FOR (c:Code) ON (c.project_id)",
            "CREATE INDEX code_content_hash IF NOT EXISTS FOR (c:Code) ON (c.content_hash)",
            "CREATE CONSTRAINT code_id_unique IF NOT EXISTS FOR (c:Code) REQUIRE c.id IS UNIQUE",
            "CREATE INDEX code_name IF NOT EXISTS FOR (c:Code) ON (c.name)",
            "CREATE INDEX code_directory_path IF NOT EXISTS FOR (c:Code) ON (c.directory_path)",
            "CREATE INDEX code_chunk_code_id IF NOT EXISTS FOR (cc:CodeChunk) ON (cc.code_id)",
            # Directory node indexes (KISS: essential indexes only)
            "CREATE INDEX directory_path IF NOT EXISTS FOR (d:Directory) ON (d.path)",
            "CREATE INDEX directory_project_id IF NOT EXISTS FOR (d:Directory) ON (d.project_id)",
            "CREATE CONSTRAINT directory_id_unique IF NOT EXISTS FOR (d:Directory) REQUIRE d.id IS UNIQUE",
        ]

        async with self.driver.session(database=self.database) as session:
            for index_query in indexes:
                try:
                    await session.run(index_query)
                    logger.info(f"Created index: {index_query}")
                except Exception as e:
                    logger.warning("Index creation skipped (may already exist): {}", e)

        logger.info("Code indexes verification complete")

    async def delete_code_node(self, code_id: str, company_id: Optional[str] = None) -> None:
        """Delete Code node and all related chunks/entities (multi-tenant safe)."""
        await self.connect()

        # If company_id provided, use it for safety
        if company_id:
            query = """
            MATCH (c:Code {id: $code_id, company_id: $company_id})
            OPTIONAL MATCH (c)-[:HAS_CHUNK]->(cc:CodeChunk)
            OPTIONAL MATCH (c)-[:CONTAINS]->(e:Entity)
            DETACH DELETE c, cc, e
            """
            params = {"code_id": code_id, "company_id": company_id}
        else:
            query = """
            MATCH (c:Code {id: $code_id})
            OPTIONAL MATCH (c)-[:HAS_CHUNK]->(cc:CodeChunk)
            OPTIONAL MATCH (c)-[:CONTAINS]->(e:Entity)
            DETACH DELETE c, cc, e
            """
            params = {"code_id": code_id}

        async with self.driver.session(database=self.database) as session:
            await session.run(query, **params)

        logger.info(f"Deleted Code node and related nodes: {code_id}")

    # Expertise Graph Operations

    async def check_duplicate_expertise(
        self, content_hash: str, company_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Check if expertise with same content hash exists within a company.

        Expertise is company-wide — duplicates are checked across the entire company.

        Args:
            content_hash: SHA256 hash of content
            company_id: Company UUID to scope the duplicate check

        Returns:
            Dict with expertise_id and title if duplicate exists, None otherwise
        """
        await self.connect()

        if not company_id:
            return None

        # Company-wide duplicate check — expertise is never project-scoped
        query = """
        MATCH (e:Expertise {company_id: $company_id})
        WHERE e.content_hash = $content_hash
        RETURN e.id AS expertise_id, e.title AS title
        LIMIT 1
        """
        params = {"company_id": company_id, "content_hash": content_hash}

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            record = await result.single()

            if record:
                return {"expertise_id": record["expertise_id"], "title": record["title"]}
            return None

    async def create_expertise_node(
        self,
        expertise_id: str,
        company_id: str,
        title: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
        content_hash: Optional[str] = None,
        when_to_use: Optional[str] = None,
    ) -> None:
        """
        Create Expertise node.

        Expertise is company-wide knowledge — it is NEVER scoped to a project.
        The node is stored with company_id only.

        The node is linked to the Company via HAS_COMPANY_EXPERTISE (best-effort;
        if the Company node is missing the Expertise node is still persisted).

        Args:
            expertise_id: Expertise UUID
            company_id: Company UUID (always required)
            title: Expertise title/summary
            content: Raw content text
            metadata: Additional metadata (type, tags, etc.)
            content_hash: SHA256 hash of content for deduplication
            when_to_use: When to use this expertise
        """
        await self.connect()

        # Convert metadata dict to JSON string (Neo4j doesn't support nested maps as properties)
        metadata_json = json.dumps(metadata or {})

        # Step 1: Unconditionally MERGE the Expertise node (company-wide, no project scope).
        # NEVER use MATCH+MERGE in a single query — if MATCH returns zero rows the MERGE
        # is silently skipped and no node is created without any error being raised.
        upsert_query = """
        MERGE (e:Expertise {id: $expertise_id})
        SET e.title = $title,
            e.content = $content,
            e.content_hash = $content_hash,
            e.company_id = $company_id,
            e.metadata = $metadata,
            e.is_company_level = true,
            e.when_to_use = $when_to_use,
            e.created_at = coalesce(e.created_at, datetime()),
            e.updated_at = datetime()
        """
        upsert_params = {
            "expertise_id": expertise_id,
            "company_id": company_id,
            "title": title,
            "content": content,
            "metadata": metadata_json,
            "content_hash": content_hash,
            "when_to_use": when_to_use,
        }

        # Step 2: Link to Company (best-effort — Company node must exist).
        link_query = """
        MATCH (c:Company {id: $company_id})
        MATCH (e:Expertise {id: $expertise_id})
        MERGE (c)-[:HAS_COMPANY_EXPERTISE]->(e)
        """
        link_params = {
            "company_id": company_id,
            "expertise_id": expertise_id,
        }

        async with self.driver.session(database=self.database) as session:
            # Always persist the Expertise node first.
            result = await session.run(upsert_query, **upsert_params)
            summary = await result.consume()
            nodes_created = summary.counters.nodes_created
            logger.debug(f"Expertise node upsert: {nodes_created} node(s) created/merged")

            # Link to Company — warn if Company node missing, but Expertise is still persisted.
            link_result = await session.run(link_query, **link_params)
            link_summary = await link_result.consume()
            rels_created = link_summary.counters.relationships_created
            if rels_created == 0:
                logger.warning(
                    f"Expertise {expertise_id} created but could not link to company {company_id} "
                    f"— Company node not found in Neo4j. Expertise node is persisted."
                )

        logger.info(f"Created/updated Expertise node (company-level): {expertise_id}")

    async def update_expertise_node(
        self, expertise_id: str, when_to_use: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Update Expertise node with COALESCE semantics (only provided fields are updated).

        Args:
            expertise_id: Expertise UUID
            when_to_use: Optional new when_to_use description

        Returns:
            Updated expertise dict with id, title, when_to_use, or None if not found
        """
        await self.connect()

        query = """
        MATCH (e:Expertise {id: $expertise_id})
        SET e.when_to_use = COALESCE($when_to_use, e.when_to_use),
            e.updated_at = datetime()
        RETURN e.id AS id,
               e.title AS title,
               e.when_to_use AS when_to_use,
               e.company_id AS company_id,
               e.project_id AS project_id
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, expertise_id=expertise_id, when_to_use=when_to_use)
            record = await result.single()

            if record:
                logger.info(f"Updated Expertise node: {expertise_id}")
                return {
                    "id": record["id"],
                    "title": record["title"],
                    "when_to_use": record["when_to_use"],
                    "company_id": record["company_id"],
                    "project_id": record["project_id"],
                }

            logger.warning("Expertise not found for update: {}", expertise_id)
            return None

    async def create_expertise_chunk(
        self,
        chunk_id: str,
        expertise_id: str,
        parent_chunk_id: Optional[str],
        content: str,
        summary: str,
        level: int,
        position: int,
        chunk_path: str,
        metadata: Dict[str, Any],
    ) -> None:
        """
        Create ExpertiseChunk node with hierarchy.
        If parent_chunk_id is None: Create (Expertise)-[:HAS_ROOT_CHUNK]->(chunk)
        Else: Create (ParentChunk)-[:HAS_CHILD]->(chunk)

        Args:
            chunk_id: Chunk UUID
            expertise_id: Expertise UUID
            parent_chunk_id: Parent chunk UUID (None for root chunks)
            content: Chunk content
            summary: Chunk summary
            level: Hierarchy level (0=root)
            position: Position within parent
            chunk_path: Path in hierarchy (e.g., "0.1.2")
            metadata: Additional metadata
        """
        await self.connect()

        # Convert metadata to JSON
        metadata_json = json.dumps(metadata)

        if parent_chunk_id is None:
            # Create root chunk
            query = """
            MATCH (e:Expertise {id: $expertise_id})
            MERGE (c:ExpertiseChunk {id: $chunk_id})
            SET c.expertise_id = $expertise_id,
                c.content = $content,
                c.summary = $summary,
                c.level = $level,
                c.position = $position,
                c.chunk_path = $chunk_path,
                c.metadata = $metadata,
                c.created_at = coalesce(c.created_at, datetime()),
                c.updated_at = datetime()
            MERGE (e)-[:HAS_ROOT_CHUNK]->(c)
            """
            params = {
                "expertise_id": expertise_id,
                "chunk_id": chunk_id,
                "content": content,
                "summary": summary,
                "level": level,
                "position": position,
                "chunk_path": chunk_path,
                "metadata": metadata_json,
            }
        else:
            # Create child chunk
            query = """
            MATCH (p:ExpertiseChunk {id: $parent_chunk_id})
            MERGE (c:ExpertiseChunk {id: $chunk_id})
            SET c.expertise_id = $expertise_id,
                c.content = $content,
                c.summary = $summary,
                c.level = $level,
                c.position = $position,
                c.chunk_path = $chunk_path,
                c.metadata = $metadata,
                c.created_at = coalesce(c.created_at, datetime()),
                c.updated_at = datetime()
            MERGE (p)-[:HAS_CHILD]->(c)
            """
            params = {
                "parent_chunk_id": parent_chunk_id,
                "expertise_id": expertise_id,
                "chunk_id": chunk_id,
                "content": content,
                "summary": summary,
                "level": level,
                "position": position,
                "chunk_path": chunk_path,
                "metadata": metadata_json,
            }

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            await result.consume()

        logger.info(f"Created ExpertiseChunk: {chunk_id} (level={level}, parent={parent_chunk_id})")

    async def update_expertise_chunk(
        self, chunk_id: str, content: str, summary: str, metadata: Dict[str, Any]
    ) -> None:
        """
        Update chunk content and metadata.

        Args:
            chunk_id: Chunk UUID
            content: Updated content
            summary: Updated summary
            metadata: Updated metadata
        """
        await self.connect()

        # Convert metadata to JSON
        metadata_json = json.dumps(metadata)

        query = """
        MATCH (c:ExpertiseChunk {id: $chunk_id})
        SET c.content = $content,
            c.summary = $summary,
            c.metadata = $metadata,
            c.updated_at = datetime()
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(
                query, chunk_id=chunk_id, content=content, summary=summary, metadata=metadata_json
            )
            await result.consume()

        logger.info(f"Updated ExpertiseChunk: {chunk_id}")

    async def delete_expertise_chunk(self, chunk_id: str, cascade: bool = True) -> None:
        """
        Delete chunk. If cascade, delete all descendants too.

        Args:
            chunk_id: Chunk UUID
            cascade: If True, delete all child chunks recursively
        """
        await self.connect()

        if cascade:
            # Delete chunk and all descendants
            query = """
            MATCH (c:ExpertiseChunk {id: $chunk_id})
            OPTIONAL MATCH (c)-[:HAS_CHILD*]->(child:ExpertiseChunk)
            DETACH DELETE c, child
            """
        else:
            # Delete only this chunk (orphans children)
            query = """
            MATCH (c:ExpertiseChunk {id: $chunk_id})
            DETACH DELETE c
            """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, chunk_id=chunk_id)
            await result.consume()

        logger.info(f"Deleted ExpertiseChunk: {chunk_id} (cascade={cascade})")

    async def get_chunk_children(self, chunk_id: str) -> List[Dict[str, Any]]:
        """
        Get immediate children of a chunk.

        Args:
            chunk_id: Chunk UUID

        Returns:
            List of child chunk dictionaries
        """
        await self.connect()

        query = """
        MATCH (p:ExpertiseChunk {id: $chunk_id})-[:HAS_CHILD]->(c:ExpertiseChunk)
        RETURN c.id AS id,
               c.expertise_id AS expertise_id,
               c.content AS content,
               c.summary AS summary,
               c.level AS level,
               c.position AS position,
               c.chunk_path AS chunk_path,
               c.metadata AS metadata
        ORDER BY c.position
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, chunk_id=chunk_id)
            records = await result.data()

        logger.info(f"Retrieved {len(records)} children for chunk {chunk_id}")
        return records

    async def get_root_chunks(self, expertise_id: str) -> List[Dict[str, Any]]:
        """
        Get root chunks for expertise.

        Args:
            expertise_id: Expertise UUID

        Returns:
            List of root chunk dictionaries
        """
        await self.connect()

        query = """
        MATCH (e:Expertise {id: $expertise_id})-[:HAS_ROOT_CHUNK]->(c:ExpertiseChunk)
        RETURN c.id AS id,
               c.expertise_id AS expertise_id,
               c.content AS content,
               c.summary AS summary,
               c.level AS level,
               c.position AS position,
               c.chunk_path AS chunk_path,
               c.metadata AS metadata
        ORDER BY c.position
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, expertise_id=expertise_id)
            records = await result.data()

        logger.info(f"Retrieved {len(records)} root chunks for expertise {expertise_id}")
        return records

    async def get_expertise_node(self, expertise_id: str) -> Optional[Dict[str, Any]]:
        """
        Get Expertise node by ID.

        Args:
            expertise_id: Expertise UUID

        Returns:
            Expertise node as dict or None if not found
        """
        await self.connect()

        query = """
        MATCH (e:Expertise {id: $expertise_id})
        RETURN e.id AS id,
               e.company_id AS company_id,
               e.project_id AS project_id,
               e.title AS title,
               e.content AS content,
               e.when_to_use AS when_to_use,
               e.metadata AS metadata,
               e.content_hash AS content_hash,
               e.created_at AS created_at,
               e.updated_at AS updated_at
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, expertise_id=expertise_id)
            record = await result.single()

        if record:
            # Parse metadata JSON
            import json

            metadata_str = record.get("metadata", "{}")
            metadata = json.loads(metadata_str) if metadata_str else {}

            return {
                "id": record["id"],
                "company_id": record.get("company_id"),
                "project_id": record.get("project_id"),
                "title": record.get("title"),
                "content": record.get("content"),
                "when_to_use": record.get("when_to_use"),
                "metadata": metadata,
                "content_hash": record.get("content_hash"),
                "created_at": record.get("created_at"),
                "updated_at": record.get("updated_at"),
            }

        return None

    async def update_expertise_node(
        self, expertise_id: str, when_to_use: Optional[str] = None
    ) -> None:
        """
        Update expertise node metadata.

        Uses COALESCE semantics - only provided fields are updated.

        Args:
            expertise_id: Expertise UUID
            when_to_use: Optional new when_to_use description
        """
        await self.connect()

        query = """
        MATCH (e:Expertise {id: $expertise_id})
        SET e.when_to_use = COALESCE($when_to_use, e.when_to_use),
            e.updated_at = datetime()
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, expertise_id=expertise_id, when_to_use=when_to_use)
            await result.consume()

        logger.info(f"Updated Expertise node: {expertise_id}")

    async def list_expertise_nodes(
        self,
        company_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        List Expertise nodes for a company.

        Expertise is company-wide — all expertise for a company is returned.

        Args:
            company_id: Company UUID (required)
            limit: Maximum results (default: 100)
            offset: Skip first N results (default: 0)

        Returns:
            List of expertise dicts with metadata
        """
        await self.connect()

        if not company_id:
            return []

        # All expertise for the company — filter out lessons (stored as Expertise with lesson_type)
        query = """
        MATCH (e:Expertise {company_id: $company_id})
        WHERE NOT coalesce(e.metadata, '') CONTAINS '"lesson_type"'
        OPTIONAL MATCH (e)-[:HAS_ROOT_CHUNK|HAS_CHILD*]->(c:ExpertiseChunk)
        WITH e, count(DISTINCT c) AS chunks_count
        RETURN e.id AS id,
               e.title AS title,
               e.metadata AS metadata,
               e.content_hash AS content_hash,
               e.created_at AS created_at,
               e.updated_at AS updated_at,
               e.is_company_level AS is_company_level,
               e.when_to_use AS when_to_use,
               chunks_count
        ORDER BY e.created_at DESC
        SKIP $offset
        LIMIT $limit
        """
        params = {"company_id": company_id, "limit": limit, "offset": offset}

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            records = await result.data()

        expertise_list = []
        for record in records:
            import json

            metadata_str = record.get("metadata", "{}")
            metadata = json.loads(metadata_str) if metadata_str else {}

            # Extract summary from metadata or create from title
            summary = metadata.get("summary", record.get("title", "")[:200])

            expertise_list.append(
                {
                    "id": record["id"],
                    "title": record.get("title", ""),
                    "summary": summary,
                    "chunks_count": record.get("chunks_count", 0),
                    "created_at": record.get("created_at"),
                    "updated_at": record.get("updated_at"),
                    "project_id": None,  # expertise is company-wide, never project-scoped
                    "is_company_level": True,
                    "when_to_use": record.get("when_to_use", ""),
                }
            )

        return expertise_list

    async def get_expertise_chunk(self, chunk_id: str) -> Optional[Dict[str, Any]]:
        """
        Get ExpertiseChunk node by ID.

        Args:
            chunk_id: Chunk UUID

        Returns:
            Chunk node as dict or None if not found
        """
        await self.connect()

        query = """
        MATCH (c:ExpertiseChunk {id: $chunk_id})
        RETURN c.id AS id,
               c.expertise_id AS expertise_id,
               c.content AS content,
               c.summary AS summary,
               c.level AS level,
               c.position AS position,
               c.chunk_path AS chunk_path,
               c.metadata AS metadata,
               c.created_at AS created_at,
               c.updated_at AS updated_at
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, chunk_id=chunk_id)
            record = await result.single()

        if record:
            # Parse metadata JSON
            import json

            metadata_str = record.get("metadata", "{}")
            metadata = json.loads(metadata_str) if metadata_str else {}

            return {
                "id": record["id"],
                "expertise_id": record.get("expertise_id"),
                "content": record.get("content"),
                "summary": record.get("summary"),
                "level": record.get("level"),
                "position": record.get("position"),
                "chunk_path": record.get("chunk_path"),
                "metadata": metadata,
                "created_at": record.get("created_at"),
                "updated_at": record.get("updated_at"),
            }

        return None

    async def delete_expertise_node(
        self, expertise_id: str, company_id: Optional[str] = None
    ) -> None:
        """Delete Expertise node and all related chunks (multi-tenant safe)."""
        await self.connect()

        # If company_id provided, use it for safety
        if company_id:
            query = """
            MATCH (e:Expertise {id: $expertise_id, company_id: $company_id})
            OPTIONAL MATCH (e)-[:HAS_ROOT_CHUNK]->(root:ExpertiseChunk)
            OPTIONAL MATCH (root)-[:HAS_CHILD*]->(child:ExpertiseChunk)
            DETACH DELETE e, root, child
            """
            params = {"expertise_id": expertise_id, "company_id": company_id}
        else:
            query = """
            MATCH (e:Expertise {id: $expertise_id})
            OPTIONAL MATCH (e)-[:HAS_ROOT_CHUNK]->(root:ExpertiseChunk)
            OPTIONAL MATCH (root)-[:HAS_CHILD*]->(child:ExpertiseChunk)
            DETACH DELETE e, root, child
            """
            params = {"expertise_id": expertise_id}

        async with self.driver.session(database=self.database) as session:
            await session.run(query, **params)

        logger.info(f"Deleted Expertise node and related chunks: {expertise_id}")

    async def create_entry_node(
        self,
        entry_id: str,
        engagement_id: str,
        entry_type: str,
        title: str,
        content: str,
        company_id: str,
        project_id: str,
        agent_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        references: Optional[List[str]] = None,
    ) -> None:
        """
        Create Entry node in Neo4j and link to Engagement.

        Creates an Entry node with all properties and establishes:
        - (Entry)-[:BELONGS_TO]->(Engagement) relationship
        - (Entry)-[:CREATED_BY]->(Agent) relationship (if agent_id provided)
        - (Entry)-[:REFERENCES]->(Entry) relationships (for each reference)

        Args:
            entry_id: Entry UUID
            engagement_id: Engagement UUID to link to
            entry_type: Type of entry (requirement, insight, decision, plan, note, question)
            title: Entry title
            content: Full entry content
            company_id: Company UUID for multi-tenant isolation
            project_id: Project UUID
            agent_id: Optional Agent UUID who created this entry
            tags: Optional list of tags for categorization
            references: Optional list of entry IDs this entry references
        """
        await self.connect()

        async with self.driver.session(database=self.database) as session:
            # Base query: Create Entry node and link to Engagement
            query = """
            MATCH (e:Engagement {id: $engagement_id})
            CREATE (en:Entry {
                id: $entry_id,
                entry_type: $entry_type,
                title: $title,
                content: $content,
                company_id: $company_id,
                project_id: $project_id,
                tags: $tags,
                created_at: datetime()
            })
            CREATE (en)-[:BELONGS_TO]->(e)
            """

            result = await session.run(
                query,
                entry_id=entry_id,
                engagement_id=engagement_id,
                entry_type=entry_type,
                title=title,
                content=content,
                company_id=company_id,
                project_id=project_id,
                tags=tags or [],
            )
            await result.consume()

            # If agent_id provided, create CREATED_BY relationship
            if agent_id:
                agent_query = """
                MATCH (en:Entry {id: $entry_id})
                MATCH (a:Agent {id: $agent_id})
                CREATE (en)-[:CREATED_BY]->(a)
                """
                agent_result = await session.run(agent_query, entry_id=entry_id, agent_id=agent_id)
                await agent_result.consume()

            # Create REFERENCES relationships for each referenced entry
            if references:
                for ref_id in references:
                    ref_query = """
                    MATCH (en:Entry {id: $entry_id})
                    MATCH (ref:Entry {id: $ref_id})
                    CREATE (en)-[:REFERENCES]->(ref)
                    """
                    ref_result = await session.run(ref_query, entry_id=entry_id, ref_id=ref_id)
                    await ref_result.consume()

        logger.info(f"Created Entry node: {entry_id} linked to Engagement: {engagement_id}")

    async def update_entry_node(
        self,
        entry_id: str,
        tags: Optional[List[str]] = None,
        references: Optional[List[str]] = None,
    ) -> None:
        """
        Update Entry node properties and references in Neo4j.

        Args:
            entry_id: Entry UUID
            tags: Optional new list of tags
            references: Optional new list of entry IDs to reference
        """
        await self.connect()

        async with self.driver.session(database=self.database) as session:
            # Update tags if provided
            if tags is not None:
                query = """
                MATCH (en:Entry {id: $entry_id})
                SET en.tags = $tags
                """
                result = await session.run(query, entry_id=entry_id, tags=tags)
                await result.consume()

            # Update references if provided
            if references is not None:
                # Remove existing REFERENCES relationships
                delete_query = """
                MATCH (en:Entry {id: $entry_id})-[r:REFERENCES]->()
                DELETE r
                """
                delete_result = await session.run(delete_query, entry_id=entry_id)
                await delete_result.consume()

                # Create new REFERENCES relationships
                for ref_id in references:
                    ref_query = """
                    MATCH (en:Entry {id: $entry_id})
                    MATCH (ref:Entry {id: $ref_id})
                    CREATE (en)-[:REFERENCES]->(ref)
                    """
                    ref_result = await session.run(ref_query, entry_id=entry_id, ref_id=ref_id)
                    await ref_result.consume()

        logger.info(f"Updated Entry node: {entry_id}")

    async def execute_custom_cypher(
        self, cypher: str, params: Optional[Dict[str, Any]] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Execute custom read-only Cypher query with security validation.

        SECURITY: All queries are validated to prevent injection attacks.
        Only read-only operations allowed (MATCH, RETURN, WHERE, WITH, etc.).
        Write operations (CREATE, DELETE, MERGE, SET, etc.) are BLOCKED.

        Args:
            cypher: Cypher query string (read-only)
            params: Optional query parameters
            limit: Maximum results to return (default: 100, max: 1000)

        Returns:
            List of result dictionaries

        Raises:
            ValueError: If query validation fails
            Exception: If query execution fails
        """
        from kgrag.cypher_validator import CypherQueryValidator

        # Validate query for read-only operations
        is_valid, error_message = CypherQueryValidator.validate(cypher)
        if not is_valid:
            logger.error("Cypher query validation failed: {}", error_message)
            raise ValueError(f"Invalid Cypher query: {error_message}")

        # Add/cap LIMIT clause
        cypher_with_limit = CypherQueryValidator.sanitize_limit(cypher, default_limit=limit)

        logger.info(f"Executing custom Cypher query: {cypher_with_limit[:200]}")

        # Ensure driver is connected before use
        await self.connect()

        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(cypher_with_limit, **(params or {}))
                records = await result.data()

                logger.info(f"Custom Cypher query returned {len(records)} results")
                return records

        except Exception as e:
            logger.error("Custom Cypher query execution failed: {}", e, exc_info=True)
            raise RuntimeError(
                f"Cypher query execution failed: {getattr(e, 'message', str(e))}"
            ) from e

    # ─── Memory Graph Operations ────────────────────────────────────

    async def create_memory_node(
        self,
        memory_id: str,
        agent_id: str,
        company_id: str,
        memory_type: str,
        title: str,
        content: str,
        importance: int = 3,
        project_id: Optional[str] = None,
        user_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        category: Optional[str] = None,
        source_origin: Optional[str] = None,
        source: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> str:
        """
        Create Memory node and link to Agent.

        Creates a Memory node with all properties and establishes:
        - (Agent)-[:HAS_MEMORY]->(Memory) relationship (REQUIRED)
        - (Memory)-[:RELATES_TO_USER]->(User) relationship (if user_id provided)
        - (Memory)-[:DERIVED_FROM]->(Entry) relationship (if source is entry_id)

        Args:
            memory_id: Memory UUID
            agent_id: Agent UUID (REQUIRED - memory belongs to agent)
            company_id: Company UUID for multi-tenant isolation
            memory_type: Type of memory (fact, preference, experience, context, relationship)
            title: Short descriptive title
            content: Full memory content
            importance: Integer 1-5 for search ranking (default: 3)
            project_id: Optional project UUID
            user_id: Optional user UUID the memory relates to
            tags: Optional list of tags for categorization
            category: Optional classification (good_practice, bad_practice, pattern, etc.)
            source_origin: How memory was created (user_explicit, agent_inferred, auto_captured)
            source: Source entity ID (engagement_id, entry_id, etc.)
            source_type: Source entity type (engagement, entry, conversation)

        Returns:
            memory_id of the created memory
        """
        await self.connect()

        async with self.driver.session(database=self.database) as session:
            # Create Memory node and link to Agent
            query = """
            MATCH (a:Agent {id: $agent_id})
            CREATE (m:Memory {
                id: $memory_id,
                company_id: $company_id,
                project_id: $project_id,
                memory_type: $memory_type,
                title: $title,
                content: $content,
                tags: $tags,
                category: $category,
                importance: $importance,
                source_origin: $source_origin,
                source: $source,
                source_type: $source_type,
                created_at: datetime(),
                updated_at: datetime(),
                last_accessed_at: datetime()
            })
            CREATE (a)-[:HAS_MEMORY]->(m)
            RETURN m.id AS memory_id
            """

            result = await session.run(
                query,
                memory_id=memory_id,
                agent_id=agent_id,
                company_id=company_id,
                project_id=project_id,
                memory_type=memory_type,
                title=title,
                content=content,
                tags=tags or [],
                category=category,
                importance=importance,
                source_origin=source_origin,
                source=source,
                source_type=source_type,
            )
            record = await result.single()

            if not record:
                raise Exception(
                    f"Failed to create Memory node {memory_id} - "
                    f"Agent node {agent_id} may not exist"
                )

            # Optional: Link to User if user_id provided
            if user_id:
                user_query = """
                MATCH (m:Memory {id: $memory_id})
                MATCH (u:User {id: $user_id})
                CREATE (m)-[:RELATES_TO_USER]->(u)
                """
                await session.run(user_query, memory_id=memory_id, user_id=user_id)

            # Optional: Link to Entry if source_type is 'entry'
            if source and source_type == "entry":
                entry_query = """
                MATCH (m:Memory {id: $memory_id})
                MATCH (e:Entry {id: $source})
                CREATE (m)-[:DERIVED_FROM]->(e)
                """
                await session.run(entry_query, memory_id=memory_id, source=source)

        logger.info(f"Created Memory node: {memory_id} for Agent: {agent_id}")
        return memory_id

    async def get_agent_memories(
        self,
        agent_id: str,
        company_id: str,
        user_id: Optional[str] = None,
        category: Optional[str] = None,
        min_importance: Optional[int] = None,
        memory_types: Optional[List[str]] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get memories for an agent with optional filters.

        Args:
            agent_id: Agent UUID
            company_id: Company UUID for multi-tenant filtering
            user_id: Optional filter for user-specific memories
            category: Optional filter by category
            min_importance: Optional minimum importance (integer 1-5)
            memory_types: Optional list of memory types to filter
            limit: Maximum results (default: 50)

        Returns:
            List of memory dictionaries
        """
        await self.connect()

        # Build dynamic WHERE clauses
        where_clauses = ["m.company_id = $company_id"]
        params = {"agent_id": agent_id, "company_id": company_id, "limit": limit}

        if user_id:
            where_clauses.append("EXISTS((m)-[:RELATES_TO_USER]->(:User {id: $user_id}))")
            params["user_id"] = user_id

        if category:
            where_clauses.append("m.category = $category")
            params["category"] = category

        if min_importance:
            where_clauses.append("m.importance >= $min_importance")
            params["min_importance"] = min_importance

        if memory_types:
            where_clauses.append("m.memory_type IN $memory_types")
            params["memory_types"] = memory_types

        where_clause = " AND ".join(where_clauses)

        query = f"""
        MATCH (a:Agent {{id: $agent_id}})-[:HAS_MEMORY]->(m:Memory)
        WHERE {where_clause}
        RETURN m.id AS id,
               m.company_id AS company_id,
               m.project_id AS project_id,
               m.memory_type AS memory_type,
               m.title AS title,
               m.content AS content,
               m.tags AS tags,
               m.category AS category,
               m.importance AS importance,
               m.source_origin AS source_origin,
               m.source AS source,
               m.source_type AS source_type,
               m.created_at AS created_at,
               m.updated_at AS updated_at,
               m.last_accessed_at AS last_accessed_at
        ORDER BY m.importance DESC, m.created_at DESC
        LIMIT $limit
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            records = await result.data()

        logger.info(f"Retrieved {len(records)} memories for agent {agent_id}")
        return records

    async def get_memory_by_id(
        self, memory_id: str, company_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get a single memory by ID with optional company check.

        Also updates last_accessed_at timestamp.

        Args:
            memory_id: Memory UUID
            company_id: Optional company UUID for multi-tenant safety

        Returns:
            Memory dict or None if not found
        """
        await self.connect()

        if company_id:
            query = """
            MATCH (m:Memory {id: $memory_id, company_id: $company_id})
            SET m.last_accessed_at = datetime()
            RETURN m.id AS id,
                   m.company_id AS company_id,
                   m.project_id AS project_id,
                   m.memory_type AS memory_type,
                   m.title AS title,
                   m.content AS content,
                   m.tags AS tags,
                   m.category AS category,
                   m.importance AS importance,
                   m.source_origin AS source_origin,
                   m.source AS source,
                   m.source_type AS source_type,
                   m.created_at AS created_at,
                   m.updated_at AS updated_at,
                   m.last_accessed_at AS last_accessed_at
            """
            params = {"memory_id": memory_id, "company_id": company_id}
        else:
            query = """
            MATCH (m:Memory {id: $memory_id})
            SET m.last_accessed_at = datetime()
            RETURN m.id AS id,
                   m.company_id AS company_id,
                   m.project_id AS project_id,
                   m.memory_type AS memory_type,
                   m.title AS title,
                   m.content AS content,
                   m.tags AS tags,
                   m.category AS category,
                   m.importance AS importance,
                   m.source_origin AS source_origin,
                   m.source AS source,
                   m.source_type AS source_type,
                   m.created_at AS created_at,
                   m.updated_at AS updated_at,
                   m.last_accessed_at AS last_accessed_at
            """
            params = {"memory_id": memory_id}

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            record = await result.single()

        if record:
            return dict(record)
        return None

    async def update_memory_node(
        self,
        memory_id: str,
        company_id: Optional[str] = None,
        title: Optional[str] = None,
        content: Optional[str] = None,
        tags: Optional[List[str]] = None,
        category: Optional[str] = None,
        importance: Optional[int] = None,
    ) -> bool:
        """
        Update Memory node properties.

        Args:
            memory_id: Memory UUID
            company_id: Optional company UUID for multi-tenant safety
            title: Optional new title
            content: Optional new content
            tags: Optional new tags list
            category: Optional new category
            importance: Optional new importance (1-5)

        Returns:
            True if updated, False if memory not found
        """
        await self.connect()

        # Build SET clauses dynamically
        set_clauses = ["m.updated_at = datetime()"]
        params = {"memory_id": memory_id}

        if company_id:
            params["company_id"] = company_id

        if title is not None:
            set_clauses.append("m.title = $title")
            params["title"] = title

        if content is not None:
            set_clauses.append("m.content = $content")
            params["content"] = content

        if tags is not None:
            set_clauses.append("m.tags = $tags")
            params["tags"] = tags

        if category is not None:
            set_clauses.append("m.category = $category")
            params["category"] = category

        if importance is not None:
            set_clauses.append("m.importance = $importance")
            params["importance"] = importance

        set_clause = ", ".join(set_clauses)

        if company_id:
            query = f"""
            MATCH (m:Memory {{id: $memory_id, company_id: $company_id}})
            SET {set_clause}
            RETURN m.id AS id
            """
        else:
            query = f"""
            MATCH (m:Memory {{id: $memory_id}})
            SET {set_clause}
            RETURN m.id AS id
            """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            record = await result.single()

        if record:
            logger.info(f"Updated Memory node: {memory_id}")
            return True
        return False

    async def delete_memory_node(self, memory_id: str, company_id: Optional[str] = None) -> bool:
        """
        Delete Memory node and all its relationships.

        Args:
            memory_id: Memory UUID
            company_id: Optional company UUID for multi-tenant safety

        Returns:
            True if deleted, False if memory not found
        """
        await self.connect()

        if company_id:
            query = """
            MATCH (m:Memory {id: $memory_id, company_id: $company_id})
            DETACH DELETE m
            RETURN count(m) AS deleted
            """
            params = {"memory_id": memory_id, "company_id": company_id}
        else:
            query = """
            MATCH (m:Memory {id: $memory_id})
            DETACH DELETE m
            RETURN count(m) AS deleted
            """
            params = {"memory_id": memory_id}

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            record = await result.single()

        deleted = record["deleted"] if record else 0
        if deleted > 0:
            logger.info(f"Deleted Memory node: {memory_id}")
            return True
        return False

    async def link_related_memories(
        self, memory_id_1: str, memory_id_2: str, company_id: Optional[str] = None
    ) -> bool:
        """
        Create RELATED_TO relationship between two memories.

        Args:
            memory_id_1: First memory UUID
            memory_id_2: Second memory UUID
            company_id: Optional company UUID for multi-tenant safety

        Returns:
            True if linked, False if either memory not found
        """
        await self.connect()

        if company_id:
            query = """
            MATCH (m1:Memory {id: $memory_id_1, company_id: $company_id})
            MATCH (m2:Memory {id: $memory_id_2, company_id: $company_id})
            MERGE (m1)-[r:RELATED_TO]->(m2)
            ON CREATE SET r.created_at = datetime()
            RETURN m1.id AS id1, m2.id AS id2
            """
            params = {
                "memory_id_1": memory_id_1,
                "memory_id_2": memory_id_2,
                "company_id": company_id,
            }
        else:
            query = """
            MATCH (m1:Memory {id: $memory_id_1})
            MATCH (m2:Memory {id: $memory_id_2})
            MERGE (m1)-[r:RELATED_TO]->(m2)
            ON CREATE SET r.created_at = datetime()
            RETURN m1.id AS id1, m2.id AS id2
            """
            params = {"memory_id_1": memory_id_1, "memory_id_2": memory_id_2}

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            record = await result.single()

        if record:
            logger.info(f"Linked memories: {memory_id_1} <-> {memory_id_2}")
            return True
        return False

    async def get_related_memories(
        self, memory_id: str, company_id: Optional[str] = None, max_depth: int = 2, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get memories related to a given memory via graph traversal.

        Traverses RELATED_TO relationships up to max_depth hops.

        Args:
            memory_id: Starting memory UUID
            company_id: Optional company UUID for multi-tenant safety
            max_depth: Maximum relationship hops (default: 2)
            limit: Maximum results (default: 20)

        Returns:
            List of related memory dictionaries with depth info
        """
        await self.connect()

        if company_id:
            query = f"""
            MATCH (m:Memory {{id: $memory_id, company_id: $company_id}})
            MATCH path = (m)-[:RELATED_TO*1..{max_depth}]-(related:Memory)
            WHERE related.company_id = $company_id AND related.id <> $memory_id
            WITH DISTINCT related, length(path) AS depth
            RETURN related.id AS id,
                   related.company_id AS company_id,
                   related.project_id AS project_id,
                   related.memory_type AS memory_type,
                   related.title AS title,
                   related.content AS content,
                   related.tags AS tags,
                   related.category AS category,
                   related.importance AS importance,
                   related.created_at AS created_at,
                   depth
            ORDER BY depth ASC, related.importance DESC
            LIMIT $limit
            """
            params = {"memory_id": memory_id, "company_id": company_id, "limit": limit}
        else:
            query = f"""
            MATCH (m:Memory {{id: $memory_id}})
            MATCH path = (m)-[:RELATED_TO*1..{max_depth}]-(related:Memory)
            WHERE related.id <> $memory_id
            WITH DISTINCT related, length(path) AS depth
            RETURN related.id AS id,
                   related.company_id AS company_id,
                   related.project_id AS project_id,
                   related.memory_type AS memory_type,
                   related.title AS title,
                   related.content AS content,
                   related.tags AS tags,
                   related.category AS category,
                   related.importance AS importance,
                   related.created_at AS created_at,
                   depth
            ORDER BY depth ASC, related.importance DESC
            LIMIT $limit
            """
            params = {"memory_id": memory_id, "limit": limit}

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            records = await result.data()

        logger.info(f"Retrieved {len(records)} related memories for {memory_id}")
        return records

    # ─── Statistics Count Operations ───────────────────────────────

    async def count_by_company(self, company_id: str) -> Dict[str, int]:
        """
        Count graph entities for a company using optimized CALL subqueries.

        Uses CALL subqueries with labeled nodes to avoid full node scans.
        Each CALL block is independent and uses label indexes.

        Args:
            company_id: Company UUID

        Returns:
            Dict with counts: {files, entities, relationships}

        Raises:
            Exception: If count operation fails
        """
        await self.connect()

        query = """
        // Subquery 1: Count File nodes (uses :File label index)
        CALL {
            MATCH (f:File {company_id: $company_id})
            RETURN count(f) AS file_count
        }
        // Subquery 2: Count Entity nodes (uses :Entity label index)
        CALL {
            MATCH (e:Entity {company_id: $company_id})
            RETURN count(e) AS entity_count
        }
        // Subquery 3: Count relationships FROM labeled nodes only
        CALL {
            MATCH (:File {company_id: $company_id})-[r1]->()
            RETURN count(r1) AS file_rels
        }
        CALL {
            MATCH (:Entity {company_id: $company_id})-[r2]->()
            RETURN count(r2) AS entity_rels
        }
        // Combine results
        RETURN file_count, entity_count, (file_rels + entity_rels) AS rel_count
        """

        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(query, company_id=company_id)
                record = await result.single()

            if record:
                counts = {
                    "files": record["file_count"],
                    "entities": record["entity_count"],
                    "relationships": record["rel_count"],
                }
            else:
                counts = {"files": 0, "entities": 0, "relationships": 0}

            logger.debug(f"Neo4j count by company {company_id}: {counts}")
            return counts

        except Exception as e:
            logger.error("Neo4j count by company {} failed: {}", company_id, e, exc_info=True)
            raise

    async def count_by_project(self, project_id: str) -> Dict[str, int]:
        """
        Count graph entities for a project using optimized CALL subqueries.

        Uses CALL subqueries with labeled nodes to avoid full node scans.
        Each CALL block is independent and uses label indexes.

        Args:
            project_id: Project UUID

        Returns:
            Dict with counts: {files, entities, relationships}

        Raises:
            Exception: If count operation fails
        """
        await self.connect()

        query = """
        // Subquery 1: Count File nodes (uses :File label index)
        CALL {
            MATCH (f:File {project_id: $project_id})
            RETURN count(f) AS file_count
        }
        // Subquery 2: Count Entity nodes (uses :Entity label index)
        CALL {
            MATCH (e:Entity {project_id: $project_id})
            RETURN count(e) AS entity_count
        }
        // Subquery 3: Count relationships FROM labeled nodes only
        CALL {
            MATCH (:File {project_id: $project_id})-[r1]->()
            RETURN count(r1) AS file_rels
        }
        CALL {
            MATCH (:Entity {project_id: $project_id})-[r2]->()
            RETURN count(r2) AS entity_rels
        }
        // Combine results
        RETURN file_count, entity_count, (file_rels + entity_rels) AS rel_count
        """

        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(query, project_id=project_id)
                record = await result.single()

            if record:
                counts = {
                    "files": record["file_count"],
                    "entities": record["entity_count"],
                    "relationships": record["rel_count"],
                }
            else:
                counts = {"files": 0, "entities": 0, "relationships": 0}

            logger.debug(f"Neo4j count by project {project_id}: {counts}")
            return counts

        except Exception as e:
            logger.error("Neo4j count by project {} failed: {}", project_id, e, exc_info=True)
            raise

    # ─── Generic Cypher Execution Methods ────────────────────────────────────

    async def execute_write(self, cypher: str, **params) -> None:
        """
        Execute a write Cypher query (CREATE, MERGE, SET, DELETE).

        Generic method for ad-hoc write operations when a specific
        method doesn't exist.

        Args:
            cypher: Cypher query string
            **params: Query parameters

        Raises:
            Exception: If query execution fails
        """
        await self.connect()

        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(cypher, **params)
                await result.consume()

            logger.debug(f"Executed write query: {cypher[:100]}...")

        except Exception as e:
            logger.error("Write query failed: {}", e, exc_info=True)
            raise RuntimeError(f"Write query failed: {e}") from e

    async def execute_read(self, cypher: str, **params) -> List[Dict[str, Any]]:
        """
        Execute a read Cypher query (MATCH, RETURN).

        Generic method for ad-hoc read operations when a specific
        method doesn't exist.

        Args:
            cypher: Cypher query string
            **params: Query parameters

        Returns:
            List of result dictionaries

        Raises:
            Exception: If query execution fails
        """
        await self.connect()

        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(cypher, **params)
                records = await result.data()

            logger.debug(f"Read query returned {len(records)} records")
            return records

        except Exception as e:
            logger.error("Read query failed: {}", e, exc_info=True)
            raise RuntimeError(f"Read query failed: {e}") from e

    async def get_expertise_agents(self, expertise_id: str) -> List[Dict[str, str]]:
        """
        Get agents assigned to an expertise (reverse lookup).

        Args:
            expertise_id: Expertise UUID

        Returns:
            List of agent dicts with agent_id, name, role
        """
        await self.connect()

        query = """
        MATCH (a:Agent)-[:HAS_SKILL]->(e:Expertise {id: $expertise_id})
        RETURN a.id as agent_id, a.name as name, a.role as role
        ORDER BY a.name
        """

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, expertise_id=expertise_id)
            records = await result.data()

        agents = []
        for record in records:
            agents.append(
                {
                    "agent_id": record["agent_id"],
                    "name": record.get("name", ""),
                    "role": record.get("role", ""),
                }
            )

        logger.debug(f"Found {len(agents)} agents for expertise {expertise_id}")
        return agents

    async def list_expertise_with_agents(
        self,
        limit: int = 100,
        offset: int = 0,
        project_id: Optional[
            str
        ] = None,  # kept for API compat; ignored — expertise is company-wide
        company_id: Optional[str] = None,
        include_company_level: bool = True,  # kept for API compat; expertise is always company-wide
        include_project_scoped_for_company: bool = False,  # kept for API compat; no-op
    ) -> List[Dict[str, Any]]:
        """
        List Expertise nodes with assigned agents.

        Expertise is company-wide — all expertise for a company is returned with agent
        assignments. project_id, include_company_level, and include_project_scoped_for_company
        parameters are accepted for backwards compatibility but are ignored.

        Args:
            limit: Maximum results (default: 100)
            offset: Skip first N results (default: 0)
            project_id: Ignored — expertise is company-wide
            company_id: Company UUID (required)
            include_company_level: Ignored — expertise is always company-wide
            include_project_scoped_for_company: Ignored — expertise is always company-wide

        Returns:
            List of expertise dicts with agent assignments
        """
        await self.connect()

        if not company_id:
            return []

        # All expertise for the company with agents — filter out lessons
        query = """
        MATCH (e:Expertise {company_id: $company_id})
        WHERE NOT coalesce(e.metadata, '') CONTAINS '"lesson_type"'
        OPTIONAL MATCH (e)-[:HAS_ROOT_CHUNK|HAS_CHILD*]->(c:ExpertiseChunk)
        OPTIONAL MATCH (a:Agent)-[:HAS_SKILL]->(e)
        WITH e, count(DISTINCT c) AS chunks_count,
             collect(DISTINCT CASE WHEN a IS NOT NULL THEN {agent_id: a.id, name: a.name, role: a.role} END) AS agents_raw
        WITH e, chunks_count, [x IN agents_raw WHERE x IS NOT NULL] AS agents
        RETURN e.id AS id,
               e.title AS title,
               e.metadata AS metadata,
               e.content AS content,
               e.content_hash AS content_hash,
               e.created_at AS created_at,
               e.updated_at AS updated_at,
               e.company_id AS company_id,
               e.is_company_level AS is_company_level,
               e.when_to_use AS when_to_use,
               chunks_count,
               agents
        ORDER BY e.created_at DESC
        SKIP $offset
        LIMIT $limit
        """
        params = {"company_id": company_id, "limit": limit, "offset": offset}

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            records = await result.data()

        expertise_list = []
        for record in records:
            import json

            metadata_str = record.get("metadata", "{}")
            metadata = json.loads(metadata_str) if metadata_str else {}

            # Extract summary from content (first 200 chars)
            content = record.get("content", "")
            summary = content[:200] if content else record.get("title", "")[:200]

            expertise_list.append(
                {
                    "id": record["id"],
                    "title": record.get("title", ""),
                    "summary": summary,
                    "chunks_count": record.get("chunks_count", 0),
                    "created_at": record.get("created_at"),
                    "updated_at": record.get("updated_at"),
                    "project_id": None,  # expertise is company-wide, never project-scoped
                    "company_id": record.get("company_id"),
                    "is_company_level": True,
                    "when_to_use": record.get("when_to_use", ""),
                    "assigned_agents": record.get("agents", []),
                }
            )

        return expertise_list

    async def count_expertise(
        self,
        project_id: Optional[
            str
        ] = None,  # kept for API compat; ignored — expertise is company-wide
        company_id: Optional[str] = None,
        include_company_level: bool = True,  # kept for API compat; expertise is always company-wide
    ) -> int:
        """
        Count expertise nodes for a company.

        Expertise is company-wide — counts all expertise for the company.
        project_id and include_company_level parameters are accepted for backwards
        compatibility but are ignored.

        Args:
            project_id: Ignored — expertise is company-wide
            company_id: Company UUID (required)
            include_company_level: Ignored — expertise is always company-wide

        Returns:
            Total count of expertise nodes
        """
        await self.connect()

        if not company_id:
            return 0

        # Count all company expertise — filter out lessons
        query = """
        MATCH (e:Expertise {company_id: $company_id})
        WHERE NOT coalesce(e.metadata, '') CONTAINS '"lesson_type"'
        RETURN count(e) AS total
        """
        params = {"company_id": company_id}

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, **params)
            record = await result.single()

        return record["total"] if record else 0

"""Storage orchestrator for Neo4j and Qdrant integration.

Handles storing code analysis results in both graph and vector databases
with proper multi-tenant scoping and embedding generation.
"""

import hashlib
import time
import uuid
from typing import Any

from loguru import logger
from kgrag.embeddings import GeminiEmbedder

from .schema import CodeAnalysisResult, StorageStats


class StorageOrchestrator:
    """Orchestrates storage of code analysis results across databases."""

    def __init__(
        self,
        neo4j_repository: Any,  # Neo4jRepository
        qdrant_repository: Any,  # QdrantRepository
    ):
        """Initialize storage orchestrator.

        Args:
            neo4j_repository: Neo4j repository instance
            qdrant_repository: Qdrant repository instance
        """
        self.neo4j_repo = neo4j_repository
        self.qdrant_repo = qdrant_repository
        self.embedder = GeminiEmbedder()

    async def store_single_result(
        self,
        result: CodeAnalysisResult,
        project_id: str,
        company_id: str,
        user_id: str,
        collection_name: str | None = None,
    ) -> StorageStats:
        """Store single analysis result immediately (for streaming).

        Args:
            result: Single code analysis result
            project_id: Project UUID
            company_id: Company UUID
            user_id: User UUID
            collection_name: Qdrant collection (optional, auto-generated)

        Returns:
            Storage stats for this single result
        """
        start_time = time.time()
        stats = StorageStats()

        # Auto-generate collection name if needed
        if not collection_name:
            collection_name = f"company_{company_id}"

        try:
            # Store in Neo4j
            neo4j_stats = await self._store_in_neo4j(
                result=result,
                project_id=project_id,
                company_id=company_id,
            )
            stats.neo4j_nodes_created = neo4j_stats["nodes"]
            stats.neo4j_relationships_created = neo4j_stats["relationships"]

            # Store in Qdrant
            qdrant_stats = await self._store_in_qdrant(
                result=result,
                company_id=company_id,
                project_id=project_id,
                user_id=user_id,
                collection_name=collection_name,
            )
            stats.qdrant_points_created = qdrant_stats["points"]

        except Exception as e:
            logger.error("Failed to store {}: {}", result.file_metadata.file_path, e)
            raise

        stats.duration_seconds = time.time() - start_time
        return stats

    async def store_analysis_results(
        self,
        results: list[CodeAnalysisResult],
        project_id: str,
        company_id: str,
        user_id: str,
    ) -> StorageStats:
        """Store multiple analysis results in Neo4j and Qdrant.

        Args:
            results: List of code analysis results
            project_id: Project UUID
            company_id: Company UUID
            user_id: User UUID

        Returns:
            Storage statistics
        """
        start_time = time.time()
        stats = StorageStats()

        logger.info(
            f"Storing {len(results)} analysis results "
            f"(project: {project_id}, company: {company_id})"
        )

        # Ensure Qdrant collection exists
        collection_name = f"company_{company_id}"
        await self.qdrant_repo.create_collection(
            collection_name=collection_name,
            vector_size=768
        )

        # Process each result
        for result in results:
            try:
                # Store in Neo4j
                neo4j_stats = await self._store_in_neo4j(
                    result=result,
                    project_id=project_id,
                    company_id=company_id,
                )
                stats.neo4j_nodes_created += neo4j_stats["nodes"]
                stats.neo4j_relationships_created += neo4j_stats["relationships"]

                # Generate embeddings and store in Qdrant
                qdrant_stats = await self._store_in_qdrant(
                    result=result,
                    company_id=company_id,
                    project_id=project_id,
                    user_id=user_id,
                    collection_name=collection_name,
                )
                stats.qdrant_points_created += qdrant_stats["points"]

            except Exception as e:
                logger.error(
                    f"Failed to store {result.file_metadata.file_path}: {e}"
                )
                raise

        stats.duration_seconds = time.time() - start_time

        logger.success(
            f"Storage complete: {stats.neo4j_nodes_created} nodes, "
            f"{stats.neo4j_relationships_created} relationships, "
            f"{stats.qdrant_points_created} vectors in {stats.duration_seconds:.2f}s"
        )

        return stats

    async def _create_directory_hierarchy(
        self,
        directory_path: str,
        project_id: str,
        company_id: str,
    ) -> None:
        """Create Directory nodes for folder hierarchy (KISS: simple, idempotent).

        Args:
            directory_path: Directory path (e.g., "api/services")
            project_id: Project UUID
            company_id: Company UUID
        """
        if not directory_path or directory_path in [".", ""]:
            return  # Root project, no directory needed

        parts = directory_path.split("/")

        # Create each directory level (YAGNI: only path, name, depth - no extra metadata)
        for i in range(len(parts)):
            current_path = "/".join(parts[:i+1])
            parent_path = "/".join(parts[:i]) if i > 0 else None

            # Deterministic ID (DRY: reuse uuid5 pattern)
            dir_id = str(uuid.uuid5(
                uuid.NAMESPACE_DNS,
                f"{company_id}:{project_id}:{current_path}"
            ))

            if parent_path:
                # Link to parent directory
                parent_id = str(uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    f"{company_id}:{project_id}:{parent_path}"
                ))

                query = """
                MATCH (parent:Directory {id: $parent_id})
                MERGE (d:Directory {id: $dir_id})
                ON CREATE SET
                    d.path = $path,
                    d.name = $name,
                    d.depth = $depth,
                    d.company_id = $company_id,
                    d.project_id = $project_id,
                    d.created_at = datetime()
                MERGE (parent)-[:HAS_SUBDIRECTORY]->(d)
                """

                async with self.neo4j_repo.driver.session(database=self.neo4j_repo.database) as session:
                    await session.run(
                        query,
                        dir_id=dir_id,
                        parent_id=parent_id,
                        path=current_path,
                        name=parts[i],
                        depth=i + 1,
                        company_id=company_id,
                        project_id=project_id,
                    )
            else:
                # Root directory - link to Project
                query = """
                MATCH (p:Project {id: $project_id})
                MERGE (d:Directory {id: $dir_id})
                ON CREATE SET
                    d.path = $path,
                    d.name = $name,
                    d.depth = 1,
                    d.company_id = $company_id,
                    d.project_id = $project_id,
                    d.created_at = datetime()
                MERGE (p)-[:HAS_DIRECTORY]->(d)
                """

                async with self.neo4j_repo.driver.session(database=self.neo4j_repo.database) as session:
                    await session.run(
                        query,
                        dir_id=dir_id,
                        project_id=project_id,
                        path=current_path,
                        name=parts[i],
                        company_id=company_id,
                    )

    async def _store_in_neo4j(
        self,
        result: CodeAnalysisResult,
        project_id: str,
        company_id: str,
    ) -> dict[str, int]:
        """Store code analysis in Neo4j graph.

        Creates:
        - :Directory nodes (folder hierarchy)
        - :Code node (file-level)
        - :Entity nodes (classes, functions, etc.)
        - Relationships between entities

        Args:
            result: Code analysis result
            project_id: Project UUID
            company_id: Company UUID

        Returns:
            Dict with node and relationship counts
        """
        await self.neo4j_repo.connect()

        # Use deterministic UUID based on file_path (idempotent)
        file_id = str(uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{company_id}:{project_id}:{result.file_metadata.file_path}"
        ))
        metadata = result.file_metadata

        # Extract filename and directory (KISS: use pathlib)
        from pathlib import Path
        file_path_obj = Path(metadata.file_path)
        filename = file_path_obj.name
        directory_path = str(file_path_obj.parent) if file_path_obj.parent != Path(".") else ""

        # Create directory hierarchy first (idempotent)
        if directory_path:
            await self._create_directory_hierarchy(
                directory_path=directory_path,
                project_id=project_id,
                company_id=company_id,
            )

        # Create/Update Code node with name and directory (idempotent with MERGE)
        code_node_query = """
        MATCH (p:Project {id: $project_id})
        MERGE (c:Code {id: $file_id})
        ON CREATE SET
            c.file_path = $file_path,
            c.name = $name,
            c.directory_path = $directory_path,
            c.language = $language,
            c.summary = $summary,
            c.primary_purpose = $primary_purpose,
            c.complexity = $complexity,
            c.loc = $loc,
            c.imports = $imports,
            c.exports = $exports,
            c.key_patterns = $key_patterns,
            c.company_id = $company_id,
            c.project_id = $project_id,
            c.created_at = datetime()
        ON MATCH SET
            c.name = $name,
            c.directory_path = $directory_path,
            c.summary = $summary,
            c.primary_purpose = $primary_purpose,
            c.complexity = $complexity,
            c.loc = $loc,
            c.imports = $imports,
            c.exports = $exports,
            c.key_patterns = $key_patterns,
            c.updated_at = datetime()
        MERGE (p)-[:HAS_CODE]->(c)
        """

        async with self.neo4j_repo.driver.session(database=self.neo4j_repo.database) as session:
            await session.run(
                code_node_query,
                file_id=file_id,
                project_id=project_id,
                company_id=company_id,
                file_path=metadata.file_path,
                name=filename,
                directory_path=directory_path,
                language=metadata.language,
                summary=metadata.summary,
                primary_purpose=metadata.primary_purpose,
                complexity=metadata.complexity,
                loc=metadata.loc,
                imports=metadata.imports,
                exports=metadata.exports,
                key_patterns=metadata.key_patterns,
            )

        # Link Code to Directory (if directory exists)
        if directory_path:
            link_query = """
            MATCH (c:Code {id: $file_id})
            MATCH (d:Directory {path: $directory_path, project_id: $project_id})
            MERGE (d)-[:CONTAINS_FILE]->(c)
            """

            async with self.neo4j_repo.driver.session(database=self.neo4j_repo.database) as session:
                await session.run(
                    link_query,
                    file_id=file_id,
                    directory_path=directory_path,
                    project_id=project_id,
                )

        nodes_created = 1

        # Create Entity nodes and relationships
        if result.entities:
            entity_data = [
                {
                    "id": entity.id,
                    "type": entity.type,
                    "name": entity.name,
                    "signature": entity.signature,
                    "return_type": entity.return_type,
                    "docstring": entity.docstring,
                    "code_snippet": entity.code_snippet,
                    "line_start": entity.line_start,
                    "line_end": entity.line_end,
                    "complexity": entity.complexity,
                    "is_async": entity.is_async,
                    "is_static": entity.is_static,
                    "is_private": entity.is_private,
                    "is_exported": entity.is_exported,
                    "decorators": entity.decorators,
                    "implements_pattern": entity.implements_pattern or "",
                    "semantic_purpose": entity.semantic_purpose,
                    "company_id": company_id,
                    "project_id": project_id,
                }
                for entity in result.entities
            ]

            entity_query = """
            MATCH (c:Code {id: $file_id})
            UNWIND $entities AS entity
            MERGE (e:Entity {id: entity.id})
            ON CREATE SET
                e.type = entity.type,
                e.name = entity.name,
                e.signature = entity.signature,
                e.return_type = entity.return_type,
                e.docstring = entity.docstring,
                e.code_snippet = entity.code_snippet,
                e.line_start = entity.line_start,
                e.line_end = entity.line_end,
                e.complexity = entity.complexity,
                e.is_async = entity.is_async,
                e.is_static = entity.is_static,
                e.is_private = entity.is_private,
                e.is_exported = entity.is_exported,
                e.decorators = entity.decorators,
                e.implements_pattern = entity.implements_pattern,
                e.semantic_purpose = entity.semantic_purpose,
                e.company_id = entity.company_id,
                e.project_id = entity.project_id,
                e.created_at = datetime()
            ON MATCH SET
                e.type = entity.type,
                e.name = entity.name,
                e.signature = entity.signature,
                e.return_type = entity.return_type,
                e.docstring = entity.docstring,
                e.code_snippet = entity.code_snippet,
                e.line_start = entity.line_start,
                e.line_end = entity.line_end,
                e.complexity = entity.complexity,
                e.semantic_purpose = entity.semantic_purpose,
                e.updated_at = datetime()
            MERGE (c)-[:CONTAINS]->(e)
            """

            async with self.neo4j_repo.driver.session(database=self.neo4j_repo.database) as session:
                await session.run(
                    entity_query,
                    file_id=file_id,
                    entities=entity_data,
                )

            nodes_created += len(entity_data)

        # Create/Update relationships between entities (idempotent with MERGE)
        relationships_created = 0
        if result.relationships:
            for rel in result.relationships:
                rel_query = f"""
                MATCH (source:Entity {{id: $source_id}})
                MATCH (target:Entity {{id: $target_id}})
                MERGE (source)-[r:{rel.type}]->(target)
                ON CREATE SET
                    r.line_number = $line_number,
                    r.context = $context,
                    r.created_at = datetime()
                ON MATCH SET
                    r.line_number = $line_number,
                    r.context = $context,
                    r.updated_at = datetime()
                """

                try:
                    async with self.neo4j_repo.driver.session(database=self.neo4j_repo.database) as session:
                        await session.run(
                            rel_query,
                            source_id=rel.source_id,
                            target_id=rel.target_id,
                            line_number=rel.line_number,
                            context=rel.context,
                        )
                    relationships_created += 1
                except Exception as e:
                    logger.warning(
                        f"Failed to create relationship {rel.type} "
                        f"from {rel.source_id} to {rel.target_id}: {e}"
                    )

        return {
            "nodes": nodes_created,
            "relationships": relationships_created,
        }

    async def _store_in_qdrant(
        self,
        result: CodeAnalysisResult,
        company_id: str,
        project_id: str,
        user_id: str,
        collection_name: str,
    ) -> dict[str, int]:
        """Store embeddings in Qdrant vector database.

        Args:
            result: Code analysis result
            company_id: Company UUID
            project_id: Project UUID
            user_id: User UUID
            collection_name: Qdrant collection name

        Returns:
            Dict with point count
        """
        points = []
        point_id_counter = 0

        # Prepare texts for embedding
        texts_to_embed = []
        entity_mapping = []

        # Add file-level embedding
        if result.embeddings_metadata.file_embedding_text:
            texts_to_embed.append(result.embeddings_metadata.file_embedding_text)
            entity_mapping.append(None)  # File-level, no entity

        # Add entity embeddings
        for emb_meta in result.embeddings_metadata.entity_embeddings:
            # Combine signature, docstring, and semantic text
            combined_text = f"{emb_meta.signature_text}\n{emb_meta.docstring_text}\n{emb_meta.semantic_text}"
            texts_to_embed.append(combined_text.strip())
            entity_mapping.append(emb_meta.entity_id)

        # Generate embeddings in batch
        if texts_to_embed:
            embeddings = self.embedder.embed_text(
                text=texts_to_embed,
                task_type="retrieval_document",
                batch_size=100
            )

            # Create Qdrant points
            for i, embedding in enumerate(embeddings):
                entity_id = entity_mapping[i]

                # Build payload
                payload = {
                    "company_id": company_id,
                    "project_id": project_id,
                    "user_id": user_id,
                    "metadata_type": "code",
                    "file_path": result.file_metadata.file_path,
                    "language": result.file_metadata.language,
                }

                if entity_id:
                    # Entity-level point
                    entity = next(
                        (e for e in result.entities if e.id == entity_id),
                        None
                    )
                    if entity:
                        payload.update({
                            "entity_id": entity.id,
                            "entity_type": entity.type,
                            "entity_name": entity.name,
                            "signature": entity.signature,
                            "semantic_purpose": entity.semantic_purpose,

                             # Add content for single-hop retrieval
                            "code_snippet": entity.code_snippet[:2000],
                            "docstring": entity.docstring[:1000],
                            "line_start": entity.line_start,
                            "line_end": entity.line_end,
                            "return_type": entity.return_type,
                            "complexity": entity.complexity
                        })
                else:
                    # File-level point
                    payload.update({
                        "summary": result.file_metadata.summary,
                        "primary_purpose": result.file_metadata.primary_purpose,
                    })

                # Generate deterministic point ID using SHA256 (idempotent across runs)
                point_id_str = f"{company_id}:{project_id}:{result.file_metadata.file_path}:{point_id_counter}"
                point_id_bytes = hashlib.sha256(point_id_str.encode()).digest()
                point_id = int.from_bytes(point_id_bytes[:8], 'little')  # First 8 bytes as uint64
                point_id_counter += 1

                points.append({
                    "id": point_id,
                    "vector": embedding,
                    "payload": payload,
                })

        # Store in Qdrant
        if points:
            await self.qdrant_repo.upsert_points(
                collection_name=collection_name,
                points=points,
            )

        return {"points": len(points)}

    async def delete_by_file_path(
        self,
        file_path: str,
        project_id: str,
        company_id: str,
    ) -> dict[str, int]:
        """Delete all data for a file from Neo4j and Qdrant.

        Removes Code node, all Entity nodes, relationships, and vector embeddings
        for a specific file. Multi-tenant safe - filters by project_id.

        Args:
            file_path: File path to delete (e.g., "api/services/foo.py")
            project_id: Project UUID (multi-tenant safety)
            company_id: Company UUID (for Qdrant collection name)

        Returns:
            Dict with deletion counts: {
                "neo4j_nodes_deleted": int,
                "qdrant_points_deleted": int
            }
        """
        logger.info(
            f"Deleting file_path={file_path} from project={project_id}, company={company_id}"
        )

        neo4j_deleted = 0
        qdrant_deleted = 0

        # 1. Delete from Neo4j first (graph structure)
        try:
            await self.neo4j_repo.connect()

            # Delete Code node and all contained entities (batched for safety)
            # DETACH DELETE handles all relationships automatically
            delete_query = """
            MATCH (c:Code {file_path: $file_path, project_id: $project_id})
            WITH c
            OPTIONAL MATCH (c)-[:CONTAINS]->(e:Entity)
            WITH c, collect(e) AS entities, size(collect(e)) AS entity_count
            CALL {
                WITH entities
                UNWIND entities AS entity
                DETACH DELETE entity
            } IN TRANSACTIONS OF 1000 ROWS
            DETACH DELETE c
            RETURN entity_count
            """

            async with self.neo4j_repo.driver.session(database=self.neo4j_repo.database) as session:
                result = await session.run(
                    delete_query,
                    file_path=file_path,
                    project_id=project_id,
                )
                record = await result.single()
                if record:
                    entity_count = record["entity_count"]
                    neo4j_deleted = entity_count + 1  # entities + code node
                    logger.info(f"Neo4j: deleted {neo4j_deleted} nodes for {file_path}")
                else:
                    logger.info(f"Neo4j: no Code node found for {file_path}")

        except Exception as e:
            logger.error("Failed to delete from Neo4j: {}", e)
            raise

        # 2. Delete from Qdrant second (vector embeddings)
        try:
            from qdrant_client.models import Filter, FieldCondition, MatchValue

            collection_name = f"company_{company_id}"

            # Check if collection exists first
            collections = self.qdrant_repo.client.get_collections().collections
            collection_exists = any(c.name == collection_name for c in collections)

            if not collection_exists:
                logger.info(f"Qdrant: collection '{collection_name}' doesn't exist, skipping")
            else:
                # Multi-tenant safe: filter by BOTH file_path AND project_id
                filter_obj = Filter(
                    must=[
                        FieldCondition(
                            key="file_path",
                            match=MatchValue(value=file_path)
                        ),
                        FieldCondition(
                            key="project_id",
                            match=MatchValue(value=project_id)
                        )
                    ]
                )

                # Get count before deletion
                count_before = self.qdrant_repo.client.count(
                    collection_name=collection_name,
                    count_filter=filter_obj,
                ).count

                if count_before > 0:
                    # Delete points
                    self.qdrant_repo.client.delete(
                        collection_name=collection_name,
                        points_selector=filter_obj,
                    )
                    qdrant_deleted = count_before
                    logger.info(f"Qdrant: deleted {qdrant_deleted} points for {file_path}")
                else:
                    logger.info(f"Qdrant: no points found for {file_path}")

        except Exception as e:
            logger.error("Failed to delete from Qdrant: {}", e)
            raise

        result = {
            "neo4j_nodes_deleted": neo4j_deleted,
            "qdrant_points_deleted": qdrant_deleted,
        }

        logger.success(
            f"Deleted {file_path}: {neo4j_deleted} Neo4j nodes, "
            f"{qdrant_deleted} Qdrant points"
        )

        return result

    async def update_file(
        self,
        result: CodeAnalysisResult,
        project_id: str,
        company_id: str,
        user_id: str,
        collection_name: str | None = None,
    ) -> dict[str, Any]:
        """Update file data using DELETE + ADD pattern.

        For MODIFIED files: delete old data, then store new analysis.
        Idempotent: same input produces same output.

        Args:
            result: New code analysis result
            project_id: Project UUID
            company_id: Company UUID
            user_id: User UUID
            collection_name: Optional Qdrant collection name

        Returns:
            Dict with combined stats: {
                "deleted": {"neo4j_nodes_deleted": X, "qdrant_points_deleted": Y},
                "created": StorageStats
            }
        """
        file_path = result.file_metadata.file_path

        logger.info(
            f"Updating file_path={file_path} (project={project_id}, company={company_id})"
        )

        # Step 1: Delete existing data
        deleted_stats = await self.delete_by_file_path(
            file_path=file_path,
            project_id=project_id,
            company_id=company_id,
        )

        # Step 2: Store new analysis
        created_stats = await self.store_single_result(
            result=result,
            project_id=project_id,
            company_id=company_id,
            user_id=user_id,
            collection_name=collection_name,
        )

        # Combine stats
        combined_stats = {
            "deleted": deleted_stats,
            "created": created_stats,
        }

        logger.success(
            f"Updated {file_path}: deleted {deleted_stats['neo4j_nodes_deleted']} nodes/"
            f"{deleted_stats['qdrant_points_deleted']} vectors, "
            f"created {created_stats.neo4j_nodes_created} nodes/"
            f"{created_stats.qdrant_points_created} vectors"
        )

        return combined_stats

"""
Feature Sync Service
Orchestrates 3-tier sync: PostgreSQL → Neo4j → Qdrant with rollback on failures.
"""

from typing import List, Dict, Any
from uuid import UUID
from loguru import logger
from qdrant_client.models import PointStruct

from api.repositories.feature_repository import FeatureRepository
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from kgrag.embeddings import GeminiEmbedder
from kgrag.config import config


class FeatureSyncService:
    """Service for syncing feature data across 3 storage layers with rollback."""

    def __init__(
        self,
        feature_repo: FeatureRepository,
        neo4j_repo: Neo4jRepository,
        qdrant_repo: QdrantRepository
    ):
        self.feature_repo = feature_repo
        self.neo4j_repo = neo4j_repo
        self.qdrant_repo = qdrant_repo
        self.embedder = GeminiEmbedder()

    async def sync_feature_to_graph(
        self,
        feature_id: str,
        feature_data: Dict[str, Any]
    ) -> None:
        """
        Sync feature to Neo4j knowledge graph.

        Creates Feature node and links to Project.

        Args:
            feature_id: Feature UUID
            feature_data: Feature dict from PostgreSQL

        Raises:
            Exception: If sync fails
        """
        try:
            await self.neo4j_repo.create_feature_node(
                feature_id=feature_id,
                project_id=feature_data["project_id"],
                company_id=feature_data["company_id"],
                name=feature_data["name"],
                description=feature_data["description"],
                status=feature_data.get("status", "ready for refinement"),
                priority=feature_data.get("priority", "medium"),
                next_prompt=feature_data.get("next_prompt")
            )

            logger.info(f"Synced feature {feature_id} to Neo4j")

        except Exception as e:
            logger.error("Failed to sync feature {} to Neo4j: {}", feature_id, e)
            raise

    async def sync_chunks_to_graph(
        self,
        chunks: List[Dict[str, Any]]
    ) -> None:
        """
        Sync feature chunks to Neo4j.

        Creates FeatureChunk nodes, Concept nodes, Dependency nodes, and relationships.

        Args:
            chunks: List of chunk dicts from PostgreSQL

        Raises:
            Exception: If sync fails
        """
        if not chunks:
            return

        try:
            # 1. Create chunk nodes
            await self.neo4j_repo.create_feature_chunk_nodes(chunks)

            # 2. Extract unique concepts and dependencies from all chunks
            all_concepts = set()
            all_dependencies = set()

            for chunk in chunks:
                if chunk.get("key_concepts"):
                    all_concepts.update(chunk["key_concepts"])
                if chunk.get("dependencies"):
                    all_dependencies.update(chunk["dependencies"])

            # 3. Create concept/dependency nodes (once for the feature)
            # Note: For features, we link concepts to chunks via feature_id
            # This is different from agents which link via (agent_id, file_path)
            if all_concepts:
                # Get feature_id from first chunk (all chunks belong to same feature)
                feature_id = chunks[0]["feature_id"]
                
                # Reuse existing method but adapt for features
                # We'll create a pseudo file_path for features
                await self.neo4j_repo.create_concept_nodes(
                    agent_id=feature_id,  # Use feature_id as identifier
                    file_path="feature",  # Placeholder for feature context
                    concepts=list(all_concepts)
                )

            if all_dependencies:
                feature_id = chunks[0]["feature_id"]
                await self.neo4j_repo.create_dependency_nodes(
                    agent_id=feature_id,
                    file_path="feature",
                    dependencies=list(all_dependencies)
                )

            logger.info(
                f"Synced {len(chunks)} chunks to Neo4j "
                f"({len(all_concepts)} concepts, {len(all_dependencies)} dependencies)"
            )

        except Exception as e:
            logger.error("Failed to sync chunks to Neo4j: {}", e)
            raise

    async def sync_chunks_to_qdrant(
        self,
        company_id: str,
        chunks: List[Dict[str, Any]]
    ) -> None:
        """
        Sync feature chunks to Qdrant vector store.

        Embeds chunk content and stores in company collection.

        Args:
            company_id: Company UUID
            chunks: List of chunk dicts from PostgreSQL

        Raises:
            Exception: If sync fails
        """
        if not chunks:
            return

        collection_name = f"company_{company_id}"

        try:
            # Ensure collection exists
            await self.qdrant_repo.create_collection(
                collection_name=collection_name,
                vector_size=config.GEMINI_EMBEDDING_DIM
            )

            # Prepare content for batch embedding
            contents = [chunk["content"] for chunk in chunks]

            # Batch embed all chunk contents
            logger.debug(f"Embedding {len(contents)} chunks...")
            embeddings = self.embedder.embed_documents(contents, batch_size=100)

            if len(embeddings) != len(chunks):
                raise ValueError(
                    f"Embedding count ({len(embeddings)}) doesn't match chunk count ({len(chunks)})"
                )

            # Prepare Qdrant points - use chunk UUID directly as point ID
            points = []
            for chunk, embedding in zip(chunks, embeddings):
                point_id = str(chunk['id'])

                # Build payload
                payload = {
                    "chunk_id": str(chunk["id"]),
                    "feature_id": str(chunk["feature_id"]),
                    "chunk_index": chunk["chunk_index"],
                    "chunk_type": chunk.get("chunk_type"),
                    "summary": chunk.get("summary"),
                    "key_concepts": chunk.get("key_concepts", []),
                    "concepts": chunk.get("key_concepts", []),  # Alias for consistency
                    "dependencies": chunk.get("dependencies", []),
                }

                points.append(
                    PointStruct(
                        id=point_id,
                        vector=embedding,
                        payload=payload
                    )
                )

            # Batch upsert to Qdrant (100 points at a time)
            batch_size = 100
            total_stored = 0

            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]

                self.qdrant_repo.client.upsert(
                    collection_name=collection_name,
                    points=batch
                )

                total_stored += len(batch)
                logger.debug(
                    f"Stored batch {i // batch_size + 1}: {len(batch)} chunks"
                )

            # Update Qdrant point IDs in PostgreSQL
            for chunk, point in zip(chunks, points):
                await self.feature_repo.update_chunk_qdrant_id(
                    chunk_id=chunk["id"],
                    qdrant_point_id=point.id
                )

            logger.info(
                f"Synced {total_stored} chunks to Qdrant collection: {collection_name}"
            )

        except Exception as e:
            logger.error("Failed to sync chunks to Qdrant: {}", e)
            raise

    async def rollback_feature(self, feature_id: str, company_id: str) -> None:
        """
        Rollback feature creation (delete from PostgreSQL, Neo4j, Qdrant).

        Best effort rollback - logs errors but doesn't raise.

        Args:
            feature_id: Feature UUID to rollback
            company_id: Company UUID for Qdrant collection lookup
        """
        logger.warning("Rolling back feature {}", feature_id)

        # Get all chunk IDs before deletion for Qdrant cleanup
        chunk_ids = []
        try:
            chunks = await self.feature_repo.get_chunks(feature_id)
            chunk_ids = [str(chunk["id"]) for chunk in chunks]
        except Exception as e:
            logger.error("Failed to get chunks for rollback: {}", e)

        # 1. Delete from PostgreSQL (cascades to chunks)
        try:
            await self.feature_repo.delete_feature(feature_id)
            logger.info(f"Deleted feature {feature_id} from PostgreSQL")
        except Exception as e:
            logger.error("Failed to delete feature from PostgreSQL: {}", e)

        # 2. Delete from Neo4j
        try:
            await self.neo4j_repo.delete_feature_graph(feature_id)
            logger.info(f"Deleted feature {feature_id} from Neo4j")
        except Exception as e:
            logger.error("Failed to delete feature from Neo4j: {}", e)

        # 3. Delete from Qdrant by filtering chunk_ids
        if chunk_ids:
            try:
                collection_name = f"company_{company_id}"

                # Check if collection exists
                collection_exists = await self.qdrant_repo.collection_exists(
                    collection_name
                )

                if collection_exists:
                    # Delete points by IDs
                    self.qdrant_repo.client.delete(
                        collection_name=collection_name,
                        points_selector=chunk_ids
                    )

                    logger.info(
                        f"Deleted {len(chunk_ids)} chunks from Qdrant collection: {collection_name}"
                    )
            except Exception as e:
                logger.error("Failed to delete chunks from Qdrant: {}", e)

        logger.info(f"Completed rollback for feature {feature_id}")

"""
Agent Sync Service
Orchestrates 3-tier sync: PostgreSQL → Neo4j → Qdrant with rollback on failures.
"""

from typing import List, Dict, Any
from uuid import UUID
from loguru import logger
from qdrant_client.models import PointStruct

from api.repositories.agent_repository import AgentRepository
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from kgrag.embeddings import GeminiEmbedder
from kgrag.config import config


class AgentSyncService:
    """Service for syncing agent data across 3 storage layers with rollback."""

    def __init__(
        self,
        agent_repo: AgentRepository,
        neo4j_repo: Neo4jRepository,
        qdrant_repo: QdrantRepository
    ):
        self.agent_repo = agent_repo
        self.neo4j_repo = neo4j_repo
        self.qdrant_repo = qdrant_repo
        self.embedder = GeminiEmbedder()

    async def sync_agent_to_graph(
        self,
        agent_id: UUID,
        agent_data: Dict[str, Any],
        file_metadata: List[Dict[str, Any]]
    ) -> None:
        """
        Sync agent to Neo4j knowledge graph.

        Creates Agent node, file nodes (Skill/Reference/Script), and relationships.

        Args:
            agent_id: Agent UUID
            agent_data: Agent dict from PostgreSQL
            file_metadata: List of file metadata dicts

        Raises:
            Exception: If sync fails
        """
        try:
            # 1. Create Agent node
            await self.neo4j_repo.create_agent_node(
                agent_id=str(agent_id),
                company_id=str(agent_data["company_id"]),
                name=agent_data["name"],
                personality=agent_data["personality"],
                main_responsibilities=agent_data["main_responsibilities"],
                when_to_use=agent_data.get("when_to_use")
            )

            # 2. Create file nodes
            if file_metadata:
                await self.neo4j_repo.create_skill_file_nodes(
                    agent_id=str(agent_id),
                    file_metadata=file_metadata
                )

            logger.info(f"Synced agent {agent_id} to Neo4j")

        except Exception as e:
            logger.error("Failed to sync agent {} to Neo4j: {}", agent_id, e)
            raise

    async def sync_chunks_to_graph(
        self,
        chunks: List[Dict[str, Any]]
    ) -> None:
        """
        Sync skill chunks to Neo4j.

        Creates SkillChunk nodes, Concept nodes, Dependency nodes, and relationships.
        Batches concept/dependency/reference creation by file to reduce API calls.

        Args:
            chunks: List of chunk dicts from PostgreSQL

        Raises:
            Exception: If sync fails
        """
        if not chunks:
            return

        try:
            # 1. Create chunk nodes
            await self.neo4j_repo.create_skill_chunk_nodes(chunks)

            # 2. Group metadata by file to batch node creation
            file_metadata = {}  # {(agent_id, file_path): {concepts, dependencies, references}}

            for chunk in chunks:
                agent_id = str(chunk["agent_id"])
                file_path = chunk["file_path"]
                key = (agent_id, file_path)

                if key not in file_metadata:
                    file_metadata[key] = {
                        "concepts": set(),
                        "dependencies": set(),
                        "references": set()
                    }

                # Collect concepts
                if chunk.get("key_concepts"):
                    file_metadata[key]["concepts"].update(chunk["key_concepts"])

                # Collect dependencies
                if chunk.get("dependencies"):
                    file_metadata[key]["dependencies"].update(chunk["dependencies"])

                # Collect file references
                if chunk.get("file_references"):
                    file_metadata[key]["references"].update(chunk["file_references"])

            # 3. Create nodes once per file
            for (agent_id, file_path), metadata in file_metadata.items():
                # Create concept nodes
                if metadata["concepts"]:
                    await self.neo4j_repo.create_concept_nodes(
                        agent_id=agent_id,
                        file_path=file_path,
                        concepts=list(metadata["concepts"])
                    )

                # Create dependency nodes
                if metadata["dependencies"]:
                    await self.neo4j_repo.create_dependency_nodes(
                        agent_id=agent_id,
                        file_path=file_path,
                        dependencies=list(metadata["dependencies"])
                    )

                # Create file references
                if metadata["references"]:
                    await self.neo4j_repo.create_file_references(
                        agent_id=agent_id,
                        source_file=file_path,
                        target_files=list(metadata["references"])
                    )

            logger.info(
                f"Synced {len(chunks)} chunks to Neo4j "
                f"({len(file_metadata)} files processed)"
            )

        except Exception as e:
            logger.error("Failed to sync chunks to Neo4j: {}", e)
            raise

    async def sync_chunks_to_qdrant(
        self,
        company_id: UUID,
        chunks: List[Dict[str, Any]]
    ) -> None:
        """
        Sync skill chunks to Qdrant vector store.

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
                # Qdrant expects pure UUID strings, not prefixed IDs
                point_id = str(chunk['id'])

                # Build payload
                payload = {
                    "chunk_id": str(chunk["id"]),
                    "agent_id": str(chunk["agent_id"]),
                    "file_path": chunk["file_path"],
                    "file_type": chunk["file_type"],
                    "chunk_index": chunk["chunk_index"],
                    "section_title": chunk.get("section_title"),
                    "chunk_type": chunk.get("chunk_type"),
                    "summary": chunk.get("summary"),
                    "key_concepts": chunk.get("key_concepts", []),
                    "concepts": chunk.get("key_concepts", []),  # Same as key_concepts for consistency
                    "dependencies": chunk.get("dependencies", []),
                    "when_to_use": chunk.get("when_to_use"),  # When to use this skill chunk
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
                await self.agent_repo.update_chunk_qdrant_id(
                    chunk_id=chunk["id"],
                    qdrant_point_id=point.id
                )

            logger.info(
                f"Synced {total_stored} chunks to Qdrant collection: {collection_name}"
            )

        except Exception as e:
            logger.error("Failed to sync chunks to Qdrant: {}", e)
            raise

    async def rollback_agent(self, agent_id: UUID, company_id: UUID) -> None:
        """
        Rollback agent creation (delete from PostgreSQL, Neo4j, Qdrant).

        Best effort rollback - logs errors but doesn't raise.

        Args:
            agent_id: Agent UUID to rollback
            company_id: Company UUID for Qdrant collection lookup
        """
        logger.warning("Rolling back agent {}", agent_id)

        # Get all chunk IDs before deletion for Qdrant cleanup
        chunk_ids = []
        try:
            chunks = await self.agent_repo.get_chunks(agent_id)
            chunk_ids = [str(chunk["id"]) for chunk in chunks]
        except Exception as e:
            logger.error("Failed to get chunks for rollback: {}", e)

        # 1. Delete from PostgreSQL (cascades to chunks)
        try:
            await self.agent_repo.delete_agent(agent_id)
            logger.info(f"Deleted agent {agent_id} from PostgreSQL")
        except Exception as e:
            logger.error("Failed to delete agent from PostgreSQL: {}", e)

        # 2. Delete from Neo4j
        try:
            await self.neo4j_repo.delete_agent_graph(str(agent_id))
            logger.info(f"Deleted agent {agent_id} from Neo4j")
        except Exception as e:
            logger.error("Failed to delete agent from Neo4j: {}", e)

        # 3. Delete from Qdrant by filtering chunk_ids
        if chunk_ids:
            try:
                collection_name = f"company_{company_id}"

                # Check if collection exists
                collection_exists = await self.qdrant_repo.collection_exists(
                    collection_name
                )

                if collection_exists:
                    # Delete points by IDs - use chunk UUIDs directly
                    point_ids = chunk_ids  # Already string UUIDs, no prefix needed

                    self.qdrant_repo.client.delete(
                        collection_name=collection_name,
                        points_selector=point_ids
                    )

                    logger.info(
                        f"Deleted {len(point_ids)} chunks from Qdrant collection: {collection_name}"
                    )
            except Exception as e:
                logger.error("Failed to delete chunks from Qdrant: {}", e)

        logger.info(f"Completed rollback for agent {agent_id}")

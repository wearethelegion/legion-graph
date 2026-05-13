"""Deletion logic for entity extraction consumer.

Handles removing file chunks from Qdrant and Neo4j when delete action
messages are received. Separated from the main processor to keep file
sizes manageable.

Note: Does NOT delete summaries — that is the summarization consumer's
responsibility.
"""

import os
import time
from typing import Dict, List

import structlog
from neo4j import AsyncGraphDatabase
from qdrant_client import AsyncQdrantClient, models as qdrant_models

from cognee_service.multi_tenancy import ensure_neo4j_database, set_company_context
from ..enriched_chunks.models import EnrichedChunkMessage

logger = structlog.get_logger(__name__)


class EntityDeletionMixin:
    """Mixin providing file deletion from Qdrant chunks and Neo4j entities."""

    async def build_dataset_name(self, msg: EnrichedChunkMessage) -> str:
        """Build Cognee dataset name — must be overridden by subclass."""
        if msg.content_type == "document" or not msg.project_id:
            return f"{msg.company_id}_knowledge"
        # Subclasses override this with async project_name resolution.
        return f"{msg.project_id}_{msg.project_id}_code"

    async def delete_files(self, messages: List[EnrichedChunkMessage]) -> Dict[str, object]:
        """Delete file data from Qdrant and Neo4j.

        Handles delete action messages. Groups by file_path and removes all
        chunks and related entities for each deleted file.

        Returns dict with status, files_deleted count, duration.
        """
        if not messages:
            return {"status": "success", "files_deleted": 0}

        t0 = time.time()
        company_id = messages[0].company_id
        dataset_name = await self.build_dataset_name(messages[0])

        # Deduplicate by file_path — one delete per file
        files_to_delete: Dict[str, EnrichedChunkMessage] = {}
        for msg in messages:
            key = f"{msg.repository}:{msg.branch}:{msg.file_path}"
            if key not in files_to_delete:
                files_to_delete[key] = msg

        logger.info(
            "processor.delete_start",
            company_id=company_id,
            dataset_name=dataset_name,
            files_to_delete=len(files_to_delete),
        )

        # Set multi-tenancy context
        await ensure_neo4j_database(company_id)
        set_company_context(company_id)

        deleted_count = 0
        for msg in files_to_delete.values():
            try:
                await self._delete_file_from_stores(msg, company_id)
                deleted_count += 1
                logger.info(
                    "processor.file_deleted",
                    file_path=msg.file_path,
                    repository=msg.repository,
                    branch=msg.branch,
                )
            except Exception as e:
                logger.error(
                    "processor.file_delete_error",
                    file_path=msg.file_path,
                    error=str(e),
                    exc_info=True,
                )

        duration = round(time.time() - t0, 2)
        logger.info(
            "processor.delete_complete",
            files_deleted=deleted_count,
            dataset_name=dataset_name,
            duration_s=duration,
        )

        return {
            "status": "success",
            "files_deleted": deleted_count,
            "dataset_name": dataset_name,
            "duration_s": duration,
        }

    async def _delete_file_from_stores(
        self,
        msg: EnrichedChunkMessage,
        company_id: str,
    ) -> None:
        """Delete a single file from Qdrant chunks and Neo4j entities.

        Steps:
        1. Qdrant: Query DocumentChunk_text by file_path+repository to get chunk IDs
        2. Qdrant: Delete chunk points (NOT summaries — that's summarization consumer)
        3. Neo4j: Delete DocumentChunk nodes by ID and orphaned entities
        """
        # Step 1+2: Query Qdrant for chunk_ids, then delete chunks only
        chunk_ids = await self._delete_chunks_from_qdrant(msg)

        # Step 3: Delete from Neo4j using chunk_ids from Qdrant
        if chunk_ids:
            await self._delete_from_neo4j(chunk_ids, msg, company_id)

    async def _delete_chunks_from_qdrant(self, msg: EnrichedChunkMessage) -> List[str]:
        """Delete file chunks from Qdrant DocumentChunk_text collection.

        Returns list of chunk_ids that were deleted (needed for Neo4j cleanup).
        Does NOT delete summaries — that's the summarization consumer's job.
        """
        qdrant_url = os.environ.get("VECTOR_DB_URL", "http://qdrant:6333")
        qdrant_api_key = os.environ.get("QDRANT_API_KEY", None) or None

        client = AsyncQdrantClient(url=qdrant_url, api_key=qdrant_api_key, port=6333)

        try:
            filter_condition = qdrant_models.Filter(
                must=[
                    qdrant_models.FieldCondition(
                        key="file_path",
                        match=qdrant_models.MatchValue(value=msg.file_path),
                    ),
                    qdrant_models.FieldCondition(
                        key="repository",
                        match=qdrant_models.MatchValue(value=msg.repository),
                    ),
                    qdrant_models.FieldCondition(
                        key="branch",
                        match=qdrant_models.MatchValue(value=msg.branch),
                    ),
                ]
            )

            chunk_collection = "DocumentChunk_text"
            if await client.collection_exists(chunk_collection):
                # Scroll through all matching points
                scroll_result = await client.scroll(
                    collection_name=chunk_collection,
                    scroll_filter=filter_condition,
                    limit=1000,
                    with_payload=True,
                )
                chunk_ids = [str(point.id) for point in scroll_result[0]]

                if chunk_ids:
                    # Delete chunk points only
                    await client.delete(
                        collection_name=chunk_collection,
                        points_selector=qdrant_models.PointIdsList(points=chunk_ids),
                    )
                    logger.info(
                        "processor.qdrant_chunks_deleted",
                        file_path=msg.file_path,
                        chunks_deleted=len(chunk_ids),
                    )

                    # Also clean up Document_name collection (if exists)
                    doc_collection = "Document_name"
                    if await client.collection_exists(doc_collection):
                        doc_filter = qdrant_models.Filter(
                            must=[
                                qdrant_models.FieldCondition(
                                    key="name",
                                    match=qdrant_models.MatchValue(value=msg.file_path),
                                ),
                            ]
                        )
                        await client.delete(
                            collection_name=doc_collection,
                            points_selector=qdrant_models.FilterSelector(filter=doc_filter),
                        )

                    return chunk_ids

            return []

        except Exception as e:
            logger.error(
                "processor.qdrant_delete_error",
                file_path=msg.file_path,
                error=str(e),
                exc_info=True,
            )
            raise
        finally:
            await client.close()

    async def _delete_from_neo4j(
        self,
        chunk_ids: List[str],
        msg: EnrichedChunkMessage,
        company_id: str,
    ) -> None:
        """Delete file chunks and orphaned entities from Neo4j.

        Uses company-specific database. Matches DocumentChunk nodes by their
        UUID id property (from Qdrant), then deletes them and any orphaned
        entities.
        """
        db_name = f"cognee-{company_id}"
        uri = os.environ["NEO4J_URI"]
        username = os.environ.get("NEO4J_USERNAME", "neo4j")
        password = os.environ["NEO4J_PASSWORD"]

        driver = AsyncGraphDatabase.driver(uri, auth=(username, password))

        try:
            async with driver.session(database=db_name) as session:
                # Step 1: Check how many chunks exist
                count_query = """
                MATCH (chunk:DocumentChunk)
                WHERE chunk.id IN $chunk_ids
                RETURN count(chunk) as chunk_count
                """
                result = await session.run(count_query, chunk_ids=chunk_ids)
                record = await result.single()

                if not record or record["chunk_count"] == 0:
                    logger.info(
                        "processor.neo4j_no_chunks",
                        file_path=msg.file_path,
                        chunk_ids_queried=len(chunk_ids),
                    )
                    return

                chunk_count = record["chunk_count"]
                logger.info(
                    "processor.neo4j_chunks_found",
                    file_path=msg.file_path,
                    chunks=chunk_count,
                )

                # Step 2: Delete chunks and orphaned entities
                delete_query = """
                MATCH (chunk:DocumentChunk)
                WHERE chunk.id IN $chunk_ids

                // Find connected entities
                OPTIONAL MATCH (chunk)-[r]-(entity:Entity)

                WITH collect(DISTINCT chunk) as chunks_to_delete,
                     collect(DISTINCT entity) as connected_entities

                // Check for orphaned entities
                UNWIND connected_entities as entity
                OPTIONAL MATCH (entity)--(other_chunk:DocumentChunk)
                WHERE NOT other_chunk IN chunks_to_delete

                WITH chunks_to_delete,
                     entity,
                     count(DISTINCT other_chunk) as other_connections
                WHERE entity IS NOT NULL

                WITH chunks_to_delete,
                     collect(CASE WHEN other_connections = 0
                             THEN entity ELSE null END) as orphaned_entities

                // Delete chunks
                UNWIND chunks_to_delete as chunk
                DETACH DELETE chunk

                WITH orphaned_entities
                UNWIND orphaned_entities as orphan
                WHERE orphan IS NOT NULL
                DETACH DELETE orphan

                RETURN count(DISTINCT orphan) as entities_deleted
                """

                result = await session.run(delete_query, chunk_ids=chunk_ids)
                record = await result.single()
                entities_deleted = record["entities_deleted"] if record else 0

                logger.info(
                    "processor.neo4j_deleted",
                    file_path=msg.file_path,
                    chunks_deleted=chunk_count,
                    entities_deleted=entities_deleted,
                )

        except Exception as e:
            logger.error(
                "processor.neo4j_delete_error",
                file_path=msg.file_path,
                error=str(e),
                exc_info=True,
            )
            raise
        finally:
            await driver.close()

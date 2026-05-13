"""EnrichedChunksProcessor — processes enriched code chunks through Cognee stages 3-6.

Pipeline stages:
- Stage 0: Batch store pre-computed chunk embeddings in Qdrant
- Stage 3a+4: Parallel extract_content_graph + summarize_text per chunk (sliding window)
- Stage 3b: Batch merge + deduplicate entities across all chunks
- Stage 5: Batch embed + store entities and summaries
- Stage 6: dlt_fk_edges (no-op for code chunks)
"""

import asyncio
import hashlib
import os
import time
from pathlib import Path
from typing import Any, List, Dict, Optional
from uuid import UUID, uuid5, NAMESPACE_URL

import structlog
from neo4j import AsyncGraphDatabase
from qdrant_client import AsyncQdrantClient, models as qdrant_models
from sqlalchemy import select
from cognee.modules.chunking.models.DocumentChunk import DocumentChunk
from cognee.modules.data.models import Data, Dataset
from cognee.modules.users.methods import get_default_user
from cognee.modules.users.models import User
from cognee.infrastructure.databases.relational import get_relational_engine
from cognee.infrastructure.llm.extraction import extract_content_graph
from cognee.tasks.summarization import summarize_text
from cognee.tasks.graph.extract_graph_from_data import integrate_chunk_graphs
from cognee.tasks.storage.add_data_points import add_data_points
from cognee.shared.data_models import KnowledgeGraph
from cognee.modules.ontology.get_default_ontology_resolver import (
    get_default_ontology_resolver,
)

from cognee_service.multi_tenancy import ensure_neo4j_database, set_company_context
from .models import EnrichedChunkMessage, build_document_chunk

logger = structlog.get_logger(__name__)


class EnrichedChunksProcessor:
    """Processes batches of enriched code chunks through Cognee pipeline."""

    def __init__(
        self,
        custom_prompt_path: str | None = None,
        metadata_writer=None,
        project_resolver=None,
    ):
        """Initialize processor with optional custom prompt and metadata writer.

        Args:
            custom_prompt_path: Path to custom code graph extraction prompt file
            metadata_writer: Optional CogneeMetadataWriter for searchability
            project_resolver: Optional ProjectNameResolver for node set naming
        """
        self.custom_prompt: str | None = None
        self.metadata_writer = metadata_writer
        self._project_resolver = project_resolver

        if custom_prompt_path and Path(custom_prompt_path).is_file():
            self.custom_prompt = Path(custom_prompt_path).read_text(encoding="utf-8").strip()
            logger.info(
                "processor.custom_prompt_loaded",
                path=custom_prompt_path,
                length=len(self.custom_prompt),
            )
        elif custom_prompt_path:
            logger.warning("processor.custom_prompt_not_found", path=custom_prompt_path)

    async def build_dataset_name(self, msg: EnrichedChunkMessage) -> str:
        """Build Cognee dataset name from message metadata.

        Documents use the company-level knowledge scope; code stays project-scoped.
        """
        if msg.content_type == "document" or not msg.project_id:
            return f"{msg.company_id}_knowledge"

        if self._project_resolver:
            project_name = await self._project_resolver.resolve(msg.project_id)
        else:
            project_name = msg.project_id
        return f"{msg.project_id}_{project_name}_code"

    async def _build_pipeline_context(
        self,
        messages: List[EnrichedChunkMessage],
        dataset_name: str,
    ) -> Dict[str, Any]:
        """Build context dict with real User, Dataset, and Data objects.

        Cognee's add_data_points guards upsert_nodes/upsert_edges behind
        ``if user and dataset and data:``. Passing None skips Postgres
        nodes/edges writes, breaking access-controlled search.

        This method:
        1. Fetches the Cognee default user (creates if absent).
        2. Creates or fetches a Dataset record by name.
        3. Creates or fetches a representative Data record for the batch.

        Returns:
            Context dict with ``user``, ``dataset``, ``data`` keys, plus
            ``pipeline_name``.
        """
        # 1. Default user
        user: User = await get_default_user()

        db_engine = get_relational_engine()
        async with db_engine.get_async_session() as session:
            # 2. Dataset — match by name, create if missing
            result = await session.execute(select(Dataset).where(Dataset.name == dataset_name))
            dataset: Optional[Dataset] = result.scalars().first()

            if dataset is None:
                dataset = Dataset(
                    id=uuid5(NAMESPACE_URL, dataset_name),
                    name=dataset_name,
                    owner_id=user.id,
                    tenant_id=user.tenant_id,
                )
                session.add(dataset)
                await session.flush()

            # 3. Data — representative record for the first file in batch
            first_msg = messages[0]
            file_content_key = (
                f"text_{first_msg.file_path}:{first_msg.repository}:{first_msg.branch}"
            )
            content_hash = hashlib.md5(file_content_key.encode()).hexdigest()
            data_id = uuid5(NAMESPACE_URL, f"text_{content_hash}")

            result = await session.execute(select(Data).where(Data.id == data_id))
            data: Optional[Data] = result.scalars().first()

            if data is None:
                data = Data(
                    id=data_id,
                    name=first_msg.file_path,
                    extension="txt",
                    mime_type="text/plain",
                    raw_data_location=f"file:///data/cognee/data/text_{content_hash}.txt",
                    content_hash=content_hash,
                    owner_id=user.id,
                    tenant_id=user.tenant_id,
                )
                session.add(data)
                await session.flush()

            await session.commit()

        logger.info(
            "processor.context_built",
            user_id=str(user.id),
            dataset_id=str(dataset.id),
            data_id=str(data.id),
        )

        return {
            "pipeline_name": "enriched_chunks_consumer",
            "user": user,
            "data": data,
            "dataset": dataset,
        }

    async def process_batch(self, messages: List[EnrichedChunkMessage]) -> Dict[str, Any]:
        """Process a batch of enriched chunks through the full pipeline.

        Routes to either normal processing or deletion based on action field.
        Returns dict with status, counts, duration.
        """
        if not messages:
            return {"status": "success", "chunks_processed": 0}

        # Route based on action
        action = messages[0].action or "process"
        if action == "delete":
            return await self.delete_files(messages)

        t0 = time.time()
        timings: Dict[str, float] = {}
        company_id = messages[0].company_id
        dataset_name = await self.build_dataset_name(messages[0])

        logger.info(
            "processor.batch_start",
            batch_size=len(messages),
            company_id=company_id,
            dataset_name=dataset_name,
        )

        # 1. Set multi-tenancy context
        t = time.time()
        await ensure_neo4j_database(company_id)
        set_company_context(company_id)
        timings["set_tenant_context_s"] = round(time.time() - t, 2)

        # 2. Build DocumentChunks from messages
        t = time.time()
        chunks: List[DocumentChunk] = [build_document_chunk(msg) for msg in messages]
        timings["build_document_chunks_s"] = round(time.time() - t, 2)

        # Stage 0: Batch store pre-computed embeddings
        t = time.time()
        await self._store_chunk_embeddings(messages, company_id, dataset_name)
        timings["store_embeddings_s"] = round(time.time() - t, 2)

        # Stages 3-6: Extract graphs, integrate, store
        # Build context with real User/Dataset/Data ORM objects so that
        # add_data_points → upsert_nodes/upsert_edges populates Postgres
        # nodes/edges tables (required for access-controlled search).
        t = time.time()
        context = await self._build_pipeline_context(messages, dataset_name)
        timings["build_pipeline_context_s"] = round(time.time() - t, 2)

        # Stage 3a+4: Parallel extract graph + summarize per chunk
        # Run both entity extraction and summarization concurrently
        t_llm = time.time()

        # Track individual extraction progress
        extraction_done = 0
        extraction_lock = asyncio.Lock()

        async def _extract_one(
            chunk: DocumentChunk, msg: EnrichedChunkMessage, idx: int
        ) -> KnowledgeGraph:
            nonlocal extraction_done
            t_ex = time.time()
            # Pass header+content to LLM for richer extraction context.
            # chunk.text is raw content only (stored in Qdrant by add_data_points).
            llm_text = f"{msg.header}\n{chunk.text}" if msg.header else chunk.text
            result = await extract_content_graph(
                llm_text, KnowledgeGraph, custom_prompt=self.custom_prompt
            )
            async with extraction_lock:
                extraction_done += 1
            logger.info(
                "processor.chunk_extracted",
                idx=idx,
                done=extraction_done,
                total=len(chunks),
                nodes=len(result.nodes),
                edges=len(result.edges),
                duration_s=round(time.time() - t_ex, 1),
            )
            return result

        chunk_graphs, summaries = await asyncio.gather(
            # Entity extraction: 50 concurrent LLM calls with progress
            asyncio.gather(
                *[
                    _extract_one(chunk, msg, i)
                    for i, (chunk, msg) in enumerate(zip(chunks, messages))
                ]
            ),
            # Summarization: internally runs 50 concurrent LLM calls
            summarize_text(chunks),
        )

        timings["llm_extract_and_summarize_s"] = round(time.time() - t_llm, 2)

        logger.info(
            "processor.llm_phase_complete",
            duration_s=round(time.time() - t_llm, 1),
            extractions=len(chunk_graphs),
            summaries=len(summaries) if summaries else 0,
        )

        # Filter edges with missing nodes
        t = time.time()
        for graph in chunk_graphs:
            valid_node_ids = {node.id for node in graph.nodes}
            graph.edges = [
                edge
                for edge in graph.edges
                if edge.source_node_id in valid_node_ids and edge.target_node_id in valid_node_ids
            ]
        timings["filter_invalid_edges_s"] = round(time.time() - t, 2)

        logger.info(
            "processor.graphs_extracted",
            batch_size=len(chunks),
            total_nodes=sum(len(g.nodes) for g in chunk_graphs),
            total_edges=sum(len(g.edges) for g in chunk_graphs),
        )

        # Stage 3b + 5a: Integrate chunk graphs (merge, deduplicate, store entities)
        t = time.time()
        ontology_resolver = get_default_ontology_resolver()
        await integrate_chunk_graphs(
            data_chunks=chunks,
            chunk_graphs=chunk_graphs,
            graph_model=KnowledgeGraph,
            ontology_resolver=ontology_resolver,
            context=context,
            pipeline_name="enriched_chunks_consumer",
            task_name="integrate_chunk_graphs",
        )
        timings["integrate_chunk_graphs_s"] = round(time.time() - t, 2)

        # Stage 5b: Store summaries in vector DB
        t = time.time()
        if summaries:
            await add_data_points(
                data_points=summaries,
                context=context,
            )
            logger.info("processor.summaries_stored", count=len(summaries))
        timings["add_data_points_summaries_s"] = round(time.time() - t, 2)

        # Stage 7: Write Cognee metadata for searchability
        t = time.time()
        if self.metadata_writer:
            try:
                await self.metadata_writer.record_batch(messages, dataset_name)
            except Exception as e:
                # Metadata write failure should not fail the batch
                logger.error(
                    "processor.metadata_write_failed",
                    dataset_name=dataset_name,
                    error=str(e),
                    exc_info=True,
                )
        timings["metadata_writer_s"] = round(time.time() - t, 2)

        duration = round(time.time() - t0, 2)

        # --- Batch profiling summary ---
        logger.info(
            "processor.batch_profile",
            batch_size=len(chunks),
            total_s=duration,
            **timings,
        )

        logger.info(
            "processor.batch_complete",
            batch_size=len(chunks),
            dataset_name=dataset_name,
            duration_s=duration,
        )

        return {
            "status": "success",
            "chunks_processed": len(chunks),
            "dataset_name": dataset_name,
            "duration_s": duration,
        }

    async def _store_chunk_embeddings(
        self,
        messages: List[EnrichedChunkMessage],
        company_id: str,
        dataset_name: str,
    ) -> None:
        """Stage 0: Batch store pre-computed chunk embeddings in Qdrant.

        Uses AsyncQdrantClient directly — bypasses Cognee's adapter since we
        already have embeddings and don't need the embed pipeline.
        """
        if not messages:
            return

        logger.info(
            "processor.storing_embeddings",
            count=len(messages),
            dataset_name=dataset_name,
        )

        qdrant_url = os.environ.get("VECTOR_DB_URL", "http://qdrant:6333")
        qdrant_api_key = os.environ.get("QDRANT_API_KEY", None) or None
        vector_size = len(messages[0].embedding) if messages[0].embedding else 768
        collection_name = "DocumentChunk_text"  # Match Cognee's collection naming

        client = AsyncQdrantClient(url=qdrant_url, api_key=qdrant_api_key, port=6333)

        try:
            # Ensure collection exists
            if not await client.collection_exists(collection_name):
                await client.create_collection(
                    collection_name=collection_name,
                    vectors_config={
                        "text": qdrant_models.VectorParams(
                            size=vector_size,
                            distance=qdrant_models.Distance.COSINE,
                        )
                    },
                )

            # Build Qdrant points — payload must match Cognee's IndexSchema format
            # so that Cognee's search (which filters by database_name, type, etc.) works.
            now_ms = int(time.time() * 1000)
            db_name = f"cognee-{company_id}"

            points = [
                qdrant_models.PointStruct(
                    id=msg.chunk_id,
                    vector={"text": msg.embedding},
                    payload={
                        # --- Cognee IndexSchema fields (required for search) ---
                        "id": msg.chunk_id,
                        "created_at": now_ms,
                        "updated_at": now_ms,
                        "ontology_valid": False,
                        "version": 1,
                        "topological_rank": 0,
                        "metadata": {"index_fields": ["text"]},
                        "type": "IndexSchema",
                        "belongs_to_set": [dataset_name],
                        "source_pipeline": "enriched_chunks_consumer",
                        "source_task": "store_chunk_embeddings",
                        "source_node_set": dataset_name,
                        "source_user": "default_user@example.com",
                        "feedback_weight": 0.5,
                        "text": msg.content,  # Full content, NOT truncated
                        "database_name": db_name,
                        # --- Bonus metadata (our extra fields) ---
                        "chunk_id": msg.chunk_id,
                        "parent_id": msg.parent_id,
                        "file_path": msg.file_path,
                        "repository": msg.repository,
                        "branch": msg.branch,
                        "language": msg.language or "unknown",
                        "chunk_index": msg.chunk_index,
                        "total_chunks": msg.total_chunks,
                    },
                )
                for msg in messages
            ]

            # Batch upsert
            await client.upsert(collection_name=collection_name, points=points)

            logger.info(
                "processor.embeddings_stored",
                count=len(points),
                collection=collection_name,
            )
        except Exception as e:
            logger.error(
                "processor.embeddings_store_error",
                error=str(e),
                count=len(messages),
                exc_info=True,
            )
            raise
        finally:
            await client.close()

    async def delete_files(self, messages: List[EnrichedChunkMessage]) -> Dict[str, Any]:
        """Delete file data from Qdrant, Neo4j, and Postgres.

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
                await self._delete_file_from_all_stores(msg, company_id)
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

    async def _delete_file_from_all_stores(
        self,
        msg: EnrichedChunkMessage,
        company_id: str,
    ) -> None:
        """Delete a single file from Qdrant, Neo4j, and Postgres.

        Steps:
        1. Qdrant: Query DocumentChunk_text by file_path+repository to get chunk IDs
        2. Qdrant: Delete chunk points + related summaries/documents
        3. Neo4j: Delete DocumentChunk nodes by ID and orphaned entities
        """
        # Step 1+2: Query Qdrant for chunk_ids, then delete
        chunk_ids = await self._delete_from_qdrant(msg)

        # Step 3: Delete from Neo4j using chunk_ids from Qdrant
        if chunk_ids:
            await self._delete_from_neo4j(chunk_ids, msg, company_id)

    async def _delete_from_qdrant(self, msg: EnrichedChunkMessage) -> List[str]:
        """Delete file chunks and related data from Qdrant.

        Returns list of chunk_ids that were deleted (needed for Neo4j cleanup).
        """
        qdrant_url = os.environ.get("VECTOR_DB_URL", "http://qdrant:6333")
        qdrant_api_key = os.environ.get("QDRANT_API_KEY", None) or None

        client = AsyncQdrantClient(url=qdrant_url, api_key=qdrant_api_key, port=6333)

        try:
            # Delete DocumentChunk points by file_path + repository filter
            # Qdrant filter: must match both file_path AND repository
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

            # Query to get chunk IDs first (needed for cleaning up summaries)
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
                    # Delete chunk points
                    await client.delete(
                        collection_name=chunk_collection,
                        points_selector=qdrant_models.PointIdsList(points=chunk_ids),
                    )
                    logger.info(
                        "processor.qdrant_chunks_deleted",
                        file_path=msg.file_path,
                        chunks_deleted=len(chunk_ids),
                    )

                    # Clean up TextSummary_text collection
                    summary_collection = "TextSummary_text"
                    if await client.collection_exists(summary_collection):
                        # Summaries reference chunk_id in payload
                        for chunk_id in chunk_ids:
                            summary_filter = qdrant_models.Filter(
                                must=[
                                    qdrant_models.FieldCondition(
                                        key="chunk_id",
                                        match=qdrant_models.MatchValue(value=str(chunk_id)),
                                    ),
                                ]
                            )
                            await client.delete(
                                collection_name=summary_collection,
                                points_selector=qdrant_models.FilterSelector(filter=summary_filter),
                            )

                    # Clean up Document_name collection (if exists)
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
        UUID id property (from Qdrant), then deletes them and any orphaned entities.
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
                # 1. Match chunks by ID
                # 2. Find entities connected to these chunks
                # 3. Check if entities have connections to OTHER chunks
                # 4. Delete chunks + orphaned entities
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
                     collect(CASE WHEN other_connections = 0 THEN entity ELSE null END) as orphaned_entities

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

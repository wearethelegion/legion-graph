"""EntityExtractionProcessor — extracts entities from enriched code chunks.

Pipeline stages (entity extraction ONLY, no summarization):
- Stage 0: Batch store pre-computed chunk embeddings in Qdrant
- Stage 3a: Parallel extract_content_graph per chunk (LLM entity extraction)
- Stage 3b: Integrate chunk graphs (merge, deduplicate, ontology, store to Neo4j + Qdrant)
- Cognee add: Register Data/Dataset in Postgres for access-controlled search
- Metadata: Write searchability metadata
"""

import asyncio
import hashlib
import os
import time
from pathlib import Path
from typing import Any, List, Dict, Optional
from uuid import uuid5, NAMESPACE_URL

import structlog
from qdrant_client import AsyncQdrantClient, models as qdrant_models
from sqlalchemy import select
from cognee.modules.chunking.models.DocumentChunk import DocumentChunk
from cognee.modules.data.models import Data, Dataset
from cognee.modules.users.methods import get_default_user
from cognee.modules.users.models import User
from cognee.infrastructure.databases.relational import get_relational_engine
from cognee.infrastructure.llm.extraction import extract_content_graph
from cognee.tasks.graph.extract_graph_from_data import integrate_chunk_graphs
from cognee.shared.data_models import KnowledgeGraph
from cognee.modules.ontology.get_default_ontology_resolver import (
    get_default_ontology_resolver,
)

from cognee_service.multi_tenancy import ensure_neo4j_database, set_company_context
from ..enriched_chunks.models import EnrichedChunkMessage, build_document_chunk
from .deletion import EntityDeletionMixin

logger = structlog.get_logger(__name__)


class EntityExtractionProcessor(EntityDeletionMixin):
    """Processes batches of enriched code chunks — entity extraction only.

    Inherits deletion logic from EntityDeletionMixin (delete_files,
    _delete_file_from_stores, _delete_chunks_from_qdrant, _delete_from_neo4j).
    """

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
                    raw_data_location=(f"file:///data/cognee/data/text_{content_hash}.txt"),
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
            "pipeline_name": "entity_extraction_consumer",
            "user": user,
            "data": data,
            "dataset": dataset,
        }

    async def process_batch(self, messages: List[EnrichedChunkMessage]) -> Dict[str, Any]:
        """Process a batch of enriched chunks — entity extraction only.

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

        # Build context with real User/Dataset/Data ORM objects so that
        # integrate_chunk_graphs → upsert_nodes/upsert_edges populates Postgres
        # nodes/edges tables (required for access-controlled search).
        t = time.time()
        context = await self._build_pipeline_context(messages, dataset_name)
        timings["build_pipeline_context_s"] = round(time.time() - t, 2)

        # Stage 3a: Parallel entity extraction per chunk (NO summarization)
        t_llm = time.time()
        chunk_graphs = await asyncio.gather(
            *[
                self._extract_one(chunk, msg, i, len(chunks))
                for i, (chunk, msg) in enumerate(zip(chunks, messages))
            ]
        )
        timings["llm_extract_entities_s"] = round(time.time() - t_llm, 2)

        logger.info(
            "processor.extraction_complete",
            duration_s=round(time.time() - t_llm, 1),
            extractions=len(chunk_graphs),
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

        # Stage 3b: Integrate chunk graphs (merge, deduplicate, store entities)
        t = time.time()
        ontology_resolver = get_default_ontology_resolver()
        await integrate_chunk_graphs(
            data_chunks=chunks,
            chunk_graphs=chunk_graphs,
            graph_model=KnowledgeGraph,
            ontology_resolver=ontology_resolver,
            context=context,
            pipeline_name="entity_extraction_consumer",
            task_name="integrate_chunk_graphs",
        )
        timings["integrate_chunk_graphs_s"] = round(time.time() - t, 2)

        # Metadata: Write Cognee metadata for searchability
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

    async def _extract_one(
        self,
        chunk: DocumentChunk,
        msg: EnrichedChunkMessage,
        idx: int,
        total: int,
    ) -> KnowledgeGraph:
        """Extract entity graph from a single chunk via LLM.

        Passes header+content to LLM for richer extraction context.
        chunk.text is raw content only (stored in Qdrant by add_data_points).
        """
        t_ex = time.time()
        llm_text = f"{msg.header}\n{chunk.text}" if msg.header else chunk.text
        result = await extract_content_graph(
            llm_text, KnowledgeGraph, custom_prompt=self.custom_prompt
        )
        logger.info(
            "processor.chunk_extracted",
            idx=idx,
            total=total,
            nodes=len(result.nodes),
            edges=len(result.edges),
            duration_s=round(time.time() - t_ex, 1),
        )
        return result

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

            # Build Qdrant points — payload must match Cognee's IndexSchema
            # format so that Cognee's search (which filters by database_name,
            # type, etc.) works.
            now_ms = int(time.time() * 1000)
            db_name = f"cognee-{company_id}"

            points = [
                qdrant_models.PointStruct(
                    id=msg.chunk_id,
                    vector={"text": msg.embedding},
                    payload={
                        # --- Cognee IndexSchema fields ---
                        "id": msg.chunk_id,
                        "created_at": now_ms,
                        "updated_at": now_ms,
                        "ontology_valid": False,
                        "version": 1,
                        "topological_rank": 0,
                        "metadata": {"index_fields": ["text"]},
                        "type": "IndexSchema",
                        "belongs_to_set": [dataset_name],
                        "source_pipeline": "entity_extraction_consumer",
                        "source_task": "store_chunk_embeddings",
                        "source_node_set": dataset_name,
                        "source_user": "default_user@example.com",
                        "feedback_weight": 0.5,
                        "text": msg.content,
                        "database_name": db_name,
                        # --- Bonus metadata ---
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

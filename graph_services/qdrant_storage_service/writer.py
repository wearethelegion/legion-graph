"""Qdrant batch write logic for Qdrant Storage Service (Service 5).

Handles:
- Collection creation/verification
- Batch upsert to DocumentChunk_text, Entity_name, TextSummary_text, EdgeType_relationship_name
- Retry with exponential backoff on transient failures
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional
from uuid import UUID

import structlog
from qdrant_client import AsyncQdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    VectorParams,
)

from .config import QdrantStorageConfig

logger = structlog.get_logger(__name__)


def _scope_key(company_id: str, project_id: Optional[str]) -> str:
    """Return the active scope key.

    Documents fall back to company_id; code stays project-scoped.
    """
    return project_id or company_id


def _build_canonical_node_set(
    company_id: str,
    scope_key: str,
    project_name: str,
    content_type: str = "code",
) -> str:
    """Build the canonical node_set / source_node_set value.

    Must match neo4j_storage_service.writer._build_node_set():
    - document -> "{company_id}_knowledge"
    - code -> "{project_id}_{project_name}_code"
    """
    if content_type == "document":
        return f"{company_id}_knowledge"
    return f"{scope_key}_{project_name}_code"


class QdrantBatchWriter:
    """Batch writer for Qdrant vector collections.

    Manages collection creation and point upserts.
    """

    def __init__(
        self,
        client: AsyncQdrantClient,
        config: type[QdrantStorageConfig] = QdrantStorageConfig,
    ) -> None:
        self._client = client
        self._config = config

    # ── Collection Management ─────────────────────────────────────────

    def _collection_vector_map(self) -> Dict[str, str]:
        """Return mapping of collection name → named vector field.

        Central source of truth used by both ensure_collections() and
        the lazy auto-creation path in _upsert_with_retry().
        """
        return {
            self._config.COLLECTION_CHUNKS: "text",
            self._config.COLLECTION_ENTITIES: "text",
            self._config.COLLECTION_SUMMARIES: "text",
            self._config.COLLECTION_TRIPLETS: "text",
            self._config.COLLECTION_EDGE_TYPES: "text",
            self._config.COLLECTION_ENTITY_TYPES: "text",
        }

    def _collection_vector_field(self, collection_name: str) -> Optional[str]:
        """Resolve the named vector field for a collection.

        Supports fixed Cognee collections plus dynamic per-company knowledge collections.
        """
        vector_field = self._collection_vector_map().get(collection_name)
        if vector_field:
            return vector_field
        if collection_name.endswith("_knowledge"):
            return "text"
        return None

    async def ensure_collections(self) -> None:
        """Create Qdrant collections if they don't exist.

        Creates collections with named vectors for Cognee compatibility:
        - DocumentChunk_text → "text" vector
        - Entity_name → "name" vector
        - TextSummary_text → "text" vector
        - Triplet_text → "text" vector
        - EdgeType_relationship_name → "relationship_name" vector
        - EntityType_name → "name" vector (FIX 1: added)
        """
        existing = await self._client.get_collections()
        existing_names = {c.name for c in existing.collections}

        for collection_name, vector_field_name in self._collection_vector_map().items():
            if collection_name not in existing_names:
                await self._client.create_collection(
                    collection_name=collection_name,
                    vectors_config={
                        vector_field_name: VectorParams(
                            size=self._config.EMBEDDING_DIMENSION,
                            distance=Distance.COSINE,
                        )
                    },
                )
                logger.info(
                    "writer.collection_created",
                    collection=collection_name,
                    vector_field=vector_field_name,
                )
            else:
                logger.debug("writer.collection_exists", collection=collection_name)

    async def _ensure_single_collection(self, collection_name: str) -> bool:
        """Lazily create a single collection if it doesn't exist.

        Called by _upsert_with_retry when a collection-not-found error is
        detected. Returns True if the collection was created.
        """
        vector_field = self._collection_vector_field(collection_name)
        if not vector_field:
            logger.error(
                "writer.unknown_collection",
                collection=collection_name,
            )
            return False

        existing = await self._client.get_collections()
        existing_names = {c.name for c in existing.collections}

        if collection_name in existing_names:
            logger.debug(
                "writer.collection_already_exists_on_recheck",
                collection=collection_name,
            )
            return False

        await self._client.create_collection(
            collection_name=collection_name,
            vectors_config={
                vector_field: VectorParams(
                    size=self._config.EMBEDDING_DIMENSION,
                    distance=Distance.COSINE,
                )
            },
        )
        logger.info(
            "writer.collection_auto_created",
            collection=collection_name,
            vector_field=vector_field,
            dimension=self._config.EMBEDDING_DIMENSION,
        )
        return True

    @staticmethod
    def _is_collection_not_found(error: Exception) -> bool:
        """Detect whether the error is a Qdrant 'collection not found' error.

        Checks UnexpectedResponse.status_code == 404 first (reliable),
        falls back to string matching for edge cases.
        """
        if isinstance(error, UnexpectedResponse):
            return error.status_code == 404
        error_str = str(error).lower()
        return "not found" in error_str and "collection" in error_str

    # ── Delete by file_version_id ────────────────────────────────────

    async def delete_by_file_version_id(
        self,
        file_version_id: str,
        company_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Delete all Qdrant points associated with a file_version_id.

        Unconditional delete for: DocumentChunk_text (where both code and
        document chunks now live), TextSummary_text, Triplet_text,
        EdgeType_relationship_name. When ``company_id`` is provided, also
        cleans up the legacy per-company ``{company_id}_knowledge`` collection
        for any historical points that may still be there.

        Conditional delete (entity survival) for: Entity_name, EntityType_name.
        Points are kept if another point with the same name exists under a
        different file_version_id.

        Returns dict mapping collection name → number of points deleted.
        """
        fv_filter = Filter(
            must=[
                FieldCondition(
                    key="file_version_id",
                    match=MatchValue(value=file_version_id),
                )
            ]
        )

        results: Dict[str, int] = {}

        # ── Unconditional collections ──
        unconditional = [
            self._config.COLLECTION_CHUNKS,
            self._config.COLLECTION_SUMMARIES,
            self._config.COLLECTION_TRIPLETS,
            self._config.COLLECTION_EDGE_TYPES,
        ]

        # Note: document chunks now live in COLLECTION_CHUNKS (already in the
        # unconditional list above). The per-company {company_id}_knowledge
        # collection is no longer written. We keep the cleanup line below for
        # any historical points that still exist in that collection.
        if company_id:
            unconditional.append(f"{company_id}_knowledge")

        for collection in unconditional:
            count = await self._delete_all_by_filter(collection, fv_filter)
            results[collection] = count

        # ── Conditional collections (entity survival) ──
        conditional = [
            (self._config.COLLECTION_ENTITIES, "name"),
            (self._config.COLLECTION_ENTITY_TYPES, "name"),
        ]

        for collection, name_field in conditional:
            count = await self._delete_with_survival(
                collection, file_version_id, fv_filter, name_field
            )
            results[collection] = count

        logger.info(
            "writer.delete_complete",
            file_version_id=file_version_id,
            company_id=company_id,
            results=results,
        )

        return results

    async def _delete_all_by_filter(self, collection_name: str, points_filter: Filter) -> int:
        """Delete all points matching filter. Returns count of deleted points.

        Scrolls first to count, then deletes. Returns 0 if collection
        doesn't exist or has no matching points.
        """
        try:
            count = await self._count_by_filter(collection_name, points_filter)
            if count == 0:
                return 0

            await self._client.delete(
                collection_name=collection_name,
                points_selector=points_filter,
            )

            logger.info(
                "writer.delete_unconditional",
                collection=collection_name,
                deleted=count,
            )
            return count

        except Exception as e:
            if self._is_collection_not_found(e):
                logger.debug(
                    "writer.delete_skip_missing_collection",
                    collection=collection_name,
                )
                return 0
            raise

    async def _delete_with_survival(
        self,
        collection_name: str,
        file_version_id: str,
        fv_filter: Filter,
        name_field: str,
    ) -> int:
        """Delete points with entity survival logic.

        For each point matching file_version_id, check if another point with
        the same name exists under a different file_version_id. If yes, the
        entity survives (skip deletion). If no, delete the point.

        Returns number of points actually deleted.
        """
        try:
            # Scroll all points for this file_version_id
            candidates = await self._scroll_all(collection_name, fv_filter)
            if not candidates:
                return 0

            delete_ids = []
            survived = 0

            for point in candidates:
                point_name = point.payload.get(name_field, "")
                if not point_name:
                    # No name → unconditionally delete
                    delete_ids.append(point.id)
                    continue

                # Check if another point with same name has different file_version_id
                survivor_filter = Filter(
                    must=[
                        FieldCondition(
                            key=name_field,
                            match=MatchValue(value=point_name),
                        ),
                    ],
                    must_not=[
                        FieldCondition(
                            key="file_version_id",
                            match=MatchValue(value=file_version_id),
                        ),
                    ],
                )

                survivor_count = await self._count_by_filter(collection_name, survivor_filter)

                if survivor_count > 0:
                    survived += 1
                else:
                    delete_ids.append(point.id)

            if delete_ids:
                await self._client.delete(
                    collection_name=collection_name,
                    points_selector=delete_ids,
                )

            logger.info(
                "writer.delete_with_survival",
                collection=collection_name,
                candidates=len(candidates),
                deleted=len(delete_ids),
                survived=survived,
            )
            return len(delete_ids)

        except Exception as e:
            if self._is_collection_not_found(e):
                logger.debug(
                    "writer.delete_skip_missing_collection",
                    collection=collection_name,
                )
                return 0
            raise

    async def _count_by_filter(self, collection_name: str, points_filter: Filter) -> int:
        """Count points matching a filter via Qdrant count API."""
        result = await self._client.count(
            collection_name=collection_name,
            count_filter=points_filter,
            exact=True,
        )
        return result.count

    async def _scroll_all(self, collection_name: str, points_filter: Filter) -> list:
        """Scroll all points matching a filter, paginating as needed."""
        all_points = []
        offset = None

        while True:
            points, next_offset = await self._client.scroll(
                collection_name=collection_name,
                scroll_filter=points_filter,
                limit=100,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )

            all_points.extend(points)

            if next_offset is None:
                break
            offset = next_offset

        return all_points

    # ── Batch Upsert ──────────────────────────────────────────────────

    async def upsert_chunks(
        self,
        chunks: List[Dict[str, Any]],
    ) -> int:
        """Batch upsert chunk points to DocumentChunk_text collection.

        Each chunk dict must have: chunk_id, embedding, content, header,
        company_id, project_id, file_path, language, repository, branch.
        Optional: content_type (defaults to "code")

        Routes to collection by content_type:
        - "document" → {company_id}_knowledge collection
        - otherwise → DocumentChunk_text (existing code collection)

        Returns number of points upserted.
        """
        if not chunks:
            return 0

        # Group chunks by collection (content_type routing)
        code_chunks = []
        doc_chunks = []

        for chunk in chunks:
            content_type = chunk.get("content_type", "") or "code"
            if content_type == "document":
                doc_chunks.append(chunk)
            else:
                code_chunks.append(chunk)

        total_upserted = 0

        # Upsert code chunks to standard collection
        if code_chunks:
            total_upserted += await self._upsert_chunks_to_collection(
                code_chunks, self._config.COLLECTION_CHUNKS, content_type="code"
            )

        # Upsert document chunks to the shared cognee chunks collection
        # (DocumentChunk_text). Tenant scoping is provided by source_node_set
        # = "{company_id}_knowledge" and dataset_id payload filters, NOT by
        # collection name. Writing here lets cognee.search find these chunks
        # via its default QDrantAdapter, alongside code chunks.
        if doc_chunks:
            total_upserted += await self._upsert_chunks_to_collection(
                doc_chunks, self._config.COLLECTION_CHUNKS, content_type="document"
            )

        return total_upserted

    async def _upsert_chunks_to_collection(
        self,
        chunks: List[Dict[str, Any]],
        collection_name: str,
        content_type: str = "code",
    ) -> int:
        """Helper to upsert chunks to a specific collection.

        Args:
            chunks: List of chunk dicts
            collection_name: Target Qdrant collection
            content_type: "code" or "document" for node_set routing

        Returns number of points upserted.
        """
        if not chunks:
            return 0

        points = []
        for chunk in chunks:
            chunk_id = str(chunk["chunk_id"])
            content = chunk.get("content", "")
            company_id = chunk.get("company_id", "")
            project_id = chunk.get("project_id") or None
            content_type = chunk.get("content_type", "code")
            scope_key = _scope_key(company_id, project_id)
            branch = chunk.get("branch", "main")
            project_name = chunk.get("project_name") or scope_key

            canonical = _build_canonical_node_set(company_id, scope_key, project_name, content_type)
            belongs_to_set = [canonical]
            source_node_set = canonical

            point = PointStruct(
                id=chunk_id,
                vector={"text": chunk["embedding"]},
                payload={
                    # ── Cognee IndexSchema fields ──
                    "id": chunk_id,
                    "created_at": int(time.time() * 1000),
                    "updated_at": int(time.time() * 1000),
                    "ontology_valid": False,
                    "version": 1,
                    "topological_rank": 0,
                    "metadata": {"index_fields": ["text"]},
                    "type": "IndexSchema",
                    "belongs_to_set": belongs_to_set,
                    "source_pipeline": "v2_ingestion",
                    "source_task": "chunk_storage",
                    "node_set": source_node_set,
                    "source_node_set": source_node_set,
                    "source_user": "default_user@example.com",
                    "database_name": f"cognee-{company_id}",
                    "dataset_id": company_id,
                    "text": content,
                    # ── Legacy/extra fields ──
                    "header": chunk.get("header", ""),
                    "file_path": chunk.get("file_path", ""),
                    "language": chunk.get("language", ""),
                    "repository": chunk.get("repository", ""),
                    "branch": branch,
                    "company_id": company_id,
                    **({"project_id": project_id} if content_type != "document" else {}),
                    "chunk_index": chunk.get("chunk_index", 0),
                    "start_line": chunk.get("start_line", 0),
                    "end_line": chunk.get("end_line", 0),
                    "ingestion_id": str(chunk.get("ingestion_id", "")),
                    "file_version_id": str(chunk.get("file_version_id", "")),
                },
            )
            points.append(point)

        return await self._batch_upsert(
            collection_name,
            points,
        )

    async def upsert_entities(
        self,
        entities: List[Dict[str, Any]],
    ) -> int:
        """Batch upsert entity points to Entity_name collection.

        Each entity dict must have: entity_id, embedding, name, entity_type,
        description, company_id, project_id, branch.

        Returns number of points upserted.
        """
        if not entities:
            return 0

        points = []
        for entity in entities:
            entity_id = str(entity["entity_id"])
            entity_name = entity.get("name", "")
            company_id = entity.get("company_id", "")
            project_id = entity.get("project_id") or None
            content_type = entity.get("content_type", "code")
            scope_key = _scope_key(company_id, project_id)
            branch = entity.get("branch", "main")
            project_name = entity.get("project_name") or scope_key
            canonical = _build_canonical_node_set(company_id, scope_key, project_name, content_type)
            belongs_to_set = [canonical]
            source_node_set = canonical

            point = PointStruct(
                id=entity_id,
                vector={"text": entity["embedding"]},
                payload={
                    # ── Cognee IndexSchema fields ──
                    "id": entity_id,
                    "created_at": int(time.time() * 1000),
                    "updated_at": int(time.time() * 1000),
                    "ontology_valid": False,
                    "version": 1,
                    "topological_rank": 0,
                    "metadata": {"index_fields": ["name"]},
                    "type": "IndexSchema",
                    "belongs_to_set": belongs_to_set,
                    "source_pipeline": "v2_ingestion",
                    "source_task": "entity_storage",
                    "node_set": source_node_set,
                    "source_node_set": source_node_set,
                    "source_user": "default_user@example.com",
                    "database_name": f"cognee-{company_id}",
                    "dataset_id": company_id,
                    "name": entity_name,
                    # ── Legacy/extra fields ──
                    "entity_type": entity.get("entity_type", ""),
                    "description": entity.get("description", ""),
                    "company_id": company_id,
                    **({"project_id": project_id} if content_type != "document" else {}),
                    "branch": branch,
                    "file_version_id": str(entity.get("file_version_id", "")),
                },
            )
            points.append(point)

        return await self._batch_upsert(
            self._config.COLLECTION_ENTITIES,
            points,
        )

    async def upsert_summaries(
        self,
        summaries: List[Dict[str, Any]],
    ) -> int:
        """Batch upsert summary points to TextSummary_text collection.

        Each summary dict must have: summary_id, embedding, summary_text,
        chunk_id, company_id, project_id, branch.

        Returns number of points upserted.
        """
        if not summaries:
            return 0

        points = []
        for summary in summaries:
            summary_id = str(summary["summary_id"])
            summary_text = summary.get("summary_text", "")
            company_id = summary.get("company_id", "")
            project_id = summary.get("project_id") or None
            content_type = summary.get("content_type", "code")
            scope_key = _scope_key(company_id, project_id)
            branch = summary.get("branch", "main")
            project_name = summary.get("project_name") or scope_key
            canonical = _build_canonical_node_set(company_id, scope_key, project_name, content_type)
            belongs_to_set = [canonical]
            source_node_set = canonical

            point = PointStruct(
                id=summary_id,
                vector={"text": summary["embedding"]},  # Named vector for Cognee
                payload={
                    # ── Cognee IndexSchema fields ──
                    "id": summary_id,
                    "created_at": int(time.time() * 1000),
                    "updated_at": int(time.time() * 1000),
                    "ontology_valid": False,
                    "version": 1,
                    "topological_rank": 0,
                    "metadata": {"index_fields": ["text"]},
                    "type": "IndexSchema",
                    "belongs_to_set": belongs_to_set,
                    "source_pipeline": "v2_ingestion",
                    "source_task": "summary_storage",
                    "node_set": source_node_set,
                    "source_node_set": source_node_set,
                    "source_user": "default_user@example.com",
                    "database_name": f"cognee-{company_id}",
                    "dataset_id": company_id,
                    "text": summary_text,
                    # ── Legacy/extra fields ──
                    "chunk_id": str(summary.get("chunk_id", "")),
                    "company_id": company_id,
                    **({"project_id": project_id} if content_type != "document" else {}),
                    "branch": branch,
                    "file_version_id": str(summary.get("file_version_id", "")),
                },
            )
            points.append(point)

        return await self._batch_upsert(
            self._config.COLLECTION_SUMMARIES,
            points,
        )

    async def upsert_triplets(
        self,
        triplets: List[Dict[str, Any]],
    ) -> int:
        """Batch upsert triplet points to Triplet_text collection.

        Each triplet dict must have: triplet_id, embedding, source_name,
        target_name, relationship_type, source_entity_id, target_entity_id,
        company_id, project_id, branch.

        Returns number of points upserted.
        """
        if not triplets:
            return 0

        points = []
        for triplet in triplets:
            triplet_id = str(triplet["triplet_id"])
            source_name = triplet.get("source_name", "")
            target_name = triplet.get("target_name", "")
            relationship = triplet.get("relationship_type", "")
            company_id = triplet.get("company_id", "")
            project_id = triplet.get("project_id") or None
            content_type = triplet.get("content_type", "code")
            scope_key = _scope_key(company_id, project_id)
            branch = triplet.get("branch", "main")
            project_name = triplet.get("project_name") or scope_key
            canonical = _build_canonical_node_set(company_id, scope_key, project_name, content_type)
            belongs_to_set = [canonical]
            source_node_set = canonical

            # Cognee triplet text format: "source-›relationship-›target"
            triplet_text = f"{source_name}-›{relationship}-›{target_name}"

            point = PointStruct(
                id=triplet_id,
                vector={"text": triplet["embedding"]},  # Named vector for Cognee
                payload={
                    # ── Cognee IndexSchema fields ──
                    "id": triplet_id,
                    "created_at": int(time.time() * 1000),
                    "updated_at": int(time.time() * 1000),
                    "ontology_valid": False,
                    "version": 1,
                    "topological_rank": 0,
                    "metadata": {"index_fields": ["text"]},
                    "type": "IndexSchema",
                    "belongs_to_set": belongs_to_set,
                    "source_pipeline": "v2_ingestion",
                    "source_task": "triplet_storage",
                    "node_set": source_node_set,
                    "source_node_set": source_node_set,
                    "source_user": "default_user@example.com",
                    "database_name": f"cognee-{company_id}",
                    "dataset_id": company_id,
                    "text": triplet_text,
                    # ── Triplet-specific fields ──
                    "from_node_id": str(triplet.get("source_entity_id", "")),
                    "to_node_id": str(triplet.get("target_entity_id", "")),
                    "relationship_type": relationship,
                    "source_name": source_name,
                    "target_name": target_name,
                    # ── Legacy/extra fields ──
                    "company_id": company_id,
                    **({"project_id": project_id} if content_type != "document" else {}),
                    "branch": branch,
                    "file_version_id": str(triplet.get("file_version_id", "")),
                },
            )
            points.append(point)

        return await self._batch_upsert(
            self._config.COLLECTION_TRIPLETS,
            points,
        )

    async def upsert_edge_types(
        self,
        edge_types: List[Dict[str, Any]],
    ) -> int:
        """Batch upsert edge type points to EdgeType_relationship_name collection.

        Each edge_type dict must have: edge_type_id, embedding, relationship_name,
        number_of_edges, company_id, project_id, branch.

        Matches Cognee's EdgeType DataPoint format with index_fields=["relationship_name"].

        Returns number of points upserted.
        """
        if not edge_types:
            return 0

        points = []
        for edge_type in edge_types:
            edge_type_id = str(edge_type["edge_type_id"])
            relationship_name = edge_type.get("relationship_name", "")
            number_of_edges = edge_type.get("number_of_edges", 0)
            company_id = edge_type.get("company_id", "")
            project_id = edge_type.get("project_id") or None
            content_type = edge_type.get("content_type", "code")
            scope_key = _scope_key(company_id, project_id)
            branch = edge_type.get("branch", "main")
            project_name = edge_type.get("project_name") or scope_key
            canonical = _build_canonical_node_set(company_id, scope_key, project_name, content_type)

            point = PointStruct(
                id=edge_type_id,
                vector={"text": edge_type["embedding"]},
                payload={
                    # ── Cognee IndexSchema fields ──
                    "id": edge_type_id,
                    "created_at": int(time.time() * 1000),
                    "updated_at": int(time.time() * 1000),
                    "ontology_valid": False,
                    "version": 1,
                    "topological_rank": 0,
                    "metadata": {"index_fields": ["relationship_name"]},
                    "type": "IndexSchema",
                    "belongs_to_set": [canonical],
                    "source_pipeline": "v2_ingestion",
                    "source_task": "edge_type_storage",
                    "node_set": canonical,
                    "source_node_set": canonical,
                    "source_user": "default_user@example.com",
                    "database_name": f"cognee-{company_id}",
                    "dataset_id": company_id,
                    "relationship_name": relationship_name,
                    # ── EdgeType-specific fields ──
                    "number_of_edges": number_of_edges,
                    # ── Legacy/extra fields ──
                    "company_id": company_id,
                    **({"project_id": project_id} if content_type != "document" else {}),
                    "branch": branch,
                    "file_version_id": str(edge_type.get("file_version_id", "")),
                },
            )
            points.append(point)

        return await self._batch_upsert(
            self._config.COLLECTION_EDGE_TYPES,
            points,
        )

    async def upsert_entity_types(
        self,
        entity_types: List[Dict[str, Any]],
    ) -> int:
        """Batch upsert entity type points to EntityType_name collection.

        Each entity_type dict must have: entity_type_id, embedding, name,
        number_of_entities, company_id, project_id, branch.

        Matches Cognee's EntityType DataPoint format with index_fields=["name"].

        Returns number of points upserted.
        """
        if not entity_types:
            return 0

        points = []
        for entity_type in entity_types:
            entity_type_id = str(entity_type["entity_type_id"])
            name = entity_type.get("name", "")
            number_of_entities = entity_type.get("number_of_entities", 0)
            company_id = entity_type.get("company_id", "")
            project_id = entity_type.get("project_id") or None
            content_type = entity_type.get("content_type", "code")
            scope_key = _scope_key(company_id, project_id)
            branch = entity_type.get("branch", "main")
            project_name = entity_type.get("project_name") or scope_key
            canonical = _build_canonical_node_set(company_id, scope_key, project_name, content_type)

            point = PointStruct(
                id=entity_type_id,
                vector={"text": entity_type["embedding"]},
                payload={
                    # ── Cognee IndexSchema fields ──
                    "id": entity_type_id,
                    "created_at": int(time.time() * 1000),
                    "updated_at": int(time.time() * 1000),
                    "ontology_valid": False,
                    "version": 1,
                    "topological_rank": 0,
                    "metadata": {"index_fields": ["name"]},
                    "type": "IndexSchema",
                    "belongs_to_set": [canonical],
                    "source_pipeline": "v2_ingestion",
                    "source_task": "entity_type_storage",
                    "node_set": canonical,
                    "source_node_set": canonical,
                    "source_user": "default_user@example.com",
                    "database_name": f"cognee-{company_id}",
                    "dataset_id": company_id,
                    "name": name,
                    # ── EntityType-specific fields ──
                    "number_of_entities": number_of_entities,
                    # ── Legacy/extra fields ──
                    "company_id": company_id,
                    **({"project_id": project_id} if content_type != "document" else {}),
                    "branch": branch,
                    "file_version_id": str(entity_type.get("file_version_id", "")),
                },
            )
            points.append(point)

        return await self._batch_upsert(
            self._config.COLLECTION_ENTITY_TYPES,
            points,
        )

    # ── Internal Batch Logic ──────────────────────────────────────────

    async def _batch_upsert(
        self,
        collection_name: str,
        points: List[PointStruct],
    ) -> int:
        """Upsert points in batches with retry logic.

        Splits points into batches of QDRANT_BATCH_SIZE, upserts each with
        exponential backoff retry on transient failures.

        Returns total number of points upserted.
        """
        if not points:
            return 0

        batch_size = self._config.QDRANT_BATCH_SIZE
        total_upserted = 0

        for i in range(0, len(points), batch_size):
            batch = points[i : i + batch_size]
            await self._upsert_with_retry(collection_name, batch)
            total_upserted += len(batch)

            logger.debug(
                "writer.batch_upserted",
                collection=collection_name,
                batch_size=len(batch),
                total_so_far=total_upserted,
                total_points=len(points),
            )

        logger.info(
            "writer.upsert_complete",
            collection=collection_name,
            total_upserted=total_upserted,
        )

        return total_upserted

    async def _upsert_with_retry(
        self,
        collection_name: str,
        points: List[PointStruct],
    ) -> None:
        """Upsert a single batch with exponential backoff retry.

        If the first failure is a collection-not-found error, auto-creates
        the collection and retries immediately (does not count as a retry
        attempt). Subsequent failures use normal exponential backoff.
        """
        last_error: Optional[Exception] = None
        collection_auto_created = False

        for attempt in range(1, self._config.MAX_RETRIES + 1):
            try:
                await self._client.upsert(
                    collection_name=collection_name,
                    points=points,
                )
                return
            except Exception as e:
                last_error = e

                # ── Auto-create missing collection (once) ──
                if not collection_auto_created and self._is_collection_not_found(e):
                    logger.warning(
                        "writer.collection_not_found_auto_creating",
                        collection=collection_name,
                        attempt=attempt,
                        error=str(e),
                    )
                    try:
                        await self._ensure_single_collection(collection_name)
                        collection_auto_created = True
                        # Retry immediately — don't count this attempt
                        continue
                    except Exception as create_err:
                        logger.error(
                            "writer.collection_auto_create_failed",
                            collection=collection_name,
                            error=str(create_err),
                        )
                        # Fall through to normal retry logic

                logger.warning(
                    "writer.upsert_retry",
                    collection=collection_name,
                    attempt=attempt,
                    max_retries=self._config.MAX_RETRIES,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                if attempt < self._config.MAX_RETRIES:
                    delay = self._config.RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)

        raise RuntimeError(
            f"Qdrant upsert failed after {self._config.MAX_RETRIES} retries "
            f"for collection {collection_name}: {last_error}"
        )

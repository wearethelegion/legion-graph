"""
Qdrant Repository
Handles Qdrant collection management for multi-tenant scoping.
"""

from typing import Optional, List, Dict, Any
from uuid import uuid4
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue,
    MatchAny,
    Range,
)
from loguru import logger
from kgrag.config import Config


class QdrantRepository:
    """Repository for Qdrant collection operations."""

    def __init__(self):
        """Initialize Qdrant client."""
        config = Config()

        self.host = config.QDRANT_HOST
        self.port = config.QDRANT_PORT
        self.api_key = config.QDRANT_API_KEY

        qdrant_kwargs = {
            "host": self.host,
            "port": self.port,
        }
        if self.api_key:
            qdrant_kwargs["api_key"] = self.api_key

        self.client = QdrantClient(**qdrant_kwargs)
        logger.info(f"Initialized QdrantRepository at {self.host}:{self.port}")

    async def create_collection(self, collection_name: str, vector_size: int = 768) -> None:
        """
        Create Qdrant collection if it doesn't exist.

        Args:
            collection_name: Name of the collection to create
            vector_size: Vector dimension size (default: 768 for Gemini embeddings)

        Raises:
            Exception: If collection creation fails
        """
        try:
            # Check if collection exists
            collections = self.client.get_collections().collections
            collection_exists = any(c.name == collection_name for c in collections)

            if collection_exists:
                logger.info(f"Collection '{collection_name}' already exists")
                return

            # Create collection with COSINE distance
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
            )
            logger.info(f"Created Qdrant collection: {collection_name}")

        except Exception as e:
            # Handle race condition: another process may have created the collection
            if "already exists" in str(e).lower() or "409" in str(e):
                logger.info(f"Collection '{collection_name}' already exists (race condition)")
                return

            logger.error("Failed to create collection '{}': {}", collection_name, e)
            raise

    async def delete_collection(self, collection_name: str) -> None:
        """
        Delete Qdrant collection.

        Args:
            collection_name: Name of the collection to delete

        Raises:
            Exception: If collection deletion fails
        """
        try:
            # Check if collection exists
            collections = self.client.get_collections().collections
            collection_exists = any(c.name == collection_name for c in collections)

            if not collection_exists:
                logger.info(f"Collection '{collection_name}' doesn't exist, nothing to delete")
                return

            # Delete collection
            self.client.delete_collection(collection_name=collection_name)
            logger.info(f"Deleted Qdrant collection: {collection_name}")

        except Exception as e:
            logger.error("Failed to delete collection '{}': {}", collection_name, e)
            raise

    async def collection_exists(self, collection_name: str) -> bool:
        """
        Check if collection exists.

        Args:
            collection_name: Name of the collection to check

        Returns:
            True if collection exists, False otherwise

        Raises:
            Exception: If connection to Qdrant fails
        """
        try:
            collections = self.client.get_collections().collections
            return any(c.name == collection_name for c in collections)
        except Exception as e:
            logger.error("Failed to check if collection '{}' exists: {}", collection_name, e)
            raise

    async def upsert_points(self, collection_name: str, points: List[Dict[str, Any]]) -> None:
        """
        Upsert points into Qdrant collection.

        Args:
            collection_name: Name of the collection
            points: List of point dicts with 'id', 'vector', 'payload'

        Raises:
            Exception: If upsert fails
        """
        try:
            # Convert dict points to PointStruct
            point_structs = [
                PointStruct(id=point["id"], vector=point["vector"], payload=point["payload"])
                for point in points
            ]

            self.client.upsert(collection_name=collection_name, points=point_structs)
            logger.debug(f"Upserted {len(points)} points to collection '{collection_name}'")

        except Exception as e:
            logger.error("Failed to upsert points to collection '{}': {}", collection_name, e)
            raise

    async def health_check(self) -> bool:
        """
        Check if Qdrant is healthy and accessible.

        Returns:
            True if healthy, False otherwise

        Note: This method returns False on errors for health check purposes.
        Does not raise exceptions.
        """
        try:
            # Try to get collections as a simple health check
            self.client.get_collections()
            return True
        except Exception as e:
            logger.error("Qdrant health check failed: {}", e)
            return False

    async def search_by_tags(
        self, collection_name: str, filters: Dict[str, Any], limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search points by metadata filters (no vector search).

        Uses Qdrant scroll API for metadata-only filtering.
        Ultra-fast queries (10-50ms) with zero LLM cost.

        Args:
            collection_name: Name of the collection to search
            filters: Metadata filters to apply:
                - project_id (str): Required - project UUID
                - user_id (str): Required - user UUID
                - keywords (List[str]): Optional - must match ANY keyword
                - chunk_type (str): Optional - prose/code/table/list
                - has_code (bool): Optional - contains code blocks
                - section_title (str): Optional - exact match
                - section_level (int): Optional - heading level
            limit: Maximum results to return (default: 100)

        Returns:
            List of matching points with payload data

        Raises:
            Exception: If search fails
        """
        try:
            # Build Qdrant filter conditions
            must_conditions = []

            # Required filters
            if "project_id" in filters:
                must_conditions.append(
                    FieldCondition(key="project_id", match=MatchValue(value=filters["project_id"]))
                )

            # Optional: user_id filter (knowledge is project-scoped, not user-scoped)
            if "user_id" in filters:
                must_conditions.append(
                    FieldCondition(key="user_id", match=MatchValue(value=filters["user_id"]))
                )

            # Optional filters
            if "chunk_type" in filters:
                must_conditions.append(
                    FieldCondition(key="chunk_type", match=MatchValue(value=filters["chunk_type"]))
                )

            if "has_code" in filters:
                must_conditions.append(
                    FieldCondition(key="has_code", match=MatchValue(value=filters["has_code"]))
                )

            if "section_title" in filters:
                must_conditions.append(
                    FieldCondition(
                        key="section_title", match=MatchValue(value=filters["section_title"])
                    )
                )

            if "section_level" in filters:
                must_conditions.append(
                    FieldCondition(
                        key="section_level", match=MatchValue(value=filters["section_level"])
                    )
                )

            # Keywords: match ANY keyword (OR logic)
            if "keywords" in filters and filters["keywords"]:
                must_conditions.append(
                    FieldCondition(key="keywords", match=MatchAny(any=filters["keywords"]))
                )

            # Build filter object
            if not must_conditions:
                logger.warning("No filter conditions provided, returning empty results")
                return []

            filter_obj = Filter(must=must_conditions)

            # Debug: Log the filter being applied
            logger.info(
                f"searchByTags filter conditions: {[f'{c.key}={c.match}' for c in must_conditions]}"
            )
            logger.info(f"searchByTags collection: {collection_name}, limit: {limit}")

            # Execute scroll (metadata-only search)
            results = self.client.scroll(
                collection_name=collection_name,
                scroll_filter=filter_obj,
                limit=limit,
                with_payload=True,
                with_vectors=False,  # Don't return vectors (faster)
            )[0]  # scroll returns (records, next_page_offset)

            logger.info(
                f"Tag search found {len(results)} results in collection '{collection_name}'"
            )

            # Format results
            formatted_results = []
            for record in results:
                formatted_results.append(
                    {
                        "id": record.id,
                        "text": record.payload.get("content", ""),
                        "metadata": {
                            "knowledge_id": record.payload.get("knowledge_id"),
                            "chunk_index": record.payload.get("chunk_index"),
                            "summary": record.payload.get("summary"),
                            "section_title": record.payload.get("section_title"),
                            "section_level": record.payload.get("section_level"),
                            "chunk_type": record.payload.get("chunk_type"),
                            "has_code": record.payload.get("has_code"),
                            "keywords": record.payload.get("keywords", []),
                            "parent_section": record.payload.get("parent_section"),
                        },
                        "score": 1.0,  # No relevance scoring for metadata search
                    }
                )

            return formatted_results

        except Exception as e:
            logger.error("Tag search failed in collection '{}': {}", collection_name, e, exc_info=True)
            raise

    async def search_skill_details(
        self, collection_name: str, query_vector: List[float], expertise_id: str, limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Semantic search within a specific expertise's chunks.

        Filters by expertise_id and returns top-k relevant chunks with full content.
        Used for progressive skill disclosure - LLM searches within a skill instead
        of scanning all section titles.

        Args:
            collection_name: Qdrant collection name (e.g., "company_{company_id}")
            query_vector: Embedded query vector (768 dims for Gemini)
            expertise_id: Filter to this expertise only
            limit: Maximum results (default: 5)

        Returns:
            List of matching chunks with content, score, and metadata

        Raises:
            Exception: If search fails
        """
        try:
            # Build filter for expertise_id
            filter_obj = Filter(
                must=[FieldCondition(key="expertise_id", match=MatchValue(value=expertise_id))]
            )

            # Execute vector search with filter using query_points
            results = self.client.query_points(
                collection_name=collection_name,
                query=query_vector,
                query_filter=filter_obj,
                limit=limit,
                with_payload=True,
            ).points

            logger.info(
                f"Skill search found {len(results)} results for expertise {expertise_id} "
                f"in collection '{collection_name}'"
            )

            # Format results with full content
            formatted_results = []
            for result in results:
                payload = result.payload or {}
                metadata = payload.get("metadata", {})

                # Extract has_code from metadata or payload
                has_code = payload.get("has_code", False)
                if isinstance(metadata, dict):
                    has_code = metadata.get("has_code", has_code)

                formatted_results.append(
                    {
                        "chunk_id": payload.get("chunk_id", str(result.id)),
                        "title": payload.get("summary", payload.get("section_title", "Section")),
                        "content": payload.get("content", ""),
                        "score": result.score,
                        "has_code": has_code,
                        "level": payload.get("level", 0),
                        "position": payload.get("position", 0),
                    }
                )

            return formatted_results

        except Exception as e:
            logger.error(
                f"Skill search failed for expertise {expertise_id} "
                f"in collection '{collection_name}': {e}",
                exc_info=True,
            )
            raise

    async def search_points(
        self,
        collection_name: str,
        query_vector: List[float],
        limit: int = 10,
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generic semantic vector search with optional metadata filters.

        Enables semantic search across any collection with flexible filtering.
        Used for searchEntries and other cross-entity searches.

        Args:
            collection_name: Qdrant collection name (e.g., "company_{company_id}")
            query_vector: Embedded query vector (768 dims for Gemini)
            limit: Maximum results (default: 10)
            filter_conditions: Optional filter dict with format:
                {"must": [{"key": "entity_type", "match": {"value": "engagement_entry"}}]}

        Returns:
            List of matching points with id, score, text, and metadata

        Raises:
            Exception: If search fails
        """
        try:
            # Build Qdrant filter from conditions dict
            filter_obj = None
            if filter_conditions:
                must_conditions = []

                for condition in filter_conditions.get("must", []):
                    key = condition.get("key")
                    match_spec = condition.get("match", {})

                    if not key:
                        continue

                    # Handle "value" match (exact match)
                    if "value" in match_spec:
                        must_conditions.append(
                            FieldCondition(key=key, match=MatchValue(value=match_spec["value"]))
                        )
                    # Handle "any" match (match any in list)
                    elif "any" in match_spec:
                        must_conditions.append(
                            FieldCondition(key=key, match=MatchAny(any=match_spec["any"]))
                        )

                if must_conditions:
                    filter_obj = Filter(must=must_conditions)

            # Execute vector search using query_points
            logger.info(
                f"[qdrant_repo] client_type={type(self.client).__name__} "
                f"prefer_grpc={getattr(self.client, '_prefer_grpc', 'N/A')} "
                f"url={getattr(self.client, '_url', getattr(self.client, 'rest_uri', 'N/A'))} "
                f"collection={collection_name} limit={limit} "
                f"filter_obj={filter_obj} query_vector_len={len(query_vector)}"
            )
            raw_response = self.client.query_points(
                collection_name=collection_name,
                query=query_vector,
                query_filter=filter_obj,
                limit=limit,
                with_payload=True,
            )
            logger.info(
                f"[qdrant_repo] raw_response type={type(raw_response).__name__} "
                f"repr={repr(raw_response)[:500]}"
            )
            results = raw_response.points

            logger.info(
                f"search_points found {len(results)} results in collection '{collection_name}'"
            )

            # Format results
            formatted_results = []
            for result in results:
                payload = result.payload or {}

                formatted_results.append(
                    {
                        "id": str(result.id),
                        "score": result.score,
                        "text": payload.get("content", ""),
                        "metadata": payload,
                    }
                )

            return formatted_results

        except Exception as e:
            from qdrant_client.http.exceptions import UnexpectedResponse
            if isinstance(e, UnexpectedResponse) and e.status_code == 404:
                logger.error(
                    f"search_points: Qdrant collection '{collection_name}' does NOT EXIST. "
                    f"Data was likely lost during a Docker rebuild. Re-indexing required."
                )
            else:
                import traceback
                logger.error(
                    f"search_points failed in '{collection_name}': type={type(e).__name__} "
                    f"str={str(e)}\n{traceback.format_exc()}"
                )
            raise

    async def delete_points_by_filter(
        self, collection_name: str, filter_field: str, filter_value: str
    ) -> int:
        """
        Delete all points matching a filter condition.

        Used for project/entity cleanup - deletes all vectors associated
        with a specific project_id, knowledge_id, or other filter.

        Args:
            collection_name: Name of the collection
            filter_field: Metadata field to filter on (e.g., "project_id")
            filter_value: Value to match (e.g., project UUID)

        Returns:
            Number of points deleted (approximate)

        Raises:
            Exception: If deletion fails
        """
        try:
            # Check if collection exists first
            collections = self.client.get_collections().collections
            collection_exists = any(c.name == collection_name for c in collections)

            if not collection_exists:
                logger.info(f"Collection '{collection_name}' doesn't exist, nothing to delete")
                return 0

            # Get count before deletion for logging
            count_before = self.client.count(
                collection_name=collection_name,
                count_filter=Filter(
                    must=[FieldCondition(key=filter_field, match=MatchValue(value=filter_value))]
                ),
            ).count

            if count_before == 0:
                logger.info(
                    f"No points found with {filter_field}={filter_value} in '{collection_name}'"
                )
                return 0

            # Delete points by filter
            self.client.delete(
                collection_name=collection_name,
                points_selector=Filter(
                    must=[FieldCondition(key=filter_field, match=MatchValue(value=filter_value))]
                ),
            )

            logger.info(
                f"Deleted {count_before} points with {filter_field}={filter_value} "
                f"from collection '{collection_name}'"
            )
            return count_before

        except Exception as e:
            logger.error(
                f"Failed to delete points with {filter_field}={filter_value} "
                f"from collection '{collection_name}': {e}"
            )
            raise

    async def transfer_project_points_between_companies(
        self,
        project_id: str,
        source_company_id: str,
        target_company_id: str,
        batch_size: int = 200,
    ) -> Dict[str, int | str]:
        """
        Move all project-scoped points from source company collection to target.

        Procedure:
        1) Read source points by project_id (with vectors)
        2) Upsert into target collection with updated payload.company_id
        3) Delete source points only after successful target upsert

        Args:
            project_id: Project UUID
            source_company_id: Source company UUID
            target_company_id: Target company UUID
            batch_size: Scroll/upsert batch size

        Returns:
            Stats with copied/deleted counts and collection names
        """
        source_collection = f"company_{source_company_id}"
        target_collection = f"company_{target_company_id}"

        if source_collection == target_collection:
            return {
                "source_collection": source_collection,
                "target_collection": target_collection,
                "points_copied": 0,
                "points_deleted_from_source": 0,
            }

        try:
            source_exists = await self.collection_exists(source_collection)
            if not source_exists:
                logger.info(
                    f"Source collection '{source_collection}' not found; treating as already migrated"
                )
                return {
                    "source_collection": source_collection,
                    "target_collection": target_collection,
                    "points_copied": 0,
                    "points_deleted_from_source": 0,
                }

            await self.create_collection(target_collection)

            filter_obj = Filter(
                must=[
                    FieldCondition(
                        key="project_id",
                        match=MatchValue(value=project_id),
                    )
                ]
            )

            copied_count = 0
            next_offset = None

            while True:
                records, next_offset = self.client.scroll(
                    collection_name=source_collection,
                    scroll_filter=filter_obj,
                    offset=next_offset,
                    limit=batch_size,
                    with_payload=True,
                    with_vectors=True,
                )

                if not records:
                    break

                points = []
                for record in records:
                    payload = dict(record.payload or {})
                    payload["company_id"] = target_company_id
                    points.append(
                        PointStruct(
                            id=record.id,
                            vector=record.vector,
                            payload=payload,
                        )
                    )

                self.client.upsert(collection_name=target_collection, points=points)
                copied_count += len(points)

                if next_offset is None:
                    break

            deleted_count = await self.delete_points_by_filter(
                collection_name=source_collection,
                filter_field="project_id",
                filter_value=project_id,
            )

            logger.info(
                f"Transferred Qdrant points for project {project_id}: "
                f"copied={copied_count}, deleted_from_source={deleted_count}, "
                f"{source_collection} -> {target_collection}"
            )

            return {
                "source_collection": source_collection,
                "target_collection": target_collection,
                "points_copied": copied_count,
                "points_deleted_from_source": deleted_count,
            }

        except Exception as e:
            logger.error(
                f"Failed to transfer Qdrant points for project {project_id} "
                f"from {source_collection} to {target_collection}: {e}",
                exc_info=True,
            )
            raise

    async def count_by_company(self, company_id: str) -> Dict[str, int]:
        """
        Count vectors by entity type for a company.

        Counts points in the company collection filtered by entity_type payload field.
        Used for statistics endpoints.

        Args:
            company_id: Company UUID

        Returns:
            Dict with counts: {knowledge, expertise, code, lessons}

        Raises:
            Exception: If count operation fails
        """
        collection_name = f"company_{company_id}"

        try:
            # Check if collection exists
            collections = self.client.get_collections().collections
            collection_exists = any(c.name == collection_name for c in collections)

            if not collection_exists:
                logger.debug(f"Collection '{collection_name}' doesn't exist, returning zeros")
                return {"knowledge": 0, "expertise": 0, "code": 0, "lessons": 0}

            # Count by entity_type
            entity_types = ["knowledge", "expertise", "code", "lesson"]
            counts = {}

            for entity_type in entity_types:
                filter_obj = Filter(
                    must=[FieldCondition(key="entity_type", match=MatchValue(value=entity_type))]
                )

                count_result = self.client.count(
                    collection_name=collection_name, count_filter=filter_obj
                )
                # Use plural for lessons key to match model
                key = "lessons" if entity_type == "lesson" else entity_type
                counts[key] = count_result.count

            logger.debug(f"Count by company {company_id}: {counts}")
            return counts

        except Exception as e:
            logger.error("Failed to count by company {}: {}", company_id, e, exc_info=True)
            raise

    async def count_by_project(self, project_id: str, company_id: str) -> Dict[str, int]:
        """
        Count vectors by entity type for a project.

        Counts points in the company collection filtered by project_id and entity_type.
        Used for statistics endpoints.

        Args:
            project_id: Project UUID
            company_id: Company UUID (for collection name)

        Returns:
            Dict with counts: {knowledge, expertise, code, lessons}

        Raises:
            Exception: If count operation fails
        """
        collection_name = f"company_{company_id}"

        try:
            # Check if collection exists
            collections = self.client.get_collections().collections
            collection_exists = any(c.name == collection_name for c in collections)

            if not collection_exists:
                logger.debug(f"Collection '{collection_name}' doesn't exist, returning zeros")
                return {"knowledge": 0, "expertise": 0, "code": 0, "lessons": 0}

            # Count by entity_type with project_id filter
            entity_types = ["knowledge", "expertise", "code", "lesson"]
            counts = {}

            for entity_type in entity_types:
                filter_obj = Filter(
                    must=[
                        FieldCondition(key="project_id", match=MatchValue(value=project_id)),
                        FieldCondition(key="entity_type", match=MatchValue(value=entity_type)),
                    ]
                )

                count_result = self.client.count(
                    collection_name=collection_name, count_filter=filter_obj
                )
                # Use plural for lessons key to match model
                key = "lessons" if entity_type == "lesson" else entity_type
                counts[key] = count_result.count

            logger.debug(f"Count by project {project_id}: {counts}")
            return counts

        except Exception as e:
            logger.error("Failed to count by project {}: {}", project_id, e, exc_info=True)
            raise

    # =========================================================================
    # AGENT MEMORY METHODS
    # =========================================================================

    async def index_agent_memory(
        self,
        company_id: str,
        agent_id: str,
        memory_id: str,
        content: str,
        vector: List[float],
        title: str,
        memory_type: str,
        category: str,
        importance: int,
        source_origin: str,
        project_id: Optional[str] = None,
        user_id: Optional[str] = None,
        summary: Optional[str] = None,
        tags: Optional[List[str]] = None,
        source: Optional[str] = None,
        source_type: Optional[str] = None,
        created_at: Optional[str] = None,
        last_accessed_at: Optional[str] = None,
    ) -> str:
        """
        Index an agent memory in Qdrant.

        Stores memory with full payload including metadata filters for
        category, importance, tags, and source_origin.

        Args:
            company_id: Company UUID (required for collection name)
            agent_id: Agent UUID (required)
            memory_id: Memory UUID (required)
            content: Full memory text (embedded)
            vector: Pre-computed embedding vector (768 dims)
            title: Memory title
            memory_type: Type enum (fact, preference, skill, relationship, context)
            category: Category enum (good_practice, pitfall, user_preference,
                      project_context, tool_usage, communication_style, domain_knowledge)
            importance: Integer 1-5
            source_origin: Origin enum (user_explicit, inferred, system_generated)
            project_id: Optional project UUID
            user_id: Optional user UUID for user-specific memories
            summary: Optional summary
            tags: Optional list of tags
            source: Optional source engagement_id or entry_id
            source_type: Optional source type (engagement_entry, user_message, etc.)
            created_at: Optional creation timestamp
            last_accessed_at: Optional last access timestamp

        Returns:
            Point ID (UUID string) of the indexed memory

        Raises:
            Exception: If indexing fails
        """
        collection_name = f"company_{company_id}"

        try:
            # Build payload
            payload = {
                # Identity
                "entity_type": "agent_memory",
                "memory_id": memory_id,
                # Multi-tenant scoping
                "company_id": company_id,
                "agent_id": agent_id,
                # Content
                "content": content,
                "title": title,
                # Classification
                "memory_type": memory_type,
                # Metadata fields (per requirement)
                "category": category,
                "importance": importance,
                "source_origin": source_origin,
                "tags": tags or [],
            }

            # Optional fields
            if project_id:
                payload["project_id"] = project_id
            if user_id:
                payload["user_id"] = user_id
            if summary:
                payload["summary"] = summary
            if source:
                payload["source"] = source
            if source_type:
                payload["source_type"] = source_type
            if created_at:
                payload["created_at"] = created_at
            if last_accessed_at:
                payload["last_accessed_at"] = last_accessed_at

            # Generate point ID
            point_id = str(uuid4())

            # Upsert to Qdrant
            point = PointStruct(id=point_id, vector=vector, payload=payload)

            self.client.upsert(collection_name=collection_name, points=[point])

            logger.info(
                f"Indexed agent memory {memory_id} for agent {agent_id} "
                f"in collection '{collection_name}' (point_id={point_id})"
            )
            return point_id

        except Exception as e:
            logger.error(
                f"Failed to index agent memory {memory_id} for agent {agent_id}: {e}", exc_info=True
            )
            raise

    async def search_agent_memories(
        self,
        company_id: str,
        agent_id: str,
        query_vector: List[float],
        user_id: Optional[str] = None,
        project_id: Optional[str] = None,
        category: Optional[str] = None,
        memory_type: Optional[str] = None,
        min_importance: Optional[int] = None,
        tags: Optional[List[str]] = None,
        source_origin: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Search agent memories with semantic similarity and optional metadata filters.

        Combines vector search with flexible filtering on category, importance,
        tags, and source_origin.

        Args:
            company_id: Company UUID (required)
            agent_id: Agent UUID (required)
            query_vector: Query embedding vector (768 dims)
            user_id: Optional filter by user UUID
            project_id: Optional filter by project UUID
            category: Optional filter by category enum
            memory_type: Optional filter by memory type enum
            min_importance: Optional minimum importance (1-5)
            tags: Optional filter by tags (match ANY)
            source_origin: Optional filter by source origin enum
            limit: Maximum results (default: 10)

        Returns:
            List of matching memories with scores and metadata

        Raises:
            Exception: If search fails
        """
        collection_name = f"company_{company_id}"

        try:
            # Build filter conditions
            must_conditions = [
                FieldCondition(key="entity_type", match=MatchValue(value="agent_memory")),
                FieldCondition(key="agent_id", match=MatchValue(value=agent_id)),
                FieldCondition(key="company_id", match=MatchValue(value=company_id)),
            ]

            # Optional filters
            if user_id:
                must_conditions.append(
                    FieldCondition(key="user_id", match=MatchValue(value=user_id))
                )

            if project_id:
                must_conditions.append(
                    FieldCondition(key="project_id", match=MatchValue(value=project_id))
                )

            if category:
                must_conditions.append(
                    FieldCondition(key="category", match=MatchValue(value=category))
                )

            if memory_type:
                must_conditions.append(
                    FieldCondition(key="memory_type", match=MatchValue(value=memory_type))
                )

            if min_importance is not None:
                must_conditions.append(
                    FieldCondition(key="importance", range=Range(gte=min_importance))
                )

            if tags:
                must_conditions.append(FieldCondition(key="tags", match=MatchAny(any=tags)))

            if source_origin:
                must_conditions.append(
                    FieldCondition(key="source_origin", match=MatchValue(value=source_origin))
                )

            filter_obj = Filter(must=must_conditions)

            # Execute vector search
            results = self.client.query_points(
                collection_name=collection_name,
                query=query_vector,
                query_filter=filter_obj,
                limit=limit,
                with_payload=True,
            ).points

            logger.info(
                f"Agent memory search found {len(results)} results for agent {agent_id} "
                f"in collection '{collection_name}'"
            )

            # Format results
            formatted_results = []
            for result in results:
                payload = result.payload or {}
                formatted_results.append(
                    {
                        "id": str(result.id),
                        "score": result.score,
                        "memory_id": payload.get("memory_id"),
                        "title": payload.get("title"),
                        "content": payload.get("content"),
                        "summary": payload.get("summary"),
                        "memory_type": payload.get("memory_type"),
                        "category": payload.get("category"),
                        "importance": payload.get("importance"),
                        "tags": payload.get("tags", []),
                        "source_origin": payload.get("source_origin"),
                        "user_id": payload.get("user_id"),
                        "project_id": payload.get("project_id"),
                        "source": payload.get("source"),
                        "source_type": payload.get("source_type"),
                        "created_at": payload.get("created_at"),
                        "last_accessed_at": payload.get("last_accessed_at"),
                    }
                )

            return formatted_results

        except Exception as e:
            logger.error(
                f"Agent memory search failed for agent {agent_id} "
                f"in collection '{collection_name}': {e}",
                exc_info=True,
            )
            raise

    async def search_memories_by_category(
        self,
        company_id: str,
        agent_id: str,
        query_vector: List[float],
        category: str,
        user_id: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Search agent memories filtered by category.

        Convenience method for category-specific memory retrieval.

        Args:
            company_id: Company UUID
            agent_id: Agent UUID
            query_vector: Query embedding vector
            category: Category to filter by (good_practice, pitfall, etc.)
            user_id: Optional user UUID filter
            limit: Maximum results

        Returns:
            List of matching memories with scores
        """
        return await self.search_agent_memories(
            company_id=company_id,
            agent_id=agent_id,
            query_vector=query_vector,
            category=category,
            user_id=user_id,
            limit=limit,
        )

    async def search_memories_by_importance(
        self,
        company_id: str,
        agent_id: str,
        query_vector: List[float],
        min_importance: int,
        user_id: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Search agent memories with minimum importance threshold.

        Retrieves only high-priority memories (importance >= min_importance).

        Args:
            company_id: Company UUID
            agent_id: Agent UUID
            query_vector: Query embedding vector
            min_importance: Minimum importance level (1-5)
            user_id: Optional user UUID filter
            limit: Maximum results

        Returns:
            List of matching memories with scores
        """
        return await self.search_agent_memories(
            company_id=company_id,
            agent_id=agent_id,
            query_vector=query_vector,
            min_importance=min_importance,
            user_id=user_id,
            limit=limit,
        )

    async def search_memories_by_tags(
        self,
        company_id: str,
        agent_id: str,
        query_vector: List[float],
        tags: List[str],
        user_id: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Search agent memories matching any of the specified tags.

        Uses MatchAny for tag filtering (OR logic).

        Args:
            company_id: Company UUID
            agent_id: Agent UUID
            query_vector: Query embedding vector
            tags: List of tags to match (matches ANY)
            user_id: Optional user UUID filter
            limit: Maximum results

        Returns:
            List of matching memories with scores
        """
        return await self.search_agent_memories(
            company_id=company_id,
            agent_id=agent_id,
            query_vector=query_vector,
            tags=tags,
            user_id=user_id,
            limit=limit,
        )

    async def search_memories_by_source_origin(
        self,
        company_id: str,
        agent_id: str,
        query_vector: List[float],
        source_origin: str,
        user_id: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Search agent memories filtered by source origin.

        Filter by how the memory was created (user_explicit, inferred, system_generated).

        Args:
            company_id: Company UUID
            agent_id: Agent UUID
            query_vector: Query embedding vector
            source_origin: Source origin to filter by
            user_id: Optional user UUID filter
            limit: Maximum results

        Returns:
            List of matching memories with scores
        """
        return await self.search_agent_memories(
            company_id=company_id,
            agent_id=agent_id,
            query_vector=query_vector,
            source_origin=source_origin,
            user_id=user_id,
            limit=limit,
        )

    async def delete_agent_memory(self, company_id: str, memory_id: str) -> int:
        """
        Delete an agent memory by memory_id.

        Args:
            company_id: Company UUID
            memory_id: Memory UUID to delete

        Returns:
            Number of points deleted (0 or 1)

        Raises:
            Exception: If deletion fails
        """
        collection_name = f"company_{company_id}"

        try:
            # Count before deletion
            count_before = self.client.count(
                collection_name=collection_name,
                count_filter=Filter(
                    must=[
                        FieldCondition(key="entity_type", match=MatchValue(value="agent_memory")),
                        FieldCondition(key="memory_id", match=MatchValue(value=memory_id)),
                    ]
                ),
            ).count

            if count_before == 0:
                logger.info(f"No agent memory found with memory_id={memory_id}")
                return 0

            # Delete by filter
            self.client.delete(
                collection_name=collection_name,
                points_selector=Filter(
                    must=[
                        FieldCondition(key="entity_type", match=MatchValue(value="agent_memory")),
                        FieldCondition(key="memory_id", match=MatchValue(value=memory_id)),
                    ]
                ),
            )

            logger.info(f"Deleted agent memory {memory_id} from collection '{collection_name}'")
            return count_before

        except Exception as e:
            logger.error(
                f"Failed to delete agent memory {memory_id} "
                f"from collection '{collection_name}': {e}",
                exc_info=True,
            )
            raise

    async def update_agent_memory(
        self,
        company_id: str,
        memory_id: str,
        vector: List[float],
        content: str,
        title: Optional[str] = None,
        summary: Optional[str] = None,
        category: Optional[str] = None,
        importance: Optional[int] = None,
        tags: Optional[List[str]] = None,
        last_accessed_at: Optional[str] = None,
    ) -> bool:
        """
        Update an existing agent memory.

        Re-embeds if content changes. Partial updates supported.

        Args:
            company_id: Company UUID
            memory_id: Memory UUID to update
            vector: New embedding vector (required, re-embed if content changed)
            content: Updated content
            title: Optional new title
            summary: Optional new summary
            category: Optional new category
            importance: Optional new importance
            tags: Optional new tags list
            last_accessed_at: Optional new last access timestamp

        Returns:
            True if update succeeded, False if memory not found

        Raises:
            Exception: If update fails
        """
        collection_name = f"company_{company_id}"

        try:
            # Find existing memory
            existing = self.client.scroll(
                collection_name=collection_name,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(key="entity_type", match=MatchValue(value="agent_memory")),
                        FieldCondition(key="memory_id", match=MatchValue(value=memory_id)),
                    ]
                ),
                limit=1,
                with_payload=True,
                with_vectors=False,
            )[0]

            if not existing:
                logger.warning("Agent memory {} not found for update", memory_id)
                return False

            # Get existing payload and point_id
            existing_record = existing[0]
            point_id = existing_record.id
            payload = existing_record.payload or {}

            # Update payload fields
            payload["content"] = content
            if title is not None:
                payload["title"] = title
            if summary is not None:
                payload["summary"] = summary
            if category is not None:
                payload["category"] = category
            if importance is not None:
                payload["importance"] = importance
            if tags is not None:
                payload["tags"] = tags
            if last_accessed_at is not None:
                payload["last_accessed_at"] = last_accessed_at

            # Upsert with new vector
            point = PointStruct(id=point_id, vector=vector, payload=payload)

            self.client.upsert(collection_name=collection_name, points=[point])

            logger.info(f"Updated agent memory {memory_id} in collection '{collection_name}'")
            return True

        except Exception as e:
            logger.error(
                f"Failed to update agent memory {memory_id} in collection '{collection_name}': {e}",
                exc_info=True,
            )
            raise

    def rank_memories_with_importance(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply importance weighting to search results.

        Formula: weighted_score = score * (importance / 5)

        This boosts high-importance memories in the ranking while
        still considering semantic relevance.

        Args:
            results: List of memory search results with 'score' and 'importance'

        Returns:
            Results sorted by weighted_score (descending)
        """
        for result in results:
            importance = result.get("importance", 3)
            score = result.get("score", 0.0)
            result["weighted_score"] = score * (importance / 5.0)

        return sorted(results, key=lambda r: r.get("weighted_score", 0), reverse=True)

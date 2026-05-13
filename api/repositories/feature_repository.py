"""
Feature Repository
Database operations for features and feature chunks.
"""

import json
from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4
from api.repositories.base_repository import BaseRepository
from api.utils.text import sanitize_text
from loguru import logger


class FeatureRepository(BaseRepository):
    """Repository for feature and feature chunk operations."""

    async def create_feature(
        self,
        company_id: str,
        project_id: str,
        name: str,
        description: str,
        status: str = "ready for refinement",
        priority: str = "medium",
        next_prompt: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create new feature.

        Args:
            company_id: Company UUID
            project_id: Project UUID
            name: Feature name
            description: Feature description
            status: Feature status (default: "ready for refinement")
            priority: Feature priority (default: "medium")
            next_prompt: Next step or prompt
            metadata: Optional metadata dictionary
            created_by: Optional UUID of creator

        Returns:
            Created feature record
        """
        feature_id = str(uuid4())

        query = """
            INSERT INTO features (
                id, company_id, project_id, name, description,
                status, priority, next_prompt, metadata, created_by,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
            RETURNING id::text, company_id::text, project_id::text, name, description,
                      status, priority, next_prompt, metadata, chunk_count,
                      created_at, updated_at, created_by::text
        """

        row = await self.fetch_one(
            query,
            UUID(feature_id),
            self.parse_uuid(company_id),
            self.parse_uuid(project_id),
            sanitize_text(name),
            sanitize_text(description),
            status,
            priority,
            sanitize_text(next_prompt),
            json.dumps(metadata or {}),
            self.parse_uuid(created_by) if created_by else None,
        )

        if not row:
            raise RuntimeError("Failed to create feature")

        logger.info(f"Created feature: {row['id']} ({name})")
        return self._parse_json_fields(row)

    async def get_feature(self, feature_id: str) -> Optional[Dict[str, Any]]:
        """
        Get feature by ID.

        Args:
            feature_id: Feature UUID

        Returns:
            Feature record or None if not found
        """
        query = """
            SELECT id::text, company_id::text, project_id::text, name, description,
                   status, priority, next_prompt, metadata, chunk_count,
                   created_at, updated_at, created_by::text
            FROM features
            WHERE id = $1
        """
        row = await self.fetch_one(query, self.parse_uuid(feature_id))
        return self._parse_json_fields(row) if row else None

    async def list_features(
        self, project_id: str, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        List all features for a project.

        Args:
            project_id: Project UUID
            limit: Maximum number of records to return
            offset: Number of records to skip

        Returns:
            List of feature records
        """
        query = """
            SELECT id::text, company_id::text, project_id::text, name, description,
                   status, priority, next_prompt, metadata, chunk_count,
                   created_at, updated_at, created_by::text
            FROM features
            WHERE project_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
        """
        rows = await self.fetch_all(query, self.parse_uuid(project_id), limit, offset)
        return [self._parse_json_fields(row) for row in rows]

    async def update_feature(
        self,
        feature_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        next_prompt: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update feature fields.

        Args:
            feature_id: Feature UUID
            name: New name (optional)
            description: New description (optional)
            status: New status (optional)
            priority: New priority (optional)
            next_prompt: New next_prompt (optional)
            metadata: New metadata (optional)

        Returns:
            Updated feature record or None if not found
        """
        # Build dynamic UPDATE query
        updates = []
        params = []
        param_count = 1

        if name is not None:
            updates.append(f"name = ${param_count}")
            params.append(sanitize_text(name))
            param_count += 1

        if description is not None:
            updates.append(f"description = ${param_count}")
            params.append(sanitize_text(description))
            param_count += 1

        if status is not None:
            updates.append(f"status = ${param_count}")
            params.append(status)
            param_count += 1

        if priority is not None:
            updates.append(f"priority = ${param_count}")
            params.append(priority)
            param_count += 1

        if next_prompt is not None:
            updates.append(f"next_prompt = ${param_count}")
            params.append(sanitize_text(next_prompt))
            param_count += 1

        if metadata is not None:
            updates.append(f"metadata = ${param_count}")
            params.append(json.dumps(metadata))
            param_count += 1

        if not updates:
            # No fields to update
            return await self.get_feature(feature_id)

        updates.append("updated_at = NOW()")
        params.append(self.parse_uuid(feature_id))

        query = f"""
            UPDATE features
            SET {", ".join(updates)}
            WHERE id = ${param_count}
            RETURNING id::text, company_id::text, project_id::text, name, description,
                      status, priority, next_prompt, metadata, chunk_count,
                      created_at, updated_at, created_by::text
        """

        row = await self.fetch_one(query, *params)
        return self._parse_json_fields(row) if row else None

    async def delete_feature(self, feature_id: str) -> bool:
        """
        Delete feature (cascades to chunks).

        Args:
            feature_id: Feature UUID

        Returns:
            True if feature was deleted, False otherwise
        """
        query = "DELETE FROM features WHERE id = $1"
        result = await self.execute(query, self.parse_uuid(feature_id))
        deleted = result.endswith("1")

        if deleted:
            logger.info(f"Deleted feature: {feature_id}")

        return deleted

    async def update_chunk_count(self, feature_id: str, chunk_count: int) -> None:
        """
        Update chunk count for feature.

        Args:
            feature_id: Feature UUID
            chunk_count: New chunk count
        """
        query = """
            UPDATE features
            SET chunk_count = $1, updated_at = NOW()
            WHERE id = $2
        """
        await self.execute(query, chunk_count, self.parse_uuid(feature_id))

    # Feature chunk operations

    async def create_chunks(
        self, feature_id: str, chunks: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Batch create feature chunks.

        Args:
            feature_id: Feature UUID
            chunks: List of chunk dictionaries with required fields

        Returns:
            List of created chunk records
        """
        if not chunks:
            return []

        query = """
            INSERT INTO feature_chunks (
                id, feature_id, chunk_index, content, summary, chunk_type,
                key_concepts, dependencies, token_count, qdrant_point_id,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
            RETURNING id::text, feature_id::text, chunk_index, content, summary,
                      chunk_type, key_concepts, dependencies, token_count,
                      qdrant_point_id, created_at
        """

        results = []
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for chunk in chunks:
                    chunk_id = str(uuid4())
                    row = await conn.fetchrow(
                        query,
                        UUID(chunk_id),
                        self.parse_uuid(feature_id),
                        chunk["chunk_index"],
                        sanitize_text(chunk["content"]),  # sanitize_text: prevent UTF-8 corruption
                        sanitize_text(chunk.get("summary")),  # sanitize_text: prevent UTF-8 corruption
                        chunk.get("chunk_type"),
                        json.dumps(chunk.get("key_concepts", [])),
                        json.dumps(chunk.get("dependencies", [])),
                        chunk.get("token_count"),
                        chunk.get("qdrant_point_id"),
                    )
                    results.append(self._parse_json_fields(dict(row)))

        logger.info(f"Created {len(results)} chunks for feature {feature_id}")
        return results

    async def get_chunks(self, feature_id: str) -> List[Dict[str, Any]]:
        """
        Get feature chunks.

        Args:
            feature_id: Feature UUID

        Returns:
            List of chunk records
        """
        query = """
            SELECT id::text, feature_id::text, chunk_index, content, summary,
                   chunk_type, key_concepts, dependencies, token_count,
                   qdrant_point_id, created_at
            FROM feature_chunks
            WHERE feature_id = $1
            ORDER BY chunk_index
        """
        rows = await self.fetch_all(query, self.parse_uuid(feature_id))
        return [self._parse_json_fields(row) for row in rows]

    async def update_chunk_qdrant_id(self, chunk_id: str, qdrant_point_id: str) -> bool:
        """
        Update Qdrant point ID for chunk.

        Args:
            chunk_id: Chunk UUID
            qdrant_point_id: Qdrant point ID

        Returns:
            True if updated, False otherwise
        """
        query = """
            UPDATE feature_chunks
            SET qdrant_point_id = $1
            WHERE id = $2
        """
        result = await self.execute(query, qdrant_point_id, self.parse_uuid(chunk_id))
        return result.endswith("1")

    async def get_company_id(self, feature_id: str) -> Optional[str]:
        """
        Get company_id for a feature.

        Args:
            feature_id: Feature UUID

        Returns:
            Company UUID or None if feature not found
        """
        query = "SELECT company_id::text FROM features WHERE id = $1"
        return await self.fetch_val(query, self.parse_uuid(feature_id))

    def _parse_json_fields(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse JSON string fields into Python objects.

        Args:
            row: Database row dictionary

        Returns:
            Row with parsed JSON fields
        """
        if not row:
            return row

        # Parse metadata field
        if "metadata" in row and isinstance(row["metadata"], str):
            try:
                row["metadata"] = json.loads(row["metadata"])
            except (json.JSONDecodeError, TypeError):
                row["metadata"] = {}

        # Parse key_concepts field
        if "key_concepts" in row and isinstance(row["key_concepts"], str):
            try:
                row["key_concepts"] = json.loads(row["key_concepts"])
            except (json.JSONDecodeError, TypeError):
                row["key_concepts"] = []

        # Parse dependencies field
        if "dependencies" in row and isinstance(row["dependencies"], str):
            try:
                row["dependencies"] = json.loads(row["dependencies"])
            except (json.JSONDecodeError, TypeError):
                row["dependencies"] = []

        return row

"""CogneeMetadataWriter — writes metadata to Cognee's Postgres tables.

This writer makes enriched chunks searchable via Cognee's standard search API
by populating the `datasets`, `data`, and `dataset_data` tables that Cognee's
search layer queries.

Writes occur AFTER successful processing of chunks through the extraction pipeline.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Dict, List, Optional
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL

import asyncpg
import structlog

if TYPE_CHECKING:
    from .models import EnrichedChunkMessage

logger = structlog.get_logger(__name__)

# Cognee default user email (used to resolve principal_id for ACLs)
_DEFAULT_USER_EMAIL = "default_user@example.com"


class CogneeMetadataWriter:
    """Writes Cognee-compatible metadata to Postgres for searchability."""

    def __init__(self, pool: asyncpg.Pool):
        """Initialize with asyncpg connection pool.

        Args:
            pool: asyncpg connection pool to the `cognee` database
        """
        self._pool = pool
        self._default_principal_id: Optional[UUID] = None
        self._permission_ids: Optional[Dict[str, UUID]] = None

    @classmethod
    async def create(cls, dsn: str) -> "CogneeMetadataWriter":
        """Factory: create writer with connection pool.

        Args:
            dsn: Postgres DSN for the `cognee` database (NOT kgrag_auth)

        Returns:
            Initialized CogneeMetadataWriter instance
        """
        pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=1,
            max_size=3,
        )
        writer = cls(pool)
        logger.info("metadata_writer.created", dsn=dsn.split("@")[-1])  # Log without creds
        return writer

    async def close(self) -> None:
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("metadata_writer.closed")

    async def _ensure_auth_context(self, conn: asyncpg.Connection) -> None:
        """Load and cache the default principal ID and permission IDs.

        Cognee's authorization model requires:
        - A principal (from the ``principals`` table, matching the default user)
        - Four permission records (read, write, delete, share)

        These are loaded once and cached for the lifetime of this writer.
        """
        if self._default_principal_id is not None and self._permission_ids is not None:
            return

        # Resolve principal_id via the users table → principals share the same UUID
        user_id = await conn.fetchval(
            "SELECT id FROM users WHERE email = $1 LIMIT 1",
            _DEFAULT_USER_EMAIL,
        )
        if user_id is None:
            logger.warning(
                "metadata_writer.default_user_not_found",
                email=_DEFAULT_USER_EMAIL,
            )
            return

        self._default_principal_id = user_id

        # Load all permission UUIDs keyed by name
        rows = await conn.fetch("SELECT id, name FROM permissions")
        self._permission_ids = {row["name"]: row["id"] for row in rows}

        logger.info(
            "metadata_writer.auth_context_loaded",
            principal_id=str(self._default_principal_id),
            permissions=list(self._permission_ids.keys()),
        )

    async def _ensure_acls(
        self,
        conn: asyncpg.Connection,
        dataset_id: UUID,
    ) -> None:
        """Create ACL records (read/write/delete/share) for a dataset.

        Mirrors Cognee's ``resolve_authorized_user_dataset()`` behaviour which
        creates 4 ACL rows per dataset so that ``authorized_search`` can find
        the dataset via ``get_authorized_existing_datasets()``.

        Idempotent — skips insertion if an ACL already exists for the same
        (principal, permission, dataset) triple.
        """
        if self._default_principal_id is None or not self._permission_ids:
            return

        for perm_name, perm_id in self._permission_ids.items():
            await conn.execute(
                """
                INSERT INTO acls (id, created_at, principal_id, permission_id, dataset_id)
                SELECT $1, NOW(), $2, $3, $4
                WHERE NOT EXISTS (
                    SELECT 1 FROM acls
                    WHERE principal_id = $2
                      AND permission_id = $3
                      AND dataset_id = $4
                )
                """,
                uuid4(),
                self._default_principal_id,
                perm_id,
                dataset_id,
            )

        logger.debug(
            "metadata_writer.acls_ensured",
            dataset_id=str(dataset_id),
        )

    async def record_batch(
        self,
        messages: List[EnrichedChunkMessage],
        dataset_name: str,
    ) -> None:
        """Record metadata for a batch of enriched chunks.

        Groups chunks by file_path and writes:
        1. Dataset record (one per dataset_name)
        2. Data records (one per unique file)
        3. dataset_data links

        All operations are idempotent (upserts).

        Args:
            messages: Batch of enriched chunk messages
            dataset_name: Dataset name (e.g., "{project_id}_{branch}_code")
        """
        if not messages:
            return

        try:
            async with self._pool.acquire() as conn:
                # Load default user principal + permission IDs (cached after first call)
                await self._ensure_auth_context(conn)

                # Step 1: Find existing dataset by name or create new one
                # Cognee uses random UUID4 for datasets, so we must look up by name
                # to avoid duplicate records with different IDs.
                existing_id = await conn.fetchval(
                    "SELECT id FROM datasets WHERE name = $1 LIMIT 1",
                    dataset_name,
                )
                if existing_id:
                    dataset_id = existing_id
                    await conn.execute(
                        "UPDATE datasets SET updated_at = NOW(), owner_id = COALESCE(owner_id, $2) WHERE id = $1",
                        dataset_id,
                        self._default_principal_id,
                    )
                else:
                    dataset_id = uuid5(NAMESPACE_URL, dataset_name)
                    await conn.execute(
                        """
                        INSERT INTO datasets (id, name, owner_id, created_at, updated_at)
                        VALUES ($1, $2, $3, NOW(), NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            updated_at = NOW(),
                            owner_id = COALESCE(datasets.owner_id, EXCLUDED.owner_id)
                        """,
                        dataset_id,
                        dataset_name,
                        self._default_principal_id,
                    )

                # Ensure ACL records exist for this dataset
                await self._ensure_acls(conn, dataset_id)

                # Step 2: Group chunks by file and upsert data records
                files: Dict[str, List[EnrichedChunkMessage]] = {}
                for msg in messages:
                    if msg.file_path not in files:
                        files[msg.file_path] = []
                    files[msg.file_path].append(msg)

                data_ids = []
                for file_path, file_chunks in files.items():
                    # Use first chunk as representative for file-level metadata
                    msg = file_chunks[0]

                    # Reconstruct full file content from chunks (for content_hash)
                    # Sort by chunk_index to ensure consistent ordering
                    sorted_chunks = sorted(
                        file_chunks, key=lambda c: c.chunk_index if c.chunk_index is not None else 0
                    )
                    full_content = "\n".join(
                        chunk.content for chunk in sorted_chunks if chunk.content
                    )

                    # Compute content hash (MD5 hex, matching Cognee convention)
                    content_hash = hashlib.md5(full_content.encode()).hexdigest()

                    # Data ID: Use file_path directly as the name (required for Cognee delete)
                    data_id = uuid5(NAMESPACE_URL, f"text_{content_hash}")

                    # Build external_metadata JSON
                    external_metadata = json.dumps(
                        {
                            "repository": msg.repository,
                            "branch": msg.branch,
                            "language": msg.language or "unknown",
                            "file_path": file_path,
                            "company_id": msg.company_id,
                            "ingestion_id": msg.ingestion_id,
                            **(
                                {"project_id": msg.project_id}
                                if msg.content_type != "document"
                                else {}
                            ),
                        }
                    )

                    # Node set for graph organization
                    scope_key = msg.project_id or msg.company_id
                    node_set = json.dumps(
                        [
                            f"knowledge_{msg.company_id}"
                            if msg.content_type == "document"
                            else f"code_{scope_key}"
                        ]
                    )

                    # Token and size estimates
                    token_count = len(full_content)  # Rough approximation
                    data_size = len(full_content.encode("utf-8"))

                    # Raw data location (virtual file path)
                    raw_data_location = f"file:///data/cognee/data/text_{content_hash}.txt"

                    # Upsert data record
                    await conn.execute(
                        """
                        INSERT INTO data (
                            id, name, extension, mime_type,
                            raw_data_location, content_hash,
                            external_metadata, node_set,
                            token_count, data_size,
                            created_at, updated_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7::json, $8::json, $9, $10, NOW(), NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            updated_at = NOW(),
                            external_metadata = EXCLUDED.external_metadata,
                            node_set = EXCLUDED.node_set,
                            token_count = EXCLUDED.token_count,
                            data_size = EXCLUDED.data_size
                        """,
                        data_id,
                        file_path,  # Use file_path as name (required for Cognee delete)
                        "txt",
                        "text/plain",
                        raw_data_location,
                        content_hash,
                        external_metadata,
                        node_set,
                        token_count,
                        data_size,
                    )

                    data_ids.append(data_id)

                # Step 3: Link dataset to data records
                for data_id in data_ids:
                    await conn.execute(
                        """
                        INSERT INTO dataset_data (dataset_id, data_id, created_at)
                        VALUES ($1, $2, NOW())
                        ON CONFLICT (dataset_id, data_id) DO NOTHING
                        """,
                        dataset_id,
                        data_id,
                    )

                logger.info(
                    "metadata_writer.batch_recorded",
                    dataset_name=dataset_name,
                    files=len(files),
                    chunks=len(messages),
                )

        except Exception as e:
            # Log but don't raise — metadata write failure shouldn't fail the batch
            logger.error(
                "metadata_writer.batch_record_error",
                dataset_name=dataset_name,
                files=len(files) if "files" in locals() else 0,
                error=str(e),
                exc_info=True,
            )

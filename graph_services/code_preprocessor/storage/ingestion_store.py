"""Postgres-backed storage for ingestion tracking.

Uses asyncpg with the code_processing schema.
All methods are async.
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Dict, List, Optional

import asyncpg

logger = logging.getLogger(__name__)


class IngestionStatus(str, Enum):
    """Status states for ingestion tracking."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class IngestionStore:
    """Track ingestion progress in Postgres with atomic updates.

    The asyncpg pool is injected — this class never creates or closes it.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── health ────────────────────────────────────────────────────────

    async def health_check(self) -> bool:
        """Verify Postgres connectivity against code_processing schema."""
        try:
            await self._pool.fetchval("SELECT 1 FROM code_processing.ingestion_batches LIMIT 0")
            return True
        except Exception as exc:
            logger.error("Postgres health check failed: %s", exc)
            return False

    # ── create / lifecycle ────────────────────────────────────────────

    async def create_ingestion(
        self,
        *,
        ingestion_id: str,
        project_id: str,
        company_id: str,
        repository: str,
        branch: str,
        total_files: int,
        commit_sha: Optional[str] = None,
        framework: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Insert a new ingestion batch record.

        Uses ON CONFLICT to be idempotent — if the ingestion already exists
        we update total_files and return the existing row.
        """
        row = await self._pool.fetchrow(
            """
            INSERT INTO code_processing.ingestion_batches
                (ingestion_id, project_id, company_id, repository, branch,
                 total_files, commit_sha, framework, user_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (ingestion_id) DO UPDATE
               SET total_files = EXCLUDED.total_files,
                   updated_at  = now()
            RETURNING *
            """,
            ingestion_id,
            project_id,
            company_id,
            repository,
            branch,
            total_files,
            commit_sha,
            framework,
            user_id or "",
        )
        logger.info(
            "Created ingestion %s for %s@%s (%d files)",
            ingestion_id,
            repository,
            branch,
            total_files,
        )
        return self._row_to_dict(row)

    async def start_ingestion(self, ingestion_id: str) -> bool:
        """Transition status pending → running."""
        tag = await self._pool.execute(
            """
            UPDATE code_processing.ingestion_batches
               SET status     = 'running',
                   started_at = now(),
                   updated_at = now()
             WHERE ingestion_id = $1
               AND status = 'pending'
            """,
            ingestion_id,
        )
        return self._affected(tag) > 0

    async def mark_completed(self, ingestion_id: str) -> bool:
        """Transition status running → completed."""
        tag = await self._pool.execute(
            """
            UPDATE code_processing.ingestion_batches
               SET status       = 'completed',
                   completed_at = now(),
                   updated_at   = now(),
                   current_file = NULL
             WHERE ingestion_id = $1
               AND status = 'running'
            """,
            ingestion_id,
        )
        if self._affected(tag) > 0:
            logger.info("Ingestion %s completed", ingestion_id)
            return True
        return False

    async def mark_failed(self, ingestion_id: str, error: str) -> bool:
        """Mark ingestion as failed with an error message."""
        tag = await self._pool.execute(
            """
            UPDATE code_processing.ingestion_batches
               SET status       = 'failed',
                   error        = $2,
                   completed_at = now(),
                   updated_at   = now()
             WHERE ingestion_id = $1
            """,
            ingestion_id,
            error,
        )
        if self._affected(tag) > 0:
            logger.error("Ingestion %s failed: %s", ingestion_id, error)
            return True
        return False

    # ── progress tracking ─────────────────────────────────────────────

    async def update_progress(
        self,
        ingestion_id: str,
        *,
        files_processed_delta: int = 0,
        files_failed_delta: int = 0,
        files_skipped_delta: int = 0,
        files_llm_fallback_delta: int = 0,
        files_filtered_size_delta: int = 0,
        files_filtered_extension_delta: int = 0,
        files_filtered_directory_delta: int = 0,
        files_enqueued_delta: int = 0,
        files_failed_enqueue_delta: int = 0,
        kafka_bytes_delta: int = 0,
        current_file: Optional[str] = None,
        failed_file: Optional[Dict[str, Any]] = None,
        path_added: Optional[str] = None,
        path_updated: Optional[str] = None,
        path_renamed: Optional[Dict[str, str]] = None,
        path_deleted: Optional[str] = None,
        path_skipped: Optional[str] = None,
        path_failed: Optional[str] = None,
        path_llm_fallback: Optional[str] = None,
    ) -> bool:
        """Atomically increment counters and insert related event rows.

        Runs inside a single transaction to keep batch row and event
        rows consistent.
        """
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Atomic counter update on batch row
                    tag = await conn.execute(
                        """
                        UPDATE code_processing.ingestion_batches
                           SET files_processed          = files_processed          + $2,
                               files_failed             = files_failed             + $3,
                               files_skipped            = files_skipped            + $4,
                               files_llm_fallback       = files_llm_fallback       + $5,
                               files_filtered_size      = files_filtered_size      + $6,
                               files_filtered_extension = files_filtered_extension + $7,
                               files_filtered_directory = files_filtered_directory + $8,
                               files_enqueued_to_kafka  = files_enqueued_to_kafka  + $9,
                               files_failed_to_enqueue  = files_failed_to_enqueue  + $10,
                               kafka_enqueue_bytes_total = kafka_enqueue_bytes_total + $11,
                               current_file             = COALESCE($12, current_file),
                               updated_at               = now()
                         WHERE ingestion_id = $1
                        """,
                        ingestion_id,
                        files_processed_delta,
                        files_failed_delta,
                        files_skipped_delta,
                        files_llm_fallback_delta,
                        files_filtered_size_delta,
                        files_filtered_extension_delta,
                        files_filtered_directory_delta,
                        files_enqueued_delta,
                        files_failed_enqueue_delta,
                        kafka_bytes_delta,
                        current_file,
                    )

                    # 2. Insert file events (replaces MongoDB capped arrays)
                    events = self._collect_events(
                        ingestion_id,
                        path_added=path_added,
                        path_updated=path_updated,
                        path_renamed=path_renamed,
                        path_deleted=path_deleted,
                        path_skipped=path_skipped,
                        path_failed=path_failed,
                        path_llm_fallback=path_llm_fallback,
                    )
                    for evt in events:
                        await conn.execute(
                            """
                            INSERT INTO code_processing.ingestion_file_events
                                (ingestion_id, file_path, previous_path, event_type)
                            VALUES ($1, $2, $3, $4)
                            """,
                            evt["ingestion_id"],
                            evt["file_path"],
                            evt.get("previous_path"),
                            evt["event_type"],
                        )

                    # 3. Record failed file as pipeline error
                    if failed_file:
                        await conn.execute(
                            """
                            INSERT INTO code_processing.pipeline_errors
                                (ingestion_id, file_path, error_type, error_message, pipeline_stage)
                            VALUES ($1, $2, $3, $4, 'preprocessing')
                            """,
                            ingestion_id,
                            failed_file.get("file_path", ""),
                            failed_file.get("error_type", "ProcessingError"),
                            failed_file.get("error", str(failed_file)),
                        )

            return self._affected(tag) > 0
        except Exception as exc:
            logger.warning(
                "Failed to update progress for ingestion %s (continuing): %s",
                ingestion_id,
                exc,
            )
            return False

    # ── skip / error / kafka recording ────────────────────────────────

    async def record_skipped_file(
        self,
        ingestion_id: str,
        file_path: str,
        service: str,
        skip_type: str,
        reason: Optional[str] = None,
        *,
        company_id: Optional[str] = None,
        project_id: Optional[str] = None,
        repository: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> None:
        """Insert a row into skipped_files."""
        await self._pool.execute(
            """
            INSERT INTO code_processing.skipped_files
                (ingestion_id, company_id, project_id, repository, branch,
                 file_path, service, skip_type, reason)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            ingestion_id,
            company_id,
            project_id,
            repository,
            branch,
            file_path,
            service,
            skip_type,
            reason,
        )

    async def record_error(
        self,
        *,
        ingestion_id: Optional[str] = None,
        repository: Optional[str] = None,
        branch: Optional[str] = None,
        file_path: Optional[str] = None,
        company_id: Optional[str] = None,
        project_id: Optional[str] = None,
        error_type: str,
        error_message: str,
        error_severity: str = "error",
        pipeline_stage: str = "preprocessing",
        stack_trace: Optional[str] = None,
    ) -> None:
        """Insert a row into pipeline_errors."""
        await self._pool.execute(
            """
            INSERT INTO code_processing.pipeline_errors
                (ingestion_id, repository, branch, file_path, company_id,
                 project_id, error_type, error_message, error_severity,
                 pipeline_stage, stack_trace)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """,
            ingestion_id,
            repository,
            branch,
            file_path,
            company_id,
            project_id,
            error_type,
            error_message,
            error_severity,
            pipeline_stage,
            stack_trace,
        )

    async def record_kafka_enqueue(
        self,
        ingestion_id: str,
        file_path: str,
        message_size: int,
        topic: str,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> None:
        """Record a successful Kafka enqueue as a file event."""
        await self._pool.execute(
            """
            INSERT INTO code_processing.ingestion_file_events
                (ingestion_id, file_path, event_type,
                 kafka_message_size, kafka_topic, kafka_partition, kafka_offset)
            VALUES ($1, $2, 'enqueued', $3, $4, $5, $6)
            """,
            ingestion_id,
            file_path,
            message_size,
            topic,
            partition,
            offset,
        )

    # ── queries ───────────────────────────────────────────────────────

    async def get_ingestion(self, ingestion_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single ingestion batch by ingestion_id."""
        row = await self._pool.fetchrow(
            "SELECT * FROM code_processing.ingestion_batches WHERE ingestion_id = $1",
            ingestion_id,
        )
        if row is None:
            return None
        return self._row_to_dict(row)

    async def list_ingestions(
        self,
        *,
        project_id: Optional[str] = None,
        company_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List ingestion batches with optional filters."""
        conditions: List[str] = []
        params: List[Any] = []
        idx = 1

        if project_id:
            conditions.append(f"project_id = ${idx}")
            params.append(project_id)
            idx += 1
        if company_id:
            conditions.append(f"company_id = ${idx}")
            params.append(company_id)
            idx += 1
        if status:
            conditions.append(f"status = ${idx}")
            params.append(status)
            idx += 1

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        params.extend([limit, offset])

        rows = await self._pool.fetch(
            f"SELECT * FROM code_processing.ingestion_batches{where}"
            f" ORDER BY created_at DESC LIMIT ${idx} OFFSET ${idx + 1}",
            *params,
        )
        return [self._row_to_dict(r) for r in rows]

    async def count_ingestions(
        self,
        *,
        project_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> int:
        """Count ingestion batches matching the filters."""
        conditions: List[str] = []
        params: List[Any] = []
        idx = 1

        if project_id:
            conditions.append(f"project_id = ${idx}")
            params.append(project_id)
            idx += 1
        if status:
            conditions.append(f"status = ${idx}")
            params.append(status)
            idx += 1

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        return await self._pool.fetchval(
            f"SELECT COUNT(*) FROM code_processing.ingestion_batches{where}",
            *params,
        )

    # ── teardown ──────────────────────────────────────────────────────

    async def close(self) -> None:
        """No-op — the pool is managed externally."""

    # ── private helpers ───────────────────────────────────────────────

    @staticmethod
    def _row_to_dict(row: asyncpg.Record) -> Dict[str, Any]:
        """Convert an asyncpg Record to a plain dict with ISO timestamps."""
        result: Dict[str, Any] = dict(row)
        # Drop the surrogate PK — callers use ingestion_id
        result.pop("id", None)
        for field in ("created_at", "updated_at", "started_at", "completed_at"):
            val = result.get(field)
            if val is not None:
                result[field] = val.isoformat()
        return result

    @staticmethod
    def _affected(command_tag: str) -> int:
        """Extract row count from an asyncpg command tag like 'UPDATE 1'."""
        parts = command_tag.split()
        if len(parts) >= 2 and parts[-1].isdigit():
            return int(parts[-1])
        return 0

    @staticmethod
    def _collect_events(
        ingestion_id: str,
        *,
        path_added: Optional[str] = None,
        path_updated: Optional[str] = None,
        path_renamed: Optional[Dict[str, str]] = None,
        path_deleted: Optional[str] = None,
        path_skipped: Optional[str] = None,
        path_failed: Optional[str] = None,
        path_llm_fallback: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Build a list of event dicts to insert into ingestion_file_events."""
        events: List[Dict[str, Any]] = []
        simple_mappings = [
            (path_added, "added"),
            (path_updated, "modified"),
            (path_deleted, "deleted"),
            (path_skipped, "added"),  # skipped files still tracked as 'added' event
            (path_failed, "added"),
            (path_llm_fallback, "llm_fallback"),
        ]
        for path, event_type in simple_mappings:
            if path:
                events.append(
                    {
                        "ingestion_id": ingestion_id,
                        "file_path": path,
                        "event_type": event_type,
                    }
                )
        if path_renamed:
            events.append(
                {
                    "ingestion_id": ingestion_id,
                    "file_path": path_renamed.get("to", ""),
                    "previous_path": path_renamed.get("from"),
                    "event_type": "renamed",
                }
            )
        return events

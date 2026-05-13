"""Kafka consumer that synchronises repositories referenced by ingestion events."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
import hashlib
import base64
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import asyncpg

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .config import KafkaProcessingSettings, get_settings
from .file_filter import FileFilter, FilterReason, FilterStats, IngestionRules
from .git_repository_manager import GitChange, GitDiffResult, GitRepositoryManager
from ..storage.repository_version_store import RepositoryVersionStore
from ..storage.ingestion_store import IngestionStore
from ..enrichment import enrich_and_store_file, store_project_tree

# Import schema from shared module
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))
from shared.id_generator import IDGenerator
from shared.canonical_path import CanonicalPath

logger = logging.getLogger(__name__)


class RepositoryIngestionConsumer:
    """Consume repository ingestion messages and sync Git state."""

    # Git status code to change type mapping
    GIT_STATUS_MAP = {
        "A": "added",
        "M": "modified",
        "D": "deleted",
        "R": "renamed",
        "C": "copied",
        "T": "modified",
        "U": "modified",
        "added": "added",
        "modified": "modified",
        "deleted": "deleted",
        "renamed": "renamed",
        "copied": "copied",
        None: "unchanged",
    }

    def __init__(
        self,
        settings: Optional[KafkaProcessingSettings] = None,
        db_pool: Optional[asyncpg.Pool] = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._db_pool = db_pool
        self._git_manager = GitRepositoryManager(self._settings, db_pool)
        self._version_store: Optional[RepositoryVersionStore] = None
        self._ingestion_store: Optional[IngestionStore] = None

    async def __aenter__(self) -> "RepositoryIngestionConsumer":
        await self.start()
        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._consumer is not None:
            return

        logger.info(
            "Starting Kafka consumer for topic %s (bootstrap=%s, group_id=%s)",
            self._settings.kafka_topic,
            self._settings.kafka_bootstrap_servers,
            self._settings.kafka_group_id,
        )

        # Initialise asyncpg-backed stores (requires running event loop)
        self._version_store = await self._initialise_version_store()
        self._ingestion_store = await self._initialise_ingestion_store()

        self._consumer = AIOKafkaConsumer(
            self._settings.kafka_topic,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._settings.kafka_group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=self._deserialize_message,
        )

        await self._consumer.start()

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            client_id=self._settings.kafka_producer_client_id,
            max_request_size=self._settings.kafka_max_request_size,
            value_serializer=lambda value: value,
        )
        await self._producer.start()

    async def stop(self) -> None:
        """Stop consumer and cleanup all resources."""
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

        # Stores are no-ops for close (pool is managed externally)
        if self._version_store:
            await self._version_store.close()
        if self._ingestion_store:
            await self._ingestion_store.close()

    async def run(self) -> None:
        if self._consumer is None:
            raise RuntimeError("Consumer must be started before run() is called")

        async for message in self._consumer:
            payload: Dict[str, Any] = message.value
            repository = payload.get("repository")
            branch = payload.get("branch")
            force_full_refresh = bool(payload.get("force_full_refresh", False))
            framework = payload.get("framework")

            # Extract multi-tenant IDs (REQUIRED)
            project_id = payload.get("project_id", "")
            company_id = payload.get("company_id", "")
            user_id = payload.get("user_id", "")

            if not repository:
                logger.warning("Received invalid payload without repository field: %s", payload)
                continue

            if not project_id or not company_id:
                logger.warning(
                    "Received invalid payload without required tenant IDs: project_id=%s company_id=%s",
                    project_id,
                    company_id,
                )
                continue

            logger.info(
                "Processing repository request: framework=%s repo=%s branch=%s "
                "project=%s company=%s force_full_refresh=%s",
                framework,
                repository,
                branch or "<default>",
                project_id,
                company_id,
                force_full_refresh,
            )

            # Fetch GitHub token from database before calling sync git operations
            github_token = None
            if project_id and self._db_pool:
                github_token = await self._git_manager._get_project_github_token(project_id)
                if github_token:
                    logger.debug("Retrieved GitHub token for project %s", project_id)

            try:
                diff_result = await asyncio.to_thread(
                    self._git_manager.sync_repository,
                    repository,
                    branch,
                    github_token=github_token,
                    force_full_refresh=force_full_refresh,
                )
            except Exception as exc:
                logger.exception(
                    "Failed to process repository %s (branch=%s, force_refresh=%s): %s",
                    repository,
                    branch or "<default>",
                    force_full_refresh,
                    exc,
                )
                continue

            self._log_diff_result(diff_result)

            # Build compact file tree for project classification
            from code_preprocessor.file_tree import build_tree

            file_tree = build_tree(str(diff_result.repo_path))
            logger.info(
                "Built file tree for %s (%d lines)",
                repository,
                file_tree.count("\n") + 1,
            )

            # Generate ingestion_id for end-to-end traceability
            ingestion_id = str(uuid.uuid4())
            await self._persist_changes(
                diff_result,
                framework,
                project_id,
                company_id,
                user_id,
                ingestion_id,
                file_tree=file_tree,
            )

    @staticmethod
    def _deserialize_message(value: bytes) -> Dict[str, Any]:
        try:
            return json.loads(value.decode("utf-8"))
        except json.JSONDecodeError:
            logger.error("Failed to decode Kafka message: %r", value)
            return {}

    def _log_diff_result(self, diff: GitDiffResult) -> None:
        if diff.is_initial_clone:
            logger.info(
                "Initial clone completed for %s@%s (commit=%s, files=%d)",
                diff.repository,
                diff.branch,
                diff.new_commit,
                len(diff.changes),
            )
            return

        if diff.force_full_refresh:
            logger.info(
                "Full refresh completed for %s@%s (commit=%s, files=%d)",
                diff.repository,
                diff.branch,
                diff.new_commit,
                len(diff.changes),
            )
            return

        if not diff.old_commit or diff.old_commit == diff.new_commit:
            logger.info(
                "No new commits for %s@%s (commit=%s)",
                diff.repository,
                diff.branch,
                diff.new_commit,
            )
            return

        logger.info(
            "Updated %s@%s: %s -> %s (%d changes)",
            diff.repository,
            diff.branch,
            diff.old_commit,
            diff.new_commit,
            len(diff.changes),
        )
        for change in diff.changes:
            if change.previous_path:
                logger.debug(
                    "    %s %s -> %s", change.change_type, change.previous_path, change.file_path
                )
            else:
                logger.debug("    %s %s", change.change_type, change.file_path)

    async def _initialise_version_store(self) -> Optional[RepositoryVersionStore]:
        """Initialise the asyncpg-backed repository version store."""
        if not self._db_pool:
            logger.warning("Repository version store disabled: no database pool")
            return None
        try:
            store = RepositoryVersionStore(self._db_pool)
            return store
        except Exception as exc:
            logger.error("Failed to initialise repository version store: %s", exc)
            return None

    async def _initialise_ingestion_store(self) -> Optional[IngestionStore]:
        """Initialise the asyncpg-backed ingestion tracking store."""
        if not self._db_pool:
            logger.warning("Ingestion store disabled: no database pool")
            return None
        try:
            store = IngestionStore(self._db_pool)
            if not await store.health_check():
                logger.warning("Ingestion store health check failed - tracking disabled")
                return None
            return store
        except Exception as exc:
            logger.error("Failed to initialise ingestion store: %s", exc)
            return None

    async def _persist_changes(
        self,
        diff: GitDiffResult,
        framework: str,
        project_id: str,
        company_id: str,
        user_id: str,
        ingestion_id: str,
        file_tree: str = "",
    ) -> None:
        """Delegate to IngestionProcessor for streaming two-stage pipeline.

        Files are chunked and embedded concurrently via embed workers,
        then published to Kafka as soon as each micro-batch is ready.
        """
        from .ingestion_processor import IngestionProcessor

        processor = IngestionProcessor(
            settings=self._settings,
            db_pool=self._db_pool,
            version_store=self._version_store,
            ingestion_store=self._ingestion_store,
            pipeline_store=None,
            producer=self._producer,
            event_emitter=self._event_emitter if hasattr(self, "_event_emitter") else None,
        )
        await processor.process_ingestion(
            diff=diff,
            framework=framework,
            project_id=project_id,
            company_id=company_id,
            user_id=user_id,
            ingestion_id=ingestion_id,
            file_tree=file_tree,
        )
        return

        repository = diff.repository
        branch = diff.branch

        # Initialize file filter with default rules
        file_filter = FileFilter(IngestionRules.defaults())
        filter_stats = FilterStats()

        # Create ingestion tracking record BEFORE filtering or processing
        # so FK constraints on ingestion_file_events are satisfied.
        if self._ingestion_store:
            try:
                await self._ingestion_store.create_ingestion(
                    ingestion_id=ingestion_id,
                    project_id=project_id,
                    company_id=company_id,
                    repository=repository,
                    branch=branch,
                    total_files=len(diff.changes),
                    commit_sha=diff.new_commit,
                    framework=framework,
                    user_id=user_id,
                )
                await self._ingestion_store.start_ingestion(ingestion_id)
            except Exception as exc:
                logger.warning(
                    "Failed to create ingestion tracking for %s (continuing): %s", ingestion_id, exc
                )

        # Create cogni_ingestion_stats row at ingestion start (all zeros)
        if self._db_pool:
            try:
                await self._db_pool.execute(
                    """
                    INSERT INTO code_processing.cogni_ingestion_stats
                        (ingestion_id, company_id, project_id,
                         files_produced, files_skipped, files_consumed,
                         files_processed, files_failed, first_consumed_at)
                    VALUES ($1, $2, $3, 0, 0, 0, 0, 0, NULL)
                    ON CONFLICT (ingestion_id) DO NOTHING
                    """,
                    ingestion_id,
                    company_id,
                    project_id,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to create cogni_ingestion_stats for %s: %s", ingestion_id, exc
                )

        # Filter files BEFORE processing
        changes_to_process: list[GitChange] = []
        for change in diff.changes:
            # Skip deleted files from filtering (they have no content to check)
            if change.change_type.upper().startswith("D"):
                changes_to_process.append(change)
                continue

            # Get file size without reading content (memory efficient)
            file_size = file_filter.get_file_size(diff.repo_path, change.file_path)
            filter_result = file_filter.check(change.file_path, file_size)

            if filter_result.filtered:
                filter_stats.increment(filter_result.reason)
                logger.debug(
                    "Filtered %s: %s (%s)",
                    change.file_path,
                    filter_result.reason.value,
                    filter_result.detail,
                )

                # Record skipped file in tracking table
                if self._db_pool:
                    skip_type_map = {
                        FilterReason.SIZE: "filtered_size",
                        FilterReason.EXTENSION: "filtered_extension",
                        FilterReason.DIRECTORY: "filtered_directory",
                    }
                    try:
                        await self._db_pool.execute(
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
                            change.file_path,
                            "preprocessor",
                            skip_type_map.get(filter_result.reason, "unknown"),
                            filter_result.detail,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to record skipped file %s: %s", change.file_path, exc
                        )

                if self._ingestion_store:
                    await self._ingestion_store.update_progress(
                        ingestion_id,
                        files_filtered_size_delta=1
                        if filter_result.reason == FilterReason.SIZE
                        else 0,
                        files_filtered_extension_delta=1
                        if filter_result.reason == FilterReason.EXTENSION
                        else 0,
                        files_filtered_directory_delta=1
                        if filter_result.reason == FilterReason.DIRECTORY
                        else 0,
                        path_skipped=change.file_path,
                    )
            else:
                changes_to_process.append(change)

        total_files = len(changes_to_process)
        total_filtered = filter_stats.total_filtered

        logger.info(
            "Starting ingestion %s for %s@%s (%d files, %d filtered: %d size, %d ext, %d dir)",
            ingestion_id,
            repository,
            branch,
            total_files,
            total_filtered,
            filter_stats.files_filtered_size,
            filter_stats.files_filtered_extension,
            filter_stats.files_filtered_directory,
        )

        # ── Clean up old ingestion metadata (not chunks!) ──
        # Chunks are NEVER bulk-deleted. They persist across ingestions and are
        # replaced per-file inside enrich_and_store_file when content changes.
        # publish_enriched_chunks queries by repo+branch, not ingestion_id,
        # so old chunks are always findable.
        if self._db_pool:
            try:
                pass  # Chunks are preserved — no bulk delete or update needed

                deleted_batches = await self._db_pool.execute(
                    "DELETE FROM code_processing.ingestion_batches "
                    "WHERE repository = $1 AND branch = $2 AND ingestion_id != $3",
                    repository,
                    branch,
                    ingestion_id,
                )
                old_batch_count = int(deleted_batches.split()[-1]) if deleted_batches else 0
                if old_batch_count > 0:
                    logger.info(
                        "Cleaned up %d old ingestion_batches for %s@%s",
                        old_batch_count,
                        repository,
                        branch,
                    )
            except Exception as exc:
                logger.warning(
                    "Failed to clean up old chunks/batches for %s@%s: %s",
                    repository,
                    branch,
                    exc,
                )

        # Update total_files now that filtering is done
        if self._ingestion_store and total_filtered > 0:
            try:
                await self._ingestion_store.update_progress(
                    ingestion_id,
                    files_skipped_delta=total_filtered,
                )
            except Exception:
                pass  # non-critical

        # Early exit if all files were filtered
        if not changes_to_process:
            logger.info("All files filtered for ingestion %s - marking completed", ingestion_id)
            if self._ingestion_store:
                await self._ingestion_store.mark_completed(ingestion_id)
            return

        # Bounded concurrency via semaphore
        semaphore = asyncio.Semaphore(self._settings.max_concurrent_files)
        files_processed = 0
        files_failed = 0
        progress_interval = self._settings.progress_update_interval

        async def process_with_backpressure(change: GitChange, file_index: int) -> bool:
            """Process a single file with semaphore-bounded concurrency."""
            async with semaphore:
                try:
                    await self._handle_change(
                        diff,
                        change,
                        framework,
                        project_id,
                        company_id,
                        user_id,
                        ingestion_id=ingestion_id,
                        file_index=file_index,
                        total_files=total_files,
                    )
                    return True
                except Exception as exc:
                    logger.error(
                        "Failed to process file %s in ingestion %s: %s",
                        change.file_path,
                        ingestion_id,
                        exc,
                    )
                    if self._ingestion_store:
                        await self._ingestion_store.update_progress(
                            ingestion_id,
                            files_failed_delta=1,
                            failed_file={
                                "file_path": change.file_path,
                                "error": str(exc),
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                    return False

        # Stream processing with bounded parallelism
        pending: set = set()

        for idx, change in enumerate(changes_to_process, start=1):
            task = asyncio.create_task(process_with_backpressure(change, idx))
            pending.add(task)

            # Update current file being processed
            if self._ingestion_store and idx % progress_interval == 0:
                await self._ingestion_store.update_progress(
                    ingestion_id,
                    current_file=change.file_path,
                )

            # Harvest completed tasks to free memory
            if len(pending) >= self._settings.max_concurrent_files * 2:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task.result():
                        files_processed += 1
                    else:
                        files_failed += 1

                # Batch progress update
                if self._ingestion_store and files_processed > 0:
                    await self._ingestion_store.update_progress(
                        ingestion_id,
                        files_processed_delta=files_processed,
                    )
                    files_processed = 0

        # Wait for remaining tasks
        if pending:
            done, _ = await asyncio.wait(pending)
            for task in done:
                if task.result():
                    files_processed += 1
                else:
                    files_failed += 1

        # Final progress update
        if self._ingestion_store:
            if files_processed > 0:
                await self._ingestion_store.update_progress(
                    ingestion_id,
                    files_processed_delta=files_processed,
                )

        # Store project tree
        if self._db_pool and file_tree:
            try:
                await store_project_tree(self._db_pool, ingestion_id, file_tree)
                logger.info("Stored project tree for ingestion %s", ingestion_id)
            except Exception as exc:
                logger.warning("Failed to store project tree for %s: %s", ingestion_id, exc)

        # Project-level analysis from folder structure (one LLM call)
        if self._db_pool:
            try:
                from ..project_classifier import analyze_project

                analysis = await analyze_project(self._db_pool, repository, branch)
                if analysis:
                    logger.info("Project analysis stored for ingestion %s", ingestion_id)
            except Exception as exc:
                logger.warning("Project analysis failed for %s: %s", ingestion_id, exc)

        # Enrich chunks with project context + file skeleton, then embed
        if self._db_pool:
            try:
                from ..enrichment import enrich_chunks, batch_embed_chunks

                enriched = await enrich_chunks(self._db_pool, ingestion_id)
                if enriched > 0:
                    logger.info("Enriched %d chunks for ingestion %s", enriched, ingestion_id)

                embedded = await batch_embed_chunks(self._db_pool, ingestion_id)
                if embedded > 0:
                    logger.info("Embedded %d chunks for ingestion %s", embedded, ingestion_id)
            except Exception as exc:
                logger.warning("Enrichment/embedding failed for %s: %s", ingestion_id, exc)

        # Publish enriched chunks to Kafka for Cognee consumer (by repo/branch,
        # so both new and reused chunks get sent)
        published = 0
        if self._db_pool and self._producer:
            try:
                from ..enrichment import publish_enriched_chunks
                import os as _os

                output_topic = _os.environ.get("ENRICHED_CHUNKS_TOPIC", "enriched-code-chunks")
                published = await publish_enriched_chunks(
                    self._db_pool,
                    self._producer,
                    repository,
                    branch,
                    company_id=company_id,
                    project_id=project_id,
                    topic=output_topic,
                    ingestion_id=ingestion_id,
                )
                if published > 0:
                    logger.info(
                        "Published %d enriched chunks to %s for %s@%s",
                        published,
                        output_topic,
                        repository,
                        branch,
                    )
            except Exception as exc:
                logger.warning(
                    "Failed to publish enriched chunks for %s@%s: %s", repository, branch, exc
                )
                published = 0

        # Note: files_produced is set correctly by enrichment.py (COUNT DISTINCT file_path).
        # `published` here is the number of Kafka CHUNKS, not files — do not overwrite.

        if self._ingestion_store:
            await self._ingestion_store.mark_completed(ingestion_id)

        logger.info(
            "Completed ingestion %s for %s@%s (%d processed, %d failed)",
            ingestion_id,
            repository,
            branch,
            total_files - files_failed,
            files_failed,
        )

    async def _handle_change(
        self,
        diff: GitDiffResult,
        change: GitChange,
        framework: str,
        project_id: str,
        company_id: str,
        user_id: str,
        *,
        ingestion_id: str = "",
        file_index: int = 0,
        total_files: int = 0,
    ) -> None:
        commit_sha = diff.new_commit
        repository = diff.repository
        branch = diff.branch

        if change.change_type.upper().startswith("R") and change.previous_path:
            deleted_doc = await self._version_store.record_change(
                repository=repository,
                branch=branch,
                framework=framework,
                file_path=change.previous_path,
                change_type="D",
                commit_sha=commit_sha,
                content_bytes=None,
                previous_path=None,
                parser_data=None,
                force_full_refresh=diff.force_full_refresh,
            )
            deleted_doc["force_full_refresh"] = diff.force_full_refresh
            deleted_doc["project_id"] = project_id
            deleted_doc["company_id"] = company_id
            deleted_doc["user_id"] = user_id
            deleted_doc["ingestion_id"] = ingestion_id
            deleted_doc["file_index"] = file_index
            deleted_doc["total_files"] = total_files
            # Phase 4: data_enrichment emit removed (no consumer exists for this topic)

            # Also emit delete to enriched-code-chunks topic for previous path
            await self._emit_enriched_chunk_delete(
                repository,
                branch,
                change.previous_path,
                ingestion_id,
                project_id,
                company_id,
            )

        content_bytes: Optional[bytes] = None
        if not change.change_type.upper().startswith("D"):
            try:
                content_bytes = self._read_file_bytes(diff.repo_path, change.file_path)
            except FileNotFoundError:
                logger.warning(
                    "File %s not found in %s when recording change %s",
                    change.file_path,
                    diff.repo_path,
                    change.change_type,
                )
            except OSError as exc:
                logger.warning(
                    "Failed to read %s for versioning (%s): %s",
                    change.file_path,
                    change.change_type,
                    exc,
                )

        # ── Skip unchanged files (content hash deduplication) ─────────
        # Chunks are adopted (not deleted) at ingestion start, so unchanged files
        # can safely skip enrichment — their chunks already exist under the new
        # ingestion_id and will be published at the end.
        if content_bytes and self._db_pool:
            new_hash = hashlib.sha256(content_bytes).hexdigest()
            existing_hash = await self._db_pool.fetchval(
                "SELECT content_hash FROM code_processing.repository_file_versions"
                " WHERE repository = $1 AND branch = $2 AND file_path = $3"
                " ORDER BY version DESC LIMIT 1",
                repository,
                branch,
                change.file_path,
            )
            if existing_hash and existing_hash == new_hash:
                logger.info("Skipped (unchanged): %s", change.file_path)

                # Record skipped file in tracking table
                try:
                    await self._db_pool.execute(
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
                        change.file_path,
                        "preprocessor",
                        "unchanged_content",
                        f"Content hash {new_hash[:8]}... unchanged",
                    )
                except Exception as exc:
                    logger.warning("Failed to record skipped file %s: %s", change.file_path, exc)

                return

        if self._version_store is None:
            raise RuntimeError("Version store not initialized - cannot persist ingested documents")

        ingested_doc = await self._version_store.record_change(
            repository=repository,
            branch=branch,
            framework=framework,
            file_path=change.file_path,
            change_type=change.change_type,
            commit_sha=commit_sha,
            content_bytes=content_bytes,
            previous_path=change.previous_path,
            force_full_refresh=diff.force_full_refresh,
        )

        # Enrichment: extract skeleton, chunk, store to Postgres
        if self._db_pool and content_bytes and not change.change_type.upper().startswith("D"):
            try:
                content_text = content_bytes.decode("utf-8", errors="replace")
                document_id = ingested_doc.get("_id")
                if document_id:
                    await enrich_and_store_file(
                        self._db_pool,
                        document_id,
                        change.file_path,
                        content_text,
                        ingestion_id,
                        project_id,
                        company_id,
                    )
            except Exception as exc:
                logger.warning(
                    "Enrichment failed for %s (continuing): %s",
                    change.file_path,
                    exc,
                )

                # Record pipeline error
                if self._db_pool:
                    try:
                        await self._db_pool.execute(
                            """
                            INSERT INTO code_processing.pipeline_errors 
                            (ingestion_id, company_id, project_id, repository, branch, 
                             file_path, service, stage, error_type, error_message)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            """,
                            ingestion_id,
                            company_id,
                            project_id,
                            repository,
                            branch,
                            change.file_path,
                            "preprocessor",
                            "enrichment",
                            type(exc).__name__,
                            str(exc),
                        )
                    except Exception as db_exc:
                        logger.warning(
                            "Failed to record pipeline error for %s: %s", change.file_path, db_exc
                        )
        elif change.change_type.upper().startswith("D"):
            # Emit delete message to enriched-code-chunks topic
            await self._emit_enriched_chunk_delete(
                repository,
                branch,
                change.file_path,
                ingestion_id,
                project_id,
                company_id,
            )

        ingested_doc["force_full_refresh"] = diff.force_full_refresh
        ingested_doc["project_id"] = project_id
        ingested_doc["company_id"] = company_id
        ingested_doc["user_id"] = user_id
        ingested_doc["ingestion_id"] = ingestion_id
        ingested_doc["file_index"] = file_index
        ingested_doc["total_files"] = total_files
        # Phase 4: data_enrichment emit removed (no consumer exists for this topic)

    @staticmethod
    def _read_file_bytes(repo_path: Path, relative_path: str) -> bytes:
        file_path = repo_path / relative_path
        with file_path.open("rb") as handle:
            return handle.read()

    def _calculate_content_metrics(self, document: dict) -> tuple[str, int]:
        """Calculate content hash and size from base64 content."""
        content_b64 = document.get("content_b64")
        if not content_b64:
            return "", 0

        content_bytes = base64.b64decode(content_b64)
        content_hash = hashlib.sha256(content_bytes).hexdigest()
        content_size = len(content_bytes)
        return content_hash, content_size

    def _extract_document_metadata(self, document: dict) -> tuple[str, str, str, str]:
        """Extract core metadata from document."""
        return (
            document.get("repository", ""),
            document.get("branch", "main"),
            document.get("file_path", ""),
            document.get("workspace", "default"),
        )

    def _generate_document_identifiers(
        self, workspace: str, repository: str, branch: str, file_path: str, content_hash: str
    ) -> tuple[str, str]:
        """Generate document ID and canonical path."""
        document_id = IDGenerator.generate_document_id(
            workspace, repository, branch, file_path, content_hash
        )
        canonical_path = CanonicalPath.build_document_path(workspace, repository, branch, file_path)
        return document_id, canonical_path

    async def _emit_enriched_chunk_delete(
        self,
        repository: str,
        branch: str,
        file_path: str,
        ingestion_id: str,
        project_id: str,
        company_id: str,
    ) -> None:
        """Emit a delete message to enriched-code-chunks topic.

        This triggers cleanup of all chunks and entities for the deleted file
        in Qdrant, Neo4j, and Postgres.
        """
        if not self._producer:
            return

        import os as _os
        import json

        output_topic = _os.environ.get("ENRICHED_CHUNKS_TOPIC", "enriched-code-chunks")

        delete_message = {
            "action": "delete",
            "company_id": company_id,
            "project_id": project_id,
            "repository": repository,
            "branch": branch,
            "file_path": file_path,
            "ingestion_id": ingestion_id,
        }

        try:
            key_bytes = file_path.encode("utf-8")
            value_bytes = json.dumps(delete_message).encode("utf-8")

            await self._producer.send(output_topic, value=value_bytes, key=key_bytes)

            logger.info(
                "Emitted enriched chunk delete for %s to %s",
                file_path,
                output_topic,
            )
        except Exception as exc:
            logger.warning(
                "Failed to emit enriched chunk delete for %s: %s",
                file_path,
                exc,
            )


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    consumer = RepositoryIngestionConsumer()

    async with AsyncExitStack() as exit_stack:
        await exit_stack.enter_async_context(consumer)
        logger.info("Repository ingestion consumer is running. Press Ctrl+C to stop.")

        try:
            await consumer.run()
        except asyncio.CancelledError:
            logger.info("Kafka consumer cancelled")
        except KeyboardInterrupt:
            logger.info("Kafka consumer interrupted by user")


if __name__ == "__main__":
    asyncio.run(main())

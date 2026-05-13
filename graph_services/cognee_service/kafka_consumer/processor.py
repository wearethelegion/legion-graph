"""CogneeProcessor — routes DataEnrichmentEvent to Cognee APIs by change_type.

For each file:
  1. Set company context (Neo4j DB = cognee-{company_id}, Qdrant namespace)
  2. Acquire per-dataset lock (serialize within same dataset)
  3. Route by change_type -> cognee.add / cognee.cognify / cognee.datasets.delete_data

Dataset naming convention: {project_id}_{project_name}_code
Branch is a graph relationship, not part of the naming string.
"""

import base64
import os
import time
from pathlib import Path
from typing import Any

import structlog
import cognee
from shared.kafka_schemas import DataEnrichmentEvent, ChangeType

from cognee_service.multi_tenancy import ensure_neo4j_database, set_company_context
from cognee_service.lock import dataset_locks
from cognee_service.chonkie_chunker import ChonkieChunker


logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Custom extraction prompt (optional)
# ---------------------------------------------------------------------------
# Set COGNEE_CUSTOM_PROMPT_PATH to a .txt file path to override the default
# Wikipedia-style extraction prompt with a code-specific one.
_CUSTOM_PROMPT: str | None = None
_prompt_path = os.environ.get("COGNEE_CUSTOM_PROMPT_PATH", "")
if _prompt_path and Path(_prompt_path).is_file():
    _CUSTOM_PROMPT = Path(_prompt_path).read_text(encoding="utf-8").strip()
    logger.info("processor.custom_prompt_loaded", path=_prompt_path, length=len(_CUSTOM_PROMPT))
elif _prompt_path:
    logger.warning("processor.custom_prompt_not_found", path=_prompt_path)


class CogneeProcessor:
    """Processes DataEnrichmentEvent messages through Cognee."""

    def __init__(self, project_resolver=None):
        """Initialize with optional project name resolver."""
        self._project_resolver = project_resolver

    @staticmethod
    def build_node_set(event: DataEnrichmentEvent) -> list[str]:
        """Build project-scoped node_set to avoid cross-project entity mixing."""
        return [f"code_{event.project_id}"]

    async def build_dataset_name(self, event: DataEnrichmentEvent) -> str:
        """Build Cognee dataset name from event metadata.

        Convention: {project_id}_{project_name}_code
        Branch is no longer part of the naming — it's a graph relationship.
        """
        if self._project_resolver:
            project_name = await self._project_resolver.resolve(event.project_id)
        else:
            project_name = event.project_id
        return f"{event.project_id}_{project_name}_code"

    async def process(self, event: DataEnrichmentEvent) -> dict[str, Any]:
        """Process a single event. Routes by change_type.

        Returns dict with keys: status, change_type, file_path, duration_s, ...
        """
        t0 = time.time()
        company_id = event.company_id
        dataset_name = await self.build_dataset_name(event)

        content_b64_len = len(event.content_b64) if event.content_b64 else 0
        logger.info(
            "processor.start",
            file_path=event.file_path,
            change_type=str(event.change_type),
            company_id=company_id,
            dataset_name=dataset_name,
            ingestion_id=event.ingestion_id,
            file_index=event.file_index,
            total_files=event.total_files,
            has_content=bool(event.content_b64),
            content_b64_len=content_b64_len,
        )

        # 1. Set multi-tenancy context
        logger.debug("processor.ensure_neo4j_start", company_id=company_id)
        await ensure_neo4j_database(company_id)
        logger.debug("processor.ensure_neo4j_done", company_id=company_id)
        set_company_context(company_id)
        logger.debug("processor.company_context_set", company_id=company_id)

        # 2. Route by change_type
        try:
            if event.change_type == ChangeType.ADDED:
                result = await self._handle_added(event, dataset_name)
            elif event.change_type == ChangeType.MODIFIED:
                result = await self._handle_modified(event, dataset_name)
            elif event.change_type == ChangeType.DELETED:
                result = await self._handle_deleted(event, dataset_name)
            elif event.change_type == ChangeType.RENAMED:
                result = await self._handle_renamed(event, dataset_name)
            else:
                # COPIED, UNCHANGED — treat as ADDED
                logger.warning(
                    "processor.unknown_change_type",
                    change_type=str(event.change_type),
                    file_path=event.file_path,
                )
                result = await self._handle_added(event, dataset_name)
        except Exception as e:
            duration = time.time() - t0
            logger.error(
                "processor.error",
                file_path=event.file_path,
                error=str(e),
                duration_s=round(duration, 2),
                exc_info=True,
            )
            raise

        result["duration_s"] = round(time.time() - t0, 2)
        return result

    # ── Batch-oriented helpers ────────────────────────────────────────────

    async def add_only(self, event: DataEnrichmentEvent) -> dict[str, Any]:
        """Add file content to Cognee WITHOUT calling cognify.

        Used by the consumer in batch mode: add N files, then cognify once.
        For DELETED/RENAMED, deletion is handled immediately (no batching).
        Returns dict with status.
        """
        t0 = time.time()
        company_id = event.company_id
        dataset_name = await self.build_dataset_name(event)

        logger.info(
            "processor.add_only.start",
            file_path=event.file_path,
            change_type=str(event.change_type),
            dataset_name=dataset_name,
        )

        await ensure_neo4j_database(company_id)
        set_company_context(company_id)

        # DELETED files: handle immediately (no content to batch)
        if event.change_type == ChangeType.DELETED:
            return await self._handle_deleted(event, dataset_name)

        # RENAMED: delete old path immediately, then add new content
        if event.change_type == ChangeType.RENAMED:
            if event.previous_path:
                await self._try_delete_file_data(
                    dataset_name=dataset_name, file_path=event.previous_path
                )

        # MODIFIED: delete old data, then add new
        if event.change_type == ChangeType.MODIFIED:
            await self._try_delete_file_data(dataset_name=dataset_name, file_path=event.file_path)

        # Add content (ADDED, MODIFIED, RENAMED, COPIED, etc.)
        content = self._decode_content(event)
        if content is None:
            return {
                "status": "skipped",
                "reason": "no_content",
                "change_type": str(event.change_type),
            }

        await cognee.add(content, dataset_name=dataset_name, node_set=self.build_node_set(event))
        logger.info(
            "processor.add_only.done",
            file_path=event.file_path,
            dataset_name=dataset_name,
            duration_s=round(time.time() - t0, 2),
        )
        return {
            "status": "added",
            "change_type": str(event.change_type),
            "file_path": event.file_path,
            "dataset_name": dataset_name,
            "duration_s": round(time.time() - t0, 2),
        }

    async def cognify_dataset(self, company_id: str, dataset_name: str) -> dict[str, Any]:
        """Run cognify for a dataset. Called once after a batch of add_only() calls."""
        t0 = time.time()
        logger.info(
            "processor.cognify_batch.start",
            dataset_name=dataset_name,
            company_id=company_id,
        )

        await ensure_neo4j_database(company_id)
        set_company_context(company_id)

        lock = await dataset_locks.acquire(company_id, dataset_name)
        try:
            await cognee.cognify(
                datasets=[dataset_name],
                chunker=ChonkieChunker,
                custom_prompt=_CUSTOM_PROMPT,
            )
        finally:
            lock.release()

        duration = round(time.time() - t0, 2)
        logger.info(
            "processor.cognify_batch.done",
            dataset_name=dataset_name,
            duration_s=duration,
        )
        return {"status": "success", "dataset_name": dataset_name, "duration_s": duration}

    # ── ADDED ────────────────────────────────────────────────────────────

    async def _handle_added(self, event: DataEnrichmentEvent, dataset_name: str) -> dict[str, Any]:
        """ADDED: decode base64 -> cognee.add(content) -> cognee.cognify()

        Exact API flow:
          1. base64.b64decode(event.content_b64).decode("utf-8") -> text content
          2. cognee.add(text, dataset_name=dataset_name)
          3. cognee.cognify(datasets=[dataset_name], chunker=ChonkieChunker)
        """
        content = self._decode_content(event)
        if content is None:
            logger.warning(
                "processor.skipped_no_content",
                file_path=event.file_path,
                change_type="added",
                content_b64_is_none=event.content_b64 is None,
                content_b64_empty=event.content_b64 == "",
            )
            return {"status": "skipped", "reason": "no_content", "change_type": "added"}

        logger.info(
            "processor.content_decoded",
            file_path=event.file_path,
            content_len=len(content),
        )

        lock = await dataset_locks.acquire(event.company_id, dataset_name)
        try:
            logger.info(
                "processor.cognee_add_start", file_path=event.file_path, dataset=dataset_name
            )
            await cognee.add(
                content, dataset_name=dataset_name, node_set=self.build_node_set(event)
            )
            logger.info(
                "processor.cognee_add_done", file_path=event.file_path, dataset=dataset_name
            )

            logger.info("processor.cognify_start", file_path=event.file_path, dataset=dataset_name)
            await cognee.cognify(
                datasets=[dataset_name],
                chunker=ChonkieChunker,
                custom_prompt=_CUSTOM_PROMPT,
            )
            logger.info("processor.cognify_done", file_path=event.file_path, dataset=dataset_name)
        finally:
            lock.release()

        return {
            "status": "success",
            "change_type": "added",
            "file_path": event.file_path,
            "dataset_name": dataset_name,
        }

    # ── MODIFIED ─────────────────────────────────────────────────────────

    async def _handle_modified(
        self, event: DataEnrichmentEvent, dataset_name: str
    ) -> dict[str, Any]:
        """MODIFIED: delete old data -> add new -> cognify

        FALLBACK: If the old data item cannot be found, treat as ADDED.
        """
        content = self._decode_content(event)
        if content is None:
            logger.warning(
                "processor.skipped_no_content",
                file_path=event.file_path,
                change_type="modified",
                content_b64_is_none=event.content_b64 is None,
                content_b64_empty=event.content_b64 == "",
            )
            return {"status": "skipped", "reason": "no_content", "change_type": "modified"}

        logger.info(
            "processor.content_decoded",
            file_path=event.file_path,
            content_len=len(content),
        )

        lock = await dataset_locks.acquire(event.company_id, dataset_name)
        try:
            # Attempt to delete old data
            deleted = await self._try_delete_file_data(
                dataset_name=dataset_name,
                file_path=event.file_path,
            )
            if deleted:
                logger.debug(
                    "processor.modified.old_data_deleted",
                    file_path=event.file_path,
                )
            else:
                logger.debug(
                    "processor.modified.no_old_data_found",
                    file_path=event.file_path,
                )

            # Add new content
            await cognee.add(
                content, dataset_name=dataset_name, node_set=self.build_node_set(event)
            )

            # Re-cognify
            await cognee.cognify(
                datasets=[dataset_name],
                chunker=ChonkieChunker,
                custom_prompt=_CUSTOM_PROMPT,
            )
        finally:
            lock.release()

        return {
            "status": "success",
            "change_type": "modified",
            "file_path": event.file_path,
            "dataset_name": dataset_name,
            "old_data_deleted": deleted,
        }

    # ── DELETED ──────────────────────────────────────────────────────────

    async def _handle_deleted(
        self, event: DataEnrichmentEvent, dataset_name: str
    ) -> dict[str, Any]:
        """DELETED: remove file data from Cognee stores.

        If file not found in Cognee: log warning, return success (idempotent).
        """
        lock = await dataset_locks.acquire(event.company_id, dataset_name)
        try:
            deleted = await self._try_delete_file_data(
                dataset_name=dataset_name,
                file_path=event.file_path,
            )
        finally:
            lock.release()

        if deleted:
            logger.info("processor.deleted.success", file_path=event.file_path)
        else:
            logger.warning(
                "processor.deleted.not_found",
                file_path=event.file_path,
                dataset_name=dataset_name,
            )

        return {
            "status": "success",
            "change_type": "deleted",
            "file_path": event.file_path,
            "dataset_name": dataset_name,
            "data_found_and_deleted": deleted,
        }

    # ── RENAMED ──────────────────────────────────────────────────────────

    async def _handle_renamed(
        self, event: DataEnrichmentEvent, dataset_name: str
    ) -> dict[str, Any]:
        """RENAMED: delete old path -> add new content at new path -> cognify

        If previous_path is None: treat as ADDED (no old data to delete).
        """
        content = self._decode_content(event)
        if content is None:
            logger.warning(
                "processor.skipped_no_content",
                file_path=event.file_path,
                change_type="renamed",
                content_b64_is_none=event.content_b64 is None,
                content_b64_empty=event.content_b64 == "",
            )
            return {"status": "skipped", "reason": "no_content", "change_type": "renamed"}

        logger.info(
            "processor.content_decoded",
            file_path=event.file_path,
            content_len=len(content),
        )

        lock = await dataset_locks.acquire(event.company_id, dataset_name)
        try:
            old_deleted = False
            if event.previous_path:
                old_deleted = await self._try_delete_file_data(
                    dataset_name=dataset_name,
                    file_path=event.previous_path,
                )
                logger.debug(
                    "processor.renamed.old_path",
                    previous_path=event.previous_path,
                    deleted=old_deleted,
                )
            else:
                logger.warning(
                    "processor.renamed.no_previous_path",
                    file_path=event.file_path,
                )

            # Add at new path
            await cognee.add(
                content, dataset_name=dataset_name, node_set=self.build_node_set(event)
            )

            # Cognify
            await cognee.cognify(
                datasets=[dataset_name],
                chunker=ChonkieChunker,
                custom_prompt=_CUSTOM_PROMPT,
            )
        finally:
            lock.release()

        return {
            "status": "success",
            "change_type": "renamed",
            "file_path": event.file_path,
            "previous_path": event.previous_path,
            "dataset_name": dataset_name,
            "old_data_deleted": old_deleted,
        }

    # ── Shared Helpers ───────────────────────────────────────────────────

    async def _try_delete_file_data(self, dataset_name: str, file_path: str) -> bool:
        """Attempt to find and delete a file's data from Cognee.

        Steps:
          1. cognee.datasets.list_datasets() — find dataset by name
          2. cognee.datasets.list_data(dataset_id) — find data item by name match
          3. cognee.datasets.delete_data(dataset_id, data_id) — delete

        Returns True if found and deleted, False if not found.
        Exceptions propagate (caller handles).
        """
        try:
            datasets = await cognee.datasets.list_datasets()
        except Exception:
            logger.debug("processor.delete.no_datasets")
            return False

        target_dataset = None
        for ds in datasets:
            if getattr(ds, "name", None) == dataset_name:
                target_dataset = ds
                break

        if target_dataset is None:
            return False

        try:
            data_items = await cognee.datasets.list_data(target_dataset.id)
        except Exception:
            return False

        target_item = None
        for item in data_items:
            item_name = getattr(item, "name", None) or ""
            if item_name == file_path or item_name.endswith(file_path):
                target_item = item
                break

        if target_item is None:
            return False

        await cognee.datasets.delete_data(
            dataset_id=target_dataset.id,
            data_id=target_item.id,
        )
        return True

    def _decode_content(self, event: DataEnrichmentEvent) -> str | None:
        """Decode base64 content from event. Returns None if no content."""
        if not event.content_b64:
            return None
        try:
            content_bytes = base64.b64decode(event.content_b64)
            return content_bytes.decode("utf-8")
        except Exception as e:
            logger.error(
                "processor.decode_error",
                file_path=event.file_path,
                error=str(e),
            )
            return None

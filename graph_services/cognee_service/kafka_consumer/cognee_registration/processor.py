"""Cognee Registration Processor.

Handles calling cognee.add() for each enriched chunk message, registering
file content in Cognee's metadata tables (datasets, data, dataset_data).

This logic was previously in the code preprocessor as _register_with_cognee().
Moved here to decouple the preprocessor from the cognee package entirely.

Key behaviours preserved from the original implementation:
- Prepends a "Source: {file_path}" header to avoid cognee.add() path bug
  (cognee.add() misinterprets text starting with '/' as a filesystem path)
- Uses asyncio.Lock to serialize cognee.add() calls and prevent Neo4j
  CREATE CONSTRAINT deadlocks from concurrent requests
- Scopes each call to the correct company via multi_tenancy helpers
- Errors are caught and logged — never propagated to the consumer loop
- Only processes "process" action messages; skips "delete" actions
"""

import asyncio
import logging
from typing import Optional

import structlog

from cognee_service.kafka_consumer.enriched_chunks.models import EnrichedChunkMessage

logger = structlog.get_logger(__name__)

# Serialized lock prevents Neo4j deadlocks from concurrent CREATE CONSTRAINT
# statements that cognee.add() triggers on first use of a new dataset.
_cognee_lock = asyncio.Lock()


class CogneeRegistrationProcessor:
    """Calls cognee.add() for each enriched chunk to register it in Cognee metadata."""

    def build_dataset_name(self, msg: EnrichedChunkMessage) -> str:
        """Build Cognee dataset name from message metadata.

        Documents use the company-level knowledge scope; code keeps the
        project-scoped dataset naming.
        """
        if msg.content_type == "document" or not msg.project_id:
            return f"{msg.company_id}_knowledge"

        branch = msg.branch.replace("/", "_").replace("-", "_")
        return f"{msg.project_id}_{branch}_code"

    async def register(self, msg: EnrichedChunkMessage) -> None:
        """Call cognee.add() for a single enriched chunk message.

        Only processes "process" action messages. Delete actions are skipped
        (cognee.add removal is handled by the enriched chunks consumer's
        delete_files method).

        Errors are caught and logged — this method never raises.
        """
        if msg.action == "delete":
            logger.debug(
                "registration.skip_delete",
                file_path=msg.file_path,
                action=msg.action,
            )
            return

        if not msg.content:
            logger.warning(
                "registration.skip_empty_content",
                file_path=msg.file_path,
                chunk_id=msg.chunk_id,
            )
            return

        dataset_name = self.build_dataset_name(msg)
        await self._call_cognee_add(
            content=msg.content,
            file_path=msg.file_path,
            company_id=msg.company_id,
            dataset_name=dataset_name,
        )

    async def _call_cognee_add(
        self,
        content: str,
        file_path: str,
        company_id: str,
        dataset_name: str,
    ) -> None:
        """Perform the actual cognee.add() call with all safeguards.

        Must NOT raise — all exceptions are caught and logged.
        Serialized via _cognee_lock to prevent Neo4j deadlocks.
        """
        try:
            import cognee_service.cognee_patches  # noqa: F401 — patches applied on import
            import cognee
            from cognee_service.multi_tenancy import ensure_neo4j_database, set_company_context

            # Workaround: cognee.add() treats strings starting with '/' as file paths.
            # Prepend a Source header so content never starts with '/'.
            # See lesson: "cognee.add() misinterprets file content starting with '/'"
            text = f"Source: {file_path}\n\n{content}"

            async with _cognee_lock:
                await ensure_neo4j_database(company_id)
                set_company_context(company_id)

                # Register file — creates all metadata + writes to filesystem.
                # Do NOT call cognee.cognify() — our v2 pipeline handles processing.
                await cognee.add(text, dataset_name=dataset_name)

            logger.debug(
                "registration.cognee_add_ok",
                file_path=file_path,
                dataset_name=dataset_name,
            )
        except Exception as exc:
            # Must NOT crash the pipeline
            logger.warning(
                "registration.cognee_add_failed",
                file_path=file_path,
                dataset_name=dataset_name,
                error=str(exc),
            )

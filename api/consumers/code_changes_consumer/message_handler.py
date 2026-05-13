"""Message handler for code change events.

Processes DataEnrichmentEvent messages: decodes base64 content,
analyzes code with CodeServiceV2, and stores results.
"""

import base64
import time
from typing import Any

from loguru import logger
from shared.kafka_schemas import DataEnrichmentEvent, ChangeType

from api.services.code_service_v2 import CodeAnalyzerService, CodeFile
from api.services.code_service_v2.batch_processor import BatchProcessor
from .config import CodeChangesConsumerConfig
from .ingestion_metrics import IngestionMetricsTracker


# Binary and document file extensions to skip
BINARY_EXTENSIONS = {
    # Images
    '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.ico', '.svg', '.webp',
    # Documents
    '.doc', '.docx', '.pdf', '.md', '.xlsx', '.xls', '.pptx', '.ppt',
    # Archives
    '.zip', '.tar', '.gz', '.bz2', '.7z', '.rar',
    # Media
    '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.wav', '.flac',
    # Fonts
    '.ttf', '.otf', '.woff', '.woff2', '.eot',
    # Executables
    '.exe', '.dll', '.so', '.dylib',
    # Data
    '.bin', '.dat', '.db', '.sqlite', '.sqlite3',
    # Java/Android
    '.jar', '.aar', '.war', '.ear', '.jks', '.keystore', '.class',
    # iOS/macOS
    '.framework', '.xcframework', '.a', '.dylib',
    # Config/lock files
    '.lock', '.sum', '.mod',
    # Gradle wrapper
    '.gradle', '.properties',
}


class MessageHandler:
    """Handles processing of individual code change messages."""

    def __init__(
        self,
        code_analyzer_service: CodeAnalyzerService,
        config: type[CodeChangesConsumerConfig] = CodeChangesConsumerConfig,
        metrics_tracker: IngestionMetricsTracker | None = None,
    ):
        """Initialize message handler.

        Args:
            code_analyzer_service: Code analysis service instance
            config: Configuration class
            metrics_tracker: Optional ingestion metrics tracker for LLM stats
        """
        self.code_service = code_analyzer_service
        self.config = config
        self.metrics_tracker = metrics_tracker

    async def handle_message(
        self,
        event: DataEnrichmentEvent,
    ) -> dict[str, Any]:
        """Process a single code change event.

        Routes based on change_type:
        - DELETED: Delete from storage (no analysis)
        - MODIFIED: Analyze + update (delete old + store new)
        - ADDED: Analyze + store new
        - RENAMED: Delete old_path + analyze + store new_path (requires old_path)

        Args:
            event: Data enrichment event from Kafka

        Returns:
            Processing result with status and statistics

        Raises:
            ValueError: If event is invalid
            Exception: If processing fails after retries
        """
        start_time = time.time()

        logger.info(
            f"Processing event {event.event_id}: {event.file_path} "
            f"({event.change_type}, stage: {event.stage})"
        )

        # Step 1: Extract multi-tenant IDs early (needed for all paths)
        try:
            project_id, company_id, user_id = self._extract_tenant_ids(event)
        except ValueError as e:
            logger.error(str(e))
            return {
                "status": "error",
                "error": str(e),
                "event_id": event.event_id,
                "duration": time.time() - start_time,
            }

        # Step 2: Route based on change_type
        if event.change_type == ChangeType.DELETED:
            # No analysis needed - just delete from storage
            return await self._handle_deleted(
                event=event,
                project_id=project_id,
                company_id=company_id,
                start_time=start_time,
            )

        # For ADDED and MODIFIED, we need content
        # Step 3: Validate and filter
        if not self._should_process(event):
            reason = self._get_skip_reason(event)
            logger.info(f"Skipping {event.file_path}: {reason}")
            return {
                "status": "skipped",
                "reason": reason,
                "event_id": event.event_id,
                "duration": time.time() - start_time,
            }

        # Step 4: Decode content
        try:
            content = self._decode_content(event)
        except Exception as e:
            logger.error("Failed to decode content for {}: {}", event.file_path, e)
            return {
                "status": "error",
                "error": f"Decode error: {e}",
                "event_id": event.event_id,
                "duration": time.time() - start_time,
            }

        # Step 5: Detect language
        language = self._detect_language(event)
        if not language:
            logger.warning(
                f"Unsupported language for {event.file_path} "
                f"(extension: {event.file_extension})"
            )
            return {
                "status": "skipped",
                "reason": "unsupported_language",
                "event_id": event.event_id,
                "duration": time.time() - start_time,
            }

        # Step 6: Route to appropriate handler
        if event.change_type == ChangeType.MODIFIED:
            return await self._handle_modified(
                event=event,
                content=content,
                language=language,
                project_id=project_id,
                company_id=company_id,
                user_id=user_id,
                start_time=start_time,
            )
        elif event.change_type == ChangeType.ADDED:
            return await self._handle_added(
                event=event,
                content=content,
                language=language,
                project_id=project_id,
                company_id=company_id,
                user_id=user_id,
                start_time=start_time,
            )
        elif event.change_type == ChangeType.RENAMED:
            # RENAMED: Delete old path, then process new path as ADDED
            return await self._handle_renamed(
                event=event,
                content=content,
                language=language,
                project_id=project_id,
                company_id=company_id,
                user_id=user_id,
                start_time=start_time,
            )
        else:
            # COPIED, UNCHANGED, or unknown
            logger.warning(
                f"Unhandled change_type {event.change_type} for {event.file_path}. "
                f"Treating as ADDED."
            )
            return await self._handle_added(
                event=event,
                content=content,
                language=language,
                project_id=project_id,
                company_id=company_id,
                user_id=user_id,
                start_time=start_time,
            )

    async def _handle_deleted(
        self,
        event: DataEnrichmentEvent,
        project_id: str,
        company_id: str,
        start_time: float,
    ) -> dict[str, Any]:
        """Handle DELETED files - remove from storage without analysis.

        Args:
            event: Data enrichment event
            project_id: Project UUID
            company_id: Company UUID
            start_time: Processing start timestamp

        Returns:
            Deletion result with stats
        """
        try:
            logger.info(f"Deleting {event.file_path} from storage")

            delete_stats = await self.code_service.storage_orchestrator.delete_by_file_path(
                file_path=event.file_path,
                project_id=project_id,
                company_id=company_id,
            )

            duration = time.time() - start_time

            logger.success(
                f"Deleted {event.file_path}: "
                f"{delete_stats['neo4j_nodes_deleted']} nodes, "
                f"{delete_stats['qdrant_points_deleted']} vectors "
                f"in {duration:.2f}s"
            )

            return {
                "status": "success",
                "change_type": "deleted",
                "event_id": event.event_id,
                "file_path": event.file_path,
                "neo4j_nodes_deleted": delete_stats["neo4j_nodes_deleted"],
                "qdrant_points_deleted": delete_stats["qdrant_points_deleted"],
                "duration": duration,
            }

        except Exception as e:
            logger.error("Failed to delete {}: {}", event.file_path, e, exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
                "event_id": event.event_id,
                "file_path": event.file_path,
                "duration": time.time() - start_time,
            }

    async def _handle_modified(
        self,
        event: DataEnrichmentEvent,
        content: str,
        language: str,
        project_id: str,
        company_id: str,
        user_id: str,
        start_time: float,
    ) -> dict[str, Any]:
        """Handle MODIFIED files - delete old data, then analyze and store new.

        Uses DELETE + ADD pattern:
        1. Delete existing file data from storage
        2. Analyze new content
        3. Store new analysis results (handled by analyze_files internally)

        Args:
            event: Data enrichment event
            content: Decoded file content
            language: Detected language
            project_id: Project UUID
            company_id: Company UUID
            user_id: User UUID
            start_time: Processing start timestamp

        Returns:
            Update result with stats
        """
        try:
            logger.debug(
                f"Handling modified file {event.file_path} "
                f"(project: {project_id}, company: {company_id})"
            )

            # Step 1: Delete old file data
            delete_stats = await self.code_service.storage_orchestrator.delete_by_file_path(
                file_path=event.file_path,
                project_id=project_id,
                company_id=company_id,
            )

            logger.debug(
                f"Deleted old data for {event.file_path}: "
                f"{delete_stats['neo4j_nodes_deleted']} nodes, "
                f"{delete_stats['qdrant_points_deleted']} vectors"
            )

            # Step 2: Create CodeFile for analysis
            code_file = CodeFile(
                file_path=event.file_path,
                content=content,
                language=language,
            )

            # Step 3: Analyze and store new content
            # analyze_files handles storage internally via store_single_result
            report = await self.code_service.analyze_files(
                files=[code_file],
                project_id=project_id,
                company_id=company_id,
                user_id=user_id,
            )

            duration = time.time() - start_time

            # Track LLM metrics in MongoDB
            if self.metrics_tracker and event.ingestion_id:
                if report.successful > 0:
                    self.metrics_tracker.record_llm_success(event.ingestion_id)
                if report.failed > 0:
                    self.metrics_tracker.record_llm_error(event.ingestion_id)

            # Check if analysis succeeded
            if report.failed > 0:
                logger.warning(
                    f"Analysis failed for modified file {event.file_path}, "
                    f"old data was deleted"
                )
                return {
                    "status": "error",
                    "error": f"Analysis failed for {event.file_path}",
                    "change_type": "modified",
                    "event_id": event.event_id,
                    "file_path": event.file_path,
                    "neo4j_nodes_deleted": delete_stats["neo4j_nodes_deleted"],
                    "qdrant_points_deleted": delete_stats["qdrant_points_deleted"],
                    "duration": duration,
                }

            logger.success(
                f"Updated {event.file_path}: "
                f"deleted {delete_stats['neo4j_nodes_deleted']} nodes/"
                f"{delete_stats['qdrant_points_deleted']} vectors, "
                f"created {report.storage_stats.neo4j_nodes_created} nodes/"
                f"{report.storage_stats.qdrant_points_created} vectors "
                f"in {duration:.2f}s"
            )

            return {
                "status": "success",
                "change_type": "modified",
                "event_id": event.event_id,
                "file_path": event.file_path,
                "neo4j_nodes_deleted": delete_stats["neo4j_nodes_deleted"],
                "qdrant_points_deleted": delete_stats["qdrant_points_deleted"],
                "neo4j_nodes_created": report.storage_stats.neo4j_nodes_created,
                "neo4j_relationships_created": report.storage_stats.neo4j_relationships_created,
                "qdrant_points_created": report.storage_stats.qdrant_points_created,
                "duration": duration,
            }

        except Exception as e:
            logger.error("Failed to update {}: {}", event.file_path, e, exc_info=True)
            # Track error in ingestion metrics
            if self.metrics_tracker and event.ingestion_id:
                self.metrics_tracker.record_llm_error(event.ingestion_id)
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
                "event_id": event.event_id,
                "file_path": event.file_path,
                "duration": time.time() - start_time,
            }

    async def _handle_added(
        self,
        event: DataEnrichmentEvent,
        content: str,
        language: str,
        project_id: str,
        company_id: str,
        user_id: str,
        start_time: float,
    ) -> dict[str, Any]:
        """Handle ADDED files - analyze and store new.

        Args:
            event: Data enrichment event
            content: Decoded file content
            language: Detected language
            project_id: Project UUID
            company_id: Company UUID
            user_id: User UUID
            start_time: Processing start timestamp

        Returns:
            Storage result with stats
        """
        try:
            logger.debug(
                f"Analyzing new file {event.file_path} "
                f"(project: {project_id}, company: {company_id})"
            )

            # Create CodeFile
            code_file = CodeFile(
                file_path=event.file_path,
                content=content,
                language=language,
            )

            # Analyze code
            report = await self.code_service.analyze_files(
                files=[code_file],
                project_id=project_id,
                company_id=company_id,
                user_id=user_id,
            )

            duration = time.time() - start_time

            # Track LLM metrics in MongoDB
            if self.metrics_tracker and event.ingestion_id:
                if report.successful > 0:
                    self.metrics_tracker.record_llm_success(event.ingestion_id)
                if report.failed > 0:
                    self.metrics_tracker.record_llm_error(event.ingestion_id)

            logger.success(
                f"Completed {event.file_path}: "
                f"{report.successful} successful, {report.failed} failed "
                f"in {duration:.2f}s"
            )

            return {
                "status": "success",
                "change_type": "added",
                "event_id": event.event_id,
                "file_path": event.file_path,
                "successful": report.successful,
                "failed": report.failed,
                "skipped": report.skipped,
                "neo4j_nodes": report.storage_stats.neo4j_nodes_created,
                "neo4j_relationships": report.storage_stats.neo4j_relationships_created,
                "qdrant_points": report.storage_stats.qdrant_points_created,
                "duration": duration,
            }

        except Exception as e:
            logger.error("Failed to analyze {}: {}", event.file_path, e, exc_info=True)
            # Track error in ingestion metrics
            if self.metrics_tracker and event.ingestion_id:
                self.metrics_tracker.record_llm_error(event.ingestion_id)
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
                "event_id": event.event_id,
                "file_path": event.file_path,
                "duration": time.time() - start_time,
            }

    async def _handle_renamed(
        self,
        event: DataEnrichmentEvent,
        content: str,
        language: str,
        project_id: str,
        company_id: str,
        user_id: str,
        start_time: float,
    ) -> dict[str, Any]:
        """Handle RENAMED files - delete old path, then analyze and store new.

        Args:
            event: Data enrichment event
            content: Decoded file content
            language: Detected language
            project_id: Project UUID
            company_id: Company UUID
            user_id: User UUID
            start_time: Processing start timestamp

        Returns:
            Rename result with stats from both delete and add operations
        """
        try:
            delete_stats = None

            # Delete old path if previous_path is available
            if event.previous_path:
                logger.info(
                    f"Handling RENAMED: deleting old path {event.previous_path}, "
                    f"adding new path {event.file_path}"
                )

                delete_stats = await self.code_service.storage_orchestrator.delete_by_file_path(
                    file_path=event.previous_path,
                    project_id=project_id,
                    company_id=company_id,
                )

                logger.debug(
                    f"Deleted old path {event.previous_path}: "
                    f"{delete_stats['neo4j_nodes_deleted']} nodes, "
                    f"{delete_stats['qdrant_points_deleted']} vectors"
                )
            else:
                logger.warning(
                    f"RENAMED event for {event.file_path} missing previous_path. "
                    f"Skipping old path deletion."
                )

            # Now process new path as ADDED
            add_result = await self._handle_added(
                event=event,
                content=content,
                language=language,
                project_id=project_id,
                company_id=company_id,
                user_id=user_id,
                start_time=start_time,
            )

            # Augment result with delete stats
            if delete_stats:
                add_result["change_type"] = "renamed"
                add_result["previous_path"] = event.previous_path
                add_result["neo4j_nodes_deleted"] = delete_stats["neo4j_nodes_deleted"]
                add_result["qdrant_points_deleted"] = delete_stats["qdrant_points_deleted"]

            return add_result

        except Exception as e:
            logger.error("Failed to handle RENAMED for {}: {}", event.file_path, e, exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
                "event_id": event.event_id,
                "file_path": event.file_path,
                "previous_path": event.previous_path,
                "duration": time.time() - start_time,
            }

    def _should_process(self, event: DataEnrichmentEvent) -> bool:
        """Determine if event should be processed.

        Note: This is called AFTER change_type routing, so DELETED files
        are already handled separately. This validates content-based events
        (ADDED, MODIFIED, etc.).

        Args:
            event: Data enrichment event

        Returns:
            True if event should be processed
        """
        # Skip binary/document files
        if event.file_extension and event.file_extension.lower() in BINARY_EXTENSIONS:
            return False

        # Skip if no content (required for analysis)
        if not event.content_b64:
            return False

        # Skip if file is too small (will be caught by BatchProcessor)
        if event.content_size_bytes < 10:  # Extremely small
            return False

        return True

    def _get_skip_reason(self, event: DataEnrichmentEvent) -> str:
        """Get reason for skipping event.

        Note: DELETED files are not skipped, they're handled separately.
        """
        if event.file_extension and event.file_extension.lower() in BINARY_EXTENSIONS:
            return "binary_file"
        if not event.content_b64:
            return "no_content"
        if event.content_size_bytes < 10:
            return "file_too_small"
        return "unknown"

    def _decode_content(self, event: DataEnrichmentEvent) -> str:
        """Decode base64 content to UTF-8 string.

        Args:
            event: Data enrichment event

        Returns:
            Decoded content as string

        Raises:
            ValueError: If content cannot be decoded
        """
        if not event.content_b64:
            raise ValueError("No content to decode")

        if event.content_encoding != "base64":
            raise ValueError(
                f"Unsupported encoding: {event.content_encoding}"
            )

        try:
            content_bytes = base64.b64decode(event.content_b64)
            return content_bytes.decode("utf-8")
        except Exception as e:
            raise ValueError(f"Failed to decode base64 content: {e}") from e

    def _detect_language(self, event: DataEnrichmentEvent) -> str | None:
        """Detect programming language from file extension.

        Args:
            event: Data enrichment event

        Returns:
            Language name or None if unsupported
        """
        # Use language from event if available
        if event.language:
            return event.language

        # Detect from file extension
        if event.file_extension:
            return BatchProcessor.detect_language(event.file_path)

        # Fallback to file path
        return BatchProcessor.detect_language(event.file_path)

    def _extract_tenant_ids(
        self, event: DataEnrichmentEvent
    ) -> tuple[str, str, str]:
        """Extract project_id, company_id, user_id from event.

        Args:
            event: Data enrichment event

        Returns:
            Tuple of (project_id, company_id, user_id)

        Raises:
            ValueError: If required tenant IDs are missing (skips legacy messages)
        """
        project_id = event.project_id
        company_id = event.company_id
        user_id = event.user_id

        if not project_id or not company_id:
            raise ValueError(
                f"Legacy message without tenant IDs (file: {event.file_path}). "
                f"This message will be skipped."
            )

        logger.debug(
            f"Extracted tenant IDs: project={project_id}, "
            f"company={company_id}, user={user_id}"
        )

        return project_id, company_id, user_id

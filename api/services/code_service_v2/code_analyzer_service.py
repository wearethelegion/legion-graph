"""Main code analyzer service - orchestrates the entire pipeline.

Facade pattern: Provides simple interface to complex subsystem of:
- File batching
- Parallel Gemini processing
- Storage orchestration
"""

import asyncio
import time
from typing import Any

from loguru import logger

from .batch_processor import BatchProcessor, CodeFile
from .config import CodeServiceConfig
from .gemini_client import GeminiCodeClient
from .parallel_executor import ParallelExecutor
from .schema import AnalysisReport, CodeAnalysisResult, StorageStats
from .storage_orchestrator import StorageOrchestrator


class CodeAnalyzerService:
    """Main service for code analysis with batching, caching, and parallel processing."""

    def __init__(
        self,
        neo4j_repository: Any,
        qdrant_repository: Any,
        project_repository: Any,
    ):
        """Initialize code analyzer service.

        Args:
            neo4j_repository: Neo4j repository instance
            qdrant_repository: Qdrant repository instance
            project_repository: Project repository instance
        """
        self.project_repo = project_repository
        self.batch_processor = BatchProcessor()
        self.parallel_executor = ParallelExecutor()
        self.storage_orchestrator = StorageOrchestrator(
            neo4j_repository=neo4j_repository,
            qdrant_repository=qdrant_repository,
        )

        # Lazy initialization of Gemini client
        self._gemini_client: GeminiCodeClient | None = None

    async def _get_gemini_client(self) -> GeminiCodeClient:
        """Get or create Gemini client with cache initialized."""
        if not self._gemini_client:
            self._gemini_client = GeminiCodeClient()
            await self._gemini_client.initialize_cache()
        return self._gemini_client

    async def analyze_files(
        self,
        files: list[CodeFile],
        project_id: str,
        company_id: str,
        user_id: str,
        max_concurrent: int = CodeServiceConfig.MAX_CONCURRENT_REQUESTS,
    ) -> AnalysisReport:
        """Analyze multiple code files with batching and parallel processing.

        This is the main entry point for code analysis.

        Workflow:
        1. Filter and batch files
        2. Initialize Gemini client with cache
        3. Process files in parallel (respecting rate limits)
        4. Store results in Neo4j + Qdrant
        5. Return analysis report

        Args:
            files: List of code files to analyze
            project_id: Project UUID
            company_id: Company UUID
            user_id: User UUID
            max_concurrent: Maximum concurrent requests

        Returns:
            Analysis report with statistics and errors
        """
        start_time = time.time()

        logger.info(
            f"Starting code analysis: {len(files)} files "
            f"(project: {project_id}, max_concurrent: {max_concurrent})"
        )

        # Step 1: Filter files
        processable_files = [
            f for f in files
            if self.batch_processor.should_process(f)
        ]

        skipped = len(files) - len(processable_files)

        if not processable_files:
            logger.warning("No files to process after filtering")
            return AnalysisReport(
                total_files=len(files),
                successful=0,
                failed=0,
                skipped=skipped,
                storage_stats=StorageStats(),
                duration_seconds=time.time() - start_time,
            )

        # Step 2: Initialize Gemini client with cache
        gemini_client = await self._get_gemini_client()

        # Step 3 & 4: Process files in parallel + store incrementally (streaming)
        logger.info(
            f"Processing {len(processable_files)} files with streaming storage "
            f"(max_concurrent: {max_concurrent})..."
        )

        # Thread-safe stats aggregation
        stats_lock = asyncio.Lock()
        aggregate_stats = StorageStats()

        async def store_result_callback(result: CodeAnalysisResult) -> None:
            """Store result immediately as it completes."""
            try:
                # Store single result (incremental storage!)
                single_stats = await self.storage_orchestrator.store_single_result(
                    result=result,
                    project_id=project_id,
                    company_id=company_id,
                    user_id=user_id,
                )

                # Aggregate stats thread-safely
                async with stats_lock:
                    aggregate_stats.neo4j_nodes_created += single_stats.neo4j_nodes_created
                    aggregate_stats.neo4j_relationships_created += single_stats.neo4j_relationships_created
                    aggregate_stats.qdrant_points_created += single_stats.qdrant_points_created
                    aggregate_stats.duration_seconds += single_stats.duration_seconds

            except Exception as e:
                logger.error(
                    f"Failed to store {result.file_metadata.file_path}: {e}",
                    exc_info=True
                )
                raise

        # Process with streaming (store as complete)
        # Returns (successful_count, error_count, fallback_count)
        successful_count, error_count, fallback_count = await self.parallel_executor.process_files_streaming(
            files=processable_files,
            gemini_client=gemini_client,
            result_callback=store_result_callback,
            max_concurrent=max_concurrent,
        )

        storage_stats = aggregate_stats

        # Step 5: Build report
        duration = time.time() - start_time
        report = AnalysisReport(
            total_files=len(files),
            successful=successful_count,
            failed=error_count,
            skipped=skipped,
            storage_stats=storage_stats,
            duration_seconds=duration,
        )

        logger.success(
            f"Analysis complete: {report.successful} successful, "
            f"{report.failed} failed, {report.skipped} skipped "
            f"in {duration:.2f}s | "
            f"Neo4j: {storage_stats.neo4j_nodes_created} nodes, "
            f"{storage_stats.neo4j_relationships_created} rels | "
            f"Qdrant: {storage_stats.qdrant_points_created} vectors"
        )

        return report

    async def close(self) -> None:
        """Clean up resources."""
        if self._gemini_client:
            await self._gemini_client.close()
            self._gemini_client = None
            logger.info("Code analyzer service closed")

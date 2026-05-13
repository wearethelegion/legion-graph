"""Parallel execution with rate limiting.

Handles concurrent processing of code files with proper rate limiting
and semaphore control to respect Gemini API limits.

ADR-3 COMPLIANCE: Includes fallback to CodeTextChunker when LLM fails.
"""

import asyncio
from typing import Callable, Union

from aiolimiter import AsyncLimiter
from loguru import logger

from .batch_processor import CodeFile
from .code_text_chunker import CodeTextChunker, TextChunk
from .config import CodeServiceConfig
from .gemini_client import GeminiCodeClient
from .schema import CodeAnalysisResult


class ParallelExecutor:
    """Executes code analysis in parallel with rate limiting.

    ADR-3: Includes text chunker fallback for when LLM analysis fails.
    """

    def __init__(self):
        """Initialize executor with rate limiter and text chunker fallback."""
        # Rate limiter: 1500 requests per minute
        self.rate_limiter = AsyncLimiter(
            max_rate=CodeServiceConfig.RATE_LIMIT_RPM,
            time_period=60  # per minute
        )
        # ADR-3: Text chunker for LLM fallback
        self._text_chunker = CodeTextChunker()

    async def process_files_parallel(
        self,
        files: list[CodeFile],
        gemini_client: GeminiCodeClient,
        max_concurrent: int = CodeServiceConfig.MAX_CONCURRENT_REQUESTS,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> tuple[list[CodeAnalysisResult], list[dict]]:
        """Process files in parallel with rate limiting.

        Args:
            files: List of code files to process
            gemini_client: Initialized Gemini client
            max_concurrent: Maximum concurrent requests
            progress_callback: Optional callback(completed, total)

        Returns:
            Tuple of (successful_results, errors)
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        total = len(files)

        logger.info(
            f"Starting parallel processing: {total} files, "
            f"max {max_concurrent} concurrent"
        )

        # Create tasks
        tasks = [
            self._process_single_file(
                file=file,
                gemini_client=gemini_client,
                semaphore=semaphore,
                file_index=i,
                total=total,
                progress_callback=progress_callback,
            )
            for i, file in enumerate(files, 1)
        ]

        # Execute all tasks and collect results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Separate successful results from errors
        successful = []
        errors = []

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                errors.append({
                    "file_path": files[i].file_path,
                    "error": str(result),
                    "error_type": type(result).__name__,
                })
            elif result is not None:
                successful.append(result)

        logger.success(
            f"Parallel processing complete: {len(successful)} successful, "
            f"{len(errors)} errors"
        )

        return successful, errors

    async def process_files_streaming(
        self,
        files: list[CodeFile],
        gemini_client: GeminiCodeClient,
        result_callback: Callable,
        error_callback: Callable | None = None,
        fallback_callback: Callable | None = None,
        max_concurrent: int = CodeServiceConfig.MAX_CONCURRENT_REQUESTS,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> tuple[int, int, int]:
        """Process files with streaming - store results as they complete.

        Instead of waiting for all files, results are passed to callback
        immediately as each file completes. This enables incremental storage.

        ADR-3 COMPLIANCE: Includes fallback handling for LLM failures.

        Args:
            files: List of code files to process
            gemini_client: Initialized Gemini client
            result_callback: async callback(result) called for each LLM success
            error_callback: async callback(file_path, error) for each failure
            fallback_callback: async callback(chunks, file_path) for text chunker fallback
            max_concurrent: Maximum concurrent requests
            progress_callback: Optional callback(completed, total)

        Returns:
            Tuple of (successful_count, error_count, fallback_count)
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        total = len(files)
        successful_count = 0
        error_count = 0
        fallback_count = 0

        logger.info(
            f"Starting streaming processing: {total} files "
            f"(results stored incrementally)"
        )

        # Create tasks
        tasks = [
            self._process_single_file_with_index(
                file=file,
                gemini_client=gemini_client,
                semaphore=semaphore,
                file_index=i,
                total=total,
                progress_callback=progress_callback,
            )
            for i, file in enumerate(files, 1)
        ]

        # Process results as they complete (streaming!)
        for coro in asyncio.as_completed(tasks):
            try:
                result, file_index, result_type = await coro

                if result:
                    if result_type == "llm":
                        # LLM analysis successful
                        await result_callback(result)
                        successful_count += 1
                        logger.debug(
                            f"[{file_index}/{total}] Stored LLM result for {result.file_metadata.file_path}"
                        )
                    elif result_type == "fallback":
                        # Text chunker fallback
                        fallback_count += 1
                        if fallback_callback:
                            file_path = files[file_index - 1].file_path if file_index <= len(files) else "unknown"
                            await fallback_callback(result, file_path)
                            logger.debug(
                                f"[{file_index}/{total}] Stored fallback chunks for {file_path}"
                            )

            except Exception as e:
                error_count += 1
                file_path = files[file_index - 1].file_path if file_index <= len(files) else "unknown"

                logger.error("Failed to process {}: {}", file_path, e)

                if error_callback:
                    await error_callback(file_path, e)

        logger.success(
            f"Streaming complete: {successful_count} LLM, {fallback_count} fallback, {error_count} errors"
        )

        return successful_count, error_count, fallback_count

    async def _process_single_file_with_index(
        self,
        file: CodeFile,
        gemini_client: GeminiCodeClient,
        semaphore: asyncio.Semaphore,
        file_index: int,
        total: int,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> tuple[Union[CodeAnalysisResult, list[TextChunk], None], int, str]:
        """Process single file, return (result, file_index, result_type) for streaming.

        ADR-3 COMPLIANCE: Falls back to text chunking when LLM fails.

        Args:
            file: Code file to process
            gemini_client: Gemini client
            semaphore: Concurrency limiter
            file_index: Current file index (1-based)
            total: Total files count
            progress_callback: Progress callback

        Returns:
            Tuple of (result, file_index, result_type)
            result_type is "llm" for successful LLM analysis, "fallback" for text chunking
        """
        async with semaphore:
            async with self.rate_limiter:
                logger.debug(
                    f"Processing [{file_index}/{total}] {file.file_path}"
                )

                try:
                    result = await gemini_client.analyze_code(
                        file_path=file.file_path,
                        language=file.language,
                        source_code=file.content,
                    )

                    if progress_callback:
                        progress_callback(file_index, total)

                    return result, file_index, "llm"

                except Exception as e:
                    # ADR-3: Fallback to text chunking when LLM fails
                    logger.warning(
                        f"LLM failed for {file.file_path}, falling back to text chunker: {e}"
                    )

                    try:
                        chunks = self._text_chunker.chunk(
                            content=file.content,
                            file_path=file.file_path,
                            language=file.language or "unknown",
                        )

                        if progress_callback:
                            progress_callback(file_index, total)

                        logger.info(
                            f"Text chunker created {len(chunks)} chunks for {file.file_path}"
                        )
                        return chunks, file_index, "fallback"

                    except Exception as fallback_error:
                        logger.error(
                            f"Both LLM and fallback failed for {file.file_path}: {fallback_error}"
                        )
                        raise

    async def _process_single_file(
        self,
        file: CodeFile,
        gemini_client: GeminiCodeClient,
        semaphore: asyncio.Semaphore,
        file_index: int,
        total: int,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> Union[CodeAnalysisResult, list[TextChunk], None]:
        """Process a single file with rate limiting.

        ADR-3 COMPLIANCE: Falls back to text chunking when LLM fails.

        Args:
            file: Code file to process
            gemini_client: Gemini client
            semaphore: Concurrency limiter
            file_index: Current file index (1-based)
            total: Total files count
            progress_callback: Progress callback

        Returns:
            Analysis result (CodeAnalysisResult for LLM, list[TextChunk] for fallback)
        """
        async with semaphore:
            async with self.rate_limiter:
                logger.debug(
                    f"Processing [{file_index}/{total}] {file.file_path}"
                )

                try:
                    result = await gemini_client.analyze_code(
                        file_path=file.file_path,
                        language=file.language,
                        source_code=file.content,
                    )

                    if progress_callback:
                        progress_callback(file_index, total)

                    return result

                except Exception as e:
                    # ADR-3: Fallback to text chunking when LLM fails
                    logger.warning(
                        f"LLM failed for {file.file_path}, falling back to text chunker: {e}"
                    )

                    try:
                        chunks = self._text_chunker.chunk(
                            content=file.content,
                            file_path=file.file_path,
                            language=file.language or "unknown",
                        )

                        if progress_callback:
                            progress_callback(file_index, total)

                        logger.info(
                            f"Text chunker created {len(chunks)} chunks for {file.file_path}"
                        )
                        return chunks

                    except Exception as fallback_error:
                        logger.error(
                            f"Both LLM and fallback failed for {file.file_path}: {fallback_error}"
                        )
                        raise

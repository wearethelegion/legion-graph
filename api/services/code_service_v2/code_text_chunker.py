"""Text-based code chunker for LLM fallback.

When Gemini LLM analysis fails, this chunker provides basic
line-based chunking so files can still be indexed and searched.
"""

from dataclasses import dataclass, field
from typing import List, Optional
import hashlib


@dataclass
class TextChunk:
    """A text-based chunk of code.

    Used when LLM analysis fails as fallback for searchability.
    """
    chunk_id: str
    file_path: str
    language: str
    content: str
    start_line: int
    end_line: int
    chunk_index: int
    total_chunks: int
    is_llm_analyzed: bool = False  # Always False for fallback chunks


@dataclass
class ChunkMetadata:
    """Metadata for text chunking."""
    file_path: str
    language: str
    total_lines: int
    total_chunks: int


class CodeTextChunker:
    """Line-based text chunker for code files.

    Used as fallback when LLM analysis fails. Produces chunks
    that can be stored in Qdrant for vector search (but NOT
    in Neo4j since we have no entity/relationship data).
    """

    DEFAULT_CHUNK_SIZE = 100  # lines per chunk
    DEFAULT_OVERLAP = 10  # lines of overlap between chunks

    def __init__(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        overlap: int = DEFAULT_OVERLAP,
    ):
        """Initialize chunker.

        Args:
            chunk_size: Number of lines per chunk
            overlap: Number of overlapping lines between chunks
        """
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk(
        self,
        content: str,
        file_path: str,
        language: str,
    ) -> List[TextChunk]:
        """Chunk code content into line-based segments.

        Args:
            content: Full source code content
            file_path: Path to the file (for metadata)
            language: Programming language

        Returns:
            List of TextChunk objects
        """
        if not content or not content.strip():
            return []

        lines = content.splitlines(keepends=True)
        total_lines = len(lines)

        if total_lines == 0:
            return []

        chunks: List[TextChunk] = []
        step = max(1, self.chunk_size - self.overlap)

        # Calculate total chunks for metadata
        total_chunks = max(1, (total_lines + step - 1) // step)

        chunk_index = 0
        start = 0

        while start < total_lines:
            end = min(start + self.chunk_size, total_lines)
            chunk_lines = lines[start:end]
            chunk_content = "".join(chunk_lines)

            # Generate deterministic chunk ID
            chunk_id = self._generate_chunk_id(file_path, chunk_index)

            chunks.append(TextChunk(
                chunk_id=chunk_id,
                file_path=file_path,
                language=language,
                content=chunk_content,
                start_line=start + 1,  # 1-indexed
                end_line=end,  # inclusive
                chunk_index=chunk_index,
                total_chunks=total_chunks,
                is_llm_analyzed=False,
            ))

            chunk_index += 1
            start += step

            # Safety: prevent infinite loop
            if start >= total_lines and end >= total_lines:
                break

        # Update total_chunks in all chunks (in case calculation changed)
        actual_total = len(chunks)
        for chunk in chunks:
            chunk.total_chunks = actual_total

        return chunks

    def _generate_chunk_id(self, file_path: str, chunk_index: int) -> str:
        """Generate deterministic chunk ID.

        Args:
            file_path: File path
            chunk_index: Index of chunk in file

        Returns:
            Unique chunk ID string
        """
        key = f"{file_path}::{chunk_index}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]

    def get_metadata(
        self,
        content: str,
        file_path: str,
        language: str,
    ) -> ChunkMetadata:
        """Get chunking metadata without actually chunking.

        Useful for pre-flight checks and progress estimation.

        Args:
            content: Full source code content
            file_path: Path to the file
            language: Programming language

        Returns:
            ChunkMetadata with line and chunk counts
        """
        lines = content.splitlines() if content else []
        total_lines = len(lines)
        step = max(1, self.chunk_size - self.overlap)
        total_chunks = max(1, (total_lines + step - 1) // step) if total_lines > 0 else 0

        return ChunkMetadata(
            file_path=file_path,
            language=language,
            total_lines=total_lines,
            total_chunks=total_chunks,
        )

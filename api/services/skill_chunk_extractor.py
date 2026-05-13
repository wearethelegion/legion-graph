"""Skill chunk extraction service using Gemini for intelligent semantic chunking.

This service extracts semantic chunks from skill package files using Gemini LLM.
It works with any file type (markdown, python, yaml, json, etc.) and extracts
rich metadata including section titles, chunk types, summaries, key concepts,
dependencies, and file references.

Example usage:
    ```python
    from api.services import get_skill_chunk_extractor

    # Initialize extractor
    extractor = get_skill_chunk_extractor()

    # Extract chunks from a file
    with open("SKILL.md") as f:
        content = f.read()

    result = await extractor.extract_chunks_from_file("SKILL.md", content)

    # Access extracted chunks
    for chunk in result.chunks:
        print(f"Section: {chunk['section_title']}")
        print(f"Type: {chunk['chunk_type']}")
        print(f"Summary: {chunk['summary']}")
        print(f"Key concepts: {chunk['key_concepts']}")
    ```

The extractor automatically:
- Detects file types
- Extracts semantic boundaries
- Generates rich metadata
- Falls back to single chunk on errors
"""

from typing import List, Dict, Any, Optional
import json
import asyncio
from loguru import logger
import google.generativeai as genai

from kgrag.config import config


class ChunkExtractionResult:
    """Result of chunk extraction from a file."""

    def __init__(
        self,
        file_path: str,
        file_type: str,
        chunks: List[Dict[str, Any]]
    ):
        self.file_path = file_path
        self.file_type = file_type
        self.chunks = chunks

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "file_path": self.file_path,
            "file_type": self.file_type,
            "chunks": self.chunks,
            "chunk_count": len(self.chunks)
        }


class SkillChunkExtractor:
    """Service for extracting semantic chunks from skill files using Gemini."""

    def __init__(self, model: str = "gemini-2.5-flash"):
        """
        Initialize chunk extractor.

        Args:
            model: Gemini model to use for extraction (flash for cost efficiency)
        """
        self.model_name = model

        # Configure and create Gemini model
        genai.configure(api_key=config.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(model)

        logger.info(f"Initialized SkillChunkExtractor with model: {model}")

    async def extract_chunks_from_file(
        self,
        file_path: str,
        file_content: str
    ) -> ChunkExtractionResult:
        """
        Extract semantic chunks from a single file using Gemini.

        Args:
            file_path: Path to file (e.g., "SKILL.md", "references/QDRANT.md")
            file_content: File content as string

        Returns:
            ChunkExtractionResult with extracted chunks
        """
        if not file_content or not file_content.strip():
            logger.warning("Empty content for {}, returning empty result", file_path)
            return ChunkExtractionResult(file_path, self._get_file_type(file_path), [])

        file_type = self._get_file_type(file_path)

        try:
            chunks = await self._call_gemini_for_chunks(file_path, file_content)
            logger.info(f"Extracted {len(chunks)} chunks from {file_path}")
            return ChunkExtractionResult(file_path, file_type, chunks)
        except Exception as e:
            logger.error("Failed to extract chunks from {}: {}", file_path, e)
            # Fallback: create single chunk with entire content
            return self._create_fallback_chunk(file_path, file_type, file_content)

    async def _call_gemini_for_chunks(
        self,
        file_path: str,
        file_content: str
    ) -> List[Dict[str, Any]]:
        """
        Call Gemini API to extract chunks.

        Uses generic semantic prompt that works for any file type.

        Args:
            file_path: Path to the file
            file_content: Content of the file

        Returns:
            List of chunk dictionaries

        Raises:
            Exception: If API call fails or response parsing fails
        """
        prompt = self._build_extraction_prompt(file_path, file_content)

        # Use async wrapper around sync API (more reliable than generate_content_async)
        response = await asyncio.wait_for(
            asyncio.to_thread(
                self.model.generate_content,
                prompt,
                generation_config=genai.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=8192
                )
            ),
            timeout=60.0  # 60 second timeout
        )

        # Parse JSON response
        response_text = response.text.strip()

        # Handle markdown code blocks
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]

        response_text = response_text.strip()

        # Try to parse JSON
        try:
            chunks_data = json.loads(response_text)
        except json.JSONDecodeError as e:
            logger.error("JSON parsing error: {}", e)
            logger.debug(f"Raw response text: {response_text[:500]}...")
            raise

        # Validate and normalize chunks
        return self._normalize_chunks(chunks_data)

    def _build_extraction_prompt(self, file_path: str, file_content: str) -> str:
        """Build the generic extraction prompt for Gemini."""
        return f"""Extract semantic chunks from this file for an AI knowledge graph.

File: {file_path}

Content:
---
{file_content}
---

TASK: Break this content into meaningful, self-contained chunks.

Guidelines:
- Each chunk = ONE coherent concept/procedure/example
- Chunks must be independently understandable
- Include enough context for clarity
- Use semantic boundaries (not arbitrary splits)

For EACH chunk, output this EXACT structure:
{{
  "content": "the actual text/code",
  "section_title": "descriptive title",
  "chunk_type": "concept|procedure|example|configuration|prerequisite|code|explanation",
  "summary": "one sentence description",
  "key_concepts": ["concept1", "concept2"],
  "dependencies": ["Tool X", "Library Y"],
  "file_references": ["related/file.md"]
}}

CRITICAL: Return ONLY a valid JSON array. No markdown, no explanation, no extra text.
Format: [{{...}}, {{...}}]

Ensure all strings are properly escaped and valid JSON."""

    def _normalize_chunks(self, chunks_data: Any) -> List[Dict[str, Any]]:
        """
        Normalize and validate chunk data from Gemini response.

        Args:
            chunks_data: Raw chunk data from Gemini (dict or list)

        Returns:
            List of normalized chunk dictionaries
        """
        if not isinstance(chunks_data, list):
            chunks_data = [chunks_data]

        normalized = []
        for idx, chunk in enumerate(chunks_data):
            if not isinstance(chunk, dict):
                logger.warning("Invalid chunk at index {}: not a dict", idx)
                continue

            normalized_chunk = {
                "content": str(chunk.get("content", "")),
                "section_title": str(chunk.get("section_title", f"Section {idx + 1}")),
                "chunk_type": str(chunk.get("chunk_type", "explanation")),
                "summary": str(chunk.get("summary", "")),
                "key_concepts": chunk.get("key_concepts", []),
                "dependencies": chunk.get("dependencies", []),
                "file_references": chunk.get("file_references", []),
            }

            # Ensure lists
            for key in ["key_concepts", "dependencies", "file_references"]:
                if not isinstance(normalized_chunk[key], list):
                    normalized_chunk[key] = []

            # Skip chunks with empty content
            if not normalized_chunk["content"].strip():
                logger.warning("Skipping chunk {} with empty content", idx + 1)
                continue

            normalized.append(normalized_chunk)

        return normalized

    def _get_file_type(self, file_path: str) -> str:
        """
        Determine file type from path.

        Args:
            file_path: Path to the file

        Returns:
            File type string
        """
        if file_path.endswith(".md"):
            return "markdown"
        elif file_path.endswith(".py"):
            return "python"
        elif file_path.endswith(".sh"):
            return "shell"
        elif file_path.endswith((".yaml", ".yml")):
            return "yaml"
        elif file_path.endswith(".json"):
            return "json"
        elif file_path.endswith(".txt"):
            return "text"
        else:
            return "text"

    def _create_fallback_chunk(
        self,
        file_path: str,
        file_type: str,
        file_content: str
    ) -> ChunkExtractionResult:
        """
        Create fallback single chunk if Gemini extraction fails.

        Args:
            file_path: Path to the file
            file_type: Type of the file
            file_content: Content of the file

        Returns:
            ChunkExtractionResult with single fallback chunk
        """
        fallback_chunk = {
            "content": file_content,
            "section_title": f"Content from {file_path}",
            "chunk_type": "explanation",
            "summary": f"Complete content of {file_path}",
            "key_concepts": [],
            "dependencies": [],
            "file_references": [],
        }

        logger.warning("Using fallback single chunk for {}", file_path)
        return ChunkExtractionResult(file_path, file_type, [fallback_chunk])


# Factory function for getting chunk extractor
def get_skill_chunk_extractor(model: str = "gemini-2.5-flash") -> SkillChunkExtractor:
    """
    Get a SkillChunkExtractor instance.

    Args:
        model: Gemini model to use

    Returns:
        SkillChunkExtractor instance
    """
    return SkillChunkExtractor(model=model)

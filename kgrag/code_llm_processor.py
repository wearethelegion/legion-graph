"""
Simple LLM processing for code files.
Uses Gemini for chunking, entity extraction, title/summary generation.
"""

from typing import List, Dict, Any, Optional
import json
import asyncio
from loguru import logger
import google.generativeai as genai
from aiolimiter import AsyncLimiter

from kgrag.config import config
from kgrag.prompts.code_processing import (
    CODE_CHUNKING_PROMPT,
    CODE_ENTITY_EXTRACTION_PROMPT,
    CODE_TITLE_GENERATION_PROMPT,
    CODE_SUMMARY_GENERATION_PROMPT
)

# Rate limiter for Gemini API (10 requests per second for code processing)
_rate_limiter = AsyncLimiter(max_rate=10, time_period=1.0)
_request_timeout = 300.0  # 5 minutes - entity extraction can take longer for large code files


class CodeLLMProcessor:
    """Simple LLM processor for code files."""

    # Known language extensions (for better prompts)
    LANGUAGE_EXTENSIONS = {
        ".py": "python",
        ".rb": "ruby",
        ".ts": "typescript",
        ".tsx": "typescript",
        ".js": "javascript",
        ".jsx": "javascript",
        ".go": "go",
        ".java": "java",
        ".rs": "rust",
        ".sh": "bash",
        ".bash": "bash",
        ".zsh": "bash",
        ".sql": "sql",
        ".kt": "kotlin",
        ".swift": "swift",
        ".c": "c",
        ".cpp": "cpp",
        ".h": "cpp",
        ".hpp": "cpp",
        ".cs": "csharp",
        ".php": "php",
        ".scala": "scala",
        ".r": "r",
        ".m": "matlab",
        ".lua": "lua",
        ".pl": "perl",
        ".pm": "perl",
        ".ex": "elixir",
        ".exs": "elixir",
        ".erl": "erlang",
        ".hrl": "erlang",
        ".hs": "haskell",
        ".ml": "ocaml",
        ".fs": "fsharp",
        ".clj": "clojure",
        ".groovy": "groovy",
        ".gradle": "groovy",
        # Infrastructure as Code
        ".tf": "terraform",
        ".tfvars": "terraform",
        ".hcl": "hcl",
        # Config/Data formats (processable)
        ".yaml": "yaml",
        ".yml": "yaml",
        ".json": "json",
        ".xml": "xml",
        ".toml": "toml",
        ".ini": "ini",
        ".cfg": "config",
        ".conf": "config",
        ".env": "dotenv",
        # Web
        ".html": "html",
        ".htm": "html",
        ".css": "css",
        ".scss": "scss",
        ".sass": "sass",
        ".less": "less",
        ".vue": "vue",
        ".svelte": "svelte",
        # Schema/API
        ".proto": "protobuf",
        ".graphql": "graphql",
        ".gql": "graphql",
        # Build/Make
        ".dockerfile": "dockerfile",
        ".makefile": "makefile",
        ".cmake": "cmake",
    }

    # Excluded extensions (blocklist - don't process these)
    EXCLUDED_EXTENSIONS = {
        # Documentation
        ".md", ".rst", ".txt", ".pdf", ".doc", ".docx", ".rtf",
        # Spreadsheets
        ".xlsx", ".xls", ".csv", ".ods",
        # Images
        ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".bmp", ".webp", ".tiff",
        # Audio/Video
        ".mp3", ".mp4", ".wav", ".avi", ".mov", ".mkv", ".webm",
        # Fonts
        ".ttf", ".otf", ".woff", ".woff2", ".eot",
        # Binaries/Executables
        ".exe", ".dll", ".so", ".dylib", ".bin", ".o", ".a", ".lib",
        # Archives
        ".zip", ".tar", ".gz", ".rar", ".7z", ".bz2", ".xz",
        # Lock files
        ".lock",
        # Minified files
        ".min.js", ".min.css",
        # Cache/temp
        ".pyc", ".pyo", ".class", ".jar",
        # Data files
        ".db", ".sqlite", ".sqlite3",
        # Certificates/Keys (security)
        ".pem", ".crt", ".key", ".p12", ".pfx",
        # Logs
        ".log",
    }

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize processor with Gemini API.

        Args:
            api_key: Gemini API key (defaults to config)
        """
        self.api_key = api_key or config.GEMINI_API_KEY
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel("gemini-2.5-flash")
        logger.info("Initialized CodeLLMProcessor with Gemini 2.5 Flash")

    def _detect_language(self, filename: str) -> str:
        """
        Detect programming language from file extension.

        Uses blocklist approach: returns 'unknown' for files not in
        LANGUAGE_EXTENSIONS but not in EXCLUDED_EXTENSIONS.
        LLM will figure out what they are.

        Args:
            filename: Name of the code file

        Returns:
            Language name (python, ruby, terraform, etc.) or "unknown"
        """
        import os
        _, ext = os.path.splitext(filename.lower())
        
        # Check exclusion list first
        if ext in self.EXCLUDED_EXTENSIONS:
            logger.debug(f"File '{filename}' has excluded extension: {ext}")
            return "excluded"
        
        # Check known languages
        language = self.LANGUAGE_EXTENSIONS.get(ext, "unknown")
        logger.debug(f"Detected language '{language}' for file '{filename}' (extension: {ext})")
        return language

    def is_excluded(self, filename: str) -> bool:
        """Check if file should be excluded from processing."""
        import os
        _, ext = os.path.splitext(filename.lower())
        return ext in self.EXCLUDED_EXTENSIONS

    async def process_code(
        self,
        code: str,
        filename: str,
        language: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process code file: chunk, extract entities, generate title/summary.

        Args:
            code: Source code content
            filename: Name of the code file
            language: Programming language (auto-detected if not provided)

        Returns:
            Dict with:
                - title: Generated title
                - summary: Brief summary
                - chunks: List of code chunks with metadata
                - entities: Extracted code entities
                - relationships: Entity relationships
        """
        try:
            # Auto-detect language if not provided
            if not language:
                language = self._detect_language(filename)

            logger.info(f"Processing code file '{filename}': {len(code)} chars, language={language}")

            # Run all LLM operations in parallel
            title_task = self._generate_title(code, filename)
            summary_task = self._generate_summary(code, filename)
            chunks_task = self._chunk_code(code, language)
            entities_task = self._extract_code_entities(code, filename, language)

            title, summary, chunks, entity_data = await asyncio.gather(
                title_task,
                summary_task,
                chunks_task,
                entities_task
            )

            logger.info(
                f"Code processed: {len(chunks)} chunks, "
                f"{len(entity_data['entities'])} entities, "
                f"{len(entity_data['relationships'])} relationships"
            )

            return {
                "title": title,
                "summary": summary,
                "chunks": chunks,
                "entities": entity_data["entities"],
                "relationships": entity_data["relationships"]
            }

        except Exception as e:
            logger.error(f"Code processing failed for '{filename}': {e}", exc_info=True)
            raise

    async def _generate_title(self, code: str, filename: str) -> str:
        """Generate concise title for code file."""
        async with _rate_limiter:
            try:
                # Use first 2000 chars for title generation
                prompt = CODE_TITLE_GENERATION_PROMPT.format(
                    filename=filename,
                    code=code[:2000]
                )

                response = await asyncio.wait_for(
                    asyncio.to_thread(
                        self.model.generate_content,
                        prompt
                    ),
                    timeout=_request_timeout
                )

                title = response.text.strip()
                # Remove quotes if present
                if title.startswith('"') and title.endswith('"'):
                    title = title[1:-1]
                return title[:200]  # Max 200 chars

            except Exception as e:
                logger.warning(f"Title generation failed for '{filename}': {e}")
                return f"Code: {filename}"

    async def _generate_summary(self, code: str, filename: str) -> str:
        """Generate brief summary for code file."""
        async with _rate_limiter:
            try:
                # Use first 3000 chars for summary generation
                prompt = CODE_SUMMARY_GENERATION_PROMPT.format(
                    filename=filename,
                    code=code[:3000]
                )

                response = await asyncio.wait_for(
                    asyncio.to_thread(
                        self.model.generate_content,
                        prompt
                    ),
                    timeout=_request_timeout
                )

                return response.text.strip()

            except Exception as e:
                logger.warning(f"Summary generation failed for '{filename}': {e}")
                return ""

    async def _chunk_code(self, code: str, language: str) -> List[Dict[str, Any]]:
        """
        Split code into logical chunks (functions, classes, sections).

        Args:
            code: Source code content
            language: Programming language

        Returns:
            List of chunks with metadata:
                - content: Chunk code
                - summary: Brief summary
                - chunk_type: class/function/config/test/utility
                - complexity: low/medium/high
                - entry_point: boolean
                - keywords: list of key terms
                - function_signature: signature if function
                - class_name: class name if method
                - decorators: list of decorators
        """
        async with _rate_limiter:
            try:
                prompt = CODE_CHUNKING_PROMPT.format(
                    language=language,
                    code=code
                )

                response = await asyncio.wait_for(
                    asyncio.to_thread(
                        self.model.generate_content,
                        prompt
                    ),
                    timeout=_request_timeout
                )

                # Parse JSON response
                response_text = response.text.strip()
                # Remove markdown code blocks if present
                if response_text.startswith("```"):
                    lines = response_text.split("\n")
                    response_text = "\n".join(lines[1:-1])

                chunks = json.loads(response_text)

                # Validate structure
                if not isinstance(chunks, list):
                    raise ValueError("Expected JSON array")

                # Ensure all required fields exist with defaults
                for i, chunk in enumerate(chunks):
                    chunk.setdefault("content", "")
                    chunk.setdefault("summary", f"Code chunk {i+1}")
                    chunk.setdefault("chunk_type", "utility")
                    chunk.setdefault("complexity", "medium")
                    chunk.setdefault("entry_point", False)
                    chunk.setdefault("keywords", [])
                    chunk.setdefault("function_signature", None)
                    chunk.setdefault("class_name", None)
                    chunk.setdefault("decorators", [])

                logger.info(f"Created {len(chunks)} code chunks with metadata")
                return chunks

            except Exception as e:
                logger.warning(f"Code chunking failed: {e}, using single chunk fallback")
                # Fallback: return entire code as single chunk
                return [
                    {
                        "content": code,
                        "summary": "Complete code file",
                        "chunk_type": "utility",
                        "complexity": "medium",
                        "entry_point": False,
                        "keywords": [],
                        "function_signature": None,
                        "class_name": None,
                        "decorators": []
                    }
                ]

    async def _extract_code_entities(
        self,
        code: str,
        filename: str,
        language: str
    ) -> Dict[str, Any]:
        """
        Extract code entities (classes, functions, patterns) and relationships.

        Args:
            code: Source code content
            filename: Name of the code file
            language: Programming language

        Returns:
            Dict with:
                - entities: [{name, type, description, attributes, confidence}]
                - relationships: [{source, target, type, context, confidence}]
        """
        async with _rate_limiter:
            try:
                # Use first 4000 chars for entity extraction
                prompt = CODE_ENTITY_EXTRACTION_PROMPT.format(
                    filename=filename,
                    language=language,
                    code=code[:4000]
                )
                logger.debug(f"Entity extraction prompt length: {len(prompt)} chars")

                response = await asyncio.wait_for(
                    asyncio.to_thread(
                        self.model.generate_content,
                        prompt
                    ),
                    timeout=_request_timeout
                )

                # Parse JSON response
                response_text = response.text.strip()
                logger.debug(f"Entity extraction response length: {len(response_text)} chars")
                # Remove markdown code blocks if present
                if response_text.startswith("```"):
                    lines = response_text.split("\n")
                    response_text = "\n".join(lines[1:-1])

                data = json.loads(response_text)

                # Validate structure
                if not isinstance(data.get("entities"), list):
                    data["entities"] = []
                if not isinstance(data.get("relationships"), list):
                    data["relationships"] = []

                # Filter by confidence threshold
                min_confidence = 0.5  # Entities >= 0.5

                # Process entities
                filtered_entities = []
                for entity in data["entities"]:
                    confidence = entity.get("confidence", 1.0)
                    if confidence >= min_confidence:
                        # Ensure required fields
                        entity.setdefault("description", "")
                        entity.setdefault("attributes", {})
                        entity.setdefault("confidence", confidence)
                        filtered_entities.append(entity)

                data["entities"] = filtered_entities

                # Process relationships
                allowed_rel_types = {
                    "INHERITS", "IMPLEMENTS", "CALLS", "USES",
                    "DEPENDS_ON", "EXPOSES", "FOLLOWS"
                }

                filtered_relationships = []
                for rel in data["relationships"]:
                    # Sanitize relationship type (prevent Cypher injection)
                    if rel.get("type") not in allowed_rel_types:
                        rel["type"] = "USES"

                    # Check confidence (lower threshold for relationships)
                    confidence = rel.get("confidence", 1.0)
                    if confidence >= 0.4:
                        # Ensure required fields
                        rel.setdefault("context", "")
                        rel.setdefault("confidence", confidence)
                        filtered_relationships.append(rel)

                data["relationships"] = filtered_relationships

                logger.info(
                    f"Extracted {len(data['entities'])} entities "
                    f"(min_confidence={min_confidence:.2f}), "
                    f"{len(data['relationships'])} relationships"
                )

                return data

            except Exception as e:
                logger.error(f"Entity extraction failed: {e}", exc_info=True)
                logger.error(
                    f"Response text (first 500 chars): "
                    f"{response_text[:500] if 'response_text' in locals() else 'N/A'}"
                )
                return {"entities": [], "relationships": []}
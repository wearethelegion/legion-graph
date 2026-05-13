"""
ChonkieChunker — Semantic & code-aware chunking for Cognee.

Uses Chonkie's SemanticChunker (with Gemini embeddings) for text/prose and
CodeChunker (AST-based, auto language detection via Magika) for source code.

Configuration:
    CHONKIE_CHUNK_SIZE  — max characters per chunk (default 512)
    GEMINI_API_KEY or LLM_API_KEY — API key for Gemini embeddings
"""

import os
import threading
from pathlib import Path
from uuid import uuid5, NAMESPACE_OID

import structlog

from cognee.modules.chunking.Chunker import Chunker
from cognee.modules.chunking.models.DocumentChunk import DocumentChunk

logger = structlog.get_logger(__name__)

# ── Extension → tree-sitter language name mapping ────────────────────────────
_EXT_TO_TREESITTER = {
    "ts": "typescript",
    "tsx": "tsx",
    "mts": "typescript",
    "cts": "typescript",
    "js": "javascript",
    "jsx": "javascript",
    "mjs": "javascript",
    "cjs": "javascript",
    "py": "python",
    "pyw": "python",
    "pyi": "python",
    "rb": "ruby",
    "rake": "ruby",
    "java": "java",
    "kt": "kotlin",
    "kts": "kotlin",
    "swift": "swift",
    "go": "go",
    "rs": "rust",
    "cs": "c_sharp",
    "cpp": "cpp",
    "cc": "cpp",
    "cxx": "cpp",
    "c": "c",
    "h": "c",
    "hpp": "cpp",
    "php": "php",
    "scala": "scala",
    "dart": "dart",
    "lua": "lua",
    "r": "r",
    "sh": "bash",
    "bash": "bash",
    "zsh": "bash",
    "sql": "sql",
    "html": "html",
    "htm": "html",
    "css": "css",
    "scss": "scss",
    "vue": "vue",
    "svelte": "svelte",
    "yml": "yaml",
    "yaml": "yaml",
    "json": "json",
    "xml": "xml",
    "toml": "toml",
    "proto": "proto",
    "ex": "elixir",
    "exs": "elixir",
    "erl": "erlang",
    "hs": "haskell",
    "clj": "clojure",
    "cljs": "clojure",
    "pl": "perl",
    "groovy": "groovy",
}

# ── Extensions known to be prose/documents (NOT code) ────────────────────────
_TEXT_EXTENSIONS = frozenset(
    {
        "txt",
        "text",
        "pdf",
        "doc",
        "docx",
        "rtf",
        "odt",
        "md",
        "markdown",
        "rst",
        "adoc",
        "asciidoc",
        "csv",
        "tsv",
        "log",
        "epub",
        "mobi",
        "htm",
        "html",  # treat HTML as text for semantic chunking
    }
)

# ── Cached SemanticChunker singleton ─────────────────────────────────────────
_semantic_chunker = None
_semantic_chunker_key = None  # (chunk_size, threshold) tuple for cache invalidation
_chunker_lock = threading.Lock()


def _get_semantic_chunker(chunk_size: int, threshold: float = 0.7):
    """Return a cached SemanticChunker instance (thread-safe singleton).

    Creating GeminiEmbeddings + SemanticChunker is expensive (API handshake,
    model validation). Cache at module level keyed by (chunk_size, threshold).
    """
    global _semantic_chunker, _semantic_chunker_key

    key = (chunk_size, threshold)
    if _semantic_chunker is not None and _semantic_chunker_key == key:
        return _semantic_chunker

    with _chunker_lock:
        # Double-check after acquiring lock
        if _semantic_chunker is not None and _semantic_chunker_key == key:
            return _semantic_chunker

        from chonkie import SemanticChunker
        from chonkie.embeddings import GeminiEmbeddings
        from chonkie.tokenizer import AutoTokenizer

        api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("LLM_API_KEY")
        if not api_key:
            raise RuntimeError("SemanticChunker requires GEMINI_API_KEY or LLM_API_KEY env var")

        embedding_model = os.environ.get("CHONKIE_EMBEDDING_MODEL") or os.environ.get(
            "EMBEDDING_MODEL", "gemini-embedding-002"
        )
        logger.info(
            "chonkie.semantic_chunker.init",
            chunk_size=chunk_size,
            threshold=threshold,
            model=embedding_model,
        )
        embeddings = GeminiEmbeddings(
            model=embedding_model,
            api_key=api_key,
        )
        # Workaround: Chonkie 1.6.1 CatsuTokenizerWrapper isn't recognized by
        # AutoTokenizer. Override get_tokenizer to return a character tokenizer
        # since our chunk_size is in characters anyway.
        embeddings.get_tokenizer = lambda: AutoTokenizer("character")

        _semantic_chunker = SemanticChunker(
            embedding_model=embeddings,
            chunk_size=chunk_size,
            threshold=threshold,
        )
        _semantic_chunker_key = key
        logger.info("chonkie.semantic_chunker.ready")
        return _semantic_chunker


def _is_code(document) -> bool:
    """Return True if the document is likely source code, False for prose/text."""
    name = getattr(document, "name", "") or ""
    ext = Path(name).suffix.lstrip(".").lower()

    # If no extension, check basename (Dockerfile, Makefile, etc.)
    if not ext:
        ext = Path(name).stem.lower()

    if ext in _TEXT_EXTENSIONS:
        return False

    # If there's an extension and it's not in the text set, assume code
    if ext:
        return True

    # No extension at all — check MIME type
    mime = getattr(document, "mime_type", "") or ""
    if any(
        mime.startswith(p)
        for p in (
            "text/x-python",
            "text/x-java",
            "text/x-c",
            "text/x-go",
            "text/x-rust",
            "text/x-ruby",
            "text/x-script",
            "text/x-shellscript",
            "text/javascript",
            "application/javascript",
            "application/typescript",
        )
    ):
        return True

    # Default: treat as text
    return False


class ChonkieChunker(Chunker):
    """Cognee-compatible chunker backed by Chonkie.

    Inherits from ``cognee.modules.chunking.Chunker`` and implements ``read()``
    as an async generator yielding ``DocumentChunk`` objects.

    Behaviour:
    * Source code → ``CodeChunker`` (AST-aware, language="auto" via Magika)
    * Text/prose → ``SemanticChunker`` (embedding-based boundary detection)
    """

    async def read(self):
        """Async generator: collect text → chunk via Chonkie → yield DocumentChunks."""
        doc_name = getattr(self.document, "name", "unknown")
        doc_id = getattr(self.document, "id", "unknown")

        # ── 1. Gather all text from the async get_text() generator ───────
        parts: list[str] = []
        async for text_part in self.get_text():
            parts.append(text_part)
        full_text = "\n".join(parts)

        if not full_text.strip():
            logger.debug("chunker.empty_text", document=doc_name, document_id=str(doc_id))
            return

        text_chars = len(full_text)
        logger.debug(
            "chunker.text_collected",
            document=doc_name,
            document_id=str(doc_id),
            text_chars=text_chars,
            parts_count=len(parts),
        )

        # ── 2. Determine chunk size (ignore Cognee's max_chunk_size) ─────
        env_size = os.environ.get("CHONKIE_CHUNK_SIZE")
        chunk_size = int(env_size) if env_size else 512

        # ── 3. Pick chunker based on content type ────────────────────────
        is_code_doc = _is_code(self.document)

        if is_code_doc:
            # Derive language from file extension for tree-sitter
            doc_ext = Path(doc_name).suffix.lstrip(".").lower()
            language = _EXT_TO_TREESITTER.get(doc_ext, "auto")
            logger.debug(
                "chunker.using_code_chunker",
                document=doc_name,
                chunk_size=chunk_size,
                language=language,
            )
            chunks = _chunk_code(full_text, chunk_size, language=language)
        else:
            logger.debug(
                "chunker.using_token_chunker",
                document=doc_name,
                chunk_size=chunk_size,
            )
            chunks = _chunk_token(full_text, chunk_size)

        # ── 4. Log chunk details ────────────────────────────────────────
        chunk_sizes = [len(c.text) for c in chunks if c.text.strip()]

        logger.debug(
            "chunker.chunks_produced",
            document=doc_name,
            total_chunks=len(chunk_sizes),
            min_chunk_chars=min(chunk_sizes) if chunk_sizes else 0,
            max_chunk_chars=max(chunk_sizes) if chunk_sizes else 0,
        )

        # ── 5. Yield DocumentChunk objects ───────────────────────────────
        yielded = 0
        for chunk in chunks:
            chunk_text = chunk.text
            if not chunk_text.strip():
                continue

            token_count = chunk.token_count if hasattr(chunk, "token_count") else len(chunk_text)

            yield DocumentChunk(
                id=uuid5(NAMESPACE_OID, f"{str(self.document.id)}-{self.chunk_index}"),
                text=chunk_text,
                chunk_size=token_count,
                is_part_of=self.document,
                chunk_index=self.chunk_index,
                cut_type="code_ast" if is_code_doc else "semantic",
                contains=[],
                metadata={"index_fields": ["text"]},
            )
            self.chunk_index += 1
            yielded += 1

        logger.debug(
            "chunker.yield_complete",
            document=doc_name,
            yielded_chunks=yielded,
        )


# ── Module-level helpers ─────────────────────────────────────────────────────


def _chunk_code(text: str, chunk_size: int, language: str = "auto"):
    """Use Chonkie CodeChunker with language detection."""
    from chonkie import CodeChunker

    try:
        logger.debug(
            "chonkie.code_chunker.init",
            language=language,
            chunk_size=chunk_size,
            text_chars=len(text),
        )
        chunker = CodeChunker(
            language=language,
            tokenizer="character",
            chunk_size=chunk_size,
        )
        result = chunker.chunk(text)
        logger.info(
            "chonkie.code_chunker.complete",
            chunks=len(result),
            chunk_size=chunk_size,
        )
        return result
    except Exception as exc:
        logger.warning(
            "chonkie.code_chunker.fallback",
            error=str(exc),
            fallback_to="TokenChunker",
        )
        return _chunk_token(text, chunk_size)


def _chunk_semantic(text: str, chunk_size: int):
    """Use cached SemanticChunker for embedding-based boundary detection."""
    chunker = _get_semantic_chunker(chunk_size, threshold=0.7)
    logger.debug(
        "chonkie.semantic_chunker.chunk",
        chunk_size=chunk_size,
        text_chars=len(text),
    )
    result = chunker.chunk(text)
    logger.info(
        "chonkie.semantic_chunker.complete",
        chunks=len(result),
        chunk_size=chunk_size,
    )
    return result


def _chunk_token(text: str, chunk_size: int):
    """Simple token-based chunking. No API calls, no embeddings."""
    from chonkie import TokenChunker

    chunker = TokenChunker(
        tokenizer="character",
        chunk_size=chunk_size,
        chunk_overlap=100,
    )
    result = chunker.chunk(text)
    logger.info(
        "chonkie.token_chunker.complete",
        chunks=len(result),
        chunk_size=chunk_size,
    )
    return result

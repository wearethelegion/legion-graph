"""Shared fixtures for Summarization Service tests.

ALL external dependencies are mocked here:
- cognee.* (heavy imports, not available in test env)
- asyncpg (no real Postgres)
- aiokafka (no real Kafka)
- cognee_service.* (depends on cognee internals)
"""

import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── Pre-import mocking: block ALL cognee and heavy dependencies ──────
# These must be installed in sys.modules BEFORE any summarization_service
# module is imported, because processor.py does `from cognee.tasks...`
# at module level.


def _install_mock_modules() -> None:
    """Install mock modules for cognee and cognee_service dependencies.

    This prevents ImportError when summarization_service modules
    try to import cognee.* at the module level.
    """
    mock_modules = [
        # Cognee core
        "cognee",
        "cognee.tasks",
        "cognee.tasks.summarization",
        "cognee.infrastructure",
        "cognee.infrastructure.llm",
        "cognee.infrastructure.llm.extraction",
        "cognee.shared",
        "cognee.shared.data_models",
        "cognee.modules",
        "cognee.modules.chunking",
        "cognee.modules.chunking.models",
        "cognee.modules.chunking.models.DocumentChunk",
        "cognee.modules.data",
        "cognee.modules.data.processing",
        "cognee.modules.data.processing.document_types",
        # Cognee service internals
        "cognee_service",
        "cognee_service.kafka_consumer",
        "cognee_service.kafka_consumer.enriched_chunks",
        "cognee_service.kafka_consumer.enriched_chunks.models",
        "cognee_service.multi_tenancy",
        "cognee_service.config",
        "cognee_service.cognee_patches",
        # External libs that may not be installed in test env
        "structlog",
        "aiokafka",
    ]

    for mod_name in mock_modules:
        if mod_name not in sys.modules:
            mock_mod = ModuleType(mod_name)
            sys.modules[mod_name] = mock_mod

    # Provide real-ish DocumentChunk and Document classes for processor imports
    from pydantic import BaseModel as _BM
    from typing import Optional as _Opt, List as _Lst
    from uuid import UUID as _UUID, uuid4 as _uuid4

    class _StubDocument(_BM):
        id: _UUID = None
        name: str = ""
        raw_data_location: str = ""
        external_metadata: _Opt[str] = None
        mime_type: str = "text/plain"

        def model_post_init(self, __context):
            if self.id is None:
                self.id = _uuid4()

    class _StubDocumentChunk(_BM):
        id: _UUID = None
        text: str = ""
        chunk_size: int = 0
        chunk_index: int = 0
        cut_type: str = ""
        is_part_of: _Opt[_StubDocument] = None
        contains: _Opt[_Lst] = None

        def model_post_init(self, __context):
            if self.id is None:
                self.id = _uuid4()

    doc_chunk_mod = sys.modules["cognee.modules.chunking.models.DocumentChunk"]
    doc_chunk_mod.DocumentChunk = _StubDocumentChunk

    doc_type_mod = sys.modules["cognee.modules.data.processing.document_types"]
    doc_type_mod.Document = _StubDocument

    # Also set on sub-path modules if processor imports from .Document
    doc_mod = sys.modules.get("cognee.modules.data.processing.document_types.Document")
    if doc_mod is None:
        doc_mod = ModuleType("cognee.modules.data.processing.document_types.Document")
        sys.modules["cognee.modules.data.processing.document_types.Document"] = doc_mod
    doc_mod.Document = _StubDocument

    # Provide structlog with a usable get_logger
    structlog_mod = sys.modules["structlog"]
    mock_logger = MagicMock()
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()
    mock_logger.debug = MagicMock()
    structlog_mod.get_logger = MagicMock(return_value=mock_logger)

    # Provide summarize_text as an AsyncMock that returns TextSummary-like objects
    from types import SimpleNamespace
    from uuid import uuid4

    async def _mock_summarize_text(chunks, **kwargs):
        return [
            SimpleNamespace(
                id=uuid4(),
                text=f"Summary of: {getattr(c, 'text', str(c))[:50]}",
                made_from=c,
            )
            for c in chunks
        ]

    summarization_mod = sys.modules["cognee.tasks.summarization"]
    summarization_mod.summarize_text = _mock_summarize_text

    # Provide EnrichedChunkMessage from cognee_service
    from pydantic import BaseModel, Field, model_validator
    from typing import Optional, List

    class MockEnrichedChunkMessage(BaseModel):
        action: str = Field(default="process")
        company_id: str = Field(...)
        project_id: Optional[str] = Field(default=None)
        repository: str = Field(...)
        branch: str = Field(...)
        file_path: str = Field(...)
        ingestion_id: str = Field(...)
        file_version_id: str = Field(default="fv-001")
        chunk_id: Optional[str] = None
        parent_id: Optional[str] = None
        language: Optional[str] = None
        chunk_index: Optional[int] = None
        total_chunks: Optional[int] = None
        content: Optional[str] = None
        header: str = ""
        embedding: Optional[List[float]] = None
        file_skeleton: str = ""
        content_type: str = Field(default="code")
        entity_type: Optional[str] = None
        document_title: Optional[str] = None
        extraction_prompt: Optional[str] = None

        @model_validator(mode="after")
        def _validate_project_scope(self):
            if self.content_type == "code" and not self.project_id:
                raise ValueError("project_id is required for code chunks")
            if self.content_type == "document" and self.project_id is not None:
                raise ValueError("project_id must be absent for document chunks")
            return self

    enriched_models_mod = sys.modules["cognee_service.kafka_consumer.enriched_chunks.models"]
    enriched_models_mod.EnrichedChunkMessage = MockEnrichedChunkMessage

    # Provide multi-tenancy stubs
    multi_tenancy_mod = sys.modules["cognee_service.multi_tenancy"]
    multi_tenancy_mod.ensure_neo4j_database = AsyncMock()
    multi_tenancy_mod.set_company_context = MagicMock()

    # Provide aiokafka stubs
    aiokafka_mod = sys.modules["aiokafka"]
    aiokafka_mod.AIOKafkaConsumer = MagicMock
    aiokafka_mod.AIOKafkaProducer = MagicMock


# Install mocks before any test imports
_install_mock_modules()


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def mock_pool():
    """Create a mock asyncpg pool with all required methods."""
    pool = AsyncMock()
    # acquire returns an async context manager
    mock_conn = AsyncMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


@pytest.fixture
def sample_enriched_chunk_message():
    """Return a sample EnrichedChunkMessage for testing."""
    from cognee_service.kafka_consumer.enriched_chunks.models import EnrichedChunkMessage

    return EnrichedChunkMessage(
        action="process",
        company_id="comp-123",
        project_id="proj-456",
        file_version_id="fv-001",
        repository="my-repo",
        branch="main",
        file_path="src/main.py",
        ingestion_id="ing-789",
        chunk_id="chunk-001",
        parent_id="parent-001",
        language="python",
        chunk_index=0,
        total_chunks=5,
        content="def hello():\n    print('hello world')",
        header="# PROJECT: my-repo\n# FILE: src/main.py",
    )

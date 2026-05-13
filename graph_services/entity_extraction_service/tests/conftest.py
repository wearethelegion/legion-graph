"""Shared fixtures for Entity Extraction Service tests.

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
# These must be installed in sys.modules BEFORE any entity_extraction_service
# module is imported, because processor.py does `from cognee.infrastructure...`
# at module level.


def _install_mock_modules() -> None:
    """Install mock modules for cognee and cognee_service dependencies.

    This prevents ImportError when entity_extraction_service modules
    try to import cognee.* at the module level.
    """
    mock_modules = [
        # Cognee core
        "cognee",
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

    # Provide structlog with a usable get_logger
    structlog_mod = sys.modules["structlog"]
    mock_logger = MagicMock()
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()
    mock_logger.debug = MagicMock()
    structlog_mod.get_logger = MagicMock(return_value=mock_logger)

    # Provide extract_content_graph as an AsyncMock
    extraction_mod = sys.modules["cognee.infrastructure.llm.extraction"]
    extraction_mod.extract_content_graph = AsyncMock()

    # Provide KnowledgeGraph as a simple mock class
    data_models_mod = sys.modules["cognee.shared.data_models"]
    data_models_mod.KnowledgeGraph = type("KnowledgeGraph", (), {})

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
        document_slug: Optional[str] = None
        business_domains: Optional[List[dict]] = None
        technical_tags: Optional[List[str]] = None
        # Phase 3.1: inline extraction prompt (None = old-format message without the field)
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

"""Shared fixtures for Embedding Service tests.

ALL external dependencies are mocked here:
- cognee.* (heavy imports, not available in test env)
- asyncpg (no real Postgres)
- aiokafka (no real Kafka)
- cognee_service.* (depends on cognee internals)
"""

import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock

import pytest


# -- Pre-import mocking: block ALL cognee and heavy dependencies ---------------
# These must be installed in sys.modules BEFORE any embedding_service
# module is imported, because processor.py does `from cognee.infrastructure...`
# at module level.


def _install_mock_modules() -> None:
    """Install mock modules for cognee and cognee_service dependencies."""
    mock_modules = [
        # Cognee core — must intercept BEFORE real cognee loads
        "cognee",
        "cognee.shared",
        "cognee.shared.logging_utils",
        "cognee.shared.data_models",
        "cognee.base_config",
        "cognee.infrastructure",
        "cognee.infrastructure.llm",
        "cognee.infrastructure.llm.extraction",
        "cognee.infrastructure.databases",
        "cognee.infrastructure.databases.vector",
        "cognee.infrastructure.databases.vector.embeddings",
        "cognee.infrastructure.databases.vector.embeddings.LiteLLMEmbeddingEngine",
        "cognee.infrastructure.databases.vector.embeddings.get_embedding_engine",
        "cognee.modules",
        "cognee.modules.chunking",
        "cognee.modules.chunking.models",
        "cognee.modules.chunking.models.DocumentChunk",
        "cognee.modules.data",
        "cognee.modules.data.processing",
        "cognee.modules.data.processing.document_types",
        # Cognee service internals
        "cognee_service",
        "cognee_service.config",
        "cognee_service.cognee_patches",
        "cognee_service.multi_tenancy",
        # External libs that may not be installed in test env
        "structlog",
        "aiokafka",
    ]

    for mod_name in mock_modules:
        if mod_name not in sys.modules:
            mock_mod = ModuleType(mod_name)
            sys.modules[mod_name] = mock_mod

    # Provide cognee.__version__
    cognee_mod = sys.modules["cognee"]
    cognee_mod.__version__ = "0.0.0-test"

    # Provide structlog with a usable get_logger and __version__
    structlog_mod = sys.modules["structlog"]
    mock_logger = MagicMock()
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()
    mock_logger.debug = MagicMock()
    structlog_mod.get_logger = MagicMock(return_value=mock_logger)
    structlog_mod.__version__ = "0.0.0-test"

    # Provide LiteLLMEmbeddingEngine as a mock class
    embedding_engine_mod = sys.modules[
        "cognee.infrastructure.databases.vector.embeddings.LiteLLMEmbeddingEngine"
    ]
    mock_engine_cls = type(
        "LiteLLMEmbeddingEngine",
        (),
        {
            "embed_text": AsyncMock(return_value=[[0.1] * 768]),
        },
    )
    embedding_engine_mod.LiteLLMEmbeddingEngine = mock_engine_cls

    # Provide create_embedding_engine
    get_engine_mod = sys.modules[
        "cognee.infrastructure.databases.vector.embeddings.get_embedding_engine"
    ]
    get_engine_mod.create_embedding_engine = MagicMock(return_value=mock_engine_cls())

    # Provide aiokafka stubs
    aiokafka_mod = sys.modules["aiokafka"]
    aiokafka_mod.AIOKafkaConsumer = MagicMock
    aiokafka_mod.AIOKafkaProducer = MagicMock

    # Provide cognee_service.config stubs
    config_mod = sys.modules["cognee_service.config"]
    config_mod.configure_cognee = MagicMock()


# Install mocks before any test imports
_install_mock_modules()


# -- Fixtures ----------------------------------------------------------------


class _AsyncContextManager:
    """Helper async context manager for mocking pool.acquire()."""

    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *args):
        return False


@pytest.fixture
def mock_pool():
    """Create a mock asyncpg pool with all required methods."""
    pool = AsyncMock()
    mock_conn = AsyncMock()

    # pool.acquire() must return an async context manager (not a coroutine).
    pool.acquire = MagicMock(return_value=_AsyncContextManager(mock_conn))

    # Attach conn for test introspection
    pool._mock_conn = mock_conn

    return pool

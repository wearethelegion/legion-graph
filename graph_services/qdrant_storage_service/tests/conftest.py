"""Shared fixtures for Qdrant Storage Service tests.

ALL external dependencies are mocked here:
- qdrant_client (no real Qdrant)
- asyncpg (no real Postgres)
- aiokafka (no real Kafka)
- structlog (optional, provide get_logger)
"""

import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock

import pytest


def _install_mock_modules() -> None:
    """Install mock modules for heavy dependencies.

    Prevents ImportError when qdrant_storage_service modules
    try to import qdrant_client, aiokafka, etc. at module level.
    """
    mock_modules = [
        "structlog",
        "aiokafka",
        "qdrant_client",
        "qdrant_client.http",
        "qdrant_client.http.exceptions",
        "qdrant_client.models",
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
    structlog_mod.configure = MagicMock()
    structlog_mod.make_filtering_bound_logger = MagicMock()
    structlog_mod.contextvars = MagicMock()
    structlog_mod.processors = MagicMock()
    structlog_mod.dev = MagicMock()
    structlog_mod.PrintLoggerFactory = MagicMock()

    # Provide qdrant_client stubs
    qdrant_mod = sys.modules["qdrant_client"]
    qdrant_mod.AsyncQdrantClient = MagicMock

    qdrant_models = sys.modules["qdrant_client.models"]
    qdrant_models.Distance = MagicMock()
    qdrant_models.Distance.COSINE = "Cosine"
    qdrant_models.PointStruct = type(
        "PointStruct",
        (),
        {"__init__": lambda self, **kw: self.__dict__.update(kw)},
    )
    qdrant_models.VectorParams = type(
        "VectorParams",
        (),
        {"__init__": lambda self, **kw: self.__dict__.update(kw)},
    )
    qdrant_models.Filter = type(
        "Filter",
        (),
        {"__init__": lambda self, **kw: self.__dict__.update(kw)},
    )
    qdrant_models.FieldCondition = type(
        "FieldCondition",
        (),
        {"__init__": lambda self, **kw: self.__dict__.update(kw)},
    )
    qdrant_models.MatchValue = type(
        "MatchValue",
        (),
        {"__init__": lambda self, **kw: self.__dict__.update(kw)},
    )

    # Provide qdrant_client.http.exceptions stubs
    qdrant_http_mod = sys.modules["qdrant_client.http"]
    qdrant_exceptions_mod = sys.modules["qdrant_client.http.exceptions"]

    class _UnexpectedResponse(Exception):
        """Stub for qdrant_client.http.exceptions.UnexpectedResponse."""

        def __init__(self, status_code: int = 500, reason_phrase: str = ""):
            self.status_code = status_code
            self.reason_phrase = reason_phrase
            super().__init__(f"Unexpected Response: {status_code}")

    qdrant_exceptions_mod.UnexpectedResponse = _UnexpectedResponse
    qdrant_http_mod.exceptions = qdrant_exceptions_mod

    # Wire submodules into parent
    qdrant_mod.http = qdrant_http_mod

    # Provide aiokafka stubs
    aiokafka_mod = sys.modules["aiokafka"]
    aiokafka_mod.AIOKafkaConsumer = MagicMock
    aiokafka_mod.AIOKafkaProducer = MagicMock


# Install mocks before any test imports
_install_mock_modules()


# ── Helper ────────────────────────────────────────────────────────────


class _AsyncContextManager:
    """Helper async context manager for mocking pool.acquire()."""

    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *args):
        return False


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def mock_pool():
    """Create a mock asyncpg pool with all required methods."""
    pool = AsyncMock()
    mock_conn = AsyncMock()
    pool.acquire = MagicMock(return_value=_AsyncContextManager(mock_conn))
    pool._mock_conn = mock_conn
    return pool


@pytest.fixture
def mock_qdrant_client():
    """Create a mock AsyncQdrantClient."""
    client = AsyncMock()
    # get_collections returns an object with .collections list
    collections_response = MagicMock()
    collections_response.collections = []
    client.get_collections.return_value = collections_response
    return client

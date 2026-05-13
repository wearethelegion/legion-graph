"""Shared fixtures for Neo4j Storage Service tests.

ALL external dependencies are mocked here:
- neo4j (no real Neo4j)
- asyncpg (no real Postgres)
- aiokafka (no real Kafka)
- structlog (provide get_logger)
"""

import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock

import pytest


def _install_mock_modules() -> None:
    """Install mock modules for heavy dependencies.

    Prevents ImportError when neo4j_storage_service modules
    try to import neo4j, aiokafka, etc. at module level.
    """
    mock_modules = [
        "structlog",
        "aiokafka",
        "neo4j",
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

    # Provide neo4j stubs
    neo4j_mod = sys.modules["neo4j"]
    neo4j_mod.AsyncDriver = type("AsyncDriver", (), {})
    neo4j_mod.AsyncGraphDatabase = MagicMock()

    # Provide aiokafka stubs
    aiokafka_mod = sys.modules["aiokafka"]
    aiokafka_mod.AIOKafkaConsumer = MagicMock
    aiokafka_mod.AIOKafkaProducer = MagicMock


# Install mocks before any test imports
_install_mock_modules()


# ── Helper ────────────────────────────────────────────────────────────


class _AsyncContextManager:
    """Helper async context manager for mocking pool.acquire() and neo4j session."""

    def __init__(self, inner):
        self.inner = inner

    async def __aenter__(self):
        return self.inner

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
def mock_neo4j_driver():
    """Create a mock AsyncDriver with session support."""
    driver = MagicMock()
    mock_session = AsyncMock()
    driver.session = MagicMock(return_value=_AsyncContextManager(mock_session))
    driver._mock_session = mock_session
    return driver

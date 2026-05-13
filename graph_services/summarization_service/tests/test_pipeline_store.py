"""Tests for SummarizationStore (pipeline_store.py).

All asyncpg calls are mocked — no real Postgres needed.
"""

import json
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import pytest_asyncio

from summarization_service.pipeline_store import SummarizationStore


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
    """Create a mock asyncpg pool."""
    pool = AsyncMock()
    mock_conn = AsyncMock()

    # pool.acquire() must return an async context manager (not a coroutine).
    # Override acquire to be a regular MagicMock so it doesn't return a coroutine.
    pool.acquire = MagicMock(return_value=_AsyncContextManager(mock_conn))

    # Attach conn for test introspection
    pool._mock_conn = mock_conn

    return pool


@pytest.fixture
def store(mock_pool):
    """Create SummarizationStore with mock pool."""
    return SummarizationStore(mock_pool)


class TestServiceName:
    """Verify the service name constant."""

    def test_service_name(self, store):
        assert store.SERVICE_NAME == "summarization"


class TestEnsureTables:
    """Test table creation DDL."""

    @pytest.mark.asyncio
    async def test_ensure_tables_executes_ddl(self, store, mock_pool):
        """Ensure all CREATE TABLE/INDEX statements are executed."""
        await store.ensure_tables()

        mock_conn = mock_pool._mock_conn

        # Should have called conn.execute multiple times for DDL
        assert mock_conn.execute.call_count > 0

        # Collect all SQL statements
        sqls = [call_args[0][0].strip() for call_args in mock_conn.execute.call_args_list]

        # Verify core tables are created
        create_stmts = [s for s in sqls if "CREATE TABLE" in s]
        assert any("processing_checkpoints" in s for s in create_stmts)
        assert any("pipeline_counters" in s for s in create_stmts)


class TestHasCheckpoint:
    """Test has_checkpoint (read-only checkpoint check)."""

    @pytest.mark.asyncio
    async def test_no_existing_checkpoint_returns_false(self, store, mock_pool):
        """First time seeing this item+stage — should return False (needs processing)."""
        mock_pool.fetchval.return_value = None

        result = await store.has_checkpoint("chunk-1", "summarization", "hash123")

        assert result is False
        mock_pool.fetchval.assert_called_once()
        # Should not call execute (read-only)
        mock_pool.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_matching_hash_returns_true(self, store, mock_pool):
        """Same content hash — should return True (skip processing)."""
        mock_pool.fetchval.return_value = "hash123"

        result = await store.has_checkpoint("chunk-1", "summarization", "hash123")

        assert result is True
        mock_pool.fetchval.assert_called_once()
        # Should not call execute (read-only)
        mock_pool.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_different_hash_returns_false(self, store, mock_pool):
        """Different content hash — should return False (needs processing)."""
        mock_pool.fetchval.return_value = "old_hash"

        result = await store.has_checkpoint("chunk-1", "summarization", "new_hash")

        assert result is False
        mock_pool.fetchval.assert_called_once()
        # Should not call execute (read-only)
        mock_pool.execute.assert_not_called()


class TestSaveCheckpoint:
    """Test save_checkpoint (write-only checkpoint save)."""

    @pytest.mark.asyncio
    async def test_saves_checkpoint(self, store, mock_pool):
        """Should insert/update checkpoint record."""
        await store.save_checkpoint("chunk-1", "summarization", "hash123", "ing-1")

        # Verify INSERT/UPSERT was called
        mock_pool.execute.assert_called_once()
        insert_sql = mock_pool.execute.call_args[0][0]
        assert "INSERT INTO code_processing.processing_checkpoints" in insert_sql
        assert "ON CONFLICT" in insert_sql


class TestCheckCheckpoint:
    """Test check_checkpoint (deprecated combined method)."""

    @pytest.mark.asyncio
    async def test_returns_false_on_first_call(self, store, mock_pool):
        """First call should return False (needs processing) and store checkpoint."""
        mock_pool.fetchval.return_value = None

        result = await store.check_checkpoint("chunk-1", "summarization", "hash123")

        assert result is False
        mock_pool.fetchval.assert_called_once()
        mock_pool.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_true_on_matching_hash(self, store, mock_pool):
        """Matching content hash should return True (skip processing)."""
        mock_pool.fetchval.return_value = "hash123"

        result = await store.check_checkpoint("chunk-1", "summarization", "hash123")

        assert result is True
        mock_pool.fetchval.assert_called_once()
        # Should not call execute when hash matches
        mock_pool.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_false_on_changed_hash(self, store, mock_pool):
        """Changed content hash should return False and update checkpoint."""
        mock_pool.fetchval.return_value = "old_hash"

        result = await store.check_checkpoint("chunk-1", "summarization", "new_hash")

        assert result is False
        mock_pool.fetchval.assert_called_once()
        mock_pool.execute.assert_called_once()


class TestIncrementCounter:
    """Test atomic counter increment."""

    @pytest.mark.asyncio
    async def test_increment_counter(self, store, mock_pool):
        await store.increment_counter("ing-1", "chunks_received", 5)

        mock_pool.execute.assert_called_once()
        sql_arg = mock_pool.execute.call_args[0][0]
        assert "INSERT INTO code_processing.pipeline_counters" in sql_arg
        assert "ON CONFLICT" in sql_arg
        assert "counter_value + EXCLUDED.counter_value" in sql_arg

        # Verify positional args
        args = mock_pool.execute.call_args[0]
        assert args[1] == "ing-1"  # ingestion_id
        assert args[2] == "summarization"  # service_name
        assert args[3] == "chunks_received"  # counter_name
        assert args[4] == 5  # delta

    @pytest.mark.asyncio
    async def test_increment_counter_default_delta(self, store, mock_pool):
        await store.increment_counter("ing-1", "chunks_received")

        args = mock_pool.execute.call_args[0]
        assert args[4] == 1  # default delta


class TestSetCounter:
    """Test absolute counter set."""

    @pytest.mark.asyncio
    async def test_set_counter(self, store, mock_pool):
        await store.set_counter("ing-1", "total_chunks", 100, "running")

        mock_pool.execute.assert_called_once()
        sql_arg = mock_pool.execute.call_args[0][0]
        assert "INSERT INTO code_processing.pipeline_counters" in sql_arg
        assert "counter_value = EXCLUDED.counter_value" in sql_arg

        args = mock_pool.execute.call_args[0]
        assert args[1] == "ing-1"
        assert args[2] == "summarization"
        assert args[3] == "total_chunks"
        assert args[4] == 100
        assert args[5] == "running"


class TestGetCounter:
    """Test counter retrieval."""

    @pytest.mark.asyncio
    async def test_get_counter_returns_value(self, store, mock_pool):
        mock_pool.fetchval.return_value = 42

        val = await store.get_counter("ing-1", "chunks_received")
        assert val == 42

        mock_pool.fetchval.assert_called_once()
        sql_arg = mock_pool.fetchval.call_args[0][0]
        assert "SELECT counter_value" in sql_arg
        assert "pipeline_counters" in sql_arg

    @pytest.mark.asyncio
    async def test_get_counter_returns_zero_when_none(self, store, mock_pool):
        mock_pool.fetchval.return_value = None

        val = await store.get_counter("ing-1", "nonexistent")
        assert val == 0


class TestGetAllCounters:
    """Test fetching all counters for an ingestion."""

    @pytest.mark.asyncio
    async def test_get_all_counters(self, store, mock_pool):
        mock_pool.fetch.return_value = [
            {"counter_name": "chunks_received", "counter_value": 10},
            {"counter_name": "summaries_produced", "counter_value": 8},
        ]

        result = await store.get_all_counters("ing-1")
        assert result == {
            "chunks_received": 10,
            "summaries_produced": 8,
        }

        mock_pool.fetch.assert_called_once()
        args = mock_pool.fetch.call_args[0]
        assert args[1] == "ing-1"
        assert args[2] == "summarization"

    @pytest.mark.asyncio
    async def test_get_all_counters_empty(self, store, mock_pool):
        mock_pool.fetch.return_value = []

        result = await store.get_all_counters("ing-1")
        assert result == {}


class TestFinalizeCounters:
    """Test counter finalization."""

    @pytest.mark.asyncio
    async def test_finalize_counters(self, store, mock_pool):
        await store.finalize_counters("ing-1")

        mock_pool.execute.assert_called_once()
        sql_arg = mock_pool.execute.call_args[0][0]
        assert "UPDATE code_processing.pipeline_counters" in sql_arg
        assert "status = 'complete'" in sql_arg

        args = mock_pool.execute.call_args[0]
        assert args[1] == "ing-1"
        assert args[2] == "summarization"


class TestGetPreprocessorTotalChunks:
    """Test reading preprocessor's total_chunks counter."""

    @pytest.mark.asyncio
    async def test_returns_value_when_set(self, store, mock_pool):
        mock_pool.fetchval.return_value = 200

        val = await store.get_preprocessor_total_chunks("ing-1")
        assert val == 200

        sql_arg = mock_pool.fetchval.call_args[0][0]
        assert "service_name = 'preprocessor'" in sql_arg
        assert "counter_name = 'chunks_produced'" in sql_arg

    @pytest.mark.asyncio
    async def test_returns_none_when_not_set(self, store, mock_pool):
        mock_pool.fetchval.return_value = None

        val = await store.get_preprocessor_total_chunks("ing-1")
        assert val is None

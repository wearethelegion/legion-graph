"""Tests for EmbeddingStore (pipeline_store.py).

All asyncpg calls are mocked — no real Postgres needed.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from embedding_service.pipeline_store import EmbeddingStore


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
    pool.acquire = MagicMock(return_value=_AsyncContextManager(mock_conn))
    pool._mock_conn = mock_conn
    return pool


@pytest.fixture
def store(mock_pool):
    """Create EmbeddingStore with mock pool."""
    return EmbeddingStore(mock_pool)


class TestServiceName:
    def test_service_name(self, store):
        assert store.SERVICE_NAME == "embedding"


class TestEnsureTables:
    @pytest.mark.asyncio
    async def test_ensure_tables_executes_ddl(self, store, mock_pool):
        await store.ensure_tables()
        mock_conn = mock_pool._mock_conn
        assert mock_conn.execute.call_count > 0
        sqls = [c[0][0].strip() for c in mock_conn.execute.call_args_list]
        create_stmts = [s for s in sqls if "CREATE TABLE" in s]
        assert any("pipeline_counters" in s for s in create_stmts)
        assert any("processing_checkpoints" in s for s in create_stmts)
        index_stmts = [s for s in sqls if "CREATE INDEX" in s]
        assert len(index_stmts) >= 1


class TestHasCheckpoint:
    """Test has_checkpoint (read-only checkpoint check)."""

    @pytest.mark.asyncio
    async def test_no_existing_checkpoint_returns_false(self, store, mock_pool):
        """First time seeing this item+stage — should return False (needs processing)."""
        mock_pool.fetchval.return_value = None  # No existing checkpoint

        result = await store.has_checkpoint("item-1", "embedding", "hash123")
        assert result is False

        # Verify SELECT was called
        mock_pool.fetchval.assert_called_once()
        select_sql = mock_pool.fetchval.call_args[0][0]
        assert "SELECT content_hash" in select_sql
        assert "processing_checkpoints" in select_sql

        # Verify INSERT was NOT called (read-only)
        mock_pool.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_existing_checkpoint_same_hash_returns_true(self, store, mock_pool):
        """Same content hash exists — should return True (skip processing)."""
        mock_pool.fetchval.return_value = "hash123"

        result = await store.has_checkpoint("item-1", "embedding", "hash123")
        assert result is True

        # Verify no writes happened (read-only)
        mock_pool.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_existing_checkpoint_different_hash_returns_false(self, store, mock_pool):
        """Content changed (different hash) — should return False (needs reprocessing)."""
        mock_pool.fetchval.return_value = "old_hash"

        result = await store.has_checkpoint("item-1", "embedding", "new_hash")
        assert result is False

        # Verify no writes happened (read-only)
        mock_pool.execute.assert_not_called()


class TestSaveCheckpoint:
    """Test save_checkpoint (write-only checkpoint recording)."""

    @pytest.mark.asyncio
    async def test_save_checkpoint_new_item(self, store, mock_pool):
        """First time checkpoint — should INSERT."""
        await store.save_checkpoint("item-1", "embedding", "hash123", "ing-1")

        mock_pool.execute.assert_called_once()
        sql_arg = mock_pool.execute.call_args[0][0]
        assert "INSERT INTO code_processing.processing_checkpoints" in sql_arg
        assert "ON CONFLICT" in sql_arg
        args = mock_pool.execute.call_args[0]
        assert args[1] == "item-1"
        assert args[2] == "embedding"
        assert args[3] == "hash123"
        assert args[4] == "ing-1"

    @pytest.mark.asyncio
    async def test_save_checkpoint_updated_item(self, store, mock_pool):
        """Content changed — should UPDATE existing checkpoint."""
        await store.save_checkpoint("item-1", "embedding", "new_hash", "ing-2")

        mock_pool.execute.assert_called_once()
        sql_arg = mock_pool.execute.call_args[0][0]
        assert "INSERT INTO code_processing.processing_checkpoints" in sql_arg
        assert "DO UPDATE SET" in sql_arg


class TestCheckCheckpoint:
    """Test check_checkpoint (DEPRECATED combined read+write)."""

    @pytest.mark.asyncio
    async def test_returns_true_when_already_processed_with_same_hash(self, store, mock_pool):
        mock_pool.fetchval = AsyncMock(return_value="hash123")
        result = await store.check_checkpoint("item-1", "embedding", "hash123")
        assert result is True
        # Should not execute INSERT when hash matches
        mock_pool.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_false_and_upserts_when_new_item(self, store, mock_pool):
        mock_pool.fetchval = AsyncMock(return_value=None)
        result = await store.check_checkpoint("item-1", "embedding", "hash456")
        assert result is False
        mock_pool.execute.assert_called_once()
        sql_arg = mock_pool.execute.call_args[0][0]
        assert "INSERT INTO code_processing.processing_checkpoints" in sql_arg

    @pytest.mark.asyncio
    async def test_returns_false_and_upserts_when_content_changed(self, store, mock_pool):
        mock_pool.fetchval = AsyncMock(return_value="old_hash")
        result = await store.check_checkpoint("item-1", "embedding", "new_hash")
        assert result is False
        mock_pool.execute.assert_called_once()


class TestIncrementCounter:
    @pytest.mark.asyncio
    async def test_increment_counter(self, store, mock_pool):
        await store.increment_counter("ing-1", "entities_received", 5)
        mock_pool.execute.assert_called_once()
        sql_arg = mock_pool.execute.call_args[0][0]
        assert "INSERT INTO code_processing.pipeline_counters" in sql_arg
        assert "ON CONFLICT" in sql_arg
        args = mock_pool.execute.call_args[0]
        assert args[1] == "ing-1"
        assert args[2] == "embedding"
        assert args[3] == "entities_received"
        assert args[4] == 5

    @pytest.mark.asyncio
    async def test_increment_counter_default_delta(self, store, mock_pool):
        await store.increment_counter("ing-1", "summaries_received")
        args = mock_pool.execute.call_args[0]
        assert args[4] == 1


class TestSetCounter:
    @pytest.mark.asyncio
    async def test_set_counter(self, store, mock_pool):
        await store.set_counter("ing-1", "embeddings_computed", 200, "running")
        mock_pool.execute.assert_called_once()
        args = mock_pool.execute.call_args[0]
        assert args[2] == "embedding"
        assert args[3] == "embeddings_computed"
        assert args[4] == 200


class TestGetCounter:
    @pytest.mark.asyncio
    async def test_get_counter_returns_value(self, store, mock_pool):
        mock_pool.fetchval.return_value = 42
        val = await store.get_counter("ing-1", "entities_received")
        assert val == 42

    @pytest.mark.asyncio
    async def test_get_counter_returns_zero_when_none(self, store, mock_pool):
        mock_pool.fetchval.return_value = None
        val = await store.get_counter("ing-1", "nonexistent")
        assert val == 0


class TestGetAllCounters:
    @pytest.mark.asyncio
    async def test_get_all_counters(self, store, mock_pool):
        mock_pool.fetch.return_value = [
            {"counter_name": "entities_received", "counter_value": 10},
            {"counter_name": "summaries_received", "counter_value": 5},
            {"counter_name": "embeddings_computed", "counter_value": 15},
        ]
        result = await store.get_all_counters("ing-1")
        assert result == {
            "entities_received": 10,
            "summaries_received": 5,
            "embeddings_computed": 15,
        }

    @pytest.mark.asyncio
    async def test_get_all_counters_empty(self, store, mock_pool):
        mock_pool.fetch.return_value = []
        result = await store.get_all_counters("ing-1")
        assert result == {}


class TestFinalizeCounters:
    @pytest.mark.asyncio
    async def test_finalize_counters(self, store, mock_pool):
        await store.finalize_counters("ing-1")
        mock_pool.execute.assert_called_once()
        sql_arg = mock_pool.execute.call_args[0][0]
        assert "UPDATE code_processing.pipeline_counters" in sql_arg
        assert "status = 'complete'" in sql_arg
        args = mock_pool.execute.call_args[0]
        assert args[1] == "ing-1"
        assert args[2] == "embedding"


class TestGetUpstreamTotal:
    @pytest.mark.asyncio
    async def test_returns_value_when_set(self, store, mock_pool):
        mock_pool.fetchval.return_value = 200
        val = await store.get_upstream_total("ing-1", "entity_extraction", "entities_extracted")
        assert val == 200

    @pytest.mark.asyncio
    async def test_returns_none_when_not_set(self, store, mock_pool):
        mock_pool.fetchval.return_value = None
        val = await store.get_upstream_total("ing-1", "entity_extraction", "entities_extracted")
        assert val is None


class TestIsUpstreamComplete:
    @pytest.mark.asyncio
    async def test_returns_true_when_complete(self, store, mock_pool):
        mock_pool.fetchval.return_value = 1
        result = await store.is_upstream_complete("ing-1", "entity_extraction")
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_not_complete(self, store, mock_pool):
        mock_pool.fetchval.return_value = 0
        result = await store.is_upstream_complete("ing-1", "entity_extraction")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_none(self, store, mock_pool):
        mock_pool.fetchval.return_value = None
        result = await store.is_upstream_complete("ing-1", "summarization")
        assert result is False

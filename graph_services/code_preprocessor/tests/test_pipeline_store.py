"""Unit tests for PipelineStore — v2 pipeline counter and chunk staging.

Tests all PipelineStore methods with a mocked asyncpg pool.
No real database required.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from code_preprocessor.storage.pipeline_store import PipelineStore


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def mock_pool():
    """Create mock asyncpg pool with async methods."""
    pool = AsyncMock()
    pool.execute = AsyncMock()
    pool.executemany = AsyncMock()
    pool.fetch = AsyncMock(return_value=[])
    pool.fetchrow = AsyncMock(return_value=None)
    pool.fetchval = AsyncMock(return_value=0)
    return pool


@pytest.fixture
def store(mock_pool):
    """Create PipelineStore with mocked pool."""
    return PipelineStore(mock_pool)


@pytest.fixture
def ingestion_id():
    return str(uuid.uuid4())


# ── PipelineStore.set_counter ─────────────────────────────────────────────


class TestSetCounter:
    @pytest.mark.asyncio
    async def test_set_counter_calls_execute_with_correct_sql(self, store, mock_pool, ingestion_id):
        """set_counter should INSERT with ON CONFLICT DO UPDATE."""
        await store.set_counter(ingestion_id, "files_discovered", 42)

        mock_pool.execute.assert_called_once()
        args = mock_pool.execute.call_args
        sql = args[0][0]
        assert "INSERT INTO code_processing.pipeline_counters" in sql
        assert "ON CONFLICT" in sql
        assert "DO UPDATE SET" in sql

    @pytest.mark.asyncio
    async def test_set_counter_passes_correct_params(self, store, mock_pool, ingestion_id):
        """set_counter should pass ingestion_id, service_name, counter_name, value, status."""
        await store.set_counter(ingestion_id, "files_filtered", 10, status="complete")

        args = mock_pool.execute.call_args
        positional = args[0]
        assert positional[1] == ingestion_id
        assert positional[2] == "preprocessor"  # SERVICE_NAME
        assert positional[3] == "files_filtered"
        assert positional[4] == 10
        assert positional[5] == "complete"

    @pytest.mark.asyncio
    async def test_set_counter_default_status_is_running(self, store, mock_pool, ingestion_id):
        """Default status should be 'running'."""
        await store.set_counter(ingestion_id, "chunks_produced", 100)

        args = mock_pool.execute.call_args
        assert args[0][5] == "running"

    @pytest.mark.asyncio
    async def test_set_counter_service_name_is_preprocessor(self, store, mock_pool, ingestion_id):
        """SERVICE_NAME should always be 'preprocessor'."""
        await store.set_counter(ingestion_id, "any_counter", 1)
        assert mock_pool.execute.call_args[0][2] == "preprocessor"
        assert store.SERVICE_NAME == "preprocessor"


# ── PipelineStore.increment_counter ───────────────────────────────────────


class TestIncrementCounter:
    @pytest.mark.asyncio
    async def test_increment_counter_calls_execute(self, store, mock_pool, ingestion_id):
        """increment_counter should INSERT with ON CONFLICT adding to existing value."""
        await store.increment_counter(ingestion_id, "chunks_produced", delta=5)

        mock_pool.execute.assert_called_once()
        sql = mock_pool.execute.call_args[0][0]
        assert "pipeline_counters.counter_value + EXCLUDED.counter_value" in sql

    @pytest.mark.asyncio
    async def test_increment_counter_default_delta_is_one(self, store, mock_pool, ingestion_id):
        """Default delta should be 1."""
        await store.increment_counter(ingestion_id, "files_parsed")

        args = mock_pool.execute.call_args[0]
        assert args[4] == 1  # delta parameter

    @pytest.mark.asyncio
    async def test_increment_counter_passes_delta(self, store, mock_pool, ingestion_id):
        """Should pass the delta value to SQL."""
        await store.increment_counter(ingestion_id, "embeddings_computed", delta=50)

        args = mock_pool.execute.call_args[0]
        assert args[4] == 50

    @pytest.mark.asyncio
    async def test_increment_counter_sets_status_running(self, store, mock_pool, ingestion_id):
        """Inserted counter should have status 'running'."""
        await store.increment_counter(ingestion_id, "files_parsed")

        sql = mock_pool.execute.call_args[0][0]
        assert "'running'" in sql


# ── PipelineStore.finalize_counters ───────────────────────────────────────


class TestFinalizeCounters:
    @pytest.mark.asyncio
    async def test_finalize_counters_sets_complete(self, store, mock_pool, ingestion_id):
        """finalize_counters should UPDATE status to 'complete' for all counters."""
        await store.finalize_counters(ingestion_id)

        mock_pool.execute.assert_called_once()
        sql = mock_pool.execute.call_args[0][0]
        assert "UPDATE code_processing.pipeline_counters" in sql
        assert "status = 'complete'" in sql

    @pytest.mark.asyncio
    async def test_finalize_counters_scopes_to_ingestion_and_service(
        self, store, mock_pool, ingestion_id
    ):
        """Should only update counters for this ingestion_id + service."""
        await store.finalize_counters(ingestion_id)

        args = mock_pool.execute.call_args[0]
        assert args[1] == ingestion_id
        assert args[2] == "preprocessor"
        assert "ingestion_id = $1" in args[0]
        assert "service_name = $2" in args[0]


# ── PipelineStore.get_counters ────────────────────────────────────────────


class TestGetCounters:
    @pytest.mark.asyncio
    async def test_get_counters_returns_dict(self, store, mock_pool, ingestion_id):
        """get_counters should return a dict of counter_name -> counter_value."""
        mock_pool.fetch.return_value = [
            {"counter_name": "files_discovered", "counter_value": 100},
            {"counter_name": "files_filtered", "counter_value": 80},
            {"counter_name": "chunks_produced", "counter_value": 500},
        ]

        result = await store.get_counters(ingestion_id)

        assert result == {
            "files_discovered": 100,
            "files_filtered": 80,
            "chunks_produced": 500,
        }

    @pytest.mark.asyncio
    async def test_get_counters_empty_when_no_rows(self, store, mock_pool, ingestion_id):
        """Should return empty dict when no counters exist."""
        mock_pool.fetch.return_value = []

        result = await store.get_counters(ingestion_id)

        assert result == {}

    @pytest.mark.asyncio
    async def test_get_counters_scopes_to_ingestion_and_service(
        self, store, mock_pool, ingestion_id
    ):
        """SELECT should filter by ingestion_id AND service_name."""
        mock_pool.fetch.return_value = []

        await store.get_counters(ingestion_id)

        args = mock_pool.fetch.call_args[0]
        assert "ingestion_id = $1" in args[0]
        assert "service_name = $2" in args[0]
        assert args[1] == ingestion_id
        assert args[2] == "preprocessor"


# ── PipelineStore.store_chunk ─────────────────────────────────────────────


class TestStoreChunk:
    @pytest.mark.asyncio
    async def test_store_chunk_inserts_with_upsert(self, store, mock_pool, ingestion_id):
        """store_chunk should INSERT with ON CONFLICT on chunk_id."""
        chunk_id = str(uuid.uuid4())

        await store.store_chunk(
            chunk_id=chunk_id,
            ingestion_id=ingestion_id,
            company_id="comp-1",
            project_id="proj-1",
            file_path="src/main.py",
            repository="owner/repo",
            branch="main",
            language="python",
            chunk_index=0,
            total_chunks=3,
            content="def hello(): pass",
            header="=== FILE ===",
            embedding=[0.1, 0.2, 0.3],
            file_skeleton="  def hello()",
        )

        mock_pool.execute.assert_called_once()
        sql = mock_pool.execute.call_args[0][0]
        assert "INSERT INTO code_processing.pipeline_chunks" in sql
        assert "ON CONFLICT (chunk_id) DO UPDATE" in sql

    @pytest.mark.asyncio
    async def test_store_chunk_passes_all_14_params(self, store, mock_pool, ingestion_id):
        """Should pass all 14 positional parameters."""
        chunk_id = str(uuid.uuid4())

        await store.store_chunk(
            chunk_id=chunk_id,
            ingestion_id=ingestion_id,
            company_id="comp-1",
            project_id="proj-1",
            file_path="src/main.py",
            repository="owner/repo",
            branch="main",
            language="python",
            chunk_index=0,
            total_chunks=3,
            content="code here",
            header="header here",
            embedding=[1.0, 2.0],
            file_skeleton="skeleton",
        )

        args = mock_pool.execute.call_args[0]
        # args[0] is SQL, args[1:] are the 14 params
        assert args[1] == chunk_id
        assert args[2] == ingestion_id
        assert args[3] == "comp-1"
        assert args[4] == "proj-1"
        assert args[5] == "src/main.py"
        assert args[6] == "owner/repo"
        assert args[7] == "main"
        assert args[8] == "python"
        assert args[9] == 0
        assert args[10] == 3
        assert args[11] == "code here"
        assert args[12] == "header here"
        assert args[13] == [1.0, 2.0]
        assert args[14] == "skeleton"

    @pytest.mark.asyncio
    async def test_store_chunk_defaults(self, store, mock_pool, ingestion_id):
        """Default values: header='', embedding=None, file_skeleton=''."""
        chunk_id = str(uuid.uuid4())

        await store.store_chunk(
            chunk_id=chunk_id,
            ingestion_id=ingestion_id,
            company_id="c",
            project_id="p",
            file_path="f.py",
            repository="r",
            branch="b",
            language=None,
            chunk_index=0,
            total_chunks=1,
            content="code",
        )

        args = mock_pool.execute.call_args[0]
        assert args[8] is None  # language
        assert args[12] == ""  # header default
        assert args[13] is None  # embedding default
        assert args[14] == ""  # file_skeleton default


# ── PipelineStore.store_chunks_batch ──────────────────────────────────────


class TestStoreChunksBatch:
    @pytest.mark.asyncio
    async def test_batch_empty_returns_zero(self, store, mock_pool):
        """Empty chunk list should return 0 without calling executemany."""
        result = await store.store_chunks_batch([])

        assert result == 0
        mock_pool.executemany.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_stores_multiple_chunks(self, store, mock_pool, ingestion_id):
        """Should call executemany with a list of tuples."""
        chunks = [
            {
                "chunk_id": str(uuid.uuid4()),
                "ingestion_id": ingestion_id,
                "company_id": "c1",
                "project_id": "p1",
                "file_path": "a.py",
                "repository": "r",
                "branch": "main",
                "language": "python",
                "chunk_index": 0,
                "total_chunks": 2,
                "content": "chunk 0",
                "header": "h0",
                "embedding": [0.1],
                "file_skeleton": "skel0",
            },
            {
                "chunk_id": str(uuid.uuid4()),
                "ingestion_id": ingestion_id,
                "company_id": "c1",
                "project_id": "p1",
                "file_path": "a.py",
                "repository": "r",
                "branch": "main",
                "language": "python",
                "chunk_index": 1,
                "total_chunks": 2,
                "content": "chunk 1",
                "header": "h1",
                "embedding": [0.2],
                "file_skeleton": "skel1",
            },
        ]

        result = await store.store_chunks_batch(chunks)

        assert result == 2
        mock_pool.executemany.assert_called_once()
        sql = mock_pool.executemany.call_args[0][0]
        assert "INSERT INTO code_processing.pipeline_chunks" in sql
        assert "ON CONFLICT (chunk_id) DO UPDATE" in sql

        records = mock_pool.executemany.call_args[0][1]
        assert len(records) == 2
        assert records[0][6] == "main"  # branch
        assert records[1][10] == "chunk 1"  # content

    @pytest.mark.asyncio
    async def test_batch_handles_missing_optional_keys(self, store, mock_pool, ingestion_id):
        """Chunks missing optional keys should get defaults via .get()."""
        chunks = [
            {
                "chunk_id": str(uuid.uuid4()),
                "ingestion_id": ingestion_id,
                "company_id": "c",
                "project_id": "p",
                "file_path": "f.py",
                "repository": "r",
                "branch": "b",
                "chunk_index": 0,
                "total_chunks": 1,
                "content": "code",
                # Missing: language, header, embedding, file_skeleton
            },
        ]

        result = await store.store_chunks_batch(chunks)

        assert result == 1
        records = mock_pool.executemany.call_args[0][1]
        record = records[0]
        assert record[7] is None  # language via .get()
        assert record[11] == ""  # header via .get("header", "")
        assert record[12] is None  # embedding via .get()
        assert record[13] == ""  # file_skeleton via .get("file_skeleton", "")


# ── PipelineStore.get_chunk_count ─────────────────────────────────────────


class TestGetChunkCount:
    @pytest.mark.asyncio
    async def test_get_chunk_count_returns_value(self, store, mock_pool, ingestion_id):
        """get_chunk_count should return fetchval result."""
        mock_pool.fetchval.return_value = 42

        result = await store.get_chunk_count(ingestion_id)

        assert result == 42
        sql = mock_pool.fetchval.call_args[0][0]
        assert "SELECT COUNT(*) FROM code_processing.pipeline_chunks" in sql
        assert "ingestion_id = $1" in sql


# ── PipelineStore.close ──────────────────────────────────────────────────


class TestClose:
    @pytest.mark.asyncio
    async def test_close_is_noop(self, store):
        """close() should be a no-op (pool managed externally)."""
        await store.close()  # Should not raise


# ── PipelineStore class attributes ────────────────────────────────────────


class TestClassAttributes:
    def test_service_name_constant(self):
        assert PipelineStore.SERVICE_NAME == "preprocessor"

    def test_constructor_stores_pool(self, mock_pool):
        store = PipelineStore(mock_pool)
        assert store._pool is mock_pool

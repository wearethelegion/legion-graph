"""Integration tests for v2 pipeline modifications.

Tests:
1. emit_ingestion_complete — event format, Kafka message shape, error handling
2. publish_enriched_chunks — pipeline_store chunk staging, file_skeleton formatting
3. _format_skeleton — helper function for JSONB→text conversion
4. Consumer pipeline counter wiring — counters set at correct points
"""

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from code_preprocessor.enrichment import (
    emit_ingestion_complete,
    publish_enriched_chunks,
    _format_skeleton,
)
from code_preprocessor.storage.pipeline_store import PipelineStore


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def mock_producer():
    """Mock AIOKafkaProducer with send and flush."""
    producer = AsyncMock()
    producer.send = AsyncMock()
    producer.flush = AsyncMock()
    producer.send_and_wait = AsyncMock()
    return producer


@pytest.fixture
def mock_pool():
    """Mock asyncpg pool."""
    pool = AsyncMock()
    pool.execute = AsyncMock()
    pool.executemany = AsyncMock()
    pool.fetch = AsyncMock(return_value=[])
    pool.fetchrow = AsyncMock(return_value=None)
    pool.fetchval = AsyncMock(return_value=0)
    return pool


@pytest.fixture
def mock_pipeline_store():
    """Mock PipelineStore."""
    store = AsyncMock(spec=PipelineStore)
    store.set_counter = AsyncMock()
    store.increment_counter = AsyncMock()
    store.finalize_counters = AsyncMock()
    store.store_chunk = AsyncMock()
    store.store_chunks_batch = AsyncMock(return_value=0)
    store.get_counters = AsyncMock(return_value={})
    return store


@pytest.fixture
def ingestion_id():
    return str(uuid.uuid4())


# ── _format_skeleton tests ────────────────────────────────────────────────


class TestFormatSkeleton:
    def test_none_returns_empty(self):
        assert _format_skeleton(None) == ""

    def test_empty_list_returns_empty(self):
        assert _format_skeleton([]) == ""

    def test_empty_string_returns_empty(self):
        assert _format_skeleton("") == ""

    def test_list_of_declarations(self):
        skel = ["class Foo", "def bar()", "def baz(x, y)"]
        result = _format_skeleton(skel)
        assert result == "  class Foo\n  def bar()\n  def baz(x, y)"

    def test_json_string_input(self):
        """Should handle JSON string (as stored in Postgres JSONB column)."""
        skel_json = json.dumps(["class App", "def index"])
        result = _format_skeleton(skel_json)
        assert result == "  class App\n  def index"

    def test_single_declaration(self):
        result = _format_skeleton(["def only_one"])
        assert result == "  def only_one"


# ── emit_ingestion_complete tests ─────────────────────────────────────────


class TestEmitIngestionComplete:
    @pytest.mark.asyncio
    async def test_emits_correct_event_format(self, mock_producer, ingestion_id):
        """Event should have required fields in the pipeline-events schema."""
        await emit_ingestion_complete(
            mock_producer,
            ingestion_id=ingestion_id,
            company_id="comp-1",
            project_id="proj-1",
            total_files=10,
            total_chunks=250,
        )

        mock_producer.send.assert_called_once()
        call_kwargs = mock_producer.send.call_args

        # Check topic
        assert call_kwargs[0][0] == "pipeline-events"

        # Check key is ingestion_id bytes
        assert call_kwargs[1]["key"] == ingestion_id.encode("utf-8")

        # Parse the value JSON
        event = json.loads(call_kwargs[1]["value"].decode("utf-8"))
        assert event["event_type"] == "ingestion_complete"
        assert event["ingestion_id"] == ingestion_id
        assert event["company_id"] == "comp-1"
        assert event["project_id"] == "proj-1"
        assert event["total_files"] == 10
        assert event["total_chunks"] == 250
        assert "timestamp" in event

    @pytest.mark.asyncio
    async def test_timestamp_is_iso_format(self, mock_producer, ingestion_id):
        """Timestamp should be a valid ISO 8601 string."""
        await emit_ingestion_complete(
            mock_producer,
            ingestion_id=ingestion_id,
            company_id="c",
            project_id="p",
            total_files=0,
            total_chunks=0,
        )

        event = json.loads(mock_producer.send.call_args[1]["value"].decode("utf-8"))
        # Should parse without error
        ts = datetime.fromisoformat(event["timestamp"])
        assert ts.tzinfo is not None  # Should be timezone-aware

    @pytest.mark.asyncio
    async def test_flush_called_after_send(self, mock_producer, ingestion_id):
        """Producer should be flushed after sending."""
        await emit_ingestion_complete(
            mock_producer,
            ingestion_id=ingestion_id,
            company_id="c",
            project_id="p",
            total_files=0,
            total_chunks=0,
        )

        mock_producer.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_custom_topic(self, mock_producer, ingestion_id):
        """Should respect custom topic parameter."""
        await emit_ingestion_complete(
            mock_producer,
            ingestion_id=ingestion_id,
            company_id="c",
            project_id="p",
            total_files=0,
            total_chunks=0,
            topic="custom-pipeline-events",
        )

        assert mock_producer.send.call_args[0][0] == "custom-pipeline-events"

    @pytest.mark.asyncio
    async def test_zero_files_zero_chunks(self, mock_producer, ingestion_id):
        """Should handle the early-exit case (all files filtered)."""
        await emit_ingestion_complete(
            mock_producer,
            ingestion_id=ingestion_id,
            company_id="c",
            project_id="p",
            total_files=0,
            total_chunks=0,
        )

        event = json.loads(mock_producer.send.call_args[1]["value"].decode("utf-8"))
        assert event["total_files"] == 0
        assert event["total_chunks"] == 0

    @pytest.mark.asyncio
    async def test_error_is_caught_not_raised(self, mock_producer, ingestion_id):
        """Producer errors should be logged but not raised."""
        mock_producer.send.side_effect = Exception("Kafka connection lost")

        # Should NOT raise
        await emit_ingestion_complete(
            mock_producer,
            ingestion_id=ingestion_id,
            company_id="c",
            project_id="p",
            total_files=5,
            total_chunks=100,
        )

        # flush should NOT be called since send failed
        mock_producer.flush.assert_not_called()


# ── publish_enriched_chunks with pipeline_store tests ─────────────────────


class TestPublishEnrichedChunksWithPipelineStore:
    def _make_chunk_row(self, idx=0, total=3, skeleton=None, ingestion_id="ing-1"):
        """Create a mock chunk row from the SELECT query in publish_enriched_chunks."""
        return {
            "chunk_id": uuid.uuid4(),
            "parent_id": uuid.uuid4(),
            "ingestion_id": ingestion_id,
            "content": f"def func_{idx}(): pass",
            "header": f"=== FILE ===\nPath: app.py\n=== CODE (chunk {idx + 1}/{total}) ===",
            "chunk_index": idx,
            "total_chunks": total,
            "embedding": [0.1, 0.2, 0.3],
            "file_path": "app.py",
            "language": "python",
            "repository": "owner/repo",
            "branch": "main",
            "file_skeleton": skeleton,
        }

    @pytest.mark.asyncio
    async def test_no_chunks_returns_zero(self, mock_pool, mock_producer):
        """When no chunks found, should return 0."""
        mock_pool.fetch.return_value = []

        result = await publish_enriched_chunks(mock_pool, mock_producer, "owner/repo", "main")

        assert result == 0

    @pytest.mark.asyncio
    async def test_chunks_published_to_kafka(self, mock_pool, mock_producer, ingestion_id):
        """Each chunk should be published to Kafka."""
        rows = [self._make_chunk_row(i, 3, ingestion_id=ingestion_id) for i in range(3)]
        mock_pool.fetch.return_value = rows
        mock_pool.fetchval.return_value = 1  # files_produced count

        result = await publish_enriched_chunks(
            mock_pool,
            mock_producer,
            "owner/repo",
            "main",
            ingestion_id=ingestion_id,
        )

        assert result == 3
        assert mock_producer.send.call_count == 3

    @pytest.mark.asyncio
    async def test_pipeline_store_receives_chunks_in_batches(
        self, mock_pool, mock_producer, mock_pipeline_store, ingestion_id
    ):
        """When pipeline_store is provided, chunks should be batch-stored."""
        rows = [self._make_chunk_row(i, 5, ingestion_id=ingestion_id) for i in range(5)]
        mock_pool.fetch.return_value = rows
        mock_pool.fetchval.return_value = 1

        result = await publish_enriched_chunks(
            mock_pool,
            mock_producer,
            "owner/repo",
            "main",
            ingestion_id=ingestion_id,
            pipeline_store=mock_pipeline_store,
        )

        assert result == 5
        # With batch_size=50, all 5 chunks fit in one batch → one call
        mock_pipeline_store.store_chunks_batch.assert_called_once()
        batch = mock_pipeline_store.store_chunks_batch.call_args[0][0]
        assert len(batch) == 5

    @pytest.mark.asyncio
    async def test_pipeline_store_batch_flushed_at_threshold(
        self, mock_pool, mock_producer, mock_pipeline_store, ingestion_id
    ):
        """Batches should be flushed when reaching batch_size (50)."""
        # Create 55 chunks to trigger one batch flush at 50 + final flush of 5
        rows = [self._make_chunk_row(i, 55, ingestion_id=ingestion_id) for i in range(55)]
        mock_pool.fetch.return_value = rows
        mock_pool.fetchval.return_value = 1

        result = await publish_enriched_chunks(
            mock_pool,
            mock_producer,
            "owner/repo",
            "main",
            ingestion_id=ingestion_id,
            pipeline_store=mock_pipeline_store,
        )

        assert result == 55
        # Should have been called twice: once at 50, once for the remaining 5
        assert mock_pipeline_store.store_chunks_batch.call_count == 2
        first_batch = mock_pipeline_store.store_chunks_batch.call_args_list[0][0][0]
        second_batch = mock_pipeline_store.store_chunks_batch.call_args_list[1][0][0]
        assert len(first_batch) == 50
        assert len(second_batch) == 5

    @pytest.mark.asyncio
    async def test_pipeline_chunk_includes_file_skeleton(
        self, mock_pool, mock_producer, mock_pipeline_store, ingestion_id
    ):
        """Pipeline chunk dict should include formatted file_skeleton."""
        skeleton_jsonb = ["class App", "def index"]
        rows = [self._make_chunk_row(0, 1, skeleton=skeleton_jsonb, ingestion_id=ingestion_id)]
        mock_pool.fetch.return_value = rows
        mock_pool.fetchval.return_value = 1

        await publish_enriched_chunks(
            mock_pool,
            mock_producer,
            "owner/repo",
            "main",
            ingestion_id=ingestion_id,
            pipeline_store=mock_pipeline_store,
        )

        batch = mock_pipeline_store.store_chunks_batch.call_args[0][0]
        chunk_dict = batch[0]
        assert chunk_dict["file_skeleton"] == "  class App\n  def index"

    @pytest.mark.asyncio
    async def test_kafka_message_includes_file_skeleton(
        self, mock_pool, mock_producer, ingestion_id
    ):
        """Kafka message JSON should include file_skeleton field."""
        skeleton_jsonb = ["def hello"]
        rows = [self._make_chunk_row(0, 1, skeleton=skeleton_jsonb, ingestion_id=ingestion_id)]
        mock_pool.fetch.return_value = rows
        mock_pool.fetchval.return_value = 1

        await publish_enriched_chunks(
            mock_pool,
            mock_producer,
            "owner/repo",
            "main",
            ingestion_id=ingestion_id,
        )

        value_bytes = mock_producer.send.call_args[1]["value"]
        msg = json.loads(value_bytes.decode("utf-8"))
        assert msg["file_skeleton"] == "  def hello"

    @pytest.mark.asyncio
    async def test_pipeline_store_failure_does_not_block_kafka(
        self, mock_pool, mock_producer, mock_pipeline_store, ingestion_id
    ):
        """If pipeline_store.store_chunks_batch fails, Kafka publishing should continue."""
        mock_pipeline_store.store_chunks_batch.side_effect = Exception("DB error")
        rows = [self._make_chunk_row(0, 1, ingestion_id=ingestion_id)]
        mock_pool.fetch.return_value = rows
        mock_pool.fetchval.return_value = 1

        result = await publish_enriched_chunks(
            mock_pool,
            mock_producer,
            "owner/repo",
            "main",
            ingestion_id=ingestion_id,
            pipeline_store=mock_pipeline_store,
        )

        # Should still publish to Kafka despite pipeline_store failure
        assert result == 1
        mock_producer.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_pipeline_store_skips_staging(self, mock_pool, mock_producer, ingestion_id):
        """When pipeline_store is None, chunk staging should be skipped."""
        rows = [self._make_chunk_row(0, 1, ingestion_id=ingestion_id)]
        mock_pool.fetch.return_value = rows
        mock_pool.fetchval.return_value = 1

        result = await publish_enriched_chunks(
            mock_pool,
            mock_producer,
            "owner/repo",
            "main",
            ingestion_id=ingestion_id,
            pipeline_store=None,
        )

        assert result == 1


# ── db_init: pipeline_counters table schema tests ─────────────────────────


class _AsyncContextManager:
    """Helper: async context manager that returns a predetermined value."""

    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, *args):
        return False


def _make_db_init_mocks():
    """Create mock_pool whose .acquire() returns an async context manager."""
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value = _AsyncContextManager(mock_conn)
    return mock_pool, mock_conn


class TestDbInitPipelineCountersSchema:
    """Verify the SQL in db_init.py creates the correct schema."""

    @pytest.mark.asyncio
    async def test_ensure_tracking_tables_creates_pipeline_counters(self):
        """ensure_tracking_tables should create pipeline_counters with UNIQUE constraint."""
        from code_preprocessor.storage.db_init import ensure_tracking_tables

        mock_pool, mock_conn = _make_db_init_mocks()
        await ensure_tracking_tables(mock_pool)

        # Collect all SQL executed
        all_sql = [call_args[0][0] for call_args in mock_conn.execute.call_args_list]
        all_sql_joined = "\n".join(all_sql)

        # Check pipeline_counters table creation (uses schema-qualified name)
        assert "CREATE TABLE IF NOT EXISTS code_processing.pipeline_counters" in all_sql_joined
        assert "UNIQUE(ingestion_id, service_name, counter_name)" in all_sql_joined

        # Check pipeline_chunks table creation (uses schema-qualified name)
        assert "CREATE TABLE IF NOT EXISTS code_processing.pipeline_chunks" in all_sql_joined
        assert "chunk_id UUID PRIMARY KEY" in all_sql_joined

        # Check indexes
        assert "idx_pipeline_counters_ingestion" in all_sql_joined
        assert "idx_pipeline_chunks_ingestion" in all_sql_joined

    @pytest.mark.asyncio
    async def test_pipeline_counters_columns(self):
        """pipeline_counters should have the expected columns."""
        from code_preprocessor.storage.db_init import ensure_tracking_tables

        mock_pool, mock_conn = _make_db_init_mocks()
        await ensure_tracking_tables(mock_pool)

        all_sql = [call_args[0][0] for call_args in mock_conn.execute.call_args_list]
        counters_sql = [s for s in all_sql if "pipeline_counters" in s and "CREATE TABLE" in s]
        assert len(counters_sql) == 1

        schema = counters_sql[0]
        assert "ingestion_id UUID NOT NULL" in schema
        assert "service_name VARCHAR(50) NOT NULL" in schema
        assert "counter_name VARCHAR(50) NOT NULL" in schema
        assert "counter_value INTEGER NOT NULL DEFAULT 0" in schema
        assert "status VARCHAR(20) NOT NULL DEFAULT 'running'" in schema

    @pytest.mark.asyncio
    async def test_pipeline_chunks_has_file_skeleton(self):
        """pipeline_chunks should include file_skeleton column."""
        from code_preprocessor.storage.db_init import ensure_tracking_tables

        mock_pool, mock_conn = _make_db_init_mocks()
        await ensure_tracking_tables(mock_pool)

        all_sql = [call_args[0][0] for call_args in mock_conn.execute.call_args_list]
        chunks_sql = [s for s in all_sql if "pipeline_chunks" in s and "CREATE TABLE" in s]
        assert len(chunks_sql) == 1
        assert "file_skeleton TEXT" in chunks_sql[0]

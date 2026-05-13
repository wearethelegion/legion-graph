"""Tests for v2 pipeline counter integration.

Verifies that IngestionProcessor.process_ingestion calls PipelineStore
counter methods at the correct points in the pipeline, including the
early-exit path.

Adapted from the original consumer._persist_changes tests to work with
the refactored IngestionProcessor + EventEmitter + consumer split.
"""

import asyncio
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from code_preprocessor.storage.pipeline_store import PipelineStore


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def ingestion_id():
    return str(uuid.uuid4())


@pytest.fixture
def mock_pipeline_store():
    """Mock PipelineStore with all async methods."""
    store = AsyncMock(spec=PipelineStore)
    store.set_counter = AsyncMock()
    store.increment_counter = AsyncMock()
    store.finalize_counters = AsyncMock()
    store.store_chunks_batch = AsyncMock(return_value=0)
    store.get_counters = AsyncMock(return_value={})
    return store


@pytest.fixture
def mock_diff():
    """Create a minimal GitDiffResult with some changes."""

    class FakeChange:
        def __init__(self, path, change_type="M"):
            self.file_path = path
            self.change_type = change_type
            self.previous_path = None

    class FakeDiff:
        def __init__(self, changes):
            self.repository = "owner/repo"
            self.branch = "main"
            self.repo_path = Path("/tmp/fake_repo")
            self.new_commit = "abc123"
            self.old_commit = "def456"
            self.is_initial_clone = False
            self.force_full_refresh = False
            self.changes = changes

    return FakeDiff, FakeChange


def _make_processor(
    pipeline_store=None,
    ingestion_store=None,
    db_pool=None,
    producer=None,
    version_store=None,
    event_emitter=None,
):
    """Create an IngestionProcessor with mocked dependencies."""
    from code_preprocessor.kafka_processing_service.ingestion_processor import (
        IngestionProcessor,
    )

    settings = MagicMock()
    settings.max_concurrent_files = 5
    settings.progress_update_interval = 10
    settings.embed_workers = 2
    settings.embed_batch_size = 8
    settings.embed_batch_timeout = 0.1

    return IngestionProcessor(
        settings=settings,
        db_pool=db_pool,
        version_store=version_store,
        ingestion_store=ingestion_store,
        pipeline_store=pipeline_store,
        producer=producer,
        event_emitter=event_emitter,
    )


# ── Counter tracking in process_ingestion ─────────────────────────────────


class TestProcessorCounterTracking:
    """Test that pipeline counters are set at correct points in process_ingestion."""

    @pytest.mark.asyncio
    async def test_files_discovered_set_on_entry(
        self, mock_pipeline_store, mock_diff, ingestion_id
    ):
        """files_discovered should be set to len(diff.changes) immediately."""
        FakeDiff, FakeChange = mock_diff
        changes = [FakeChange("a.py"), FakeChange("b.py"), FakeChange("c.py")]
        diff = FakeDiff(changes)

        processor = _make_processor(pipeline_store=mock_pipeline_store)

        await processor.process_ingestion(
            diff, "python", "proj-1", "comp-1", "user-1", ingestion_id, file_tree=""
        )

        # files_discovered should be the first counter set
        calls = mock_pipeline_store.set_counter.call_args_list
        first_call = calls[0]
        assert first_call[0] == (ingestion_id, "files_discovered", 3)

    @pytest.mark.asyncio
    async def test_files_filtered_set_after_filtering(
        self, mock_pipeline_store, mock_diff, ingestion_id
    ):
        """files_filtered should be set to count of files remaining after filter."""
        FakeDiff, FakeChange = mock_diff
        changes = [FakeChange("a.py"), FakeChange("b.py")]
        diff = FakeDiff(changes)

        processor = _make_processor(pipeline_store=mock_pipeline_store)

        with patch(
            "code_preprocessor.kafka_processing_service.ingestion_processor.FileFilter"
        ) as MockFilter:
            mock_filter_instance = MagicMock()
            # Make all files pass filtering
            mock_result = MagicMock()
            mock_result.filtered = False
            mock_filter_instance.check.return_value = mock_result
            mock_filter_instance.get_file_size.return_value = 100
            MockFilter.return_value = mock_filter_instance

            await processor.process_ingestion(
                diff, "python", "proj-1", "comp-1", "user-1", ingestion_id, file_tree=""
            )

        # files_filtered should be set to 2 (all files pass)
        set_counter_calls = {
            (c[0][1], c[0][2]) for c in mock_pipeline_store.set_counter.call_args_list
        }
        assert ("files_discovered", 2) in set_counter_calls
        assert ("files_filtered", 2) in set_counter_calls

    @pytest.mark.asyncio
    async def test_early_exit_all_filtered_sets_zero_counters(
        self, mock_pipeline_store, mock_diff, ingestion_id
    ):
        """When all files are filtered, remaining counters should be set to 0."""
        FakeDiff, FakeChange = mock_diff
        changes = [FakeChange("a.min.js"), FakeChange("b.min.js")]
        diff = FakeDiff(changes)

        mock_emitter = AsyncMock()
        processor = _make_processor(
            pipeline_store=mock_pipeline_store,
            ingestion_store=AsyncMock(),
            event_emitter=mock_emitter,
        )
        processor._ingestion_store.mark_completed = AsyncMock()

        with patch(
            "code_preprocessor.kafka_processing_service.ingestion_processor.FileFilter"
        ) as MockFilter:
            mock_filter_instance = MagicMock()
            # Filter ALL files
            mock_result = MagicMock()
            mock_result.filtered = True
            mock_result.reason = MagicMock()
            mock_result.reason.value = "extension"
            mock_result.detail = "*.min.js excluded"
            mock_filter_instance.check.return_value = mock_result
            mock_filter_instance.get_file_size.return_value = 100
            MockFilter.return_value = mock_filter_instance

            await processor.process_ingestion(
                diff, "js", "proj-1", "comp-1", "user-1", ingestion_id, file_tree=""
            )

        # Check that zero counters were set
        set_counter_calls = mock_pipeline_store.set_counter.call_args_list
        counter_map = {c[0][1]: c[0][2] for c in set_counter_calls}

        assert counter_map.get("files_parsed") == 0
        assert counter_map.get("chunks_produced") == 0
        assert counter_map.get("embeddings_computed") == 0

        # Counters should be finalized
        mock_pipeline_store.finalize_counters.assert_called_once_with(ingestion_id)

    @pytest.mark.asyncio
    async def test_early_exit_emits_ingestion_complete_with_zero(
        self, mock_pipeline_store, mock_diff, ingestion_id
    ):
        """Early exit path should emit ingestion_complete with total_files=0, total_chunks=0."""
        FakeDiff, FakeChange = mock_diff
        changes = [FakeChange("data.csv")]
        diff = FakeDiff(changes)

        mock_emitter = AsyncMock()
        mock_emitter.emit_ingestion_complete = AsyncMock()
        processor = _make_processor(
            pipeline_store=mock_pipeline_store,
            ingestion_store=AsyncMock(),
            event_emitter=mock_emitter,
        )
        processor._ingestion_store.mark_completed = AsyncMock()

        with patch(
            "code_preprocessor.kafka_processing_service.ingestion_processor.FileFilter"
        ) as MockFilter:
            mock_filter_instance = MagicMock()
            mock_result = MagicMock()
            mock_result.filtered = True
            mock_result.reason = MagicMock()
            mock_result.reason.value = "extension"
            mock_result.detail = "*.csv excluded"
            mock_filter_instance.check.return_value = mock_result
            mock_filter_instance.get_file_size.return_value = 50
            MockFilter.return_value = mock_filter_instance

            await processor.process_ingestion(
                diff, "data", "proj-1", "comp-1", "user-1", ingestion_id, file_tree=""
            )

            # emit_ingestion_complete should be called with 0 files, 0 chunks
            mock_emitter.emit_ingestion_complete.assert_called_once_with(
                ingestion_id=ingestion_id,
                company_id="comp-1",
                project_id="proj-1",
                total_files=0,
                total_chunks=0,
            )

    @pytest.mark.asyncio
    async def test_finalize_counters_called_on_normal_completion(
        self, mock_pipeline_store, mock_diff, ingestion_id
    ):
        """On normal completion, finalize_counters should be called."""
        FakeDiff, FakeChange = mock_diff
        changes = [FakeChange("a.py")]
        diff = FakeDiff(changes)

        mock_emitter = AsyncMock()
        mock_db = AsyncMock()
        mock_db.execute = AsyncMock(return_value="DELETE 0")
        mock_db.fetchval = AsyncMock(return_value=0)
        mock_vs = AsyncMock()
        mock_vs.record_change = AsyncMock(return_value={"_id": "doc-1", "content_b64": None})

        processor = _make_processor(
            pipeline_store=mock_pipeline_store,
            ingestion_store=AsyncMock(),
            db_pool=mock_db,
            producer=AsyncMock(),
            version_store=mock_vs,
            event_emitter=mock_emitter,
        )
        processor._ingestion_store.mark_completed = AsyncMock()
        processor._ingestion_store.update_progress = AsyncMock()

        with patch(
            "code_preprocessor.kafka_processing_service.ingestion_processor.FileFilter"
        ) as MockFilter:
            mock_filter_instance = MagicMock()
            mock_result = MagicMock()
            mock_result.filtered = False
            mock_filter_instance.check.return_value = mock_result
            mock_filter_instance.get_file_size.return_value = 100
            MockFilter.return_value = mock_filter_instance

            with (
                patch(
                    "code_preprocessor.kafka_processing_service._file_processing.process_file",
                    new_callable=AsyncMock,
                    return_value=None,
                ),
                patch(
                    "code_preprocessor.kafka_processing_service.ingestion_processor.embed_and_publish_batch",
                    new_callable=AsyncMock,
                    return_value=5,
                ),
                patch(
                    "code_preprocessor.kafka_processing_service.ingestion_processor.store_project_tree",
                    new_callable=AsyncMock,
                ),
                patch(
                    "code_preprocessor.project_classifier.analyze_project",
                    new_callable=AsyncMock,
                    return_value=None,
                ),
            ):
                await processor.process_ingestion(
                    diff, "python", "proj-1", "comp-1", "user-1", ingestion_id, file_tree="tree"
                )

        mock_pipeline_store.finalize_counters.assert_called_once_with(ingestion_id)


# ── PipelineStore initialization in consumer ──────────────────────────────


class TestConsumerPipelineStoreInit:
    def test_initialise_pipeline_store_with_pool(self):
        """_initialise_pipeline_store should create a PipelineStore when pool exists."""
        from code_preprocessor.kafka_processing_service.consumer import (
            RepositoryIngestionConsumer,
        )

        mock_pool = MagicMock()
        consumer = RepositoryIngestionConsumer.__new__(RepositoryIngestionConsumer)
        consumer._db_pool = mock_pool

        store = consumer._initialise_pipeline_store()

        assert isinstance(store, PipelineStore)
        assert store._pool is mock_pool

    def test_initialise_pipeline_store_without_pool(self):
        """_initialise_pipeline_store should return None when no pool."""
        from code_preprocessor.kafka_processing_service.consumer import (
            RepositoryIngestionConsumer,
        )

        consumer = RepositoryIngestionConsumer.__new__(RepositoryIngestionConsumer)
        consumer._db_pool = None

        store = consumer._initialise_pipeline_store()

        assert store is None


# ── Counter non-blocking behavior ─────────────────────────────────────────


class TestCounterNonBlocking:
    """Counter failures should not interrupt the pipeline."""

    @pytest.mark.asyncio
    async def test_set_counter_failure_does_not_propagate(
        self, mock_pipeline_store, mock_diff, ingestion_id
    ):
        """If set_counter raises, processing should continue."""
        FakeDiff, FakeChange = mock_diff
        changes = [FakeChange("a.py")]
        diff = FakeDiff(changes)

        # Make set_counter fail
        mock_pipeline_store.set_counter.side_effect = Exception("DB connection lost")

        processor = _make_processor(pipeline_store=mock_pipeline_store)

        with patch(
            "code_preprocessor.kafka_processing_service.ingestion_processor.FileFilter"
        ) as MockFilter:
            mock_filter_instance = MagicMock()
            mock_result = MagicMock()
            mock_result.filtered = False
            mock_filter_instance.check.return_value = mock_result
            mock_filter_instance.get_file_size.return_value = 100
            MockFilter.return_value = mock_filter_instance

            # Should NOT raise despite counter failures
            await processor.process_ingestion(
                diff, "python", "proj-1", "comp-1", "user-1", ingestion_id, file_tree=""
            )

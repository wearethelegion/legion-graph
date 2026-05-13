"""Tests for SummarizationProcessor.

Mocks:
- summarize_text (LLM call)
- SummarizationStore (Postgres)
- EnrichedChunkMessage (input)
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from summarization_service.models import TextSummaryEvent
from summarization_service.pipeline_store import SummarizationStore


def _mock_summaries(text: str) -> list:
    """Build a mock return value matching summarize_text's list[TextSummary] shape."""
    return [SimpleNamespace(text=text)]


def _make_chunk_msg(**overrides):
    """Build a mock EnrichedChunkMessage."""
    from cognee_service.kafka_consumer.enriched_chunks.models import EnrichedChunkMessage

    defaults = dict(
        action="process",
        company_id="comp-1",
        project_id="proj-1",
        repository="my-repo",
        branch="main",
        file_path="src/app.py",
        ingestion_id="ing-1",
        chunk_id="chunk-1",
        parent_id="parent-1",
        file_version_id="fv-1",
        language="python",
        chunk_index=0,
        total_chunks=3,
        content="class Foo:\n    pass",
        header="# FILE: src/app.py",
    )
    defaults.update(overrides)
    return EnrichedChunkMessage(**defaults)


@pytest.fixture
def mock_store():
    """Create a mock SummarizationStore."""
    store = AsyncMock(spec=SummarizationStore)
    store.increment_counter = AsyncMock()
    store.has_checkpoint = AsyncMock(return_value=False)  # Not processed yet by default
    store.save_checkpoint = AsyncMock()
    store.check_checkpoint = AsyncMock(
        return_value=False
    )  # Deprecated method (always needs processing)
    store.get_counter = AsyncMock(return_value=0)
    store.get_preprocessor_total_chunks = AsyncMock(return_value=None)
    return store


@pytest.fixture
def mock_config():
    """Create a mock config class."""

    class MockConfig:
        MAX_PARALLEL_WORKERS = 5
        MAX_RETRIES = 2
        RETRY_BASE_DELAY = 0.01  # Fast retries in tests
        VERTEXAI_LLM_REGIONS = "us-central1,us-east1"

    return MockConfig


@pytest.fixture
def processor(mock_store, mock_config):
    """Create a SummarizationProcessor with mocked dependencies."""
    from summarization_service.processor import SummarizationProcessor

    return SummarizationProcessor(
        store=mock_store,
        config=mock_config,
    )


class TestProcessorInit:
    """Test processor initialization."""

    def test_creates_semaphore(self, processor, mock_config):
        assert processor._semaphore._value == mock_config.MAX_PARALLEL_WORKERS


class TestProcessBatch:
    """Test batch processing flow."""

    @pytest.mark.asyncio
    async def test_empty_batch_returns_empty(self, processor):
        result = await processor.process_batch([])
        assert result == []

    @pytest.mark.asyncio
    async def test_successful_summarization(self, processor, mock_store):
        """Test full summarization flow with mocked LLM."""
        with patch(
            "summarization_service.processor.summarize_text",
            new_callable=AsyncMock,
            return_value=_mock_summaries("This class defines Foo with no methods."),
        ):
            messages = [_make_chunk_msg()]
            events = await processor.process_batch(messages)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, TextSummaryEvent)
        assert event.ingestion_id == "ing-1"
        assert event.chunk_id == "chunk-1"
        assert event.company_id == "comp-1"
        assert event.chunk_index == 0
        assert event.summary_text == "This class defines Foo with no methods."
        assert event.summary_id  # auto-generated UUID

        # Verify checkpoint flow: check before processing, save after success
        mock_store.increment_counter.assert_any_call("ing-1", "chunks_received", 1)
        mock_store.has_checkpoint.assert_called_once()
        mock_store.save_checkpoint.assert_called_once()

    @pytest.mark.asyncio
    async def test_header_prepended_to_content(self, processor, mock_store):
        """Verify LLM input combines header + content."""
        with patch(
            "summarization_service.processor.summarize_text",
            new_callable=AsyncMock,
            return_value=_mock_summaries("summary"),
        ) as mock_summarize:
            msg = _make_chunk_msg(
                header="# FILE: test.py",
                content="x = 1",
            )
            await processor.process_batch([msg])

            # First positional arg is a list of chunk objects; inspect the text
            call_args = mock_summarize.call_args
            chunk_list = call_args[0][0]
            llm_text = chunk_list[0].text
            assert "# FILE: test.py" in llm_text
            assert "x = 1" in llm_text

    @pytest.mark.asyncio
    async def test_no_header_uses_content_only(self, processor, mock_store):
        """When header is empty, only content is passed to LLM."""
        with patch(
            "summarization_service.processor.summarize_text",
            new_callable=AsyncMock,
            return_value=_mock_summaries("summary"),
        ) as mock_summarize:
            msg = _make_chunk_msg(header="", content="y = 2")
            await processor.process_batch([msg])

            chunk_list = mock_summarize.call_args[0][0]
            llm_text = chunk_list[0].text
            assert llm_text == "y = 2"

    @pytest.mark.asyncio
    async def test_multiple_chunks_in_batch(self, processor, mock_store):
        """Test processing multiple chunks in one batch."""
        with patch(
            "summarization_service.processor.summarize_text",
            new_callable=AsyncMock,
            return_value=_mock_summaries("A summary."),
        ):
            messages = [
                _make_chunk_msg(chunk_id="c1"),
                _make_chunk_msg(chunk_id="c2"),
                _make_chunk_msg(chunk_id="c3"),
            ]
            events = await processor.process_batch(messages)

        assert len(events) == 3
        chunk_ids = {e.chunk_id for e in events}
        assert chunk_ids == {"c1", "c2", "c3"}

    @pytest.mark.asyncio
    async def test_counter_increments(self, processor, mock_store):
        """Verify correct counter increment calls."""
        with patch(
            "summarization_service.processor.summarize_text",
            new_callable=AsyncMock,
            return_value=_mock_summaries("A summary."),
        ):
            await processor.process_batch([_make_chunk_msg()])

        # Should increment: chunks_received, summaries_produced
        counter_calls = {
            (c.args[1], c.args[2]) for c in mock_store.increment_counter.call_args_list
        }
        assert ("chunks_received", 1) in counter_calls
        assert ("summaries_produced", 1) in counter_calls

    @pytest.mark.asyncio
    async def test_summary_id_stored_matches_event(self, processor, mock_store):
        """Verify the summary_id in Postgres matches the one in the Kafka event."""
        with patch(
            "summarization_service.processor.summarize_text",
            new_callable=AsyncMock,
            return_value=_mock_summaries("Some summary."),
        ):
            events = await processor.process_batch([_make_chunk_msg()])

        assert len(events) == 1
        event = events[0]

        # The event should have a valid summary_id
        assert event.summary_id
        # Checkpoint should have been checked via the non-destructive API
        mock_store.has_checkpoint.assert_called_once()


class TestRetryLogic:
    """Test summarization retry with exponential backoff."""

    @pytest.mark.asyncio
    async def test_retries_on_llm_failure(self, processor, mock_store):
        """Should retry up to MAX_RETRIES on LLM failure."""
        call_count = 0

        async def failing_summarize(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("LLM timeout")
            return _mock_summaries("Summary after retry.")

        with patch(
            "summarization_service.processor.summarize_text",
            side_effect=failing_summarize,
        ):
            events = await processor.process_batch([_make_chunk_msg()])

        assert len(events) == 1
        assert events[0].summary_text == "Summary after retry."
        assert call_count == 2  # Failed once, succeeded second time

    @pytest.mark.asyncio
    async def test_returns_none_after_exhausted_retries(self, processor, mock_store):
        """Should return None (skip chunk) after all retries exhausted."""
        with patch(
            "summarization_service.processor.summarize_text",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Permanent LLM failure"),
        ):
            events = await processor.process_batch([_make_chunk_msg()])

        # Chunk should be skipped (not in events)
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_failed_chunks_dont_block_others(self, processor, mock_store):
        """One failing chunk should not prevent others from succeeding."""
        per_chunk_calls = {}

        async def selective_failure(chunks, *args, **kwargs):
            text = chunks[0].text if chunks else ""
            is_fail_chunk = "FAIL_MARKER" in text
            chunk_key = "fail" if is_fail_chunk else "ok"
            per_chunk_calls.setdefault(chunk_key, 0)
            per_chunk_calls[chunk_key] += 1

            if is_fail_chunk:
                raise RuntimeError("permanent failure for this chunk")

            return _mock_summaries("Good summary.")

        with patch(
            "summarization_service.processor.summarize_text",
            side_effect=selective_failure,
        ):
            messages = [
                _make_chunk_msg(chunk_id="fail-chunk", content="FAIL_MARKER code here"),
                _make_chunk_msg(chunk_id="ok-chunk", content="good code here"),
            ]
            events = await processor.process_batch(messages)

        # Only the successful chunk should produce an event
        assert len(events) == 1
        assert events[0].chunk_id == "ok-chunk"

        # The failing chunk should have been retried MAX_RETRIES times
        assert per_chunk_calls.get("fail", 0) == processor._config.MAX_RETRIES


class TestCheckIngestionComplete:
    """Test ingestion completion check."""

    @pytest.mark.asyncio
    async def test_complete_when_received_equals_total(self, processor, mock_store):
        mock_store.get_counter.return_value = 10
        mock_store.get_preprocessor_total_chunks.return_value = 10

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is True
        assert received == 10
        assert total == 10

    @pytest.mark.asyncio
    async def test_not_complete_when_received_less_than_total(self, processor, mock_store):
        mock_store.get_counter.return_value = 5
        mock_store.get_preprocessor_total_chunks.return_value = 10

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is False
        assert received == 5
        assert total == 10

    @pytest.mark.asyncio
    async def test_not_complete_when_preprocessor_not_done(self, processor, mock_store):
        mock_store.get_counter.return_value = 5
        mock_store.get_preprocessor_total_chunks.return_value = None

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is False
        assert received == 5
        assert total is None

    @pytest.mark.asyncio
    async def test_complete_when_received_exceeds_total(self, processor, mock_store):
        """Edge case: received > total (e.g. reprocessing)."""
        mock_store.get_counter.return_value = 15
        mock_store.get_preprocessor_total_chunks.return_value = 10

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is True
        assert received == 15
        assert total == 10

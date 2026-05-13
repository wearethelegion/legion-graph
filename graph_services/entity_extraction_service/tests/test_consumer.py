"""Tests for EntityExtractionConsumer.

Mocks: AIOKafkaConsumer, AIOKafkaProducer, EntityExtractionProcessor,
       multi-tenancy functions, all Kafka I/O.
"""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from entity_extraction_service.models import (
    ExtractedEntitiesEvent,
    EntityPayload,
    PipelineEvent,
)


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
        language="python",
        chunk_index=0,
        total_chunks=3,
        content="class Foo: pass",
        header="# FILE: src/app.py",
    )
    defaults.update(overrides)
    return EnrichedChunkMessage(**defaults)


@pytest.fixture
def mock_processor():
    """Create a mock EntityExtractionProcessor."""
    proc = AsyncMock()
    proc.process_batch = AsyncMock(return_value=[])
    proc.check_ingestion_complete = AsyncMock(return_value=(False, 0, None))
    proc._store = AsyncMock()
    proc._store.get_all_counters = AsyncMock(return_value={})
    proc._store.finalize_counters = AsyncMock()
    return proc


@pytest.fixture
def mock_config():
    """Create a mock config."""

    class MockConfig:
        KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
        KAFKA_INPUT_TOPIC = "enriched-code-chunks"
        KAFKA_OUTPUT_TOPIC = "extracted-entities"
        KAFKA_EVENTS_TOPIC = "pipeline-events"
        KAFKA_CONSUMER_GROUP_ID = "test-group"
        KAFKA_AUTO_COMMIT = True
        KAFKA_AUTO_OFFSET_RESET = "earliest"
        KAFKA_FETCH_TIMEOUT_MS = 100
        BATCH_SIZE = 3
        MAX_PARALLEL_WORKERS = 5

    return MockConfig


@pytest.fixture
def consumer(mock_processor, mock_config):
    """Create an EntityExtractionConsumer with mocked dependencies."""
    from entity_extraction_service.consumer import EntityExtractionConsumer

    return EntityExtractionConsumer(
        processor=mock_processor,
        config=mock_config,
    )


class TestConsumerInit:
    """Test consumer initialization state."""

    def test_initial_state(self, consumer):
        assert consumer._consumer is None
        assert consumer._producer is None
        assert consumer._running is False
        assert len(consumer._completed_ingestions) == 0


class TestDeserializeMessage:
    """Test Kafka message deserialization."""

    def test_deserialize_valid_message(self, consumer):
        data = {
            "action": "process",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "repository": "repo",
            "branch": "main",
            "file_path": "test.py",
            "ingestion_id": "ing-1",
            "chunk_id": "chunk-1",
            "content": "x = 1",
        }
        raw = json.dumps(data).encode("utf-8")
        result = consumer._deserialize_message(raw)
        assert result.company_id == "comp-1"
        assert result.content == "x = 1"

    def test_deserialize_invalid_json(self, consumer):
        with pytest.raises(ValueError, match="Invalid message format"):
            consumer._deserialize_message(b"not json")

    def test_deserialize_missing_required_fields(self, consumer):
        data = {"action": "process"}  # Missing required fields
        raw = json.dumps(data).encode("utf-8")
        with pytest.raises(ValueError, match="Invalid message format"):
            consumer._deserialize_message(raw)


class TestSerializeEvent:
    """Test Kafka event serialization."""

    def test_serialize_pydantic_model(self, consumer):
        event = ExtractedEntitiesEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
        )
        result = consumer._serialize_event(event)
        assert isinstance(result, bytes)
        parsed = json.loads(result)
        assert parsed["ingestion_id"] == "ing-1"
        assert parsed["chunk_id"] == "chunk-1"

    def test_serialize_dict(self, consumer):
        data = {"key": "value"}
        result = consumer._serialize_event(data)
        assert json.loads(result) == {"key": "value"}


class TestProcessChunk:
    """Test _process_chunk behavior (streaming architecture)."""

    @pytest.mark.asyncio
    async def test_calls_processor_extract(self, consumer, mock_processor):
        """Verify processor._extract_with_retries is called for the chunk."""
        event = ExtractedEntitiesEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            entities=[
                EntityPayload(entity_id="e1", name="X", entity_type="class", description="X class")
            ],
        )
        mock_processor._extract_with_retries = AsyncMock(return_value=event)
        mock_processor._store.increment_counter = AsyncMock()

        # Need a producer to publish events
        consumer._producer = AsyncMock()
        consumer._producer.send_and_wait = AsyncMock()

        chunk = _make_chunk_msg()
        await consumer._process_chunk(chunk)

        mock_processor._extract_with_retries.assert_called_once()

    @pytest.mark.asyncio
    async def test_publishes_event_to_output_topic(self, consumer, mock_processor):
        """Verify event is published to the output Kafka topic."""
        event = ExtractedEntitiesEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
        )
        mock_processor._extract_with_retries = AsyncMock(return_value=event)
        mock_processor._store.increment_counter = AsyncMock()

        consumer._producer = AsyncMock()
        consumer._producer.send_and_wait = AsyncMock()

        await consumer._process_chunk(_make_chunk_msg())

        consumer._producer.send_and_wait.assert_called()
        call_args = consumer._producer.send_and_wait.call_args_list
        # Should publish to output topic
        assert call_args[0][0][0] == "extracted-entities"

    @pytest.mark.asyncio
    async def test_handles_processor_exception(self, consumer, mock_processor):
        """Should log error and return without crashing on extraction failure."""
        mock_processor._extract_with_retries = AsyncMock(return_value=None)
        mock_processor._store.increment_counter = AsyncMock()

        consumer._producer = AsyncMock()

        # Should not raise
        await consumer._process_chunk(_make_chunk_msg())

        # Producer should NOT have been called (no event to publish)
        consumer._producer.send_and_wait.assert_not_called()


class TestCompletionCheck:
    """Test ingestion completion detection and event emission."""

    @pytest.mark.asyncio
    async def test_emits_completion_event_when_done(self, consumer, mock_processor):
        """When all chunks processed, should emit extraction_complete."""
        mock_processor.check_ingestion_complete.return_value = (True, 10, 10)
        mock_processor._store.get_all_counters.return_value = {
            "chunks_received": 10,
            "entities_extracted": 50,
            "edges_extracted": 30,
        }

        consumer._producer = AsyncMock()
        consumer._producer.send_and_wait = AsyncMock()

        await consumer._check_and_emit_completion("ing-1", "comp-1", "proj-1")

        # Should have published to pipeline-events
        consumer._producer.send_and_wait.assert_called_once()
        topic, event = consumer._producer.send_and_wait.call_args[0]
        assert topic == "pipeline-events"
        assert isinstance(event, PipelineEvent)
        assert event.event_type == "extraction_complete"
        assert event.chunks_processed == 10
        assert event.total_entities == 50
        assert event.total_edges == 30

        # Should mark ingestion as completed
        assert "ing-1" in consumer._completed_ingestions

        # Should finalize counters
        mock_processor._store.finalize_counters.assert_called_once_with("ing-1")

    @pytest.mark.asyncio
    async def test_no_event_when_not_complete(self, consumer, mock_processor):
        """Should not emit when chunks still pending."""
        mock_processor.check_ingestion_complete.return_value = (False, 5, 10)

        consumer._producer = AsyncMock()
        consumer._producer.send_and_wait = AsyncMock()

        await consumer._check_and_emit_completion("ing-1", "comp-1", "proj-1")

        consumer._producer.send_and_wait.assert_not_called()
        assert "ing-1" not in consumer._completed_ingestions

    @pytest.mark.asyncio
    async def test_no_duplicate_completion_events(self, consumer, mock_processor):
        """Already-completed ingestions should not emit again."""
        consumer._completed_ingestions.add("ing-1")

        consumer._producer = AsyncMock()
        mock_processor._extract_with_retries = AsyncMock(
            return_value=ExtractedEntitiesEvent(
                ingestion_id="ing-1",
                chunk_id="chunk-1",
                company_id="comp-1",
                project_id="proj-1",
                file_version_id="fv-1",
            )
        )
        mock_processor._store.increment_counter = AsyncMock()
        consumer._producer.send_and_wait = AsyncMock()

        await consumer._process_chunk(_make_chunk_msg())

        # check_ingestion_complete should NOT be called since ing-1 is in _completed
        mock_processor.check_ingestion_complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_completion_check_error(self, consumer, mock_processor):
        """Errors during completion check should be caught and logged."""
        mock_processor.check_ingestion_complete.side_effect = RuntimeError("DB error")

        consumer._producer = AsyncMock()
        consumer._producer.send_and_wait = AsyncMock()

        # Should not raise
        await consumer._check_and_emit_completion("ing-1", "comp-1", "proj-1")


class TestPublishEvent:
    """Test _publish_event method."""

    @pytest.mark.asyncio
    async def test_publish_with_no_producer(self, consumer):
        """Should handle gracefully when producer is None."""
        consumer._producer = None
        # Should not raise
        await consumer._publish_event("topic", {"data": 1})

    @pytest.mark.asyncio
    async def test_publish_handles_send_error(self, consumer):
        """Should catch and log send errors."""
        consumer._producer = AsyncMock()
        consumer._producer.send_and_wait = AsyncMock(side_effect=RuntimeError("Kafka down"))

        # Should not raise
        await consumer._publish_event("topic", {"data": 1})


class TestStop:
    """Test graceful shutdown."""

    @pytest.mark.asyncio
    async def test_stop_sets_flags(self, consumer):
        consumer._running = True
        consumer._consumer = AsyncMock()
        consumer._producer = AsyncMock()
        consumer._work_queue = AsyncMock()
        consumer._workers = []  # No workers to wait for in this test

        await consumer.stop()

        assert consumer._running is False
        assert consumer._shutdown_event.is_set()
        assert consumer._consumer is None
        assert consumer._producer is None

    @pytest.mark.asyncio
    async def test_stop_handles_producer_error(self, consumer):
        consumer._producer = AsyncMock()
        consumer._producer.stop = AsyncMock(side_effect=RuntimeError("stop failed"))
        consumer._consumer = AsyncMock()

        # Should not raise
        await consumer.stop()
        assert consumer._producer is None

    @pytest.mark.asyncio
    async def test_stop_handles_consumer_error(self, consumer):
        consumer._consumer = AsyncMock()
        consumer._consumer.stop = AsyncMock(side_effect=RuntimeError("stop failed"))
        consumer._producer = None

        # Should not raise
        await consumer.stop()
        assert consumer._consumer is None

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self, consumer):
        """Should be safe to stop when never started."""
        await consumer.stop()
        assert consumer._running is False


class TestConsumeLoop:
    """Test the main consume() loop behavior (streaming architecture)."""

    @pytest.mark.asyncio
    async def test_consume_requires_start(self, consumer):
        """Should raise RuntimeError if consume called before start."""
        with pytest.raises(RuntimeError, match="Consumer not started"):
            await consumer.consume()

    @pytest.mark.asyncio
    async def test_consume_enqueues_messages(self, consumer, mock_processor):
        """Consume loop should enqueue valid messages to work queue."""
        msg1 = _make_chunk_msg(chunk_id="c1")
        msg2 = _make_chunk_msg(chunk_id="c2")

        # Mock consumer.getmany returns messages once, then stops
        mock_kafka_consumer = AsyncMock()
        call_count = 0

        async def mock_getmany(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                mock_msg1 = SimpleNamespace(value=msg1)
                mock_msg2 = SimpleNamespace(value=msg2)
                return {"partition-0": [mock_msg1, mock_msg2]}
            else:
                consumer._running = False
                consumer._shutdown_event.set()
                return {}

        mock_kafka_consumer.getmany = mock_getmany
        consumer._consumer = mock_kafka_consumer
        consumer._work_queue = AsyncMock()
        consumer._work_queue.put = AsyncMock()

        await consumer.consume()

        # Should have enqueued both messages
        assert consumer._work_queue.put.call_count == 2

    @pytest.mark.asyncio
    async def test_consume_skips_non_process_actions(self, consumer, mock_processor):
        """Messages with action != 'process' should be skipped."""
        msg = _make_chunk_msg(action="delete", chunk_id="c1")

        call_count = 0

        async def mock_getmany(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"p0": [SimpleNamespace(value=msg)]}
            else:
                consumer._running = False
                consumer._shutdown_event.set()
                return {}

        mock_kafka_consumer = AsyncMock()
        mock_kafka_consumer.getmany = mock_getmany
        consumer._consumer = mock_kafka_consumer
        consumer._work_queue = AsyncMock()
        consumer._work_queue.put = AsyncMock()

        await consumer.consume()

        # Work queue should NOT have received the skipped message
        consumer._work_queue.put.assert_not_called()

    @pytest.mark.asyncio
    async def test_consume_skips_messages_with_missing_fields(self, consumer, mock_processor):
        """Messages with missing required fields should be skipped."""
        msg = _make_chunk_msg(content=None, chunk_id="c1")  # content is required

        call_count = 0

        async def mock_getmany(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"p0": [SimpleNamespace(value=msg)]}
            else:
                consumer._running = False
                consumer._shutdown_event.set()
                return {}

        mock_kafka_consumer = AsyncMock()
        mock_kafka_consumer.getmany = mock_getmany
        consumer._consumer = mock_kafka_consumer
        consumer._work_queue = AsyncMock()
        consumer._work_queue.put = AsyncMock()

        await consumer.consume()

        consumer._work_queue.put.assert_not_called()

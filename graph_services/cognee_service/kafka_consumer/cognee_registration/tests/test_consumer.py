"""Unit tests for CogneeRegistrationKafkaConsumer.

Tests message routing, deserialization, retry logic, and signal handling.
All Kafka and cognee dependencies are mocked.
"""

import asyncio
import json
import sys
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest


# ── Mock cognee at module level — must be before any import that touches cognee

_mock_document_chunk = MagicMock()
_mock_document = MagicMock()


def _install_cognee_mocks():
    mods = {
        "cognee": MagicMock(),
        "cognee.modules": MagicMock(),
        "cognee.modules.chunking": MagicMock(),
        "cognee.modules.chunking.models": MagicMock(),
        "cognee.modules.chunking.models.DocumentChunk": MagicMock(
            DocumentChunk=_mock_document_chunk
        ),
        "cognee.modules.data": MagicMock(),
        "cognee.modules.data.processing": MagicMock(),
        "cognee.modules.data.processing.document_types": MagicMock(Document=_mock_document),
        "cognee_service.cognee_patches": MagicMock(),
        "cognee_service.multi_tenancy": MagicMock(
            ensure_neo4j_database=AsyncMock(),
            set_company_context=MagicMock(),
        ),
        "aiokafka": MagicMock(),
    }
    for name, mod in mods.items():
        if name not in sys.modules:
            sys.modules[name] = mod


_install_cognee_mocks()

from cognee_service.kafka_consumer.enriched_chunks.models import EnrichedChunkMessage  # noqa: E402
from cognee_service.kafka_consumer.cognee_registration.consumer import (  # noqa: E402
    CogneeRegistrationKafkaConsumer,
)
from cognee_service.kafka_consumer.cognee_registration.config import (  # noqa: E402
    CogneeRegistrationConsumerConfig,
)
from cognee_service.kafka_consumer.cognee_registration.processor import (  # noqa: E402
    CogneeRegistrationProcessor,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_msg(**kwargs) -> EnrichedChunkMessage:
    defaults = {
        "action": "process",
        "company_id": str(uuid.uuid4()),
        "project_id": str(uuid.uuid4()),
        "repository": "my-org/my-repo",
        "branch": "main",
        "file_path": "src/app.py",
        "ingestion_id": str(uuid.uuid4()),
        "file_version_id": str(uuid.uuid4()),
        "chunk_id": str(uuid.uuid4()),
        "parent_id": str(uuid.uuid4()),
        "content": "def hello(): pass",
        "chunk_index": 0,
        "total_chunks": 1,
    }
    defaults.update(kwargs)
    if defaults.get("content_type") == "document":
        defaults["project_id"] = None
    return EnrichedChunkMessage(**defaults)


def _raw(msg: EnrichedChunkMessage) -> bytes:
    return json.dumps(msg.model_dump()).encode("utf-8")


def _make_consumer() -> CogneeRegistrationKafkaConsumer:
    processor = MagicMock(spec=CogneeRegistrationProcessor)
    processor.register = AsyncMock()
    return CogneeRegistrationKafkaConsumer(
        processor=processor,
        config=CogneeRegistrationConsumerConfig,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestCogneeRegistrationKafkaConsumer:
    """Tests for CogneeRegistrationKafkaConsumer."""

    def test_deserialize_valid_message(self):
        """_deserialize_message correctly parses a valid JSON payload."""
        consumer = _make_consumer()
        msg = _make_msg()
        result = consumer._deserialize_message(_raw(msg))
        assert isinstance(result, EnrichedChunkMessage)
        assert result.file_path == msg.file_path
        assert result.action == "process"

    def test_deserialize_invalid_message_raises(self):
        """_deserialize_message raises ValueError for invalid JSON."""
        consumer = _make_consumer()
        with pytest.raises(ValueError, match="Invalid message format"):
            consumer._deserialize_message(b"not-json")

    @pytest.mark.asyncio
    async def test_process_with_retries_calls_register_on_success(self):
        """_process_with_retries calls processor.register once on success."""
        consumer = _make_consumer()
        msg = _make_msg()
        await consumer._process_with_retries(msg)
        consumer.processor.register.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_process_with_retries_retries_on_exception(self):
        """_process_with_retries retries up to MAX_RETRIES on exception."""
        consumer = _make_consumer()
        consumer.processor.register = AsyncMock(side_effect=RuntimeError("transient"))

        original_max = CogneeRegistrationConsumerConfig.MAX_RETRIES
        original_delay = CogneeRegistrationConsumerConfig.RETRY_DELAY
        try:
            # Patch config class attributes directly
            CogneeRegistrationConsumerConfig.MAX_RETRIES = 3
            CogneeRegistrationConsumerConfig.RETRY_DELAY = 0.0
            consumer.config = CogneeRegistrationConsumerConfig

            msg = _make_msg()
            await consumer._process_with_retries(msg)
        finally:
            CogneeRegistrationConsumerConfig.MAX_RETRIES = original_max
            CogneeRegistrationConsumerConfig.RETRY_DELAY = original_delay

        assert consumer.processor.register.await_count == 3

    @pytest.mark.asyncio
    async def test_process_with_retries_does_not_raise_after_all_failures(self):
        """_process_with_retries logs and returns (never raises) after max retries."""
        consumer = _make_consumer()
        consumer.processor.register = AsyncMock(side_effect=RuntimeError("always fails"))

        original_max = CogneeRegistrationConsumerConfig.MAX_RETRIES
        original_delay = CogneeRegistrationConsumerConfig.RETRY_DELAY
        try:
            CogneeRegistrationConsumerConfig.MAX_RETRIES = 1
            CogneeRegistrationConsumerConfig.RETRY_DELAY = 0.0
            consumer.config = CogneeRegistrationConsumerConfig

            msg = _make_msg()
            # Must not raise
            await consumer._process_with_retries(msg)
        finally:
            CogneeRegistrationConsumerConfig.MAX_RETRIES = original_max
            CogneeRegistrationConsumerConfig.RETRY_DELAY = original_delay

    @pytest.mark.asyncio
    async def test_consume_skips_delete_action_messages(self):
        """Consume loop skips messages with action='delete'."""
        consumer = _make_consumer()

        delete_msg = _make_msg(action="delete", content=None)
        deserialized = consumer._deserialize_message(_raw(delete_msg))

        mock_kafka_msg = MagicMock()
        mock_kafka_msg.value = deserialized

        call_count = 0

        async def fake_getmany(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"p0": [mock_kafka_msg]}
            consumer._running = False
            consumer._shutdown_event.set()
            return {}

        consumer.consumer = MagicMock()
        consumer.consumer.getmany = fake_getmany
        consumer._running = True

        await consumer.consume()

        # register() should NOT have been called for delete messages
        consumer.processor.register.assert_not_called()

    @pytest.mark.asyncio
    async def test_consume_skips_messages_with_missing_tenant_ids(self):
        """Consume loop skips messages missing company_id or project_id."""
        consumer = _make_consumer()

        data = {
            "action": "process",
            "company_id": "",
            "project_id": str(uuid.uuid4()),
            "repository": "repo",
            "branch": "main",
            "file_path": "src/foo.py",
            "ingestion_id": str(uuid.uuid4()),
            "file_version_id": str(uuid.uuid4()),
        }
        deserialized = consumer._deserialize_message(json.dumps(data).encode())

        mock_kafka_msg = MagicMock()
        mock_kafka_msg.value = deserialized

        call_count = 0

        async def fake_getmany(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"p0": [mock_kafka_msg]}
            consumer._running = False
            consumer._shutdown_event.set()
            return {}

        consumer.consumer = MagicMock()
        consumer.consumer.getmany = fake_getmany
        consumer._running = True

        await consumer.consume()
        consumer.processor.register.assert_not_called()

    @pytest.mark.asyncio
    async def test_consume_processes_valid_message(self):
        """Consume loop calls _process_with_retries for a valid process message."""
        consumer = _make_consumer()

        msg = _make_msg()
        deserialized = consumer._deserialize_message(_raw(msg))

        mock_kafka_msg = MagicMock()
        mock_kafka_msg.value = deserialized

        call_count = 0

        async def fake_getmany(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"p0": [mock_kafka_msg]}
            consumer._running = False
            consumer._shutdown_event.set()
            return {}

        consumer.consumer = MagicMock()
        consumer.consumer.getmany = fake_getmany
        consumer._running = True

        await consumer.consume()

        consumer.processor.register.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_consume_processes_document_without_project_id(self):
        """Document messages without project_id should still be processed."""
        consumer = _make_consumer()

        msg = _make_msg(content_type="document", project_id=None, content="doc text")
        deserialized = consumer._deserialize_message(_raw(msg))

        mock_kafka_msg = MagicMock()
        mock_kafka_msg.value = deserialized

        call_count = 0

        async def fake_getmany(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"p0": [mock_kafka_msg]}
            consumer._running = False
            consumer._shutdown_event.set()
            return {}

        consumer.consumer = MagicMock()
        consumer.consumer.getmany = fake_getmany
        consumer._running = True

        await consumer.consume()

        consumer.processor.register.assert_awaited_once_with(deserialized)

    @pytest.mark.asyncio
    async def test_stop_clears_consumer(self):
        """stop() calls consumer.stop() and clears consumer reference."""
        consumer = _make_consumer()
        mock_kafka = AsyncMock()
        consumer.consumer = mock_kafka

        await consumer.stop()

        mock_kafka.stop.assert_awaited_once()
        assert consumer.consumer is None


class TestCogneeRegistrationConsumerConfig:
    """Tests for CogneeRegistrationConsumerConfig."""

    def test_validate_passes_with_defaults(self):
        """validate() should not raise with default configuration."""
        CogneeRegistrationConsumerConfig.validate()

    def test_consumer_group_is_independent(self):
        """Consumer group is distinct from the enriched chunks processor group."""
        group = CogneeRegistrationConsumerConfig.KAFKA_CONSUMER_GROUP_ID
        assert group == "cognee-registration-group"
        assert group != "cognee-enriched-chunks-processor"

    def test_kafka_topic_matches_enriched_chunks_topic(self):
        """Consumer subscribes to the same topic as other enriched chunk consumers."""
        assert CogneeRegistrationConsumerConfig.KAFKA_TOPIC == "enriched-code-chunks"

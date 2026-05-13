"""Tests for dedup.py — entity_id + counter consistency validation.

Entity/edge dedup methods were removed (data flows via Kafka, not Postgres).
All asyncpg calls are mocked — no real Postgres needed.
"""

from unittest.mock import AsyncMock
from uuid import UUID, uuid5, NAMESPACE_OID

import pytest

from qdrant_storage_service.dedup import EntityDeduplicator, entity_id, entity_name_to_uuid


class TestEntityNameToUuid:
    """Test deterministic UUID generation from entity names."""

    def test_basic_name(self):
        result = entity_name_to_uuid("MyClass", "knowledge_x")
        expected = uuid5(NAMESPACE_OID, "myclass|knowledge_x")
        assert result == expected

    def test_spaces_replaced(self):
        result = entity_name_to_uuid("My Class Name", "knowledge_x")
        expected = uuid5(NAMESPACE_OID, "my_class_name|knowledge_x")
        assert result == expected

    def test_deterministic(self):
        a = entity_name_to_uuid("hello_world", "knowledge_x")
        b = entity_name_to_uuid("hello_world", "knowledge_x")
        assert a == b

    def test_case_insensitive(self):
        a = entity_name_to_uuid("MyFunction", "knowledge_x")
        b = entity_name_to_uuid("myfunction", "knowledge_x")
        assert a == b

    def test_returns_uuid_type(self):
        result = entity_name_to_uuid("test", "knowledge_x")
        assert isinstance(result, UUID)

    def test_different_names_differ(self):
        a = entity_name_to_uuid("ClassA", "knowledge_x")
        b = entity_name_to_uuid("ClassB", "knowledge_x")
        assert a != b

    def test_same_name_different_node_set_differs(self):
        a = entity_id("Axios Client", "knowledge_x")
        b = entity_id("Axios Client", "code_y")
        assert a != b

    def test_three_implementations_match(self):
        from entity_extraction_service.models import make_entity_id
        from neo4j_storage_service.writer import _entity_id

        expected = str(entity_id("Axios Client", "knowledge_x"))
        assert str(make_entity_id("Axios Client", "knowledge_x")) == expected
        assert str(_entity_id("Axios Client", "knowledge_x")) == expected


class TestValidateCounterConsistency:
    """Test counter consistency validation."""

    @pytest.mark.asyncio
    async def test_all_match_returns_valid(self):
        pool = AsyncMock()
        pool.fetch.return_value = [
            {
                "service_name": "preprocessor",
                "counter_name": "chunks_produced",
                "counter_value": 100,
                "status": "complete",
            },
            {
                "service_name": "entity_extraction",
                "counter_name": "chunks_received",
                "counter_value": 100,
                "status": "complete",
            },
            {
                "service_name": "summarization",
                "counter_name": "chunks_received",
                "counter_value": 100,
                "status": "complete",
            },
        ]

        dedup = EntityDeduplicator(pool)
        is_valid, summary = await dedup.validate_counter_consistency("ing-1")
        assert is_valid is True
        assert summary["preprocessor_chunks"] == 100
        assert summary["extraction_received"] == 100
        assert summary["summarization_received"] == 100

    @pytest.mark.asyncio
    async def test_extraction_mismatch_returns_invalid(self):
        pool = AsyncMock()
        pool.fetch.return_value = [
            {
                "service_name": "preprocessor",
                "counter_name": "chunks_produced",
                "counter_value": 100,
                "status": "complete",
            },
            {
                "service_name": "entity_extraction",
                "counter_name": "chunks_received",
                "counter_value": 95,
                "status": "complete",
            },
            {
                "service_name": "summarization",
                "counter_name": "chunks_received",
                "counter_value": 100,
                "status": "complete",
            },
        ]

        dedup = EntityDeduplicator(pool)
        is_valid, summary = await dedup.validate_counter_consistency("ing-1")
        assert is_valid is False

    @pytest.mark.asyncio
    async def test_summarization_mismatch_returns_invalid(self):
        pool = AsyncMock()
        pool.fetch.return_value = [
            {
                "service_name": "preprocessor",
                "counter_name": "chunks_produced",
                "counter_value": 100,
                "status": "complete",
            },
            {
                "service_name": "entity_extraction",
                "counter_name": "chunks_received",
                "counter_value": 100,
                "status": "complete",
            },
            {
                "service_name": "summarization",
                "counter_name": "chunks_received",
                "counter_value": 90,
                "status": "complete",
            },
        ]

        dedup = EntityDeduplicator(pool)
        is_valid, summary = await dedup.validate_counter_consistency("ing-1")
        assert is_valid is False

    @pytest.mark.asyncio
    async def test_no_preprocessor_returns_valid(self):
        """If preprocessor hasn't reported yet (0 chunks), validation passes."""
        pool = AsyncMock()
        pool.fetch.return_value = []

        dedup = EntityDeduplicator(pool)
        is_valid, summary = await dedup.validate_counter_consistency("ing-1")
        assert is_valid is True
        assert summary["preprocessor_chunks"] == 0

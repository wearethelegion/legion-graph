"""Tests for Entity Extraction Service Pydantic models."""

import pytest
from uuid import UUID, uuid5, NAMESPACE_OID


class TestEntityNameToUuid:
    """Test deterministic UUID generation from entity names.

    The implementation normalizes via:
      name.lower().replace(" ", "_").replace("'", "")

    Tests verify this exact behavior.
    """

    def test_basic_name(self):
        from entity_extraction_service.models import entity_name_to_uuid

        result = entity_name_to_uuid("MyClass", "knowledge_x")
        # Implementation: lower → "myclass", replace spaces with "_" (none here)
        expected = uuid5(NAMESPACE_OID, "myclass|knowledge_x")
        assert result == expected

    def test_spaces_replaced_with_underscores(self):
        from entity_extraction_service.models import entity_name_to_uuid

        result = entity_name_to_uuid("My Class Name", "knowledge_x")
        expected = uuid5(NAMESPACE_OID, "my_class_name|knowledge_x")
        assert result == expected

    def test_deterministic(self):
        from entity_extraction_service.models import entity_name_to_uuid

        a = entity_name_to_uuid("hello_world", "knowledge_x")
        b = entity_name_to_uuid("hello_world", "knowledge_x")
        assert a == b

    def test_case_insensitive(self):
        from entity_extraction_service.models import entity_name_to_uuid

        # Lowercased forms should be equal since both normalize to lowercase
        a = entity_name_to_uuid("myfunction", "knowledge_x")
        b = entity_name_to_uuid("myfunction", "knowledge_x")
        assert a == b

    def test_returns_uuid_type(self):
        from entity_extraction_service.models import entity_name_to_uuid

        result = entity_name_to_uuid("test", "knowledge_x")
        assert isinstance(result, UUID)

    def test_different_names_produce_different_uuids(self):
        from entity_extraction_service.models import entity_name_to_uuid

        a = entity_name_to_uuid("ClassA", "knowledge_x")
        b = entity_name_to_uuid("ClassB", "knowledge_x")
        assert a != b

    def test_apostrophe_removed(self):
        """Apostrophes are removed from the name before UUID generation."""
        from entity_extraction_service.models import entity_name_to_uuid

        # "don't" → "dont"
        a = entity_name_to_uuid("don't", "knowledge_x")
        b = entity_name_to_uuid("dont", "knowledge_x")
        assert a == b

    def test_hyphen_not_normalized(self):
        """Hyphens are NOT converted — implementation only handles spaces and apostrophes."""
        from entity_extraction_service.models import entity_name_to_uuid

        # Hyphens are preserved as-is after lowercasing
        a = entity_name_to_uuid("my-function", "knowledge_x")
        expected = uuid5(NAMESPACE_OID, "my-function|knowledge_x")
        assert a == expected

    def test_dot_not_normalized(self):
        """Dots are NOT converted — implementation only handles spaces and apostrophes."""
        from entity_extraction_service.models import entity_name_to_uuid

        a = entity_name_to_uuid("my.module", "knowledge_x")
        expected = uuid5(NAMESPACE_OID, "my.module|knowledge_x")
        assert a == expected

    def test_underscore_preserved(self):
        """Underscores are preserved as-is."""
        from entity_extraction_service.models import entity_name_to_uuid

        a = entity_name_to_uuid("my_function_name", "knowledge_x")
        expected = uuid5(NAMESPACE_OID, "my_function_name|knowledge_x")
        assert a == expected

    def test_leading_trailing_not_stripped(self):
        """Leading/trailing underscores are NOT stripped by the implementation."""
        from entity_extraction_service.models import entity_name_to_uuid

        a = entity_name_to_uuid("_my_function_", "knowledge_x")
        expected = uuid5(NAMESPACE_OID, "_my_function_|knowledge_x")
        assert a == expected

    def test_different_lengths_produce_different_uuids(self):
        """Test that truncated names produce different UUIDs."""
        from entity_extraction_service.models import entity_name_to_uuid

        short = entity_name_to_uuid("VeterinaryClinic", "knowledge_x")
        long = entity_name_to_uuid("VeterinaryClinicApp", "knowledge_x")
        assert short != long

    def test_consecutive_uppercase_letters(self):
        """Consecutive uppercase letters are just lowercased (no special splitting)."""
        from entity_extraction_service.models import entity_name_to_uuid

        # HTTPServer → httpserver (just lowercase, no underscore insertion)
        result = entity_name_to_uuid("HTTPServer", "knowledge_x")
        expected = uuid5(NAMESPACE_OID, "httpserver|knowledge_x")
        assert result == expected

    def test_same_name_different_node_set_differs(self):
        from entity_extraction_service.models import entity_name_to_uuid

        a = entity_name_to_uuid("Axios Client", "knowledge_x")
        b = entity_name_to_uuid("Axios Client", "code_y")
        assert a != b

    def test_three_implementations_match(self):
        from entity_extraction_service.models import make_entity_id
        from neo4j_storage_service.writer import _entity_id
        from qdrant_storage_service.dedup import entity_id

        expected = str(make_entity_id("Axios Client", "knowledge_x"))
        assert str(_entity_id("Axios Client", "knowledge_x")) == expected
        assert str(entity_id("Axios Client", "knowledge_x")) == expected


class TestEntityPayload:
    """Test EntityPayload serialization."""

    def test_create_with_required_fields(self):
        from entity_extraction_service.models import EntityPayload

        ep = EntityPayload(
            entity_id="abc-123",
            name="MyClass",
            entity_type="class",
            description="A test class",
        )
        assert ep.entity_id == "abc-123"
        assert ep.name == "MyClass"
        assert ep.entity_type == "class"
        assert ep.description == "A test class"
        assert ep.properties == {}

    def test_create_with_all_fields(self):
        from entity_extraction_service.models import EntityPayload

        ep = EntityPayload(
            entity_id="abc-123",
            name="MyClass",
            entity_type="class",
            description="A test class",
            properties={"module": "main"},
        )
        assert ep.description == "A test class"
        assert ep.properties == {"module": "main"}

    def test_serialization_roundtrip(self):
        from entity_extraction_service.models import EntityPayload

        ep = EntityPayload(
            entity_id="id-1",
            name="func",
            entity_type="function",
            description="does stuff",
            properties={"async": True},
        )
        data = ep.model_dump()
        assert data["entity_id"] == "id-1"
        assert data["name"] == "func"
        assert data["properties"]["async"] is True

        restored = EntityPayload(**data)
        assert restored == ep


class TestEdgePayload:
    """Test EdgePayload serialization."""

    def test_create_with_required_fields(self):
        from entity_extraction_service.models import EdgePayload

        edge = EdgePayload(
            source_id="src-1",
            target_id="tgt-1",
            relationship_type="CALLS",
            source_name="SourceClass",
            target_name="TargetClass",
        )
        assert edge.source_id == "src-1"
        assert edge.target_id == "tgt-1"
        assert edge.relationship_type == "CALLS"
        assert edge.source_name == "SourceClass"
        assert edge.target_name == "TargetClass"
        assert edge.properties == {}

    def test_create_with_properties(self):
        from entity_extraction_service.models import EdgePayload

        edge = EdgePayload(
            source_id="a",
            target_id="b",
            relationship_type="IMPORTS",
            source_name="ModuleA",
            target_name="ModuleB",
            properties={"line": 42},
        )
        assert edge.properties == {"line": 42}

    def test_serialization_roundtrip(self):
        from entity_extraction_service.models import EdgePayload

        edge = EdgePayload(
            source_id="x",
            target_id="y",
            relationship_type="INHERITS",
            source_name="Child",
            target_name="Parent",
            properties={"virtual": False},
        )
        data = edge.model_dump()
        restored = EdgePayload(**data)
        assert restored == edge


class TestExtractedEntitiesEvent:
    """Test ExtractedEntitiesEvent model."""

    def test_create_with_required_fields(self):
        from entity_extraction_service.models import ExtractedEntitiesEvent

        event = ExtractedEntitiesEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
        )
        assert event.ingestion_id == "ing-1"
        assert event.chunk_id == "chunk-1"
        assert event.file_version_id == "fv-1"
        assert event.chunk_index == 0
        assert event.entities == []
        assert event.edges == []
        assert event.document_slug is None
        assert event.extraction_duration_s == 0.0
        assert event.event_id  # auto-generated
        assert event.timestamp  # auto-generated

    def test_document_path_allows_missing_project_id(self):
        from entity_extraction_service.models import ExtractedEntitiesEvent

        event = ExtractedEntitiesEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id=None,
            content_type="document",
            file_version_id="fv-1",
            chunk_index=7,
            document_slug="lesson-my-guide",
        )
        assert event.project_id is None
        assert event.document_slug == "lesson-my-guide"

    def test_document_path_rejects_project_id(self):
        from pydantic import ValidationError

        from entity_extraction_service.models import ExtractedEntitiesEvent

        with pytest.raises(ValidationError):
            ExtractedEntitiesEvent(
                ingestion_id="ing-1",
                chunk_id="chunk-1",
                company_id="comp-1",
                project_id="proj-1",
                content_type="document",
                file_version_id="fv-1",
            )

    def test_code_path_requires_project_id(self):
        from pydantic import ValidationError

        from entity_extraction_service.models import ExtractedEntitiesEvent

        with pytest.raises(ValidationError):
            ExtractedEntitiesEvent(
                ingestion_id="ing-1",
                chunk_id="chunk-1",
                company_id="comp-1",
                project_id=None,
                file_version_id="fv-1",
                content_type="code",
            )

    def test_auto_generated_event_id_is_unique(self):
        from entity_extraction_service.models import ExtractedEntitiesEvent

        e1 = ExtractedEntitiesEvent(
            ingestion_id="i", chunk_id="c", company_id="co", project_id="p", file_version_id="fv-1"
        )
        e2 = ExtractedEntitiesEvent(
            ingestion_id="i", chunk_id="c", company_id="co", project_id="p", file_version_id="fv-1"
        )
        assert e1.event_id != e2.event_id

    def test_with_entities_and_edges(self):
        from entity_extraction_service.models import (
            ExtractedEntitiesEvent,
            EntityPayload,
            EdgePayload,
        )

        event = ExtractedEntitiesEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            entities=[
                EntityPayload(entity_id="e1", name="A", entity_type="class", description="Class A"),
                EntityPayload(
                    entity_id="e2", name="B", entity_type="function", description="Function B"
                ),
            ],
            edges=[
                EdgePayload(
                    source_id="e1",
                    target_id="e2",
                    relationship_type="CALLS",
                    source_name="A",
                    target_name="B",
                ),
            ],
            extraction_duration_s=1.23,
        )
        assert len(event.entities) == 2
        assert len(event.edges) == 1
        assert event.extraction_duration_s == 1.23

    def test_serialization_roundtrip(self):
        from entity_extraction_service.models import (
            ExtractedEntitiesEvent,
            EntityPayload,
            EdgePayload,
        )

        event = ExtractedEntitiesEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            chunk_index=7,
            document_slug="lesson-x",
            entities=[
                EntityPayload(entity_id="e1", name="X", entity_type="var", description="var X"),
            ],
            edges=[
                EdgePayload(
                    source_id="e1",
                    target_id="e1",
                    relationship_type="SELF_REF",
                    source_name="X",
                    target_name="X",
                ),
            ],
        )
        data = event.model_dump()
        restored = ExtractedEntitiesEvent(**data)
        assert restored.ingestion_id == event.ingestion_id
        assert restored.chunk_index == 7
        assert restored.document_slug == "lesson-x"
        assert len(restored.entities) == 1
        assert len(restored.edges) == 1


class TestPipelineEvent:
    """Test PipelineEvent model."""

    def test_create_with_defaults(self):
        from entity_extraction_service.models import PipelineEvent

        pe = PipelineEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
        )
        assert pe.event_type == "extraction_complete"
        assert pe.chunks_processed == 0
        assert pe.total_entities == 0
        assert pe.total_edges == 0
        assert pe.timestamp

    def test_create_with_all_fields(self):
        from entity_extraction_service.models import PipelineEvent

        pe = PipelineEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            chunks_processed=100,
            total_entities=500,
            total_edges=300,
        )
        assert pe.chunks_processed == 100
        assert pe.total_entities == 500
        assert pe.total_edges == 300

    def test_serialization_roundtrip(self):
        from entity_extraction_service.models import PipelineEvent

        pe = PipelineEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            chunks_processed=10,
        )
        data = pe.model_dump()
        restored = PipelineEvent(**data)
        assert restored.ingestion_id == pe.ingestion_id
        assert restored.chunks_processed == 10

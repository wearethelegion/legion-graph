"""Tests for Embedding Service Pydantic models."""

import pytest


class TestEntityInputEvent:
    """Test EntityInputEvent deserialization."""

    def test_create_with_required_fields(self):
        from embedding_service.models import EntityInputEvent

        event = EntityInputEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
        )
        assert event.ingestion_id == "ing-1"
        assert event.chunk_id == "chunk-1"
        assert event.entities == []
        assert event.event_id == ""

    def test_create_with_entities(self):
        from embedding_service.models import EntityInputEvent

        event = EntityInputEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            entities=[
                {"entity_id": "e1", "name": "Foo", "entity_type": "class"},
                {"entity_id": "e2", "name": "bar", "entity_type": "function"},
            ],
        )
        assert len(event.entities) == 2
        assert event.entities[0]["name"] == "Foo"

    def test_serialization_roundtrip(self):
        from embedding_service.models import EntityInputEvent

        event = EntityInputEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            entities=[{"entity_id": "e1", "name": "X", "entity_type": "var"}],
        )
        data = event.model_dump()
        restored = EntityInputEvent(**data)
        assert restored.ingestion_id == event.ingestion_id
        assert len(restored.entities) == 1

    def test_document_path_allows_missing_project_id(self):
        from embedding_service.models import EntityInputEvent

        event = EntityInputEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id=None,
            content_type="document",
            file_version_id="fv-1",
        )
        assert event.project_id is None

    def test_document_path_rejects_project_id(self):
        from pydantic import ValidationError

        from embedding_service.models import EntityInputEvent

        with pytest.raises(ValidationError):
            EntityInputEvent(
                ingestion_id="ing-1",
                chunk_id="chunk-1",
                company_id="comp-1",
                project_id="proj-1",
                content_type="document",
                file_version_id="fv-1",
            )


class TestSummaryInputEvent:
    """Test SummaryInputEvent deserialization."""

    def test_create_with_required_fields(self):
        from embedding_service.models import SummaryInputEvent

        event = SummaryInputEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            summary_text="This function handles authentication.",
        )
        assert event.ingestion_id == "ing-1"
        assert event.summary_text == "This function handles authentication."
        assert event.summary_id == ""

    def test_serialization_roundtrip(self):
        from embedding_service.models import SummaryInputEvent

        event = SummaryInputEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            summary_text="Summary of code logic.",
            summary_id="sum-1",
        )
        data = event.model_dump()
        restored = SummaryInputEvent(**data)
        assert restored.summary_text == event.summary_text
        assert restored.summary_id == "sum-1"

    def test_document_path_allows_missing_project_id(self):
        from embedding_service.models import SummaryInputEvent

        event = SummaryInputEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id=None,
            content_type="document",
            file_version_id="fv-1",
            summary_text="Summary of a doc.",
        )
        assert event.project_id is None

    def test_document_path_rejects_project_id(self):
        from pydantic import ValidationError

        from embedding_service.models import SummaryInputEvent

        with pytest.raises(ValidationError):
            SummaryInputEvent(
                ingestion_id="ing-1",
                chunk_id="chunk-1",
                company_id="comp-1",
                project_id="proj-1",
                content_type="document",
                file_version_id="fv-1",
                summary_text="Summary of a doc.",
            )


class TestEmbeddingPayload:
    """Test EmbeddingPayload serialization."""

    def test_create_entity_embedding(self):
        from embedding_service.models import EmbeddingPayload

        payload = EmbeddingPayload(
            source_id="e1",
            source_type="entity",
            text="MyClass",
            embedding=[0.1, 0.2, 0.3],
        )
        assert payload.source_id == "e1"
        assert payload.source_type == "entity"
        assert payload.text == "MyClass"
        assert len(payload.embedding) == 3

    def test_create_summary_embedding(self):
        from embedding_service.models import EmbeddingPayload

        payload = EmbeddingPayload(
            source_id="s1",
            source_type="summary",
            text="Handles auth",
            embedding=[0.5] * 768,
        )
        assert payload.source_type == "summary"
        assert len(payload.embedding) == 768

    def test_serialization_roundtrip(self):
        from embedding_service.models import EmbeddingPayload

        payload = EmbeddingPayload(
            source_id="x",
            source_type="entity",
            text="func",
            embedding=[1.0, 2.0],
        )
        data = payload.model_dump()
        restored = EmbeddingPayload(**data)
        assert restored == payload


class TestEmbeddingReadyEvent:
    """Test EmbeddingReadyEvent model."""

    def test_create_with_required_fields(self):
        from embedding_service.models import EmbeddingReadyEvent

        event = EmbeddingReadyEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            repository="vet_backend",
            branch="main",
        )
        assert event.ingestion_id == "ing-1"
        assert event.embeddings == []
        assert event.embedding_duration_s == 0.0
        assert event.event_id  # auto-generated
        assert event.timestamp  # auto-generated

    def test_document_path_allows_missing_project_id(self):
        from embedding_service.models import EmbeddingReadyEvent

        event = EmbeddingReadyEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id=None,
            content_type="document",
            file_version_id="fv-1",
            repository="kgrag-documents",
            branch="main",
        )
        assert event.project_id is None

    def test_document_path_rejects_project_id(self):
        from pydantic import ValidationError

        from embedding_service.models import EmbeddingReadyEvent

        with pytest.raises(ValidationError):
            EmbeddingReadyEvent(
                ingestion_id="ing-1",
                company_id="comp-1",
                project_id="proj-1",
                content_type="document",
                file_version_id="fv-1",
                repository="kgrag-documents",
                branch="main",
            )

    def test_code_path_requires_project_id(self):
        from pydantic import ValidationError

        from embedding_service.models import EmbeddingReadyEvent

        with pytest.raises(ValidationError):
            EmbeddingReadyEvent(
                ingestion_id="ing-1",
                company_id="comp-1",
                project_id=None,
                file_version_id="fv-1",
                repository="vet_backend",
                branch="main",
            )

    def test_auto_generated_event_id_is_unique(self):
        from embedding_service.models import EmbeddingReadyEvent

        e1 = EmbeddingReadyEvent(
            ingestion_id="i",
            company_id="c",
            project_id="p",
            file_version_id="fv-1",
            repository="vet_backend",
            branch="main",
        )
        e2 = EmbeddingReadyEvent(
            ingestion_id="i",
            company_id="c",
            project_id="p",
            file_version_id="fv-1",
            repository="vet_backend",
            branch="main",
        )
        assert e1.event_id != e2.event_id

    def test_with_embeddings(self):
        from embedding_service.models import EmbeddingReadyEvent, EmbeddingPayload

        event = EmbeddingReadyEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            repository="vet_backend",
            branch="main",
            embeddings=[
                EmbeddingPayload(
                    source_id="e1",
                    source_type="entity",
                    text="Foo",
                    embedding=[0.1] * 768,
                ),
            ],
            embedding_duration_s=1.5,
        )
        assert len(event.embeddings) == 1
        assert event.embedding_duration_s == 1.5

    def test_serialization_roundtrip(self):
        from embedding_service.models import EmbeddingReadyEvent, EmbeddingPayload

        event = EmbeddingReadyEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            repository="vet_backend",
            branch="main",
            embeddings=[
                EmbeddingPayload(
                    source_id="e1",
                    source_type="entity",
                    text="X",
                    embedding=[0.5, 0.6],
                ),
            ],
        )
        data = event.model_dump()
        restored = EmbeddingReadyEvent(**data)
        assert restored.ingestion_id == event.ingestion_id
        assert len(restored.embeddings) == 1


class TestPipelineEvent:
    """Test PipelineEvent model."""

    def test_create_with_defaults(self):
        from embedding_service.models import PipelineEvent

        pe = PipelineEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
        )
        assert pe.event_type == "embedding_complete"
        assert pe.entities_received == 0
        assert pe.summaries_received == 0
        assert pe.embeddings_computed == 0
        assert pe.timestamp

    def test_create_with_all_fields(self):
        from embedding_service.models import PipelineEvent

        pe = PipelineEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            entities_received=100,
            summaries_received=50,
            embeddings_computed=150,
        )
        assert pe.entities_received == 100
        assert pe.summaries_received == 50
        assert pe.embeddings_computed == 150

    def test_serialization_roundtrip(self):
        from embedding_service.models import PipelineEvent

        pe = PipelineEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            entities_received=10,
            summaries_received=5,
            embeddings_computed=15,
        )
        data = pe.model_dump()
        restored = PipelineEvent(**data)
        assert restored.ingestion_id == pe.ingestion_id
        assert restored.embeddings_computed == 15

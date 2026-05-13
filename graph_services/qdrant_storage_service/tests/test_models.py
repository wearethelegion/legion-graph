"""Tests for Qdrant Storage Service Pydantic models."""

import pytest


class TestQdrantChunkPoint:
    """Test QdrantChunkPoint model."""

    def test_create_with_required_fields(self):
        from qdrant_storage_service.models import QdrantChunkPoint

        point = QdrantChunkPoint(
            point_id="chunk-001",
            embedding=[0.1] * 768,
        )
        assert point.point_id == "chunk-001"
        assert len(point.embedding) == 768
        assert point.payload == {}

    def test_create_with_payload(self):
        from qdrant_storage_service.models import QdrantChunkPoint

        point = QdrantChunkPoint(
            point_id="chunk-001",
            embedding=[0.5] * 768,
            payload={"text": "hello", "file_path": "src/main.py"},
        )
        assert point.payload["text"] == "hello"
        assert point.payload["file_path"] == "src/main.py"

    def test_serialization_roundtrip(self):
        from qdrant_storage_service.models import QdrantChunkPoint

        point = QdrantChunkPoint(
            point_id="id-1",
            embedding=[0.0, 1.0, 0.5],
            payload={"key": "value"},
        )
        data = point.model_dump()
        restored = QdrantChunkPoint(**data)
        assert restored.point_id == point.point_id
        assert restored.embedding == point.embedding
        assert restored.payload == point.payload


class TestQdrantEntityPoint:
    """Test QdrantEntityPoint model."""

    def test_create_with_required_fields(self):
        from qdrant_storage_service.models import QdrantEntityPoint

        point = QdrantEntityPoint(
            point_id="entity-001",
            embedding=[0.2] * 768,
        )
        assert point.point_id == "entity-001"
        assert len(point.embedding) == 768
        assert point.payload == {}

    def test_create_with_payload(self):
        from qdrant_storage_service.models import QdrantEntityPoint

        point = QdrantEntityPoint(
            point_id="entity-001",
            embedding=[0.3] * 768,
            payload={"name": "MyClass", "entity_type": "class"},
        )
        assert point.payload["name"] == "MyClass"
        assert point.payload["entity_type"] == "class"


class TestQdrantSummaryPoint:
    """Test QdrantSummaryPoint model."""

    def test_create_with_required_fields(self):
        from qdrant_storage_service.models import QdrantSummaryPoint

        point = QdrantSummaryPoint(
            point_id="summary-001",
            embedding=[0.4] * 768,
        )
        assert point.point_id == "summary-001"
        assert len(point.embedding) == 768
        assert point.payload == {}

    def test_create_with_payload(self):
        from qdrant_storage_service.models import QdrantSummaryPoint

        point = QdrantSummaryPoint(
            point_id="summary-001",
            embedding=[0.6] * 768,
            payload={"text": "A summary of the code", "chunk_id": "chunk-001"},
        )
        assert point.payload["text"] == "A summary of the code"
        assert point.payload["chunk_id"] == "chunk-001"


class TestChunkMessage:
    """Test ChunkMessage model (streaming input)."""

    def test_create_with_required_fields(self):
        from qdrant_storage_service.models import ChunkMessage

        msg = ChunkMessage(
            chunk_id="chunk-1",
            embedding=[0.1] * 768,
            chunk_text="test code",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            file_path="src/main.py",
            repository="vet_backend",
            branch="main",
            chunk_index=0,
            ingestion_id="ing-1",
        )
        assert msg.chunk_id == "chunk-1"
        assert len(msg.embedding) == 768
        assert msg.chunk_text == "test code"

    def test_document_chunk_allows_missing_project_id(self):
        from qdrant_storage_service.models import ChunkMessage

        msg = ChunkMessage(
            chunk_id="chunk-1",
            embedding=[0.1] * 768,
            chunk_text="test document",
            company_id="comp-1",
            project_id=None,
            content_type="document",
            file_version_id="fv-1",
            file_path="document://lesson/1",
            repository="kgrag-documents",
            branch="main",
            chunk_index=0,
            ingestion_id="ing-1",
        )
        assert msg.project_id is None

    def test_document_chunk_rejects_project_id(self):
        from pydantic import ValidationError

        from qdrant_storage_service.models import ChunkMessage

        with pytest.raises(ValidationError):
            ChunkMessage(
                chunk_id="chunk-1",
                embedding=[0.1] * 768,
                chunk_text="test document",
                company_id="comp-1",
                project_id="proj-1",
                content_type="document",
                file_version_id="fv-1",
                file_path="document://lesson/1",
                repository="kgrag-documents",
                branch="main",
                chunk_index=0,
                ingestion_id="ing-1",
            )

    def test_code_chunk_requires_project_id(self):
        from pydantic import ValidationError

        from qdrant_storage_service.models import ChunkMessage

        with pytest.raises(ValidationError):
            ChunkMessage(
                chunk_id="chunk-1",
                embedding=[0.1] * 768,
                chunk_text="test code",
                company_id="comp-1",
                project_id=None,
                content_type="code",
                file_version_id="fv-1",
                file_path="src/main.py",
                repository="vet_backend",
                branch="main",
                chunk_index=0,
                ingestion_id="ing-1",
            )


class TestEmbeddingReadyEvent:
    """Test EmbeddingReadyEvent model (streaming input)."""

    def test_create_with_embeddings(self):
        from qdrant_storage_service.models import EmbeddingPayload, EmbeddingReadyEvent

        event = EmbeddingReadyEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            file_version_id="fv-1",
            repository="vet_backend",
            branch="main",
            embeddings=[
                EmbeddingPayload(
                    source_id="ent-1",
                    source_type="entity",
                    text="MyClass",
                    embedding=[0.5] * 768,
                )
            ],
        )
        assert event.ingestion_id == "ing-1"
        assert len(event.embeddings) == 1
        assert event.embeddings[0].source_type == "entity"

    def test_document_path_allows_missing_project_id(self):
        from qdrant_storage_service.models import EmbeddingPayload, EmbeddingReadyEvent

        event = EmbeddingReadyEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id=None,
            content_type="document",
            file_version_id="fv-1",
            repository="kgrag-documents",
            branch="main",
            embeddings=[
                EmbeddingPayload(
                    source_id="ent-1",
                    source_type="entity",
                    text="MyClass",
                    embedding=[0.5] * 768,
                )
            ],
        )
        assert event.project_id is None

    def test_code_path_requires_project_id(self):
        from pydantic import ValidationError

        from qdrant_storage_service.models import EmbeddingPayload, EmbeddingReadyEvent

        with pytest.raises(ValidationError):
            EmbeddingReadyEvent(
                ingestion_id="ing-1",
                company_id="comp-1",
                project_id=None,
                file_version_id="fv-1",
                repository="vet_backend",
                branch="main",
                content_type="code",
                embeddings=[
                    EmbeddingPayload(
                        source_id="ent-1",
                        source_type="entity",
                        text="MyClass",
                        embedding=[0.5] * 768,
                    )
                ],
            )

    def test_document_path_rejects_project_id(self):
        from pydantic import ValidationError

        from qdrant_storage_service.models import EmbeddingPayload, EmbeddingReadyEvent

        with pytest.raises(ValidationError):
            EmbeddingReadyEvent(
                ingestion_id="ing-1",
                company_id="comp-1",
                project_id="proj-1",
                content_type="document",
                file_version_id="fv-1",
                repository="kgrag-documents",
                branch="main",
                embeddings=[
                    EmbeddingPayload(
                        source_id="ent-1",
                        source_type="entity",
                        text="MyClass",
                        embedding=[0.5] * 768,
                    )
                ],
            )


class TestTripletEmbedding:
    """Test triplet embedding payload parsing."""

    def test_create_triplet_embedding(self):
        """Should create embedding payload with triplet metadata."""
        from qdrant_storage_service.models import EmbeddingPayload

        triplet_payload = EmbeddingPayload(
            source_id="trip-1",
            source_type="triplet",
            text="AuthHandler-›calls-›AuthService",
            embedding=[0.1] * 768,
            from_node_id="e1",
            to_node_id="e2",
        )

        assert triplet_payload.source_id == "trip-1"
        assert triplet_payload.source_type == "triplet"
        assert triplet_payload.text == "AuthHandler-›calls-›AuthService"
        assert triplet_payload.from_node_id == "e1"
        assert triplet_payload.to_node_id == "e2"
        assert len(triplet_payload.embedding) == 768

    def test_embedding_ready_event_with_triplets(self):
        """Should support triplets in EmbeddingReadyEvent."""
        from qdrant_storage_service.models import EmbeddingReadyEvent, EmbeddingPayload

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
                    text="MyClass",
                    embedding=[0.1] * 768,
                ),
                EmbeddingPayload(
                    source_id="trip-1",
                    source_type="triplet",
                    text="MyClass-›uses-›MyService",
                    embedding=[0.2] * 768,
                    from_node_id="e1",
                    to_node_id="e2",
                ),
            ],
        )

        assert len(event.embeddings) == 2
        assert event.embeddings[0].source_type == "entity"
        assert event.embeddings[1].source_type == "triplet"
        assert event.embeddings[1].from_node_id == "e1"

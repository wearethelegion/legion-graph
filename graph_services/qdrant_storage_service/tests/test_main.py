"""Tests for QdrantStorageService._parse_embedding_event routing.

Focused on the source_type dispatch logic — specifically the new "edge_type" handler.
Full Kafka/Qdrant integration is excluded; only unit-level routing is tested here.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from qdrant_storage_service.config import QdrantStorageConfig
from qdrant_storage_service.main import QdrantStorageService
from qdrant_storage_service.writer import QdrantBatchWriter


@pytest.fixture
def mock_writer():
    writer = AsyncMock(spec=QdrantBatchWriter)
    writer.upsert_chunks = AsyncMock(return_value=0)
    writer.upsert_entities = AsyncMock(return_value=0)
    writer.upsert_summaries = AsyncMock(return_value=0)
    writer.upsert_triplets = AsyncMock(return_value=0)
    writer.upsert_edge_types = AsyncMock(return_value=0)
    writer.upsert_entity_types = AsyncMock(return_value=0)  # FIX 1: Added entity_types
    return writer


@pytest.fixture
def mock_store():
    return AsyncMock()


@pytest.fixture
def service(mock_store, mock_writer):
    return QdrantStorageService(
        store=mock_store,
        writer=mock_writer,
        config=QdrantStorageConfig,
    )


def _make_embedding_event(source_type: str, **extra) -> dict:
    """Build a minimal EmbeddingReadyEvent dict for a single payload."""
    payload = {
        "source_id": "test-id",
        "source_type": source_type,
        "text": extra.pop("text", "test_text"),
        "embedding": [0.1] * 768,
        **extra,
    }
    return {
        "ingestion_id": "ing-1",
        "company_id": "comp-1",
        "project_id": "proj-1",
        "branch": "main",
        "embeddings": [payload],
    }


def _make_chunk_message(**extra) -> dict:
    return {
        "chunk_id": "chunk-1",
        "embedding": [0.1] * 768,
        "chunk_text": "document content",
        "company_id": "comp-1",
        "project_id": None,
        "content_type": "document",
        "repository": "kgrag-documents",
        "branch": "main",
        **extra,
    }


class TestParseEmbeddingEventRouting:
    """Test _parse_embedding_event routes source_type correctly."""

    def test_routes_entity_type(self, service):
        data = _make_embedding_event("entity", text="MyClass")
        results = service._parse_embedding_event(data)
        assert len(results) == 1
        item_type, record = results[0]
        assert item_type == "entity"
        assert record["name"] == "MyClass"
        assert record["entity_id"] == "test-id"

    def test_routes_summary_type(self, service):
        data = _make_embedding_event("summary", text="Module summary")
        results = service._parse_embedding_event(data)
        assert len(results) == 1
        item_type, record = results[0]
        assert item_type == "summary"
        assert record["summary_text"] == "Module summary"
        assert record["summary_id"] == "test-id"

    def test_routes_triplet_type(self, service):
        data = _make_embedding_event("triplet", text="ClassA-›calls-›ClassB")
        results = service._parse_embedding_event(data)
        assert len(results) == 1
        item_type, record = results[0]
        assert item_type == "triplet"
        assert record["triplet_id"] == "test-id"
        assert record["source_name"] == "ClassA"
        assert record["relationship_type"] == "calls"
        assert record["target_name"] == "ClassB"

    def test_routes_edge_type(self, service):
        data = _make_embedding_event("edge_type", text="calls", number_of_edges=15)
        results = service._parse_embedding_event(data)
        assert len(results) == 1
        item_type, record = results[0]
        assert item_type == "edge_type"
        assert record["edge_type_id"] == "test-id"
        assert record["relationship_name"] == "calls"
        assert record["number_of_edges"] == 15

    def test_routes_entity_type_type(self, service):
        """FIX 1: Test entity_type routing."""
        data = _make_embedding_event("entity_type", text="class", number_of_entities=10)
        results = service._parse_embedding_event(data)
        assert len(results) == 1
        item_type, record = results[0]
        assert item_type == "entity_type"
        assert record["entity_type_id"] == "test-id"
        assert record["name"] == "class"
        assert record["number_of_entities"] == 10

    def test_entity_type_number_of_entities_defaults_to_zero(self, service):
        """FIX 1: Test entity_type default number_of_entities."""
        data = _make_embedding_event("entity_type", text="function")
        results = service._parse_embedding_event(data)
        item_type, record = results[0]
        assert record["number_of_entities"] == 0

    def test_entity_type_carries_company_project_branch(self, service):
        """FIX 1: Test entity_type metadata propagation."""
        data = _make_embedding_event("entity_type", text="variable")
        results = service._parse_embedding_event(data)
        _, record = results[0]
        assert record["company_id"] == "comp-1"
        assert record["project_id"] == "proj-1"
        assert record["branch"] == "main"

    def test_edge_type_number_of_edges_defaults_to_zero(self, service):
        data = _make_embedding_event("edge_type", text="inherits")
        # No number_of_edges in payload
        results = service._parse_embedding_event(data)
        item_type, record = results[0]
        assert record["number_of_edges"] == 0

    def test_edge_type_carries_company_project_branch(self, service):
        data = _make_embedding_event("edge_type", text="queries")
        results = service._parse_embedding_event(data)
        _, record = results[0]
        assert record["company_id"] == "comp-1"
        assert record["project_id"] == "proj-1"
        assert record["branch"] == "main"

    def test_mixed_batch_includes_edge_type(self, service):
        """A batch with entity, triplet, and edge_type payloads routes all correctly."""
        data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "branch": "main",
            "embeddings": [
                {
                    "source_id": "ent-1",
                    "source_type": "entity",
                    "text": "MyClass",
                    "embedding": [0.1] * 768,
                },
                {
                    "source_id": "et-1",
                    "source_type": "edge_type",
                    "text": "calls",
                    "number_of_edges": 7,
                    "embedding": [0.2] * 768,
                },
            ],
        }
        results = service._parse_embedding_event(data)
        assert len(results) == 2

        types = {item_type for item_type, _ in results}
        assert types == {"entity", "edge_type"}

    def test_skips_unknown_source_type(self, service):
        """Unknown source_type payloads are silently skipped."""
        data = _make_embedding_event("unknown_future_type", text="something")
        results = service._parse_embedding_event(data)
        assert len(results) == 0

    def test_skips_payload_missing_embedding(self, service):
        """Payloads without embedding are skipped."""
        data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "branch": "main",
            "embeddings": [
                {
                    "source_id": "et-1",
                    "source_type": "edge_type",
                    "text": "calls",
                    # no embedding
                }
            ],
        }
        results = service._parse_embedding_event(data)
        assert len(results) == 0

    def test_empty_embeddings_returns_empty(self, service):
        data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "embeddings": [],
        }
        results = service._parse_embedding_event(data)
        assert results == []

    def test_document_embedding_with_project_id_is_rejected(self, service):
        data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "content_type": "document",
            "branch": "main",
            "embeddings": [
                {
                    "source_id": "ent-1",
                    "source_type": "entity",
                    "text": "MyClass",
                    "embedding": [0.1] * 768,
                }
            ],
        }
        results = service._parse_embedding_event(data)
        assert results == []


class TestParseChunkMessageRouting:
    def test_document_chunk_without_project_id_passes(self, service):
        data = _make_chunk_message()
        parsed = service._parse_chunk_message(data)
        assert parsed is not None
        assert parsed["project_id"] is None

    def test_document_chunk_with_project_id_is_rejected(self, service):
        data = _make_chunk_message(project_id="proj-1")
        parsed = service._parse_chunk_message(data)
        assert parsed is None

    def test_code_chunk_without_project_id_fails(self, service):
        data = _make_chunk_message(content_type="code", project_id=None)
        parsed = service._parse_chunk_message(data)
        assert parsed is None


class TestProcessBatchEdgeTypeRouting:
    """Test _process_batch correctly routes edge_type items to upsert_edge_types."""

    @pytest.mark.asyncio
    async def test_edge_type_routed_to_writer(self, service, mock_writer):
        """edge_type items in a batch should call writer.upsert_edge_types."""
        embedding_data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "branch": "main",
            "embeddings": [
                {
                    "source_id": "et-1",
                    "source_type": "edge_type",
                    "text": "calls",
                    "number_of_edges": 5,
                    "embedding": [0.1] * 768,
                }
            ],
        }

        batch = [("embedding", embedding_data)]
        await service._process_batch(batch, worker_id=0)

        mock_writer.upsert_edge_types.assert_called_once()
        edge_types_arg = mock_writer.upsert_edge_types.call_args[0][0]
        assert len(edge_types_arg) == 1
        assert edge_types_arg[0]["relationship_name"] == "calls"
        assert edge_types_arg[0]["number_of_edges"] == 5

    @pytest.mark.asyncio
    async def test_entity_not_routed_to_edge_types(self, service, mock_writer):
        """Entity payloads must NOT reach upsert_edge_types."""
        embedding_data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "branch": "main",
            "embeddings": [
                {
                    "source_id": "ent-1",
                    "source_type": "entity",
                    "text": "MyClass",
                    "embedding": [0.1] * 768,
                }
            ],
        }

        batch = [("embedding", embedding_data)]
        await service._process_batch(batch, worker_id=0)

        mock_writer.upsert_edge_types.assert_not_called()
        mock_writer.upsert_entities.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_edge_types_not_written(self, service, mock_writer):
        """upsert_edge_types should not be called when batch contains no edge_type items."""
        embedding_data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "branch": "main",
            "embeddings": [],
        }

        batch = [("embedding", embedding_data)]
        await service._process_batch(batch, worker_id=0)

        mock_writer.upsert_edge_types.assert_not_called()


class TestProcessBatchEntityTypeRouting:
    """Test _process_batch correctly routes entity_type items to upsert_entity_types (FIX 1)."""

    @pytest.mark.asyncio
    async def test_entity_type_routed_to_writer(self, service, mock_writer):
        """entity_type items in a batch should call writer.upsert_entity_types."""
        embedding_data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "branch": "main",
            "embeddings": [
                {
                    "source_id": "et-1",
                    "source_type": "entity_type",
                    "text": "class",
                    "number_of_entities": 12,
                    "embedding": [0.2] * 768,
                }
            ],
        }

        batch = [("embedding", embedding_data)]
        await service._process_batch(batch, worker_id=0)

        mock_writer.upsert_entity_types.assert_called_once()
        entity_types_arg = mock_writer.upsert_entity_types.call_args[0][0]
        assert len(entity_types_arg) == 1
        assert entity_types_arg[0]["name"] == "class"
        assert entity_types_arg[0]["number_of_entities"] == 12

    @pytest.mark.asyncio
    async def test_entity_not_routed_to_entity_types(self, service, mock_writer):
        """Entity payloads must NOT reach upsert_entity_types."""
        embedding_data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "branch": "main",
            "embeddings": [
                {
                    "source_id": "ent-1",
                    "source_type": "entity",
                    "text": "MyClass",
                    "embedding": [0.1] * 768,
                }
            ],
        }

        batch = [("embedding", embedding_data)]
        await service._process_batch(batch, worker_id=0)

        mock_writer.upsert_entity_types.assert_not_called()
        mock_writer.upsert_entities.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_entity_types_not_written(self, service, mock_writer):
        """upsert_entity_types should not be called when batch contains no entity_type items."""
        embedding_data = {
            "ingestion_id": "ing-1",
            "company_id": "comp-1",
            "project_id": "proj-1",
            "branch": "main",
            "embeddings": [],
        }

        batch = [("embedding", embedding_data)]
        await service._process_batch(batch, worker_id=0)

        mock_writer.upsert_entity_types.assert_not_called()


class TestDeleteHandling:
    """Test delete message detection and routing in consume loop."""

    @pytest.mark.asyncio
    async def test_handle_delete_calls_writer(self, service, mock_writer):
        """_handle_delete calls writer.delete_by_file_version_id."""
        mock_writer.delete_by_file_version_id = AsyncMock(
            return_value={
                "DocumentChunk_text": 10,
                "TextSummary_text": 5,
                "Triplet_text": 3,
                "EdgeType_relationship_name": 1,
                "Entity_name": 2,
                "EntityType_name": 0,
            }
        )

        data = {
            "action": "delete",
            "file_version_id": "fv-123",
            "company_id": "comp-1",
            "project_id": "proj-1",
        }

        await service._handle_delete(data)

        mock_writer.delete_by_file_version_id.assert_called_once_with(
            "fv-123",
            company_id="comp-1",
        )

    @pytest.mark.asyncio
    async def test_handle_delete_missing_file_version_id(self, service, mock_writer):
        """_handle_delete skips when file_version_id is missing."""
        mock_writer.delete_by_file_version_id = AsyncMock()

        data = {
            "action": "delete",
            # no file_version_id
        }

        await service._handle_delete(data)

        mock_writer.delete_by_file_version_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_delete_survives_writer_error(self, service, mock_writer):
        """_handle_delete logs error but doesn't crash on writer failure."""
        mock_writer.delete_by_file_version_id = AsyncMock(side_effect=RuntimeError("Qdrant down"))

        data = {
            "action": "delete",
            "file_version_id": "fv-123",
        }

        # Should not raise
        await service._handle_delete(data)

        mock_writer.delete_by_file_version_id.assert_called_once_with(
            "fv-123",
            company_id=None,
        )

    def test_delete_message_not_parsed_as_chunk(self, service):
        """Delete messages should NOT be parsed by _parse_chunk_message."""
        data = {
            "action": "delete",
            "file_version_id": "fv-123",
            # No chunk_id or embedding
        }

        result = service._parse_chunk_message(data)
        assert result is None  # Missing chunk_id/embedding

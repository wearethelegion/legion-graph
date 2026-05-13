"""Tests for QdrantBatchWriter (writer.py).

Qdrant client is fully mocked — no real Qdrant needed.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from qdrant_client.http.exceptions import UnexpectedResponse
from neo4j_storage_service.writer import _build_node_set as _build_neo4j_node_set
from qdrant_storage_service.config import QdrantStorageConfig
from qdrant_storage_service.writer import QdrantBatchWriter, _build_canonical_node_set


@pytest.fixture
def mock_client():
    """Create a mock AsyncQdrantClient."""
    client = AsyncMock()
    collections_response = MagicMock()
    collections_response.collections = []
    client.get_collections.return_value = collections_response
    return client


@pytest.fixture
def writer(mock_client):
    """Create QdrantBatchWriter with mock client."""
    return QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)


def _assert_scope_payload(payload, expected):
    assert payload["belongs_to_set"] == [expected]
    assert payload["node_set"] == expected
    assert payload["source_node_set"] == expected


class TestEnsureCollections:
    """Test collection creation logic."""

    @pytest.mark.asyncio
    async def test_creates_all_collections_when_none_exist(self, writer, mock_client):
        await writer.ensure_collections()

        assert mock_client.create_collection.call_count == 6  # FIX 1: Added EntityType_name
        created_names = [
            call.kwargs["collection_name"] for call in mock_client.create_collection.call_args_list
        ]
        assert "DocumentChunk_text" in created_names
        assert "Entity_name" in created_names
        assert "TextSummary_text" in created_names
        assert "Triplet_text" in created_names
        assert "EdgeType_relationship_name" in created_names
        assert "EntityType_name" in created_names  # FIX 1: Added EntityType_name

    @pytest.mark.asyncio
    async def test_skips_existing_collections(self, writer, mock_client):
        # Simulate one collection already existing
        existing_col = MagicMock()
        existing_col.name = "DocumentChunk_text"
        collections_response = MagicMock()
        collections_response.collections = [existing_col]
        mock_client.get_collections.return_value = collections_response

        await writer.ensure_collections()

        assert mock_client.create_collection.call_count == 5  # FIX 1: 6 total - 1 existing = 5
        created_names = [
            call.kwargs["collection_name"] for call in mock_client.create_collection.call_args_list
        ]
        assert "DocumentChunk_text" not in created_names
        assert "Entity_name" in created_names
        assert "TextSummary_text" in created_names
        assert "Triplet_text" in created_names
        assert "EdgeType_relationship_name" in created_names
        assert "EntityType_name" in created_names  # FIX 1: Added EntityType_name


class TestUpsertChunks:
    """Test chunk upsert logic."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.upsert_chunks([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_upserts_chunks(self, writer, mock_client):
        chunks = [
            {
                "chunk_id": "ch-1",
                "embedding": [0.1] * 768,
                "content": "def hello():",
                "header": "# main.py",
                "file_path": "src/main.py",
                "language": "python",
                "repository": "my-repo",
                "branch": "main",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "chunk_index": 0,
                "ingestion_id": "ing-1",
            },
            {
                "chunk_id": "ch-2",
                "embedding": [0.2] * 768,
                "content": "class Foo:",
                "header": "# foo.py",
                "file_path": "src/foo.py",
                "language": "python",
                "repository": "my-repo",
                "branch": "main",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "chunk_index": 1,
                "ingestion_id": "ing-1",
            },
        ]

        result = await writer.upsert_chunks(chunks)
        assert result == 2
        mock_client.upsert.assert_called_once()

        call_kwargs = mock_client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == "DocumentChunk_text"
        assert len(call_kwargs["points"]) == 2

    @pytest.mark.asyncio
    async def test_chunk_payload_structure(self, writer, mock_client):
        chunks = [
            {
                "chunk_id": "ch-1",
                "embedding": [0.1] * 768,
                "content": "code here",
                "header": "header text",
                "file_path": "/src/app.py",
                "language": "python",
                "repository": "repo",
                "branch": "main",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
                "chunk_index": 5,
                "ingestion_id": "ing-1",
            },
        ]

        await writer.upsert_chunks(chunks)

        points = mock_client.upsert.call_args.kwargs["points"]
        point = points[0]
        payload = point.payload

        # Verify named vector format
        assert "text" in point.vector
        assert point.vector["text"] == [0.1] * 768

        # Verify Cognee IndexSchema fields
        assert payload["id"] == "ch-1"
        assert payload["type"] == "IndexSchema"
        assert payload["ontology_valid"] is False
        assert payload["version"] == 1
        assert payload["metadata"] == {"index_fields": ["text"]}
        _assert_scope_payload(payload, "proj-1_my-project_code")
        assert payload["source_pipeline"] == "v2_ingestion"
        assert payload["source_task"] == "chunk_storage"
        assert payload["database_name"] == "cognee-comp-1"

        # Verify content fields
        assert payload["text"] == "code here"
        assert payload["header"] == "header text"
        assert payload["file_path"] == "/src/app.py"
        assert payload["language"] == "python"
        assert payload["company_id"] == "comp-1"
        assert payload["chunk_index"] == 5

    @pytest.mark.asyncio
    async def test_document_chunks_auto_create_knowledge_collection_on_404(self, mock_client):
        mock_client.upsert.side_effect = [
            UnexpectedResponse(status_code=404, reason_phrase="Not found"),
            None,
        ]

        writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)
        chunks = [
            {
                "chunk_id": "doc-1",
                "embedding": [0.9] * 768,
                "content": "This is a document chunk.",
                "header": "doc header",
                "file_path": "document://lesson/1",
                "language": "markdown",
                "repository": "kgrag-documents",
                "branch": "main",
                "company_id": "comp-1",
                "content_type": "document",
                "chunk_index": 0,
                "ingestion_id": "ing-1",
            }
        ]

        result = await writer.upsert_chunks(chunks)
        assert result == 1

        point = mock_client.upsert.call_args.kwargs["points"][0]
        assert point.payload["belongs_to_set"] == ["comp-1_knowledge"]
        assert point.payload["node_set"] == "comp-1_knowledge"
        assert point.payload["source_node_set"] == "comp-1_knowledge"
        assert "project_id" not in point.payload

        create_kwargs = mock_client.create_collection.call_args.kwargs
        assert create_kwargs["collection_name"] == writer._config.COLLECTION_CHUNKS
        assert create_kwargs["vectors_config"]["text"].size == 3072
        assert create_kwargs["vectors_config"]["text"].distance == "Cosine"


class TestUpsertEntities:
    """Test entity upsert logic."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.upsert_entities([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_upserts_entities(self, writer, mock_client):
        entities = [
            {
                "entity_id": "ent-1",
                "embedding": [0.3] * 768,
                "name": "MyClass",
                "entity_type": "class",
                "description": "A test class",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
                "branch": "main",
            },
        ]

        result = await writer.upsert_entities(entities)
        assert result == 1
        mock_client.upsert.assert_called_once()

        call_kwargs = mock_client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == "Entity_name"

        point = call_kwargs["points"][0]
        payload = point.payload

        # Verify named vector format
        assert "text" in point.vector
        assert point.vector["text"] == [0.3] * 768

        # Verify Cognee IndexSchema fields
        assert payload["type"] == "IndexSchema"
        assert payload["metadata"] == {"index_fields": ["name"]}
        _assert_scope_payload(payload, "proj-1_my-project_code")
        assert payload["source_task"] == "entity_storage"

        # Verify entity fields
        assert payload["name"] == "MyClass"
        assert payload["entity_type"] == "class"
        assert payload["description"] == "A test class"


class TestUpsertSummaries:
    """Test summary upsert logic."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.upsert_summaries([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_upserts_summaries(self, writer, mock_client):
        summaries = [
            {
                "summary_id": "sum-1",
                "embedding": [0.5] * 768,
                "summary_text": "This module handles authentication",
                "chunk_id": "ch-1",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
                "branch": "main",
            },
        ]

        result = await writer.upsert_summaries(summaries)
        assert result == 1
        mock_client.upsert.assert_called_once()

        call_kwargs = mock_client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == "TextSummary_text"

        point = call_kwargs["points"][0]
        payload = point.payload

        # Verify named vector format
        assert "text" in point.vector
        assert point.vector["text"] == [0.5] * 768

        # Verify Cognee IndexSchema fields
        assert payload["type"] == "IndexSchema"
        assert payload["metadata"] == {"index_fields": ["text"]}
        _assert_scope_payload(payload, "proj-1_my-project_code")
        assert payload["source_task"] == "summary_storage"

        # Verify summary fields
        assert payload["text"] == "This module handles authentication"
        assert payload["chunk_id"] == "ch-1"


class TestUpsertTriplets:
    """Test triplet upsert logic."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.upsert_triplets([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_upserts_triplets(self, writer, mock_client):
        triplets = [
            {
                "triplet_id": "trip-1",
                "embedding": [0.6] * 768,
                "source_name": "MyClass",
                "target_name": "BaseClass",
                "relationship_type": "inherits",
                "source_entity_id": "ent-1",
                "target_entity_id": "ent-2",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
                "branch": "main",
            },
        ]

        result = await writer.upsert_triplets(triplets)
        assert result == 1
        mock_client.upsert.assert_called_once()

        call_kwargs = mock_client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == "Triplet_text"

        point = call_kwargs["points"][0]
        payload = point.payload

        # Verify named vector format
        assert "text" in point.vector
        assert point.vector["text"] == [0.6] * 768

        # Verify Cognee IndexSchema fields
        assert payload["type"] == "IndexSchema"
        assert payload["metadata"] == {"index_fields": ["text"]}
        _assert_scope_payload(payload, "proj-1_my-project_code")
        assert payload["source_task"] == "triplet_storage"

        # Verify triplet fields
        assert payload["text"] == "MyClass-›inherits-›BaseClass"
        assert payload["from_node_id"] == "ent-1"
        assert payload["to_node_id"] == "ent-2"
        assert payload["relationship_type"] == "inherits"
        assert payload["source_name"] == "MyClass"
        assert payload["target_name"] == "BaseClass"


class TestUpsertEdgeTypes:
    """Test edge type upsert logic."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.upsert_edge_types([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_upserts_edge_types(self, writer, mock_client):
        edge_types = [
            {
                "edge_type_id": "et-1",
                "embedding": [0.7] * 768,
                "relationship_name": "calls",
                "number_of_edges": 42,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
                "branch": "main",
            },
        ]

        result = await writer.upsert_edge_types(edge_types)
        assert result == 1
        mock_client.upsert.assert_called_once()

        call_kwargs = mock_client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == "EdgeType_relationship_name"

        point = call_kwargs["points"][0]
        payload = point.payload

        # Verify named vector format — uses named vector "text"
        assert "text" in point.vector
        assert point.vector["text"] == [0.7] * 768

        # Verify Cognee IndexSchema fields
        assert payload["id"] == "et-1"
        assert payload["type"] == "IndexSchema"
        assert payload["ontology_valid"] is False
        assert payload["version"] == 1
        assert payload["metadata"] == {"index_fields": ["relationship_name"]}
        _assert_scope_payload(payload, "proj-1_my-project_code")
        assert payload["source_pipeline"] == "v2_ingestion"
        assert payload["source_task"] == "edge_type_storage"
        assert payload["database_name"] == "cognee-comp-1"

        # Verify EdgeType-specific fields
        assert payload["relationship_name"] == "calls"
        assert payload["number_of_edges"] == 42
        assert payload["company_id"] == "comp-1"
        assert payload["project_id"] == "proj-1"
        assert payload["branch"] == "main"

    @pytest.mark.asyncio
    async def test_multiple_edge_types(self, writer, mock_client):
        edge_types = [
            {
                "edge_type_id": "et-1",
                "embedding": [0.7] * 768,
                "relationship_name": "calls",
                "number_of_edges": 10,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "branch": "main",
            },
            {
                "edge_type_id": "et-2",
                "embedding": [0.8] * 768,
                "relationship_name": "inherits",
                "number_of_edges": 5,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "branch": "main",
            },
        ]

        result = await writer.upsert_edge_types(edge_types)
        assert result == 2
        mock_client.upsert.assert_called_once()

        call_kwargs = mock_client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == "EdgeType_relationship_name"
        assert len(call_kwargs["points"]) == 2

    @pytest.mark.asyncio
    async def test_edge_type_auto_create_collection_on_404(self, mock_client):
        """Auto-creation works for EdgeType_relationship_name collection too."""
        mock_client.upsert.side_effect = [
            UnexpectedResponse(status_code=404, reason_phrase="Not found"),
            None,
        ]

        with patch.object(QdrantStorageConfig, "RETRY_BASE_DELAY", 0.01):
            writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)

            edge_types = [
                {
                    "edge_type_id": "et-1",
                    "embedding": [0.7] * 768,
                    "relationship_name": "calls",
                    "number_of_edges": 3,
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                    "branch": "main",
                },
            ]

            result = await writer.upsert_edge_types(edge_types)
            assert result == 1

            create_kwargs = mock_client.create_collection.call_args.kwargs
            assert create_kwargs["collection_name"] == "EdgeType_relationship_name"
            # Verify the named vector is "text"
            assert "text" in create_kwargs["vectors_config"]


class TestUpsertEntityTypes:
    """Test EntityType upsert logic (FIX 1)."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.upsert_entity_types([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_upserts_entity_types(self, writer, mock_client):
        entity_types = [
            {
                "entity_type_id": "et-1",
                "embedding": [0.6] * 768,
                "name": "class",
                "number_of_entities": 15,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
                "branch": "main",
            },
        ]

        result = await writer.upsert_entity_types(entity_types)
        assert result == 1
        mock_client.upsert.assert_called_once()

        call_kwargs = mock_client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == "EntityType_name"

        point = call_kwargs["points"][0]
        payload = point.payload

        # Verify named vector format — uses named vector "text"
        assert "text" in point.vector
        assert point.vector["text"] == [0.6] * 768

        # Verify Cognee IndexSchema fields
        assert payload["id"] == "et-1"
        assert payload["type"] == "IndexSchema"
        assert payload["ontology_valid"] is False
        assert payload["version"] == 1
        assert payload["metadata"] == {"index_fields": ["name"]}
        _assert_scope_payload(payload, "proj-1_my-project_code")
        assert payload["source_pipeline"] == "v2_ingestion"
        assert payload["source_task"] == "entity_type_storage"
        assert payload["database_name"] == "cognee-comp-1"

        # Verify EntityType-specific fields
        assert payload["name"] == "class"
        assert payload["number_of_entities"] == 15
        assert payload["company_id"] == "comp-1"
        assert payload["project_id"] == "proj-1"
        assert payload["branch"] == "main"

    @pytest.mark.asyncio
    async def test_multiple_entity_types(self, writer, mock_client):
        entity_types = [
            {
                "entity_type_id": "et-1",
                "embedding": [0.6] * 768,
                "name": "class",
                "number_of_entities": 10,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "branch": "main",
            },
            {
                "entity_type_id": "et-2",
                "embedding": [0.7] * 768,
                "name": "function",
                "number_of_entities": 25,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "branch": "main",
            },
        ]

        result = await writer.upsert_entity_types(entity_types)
        assert result == 2
        mock_client.upsert.assert_called_once()

        call_kwargs = mock_client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == "EntityType_name"
        assert len(call_kwargs["points"]) == 2

    @pytest.mark.asyncio
    async def test_entity_type_auto_create_collection_on_404(self, mock_client):
        """Auto-creation works for EntityType_name collection too."""
        mock_client.upsert.side_effect = [
            UnexpectedResponse(status_code=404, reason_phrase="Not found"),
            None,
        ]

        with patch.object(QdrantStorageConfig, "RETRY_BASE_DELAY", 0.01):
            writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)

            entity_types = [
                {
                    "entity_type_id": "et-1",
                    "embedding": [0.6] * 768,
                    "name": "class",
                    "number_of_entities": 5,
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                    "branch": "main",
                },
            ]

            result = await writer.upsert_entity_types(entity_types)
            assert result == 1

            create_kwargs = mock_client.create_collection.call_args.kwargs
            assert create_kwargs["collection_name"] == "EntityType_name"
            # Verify the named vector is "text"
            assert "text" in create_kwargs["vectors_config"]


class TestCanonicalScopeKeys:
    """Test canonical scope key parity across storage layers and collections."""

    @pytest.mark.parametrize(
        "item",
        [
            {
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
                "content_type": "code",
            },
            {
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
                "content_type": "document",
            },
        ],
    )
    def test_qdrant_helper_matches_neo4j_helper(self, item):
        expected = _build_neo4j_node_set(item)
        actual = _build_canonical_node_set(
            item["company_id"],
            item.get("project_id") or item["company_id"],
            item.get("project_name") or item.get("project_id") or item["company_id"],
            item["content_type"],
        )
        assert actual == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("method_name", "collection_name", "item"),
        [
            (
                "upsert_chunks",
                "DocumentChunk_text",
                {
                    "chunk_id": "doc-1",
                    "embedding": [0.9] * 768,
                    "content": "This is a document chunk.",
                    "header": "doc header",
                    "file_path": "document://lesson/1",
                    "language": "markdown",
                    "repository": "kgrag-documents",
                    "branch": "main",
                    "company_id": "comp-1",
                    "content_type": "document",
                    "chunk_index": 0,
                    "ingestion_id": "ing-1",
                },
            ),
            (
                "upsert_entities",
                "Entity_name",
                {
                    "entity_id": "ent-1",
                    "embedding": [0.3] * 768,
                    "name": "MyClass",
                    "entity_type": "class",
                    "description": "A test class",
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                    "content_type": "document",
                    "branch": "main",
                },
            ),
            (
                "upsert_summaries",
                "TextSummary_text",
                {
                    "summary_id": "sum-1",
                    "embedding": [0.5] * 768,
                    "summary_text": "This module handles authentication",
                    "chunk_id": "ch-1",
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                    "content_type": "document",
                    "branch": "main",
                },
            ),
            (
                "upsert_triplets",
                "Triplet_text",
                {
                    "triplet_id": "trip-1",
                    "embedding": [0.6] * 768,
                    "source_name": "MyClass",
                    "target_name": "BaseClass",
                    "relationship_type": "inherits",
                    "source_entity_id": "ent-1",
                    "target_entity_id": "ent-2",
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                    "content_type": "document",
                    "branch": "main",
                },
            ),
            (
                "upsert_edge_types",
                "EdgeType_relationship_name",
                {
                    "edge_type_id": "et-1",
                    "embedding": [0.7] * 768,
                    "relationship_name": "calls",
                    "number_of_edges": 42,
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                    "content_type": "document",
                    "branch": "main",
                },
            ),
            (
                "upsert_entity_types",
                "EntityType_name",
                {
                    "entity_type_id": "et-1",
                    "embedding": [0.6] * 768,
                    "name": "class",
                    "number_of_entities": 15,
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                    "content_type": "document",
                    "branch": "main",
                },
            ),
        ],
    )
    async def test_document_content_type_uses_company_knowledge_scope(
        self,
        writer,
        mock_client,
        method_name,
        collection_name,
        item,
    ):
        result = await getattr(writer, method_name)([item])
        assert result == 1

        call_kwargs = mock_client.upsert.call_args.kwargs
        assert call_kwargs["collection_name"] == collection_name

        payload = call_kwargs["points"][0].payload
        _assert_scope_payload(payload, "comp-1_knowledge")
        assert "project_id" not in payload


class TestBatchingLogic:
    """Test internal batch splitting and retry logic."""

    @pytest.mark.asyncio
    async def test_splits_into_batches(self, mock_client):
        """Verify large datasets are split into QDRANT_BATCH_SIZE batches."""
        # Use a small batch size for testing
        with patch.object(QdrantStorageConfig, "QDRANT_BATCH_SIZE", 2):
            writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)

            chunks = [
                {
                    "chunk_id": f"ch-{i}",
                    "embedding": [float(i)] * 768,
                    "content": f"content {i}",
                    "ingestion_id": "ing-1",
                }
                for i in range(5)
            ]

            result = await writer.upsert_chunks(chunks)
            assert result == 5
            # 5 chunks / batch_size 2 = 3 batches
            assert mock_client.upsert.call_count == 3

    @pytest.mark.asyncio
    async def test_retry_on_failure(self, mock_client):
        """Verify retry logic on transient failures."""
        mock_client.upsert.side_effect = [
            Exception("Connection refused"),
            None,  # succeeds on retry
        ]

        with patch.object(QdrantStorageConfig, "RETRY_BASE_DELAY", 0.01):
            writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)

            chunks = [
                {
                    "chunk_id": "ch-1",
                    "embedding": [0.1] * 768,
                    "content": "test",
                    "ingestion_id": "ing-1",
                },
            ]

            result = await writer.upsert_chunks(chunks)
            assert result == 1
            assert mock_client.upsert.call_count == 2

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self, mock_client):
        """Verify RuntimeError after exhausting all retries."""
        mock_client.upsert.side_effect = Exception("Permanent failure")

        with patch.object(QdrantStorageConfig, "RETRY_BASE_DELAY", 0.01):
            writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)

            chunks = [
                {
                    "chunk_id": "ch-1",
                    "embedding": [0.1] * 768,
                    "content": "test",
                    "ingestion_id": "ing-1",
                },
            ]

            with pytest.raises(RuntimeError, match="failed after"):
                await writer.upsert_chunks(chunks)

            assert mock_client.upsert.call_count == QdrantStorageConfig.MAX_RETRIES


class TestCollectionAutoCreation:
    """Test lazy collection auto-creation on 'not found' errors."""

    @pytest.mark.asyncio
    async def test_auto_creates_collection_on_404(self, mock_client):
        """When upsert gets 404 (collection not found), auto-create and retry."""
        # First upsert raises 404, second succeeds
        mock_client.upsert.side_effect = [
            UnexpectedResponse(status_code=404, reason_phrase="Not found"),
            None,
        ]

        with patch.object(QdrantStorageConfig, "RETRY_BASE_DELAY", 0.01):
            writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)

            entities = [
                {
                    "entity_id": "ent-1",
                    "embedding": [0.3] * 768,
                    "name": "MyClass",
                    "entity_type": "class",
                    "description": "A test class",
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                    "branch": "main",
                },
            ]

            result = await writer.upsert_entities(entities)
            assert result == 1

            # Verify collection was auto-created
            mock_client.create_collection.assert_called_once()
            create_kwargs = mock_client.create_collection.call_args.kwargs
            assert create_kwargs["collection_name"] == "Entity_name"

            # Verify upsert was called twice (fail + retry after create)
            assert mock_client.upsert.call_count == 2

    @pytest.mark.asyncio
    async def test_auto_creates_only_once(self, mock_client):
        """Auto-creation is attempted only once; subsequent 404s use normal retry."""
        mock_client.upsert.side_effect = UnexpectedResponse(
            status_code=404, reason_phrase="Not found"
        )

        with patch.object(QdrantStorageConfig, "RETRY_BASE_DELAY", 0.01):
            writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)

            chunks = [
                {
                    "chunk_id": "ch-1",
                    "embedding": [0.1] * 768,
                    "content": "test",
                    "ingestion_id": "ing-1",
                },
            ]

            with pytest.raises(RuntimeError, match="failed after"):
                await writer.upsert_chunks(chunks)

            # create_collection called once (auto-create), not on every retry
            mock_client.create_collection.assert_called_once()

    @pytest.mark.asyncio
    async def test_is_collection_not_found_with_unexpected_response(self):
        """_is_collection_not_found detects UnexpectedResponse with 404."""
        error = UnexpectedResponse(status_code=404)
        assert QdrantBatchWriter._is_collection_not_found(error) is True

    @pytest.mark.asyncio
    async def test_is_collection_not_found_ignores_non_404(self):
        """_is_collection_not_found rejects non-404 UnexpectedResponse."""
        error = UnexpectedResponse(status_code=500)
        assert QdrantBatchWriter._is_collection_not_found(error) is False

    @pytest.mark.asyncio
    async def test_is_collection_not_found_string_fallback(self):
        """_is_collection_not_found uses string fallback for generic exceptions."""
        error = Exception("Collection 'Entity_name' not found")
        assert QdrantBatchWriter._is_collection_not_found(error) is True

        error = Exception("Connection refused")
        assert QdrantBatchWriter._is_collection_not_found(error) is False

    @pytest.mark.asyncio
    async def test_collection_vector_map(self, mock_client):
        """_collection_vector_map returns correct mapping for all collections."""
        writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)
        mapping = writer._collection_vector_map()

        assert mapping["DocumentChunk_text"] == "text"
        assert mapping["Entity_name"] == "text"
        assert mapping["TextSummary_text"] == "text"
        assert mapping["Triplet_text"] == "text"
        assert mapping["EdgeType_relationship_name"] == "text"
        assert mapping["EntityType_name"] == "text"  # FIX 1: Added EntityType_name

    @pytest.mark.asyncio
    async def test_auto_create_works_for_all_collection_types(self, mock_client):
        """Auto-creation works for chunks, summaries, and triplets too."""
        # Test with summaries
        mock_client.upsert.side_effect = [
            UnexpectedResponse(status_code=404, reason_phrase="Not found"),
            None,
        ]

        with patch.object(QdrantStorageConfig, "RETRY_BASE_DELAY", 0.01):
            writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)

            summaries = [
                {
                    "summary_id": "sum-1",
                    "embedding": [0.5] * 768,
                    "summary_text": "Test summary",
                    "chunk_id": "ch-1",
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                    "branch": "main",
                },
            ]

            result = await writer.upsert_summaries(summaries)
            assert result == 1

            create_kwargs = mock_client.create_collection.call_args.kwargs
            assert create_kwargs["collection_name"] == "TextSummary_text"


class TestDeleteByFileVersionId:
    """Test delete_by_file_version_id with entity survival logic."""

    @pytest.fixture
    def delete_writer(self, mock_client):
        """Writer with pre-configured mock client for delete tests."""
        # Default: count returns 0, scroll returns empty, delete succeeds
        count_result = MagicMock()
        count_result.count = 0
        mock_client.count.return_value = count_result
        mock_client.scroll.return_value = ([], None)
        mock_client.delete.return_value = None
        return QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)

    @pytest.mark.asyncio
    async def test_returns_zero_counts_when_no_points(self, delete_writer):
        """All collections return 0 when no points match."""
        results = await delete_writer.delete_by_file_version_id("fv-999")

        assert results["DocumentChunk_text"] == 0
        assert results["TextSummary_text"] == 0
        assert results["Triplet_text"] == 0
        assert results["EdgeType_relationship_name"] == 0
        assert results["Entity_name"] == 0
        assert results["EntityType_name"] == 0

    @pytest.mark.asyncio
    async def test_unconditional_delete_calls_qdrant(self, mock_client):
        """Unconditional collections call client.delete when count > 0."""
        count_result = MagicMock()
        count_result.count = 5
        mock_client.count.return_value = count_result
        mock_client.scroll.return_value = ([], None)
        mock_client.delete.return_value = None

        writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)
        results = await writer.delete_by_file_version_id("fv-123")

        # 4 unconditional collections with count=5 each
        assert results["DocumentChunk_text"] == 5
        assert results["TextSummary_text"] == 5
        assert results["Triplet_text"] == 5
        assert results["EdgeType_relationship_name"] == 5

        # client.delete should be called for each unconditional collection
        delete_calls = mock_client.delete.call_args_list
        deleted_collections = [call.kwargs["collection_name"] for call in delete_calls]
        assert "DocumentChunk_text" in deleted_collections
        assert "TextSummary_text" in deleted_collections
        assert "Triplet_text" in deleted_collections
        assert "EdgeType_relationship_name" in deleted_collections

    @pytest.mark.asyncio
    async def test_entity_survival_deletes_when_no_survivor(self, mock_client):
        """Entity points are deleted when no other file_version_id has same name."""
        # count returns 0 for unconditional (skip those), then handle Entity_name
        unconditional_count = MagicMock()
        unconditional_count.count = 0

        # For entity survival: count_by_filter for survivors returns 0
        survivor_count = MagicMock()
        survivor_count.count = 0

        mock_client.count.return_value = unconditional_count

        # Scroll returns one entity point
        entity_point = MagicMock()
        entity_point.id = "ent-1"
        entity_point.payload = {"name": "MyClass", "file_version_id": "fv-123"}

        # First scroll call for Entity_name returns the point, second for EntityType_name returns empty
        mock_client.scroll.side_effect = [
            ([entity_point], None),  # Entity_name scroll
            ([], None),  # EntityType_name scroll
        ]

        # Override count: first 4 calls (unconditional) → 0; then survivor check → 0
        call_count = {"n": 0}
        original_count = unconditional_count

        async def count_side_effect(**kwargs):
            call_count["n"] += 1
            result = MagicMock()
            result.count = 0
            return result

        mock_client.count = AsyncMock(side_effect=count_side_effect)
        mock_client.delete.return_value = None

        writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)
        results = await writer.delete_by_file_version_id("fv-123")

        assert results["Entity_name"] == 1

        # Verify delete was called with point IDs (not filter) for Entity_name
        entity_delete_calls = [
            call
            for call in mock_client.delete.call_args_list
            if call.kwargs.get("collection_name") == "Entity_name"
        ]
        assert len(entity_delete_calls) == 1
        assert entity_delete_calls[0].kwargs["points_selector"] == ["ent-1"]

    @pytest.mark.asyncio
    async def test_entity_survival_keeps_shared_entities(self, mock_client):
        """Entity points survive when another file_version_id has same name."""
        mock_client.delete.return_value = None

        # Scroll returns one entity point
        entity_point = MagicMock()
        entity_point.id = "ent-1"
        entity_point.payload = {"name": "SharedClass", "file_version_id": "fv-123"}

        mock_client.scroll.side_effect = [
            ([entity_point], None),  # Entity_name scroll
            ([], None),  # EntityType_name scroll
        ]

        # Count calls: unconditional → 0, survivor check → 1 (entity survives!)
        call_count = {"n": 0}

        async def count_side_effect(**kwargs):
            call_count["n"] += 1
            result = MagicMock()
            # 4 unconditional counts = 0, then survivor check = 1
            if call_count["n"] <= 4:
                result.count = 0
            else:
                result.count = 1  # Survivor found
            return result

        mock_client.count = AsyncMock(side_effect=count_side_effect)

        writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)
        results = await writer.delete_by_file_version_id("fv-123")

        # Entity survived — 0 deleted
        assert results["Entity_name"] == 0

        # No delete call for Entity_name
        entity_delete_calls = [
            call
            for call in mock_client.delete.call_args_list
            if call.kwargs.get("collection_name") == "Entity_name"
        ]
        assert len(entity_delete_calls) == 0

    @pytest.mark.asyncio
    async def test_entity_without_name_deleted_unconditionally(self, mock_client):
        """Entity points with empty name are deleted without survival check."""
        mock_client.delete.return_value = None

        # Entity with no name
        entity_point = MagicMock()
        entity_point.id = "ent-1"
        entity_point.payload = {"name": "", "file_version_id": "fv-123"}

        mock_client.scroll.side_effect = [
            ([entity_point], None),  # Entity_name scroll
            ([], None),  # EntityType_name scroll
        ]

        async def count_side_effect(**kwargs):
            result = MagicMock()
            result.count = 0
            return result

        mock_client.count = AsyncMock(side_effect=count_side_effect)

        writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)
        results = await writer.delete_by_file_version_id("fv-123")

        assert results["Entity_name"] == 1

    @pytest.mark.asyncio
    async def test_skips_missing_collections(self, mock_client):
        """Collections that don't exist are skipped gracefully."""
        mock_client.count.side_effect = UnexpectedResponse(
            status_code=404, reason_phrase="Not found"
        )
        mock_client.scroll.side_effect = UnexpectedResponse(
            status_code=404, reason_phrase="Not found"
        )

        writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)
        results = await writer.delete_by_file_version_id("fv-123")

        # All should be 0 (skipped)
        for collection, count in results.items():
            assert count == 0, f"{collection} should be 0 but was {count}"

    @pytest.mark.asyncio
    async def test_scroll_pagination(self, mock_client):
        """_scroll_all paginates through multiple pages of results."""
        mock_client.delete.return_value = None

        # Two pages of results
        point1 = MagicMock()
        point1.id = "p1"
        point1.payload = {"name": "A", "file_version_id": "fv-1"}

        point2 = MagicMock()
        point2.id = "p2"
        point2.payload = {"name": "B", "file_version_id": "fv-1"}

        mock_client.scroll.side_effect = [
            ([point1], "offset-2"),  # Page 1 of Entity_name
            ([point2], None),  # Page 2 of Entity_name (last)
            ([], None),  # EntityType_name
        ]

        async def count_side_effect(**kwargs):
            result = MagicMock()
            result.count = 0  # No survivors
            return result

        mock_client.count = AsyncMock(side_effect=count_side_effect)

        writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)
        results = await writer.delete_by_file_version_id("fv-1")

        # Both points should be deleted
        assert results["Entity_name"] == 2

    @pytest.mark.asyncio
    async def test_mixed_survival_and_delete(self, mock_client):
        """Some entities survive while others are deleted in the same batch."""
        mock_client.delete.return_value = None

        # Two entities: one shared, one unique
        shared_point = MagicMock()
        shared_point.id = "ent-shared"
        shared_point.payload = {"name": "SharedClass", "file_version_id": "fv-1"}

        unique_point = MagicMock()
        unique_point.id = "ent-unique"
        unique_point.payload = {"name": "UniqueClass", "file_version_id": "fv-1"}

        mock_client.scroll.side_effect = [
            ([shared_point, unique_point], None),  # Entity_name
            ([], None),  # EntityType_name
        ]

        survivor_calls = {"n": 0}

        async def count_side_effect(**kwargs):
            survivor_calls["n"] += 1
            result = MagicMock()
            if survivor_calls["n"] <= 4:
                result.count = 0  # Unconditional collections
            elif survivor_calls["n"] == 5:
                result.count = 1  # SharedClass has survivor
            else:
                result.count = 0  # UniqueClass has no survivor
            return result

        mock_client.count = AsyncMock(side_effect=count_side_effect)

        writer = QdrantBatchWriter(client=mock_client, config=QdrantStorageConfig)
        results = await writer.delete_by_file_version_id("fv-1")

        assert results["Entity_name"] == 1  # Only UniqueClass deleted

        entity_delete_calls = [
            call
            for call in mock_client.delete.call_args_list
            if call.kwargs.get("collection_name") == "Entity_name"
        ]
        assert len(entity_delete_calls) == 1
        assert entity_delete_calls[0].kwargs["points_selector"] == ["ent-unique"]

"""Tests for Neo4jBatchWriter (writer.py).

Neo4j driver is fully mocked — no real Neo4j needed.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from neo4j_storage_service.writer import Neo4jBatchWriter, _build_node_set, _entity_id
from neo4j_storage_service.config import Neo4jStorageConfig


class _AsyncContextManager:
    """Helper async context manager for mocking neo4j session."""

    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *args):
        return False


class _MockCounters:
    def __init__(self, nodes_created=0, relationships_created=0):
        self.nodes_created = nodes_created
        self.nodes_deleted = 0
        self.relationships_created = relationships_created
        self.relationships_deleted = 0
        self.properties_set = 0


class _MockSummary:
    def __init__(self, nodes_created=0, relationships_created=0):
        self.counters = _MockCounters(nodes_created, relationships_created)


class _MockResult:
    def __init__(self, row_count=0, nodes_created=0, relationships_created=0):
        self._row_count = row_count
        self._summary = _MockSummary(nodes_created, relationships_created)

    def __aiter__(self):
        async def _gen():
            for _ in range(self._row_count):
                yield object()

        return _gen()

    async def consume(self):
        return self._summary


@pytest.fixture
def mock_session():
    """Create a mock Neo4j async session."""
    session = AsyncMock()

    async def _run(query, parameters=None):
        params = parameters or {}
        row_count = len(params.get("nodes", params.get("edges", params.get("mappings", []))))
        is_relationship = "apoc.merge.relationship" in query
        return _MockResult(
            row_count=row_count,
            nodes_created=0 if is_relationship else row_count,
            relationships_created=row_count if is_relationship else 0,
        )

    session.run = AsyncMock(side_effect=_run)
    return session


@pytest.fixture
def mock_driver(mock_session):
    """Create a mock Neo4j AsyncDriver."""
    driver = MagicMock()
    driver.session = MagicMock(return_value=_AsyncContextManager(mock_session))
    return driver


@pytest.fixture
def writer(mock_driver):
    """Create Neo4jBatchWriter with mock driver."""
    return Neo4jBatchWriter(driver=mock_driver, config=Neo4jStorageConfig)


class TestConstraints:
    """Test constraint initialization."""

    @pytest.mark.asyncio
    async def test_ensure_constraints(self, writer, mock_session):
        await writer.ensure_constraints()

        mock_session.run.assert_called_once()
        query = mock_session.run.call_args[0][0]
        assert "CREATE CONSTRAINT IF NOT EXISTS" in query
        assert "FOR (n:`__Node__`)" in query
        assert "REQUIRE n.id IS UNIQUE" in query


class TestWriteEntityNodes:
    """Test entity node MERGE logic with Cognee schema."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.write_entity_nodes([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_entities(self, writer, mock_session):
        entities = [
            {
                "entity_id": "eid-1",
                "name": "MyClass",
                "entity_type": "class",
                "description": "A test class",
                "company_id": "comp-1",
                "project_id": "proj-1",
            },
            {
                "entity_id": "eid-2",
                "name": "my_func",
                "entity_type": "function",
                "description": "",
                "company_id": "comp-1",
                "project_id": "proj-1",
            },
        ]

        result = await writer.write_entity_nodes(entities)
        assert result == 2

        mock_session.run.assert_called_once()
        query = mock_session.run.call_args[0][0]
        assert "UNWIND $nodes AS node" in query
        assert "MERGE (n:`__Node__` {id: node.id})" in query
        assert "apoc.create.addLabels" in query

        params = mock_session.run.call_args[0][1]
        assert len(params["nodes"]) == 2
        assert params["nodes"][0]["id"] == "eid-1"
        assert params["nodes"][0]["label"] == "Entity"
        assert params["nodes"][1]["id"] == "eid-2"

    @pytest.mark.asyncio
    async def test_entity_node_properties(self, writer, mock_session):
        entities = [
            {
                "entity_id": "eid-1",
                "name": "AuthService",
                "entity_type": "class",
                "description": "Handles authentication",
                "company_id": "comp-1",
                "project_id": "proj-1",
            },
        ]

        await writer.write_entity_nodes(entities)
        params = mock_session.run.call_args[0][1]
        node = params["nodes"][0]
        props = node["properties"]
        assert props["name"] == "AuthService"
        assert props["entity_type"] == "class"
        assert props["description"] == "Handles authentication"
        assert props["company_id"] == "comp-1"
        assert props["node_set"] == _build_node_set(entities[0])
        # Check DataPoint properties
        assert props["type"] == "Entity"
        assert props["ontology_valid"] is False
        assert "created_at" in props
        assert "updated_at" in props


class TestWriteEntityTypeNodes:
    """Test EntityType node MERGE logic with Cognee schema."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.write_entity_type_nodes([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_entity_types(self, writer, mock_session):
        types = [
            {"name": "class", "node_set": "proj-1_my-project_code"},
            {"name": "function", "node_set": "proj-1_my-project_code"},
            {"name": "variable", "node_set": "proj-1_my-project_code"},
        ]

        result = await writer.write_entity_type_nodes(types)
        assert result == 3

        mock_session.run.assert_called_once()
        query = mock_session.run.call_args[0][0]
        assert "MERGE (n:`__Node__` {id: node.id})" in query
        assert "apoc.create.addLabels" in query

        params = mock_session.run.call_args[0][1]
        assert len(params["nodes"]) == 3
        names = [n["properties"]["name"] for n in params["nodes"]]
        assert "class" in names
        assert "function" in names
        assert "variable" in names
        # Check DataPoint properties
        assert params["nodes"][0]["properties"]["type"] == "EntityType"
        assert params["nodes"][0]["label"] == "EntityType"
        assert params["nodes"][0]["properties"]["node_set"] == "proj-1_my-project_code"


class TestWriteChunkNodes:
    """Test DocumentChunk node MERGE logic with Cognee schema."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.write_chunk_nodes([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_chunks(self, writer, mock_session):
        chunks = [
            {
                "chunk_id": "ch-1",
                "file_path": "src/main.py",
                "repository": "my-repo",
                "branch": "main",
                "language": "python",
                "chunk_index": 0,
                "company_id": "comp-1",
                "project_id": "proj-1",
            },
        ]

        result = await writer.write_chunk_nodes(chunks)
        assert result == 1

        query = mock_session.run.call_args[0][0]
        assert "MERGE (n:`__Node__` {id: node.id})" in query
        assert "apoc.create.addLabels" in query

        params = mock_session.run.call_args[0][1]
        assert params["nodes"][0]["id"] == "ch-1"
        assert params["nodes"][0]["label"] == "DocumentChunk"
        assert params["nodes"][0]["properties"]["file_path"] == "src/main.py"
        assert params["nodes"][0]["properties"]["chunk_index"] == 0
        assert params["nodes"][0]["properties"]["type"] == "DocumentChunk"
        assert "n.chunk_index = node.properties.chunk_index" in query

    @pytest.mark.asyncio
    async def test_writes_document_chunks_preserve_real_metadata(self, writer, mock_session):
        chunks = [
            {
                "chunk_id": "doc-1",
                "file_path": "document://knowledge/doc-1",
                "repository": "kgrag-documents",
                "branch": "main",
                "language": "markdown",
                "chunk_index": 0,
                "start_line": 0,
                "end_line": 99,
                "company_id": "comp-1",
                "project_id": None,
                "content_type": "document",
                "description": "Intro chunk",
            },
            {
                "chunk_id": "doc-2",
                "file_path": "document://knowledge/doc-1",
                "repository": "kgrag-documents",
                "branch": "main",
                "language": "markdown",
                "chunk_index": 1,
                "start_line": 100,
                "end_line": 199,
                "company_id": "comp-1",
                "project_id": None,
                "content_type": "document",
            },
            {
                "chunk_id": "doc-3",
                "file_path": "document://knowledge/doc-1",
                "repository": "kgrag-documents",
                "branch": "main",
                "language": "markdown",
                "chunk_index": 2,
                "start_line": 200,
                "end_line": 299,
                "company_id": "comp-1",
                "project_id": None,
                "content_type": "document",
            },
        ]

        result = await writer.write_chunk_nodes(chunks)
        assert result == 3

        params = mock_session.run.call_args[0][1]
        assert [node["properties"]["chunk_index"] for node in params["nodes"]] == [0, 1, 2]
        assert [node["properties"]["start_line"] for node in params["nodes"]] == [0, 100, 200]
        assert [node["properties"]["end_line"] for node in params["nodes"]] == [99, 199, 299]
        assert params["nodes"][0]["properties"]["description"] == "Intro chunk"
        assert params["nodes"][1]["properties"]["description"] == (
            "Chunk 1 from document://knowledge/doc-1"
        )
        assert params["nodes"][2]["properties"]["name"].endswith("#2")

    @pytest.mark.asyncio
    async def test_writes_code_chunks_preserve_real_metadata(self, writer, mock_session):
        chunks = [
            {
                "chunk_id": "code-1",
                "file_path": "src/app.py",
                "repository": "my-repo",
                "branch": "main",
                "language": "python",
                "chunk_index": 0,
                "start_line": 1,
                "end_line": 50,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "content_type": "code",
            },
            {
                "chunk_id": "code-2",
                "file_path": "src/app.py",
                "repository": "my-repo",
                "branch": "main",
                "language": "python",
                "chunk_index": 1,
                "start_line": 51,
                "end_line": 100,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "content_type": "code",
            },
        ]

        result = await writer.write_chunk_nodes(chunks)
        assert result == 2

        params = mock_session.run.call_args[0][1]
        assert [node["properties"]["chunk_index"] for node in params["nodes"]] == [0, 1]
        assert [node["properties"]["start_line"] for node in params["nodes"]] == [1, 51]
        assert [node["properties"]["end_line"] for node in params["nodes"]] == [50, 100]
        assert params["nodes"][0]["properties"]["name"].endswith("#0")
        assert params["nodes"][1]["properties"]["name"].endswith("#1")


class TestNodeSetPropagation:
    """Test node_set scalar propagation and linkage consistency."""

    def _assert_node_set(self, props, expected):
        assert props["node_set"] == expected
        assert props["source_node_set"] == expected

    @pytest.mark.asyncio
    async def test_canonical_node_set_values_and_linkage(self, writer, mock_session):
        entities = [
            {
                "entity_id": "eid-code",
                "name": "CodeClass",
                "entity_type": "class",
                "description": "",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "alpha",
                "content_type": "code",
            },
            {
                "entity_id": "eid-doc",
                "name": "DocConcept",
                "entity_type": "concept",
                "description": "",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "alpha",
                "content_type": "document",
            },
        ]
        chunks = [
            {
                "chunk_id": "ch-code",
                "text": "print('hi')",
                "file_path": "src/app.py",
                "repository": "repo",
                "branch": "main",
                "language": "python",
                "chunk_index": 0,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "alpha",
                "content_type": "code",
            },
            {
                "chunk_id": "ch-doc",
                "text": "# Guide",
                "file_path": "docs/guide.md",
                "repository": "repo",
                "branch": "main",
                "language": "markdown",
                "chunk_index": 0,
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "alpha",
                "content_type": "document",
            },
        ]
        summaries = [
            {
                "summary_id": "sum-code",
                "chunk_id": "ch-code",
                "summary_text": "Code summary",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "alpha",
                "file_path": "src/app.py",
                "content_type": "code",
            },
            {
                "summary_id": "sum-doc",
                "chunk_id": "ch-doc",
                "summary_text": "Doc summary",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "alpha",
                "file_path": "docs/guide.md",
                "content_type": "document",
            },
        ]

        await writer.write_entity_nodes(entities)
        entity_records = mock_session.run.call_args[0][1]["nodes"]
        self._assert_node_set(entity_records[0]["properties"], "proj-1_alpha_code")
        self._assert_node_set(entity_records[1]["properties"], "comp-1_knowledge")

        mock_session.run.reset_mock()
        await writer.write_chunk_nodes(chunks)
        chunk_records = mock_session.run.call_args[0][1]["nodes"]
        self._assert_node_set(chunk_records[0]["properties"], "proj-1_alpha_code")
        self._assert_node_set(chunk_records[1]["properties"], "comp-1_knowledge")

        mock_session.run.reset_mock()
        await writer.write_document_nodes(chunks)
        document_records = mock_session.run.call_args_list[0].args[1]["nodes"]
        self._assert_node_set(document_records[0]["properties"], "proj-1_alpha_code")
        self._assert_node_set(document_records[1]["properties"], "comp-1_knowledge")

        mock_session.run.reset_mock()
        await writer.write_summary_nodes(summaries)
        summary_records = mock_session.run.call_args[0][1]["nodes"]
        self._assert_node_set(summary_records[0]["properties"], "proj-1_alpha_code")
        self._assert_node_set(summary_records[1]["properties"], "comp-1_knowledge")
        assert summary_records[0]["properties"]["chunk_index"] == 0
        assert summary_records[1]["properties"]["chunk_index"] == 0

        mock_session.run.reset_mock()
        await writer.write_node_sets(chunks)
        node_set_records = mock_session.run.call_args_list[0].args[1]["nodes"]
        node_set_names = {record["properties"]["source_node_set"] for record in node_set_records}
        assert node_set_names == {"proj-1_alpha_code", "comp-1_knowledge"}

        expected = {_build_node_set(c) for c in chunks}
        assert node_set_names == expected

    @pytest.mark.asyncio
    async def test_document_nodes_use_title_when_available(self, writer, mock_session):
        chunks = [
            {
                "chunk_id": "ch-doc",
                "text": "# Guide",
                "file_path": "document://knowledge/doc-1",
                "repository": "kgrag-documents",
                "branch": "main",
                "language": "markdown",
                "chunk_index": 0,
                "company_id": "comp-1",
                "project_id": None,
                "project_name": None,
                "content_type": "document",
                "document_title": "My Test Lesson",
                "document_slug": "my-test-lesson",
            }
        ]

        await writer.write_document_nodes(chunks)

        document_records = mock_session.run.call_args_list[0].args[1]["nodes"]
        props = document_records[0]["properties"]
        assert props["title"] == "My Test Lesson"
        assert props["name"] == "My Test Lesson"
        assert props["slug"] == "my-test-lesson"
        assert props["content_type"] == "document"

    @pytest.mark.asyncio
    async def test_document_nodes_fallback_name_uses_file_path(self, writer, mock_session):
        chunks = [
            {
                "chunk_id": "ch-doc",
                "text": "# Guide",
                "file_path": "document://knowledge/doc-1",
                "repository": "kgrag-documents",
                "branch": "main",
                "language": "markdown",
                "chunk_index": 0,
                "company_id": "comp-1",
                "project_id": None,
                "project_name": None,
                "content_type": "document",
                "document_slug": "doc-1",
            }
        ]

        await writer.write_document_nodes(chunks)

        document_records = mock_session.run.call_args_list[0].args[1]["nodes"]
        props = document_records[0]["properties"]
        assert props["title"] is None
        assert props["name"] == "document://knowledge/doc-1"
        assert props["slug"] == "doc-1"

    @pytest.mark.asyncio
    async def test_document_nodes_use_file_version_identity(self, writer, mock_session):
        chunks = [
            {
                "chunk_id": "ch-doc-v1",
                "text": "# Guide v1",
                "file_path": "document://knowledge/doc-1-v1",
                "repository": "kgrag-documents",
                "branch": "main",
                "language": "markdown",
                "chunk_index": 0,
                "company_id": "comp-1",
                "project_id": None,
                "project_name": None,
                "content_type": "document",
                "document_title": "My Test Lesson",
                "document_slug": "my-test-lesson",
                "file_version_id": "fv-1",
            },
            {
                "chunk_id": "ch-doc-v2",
                "text": "# Guide v2",
                "file_path": "document://knowledge/doc-1-v2",
                "repository": "kgrag-documents",
                "branch": "main",
                "language": "markdown",
                "chunk_index": 0,
                "company_id": "comp-1",
                "project_id": None,
                "project_name": None,
                "content_type": "document",
                "document_title": "My Test Lesson",
                "document_slug": "my-test-lesson",
                "file_version_id": "fv-2",
            },
        ]

        result = await writer.write_document_nodes(chunks)

        assert result == 2
        first_call = mock_session.run.call_args_list[0]
        document_records = first_call.args[1]["nodes"]
        assert len(document_records) == 2
        assert {record["id"] for record in document_records} == {"fv-1", "fv-2"}
        assert {record["properties"]["file_version_id"] for record in document_records} == {
            "fv-1",
            "fv-2",
        }
        assert all(
            record["properties"]["content_type"] == "document" for record in document_records
        )

        second_call = mock_session.run.call_args_list[1]
        edge_records = second_call.args[1]["edges"]
        assert len(edge_records) == 2
        assert {edge["doc_id"] for edge in edge_records} == {"fv-1", "fv-2"}

    @pytest.mark.asyncio
    async def test_write_repository_node_skips_document_chunks(self, writer, mock_session):
        chunks = [
            {
                "chunk_id": "doc-chunk-1",
                "file_path": "document://knowledge/doc-1",
                "repository": "kgrag-documents",
                "branch": "main",
                "company_id": "comp-1",
                "project_id": None,
                "content_type": "document",
            }
        ]

        result = await writer.write_repository_node(chunks)

        assert result == 0
        mock_session.run.assert_not_called()


class TestWriteLLMEdges:
    """Test LLM relationship edge MERGE logic with APOC dynamic types."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.write_llm_edges([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_edges(self, writer, mock_session):
        edges = [
            {
                "source_entity_id": "eid-1",
                "target_entity_id": "eid-2",
                "relationship_type": "CALLS",
            },
            {
                "source_entity_id": "eid-1",
                "target_entity_id": "eid-3",
                "relationship_type": "IMPORTS",
            },
        ]

        result = await writer.write_llm_edges(edges)
        assert result == 2

        query = mock_session.run.call_args[0][0]
        assert "MATCH (from_node:`__Node__` {id: edge.source_id})" in query
        assert "MATCH (to_node:`__Node__` {id: edge.target_id})" in query
        assert "apoc.merge.relationship" in query

        params = mock_session.run.call_args[0][1]
        assert len(params["edges"]) == 2
        assert params["edges"][0]["source_id"] == "eid-1"
        assert params["edges"][0]["rel_type"] == "CALLS"
        assert params["edges"][0]["properties"]["relationship_name"] == "CALLS"
        assert params["edges"][1]["rel_type"] == "IMPORTS"
        assert params["edges"][1]["properties"]["ontology_valid"] is False


class TestWriteContainsEdges:
    """Test 'contains' edge MERGE logic with APOC and lowercase convention."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.write_contains_edges([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_contains_edges(self, writer, mock_session):
        mappings = [
            {"chunk_id": "ch-1", "entity_id": "eid-1"},
            {"chunk_id": "ch-1", "entity_id": "eid-2"},
            {"chunk_id": "ch-2", "entity_id": "eid-3"},
        ]

        result = await writer.write_contains_edges(mappings)
        assert result == 3

        query = mock_session.run.call_args[0][0]
        assert "MATCH (from_node:`__Node__` {id: edge.chunk_id})" in query
        assert "MATCH (to_node:`__Node__` {id: edge.entity_id})" in query
        assert "apoc.merge.relationship" in query
        assert "'contains'" in query

        params = mock_session.run.call_args[0][1]
        assert params["edges"][0]["properties"]["relationship_name"] == "contains"


class TestWriteIsAEdges:
    """Test 'is_a' edge MERGE logic with APOC and lowercase convention."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, writer):
        result = await writer.write_is_a_edges([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_is_a_edges(self, writer, mock_session):
        entities = [
            {
                "entity_id": "eid-1",
                "entity_type": "class",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
            },
            {
                "entity_id": "eid-2",
                "entity_type": "function",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
            },
        ]

        result = await writer.write_is_a_edges(entities)
        assert result == 2

        query = mock_session.run.call_args[0][0]
        assert "MATCH (from_node:`__Node__` {id: edge.entity_id})" in query
        assert "MATCH (to_node:`__Node__` {id: edge.type_id})" in query
        assert "apoc.merge.relationship" in query
        assert "'is_a'" in query

        params = mock_session.run.call_args[0][1]
        assert params["edges"][0]["properties"]["relationship_name"] == "is_a"
        assert params["edges"][0]["type_id"] == _entity_id("class", "proj-1_my-project_code")

    @pytest.mark.asyncio
    async def test_skips_entities_without_type(self, writer, mock_session):
        entities = [
            {
                "entity_id": "eid-1",
                "entity_type": "class",
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
            },
            {
                "entity_id": "eid-2",
                "entity_type": "",  # empty type
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
            },
            {
                "entity_id": "eid-3",  # missing type
                "company_id": "comp-1",
                "project_id": "proj-1",
                "project_name": "my-project",
            },
        ]

        result = await writer.write_is_a_edges(entities)
        assert result == 1  # only eid-1 has a valid type

        params = mock_session.run.call_args[0][1]
        assert len(params["edges"]) == 1
        assert params["edges"][0]["entity_id"] == "eid-1"


class TestBatchingAndRetry:
    """Test internal batching and retry logic."""

    @pytest.mark.asyncio
    async def test_splits_into_batches(self, mock_session):
        """Verify large datasets are split into NEO4J_BATCH_SIZE batches."""
        driver = MagicMock()
        driver.session = MagicMock(return_value=_AsyncContextManager(mock_session))

        with patch.object(Neo4jStorageConfig, "NEO4J_BATCH_SIZE", 2):
            writer = Neo4jBatchWriter(driver=driver, config=Neo4jStorageConfig)

            entities = [
                {
                    "entity_id": f"eid-{i}",
                    "name": f"Entity{i}",
                    "entity_type": "class",
                    "description": "",
                    "company_id": "comp-1",
                    "project_id": "proj-1",
                }
                for i in range(5)
            ]

            result = await writer.write_entity_nodes(entities)
            assert result == 5
            # 5 entities / batch_size 2 = 3 batches
            assert mock_session.run.call_count == 3

    @pytest.mark.asyncio
    async def test_retry_on_failure(self, mock_session):
        """Verify retry logic on transient failures."""
        mock_session.run.side_effect = [
            Exception("Connection reset"),
            _MockResult(row_count=1, nodes_created=1),  # succeeds on retry
        ]

        driver = MagicMock()
        driver.session = MagicMock(return_value=_AsyncContextManager(mock_session))

        with patch.object(Neo4jStorageConfig, "RETRY_BASE_DELAY", 0.01):
            writer = Neo4jBatchWriter(driver=driver, config=Neo4jStorageConfig)

            types = [{"name": "class", "node_set": "proj-1_my-project_code"}]
            result = await writer.write_entity_type_nodes(types)
            assert result == 1
            assert mock_session.run.call_count == 2

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self, mock_session):
        """Verify RuntimeError after exhausting all retries."""
        mock_session.run.side_effect = Exception("Permanent failure")

        driver = MagicMock()
        driver.session = MagicMock(return_value=_AsyncContextManager(mock_session))

        with patch.object(Neo4jStorageConfig, "RETRY_BASE_DELAY", 0.01):
            writer = Neo4jBatchWriter(driver=driver, config=Neo4jStorageConfig)

            with pytest.raises(RuntimeError, match="failed after"):
                await writer.write_entity_type_nodes(
                    [{"name": "class", "node_set": "proj-1_my-project_code"}]
                )

            assert mock_session.run.call_count == Neo4jStorageConfig.MAX_RETRIES

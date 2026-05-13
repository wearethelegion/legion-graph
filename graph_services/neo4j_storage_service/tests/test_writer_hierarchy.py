"""Unit tests for Neo4jHierarchyWriter (Phase 4.1)."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict

from neo4j_storage_service.writer_hierarchy import Neo4jHierarchyWriter, _hierarchy_id
from neo4j_storage_service.config import Neo4jStorageConfig


# ── Fixtures ──────────────────────────────────────────────────────────────────


class MockCounters:
    def __init__(self, nodes_created=0, relationships_created=0):
        self.nodes_created = nodes_created
        self.relationships_created = relationships_created


class MockSummary:
    def __init__(self, nodes_created=0, relationships_created=0):
        self.counters = MockCounters(nodes_created, relationships_created)


class MockResult:
    def __init__(self, nodes_created=0, relationships_created=0):
        self._summary = MockSummary(nodes_created, relationships_created)

    async def consume(self):
        return self._summary


class MockSession:
    def __init__(self, nodes_created=0, relationships_created=0):
        self._result = MockResult(nodes_created, relationships_created)
        self.last_query = None
        self.last_params = None

    async def run(self, query, params=None):
        self.last_query = query
        self.last_params = params or {}
        return self._result

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class MockDriver:
    def __init__(self, nodes_created=1, relationships_created=1):
        self._session = MockSession(nodes_created, relationships_created)

    def session(self, **kwargs):
        return self._session


@pytest.fixture
def mock_driver():
    return MockDriver()


@pytest.fixture
def hierarchy_writer(mock_driver):
    return Neo4jHierarchyWriter(driver=mock_driver, config=Neo4jStorageConfig)


# ── Tests: _hierarchy_id ────────────────────────────────────────────────────


class TestHierarchyId:
    """Test UUID5 generation for hierarchy nodes."""

    def test_deterministic(self):
        """Same input always produces same ID."""
        id1 = _hierarchy_id("company:abc-123")
        id2 = _hierarchy_id("company:abc-123")
        assert id1 == id2

    def test_different_inputs_different_ids(self):
        """Different inputs produce different IDs."""
        id1 = _hierarchy_id("company:abc-123")
        id2 = _hierarchy_id("project:abc-123")
        assert id1 != id2

    def test_returns_string(self):
        """Returns a string UUID."""
        result = _hierarchy_id("test")
        assert isinstance(result, str)
        assert len(result) == 36  # UUID format


# ── Tests: Company Node ────────────────────────────────────────────────────


class TestWriteCompanyNode:
    """Test Company node writes."""

    @pytest.mark.asyncio
    async def test_writes_company_node(self, hierarchy_writer, mock_driver):
        """Should write company node and return nodes_created count."""
        result = await hierarchy_writer.write_company_node(
            company_id="company-uuid-1",
            company_name="ACME Corp",
            database="test-db",
        )
        assert result == 1  # MockDriver returns 1 node created

    @pytest.mark.asyncio
    async def test_company_node_query_contains_merge(self, hierarchy_writer, mock_driver):
        """Query should use MERGE pattern."""
        await hierarchy_writer.write_company_node(
            company_id="company-uuid-1",
            database="test-db",
        )
        assert "MERGE" in mock_driver._session.last_query
        assert "Company" in mock_driver._session.last_query

    @pytest.mark.asyncio
    async def test_company_node_params(self, hierarchy_writer, mock_driver):
        """Should pass company_id and name to query."""
        await hierarchy_writer.write_company_node(
            company_id="test-company",
            company_name="Test Co",
            database="test-db",
        )
        params = mock_driver._session.last_params
        assert params["company_id"] == "test-company"
        assert params["company_name"] == "Test Co"


# ── Tests: Project Node ────────────────────────────────────────────────────


class TestWriteProjectNode:
    """Test Project node writes."""

    @pytest.mark.asyncio
    async def test_writes_project_node(self, hierarchy_writer, mock_driver):
        """Should write project node."""
        result = await hierarchy_writer.write_project_node(
            project_id="proj-uuid-1",
            company_id="company-uuid-1",
            project_name="my-project",
            database="test-db",
        )
        assert result == 1

    @pytest.mark.asyncio
    async def test_project_query_has_project_edge(self, hierarchy_writer, mock_driver):
        """Query should create has_project edge."""
        await hierarchy_writer.write_project_node(
            project_id="proj-1",
            company_id="co-1",
            database="test-db",
        )
        assert "has_project" in mock_driver._session.last_query
        assert "Project" in mock_driver._session.last_query


# ── Tests: Branch Node ────────────────────────────────────────────────────


class TestWriteBranchNode:
    """Test Branch node writes."""

    @pytest.mark.asyncio
    async def test_writes_branch_node(self, hierarchy_writer, mock_driver):
        """Should write branch node."""
        result = await hierarchy_writer.write_branch_node(
            project_id="proj-1",
            branch_name="develop",
            database="test-db",
        )
        assert result == 1
        assert mock_driver._session.last_params["node_set"] == "proj-1_proj-1_code"

    @pytest.mark.asyncio
    async def test_branch_query_has_branch_edge(self, hierarchy_writer, mock_driver):
        """Query should create has_branch edge."""
        await hierarchy_writer.write_branch_node(
            project_id="proj-1",
            branch_name="main",
            database="test-db",
        )
        assert "has_branch" in mock_driver._session.last_query
        assert "Branch" in mock_driver._session.last_query

    @pytest.mark.asyncio
    async def test_branch_id_is_deterministic(self, hierarchy_writer, mock_driver):
        """Same project_id + branch_name always produces same branch_id."""
        node_set = "proj-1_proj-1_code"
        id1 = _hierarchy_id(f"branch:proj-1:develop:{node_set}")
        id2 = _hierarchy_id(f"branch:proj-1:develop:{node_set}")
        assert id1 == id2

    @pytest.mark.asyncio
    async def test_different_branches_different_ids(self, hierarchy_writer, mock_driver):
        """Different branch names produce different IDs."""
        node_set = "proj-1_proj-1_code"
        id1 = _hierarchy_id(f"branch:proj-1:develop:{node_set}")
        id2 = _hierarchy_id(f"branch:proj-1:main:{node_set}")
        assert id1 != id2


# ── Tests: Full Hierarchy ─────────────────────────────────────────────────


class TestWriteHierarchy:
    """Test full Company → Project → Branch hierarchy."""

    @pytest.mark.asyncio
    async def test_writes_full_hierarchy(self, hierarchy_writer, mock_driver):
        """Should write all three nodes in one query."""
        result = await hierarchy_writer.write_hierarchy(
            company_id="co-1",
            project_id="proj-1",
            branch_name="develop",
            project_name="my-project",
            company_name="ACME",
            database="test-db",
        )
        assert result == 1  # MockDriver returns 1

    @pytest.mark.asyncio
    async def test_hierarchy_query_covers_all_nodes(self, hierarchy_writer, mock_driver):
        """Query should touch Company, Project, and Branch in one pass."""
        await hierarchy_writer.write_hierarchy(
            company_id="co-1",
            project_id="proj-1",
            branch_name="develop",
            database="test-db",
        )
        query = mock_driver._session.last_query
        assert "Company" in query
        assert "Project" in query
        assert "Branch" in query
        assert "has_project" in query
        assert "has_branch" in query


# ── Tests: Business Domain Nodes ────────────────────────────────────────────


class TestWriteBusinessDomainNodes:
    """Test BusinessDomain node writes."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, hierarchy_writer):
        result = await hierarchy_writer.write_business_domain_nodes(
            domains=[], company_id="co-1", database="test-db"
        )
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_domains(self, hierarchy_writer, mock_driver):
        domains = [
            {"canonical_name": "Appointments", "normalised_key": "appointments"},
            {"canonical_name": "Billing", "normalised_key": "billing"},
        ]
        result = await hierarchy_writer.write_business_domain_nodes(
            domains=domains, company_id="co-1", database="test-db"
        )
        assert result == 1  # MockDriver returns 1

    @pytest.mark.asyncio
    async def test_domain_query_has_business_domain_edge(self, hierarchy_writer, mock_driver):
        domains = [{"canonical_name": "Appointments", "normalised_key": "appointments"}]
        await hierarchy_writer.write_business_domain_nodes(
            domains=domains, company_id="co-1", database="test-db"
        )
        assert "BusinessDomain" in mock_driver._session.last_query
        assert "has_business_domain" in mock_driver._session.last_query


# ── Tests: Technical Domain Nodes ────────────────────────────────────────────


class TestWriteTechnicalDomainNodes:
    """Test TechnicalDomain node writes."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, hierarchy_writer):
        result = await hierarchy_writer.write_technical_domain_nodes(
            technical_tags=[], project_id="proj-1", database="test-db"
        )
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_technical_domains(self, hierarchy_writer, mock_driver):
        result = await hierarchy_writer.write_technical_domain_nodes(
            technical_tags=["API Hooks", "State Management"],
            project_id="proj-1",
            database="test-db",
        )
        assert result == 1
        assert mock_driver._session.last_params["node_set"] == "proj-1_proj-1_code"

    @pytest.mark.asyncio
    async def test_technical_domain_query(self, hierarchy_writer, mock_driver):
        await hierarchy_writer.write_technical_domain_nodes(
            technical_tags=["Controllers"],
            project_id="proj-1",
            database="test-db",
        )
        assert "TechnicalDomain" in mock_driver._session.last_query
        assert "has_technical_domain" in mock_driver._session.last_query


# ── Tests: CodeBlock Nodes ────────────────────────────────────────────────────


class TestWriteCodeBlockNodes:
    """Test CodeBlock node writes."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, hierarchy_writer):
        result = await hierarchy_writer.write_code_block_nodes(code_blocks=[], database="test-db")
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_code_blocks(self, hierarchy_writer, mock_driver):
        code_blocks = [
            {
                "entity_id": "ent-1",
                "text": "def authenticate(user, password):",
                "start_line": 10,
                "end_line": 25,
                "file_path": "src/auth.py",
                "language": "python",
                "file_version_id": "fv-1",
                "project_id": "proj-1",
                "branch": "develop",
            }
        ]
        result = await hierarchy_writer.write_code_block_nodes(
            code_blocks=code_blocks, database="test-db"
        )
        assert result == 1
        assert mock_driver._session.last_params["blocks"][0]["node_set"] == "proj-1_proj-1_code"
        assert (
            mock_driver._session.last_params["blocks"][0]["source_node_set"] == "proj-1_proj-1_code"
        )

    @pytest.mark.asyncio
    async def test_code_block_query_has_code_edge(self, hierarchy_writer, mock_driver):
        code_blocks = [
            {
                "entity_id": "ent-1",
                "text": "class Foo:",
                "file_path": "src/foo.py",
                "language": "python",
                "file_version_id": "fv-1",
                "project_id": "proj-1",
                "branch": "main",
            }
        ]
        await hierarchy_writer.write_code_block_nodes(code_blocks=code_blocks, database="test-db")
        assert "CodeBlock" in mock_driver._session.last_query
        assert "has_code" in mock_driver._session.last_query
        assert "has_code_block" in mock_driver._session.last_query

    @pytest.mark.asyncio
    async def test_skips_blocks_without_entity_id(self, hierarchy_writer, mock_driver):
        """Should skip code blocks without entity_id or file_version_id."""
        code_blocks = [
            {
                "entity_id": "",
                "text": "class Foo:",
                "file_path": "src/foo.py",
                "file_version_id": "fv-1",
                "project_id": "proj-1",
                "branch": "main",
            }
        ]
        result = await hierarchy_writer.write_code_block_nodes(
            code_blocks=code_blocks, database="test-db"
        )
        assert result == 0


# ── Tests: Entity Branch Edges ────────────────────────────────────────────────


class TestWriteEntityBranchEdges:
    """Test Entity -[:exists_on]-> Branch edges."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, hierarchy_writer):
        result = await hierarchy_writer.write_entity_branch_edges(
            entity_branch_mappings=[], database="test-db"
        )
        assert result == 0

    @pytest.mark.asyncio
    async def test_writes_edges(self, hierarchy_writer, mock_driver):
        mappings = [
            {"entity_id": "ent-1", "project_id": "proj-1", "branch_name": "develop"},
        ]
        result = await hierarchy_writer.write_entity_branch_edges(
            entity_branch_mappings=mappings, database="test-db"
        )
        assert result == 1  # MockDriver returns 1 relationship

    @pytest.mark.asyncio
    async def test_edge_query_uses_exists_on(self, hierarchy_writer, mock_driver):
        mappings = [{"entity_id": "ent-1", "project_id": "proj-1", "branch_name": "main"}]
        await hierarchy_writer.write_entity_branch_edges(
            entity_branch_mappings=mappings, database="test-db"
        )
        assert "exists_on" in mock_driver._session.last_query

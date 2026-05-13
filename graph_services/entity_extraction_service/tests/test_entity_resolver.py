"""Tests for entity_extraction_service.entity_resolver.

Covers:
- _is_definition_type helper
- ResolutionStats dataclass
- EntityResolver._classify_appearances logic (pure, no I/O)
- EntityResolver.resolve() end-to-end with mocked Neo4j driver
- Edge write batching
- Idempotency (safe to call twice)
- Empty-input guard paths
"""

import time
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from entity_extraction_service.entity_resolver import (
    EntityResolver,
    ResolutionStats,
    _is_definition_type,
    _DEFINITION_TYPES,
)


# ── Helpers ───────────────────────────────────────────────────────────


def _make_appearance(
    entity_id: str = "eid-1",
    entity_type: str = "class",
    entity_name: str = "Foo",
    chunk_id: str = "chunk-1",
    doc_id: str = "doc-1",
    file_path: str = "src/foo.py",
) -> Dict[str, Any]:
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "entity_name": entity_name,
        "chunk_id": chunk_id,
        "doc_id": doc_id,
        "file_path": file_path,
    }


def _make_mock_driver(appearances: List[Dict[str, Any]], relationships_created: int = 1):
    """Build a mock async Neo4j driver that returns the given appearances on fetch."""

    # Build mock result for _fetch_entity_appearances
    mock_fetch_result = AsyncMock()
    mock_fetch_result.data = AsyncMock(return_value=appearances)

    # Build mock summary for _write_edges
    mock_summary = MagicMock()
    mock_summary.counters.relationships_created = relationships_created

    mock_write_result = AsyncMock()
    mock_write_result.consume = AsyncMock(return_value=mock_summary)

    # session.run returns either fetch or write result depending on call sequence
    call_count = {"n": 0}

    async def mock_run(query, params=None):
        call_count["n"] += 1
        # First call: fetch appearances
        if call_count["n"] == 1:
            return mock_fetch_result
        # Subsequent calls: edge writes
        return mock_write_result

    mock_session = AsyncMock()
    mock_session.run = mock_run
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_driver = MagicMock()
    mock_driver.session = MagicMock(return_value=mock_session)

    return mock_driver, mock_session


# ── _is_definition_type ───────────────────────────────────────────────


class TestIsDefinitionType:
    def test_class_is_definition(self):
        assert _is_definition_type("class") is True

    def test_function_is_definition(self):
        assert _is_definition_type("function") is True

    def test_method_is_definition(self):
        assert _is_definition_type("method") is True

    def test_interface_is_definition(self):
        assert _is_definition_type("interface") is True

    def test_enum_is_definition(self):
        assert _is_definition_type("enum") is True

    def test_import_is_not_definition(self):
        assert _is_definition_type("import") is False

    def test_variable_is_not_definition(self):
        assert _is_definition_type("variable") is False

    def test_call_is_not_definition(self):
        assert _is_definition_type("call") is False

    def test_unknown_type_is_not_definition(self):
        assert _is_definition_type("unknown_xyz") is False

    def test_empty_string_is_not_definition(self):
        assert _is_definition_type("") is False

    def test_case_insensitive_class(self):
        assert _is_definition_type("Class") is True
        assert _is_definition_type("CLASS") is True

    def test_case_insensitive_function(self):
        assert _is_definition_type("FUNCTION") is True

    def test_all_definition_types_recognized(self):
        for dtype in _DEFINITION_TYPES:
            assert _is_definition_type(dtype), f"Expected {dtype!r} to be a definition type"


# ── ResolutionStats ───────────────────────────────────────────────────


class TestResolutionStats:
    def test_initial_values_are_zero(self):
        stats = ResolutionStats("ing-1", "proj-1")
        assert stats.entities_analysed == 0
        assert stats.defined_in_created == 0
        assert stats.imported_by_created == 0
        assert stats.errors == 0

    def test_to_dict(self):
        stats = ResolutionStats("ing-1", "proj-1")
        stats.entities_analysed = 5
        stats.defined_in_created = 3
        stats.imported_by_created = 2
        stats.errors = 0
        d = stats.to_dict()
        assert d["ingestion_id"] == "ing-1"
        assert d["project_id"] == "proj-1"
        assert d["entities_analysed"] == 5
        assert d["defined_in_created"] == 3
        assert d["imported_by_created"] == 2
        assert d["errors"] == 0


# ── EntityResolver._classify_appearances ─────────────────────────────


class TestClassifyAppearances:
    """Unit tests for the pure classification logic — no I/O."""

    def setup_method(self):
        self.resolver = EntityResolver(driver=MagicMock(), batch_size=200)

    def _classify(self, appearances):
        stats = ResolutionStats("test-ing", "test-proj")
        defined, imported = self.resolver._classify_appearances(appearances, stats)
        return defined, imported, stats

    def test_single_definition(self):
        apps = [_make_appearance(entity_type="class", doc_id="doc-1")]
        defined, imported, stats = self._classify(apps)
        assert len(defined) == 1
        assert defined[0] == {"entity_id": "eid-1", "doc_id": "doc-1"}
        assert imported == []
        assert stats.entities_analysed == 1

    def test_single_import(self):
        apps = [_make_appearance(entity_type="import", doc_id="doc-1")]
        defined, imported, stats = self._classify(apps)
        assert defined == []
        assert len(imported) == 1
        assert imported[0] == {"entity_id": "eid-1", "doc_id": "doc-1"}

    def test_cross_file_definition_and_import(self):
        """Entity defined in file A, imported in file B."""
        apps = [
            _make_appearance(entity_type="class", doc_id="doc-a"),
            _make_appearance(entity_type="import", doc_id="doc-b"),
        ]
        defined, imported, stats = self._classify(apps)
        assert len(defined) == 1
        assert defined[0]["doc_id"] == "doc-a"
        assert len(imported) == 1
        assert imported[0]["doc_id"] == "doc-b"
        assert stats.entities_analysed == 1  # same entity_id

    def test_definition_type_wins_over_import_in_same_doc(self):
        """When entity appears as 'import' then 'class' in same doc, class wins."""
        apps = [
            _make_appearance(entity_type="import", doc_id="doc-1"),
            _make_appearance(entity_type="class", doc_id="doc-1"),
        ]
        defined, imported, stats = self._classify(apps)
        # Should only produce a defined_in edge (class wins)
        assert len(defined) == 1
        assert imported == []

    def test_multiple_entities_classified_independently(self):
        apps = [
            _make_appearance(entity_id="e1", entity_type="class", doc_id="doc-1"),
            _make_appearance(entity_id="e2", entity_type="variable", doc_id="doc-1"),
            _make_appearance(entity_id="e2", entity_type="variable", doc_id="doc-2"),
        ]
        defined, imported, stats = self._classify(apps)
        defined_ids = {e["entity_id"] for e in defined}
        imported_ids = {e["entity_id"] for e in imported}
        assert "e1" in defined_ids
        assert "e2" in imported_ids
        assert "e1" not in imported_ids
        assert stats.entities_analysed == 2

    def test_empty_appearances_returns_empty(self):
        defined, imported, stats = self._classify([])
        assert defined == []
        assert imported == []
        assert stats.entities_analysed == 0

    def test_unknown_entity_type_goes_to_imported(self):
        apps = [_make_appearance(entity_type="totally_unknown_type", doc_id="doc-1")]
        defined, imported, stats = self._classify(apps)
        assert defined == []
        assert len(imported) == 1

    def test_same_entity_same_doc_deduplicated(self):
        """Same entity in same doc appears in two chunks — should produce only one edge."""
        apps = [
            _make_appearance(chunk_id="chunk-1", entity_type="class", doc_id="doc-1"),
            _make_appearance(chunk_id="chunk-2", entity_type="class", doc_id="doc-1"),
        ]
        defined, imported, _ = self._classify(apps)
        # Only one defined_in edge per (entity, doc) pair
        assert len(defined) == 1

    def test_entity_defined_in_multiple_files(self):
        """Entity with class type in both files → two defined_in edges."""
        apps = [
            _make_appearance(entity_id="e1", entity_type="class", doc_id="doc-a"),
            _make_appearance(entity_id="e1", entity_type="class", doc_id="doc-b"),
        ]
        defined, imported, stats = self._classify(apps)
        assert len(defined) == 2
        assert imported == []


# ── EntityResolver.resolve() — integration with mocked driver ─────────


class TestEntityResolverResolve:
    """End-to-end tests with mocked Neo4j driver."""

    @pytest.mark.asyncio
    async def test_resolve_empty_project_returns_stats(self):
        """When no entities found, resolver returns clean stats with no errors."""
        appearances = []
        mock_driver, _ = _make_mock_driver(appearances)

        resolver = EntityResolver(driver=mock_driver)
        stats = await resolver.resolve(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
        )

        assert stats.entities_analysed == 0
        assert stats.defined_in_created == 0
        assert stats.imported_by_created == 0
        assert stats.errors == 0

    @pytest.mark.asyncio
    async def test_resolve_creates_defined_in_edge(self):
        """Resolver creates defined_in edge for class entity."""
        appearances = [_make_appearance(entity_type="class", doc_id="doc-1")]
        mock_driver, _ = _make_mock_driver(appearances, relationships_created=1)

        resolver = EntityResolver(driver=mock_driver)
        stats = await resolver.resolve(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
        )

        assert stats.entities_analysed == 1
        assert stats.defined_in_created == 1
        assert stats.imported_by_created == 0
        assert stats.errors == 0

    @pytest.mark.asyncio
    async def test_resolve_creates_imported_by_edge(self):
        """Resolver creates imported_by edge for import-type entity."""
        appearances = [_make_appearance(entity_type="import", doc_id="doc-1")]
        mock_driver, _ = _make_mock_driver(appearances, relationships_created=1)

        resolver = EntityResolver(driver=mock_driver)
        stats = await resolver.resolve(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
        )

        assert stats.imported_by_created == 1
        assert stats.defined_in_created == 0
        assert stats.errors == 0

    @pytest.mark.asyncio
    async def test_resolve_uses_correct_database_name(self):
        """Resolver uses `cognee-{company_id}` as default database name."""
        appearances = []
        mock_driver, _ = _make_mock_driver(appearances)

        resolver = EntityResolver(driver=mock_driver)
        await resolver.resolve(
            ingestion_id="ing-1",
            company_id="my-company",
            project_id="proj-1",
        )

        # Verify session was opened with the right database
        mock_driver.session.assert_called_with(database="cognee-my-company")

    @pytest.mark.asyncio
    async def test_resolve_uses_explicit_database(self):
        """Resolver respects explicitly passed database name."""
        appearances = []
        mock_driver, _ = _make_mock_driver(appearances)

        resolver = EntityResolver(driver=mock_driver)
        await resolver.resolve(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            database="my-custom-db",
        )

        mock_driver.session.assert_called_with(database="my-custom-db")

    @pytest.mark.asyncio
    async def test_resolve_fetch_error_returns_error_stats(self):
        """When Neo4j fetch fails, resolver returns stats with error count = 1."""
        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.run = AsyncMock(side_effect=RuntimeError("Neo4j unavailable"))
        mock_driver.session = MagicMock(return_value=mock_session)

        resolver = EntityResolver(driver=mock_driver)
        stats = await resolver.resolve(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
        )

        assert stats.errors >= 1
        assert stats.defined_in_created == 0

    @pytest.mark.asyncio
    async def test_resolve_cross_file_entity_both_edges(self):
        """Entity defined in file A and imported in file B → both edges created."""
        appearances = [
            _make_appearance(entity_id="e1", entity_type="class", doc_id="doc-a"),
            _make_appearance(entity_id="e1", entity_type="import", doc_id="doc-b"),
        ]

        # Build a more flexible mock that tracks call sequence
        call_index = {"n": 0}
        mock_summary = MagicMock()
        mock_summary.counters.relationships_created = 1

        fetch_result = AsyncMock()
        fetch_result.data = AsyncMock(return_value=appearances)

        write_result = AsyncMock()
        write_result.consume = AsyncMock(return_value=mock_summary)

        async def run_side_effect(query, params=None):
            call_index["n"] += 1
            if call_index["n"] == 1:
                return fetch_result
            return write_result

        mock_session = AsyncMock()
        mock_session.run = run_side_effect
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        resolver = EntityResolver(driver=mock_driver)
        stats = await resolver.resolve(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
        )

        assert stats.defined_in_created == 1  # one for doc-a
        assert stats.imported_by_created == 1  # one for doc-b
        assert stats.errors == 0


# ── EntityResolver._write_edges — batching ────────────────────────────


class TestWriteEdgesBatching:
    """Test that _write_edges respects batch_size correctly."""

    @pytest.mark.asyncio
    async def test_empty_edges_no_write(self):
        mock_driver = MagicMock()
        resolver = EntityResolver(driver=mock_driver, batch_size=10)
        total = await resolver._write_edges([], "defined_in", "test-db")
        assert total == 0
        mock_driver.session.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_batch(self):
        """Less than batch_size edges → exactly one session call."""
        mock_summary = MagicMock()
        mock_summary.counters.relationships_created = 3

        mock_result = AsyncMock()
        mock_result.consume = AsyncMock(return_value=mock_summary)

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        edges = [{"entity_id": f"e{i}", "doc_id": f"d{i}"} for i in range(5)]
        resolver = EntityResolver(driver=mock_driver, batch_size=10)
        total = await resolver._write_edges(edges, "defined_in", "test-db")

        assert total == 3
        # One session opened for one batch
        assert mock_driver.session.call_count == 1

    @pytest.mark.asyncio
    async def test_multiple_batches(self):
        """More than batch_size edges → multiple session calls."""
        mock_summary = MagicMock()
        mock_summary.counters.relationships_created = 2

        mock_result = AsyncMock()
        mock_result.consume = AsyncMock(return_value=mock_summary)

        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        edges = [{"entity_id": f"e{i}", "doc_id": f"d{i}"} for i in range(25)]
        resolver = EntityResolver(driver=mock_driver, batch_size=10)
        total = await resolver._write_edges(edges, "imported_by", "test-db")

        # 25 edges / 10 batch_size = 3 batches → 3 sessions
        assert mock_driver.session.call_count == 3
        # Each batch returns 2 relationships_created → 3 * 2 = 6
        assert total == 6

    @pytest.mark.asyncio
    async def test_write_edges_passes_correct_rel_type(self):
        """The relationship type is passed as a parameter (not interpolated into query string)."""
        captured_params: list = []

        mock_summary = MagicMock()
        mock_summary.counters.relationships_created = 0

        mock_result = AsyncMock()
        mock_result.consume = AsyncMock(return_value=mock_summary)

        async def run_capture(query, params=None):
            captured_params.append(params)
            return mock_result

        mock_session = AsyncMock()
        mock_session.run = run_capture
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        edges = [{"entity_id": "e1", "doc_id": "d1"}]
        resolver = EntityResolver(driver=mock_driver, batch_size=10)
        await resolver._write_edges(edges, "defined_in", "test-db")

        assert len(captured_params) == 1
        assert captured_params[0]["rel_type"] == "defined_in"


# ── Idempotency ───────────────────────────────────────────────────────


class TestIdempotency:
    """Running resolve() twice must be safe (MERGE is idempotent)."""

    @pytest.mark.asyncio
    async def test_resolve_twice_no_errors(self):
        """Calling resolve() twice produces no errors (MERGE semantics)."""
        appearances = [_make_appearance(entity_type="class", doc_id="doc-1")]

        # Each resolve call gets its own call_index
        def make_driver_with_appearances(apps):
            call_index = {"n": 0}
            mock_summary = MagicMock()
            mock_summary.counters.relationships_created = 1

            fetch_result = AsyncMock()
            fetch_result.data = AsyncMock(return_value=apps)

            write_result = AsyncMock()
            write_result.consume = AsyncMock(return_value=mock_summary)

            async def run_side(query, params=None):
                call_index["n"] += 1
                return fetch_result if call_index["n"] == 1 else write_result

            sess = AsyncMock()
            sess.run = run_side
            sess.__aenter__ = AsyncMock(return_value=sess)
            sess.__aexit__ = AsyncMock(return_value=False)

            drv = MagicMock()
            drv.session = MagicMock(return_value=sess)
            return drv

        resolver = EntityResolver(driver=make_driver_with_appearances(appearances))
        stats1 = await resolver.resolve("ing-1", "comp-1", "proj-1")

        # Second call with fresh driver (simulates re-run)
        resolver2 = EntityResolver(driver=make_driver_with_appearances(appearances))
        stats2 = await resolver2.resolve("ing-1", "comp-1", "proj-1")

        assert stats1.errors == 0
        assert stats2.errors == 0
        # Both runs report same counts
        assert stats1.defined_in_created == stats2.defined_in_created

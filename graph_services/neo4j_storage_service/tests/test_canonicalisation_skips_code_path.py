"""Code-path batches must not trigger document canonicalisation."""

import pytest

import neo4j_storage_service.canonicalisation as canonicalisation
from neo4j_storage_service.canonicalisation import canonicalise_document_entities
from neo4j_storage_service.tests.canonicalisation_fakes import FakeDriver, FakeGraph


@pytest.mark.asyncio
async def test_code_path_batch_is_untouched():
    graph = FakeGraph()
    driver = FakeDriver({"cognee-company-a": graph})
    writer = type("Writer", (), {"_driver": driver})()

    outcome = await canonicalise_document_entities(
        writer,
        company_id="company-a",
        database="cognee-company-a",
        document_entities=[],
        content_type="code",
    )

    assert outcome.merge_count == 0
    assert driver.session_databases == []
    assert graph.nodes == {}


@pytest.mark.asyncio
async def test_code_path_still_skipped(monkeypatch):
    graph = FakeGraph()
    driver = FakeDriver({"cognee-company-a": graph})
    writer = type("Writer", (), {"_driver": driver})()

    called = False

    def _boom(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("suffix guard must not run for code batches")

    monkeypatch.setattr(canonicalisation, "_should_block_merge", _boom)

    outcome = await canonicalise_document_entities(
        writer,
        company_id="company-a",
        database="cognee-company-a",
        document_entities=[],
        content_type="code",
    )

    assert called is False
    assert outcome.merge_count == 0
    assert driver.session_databases == []
    assert graph.nodes == {}

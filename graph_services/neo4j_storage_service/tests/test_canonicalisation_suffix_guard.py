"""Trailing suffix guard should prevent unsafe document merges."""

import pytest

from neo4j_storage_service.canonicalisation import canonicalise_document_entities
from neo4j_storage_service.tests.canonicalisation_fakes import FakeDriver, FakeGraph


def _make_writer(graph: FakeGraph):
    driver = FakeDriver({"cognee-company-a": graph})
    writer = type("Writer", (), {"_driver": driver})()
    return writer, driver


def _document_entities(graph: FakeGraph):
    return [
        {
            "entity_id": node_id,
            "name": node["name"],
            "entity_type": node["entity_type"],
            "description": node.get("description", ""),
            "company_id": node.get("company_id", "company-a"),
            "file_path": node.get("file_path", "document://lesson/1"),
        }
        for node_id, node in graph.nodes.items()
    ]


@pytest.mark.asyncio
async def test_reject_product_tier_suffix():
    graph = FakeGraph()
    graph.add_node(
        "ezyvet-go",
        name="ezyVet Go",
        entity_type="tool",
        description="",
        company_id="company-a",
        file_path="document://lesson/1",
    )
    graph.add_node(
        "ezyvet",
        name="ezyVet",
        entity_type="tool",
        description="",
        company_id="company-a",
        file_path="document://lesson/1",
    )

    writer, driver = _make_writer(graph)
    outcome = await canonicalise_document_entities(
        writer,
        company_id="company-a",
        database="cognee-company-a",
        document_entities=_document_entities(graph),
        content_type="document",
    )

    assert outcome.merge_count == 0
    assert graph.nodes["ezyvet-go"]["name"] == "ezyVet Go"
    assert graph.nodes["ezyvet"]["name"] == "ezyVet"
    assert driver.runs == [] or all("mergeNodes" not in run["query"] for run in driver.runs)


@pytest.mark.asyncio
async def test_reject_differing_numeric_ratings():
    graph = FakeGraph()
    graph.add_node(
        "rating-6",
        name="Inventory Strength Rating: 6/10",
        entity_type="tool",
        description="",
        company_id="company-a",
        file_path="document://lesson/1",
    )
    graph.add_node(
        "rating-3",
        name="Inventory Strength Rating 3/10",
        entity_type="tool",
        description="",
        company_id="company-a",
        file_path="document://lesson/1",
    )

    writer, _ = _make_writer(graph)
    outcome = await canonicalise_document_entities(
        writer,
        company_id="company-a",
        database="cognee-company-a",
        document_entities=_document_entities(graph),
        content_type="document",
    )

    assert outcome.merge_count == 0
    assert set(graph.nodes) == {"rating-6", "rating-3"}


@pytest.mark.asyncio
async def test_allow_non_numeric_containment():
    graph = FakeGraph()
    graph.add_node(
        "dyn-365-scm",
        name="Dynamics 365 SCM",
        entity_type="tool",
        description="ERP suite",
        company_id="company-a",
        file_path="document://lesson/1",
    )
    graph.add_node(
        "dyn-365",
        name="Dynamics 365",
        entity_type="tool",
        description="ERP suite",
        company_id="company-a",
        file_path="document://lesson/1",
    )

    writer, _ = _make_writer(graph)
    outcome = await canonicalise_document_entities(
        writer,
        company_id="company-a",
        database="cognee-company-a",
        document_entities=_document_entities(graph),
        content_type="document",
    )

    assert outcome.merge_count == 1
    assert outcome.duplicate_to_canonical == {"dyn-365": "dyn-365-scm"}
    assert set(graph.nodes) == {"dyn-365-scm"}


@pytest.mark.asyncio
async def test_allow_vendor_prefix_only():
    graph = FakeGraph()
    graph.add_node(
        "oracle-netsuite",
        name="Oracle NetSuite",
        entity_type="tool",
        description="ERP suite",
        company_id="company-a",
        file_path="document://lesson/1",
    )
    graph.add_node(
        "netsuite",
        name="NetSuite",
        entity_type="tool",
        description="ERP suite",
        company_id="company-a",
        file_path="document://lesson/1",
    )

    writer, _ = _make_writer(graph)
    outcome = await canonicalise_document_entities(
        writer,
        company_id="company-a",
        database="cognee-company-a",
        document_entities=_document_entities(graph),
        content_type="document",
    )

    assert outcome.merge_count == 1
    assert outcome.duplicate_to_canonical == {"netsuite": "oracle-netsuite"}
    assert set(graph.nodes) == {"oracle-netsuite"}


@pytest.mark.asyncio
async def test_reject_one_has_tier_other_doesnt():
    graph = FakeGraph()
    graph.add_node(
        "windows-pro",
        name="Windows Pro",
        entity_type="tool",
        description="",
        company_id="company-a",
        file_path="document://lesson/1",
    )
    graph.add_node(
        "windows",
        name="Windows",
        entity_type="tool",
        description="",
        company_id="company-a",
        file_path="document://lesson/1",
    )

    writer, _ = _make_writer(graph)
    outcome = await canonicalise_document_entities(
        writer,
        company_id="company-a",
        database="cognee-company-a",
        document_entities=_document_entities(graph),
        content_type="document",
    )

    assert outcome.merge_count == 0
    assert set(graph.nodes) == {"windows-pro", "windows"}

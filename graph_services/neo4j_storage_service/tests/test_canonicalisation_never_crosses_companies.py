"""Canonicalisation must stay inside the current company's Neo4j database."""

import pytest

from neo4j_storage_service.canonicalisation import canonicalise_document_entities
from neo4j_storage_service.tests.canonicalisation_fakes import FakeDriver, FakeGraph


@pytest.mark.asyncio
async def test_canonicalisation_never_touches_second_company_database():
    graph_a = FakeGraph()
    graph_b = FakeGraph()

    graph_a.add_node(
        "a-1",
        name="Microsoft Dynamics 365 Supply Chain Management",
        entity_type="tool",
        description="ERP suite",
        company_id="company-a",
        file_path="document://lesson/1",
    )
    graph_a.add_node(
        "a-2",
        name="Dynamics 365 Supply Chain Management",
        entity_type="tool",
        description="ERP suite",
        company_id="company-a",
        file_path="document://lesson/1",
    )
    graph_b.add_node(
        "b-1",
        name="Dynamics 365 Supply Chain Management",
        entity_type="tool",
        description="ERP suite",
        company_id="company-b",
        file_path="document://lesson/99",
    )

    driver = FakeDriver({"cognee-company-a": graph_a, "cognee-company-b": graph_b})
    writer = type("Writer", (), {"_driver": driver})()

    await canonicalise_document_entities(
        writer,
        company_id="company-a",
        database="cognee-company-a",
        document_entities=[
            {
                "entity_id": "a-1",
                "name": "Microsoft Dynamics 365 Supply Chain Management",
                "entity_type": "tool",
                "description": "ERP suite",
                "company_id": "company-a",
                "file_path": "document://lesson/1",
            },
            {
                "entity_id": "a-2",
                "name": "Dynamics 365 Supply Chain Management",
                "entity_type": "tool",
                "description": "ERP suite",
                "company_id": "company-a",
                "file_path": "document://lesson/1",
            },
        ],
        content_type="document",
    )

    assert driver.session_databases == ["cognee-company-a", "cognee-company-a"]
    assert set(graph_b.nodes) == {"b-1"}
    assert graph_b.nodes["b-1"]["name"] == "Dynamics 365 Supply Chain Management"

"""Document canonicalisation should merge near-duplicates and redirect edges."""

import pytest

from neo4j_storage_service.canonicalisation import canonicalise_document_entities
from neo4j_storage_service.tests.canonicalisation_fakes import FakeDriver, FakeGraph


@pytest.mark.asyncio
async def test_document_duplicates_merge_to_one_canonical_node():
    graph = FakeGraph()
    graph.add_node(
        "src-1",
        name="Source System",
        entity_type="system",
        description="",
        company_id="company-a",
        file_path="document://system/1",
    )
    graph.add_node(
        "tgt-1",
        name="Target System",
        entity_type="system",
        description="",
        company_id="company-a",
        file_path="document://system/1",
    )

    variants = [
        ("eid-1", "Dynamics 365", "ERP suite"),
        ("eid-2", "Dynamics 365 Supply Chain Management", "ERP suite"),
        ("eid-3", "Microsoft Dynamics 365 Supply Chain Management", "ERP suite"),
        ("eid-4", "Microsoft Dynamics 365 Supply Chain Management (IDEXX)", "ERP suite"),
        ("eid-5", "Dynamics 365 Supply Chain Management (IDEXX)", "ERP suite"),
    ]
    for node_id, name, description in variants:
        graph.add_node(
            node_id,
            name=name,
            entity_type="tool",
            description=description,
            company_id="company-a",
            file_path="document://lesson/1",
        )
        graph.add_relationship("src-1", "relates_to", node_id)
        graph.add_relationship(node_id, "uses", "tgt-1")

    driver = FakeDriver({"cognee-company-a": graph})
    writer = type("Writer", (), {"_driver": driver})()

    outcome = await canonicalise_document_entities(
        writer,
        company_id="company-a",
        database="cognee-company-a",
        document_entities=[
            {
                "entity_id": node_id,
                "name": name,
                "entity_type": "tool",
                "description": description,
                "company_id": "company-a",
                "file_path": "document://lesson/1",
            }
            for node_id, name, description in variants
        ],
        content_type="document",
    )

    canonical_id = "eid-3"
    assert outcome.merge_count == 4
    assert outcome.duplicate_to_canonical == {
        "eid-1": canonical_id,
        "eid-2": canonical_id,
        "eid-4": canonical_id,
        "eid-5": canonical_id,
    }
    assert list(driver.session_databases) == ["cognee-company-a", "cognee-company-a"]

    assert set(graph.nodes) == {"src-1", "tgt-1", canonical_id}
    assert graph.nodes[canonical_id]["aliases"] == [
        "Dynamics 365",
        "Dynamics 365 Supply Chain Management",
        "Dynamics 365 Supply Chain Management (IDEXX)",
        "Microsoft Dynamics 365 Supply Chain Management (IDEXX)",
    ]

    assert len(graph.relationships) == 2
    assert {rel["source"] for rel in graph.relationships} == {"src-1", canonical_id}
    assert {rel["target"] for rel in graph.relationships} == {canonical_id, "tgt-1"}

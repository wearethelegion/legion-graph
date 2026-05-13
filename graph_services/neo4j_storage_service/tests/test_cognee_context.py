"""Tests for Cognee context conversion helpers."""

from uuid import uuid4


def test_convert_documents_to_datapoints_uses_company_scope_when_project_missing():
    from neo4j_storage_service.cognee_context import convert_documents_to_datapoints

    datapoints = convert_documents_to_datapoints(
        [
            {
                "chunk_id": str(uuid4()),
                "file_path": "document://lesson/1",
                "repository": "kgrag-documents",
                "branch": "main",
                "company_id": "comp-1",
                "project_id": None,
            }
        ]
    )

    assert len(datapoints) == 1
    assert datapoints[0].project_id == "comp-1"


def test_convert_entities_to_datapoints_uses_company_scope_when_project_missing():
    from neo4j_storage_service.cognee_context import convert_entities_to_datapoints

    datapoints = convert_entities_to_datapoints(
        [
            {
                "entity_id": str(uuid4()),
                "name": "MyClass",
                "company_id": "comp-1",
                "project_id": None,
            }
        ]
    )

    assert len(datapoints) == 1
    assert datapoints[0].project_id == "comp-1"


def test_convert_chunks_to_datapoints_uses_company_scope_when_project_missing():
    from neo4j_storage_service.cognee_context import convert_chunks_to_datapoints

    datapoints = convert_chunks_to_datapoints(
        [
            {
                "chunk_id": str(uuid4()),
                "file_path": "document://lesson/1",
                "company_id": "comp-1",
                "project_id": None,
            }
        ]
    )

    assert len(datapoints) == 1
    assert datapoints[0].project_id == "comp-1"


def test_convert_summaries_to_datapoints_uses_company_scope_when_project_missing():
    from neo4j_storage_service.cognee_context import convert_summaries_to_datapoints

    class Event:
        summary_id = str(uuid4())
        summary_text = "hello"
        chunk_id = str(uuid4())
        company_id = "comp-1"
        project_id = None
        file_path = "document://lesson/1"

    datapoints = convert_summaries_to_datapoints([Event()])

    assert len(datapoints) == 1
    assert datapoints[0].project_id == "comp-1"

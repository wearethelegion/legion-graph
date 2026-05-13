"""Tests for Neo4j Storage Service Pydantic models."""

import pytest


class TestNeo4jEntityNode:
    def test_document_path_allows_missing_project_id(self):
        from neo4j_storage_service.models import Neo4jEntityNode

        node = Neo4jEntityNode(
            entity_id="eid-1",
            name="MyClass",
            entity_type="class",
            description="A node description",
            company_id="comp-1",
            project_id=None,
        )
        assert node.project_id is None


class TestEntityIdLockstep:
    def test_entity_id_matches_entity_extraction_service(self):
        from entity_extraction_service.models import make_entity_id
        from neo4j_storage_service.writer import _entity_id
        from qdrant_storage_service.dedup import entity_id

        assert str(_entity_id("Axios Client", "knowledge_x")) == str(
            make_entity_id("Axios Client", "knowledge_x")
        )
        assert str(entity_id("Axios Client", "knowledge_x")) == str(
            make_entity_id("Axios Client", "knowledge_x")
        )

    def test_same_name_different_node_set_differs(self):
        from neo4j_storage_service.writer import _entity_id

        assert _entity_id("Axios Client", "knowledge_x") != _entity_id("Axios Client", "code_y")


class TestNeo4jChunkNode:
    def test_document_path_allows_missing_project_id(self):
        from neo4j_storage_service.models import Neo4jChunkNode

        node = Neo4jChunkNode(
            chunk_id="ch-1",
            text="hello",
            file_path="document://lesson/1",
            repository="kgrag-documents",
            branch="main",
            language="markdown",
            chunk_index=0,
            company_id="comp-1",
            project_id=None,
        )
        assert node.project_id is None


class TestExtractedEntitiesEvent:
    def test_document_path_allows_missing_project_id(self):
        from neo4j_storage_service.models import ExtractedEntitiesEvent

        event = ExtractedEntitiesEvent(
            ingestion_id="ing-1",
            chunk_id="ch-1",
            company_id="comp-1",
            project_id=None,
            file_version_id="fv-1",
            file_path="document://lesson/1",
            repository="kgrag-documents",
            branch="main",
            language="markdown",
            chunk_text="doc text",
            entities=[],
            edges=[],
            extraction_duration_s=0.1,
            content_type="document",
            document_slug="lesson-overview",
        )
        assert event.project_id is None
        assert event.document_slug == "lesson-overview"

    def test_code_path_requires_project_id(self):
        from pydantic import ValidationError

        from neo4j_storage_service.models import ExtractedEntitiesEvent

        with pytest.raises(ValidationError):
            ExtractedEntitiesEvent(
                ingestion_id="ing-1",
                chunk_id="ch-1",
                company_id="comp-1",
                project_id=None,
                file_version_id="fv-1",
                file_path="src/main.py",
                repository="my-repo",
                branch="main",
                language="python",
                chunk_text="code text",
                entities=[],
                edges=[],
                extraction_duration_s=0.1,
                content_type="code",
            )


class TestTextSummaryEvent:
    def test_document_path_allows_missing_project_id(self):
        from neo4j_storage_service.models import TextSummaryEvent

        event = TextSummaryEvent(
            ingestion_id="ing-1",
            chunk_id="ch-1",
            company_id="comp-1",
            project_id=None,
            content_type="document",
            file_version_id="fv-1",
            file_path="document://lesson/1",
            repository="kgrag-documents",
            branch="main",
            language="markdown",
            summary_text="hello",
            summarization_duration_s=0.1,
        )
        assert event.project_id is None


class TestDeleteEvent:
    def test_document_path_allows_missing_project_id(self):
        from neo4j_storage_service.models import DeleteEvent

        event = DeleteEvent(
            company_id="comp-1",
            project_id=None,
            repository="kgrag-documents",
            branch="main",
            file_path="document://lesson/1",
            ingestion_id="ing-1",
        )
        assert event.project_id is None


class TestNeo4jPayloads:
    def test_entity_and_edge_payloads(self):
        from neo4j_storage_service.models import EdgePayload, EntityPayload

        entity = EntityPayload(
            entity_id="e1",
            name="MyFunction",
            entity_type="function",
            description="desc",
        )
        edge = EdgePayload(
            source_id="e1",
            target_id="e2",
            relationship_type="CALLS",
            source_name="MyFunction",
            target_name="OtherFunction",
        )

        assert entity.name == "MyFunction"
        assert edge.relationship_type == "CALLS"


class TestPipelineEvent:
    def test_document_path_allows_missing_project_id(self):
        from neo4j_storage_service.models import PipelineEvent

        event = PipelineEvent(
            event_type="neo4j_complete",
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id=None,
            chunks_processed=0,
            total_entities=0,
            total_edges=0,
            nodes_written=0,
            edges_written=0,
            entity_nodes=0,
            entity_type_nodes=0,
            chunk_nodes=0,
        )

        assert event.project_id is None

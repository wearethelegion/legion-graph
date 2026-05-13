"""Tests for Summarization Service Pydantic models."""

import pytest


class TestSummaryPayload:
    """Test SummaryPayload serialization."""

    def test_create_with_required_fields(self):
        from summarization_service.models import SummaryPayload

        sp = SummaryPayload(
            chunk_id="chunk-1",
            summary_text="This function prints hello world.",
        )
        assert sp.chunk_id == "chunk-1"
        assert sp.summary_text == "This function prints hello world."
        assert sp.summary_id  # auto-generated

    def test_auto_generated_summary_id_is_unique(self):
        from summarization_service.models import SummaryPayload

        s1 = SummaryPayload(chunk_id="c1", summary_text="summary 1")
        s2 = SummaryPayload(chunk_id="c2", summary_text="summary 2")
        assert s1.summary_id != s2.summary_id

    def test_explicit_summary_id(self):
        from summarization_service.models import SummaryPayload

        sp = SummaryPayload(
            summary_id="my-custom-id",
            chunk_id="chunk-1",
            summary_text="A summary.",
        )
        assert sp.summary_id == "my-custom-id"

    def test_serialization_roundtrip(self):
        from summarization_service.models import SummaryPayload

        sp = SummaryPayload(
            chunk_id="chunk-1",
            summary_text="Defines a helper utility for parsing JSON.",
        )
        data = sp.model_dump()
        assert data["chunk_id"] == "chunk-1"
        assert data["summary_text"] == "Defines a helper utility for parsing JSON."

        restored = SummaryPayload(**data)
        assert restored == sp


class TestTextSummaryEvent:
    """Test TextSummaryEvent model."""

    def test_create_with_required_fields(self):
        from summarization_service.models import TextSummaryEvent

        event = TextSummaryEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            chunk_index=4,
            file_version_id="fv-1",
            file_path="document://lesson/1",
            repository="vet_backend",
            branch="main",
            language="markdown",
            summary_text="A summary of the chunk.",
        )
        assert event.ingestion_id == "ing-1"
        assert event.chunk_id == "chunk-1"
        assert event.chunk_index == 4
        assert event.summary_text == "A summary of the chunk."
        assert event.summarization_duration_s == 0.0
        assert event.event_id  # auto-generated
        assert event.summary_id  # auto-generated
        assert event.timestamp  # auto-generated

    def test_document_path_allows_missing_project_id(self):
        from summarization_service.models import TextSummaryEvent

        event = TextSummaryEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id=None,
            content_type="document",
            chunk_index=0,
            file_version_id="fv-1",
            file_path="document://lesson/1",
            repository="kgrag-documents",
            branch="main",
            language="markdown",
            summary_text="A summary of the chunk.",
        )
        assert event.project_id is None

    def test_document_path_rejects_project_id(self):
        from pydantic import ValidationError

        from summarization_service.models import TextSummaryEvent

        with pytest.raises(ValidationError):
            TextSummaryEvent(
                ingestion_id="ing-1",
                chunk_id="chunk-1",
                company_id="comp-1",
                project_id="proj-1",
                content_type="document",
                chunk_index=0,
                file_version_id="fv-1",
                file_path="document://lesson/1",
                repository="kgrag-documents",
                branch="main",
                language="markdown",
                summary_text="A summary of the chunk.",
            )

    def test_code_path_requires_project_id(self):
        from pydantic import ValidationError

        from summarization_service.models import TextSummaryEvent

        with pytest.raises(ValidationError):
            TextSummaryEvent(
                ingestion_id="ing-1",
                chunk_id="chunk-1",
                company_id="comp-1",
                project_id=None,
                chunk_index=0,
                file_version_id="fv-1",
                file_path="src/main.py",
                repository="vet_backend",
                branch="main",
                language="python",
                summary_text="A summary of the chunk.",
            )

    def test_auto_generated_event_id_is_unique(self):
        from summarization_service.models import TextSummaryEvent

        e1 = TextSummaryEvent(
            ingestion_id="i",
            chunk_id="c",
            company_id="co",
            project_id="p",
            file_version_id="fv-1",
            file_path="document://lesson/1",
            repository="vet_backend",
            branch="main",
            language="markdown",
            summary_text="s1",
        )
        e2 = TextSummaryEvent(
            ingestion_id="i",
            chunk_id="c",
            company_id="co",
            project_id="p",
            file_version_id="fv-1",
            file_path="document://lesson/1",
            repository="vet_backend",
            branch="main",
            language="markdown",
            summary_text="s2",
        )
        assert e1.event_id != e2.event_id

    def test_with_duration(self):
        from summarization_service.models import TextSummaryEvent

        event = TextSummaryEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            chunk_index=2,
            file_version_id="fv-1",
            file_path="document://lesson/1",
            repository="vet_backend",
            branch="main",
            language="markdown",
            summary_text="A summary.",
            summarization_duration_s=2.45,
        )
        assert event.summarization_duration_s == 2.45

    def test_serialization_roundtrip(self):
        from summarization_service.models import TextSummaryEvent

        event = TextSummaryEvent(
            ingestion_id="ing-1",
            chunk_id="chunk-1",
            company_id="comp-1",
            project_id="proj-1",
            chunk_index=2,
            file_version_id="fv-1",
            file_path="document://lesson/1",
            repository="vet_backend",
            branch="main",
            language="markdown",
            summary_text="Summary of code chunk.",
            summary_id="sum-123",
            summarization_duration_s=1.5,
        )
        data = event.model_dump()
        restored = TextSummaryEvent(**data)
        assert restored.ingestion_id == event.ingestion_id
        assert restored.chunk_index == 2
        assert restored.summary_text == event.summary_text
        assert restored.summary_id == "sum-123"


class TestPipelineEvent:
    """Test PipelineEvent model."""

    def test_create_with_defaults(self):
        from summarization_service.models import PipelineEvent

        pe = PipelineEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
        )
        assert pe.event_type == "summarization_complete"
        assert pe.chunks_processed == 0
        assert pe.total_summaries == 0
        assert pe.timestamp

    def test_create_with_all_fields(self):
        from summarization_service.models import PipelineEvent

        pe = PipelineEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            chunks_processed=100,
            total_summaries=95,
        )
        assert pe.chunks_processed == 100
        assert pe.total_summaries == 95

    def test_serialization_roundtrip(self):
        from summarization_service.models import PipelineEvent

        pe = PipelineEvent(
            ingestion_id="ing-1",
            company_id="comp-1",
            project_id="proj-1",
            chunks_processed=10,
        )
        data = pe.model_dump()
        restored = PipelineEvent(**data)
        assert restored.ingestion_id == pe.ingestion_id
        assert restored.chunks_processed == 10

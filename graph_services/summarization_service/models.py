"""Pydantic models for Summarization Service (Service 3).

Defines:
- SummaryPayload: wire-format for a single summary
- TextSummaryEvent: Kafka output message published to text-summaries topic
- PipelineEvent: completion signal published to pipeline-events topic
"""

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


# ── Kafka Output Payloads ────────────────────────────────────────────


class SummaryPayload(BaseModel):
    """Single summary result in the Kafka output message."""

    summary_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique summary ID",
    )
    chunk_id: str = Field(..., description="Source chunk UUID")
    summary_text: str = Field(..., description="Generated summary text")


class TextSummaryEvent(BaseModel):
    """Kafka message published to text-summaries topic.

    One event per chunk — contains the summary produced for that chunk.
    """

    event_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique event ID",
    )
    ingestion_id: str = Field(..., description="Ingestion/repo version ID")
    chunk_id: str = Field(..., description="Source chunk UUID")
    company_id: str = Field(..., description="Company UUID (multi-tenancy)")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    content_type: str = Field(default="code", description="Content type: code or document")
    chunk_index: int = Field(default=0, description="0-based chunk index within the source file")
    file_version_id: str = Field(
        ..., description="File version ID from code_processing.repository_file_versions"
    )
    file_path: str = Field(..., description="File path relative to repository root")
    repository: str = Field(..., description="Repository name")
    branch: str = Field(..., description="Git branch name")
    language: str = Field(..., description="Programming language")
    summary_text: str = Field(..., description="Generated summary text")
    summary_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique summary record ID",
    )
    summarization_duration_s: float = Field(
        default=0.0,
        description="Time spent on LLM summarization for this chunk",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Event timestamp (ISO 8601)",
    )

    @model_validator(mode="after")
    def _validate_project_scope(self) -> "TextSummaryEvent":
        if self.content_type == "code" and not self.project_id:
            raise ValueError("project_id is required for code summaries")
        if self.content_type == "document" and self.project_id is not None:
            raise ValueError("project_id must be absent for document summaries")
        return self


# ── Pipeline Events ─────────────────────────────────────────────────


class PipelineEvent(BaseModel):
    """Completion signal published to pipeline-events topic.

    Emitted when all chunks for an ingestion have been summarized.
    """

    event_type: str = Field(
        default="summarization_complete",
        description="Event type identifier",
    )
    ingestion_id: str = Field(..., description="Ingestion/repo version ID")
    company_id: str = Field(..., description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    chunks_processed: int = Field(
        default=0, description="Total chunks processed for this ingestion"
    )
    total_summaries: int = Field(
        default=0, description="Total summaries produced across all chunks"
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
    )

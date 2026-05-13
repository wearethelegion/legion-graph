"""Pydantic models for Embedding Service (Service 4).

Defines:
- EntityInputEvent: incoming message from extracted-entities topic
- SummaryInputEvent: incoming message from text-summaries topic
- EmbeddingPayload: single embedding record
- EmbeddingReadyEvent: Kafka output message published to embeddings-ready topic
- PipelineEvent: completion signal published to pipeline-events topic
"""

from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


# -- Input Events (consumed from upstream topics) ----------------------------


class EntityInputEvent(BaseModel):
    """Message consumed from extracted-entities topic.

    Maps to ExtractedEntitiesEvent from entity_extraction_service.
    Includes both entities (for embedding entity names) and edges (for triplet generation).
    """

    event_id: str = Field(default="", description="Event ID")
    ingestion_id: str = Field(..., min_length=1, description="Ingestion/repo version ID")
    chunk_id: str = Field(..., min_length=1, description="Source chunk UUID")
    company_id: str = Field(..., min_length=1, description="Company UUID (multi-tenancy)")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    content_type: str = Field(default="code", description="Content type: code or document")
    file_version_id: str = Field(
        ...,
        min_length=1,
        description="File version ID from code_processing.repository_file_versions",
    )
    repository: str = Field(default="", description="Repository identifier")
    branch: str = Field(default="", description="Git branch")
    entities: List[dict] = Field(
        default_factory=list, description="Entities with at minimum 'entity_id' and 'name' keys"
    )
    edges: List[dict] = Field(
        default_factory=list,
        description="Edges with source_id, target_id, relationship_type for triplet generation",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Event timestamp",
    )

    @model_validator(mode="after")
    def _validate_project_scope(self) -> "EntityInputEvent":
        if self.content_type == "code" and not self.project_id:
            raise ValueError("project_id is required for code events")
        if self.content_type == "document" and self.project_id is not None:
            raise ValueError("project_id must be absent for document events")
        return self


class SummaryInputEvent(BaseModel):
    """Message consumed from text-summaries topic.

    Maps to TextSummaryEvent from summarization_service.
    """

    event_id: str = Field(default="", description="Event ID")
    ingestion_id: str = Field(..., min_length=1, description="Ingestion/repo version ID")
    chunk_id: str = Field(..., min_length=1, description="Source chunk UUID")
    company_id: str = Field(..., min_length=1, description="Company UUID (multi-tenancy)")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    content_type: str = Field(default="code", description="Content type: code or document")
    file_version_id: str = Field(
        ...,
        min_length=1,
        description="File version ID from code_processing.repository_file_versions",
    )
    repository: str = Field(default="", description="Repository identifier")
    branch: str = Field(default="", description="Git branch")
    summary_text: str = Field(..., description="Generated summary text")
    summary_id: str = Field(default="", description="Summary ID")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Event timestamp",
    )

    @model_validator(mode="after")
    def _validate_project_scope(self) -> "SummaryInputEvent":
        if self.content_type == "code" and not self.project_id:
            raise ValueError("project_id is required for code events")
        if self.content_type == "document" and self.project_id is not None:
            raise ValueError("project_id must be absent for document events")
        return self


# -- Output Payloads --------------------------------------------------------


class EmbeddingPayload(BaseModel):
    """Single embedding record in the Kafka output message."""

    source_id: str = Field(
        ..., min_length=1, description="Entity UUID, Summary UUID, Triplet UUID, or EdgeType UUID"
    )
    source_type: str = Field(
        ..., min_length=1, description="'entity', 'summary', 'triplet', or 'edge_type'"
    )
    text: str = Field(..., min_length=1, description="Original text that was embedded")
    embedding: List[float] = Field(..., min_length=1, description="Embedding vector")
    # Triplet-specific fields (empty for entity/summary/edge_type types)
    from_node_id: str = Field(default="", description="Source entity ID (for triplets only)")
    to_node_id: str = Field(default="", description="Target entity ID (for triplets only)")
    # Entity-specific fields (empty for summary/triplet/edge_type types)
    entity_type: str = Field(
        default="", description="Entity type (class, function, etc.) — for entity only"
    )
    description: str = Field(default="", description="Entity description — for entity only")
    # EdgeType-specific fields (zero for entity/summary/triplet types)
    number_of_edges: int = Field(default=0, description="Edge count (for edge_type only)")


class EmbeddingReadyEvent(BaseModel):
    """Kafka message published to embeddings-ready topic.

    Contains one or more embedding results from a batch.
    """

    event_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique event ID",
    )
    ingestion_id: str = Field(..., min_length=1, description="Ingestion/repo version ID")
    company_id: str = Field(..., min_length=1, description="Company UUID (multi-tenancy)")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    content_type: str = Field(default="code", description="Content type: code or document")
    file_version_id: str = Field(
        ...,
        min_length=1,
        description="File version ID from code_processing.repository_file_versions",
    )
    embeddings: List[EmbeddingPayload] = Field(
        default_factory=list, description="Embedding results produced in this batch"
    )
    embedding_duration_s: float = Field(
        default=0.0, description="Time spent on embedding for this batch"
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Event timestamp (ISO 8601)",
    )
    repository: str = Field(..., description="Repository identifier")
    branch: str = Field(..., description="Git branch")

    @model_validator(mode="after")
    def _validate_project_scope(self) -> "EmbeddingReadyEvent":
        if self.content_type == "code" and not self.project_id:
            raise ValueError("project_id is required for code embeddings")
        if self.content_type == "document" and self.project_id is not None:
            raise ValueError("project_id must be absent for document embeddings")
        return self


# -- Pipeline Events ---------------------------------------------------------


class PipelineEvent(BaseModel):
    """Completion signal published to pipeline-events topic.

    Emitted when all entities + summaries for an ingestion have been embedded.
    """

    event_type: str = Field(
        default="embedding_complete",
        description="Event type identifier",
    )
    ingestion_id: str = Field(..., min_length=1, description="Ingestion/repo version ID")
    company_id: str = Field(..., min_length=1, description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    entities_received: int = Field(default=0, description="Total entity events received")
    summaries_received: int = Field(default=0, description="Total summary events received")
    embeddings_computed: int = Field(default=0, description="Total embeddings computed")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
    )

"""Pydantic models for Qdrant Storage Service (Service 5) — STREAMING VERSION.

Defines:
- QdrantChunkPoint: Qdrant point for DocumentChunk_text collection
- QdrantEntityPoint: Qdrant point for Entity_name collection
- QdrantSummaryPoint: Qdrant point for TextSummary_text collection
- ChunkMessage: enriched chunk message from enriched-code-chunks topic
- EmbeddingReadyEvent: embedding event from embeddings-ready topic
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


# ── Qdrant Point Models ──────────────────────────────────────────────


class QdrantChunkPoint(BaseModel):
    """Qdrant point for the DocumentChunk_text collection.

    Embedding comes from pipeline_chunks.embedding (pre-computed by preprocessor).
    """

    point_id: str = Field(..., description="Chunk UUID — used as Qdrant point ID")
    embedding: List[float] = Field(..., description="768d embedding vector")
    payload: Dict = Field(default_factory=dict, description="Qdrant payload metadata")


class QdrantEntityPoint(BaseModel):
    """Qdrant point for the Entity_name collection.

    Embedding comes from Kafka EmbeddingReadyEvent (computed by embedding service).
    """

    point_id: str = Field(..., description="Entity UUID — used as Qdrant point ID")
    embedding: List[float] = Field(..., description="768d embedding vector")
    payload: Dict = Field(default_factory=dict, description="Qdrant payload metadata")


class QdrantSummaryPoint(BaseModel):
    """Qdrant point for the TextSummary_text collection.

    Embedding comes from Kafka EmbeddingReadyEvent (computed by embedding service).
    """

    point_id: str = Field(..., description="Summary UUID — used as Qdrant point ID")
    embedding: List[float] = Field(..., description="768d embedding vector")
    payload: Dict = Field(default_factory=dict, description="Qdrant payload metadata")


# ── Streaming Input Models ───────────────────────────────────────────


class ChunkMessage(BaseModel):
    """Enriched chunk message from enriched-code-chunks topic.

    Published by preprocessor after chunking and embedding.
    """

    chunk_id: str = Field(..., description="Chunk UUID")
    file_version_id: str = Field(
        ..., description="File version ID from code_processing.repository_file_versions"
    )
    embedding: List[float] = Field(..., description="768d pre-computed embedding")
    chunk_text: str = Field(..., description="Chunk content")
    header: str = Field(default="", description="Chunk header/context")
    file_path: str = Field(..., description="Source file path")
    language: str = Field(default="", description="Programming language")
    repository: str = Field(..., description="Repository identifier")
    branch: str = Field(..., description="Git branch")
    company_id: str = Field(..., description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    content_type: str = Field(default="code", description="Content type: code or document")
    chunk_index: int = Field(..., description="Chunk index within file")
    ingestion_id: str = Field(..., description="Ingestion batch ID")

    @model_validator(mode="after")
    def _validate_project_scope(self) -> "ChunkMessage":
        if self.content_type == "code" and not self.project_id:
            raise ValueError("project_id is required for code chunks")
        if self.content_type == "document" and self.project_id is not None:
            raise ValueError("project_id must be absent for document chunks")
        return self


class EmbeddingPayload(BaseModel):
    """Single embedding within an EmbeddingReadyEvent."""

    source_id: str = Field(..., description="Entity UUID, Summary UUID, or Triplet UUID")
    source_type: str = Field(..., description="'entity', 'summary', or 'triplet'")
    text: str = Field(..., description="Original text that was embedded")
    embedding: List[float] = Field(..., description="768d embedding vector")
    # Triplet-specific fields (empty for entity/summary types)
    from_node_id: str = Field(default="", description="Source entity ID (for triplets only)")
    to_node_id: str = Field(default="", description="Target entity ID (for triplets only)")


class EmbeddingReadyEvent(BaseModel):
    """Embedding event from embeddings-ready topic.

    Published by embedding service after computing embeddings.
    """

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    ingestion_id: str = Field(..., description="Ingestion batch ID")
    company_id: str = Field(..., description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    content_type: str = Field(default="code", description="Content type: code or document")
    file_version_id: str = Field(
        ..., description="File version ID from code_processing.repository_file_versions"
    )
    embeddings: List[EmbeddingPayload] = Field(
        default_factory=list,
        description="Batch of embeddings",
    )
    embedding_duration_s: float = Field(default=0.0)
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
    )
    # TODO: Add repository/branch metadata when upstream services provide it
    repository: str = Field(default="", description="Repository identifier (not yet populated)")
    branch: str = Field(
        default="main", description="Git branch (defaulted until upstream provides it)"
    )

    @model_validator(mode="after")
    def _validate_project_scope(self) -> "EmbeddingReadyEvent":
        if self.content_type == "code" and not self.project_id:
            raise ValueError("project_id is required for code embeddings")
        if self.content_type == "document" and self.project_id is not None:
            raise ValueError("project_id must be absent for document embeddings")
        return self

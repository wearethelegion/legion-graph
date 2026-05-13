"""Pydantic models for Entity Extraction Service (Service 2).

Defines:
- EntityPayload / EdgePayload: wire-format for extracted entities and edges
- ExtractedEntitiesEvent: Kafka output message published to extracted-entities topic
- PipelineEvent: completion signal published to pipeline-events topic
"""

from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID, uuid4, uuid5, NAMESPACE_OID

from pydantic import BaseModel, Field, model_validator


# ── Entity ID generation ─────────────────────────────────────────────


def make_entity_id(name: str, node_set: str) -> UUID:
    """Generate deterministic UUID5 from entity name + node_set.

    Uses the same normalisation as Cognee's generate_node_id / generate_edge_id:
      .lower().replace(" ", "_").replace("'", "")

    This ensures entity IDs stay scope-aware and consistent across services.

    Args:
        name: Entity name to normalize and convert to UUID
        node_set: Scope partition (knowledge/code)

    Returns:
        Deterministic UUID5 based on normalized name and node_set
    """
    return uuid5(
        NAMESPACE_OID,
        f"{name}|{node_set}".lower().replace(" ", "_").replace("'", ""),
    )


def entity_name_to_uuid(name: str, node_set: str) -> UUID:
    """Backward-compatible alias for make_entity_id."""
    return make_entity_id(name, node_set)


# ── Kafka Output Payloads ────────────────────────────────────────────


class EntityPayload(BaseModel):
    """Single extracted entity in the Kafka output message."""

    entity_id: str = Field(
        ..., min_length=1, description="UUID5 derived from entity name + node_set"
    )
    name: str = Field(..., min_length=1, description="Entity name as extracted by LLM")
    entity_type: str = Field(..., min_length=1, description="Entity type (class, function, etc.)")
    description: str = Field(..., min_length=1, description="Entity description from LLM")
    properties: dict = Field(
        default_factory=dict,
        description="Extra properties extracted by LLM",
    )


class EdgePayload(BaseModel):
    """Single extracted edge in the Kafka output message."""

    source_id: str = Field(..., min_length=1, description="Source entity UUID")
    target_id: str = Field(..., min_length=1, description="Target entity UUID")
    relationship_type: str = Field(..., min_length=1, description="Relationship label")
    source_name: str = Field(
        ..., min_length=1, description="Source entity name for edge_text construction"
    )
    target_name: str = Field(
        ..., min_length=1, description="Target entity name for edge_text construction"
    )
    properties: dict = Field(
        default_factory=dict,
        description="Extra edge properties",
    )


class ExtractedEntitiesEvent(BaseModel):
    """Kafka message published to extracted-entities topic.

    One event per chunk — contains all entities and edges extracted from that chunk.
    """

    event_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique event ID",
    )
    ingestion_id: str = Field(..., description="Ingestion/repo version ID")
    chunk_id: str = Field(..., description="Source chunk UUID")
    company_id: str = Field(..., description="Company UUID (multi-tenancy)")
    project_id: Optional[str] = Field(
        default=None, description="Project UUID (None for company-level documents)"
    )
    file_version_id: str = Field(
        ..., description="File version ID from code_processing.repository_file_versions"
    )
    file_path: str = Field(default="", description="File path relative to repository root")
    repository: str = Field(default="", description="Repository name")
    branch: str = Field(default="", description="Git branch name")
    language: str = Field(default="", description="Programming language")
    content_type: str = Field(default="code", description="Content type: code or document")
    document_title: Optional[str] = Field(
        default=None,
        description="Human-readable title of the source document (documents only)",
    )
    document_slug: Optional[str] = Field(
        default=None,
        description="Machine-friendly slug for the source document (documents only)",
    )
    start_line: int = Field(
        default=0, description="Start line in source file (1-indexed, 0=unknown)"
    )
    end_line: int = Field(default=0, description="End line in source file (1-indexed, 0=unknown)")
    chunk_index: int = Field(default=0, description="0-based chunk index within the source file")
    chunk_text: str = Field(
        default="",
        description="Raw chunk text content for storage on Neo4j DocumentChunk nodes",
    )
    entities: List[EntityPayload] = Field(
        default_factory=list,
        description="Entities extracted from this chunk",
    )
    edges: List[EdgePayload] = Field(
        default_factory=list,
        description="Edges extracted from this chunk",
    )
    extraction_duration_s: float = Field(
        default=0.0,
        description="Time spent on LLM extraction for this chunk",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Event timestamp (ISO 8601)",
    )

    @model_validator(mode="after")
    def _validate_project_scope(self) -> "ExtractedEntitiesEvent":
        if self.content_type == "code" and not self.project_id:
            raise ValueError("project_id is required for code chunks")
        if self.content_type == "document" and self.project_id is not None:
            raise ValueError("project_id must be absent for document chunks")
        return self


# ── Pipeline Events ─────────────────────────────────────────────────


class PipelineEvent(BaseModel):
    """Completion signal published to pipeline-events topic.

    Emitted when all chunks for an ingestion have been processed.
    """

    event_type: str = Field(
        default="extraction_complete",
        description="Event type identifier",
    )
    ingestion_id: str = Field(..., description="Ingestion/repo version ID")
    company_id: str = Field(..., description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    chunks_processed: int = Field(
        default=0, description="Total chunks processed for this ingestion"
    )
    total_entities: int = Field(default=0, description="Total entities extracted across all chunks")
    total_edges: int = Field(default=0, description="Total edges extracted across all chunks")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
    )

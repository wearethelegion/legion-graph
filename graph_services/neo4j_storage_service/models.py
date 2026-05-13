"""Pydantic models for Neo4j Storage Service (Service 6).

Defines:
- Neo4jEntityNode: entity node to be MERGE'd into Neo4j
- Neo4jEntityTypeNode: entity type node (class, function, etc.)
- Neo4jChunkNode: DocumentChunk node
- Neo4jEdge: relationship edge between nodes
- ExtractedEntitiesEvent: consumed from extracted-entities topic
- EntityPayload, EdgePayload: sub-models for extracted entities
- PipelineEvent: completion/trigger signal from/to pipeline-events topic

Phase 4.1 additions:
- Neo4jCompanyNode: top-level company hierarchy node
- Neo4jProjectNode: project hierarchy node (under Company)
- Neo4jBranchNode: branch hierarchy node (under Project)
- Neo4jBusinessDomainNode: company-level business domain classification
- Neo4jTechnicalDomainNode: project-level technical domain classification
- Neo4jCodeBlockNode: code text block linked to Entity (separate from DocumentChunk)
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


# ── Kafka Input Models (from entity extraction service) ─────────────────


class EntityPayload(BaseModel):
    """Single extracted entity from Kafka message."""

    entity_id: str = Field(..., min_length=1, description="UUID5 derived from entity name")
    name: str = Field(..., min_length=1, description="Entity name as extracted by LLM")
    entity_type: str = Field(..., min_length=1, description="Entity type (class, function, etc.)")
    description: str = Field(..., min_length=1, description="Entity description from LLM")
    properties: dict = Field(
        default_factory=dict,
        description="Extra properties extracted by LLM",
    )


class EdgePayload(BaseModel):
    """Single extracted edge from Kafka message."""

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
    """Kafka message consumed from extracted-entities topic.

    One event per chunk — contains all entities and edges extracted from that chunk.
    """

    event_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique event ID",
    )
    ingestion_id: str = Field(..., min_length=1, description="Ingestion/repo version ID")
    chunk_id: str = Field(..., min_length=1, description="Source chunk UUID")
    company_id: str = Field(..., min_length=1, description="Company UUID (multi-tenancy)")
    project_id: Optional[str] = Field(
        default=None, description="Project UUID (None for company-level documents)"
    )
    content_type: str = Field(default="code", description="Content type: code or document")
    document_title: Optional[str] = Field(
        default=None,
        description="Human-readable title of the source document (documents only)",
    )
    document_slug: Optional[str] = Field(
        default=None,
        description="Machine-friendly slug for the source document (documents only)",
    )
    chunk_index: int = Field(
        default=0,
        description="0-based chunk index within the source file/document",
    )
    start_line: int = Field(
        default=0,
        description="1-indexed start line (0 = unknown; documents may not carry line data)",
    )
    end_line: int = Field(
        default=0,
        description="1-indexed end line (0 = unknown; documents may not carry line data)",
    )
    description: Optional[str] = Field(
        default=None,
        description="Chunk description (derived from chunk_index+file_path when absent)",
    )
    file_version_id: str = Field(
        ...,
        min_length=1,
        description="File version ID from code_processing.repository_file_versions",
    )
    file_path: str = Field(..., min_length=1, description="File path relative to repository root")
    repository: str = Field(..., min_length=1, description="Repository name")
    branch: str = Field(..., min_length=1, description="Git branch name")
    language: str = Field(..., description="Programming language (empty for non-code files)")
    chunk_text: str = Field(
        ...,
        min_length=1,
        description="Raw chunk text content for storage on Neo4j DocumentChunk nodes",
    )
    entities: List[EntityPayload] = Field(
        ...,
        description="Entities extracted from this chunk (empty for non-code files)",
    )
    edges: List[EdgePayload] = Field(
        ...,
        description="Edges extracted from this chunk",
    )
    extraction_duration_s: float = Field(
        ..., description="Time spent on LLM extraction for this chunk"
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


# ── Neo4j Node Models ────────────────────────────────────────────────


class Neo4jEntityNode(BaseModel):
    """Entity node to be written to Neo4j.

    Label: Entity
    Unique key: entity_id
    """

    entity_id: str = Field(..., min_length=1, description="UUID5 entity ID")
    name: str = Field(..., min_length=1, description="Entity name")
    entity_type: str = Field(..., min_length=1, description="Entity type (class, function, etc.)")
    description: str = Field(..., min_length=1, description="Entity description")
    company_id: str = Field(..., min_length=1, description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    properties: Dict = Field(default_factory=dict, description="Extra properties")


class Neo4jEntityTypeNode(BaseModel):
    """EntityType node to be written to Neo4j.

    Label: EntityType
    Unique key: name (e.g. 'class', 'function', 'variable')
    """

    name: str = Field(..., min_length=1, description="Entity type name")


class Neo4jChunkNode(BaseModel):
    """DocumentChunk node to be written to Neo4j.

    Label: DocumentChunk
    Unique key: chunk_id
    """

    chunk_id: str = Field(..., min_length=1, description="Chunk UUID")
    text: str = Field(..., min_length=1, description="Raw chunk text content")
    file_path: str = Field(..., min_length=1, description="Source file path")
    repository: str = Field(..., min_length=1, description="Repository name")
    branch: str = Field(..., min_length=1, description="Branch name")
    language: str = Field(..., description="Programming language (empty for non-code files)")
    chunk_index: int = Field(..., description="Chunk index within file")
    company_id: str = Field(..., min_length=1, description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")


# ── Neo4j Edge Models ────────────────────────────────────────────────


class Neo4jEdge(BaseModel):
    """Relationship edge to be written to Neo4j.

    Covers:
    - LLM-extracted relationships between entities
    - contains: DocumentChunk -> Entity
    - is_a: Entity -> EntityType
    """

    source_id: str = Field(..., min_length=1, description="Source node UUID")
    target_id: str = Field(..., min_length=1, description="Target node UUID")
    relationship_type: str = Field(..., min_length=1, description="Relationship label")
    properties: Dict = Field(default_factory=dict, description="Edge properties")


# ── Kafka Input Models (from summarization service) ──────────────────


class TextSummaryEvent(BaseModel):
    """Kafka message consumed from text-summaries topic.

    One event per chunk — contains the summary produced for that chunk.
    Mirrors summarization_service.models.TextSummaryEvent schema.
    """

    event_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique event ID",
    )
    ingestion_id: str = Field(..., min_length=1, description="Ingestion/repo version ID")
    chunk_id: str = Field(..., min_length=1, description="Source chunk UUID")
    company_id: str = Field(..., min_length=1, description="Company UUID (multi-tenancy)")
    project_id: Optional[str] = Field(
        default=None, description="Project UUID (None for company-level documents)"
    )
    content_type: str = Field(default="code", description="Content type: code or document")
    chunk_index: int = Field(default=0, description="0-based chunk index within the source file")
    file_version_id: str = Field(
        ...,
        min_length=1,
        description="File version ID from code_processing.repository_file_versions",
    )
    file_path: str = Field(..., min_length=1, description="File path relative to repository root")
    repository: str = Field(..., min_length=1, description="Repository name")
    branch: str = Field(..., min_length=1, description="Git branch name")
    language: str = Field(..., description="Programming language (empty for non-code files)")
    summary_text: str = Field(..., min_length=1, description="Generated summary text")
    summary_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique summary record ID",
    )
    summarization_duration_s: float = Field(..., description="Time spent on LLM summarization")
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


# ── Delete Events (from enriched-code-chunks topic) ─────────────────


class DeleteEvent(BaseModel):
    """Delete message consumed from enriched-code-chunks topic.

    Triggers cleanup of all nodes/edges for the deleted file in Neo4j.
    The delete message does NOT include file_version_id — it must be
    resolved from Neo4j by file_path + project_id lookup.
    """

    action: str = Field("delete", description="Action type (always 'delete')")
    company_id: str = Field(..., min_length=1, description="Company UUID (multi-tenancy)")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    repository: str = Field(..., min_length=1, description="Repository name")
    branch: str = Field(..., min_length=1, description="Git branch name")
    file_path: str = Field(..., min_length=1, description="File path relative to repository root")
    ingestion_id: str = Field(..., min_length=1, description="Ingestion/repo version ID")
    file_version_id: Optional[str] = Field(
        None, description="File version ID (optional — resolved from Neo4j if absent)"
    )

    @model_validator(mode="after")
    def _validate_project_scope(self) -> "DeleteEvent":
        if self.repository == "kgrag-documents" and self.project_id is not None:
            raise ValueError("project_id must be absent for document deletes")
        if self.repository != "kgrag-documents" and not self.project_id:
            raise ValueError("project_id is required for code deletes")
        return self


# ── Pipeline Events ──────────────────────────────────────────────────


class PipelineEvent(BaseModel):
    """Pipeline event consumed from or published to pipeline-events topic.

    Used for:
    - Consuming: extraction_complete, summarization_complete, embedding_complete
    - Publishing: neo4j_complete
    """

    event_type: str = Field(..., min_length=1, description="Event type identifier")
    ingestion_id: str = Field(..., min_length=1, description="Ingestion/repo version ID")
    company_id: str = Field(..., min_length=1, description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    # Consumed fields (from Phase A services)
    chunks_processed: int = Field(..., description="Chunks processed")
    total_entities: int = Field(..., description="Total entities")
    total_edges: int = Field(..., description="Total edges")
    # Published fields (for neo4j_complete)
    nodes_written: int = Field(..., description="Total Neo4j nodes created")
    edges_written: int = Field(..., description="Total Neo4j edges created")
    entity_nodes: int = Field(..., description="Entity nodes created")
    entity_type_nodes: int = Field(..., description="EntityType nodes created")
    chunk_nodes: int = Field(..., description="DocumentChunk nodes created")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
    )


# ── Phase 4.1: Hierarchy Node Models ────────────────────────────────


class Neo4jCompanyNode(BaseModel):
    """Company hierarchy node.

    Label: Company
    Unique key: company_id
    Created once per company — top of Company → Project → Branch hierarchy.
    """

    company_id: str = Field(..., min_length=1, description="Company UUID")
    name: str = Field(default="", description="Company name (optional, resolved from DB)")


class Neo4jProjectNode(BaseModel):
    """Project hierarchy node.

    Label: Project
    Unique key: project_id
    Connected to Company via has_project edge.
    """

    project_id: str = Field(..., min_length=1, description="Project UUID")
    company_id: str = Field(..., min_length=1, description="Parent company UUID")
    name: str = Field(default="", description="Project name")
    language: str = Field(default="", description="Primary programming language")
    framework: str = Field(default="", description="Primary framework")


class Neo4jBranchNode(BaseModel):
    """Branch hierarchy node.

    Label: Branch
    Unique key: branch_id (UUID5 of project_id + branch_name)
    Connected to Project via has_branch edge.
    """

    branch_id: str = Field(..., min_length=1, description="UUID5(project_id:branch_name)")
    project_id: str = Field(..., min_length=1, description="Parent project UUID")
    name: str = Field(..., min_length=1, description="Branch name (e.g. 'develop', 'main')")


class Neo4jBusinessDomainNode(BaseModel):
    """Business domain classification node — company-level.

    Label: BusinessDomain
    Unique key: domain_id (UUID5 of company_id + normalised_key)
    Connected to Company via has_business_domain edge.
    Entity → belongs_to_domain → BusinessDomain.

    Examples: Appointments, Billing, Patients, Inventory, Staff
    """

    domain_id: str = Field(..., min_length=1, description="UUID5(company_id:normalised_key)")
    company_id: str = Field(..., min_length=1, description="Company UUID")
    canonical_name: str = Field(..., min_length=1, description="Human-readable domain name")
    normalised_key: str = Field(
        ..., min_length=1, description="Lowercase slug (e.g. 'appointments')"
    )
    description: str = Field(default="", description="Domain description")


class Neo4jTechnicalDomainNode(BaseModel):
    """Technical domain classification node — project-level.

    Label: TechnicalDomain
    Unique key: domain_id (UUID5 of project_id + name)
    Connected to Project via has_technical_domain edge.
    Entity → belongs_to_technical_domain → TechnicalDomain.

    Examples: API Hooks, UI Components, State Management, Controllers
    """

    domain_id: str = Field(..., min_length=1, description="UUID5(project_id:name)")
    project_id: str = Field(..., min_length=1, description="Project UUID")
    name: str = Field(..., min_length=1, description="Technical domain name")
    description: str = Field(default="", description="Domain description")


class Neo4jCodeBlockNode(BaseModel):
    """Code block node — holds code text for an entity (Phase 4.1).

    Label: CodeBlock
    Unique key: code_block_id (UUID5 of entity_id + file_version_id)

    Replaces embedding code text on Entity nodes — Entity nodes are now
    lightweight. Code lives here, linked via Entity -[:has_code]-> CodeBlock.

    Also linked to Branch: Branch -[:has_code_block]-> CodeBlock (branch scoping).
    """

    code_block_id: str = Field(..., min_length=1, description="UUID5(entity_id:file_version_id)")
    entity_id: str = Field(..., min_length=1, description="Linked entity UUID")
    text: str = Field(..., min_length=1, description="Source code text")
    start_line: int = Field(default=0, description="1-indexed start line (0 = unknown)")
    end_line: int = Field(default=0, description="1-indexed end line (0 = unknown)")
    file_path: str = Field(..., min_length=1, description="Source file path")
    language: str = Field(default="", description="Programming language")
    file_version_id: str = Field(..., min_length=1, description="File version ID for scoping")
    project_id: str = Field(..., min_length=1, description="Project UUID")
    branch: str = Field(default="", description="Branch name")

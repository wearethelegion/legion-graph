"""
Unified Kafka event schemas for the Code Intelligence RAG system.
All services should use these models for producing and consuming Kafka messages.
"""

from datetime import datetime, timezone
from enum import Enum
import os
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field
import json
import hashlib
from pathlib import Path

from shared.data_models import CodeChunk

# ── Extension → Language mapping for code files ──────────────────────────────
_EXTENSION_TO_LANGUAGE = {
    ".ts": "typescript",
    ".tsx": "typescript",
    ".mts": "typescript",
    ".cts": "typescript",
    ".js": "javascript",
    ".jsx": "javascript",
    ".mjs": "javascript",
    ".cjs": "javascript",
    ".py": "python",
    ".pyw": "python",
    ".pyi": "python",
    ".rb": "ruby",
    ".rake": "ruby",
    ".java": "java",
    ".kt": "kotlin",
    ".kts": "kotlin",
    ".swift": "swift",
    ".go": "go",
    ".rs": "rust",
    ".cs": "csharp",
    ".cpp": "cpp",
    ".cc": "cpp",
    ".cxx": "cpp",
    ".c": "c",
    ".h": "c",
    ".hpp": "cpp",
    ".php": "php",
    ".scala": "scala",
    ".dart": "dart",
    ".lua": "lua",
    ".r": "r",
    ".m": "objectivec",
    ".mm": "objectivec",
    ".sh": "bash",
    ".bash": "bash",
    ".zsh": "bash",
    ".ps1": "powershell",
    ".sql": "sql",
    ".html": "html",
    ".htm": "html",
    ".css": "css",
    ".scss": "scss",
    ".sass": "sass",
    ".less": "less",
    ".vue": "vue",
    ".svelte": "svelte",
    ".yml": "yaml",
    ".yaml": "yaml",
    ".json": "json",
    ".xml": "xml",
    ".toml": "toml",
    ".ini": "ini",
    ".cfg": "ini",
    ".tf": "terraform",
    ".hcl": "terraform",
    ".proto": "protobuf",
    ".graphql": "graphql",
    ".gql": "graphql",
    ".ex": "elixir",
    ".exs": "elixir",
    ".erl": "erlang",
    ".hs": "haskell",
    ".clj": "clojure",
    ".cljs": "clojure",
    ".pl": "perl",
    ".groovy": "groovy",
}


class ChangeType(str, Enum):
    """Types of file changes in version control"""

    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    RENAMED = "renamed"
    COPIED = "copied"
    UNCHANGED = "unchanged"


class EnrichmentStage(str, Enum):
    """Processing stages for document enrichment"""

    INGESTION_REQUESTED = "INGESTION_REQUESTED"
    PREPROCESSING = "PREPROCESSING"
    AI_ANALYSIS_PENDING = "AI_ANALYSIS_PENDING"
    AI_ANALYSIS_COMPLETE = "AI_ANALYSIS_COMPLETE"
    VECTOR_EMBEDDING = "VECTOR_EMBEDDING"
    GRAPH_INDEXING = "GRAPH_INDEXING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ContextualKeyword(BaseModel):
    """A contextual keyword with probability score"""

    keyword: str = Field(description="The keyword text")
    probability: float = Field(description="Confidence probability (0.0-1.0)")


class FilePathKeyword(BaseModel):
    """A contextual keyword with probability score"""

    keyword: str = Field(description="The keyword text")
    order_number: int = Field(description="Order number for sorting")


class ValidationResult(BaseModel):
    """Quality validation result for code extraction"""

    validation_passed: bool = Field(description="Whether validation passed")
    confidence_score: float = Field(description="Overall confidence (0.0-1.0)")
    completeness_score: float = Field(description="Completeness score (0.0-1.0)")
    accuracy_score: float = Field(description="Accuracy score (0.0-1.0)")
    consistency_score: float = Field(description="Consistency score (0.0-1.0)")
    total_methods_found: int = Field(default=0, description="Total methods extracted")
    total_symbols_found: int = Field(default=0, description="Total symbols extracted")
    missing_elements: List[str] = Field(
        default_factory=list, description="Detected missing elements"
    )
    validation_notes: str = Field(default="", description="Quality concerns or confirmations")
    needs_review: bool = Field(default=False, description="Requires human review")


class ParameterDetail(BaseModel):
    """Detailed parameter information"""

    name: str = Field(description="Parameter name")
    type: str = Field(description="Parameter type")
    default_value: Optional[str] = Field(None, description="Default value if any")
    position: int = Field(description="Position in parameter list")


class VariableScope(BaseModel):
    """Variable scope and usage information"""

    name: str = Field(description="Variable name")
    line: int = Field(description="Line where variable is declared")
    type: str = Field(description="Variable type")
    scope: str = Field(description="Scope: method_name, 'class', or 'file'")
    role: str = Field(description="Role: local, instance, class, const, env, config")
    used_by_methods: List[str] = Field(
        default_factory=list, description="Methods that use this variable"
    )


class DataFlow(BaseModel):
    """Data flow tracking through methods"""

    flow_id: str = Field(description="Descriptive flow identifier")
    entry_point: str = Field(description="Where data enters: param, io, api, db")
    transformations: List[str] = Field(default_factory=list, description="Transformation steps")
    exit_point: str = Field(description="Where data exits: return, io, api, db")
    methods_involved: List[str] = Field(
        default_factory=list, description="Methods in the flow chain"
    )


class ControlBlock(BaseModel):
    """Control flow block information"""

    block_type: str = Field(description="Type: conditional, loop, try_catch, switch")
    line: int = Field(description="Starting line")
    end_line: int = Field(description="Ending line")
    condition: str = Field(description="Conditional expression")
    purpose: str = Field(description="Why this control flow exists")
    containing_method: str = Field(description="Method containing this block")


class BaseKafkaMessage(BaseModel):
    """Base class for all Kafka messages with common metadata"""

    event_id: str = Field(description="Unique event identifier")
    event_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="Event creation timestamp"
    )
    schema_version: str = Field(default="1.0.0", description="Schema version for compatibility")

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes for Kafka"""
        return json.dumps(self.model_dump(), default=str).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, data: bytes):
        """Deserialize from JSON bytes"""
        return cls(**json.loads(data))


# ── Topic constants ──────────────────────────────────────────────────────────
BRAIN_EVENTS_TOPIC = "brain_events"


class BrainEvent(BaseKafkaMessage):
    """
    Schema for brain_events topic.
    Emitted when a Brain v2 entity (knowledge, expertise, lesson, etc.)
    is created, updated, or deleted.  The Kafka consumer uses these
    events to trigger async enrichment (Cognee pipeline).
    """

    entity_type: str = Field(
        description="Entity kind: knowledge | expertise | expertise_chunk | lesson | engagement | entry"
    )
    entity_id: str = Field(description="UUID of the Postgres row")
    company_id: str = Field(description="Company UUID (required)")
    project_id: Optional[str] = Field(
        default=None, description="Project UUID (optional for company-level entities)"
    )
    text_content: str = Field(description="Full text for enrichment pipeline")
    title: str = Field(description="Human-readable title")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Additional context for enrichment"
    )
    action: str = Field(description="Mutation type: create | update | delete")
    cognee_data_id: Optional[str] = Field(
        default=None,
        description="Cognee data UUID — populated on delete events so the consumer "
        "can remove the correct Cognee data item without label-matching",
    )


class RepositoryIngestionRequest(BaseKafkaMessage):
    """
    Schema for incoming_requests topic.
    Triggers repository analysis pipeline.
    """

    framework: str = Field(description="Framework to process")
    repository: str = Field(description="Repository URL or identifier")
    branch: str = Field(default="main", description="Branch to analyze")
    commit_sha: Optional[str] = Field(None, description="Specific commit to analyze")
    force_full_refresh: bool = Field(False, description="Force complete re-analysis")
    requested_at: Optional[str] = Field(
        None, description="Timestamp when request was made"
    )  # For compatibility
    requested_by: Optional[str] = Field(None, description="User or system that requested ingestion")
    workspace: str = Field(default="default", description="Workspace identifier for isolation")
    priority: int = Field(default=5, description="Processing priority (1=highest, 10=lowest)")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Additional request metadata"
    )

    # Multi-tenant fields (REQUIRED - no defaults)
    project_id: str = Field(description="Project UUID (required)")
    company_id: str = Field(description="Company UUID (required)")
    user_id: str = Field(default="", description="User UUID who initiated the request")


class DataEnrichmentEvent(BaseKafkaMessage):
    """
    Schema for data_enrichment topic.
    Contains file content and metadata for enrichment pipeline.
    """

    # Repository context
    repository: str = Field(description="Repository identifier")
    branch: str = Field(description="Branch name")
    framework: str = Field(description="Framework identifier")
    commit_sha: str = Field(description="Git commit SHA")
    workspace: str = Field(description="Workspace identifier")

    # Multi-tenant fields (REQUIRED - no defaults)
    project_id: str = Field(description="Project UUID (required)")
    company_id: str = Field(description="Company UUID (required)")
    user_id: str = Field(default="", description="User UUID who initiated the request")

    # Document identifiers
    document_id: str = Field(description="Unique document identifier (hash-based)")
    canonical_path: str = Field(description="Human-readable hierarchical path for debugging")

    # File information
    file_path: str = Field(description="Relative file path from repository root")
    file_extension: Optional[str] = Field(None, description="File extension for language detection")
    language: Optional[str] = Field(None, description="Detected programming language")

    # Change metadata
    change_type: ChangeType = Field(description="Type of file change")
    previous_commit_sha: Optional[str] = Field(
        None, description="Previous commit for diff analysis"
    )
    previous_path: Optional[str] = Field(None, description="For RENAMED: the old file path")

    # Content
    content_encoding: str = Field(default="base64", description="Content encoding method")
    content_b64: Optional[str] = Field(None, description="Base64 encoded file content")
    content_hash: str = Field(description="SHA256 hash of content for deduplication")
    content_size_bytes: int = Field(description="Original content size before encoding")

    # # SCIP data (Source Code Intelligence Protocol)
    # scip_data: Optional[Dict[str, Any]] = Field(None, description="SCIP symbols, occurrences, and relationships")
    # scip_symbols: Optional[List[Dict[str, Any]]] = Field(default=None, description="SCIP symbol definitions")
    # scip_occurrences: Optional[List[Dict[str, Any]]] = Field(default=None, description="SCIP symbol occurrences/references")
    # scip_relationships: Optional[List[Dict[str, Any]]] = Field(default=None, description="SCIP symbol relationships")
    # scip_dependencies: Optional[List[Dict[str, Any]]] = Field(default=None, description="SCIP classified dependencies")
    # scip_indexed_at: Optional[datetime] = Field(default=None, description="SCIP indexing timestamp")
    # scip_stats: Optional[Dict[str, Any]] = Field(default=None, description="SCIP indexing statistics")

    # NEW: Parser data (Code Structure Intelligence - AST level)
    parser_nodes: Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Parsed code nodes (classes, methods, constants, etc.)"
    )
    parser_relationships: Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Code relationships (CALLS, DEFINES, INHERITS_FROM, etc.)"
    )
    parser_metadata: Optional[Dict[str, Any]] = Field(
        default=None, description="Parser execution metadata (language, LOC, parsed_at)"
    )

    # Ingestion tracking (for end-to-end traceability)
    ingestion_id: str = Field(
        default="", description="UUID linking all files in this ingestion batch"
    )
    file_index: int = Field(default=0, description="1-based position within ingestion batch")
    total_files: int = Field(default=0, description="Total files in this ingestion")

    # Processing metadata
    stage: EnrichmentStage = Field(default=EnrichmentStage.PREPROCESSING)
    force_full_refresh: bool = Field(default=False)

    # Versioning
    document_version: int = Field(default=1, description="Document version number")
    is_latest: bool = Field(default=True, description="Whether this is the latest version")

    @classmethod
    def create_from_file(
        cls,
        repository: str,
        branch: str,
        framework: str,
        commit_sha: str,
        file_path: str,
        content: bytes,
        change_type: ChangeType,
        workspace: str = "default",
        **kwargs,
    ) -> "DataEnrichmentEvent":
        """Factory method to create event from file content"""
        import base64
        import uuid

        content_b64 = base64.b64encode(content).decode("utf-8")
        content_hash = hashlib.sha256(content).hexdigest()

        file_extension = Path(file_path).suffix
        language = _EXTENSION_TO_LANGUAGE.get(file_extension.lower())

        return cls(
            event_id=str(uuid.uuid4()),
            repository=repository,
            branch=branch,
            framework=framework,
            commit_sha=commit_sha,
            workspace=workspace,
            file_path=file_path,
            file_extension=file_extension,
            language=language,
            change_type=change_type,
            content_b64=content_b64,
            content_hash=content_hash,
            content_size_bytes=len(content),
            **kwargs,
        )


class AIEnrichedDocument(BaseKafkaMessage):
    """
    Schema for ai_enriched_documents topic.
    Contains AI analysis results with SCIP-based semantic chunks (V2).
    """

    # Source reference
    repository: str = Field(description="Repository identifier")
    branch: str = Field(description="Branch name")
    commit_sha: str = Field(description="Git commit SHA")
    workspace: str = Field(description="Workspace identifier")
    file_path: str = Field(description="Analyzed file path")

    # Document identifiers
    document_id: str = Field(description="Unique document identifier")
    content_hash: str = Field(description="SHA256 hash of original content")
    content: str = Field(description="Original content")

    # AI Analysis results (V2 Schema)
    change_type: ChangeType = Field(description="Type of file change")
    framework: str = Field(default="", description="Framework used (rails|react|django|unknown)")
    analysis_model: str = Field(
        default=os.environ.get("GEMINI_MODEL", "gemini-2.5-flash"),
        description="LLM model used for analysis",
    )

    # V2 Core Fields
    file_metadata: Dict[str, Any] = Field(
        description="File-level metadata: language, framework, layer, primary_entity, framework_patterns, file_purpose"
    )
    chunks: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Semantic chunks with business descriptions, Neo4j nodes/relationships, SCIP symbols",
    )
    file_analysis: Dict[str, Any] = Field(
        default_factory=dict,
        description="File-level analysis: primary_purpose, key_dependencies, external_packages, architectural_role",
    )
    quality_validation: Dict[str, Any] = Field(
        default_factory=dict,
        description="Quality validation: confidence_score, validation_passed, warnings",
    )

    # Success indicators
    parse_success: bool = Field(default=False, description="Whether LLM parsing was successful")

    # Processing metadata
    enrichment_stage: EnrichmentStage = Field(default=EnrichmentStage.AI_ANALYSIS_COMPLETE)
    enriched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    processing_time_ms: Optional[int] = Field(
        None, description="Processing duration in milliseconds"
    )


class DocumentStoreReference(BaseModel):
    """Reference to a document in the persistent document store"""

    document_id: str = Field(description="Unique document identifier")
    content_hash: str = Field(description="Content hash for verification")
    storage_path: str = Field(description="Storage location or key")
    created_at: datetime = Field(description="Document creation timestamp")
    size_bytes: int = Field(description="Document size")
    compressed: bool = Field(default=False, description="Whether content is compressed")
    compression_type: Optional[str] = Field(None, description="Compression algorithm used")


class KafkaReplayRequest(BaseModel):
    """Request schema for Kafka topic replay operations"""

    topic: str = Field(description="Topic to replay")
    start_offset: Optional[int] = Field(None, description="Starting offset")
    end_offset: Optional[int] = Field(None, description="Ending offset")
    start_timestamp: Optional[datetime] = Field(None, description="Start from timestamp")
    end_timestamp: Optional[datetime] = Field(None, description="End at timestamp")
    target_service: str = Field(description="Service to replay into")
    batch_size: int = Field(default=100, description="Messages per batch")
    dry_run: bool = Field(default=False, description="Simulate without processing")


class DocumentDeleteEvent(BaseKafkaMessage):
    """
    Schema for document_deletes topic.
    Triggers deletion across all storage layers.
    """

    # Document identifier
    document_id: str = Field(description="Unique document identifier to delete")

    # Context (for logging/auditing)
    workspace: str = Field(description="Workspace identifier")
    repository: str = Field(description="Repository identifier")
    branch: str = Field(description="Branch name")
    file_path: str = Field(description="File path being deleted")

    # Deletion metadata
    deleted_by: Optional[str] = Field(None, description="User or system that triggered deletion")
    deletion_reason: Optional[str] = Field(None, description="Reason for deletion")


class DocumentUpdateEvent(BaseKafkaMessage):
    """
    Schema for document_updates topic.
    Triggers delete-and-reinsert workflow.
    """

    # Document identifier (same ID will be reused after reprocessing)
    document_id: str = Field(description="Document identifier to update")

    # Repository context
    workspace: str = Field(description="Workspace identifier")
    repository: str = Field(description="Repository identifier")
    branch: str = Field(description="Branch name")
    framework: str = Field(description="Framework identifier")
    commit_sha: str = Field(description="New commit SHA")

    # File information
    file_path: str = Field(description="File path being updated")
    file_extension: Optional[str] = Field(None, description="File extension")
    language: Optional[str] = Field(None, description="Programming language")

    # Updated content
    content_encoding: str = Field(default="base64", description="Content encoding")
    content_b64: str = Field(description="Base64 encoded updated content")
    content_hash: str = Field(description="SHA256 hash of new content")
    content_size_bytes: int = Field(description="New content size")

    # Update metadata
    previous_commit_sha: Optional[str] = Field(None, description="Previous commit SHA")
    updated_by: Optional[str] = Field(None, description="User or system that triggered update")
    update_reason: Optional[str] = Field(None, description="Reason for update")

    @classmethod
    def create_from_file(
        cls,
        document_id: str,
        repository: str,
        branch: str,
        framework: str,
        commit_sha: str,
        file_path: str,
        content: bytes,
        workspace: str = "default",
        **kwargs,
    ) -> "DocumentUpdateEvent":
        """Factory method to create update event from file content"""
        import base64
        import uuid

        content_b64 = base64.b64encode(content).decode("utf-8")
        content_hash = hashlib.sha256(content).hexdigest()
        file_extension = Path(file_path).suffix
        language = _EXTENSION_TO_LANGUAGE.get(file_extension.lower())

        return cls(
            event_id=str(uuid.uuid4()),
            document_id=document_id,
            repository=repository,
            branch=branch,
            framework=framework,
            commit_sha=commit_sha,
            workspace=workspace,
            file_path=file_path,
            file_extension=file_extension,
            language=language,
            content_b64=content_b64,
            content_hash=content_hash,
            content_size_bytes=len(content),
            **kwargs,
        )

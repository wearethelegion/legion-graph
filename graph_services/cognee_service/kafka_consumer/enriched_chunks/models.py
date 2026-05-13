"""Kafka message schema and DocumentChunk builder for enriched code chunks."""

from uuid import UUID
from typing import List, Optional

from pydantic import BaseModel, Field, model_validator

from cognee.modules.chunking.models.DocumentChunk import DocumentChunk
from cognee.modules.data.processing.document_types import Document


class EnrichedChunkMessage(BaseModel):
    """Schema for enriched chunk messages from Kafka.

    Produced by the code preprocessor (TreeSitter extraction + embedding) or the
    document preprocessor (markdown-aware chunking of Brain v2 entities).
    Supports both process and delete actions.
    """

    action: str = Field(default="process", description="Action type: 'process' or 'delete'")

    # Required for all actions
    company_id: str = Field(..., description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    repository: str = Field(..., description="Repository name")
    branch: str = Field(..., description="Git branch name")
    file_path: str = Field(..., description="File path relative to repository root")
    ingestion_id: str = Field(..., description="Ingestion/repository version ID")
    file_version_id: str = Field(
        ..., description="File version ID from code_processing.repository_file_versions"
    )

    # Content routing — differentiates code chunks from document chunks
    content_type: str = Field(
        default="code",
        description="Content type: 'code' for source code, 'document' for Brain v2 entities",
    )
    entity_type: Optional[str] = Field(
        default=None,
        description="Document sub-type: knowledge|expertise|lesson|entry|engagement (documents only)",
    )
    document_title: Optional[str] = Field(
        default=None,
        description="Human-readable title of the source document (documents only)",
    )
    document_slug: Optional[str] = Field(
        default=None,
        description="Machine-friendly slug for the source document (documents only)",
    )

    # Optional for delete action, required for process action
    chunk_id: Optional[str] = Field(None, description="Unique chunk ID (UUID)")
    parent_id: Optional[str] = Field(
        None, description="File version ID this chunk belongs to (legacy alias for file_version_id)"
    )
    language: Optional[str] = Field(
        None,
        description="Programming language (e.g., python, ruby). None for unsupported file types.",
    )
    chunk_index: Optional[int] = Field(None, description="0-based chunk index within file")
    total_chunks: Optional[int] = Field(None, description="Total number of chunks for this file")
    content: Optional[str] = Field(
        None,
        description="Raw chunk text (code only, no header prepended)",
    )
    header: str = Field(
        default="",
        description="Context header (PROJECT/FILE/SIGNATURE sections) stored separately from content",
    )
    embedding: Optional[List[float]] = Field(
        None, description="Pre-computed 768-dim embedding vector"
    )
    file_skeleton: str = Field(
        default="",
        description="Formatted file skeleton (class/function declarations) from TreeSitter parse. Empty for documents.",
    )
    start_line: int = Field(
        default=0,
        description="1-indexed line number where this chunk starts in the source file. 0 means unknown.",
    )
    end_line: int = Field(
        default=0,
        description="1-indexed line number where this chunk ends in the source file. 0 means unknown.",
    )
    business_domains: Optional[List[dict]] = Field(
        default=None, description="List of {name, key_concepts} dicts from project analysis"
    )
    technical_tags: Optional[List[str]] = Field(
        default=None, description="Design pattern names from project analysis"
    )
    extraction_prompt: Optional[str] = Field(
        default=None,
        description=(
            "Project-specific extraction prompt from code_processing.project_profiles. "
            "REQUIRED for V3 pipeline — null means legacy message or no profile yet."
        ),
    )

    @model_validator(mode="after")
    def _validate_project_scope(self) -> "EnrichedChunkMessage":
        if self.content_type == "code" and not self.project_id:
            raise ValueError("project_id is required for code chunks")
        if self.content_type == "document" and self.project_id is not None:
            raise ValueError("project_id must be absent for document chunks")
        return self


def build_document_chunk(msg: EnrichedChunkMessage) -> DocumentChunk:
    """Build a Cognee DocumentChunk from an EnrichedChunkMessage.

    The DocumentChunk is the input format expected by Cognee's extract_graph_from_data.
    We create a minimal Document stub as the parent container.

    Requires action="process" — not used for delete messages.
    """
    import json

    if msg.action == "delete":
        raise ValueError("build_document_chunk cannot be used for delete action messages")

    if not all(
        [msg.chunk_id, msg.parent_id, msg.content, msg.chunk_index is not None, msg.total_chunks]
    ):
        raise ValueError(
            "process action requires chunk_id, parent_id, content, chunk_index, total_chunks"
        )

    # Minimal Document stub — Cognee uses this for provenance tracking
    # Document fields: name, raw_data_location, external_metadata, mime_type, metadata
    is_document = msg.content_type == "document"
    doc = Document(
        id=UUID(msg.parent_id),  # Use file version ID as document ID
        name=msg.file_path,
        raw_data_location=f"{msg.repository}/{msg.file_path}",
        mime_type="text/markdown" if is_document else "text/x-code",
        external_metadata=json.dumps(
            {
                "repository": msg.repository,
                "branch": msg.branch,
                "language": msg.language or "unknown",
                "ingestion_id": msg.ingestion_id,
                "company_id": msg.company_id,
                **({"project_id": msg.project_id} if not is_document else {}),
            }
        ),
        metadata={"index_fields": ["name"]},
    )

    # Build DocumentChunk
    # chunk.text = raw content ONLY — this is what add_data_points stores in Qdrant.
    # Header is passed separately to extract_content_graph for LLM context.
    chunk = DocumentChunk(
        id=UUID(msg.chunk_id),
        text=msg.content,
        chunk_size=len(msg.content),
        chunk_index=msg.chunk_index,
        cut_type="code",  # Indicates code-specific chunking strategy
        is_part_of=doc,
        metadata={
            "index_fields": ["text"],
            "total_chunks": msg.total_chunks,
            "language": msg.language or "unknown",
            "file_path": msg.file_path,
            "repository": msg.repository,
            "branch": msg.branch,
        },
        # Cognee provenance fields (will be stamped by pipeline)
        source_pipeline="enriched_chunks_consumer",
        source_task="process_enriched_chunk",
        source_node_set=(
            f"{msg.company_id}_knowledge" if is_document else f"code_{msg.project_id}"
        ),
        belongs_to_set=[
            (f"{msg.company_id}_knowledge" if is_document else f"code_{msg.project_id}")
        ],
    )

    return chunk

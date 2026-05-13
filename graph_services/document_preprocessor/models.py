"""Internal Pydantic models for the document preprocessor pipeline."""

from typing import Optional

from pydantic import BaseModel, Field


class DocumentChunkResult(BaseModel):
    """A single chunk produced by the document chunker.

    Carries the chunk text, its position within the document, and the
    section heading (if the chunk originated from a markdown heading split).
    """

    text: str = Field(description="Raw chunk text content")
    chunk_index: int = Field(description="0-based position within the document")
    total_chunks: int = Field(description="Total number of chunks for this document")
    section_heading: Optional[str] = Field(
        default=None,
        description="Heading text if chunk starts at a markdown heading boundary",
    )


class DocumentVersion(BaseModel):
    """Tracks a processed document version for dedup and re-processing."""

    entity_id: str = Field(description="UUID of the Brain v2 entity")
    entity_type: str = Field(description="knowledge|expertise|lesson|entry|engagement")
    company_id: str = Field(description="Company UUID")
    project_id: Optional[str] = Field(default=None, description="Project UUID")
    content_hash: str = Field(description="SHA-256 hex digest of text_content")
    title: str = Field(description="Document title")
    chunk_count: int = Field(default=0, description="Number of chunks produced")

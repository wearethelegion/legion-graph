"""
Brain v2 — Knowledge Pydantic models.

Column names match the ``knowledge`` and ``knowledge_chunks`` tables
created in migration 060.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# =============================================================================
# Request Models
# =============================================================================


class KnowledgeCreateRequest(BaseModel):
    """POST /v2/knowledge — create a knowledge document."""

    title: str = Field(..., min_length=1, max_length=500, description="Knowledge title")
    text_content: str = Field(
        ..., min_length=1, max_length=500_000, description="Full markdown content"
    )
    company_id: str = Field(..., description="Company UUID (required)")
    project_id: str = Field(..., description="Project UUID (required)")
    when_to_use: Optional[str] = Field(
        None, max_length=2000, description="When this knowledge should be consulted"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Arbitrary tags / categorization"
    )

    @field_validator("metadata")
    @classmethod
    def validate_metadata_size(cls, v):
        if v is not None:
            import json

            serialized = json.dumps(v)
            if len(serialized) > 65536:  # 64KB limit
                raise ValueError("metadata exceeds 64KB limit")
        return v


class KnowledgeUpdateRequest(BaseModel):
    """PUT /v2/knowledge/{id} — partial update of a knowledge document."""

    title: Optional[str] = Field(None, min_length=1, max_length=500, description="New title")
    text_content: Optional[str] = Field(
        None, min_length=1, max_length=500_000, description="New content"
    )
    when_to_use: Optional[str] = Field(None, max_length=2000, description="Updated usage guidance")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Replacement metadata")

    @field_validator("metadata")
    @classmethod
    def validate_metadata_size(cls, v):
        if v is not None:
            import json

            serialized = json.dumps(v)
            if len(serialized) > 65536:  # 64KB limit
                raise ValueError("metadata exceeds 64KB limit")
        return v


# =============================================================================
# Response Models
# =============================================================================


class KnowledgeChunkResponse(BaseModel):
    """Single chunk belonging to a knowledge document."""

    id: str = Field(..., description="Chunk UUID")
    knowledge_id: str = Field(..., description="Parent knowledge UUID")
    content: str = Field(..., description="Chunk text")
    summary: Optional[str] = None

    position: int = Field(0, description="Ordering position")
    level: int = Field(0, description="Heading depth (0 = top)")
    parent_chunk_id: Optional[str] = None

    chunk_type: Optional[str] = Field(None, description="prose | code | heading | mixed")
    section_title: Optional[str] = None
    has_code: bool = False
    keywords: List[str] = Field(default_factory=list)

    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class KnowledgeResponse(BaseModel):
    """Full knowledge document returned by GET / POST."""

    id: str = Field(..., description="Knowledge UUID")
    company_id: str
    project_id: str

    title: str
    text_content: str
    when_to_use: Optional[str] = None
    content_hash: Optional[str] = None

    metadata: Dict[str, Any] = Field(default_factory=dict)

    created_by_user_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    is_duplicate: bool = Field(
        default=False,
        description="True when an existing document was returned via deduplication",
    )

    chunks: List[KnowledgeChunkResponse] = Field(
        default_factory=list, description="Ordered list of chunks"
    )

    model_config = ConfigDict(from_attributes=True)

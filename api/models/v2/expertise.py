"""
Brain v2 — Expertise Pydantic models.

Column names match the ``expertise`` and ``expertise_chunks`` tables
created in migration 060.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# =============================================================================
# Request Models
# =============================================================================


class ExpertiseCreateRequest(BaseModel):
    """POST /v2/expertise — create a structured expertise document."""

    title: str = Field(..., min_length=1, max_length=500, description="Expertise title")
    content: str = Field(..., min_length=1, max_length=500_000, description="Full markdown content")
    company_id: str = Field(..., description="Company UUID (required)")
    project_id: Optional[str] = Field(
        None, description="Project UUID (optional — omit for company-level)"
    )
    when_to_use: Optional[str] = Field(
        None, max_length=2000, description="When this expertise should be consulted"
    )
    is_company_level: bool = Field(default=False, description="True if accessible to all projects")
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


class ExpertiseUpdateRequest(BaseModel):
    """PUT /v2/expertise/{id} — partial update of an expertise document."""

    title: Optional[str] = Field(None, min_length=1, max_length=500, description="New title")
    content: Optional[str] = Field(
        None, min_length=1, max_length=500_000, description="New content"
    )
    when_to_use: Optional[str] = Field(None, max_length=2000, description="Updated usage guidance")
    is_company_level: Optional[bool] = None
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


class ExpertiseChunkCreateRequest(BaseModel):
    """POST /v2/expertise/{id}/chunks — add a section to an expertise doc."""

    content: str = Field(..., min_length=1, max_length=200_000, description="Section markdown")
    parent_chunk_id: Optional[str] = Field(None, description="Parent chunk UUID for nesting")


# =============================================================================
# Response Models
# =============================================================================


class ExpertiseChunkResponse(BaseModel):
    """Single chunk belonging to an expertise document."""

    id: str = Field(..., description="Chunk UUID")
    expertise_id: str = Field(..., description="Parent expertise UUID")
    content: str = Field(..., description="Chunk text")
    summary: Optional[str] = None

    position: int = Field(0, description="Ordering position")
    level: int = Field(0, description="Heading depth (0 = top)")
    parent_chunk_id: Optional[str] = None
    chunk_path: Optional[str] = None

    chunk_type: Optional[str] = Field(None, description="prose | code | heading | mixed")
    section_title: Optional[str] = None
    has_code: bool = False
    keywords: List[str] = Field(default_factory=list)

    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ExpertiseResponse(BaseModel):
    """Full expertise document returned by GET / POST."""

    id: str = Field(..., description="Expertise UUID")
    company_id: str
    project_id: Optional[str] = None

    title: str
    content: str
    summary: Optional[str] = None
    when_to_use: Optional[str] = None
    is_company_level: bool = False
    content_hash: Optional[str] = None

    metadata: Dict[str, Any] = Field(default_factory=dict)

    created_by_user_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    is_duplicate: bool = Field(
        default=False,
        description="True when an existing document was returned via deduplication",
    )

    chunks: List[ExpertiseChunkResponse] = Field(
        default_factory=list, description="Ordered list of chunks"
    )

    model_config = ConfigDict(from_attributes=True)

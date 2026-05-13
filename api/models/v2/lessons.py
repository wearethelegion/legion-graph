"""
Brain v2 — Lessons Learned Pydantic models.

Column names match the ``lessons_learned`` table created in migration 060.
"""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

VALID_SEVERITIES = Literal["low", "medium", "high", "critical"]


# =============================================================================
# Request Models
# =============================================================================


class LessonCreateRequest(BaseModel):
    """POST /v2/lessons — record a resolved issue."""

    title: str = Field(..., min_length=1, max_length=500, description="Short descriptive title")
    category: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Category path (e.g. Infrastructure/Docker)",
    )

    symptom: str = Field(
        ..., min_length=1, max_length=50_000, description="What error / behavior was observed"
    )
    root_cause: str = Field(..., min_length=1, max_length=50_000, description="Why it happened")
    solution: str = Field(..., min_length=1, max_length=50_000, description="Step-by-step fix")
    prevention: str = Field(
        ..., min_length=1, max_length=50_000, description="How to avoid in future"
    )

    severity: VALID_SEVERITIES = Field(
        default="medium",
        description="Severity: low | medium | high | critical",
    )

    tags: List[str] = Field(
        default_factory=list, max_length=50, description="Keywords for search (max 50 items)"
    )
    files_changed: List[str] = Field(
        default_factory=list, max_length=100, description="Files modified to fix (max 100 items)"
    )

    company_id: str = Field(..., description="Company UUID (required)")
    project_id: str = Field(..., description="Project UUID (required)")

    metadata: Dict[str, Any] = Field(default_factory=dict, description="Extra context")

    @field_validator("metadata")
    @classmethod
    def validate_metadata_size(cls, v):
        if v is not None:
            import json

            serialized = json.dumps(v)
            if len(serialized) > 65536:  # 64KB limit
                raise ValueError("metadata exceeds 64KB limit")
        return v


class LessonUpdateRequest(BaseModel):
    """PUT /v2/lessons/{id} — partial update of a lesson."""

    title: Optional[str] = Field(None, min_length=1, max_length=500)
    category: Optional[str] = Field(None, min_length=1, max_length=200)

    symptom: Optional[str] = Field(None, max_length=50_000)
    root_cause: Optional[str] = Field(None, max_length=50_000)
    solution: Optional[str] = Field(None, max_length=50_000)
    prevention: Optional[str] = Field(None, max_length=50_000)

    severity: Optional[VALID_SEVERITIES] = Field(None, description="low | medium | high | critical")

    tags: Optional[List[str]] = None
    files_changed: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None

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


class LessonResponse(BaseModel):
    """Full lesson document returned by GET / POST."""

    id: str = Field(..., description="Lesson UUID")
    company_id: str
    project_id: str

    title: str
    category: str

    symptom: str
    root_cause: str
    solution: str
    prevention: str

    severity: str = "medium"

    tags: List[str] = Field(default_factory=list)
    files_changed: List[str] = Field(default_factory=list)

    content: Optional[str] = None
    content_hash: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    created_by_user_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    is_duplicate: bool = Field(
        default=False,
        description="True when an existing lesson was returned via deduplication",
    )

    model_config = ConfigDict(from_attributes=True)

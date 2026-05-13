"""
Lessons Learned Pydantic Models
Request/response schemas for lessons learned operations.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any


# =============================================================================
# Lesson Create Models
# =============================================================================


class LessonCreate(BaseModel):
    """Request to record a lesson learned."""

    category: str = Field(
        ..., description="Category path (e.g., 'Infrastructure/Docker', 'API/Authentication')"
    )
    title: str = Field(..., description="Short descriptive title")
    symptom: str = Field(..., description="What error/behavior was observed")
    root_cause: str = Field(..., description="Why it happened (the actual cause)")
    solution: str = Field(..., description="Step-by-step fix that worked")
    prevention: str = Field(..., description="How to avoid this in the future")
    severity: str = Field("medium", description="Severity level: low, medium, high, critical")
    tags: Optional[List[str]] = Field(None, description="Keywords for search")
    files_changed: Optional[List[str]] = Field(None, description="Files modified to fix")


class LessonResponse(BaseModel):
    """Response for lesson creation."""

    status: str
    message: str
    expertise_id: str
    project_id: str
    company_id: str
    title: str
    category: str
    severity: str
    cognee_status: Optional[str] = Field(None, description="Cognee processing status")

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Lesson Query Models
# =============================================================================


class LessonQueryParams(BaseModel):
    """Query parameters for lesson search."""

    query: str = Field(..., description="Describe the problem")
    category_filter: Optional[str] = Field(None, description="Optional category filter")
    limit: int = Field(10, ge=1, le=100, description="Maximum results to return")


class LessonQueryResult(BaseModel):
    """Single lesson search result."""

    expertise_id: str
    title: str
    category: str
    severity: str
    symptom: str
    root_cause: str
    solution: str
    prevention: str
    tags: List[str] = []
    files_changed: List[str] = []
    score: float
    cognee_status: Optional[str] = Field(None, description="Cognee processing status")


class LessonQueryResponse(BaseModel):
    """Response for lesson query."""

    status: str
    query: str
    project_id: str
    category_filter: Optional[str]
    results_count: int
    results: List[LessonQueryResult]

    model_config = ConfigDict(from_attributes=True)

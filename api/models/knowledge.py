"""
Knowledge Pydantic Models
Request/response schemas for knowledge operations.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any


# =============================================================================
# Knowledge Create/Query Models
# =============================================================================


class KnowledgeCreate(BaseModel):
    """Request to create knowledge."""

    text: str = Field(..., description="Text content to store and index")
    metadata: Optional[Dict[str, str]] = Field(None, description="Optional tags for categorization")
    when_to_use: Optional[str] = Field(None, description="When to use this knowledge")


class KnowledgeResponse(BaseModel):
    """Response for knowledge creation (V4 async architecture).

    Processing happens asynchronously after creation.
    Poll /processing-status/{processing_job_id} for chunk/embedding completion.
    """

    status: str
    message: str
    knowledge_id: str
    processing_job_id: str
    processing_status: str
    project_id: str
    company_id: str
    title: str

    model_config = ConfigDict(from_attributes=True)


class KnowledgeQuery(BaseModel):
    """Query parameters for knowledge search."""

    query: str = Field(..., description="Natural language search query")
    limit: int = Field(10, ge=1, le=100, description="Maximum results to return")


class KnowledgeQueryResult(BaseModel):
    """Single knowledge search result."""

    text: str
    score: float
    metadata: Optional[Dict[str, Any]] = None


class KnowledgeQueryResponse(BaseModel):
    """Response for knowledge query."""

    status: str
    query: str
    project_id: str
    results_count: int
    results: List[Dict[str, Any]]

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Tag Search Models
# =============================================================================


class KnowledgeTagSearchParams(BaseModel):
    """Parameters for tag-based knowledge search."""

    keywords: Optional[List[str]] = Field(None, description="Filter by keywords list")
    chunk_type: Optional[str] = Field(None, description="Filter by type: prose, code, heading")
    has_code: Optional[bool] = Field(
        None, description="True = only chunks with code, False = only without"
    )
    section_title: Optional[str] = Field(None, description="Filter by section title match")
    section_level: Optional[int] = Field(
        None, description="Filter by heading level (1=h1, 2=h2, etc.)"
    )
    limit: int = Field(10, ge=1, le=100, description="Maximum results to return")


class KnowledgeTagSearchResponse(BaseModel):
    """Response for tag-based knowledge search."""

    status: str
    query: str
    project_id: str
    results_count: int
    results: List[Dict[str, Any]]

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# List Response Models
# =============================================================================


class KnowledgeListItem(BaseModel):
    """Lightweight item for list responses."""

    knowledge_id: str = Field(..., description="Unique identifier")
    title: str = Field(..., description="Knowledge title")
    summary: str = Field(..., description="First 200 chars of content")
    chunks_count: int = Field(..., description="Number of chunks")
    project_id: str
    metadata: Optional[Dict[str, Any]] = None
    cognee_status: Optional[str] = Field(None, description="Cognee processing status")
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class KnowledgeListResponse(BaseModel):
    """Paginated list response."""

    status: str = "success"
    total_count: int
    knowledge_list: List[KnowledgeListItem]
    limit: int
    offset: int

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Detail Response Model
# =============================================================================


class KnowledgeDetailResponse(BaseModel):
    """Full knowledge with content."""

    status: str = "success"
    knowledge_id: str
    title: str
    content: str = Field(..., description="Full markdown content")
    summary: str
    chunks_count: int
    company_id: str
    project_id: str
    metadata: Optional[Dict[str, Any]] = None
    cognee_status: Optional[str] = Field(None, description="Cognee processing status")
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Update Request Model
# =============================================================================


class KnowledgeUpdate(BaseModel):
    """Request body for PUT (modify)."""

    text: str = Field(..., min_length=1, max_length=500000, description="New markdown content")
    metadata: Optional[Dict[str, str]] = Field(None, description="Optional tags to replace")


# =============================================================================
# Delete Response Model
# =============================================================================


class KnowledgeDeleteResponse(BaseModel):
    """Response for DELETE."""

    status: str = "success"
    knowledge_id: str
    message: str = "Knowledge deleted successfully"

    model_config = ConfigDict(from_attributes=True)

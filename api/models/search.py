"""
Unified Search Pydantic Models
Request/response schemas for unified hybrid search endpoint.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any, Literal


# =============================================================================
# Unified Search Request/Response Models
# =============================================================================

class UnifiedSearchRequest(BaseModel):
    """Unified search request across all entity types."""

    query: str = Field(..., min_length=1, description="Search query text")
    entity_type: Literal["knowledge", "expertise", "entries", "code", "all"] = Field(
        default="all",
        description="Entity type to search"
    )
    project_id: Optional[str] = Field(
        default=None,
        description="Narrow scope to specific project (for knowledge and code search)"
    )
    page: int = Field(default=1, ge=1, description="Page number")
    page_size: int = Field(default=20, ge=1, le=100, description="Results per page")


class UnifiedSearchResult(BaseModel):
    """Single search result across any entity type."""

    entity_type: Literal["knowledge", "expertise", "entries", "code"]
    entity_id: str
    title: str
    content_preview: str = Field(..., max_length=500)
    score: float = Field(..., ge=0.0, le=1.0)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Parent context fields (for all types)
    parent_id: Optional[str] = None       # Source entity UUID
    parent_title: Optional[str] = None    # Source entity title

    # Temporal fields (for all types)
    sub_type: Optional[str] = None        # Entry type for engagement entries
    created_at: Optional[str] = None      # ISO 8601 timestamp
    updated_at: Optional[str] = None      # ISO 8601 timestamp

    # Type-specific fields (optional)
    file_path: Optional[str] = None      # For code
    language: Optional[str] = None       # For code
    entry_type: Optional[str] = None     # For entries
    engagement_id: Optional[str] = None  # For entries

    model_config = ConfigDict(from_attributes=True)


class PaginationInfo(BaseModel):
    """Pagination metadata."""

    page: int
    page_size: int
    total_results: int
    total_pages: int
    has_next: bool
    has_previous: bool


class UnifiedSearchResponse(BaseModel):
    """Unified search response."""

    status: Literal["success", "error"] = "success"
    query: str
    entity_type: str
    company_id: str
    project_id: Optional[str] = None
    pagination: PaginationInfo
    results: List[UnifiedSearchResult]

    model_config = ConfigDict(from_attributes=True)

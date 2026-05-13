"""
Brain v2 — Unified Search Pydantic models.

Supports tsvector-based Postgres search across all Brain entity types.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


# =============================================================================
# Request
# =============================================================================


class UnifiedSearchRequest(BaseModel):
    """POST /v2/search — search across brain entities."""

    query: str = Field(..., min_length=1, max_length=1000, description="Search query text")
    company_id: str = Field(
        ..., max_length=36, description="Company UUID (required for collection scoping)"
    )
    node_sets: Optional[List[str]] = Field(
        None,
        description=(
            "Entity types to search. Omit for all. Choices: knowledge, expertise, lessons, entries"
        ),
    )
    project_id: Optional[str] = Field(None, description="Narrow scope to a specific project")
    limit: int = Field(default=20, ge=1, le=100, description="Max results")


# =============================================================================
# Response
# =============================================================================


class UnifiedSearchResultItem(BaseModel):
    """Single search result across any entity type."""

    id: str = Field(..., description="Entity UUID")
    node_set: str = Field(..., description="Source collection / table")
    entity_type: str = Field(..., description="Specific type within the node set")
    name: str = Field(..., description="Title or display name")
    text: str = Field(..., description="Matched text / content preview")
    score: float = Field(..., description="Relevance score")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Type-specific metadata")

    model_config = ConfigDict(from_attributes=True)


class UnifiedSearchResponse(BaseModel):
    """Response envelope for unified search."""

    status: str = "success"
    query: str
    results_count: int
    results: List[UnifiedSearchResultItem]

    model_config = ConfigDict(from_attributes=True)

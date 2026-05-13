"""
Engagement Pydantic Models
Request/response schemas for engagements and engagement entries.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime


# =============================================================================
# Engagement Models
# =============================================================================


class EngagementCreate(BaseModel):
    """Request to create a new engagement."""

    name: str = Field(..., description="Engagement name")
    ultimate_goal: str = Field(
        ...,
        min_length=10,
        description="The overarching objective this engagement aims to achieve. MUST be discussed with stakeholder before setting.",
    )
    agent_id: Optional[str] = Field(None, description="Optional agent UUID")
    user_id: Optional[str] = Field(None, description="Optional user UUID")
    summary: Optional[str] = Field(None, description="Optional summary/description")
    engagement_id: Optional[str] = Field(
        None, description="Optional parent engagement UUID for hierarchical relationships"
    )


class EngagementUpdate(BaseModel):
    """Request to update an engagement."""

    name: Optional[str] = Field(None, description="New name")
    status: Optional[str] = Field(
        None, description="New status (created, preparation, execution, validation, done)"
    )
    summary: Optional[str] = Field(None, description="New summary")
    ultimate_goal: Optional[str] = Field(
        None, min_length=10, description="Updated goal (only if re-aligning)"
    )
    engagement_id: Optional[str] = Field(
        None, description="Optional parent engagement UUID for hierarchical relationships"
    )


class EngagementResponse(BaseModel):
    """Engagement response."""

    id: str
    project_id: str
    company_id: str
    name: str
    ultimate_goal: Optional[str] = None
    agent_id: Optional[str] = None
    user_id: Optional[str] = None
    summary: Optional[str] = None
    status: str
    engagement_id: Optional[str] = None
    cognee_status: Optional[str] = Field(None, description="Cognee processing status")
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class RootEngagementResponse(EngagementResponse):
    """Engagement response for root engagements (no parent).

    Extends EngagementResponse with hierarchy counts:
    - thread_count: number of direct children (threads)
    - episode_count: total grandchildren across all threads (episodes)
    """

    thread_count: int = 0
    episode_count: int = 0


class ChildEngagementResponse(BaseModel):
    """Engagement response for child (thread/episode) engagements."""

    id: str
    parent_id: str
    project_id: str
    company_id: str
    name: str
    ultimate_goal: Optional[str] = None
    agent_id: Optional[str] = None
    user_id: Optional[str] = None
    summary: Optional[str] = None
    status: str
    engagement_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    episode_count: int = 0

    model_config = ConfigDict(from_attributes=True)


class EngagementListResponse(BaseModel):
    """Response for listing engagements (roots only)."""

    total_count: int
    engagements: List[RootEngagementResponse]


class ChildrenListResponse(BaseModel):
    """Response for listing children of an engagement."""

    total_count: int
    children: List[ChildEngagementResponse]


# =============================================================================
# Entry Metadata (for lightweight listing)
# =============================================================================


class EntryMetadata(BaseModel):
    """Entry metadata without full content."""

    id: str
    entry_type: str
    title: str
    content_preview: Optional[str] = None
    content_length: Optional[int] = None
    created_by_agent_id: Optional[str] = None
    references: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    summary: Optional[str] = None
    version: int
    cognee_status: Optional[str] = Field(None, description="Cognee processing status")
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class EngagementWithEntriesResponse(BaseModel):
    """Engagement response with entry metadata."""

    id: str
    project_id: str
    company_id: str
    name: str
    ultimate_goal: Optional[str] = None
    agent_id: Optional[str] = None
    user_id: Optional[str] = None
    summary: Optional[str] = None
    status: str
    engagement_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    entries: List[EntryMetadata] = []

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Entry Models
# =============================================================================


class EntryCreate(BaseModel):
    """Request to add an entry to an engagement."""

    entry_type: str = Field(
        ...,
        description=(
            "Entry type: requirement, insight, decision, plan, note, question, "
            "business_requirement, user_requirement, architecture, technical_insight, "
            "implementation_plan, delegation_result, blocker"
        ),
    )
    title: str = Field(..., description="Entry title")
    content: str = Field(..., description="Entry content (markdown supported)")
    agent_id: Optional[str] = Field(None, description="Optional agent UUID who created this entry")
    references: Optional[List[str]] = Field(
        None, description="Optional list of entry IDs this entry references"
    )
    tags: Optional[List[str]] = Field(None, description="Optional list of tags for categorization")


class EntryUpdate(BaseModel):
    """Request to update an entry."""

    content: Optional[str] = Field(None, description="New content")
    references: Optional[List[str]] = Field(None, description="New list of references")
    tags: Optional[List[str]] = Field(None, description="New list of tags")


class EntryResponse(BaseModel):
    """Full entry response."""

    id: str
    engagement_id: str
    entry_type: str
    title: str
    content: str
    created_by_agent_id: Optional[str] = None
    references: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    summary: Optional[str] = None
    version: int
    cognee_status: Optional[str] = Field(None, description="Cognee processing status")
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class EntryListResponse(BaseModel):
    """Response for listing entries."""

    total_count: int
    entries: List[EntryMetadata]


# =============================================================================
# Search Models
# =============================================================================


class EntrySearchRequest(BaseModel):
    """Request for searching entries."""

    query: str = Field(..., description="Search query")
    engagement_id: Optional[str] = Field(
        None, description="Optional engagement UUID to filter within"
    )
    entry_type: Optional[str] = Field(None, description="Optional entry type filter")
    limit: int = Field(10, ge=1, le=100, description="Maximum results")


class EntrySearchResult(BaseModel):
    """Search result entry."""

    id: str
    entry_type: str
    title: str
    content: Optional[str] = None
    text: Optional[str] = None  # For Qdrant results
    engagement_id: Optional[str] = None
    score: float = 0.0
    vector_score: Optional[float] = None
    metadata: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)


class EntrySearchResponse(BaseModel):
    """Response for entry search."""

    results_count: int
    results: List[EntrySearchResult]


# =============================================================================
# Export Models
# =============================================================================


class EntryExport(BaseModel):
    """Full entry for export (all fields, full content)."""

    id: str
    entry_type: str
    title: str
    content: str
    created_by_agent_id: Optional[str] = None
    references: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    created_at: str


class EpisodeExport(BaseModel):
    """Episode (grandchild engagement) with its entries for export."""

    id: str
    name: str
    status: str
    summary: Optional[str] = None
    ultimate_goal: Optional[str] = None
    created_at: str
    entries: List[EntryExport] = []


class ThreadExport(BaseModel):
    """Thread (child engagement) with its entries and episodes for export."""

    id: str
    name: str
    status: str
    summary: Optional[str] = None
    ultimate_goal: Optional[str] = None
    created_at: str
    entries: List[EntryExport] = []
    episodes: List[EpisodeExport] = []


class EngagementExportResponse(BaseModel):
    """Full hierarchical export of a root engagement."""

    id: str
    name: str
    status: str
    summary: Optional[str] = None
    ultimate_goal: Optional[str] = None
    created_at: str
    entries: List[EntryExport] = []
    threads: List[ThreadExport] = []


class ProjectEngagementsExportResponse(BaseModel):
    """Bulk export of all root engagements for a project."""

    project_id: str
    exported_at: str  # ISO timestamp
    total_count: int
    engagements: List[EngagementExportResponse]


class ProjectEngagementsImportRequest(BaseModel):
    """Request body for bulk import of engagements into a project."""

    engagements: List[EngagementExportResponse]


class EngagementImportResponse(BaseModel):
    """Response after bulk import of engagements."""

    imported_count: int
    engagement_ids: List[str]


# =============================================================================
# Resume Context Models
# =============================================================================


class EntriesByType(BaseModel):
    """Entries grouped by type."""

    entry_type: str
    entries: List[dict]


class ResumeContextResponse(BaseModel):
    """Response for engagement resumption context."""

    engagement_id: str
    name: str
    ultimate_goal: Optional[str] = None
    status: str
    summary: Optional[str] = None
    entries_by_type: dict  # Dict[str, List[entry]]
    total_entries: int

    model_config = ConfigDict(from_attributes=True)

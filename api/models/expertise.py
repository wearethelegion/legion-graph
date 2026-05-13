"""
Expertise Pydantic Models
Request/response schemas for Expertise REST API endpoints.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, ConfigDict


# =============================================================================
# Agent Reference
# =============================================================================


class AgentRef(BaseModel):
    """Lightweight agent reference for expertise assignments."""

    agent_id: str
    name: str
    role: str = "unknown"  # Default for agents without role property in Neo4j

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# List Response Models
# =============================================================================


class ExpertiseListItem(BaseModel):
    """Lightweight item for list responses."""

    expertise_id: str = Field(..., description="Unique identifier")
    title: str = Field(..., description="Expertise title")
    summary: str = Field(..., description="First 200 chars of content")
    chunks_count: int = Field(..., description="Number of chunks")
    when_to_use: Optional[str] = Field(None, description="When this expertise should be consulted")
    assigned_agents: List[AgentRef] = Field(
        default_factory=list, description="Agents with this skill"
    )
    is_company_level: bool = Field(default=False, description="True if accessible to all projects")
    project_id: Optional[str] = Field(None, description="Project UUID if scoped")
    cognee_status: Optional[str] = Field(None, description="Cognee processing status")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ExpertiseListResponse(BaseModel):
    """Paginated list response."""

    status: str = "success"
    total_count: int
    expertise_list: List[ExpertiseListItem]
    limit: int
    offset: int

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Detail Response Model
# =============================================================================


class ExpertiseDetailResponse(BaseModel):
    """Full expertise with content."""

    status: str = "success"
    expertise_id: str
    title: str
    content: str = Field(..., description="Full markdown content")
    summary: str
    chunks_count: int
    when_to_use: Optional[str] = Field(None, description="When this expertise should be consulted")
    assigned_agents: List[AgentRef] = Field(default_factory=list)
    is_company_level: bool
    company_id: str
    project_id: Optional[str]
    metadata: Optional[Dict[str, Any]] = None
    cognee_status: Optional[str] = Field(None, description="Cognee processing status")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Update Request Model
# =============================================================================


class ExpertiseUpdate(BaseModel):
    """Request body for PUT (modify)."""

    text: str = Field(..., min_length=1, max_length=500000, description="New markdown content")
    metadata: Optional[Dict[str, str]] = Field(None, description="Optional tags to replace")
    agent_ids: Optional[List[str]] = Field(
        None, description="Agent IDs to reassign (if None, preserve existing)"
    )
    when_to_use: Optional[str] = Field(None, description="When this expertise should be consulted")


class ExpertiseMetadataUpdate(BaseModel):
    """Request body for PATCH (lightweight metadata update)."""

    when_to_use: Optional[str] = Field(None, description="When to use this expertise")


class ExpertiseUpdateResponse(BaseModel):
    """Response for PATCH (metadata update)."""

    status: str = "success"
    expertise_id: str
    title: str
    when_to_use: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Delete Response Model
# =============================================================================


class ExpertiseDeleteResponse(BaseModel):
    """Response for DELETE."""

    status: str = "success"
    expertise_id: str
    message: str = "Expertise deleted successfully"

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Agents Response Model
# =============================================================================


class ExpertiseAgentsResponse(BaseModel):
    """Response for GET /expertise/{id}/agents."""

    status: str = "success"
    expertise_id: str
    agents: List[AgentRef]
    agents_count: int

    model_config = ConfigDict(from_attributes=True)

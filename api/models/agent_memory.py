"""
Agent Memory Pydantic Models
Request/response schemas for agent memory and status logging operations.
"""

from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, ConfigDict


# =============================================================================
# Enums
# =============================================================================

class MemoryType(str, Enum):
    """Type of memory stored by an agent."""
    FACT = "fact"
    PREFERENCE = "preference"
    EXPERIENCE = "experience"
    CONTEXT = "context"
    RELATIONSHIP = "relationship"


class Category(str, Enum):
    """Category classification for memories."""
    GOOD_PRACTICE = "good_practice"
    BAD_PRACTICE = "bad_practice"
    PATTERN = "pattern"
    PREFERENCE = "preference"
    SOLUTION = "solution"
    BLOCKER = "blocker"
    DECISION = "decision"


class SourceOrigin(str, Enum):
    """Origin of how the memory was captured."""
    USER_EXPLICIT = "user_explicit"
    AGENT_INFERRED = "agent_inferred"
    AUTO_CAPTURED = "auto_captured"


class StatusType(str, Enum):
    """Agent status types for status logging."""
    THINKING = "thinking"
    WORKING = "working"
    WAITING = "waiting"
    COMPLETED = "completed"
    ERROR = "error"
    IDLE = "idle"


# =============================================================================
# Memory Request Models
# =============================================================================

class CreateMemoryRequest(BaseModel):
    """Request to create a new agent memory."""
    memory_type: MemoryType = Field(..., description="Type of memory")
    title: str = Field(..., min_length=1, max_length=255, description="Memory title")
    content: str = Field(..., min_length=1, description="Memory content")

    # Metadata fields
    tags: Optional[List[str]] = Field(None, description="Tags for categorization")
    category: Optional[Category] = Field(None, description="Category classification")
    importance: int = Field(default=3, ge=1, le=5, description="Importance level (1-5)")
    source_origin: Optional[SourceOrigin] = Field(None, description="How the memory was captured")

    # Source fields
    source: Optional[str] = Field(None, description="Source reference")
    source_type: Optional[str] = Field(None, description="Type of source")

    # Optional scoping
    user_id: Optional[str] = Field(None, description="User UUID for user-specific memories")
    project_id: Optional[str] = Field(None, description="Project UUID for project-specific memories")
    expires_at: Optional[datetime] = Field(None, description="When the memory expires")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class UpdateMemoryRequest(BaseModel):
    """Request to update an existing memory."""
    title: Optional[str] = Field(None, min_length=1, max_length=255, description="New title")
    content: Optional[str] = Field(None, min_length=1, description="New content")
    tags: Optional[List[str]] = Field(None, description="New tags")
    category: Optional[Category] = Field(None, description="New category")
    importance: Optional[int] = Field(None, ge=1, le=5, description="New importance level (1-5)")
    source_origin: Optional[SourceOrigin] = Field(None, description="New source origin")
    expires_at: Optional[datetime] = Field(None, description="New expiration time")
    metadata: Optional[Dict[str, Any]] = Field(None, description="New metadata")


class QueryMemoriesRequest(BaseModel):
    """Request to query agent memories."""
    query: str = Field(..., min_length=1, description="Search query")
    agent_id: str = Field(..., description="Agent UUID")
    user_id: Optional[str] = Field(None, description="Filter by user UUID")

    # Type filters
    memory_types: Optional[List[MemoryType]] = Field(None, description="Filter by memory types")

    # Metadata filters
    categories: Optional[List[Category]] = Field(None, description="Filter by categories")
    tags_filter: Optional[List[str]] = Field(None, description="Filter by tags")
    min_importance: Optional[int] = Field(None, ge=1, le=5, description="Minimum importance level")
    source_origins: Optional[List[SourceOrigin]] = Field(None, description="Filter by source origins")

    limit: int = Field(default=10, ge=1, le=100, description="Maximum results to return")


# =============================================================================
# Status Log Request Models
# =============================================================================

class LogStatusRequest(BaseModel):
    """Request to log an agent status update."""
    status_type: StatusType = Field(..., description="Status type")
    message: str = Field(..., min_length=1, description="Status message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional status details")

    # Metadata fields
    tags: Optional[List[str]] = Field(None, description="Tags for categorization")
    category: Optional[Category] = Field(None, description="Category classification")
    importance: int = Field(default=3, ge=1, le=5, description="Importance level (1-5)")
    # Note: source_origin is always 'auto_captured' for status logs (set server-side)

    # Context references
    engagement_id: Optional[str] = Field(None, description="Engagement UUID")
    task_id: Optional[str] = Field(None, description="Task UUID")
    delegation_id: Optional[str] = Field(None, description="Delegation UUID")
    user_id: Optional[str] = Field(None, description="User UUID")


# =============================================================================
# Memory Response Models
# =============================================================================

class MemoryResponse(BaseModel):
    """Response model for a single memory."""
    id: str
    company_id: str
    project_id: Optional[str] = None
    agent_id: str
    user_id: Optional[str] = None
    memory_type: MemoryType
    title: str
    content: str

    # Metadata fields
    tags: Optional[List[str]] = None
    category: Optional[Category] = None
    importance: int  # Integer 1-5
    source_origin: Optional[SourceOrigin] = None

    # Source
    source: Optional[str] = None
    source_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    # Lifecycle
    expires_at: Optional[datetime] = None
    last_accessed_at: Optional[datetime] = None
    access_count: int = 0
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class MemorySearchResult(BaseModel):
    """Search result containing a memory with relevance scores."""
    memory: MemoryResponse
    score: float = Field(..., description="Raw relevance score")
    weighted_score: float = Field(..., description="Score weighted by importance (score * importance/5)")


class MemorySearchResponse(BaseModel):
    """Response for memory search queries."""
    results_count: int
    results: List[MemorySearchResult]


class MemoryListResponse(BaseModel):
    """Response for listing memories."""
    total_count: int
    memories: List[MemoryResponse]


# =============================================================================
# Status Log Response Models
# =============================================================================

class StatusLogResponse(BaseModel):
    """Response model for a single status log entry."""
    id: str
    company_id: str
    agent_id: str
    user_id: Optional[str] = None
    status_type: StatusType
    message: str
    details: Optional[Dict[str, Any]] = None

    # Metadata fields
    tags: Optional[List[str]] = None
    category: Optional[Category] = None
    importance: int
    source_origin: SourceOrigin

    # Context references
    engagement_id: Optional[str] = None
    task_id: Optional[str] = None
    delegation_id: Optional[str] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class StatusTimelineResponse(BaseModel):
    """Response containing a timeline of status log entries."""
    entries: List[StatusLogResponse]
    total_count: int

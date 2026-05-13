"""
Memory Pydantic Models
Request/response schemas for the Agent Memory REST API.
"""

from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, ConfigDict


# =============================================================================
# Enums
# =============================================================================

class MemoryCategory(str, Enum):
    """Category for permanent memories."""
    PREFERENCE = "preference"
    FACT = "fact"
    PROCEDURE = "procedure"
    CONTEXT = "context"
    DECISION = "decision"
    LEARNING = "learning"
    GENERAL = "general"


class MemoryLevel(str, Enum):
    """Scope level for memory retrieval."""
    USER = "user"
    PROJECT = "project"
    REPOSITORY = "repository"
    BRANCH = "branch"


class TimeRange(str, Enum):
    """Time range filter for queries."""
    HOUR_1 = "1h"
    HOUR_24 = "24h"
    DAY_7 = "7d"
    DAY_30 = "30d"
    DAY_90 = "90d"
    ALL = "all"


# =============================================================================
# Permanent Memory CRUD Models
# =============================================================================

class PermanentMemoryCreate(BaseModel):
    """Request to create a permanent memory.

    NOTE: user_id is extracted from auth token, not provided in request.
    """
    text: str = Field(
        ...,
        min_length=1,
        max_length=10000,
        description="Memory text content"
    )
    category: MemoryCategory = Field(
        default=MemoryCategory.GENERAL,
        description="Memory category"
    )
    repository_id: Optional[str] = Field(
        None,
        description="Optional repository UUID for scoping"
    )
    branch_id: Optional[str] = Field(
        None,
        description="Optional branch UUID for scoping"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional metadata for the memory"
    )


class PermanentMemoryUpdate(BaseModel):
    """Request to update a permanent memory."""
    text: Optional[str] = Field(
        None,
        min_length=1,
        max_length=10000,
        description="Updated memory text"
    )
    category: Optional[MemoryCategory] = Field(
        None,
        description="Updated category"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Updated metadata"
    )


class PermanentMemoryResponse(BaseModel):
    """Response model for a permanent memory."""
    id: str
    text: str
    category: MemoryCategory
    user_id: str
    company_id: str
    project_id: str
    repository_id: Optional[str] = None
    branch_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PermanentMemoryListResponse(BaseModel):
    """Response for listing permanent memories."""
    total_count: int
    memories: List[PermanentMemoryResponse]


# =============================================================================
# Remember Operation Models
# =============================================================================

class ConversationMessage(BaseModel):
    """A single message from a conversation."""
    role: str = Field(
        ...,
        pattern="^(user|assistant|system)$",
        description="Message role: user, assistant, or system"
    )
    content: str = Field(
        ...,
        min_length=1,
        description="Message content"
    )


class RememberRequest(BaseModel):
    """Request to extract and store memories from a conversation."""
    messages: List[ConversationMessage] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Conversation messages to extract memories from"
    )
    session_id: Optional[str] = Field(
        None,
        description="Optional session identifier for grouping"
    )
    repository_id: Optional[str] = Field(
        None,
        description="Optional repository UUID for scoping"
    )
    branch_id: Optional[str] = Field(
        None,
        description="Optional branch UUID for scoping"
    )
    auto_consolidate: bool = Field(
        default=True,
        description="Whether to automatically consolidate similar memories"
    )
    extraction_hints: Optional[List[str]] = Field(
        None,
        description="Optional hints to guide memory extraction"
    )


class ExtractedMemory(BaseModel):
    """A memory extracted from conversation."""
    id: str
    text: str
    category: MemoryCategory
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Extraction confidence score"
    )
    source_message_index: int = Field(
        ...,
        ge=0,
        description="Index of source message in conversation"
    )


class RememberResponse(BaseModel):
    """Response from remember operation."""
    status: str
    session_id: Optional[str] = None
    memories_extracted: int
    memories_consolidated: int
    memories: List[ExtractedMemory]

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Recall Models
# =============================================================================

class RecallResult(BaseModel):
    """A single recall search result."""
    id: str
    text: str
    category: MemoryCategory
    score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Relevance score"
    )
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime


class RecallResponse(BaseModel):
    """Response for memory recall query."""
    status: str
    query: str
    level: MemoryLevel
    results_count: int
    results: List[RecallResult]

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Active Work Status Models
# =============================================================================

class ActiveWorkItem(BaseModel):
    """An active work item (engagement or task)."""
    type: str = Field(
        ...,
        description="Item type: engagement or task"
    )
    id: str
    title: str
    status: str
    started_at: Optional[datetime] = None
    last_activity: Optional[datetime] = None
    context: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional context for the work item"
    )


class ActiveWorkStatusResponse(BaseModel):
    """Response for active work status query."""
    status: str
    user_id: str
    project_id: str
    active_items_count: int
    active_engagements: List[ActiveWorkItem]
    active_tasks: List[ActiveWorkItem]
    recent_memories: List[PermanentMemoryResponse]
    last_checked: datetime

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Error Models
# =============================================================================

class ErrorDetail(BaseModel):
    """Detailed error information."""
    code: str = Field(
        ...,
        description="Error code"
    )
    message: str = Field(
        ...,
        description="Human-readable error message"
    )
    field: Optional[str] = Field(
        None,
        description="Field that caused the error, if applicable"
    )


class ErrorResponse(BaseModel):
    """Standard error response."""
    status: str = Field(
        default="error",
        description="Response status"
    )
    error: ErrorDetail

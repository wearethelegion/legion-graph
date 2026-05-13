"""
Task Pydantic Models
Request/response schemas for task management endpoints.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime


# =============================================================================
# Task Request Models
# =============================================================================

class TaskCreate(BaseModel):
    """Request to create a new task."""
    title: str = Field(..., description="Task title")
    ultimate_goal: str = Field(
        ...,
        min_length=10,
        description="The overarching objective this task aims to achieve. May inherit from engagement's goal or be task-specific."
    )
    description: Optional[str] = Field(None, description="Task description")
    engagement_id: Optional[str] = Field(None, description="Parent engagement UUID")
    priority: str = Field("medium", description="Priority: low, medium, high, critical")
    assigned_agent_id: Optional[str] = Field(None, description="Agent UUID to assign task to")
    created_by_agent_id: Optional[str] = Field(None, description="Agent UUID who created task")


class TaskUpdate(BaseModel):
    """Request to update a task."""
    status: Optional[str] = Field(None, description="Status: pending, assigned, in_progress, blocked, completed")
    priority: Optional[str] = Field(None, description="Priority: low, medium, high, critical")
    assigned_agent_id: Optional[str] = Field(None, description="Agent UUID to assign task to")
    blockers: Optional[str] = Field(None, description="Description of blockers")
    ultimate_goal: Optional[str] = Field(
        None,
        min_length=10,
        description="Updated goal (only if re-aligning)"
    )


class TaskAssign(BaseModel):
    """Request to assign a task to an agent."""
    agent_id: str = Field(..., description="Agent UUID to assign task to")


class ArtifactLink(BaseModel):
    """Request to link an artifact to a task."""
    artifact_type: str = Field(..., description="Artifact type: code, knowledge, expertise, lesson")
    artifact_id: str = Field(..., description="Artifact UUID")


# =============================================================================
# Task Response Models
# =============================================================================

class ArtifactResponse(BaseModel):
    """Artifact link response."""
    id: str
    task_id: str
    artifact_type: str
    artifact_id: str
    linked_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TaskResponse(BaseModel):
    """Task response model."""
    id: str
    company_id: str
    project_id: str
    engagement_id: Optional[str] = None
    title: str
    ultimate_goal: str
    description: Optional[str] = None
    status: str
    priority: str
    assigned_agent_id: Optional[str] = None
    created_by_agent_id: Optional[str] = None
    blockers: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None
    artifacts: Optional[List[ArtifactResponse]] = None

    model_config = ConfigDict(from_attributes=True)


class TaskListResponse(BaseModel):
    """Response for listing tasks."""
    total_count: int
    tasks: List[TaskResponse]


class ArtifactListResponse(BaseModel):
    """Response for listing task artifacts."""
    total_count: int
    artifacts: List[ArtifactResponse]

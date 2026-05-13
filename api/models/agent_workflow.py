"""
Agent Workflow Pydantic Models
Request/response schemas for agent behavioral workflow operations.

Workflow scoping hierarchy (most to least specific):
- agent_id: Workflow for specific agent
- role: Workflow for agents with specific role
- project_id: Workflow for all agents in project
- company_id: Workflow for all agents in company

Access control:
- user_id: Owner of the workflow (NULL = system workflow, admin-only modifiable)
- public: If TRUE, visible to others in same company/project
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime


class AgentWorkflowCreate(BaseModel):
    """Request to create a new agent workflow.

    Note: user_id is NOT accepted in request - it's set from current_user.user_id
    to prevent ownership spoofing.
    """
    name: str = Field(..., min_length=1, max_length=255, description="Workflow name")
    content: str = Field(..., min_length=1, description="Markdown workflow content")
    description: Optional[str] = Field(None, description="Optional workflow description")
    project_id: Optional[str] = Field(
        default=None,
        description="Scope to project. NULL = company-wide"
    )
    agent_id: Optional[str] = Field(
        default=None,
        description="Scope to specific agent"
    )
    role: Optional[str] = Field(
        default=None,
        max_length=50,
        description="Scope to agent role (e.g., developer, researcher)"
    )
    public: bool = Field(
        default=False,
        description="If true, visible to others in same company/project"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional flexible metadata"
    )
    signals: Optional[List[str]] = Field(
        default=None,
        description="Trigger hints for when to use this workflow (e.g., 'user asks about deployment')"
    )
    when_to_use: Optional[str] = Field(None, description="When to use this workflow")


class AgentWorkflowUpdate(BaseModel):
    """Request to update an existing agent workflow (all fields optional).

    Note: user_id cannot be changed (ownership transfer is not supported).

    Scope fields (project_id, agent_id, role) CAN be updated:
    - Omitted field = no change
    - Field set to null = clear scope (move towards company-scope)
    - Field set to value = set new scope

    Use model_dump(exclude_unset=True) to distinguish "not sent" from "sent as null".
    """
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="Workflow name")
    content: Optional[str] = Field(None, min_length=1, description="Markdown workflow content")
    description: Optional[str] = Field(None, description="Optional workflow description")
    project_id: Optional[str] = Field(
        default=None,
        description="Scope to project. Omit to keep current, set null to clear"
    )
    agent_id: Optional[str] = Field(
        default=None,
        description="Scope to specific agent. Omit to keep current, set null to clear"
    )
    role: Optional[str] = Field(
        default=None,
        max_length=50,
        description="Scope to agent role. Omit to keep current, set null to clear"
    )
    public: Optional[bool] = Field(
        default=None,
        description="Change visibility"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional flexible metadata"
    )
    signals: Optional[List[str]] = Field(
        default=None,
        description="Trigger hints for when to use this workflow"
    )
    when_to_use: Optional[str] = Field(None, description="When to use this workflow")


class AgentWorkflowResponse(BaseModel):
    """Agent workflow information response."""
    id: str
    company_id: str
    project_id: Optional[str] = None
    agent_id: Optional[str] = None
    role: Optional[str] = None
    user_id: Optional[str] = Field(
        default=None,
        description="Owner. NULL = system workflow"
    )
    public: bool = Field(
        default=False,
        description="If true, visible to others"
    )
    name: str
    content: str
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    signals: List[str] = Field(
        default_factory=list,
        description="Trigger hints for when to use this workflow"
    )
    when_to_use: Optional[str] = Field(None, description="When to use this workflow")
    version: int = 1
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class AgentWorkflowListResponse(BaseModel):
    """List of agent workflows response with pagination."""
    workflows: List[AgentWorkflowResponse]
    total: int


class ApplicableWorkflowsQuery(BaseModel):
    """Query parameters for getting applicable workflows for an agent."""
    agent_id: Optional[str] = Field(
        default=None,
        description="Agent UUID to find applicable workflows for"
    )
    role: Optional[str] = Field(
        default=None,
        description="Agent role to find applicable workflows for"
    )
    project_id: Optional[str] = Field(
        default=None,
        description="Project context for workflow resolution"
    )

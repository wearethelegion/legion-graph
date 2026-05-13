"""
Workflow Definition Pydantic Models
Request/response schemas for workflow configuration API.

Based on ADR-002: Workflow Configuration API Architecture
"""

from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Optional, List, Literal
from datetime import datetime
import re


# =============================================================================
# Step Models
# =============================================================================

class WorkflowStepCreate(BaseModel):
    """Request to create/add a workflow step."""
    name: str = Field(..., description="Step name (snake_case, becomes LangGraph node)")
    display_name: Optional[str] = Field(None, description="Human-readable name")
    step_order: int = Field(..., ge=1, description="Execution order (1-indexed)")

    # Agent assignment (provide one or the other)
    agent_id: Optional[str] = Field(None, description="Specific agent UUID")
    agent_role: Optional[str] = Field(None, description="Role: researcher, architect, developer, protector")

    # Task template
    task_template: str = Field(..., description="Task template with placeholders")

    # Execution settings
    timeout_seconds: int = Field(600, ge=60, le=3600, description="Step timeout in seconds")
    max_retries: int = Field(0, ge=0, le=5, description="Max retry attempts")

    # Flow control
    on_success: str = Field("END", description="Next step name or 'END'")
    on_failure: str = Field("error", description="'retry', 'error', step name, or 'END'")
    requires_approval: bool = Field(False, description="Require human approval before execution")

    # Step type
    step_type: Literal["agent", "approval_gate", "error_handler"] = Field("agent")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not re.match(r'^[a-z][a-z0-9_]*$', v):
            raise ValueError("Step name must be snake_case (lowercase, underscores, start with letter)")
        return v

    # Role validation removed - roles are now dynamic per company
    # Future: validate against company_roles table when available


class WorkflowStepUpdate(BaseModel):
    """Request to update a workflow step."""
    display_name: Optional[str] = None
    step_order: Optional[int] = Field(None, ge=1)
    agent_id: Optional[str] = None
    agent_role: Optional[str] = None
    task_template: Optional[str] = None
    timeout_seconds: Optional[int] = Field(None, ge=60, le=3600)
    max_retries: Optional[int] = Field(None, ge=0, le=5)
    on_success: Optional[str] = None
    on_failure: Optional[str] = None
    requires_approval: Optional[bool] = None


class WorkflowStepResponse(BaseModel):
    """Workflow step response."""
    id: str
    workflow_definition_id: str
    name: str
    display_name: Optional[str] = None
    step_order: int
    agent_id: Optional[str] = None
    agent_role: Optional[str] = None
    task_template: str
    timeout_seconds: int
    max_retries: int
    on_success: str
    on_failure: str
    requires_approval: bool
    step_type: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# Definition Models
# =============================================================================

class WorkflowDefinitionCreate(BaseModel):
    """Request to create a workflow definition."""
    name: str = Field(..., description="Workflow name (unique per company)")
    description: Optional[str] = Field(None, description="Workflow description")
    project_id: Optional[str] = Field(None, description="Project UUID (null = company-wide)")
    steps: Optional[List[WorkflowStepCreate]] = Field(None, description="Initial steps")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Workflow name cannot be empty")
        if len(v) > 255:
            raise ValueError("Workflow name must be 255 characters or less")
        return v


class WorkflowDefinitionUpdate(BaseModel):
    """Request to update a workflow definition."""
    name: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None


class WorkflowDefinitionResponse(BaseModel):
    """Workflow definition response (without steps)."""
    id: str
    name: str
    description: Optional[str] = None
    company_id: str
    project_id: Optional[str] = None
    is_active: bool
    is_builtin: bool
    version: int
    created_at: datetime
    updated_at: datetime
    created_by_agent_id: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class WorkflowDefinitionWithStepsResponse(WorkflowDefinitionResponse):
    """Workflow definition response with steps included."""
    steps: List[WorkflowStepResponse] = []


class WorkflowDefinitionListResponse(BaseModel):
    """Response for listing workflow definitions."""
    total_count: int
    definitions: List[WorkflowDefinitionResponse]


# =============================================================================
# Validation Models
# =============================================================================

class WorkflowValidationResult(BaseModel):
    """Result of workflow definition validation."""
    is_valid: bool
    errors: List[str] = []
    warnings: List[str] = []

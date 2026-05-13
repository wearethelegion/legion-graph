"""
Delegation Pydantic models for REST API.
Request and response models for delegation operations.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List


class ProgressStep(BaseModel):
    """Progress step in delegation execution."""
    step: int = Field(..., description="Step number")
    tool: str = Field(..., description="Tool being used")
    input_summary: str = Field("", description="Summary of input")
    timestamp: str = Field("", description="ISO timestamp")


class DelegationStatusResponse(BaseModel):
    """Response model for delegation status."""
    id: str = Field(..., description="Delegation UUID")
    status: str = Field(..., description="Delegation status (pending, running, completed, failed, cancelled, interrupted)")
    agent_name: str = Field(..., description="Agent name")
    agent_role: str = Field(..., description="Agent role")
    task: Optional[str] = Field(None, description="Full task description given to the agent")
    current_action: Optional[str] = Field(None, description="Current action being performed")
    steps_completed: int = Field(0, description="Number of steps completed")
    progress: List[ProgressStep] = Field(default_factory=list, description="Recent progress steps")
    started_at: Optional[datetime] = Field(None, description="When execution started")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")


class DelegationResultResponse(BaseModel):
    """Response model for delegation result."""
    id: str = Field(..., description="Delegation UUID")
    status: str = Field(..., description="Delegation status")
    agent_name: str = Field(..., description="Agent name")
    agent_role: str = Field(..., description="Agent role")
    result_summary: Optional[str] = Field(None, description="Result summary text")
    tools_used: List[str] = Field(default_factory=list, description="Tools used during execution")
    turns: int = Field(0, description="Number of turns/iterations")
    cost_usd: float = Field(0.0, description="Estimated cost in USD")
    error_detail: Optional[str] = Field(None, description="Error message if failed")
    started_at: Optional[datetime] = Field(None, description="When execution started")
    completed_at: Optional[datetime] = Field(None, description="When execution completed")


class DelegationSummary(BaseModel):
    """Summary model for delegation list items."""
    id: str = Field(..., description="Delegation UUID")
    agent_name: str = Field(..., description="Agent name")
    agent_role: str = Field(..., description="Agent role")
    status: str = Field(..., description="Delegation status")
    task_summary: str = Field("", description="First 100 chars of task description")
    steps_completed: int = Field(0, description="Number of steps completed")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")


class DelegationListResponse(BaseModel):
    """Response model for listing delegations."""
    total_count: int = Field(..., description="Total number of delegations matching filters")
    delegations: List[DelegationSummary] = Field(..., description="List of delegation summaries")


class CancelDelegationResponse(BaseModel):
    """Response model for cancellation."""
    id: str = Field(..., description="Delegation UUID")
    status: str = Field(..., description="New status after cancellation")
    message: str = Field(..., description="Cancellation message")

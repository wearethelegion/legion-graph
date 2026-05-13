"""
Project Pydantic Models
Request/response schemas for project operations.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    pass

# Forward reference for circular import avoidance
from api.models.instructions import ProjectInstructionsCreate


class ProjectInstructionsEmbedded(BaseModel):
    """Project instructions embedded in project response (excludes project_id)."""

    id: str
    description: Optional[str] = None
    languages: Optional[list[str]] = None
    frameworks: Optional[list[str]] = None
    tools: Optional[list[str]] = None
    architecture_notes: Optional[str] = None
    conventions: Optional[str] = None
    custom_instructions: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ProjectCreate(BaseModel):
    """Request to create a new project."""

    name: str = Field(..., min_length=1, max_length=255, description="Project name")
    description: Optional[str] = Field(None, description="Project description")
    instructions: Optional[ProjectInstructionsCreate] = Field(
        None, description="Project instructions"
    )


class ProjectUpdate(BaseModel):
    """Request to update a project."""

    name: Optional[str] = Field(None, min_length=1, max_length=255, description="New project name")
    description: Optional[str] = Field(None, description="New project description")
    github_token: Optional[str] = Field(None, description="GitHub Personal Access Token")
    instructions: Optional[ProjectInstructionsCreate] = Field(
        None, description="Project instructions"
    )


class ProjectTransferRequest(BaseModel):
    """Request to transfer a project to a different company."""

    target_company_id: str = Field(..., min_length=1, description="Target company UUID")


class ProjectTransferResponse(BaseModel):
    """Response for project company transfer operations."""

    project_id: str
    project_name: str
    source_company_id: str
    target_company_id: str
    transferred: bool
    postgresql: dict
    neo4j: dict
    qdrant: dict
    mongodb: dict
    message: str


class ProjectResponse(BaseModel):
    """Project information response."""

    id: str
    company_id: str
    name: str
    description: Optional[str]
    webhook_url: Optional[str] = None
    github_webhook_secret: Optional[str] = None
    github_token_set: bool = False
    created_at: str
    updated_at: str
    instructions: Optional[ProjectInstructionsEmbedded] = None

    model_config = ConfigDict(from_attributes=True)


class WebhookSecretResponse(BaseModel):
    """Response for webhook secret regeneration - shows full secret ONCE."""

    project_id: str
    webhook_url: str
    github_webhook_secret: str
    message: str = "Save this secret now. It will not be shown again."


class ProjectListResponse(BaseModel):
    """List of projects response."""

    projects: list[ProjectResponse]
    total: int

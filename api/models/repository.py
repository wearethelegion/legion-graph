"""
Repository Pydantic Models
Request/response schemas for repository operations.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional


class RepositoryCreate(BaseModel):
    """Request to create a new repository."""
    name: str = Field(..., min_length=1, max_length=255, description="Repository name")
    url: Optional[str] = Field(None, description="Repository URL")


class RepositoryResponse(BaseModel):
    """Repository information response."""
    id: str
    project_id: str
    name: str
    url: Optional[str]
    created_at: str
    updated_at: str

    model_config = ConfigDict(from_attributes=True)


class RepositoryListResponse(BaseModel):
    """List of repositories response."""
    repositories: list[RepositoryResponse]
    total: int

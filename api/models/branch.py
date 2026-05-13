"""
Branch Pydantic Models
Request/response schemas for branch operations.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional


class BranchCreate(BaseModel):
    """Request to create a new branch."""
    name: str = Field(..., min_length=1, max_length=255, description="Branch name")
    commit_sha: Optional[str] = Field(None, description="Commit SHA")


class BranchResponse(BaseModel):
    """Branch information response."""
    id: str
    repository_id: str
    name: str
    commit_sha: Optional[str]
    created_at: str
    updated_at: str

    model_config = ConfigDict(from_attributes=True)


class BranchListResponse(BaseModel):
    """List of branches response."""
    branches: list[BranchResponse]
    total: int

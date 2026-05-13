"""
Feature Pydantic Models
Request/response schemas for feature operations.
"""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict
from datetime import datetime


class FeatureCreate(BaseModel):
    """Request to create a new feature."""
    name: str = Field(..., min_length=1, max_length=255, description="Feature name")
    description: str = Field(..., min_length=1, description="Feature description")
    status: Optional[str] = Field("ready for refinement", max_length=50)
    priority: Optional[str] = Field("medium", max_length=20)
    next_prompt: Optional[str] = Field(None, description="Next step or prompt")
    metadata: Optional[Dict] = Field(default_factory=dict)


class FeatureUpdate(BaseModel):
    """Request to update a feature."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, min_length=1)
    status: Optional[str] = Field(None, max_length=50)
    priority: Optional[str] = Field(None, max_length=20)
    next_prompt: Optional[str] = None
    metadata: Optional[Dict] = None


class FeatureResponse(BaseModel):
    """Feature information response."""
    id: str
    company_id: str
    project_id: str
    name: str
    description: str
    status: str
    priority: str
    next_prompt: Optional[str]
    metadata: Dict
    chunk_count: int
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str]

    model_config = ConfigDict(from_attributes=True)


class FeatureListResponse(BaseModel):
    """List of features response."""
    features: list[FeatureResponse]
    total: int

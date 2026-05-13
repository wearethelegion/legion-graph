"""
Company Pydantic Models
Request/response schemas for company operations.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class CompanyCreate(BaseModel):
    """Request to create a new company."""

    name: str = Field(..., min_length=1, max_length=255, description="Company name")
    description: Optional[str] = Field(None, description="Company description")


class CompanyResponse(BaseModel):
    """Company information response."""

    id: str
    name: str
    description: Optional[str]
    cognee_enabled: bool = False
    created_at: str
    updated_at: str

    model_config = ConfigDict(from_attributes=True)


class CompanyListResponse(BaseModel):
    """List of companies response."""

    companies: list[CompanyResponse]
    total: int

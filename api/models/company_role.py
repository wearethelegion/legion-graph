"""
Company Role Pydantic Models
Request/response schemas for company role operations.

Per architecture plan (entry 733d587e-3a7b-48a0-88e0-d48f9298cfd3):
- Dynamic roles per company (company_roles table)
- System roles cannot be deleted (is_system=true)
- Role names: lowercase, hyphen-separated pattern
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class CompanyRoleBase(BaseModel):
    """Base fields for company role."""
    name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        pattern=r'^[a-z][a-z0-9-]*$',
        description="Role identifier (lowercase, hyphen-separated, e.g., 'developer', 'ui-developer')"
    )
    display_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Human-readable role name (e.g., 'Developer', 'UI Developer')"
    )
    description: Optional[str] = Field(
        None,
        description="Role description"
    )
    color: Optional[str] = Field(
        None,
        pattern=r'^#[0-9A-Fa-f]{6}$',
        description="Hex color for UI (e.g., '#10B981')"
    )
    icon: Optional[str] = Field(
        None,
        max_length=50,
        description="Icon identifier for UI"
    )
    sort_order: int = Field(
        default=0,
        description="UI ordering (lower = higher priority)"
    )


class CompanyRoleCreate(CompanyRoleBase):
    """Request to create a new company role."""
    pass


class CompanyRoleUpdate(BaseModel):
    """Request to update an existing company role.

    Note: name and is_system are immutable after creation.
    """
    display_name: Optional[str] = Field(
        None,
        min_length=1,
        max_length=100,
        description="Human-readable role name"
    )
    description: Optional[str] = Field(
        None,
        description="Role description"
    )
    color: Optional[str] = Field(
        None,
        pattern=r'^#[0-9A-Fa-f]{6}$',
        description="Hex color for UI"
    )
    icon: Optional[str] = Field(
        None,
        max_length=50,
        description="Icon identifier for UI"
    )
    is_active: Optional[bool] = Field(
        None,
        description="Toggle role active status"
    )
    sort_order: Optional[int] = Field(
        None,
        description="UI ordering"
    )


class CompanyRoleResponse(CompanyRoleBase):
    """Company role response."""
    id: str
    company_id: str
    is_system: bool = Field(
        default=False,
        description="System roles cannot be deleted"
    )
    is_active: bool = Field(
        default=True,
        description="Inactive roles are hidden from selection"
    )
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CompanyRoleListResponse(BaseModel):
    """List of company roles response."""
    roles: list[CompanyRoleResponse]
    total: int

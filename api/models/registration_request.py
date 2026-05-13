"""
Registration Request Pydantic Models
Request/response schemas for public registration requests.
"""

from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime


class RegistrationRequestCreate(BaseModel):
    """Request to create a new registration request (public endpoint)."""
    full_name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Full name of the person requesting access"
    )
    email: EmailStr = Field(
        ...,
        description="Email address"
    )
    organisation: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Organisation name"
    )
    organisation_size: str = Field(
        ...,
        pattern=r"^(1-5|6-20|21-50|51-200|200\+)$",
        description="Organisation size range (1-5, 6-20, 21-50, 51-200, 200+)"
    )
    referral_source: Optional[str] = Field(
        None,
        description="How did you hear about us"
    )


class RegistrationRequestResponse(BaseModel):
    """Registration request response."""
    id: str
    full_name: str
    email: str
    organisation: str
    organisation_size: str
    referral_source: Optional[str]
    status: str
    created_at: datetime

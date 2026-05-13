"""
Company Config Pydantic Models
Request/response schemas for company-level configuration.

Includes session interceptors:
- summariser: Rolling Haiku extraction (default: ON)
- advisor_mode_a: Advisory mode A (default: OFF)
- advisor_mode_b: Advisory mode B (default: OFF)
- auditor: Session auditor (default: ON)

Plus extensible settings JSONB for future company-level config.

Each interceptor has an enabled flag and optional custom system prompt.
Configuration is scoped per company.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, Any
from datetime import datetime


class CompanyConfigResponse(BaseModel):
    """Company configuration response."""

    id: str
    company_id: str
    summariser_enabled: bool
    advisor_mode_a_enabled: bool
    advisor_mode_b_enabled: bool
    auditor_enabled: bool
    summariser_prompt: Optional[str] = None
    advisor_prompt: Optional[str] = None
    settings: Optional[Dict[str, Any]] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CompanyConfigUpdate(BaseModel):
    """Request to update company configuration (all fields optional)."""

    summariser_enabled: Optional[bool] = Field(
        None, description="Enable/disable summariser interceptor"
    )
    advisor_mode_a_enabled: Optional[bool] = Field(
        None, description="Enable/disable advisor mode A"
    )
    advisor_mode_b_enabled: Optional[bool] = Field(
        None, description="Enable/disable advisor mode B"
    )
    auditor_enabled: Optional[bool] = Field(None, description="Enable/disable auditor interceptor")
    summariser_prompt: Optional[str] = Field(
        None, description="Custom system prompt for summariser"
    )
    advisor_prompt: Optional[str] = Field(None, description="Custom system prompt for advisor")
    settings: Optional[Dict[str, Any]] = Field(
        None, description="Extensible settings JSONB for future company-level config"
    )

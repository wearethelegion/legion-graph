"""
KGRAG API - Company Config Routes
RESTful endpoints for company-level configuration.

Configuration is per-company. Includes session interceptors:
- summariser: Rolling Haiku extraction
- advisor_mode_a: Advisory mode A
- advisor_mode_b: Advisory mode B
- auditor: Session auditor

Plus extensible settings JSONB for future config.

Access Control:
- All endpoints require authentication and company access validation
- GET / auto-creates config with defaults if none exists
- PUT / performs partial update (COALESCE semantics)
- GET /list returns all configs (paginated)
"""

from fastapi import APIRouter, Depends, Query, status
from loguru import logger

from api.services.company_config_service import CompanyConfigService
from api.models.company_config import (
    CompanyConfigResponse,
    CompanyConfigUpdate,
)
from api.auth import CurrentUser, get_current_user, validate_company_access
from api.database import get_db_pool
from api.repositories.company_config_repository import CompanyConfigRepository

router = APIRouter(
    prefix="/api/v1/companies/{company_id}/config",
    tags=["company-config"],
)


async def get_company_config_service() -> CompanyConfigService:
    """Dependency to provide CompanyConfigService instance."""
    pool = await get_db_pool()
    repository = CompanyConfigRepository(pool)
    return CompanyConfigService(repository)


@router.get("", response_model=CompanyConfigResponse)
async def get_config(
    company_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: CompanyConfigService = Depends(get_company_config_service),
):
    """
    Get company configuration for a company.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Auto-Creation**: If no config exists, one is created with default values:
    - summariser_enabled: true
    - advisor_mode_a_enabled: false
    - advisor_mode_b_enabled: false
    - auditor_enabled: true
    - prompts: null
    - settings: {}

    **Response**: 200 OK with CompanyConfigResponse
    """
    # Validate company access
    validate_company_access(current_user, company_id)

    config = await service.get_config(company_id)
    return CompanyConfigResponse(**config)


@router.put("", response_model=CompanyConfigResponse)
async def update_config(
    company_id: str,
    data: CompanyConfigUpdate,
    current_user: CurrentUser = Depends(get_current_user),
    service: CompanyConfigService = Depends(get_company_config_service),
):
    """
    Update company configuration for a company.

    **Authentication Required**: User must be authenticated and have access to the company.

    **Partial Update**: Only provided fields are updated (COALESCE semantics).
    Omitted fields retain their current values.

    **Request Body** (all fields optional):
    - summariser_enabled: Enable/disable summariser
    - advisor_mode_a_enabled: Enable/disable advisor mode A
    - advisor_mode_b_enabled: Enable/disable advisor mode B
    - auditor_enabled: Enable/disable auditor
    - summariser_prompt: Custom system prompt for summariser
    - advisor_prompt: Custom system prompt for advisor
    - settings: Extensible settings dict for future config

    **Response**: 200 OK with updated CompanyConfigResponse
    """
    # Validate company access
    validate_company_access(current_user, company_id)

    updated = await service.update_config(company_id, data)
    return CompanyConfigResponse(**updated)


@router.get("/list")
async def list_configs(
    company_id: str,
    limit: int = Query(default=50, ge=1, le=100, description="Maximum number of results"),
    offset: int = Query(default=0, ge=0, description="Skip first N results"),
    current_user: CurrentUser = Depends(get_current_user),
    service: CompanyConfigService = Depends(get_company_config_service),
):
    """
    List all company configurations (paginated).

    **Authentication Required**: User must be authenticated and have access to the company.

    **Pagination**:
    - limit: Maximum results per page (1-100, default: 50)
    - offset: Skip first N results (default: 0)

    **Response**: 200 OK with:
    ```json
    {
        "total_count": 42,
        "configs": [...]
    }
    ```
    """
    # Validate company access
    validate_company_access(current_user, company_id)

    result = await service.list_configs(limit, offset)
    return {
        "total_count": result["total_count"],
        "configs": [CompanyConfigResponse(**c) for c in result["configs"]],
    }

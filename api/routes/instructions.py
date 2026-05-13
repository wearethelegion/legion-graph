"""
Instructions Management Routes (Layered Architecture)
Thin HTTP controllers that delegate to service layer.
Handles company and project instructions endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Depends, status, Request
from api.auth import get_current_user, CurrentUser
from api.database import get_db_pool
from api.repositories import ProjectRepository
from api.services.instructions_service import InstructionsService
from api.models import (
    CompanyInstructionsCreate,
    CompanyInstructionsResponse,
    ProjectInstructionsCreate,
    ProjectInstructionsResponse,
)
import asyncpg

router = APIRouter(prefix="/api/v1", tags=["instructions"])


# =============================================================================
# Service Dependencies
# =============================================================================

async def get_instructions_service(
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> InstructionsService:
    """Dependency to get instructions service with repository."""
    project_repository = ProjectRepository(pool)
    return InstructionsService(pool, project_repository=project_repository)


# =============================================================================
# Company Instructions Endpoints
# =============================================================================

@router.get(
    "/companies/{company_id}/instructions",
    response_model=Optional[CompanyInstructionsResponse],
    summary="Get company instructions"
)
async def get_company_instructions(
    company_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: InstructionsService = Depends(get_instructions_service)
) -> Optional[CompanyInstructionsResponse]:
    """
    Get instructions for a company.

    Returns company-wide instructions including ground rules, coding standards,
    communication style, and forbidden actions.

    - **company_id**: Company UUID

    Returns:
        Company instructions or null if not configured

    **Authentication Required**
    """
    return await service.get_company_instructions(company_id, current_user)


@router.put(
    "/companies/{company_id}/instructions",
    response_model=CompanyInstructionsResponse,
    summary="Update company instructions"
)
async def upsert_company_instructions(
    company_id: str,
    data: CompanyInstructionsCreate,
    current_user: CurrentUser = Depends(get_current_user),
    service: InstructionsService = Depends(get_instructions_service)
) -> CompanyInstructionsResponse:
    """
    Create or update company instructions.

    All fields are optional - only provided fields will be updated.
    Use COALESCE semantics: null/missing fields retain existing values.

    - **company_id**: Company UUID
    - **ground_rules**: General principles and values
    - **coding_standards**: Code style guidelines (SOLID, DRY, KISS, etc.)
    - **communication_style**: How agents should communicate
    - **forbidden_actions**: Things agents should never do
    - **custom_instructions**: Any other company-wide instructions

    Returns:
        Created/updated company instructions

    **Authentication Required**
    """
    return await service.upsert_company_instructions(company_id, data, current_user)


# =============================================================================
# Project Instructions Endpoints
# =============================================================================

@router.get(
    "/projects/{project_id}/instructions",
    response_model=Optional[ProjectInstructionsResponse],
    summary="Get project instructions"
)
async def get_project_instructions(
    project_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: InstructionsService = Depends(get_instructions_service)
) -> Optional[ProjectInstructionsResponse]:
    """
    Get instructions for a project.

    Returns project-specific instructions including tech stack,
    architecture notes, and conventions.

    - **project_id**: Project UUID

    Returns:
        Project instructions or null if not configured

    **Authentication Required**
    """
    return await service.get_project_instructions(project_id, current_user)


@router.put(
    "/projects/{project_id}/instructions",
    response_model=ProjectInstructionsResponse,
    summary="Update project instructions"
)
async def upsert_project_instructions(
    project_id: str,
    data: ProjectInstructionsCreate,
    current_user: CurrentUser = Depends(get_current_user),
    service: InstructionsService = Depends(get_instructions_service)
) -> ProjectInstructionsResponse:
    """
    Create or update project instructions.

    All fields are optional - only provided fields will be updated.
    Use COALESCE semantics: null/missing fields retain existing values.

    - **project_id**: Project UUID
    - **description**: What the project is
    - **languages**: Programming languages used (list)
    - **frameworks**: Frameworks used (list)
    - **tools**: Tools and infrastructure (list)
    - **architecture_notes**: Key architectural decisions
    - **conventions**: Project-specific patterns and conventions
    - **custom_instructions**: Any other project-specific instructions

    Returns:
        Created/updated project instructions

    **Authentication Required**
    """
    return await service.upsert_project_instructions(project_id, data, current_user)

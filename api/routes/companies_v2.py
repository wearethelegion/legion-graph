"""
Company Management Routes (Layered Architecture)
Thin HTTP controllers that delegate to service layer.
"""

from typing import Dict, Any
from fastapi import APIRouter, Depends, status, Request
from api.auth import get_current_user, CurrentUser
from api.database import get_db_pool
from api.repositories import CompanyRepository, ProjectRepository
from api.services import CompanyService
from api.models import CompanyCreate, CompanyResponse, CompanyListResponse
import asyncpg

router = APIRouter(prefix="/api/v1/companies", tags=["companies-v2"])


async def get_company_service(
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> CompanyService:
    """Dependency to get company service with repository."""
    repository = CompanyRepository(pool)
    project_repository = ProjectRepository(pool)
    # Use singletons from app.state to avoid resource leaks
    neo4j_repository = request.app.state.neo4j_repository
    qdrant_repository = request.app.state.qdrant_repository
    return CompanyService(
        repository, 
        neo4j_repository, 
        qdrant_repository,
        project_repository=project_repository
    )


@router.post(
    "",
    response_model=CompanyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new company"
)
async def create_company(
    data: CompanyCreate,
    current_user: CurrentUser = Depends(get_current_user),
    service: CompanyService = Depends(get_company_service)
) -> CompanyResponse:
    """
    Create a new company.

    - **name**: Company name (required)
    - **description**: Company description (optional)

    Returns the created company with ID and timestamps.

    **Authentication Required**
    """
    return await service.create_company(data, current_user)


@router.get(
    "",
    response_model=CompanyListResponse,
    summary="List companies"
)
async def list_companies(
    current_user: CurrentUser = Depends(get_current_user),
    service: CompanyService = Depends(get_company_service)
) -> CompanyListResponse:
    """
    List all companies.

    - Super admin sees all companies
    - Regular users see only their companies

    **Authentication Required**
    """
    return await service.list_companies(current_user)


@router.get(
    "/{company_id}",
    response_model=CompanyResponse,
    summary="Get company details"
)
async def get_company(
    company_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: CompanyService = Depends(get_company_service)
) -> CompanyResponse:
    """
    Get company details by ID.

    - Validates company exists
    - Checks user has access (super admin or company member)

    **Authentication Required**
    """
    return await service.get_company(company_id, current_user)


@router.delete(
    "/{company_id}",
    status_code=status.HTTP_200_OK,
    summary="Delete company and all data"
)
async def delete_company(
    company_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: CompanyService = Depends(get_company_service)
) -> Dict[str, Any]:
    """
    Delete company and ALL related data from all databases.

    **WARNING: This is a destructive operation that cannot be undone!**

    **Deletes from:**
    - **PostgreSQL**: Company and all related data (CASCADE)
      - Projects, Repositories, Branches, Engagements, Tasks, etc.
    - **Neo4j**: ALL nodes with company_id (Company, Project, Code, Entity, etc.)
    - **Qdrant**: Entire company collection (all vectors)
    - **Local filesystem**: Git repository clones

    Returns deletion statistics showing what was removed.

    **Authentication Required: Super Admin Only**
    """
    return await service.delete_company(company_id, current_user)
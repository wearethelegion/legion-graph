"""
Repository Management Routes (Layered Architecture)
Thin HTTP controllers that delegate to service layer.
"""

from fastapi import APIRouter, Depends, status, Request
from api.auth import get_current_user, CurrentUser
from api.database import get_db_pool
from api.repositories import (
    RepositoryRepository,
    ProjectRepository,
    CompanyRepository
)
from api.services import RepositoryService
from api.models import RepositoryCreate, RepositoryResponse, RepositoryListResponse
import asyncpg

router = APIRouter(prefix="/api/v1", tags=["repositories-v2"])


async def get_repository_service(
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> RepositoryService:
    """Dependency to get repository service with repositories."""
    repository_repository = RepositoryRepository(pool)
    project_repository = ProjectRepository(pool)
    company_repository = CompanyRepository(pool)
    # Use singleton from app.state to avoid resource leaks
    neo4j_repository = request.app.state.neo4j_repository
    return RepositoryService(
        repository_repository,
        project_repository,
        company_repository,
        neo4j_repository
    )


@router.post(
    "/projects/{project_id}/repositories",
    response_model=RepositoryResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new repository"
)
async def create_repository(
    project_id: str,
    data: RepositoryCreate,
    current_user: CurrentUser = Depends(get_current_user),
    service: RepositoryService = Depends(get_repository_service)
) -> RepositoryResponse:
    """
    Create a new repository under a project.

    - **name**: Repository name (required)
    - **url**: Repository URL (optional)

    Returns the created repository with ID and timestamps.

    **Authentication Required**
    """
    return await service.create_repository(project_id, data, current_user)


@router.get(
    "/projects/{project_id}/repositories",
    response_model=RepositoryListResponse,
    summary="List project repositories"
)
async def list_repositories(
    project_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: RepositoryService = Depends(get_repository_service)
) -> RepositoryListResponse:
    """
    List all repositories for a project.

    - Validates project exists
    - Checks user has access to project via company membership

    **Authentication Required**
    """
    return await service.list_repositories(project_id, current_user)


@router.get(
    "/repositories/{repository_id}",
    response_model=RepositoryResponse,
    summary="Get repository details"
)
async def get_repository(
    repository_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: RepositoryService = Depends(get_repository_service)
) -> RepositoryResponse:
    """
    Get repository details by ID.

    - Validates repository exists
    - Checks user has access via project and company membership

    **Authentication Required**
    """
    return await service.get_repository(repository_id, current_user)

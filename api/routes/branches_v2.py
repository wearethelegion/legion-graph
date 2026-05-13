"""
Branch Management Routes (Layered Architecture)
Thin HTTP controllers that delegate to service layer.
"""

from fastapi import APIRouter, Depends, status, Request
from api.auth import get_current_user, CurrentUser
from api.database import get_db_pool
from api.repositories import (
    BranchRepository,
    RepositoryRepository,
    ProjectRepository,
    CompanyRepository
)
from api.services import BranchService
from api.models import BranchCreate, BranchResponse, BranchListResponse
import asyncpg

router = APIRouter(prefix="/api/v1", tags=["branches-v2"])


async def get_branch_service(
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> BranchService:
    """Dependency to get branch service with repositories."""
    branch_repository = BranchRepository(pool)
    repository_repository = RepositoryRepository(pool)
    project_repository = ProjectRepository(pool)
    company_repository = CompanyRepository(pool)
    # Use singleton from app.state to avoid resource leaks
    neo4j_repository = request.app.state.neo4j_repository
    return BranchService(
        branch_repository,
        repository_repository,
        project_repository,
        company_repository,
        neo4j_repository
    )


@router.post(
    "/repositories/{repository_id}/branches",
    response_model=BranchResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new branch"
)
async def create_branch(
    repository_id: str,
    data: BranchCreate,
    current_user: CurrentUser = Depends(get_current_user),
    service: BranchService = Depends(get_branch_service)
) -> BranchResponse:
    """
    Create a new branch under a repository.

    - **name**: Branch name (required)
    - **commit_sha**: Commit SHA (optional)

    Returns the created branch with ID and timestamps.

    **Authentication Required**
    """
    return await service.create_branch(repository_id, data, current_user)


@router.get(
    "/repositories/{repository_id}/branches",
    response_model=BranchListResponse,
    summary="List repository branches"
)
async def list_branches(
    repository_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: BranchService = Depends(get_branch_service)
) -> BranchListResponse:
    """
    List all branches for a repository.

    - Validates repository exists
    - Checks user has access via repository, project, and company membership

    **Authentication Required**
    """
    return await service.list_branches(repository_id, current_user)


@router.get(
    "/branches/{branch_id}",
    response_model=BranchResponse,
    summary="Get branch details"
)
async def get_branch(
    branch_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: BranchService = Depends(get_branch_service)
) -> BranchResponse:
    """
    Get branch details by ID.

    - Validates branch exists
    - Checks user has access via repository, project, and company membership

    **Authentication Required**
    """
    return await service.get_branch(branch_id, current_user)

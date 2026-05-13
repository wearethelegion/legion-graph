"""
Ingestion Management Routes (Layered Architecture)
Thin HTTP controllers that delegate to service layer.
Handles ingestion status monitoring endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from api.auth import get_current_user, CurrentUser
from api.database import get_db_pool
from api.repositories import ProjectRepository
from api.services.ingestion_service import IngestionService
from api.models.ingestion import (
    IngestionStatusResponse,
    IngestionListResponse,
    IngestionProgressResponse,
)
import asyncpg

router = APIRouter(prefix="/api/v1", tags=["ingestions"])


# =============================================================================
# Service Dependencies
# =============================================================================


async def get_ingestion_service(pool: asyncpg.Pool = Depends(get_db_pool)) -> IngestionService:
    """Dependency to get ingestion service with repository."""
    project_repository = ProjectRepository(pool)
    return IngestionService(pool, project_repository=project_repository)


# =============================================================================
# Ingestion Endpoints
# =============================================================================


@router.get(
    "/ingestions/{ingestion_id}",
    response_model=IngestionStatusResponse,
    summary="Get ingestion status",
)
async def get_ingestion_status(
    ingestion_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: IngestionService = Depends(get_ingestion_service),
) -> IngestionStatusResponse:
    """
    Get status and progress of a specific ingestion.

    Returns detailed status including files processed, failed files,
    and completion percentage.

    - **ingestion_id**: Ingestion UUID

    Returns:
        Ingestion status with progress details

    **Authentication Required**
    """
    return await service.get_ingestion_status(ingestion_id, current_user)


@router.get(
    "/ingestions/{ingestion_id}/progress",
    response_model=IngestionProgressResponse,
    summary="Get ingestion progress",
)
async def get_ingestion_progress(
    ingestion_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: IngestionService = Depends(get_ingestion_service),
) -> IngestionProgressResponse:
    """
    Get lightweight progress for a specific ingestion.

    - **ingestion_id**: Ingestion UUID

    Returns:
        Ingestion progress with stage counters

    **Authentication Required**
    """
    return await service.get_ingestion_progress(ingestion_id, current_user)


@router.get(
    "/projects/{project_id}/ingestions",
    response_model=IngestionListResponse,
    summary="List ingestions for a project",
)
async def list_ingestions(
    project_id: str,
    status_filter: Optional[str] = Query(
        None, description="Filter by status (pending, running, completed, failed)"
    ),
    limit: int = Query(20, ge=1, le=100, description="Maximum results to return"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    current_user: CurrentUser = Depends(get_current_user),
    service: IngestionService = Depends(get_ingestion_service),
) -> IngestionListResponse:
    """
    List ingestions for a project.

    Returns ingestion summaries with repository info, status, and progress.

    - **project_id**: Project UUID
    - **status_filter**: Optional filter by ingestion status
    - **limit**: Maximum results (1-100, default 20)

    Returns:
        List of ingestion summaries

    **Authentication Required**
    """
    return await service.list_ingestions(
        project_id=project_id,
        current_user=current_user,
        status_filter=status_filter,
        limit=limit,
        offset=offset,
    )

"""Ingestion API routes for queueing repository processing requests."""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from ..auth import get_current_user, CurrentUser
from ..database import get_db_pool
from ..repositories import ProjectRepository
from ..services.kafka_service import (
    KafkaProducerService,
    KafkaPublishError,
    get_kafka_service,
)

router = APIRouter(prefix="/api/v1", tags=["projects-v2"])


class RepositoryRequest(BaseModel):
    """Payload expected when queueing a repository for processing."""

    repository: str = Field(..., description="Repository name to process", min_length=1)
    branch: str = Field(..., description="Branch to process", min_length=1)
    framework: str = Field(..., description="Framework to process", min_length=1)
    project_id: str = Field(..., description="Project UUID (required)", min_length=1)
    force_full_refresh: bool = Field(
        False,
        description="Reprocess all files even if no new commits are detected",
    )

@router.post(
    "/code_ingestion",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue repository ingestion",
    response_description="Confirmation that the request has been enqueued",
)
async def enqueue_repository_request(
    payload: RepositoryRequest,
    current_user: CurrentUser = Depends(get_current_user),
    kafka_service: KafkaProducerService = Depends(get_kafka_service),
):
    """
    Accept repository ingestion requests and forward them to Kafka.

    Authentication: Requires Bearer token in Authorization header.
    Authorization: User must have access to the project's company.
    """

    # Get project and validate access
    pool = await get_db_pool()
    project_repo = ProjectRepository(pool)

    project = await project_repo.get_by_id(payload.project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {payload.project_id} not found"
        )

    company_id = project["company_id"]

    # Verify user has access to this company
    if company_id not in current_user.companies and not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {payload.project_id} not found"  # Don't leak info
        )

    try:
        await kafka_service.publish_repository(
            repository=payload.repository,
            branch=payload.branch,
            framework=payload.framework,
            project_id=payload.project_id,
            company_id=company_id,
            user_id=current_user.user_id,
            force_full_refresh=payload.force_full_refresh,
        )
    except KafkaPublishError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "status": "queued",
        "repository": payload.repository,
        "project_id": payload.project_id,
        "company_id": company_id,
        "topic": kafka_service.topic,
        "branch": payload.branch,
        "framework": payload.framework,
        "force_full_refresh": payload.force_full_refresh,
    }

"""
Feature Management Routes (Layered Architecture)
Thin HTTP controllers that delegate to service layer.
"""

from fastapi import APIRouter, Depends, status, Request, HTTPException
from api.auth import get_current_user, CurrentUser, validate_company_access
from uuid import UUID
from api.database import get_db_pool
from api.repositories import FeatureRepository, ProjectRepository
from api.services.feature_service import FeatureService
from api.services.feature_sync_service import FeatureSyncService
from api.models.feature import FeatureCreate, FeatureUpdate, FeatureResponse, FeatureListResponse
import asyncpg

router = APIRouter(prefix="/api/v1", tags=["features"])


async def get_feature_service(
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> FeatureService:
    """Dependency to get feature service with repositories."""
    feature_repository = FeatureRepository(pool)
    project_repository = ProjectRepository(pool)
    
    # Use singletons from app.state to avoid resource leaks
    neo4j_repository = request.app.state.neo4j_repository
    qdrant_repository = request.app.state.qdrant_repository
    
    # Create sync service
    sync_service = FeatureSyncService(
        feature_repo=feature_repository,
        neo4j_repo=neo4j_repository,
        qdrant_repo=qdrant_repository
    )
    
    return FeatureService(
        feature_repo=feature_repository,
        project_repo=project_repository,
        sync_service=sync_service
    )


@router.post(
    "/projects/{project_id}/features",
    response_model=FeatureResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new feature"
)
async def create_feature(
    project_id: str,
    data: FeatureCreate,
    current_user: CurrentUser = Depends(get_current_user),
    service: FeatureService = Depends(get_feature_service)
) -> FeatureResponse:
    """
    Create a new feature under a project.

    - **name**: Feature name (required)
    - **description**: Feature description (required)
    - **status**: Feature status (default: "ready for refinement")
    - **priority**: Feature priority (default: "medium")
    - **next_prompt**: Next step or prompt (optional)
    - **metadata**: Additional metadata (optional)

    Smart chunking:
    - Automatically chunks description if >500 chars OR has markdown sections
    - Syncs to Neo4j knowledge graph and Qdrant vector store

    Returns the created feature with ID, timestamps, and chunk count.

    **Authentication Required**
    """
    # Get project to find its company_id
    project = await service.project_repo.get_by_id(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    company_id = project["company_id"]
    
    # Validate user has access to this company
    validate_company_access(current_user, company_id)
    
    # Convert user_id to UUID
    user_uuid = UUID(current_user.user_id)
    
    return await service.create_feature(
        company_id=company_id,
        project_id=project_id,
        name=data.name,
        description=data.description,
        status=data.status or "ready for refinement",
        priority=data.priority or "medium",
        next_prompt=data.next_prompt,
        metadata=data.metadata,
        created_by=str(user_uuid)
    )


@router.get(
    "/projects/{project_id}/features",
    response_model=FeatureListResponse,
    summary="List project features"
)
async def list_features(
    project_id: str,
    limit: int = 50,
    offset: int = 0,
    current_user: CurrentUser = Depends(get_current_user),
    service: FeatureService = Depends(get_feature_service)
) -> FeatureListResponse:
    """
    List all features for a project.

    - **limit**: Maximum number of results (default: 50)
    - **offset**: Results offset for pagination (default: 0)

    **Authentication Required**
    """
    return await service.list_features(
        project_id=project_id,
        limit=limit,
        offset=offset
    )


@router.get(
    "/features/{feature_id}",
    response_model=FeatureResponse,
    summary="Get feature details"
)
async def get_feature(
    feature_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: FeatureService = Depends(get_feature_service)
) -> FeatureResponse:
    """
    Get feature details by ID.

    - Validates feature exists
    - Returns feature with chunk count

    **Authentication Required**
    """
    return await service.get_feature(feature_id)


@router.put(
    "/features/{feature_id}",
    response_model=FeatureResponse,
    summary="Update feature"
)
async def update_feature(
    feature_id: str,
    data: FeatureUpdate,
    current_user: CurrentUser = Depends(get_current_user),
    service: FeatureService = Depends(get_feature_service)
) -> FeatureResponse:
    """
    Update feature fields.

    - **name**: New feature name (optional)
    - **description**: New description (optional)
    - **status**: New status (optional)
    - **priority**: New priority (optional)
    - **next_prompt**: New next_prompt (optional)
    - **metadata**: New metadata (optional)

    Note: Updating description does NOT automatically re-chunk.
    Only provided fields will be updated.

    **Authentication Required**
    """
    return await service.update_feature(
        feature_id=feature_id,
        name=data.name,
        description=data.description,
        status=data.status,
        priority=data.priority,
        next_prompt=data.next_prompt,
        metadata=data.metadata
    )


@router.delete(
    "/features/{feature_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete feature"
)
async def delete_feature(
    feature_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: FeatureService = Depends(get_feature_service)
) -> None:
    """
    Delete feature and all associated data.

    - Removes from PostgreSQL (cascades to chunks)
    - Removes from Neo4j knowledge graph
    - Removes from Qdrant vector store

    **Authentication Required**
    """
    # Get feature to find its company_id
    feature = await service.get_feature(feature_id)
    company_id = feature["company_id"]
    
    # Validate user has access to this company
    validate_company_access(current_user, company_id)
    
    await service.delete_feature(feature_id, company_id)

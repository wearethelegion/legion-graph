"""
Project Management Routes (Layered Architecture)
Thin HTTP controllers that delegate to service layer.
"""

from typing import Dict, Any
from fastapi import APIRouter, Depends, status, Request
from api.auth import get_current_user, CurrentUser
from api.database import get_db_pool
from api.repositories import (
    ProjectRepository,
    CompanyRepository,
    RepositoryRepository,
    InstructionsRepository,
)
from api.services import ProjectService
from api.models import (
    ProjectCreate,
    ProjectUpdate,
    ProjectTransferRequest,
    ProjectTransferResponse,
    ProjectResponse,
    ProjectListResponse,
    WebhookSecretResponse,
)
import asyncpg

router = APIRouter(prefix="/api/v1", tags=["projects-v2"])


async def get_project_service(
    request: Request, pool: asyncpg.Pool = Depends(get_db_pool)
) -> ProjectService:
    """Dependency to get project service with repositories."""
    project_repository = ProjectRepository(pool)
    company_repository = CompanyRepository(pool)
    repository_repository = RepositoryRepository(pool)
    instructions_repository = InstructionsRepository(pool)
    # Use singletons from app.state to avoid resource leaks
    neo4j_repository = request.app.state.neo4j_repository
    qdrant_repository = request.app.state.qdrant_repository
    return ProjectService(
        project_repository,
        company_repository,
        neo4j_repository,
        qdrant_repository,
        repository_repository=repository_repository,
        instructions_repository=instructions_repository,
    )


@router.post(
    "/companies/{company_id}/projects",
    response_model=ProjectResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new project",
)
async def create_project(
    company_id: str,
    data: ProjectCreate,
    current_user: CurrentUser = Depends(get_current_user),
    service: ProjectService = Depends(get_project_service),
) -> ProjectResponse:
    """
    Create a new project under a company.

    - **name**: Project name (required)
    - **description**: Project description (optional)
    - **instructions**: Project instructions (optional) - languages, frameworks, tools, etc.

    Returns the created project with ID, timestamps, and embedded instructions.

    **Authentication Required**
    """
    return await service.create_project(company_id, data, current_user)


@router.get(
    "/companies/{company_id}/projects",
    response_model=ProjectListResponse,
    summary="List company projects",
)
async def list_projects(
    company_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: ProjectService = Depends(get_project_service),
) -> ProjectListResponse:
    """
    List all projects for a company.

    - Validates company exists
    - Checks user has access to company

    **Authentication Required**
    """
    return await service.list_projects(company_id, current_user)


@router.get("/projects/{project_id}", response_model=ProjectResponse, summary="Get project details")
async def get_project(
    project_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: ProjectService = Depends(get_project_service),
) -> ProjectResponse:
    """
    Get project details by ID.

    - Validates project exists
    - Checks user has access via company membership

    **Authentication Required**
    """
    return await service.get_project(project_id, current_user)


@router.delete(
    "/projects/{project_id}",
    status_code=status.HTTP_200_OK,
    summary="Delete project and all associated data",
)
async def delete_project(
    project_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: ProjectService = Depends(get_project_service),
) -> Dict[str, Any]:
    """
    Delete a project and ALL associated data across all databases.

    **WARNING: This is a destructive operation that cannot be undone!**

    Deletes from:
    - **PostgreSQL**: projects table (cascades to repositories, branches,
      engagements, tasks, task_artifacts, project_instructions)
    - **Neo4j**: All graph nodes with project_id (Project, Repository, Branch,
      Document, Code, Entity, Knowledge, Expertise, Fact)
    - **Qdrant**: All vector embeddings with project_id metadata
    - **Local filesystem**: Git repository clones in rag_storage/repos

    Returns deletion statistics showing what was removed from each database.

    **Authentication Required**
    """
    return await service.delete_project(project_id, current_user)


@router.put(
    "/projects/{project_id}", response_model=ProjectResponse, summary="Update project details"
)
async def update_project(
    project_id: str,
    data: ProjectUpdate,
    current_user: CurrentUser = Depends(get_current_user),
    service: ProjectService = Depends(get_project_service),
) -> ProjectResponse:
    """
    Update project fields.

    - **name**: Optional new project name
    - **description**: Optional new description
    - **github_token**: Optional GitHub Personal Access Token
    - **instructions**: Optional project instructions (upserts if provided)

    Only provided fields will be updated (COALESCE semantics).
    Sensitive fields like `github_token` are never returned -
    only `github_token_set: true/false` indicates if a token is configured.

    Returns project with embedded instructions.

    **Authentication Required**
    """
    return await service.update_project(project_id, data, current_user)


@router.post(
    "/projects/{project_id}/transfer-company",
    response_model=ProjectTransferResponse,
    status_code=status.HTTP_200_OK,
    summary="Transfer project to another company",
)
async def transfer_project_company(
    project_id: str,
    data: ProjectTransferRequest,
    current_user: CurrentUser = Depends(get_current_user),
    service: ProjectService = Depends(get_project_service),
) -> ProjectTransferResponse:
    """
    Transfer a project and all project-scoped data to another company.

    Migrates ownership/scope across:
    - PostgreSQL
    - Neo4j
    - Qdrant
    - MongoDB (ingestions collection)

    Includes validation, authorization, idempotency checks, and
    compensation attempt if PostgreSQL transfer fails after external moves.

    **Authentication Required**
    """
    return await service.transfer_project_company(project_id, data, current_user)


@router.post(
    "/projects/{project_id}/regenerate-webhook-secret",
    response_model=WebhookSecretResponse,
    summary="Regenerate webhook secret",
)
async def regenerate_webhook_secret(
    project_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: ProjectService = Depends(get_project_service),
) -> WebhookSecretResponse:
    """
    Regenerate the GitHub webhook secret for a project.

    **IMPORTANT: The new secret is shown ONLY ONCE in this response.**
    After this, only a masked version will be available via GET /projects/{id}.

    Use this when:
    - Setting up a new webhook integration
    - The current secret may have been compromised
    - Rotating secrets as a security practice

    **Authentication Required**
    """
    return await service.regenerate_webhook_secret(project_id, current_user)

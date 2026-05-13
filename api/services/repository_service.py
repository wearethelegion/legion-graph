"""
Repository Service
Business logic for repository operations.
"""

from typing import List
from fastapi import HTTPException, status
from api.repositories import (
    RepositoryRepository,
    ProjectRepository,
    CompanyRepository,
    Neo4jRepository
)
from api.models import RepositoryCreate, RepositoryResponse, RepositoryListResponse
from api.auth import CurrentUser
from loguru import logger


class RepositoryService:
    """Service for repository business logic."""

    def __init__(
        self,
        repository_repository: RepositoryRepository,
        project_repository: ProjectRepository,
        company_repository: CompanyRepository,
        neo4j_repository: Neo4jRepository
    ):
        self.repository_repository = repository_repository
        self.project_repository = project_repository
        self.company_repository = company_repository
        self.neo4j_repository = neo4j_repository

    async def create_repository(
        self,
        project_id: str,
        data: RepositoryCreate,
        current_user: CurrentUser
    ) -> RepositoryResponse:
        """
        Create a new repository under a project.

        Args:
            project_id: Project UUID
            data: Repository creation data
            current_user: Current authenticated user

        Returns:
            Created repository response

        Raises:
            HTTPException: On validation or database errors
        """
        # Check if project exists
        if not await self.project_repository.exists(project_id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Project {project_id} not found"
            )

        # Get company_id for access check
        company_id = await self.project_repository.get_company_id(project_id)
        if not company_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Project {project_id} not found"
            )

        # Check access (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.company_repository.user_has_access(
                current_user.user_id,
                company_id
            )

            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied to this project"
                )

        repository = None
        try:
            # Create repository in database
            repository = await self.repository_repository.create(
                project_id=project_id,
                name=data.name,
                url=data.url
            )

            # Create repository node in Neo4j (single 'kgrag' database)
            try:
                await self.neo4j_repository.create_repository_node(
                    repository_id=repository["id"],
                    project_id=project_id,
                    name=repository["name"],
                    url=repository.get("url"),
                    company_id=company_id
                )
            except Exception as neo4j_error:
                # Rollback: delete repository from PostgreSQL
                await self.repository_repository.delete(repository["id"])
                logger.error("Neo4j creation failed, rolled back repository {}: {}", repository['id'], neo4j_error)
                raise

            logger.info(
                f"User {current_user.user_id} created repository {repository['id']} "
                f"under project {project_id}"
            )

            return RepositoryResponse(
                id=repository["id"],
                project_id=repository["project_id"],
                name=repository["name"],
                url=repository["url"],
                created_at=repository["created_at"].isoformat(),
                updated_at=repository["updated_at"].isoformat()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to create repository: {}", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create repository: {str(e)}"
            )

    async def get_repository(
        self,
        repository_id: str,
        current_user: CurrentUser
    ) -> RepositoryResponse:
        """
        Get repository by ID.

        Args:
            repository_id: Repository UUID
            current_user: Current authenticated user

        Returns:
            Repository response

        Raises:
            HTTPException: If repository not found or access denied
        """
        # Check if repository exists
        repository = await self.repository_repository.get_by_id(repository_id)

        if not repository:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Repository {repository_id} not found"
            )

        # Get company_id via project for access check
        company_id = await self.project_repository.get_company_id(repository["project_id"])
        if not company_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Project {repository['project_id']} not found"
            )

        # Check access via company (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.company_repository.user_has_access(
                current_user.user_id,
                company_id
            )

            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied to this repository"
                )

        return RepositoryResponse(
            id=repository["id"],
            project_id=repository["project_id"],
            name=repository["name"],
            url=repository["url"],
            created_at=repository["created_at"].isoformat(),
            updated_at=repository["updated_at"].isoformat()
        )

    async def list_repositories(
        self,
        project_id: str,
        current_user: CurrentUser
    ) -> RepositoryListResponse:
        """
        List all repositories for a project.

        Args:
            project_id: Project UUID
            current_user: Current authenticated user

        Returns:
            List of repositories

        Raises:
            HTTPException: If project not found or access denied
        """
        # Check if project exists
        if not await self.project_repository.exists(project_id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Project {project_id} not found"
            )

        # Get company_id for access check
        company_id = await self.project_repository.get_company_id(project_id)
        if not company_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Project {project_id} not found"
            )

        # Check access (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.company_repository.user_has_access(
                current_user.user_id,
                company_id
            )

            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied to this project"
                )

        try:
            repositories = await self.repository_repository.get_by_project(project_id)

            repository_responses = [
                RepositoryResponse(
                    id=r["id"],
                    project_id=r["project_id"],
                    name=r["name"],
                    url=r["url"],
                    created_at=r["created_at"].isoformat(),
                    updated_at=r["updated_at"].isoformat()
                )
                for r in repositories
            ]

            return RepositoryListResponse(
                repositories=repository_responses,
                total=len(repository_responses)
            )

        except Exception as e:
            logger.error("Failed to list repositories: {}", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list repositories: {str(e)}"
            )

"""
Branch Service
Business logic for branch operations.
"""

from typing import List
from fastapi import HTTPException, status
from api.repositories import (
    BranchRepository,
    RepositoryRepository,
    ProjectRepository,
    CompanyRepository,
    Neo4jRepository
)
from api.models import BranchCreate, BranchResponse, BranchListResponse
from api.auth import CurrentUser
from loguru import logger


class BranchService:
    """Service for branch business logic."""

    def __init__(
        self,
        branch_repository: BranchRepository,
        repository_repository: RepositoryRepository,
        project_repository: ProjectRepository,
        company_repository: CompanyRepository,
        neo4j_repository: Neo4jRepository
    ):
        self.branch_repository = branch_repository
        self.repository_repository = repository_repository
        self.project_repository = project_repository
        self.company_repository = company_repository
        self.neo4j_repository = neo4j_repository

    async def create_branch(
        self,
        repository_id: str,
        data: BranchCreate,
        current_user: CurrentUser
    ) -> BranchResponse:
        """
        Create a new branch under a repository.

        Args:
            repository_id: Repository UUID
            data: Branch creation data
            current_user: Current authenticated user

        Returns:
            Created branch response

        Raises:
            HTTPException: On validation or database errors
        """
        # Check if repository exists
        if not await self.repository_repository.exists(repository_id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Repository {repository_id} not found"
            )

        # Get project_id and company_id for access check
        project_id = await self.repository_repository.get_project_id(repository_id)
        if not project_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Repository {repository_id} not found"
            )

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
                    detail="Access denied to this repository"
                )

        branch = None
        try:
            # Create branch in database
            branch = await self.branch_repository.create(
                repository_id=repository_id,
                name=data.name,
                commit_sha=data.commit_sha
            )

            # Create branch node in Neo4j (single 'kgrag' database)
            try:
                await self.neo4j_repository.create_branch_node(
                    branch_id=branch["id"],
                    repository_id=repository_id,
                    name=branch["name"],
                    commit_sha=branch.get("commit_sha"),
                    company_id=company_id,
                    project_id=project_id
                )
            except Exception as neo4j_error:
                # Rollback: delete branch from PostgreSQL
                await self.branch_repository.delete(branch["id"])
                logger.error("Neo4j creation failed, rolled back branch {}: {}", branch['id'], neo4j_error)
                raise

            logger.info(
                f"User {current_user.user_id} created branch {branch['id']} "
                f"under repository {repository_id}"
            )

            return BranchResponse(
                id=branch["id"],
                repository_id=branch["repository_id"],
                name=branch["name"],
                commit_sha=branch["commit_sha"],
                created_at=branch["created_at"].isoformat(),
                updated_at=branch["updated_at"].isoformat()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to create branch: {}", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create branch: {str(e)}"
            )

    async def get_branch(
        self,
        branch_id: str,
        current_user: CurrentUser
    ) -> BranchResponse:
        """
        Get branch by ID.

        Args:
            branch_id: Branch UUID
            current_user: Current authenticated user

        Returns:
            Branch response

        Raises:
            HTTPException: If branch not found or access denied
        """
        # Check if branch exists
        branch = await self.branch_repository.get_by_id(branch_id)

        if not branch:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Branch {branch_id} not found"
            )

        # Get company_id via repository and project for access check
        project_id = await self.repository_repository.get_project_id(branch["repository_id"])
        if not project_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Repository {branch['repository_id']} not found"
            )

        company_id = await self.project_repository.get_company_id(project_id)
        if not company_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Project {project_id} not found"
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
                    detail="Access denied to this branch"
                )

        return BranchResponse(
            id=branch["id"],
            repository_id=branch["repository_id"],
            name=branch["name"],
            commit_sha=branch["commit_sha"],
            created_at=branch["created_at"].isoformat(),
            updated_at=branch["updated_at"].isoformat()
        )

    async def list_branches(
        self,
        repository_id: str,
        current_user: CurrentUser
    ) -> BranchListResponse:
        """
        List all branches for a repository.

        Args:
            repository_id: Repository UUID
            current_user: Current authenticated user

        Returns:
            List of branches

        Raises:
            HTTPException: If repository not found or access denied
        """
        # Check if repository exists
        if not await self.repository_repository.exists(repository_id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Repository {repository_id} not found"
            )

        # Get project_id and company_id for access check
        project_id = await self.repository_repository.get_project_id(repository_id)
        if not project_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Repository {repository_id} not found"
            )

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
                    detail="Access denied to this repository"
                )

        try:
            branches = await self.branch_repository.get_by_repository(repository_id)

            branch_responses = [
                BranchResponse(
                    id=b["id"],
                    repository_id=b["repository_id"],
                    name=b["name"],
                    commit_sha=b["commit_sha"],
                    created_at=b["created_at"].isoformat(),
                    updated_at=b["updated_at"].isoformat()
                )
                for b in branches
            ]

            return BranchListResponse(
                branches=branch_responses,
                total=len(branch_responses)
            )

        except Exception as e:
            logger.error("Failed to list branches: {}", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list branches: {str(e)}"
            )

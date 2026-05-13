"""
Instructions Service
Business logic for company and project instructions operations.
"""

from typing import Optional, Dict, Any
from fastapi import HTTPException, status
from api.repositories import InstructionsRepository, ProjectRepository
from api.models import (
    CompanyInstructionsCreate,
    CompanyInstructionsResponse,
    ProjectInstructionsCreate,
    ProjectInstructionsResponse,
)
from api.auth import CurrentUser, validate_company_access
from loguru import logger
import asyncpg


class InstructionsService:
    """Service for instructions business logic."""

    def __init__(self, pool: asyncpg.Pool, project_repository: Optional[ProjectRepository] = None):
        self.repo = InstructionsRepository(pool)
        self.project_repo = project_repository or ProjectRepository(pool)

    # =========================================================================
    # Company Instructions
    # =========================================================================

    async def get_company_instructions(
        self, company_id: str, current_user: CurrentUser
    ) -> Optional[CompanyInstructionsResponse]:
        """
        Get instructions for a company.

        Args:
            company_id: Company UUID
            current_user: Current authenticated user

        Returns:
            Company instructions response or None if not found

        Raises:
            HTTPException: On authorization errors
        """
        # Validate company access
        validate_company_access(current_user, company_id)

        try:
            result = await self.repo.get_company_instructions(company_id)

            if not result:
                return None

            return CompanyInstructionsResponse(
                id=result["id"],
                company_id=result["company_id"],
                ground_rules=result.get("ground_rules"),
                coding_standards=result.get("coding_standards"),
                communication_style=result.get("communication_style"),
                forbidden_actions=result.get("forbidden_actions"),
                custom_instructions=result.get("custom_instructions"),
                created_at=result["created_at"],
                updated_at=result["updated_at"],
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to get company instructions: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get company instructions: {str(e)}",
            )

    async def upsert_company_instructions(
        self, company_id: str, data: CompanyInstructionsCreate, current_user: CurrentUser
    ) -> CompanyInstructionsResponse:
        """
        Create or update company instructions.

        Args:
            company_id: Company UUID
            data: Instructions data
            current_user: Current authenticated user

        Returns:
            Created/updated company instructions response

        Raises:
            HTTPException: On authorization or database errors
        """
        # Validate company access
        validate_company_access(current_user, company_id)

        try:
            logger.info(
                f"Upserting company instructions for {company_id} by user {current_user.user_id}"
            )

            result = await self.repo.upsert_company_instructions(
                company_id=company_id,
                ground_rules=data.ground_rules,
                coding_standards=data.coding_standards,
                communication_style=data.communication_style,
                forbidden_actions=data.forbidden_actions,
                custom_instructions=data.custom_instructions,
            )

            return CompanyInstructionsResponse(
                id=result["id"],
                company_id=result["company_id"],
                ground_rules=result.get("ground_rules"),
                coding_standards=result.get("coding_standards"),
                communication_style=result.get("communication_style"),
                forbidden_actions=result.get("forbidden_actions"),
                custom_instructions=result.get("custom_instructions"),
                created_at=result["created_at"],
                updated_at=result["updated_at"],
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to upsert company instructions: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upsert company instructions: {str(e)}",
            )

    # =========================================================================
    # Project Instructions
    # =========================================================================

    async def get_project_instructions(
        self, project_id: str, current_user: CurrentUser
    ) -> Optional[ProjectInstructionsResponse]:
        """
        Get instructions for a project.

        Args:
            project_id: Project UUID
            current_user: Current authenticated user

        Returns:
            Project instructions response or None if not found

        Raises:
            HTTPException: On authorization errors or project not found
        """
        # Get company_id from project for authorization
        company_id = await self.project_repo.get_company_id(project_id)

        if not company_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Project {project_id} not found"
            )

        # Validate company access
        validate_company_access(current_user, company_id)

        try:
            result = await self.repo.get_project_instructions(project_id)

            if not result:
                return None

            return ProjectInstructionsResponse(
                id=result["id"],
                project_id=result["project_id"],
                description=result.get("description"),
                languages=result.get("languages"),
                frameworks=result.get("frameworks"),
                tools=result.get("tools"),
                architecture_notes=result.get("architecture_notes"),
                conventions=result.get("conventions"),
                custom_instructions=result.get("custom_instructions"),
                created_at=result["created_at"],
                updated_at=result["updated_at"],
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to get project instructions: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get project instructions: {str(e)}",
            )

    async def upsert_project_instructions(
        self, project_id: str, data: ProjectInstructionsCreate, current_user: CurrentUser
    ) -> ProjectInstructionsResponse:
        """
        Create or update project instructions.

        Args:
            project_id: Project UUID
            data: Instructions data
            current_user: Current authenticated user

        Returns:
            Created/updated project instructions response

        Raises:
            HTTPException: On authorization or database errors
        """
        # Get company_id from project for authorization
        company_id = await self.project_repo.get_company_id(project_id)

        if not company_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Project {project_id} not found"
            )

        # Validate company access
        validate_company_access(current_user, company_id)

        try:
            logger.info(
                f"Upserting project instructions for {project_id} by user {current_user.user_id}"
            )

            result = await self.repo.upsert_project_instructions(
                project_id=project_id,
                description=data.description,
                languages=data.languages,
                frameworks=data.frameworks,
                tools=data.tools,
                architecture_notes=data.architecture_notes,
                conventions=data.conventions,
                custom_instructions=data.custom_instructions,
            )

            return ProjectInstructionsResponse(
                id=result["id"],
                project_id=result["project_id"],
                description=result.get("description"),
                languages=result.get("languages"),
                frameworks=result.get("frameworks"),
                tools=result.get("tools"),
                architecture_notes=result.get("architecture_notes"),
                conventions=result.get("conventions"),
                custom_instructions=result.get("custom_instructions"),
                created_at=result["created_at"],
                updated_at=result["updated_at"],
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to upsert project instructions: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upsert project instructions: {str(e)}",
            )

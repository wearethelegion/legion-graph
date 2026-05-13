"""
Instructions Repository
Database operations for company and project instructions.
"""

from typing import Optional, Dict, Any, List
from uuid import UUID, uuid4
from api.repositories.base_repository import BaseRepository
from api.utils.text import sanitize_text
from loguru import logger


class InstructionsRepository(BaseRepository):
    """Repository for company and project instructions operations."""

    # =========================================================================
    # Company Instructions
    # =========================================================================

    async def get_company_instructions(self, company_id: str) -> Optional[Dict[str, Any]]:
        """
        Get instructions for a company.

        Args:
            company_id: Company UUID

        Returns:
            Company instructions record or None if not found
        """
        query = """
            SELECT id, company_id, ground_rules, coding_standards,
                   communication_style, forbidden_actions, custom_instructions,
                   created_at, updated_at
            FROM company_instructions
            WHERE company_id = $1
        """
        return await self.fetch_one(query, company_id)

    async def upsert_company_instructions(
        self,
        company_id: str,
        ground_rules: Optional[str] = None,
        coding_standards: Optional[str] = None,
        communication_style: Optional[str] = None,
        forbidden_actions: Optional[str] = None,
        custom_instructions: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create or update company instructions.

        Args:
            company_id: Company UUID
            ground_rules: General principles and values
            coding_standards: Code style guidelines
            communication_style: How agents should communicate
            forbidden_actions: Things agents should never do
            custom_instructions: Any other company-wide instructions

        Returns:
            Created/updated company instructions record
        """
        # Check if exists
        existing = await self.get_company_instructions(company_id)

        if existing:
            # Update existing
            query = """
                UPDATE company_instructions
                SET ground_rules = COALESCE($2, ground_rules),
                    coding_standards = COALESCE($3, coding_standards),
                    communication_style = COALESCE($4, communication_style),
                    forbidden_actions = COALESCE($5, forbidden_actions),
                    custom_instructions = COALESCE($6, custom_instructions),
                    updated_at = NOW()
                WHERE company_id = $1
                RETURNING id, company_id, ground_rules, coding_standards,
                          communication_style, forbidden_actions, custom_instructions,
                          created_at, updated_at
            """
            row = await self.fetch_one(
                query,
                company_id,
                sanitize_text(ground_rules),
                sanitize_text(coding_standards),
                sanitize_text(communication_style),
                sanitize_text(forbidden_actions),
                sanitize_text(custom_instructions),
            )
            logger.info(f"Updated company instructions for company {company_id}")
        else:
            # Create new
            instructions_id = str(uuid4())
            query = """
                INSERT INTO company_instructions (
                    id, company_id, ground_rules, coding_standards,
                    communication_style, forbidden_actions, custom_instructions,
                    created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
                RETURNING id, company_id, ground_rules, coding_standards,
                          communication_style, forbidden_actions, custom_instructions,
                          created_at, updated_at
            """
            row = await self.fetch_one(
                query,
                instructions_id,
                company_id,
                sanitize_text(ground_rules),
                sanitize_text(coding_standards),
                sanitize_text(communication_style),
                sanitize_text(forbidden_actions),
                sanitize_text(custom_instructions),
            )
            logger.info(f"Created company instructions for company {company_id}")

        return row

    async def delete_company_instructions(self, company_id: str) -> bool:
        """
        Delete company instructions.

        Args:
            company_id: Company UUID

        Returns:
            True if deleted, False otherwise
        """
        query = "DELETE FROM company_instructions WHERE company_id = $1"
        result = await self.execute(query, company_id)
        deleted = result.endswith("1")

        if deleted:
            logger.info(f"Deleted company instructions for company {company_id}")

        return deleted

    # =========================================================================
    # Project Instructions
    # =========================================================================

    async def get_project_instructions(self, project_id: str) -> Optional[Dict[str, Any]]:
        """
        Get instructions for a project.

        Args:
            project_id: Project UUID

        Returns:
            Project instructions record or None if not found
        """
        query = """
            SELECT id, project_id, description, languages, frameworks, tools,
                   architecture_notes, conventions, custom_instructions,
                   created_at, updated_at
            FROM project_instructions
            WHERE project_id = $1
        """
        return await self.fetch_one(query, project_id)

    async def upsert_project_instructions(
        self,
        project_id: str,
        description: Optional[str] = None,
        languages: Optional[List[str]] = None,
        frameworks: Optional[List[str]] = None,
        tools: Optional[List[str]] = None,
        architecture_notes: Optional[str] = None,
        conventions: Optional[str] = None,
        custom_instructions: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create or update project instructions.

        Args:
            project_id: Project UUID
            description: What the project is
            languages: Programming languages used
            frameworks: Frameworks used
            tools: Tools and infrastructure
            architecture_notes: Key architectural decisions
            conventions: Project-specific patterns
            custom_instructions: Any other project-specific instructions

        Returns:
            Created/updated project instructions record
        """
        # Check if exists
        existing = await self.get_project_instructions(project_id)

        if existing:
            # Update existing
            query = """
                UPDATE project_instructions
                SET description = COALESCE($2, description),
                    languages = COALESCE($3, languages),
                    frameworks = COALESCE($4, frameworks),
                    tools = COALESCE($5, tools),
                    architecture_notes = COALESCE($6, architecture_notes),
                    conventions = COALESCE($7, conventions),
                    custom_instructions = COALESCE($8, custom_instructions),
                    updated_at = NOW()
                WHERE project_id = $1
                RETURNING id, project_id, description, languages, frameworks, tools,
                          architecture_notes, conventions, custom_instructions,
                          created_at, updated_at
            """
            row = await self.fetch_one(
                query,
                project_id,
                sanitize_text(description),
                languages,
                frameworks,
                tools,
                sanitize_text(architecture_notes),
                sanitize_text(conventions),
                sanitize_text(custom_instructions),
            )
            logger.info(f"Updated project instructions for project {project_id}")
        else:
            # Create new
            instructions_id = str(uuid4())
            query = """
                INSERT INTO project_instructions (
                    id, project_id, description, languages, frameworks, tools,
                    architecture_notes, conventions, custom_instructions,
                    created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
                RETURNING id, project_id, description, languages, frameworks, tools,
                          architecture_notes, conventions, custom_instructions,
                          created_at, updated_at
            """
            row = await self.fetch_one(
                query,
                instructions_id,
                project_id,
                sanitize_text(description),
                languages,
                frameworks,
                tools,
                sanitize_text(architecture_notes),
                sanitize_text(conventions),
                sanitize_text(custom_instructions),
            )
            logger.info(f"Created project instructions for project {project_id}")

        return row

    async def delete_project_instructions(self, project_id: str) -> bool:
        """
        Delete project instructions.

        Args:
            project_id: Project UUID

        Returns:
            True if deleted, False otherwise
        """
        query = "DELETE FROM project_instructions WHERE project_id = $1"
        result = await self.execute(query, project_id)
        deleted = result.endswith("1")

        if deleted:
            logger.info(f"Deleted project instructions for project {project_id}")

        return deleted

    # =========================================================================
    # Combined Context (for delegation)
    # =========================================================================

    async def get_project_with_company(self, project_id: str) -> Optional[Dict[str, Any]]:
        """
        Get project with its company_id (needed to fetch company instructions).

        Args:
            project_id: Project UUID

        Returns:
            Project record with company_id or None
        """
        query = """
            SELECT id, company_id, name, description
            FROM projects
            WHERE id = $1
        """
        return await self.fetch_one(query, project_id)

    async def get_context_instructions(self, project_id: str) -> Dict[str, Any]:
        """
        Get both company and project instructions for a project.
        This is the main method used by delegation.

        Args:
            project_id: Project UUID

        Returns:
            Dict with 'company' and 'project' instructions (either can be None)
        """
        # Get project to find company_id
        project = await self.get_project_with_company(project_id)

        result = {"company": None, "project": None, "company_id": None, "project_id": project_id}

        if project:
            result["company_id"] = project["company_id"]

            # Fetch company instructions
            company_instructions = await self.get_company_instructions(project["company_id"])
            if company_instructions:
                result["company"] = company_instructions

            # Fetch project instructions
            project_instructions = await self.get_project_instructions(project_id)
            if project_instructions:
                result["project"] = project_instructions

        return result

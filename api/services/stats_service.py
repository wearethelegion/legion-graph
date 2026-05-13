"""
Statistics Service
Business logic for company statistics aggregation.
Queries PostgreSQL for table counts and Neo4j for node counts.
"""

import asyncio
from fastapi import HTTPException, status
from loguru import logger

from api.repositories import CompanyRepository, ProjectRepository, Neo4jRepository
from api.auth import CurrentUser
from api.models.stats import CompanyStatsResponse


class StatsService:
    """Service for aggregating company statistics from PostgreSQL and Neo4j."""

    def __init__(
        self,
        company_repository: CompanyRepository,
        project_repository: ProjectRepository,
        neo4j_repository: Neo4jRepository
    ):
        self.company_repo = company_repository
        self.project_repo = project_repository
        self.neo4j_repo = neo4j_repository

    async def get_company_stats(
        self,
        company_id: str,
        current_user: CurrentUser
    ) -> CompanyStatsResponse:
        """
        Get aggregate statistics for a company.

        PostgreSQL counts:
        - projects_count, agents_count, engagements_count, tasks_count, delegations_count

        Neo4j counts:
        - knowledge_count (Knowledge nodes)
        - code_count (File nodes)
        - expertise_count (Expertise nodes)

        Args:
            company_id: Company UUID
            current_user: Authenticated user

        Returns:
            CompanyStatsResponse with all record counts

        Raises:
            HTTPException: 403 if access denied, 404 if company not found
        """
        # 1. Verify company exists
        company = await self.company_repo.get_by_id(company_id)
        if not company:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Company {company_id} not found"
            )

        # 2. Authorization - check user has access
        if not current_user.is_superuser:
            has_access = await self.company_repo.user_has_access(
                current_user.user_id,
                company_id
            )
            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied to this company"
                )

        # 3. Run all counts in parallel
        (
            projects_count,
            agents_count,
            engagements_count,
            tasks_count,
            delegations_count,
            knowledge_count,
            code_count,
            expertise_count
        ) = await asyncio.gather(
            self._count_projects(company_id),
            self._count_agents(company_id),
            self._count_engagements(company_id),
            self._count_tasks(company_id),
            self._count_delegations(company_id),
            self._count_knowledge_nodes(company_id),
            self._count_code_nodes(company_id),
            self._count_expertise_nodes(company_id)
        )

        logger.info(
            f"Stats for company {company_id}: "
            f"projects={projects_count}, agents={agents_count}, "
            f"engagements={engagements_count}, tasks={tasks_count}, "
            f"delegations={delegations_count}, knowledge={knowledge_count}, "
            f"code={code_count}, expertise={expertise_count}"
        )

        return CompanyStatsResponse(
            company_id=company_id,
            projects_count=projects_count,
            agents_count=agents_count,
            engagements_count=engagements_count,
            tasks_count=tasks_count,
            delegations_count=delegations_count,
            knowledge_count=knowledge_count,
            code_count=code_count,
            expertise_count=expertise_count
        )

    # ─── PostgreSQL Count Methods ───────────────────────────────

    async def _count_projects(self, company_id: str) -> int:
        """Count projects for company."""
        query = "SELECT COUNT(*) FROM projects WHERE company_id = $1"
        count = await self.company_repo.fetch_val(query, company_id)
        return count or 0

    async def _count_agents(self, company_id: str) -> int:
        """Count agents for company."""
        query = "SELECT COUNT(*) FROM agents WHERE company_id = $1"
        count = await self.company_repo.fetch_val(query, company_id)
        return count or 0

    async def _count_engagements(self, company_id: str) -> int:
        """Count engagements for company."""
        query = "SELECT COUNT(*) FROM engagements WHERE company_id = $1"
        count = await self.company_repo.fetch_val(query, company_id)
        return count or 0

    async def _count_tasks(self, company_id: str) -> int:
        """Count tasks for company."""
        query = "SELECT COUNT(*) FROM tasks WHERE company_id = $1"
        count = await self.company_repo.fetch_val(query, company_id)
        return count or 0

    async def _count_delegations(self, company_id: str) -> int:
        """Count delegations for company."""
        query = "SELECT COUNT(*) FROM delegations WHERE company_id = $1"
        count = await self.company_repo.fetch_val(query, company_id)
        return count or 0

    # ─── Neo4j Count Methods ────────────────────────────────────

    async def _count_knowledge_nodes(self, company_id: str) -> int:
        """Count Knowledge nodes for company in Neo4j."""
        try:
            query = "MATCH (k:Knowledge {company_id: $company_id}) RETURN count(k) AS count"
            result = await self.neo4j_repo.execute_query(query, {"company_id": company_id})
            if result and len(result) > 0:
                return result[0].get("count", 0)
            return 0
        except Exception as e:
            logger.warning("Failed to count Knowledge nodes: {}", e)
            return 0

    async def _count_code_nodes(self, company_id: str) -> int:
        """Count Code/File nodes for company in Neo4j."""
        try:
            query = "MATCH (f:File {company_id: $company_id}) RETURN count(f) AS count"
            result = await self.neo4j_repo.execute_query(query, {"company_id": company_id})
            if result and len(result) > 0:
                return result[0].get("count", 0)
            return 0
        except Exception as e:
            logger.warning("Failed to count Code/File nodes: {}", e)
            return 0

    async def _count_expertise_nodes(self, company_id: str) -> int:
        """Count Expertise nodes for company in Neo4j."""
        try:
            query = "MATCH (e:Expertise {company_id: $company_id}) RETURN count(e) AS count"
            result = await self.neo4j_repo.execute_query(query, {"company_id": company_id})
            if result and len(result) > 0:
                return result[0].get("count", 0)
            return 0
        except Exception as e:
            logger.warning("Failed to count Expertise nodes: {}", e)
            return 0

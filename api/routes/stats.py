"""
Statistics Routes
Endpoint for company-level statistics (flat record counts).

Endpoint:
- GET /api/v1/companies/{company_id}/stats
"""

from fastapi import APIRouter, Depends, Request
from api.auth import get_current_user, CurrentUser
from api.database import get_db_pool
from api.repositories import CompanyRepository, ProjectRepository, Neo4jRepository
from api.services.stats_service import StatsService
from api.models.stats import CompanyStatsResponse
import asyncpg


router = APIRouter(tags=["statistics"])


async def get_stats_service(
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool)
) -> StatsService:
    """Dependency to get stats service with repositories."""
    company_repository = CompanyRepository(pool)
    project_repository = ProjectRepository(pool)
    neo4j_repository = request.app.state.neo4j_repository

    return StatsService(
        company_repository=company_repository,
        project_repository=project_repository,
        neo4j_repository=neo4j_repository
    )


@router.get(
    "/api/v1/companies/{company_id}/stats",
    response_model=CompanyStatsResponse,
    summary="Get company statistics"
)
async def get_company_stats(
    company_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    service: StatsService = Depends(get_stats_service)
) -> CompanyStatsResponse:
    """
    Get aggregate statistics for a company.

    **PostgreSQL counts:**
    - projects_count, agents_count, engagements_count, tasks_count, delegations_count

    **Neo4j node counts:**
    - knowledge_count (Knowledge nodes)
    - code_count (File nodes)
    - expertise_count (Expertise nodes)

    **Authentication Required**
    """
    return await service.get_company_stats(company_id, current_user)

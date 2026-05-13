"""
Company Service
Business logic for company operations.
"""

from typing import List, Optional, Dict, Any
from pathlib import Path
import shutil
from fastapi import HTTPException, status
from api.repositories import CompanyRepository, Neo4jRepository, QdrantRepository, ProjectRepository
from api.models import CompanyCreate, CompanyResponse, CompanyListResponse
from api.auth import CurrentUser
from loguru import logger


# Default repo storage path (same as code_preprocessor config)
DEFAULT_REPO_STORAGE_ROOT = Path("rag_storage/repos")


class CompanyService:
    """Service for company business logic."""

    def __init__(
        self,
        repository: CompanyRepository,
        neo4j_repository: Neo4jRepository,
        qdrant_repository: QdrantRepository,
        project_repository: Optional[ProjectRepository] = None,
        repo_storage_root: Optional[Path] = None,
    ):
        self.repository = repository
        self.neo4j_repository = neo4j_repository
        self.qdrant_repository = qdrant_repository
        self.project_repository = project_repository
        self.repo_storage_root = repo_storage_root or DEFAULT_REPO_STORAGE_ROOT

    async def create_company(
        self, data: CompanyCreate, current_user: CurrentUser
    ) -> CompanyResponse:
        """
        Create a new company.

        Args:
            data: Company creation data
            current_user: Current authenticated user

        Returns:
            Created company response

        Raises:
            HTTPException: On validation or database errors
        """
        company = None
        try:
            logger.info(f"Creating company '{data.name}' by user {current_user.user_id}")

            # PLAN LIMIT CHECK
            if not current_user.is_superuser:
                company_count = await self.repository.count_by_user(current_user.user_id)
                if company_count >= 1:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="Plan limit reached: maximum 1 company per user",
                    )

            # Create company in database
            company = await self.repository.create(name=data.name, description=data.description)
            logger.debug(f"Created company in PostgreSQL: {company['id']}")

            # Add current user as owner of the company
            await self.repository.add_user_to_company(
                company_id=company["id"], user_id=current_user.user_id, role="owner"
            )
            logger.debug(f"Added user {current_user.user_id} as owner to company {company['id']}")

            # Create company node in Neo4j (single 'kgrag' database)
            try:
                await self.neo4j_repository.create_company_node(
                    company_id=company["id"],
                    name=company["name"],
                    description=company.get("description"),
                    is_active=True,
                )
                logger.debug(f"Created company node in Neo4j: {company['id']}")
            except Exception as neo4j_node_error:
                # Rollback: delete company from PostgreSQL
                logger.error(
                    f"Neo4j node creation failed for company {company['id']}: {neo4j_node_error}"
                )
                await self.repository.delete(company["id"])
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to create company in Neo4j: {str(neo4j_node_error)}",
                )

            # Create Qdrant collection for company-level knowledge
            try:
                collection_name = f"company_{company['id']}"
                await self.qdrant_repository.create_collection(collection_name)
                logger.debug(f"Created Qdrant collection: {collection_name}")
            except Exception as qdrant_error:
                # Rollback: delete from PostgreSQL and Neo4j
                logger.error(
                    f"Qdrant collection creation failed for company {company['id']}: {qdrant_error}"
                )
                await self.repository.delete(company["id"])
                await self.neo4j_repository.delete_company_node(company["id"])
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to create Qdrant collection: {str(qdrant_error)}",
                )

            logger.info(
                f"Successfully created company {company['id']} ('{company['name']}') by user {current_user.user_id}"
            )

            return CompanyResponse(
                id=company["id"],
                name=company["name"],
                description=company["description"],
                created_at=company["created_at"].isoformat(),
                updated_at=company["updated_at"].isoformat(),
            )

        except HTTPException:
            raise
        except Exception as e:
            # Fix: Don't use f-string with exc_info=True - loguru handles formatting
            logger.error("Unexpected error creating company", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create company: {str(e)}",
            )

    async def get_company(self, company_id: str, current_user: CurrentUser) -> CompanyResponse:
        """
        Get company by ID.

        Args:
            company_id: Company UUID
            current_user: Current authenticated user

        Returns:
            Company response

        Raises:
            HTTPException: If company not found or access denied
        """
        # Check if company exists
        company = await self.repository.get_by_id(company_id)

        if not company:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Company {company_id} not found"
            )

        # Check access (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.repository.user_has_access(current_user.user_id, company_id)

            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this company"
                )

        return CompanyResponse(
            id=company["id"],
            name=company["name"],
            description=company["description"],
            cognee_enabled=bool(company.get("cognee_enabled", False)),
            created_at=company["created_at"].isoformat(),
            updated_at=company["updated_at"].isoformat(),
        )

    async def list_companies(self, current_user: CurrentUser) -> CompanyListResponse:
        """
        List companies (all for super admin, user's companies otherwise).

        Args:
            current_user: Current authenticated user

        Returns:
            List of companies
        """
        try:
            logger.debug(
                f"Listing companies for user {current_user.user_id} (superuser={current_user.is_superuser})"
            )

            if current_user.is_superuser:
                # Super admin sees all companies
                companies = await self.repository.get_all()
                logger.debug(f"Retrieved {len(companies)} companies (all)")
            else:
                # Regular user sees only their companies
                companies = await self.repository.get_by_user(current_user.user_id)
                logger.debug(
                    f"Retrieved {len(companies)} companies for user {current_user.user_id}"
                )

            company_responses = [
                CompanyResponse(
                    id=c["id"],
                    name=c["name"],
                    description=c["description"],
                    cognee_enabled=bool(c.get("cognee_enabled", False)),
                    created_at=c["created_at"].isoformat(),
                    updated_at=c["updated_at"].isoformat(),
                )
                for c in companies
            ]

            return CompanyListResponse(companies=company_responses, total=len(company_responses))

        except Exception as e:
            # Fix: Don't use f-string with exc_info=True - loguru handles formatting
            logger.error("Failed to list companies", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list companies: {str(e)}",
            )

    async def delete_company(self, company_id: str, current_user: CurrentUser) -> Dict[str, Any]:
        """
        Delete company and ALL associated data from all databases.

        Comprehensive cleanup:
        1. PostgreSQL - Company and all related data (CASCADE)
        2. Neo4j - ALL nodes with company_id (not just Company node)
        3. Qdrant - Company collection (deletes all vectors)
        4. Local filesystem - Git repository clones

        Args:
            company_id: Company UUID
            current_user: Current authenticated user

        Returns:
            Deletion statistics

        Raises:
            HTTPException: If company not found, access denied, or deletion fails
        """
        try:
            logger.info(f"Deleting company {company_id} by user {current_user.user_id}")

            # 1. Verify company exists
            company = await self.repository.get_by_id(company_id)
            if not company:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail=f"Company {company_id} not found"
                )

            # 2. Check permissions - Super admin only
            if not current_user.is_superuser:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only super admin can delete companies",
                )

            company_name = company["name"]
            logger.warning(
                f"User {current_user.user_id} initiating FULL deletion of company "
                f"{company_id} ({company_name})"
            )

            deletion_stats = {
                "company_id": company_id,
                "company_name": company_name,
                "postgresql": {},
                "neo4j": {},
                "qdrant": {},
                "local_repos": {},
            }

            # 3. Get all projects for this company (for local repo cleanup)
            project_repos = []
            if self.project_repository:
                try:
                    projects = await self.project_repository.get_by_company(company_id)
                    # Get repository URLs/names for each project to cleanup local clones
                    for project in projects:
                        # Query repositories for this project
                        repos = await self._get_project_repositories(project["id"])
                        project_repos.extend(repos)
                except Exception as e:
                    logger.warning("Could not fetch project repositories: {}", e)

            # 4. Delete from Qdrant (entire company collection)
            try:
                collection_name = f"company_{company_id}"
                await self.qdrant_repository.delete_collection(collection_name)
                deletion_stats["qdrant"] = {"collection": collection_name, "deleted": True}
                logger.info(f"Qdrant: Deleted collection {collection_name}")
            except Exception as qdrant_error:
                logger.warning("Qdrant deletion failed (continuing): {}", qdrant_error)
                deletion_stats["qdrant"] = {"error": str(qdrant_error)}

            # 5. Delete from Neo4j (ALL nodes with company_id)
            try:
                neo4j_counts = await self._delete_all_company_nodes(company_id)
                deletion_stats["neo4j"] = {
                    "nodes_by_label": neo4j_counts,
                    "total_nodes": sum(neo4j_counts.values()) if neo4j_counts else 0,
                }
                logger.info(f"Neo4j: Deleted {deletion_stats['neo4j']['total_nodes']} nodes")
            except Exception as neo4j_error:
                logger.warning("Neo4j deletion failed (continuing): {}", neo4j_error)
                deletion_stats["neo4j"] = {"error": str(neo4j_error)}

            # 6. Delete local git repository clones
            repos_deleted = 0
            for repo_name in project_repos:
                try:
                    deleted = self._delete_local_repository(repo_name)
                    if deleted:
                        repos_deleted += 1
                except Exception as repo_error:
                    logger.warning("Failed to delete local repo {}: {}", repo_name, repo_error)
            deletion_stats["local_repos"] = {"deleted": repos_deleted, "total": len(project_repos)}

            # 7. Delete from PostgreSQL (CASCADE to all related tables)
            try:
                await self.repository.delete(company_id)
                deletion_stats["postgresql"] = {
                    "deleted": True,
                    "cascade_tables": [
                        "projects",
                        "repositories",
                        "branches",
                        "engagements",
                        "tasks",
                        "task_artifacts",
                        "project_instructions",
                        "company_users",
                    ],
                }
                logger.info(f"PostgreSQL: Deleted company {company_id} (cascade applied)")
            except Exception as postgres_error:
                logger.error("PostgreSQL deletion failed: {}", postgres_error)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to delete company from database: {str(postgres_error)}",
                )

            logger.warning(
                f"Company {company_id} ({company_name}) FULLY deleted by {current_user.user_id}. "
                f"Stats: {deletion_stats}"
            )

            return deletion_stats

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Unexpected error deleting company {}", company_id, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete company: {str(e)}",
            )

    async def _delete_all_company_nodes(self, company_id: str) -> Dict[str, int]:
        """Delete ALL Neo4j nodes with company_id property."""
        # First count nodes by label
        count_query = """
        MATCH (n)
        WHERE n.company_id = $company_id
        RETURN labels(n)[0] AS label, count(n) AS count
        """

        counts = {}
        async with self.neo4j_repository.driver.session(
            database=self.neo4j_repository.database
        ) as session:
            result = await session.run(count_query, company_id=company_id)
            counts = {record["label"]: record["count"] async for record in result}

            if counts:
                # Delete all nodes with company_id
                delete_query = """
                MATCH (n)
                WHERE n.company_id = $company_id
                DETACH DELETE n
                """
                await session.run(delete_query, company_id=company_id)

        return counts

    async def _get_project_repositories(self, project_id: str) -> List[str]:
        """Get repository names for a project from PostgreSQL."""
        if not self.project_repository:
            return []

        query = "SELECT name, url FROM repositories WHERE project_id = $1"
        try:
            async with self.project_repository.pool.acquire() as conn:
                rows = await conn.fetch(query, project_id)
                # Return repository names (used for local path derivation)
                return [row["name"] for row in rows if row["name"]]
        except Exception as e:
            logger.warning("Failed to get repositories for project {}: {}", project_id, e)
            return []

    def _delete_local_repository(self, repo_name: str) -> bool:
        """Delete local git clone for a repository."""
        # Sanitize repo name same way as GitRepositoryManager
        sanitized_name = repo_name.replace("/", "__")
        repo_path = self.repo_storage_root / sanitized_name

        if not repo_path.exists():
            logger.debug(f"Local repo path doesn't exist: {repo_path}")
            return False

        try:
            shutil.rmtree(repo_path)
            logger.info(f"Deleted local repository: {repo_path}")
            return True
        except Exception as e:
            logger.error("Failed to delete local repo {}: {}", repo_path, e)
            raise

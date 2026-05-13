"""
Project Service
Business logic for project operations.
"""

from typing import List, Dict, Any, Optional
from pathlib import Path
import shutil
import os
from urllib.parse import quote_plus
from fastapi import HTTPException, status
from api.repositories import (
    ProjectRepository,
    CompanyRepository,
    Neo4jRepository,
    QdrantRepository,
    RepositoryRepository,
    InstructionsRepository,
)
from api.models import (
    ProjectCreate,
    ProjectUpdate,
    ProjectTransferRequest,
    ProjectTransferResponse,
    ProjectResponse,
    ProjectListResponse,
    WebhookSecretResponse,
    ProjectInstructionsEmbedded,
)
from api.auth import CurrentUser
from loguru import logger


# Default repo storage path (same as code_preprocessor config)
DEFAULT_REPO_STORAGE_ROOT = Path("rag_storage/repos")

_mongo_client = None
_ingestions_collection = None


class ProjectService:
    """Service for project business logic."""

    @staticmethod
    def _build_project_response(
        project: Dict[str, Any], instructions: Optional[Dict[str, Any]] = None
    ) -> ProjectResponse:
        """
        Build a ProjectResponse from a project record.

        Args:
            project: Project record from database
            instructions: Optional project instructions record

        Returns:
            ProjectResponse with embedded instructions
        """
        embedded_instructions = None
        if instructions:
            embedded_instructions = ProjectInstructionsEmbedded(
                id=instructions["id"],
                description=instructions.get("description"),
                languages=instructions.get("languages"),
                frameworks=instructions.get("frameworks"),
                tools=instructions.get("tools"),
                architecture_notes=instructions.get("architecture_notes"),
                conventions=instructions.get("conventions"),
                custom_instructions=instructions.get("custom_instructions"),
                created_at=instructions["created_at"],
                updated_at=instructions["updated_at"],
            )

        return ProjectResponse(
            id=project["id"],
            company_id=project["company_id"],
            name=project["name"],
            description=project["description"],
            webhook_url=project.get("webhook_url"),
            github_webhook_secret=project.get("github_webhook_secret"),
            github_token_set=bool(project.get("github_token")),
            created_at=project["created_at"].isoformat(),
            updated_at=project["updated_at"].isoformat(),
            instructions=embedded_instructions,
        )

    def __init__(
        self,
        project_repository: ProjectRepository,
        company_repository: CompanyRepository,
        neo4j_repository: Neo4jRepository,
        qdrant_repository: QdrantRepository,
        repository_repository: Optional[RepositoryRepository] = None,
        instructions_repository: Optional[InstructionsRepository] = None,
        repo_storage_root: Optional[Path] = None,
    ):
        self.project_repository = project_repository
        self.company_repository = company_repository
        self.neo4j_repository = neo4j_repository
        self.qdrant_repository = qdrant_repository
        self.repository_repository = repository_repository
        self.instructions_repository = instructions_repository
        self.repo_storage_root = repo_storage_root or DEFAULT_REPO_STORAGE_ROOT

    async def create_project(
        self, company_id: str, data: ProjectCreate, current_user: CurrentUser
    ) -> ProjectResponse:
        """
        Create a new project under a company.

        Args:
            company_id: Company UUID
            data: Project creation data
            current_user: Current authenticated user

        Returns:
            Created project response

        Raises:
            HTTPException: On validation or database errors
        """
        # Check if company exists
        if not await self.company_repository.exists(company_id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Company {company_id} not found"
            )

        # Check access (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.company_repository.user_has_access(
                current_user.user_id, company_id
            )

            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this company"
                )

        # PLAN LIMIT CHECK
        if not current_user.is_superuser:
            project_count = await self.project_repository.count_by_company(company_id)
            if project_count >= 2:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Plan limit reached: maximum 2 projects per company",
                )

        project = None
        try:
            # Create project in database
            project = await self.project_repository.create(
                company_id=company_id, name=data.name, description=data.description
            )

            # Create project node in Neo4j (single 'kgrag' database)
            try:
                await self.neo4j_repository.create_project_node(
                    project_id=project["id"],
                    company_id=company_id,
                    name=project["name"],
                    description=project.get("description"),
                )
            except Exception as neo4j_error:
                # Rollback: delete project from PostgreSQL
                await self.project_repository.delete(project["id"])
                logger.error(
                    "Neo4j creation failed, rolled back project %s",
                    project["id"],
                    exc_info=neo4j_error,
                )
                raise

            # Projects no longer create Qdrant collections
            # All project data is stored in company-level collection: company_{company_id}
            # with project_id as metadata for filtering
            # This simplifies management and improves query efficiency

            # Create instructions if provided
            instructions = None
            if data.instructions and self.instructions_repository:
                instructions = await self.instructions_repository.upsert_project_instructions(
                    project_id=project["id"],
                    description=data.instructions.description,
                    languages=data.instructions.languages,
                    frameworks=data.instructions.frameworks,
                    tools=data.instructions.tools,
                    architecture_notes=data.instructions.architecture_notes,
                    conventions=data.instructions.conventions,
                    custom_instructions=data.instructions.custom_instructions,
                )
                logger.info(f"Created instructions for project {project['id']}")

            logger.info(
                f"User {current_user.user_id} created project {project['id']} "
                f"under company {company_id}"
            )

            return self._build_project_response(project, instructions)

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to create project: {}", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create project: {str(e)}",
            )

    async def get_project(self, project_id: str, current_user: CurrentUser) -> ProjectResponse:
        """
        Get project by ID with embedded instructions.

        Args:
            project_id: Project UUID
            current_user: Current authenticated user

        Returns:
            Project response with instructions

        Raises:
            HTTPException: If project not found or access denied
        """
        # Check if project exists
        project = await self.project_repository.get_by_id(project_id)

        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Project {project_id} not found"
            )

        # Check access via company (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.company_repository.user_has_access(
                current_user.user_id, project["company_id"]
            )

            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this project"
                )

        # Fetch instructions if repository is available
        instructions = None
        if self.instructions_repository:
            instructions = await self.instructions_repository.get_project_instructions(project_id)

        return self._build_project_response(project, instructions)

    async def list_projects(
        self, company_id: str, current_user: CurrentUser
    ) -> ProjectListResponse:
        """
        List all projects for a company.

        Args:
            company_id: Company UUID
            current_user: Current authenticated user

        Returns:
            List of projects

        Raises:
            HTTPException: If company not found or access denied
        """
        # Check if company exists
        if not await self.company_repository.exists(company_id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Company {company_id} not found"
            )

        # Check access (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.company_repository.user_has_access(
                current_user.user_id, company_id
            )

            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this company"
                )

        try:
            projects = await self.project_repository.get_by_company(company_id)

            project_responses = [self._build_project_response(p) for p in projects]

            return ProjectListResponse(projects=project_responses, total=len(project_responses))

        except Exception as e:
            logger.error("Failed to list projects: {}", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list projects: {str(e)}",
            )

    async def delete_project(self, project_id: str, current_user: CurrentUser) -> Dict[str, Any]:
        """
        Delete a project and ALL associated data across all databases.

        Deletes from:
        - PostgreSQL: projects table (cascades to repositories, branches,
          engagements, tasks, task_artifacts, project_instructions)
        - Neo4j: All nodes with project_id (Project, Repository, Branch,
          Document, Code, Entity, Knowledge, Expertise, Fact)
        - Qdrant: All vectors with project_id metadata in company collection
        - Local filesystem: Git repository clones in repo_storage_root

        Args:
            project_id: Project UUID to delete
            current_user: Current authenticated user

        Returns:
            Dict with deletion statistics

        Raises:
            HTTPException: If project not found, access denied, or deletion fails
        """
        # 1. Verify project exists and get company_id
        project = await self.project_repository.get_by_id(project_id)
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Project {project_id} not found"
            )

        company_id = project["company_id"]
        project_name = project["name"]

        # 2. Check access (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.company_repository.user_has_access(
                current_user.user_id, company_id
            )
            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this project"
                )

        logger.warning(
            f"User {current_user.user_id} initiating deletion of project "
            f"{project_id} ({project_name}) in company {company_id}"
        )

        # 2.5. Get repository names BEFORE deleting from PostgreSQL (needed for local cleanup)
        repo_names: List[str] = []
        if self.repository_repository:
            try:
                repos = await self.repository_repository.get_by_project(project_id)
                repo_names = [r["name"] for r in repos if r.get("name")]
                logger.info(
                    f"Found {len(repo_names)} repositories to clean up for project {project_id}"
                )
            except Exception as e:
                logger.warning("Failed to get repositories for local cleanup: {}", e)

        deletion_stats = {
            "project_id": project_id,
            "project_name": project_name,
            "company_id": str(company_id),
            "postgresql": {},
            "neo4j": {},
            "qdrant": {},
            "local_repos": {},
        }

        try:
            # 3. Delete from Qdrant first (vectors with project_id metadata)
            try:
                collection_name = f"company_{company_id}"
                qdrant_deleted = await self.qdrant_repository.delete_points_by_filter(
                    collection_name=collection_name,
                    filter_field="project_id",
                    filter_value=project_id,
                )
                deletion_stats["qdrant"] = {
                    "collection": collection_name,
                    "points_deleted": qdrant_deleted,
                }
                logger.info(f"Qdrant: Deleted {qdrant_deleted} points for project {project_id}")
            except Exception as qdrant_error:
                logger.error("Qdrant deletion failed (continuing): {}", qdrant_error)
                deletion_stats["qdrant"] = {"error": str(qdrant_error)}

            # 4. Delete from Neo4j (all nodes with project_id)
            try:
                neo4j_counts = await self.neo4j_repository.delete_project_node(
                    project_id=project_id, company_id=str(company_id)
                )
                deletion_stats["neo4j"] = {
                    "nodes_by_label": neo4j_counts,
                    "total_nodes": sum(neo4j_counts.values()) if neo4j_counts else 0,
                }
                logger.info(
                    f"Neo4j: Deleted {deletion_stats['neo4j']['total_nodes']} nodes for project {project_id}"
                )
            except Exception as neo4j_error:
                logger.error("Neo4j deletion failed (continuing): {}", neo4j_error)
                deletion_stats["neo4j"] = {"error": str(neo4j_error)}

            # 5. Delete local git repository clones
            deleted_repos = []
            failed_repos = []
            for repo_name in repo_names:
                success = self._delete_local_repository(repo_name)
                if success:
                    deleted_repos.append(repo_name)
                else:
                    failed_repos.append(repo_name)

            deletion_stats["local_repos"] = {
                "deleted": deleted_repos,
                "failed": failed_repos,
                "total_deleted": len(deleted_repos),
            }
            if deleted_repos:
                logger.info(
                    f"Local repos: Deleted {len(deleted_repos)} repositories for project {project_id}"
                )

            # 6. Delete from PostgreSQL (CASCADE handles related tables)
            await self.project_repository.delete(project_id)
            deletion_stats["postgresql"] = {
                "project_deleted": True,
                "cascade_tables": [
                    "repositories",
                    "branches",
                    "engagements",
                    "tasks",
                    "task_artifacts",
                    "project_instructions",
                ],
            }
            logger.info(f"PostgreSQL: Deleted project {project_id} (cascade applied)")

            logger.warning(
                f"Project {project_id} ({project_name}) fully deleted by user {current_user.user_id}. "
                f"Stats: {deletion_stats}"
            )

            return deletion_stats

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to delete project {}: {}", project_id, e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete project: {str(e)}",
            )

    def _delete_local_repository(self, repo_name: str) -> bool:
        """
        Delete a local git repository clone.

        Args:
            repo_name: Repository name (e.g., "org/repo")

        Returns:
            True if deleted successfully, False otherwise
        """
        # Sanitize repo name (same as GitRepositoryManager)
        sanitized_name = repo_name.replace("/", "__")
        repo_path = self.repo_storage_root / sanitized_name

        if not repo_path.exists():
            logger.debug(f"Local repo not found (already deleted?): {repo_path}")
            return True  # Not an error, just doesn't exist

        try:
            shutil.rmtree(repo_path)
            logger.info(f"Deleted local repository: {repo_path}")
            return True
        except Exception as e:
            logger.error("Failed to delete local repository {}: {}", repo_path, e)
            return False

    @staticmethod
    def _get_ingestions_collection():
        """Get or create MongoDB ingestions collection handle."""
        global _mongo_client, _ingestions_collection

        if _ingestions_collection is None:
            from pymongo import MongoClient

            host = os.environ.get("MONGODB_HOST", "localhost")
            port = os.environ.get("MONGODB_PORT", "27017")
            username = os.environ.get("MONGODB_USERNAME", "")
            password = os.environ.get("MONGODB_PASSWORD", "")
            database = os.environ.get("MONGODB_DATABASE", "code_intel")
            auth_db = os.environ.get("MONGODB_AUTH_DATABASE", "admin")

            if username and password:
                uri = f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{auth_db}"
            else:
                uri = f"mongodb://{host}:{port}/{auth_db}"

            _mongo_client = MongoClient(uri)
            _ingestions_collection = _mongo_client[database]["ingestions"]

        return _ingestions_collection

    async def _migrate_mongodb_project_scope(
        self,
        project_id: str,
        source_company_id: str,
        target_company_id: str,
    ) -> Dict[str, Any]:
        """Move MongoDB ingestion documents for a project to target company."""
        if source_company_id == target_company_id:
            return {"matched": 0, "modified": 0}

        collection = self._get_ingestions_collection()
        result = collection.update_many(
            {
                "project_id": project_id,
                "$or": [
                    {"company_id": source_company_id},
                    {"company_id": {"$exists": False}},
                    {"company_id": None},
                ],
            },
            {"$set": {"company_id": target_company_id}},
        )

        return {"matched": result.matched_count, "modified": result.modified_count}

    async def transfer_project_company(
        self,
        project_id: str,
        data: ProjectTransferRequest,
        current_user: CurrentUser,
    ) -> ProjectTransferResponse:
        """Transfer a project to another company across all storage layers."""
        project = await self.project_repository.get_by_id(project_id)
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Project {project_id} not found",
            )

        source_company_id = project["company_id"]
        target_company_id = data.target_company_id

        if not await self.company_repository.exists(target_company_id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Target company {target_company_id} not found",
            )

        if not current_user.is_superuser:
            has_source_access = await self.company_repository.user_has_access(
                current_user.user_id,
                source_company_id,
            )
            has_target_access = await self.company_repository.user_has_access(
                current_user.user_id,
                target_company_id,
            )
            if not has_source_access or not has_target_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied: user must have access to both source and target companies",
                )

        if source_company_id == target_company_id:
            return ProjectTransferResponse(
                project_id=project_id,
                project_name=project["name"],
                source_company_id=source_company_id,
                target_company_id=target_company_id,
                transferred=False,
                postgresql={"project_updated": False, "updated_tables": {}},
                neo4j={"project_nodes_updated": 0},
                qdrant={"points_copied": 0, "points_deleted_from_source": 0},
                mongodb={"matched": 0, "modified": 0},
                message="Project is already assigned to target company",
            )

        qdrant_result: Dict[str, Any] = {}
        neo4j_result: Dict[str, Any] = {}
        mongo_result: Dict[str, Any] = {}
        external_migration_complete = {"qdrant": False, "neo4j": False, "mongodb": False}

        try:
            qdrant_result = await self.qdrant_repository.transfer_project_points_between_companies(
                project_id=project_id,
                source_company_id=source_company_id,
                target_company_id=target_company_id,
            )
            external_migration_complete["qdrant"] = True

            neo4j_result = await self.neo4j_repository.transfer_project_scope(
                project_id=project_id,
                source_company_id=source_company_id,
                target_company_id=target_company_id,
            )
            external_migration_complete["neo4j"] = True

            mongo_result = await self._migrate_mongodb_project_scope(
                project_id=project_id,
                source_company_id=source_company_id,
                target_company_id=target_company_id,
            )
            external_migration_complete["mongodb"] = True

            try:
                postgres_result = await self.project_repository.transfer_company_scope(
                    project_id=project_id,
                    source_company_id=source_company_id,
                    target_company_id=target_company_id,
                )
            except Exception as postgres_error:
                compensation: Dict[str, Any] = {}

                if external_migration_complete["qdrant"]:
                    try:
                        compensation[
                            "qdrant"
                        ] = await self.qdrant_repository.transfer_project_points_between_companies(
                            project_id=project_id,
                            source_company_id=target_company_id,
                            target_company_id=source_company_id,
                        )
                    except Exception as rollback_error:
                        compensation["qdrant"] = {"rollback_error": str(rollback_error)}

                if external_migration_complete["neo4j"]:
                    try:
                        compensation["neo4j"] = await self.neo4j_repository.transfer_project_scope(
                            project_id=project_id,
                            source_company_id=target_company_id,
                            target_company_id=source_company_id,
                        )
                    except Exception as rollback_error:
                        compensation["neo4j"] = {"rollback_error": str(rollback_error)}

                if external_migration_complete["mongodb"]:
                    try:
                        compensation["mongodb"] = await self._migrate_mongodb_project_scope(
                            project_id=project_id,
                            source_company_id=target_company_id,
                            target_company_id=source_company_id,
                        )
                    except Exception as rollback_error:
                        compensation["mongodb"] = {"rollback_error": str(rollback_error)}

                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={
                        "message": "PostgreSQL transfer failed after external migration; compensation attempted",
                        "error": str(postgres_error),
                        "compensation": compensation,
                    },
                ) from postgres_error

            return ProjectTransferResponse(
                project_id=project_id,
                project_name=project["name"],
                source_company_id=source_company_id,
                target_company_id=target_company_id,
                transferred=True,
                postgresql=postgres_result,
                neo4j=neo4j_result,
                qdrant=qdrant_result,
                mongodb=mongo_result,
                message="Project transfer completed",
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"Failed to transfer project {project_id} from {source_company_id} to {target_company_id}: {e}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Project transfer failed: {str(e)}",
            )

    async def update_project(
        self, project_id: str, data: ProjectUpdate, current_user: CurrentUser
    ) -> ProjectResponse:
        """
        Update project fields.

        Args:
            project_id: Project UUID
            data: Project update data
            current_user: Current authenticated user

        Returns:
            Updated project response

        Raises:
            HTTPException: If project not found or access denied
        """
        # Check if project exists
        project = await self.project_repository.get_by_id(project_id)

        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Project {project_id} not found"
            )

        # Check access via company (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.company_repository.user_has_access(
                current_user.user_id, project["company_id"]
            )

            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this project"
                )

        try:
            updated_project = await self.project_repository.update(
                project_id=project_id,
                name=data.name,
                description=data.description,
                github_token=data.github_token,
            )

            if not updated_project:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail=f"Project {project_id} not found"
                )

            # Upsert instructions if provided
            instructions = None
            if data.instructions and self.instructions_repository:
                instructions = await self.instructions_repository.upsert_project_instructions(
                    project_id=project_id,
                    description=data.instructions.description,
                    languages=data.instructions.languages,
                    frameworks=data.instructions.frameworks,
                    tools=data.instructions.tools,
                    architecture_notes=data.instructions.architecture_notes,
                    conventions=data.instructions.conventions,
                    custom_instructions=data.instructions.custom_instructions,
                )
                logger.info(f"Upserted instructions for project {project_id}")
            elif self.instructions_repository:
                # Fetch existing instructions for response
                instructions = await self.instructions_repository.get_project_instructions(
                    project_id
                )

            logger.info(f"User {current_user.user_id} updated project {project_id}")

            return self._build_project_response(updated_project, instructions)

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to update project: {}", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update project: {str(e)}",
            )

    async def regenerate_webhook_secret(
        self, project_id: str, current_user: CurrentUser
    ) -> WebhookSecretResponse:
        """
        Regenerate the webhook secret for a project.

        Args:
            project_id: Project UUID
            current_user: Current authenticated user

        Returns:
            WebhookSecretResponse with the NEW secret (shown only once)

        Raises:
            HTTPException: If project not found or access denied
        """
        # Check if project exists
        project = await self.project_repository.get_by_id(project_id)

        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Project {project_id} not found"
            )

        # Check access via company (super admin or company member)
        if not current_user.is_superuser:
            has_access = await self.company_repository.user_has_access(
                current_user.user_id, project["company_id"]
            )

            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this project"
                )

        try:
            updated_project = await self.project_repository.regenerate_webhook_secret(
                project_id=project_id
            )

            if not updated_project:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail=f"Project {project_id} not found"
                )

            logger.info(
                f"User {current_user.user_id} regenerated webhook secret for project {project_id}"
            )

            # Return full secret ONCE - this is the only time it will be shown
            return WebhookSecretResponse(
                project_id=updated_project["id"],
                webhook_url=updated_project["webhook_url"],
                github_webhook_secret=updated_project["github_webhook_secret"],
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to regenerate webhook secret: {}", e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to regenerate webhook secret: {str(e)}",
            )

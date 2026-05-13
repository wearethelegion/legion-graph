"""
Ingestion Service
Business logic for ingestion status operations.
Uses MongoDB for ingestion data (same as gRPC servicer).
"""

import json
import os
from typing import Optional
from urllib.parse import quote_plus
from fastapi import HTTPException, status
from api.repositories.project_repository import ProjectRepository
from api.repositories.ingestion_progress_repository import IngestionProgressRepository
from api.models.ingestion import (
    IngestionStatusResponse,
    IngestionListResponse,
    FailedFile,
    ProjectIngestionSummary,
    IngestionProgressResponse,
    IngestionStageProgress,
)
from api.auth import CurrentUser, validate_company_access
from loguru import logger
import asyncpg


# MongoDB connection - lazy initialized
_mongo_client = None
_ingestions_collection = None


def _get_ingestions_collection():
    """Get or create MongoDB connection to ingestions collection."""
    global _mongo_client, _ingestions_collection

    if _ingestions_collection is None:
        from pymongo import MongoClient

        # Build MongoDB URI from environment
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

        logger.info(f"Connecting to MongoDB at {host}:{port}")
        _mongo_client = MongoClient(uri)
        _ingestions_collection = _mongo_client[database]["ingestions"]
        logger.info("MongoDB ingestions collection initialized")

    return _ingestions_collection


class IngestionService:
    """Service for ingestion status business logic."""

    def __init__(self, pool: asyncpg.Pool, project_repository: Optional[ProjectRepository] = None):
        self.project_repo = project_repository or ProjectRepository(pool)
        self.ingestion_progress_repo = IngestionProgressRepository(pool)

    @staticmethod
    def _round_percentage(processed: int, total: int) -> float:
        if total <= 0:
            return 0.0
        return round(min((processed / total) * 100.0, 100.0), 2)

    @staticmethod
    def _to_project_ingestion_summary(row: dict) -> ProjectIngestionSummary:
        return ProjectIngestionSummary(
            ingestion_id=str(row.get("ingestion_id", "")),
            repository=row.get("repository", ""),
            branch=row.get("branch", ""),
            status=row.get("status", "running"),
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            total_chunks=row.get("total_chunks", 0),
        )

    # =========================================================================
    # Get Ingestion Status
    # =========================================================================

    async def get_ingestion_status(
        self, ingestion_id: str, current_user: CurrentUser
    ) -> IngestionStatusResponse:
        """
        Get status and progress of a specific ingestion.

        Args:
            ingestion_id: Ingestion UUID
            current_user: Current authenticated user

        Returns:
            Ingestion status response

        Raises:
            HTTPException: On authorization errors or not found
        """
        try:
            collection = _get_ingestions_collection()
            ingestion = collection.find_one({"ingestion_id": ingestion_id})

            if not ingestion:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Ingestion {ingestion_id} not found",
                )

            # Validate access via project
            project_id = ingestion.get("project_id")
            if project_id:
                company_id = await self.project_repo.get_company_id(project_id)
                if company_id:
                    validate_company_access(current_user, company_id)

            # Calculate percentage
            total_files = ingestion.get("total_files", 0)
            files_processed = ingestion.get("files_processed", 0)
            percentage = 0.0
            if total_files > 0:
                percentage = (files_processed / total_files) * 100.0

            # Convert failed_files to response models
            failed_files_response = []
            failed_files = ingestion.get("failed_files", [])
            if isinstance(failed_files, list):
                for ff in failed_files[:50]:  # Limit to 50 for response size
                    failed_files_response.append(
                        FailedFile(
                            file_path=ff.get("file_path", ""),
                            error=ff.get("error", ""),
                            timestamp=str(ff.get("timestamp", "")),
                        )
                    )

            # Extract LLM metrics from MongoDB
            files_llm_fallback = ingestion.get("files_llm_fallback", 0)
            llm_successful = ingestion.get("llm_successful", 0)
            llm_errors = ingestion.get("llm_errors", 0)

            return IngestionStatusResponse(
                ingestion_id=str(ingestion.get("ingestion_id", "")),
                status=ingestion.get("status", "pending"),
                repository=ingestion.get("repository", ""),
                branch=ingestion.get("branch", ""),
                total_files=total_files,
                files_processed=files_processed,
                files_failed=ingestion.get("files_failed", 0),
                files_skipped=ingestion.get("files_skipped", 0),
                current_file=ingestion.get("current_file"),
                failed_files=failed_files_response,
                started_at=ingestion.get("started_at"),
                completed_at=ingestion.get("completed_at"),
                percentage=percentage,
                # LLM processing metrics (Observability v2)
                files_llm_fallback=files_llm_fallback,
                files_filtered_size=ingestion.get("files_filtered_size", 0),
                files_filtered_extension=ingestion.get("files_filtered_extension", 0),
                files_filtered_directory=ingestion.get("files_filtered_directory", 0),
                # LLM analysis metrics (user-facing summary)
                llm_successful=llm_successful,
                llm_errors=llm_errors,
                llm_fallback=files_llm_fallback,  # Alias for files_llm_fallback
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to get ingestion status: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get ingestion status: {str(e)}",
            )

    # =========================================================================
    # List Ingestions
    # =========================================================================

    async def list_ingestions(
        self,
        project_id: str,
        current_user: CurrentUser,
        status_filter: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> IngestionListResponse:
        """
        List ingestions for a project.

        Args:
            project_id: Project UUID
            current_user: Current authenticated user
            status_filter: Optional status filter
            limit: Maximum results

        Returns:
            List of ingestion summaries

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
            result = await self.ingestion_progress_repo.list_project_ingestions(
                project_id=project_id,
                status_filter=status_filter,
                limit=limit,
                offset=offset,
            )

            ingestions = result.get("ingestions", []) or []
            if isinstance(ingestions, str):
                ingestions = json.loads(ingestions)
            summaries = [self._to_project_ingestion_summary(row) for row in ingestions]

            return IngestionListResponse(
                total_count=result.get("total_count", 0),
                ingestions=summaries,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to list ingestions: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list ingestions: {str(e)}",
            )

    # =========================================================================
    # Ingestion Progress
    # =========================================================================

    async def get_ingestion_progress(
        self,
        ingestion_id: str,
        current_user: CurrentUser,
    ) -> IngestionProgressResponse:
        """Get postgres-backed progress for a single ingestion."""
        try:
            project_id = await self.ingestion_progress_repo.get_ingestion_project_id(ingestion_id)
            if not project_id:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Ingestion {ingestion_id} not found",
                )

            company_id = await self.project_repo.get_company_id(project_id)
            if not company_id:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Project {project_id} not found",
                )

            validate_company_access(current_user, company_id)

            progress = await self.ingestion_progress_repo.get_ingestion_progress(ingestion_id)
            if not progress:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Ingestion {ingestion_id} not found",
                )

            total_chunks = await self.ingestion_progress_repo.get_ingestion_total_chunks(
                ingestion_id
            )

            extracted = progress.get("extracted", 0)
            entities_extracted = progress.get("entities_extracted", 0)
            edges_extracted = progress.get("edges_extracted", 0)
            summarised = progress.get("summarised", 0)
            embedded = progress.get("embedded", 0)
            stored_neo4j = progress.get("stored_neo4j", 0)

            status_value = "failed" if progress.get("has_failed", 0) == 1 else "running"
            if status_value != "failed":
                if (
                    total_chunks > 0
                    and progress.get("has_entity_extraction", 0) == 1
                    and progress.get("has_summarization", 0) == 1
                    and progress.get("has_embedding", 0) == 1
                    and progress.get("has_neo4j_storage", 0) == 1
                    and progress.get("all_completed", False)
                    and stored_neo4j >= extracted
                ):
                    status_value = "completed"

            stages = [
                IngestionStageProgress(
                    name="extracted",
                    total=total_chunks,
                    processed=extracted,
                    percentage=self._round_percentage(extracted, total_chunks),
                ),
                IngestionStageProgress(
                    name="summarised",
                    total=extracted,
                    processed=summarised,
                    percentage=self._round_percentage(summarised, extracted),
                ),
                IngestionStageProgress(
                    name="embedded",
                    total=extracted + entities_extracted + edges_extracted + summarised,
                    processed=embedded,
                    percentage=self._round_percentage(
                        embedded,
                        extracted + entities_extracted + edges_extracted + summarised,
                    ),
                ),
                IngestionStageProgress(
                    name="stored_neo4j",
                    total=extracted,
                    processed=stored_neo4j,
                    percentage=self._round_percentage(stored_neo4j, extracted),
                ),
            ]

            return IngestionProgressResponse(
                ingestion_id=str(progress.get("ingestion_id", ingestion_id)),
                status=status_value,
                total_chunks=total_chunks,
                stages=stages,
                updated_at=progress.get("updated_at"),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Failed to get ingestion progress: {}", e, exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get ingestion progress: {str(e)}",
            )

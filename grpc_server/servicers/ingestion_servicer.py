"""
IngestionServicer - gRPC service implementation for ingestion status queries.

Phase 4 REWRITE: Replaced MongoDB-backed implementation with Postgres-backed
IngestionStore (graph_services/code_preprocessor/storage/ingestion_store.py).

MongoDB was not in docker-compose and the old servicer always returned NOT_FOUND.
The IngestionStore (asyncpg + code_processing schema) is what the v2 preprocessor
actually writes to — this rewrite makes GetIngestionStatus and ListIngestions
functional for the first time.
"""

import grpc
from typing import Optional
from loguru import logger

from grpc_server.protos.loader import ingestion_pb2, ingestion_pb2_grpc
from grpc_server.utils.auth import get_current_user_from_context
from api.repositories.project_repository import ProjectRepository
from api.database.connection import _db_pool

# Singleton repository
_project_repository: Optional[ProjectRepository] = None


def get_project_repository() -> ProjectRepository:
    """Get or create singleton ProjectRepository."""
    global _project_repository

    if _project_repository is None:
        logger.info("Initializing singleton ProjectRepository")
        db_pool = _db_pool.get_pool()
        _project_repository = ProjectRepository(db_pool)
        logger.info("ProjectRepository initialized")

    return _project_repository


def _get_ingestion_store():
    """Get an IngestionStore bound to the current DB pool."""
    import sys
    from pathlib import Path

    # Ensure the graph_services path is resolvable
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from graph_services.code_preprocessor.storage.ingestion_store import IngestionStore

    pool = _db_pool.get_pool()
    return IngestionStore(pool)


class IngestionServicer(ingestion_pb2_grpc.IngestionServiceServicer):
    """
    gRPC servicer for ingestion status queries.
    Implements GetIngestionStatus and ListIngestions.

    Backed by Postgres code_processing.ingestion_batches table written
    by the v2 code_preprocessor pipeline.
    """

    async def GetIngestionStatus(
        self,
        request: ingestion_pb2.GetIngestionStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> ingestion_pb2.GetIngestionStatusResponse:
        """Get status and progress of a specific ingestion."""
        try:
            current_user = get_current_user_from_context(context)
            if not current_user:
                return ingestion_pb2.GetIngestionStatusResponse(
                    status="error",
                    error_message="Authentication required",
                    error_code="UNAUTHENTICATED",
                )

            logger.info(
                "GetIngestionStatus RPC called by {} for ingestion {}",
                current_user.email,
                request.ingestion_id,
            )

            store = _get_ingestion_store()
            ingestion = await store.get_ingestion(request.ingestion_id)

            if not ingestion:
                return ingestion_pb2.GetIngestionStatusResponse(
                    status="error",
                    error_message="Ingestion not found",
                    error_code="NOT_FOUND",
                )

            # SECURITY: verify ingestion belongs to caller's company
            if ingestion.get("company_id") not in current_user.companies:
                logger.warning(
                    "GetIngestionStatus DENIED: ingestion {} company {} not in user {} companies {}",
                    request.ingestion_id,
                    ingestion.get("company_id"),
                    current_user.email,
                    current_user.companies,
                )
                return ingestion_pb2.GetIngestionStatusResponse(
                    status="error",
                    error_message="Ingestion not found",
                    error_code="NOT_FOUND",
                )

            # Calculate percentage
            total_files = ingestion.get("total_files", 0)
            files_processed = ingestion.get("files_processed", 0)
            percentage = 0.0
            if total_files > 0:
                percentage = (files_processed / total_files) * 100.0

            return ingestion_pb2.GetIngestionStatusResponse(
                status="success",
                ingestion_id=str(ingestion.get("ingestion_id", "")),
                ingestion_status=ingestion.get("status", "pending"),
                repository=ingestion.get("repository", ""),
                branch=ingestion.get("branch", ""),
                total_files=total_files,
                files_processed=files_processed,
                files_failed=ingestion.get("files_failed", 0),
                files_skipped=ingestion.get("files_skipped", 0),
                current_file=ingestion.get("current_file", "") or "",
                # failed_files: Postgres stores these in ingestion_file_events, not
                # as a nested array in the batch row — returning empty list is correct
                failed_files=[],
                started_at=ingestion.get("started_at", "") or "",
                completed_at=ingestion.get("completed_at", "") or "",
                percentage=percentage,
            )

        except Exception as e:
            logger.error("GetIngestionStatus RPC failed: {}", e, exc_info=True)
            return ingestion_pb2.GetIngestionStatusResponse(
                status="error",
                error_message=str(e),
                error_code="INTERNAL",
            )

    async def ListIngestions(
        self,
        request: ingestion_pb2.ListIngestionsRequest,
        context: grpc.aio.ServicerContext,
    ) -> ingestion_pb2.ListIngestionsResponse:
        """List ingestions for a project with optional filters."""
        try:
            current_user = get_current_user_from_context(context)
            if not current_user:
                return ingestion_pb2.ListIngestionsResponse(
                    status="error",
                    error_message="Authentication required",
                    error_code="UNAUTHENTICATED",
                )

            logger.info(
                "ListIngestions RPC called by {} for project {}",
                current_user.email,
                request.project_id,
            )

            if not request.project_id:
                return ingestion_pb2.ListIngestionsResponse(
                    status="error",
                    error_message="project_id is required",
                    error_code="INVALID_ARGUMENT",
                )

            # SECURITY: verify project belongs to caller's company
            project_repo = get_project_repository()
            project_company_id = await project_repo.get_company_id(request.project_id)
            if project_company_id not in current_user.companies:
                logger.warning(
                    "ListIngestions DENIED: project {} company {} not in user {} companies {}",
                    request.project_id,
                    project_company_id,
                    current_user.email,
                    current_user.companies,
                )
                return ingestion_pb2.ListIngestionsResponse(
                    status="error",
                    error_message="Project not found",
                    error_code="NOT_FOUND",
                )

            # Apply limit with hard cap
            limit = min(request.limit or 50, 200)
            offset = request.offset or 0
            status_filter = request.status_filter or None

            store = _get_ingestion_store()
            ingestions = await store.list_ingestions(
                project_id=request.project_id,
                status=status_filter,
                limit=limit,
                offset=offset,
            )
            total_count = await store.count_ingestions(
                project_id=request.project_id,
                status=status_filter,
            )

            # Convert to proto messages
            ingestion_summaries = []
            for ing in ingestions:
                total_files = ing.get("total_files", 0)
                files_processed = ing.get("files_processed", 0)
                p = 0.0
                if total_files > 0:
                    p = (files_processed / total_files) * 100.0

                ingestion_summaries.append(
                    ingestion_pb2.IngestionSummary(
                        ingestion_id=str(ing.get("ingestion_id", "")),
                        repository=ing.get("repository", ""),
                        branch=ing.get("branch", ""),
                        ingestion_status=ing.get("status", "pending"),
                        total_files=total_files,
                        files_processed=files_processed,
                        started_at=ing.get("started_at", "") or "",
                        percentage=p,
                    )
                )

            return ingestion_pb2.ListIngestionsResponse(
                status="success",
                ingestions=ingestion_summaries,
                total_count=total_count,
            )

        except Exception as e:
            logger.error("ListIngestions RPC failed: {}", e, exc_info=True)
            return ingestion_pb2.ListIngestionsResponse(
                status="error",
                error_message=str(e),
                error_code="INTERNAL",
            )

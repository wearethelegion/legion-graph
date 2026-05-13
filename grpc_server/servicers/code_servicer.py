"""
CodeServicer - gRPC service implementation for code management
Delegates to existing CodeService and CodeQueryService (100% reuse).
"""

import os
from typing import Optional
from loguru import logger

from grpc_server.protos.loader import code_pb2, code_pb2_grpc
from api.services.code_service import CodeService
from api.services.code_query_service import CodeQueryService
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from api.repositories.project_repository import ProjectRepository
from api.database import get_db_pool
from grpc_server.utils.auth import get_current_user_from_context
from grpc_server.utils.adapters import (
    code_result_to_proto,
    entity_info_to_proto,
    impact_path_to_proto,
    execution_path_to_proto
)


# Singleton instances (shared across servicers)
_neo4j_repo: Optional[Neo4jRepository] = None
_qdrant_repo: Optional[QdrantRepository] = None
_project_repo: Optional[ProjectRepository] = None
_code_service: Optional[CodeService] = None
_code_query_service: Optional[CodeQueryService] = None


async def _get_code_service() -> CodeService:
    """Get or create singleton CodeService instance."""
    global _neo4j_repo, _qdrant_repo, _project_repo, _code_service

    if _code_service is None:
        # Initialize repositories (singleton pattern)
        if _neo4j_repo is None:
            _neo4j_repo = Neo4jRepository()
        if _qdrant_repo is None:
            _qdrant_repo = QdrantRepository()
        if _project_repo is None:
            db_pool = await get_db_pool()
            _project_repo = ProjectRepository(db_pool)

        # Initialize CodeService
        _code_service = CodeService(
            neo4j_repository=_neo4j_repo,
            qdrant_repository=_qdrant_repo,
            project_repository=_project_repo
        )
        logger.info("CodeService singleton initialized")

    return _code_service


async def _get_code_query_service() -> CodeQueryService:
    """Get or create singleton CodeQueryService instance."""
    global _neo4j_repo, _qdrant_repo, _code_query_service

    if _code_query_service is None:
        # Initialize repositories (singleton pattern)
        if _neo4j_repo is None:
            _neo4j_repo = Neo4jRepository()
        if _qdrant_repo is None:
            _qdrant_repo = QdrantRepository()

        # Initialize CodeQueryService
        _code_query_service = CodeQueryService(
            neo4j_repository=_neo4j_repo,
            qdrant_repository=_qdrant_repo
        )
        logger.info("CodeQueryService singleton initialized")

    return _code_query_service


async def _get_project_repo() -> ProjectRepository:
    """Get or create singleton ProjectRepository instance."""
    global _project_repo

    if _project_repo is None:
        db_pool = await get_db_pool()
        _project_repo = ProjectRepository(db_pool)
        logger.info("ProjectRepository singleton initialized")

    return _project_repo


class CodeServicer(code_pb2_grpc.CodeServiceServicer):
    """
    gRPC servicer for Code Management.

    Delegates to existing CodeService and CodeQueryService (100% reuse).
    """

    def __init__(self):
        """Initialize servicer (services initialized lazily on first RPC call)."""
        self.code_service = None
        self.code_query_service = None
        logger.info("CodeServicer initialized")

    async def CreateCode(self, request, context):
        """
        Create code: process with LLM, store in Neo4j + Qdrant.

        Args:
            request: CreateCodeRequest protobuf
            context: gRPC context (contains auth metadata)

        Returns:
            CreateCodeResponse protobuf
        """
        try:
            logger.info(f"CreateCode called: filename={request.filename}, code_length={len(request.code)}")

            # Lazy initialization of services
            if self.code_service is None:
                self.code_service = await _get_code_service()

            # Get current user from gRPC context
            current_user = get_current_user_from_context(context)
            if not current_user:
                return code_pb2.CreateCodeResponse(
                    status="error",
                    error_message="Authentication required",
                    error_code="unauthenticated"
                )

            # Get project ID from environment (for simplicity in MVP)
            project_id = os.getenv("MCP_PROJECT_ID", "")
            if not project_id:
                return code_pb2.CreateCodeResponse(
                    status="error",
                    error_message="MCP_PROJECT_ID not configured",
                    error_code="configuration_error"
                )

            # Delegate to service layer (100% reuse)
            service_result = await self.code_service.create_code(
                code=request.code,
                filename=request.filename,
                project_id=project_id,
                current_user=current_user,
                metadata=dict(request.metadata) if request.metadata else None
            )

            # Convert service layer response to protobuf
            response = code_pb2.CreateCodeResponse(
                status=service_result.get("status", "success"),
                message=f"Code created successfully",
                code_id=service_result.get("code_id", ""),
                filename=request.filename,
                language=service_result.get("language", ""),
                title=service_result.get("title", ""),
                summary=service_result.get("summary", ""),
                chunks_count=service_result.get("chunks_count", 0),
                entities_count=service_result.get("entities_count", 0),
                relationships_count=service_result.get("relationships_count", 0)
            )

            logger.info(f"Code created: {response.code_id}")
            return response

        except Exception as e:
            logger.error(f"CreateCode failed: {e}", exc_info=True)
            return code_pb2.CreateCodeResponse(
                status="error",
                message=f"Creation failed: {str(e)}",
                error_message=str(e),
                error_code="execution_failed"
            )

    async def FindSimilarCode(self, request, context):
        """
        Find similar code using vector search + graph enrichment.

        Args:
            request: FindSimilarCodeRequest protobuf
            context: gRPC context

        Returns:
            FindSimilarCodeResponse protobuf
        """
        try:
            logger.info(f"FindSimilarCode called: query='{request.query}', language={request.language}, project_id={request.project_id}")

            # Lazy initialization of services
            if self.code_query_service is None:
                self.code_query_service = await _get_code_query_service()

            # Get current user from context
            current_user = get_current_user_from_context(context)
            if not current_user:
                return code_pb2.FindSimilarCodeResponse(
                    status="error",
                    error_message="Authentication required",
                    error_code="unauthenticated"
                )

            # Look up project to get correct company_id (multi-tenant fix)
            if request.project_id:
                project_repo = await _get_project_repo()
                project = await project_repo.get_by_id(request.project_id)
                if not project:
                    return code_pb2.FindSimilarCodeResponse(
                        status="error",
                        error_message=f"Project {request.project_id} not found",
                        error_code="not_found"
                    )
                company_id = str(project["company_id"])
                
                # Verify user has access to this company
                if company_id not in current_user.companies:
                    logger.warning(f"User {current_user.email} denied access to company {company_id} (user companies: {current_user.companies})")
                    return code_pb2.FindSimilarCodeResponse(
                        status="error",
                        error_message="Access denied to project's company",
                        error_code="authorization_failed"
                    )
            else:
                # project_id is required to resolve company scope for code search
                return code_pb2.FindSimilarCodeResponse(
                    status="error",
                    error_message="project_id is required for code search",
                    error_code="invalid_argument"
                )

            # Delegate to service layer
            results = await self.code_query_service.find_similar_code(
                query=request.query,
                language=request.language,
                project_id=request.project_id,
                company_id=company_id,
                limit=request.limit or 10
            )

            # Convert results to protobuf
            proto_results = [code_result_to_proto(result) for result in results]

            response = code_pb2.FindSimilarCodeResponse(
                status="success",
                results=proto_results,
                total=len(proto_results)
            )

            logger.info(f"FindSimilarCode returned {len(proto_results)} results")
            return response

        except Exception as e:
            logger.error(f"FindSimilarCode failed: {e}", exc_info=True)
            return code_pb2.FindSimilarCodeResponse(
                status="error",
                total=0,
                error_message=str(e),
                error_code="execution_failed"
            )

    async def AnalyzeImpact(self, request, context):
        """
        Analyze impact of changing an entity (function, class, method).

        Args:
            request: AnalyzeImpactRequest protobuf
            context: gRPC context

        Returns:
            AnalyzeImpactResponse protobuf
        """
        try:
            logger.info(f"AnalyzeImpact called: entity={request.entity_name} ({request.entity_type})")

            # Lazy initialization of services
            if self.code_query_service is None:
                self.code_query_service = await _get_code_query_service()

            # Get current user
            current_user = get_current_user_from_context(context)
            if not current_user:
                return code_pb2.AnalyzeImpactResponse(
                    status="error",
                    error_message="Authentication required",
                    error_code="unauthenticated"
                )

            # SECURITY: verify project_id belongs to caller's company
            if request.project_id:
                project_repo = await _get_project_repo()
                project = await project_repo.get_by_id(request.project_id)
                if not project:
                    return code_pb2.AnalyzeImpactResponse(
                        status="error",
                        error_message=f"Project {request.project_id} not found",
                        error_code="not_found"
                    )
                company_id = str(project["company_id"])
                if company_id not in current_user.companies:
                    logger.warning(
                        f"AnalyzeImpact DENIED: user {current_user.email} attempted access "
                        f"to project {request.project_id} (company {company_id}), "
                        f"user companies: {current_user.companies}"
                    )
                    return code_pb2.AnalyzeImpactResponse(
                        status="error",
                        error_message=f"Project {request.project_id} not found",
                        error_code="not_found"
                    )

            # Delegate to service layer
            impact_result = await self.code_query_service.analyze_impact(
                entity_name=request.entity_name,
                entity_type=request.entity_type,
                project_id=request.project_id,
                max_depth=request.max_depth or 3
            )

            # Convert to protobuf
            entity_proto = entity_info_to_proto(impact_result["entity"])
            upstream_proto = [impact_path_to_proto(path) for path in impact_result["upstream"]]
            downstream_proto = [impact_path_to_proto(path) for path in impact_result["downstream"]]

            response = code_pb2.AnalyzeImpactResponse(
                status="success",
                entity=entity_proto,
                upstream=upstream_proto,
                downstream=downstream_proto,
                risk_level=impact_result["risk_level"],
                upstream_count=impact_result["upstream_count"],
                downstream_count=impact_result["downstream_count"]
            )

            logger.info(f"AnalyzeImpact completed: {response.upstream_count} upstream, {response.downstream_count} downstream")
            return response

        except ValueError as e:
            # Entity not found
            logger.warning(f"AnalyzeImpact entity not found: {e}")
            return code_pb2.AnalyzeImpactResponse(
                status="error",
                error_message=str(e),
                error_code="not_found"
            )
        except Exception as e:
            logger.error(f"AnalyzeImpact failed: {e}", exc_info=True)
            return code_pb2.AnalyzeImpactResponse(
                status="error",
                error_message=str(e),
                error_code="execution_failed"
            )

    async def TraceExecutionFlow(self, request, context):
        """
        Trace execution flow from an entry point (DFS via :calls relationships).

        Args:
            request: TraceExecutionFlowRequest protobuf
            context: gRPC context

        Returns:
            TraceExecutionFlowResponse protobuf
        """
        try:
            logger.info(f"TraceExecutionFlow called: entry_point={request.entry_point}")

            # Lazy initialization of services
            if self.code_query_service is None:
                self.code_query_service = await _get_code_query_service()

            # Get current user
            current_user = get_current_user_from_context(context)
            if not current_user:
                return code_pb2.TraceExecutionFlowResponse(
                    status="error",
                    error_message="Authentication required",
                    error_code="unauthenticated"
                )

            # SECURITY: verify project_id belongs to caller's company
            if request.project_id:
                project_repo = await _get_project_repo()
                project = await project_repo.get_by_id(request.project_id)
                if not project:
                    return code_pb2.TraceExecutionFlowResponse(
                        status="error",
                        error_message=f"Project {request.project_id} not found",
                        error_code="not_found"
                    )
                company_id = str(project["company_id"])
                if company_id not in current_user.companies:
                    logger.warning(
                        f"TraceExecutionFlow DENIED: user {current_user.email} attempted access "
                        f"to project {request.project_id} (company {company_id}), "
                        f"user companies: {current_user.companies}"
                    )
                    return code_pb2.TraceExecutionFlowResponse(
                        status="error",
                        error_message=f"Project {request.project_id} not found",
                        error_code="not_found"
                    )

            # Delegate to service layer
            execution_paths = await self.code_query_service.trace_execution_flow(
                entry_point=request.entry_point,
                project_id=request.project_id,
                max_depth=request.max_depth or 5
            )

            # Convert to protobuf
            proto_paths = [execution_path_to_proto(path) for path in execution_paths]

            response = code_pb2.TraceExecutionFlowResponse(
                status="success",
                paths=proto_paths,
                total_paths=len(proto_paths)
            )

            logger.info(f"TraceExecutionFlow completed: {len(proto_paths)} paths found")
            return response

        except ValueError as e:
            # Entry point not found
            logger.warning(f"TraceExecutionFlow entry point not found: {e}")
            return code_pb2.TraceExecutionFlowResponse(
                status="error",
                error_message=str(e),
                error_code="not_found"
            )
        except Exception as e:
            logger.error(f"TraceExecutionFlow failed: {e}", exc_info=True)
            return code_pb2.TraceExecutionFlowResponse(
                status="error",
                error_message=str(e),
                error_code="execution_failed"
            )
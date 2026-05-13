"""
CodeSearchServicer — gRPC facade for CodeSearchService.

Mirrors unified_search_servicer.py style (singletons, lazy init, helper pattern).
Multi-tenancy: company_id is derived from the JWT (same pattern as unified_search_servicer.py).
The client MUST NOT supply company_id — it is removed from all 7 RPC request messages (T10.A).

RPCs (all unary — 7 deterministic RPCs, no LLM dependency):
  GetDomains, SearchEntities, SearchSummaries, GetCodeForEntity,
  TraverseGraph, GetEntityGraph, FullSearch, Health

Agentic search runs in the MCP tool layer (kgrag-mcp/tools/code_search.py).
ANTHROPIC_API_KEY belongs on the MCP host, not here.
"""

import time
from typing import Optional
from loguru import logger

from grpc_server.protos.loader import code_search_pb2, code_search_pb2_grpc
from grpc_server.utils.auth import get_current_user_from_context
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from api.repositories.project_repository import ProjectRepository
from api.database import get_db_pool
from api.services.code_search_service import CodeSearchService
from grpc_server.servicers.code_search_metrics import observe_rpc


# ── Singletons ────────────────────────────────────────────────────────────────

_neo4j_repo: Optional[Neo4jRepository] = None
_qdrant_repo: Optional[QdrantRepository] = None
_project_repo: Optional[ProjectRepository] = None
_code_search_service: Optional[CodeSearchService] = None


async def _init_repos() -> None:
    """Lazy-initialize shared singleton repositories."""
    global _neo4j_repo, _qdrant_repo, _project_repo
    if _neo4j_repo is None:
        _neo4j_repo = Neo4jRepository()
    if _qdrant_repo is None:
        _qdrant_repo = QdrantRepository()
    if _project_repo is None:
        db_pool = await get_db_pool()
        _project_repo = ProjectRepository(db_pool)


async def _get_code_search_service() -> CodeSearchService:
    """Get or create singleton CodeSearchService."""
    global _code_search_service
    if _code_search_service is None:
        await _init_repos()
        _code_search_service = CodeSearchService(
            neo4j_repository=_neo4j_repo,
            qdrant_repository=_qdrant_repo,
        )
        logger.info("CodeSearchService singleton initialized")
    return _code_search_service


async def _resolve_project_name(company_id: str, project_id: str) -> str:
    """Resolve a project's canonical name for scope-key construction."""
    await _init_repos()
    project = await _project_repo.get_by_id(project_id)
    if not project or str(project.get("company_id") or "") != company_id:
        raise ValueError("project_id not found")
    project_name = str(project.get("name") or "").strip()
    if not project_name:
        raise ValueError("project_name required when project_id is set")
    return project_name


def _project_resolution_error_code(message: str) -> str:
    return "NOT_FOUND" if message == "project_id not found" else "INVALID_ARGUMENT"


# ── Proto mapping helpers ─────────────────────────────────────────────────────


def _domain_info_to_proto(d: dict) -> code_search_pb2.DomainInfo:
    return code_search_pb2.DomainInfo(
        name=str(d.get("name") or ""),
        description=str(d.get("description") or ""),
        project_id=str(d.get("project_id") or ""),
        entity_count=int(d.get("entity_count") or 0),
    )


def _entity_result_to_proto(e: dict) -> code_search_pb2.EntityResult:
    related = [
        code_search_pb2.RelatedEntity(
            relationship=str(r.get("relationship") or ""),
            name=str(r.get("name") or ""),
            type=str(r.get("type") or ""),
        )
        for r in (e.get("related") or [])
    ]
    return code_search_pb2.EntityResult(
        entity_id=str(e.get("entity_id") or ""),
        name=str(e.get("name") or ""),
        entity_type=str(e.get("entity_type") or ""),
        description=str(e.get("description") or ""),
        project_id=str(e.get("project_id") or ""),
        branch=str(e.get("branch") or ""),
        score=float(e.get("score") or 0.0),
        related=related,
    )


def _summary_result_to_proto(s: dict) -> code_search_pb2.SummaryResult:
    return code_search_pb2.SummaryResult(
        text=str(s.get("text") or ""),
        file_path=str(s.get("file_path") or ""),
        language=str(s.get("language") or ""),
        project_id=str(s.get("project_id") or ""),
        score=float(s.get("score") or 0.0),
    )


def _code_chunk_to_proto(c: dict) -> code_search_pb2.CodeChunk:
    return code_search_pb2.CodeChunk(
        file_path=str(c.get("file_path") or ""),
        language=str(c.get("language") or ""),
        text=str(c.get("text") or ""),
        start_line=int(c.get("start_line") or 0),
        end_line=int(c.get("end_line") or 0),
    )


def _graph_edge_to_proto(g: dict) -> code_search_pb2.GraphEdge:
    return code_search_pb2.GraphEdge(
        source=str(g.get("source") or ""),
        relationship=str(g.get("relationship") or ""),
        target=str(g.get("target") or ""),
        target_type=str(g.get("target_type") or ""),
        file_path=str(g.get("file_path") or ""),
    )


# ── Servicer ──────────────────────────────────────────────────────────────────


class CodeSearchServicer(code_search_pb2_grpc.CodeSearchServiceServicer):
    """
    gRPC servicer for Code Search (six primitives + FullSearch + Health).

    Thin facade: validates inputs → extracts company_id from JWT (T10.A) →
    delegates to CodeSearchService → maps result dicts to proto responses.

    Multi-tenancy: company_id is derived from the authenticated JWT only.
    Mirrors unified_search_servicer.py pattern exactly.
    """

    def __init__(self):
        logger.info("CodeSearchServicer initialized")

    # ── GetDomains ────────────────────────────────────────────────────────────

    async def GetDomains(self, request, context):
        _ERR = code_search_pb2.GetDomainsResponse
        _t0 = time.monotonic()
        _status = "ok"
        try:
            current_user = get_current_user_from_context(context)
            if not current_user:
                _status = "error"
                return _ERR(error_message="Authentication required", error_code="UNAUTHENTICATED")

            if not current_user.companies:
                _status = "error"
                return _ERR(
                    error_message="No company associated with this account",
                    error_code="UNAUTHENTICATED",
                )

            company_id = current_user.companies[0]
            if request.project_id:
                try:
                    await _resolve_project_name(company_id, request.project_id)
                except ValueError as e:
                    _status = "error"
                    return _ERR(
                        error_message=str(e),
                        error_code=_project_resolution_error_code(str(e)),
                    )

            # proto3 bool default is False; business default is True.
            include_technical = True

            svc = await _get_code_search_service()
            result = await svc.get_domains(
                company_id=company_id,
                project_id=request.project_id or None,
                include_technical=include_technical,
            )

            return _ERR(
                business_domains=[
                    _domain_info_to_proto(d) for d in result.get("business_domains", [])
                ],
                technical_tags=[_domain_info_to_proto(d) for d in result.get("technical_tags", [])],
            )
        except Exception as e:
            _status = "error"
            logger.error("GetDomains failed: %s", e, exc_info=True)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            observe_rpc("GetDomains", _status, time.monotonic() - _t0)

    # ── SearchEntities ────────────────────────────────────────────────────────

    async def SearchEntities(self, request, context):
        _ERR = code_search_pb2.SearchEntitiesResponse
        _t0 = time.monotonic()
        _status = "ok"
        try:
            current_user = get_current_user_from_context(context)
            if not current_user:
                _status = "error"
                return _ERR(error_message="Authentication required", error_code="UNAUTHENTICATED")

            if not current_user.companies:
                _status = "error"
                return _ERR(
                    error_message="No company associated with this account",
                    error_code="UNAUTHENTICATED",
                )

            if not request.query or not request.query.strip():
                _status = "error"
                return _ERR(error_message="query is required", error_code="INVALID_ARGUMENT")

            company_id = current_user.companies[0]
            project_name = None
            if request.project_id:
                try:
                    project_name = await _resolve_project_name(company_id, request.project_id)
                except ValueError as e:
                    _status = "error"
                    return _ERR(
                        error_message=str(e),
                        error_code=_project_resolution_error_code(str(e)),
                    )

            # Proto bool defaults: False. We want exclude_tests=True by default.
            exclude_tests = request.exclude_tests if request.exclude_tests else True
            exclude_domain_meta = (
                request.exclude_domain_meta if request.exclude_domain_meta else True
            )
            limit = request.limit if request.limit > 0 else 10
            branch = request.branch or "develop"

            svc = await _get_code_search_service()
            results = await svc.search_entities(
                query=request.query,
                company_id=company_id,
                project_id=request.project_id or None,
                project_name=project_name,
                branch=branch,
                domain=request.domain or None,
                entity_type=request.entity_type or None,
                limit=limit,
                exclude_tests=exclude_tests,
                exclude_domain_meta=exclude_domain_meta,
            )

            low_confidence_reason = getattr(svc, "_last_low_confidence_reason", "")
            return _ERR(
                results=[_entity_result_to_proto(e) for e in results],
                low_confidence_reason=low_confidence_reason,
            )
        except Exception as e:
            _status = "error"
            logger.error("SearchEntities failed: %s", e, exc_info=True)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            observe_rpc("SearchEntities", _status, time.monotonic() - _t0)

    # ── SearchSummaries ───────────────────────────────────────────────────────

    async def SearchSummaries(self, request, context):
        _ERR = code_search_pb2.SearchSummariesResponse
        _t0 = time.monotonic()
        _status = "ok"
        try:
            current_user = get_current_user_from_context(context)
            if not current_user:
                _status = "error"
                return _ERR(error_message="Authentication required", error_code="UNAUTHENTICATED")

            if not current_user.companies:
                _status = "error"
                return _ERR(
                    error_message="No company associated with this account",
                    error_code="UNAUTHENTICATED",
                )

            if not request.query or not request.query.strip():
                _status = "error"
                return _ERR(error_message="query is required", error_code="INVALID_ARGUMENT")

            company_id = current_user.companies[0]
            project_name = None
            if request.project_id:
                try:
                    project_name = await _resolve_project_name(company_id, request.project_id)
                except ValueError as e:
                    _status = "error"
                    return _ERR(
                        error_message=str(e),
                        error_code=_project_resolution_error_code(str(e)),
                    )

            exclude_tests = request.exclude_tests if request.exclude_tests else True
            limit = request.limit if request.limit > 0 else 10

            svc = await _get_code_search_service()
            results = await svc.search_summaries(
                query=request.query,
                company_id=company_id,
                project_id=request.project_id or None,
                project_name=project_name,
                limit=limit,
                exclude_tests=exclude_tests,
            )

            low_confidence_reason = getattr(svc, "_last_low_confidence_reason", "")
            return _ERR(
                results=[_summary_result_to_proto(s) for s in results],
                low_confidence_reason=low_confidence_reason,
            )
        except Exception as e:
            _status = "error"
            logger.error("SearchSummaries failed: %s", e, exc_info=True)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            observe_rpc("SearchSummaries", _status, time.monotonic() - _t0)

    # ── GetCodeForEntity ──────────────────────────────────────────────────────

    async def GetCodeForEntity(self, request, context):
        _ERR = code_search_pb2.GetCodeForEntityResponse
        _t0 = time.monotonic()
        _status = "ok"
        try:
            current_user = get_current_user_from_context(context)
            if not current_user:
                _status = "error"
                return _ERR(error_message="Authentication required", error_code="UNAUTHENTICATED")

            if not current_user.companies:
                _status = "error"
                return _ERR(
                    error_message="No company associated with this account",
                    error_code="UNAUTHENTICATED",
                )

            if not request.entity_name or not request.entity_name.strip():
                _status = "error"
                return _ERR(error_message="entity_name is required", error_code="INVALID_ARGUMENT")

            company_id = current_user.companies[0]

            exclude_tests = request.exclude_tests if request.exclude_tests else True

            # Default 10, cap at 100. proto3 scalar ints default to 0 when unset.
            limit = request.limit if request.limit and request.limit > 0 else 10
            if limit > 100:
                limit = 100

            svc = await _get_code_search_service()
            results = await svc.get_code_for_entity(
                entity_name=request.entity_name,
                company_id=company_id,
                exclude_tests=exclude_tests,
                limit=limit,
            )

            return _ERR(results=[_code_chunk_to_proto(c) for c in results])
        except Exception as e:
            _status = "error"
            logger.error("GetCodeForEntity failed: %s", e, exc_info=True)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            observe_rpc("GetCodeForEntity", _status, time.monotonic() - _t0)

    # ── TraverseGraph ─────────────────────────────────────────────────────────

    async def TraverseGraph(self, request, context):
        _ERR = code_search_pb2.TraverseGraphResponse
        _t0 = time.monotonic()
        _status = "ok"
        try:
            current_user = get_current_user_from_context(context)
            if not current_user:
                _status = "error"
                return _ERR(error_message="Authentication required", error_code="UNAUTHENTICATED")

            if not current_user.companies:
                _status = "error"
                return _ERR(
                    error_message="No company associated with this account",
                    error_code="UNAUTHENTICATED",
                )

            if not request.entity_name or not request.entity_name.strip():
                _status = "error"
                return _ERR(error_message="entity_name is required", error_code="INVALID_ARGUMENT")

            company_id = current_user.companies[0]

            exclude_tests = request.exclude_tests if request.exclude_tests else True

            svc = await _get_code_search_service()
            edges = await svc.traverse_graph(
                entity_name=request.entity_name,
                company_id=company_id,
                exclude_tests=exclude_tests,
            )

            return _ERR(edges=[_graph_edge_to_proto(e) for e in edges])
        except Exception as e:
            _status = "error"
            logger.error("TraverseGraph failed: %s", e, exc_info=True)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            observe_rpc("TraverseGraph", _status, time.monotonic() - _t0)

    # ── GetEntityGraph ────────────────────────────────────────────────────────

    async def GetEntityGraph(self, request, context):
        _ERR = code_search_pb2.GetEntityGraphResponse
        _t0 = time.monotonic()
        _status = "ok"
        try:
            current_user = get_current_user_from_context(context)
            if not current_user:
                _status = "error"
                return _ERR(error_message="Authentication required", error_code="UNAUTHENTICATED")

            if not current_user.companies:
                _status = "error"
                return _ERR(
                    error_message="No company associated with this account",
                    error_code="UNAUTHENTICATED",
                )

            if not request.entity_name or not request.entity_name.strip():
                _status = "error"
                return _ERR(error_message="entity_name is required", error_code="INVALID_ARGUMENT")

            company_id = current_user.companies[0]

            depth = request.depth if request.depth > 0 else 2
            exclude_tests = request.exclude_tests if request.exclude_tests else True

            svc = await _get_code_search_service()
            edges = await svc.get_entity_graph(
                entity_name=request.entity_name,
                company_id=company_id,
                depth=depth,
                exclude_tests=exclude_tests,
            )

            return _ERR(edges=[_graph_edge_to_proto(e) for e in edges])
        except Exception as e:
            _status = "error"
            logger.error("GetEntityGraph failed: %s", e, exc_info=True)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            observe_rpc("GetEntityGraph", _status, time.monotonic() - _t0)

    # ── FullSearch ────────────────────────────────────────────────────────────

    async def FullSearch(self, request, context):
        _ERR = code_search_pb2.FullSearchResponse
        _t0 = time.monotonic()
        _status = "ok"
        try:
            current_user = get_current_user_from_context(context)
            if not current_user:
                _status = "error"
                return _ERR(error_message="Authentication required", error_code="UNAUTHENTICATED")

            if not current_user.companies:
                _status = "error"
                return _ERR(
                    error_message="No company associated with this account",
                    error_code="UNAUTHENTICATED",
                )

            if not request.query or not request.query.strip():
                _status = "error"
                return _ERR(error_message="query is required", error_code="INVALID_ARGUMENT")

            company_id = current_user.companies[0]
            project_name = None
            if request.project_id:
                try:
                    project_name = await _resolve_project_name(company_id, request.project_id)
                except ValueError as e:
                    _status = "error"
                    return _ERR(
                        error_message=str(e),
                        error_code=_project_resolution_error_code(str(e)),
                    )

            depth = max(0, min(3, request.depth if request.depth > 0 else 1))
            limit = request.limit if request.limit > 0 else 10
            exclude_tests = request.exclude_tests if request.exclude_tests else True

            svc = await _get_code_search_service()
            result = await svc.full_search(
                query=request.query,
                company_id=company_id,
                depth=depth,
                project_id=request.project_id or None,
                project_name=project_name,
                domain=request.domain or None,
                entity_type=request.entity_type or None,
                limit=limit,
                exclude_tests=exclude_tests,
            )

            # Build DomainsBlock if present
            domains_block = None
            if "domains" in result:
                d = result["domains"]
                domains_block = code_search_pb2.DomainsBlock(
                    business_domains=[
                        _domain_info_to_proto(x) for x in d.get("business_domains", [])
                    ],
                    technical_tags=[_domain_info_to_proto(x) for x in d.get("technical_tags", [])],
                )

            return _ERR(
                domains=domains_block,
                entities=[_entity_result_to_proto(e) for e in result.get("entities", [])],
                summaries=[_summary_result_to_proto(s) for s in result.get("summaries", [])],
                code=[_code_chunk_to_proto(c) for c in result.get("code", [])],
            )
        except Exception as e:
            _status = "error"
            logger.error("FullSearch failed: %s", e, exc_info=True)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            observe_rpc("FullSearch", _status, time.monotonic() - _t0)

    # ── Health ────────────────────────────────────────────────────────────────

    async def Health(self, request, context):
        return code_search_pb2.HealthResponse(status="ok", message="CodeSearchService healthy")

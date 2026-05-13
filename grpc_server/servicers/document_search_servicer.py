"""
DocumentSearchServicer — gRPC facade for DocumentSearchService.

Mirrors code_search_servicer.py style exactly (singletons, lazy init,
multi-tenancy via JWT, proto mapping helpers).

RPCs (all unary — 6 deterministic primitives + Health = 7):
  GetCollections, SearchDocuments, GetDocumentChunk,
  SearchDocumentSummaries, TraverseDocumentGraph, FullDocumentSearch, Health

Multi-tenancy: company_id is derived from the authenticated JWT only.
  Every cognee query is scoped to `cognee-{company_id}` in Neo4j and
  to the `{company_id}_knowledge` collection in Qdrant.

Storage contract (confirmed 2026-04-29):
  - NodeSet name     : ``{company_id}_knowledge``
  - Qdrant (chunks)  : collection ``{company_id}_knowledge``
  - Entity_name      : source_node_set = ``{company_id}_knowledge``
  - TextSummary_text : source_node_set = ``{company_id}_knowledge``
"""

import time
from typing import Optional

from loguru import logger

from grpc_server.protos.loader import document_search_pb2, document_search_pb2_grpc
from grpc_server.utils.auth import get_current_user_from_context
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from api.services.document_search import DocumentSearchService


# ── Singletons ────────────────────────────────────────────────────────────────

_neo4j_repo: Optional[Neo4jRepository] = None
_qdrant_repo: Optional[QdrantRepository] = None
_document_search_service: Optional[DocumentSearchService] = None


async def _init_repos() -> None:
    """Lazy-initialize shared singleton repositories."""
    global _neo4j_repo, _qdrant_repo
    if _neo4j_repo is None:
        _neo4j_repo = Neo4jRepository()
    if _qdrant_repo is None:
        _qdrant_repo = QdrantRepository()


async def _get_document_search_service() -> DocumentSearchService:
    """Get or create singleton DocumentSearchService."""
    global _document_search_service
    if _document_search_service is None:
        await _init_repos()
        _document_search_service = DocumentSearchService(
            neo4j_repository=_neo4j_repo,
            qdrant_repository=_qdrant_repo,
        )
        logger.info("DocumentSearchService singleton initialized")
    return _document_search_service


# ── Proto mapping helpers ─────────────────────────────────────────────────────


def _collection_info_to_proto(c: dict) -> document_search_pb2.CollectionInfo:
    return document_search_pb2.CollectionInfo(
        name=str(c.get("name") or ""),
        id=str(c.get("id") or ""),
        description=str(c.get("description") or ""),
    )


def _document_chunk_to_proto(c: dict) -> document_search_pb2.DocumentChunk:
    return document_search_pb2.DocumentChunk(
        chunk_id=str(c.get("chunk_id") or ""),
        text=str(c.get("text") or ""),
        file_path=str(c.get("file_path") or ""),
        language=str(c.get("language") or ""),
        repository=str(c.get("repository") or ""),
        chunk_index=int(c.get("chunk_index") or 0),
        file_version_id=str(c.get("file_version_id") or ""),
        source_node_set=str(c.get("source_node_set") or ""),
        score=float(c.get("score") or 0.0),
    )


def _document_summary_to_proto(s: dict) -> document_search_pb2.DocumentSummary:
    return document_search_pb2.DocumentSummary(
        text=str(s.get("text") or ""),
        file_version_id=str(s.get("file_version_id") or ""),
        source_node_set=str(s.get("source_node_set") or ""),
        score=float(s.get("score") or 0.0),
    )


def _document_entity_to_proto(e: dict) -> document_search_pb2.DocumentEntity:
    return document_search_pb2.DocumentEntity(
        entity_id=str(e.get("entity_id") or ""),
        name=str(e.get("name") or ""),
        entity_type=str(e.get("entity_type") or ""),
        description=str(e.get("description") or ""),
        source_node_set=str(e.get("source_node_set") or ""),
        score=float(e.get("score") or 0.0),
    )


def _graph_edge_to_proto(g: dict) -> document_search_pb2.DocumentGraphEdge:
    return document_search_pb2.DocumentGraphEdge(
        source=str(g.get("source") or ""),
        relationship=str(g.get("relationship") or ""),
        target=str(g.get("target") or ""),
        target_type=str(g.get("target_type") or ""),
        source_node_set=str(g.get("source_node_set") or ""),
    )


def _auth_guard(context) -> tuple:
    """
    Validate JWT and extract company_id.

    Returns:
        (current_user, company_id) or (None, error_proto_fields_dict)
    """
    current_user = get_current_user_from_context(context)
    if not current_user:
        return None, {"error_message": "Authentication required", "error_code": "UNAUTHENTICATED"}
    if not current_user.companies:
        return None, {
            "error_message": "No company associated with this account",
            "error_code": "UNAUTHENTICATED",
        }
    return current_user, current_user.companies[0]


# ── Servicer ──────────────────────────────────────────────────────────────────


class DocumentSearchServicer(document_search_pb2_grpc.DocumentSearchServiceServicer):
    """
    gRPC servicer for Document Search (six primitives + Health).

    Thin facade: validates JWT → derives company_id → delegates to
    DocumentSearchService → maps result dicts to proto responses.

    Multi-tenancy: every cognee query is company-scoped via JWT. Mirrors
    CodeSearchServicer pattern exactly.
    """

    def __init__(self):
        logger.info("DocumentSearchServicer initialized")

    # ── GetCollections ────────────────────────────────────────────────────────

    async def GetCollections(self, request, context):
        _ERR = document_search_pb2.GetCollectionsResponse
        _t0 = time.monotonic()
        try:
            current_user, result = _auth_guard(context)
            if current_user is None:
                return _ERR(**result)

            company_id = result

            svc = await _get_document_search_service()
            collections = await svc.get_collections(company_id=company_id)

            return _ERR(collections=[_collection_info_to_proto(c) for c in collections])
        except Exception as e:
            logger.opt(exception=True).error("GetCollections failed: %s", e)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            logger.debug("GetCollections completed in %.3fs", time.monotonic() - _t0)

    # ── SearchDocuments ───────────────────────────────────────────────────────

    async def SearchDocuments(self, request, context):
        _ERR = document_search_pb2.SearchDocumentsResponse
        _t0 = time.monotonic()
        try:
            current_user, result = _auth_guard(context)
            if current_user is None:
                return _ERR(**result)

            if not request.query or not request.query.strip():
                return _ERR(error_message="query is required", error_code="INVALID_ARGUMENT")

            company_id = result
            limit = request.limit if request.limit > 0 else 10
            collection = request.collection or None

            svc = await _get_document_search_service()
            chunks = await svc.search_documents(
                query=request.query,
                company_id=company_id,
                collection=collection,
                limit=limit,
            )

            return _ERR(results=[_document_chunk_to_proto(c) for c in chunks])
        except Exception as e:
            logger.opt(exception=True).error("SearchDocuments failed: %s", e)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            logger.debug("SearchDocuments completed in %.3fs", time.monotonic() - _t0)

    # ── GetDocumentChunk ──────────────────────────────────────────────────────

    async def GetDocumentChunk(self, request, context):
        _ERR = document_search_pb2.GetDocumentChunkResponse
        _t0 = time.monotonic()
        try:
            current_user, result = _auth_guard(context)
            if current_user is None:
                return _ERR(status="error", **result)

            if not request.chunk_id or not request.chunk_id.strip():
                return _ERR(
                    status="error",
                    error_message="chunk_id is required",
                    error_code="INVALID_ARGUMENT",
                )

            company_id = result

            svc = await _get_document_search_service()
            chunk = await svc.get_document_chunk(
                chunk_id=request.chunk_id,
                company_id=company_id,
            )

            if chunk is None:
                return _ERR(
                    status="error",
                    chunk_id=request.chunk_id,
                    error_message="Document chunk not found",
                    error_code="NOT_FOUND",
                )

            return _ERR(
                status="ok",
                chunk_id=chunk.get("chunk_id", ""),
                text=chunk.get("text", ""),
                file_path=chunk.get("file_path", ""),
                language=chunk.get("language", ""),
                repository=chunk.get("repository", ""),
                branch=chunk.get("branch", ""),
                chunk_index=int(chunk.get("chunk_index", 0) or 0),
                chunk_size=int(chunk.get("chunk_size", 0) or 0),
                file_version_id=chunk.get("file_version_id", ""),
                source_node_set=chunk.get("source_node_set", ""),
                company_id=chunk.get("company_id", ""),
                description=chunk.get("description", ""),
            )
        except Exception as e:
            logger.opt(exception=True).error("GetDocumentChunk failed: %s", e)
            return _ERR(status="error", error_message=str(e), error_code="INTERNAL")
        finally:
            logger.debug("GetDocumentChunk completed in %.3fs", time.monotonic() - _t0)

    # ── SearchDocumentSummaries ───────────────────────────────────────────────

    async def SearchDocumentSummaries(self, request, context):
        _ERR = document_search_pb2.SearchDocumentSummariesResponse
        _t0 = time.monotonic()
        try:
            current_user, result = _auth_guard(context)
            if current_user is None:
                return _ERR(**result)

            if not request.query or not request.query.strip():
                return _ERR(error_message="query is required", error_code="INVALID_ARGUMENT")

            company_id = result
            limit = request.limit if request.limit > 0 else 10

            svc = await _get_document_search_service()
            summaries = await svc.search_document_summaries(
                query=request.query,
                company_id=company_id,
                limit=limit,
            )

            return _ERR(results=[_document_summary_to_proto(s) for s in summaries])
        except Exception as e:
            logger.opt(exception=True).error("SearchDocumentSummaries failed: %s", e)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            logger.debug("SearchDocumentSummaries completed in %.3fs", time.monotonic() - _t0)

    # ── TraverseDocumentGraph ─────────────────────────────────────────────────

    async def TraverseDocumentGraph(self, request, context):
        _ERR = document_search_pb2.TraverseDocumentGraphResponse
        _t0 = time.monotonic()
        try:
            current_user, result = _auth_guard(context)
            if current_user is None:
                return _ERR(**result)

            if not request.entity_name or not request.entity_name.strip():
                return _ERR(
                    error_message="entity_name is required",
                    error_code="INVALID_ARGUMENT",
                )

            company_id = result

            svc = await _get_document_search_service()
            edges = await svc.traverse_document_graph(
                entity_name=request.entity_name,
                company_id=company_id,
            )

            return _ERR(edges=[_graph_edge_to_proto(e) for e in edges])
        except Exception as e:
            logger.opt(exception=True).error("TraverseDocumentGraph failed: %s", e)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            logger.debug("TraverseDocumentGraph completed in %.3fs", time.monotonic() - _t0)

    # ── FullDocumentSearch ────────────────────────────────────────────────────

    async def FullDocumentSearch(self, request, context):
        _ERR = document_search_pb2.FullDocumentSearchResponse
        _t0 = time.monotonic()
        try:
            current_user, result = _auth_guard(context)
            if current_user is None:
                return _ERR(**result)

            if not request.query or not request.query.strip():
                return _ERR(error_message="query is required", error_code="INVALID_ARGUMENT")

            company_id = result
            limit = request.limit if request.limit > 0 else 10

            svc = await _get_document_search_service()
            result_dict = await svc.full_document_search(
                query=request.query,
                company_id=company_id,
                limit=limit,
            )

            return _ERR(
                collections=[
                    _collection_info_to_proto(c) for c in result_dict.get("collections", [])
                ],
                chunks=[_document_chunk_to_proto(c) for c in result_dict.get("chunks", [])],
                summaries=[_document_summary_to_proto(s) for s in result_dict.get("summaries", [])],
                entities=[_document_entity_to_proto(e) for e in result_dict.get("entities", [])],
            )
        except Exception as e:
            logger.opt(exception=True).error("FullDocumentSearch failed: %s", e)
            return _ERR(error_message=str(e), error_code="INTERNAL")
        finally:
            logger.debug("FullDocumentSearch completed in %.3fs", time.monotonic() - _t0)

    # ── Health ────────────────────────────────────────────────────────────────

    async def Health(self, request, context):
        return document_search_pb2.HealthResponse(
            status="ok", message="DocumentSearchService healthy"
        )

"""
Cognee gRPC Servicer

Implements CogneeServiceServicer: Cognify, Search, Prune, Health.
cognify_lock serialises cognee.add() calls across concurrent requests.
"""

import asyncio
import os
import grpc
import structlog

import cognee
from cognee.api.v1.search import SearchType

from cognee_service.auth_interceptor import current_user_context
from cognee_service.lock import dataset_locks
from cognee_service.multi_tenancy import ensure_neo4j_database, set_company_context
from cognee_service.query_expansion import expand_query
from cognee_service.cognee_patches import (
    SearchContractError,
    feeling_lucky_admin_context,
    feeling_lucky_resolved_type_context,
)
from cognee_service.generated import cognee_pb2, cognee_pb2_grpc

logger = structlog.get_logger(__name__)

# Backward-compatible module-level lock used by the unit-test fixture to swap in
# an isolated lock. Runtime uses dataset_locks for actual concurrency control.
cognify_lock = asyncio.Lock()

# ── Admin-only search modes ───────────────────────────────────────────────────
# CYPHER and NATURAL_LANGUAGE execute LLM-generated or user-supplied Cypher
# against the company's Neo4j database. While tenant-scoped, they expose all
# node data in the company's database to adversarial queries and are within-
# tenant injection risks. Gate them behind admin/superuser role.
_ADMIN_ONLY_MODES = {
    cognee_pb2.CYPHER,
    cognee_pb2.NATURAL_LANGUAGE,
}

_QUERY_EXPANSION_MODES = {
    SearchType.GRAPH_COMPLETION,
    SearchType.GRAPH_SUMMARY_COMPLETION,
    SearchType.RAG_COMPLETION,
    SearchType.TRIPLET_COMPLETION,
    SearchType.TEMPORAL,
}


def _env_bool(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() not in {"0", "false", "no", "off"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _is_admin(user) -> bool:
    """Return True if the authenticated user has admin/superuser rights."""
    if user is None:
        return False
    if user.is_superuser:
        return True
    return "admin" in (r.lower() for r in (user.roles or []))


def _scope_to_node_name(company_id: str, scope: str):
    """Translate the public scope string into cognee's node_name filter."""
    if not scope:
        return None

    if scope == "knowledge":
        if not company_id:
            logger.warning("search.scope_invalid", scope=scope, reason="missing company_id")
            return None
        return [f"{company_id}_knowledge"]

    if scope.startswith("code:"):
        parts = scope.split(":", 2)
        if len(parts) == 3 and parts[1] and parts[2]:
            return [f"{parts[1]}_{parts[2]}_code"]

    logger.warning("search.scope_invalid", scope=scope)
    return None


def _low_confidence_message(threshold: float) -> str:
    return f"no high-confidence matches above threshold {threshold:.2f}"


def _unwrap_search_payload(item):
    return item.search_result if hasattr(item, "search_result") else item


def _resolve_company_id_from_context() -> str:
    user = current_user_context.get(None)
    companies = getattr(user, "companies", None) or []
    return str(companies[0]) if companies else ""


def _search_payload_parts(payload):
    if isinstance(payload, dict):
        item_id = str(payload.get("node_id") or payload.get("id") or "")
        item_text = str(payload.get("text", payload.get("description", str(payload))))
        item_score = float(payload.get("score", 0.0))
        meta = {
            k: str(v)
            for k, v in payload.items()
            if k
            not in {
                "node_id",
                "id",
                "text",
                "description",
                "score",
                "header",
                "metadata",
                "created_at",
                "updated_at",
                "ontology_valid",
                "version",
                "topological_rank",
                "type",
                "belongs_to_set",
                "source_pipeline",
                "source_task",
                "source_user",
                "database_name",
            }
        }
    else:
        item_id = str(getattr(payload, "node_id", getattr(payload, "id", "")))
        item_text = str(getattr(payload, "text", getattr(payload, "description", str(payload))))
        item_score = float(getattr(payload, "score", 0.0))
        meta = {}
    return item_id, item_text, item_score, meta


def _merge_search_results(result_groups: list[list], limit: int) -> list:
    merged: dict[str, dict] = {}
    order = 0
    for group in result_groups:
        for item in group or []:
            payload = _unwrap_search_payload(item)
            item_id, item_text, item_score, _ = _search_payload_parts(payload)
            if not item_text.strip():
                continue
            key = item_id or f"__anon__:{hash(item_text)}"
            current = merged.get(key)
            if current is None or item_score > current["score"]:
                merged[key] = {"payload": payload, "score": item_score, "order": order}
            order += 1

    ordered = sorted(merged.values(), key=lambda item: (-item["score"], item["order"]))
    return [item["payload"] for item in ordered[:limit]]


def _should_expand_query(query_type, scope: str, query: str) -> bool:
    if not _env_bool("QUERY_EXPANSION_ENABLED", "true"):
        return False
    if query_type not in _QUERY_EXPANSION_MODES:
        return False
    if not scope:
        return False
    min_words = max(1, _env_int("QUERY_EXPANSION_MIN_WORDS", 5))
    return len(query.split()) >= min_words


class CogneeServicer(cognee_pb2_grpc.CogneeServiceServicer):
    """gRPC servicer for Cognee knowledge enrichment."""

    # ── Search type mapping ───────────────────────────────────────────────────
    # Maps gRPC proto enum int values → cognee Python SearchType enum members.
    # All 14 cognee SearchType modes are represented.
    # Backwards compatible: existing callers using values 0-4 are unaffected.
    _SEARCH_TYPE_MAP = {
        cognee_pb2.GRAPH_COMPLETION: SearchType.GRAPH_COMPLETION,
        cognee_pb2.TRIPLET_COMPLETION: SearchType.TRIPLET_COMPLETION,
        cognee_pb2.CHUNKS: SearchType.CHUNKS,
        cognee_pb2.RAG_COMPLETION: SearchType.RAG_COMPLETION,
        cognee_pb2.SUMMARIES: SearchType.SUMMARIES,
        cognee_pb2.GRAPH_SUMMARY_COMPLETION: SearchType.GRAPH_SUMMARY_COMPLETION,
        cognee_pb2.CYPHER: SearchType.CYPHER,  # ADMIN ONLY
        cognee_pb2.NATURAL_LANGUAGE: SearchType.NATURAL_LANGUAGE,  # ADMIN ONLY
        cognee_pb2.GRAPH_COMPLETION_COT: SearchType.GRAPH_COMPLETION_COT,
        cognee_pb2.GRAPH_COMPLETION_CONTEXT_EXTENSION: SearchType.GRAPH_COMPLETION_CONTEXT_EXTENSION,
        cognee_pb2.FEELING_LUCKY: SearchType.FEELING_LUCKY,  # logs actual type
        cognee_pb2.TEMPORAL: SearchType.TEMPORAL,
        cognee_pb2.CODING_RULES: SearchType.CODING_RULES,
        cognee_pb2.CHUNKS_LEXICAL: SearchType.CHUNKS_LEXICAL,
    }

    # ── Cognify ──────────────────────────────────────────────────────────────

    async def Cognify(
        self,
        request: cognee_pb2.CognifyRequest,
        context,
    ) -> cognee_pb2.CognifyResponse:
        """Add text to dataset and run graph enrichment (cognify)."""
        if not request.company_id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "company_id is required")
            return cognee_pb2.CognifyResponse(success=False, message="company_id is required")

        dataset_name = request.company_id
        logger.info(
            "cognify.start",
            dataset=dataset_name,
            entity_id=request.entity_id,
        )
        try:
            company_id = getattr(request, "company_id", "") or "default"
            lock = await dataset_locks.acquire(company_id, dataset_name)
            try:
                await cognee.add(request.text, dataset_name=dataset_name)
                await cognee.cognify(datasets=[dataset_name])
            finally:
                lock.release()

            logger.info(
                "cognify.complete",
                dataset=dataset_name,
                entity_id=request.entity_id,
            )
            return cognee_pb2.CognifyResponse(
                success=True,
                message="cognify complete",
                dataset_name=dataset_name,
                entity_id=request.entity_id,
            )
        except Exception as exc:
            logger.error(
                "cognify.error",
                dataset=dataset_name,
                entity_id=request.entity_id,
                error=str(exc),
                exc_info=True,
            )
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    # ── Search ───────────────────────────────────────────────────────────────

    async def _run_search(
        self,
        request,
        context,
        response_cls,
    ):
        logger.info("search.start", query=request.query, limit=request.limit)
        try:
            limit = request.limit if request.limit > 0 else 10

            if request.search_type in _ADMIN_ONLY_MODES:
                user = current_user_context.get(None)
                if not _is_admin(user):
                    mode_name = cognee_pb2.SearchType.Name(request.search_type)
                    logger.warning(
                        "search.access_denied",
                        search_type=mode_name,
                        user_id=getattr(user, "user_id", "<unauthenticated>"),
                    )
                    await context.abort(
                        grpc.StatusCode.PERMISSION_DENIED,
                        f"Search mode {mode_name} requires admin role",
                    )
                    return response_cls(
                        success=False, message=f"Search mode {mode_name} requires admin role"
                    )

            await ensure_neo4j_database(request.company_id)
            set_company_context(request.company_id)
            logger.info("search.company_context_set", company_id=request.company_id)

            query_type = self._SEARCH_TYPE_MAP.get(request.search_type, SearchType.GRAPH_COMPLETION)
            logger.info("search.query_type", search_type=query_type.name)

            node_name = None
            if getattr(request, "scope", ""):
                node_name = _scope_to_node_name(request.company_id, request.scope)
                logger.info("search.scope_filter", scope=request.scope, node_name=node_name)
            elif request.project_id or request.branch:
                logger.warning(
                    "search.scope_deprecated",
                    project_id=request.project_id,
                    branch=request.branch,
                    note="scope is empty; legacy project_id/branch scoping is ignored",
                )

            # Search-quality tuning. Defaults below are larger than cognee
            # stock to produce more thorough answers out of the box; callers
            # can override via the proto fields (zero / empty = "use default").
            _DEFAULT_WIDE_SEARCH_TOP_K = 200  # cognee default = 100
            _DEFAULT_TOP_K = 20  # cognee default = 5–10
            extra_kwargs = {
                "wide_search_top_k": request.wide_search_top_k or _DEFAULT_WIDE_SEARCH_TOP_K,
            }
            if request.triplet_distance_penalty != 0.0:
                extra_kwargs["triplet_distance_penalty"] = request.triplet_distance_penalty
            # Forward optional answer-quality knobs (Flexible* requests only;
            # legacy SearchRequest doesn't carry these fields).
            req_system_prompt = str(getattr(request, "system_prompt", "") or "").strip()
            if req_system_prompt:
                extra_kwargs["system_prompt"] = req_system_prompt
            requested_top_k = int(getattr(request, "top_k", 0) or 0)
            effective_top_k = requested_top_k if requested_top_k > 0 else _DEFAULT_TOP_K
            extra_kwargs["top_k"] = effective_top_k
            logger.info(
                "search.tuning_params",
                wide_search_top_k=extra_kwargs["wide_search_top_k"],
                top_k=effective_top_k,
                has_system_prompt=bool(extra_kwargs.get("system_prompt")),
            )

            expanded_queries = [request.query]
            query_expansion_ran = _should_expand_query(
                query_type, getattr(request, "scope", ""), request.query
            )
            if query_expansion_ran:
                expanded_queries = await expand_query(request.query)
                expanded_queries = expanded_queries[
                    : max(1, _env_int("QUERY_EXPANSION_MAX_VARIANTS", 3)) + 1
                ]
                logger.info(
                    "search.query_expansion",
                    original_query=request.query,
                    expanded_queries=expanded_queries,
                )

            user = current_user_context.get(None)
            _fl_token = feeling_lucky_admin_context.set(_is_admin(user))
            # Note: top_k is supplied via extra_kwargs (not as the positional
            # arg here) so the tuning knob actually controls per-retriever
            # context size. The proto's `limit` is enforced as a result-count
            # cap downstream when shaping the response.
            try:
                if len(expanded_queries) == 1:
                    results = await cognee.search(
                        query_text=request.query,
                        query_type=query_type,
                        datasets=None,
                        only_context=request.only_context,
                        node_name=node_name,
                        **extra_kwargs,
                    )
                else:
                    search_tasks = [
                        cognee.search(
                            query_text=expanded_query,
                            query_type=query_type,
                            datasets=None,
                            only_context=request.only_context,
                            node_name=node_name,
                            **extra_kwargs,
                        )
                        for expanded_query in expanded_queries
                    ]
                    results_by_query = await asyncio.gather(*search_tasks, return_exceptions=True)
                    search_groups = []
                    for expanded_query, expanded_results in zip(expanded_queries, results_by_query):
                        if isinstance(expanded_results, Exception):
                            logger.warning(
                                "search.query_expansion.search_error",
                                query=expanded_query,
                                error=str(expanded_results),
                            )
                            search_groups.append([])
                        else:
                            search_groups.append(list(expanded_results or []))
                    results = _merge_search_results(search_groups, limit)
                if query_expansion_ran:
                    logger.info(
                        "search.query_expansion.complete",
                        original_query=request.query,
                        expanded_queries=expanded_queries,
                        merged_hit_count=len(results),
                    )
            except PermissionError as perm_exc:
                logger.warning("search.feeling_lucky.permission_denied", error=str(perm_exc))
                return response_cls(
                    success=False,
                    message=str(perm_exc),
                    error_code="PERMISSION_DENIED",
                    actual_search_type=feeling_lucky_resolved_type_context.get(),
                )
            except SearchContractError as contract_exc:
                logger.warning(
                    "search.contract_error",
                    error_code=contract_exc.error_code,
                    error=str(contract_exc),
                )
                return response_cls(
                    success=False,
                    message=str(contract_exc),
                    error_code=contract_exc.error_code,
                    actual_search_type=feeling_lucky_resolved_type_context.get(),
                )
            finally:
                feeling_lucky_admin_context.reset(_fl_token)

            actual_search_type = ""
            if query_type == SearchType.FEELING_LUCKY:
                resolved = None
                for item in results or []:
                    payload = item.search_result if hasattr(item, "search_result") else item
                    if isinstance(payload, dict):
                        resolved = payload.get("search_type") or payload.get("query_type")
                    if resolved:
                        break
                if resolved:
                    actual_search_type = str(resolved)
                    logger.info(
                        "search.feeling_lucky.resolved", actual_search_type=actual_search_type
                    )
                else:
                    actual_search_type = (
                        feeling_lucky_resolved_type_context.get() or "FEELING_LUCKY:<unknown>"
                    )
                    logger.info(
                        "search.feeling_lucky.unknown",
                        note="cognee does not expose selected mode in SearchResult; see TODO in servicer.py",
                    )

            grpc_results = []
            for item in results or []:
                if len(grpc_results) >= limit:
                    break
                payload = _unwrap_search_payload(item)
                item_id, item_text, item_score, meta = _search_payload_parts(payload)
                if not item_text.strip():
                    continue
                grpc_results.append(
                    cognee_pb2.SearchResult(
                        id=item_id, text=item_text, score=item_score, metadata=meta
                    )
                )

            logger.info("search.complete", result_count=len(grpc_results))
            message = "search complete"
            if query_type == SearchType.CHUNKS and not grpc_results:
                try:
                    threshold = float(os.getenv("COGNEE_CHUNKS_MIN_SCORE", "0.65"))
                except (TypeError, ValueError):
                    threshold = 0.65
                message = _low_confidence_message(threshold)
            return response_cls(
                success=True,
                message=message,
                results=grpc_results,
                actual_search_type=actual_search_type,
                error_code="",
            )
        except Exception as exc:
            logger.error("search.error", error=str(exc), exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    async def Search(self, request: cognee_pb2.SearchRequest, context) -> cognee_pb2.SearchResponse:
        if not request.company_id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "company_id is required")
            return cognee_pb2.SearchResponse(success=False, message="company_id is required")
        return await self._run_search(request, context, cognee_pb2.SearchResponse)

    async def FlexibleCogneeSearch(
        self,
        request: cognee_pb2.FlexibleCogneeSearchRequest,
        context,
    ) -> cognee_pb2.FlexibleCogneeSearchResponse:
        company_id = _resolve_company_id_from_context()
        if not company_id:
            return cognee_pb2.FlexibleCogneeSearchResponse(
                success=False,
                message="authenticated user has no company context",
                error_code="INVALID_ARGUMENT",
            )
        internal_request = cognee_pb2.SearchRequest(
            query=request.query,
            limit=request.limit,
            company_id=company_id,
            search_type=request.search_type,
            only_context=request.only_context,
            dataset_name=request.dataset_name,
            branch=request.branch,
            wide_search_top_k=request.wide_search_top_k,
            triplet_distance_penalty=request.triplet_distance_penalty,
            scope="knowledge",
            system_prompt=request.system_prompt,
            top_k=request.top_k,
        )
        return await self._run_search(
            internal_request, context, cognee_pb2.FlexibleCogneeSearchResponse
        )

    async def FlexibleCodeSearch(
        self,
        request: cognee_pb2.FlexibleCodeSearchRequest,
        context,
    ) -> cognee_pb2.FlexibleCodeSearchResponse:
        if not str(request.project_id).strip() or not str(request.project_name).strip():
            return cognee_pb2.FlexibleCodeSearchResponse(
                success=False,
                message="project_id and project_name are required",
                error_code="INVALID_ARGUMENT",
            )
        company_id = _resolve_company_id_from_context()
        if not company_id:
            return cognee_pb2.FlexibleCodeSearchResponse(
                success=False,
                message="authenticated user has no company context",
                error_code="INVALID_ARGUMENT",
            )
        internal_request = cognee_pb2.SearchRequest(
            query=request.query,
            limit=request.limit,
            company_id=company_id,
            search_type=request.search_type,
            only_context=request.only_context,
            dataset_name=request.dataset_name,
            branch=request.branch,
            project_id=request.project_id,
            wide_search_top_k=request.wide_search_top_k,
            triplet_distance_penalty=request.triplet_distance_penalty,
            scope=f"code:{request.project_id}:{request.project_name}",
            system_prompt=request.system_prompt,
            top_k=request.top_k,
        )
        return await self._run_search(
            internal_request, context, cognee_pb2.FlexibleCodeSearchResponse
        )

    # ── Prune ────────────────────────────────────────────────────────────────

    async def Prune(
        self,
        request: cognee_pb2.PruneRequest,
        context,
    ) -> cognee_pb2.PruneResponse:
        """Prune (delete) a dataset or all datasets from the knowledge graph."""
        if not request.company_id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "company_id is required")
            return cognee_pb2.PruneResponse(success=False, message="company_id is required")

        dataset_name = request.company_id
        logger.info("prune.start", dataset=dataset_name)
        try:
            await cognee.prune.prune_data(dataset_name=dataset_name)

            # Explicitly delete all Qdrant collections to ensure full cleanup
            from qdrant_client import AsyncQdrantClient as _QdrantClient

            qdrant_url = os.environ.get("VECTOR_DB_URL", "http://qdrant:6333")
            _qc = _QdrantClient(url=qdrant_url)
            try:
                _cols = await _qc.get_collections()
                for _col in _cols.collections:
                    await _qc.delete_collection(_col.name)
            finally:
                await _qc.close()

            logger.info("prune.complete", dataset=dataset_name)
            return cognee_pb2.PruneResponse(success=True, message="prune complete")
        except Exception as exc:
            logger.error("prune.error", dataset=dataset_name, error=str(exc), exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    # ── Health ───────────────────────────────────────────────────────────────

    async def Health(
        self,
        request: cognee_pb2.HealthRequest,
        context,
    ) -> cognee_pb2.HealthResponse:
        """Basic health probe — returns 'ok' if the servicer is alive."""
        return cognee_pb2.HealthResponse(status="ok", message="cognee service healthy")

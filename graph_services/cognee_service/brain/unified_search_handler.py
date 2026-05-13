"""
Brain v2 — UnifiedSearch gRPC handler

Searches the company Cognee graph (Neo4j + Qdrant) with dataset-level filtering
via Cognee's ``node_name`` parameter which maps to Qdrant's ``belongs_to_set``
payload filter.

Dataset filtering works WITHOUT ``ENABLE_BACKEND_ACCESS_CONTROL=true`` because
it uses a separate mechanism: each data point ingested by the brain events
consumer is tagged with a ``node_set`` (e.g. ``knowledge_{project_id}``), and
Qdrant's ``belongs_to_set`` field is used to filter at query time.

Filter behaviour:
- ``types`` is empty → search the entire graph (no ``node_name`` filter)
- ``types`` specified → run one search per type with ``node_name`` filtering,
  aggregate results, and tag each result with its source ``entity_type``
"""

import asyncio
import time

import grpc
import structlog

import cognee
from cognee.api.v1.search import SearchType

from cognee_service.generated import brain_pb2
from cognee_service.multi_tenancy import (
    ensure_neo4j_database,
    set_company_context,
)

logger = structlog.get_logger(__name__)

# Default search type for unified search.
# GRAPH_COMPLETION returns graph-context results from the knowledge graph.
_DEFAULT_SEARCH_TYPE = SearchType.GRAPH_COMPLETION

# Maximum results cap to prevent runaway queries.
_MAX_RESULTS = 100
_DEFAULT_RESULTS = 20

# ── Proto ``types`` value → entity_type used in Cognee node_sets ─────────────
# The brain events consumer tags data points with node_sets like
# ``{entity_type}_{scope_id}`` (see BrainEventProcessor.build_node_set).
# Proto ``types`` values may be plural ("lessons", "entries") but the
# entity_type in node_sets is singular ("lesson", "entry").
_PROTO_TYPE_TO_ENTITY: dict[str, str] = {
    "knowledge": "knowledge",
    "expertise": "expertise",
    "lessons": "lesson",
    "lesson": "lesson",
    "entries": "entry",
    "entry": "entry",
    "engagements": "engagement",
    "engagement": "engagement",
    # Code uses a different naming scheme: ``code_{project_id}``
    "code": "code",
}


def _unwrap_cognee_result(
    item,
    entity_type: str = "",
) -> brain_pb2.UnifiedSearchResult | None:
    """Convert a single Cognee search result to a UnifiedSearchResult proto.

    Returns ``None`` for empty results that should be skipped.

    Args:
        item: A raw Cognee search result (may have ``.search_result`` wrapper).
        entity_type: The KGRAG entity type to tag the result with
            (e.g. ``"knowledge"``, ``"expertise"``).
    """
    payload = item.search_result if hasattr(item, "search_result") else item

    if isinstance(payload, dict):
        item_id = str(payload.get("id", ""))
        item_text = str(payload.get("text", payload.get("description", str(payload))))
        # Extract score BEFORE building meta dict so it doesn't appear in metadata
        item_score = float(payload.get("score", 0.0))
        meta = {
            k: str(v) for k, v in payload.items() if k not in ("id", "text", "description", "score")
        }
    else:
        item_id = str(getattr(payload, "id", ""))
        item_text = str(getattr(payload, "text", getattr(payload, "description", str(payload))))
        item_score = 0.0
        meta = {}

    # Skip empty results (only_context returns '' when no graph context found)
    if not item_text.strip():
        return None

    # Build a Struct from metadata dict
    metadata_struct = None
    if meta:
        from cognee_service.brain._proto_helpers import dict_to_struct

        metadata_struct = dict_to_struct(meta)

    return brain_pb2.UnifiedSearchResult(
        entity_type=entity_type,
        id=item_id,
        title="",  # Cognee results don't carry titles
        snippet=item_text,
        score=item_score,
        metadata=metadata_struct,
    )


def _build_node_names(
    types: list[str],
    scope_id: str,
) -> dict[str, list[str]] | None:
    """Build a mapping of proto_type → node_name list for each requested type.

    Returns ``None`` if no valid types are requested (search everything).

    The node_set naming convention matches ``BrainEventProcessor.build_node_set``
    and ``CogneeProcessor.build_node_set``:
    - Brain entities: ``{entity_type}_{scope_id}`` (e.g. ``knowledge_{project_id}``)
    - Code entities: ``code_{project_id}``

    Args:
        types: Proto ``types`` field values (e.g. ``["knowledge", "expertise"]``).
        scope_id: The project_id (preferred) or company_id for scoping.

    Returns:
        Dict mapping proto type name → list of node_names for Cognee search,
        or None if no valid types found.
    """
    if not types:
        return None

    mapping: dict[str, list[str]] = {}
    for proto_type in types:
        entity = _PROTO_TYPE_TO_ENTITY.get(proto_type)
        if entity is None:
            logger.warning(
                "unified_search.unknown_type",
                proto_type=proto_type,
                known_types=list(_PROTO_TYPE_TO_ENTITY.keys()),
            )
            continue
        mapping[proto_type] = [f"{entity}_{scope_id}"]

    return mapping if mapping else None


async def _search_single_type(
    query: str,
    node_name: list[str],
    top_k: int,
) -> list:
    """Run a single Cognee search with a node_name filter.

    Isolated for cleaner parallel execution and error handling.
    """
    return await cognee.search(
        query_text=query,
        query_type=_DEFAULT_SEARCH_TYPE,
        top_k=top_k,
        only_context=True,
        node_name=node_name,
    )


async def handle_unified_search(
    request: brain_pb2.UnifiedSearchRequest,
    context,
) -> brain_pb2.UnifiedSearchResponse:
    """Search the company Cognee graph with optional dataset-level filtering.

    Filtering by ``request.types`` uses Cognee's ``node_name`` parameter
    which maps to Qdrant's ``belongs_to_set`` payload filter. This works
    regardless of the ``ENABLE_BACKEND_ACCESS_CONTROL`` setting.

    Behaviour:
    - ``types`` empty → single unfiltered search (all datasets)
    - ``types`` specified → parallel per-type searches, results tagged with
      ``entity_type``
    """
    company_id = request.company_id
    if not company_id:
        await context.abort(
            grpc.StatusCode.INVALID_ARGUMENT,
            "company_id is required",
        )
    if not request.query or not request.query.strip():
        await context.abort(
            grpc.StatusCode.INVALID_ARGUMENT,
            "query is required",
        )

    limit = request.limit if request.limit > 0 else _DEFAULT_RESULTS
    limit = min(limit, _MAX_RESULTS)

    requested_types = list(request.types)
    scope_id = request.project_id or company_id

    logger.info(
        "unified_search.start",
        company_id=company_id,
        project_id=request.project_id,
        query=request.query,
        limit=limit,
        types=requested_types,
        scope_id=scope_id,
    )
    t0 = time.time()

    try:
        # ── Multi-tenancy: scope to company Neo4j + Qdrant ──────────────
        await ensure_neo4j_database(company_id)
        set_company_context(company_id)

        # ── Build node_name filters ─────────────────────────────────────
        type_to_node_names = _build_node_names(requested_types, scope_id)

        grpc_results: list[brain_pb2.UnifiedSearchResult] = []
        skipped_empty = 0

        if type_to_node_names is None:
            # ── No type filter: search the entire graph ─────────────────
            logger.debug(
                "unified_search.unfiltered_search",
                query_type=getattr(_DEFAULT_SEARCH_TYPE, "name", str(_DEFAULT_SEARCH_TYPE)),
            )
            results = await cognee.search(
                query_text=request.query,
                query_type=_DEFAULT_SEARCH_TYPE,
                top_k=limit,
                only_context=True,
            )
            search_duration = time.time() - t0
            logger.debug(
                "unified_search.unfiltered_search_complete",
                raw_result_count=len(results) if results else 0,
                duration_s=round(search_duration, 2),
            )

            for item in results or []:
                result = _unwrap_cognee_result(item)
                if result is None:
                    skipped_empty += 1
                    continue
                grpc_results.append(result)
        else:
            # ── Per-type filtered search ────────────────────────────────
            # Run one search per type in parallel. Each search uses
            # node_name to filter Qdrant's belongs_to_set field.
            type_keys = list(type_to_node_names.keys())
            per_type_limit = limit  # each type gets the full limit

            logger.debug(
                "unified_search.filtered_search_start",
                type_count=len(type_keys),
                types=type_keys,
                node_names={k: v for k, v in type_to_node_names.items()},
            )

            search_tasks = [
                _search_single_type(
                    query=request.query,
                    node_name=type_to_node_names[type_key],
                    top_k=per_type_limit,
                )
                for type_key in type_keys
            ]

            results_per_type = await asyncio.gather(*search_tasks, return_exceptions=True)
            search_duration = time.time() - t0

            for type_key, type_results in zip(type_keys, results_per_type):
                if isinstance(type_results, Exception):
                    logger.error(
                        "unified_search.type_search_error",
                        type=type_key,
                        error=str(type_results),
                    )
                    continue

                type_result_count = len(type_results) if type_results else 0
                logger.debug(
                    "unified_search.type_search_complete",
                    type=type_key,
                    raw_result_count=type_result_count,
                )

                for item in type_results or []:
                    result = _unwrap_cognee_result(item, entity_type=type_key)
                    if result is None:
                        skipped_empty += 1
                        continue
                    grpc_results.append(result)

            logger.debug(
                "unified_search.filtered_search_complete",
                total_raw_results=sum(
                    len(r) if not isinstance(r, Exception) and r else 0 for r in results_per_type
                ),
                duration_s=round(search_duration, 2),
            )

        duration = time.time() - t0
        logger.info(
            "unified_search.complete",
            result_count=len(grpc_results),
            skipped_empty=skipped_empty,
            filtered=type_to_node_names is not None,
            types=requested_types,
            duration_s=round(duration, 2),
        )

        return brain_pb2.UnifiedSearchResponse(
            results=grpc_results,
            total_count=len(grpc_results),
        )

    except Exception as exc:
        duration = time.time() - t0
        logger.error(
            "unified_search.error",
            company_id=company_id,
            error=str(exc),
            duration_s=round(duration, 2),
            exc_info=True,
        )
        await context.abort(
            grpc.StatusCode.INTERNAL,
            "Search failed due to an internal error. Please try again.",
        )

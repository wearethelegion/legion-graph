"""REST passthrough for cognee FlexibleCodeSearch.

Mirrors the shape of `api/routes/brain.py:search_brain` but for code chunks.
Code is project-scoped (unlike brain content, which is company-scoped) so the
caller must supply project_id and project_name. company_id is derived from
the JWT — never client-supplied.
"""

from __future__ import annotations

from typing import Any, Dict
from uuid import UUID

import grpc
from fastapi import APIRouter, Depends, HTTPException, Request

from api.auth import CurrentUser, get_current_user
from api.services.cognee_service import CogneeGrpcClient

# Reuse the existing helpers in brain.py rather than duplicate JWT minting and
# company resolution. They are pure helpers that don't depend on the brain
# router itself.
from api.routes.brain import _mint_cognee_token, _require_company
from api.routes._search_presets import resolve_depth_preset

# Pipeline ingestion uses ProjectNameResolver._slugify() to build node_set
# names like "<project_id>_<slug>_code". To search the same scope we MUST
# apply the identical slug transform — otherwise raw names like "vet_ui"
# never match the indexed "vet-ui" scope.
from shared.slugify import slugify


router = APIRouter(prefix="/api/v1/code", tags=["code"])


# Allowed search types — same whitelist the brain search endpoint uses.
_VALID_SEARCH_TYPES = {
    "GRAPH_COMPLETION",
    "TRIPLET_COMPLETION",
    "CHUNKS",
    "RAG_COMPLETION",
    "SUMMARIES",
    "GRAPH_SUMMARY_COMPLETION",
    "GRAPH_COMPLETION_COT",
    "GRAPH_COMPLETION_CONTEXT_EXTENSION",
    "FEELING_LUCKY",
    "TEMPORAL",
    "CHUNKS_LEXICAL",
}


async def _get_cognee_client(request: Request) -> CogneeGrpcClient:
    client = getattr(request.app.state, "cognee_service", None)
    if client is None or getattr(client, "_stub", None) is None:
        raise HTTPException(status_code=503, detail="Cognee service not initialised")
    return client


@router.post("/search")
async def search_code(
    payload: Dict[str, Any],
    current_user: CurrentUser = Depends(get_current_user),
    cognee: CogneeGrpcClient = Depends(_get_cognee_client),
) -> Dict[str, Any]:
    """Project-scoped code search via cognee FlexibleCodeSearch.

    Body:
        {
            "query":       "<search text>",
            "project_id":  "<UUID>",
            "project_name":"<short slug>",
            "search_type": "CHUNKS_LEXICAL" (default) | "CHUNKS" | "GRAPH_COMPLETION" | ...,
            "limit":       1..100 (default 10)
        }

    company_id is derived from the JWT.
    """
    _require_company(current_user)

    query = str(payload.get("query") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    project_id_raw = str(payload.get("project_id") or "").strip()
    if not project_id_raw:
        raise HTTPException(status_code=400, detail="project_id is required")
    try:
        UUID(project_id_raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="project_id must be a valid UUID")

    project_name_raw = str(payload.get("project_name") or "").strip()
    if not project_name_raw:
        raise HTTPException(status_code=400, detail="project_name is required")
    # Apply the same slug transform the ingestion pipeline uses so the search
    # scope matches the indexed node_set name. e.g. "vet_ui" → "vet-ui".
    project_name = slugify(project_name_raw) or project_name_raw

    search_type = str(payload.get("search_type") or "CHUNKS_LEXICAL").upper()
    if search_type not in _VALID_SEARCH_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported search_type. Allowed: {sorted(_VALID_SEARCH_TYPES)}",
        )

    raw_limit = payload.get("limit", 10)
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="limit must be an integer")
    limit = max(1, min(limit, 100))

    quality = resolve_depth_preset(payload)

    try:
        return await cognee.flexible_code_search(
            query=query,
            project_id=project_id_raw,
            project_name=project_name,
            search_type=search_type,
            limit=limit,
            system_prompt=quality["system_prompt"],
            top_k=quality["top_k"],
            wide_search_top_k=quality["wide_search_top_k"],
            authorization_header=_mint_cognee_token(current_user),
        )
    except grpc.aio.AioRpcError as exc:
        status_map = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.PERMISSION_DENIED: 403,
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.UNIMPLEMENTED: 501,
        }
        raise HTTPException(
            status_code=status_map.get(exc.code(), 500),
            detail=exc.details() or "Code search failed",
        ) from exc

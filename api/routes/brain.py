"""REST routes for the additive BrainContentService."""

from __future__ import annotations

from enum import Enum
import os
import time
from uuid import uuid4
from typing import Any, Dict, Optional

import grpc
from fastapi import APIRouter, Depends, Query, Request, status

from fastapi import HTTPException
import jwt

from api.auth import CurrentUser, get_current_user
from api.services.brain_content_service import BrainContentGrpcClient
from api.services.cognee_service import CogneeGrpcClient
from api.routes._search_presets import resolve_depth_preset


class BrainContentKind(str, Enum):
    KNOWLEDGE = "KNOWLEDGE"
    EXPERTISE = "EXPERTISE"
    LESSON = "LESSON"


_KIND_TO_PROTO = {
    BrainContentKind.EXPERTISE: 1,
    BrainContentKind.KNOWLEDGE: 2,
    BrainContentKind.LESSON: 3,
}


router = APIRouter(prefix="/api/v1/brain", tags=["brain"])


def _kind_value(kind: BrainContentKind) -> int:
    return _KIND_TO_PROTO[kind]


def _parse_kind(raw: str) -> BrainContentKind:
    try:
        return BrainContentKind(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid kind: {raw}") from exc


async def get_brain_content_service(request: Request) -> BrainContentGrpcClient:
    service = getattr(request.app.state, "brain_content_service", None)
    if service is None or getattr(service, "_stub", None) is None:
        raise HTTPException(status_code=503, detail="Brain content service not initialised")
    return service


def _company_id(user: CurrentUser) -> Optional[str]:
    return user.companies[0] if user.companies else None


def _require_company(user: CurrentUser) -> str:
    company_id = _company_id(user)
    if not company_id:
        raise HTTPException(status_code=403, detail="Access denied to this company")
    return company_id


def _mint_cognee_token(user: CurrentUser) -> str:
    secret = os.getenv("COGNEE_JWT_SECRET_KEY", "CHANGE_THIS_IN_PRODUCTION_PLEASE").encode()
    now = int(time.time())
    payload = {
        "sub": user.user_id,
        "email": user.email,
        "roles": user.roles,
        "companies": user.companies,
        "is_superuser": user.is_superuser,
        "type": "access",
        "iat": now,
        "exp": now + 300,
        "jti": str(uuid4()),
    }
    return f"Bearer {jwt.encode(payload, secret, algorithm='HS256')}"


_KIND_EXTRA_FIELDS = (
    "when_to_use",
    "symptom",
    "root_cause",
    "solution",
    "prevention",
    "severity",
)


def _extras_from_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    return {
        k: str(payload[k])
        for k in _KIND_EXTRA_FIELDS
        if payload.get(k) is not None and str(payload.get(k) or "").strip() != ""
    }


@router.post("", status_code=status.HTTP_201_CREATED)
async def add_to_brain(
    payload: Dict[str, Any],
    current_user: CurrentUser = Depends(get_current_user),
    service: BrainContentGrpcClient = Depends(get_brain_content_service),
) -> Dict[str, Any]:
    if not payload.get("kind") or not payload.get("title") or not payload.get("content"):
        raise HTTPException(status_code=400, detail="kind, title, and content are required")
    _require_company(current_user)
    result = await service.add_to_brain(
        kind=_kind_value(_parse_kind(str(payload["kind"]))),
        title=str(payload["title"]),
        content=str(payload["content"]),
        metadata={str(k): str(v) for k, v in (payload.get("metadata") or {}).items()},
        extras=_extras_from_payload(payload),
        authorization_header=_mint_cognee_token(current_user),
    )
    return result


@router.put("/{content_id}")
async def update_brain(
    content_id: str,
    payload: Dict[str, Any],
    current_user: CurrentUser = Depends(get_current_user),
    service: BrainContentGrpcClient = Depends(get_brain_content_service),
) -> Dict[str, Any]:
    _require_company(current_user)
    if not payload.get("kind"):
        raise HTTPException(status_code=400, detail="kind is required")
    return await service.update_brain(
        id=content_id,
        kind=_kind_value(_parse_kind(str(payload["kind"]))),
        title=str(payload.get("title") or ""),
        content=str(payload.get("content") or ""),
        metadata={str(k): str(v) for k, v in (payload.get("metadata") or {}).items()},
        extras=_extras_from_payload(payload),
        authorization_header=_mint_cognee_token(current_user),
    )


@router.delete("/{content_id}")
async def delete_brain(
    content_id: str,
    kind: BrainContentKind = Query(...),
    current_user: CurrentUser = Depends(get_current_user),
    service: BrainContentGrpcClient = Depends(get_brain_content_service),
) -> Dict[str, Any]:
    _require_company(current_user)
    return await service.delete_from_brain(
        id=content_id,
        kind=_kind_value(kind),
        authorization_header=_mint_cognee_token(current_user),
    )


@router.get("/{content_id}")
async def get_brain(
    content_id: str,
    kind: BrainContentKind = Query(...),
    current_user: CurrentUser = Depends(get_current_user),
    service: BrainContentGrpcClient = Depends(get_brain_content_service),
) -> Dict[str, Any]:
    _require_company(current_user)
    return await service.get_brain_content(
        id=content_id,
        kind=_kind_value(kind),
        authorization_header=_mint_cognee_token(current_user),
    )


@router.get("")
async def list_brain(
    kind: BrainContentKind = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    current_user: CurrentUser = Depends(get_current_user),
    service: BrainContentGrpcClient = Depends(get_brain_content_service),
) -> Dict[str, Any]:
    _require_company(current_user)
    return await service.list_brain_content(
        kind=_kind_value(kind),
        page=page,
        page_size=page_size,
        authorization_header=_mint_cognee_token(current_user),
    )


# ── Search passthrough to cognee FlexibleCogneeSearch ────────────────────────


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
        raise HTTPException(status_code=503, detail="Cognee search service not initialised")
    return client


@router.post("/search")
async def search_brain(
    payload: Dict[str, Any],
    current_user: CurrentUser = Depends(get_current_user),
    cognee: CogneeGrpcClient = Depends(_get_cognee_client),
) -> Dict[str, Any]:
    """Hybrid semantic search over the company's brain content.

    Body:
        {
            "query":         "<search text>",
            "search_type":   "CHUNKS" | "CHUNKS_LEXICAL" | "GRAPH_COMPLETION" | ...,
            "limit":         1..100  (default 10),
            "depth":         "concise" | "standard" | "thorough"  (default "thorough"),
            "system_prompt": "..."   (raw override; takes precedence over depth),
            "top_k":         <int>   (raw override),
            "wide_search_top_k": <int> (raw override)
        }

    company_id is derived from the JWT — no client-supplied scope.
    """
    _require_company(current_user)

    query = str(payload.get("query") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    search_type = str(payload.get("search_type") or "CHUNKS").upper()
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
        return await cognee.flexible_search(
            query=query,
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
            detail=exc.details() or "Brain search failed",
        ) from exc

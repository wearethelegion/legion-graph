"""BrainContentService gRPC servicer."""

from __future__ import annotations

from typing import Any, Dict, Optional

import asyncpg
import grpc
import structlog

from cognee_service.auth_interceptor import current_user_context
from cognee_service.brain._proto_helpers import to_timestamp
from cognee_service.brain.kafka_producer import publish_brain_event
from cognee_service.brain_content.repositories import (
    BrainContentRepositories,
    KIND_EXPERTISE,
    KIND_KNOWLEDGE,
    KIND_LESSON,
)
from cognee_service.generated import brain_pb2, brain_pb2_grpc

logger = structlog.get_logger(__name__)

_CONTENT_BY_KIND = {
    KIND_KNOWLEDGE: "content",
    KIND_EXPERTISE: "content",
    KIND_LESSON: "content",
}

_ENTITY_BY_KIND = {
    KIND_KNOWLEDGE: "knowledge",
    KIND_EXPERTISE: "expertise",
    KIND_LESSON: "lesson",
}


def _content_full(row: Dict[str, Any], kind: int) -> str:
    return str(row.get(_CONTENT_BY_KIND.get(kind, "content")) or "")


def _content_preview(row: Dict[str, Any], kind: int, limit: int = 240) -> str:
    return _content_full(row, kind)[:limit]


def _kind_from_request(kind: int) -> int:
    if kind not in _ENTITY_BY_KIND:
        raise ValueError(f"Unsupported kind: {kind}")
    return kind


def _str_or_empty(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _response(
    *,
    kind: int,
    row: Dict[str, Any],
    success: bool = True,
    message: str = "",
    error_code: str = "",
) -> brain_pb2.BrainContentResponse:
    # Kind-specific fields are returned as empty strings when not applicable
    # so the proto contract is uniform across kinds.
    when_to_use = ""
    symptom = ""
    root_cause = ""
    solution = ""
    prevention = ""
    severity = ""
    if kind == 1:  # EXPERTISE
        when_to_use = _str_or_empty(row.get("when_to_use"))
    elif kind == 3:  # LESSON
        symptom = _str_or_empty(row.get("symptom"))
        root_cause = _str_or_empty(row.get("root_cause"))
        solution = _str_or_empty(row.get("solution"))
        prevention = _str_or_empty(row.get("prevention"))
        severity = _str_or_empty(row.get("severity"))

    return brain_pb2.BrainContentResponse(
        success=success,
        message=message,
        error_code=error_code,
        id=str(row.get("id") or ""),
        kind=kind,
        title=str(row.get("title") or ""),
        content_preview=_content_preview(row, kind),
        content=_content_full(row, kind),
        created_at=to_timestamp(row.get("created_at")),
        updated_at=to_timestamp(row.get("updated_at")),
        metadata={str(k): str(v) for k, v in (row.get("metadata") or {}).items()}
        if isinstance(row.get("metadata"), dict)
        else {},
        when_to_use=when_to_use,
        symptom=symptom,
        root_cause=root_cause,
        solution=solution,
        prevention=prevention,
        severity=severity,
    )


def _delete_response(*, deleted_id: str, message: str = "Deleted") -> brain_pb2.DeleteResponse:
    return brain_pb2.DeleteResponse(
        success=True,
        message=message,
        error_code="",
        deleted_id=deleted_id,
    )


def _primary_company_id(user) -> Optional[str]:
    if user is None or not user.companies:
        return None
    return str(user.companies[0])


def _metadata_from_map(metadata: Dict[str, str]) -> Dict[str, str]:
    return {str(k): str(v) for k, v in (metadata or {}).items()}


_VALID_SEVERITIES = {"low", "medium", "high", "critical"}


def _normalize_severity(value: Any) -> Optional[str]:
    if not value:
        return None
    v = str(value).strip().lower()
    if v not in _VALID_SEVERITIES:
        return None
    return v


def _kind_specific_create_kwargs(request, kind: int) -> Dict[str, Any]:
    """Pluck the proto fields relevant to the given kind. Empty values stay empty
    so the repository's _opt() helper can normalise them to NULL."""
    if kind == KIND_EXPERTISE:
        return {"when_to_use": str(getattr(request, "when_to_use", "") or "")}
    if kind == KIND_LESSON:
        return {
            "symptom": str(getattr(request, "symptom", "") or ""),
            "root_cause": str(getattr(request, "root_cause", "") or ""),
            "solution": str(getattr(request, "solution", "") or ""),
            "prevention": str(getattr(request, "prevention", "") or ""),
            "severity": _normalize_severity(getattr(request, "severity", "")) or "",
        }
    return {}


def _kind_specific_update_kwargs(request, kind: int) -> Dict[str, Any]:
    """Same as _kind_specific_create_kwargs but for UpdateBrainRequest.
    Empty proto strings mean "do not change" — caller drops empty keys."""
    raw = _kind_specific_create_kwargs(request, kind)
    return {k: v for k, v in raw.items() if v}


class BrainContentServicer(brain_pb2_grpc.BrainContentServiceServicer):
    """Additive CRUD service for brain content."""

    def __init__(self) -> None:
        self._repos: Optional[BrainContentRepositories] = None

    async def _repositories(self) -> BrainContentRepositories:
        if self._repos is None:
            self._repos = await BrainContentRepositories.create()
        return self._repos

    async def _user(self, context) -> Any:
        user = current_user_context.get()
        if user is None:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing authentication context")
        return user

    async def _company(self, context) -> str:
        user = await self._user(context)
        company_id = _primary_company_id(user)
        if not company_id:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "No company in JWT")
        return company_id

    async def AddToBrain(self, request, context):
        try:
            kind = _kind_from_request(int(request.kind))
            if not request.title or not request.content:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "title and content are required"
                )
            user = await self._user(context)
            company_id = await self._company(context)
            repos = await self._repositories()
            row = await repos.create_item(
                kind,
                company_id=company_id,
                title=str(request.title),
                content=str(request.content),
                metadata=_metadata_from_map(dict(request.metadata)),
                created_by_user_id=str(user.user_id),
                **_kind_specific_create_kwargs(request, kind),
            )

            await repos.update_cognee_status(kind, row["id"], "queued")

            await publish_brain_event(
                entity_type=_ENTITY_BY_KIND[kind],
                entity_id=str(row["id"]),
                company_id=company_id,
                project_id=None,
                title=str(row.get("title") or ""),
                text_content=str(row.get("text_content") or row.get("content") or ""),
                metadata=_metadata_from_map(dict(request.metadata)),
                action="create",
            )
            return _response(kind=kind, row=row, message="Created")
        except grpc.RpcError:
            raise
        except asyncpg.UniqueViolationError:
            logger.info("brain_content.add_duplicate", kind=kind)
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Content with identical body already exists in this company's brain",
            )
        except Exception as exc:
            logger.exception("brain_content.add_failed", error=str(exc))
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    async def UpdateBrain(self, request, context):
        try:
            kind = _kind_from_request(int(request.kind))
            if not request.id:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
            user = await self._user(context)
            company_id = await self._company(context)
            repos = await self._repositories()
            existing = await repos.get_item(kind, request.id)
            if not existing:
                await context.abort(
                    grpc.StatusCode.NOT_FOUND, f"Brain content {request.id} not found"
                )

            record_company_id = str(existing.get("company_id") or "")
            if record_company_id not in [str(c) for c in user.companies]:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Access denied")

            updates: Dict[str, Any] = {}
            if request.title:
                updates["title"] = str(request.title)
            if request.content:
                updates["content"] = str(request.content)
            if request.metadata:
                updates["metadata"] = _metadata_from_map(dict(request.metadata))
            updates.update(_kind_specific_update_kwargs(request, kind))

            row = await repos.update_item(kind, request.id, **updates) if updates else existing
            if row is None:
                await context.abort(
                    grpc.StatusCode.NOT_FOUND, f"Brain content {request.id} not found"
                )

            await repos.update_cognee_status(kind, row["id"], "queued")

            await publish_brain_event(
                entity_type=_ENTITY_BY_KIND[kind],
                entity_id=str(row["id"]),
                company_id=company_id,
                project_id=None,
                title=str(row.get("title") or ""),
                text_content=str(row.get("text_content") or row.get("content") or ""),
                metadata=_metadata_from_map(dict(request.metadata)),
                action="update",
            )
            return _response(kind=kind, row=row, message="Updated")
        except grpc.RpcError:
            raise
        except asyncpg.UniqueViolationError:
            logger.info("brain_content.update_duplicate", kind=kind, item_id=request.id)
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Content with identical body already exists in this company's brain",
            )
        except Exception as exc:
            logger.exception("brain_content.update_failed", error=str(exc))
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    async def DeleteFromBrain(self, request, context):
        try:
            kind = _kind_from_request(int(request.kind))
            if not request.id:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")

            user = await self._user(context)
            company_id = await self._company(context)
            repos = await self._repositories()
            existing = await repos.get_item(kind, request.id)
            if not existing:
                await context.abort(
                    grpc.StatusCode.NOT_FOUND, f"Brain content {request.id} not found"
                )
            if str(existing.get("company_id") or "") not in [str(c) for c in user.companies]:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Access denied")

            deleted_id = await repos.delete_item(kind, request.id)
            if not deleted_id:
                await context.abort(
                    grpc.StatusCode.NOT_FOUND, f"Brain content {request.id} not found"
                )

            await publish_brain_event(
                entity_type=_ENTITY_BY_KIND[kind],
                entity_id=deleted_id,
                company_id=company_id,
                project_id=None,
                title=str(existing.get("title") or ""),
                text_content=str(existing.get("text_content") or existing.get("content") or ""),
                metadata={"deleted": "true"},
                action="delete",
            )
            logger.warning(
                "brain_content.delete_gap",
                kind=kind,
                entity_id=deleted_id,
                detail="document_preprocessor consumer does not currently branch on deletion metadata",
            )
            return _delete_response(deleted_id=deleted_id, message="Deleted")
        except grpc.RpcError:
            raise
        except Exception as exc:
            logger.exception("brain_content.delete_failed", error=str(exc))
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    async def GetBrainContent(self, request, context):
        try:
            kind = _kind_from_request(int(request.kind))
            if not request.id:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
            repos = await self._repositories()
            row = await repos.get_item(kind, request.id)
            if row is None:
                await context.abort(
                    grpc.StatusCode.NOT_FOUND, f"Brain content {request.id} not found"
                )

            user = await self._user(context)
            if str(row.get("company_id") or "") not in [str(c) for c in user.companies]:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Access denied")

            return _response(kind=kind, row=row, message="OK")
        except grpc.RpcError:
            raise
        except Exception as exc:
            logger.exception("brain_content.get_failed", error=str(exc))
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    async def ListBrainContent(self, request, context):
        try:
            kind = _kind_from_request(int(request.kind))
            page = max(int(request.page or 1), 1)
            page_size = max(min(int(request.page_size or 50), 200), 1)
            company_id = await self._company(context)
            repos = await self._repositories()
            result = await repos.list_items(
                kind,
                company_id=company_id,
                page=page,
                page_size=page_size,
            )
            items = [_response(kind=kind, row=row, message="OK") for row in result.get("items", [])]
            return brain_pb2.ListBrainContentResponse(
                success=True,
                message="OK",
                error_code="",
                items=items,
                total=int(result.get("total_count", len(items))),
                page=page,
                page_size=page_size,
            )
        except grpc.RpcError:
            raise
        except Exception as exc:
            logger.exception("brain_content.list_failed", error=str(exc))
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

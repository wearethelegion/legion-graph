"""Brain v2 — Expertise CRUD handlers (5 RPCs). Extras in expertise_extras_handler."""

import hashlib

import grpc
import structlog
from cognee_service.auth_interceptor import current_user_context
from cognee_service.generated import brain_pb2
from cognee_service.brain.db import get_pool
from cognee_service.brain.kafka_producer import publish_brain_event
from cognee_service.brain._proto_helpers import (
    struct_to_json,
    dict_to_struct,
    to_timestamp,
    parse_jsonb,
    parse_jsonb_list,
    _sanitize,
    _validate_uuid,
)
from cognee_service.brain.expertise_extras_handler import (  # noqa: F401  re-export
    add_expertise_chunk,
    link_expertise_to_agent,
    unlink_expertise_from_agent,
)

logger = structlog.get_logger(__name__)


async def _check_company_ownership(record_company_id: str, context) -> None:
    """Abort with PERMISSION_DENIED if the authenticated user is not in record's company."""
    user = current_user_context.get()
    if user is None:
        return
    if str(record_company_id) not in [str(c) for c in user.companies]:
        await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Access denied")


def _row_to_expertise(row) -> brain_pb2.ExpertiseResponse:
    return brain_pb2.ExpertiseResponse(
        id=str(row["id"]),
        company_id=row["company_id"] or "",
        project_id=row["project_id"] or "",
        title=row["title"] or "",
        content=row["content"] or "",
        summary=row["summary"] or "",
        when_to_use=row["when_to_use"] or "",
        is_company_level=row["is_company_level"],
        content_hash=row["content_hash"] or "",
        metadata=dict_to_struct(parse_jsonb(row["metadata"])),
        created_by_user_id=row["created_by_user_id"] or "",
        created_at=to_timestamp(row["created_at"]),
        updated_at=to_timestamp(row["updated_at"]),
    )


def _row_to_chunk(cr) -> brain_pb2.ExpertiseChunk:
    return brain_pb2.ExpertiseChunk(
        id=str(cr["id"]),
        expertise_id=str(cr["expertise_id"]),
        content=cr["content"] or "",
        summary=cr["summary"] or "",
        position=cr["position"],
        level=cr["level"],
        parent_chunk_id=str(cr["parent_chunk_id"]) if cr["parent_chunk_id"] else "",
        chunk_path=cr["chunk_path"] or "",
        chunk_type=cr["chunk_type"] or "",
        section_title=cr["section_title"] or "",
        has_code=cr["has_code"],
        keywords=parse_jsonb_list(cr["keywords"]),
        created_at=to_timestamp(cr["created_at"]),
    )


_MAX_TITLE_LEN = 500
_MAX_CONTENT_LEN = 500_000  # 500KB — matches REST layer limits


async def create_expertise(request, context) -> brain_pb2.ExpertiseResponse:
    if not request.title or not request.content:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "title and content required")
    title = _sanitize(request.title)
    content = _sanitize(request.content)
    summary = _sanitize(request.summary)
    when_to_use = _sanitize(request.when_to_use)
    if len(title) > _MAX_TITLE_LEN:
        await context.abort(
            grpc.StatusCode.INVALID_ARGUMENT, f"title exceeds {_MAX_TITLE_LEN} chars"
        )
    if len(content) > _MAX_CONTENT_LEN:
        await context.abort(
            grpc.StatusCode.INVALID_ARGUMENT, f"content exceeds {_MAX_CONTENT_LEN} chars"
        )
    content_hash = hashlib.sha256(content.encode()).hexdigest()
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO expertise
                   (company_id, project_id, title, content, summary, when_to_use,
                    is_company_level, metadata, created_by_user_id, content_hash)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10) RETURNING *""",
            request.company_id,
            request.project_id or None,
            title,
            content,
            summary,
            when_to_use,
            request.is_company_level,
            struct_to_json(request.metadata),
            request.created_by_user_id,
            content_hash,
        )
    await publish_brain_event(
        entity_type="expertise",
        entity_id=str(row["id"]),
        company_id=request.company_id,
        project_id=request.project_id,
        title=title,
        text_content=content,
        action="create",
    )
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE expertise SET cognee_status = 'queued' WHERE id = $1",
            row["id"],
        )
    return _row_to_expertise(row)


async def get_expertise(request, context) -> brain_pb2.ExpertiseResponse:
    await _validate_uuid(request.id, "id", context)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM expertise WHERE id = $1", request.id)
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Expertise {request.id} not found")
        # IDOR: verify caller belongs to record's company
        await _check_company_ownership(row["company_id"], context)
        chunks = await conn.fetch(
            "SELECT * FROM expertise_chunks WHERE expertise_id = $1 ORDER BY position",
            row["id"],
        )
    resp = _row_to_expertise(row)
    for cr in chunks:
        resp.chunks.append(_row_to_chunk(cr))
    return resp


async def list_expertise(request, context) -> brain_pb2.ListExpertiseResponse:
    if not request.company_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "company_id is required")
    limit, offset = min(request.limit or 50, 200), max(request.offset or 0, 0)
    conds, params, idx = ["company_id = $1"], [request.company_id], 2
    if request.project_id:
        conds.append(f"project_id = ${idx}")
        params.append(request.project_id)
        idx += 1
    if request.filter_company_level:
        conds.append(f"is_company_level = ${idx}")
        params.append(request.is_company_level)
        idx += 1
    if request.query:
        conds.append(f"search_vector @@ plainto_tsquery('english', ${idx})")
        params.append(request.query)
        idx += 1
    where = " AND ".join(conds)
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT count(*) FROM expertise WHERE {where}", *params)
        rows = await conn.fetch(
            f"SELECT * FROM expertise WHERE {where} ORDER BY created_at DESC"
            f" LIMIT ${idx} OFFSET ${idx + 1}",
            *params,
            limit,
            offset,
        )
    return brain_pb2.ListExpertiseResponse(
        items=[_row_to_expertise(r) for r in rows],
        total_count=total,
    )


async def update_expertise(request, context) -> brain_pb2.ExpertiseResponse:
    if not request.id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
    await _validate_uuid(request.id, "id", context)
    # Fetch first to verify existence and ownership
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT company_id FROM expertise WHERE id = $1", request.id)
    if existing is None:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Expertise {request.id} not found")
    await _check_company_ownership(existing["company_id"], context)

    sets, params, idx = [], [], 1
    for fld, col in [
        ("title", "title"),
        ("content", "content"),
        ("summary", "summary"),
        ("when_to_use", "when_to_use"),
    ]:
        val = getattr(request, fld)
        if val:
            sets.append(f"{col} = ${idx}")
            params.append(_sanitize(val))
            idx += 1
    # is_company_level is a proto3 scalar bool — can only detect when True
    if request.is_company_level:
        sets.append(f"is_company_level = ${idx}")
        params.append(request.is_company_level)
        idx += 1
    if request.metadata and request.metadata.fields:
        sets.append(f"metadata = ${idx}::jsonb")
        params.append(struct_to_json(request.metadata))
        idx += 1
    if not sets:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "No fields to update")
    sets.append("updated_at = now()")
    params.append(request.id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE expertise SET {', '.join(sets)} WHERE id = ${idx} RETURNING *",
            *params,
        )
    if row is None:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Expertise {request.id} not found")
    await publish_brain_event(
        entity_type="expertise",
        entity_id=str(row["id"]),
        company_id=row["company_id"],
        project_id=row.get("project_id") or None,
        title=row["title"],
        text_content=row["content"],
        action="update",
    )
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE expertise SET cognee_status = 'queued' WHERE id = $1",
            row["id"],
        )
    return _row_to_expertise(row)


async def delete_expertise(request, context) -> brain_pb2.DeleteResponse:
    if not request.id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
    await _validate_uuid(request.id, "id", context)
    # Fetch first to verify existence and ownership before deleting
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT company_id FROM expertise WHERE id = $1", request.id)
    if existing is None:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Expertise {request.id} not found")
    await _check_company_ownership(existing["company_id"], context)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM expertise WHERE id = $1 RETURNING id, company_id, project_id, title",
            request.id,
        )
    if row is None:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Expertise {request.id} not found")
    await publish_brain_event(
        entity_type="expertise",
        entity_id=str(row["id"]),
        company_id=row["company_id"],
        project_id=str(row.get("project_id") or ""),
        title=str(row.get("title") or ""),
        action="delete",
    )
    return brain_pb2.DeleteResponse(success=True, message="Expertise deleted")

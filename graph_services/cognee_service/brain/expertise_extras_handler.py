"""
Brain v2 — Expertise extras: AddExpertiseChunk, LinkExpertiseToAgent,
UnlinkExpertiseFromAgent.

Split from expertise_handler.py to keep each file ≤200 LOC.
"""

import grpc
import structlog

from cognee_service.auth_interceptor import current_user_context
from cognee_service.generated import brain_pb2
from cognee_service.brain.db import get_pool
from cognee_service.brain.kafka_producer import publish_brain_event
from cognee_service.brain._proto_helpers import (
    to_timestamp,
    parse_jsonb_list,
    _sanitize,
    _validate_uuid,
)

logger = structlog.get_logger(__name__)


async def _check_company_ownership(record_company_id: str, context) -> None:
    """Abort with PERMISSION_DENIED if the authenticated user is not in record's company."""
    user = current_user_context.get()
    if user is None:
        return
    if str(record_company_id) not in [str(c) for c in user.companies]:
        await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Access denied")


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


async def add_expertise_chunk(request, context) -> brain_pb2.ExpertiseChunkResponse:
    if not request.expertise_id or not request.content:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "expertise_id and content required")
    await _validate_uuid(request.expertise_id, "expertise_id", context)

    content = _sanitize(request.content)
    summary = _sanitize(request.summary)
    section_title = _sanitize(request.section_title)

    parent = request.parent_chunk_id if request.parent_chunk_id else None

    # Verify parent expertise exists and check company ownership
    pool = await get_pool()
    async with pool.acquire() as conn:
        parent_expertise = await conn.fetchrow(
            "SELECT id, company_id, project_id FROM expertise WHERE id = $1",
            request.expertise_id,
        )
    if parent_expertise is None:
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Expertise {request.expertise_id} not found"
        )
    # IDOR: verify company ownership of parent expertise
    await _check_company_ownership(parent_expertise["company_id"], context)

    async with pool.acquire() as conn:
        max_pos = await conn.fetchval(
            "SELECT coalesce(max(position), -1) FROM expertise_chunks WHERE expertise_id = $1",
            request.expertise_id,
        )
        row = await conn.fetchrow(
            """INSERT INTO expertise_chunks
                   (expertise_id, content, summary, position, level, parent_chunk_id,
                    chunk_type, section_title)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *""",
            request.expertise_id,
            content,
            summary,
            max_pos + 1,
            0,  # default level — prevents NOT NULL failure if column has no DB DEFAULT
            parent,
            request.chunk_type or "prose",
            section_title,
        )
    await publish_brain_event(
        entity_type="expertise_chunk",
        entity_id=str(row["id"]),
        company_id=str(parent_expertise["company_id"]),
        project_id=str(parent_expertise.get("project_id") or "") or None,
        text_content=content,
        action="create",
        title=section_title or "",
        metadata={"expertise_id": request.expertise_id},
    )
    # Mark the PARENT expertise as queued (chunks are indexed via the parent).
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE expertise SET cognee_status = 'queued' WHERE id = $1::uuid",
            request.expertise_id,
        )
    return brain_pb2.ExpertiseChunkResponse(chunk=_row_to_chunk(row))


async def link_expertise_to_agent(request, context) -> brain_pb2.LinkResponse:
    if not request.agent_id or not request.expertise_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "agent_id and expertise_id required")
    await _validate_uuid(request.expertise_id, "expertise_id", context)

    # IDOR: verify expertise belongs to caller's company
    pool = await get_pool()
    async with pool.acquire() as conn:
        expertise_row = await conn.fetchrow(
            "SELECT company_id, project_id, title, content FROM expertise WHERE id = $1",
            request.expertise_id,
        )
    if expertise_row is None:
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Expertise {request.expertise_id} not found"
        )
    await _check_company_ownership(expertise_row["company_id"], context)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO agent_expertise_links
                   (agent_id, expertise_id, company_id, linked_by)
               VALUES ($1, $2::uuid, $3, $4)
               ON CONFLICT (agent_id, expertise_id) DO NOTHING
               RETURNING id""",
            request.agent_id,
            request.expertise_id,
            request.company_id,
            request.linked_by,
        )
    if row is None:
        return brain_pb2.LinkResponse(success=True, message="Link already exists")

    await publish_brain_event(
        entity_type="expertise",
        entity_id=str(request.expertise_id),
        company_id=str(expertise_row["company_id"]),
        project_id=str(expertise_row.get("project_id") or "") or None,
        text_content=expertise_row["content"] or "",
        action="update",
        title=expertise_row["title"] or "",
        metadata={"agent_id": request.agent_id, "link_id": str(row["id"])},
    )
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE expertise SET cognee_status = 'queued' WHERE id = $1::uuid",
            request.expertise_id,
        )
    return brain_pb2.LinkResponse(success=True, message="Linked", link_id=str(row["id"]))


async def unlink_expertise_from_agent(request, context) -> brain_pb2.LinkResponse:
    if not request.agent_id or not request.expertise_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "agent_id and expertise_id required")
    await _validate_uuid(request.expertise_id, "expertise_id", context)

    # IDOR: verify expertise belongs to caller's company
    pool = await get_pool()
    async with pool.acquire() as conn:
        expertise_row = await conn.fetchrow(
            "SELECT company_id, project_id, title, content FROM expertise WHERE id = $1",
            request.expertise_id,
        )
    if expertise_row is None:
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Expertise {request.expertise_id} not found"
        )
    await _check_company_ownership(expertise_row["company_id"], context)

    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM agent_expertise_links WHERE agent_id = $1 AND expertise_id = $2::uuid",
            request.agent_id,
            request.expertise_id,
        )
    deleted = result.split()[-1] != "0"
    if deleted:
        await publish_brain_event(
            entity_type="expertise",
            entity_id=str(request.expertise_id),
            company_id=str(expertise_row["company_id"]),
            project_id=str(expertise_row.get("project_id") or "") or None,
            text_content=expertise_row["content"] or "",
            action="update",
            title=expertise_row["title"] or "",
            metadata={"agent_id": request.agent_id},
        )
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE expertise SET cognee_status = 'queued' WHERE id = $1::uuid",
                request.expertise_id,
            )
    return brain_pb2.LinkResponse(
        success=deleted,
        message="Unlinked" if deleted else "Link not found",
    )

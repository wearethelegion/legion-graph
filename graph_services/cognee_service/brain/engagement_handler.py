"""
Brain v2 — Engagement Handler

4 gRPC RPCs for the engagements table:
  CreateEngagement, GetEngagement, ListEngagements, UpdateEngagement

Engagements table is pre-existing (pre-Brain v2).
Columns: id, company_id, project_id, name, ultimate_goal, agent_id,
         user_id, summary, status, session_id, engagement_id (parent),
         created_at, updated_at.
"""

import uuid

import grpc
import structlog
from google.protobuf import timestamp_pb2
from google.protobuf import empty_pb2

from cognee_service.auth_interceptor import current_user_context
from cognee_service.brain.db import get_pool
from cognee_service.brain.kafka_producer import publish_brain_event
from cognee_service.brain._proto_helpers import _sanitize, _validate_uuid
from cognee_service.generated import brain_pb2

logger = structlog.get_logger(__name__)


async def _check_company_ownership(record_company_id: str, context) -> None:
    """Abort with PERMISSION_DENIED if the authenticated user is not in record's company."""
    user = current_user_context.get()
    if user is None:
        return
    if str(record_company_id) not in [str(c) for c in user.companies]:
        await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Access denied")


# ── Helpers ──────────────────────────────────────────────────────────────────


def _ts(dt) -> timestamp_pb2.Timestamp:
    """Convert a Python datetime to a protobuf Timestamp."""
    ts = timestamp_pb2.Timestamp()
    if dt is not None:
        ts.FromDatetime(dt)
    return ts


def _row_to_response(row, entry_count: int = 0) -> brain_pb2.EngagementResponse:
    """Map an asyncpg Record to an EngagementResponse protobuf."""
    return brain_pb2.EngagementResponse(
        id=str(row["id"]),
        company_id=str(row.get("company_id") or ""),
        project_id=str(row.get("project_id") or ""),
        name=row.get("name") or "",
        ultimate_goal=row.get("ultimate_goal") or "",
        agent_id=str(row.get("agent_id") or ""),
        user_id=str(row.get("user_id") or ""),
        summary=row.get("summary") or "",
        status=row.get("status") or "created",
        session_id=row.get("session_id") or "",
        parent_engagement_id=str(row.get("engagement_id") or ""),
        created_at=_ts(row.get("created_at")),
        updated_at=_ts(row.get("updated_at")),
        entry_count=entry_count,
    )


# ── RPC Handlers ─────────────────────────────────────────────────────────────


async def create_engagement(request, context) -> brain_pb2.EngagementResponse:
    """CreateEngagement — INSERT into engagements."""
    user = current_user_context.get()
    if not request.company_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "company_id is required")

    if not request.name:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "name is required")

    name = _sanitize(request.name)
    ultimate_goal = _sanitize(request.ultimate_goal)
    summary = _sanitize(request.summary) if request.summary else None

    engagement_id = str(uuid.uuid4())
    parent = request.parent_engagement_id or None

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO engagements (
                id, company_id, project_id, name, ultimate_goal,
                agent_id, user_id, summary, session_id,
                engagement_id, status, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, 'created', NOW(), NOW()
            )
            RETURNING id::text, company_id::text, project_id::text, name,
                      ultimate_goal, agent_id::text, user_id::text, summary,
                      status, session_id, engagement_id::text,
                      created_at, updated_at
            """,
            engagement_id,
            request.company_id,
            request.project_id,
            name,
            ultimate_goal,
            request.agent_id or None,
            request.user_id or (user.user_id if user else None),
            summary,
            request.session_id or None,
            parent,
        )

    resp = _row_to_response(row)

    await publish_brain_event(
        entity_type="engagement",
        entity_id=engagement_id,
        company_id=request.company_id,
        project_id=request.project_id or None,
        action="create",
        title=name,
        text_content=ultimate_goal or "",
    )
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE engagements SET cognee_status = 'queued' WHERE id = $1",
            engagement_id,
        )

    return resp


async def get_engagement(request, context) -> brain_pb2.EngagementResponse:
    """GetEngagement — SELECT with entry count metadata."""
    await _validate_uuid(request.id, "id", context)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id::text, company_id::text, project_id::text, name,
                   ultimate_goal, agent_id::text, user_id::text, summary,
                   status, session_id, engagement_id::text,
                   created_at, updated_at
            FROM engagements
            WHERE id = $1
            """,
            request.id,
        )
        if not row:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Engagement {request.id} not found")

        # IDOR: verify caller belongs to record's company
        await _check_company_ownership(row.get("company_id") or "", context)

        count_row = await conn.fetchrow(
            "SELECT COUNT(*)::int AS cnt FROM engagement_entries WHERE engagement_id = $1",
            request.id,
        )

    entry_count = count_row["cnt"] if count_row else 0
    return _row_to_response(row, entry_count=entry_count)


async def list_engagements(request, context) -> brain_pb2.ListEngagementsResponse:
    """ListEngagements — SELECT with filters + pagination."""
    if not request.company_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "company_id is required")

    conditions = ["company_id = $1"]
    params: list = [request.company_id]
    idx = 2

    if request.project_id:
        conditions.append(f"project_id = ${idx}")
        params.append(request.project_id)
        idx += 1

    if request.status:
        conditions.append(f"status = ${idx}")
        params.append(request.status)
        idx += 1

    if request.parent_engagement_id:
        conditions.append(f"engagement_id = ${idx}")
        params.append(request.parent_engagement_id)
        idx += 1

    if request.query:
        conditions.append(
            f"to_tsvector('english', name || ' ' || COALESCE(ultimate_goal, '') "
            f"|| ' ' || COALESCE(summary, '')) "
            f"@@ plainto_tsquery('english', ${idx})"
        )
        params.append(request.query)
        idx += 1

    where = " AND ".join(conditions)
    limit = request.limit or 50
    offset = request.offset or 0
    params.extend([limit, offset])

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id::text, company_id::text, project_id::text, name,
                   ultimate_goal, agent_id::text, user_id::text, summary,
                   status, session_id, engagement_id::text,
                   created_at, updated_at,
                   COUNT(*) OVER() AS total_count
            FROM engagements
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
            """,
            *params,
        )

    total = rows[0]["total_count"] if rows else 0
    items = [_row_to_response(r) for r in rows]

    return brain_pb2.ListEngagementsResponse(items=items, total_count=total)


async def update_engagement(request, context) -> brain_pb2.EngagementResponse:
    """UpdateEngagement — partial UPDATE."""
    if not request.id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
    await _validate_uuid(request.id, "id", context)

    # Fetch first to verify existence and ownership
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT company_id FROM engagements WHERE id = $1", request.id
        )
    if not existing:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Engagement {request.id} not found")
    await _check_company_ownership(existing.get("company_id") or "", context)

    updates = []
    params: list = []
    idx = 1

    for field, col in [
        ("name", "name"),
        ("status", "status"),
        ("summary", "summary"),
        ("ultimate_goal", "ultimate_goal"),
    ]:
        val = getattr(request, field, "")
        if val:
            updates.append(f"{col} = ${idx}")
            params.append(_sanitize(val))
            idx += 1

    if request.parent_engagement_id:
        updates.append(f"engagement_id = ${idx}")
        params.append(request.parent_engagement_id)
        idx += 1

    if not updates:
        # No fields to update — return current
        return await get_engagement(
            brain_pb2.GetByIdRequest(id=request.id),
            context,
        )

    updates.append("updated_at = NOW()")
    params.append(request.id)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE engagements
            SET {", ".join(updates)}
            WHERE id = ${idx}
            RETURNING id::text, company_id::text, project_id::text, name,
                      ultimate_goal, agent_id::text, user_id::text, summary,
                      status, session_id, engagement_id::text,
                      created_at, updated_at
            """,
            *params,
        )

    if not row:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Engagement {request.id} not found")

    resp = _row_to_response(row)

    await publish_brain_event(
        entity_type="engagement",
        entity_id=str(row["id"]),
        company_id=str(row["company_id"]),
        project_id=str(row.get("project_id") or ""),
        action="update",
        title=row.get("name") or "",
        text_content=row.get("ultimate_goal") or "",
    )
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE engagements SET cognee_status = 'queued' WHERE id = $1",
            str(row["id"]),
        )

    return resp


async def delete_engagement(request, context) -> empty_pb2.Empty:
    """DeleteEngagement — ownership check, atomic DELETE, publish brain event.

    Child entries are NOT cascade-deleted — they remain as orphaned records
    and can be cleaned up separately.
    """
    await _validate_uuid(request.id, "id", context)

    # Fetch first to verify existence and company ownership
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT company_id FROM engagements WHERE id = $1", request.id
        )
    if not existing:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Engagement {request.id} not found")
    await _check_company_ownership(existing.get("company_id") or "", context)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            DELETE FROM engagements
            WHERE id = $1
            RETURNING id::text, company_id::text, project_id::text, name, ultimate_goal
            """,
            request.id,
        )

    if not row:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Engagement {request.id} not found")

    await publish_brain_event(
        entity_type="engagement",
        entity_id=str(row["id"]),
        company_id=str(row["company_id"]),
        project_id=str(row.get("project_id") or ""),
        action="delete",
        title=row.get("name") or "",
        text_content=row.get("ultimate_goal") or "",
    )

    logger.info("engagement.deleted", id=str(row["id"]))
    return empty_pb2.Empty()

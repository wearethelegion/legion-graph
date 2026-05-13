"""
Brain v2 — Entry Handler

5 gRPC RPCs for the engagement_entries table:
  AddEntry, GetEntry, ListEntries, UpdateEntry, SearchEntries

engagement_entries table is pre-existing (pre-Brain v2).
Columns: id, engagement_id, entry_type, title, content,
         created_by_agent_id, "references" (TEXT[]), tags (TEXT[]),
         summary, session_id, version, memory_level, created_at, updated_at.
"""

import json
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


def _row_to_response(row) -> brain_pb2.EntryResponse:
    """Map an asyncpg Record to an EntryResponse protobuf."""
    # references and tags are TEXT[] in Postgres, arrive as Python lists
    refs = row.get("references") or []
    if isinstance(refs, str):
        refs = json.loads(refs)
    tags = row.get("tags") or []
    if isinstance(tags, str):
        tags = json.loads(tags)

    return brain_pb2.EntryResponse(
        id=str(row["id"]),
        engagement_id=str(row.get("engagement_id") or ""),
        entry_type=row.get("entry_type") or "",
        title=row.get("title") or "",
        content=row.get("content") or "",
        created_by_agent_id=str(row.get("created_by_agent_id") or ""),
        references=list(refs),
        tags=list(tags),
        summary=row.get("summary") or "",
        session_id=row.get("session_id") or "",
        version=row.get("version") or 1,
        memory_level=row.get("memory_level") or "",
        created_at=_ts(row.get("created_at")),
        updated_at=_ts(row.get("updated_at")),
    )


# ── RPC Handlers ─────────────────────────────────────────────────────────────


async def add_entry(request, context) -> brain_pb2.EntryResponse:
    """AddEntry — INSERT into engagement_entries (verify parent exists)."""
    if not request.engagement_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "engagement_id is required")

    if not request.entry_type:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "entry_type is required")

    await _validate_uuid(request.engagement_id, "engagement_id", context)

    title = _sanitize(request.title)
    content = _sanitize(request.content)
    summary = _sanitize(request.summary) if request.summary else None

    entry_id = str(uuid.uuid4())
    refs = list(request.references) if request.references else []
    tags = list(request.tags) if request.tags else []

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Verify parent engagement exists; also fetch company_id for IDOR check
        parent = await conn.fetchrow(
            "SELECT id, company_id FROM engagements WHERE id = $1",
            request.engagement_id,
        )
        if not parent:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Parent engagement {request.engagement_id} not found",
            )

        # IDOR: verify caller belongs to parent engagement's company
        await _check_company_ownership(parent.get("company_id") or "", context)

        row = await conn.fetchrow(
            """
            INSERT INTO engagement_entries (
                id, engagement_id, entry_type, title, content,
                created_by_agent_id, "references", tags, summary,
                session_id, version, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, NOW())
            RETURNING id::text, engagement_id::text, entry_type, title,
                      content, created_by_agent_id::text,
                      "references", tags, summary, session_id,
                      version, created_at, updated_at
            """,
            entry_id,
            request.engagement_id,
            request.entry_type,
            title,
            content,
            request.created_by_agent_id or None,
            refs,
            tags,
            summary,
            request.session_id or None,
        )

    resp = _row_to_response(row)

    # Fetch company_id from parent engagement for Kafka
    async with pool.acquire() as conn:
        eng = await conn.fetchrow(
            "SELECT company_id::text, project_id::text FROM engagements WHERE id = $1",
            request.engagement_id,
        )

    company_id = eng["company_id"] if eng else ""
    project_id = eng.get("project_id") if eng else ""

    await publish_brain_event(
        entity_type="entry",
        entity_id=entry_id,
        company_id=company_id,
        project_id=project_id or None,
        action="create",
        title=title,
        text_content=content or "",
        metadata={"entry_type": request.entry_type, "engagement_id": request.engagement_id},
    )
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE engagement_entries SET cognee_status = 'queued' WHERE id = $1",
            entry_id,
        )

    return resp


async def get_entry(request, context) -> brain_pb2.EntryResponse:
    """GetEntry — SELECT by id with full content."""
    await _validate_uuid(request.id, "id", context)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id::text, engagement_id::text, entry_type, title,
                   content, created_by_agent_id::text,
                   "references", tags, summary, session_id,
                   version, memory_level, created_at, updated_at
            FROM engagement_entries
            WHERE id = $1
            """,
            request.id,
        )

    if not row:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Entry {request.id} not found")

    # IDOR: verify caller belongs to parent engagement's company
    async with pool.acquire() as conn:
        eng = await conn.fetchrow(
            "SELECT company_id::text FROM engagements WHERE id = $1",
            str(row["engagement_id"]),
        )
    if eng:
        await _check_company_ownership(eng.get("company_id") or "", context)

    return _row_to_response(row)


async def list_entries(request, context) -> brain_pb2.ListEntriesResponse:
    """ListEntries — SELECT by engagement_id with optional type filter."""
    if not request.engagement_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "engagement_id is required")

    await _validate_uuid(request.engagement_id, "engagement_id", context)

    # IDOR: verify caller belongs to parent engagement's company before listing
    pool = await get_pool()
    async with pool.acquire() as conn:
        eng = await conn.fetchrow(
            "SELECT company_id::text FROM engagements WHERE id = $1",
            request.engagement_id,
        )
    if not eng:
        await context.abort(
            grpc.StatusCode.NOT_FOUND,
            f"Engagement {request.engagement_id} not found",
        )
    await _check_company_ownership(eng.get("company_id") or "", context)

    conditions = ["engagement_id = $1"]
    params: list = [request.engagement_id]
    idx = 2

    if request.entry_type:
        conditions.append(f"entry_type = ${idx}")
        params.append(request.entry_type)
        idx += 1

    where = " AND ".join(conditions)
    limit = request.limit or 100
    offset = request.offset or 0
    params.extend([limit, offset])

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id::text, engagement_id::text, entry_type, title,
                   content, created_by_agent_id::text,
                   "references", tags, summary, session_id,
                   version, memory_level, created_at, updated_at,
                   COUNT(*) OVER() AS total_count
            FROM engagement_entries
            WHERE {where}
            ORDER BY created_at ASC
            LIMIT ${idx} OFFSET ${idx + 1}
            """,
            *params,
        )

    total = rows[0]["total_count"] if rows else 0
    items = [_row_to_response(r) for r in rows]

    return brain_pb2.ListEntriesResponse(items=items, total_count=total)


async def update_entry(request, context) -> brain_pb2.EntryResponse:
    """UpdateEntry — partial UPDATE, increments version."""
    if not request.id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
    await _validate_uuid(request.id, "id", context)

    # Fetch first to verify existence and ownership via parent engagement
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT engagement_id::text FROM engagement_entries WHERE id = $1", request.id
        )
    if not existing:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Entry {request.id} not found")

    async with pool.acquire() as conn:
        eng = await conn.fetchrow(
            "SELECT company_id::text FROM engagements WHERE id = $1",
            str(existing["engagement_id"]),
        )
    if eng:
        await _check_company_ownership(eng.get("company_id") or "", context)

    updates = []
    params: list = []
    idx = 1

    if request.content:
        updates.append(f"content = ${idx}")
        params.append(_sanitize(request.content))
        idx += 1

    if request.summary:
        updates.append(f"summary = ${idx}")
        params.append(_sanitize(request.summary))
        idx += 1

    # references and tags use explicit flags to allow clearing
    if request.update_references:
        updates.append(f'"references" = ${idx}')
        params.append(list(request.references))
        idx += 1

    if request.update_tags:
        updates.append(f"tags = ${idx}")
        params.append(list(request.tags))
        idx += 1

    if not updates:
        # No fields to update — re-fetch and return current
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id::text, engagement_id::text, entry_type, title,
                       content, created_by_agent_id::text,
                       "references", tags, summary, session_id,
                       version, memory_level, created_at, updated_at
                FROM engagement_entries
                WHERE id = $1
                """,
                request.id,
            )
        if not row:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Entry {request.id} not found")
        return _row_to_response(row)

    updates.append("version = version + 1")
    updates.append("updated_at = NOW()")
    params.append(request.id)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE engagement_entries
            SET {", ".join(updates)}
            WHERE id = ${idx}
            RETURNING id::text, engagement_id::text, entry_type, title,
                      content, created_by_agent_id::text,
                      "references", tags, summary, session_id,
                      version, memory_level, created_at, updated_at
            """,
            *params,
        )

    if not row:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Entry {request.id} not found")

    resp = _row_to_response(row)

    # Fetch company_id from parent engagement
    async with pool.acquire() as conn:
        eng = await conn.fetchrow(
            "SELECT company_id::text, project_id::text FROM engagements WHERE id = $1",
            str(row["engagement_id"]),
        )

    company_id = eng["company_id"] if eng else ""
    project_id = eng.get("project_id") if eng else ""

    await publish_brain_event(
        entity_type="entry",
        entity_id=str(row["id"]),
        company_id=company_id,
        project_id=project_id or None,
        action="update",
        title=row.get("title") or "",
        text_content=row.get("content") or "",
        metadata={"entry_type": row.get("entry_type") or ""},
    )
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE engagement_entries SET cognee_status = 'queued' WHERE id = $1",
            str(row["id"]),
        )

    return resp


async def delete_entry(request, context) -> empty_pb2.Empty:
    """DeleteEntry — atomic DELETE from engagement_entries + publish brain event."""
    await _validate_uuid(request.id, "id", context)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            DELETE FROM engagement_entries
            WHERE id = $1
            RETURNING id::text, engagement_id::text, entry_type, title, content
            """,
            request.id,
        )

    if not row:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Entry {request.id} not found")

    # Fetch company_id + project_id from parent engagement for Kafka
    async with pool.acquire() as conn:
        eng = await conn.fetchrow(
            "SELECT company_id::text, project_id::text FROM engagements WHERE id = $1",
            str(row["engagement_id"]),
        )

    company_id = eng["company_id"] if eng else ""
    project_id = eng.get("project_id") if eng else ""

    await publish_brain_event(
        entity_type="entry",
        entity_id=str(row["id"]),
        company_id=company_id,
        project_id=project_id or None,
        action="delete",
        title=row.get("title") or "",
        text_content=row.get("content") or "",
        metadata={
            "entry_type": row.get("entry_type") or "",
            "engagement_id": str(row["engagement_id"]),
        },
    )

    logger.info("entry.deleted", id=str(row["id"]))
    return empty_pb2.Empty()


async def search_entries(request, context) -> brain_pb2.ListEntriesResponse:
    """SearchEntries — Postgres tsvector search across entries."""
    if not request.query:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "query is required")

    if not request.company_id and not request.engagement_id:
        await context.abort(
            grpc.StatusCode.INVALID_ARGUMENT,
            "company_id or engagement_id is required for scoping",
        )

    # IDOR: when scoped by engagement_id only (no company_id), verify ownership
    if request.engagement_id and not request.company_id:
        await _validate_uuid(request.engagement_id, "engagement_id", context)
        pool = await get_pool()
        async with pool.acquire() as conn:
            eng = await conn.fetchrow(
                "SELECT company_id::text FROM engagements WHERE id = $1",
                request.engagement_id,
            )
        if not eng:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Engagement {request.engagement_id} not found",
            )
        await _check_company_ownership(eng.get("company_id") or "", context)

    conditions = []
    params: list = []
    idx = 1

    # tsvector search condition
    conditions.append(
        f"to_tsvector('english', ee.title || ' ' || ee.content) "
        f"@@ plainto_tsquery('english', ${idx})"
    )
    params.append(request.query)
    idx += 1

    # Scope to company via engagement join
    if request.company_id:
        conditions.append(f"e.company_id = ${idx}")
        params.append(request.company_id)
        idx += 1

    if request.project_id:
        conditions.append(f"e.project_id = ${idx}")
        params.append(request.project_id)
        idx += 1

    if request.engagement_id:
        conditions.append(f"ee.engagement_id = ${idx}")
        params.append(request.engagement_id)
        idx += 1

    if request.entry_type:
        conditions.append(f"ee.entry_type = ${idx}")
        params.append(request.entry_type)
        idx += 1

    where = " AND ".join(conditions)
    limit = request.limit or 20
    params.append(limit)

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT ee.id::text, ee.engagement_id::text, ee.entry_type,
                   ee.title, ee.content, ee.created_by_agent_id::text,
                   ee."references", ee.tags, ee.summary, ee.session_id,
                   ee.version, ee.memory_level, ee.created_at, ee.updated_at,
                   COUNT(*) OVER() AS total_count
            FROM engagement_entries ee
            JOIN engagements e ON e.id = ee.engagement_id
            WHERE {where}
            ORDER BY ee.created_at DESC
            LIMIT ${idx}
            """,
            *params,
        )

    total = rows[0]["total_count"] if rows else 0
    items = [_row_to_response(r) for r in rows]

    return brain_pb2.ListEntriesResponse(items=items, total_count=total)

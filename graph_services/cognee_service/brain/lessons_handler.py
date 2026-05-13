"""
Brain v2 — Lessons Learned Handler

5 gRPC RPCs for the lessons_learned table:
  RecordLesson, GetLesson, ListLessons, UpdateLesson, DeleteLesson

Pattern: asyncpg direct queries, Kafka fire-and-forget, auth via ContextVar.
"""

import hashlib
import json
import uuid

import grpc
import structlog
from google.protobuf import struct_pb2, timestamp_pb2

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


def _struct(data: dict) -> struct_pb2.Struct:
    """Convert a Python dict to a protobuf Struct."""
    s = struct_pb2.Struct()
    if data:
        s.update(data)
    return s


def _content_hash(text: str) -> str:
    """Full SHA-256 hex digest — matches REST service layer for cross-path dedup consistency."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _assemble_text(row) -> str:
    """Build composite text_content for Kafka enrichment."""
    parts = [
        row.get("title") or "",
        row.get("symptom") or "",
        row.get("root_cause") or "",
        row.get("solution") or "",
        row.get("prevention") or "",
        row.get("content") or "",
    ]
    return "\n\n".join(p for p in parts if p)


def _row_to_response(row) -> brain_pb2.LessonResponse:
    """Map an asyncpg Record to a LessonResponse protobuf."""
    tags = row.get("tags") or []
    if isinstance(tags, str):
        tags = json.loads(tags)
    files_changed = row.get("files_changed") or []
    if isinstance(files_changed, str):
        files_changed = json.loads(files_changed)
    metadata = row.get("metadata") or {}
    if isinstance(metadata, str):
        metadata = json.loads(metadata)

    return brain_pb2.LessonResponse(
        id=str(row["id"]),
        company_id=str(row.get("company_id") or ""),
        project_id=str(row.get("project_id") or ""),
        title=row.get("title") or "",
        category=row.get("category") or "",
        symptom=row.get("symptom") or "",
        root_cause=row.get("root_cause") or "",
        solution=row.get("solution") or "",
        prevention=row.get("prevention") or "",
        severity=row.get("severity") or "medium",
        tags=list(tags),
        files_changed=list(files_changed),
        content=row.get("content") or "",
        content_hash=row.get("content_hash") or "",
        metadata=_struct(metadata),
        created_by_user_id=str(row.get("created_by_user_id") or ""),
        created_at=_ts(row.get("created_at")),
        updated_at=_ts(row.get("updated_at")),
    )


# ── RPC Handlers ─────────────────────────────────────────────────────────────


async def record_lesson(request, context) -> brain_pb2.LessonResponse:
    """RecordLesson — INSERT into lessons_learned."""
    user = current_user_context.get()
    if not request.company_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "company_id is required")

    lesson_id = str(uuid.uuid4())

    # Sanitize all text inputs
    title = _sanitize(request.title)
    category = _sanitize(request.category)
    symptom = _sanitize(request.symptom)
    root_cause = _sanitize(request.root_cause)
    solution = _sanitize(request.solution)
    prevention = _sanitize(request.prevention)
    content = _sanitize(request.content)

    composite = "\n".join([title, symptom, root_cause, solution, prevention, content])
    chash = _content_hash(composite)

    metadata = {}
    if request.HasField("metadata"):
        metadata = dict(request.metadata)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO lessons_learned (
                id, company_id, project_id, title, category,
                symptom, root_cause, solution, prevention, severity,
                tags, files_changed, content, content_hash, metadata,
                created_by_user_id, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11::jsonb, $12::jsonb, $13, $14, $15::jsonb,
                $16, NOW(), NOW()
            )
            RETURNING *
            """,
            lesson_id,
            request.company_id,
            request.project_id,
            title,
            category,
            symptom,
            root_cause,
            solution,
            prevention,
            request.severity or "medium",
            json.dumps(list(request.tags)),
            json.dumps(list(request.files_changed)),
            content,
            chash,
            json.dumps(metadata),
            request.created_by_user_id or (user.user_id if user else None),
        )

    resp = _row_to_response(row)

    await publish_brain_event(
        entity_type="lesson",
        entity_id=lesson_id,
        company_id=request.company_id,
        project_id=request.project_id or None,
        action="create",
        title=title,
        text_content=_assemble_text(dict(row)),
        metadata=metadata,
    )
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE lessons_learned SET cognee_status = 'queued' WHERE id = $1",
            lesson_id,
        )

    return resp


async def get_lesson(request, context) -> brain_pb2.LessonResponse:
    """GetLesson — SELECT by id."""
    await _validate_uuid(request.id, "id", context)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM lessons_learned WHERE id = $1",
            request.id,
        )

    if not row:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Lesson {request.id} not found")

    # IDOR: verify caller belongs to record's company
    await _check_company_ownership(row.get("company_id") or "", context)

    return _row_to_response(row)


async def list_lessons(request, context) -> brain_pb2.ListLessonsResponse:
    """ListLessons — SELECT with filters + pagination."""
    if not request.company_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "company_id is required")

    conditions = ["company_id = $1"]
    params: list = [request.company_id]
    idx = 2

    if request.project_id:
        conditions.append(f"project_id = ${idx}")
        params.append(request.project_id)
        idx += 1

    if request.category:
        # Escape LIKE wildcards (% and _) in user input to prevent pattern injection
        safe_category = (
            request.category.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        conditions.append(f"category ILIKE ${idx}")
        params.append(f"{safe_category}%")
        idx += 1

    if request.severity:
        conditions.append(f"severity = ${idx}")
        params.append(request.severity)
        idx += 1

    if request.query:
        conditions.append(
            f"to_tsvector('english', title || ' ' || symptom || ' ' || solution) "
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
            SELECT *, COUNT(*) OVER() AS total_count
            FROM lessons_learned
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
            """,
            *params,
        )

    total = rows[0]["total_count"] if rows else 0
    items = [_row_to_response(r) for r in rows]

    return brain_pb2.ListLessonsResponse(items=items, total_count=total)


async def update_lesson(request, context) -> brain_pb2.LessonResponse:
    """UpdateLesson — partial UPDATE."""
    if not request.id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
    await _validate_uuid(request.id, "id", context)

    # Fetch first to verify existence and ownership
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT company_id FROM lessons_learned WHERE id = $1", request.id
        )
    if not existing:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Lesson {request.id} not found")
    await _check_company_ownership(existing.get("company_id") or "", context)

    updates = []
    params: list = []
    idx = 1

    for field, col in [
        ("title", "title"),
        ("category", "category"),
        ("symptom", "symptom"),
        ("root_cause", "root_cause"),
        ("solution", "solution"),
        ("prevention", "prevention"),
        ("severity", "severity"),
        ("content", "content"),
    ]:
        val = getattr(request, field, "")
        if val:
            updates.append(f"{col} = ${idx}")
            params.append(_sanitize(val))
            idx += 1

    if request.tags:
        updates.append(f"tags = ${idx}::jsonb")
        params.append(json.dumps(list(request.tags)))
        idx += 1

    if request.files_changed:
        updates.append(f"files_changed = ${idx}::jsonb")
        params.append(json.dumps(list(request.files_changed)))
        idx += 1

    if request.HasField("metadata"):
        updates.append(f"metadata = ${idx}::jsonb")
        params.append(json.dumps(dict(request.metadata)))
        idx += 1

    if not updates:
        # No fields to update — re-fetch and return current
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM lessons_learned WHERE id = $1", request.id)
        if not row:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Lesson {request.id} not found")
        return _row_to_response(row)

    updates.append("updated_at = NOW()")
    params.append(request.id)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE lessons_learned
            SET {", ".join(updates)}
            WHERE id = ${idx}
            RETURNING *
            """,
            *params,
        )

    if not row:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Lesson {request.id} not found")

    resp = _row_to_response(row)

    await publish_brain_event(
        entity_type="lesson",
        entity_id=str(row["id"]),
        company_id=str(row["company_id"]),
        project_id=str(row.get("project_id") or ""),
        action="update",
        title=row.get("title") or "",
        text_content=_assemble_text(dict(row)),
    )
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE lessons_learned SET cognee_status = 'queued' WHERE id = $1",
            row["id"],
        )

    return resp


async def delete_lesson(request, context) -> brain_pb2.DeleteResponse:
    """DeleteLesson — verify ownership then atomic DELETE with RETURNING for Kafka event data."""
    if not request.id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
    await _validate_uuid(request.id, "id", context)

    # Fetch first to verify existence and ownership
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT company_id FROM lessons_learned WHERE id = $1", request.id
        )
    if not existing:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Lesson {request.id} not found")
    await _check_company_ownership(existing.get("company_id") or "", context)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM lessons_learned WHERE id = $1 RETURNING id, company_id, project_id, title",
            request.id,
        )

    if not row:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Lesson {request.id} not found")

    await publish_brain_event(
        entity_type="lesson",
        entity_id=str(row["id"]),
        company_id=str(row["company_id"]),
        project_id=str(row.get("project_id") or ""),
        action="delete",
        title=str(row.get("title") or ""),
    )

    return brain_pb2.DeleteResponse(success=True, message="Lesson deleted")

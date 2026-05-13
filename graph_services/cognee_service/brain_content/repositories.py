"""Standalone repository dispatch for BrainContentService.

This module intentionally avoids importing ``api.*`` so it can run inside the
``kgrag-cognee`` container without the API package on sys.path.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from cognee_service.brain.db import get_pool


KIND_KNOWLEDGE = 2
KIND_EXPERTISE = 1
KIND_LESSON = 3


def _parse_jsonb(value):
    if not value:
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return value


def _table_for_kind(kind: int) -> str:
    if kind == KIND_KNOWLEDGE:
        return "brain_knowledge"
    if kind == KIND_EXPERTISE:
        return "brain_expertise"
    if kind == KIND_LESSON:
        return "brain_lessons"
    raise ValueError(f"Unsupported kind: {kind}")


@dataclass(slots=True)
class BrainContentRepositories:
    @classmethod
    async def create(cls) -> "BrainContentRepositories":
        await get_pool()
        return cls()

    async def create_item(
        self,
        kind: int,
        *,
        company_id: str,
        title: str,
        content: str,
        metadata: Optional[Dict[str, Any]],
        created_by_user_id: Optional[str],
        # Kind-specific extras (all optional, ignored when not relevant to the kind)
        when_to_use: Optional[str] = None,
        symptom: Optional[str] = None,
        root_cause: Optional[str] = None,
        solution: Optional[str] = None,
        prevention: Optional[str] = None,
        severity: Optional[str] = None,
    ) -> Dict[str, Any]:
        pool = await get_pool()
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        table = _table_for_kind(kind)

        # Normalise empty strings to NULL so optional columns stay clean.
        def _opt(v: Optional[str]) -> Optional[str]:
            if v is None:
                return None
            v = v.strip()
            return v if v else None

        if kind == KIND_KNOWLEDGE:
            insert_columns = "id, company_id, title, content, metadata, content_hash, created_by_user_id, created_at, updated_at"
            values = "$1, $2, $3, $4::jsonb, $5, $6, NOW(), NOW()"
            returning = "id::text, company_id, title, content, metadata, content_hash, created_by_user_id, created_at, updated_at"
            params = [
                company_id,
                title,
                content,
                json.dumps(metadata or {}),
                content_hash,
                created_by_user_id,
            ]
        elif kind == KIND_EXPERTISE:
            insert_columns = (
                "id, company_id, title, content, when_to_use, metadata, content_hash, "
                "created_by_user_id, created_at, updated_at"
            )
            values = "$1, $2, $3, $4, $5::jsonb, $6, $7, NOW(), NOW()"
            returning = (
                "id::text, company_id, title, content, when_to_use, metadata, "
                "content_hash, created_by_user_id, created_at, updated_at"
            )
            params = [
                company_id,
                title,
                content,
                _opt(when_to_use) or "",  # column is NULL-able but historically "" was used
                json.dumps(metadata or {}),
                content_hash,
                created_by_user_id,
            ]
        else:
            insert_columns = (
                "id, company_id, title, content, metadata, content_hash, "
                "created_by_user_id, symptom, root_cause, solution, prevention, "
                "severity, created_at, updated_at"
            )
            values = "$1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW()"
            returning = "*"
            params = [
                company_id,
                title,
                content,
                json.dumps(metadata or {}),
                content_hash,
                created_by_user_id,
                _opt(symptom),
                _opt(root_cause),
                _opt(solution),
                _opt(prevention),
                _opt(severity),
            ]
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {table} ({insert_columns})
                VALUES (gen_random_uuid(), {values})
                RETURNING {returning}
                """,
                *params,
            )
        return dict(row)

    async def get_item(self, kind: int, item_id: str) -> Optional[Dict[str, Any]]:
        pool = await get_pool()
        table = _table_for_kind(kind)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT * FROM {table} WHERE id = $1", item_id)
        return dict(row) if row else None

    async def update_item(self, kind: int, item_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        pool = await get_pool()
        table = _table_for_kind(kind)
        if kind == KIND_KNOWLEDGE:
            allowed = {
                "title",
                "content",
                "metadata",
                "content_hash",
            }
            updates = []
            params = []
            idx = 1
            for field, value in kwargs.items():
                if field not in allowed:
                    continue
                if field == "metadata":
                    updates.append(f"metadata = ${idx}::jsonb")
                    params.append(json.dumps(value) if isinstance(value, dict) else value)
                else:
                    updates.append(f"{field} = ${idx}")
                    params.append(value)
                idx += 1
            if "content" in kwargs and "content_hash" not in kwargs:
                updates.append(f"content_hash = ${idx}")
                params.append(hashlib.sha256(str(kwargs["content"]).encode("utf-8")).hexdigest())
                idx += 1
            if not updates:
                return await self.get_item(kind, item_id)
            updates.append("updated_at = NOW()")
            params.append(item_id)
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"UPDATE {table} SET {', '.join(updates)} WHERE id = ${idx} RETURNING *",
                    *params,
                )
            return dict(row) if row else None

        if kind == KIND_EXPERTISE:
            allowed = {
                "title",
                "content",
                "when_to_use",
                "metadata",
                "content_hash",
            }
            updates = []
            params = []
            idx = 1
            for field, value in kwargs.items():
                if field not in allowed:
                    continue
                if field == "metadata":
                    updates.append(f"metadata = ${idx}::jsonb")
                    params.append(json.dumps(value) if isinstance(value, dict) else value)
                else:
                    updates.append(f"{field} = ${idx}")
                    params.append(value)
                idx += 1
            if "content" in kwargs and "content_hash" not in kwargs:
                updates.append(f"content_hash = ${idx}")
                params.append(hashlib.sha256(str(kwargs["content"]).encode("utf-8")).hexdigest())
                idx += 1
            if not updates:
                return await self.get_item(kind, item_id)
            updates.append("updated_at = NOW()")
            params.append(item_id)
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"UPDATE {table} SET {', '.join(updates)} WHERE id = ${idx} RETURNING *",
                    *params,
                )
            return dict(row) if row else None

        # KIND_LESSON
        allowed = {
            "title",
            "content",
            "metadata",
            "content_hash",
            "symptom",
            "root_cause",
            "solution",
            "prevention",
            "severity",
        }
        updates = []
        params = []
        idx = 1
        for field, value in kwargs.items():
            if field not in allowed:
                continue
            if field == "metadata":
                updates.append(f"{field} = ${idx}::jsonb")
                params.append(json.dumps(value) if isinstance(value, dict) else value)
            else:
                updates.append(f"{field} = ${idx}")
                params.append(value)
            idx += 1
        if not updates:
            return await self.get_item(kind, item_id)
        updates.append("updated_at = NOW()")
        if "content" in kwargs and "content_hash" not in kwargs:
            updates.append(f"content_hash = ${idx}")
            params.append(hashlib.sha256(str(kwargs["content"]).encode("utf-8")).hexdigest())
            idx += 1
        params.append(item_id)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"UPDATE {table} SET {', '.join(updates)} WHERE id = ${idx} RETURNING *",
                *params,
            )
        return dict(row) if row else None

    async def delete_item(self, kind: int, item_id: str) -> Optional[str]:
        pool = await get_pool()
        table = _table_for_kind(kind)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"DELETE FROM {table} WHERE id = $1 RETURNING id::text", item_id
            )
        return row["id"] if row else None

    async def list_items(
        self,
        kind: int,
        *,
        company_id: str,
        page: int,
        page_size: int,
    ) -> Dict[str, Any]:
        pool = await get_pool()
        offset = max(page - 1, 0) * page_size
        table = _table_for_kind(kind)

        if kind == KIND_KNOWLEDGE:
            query = """
                SELECT *
                FROM brain_knowledge
                WHERE company_id = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
            """
            count_query = "SELECT count(*) FROM brain_knowledge WHERE company_id = $1"
        elif kind == KIND_EXPERTISE:
            query = """
                SELECT *
                FROM brain_expertise
                WHERE company_id = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
            """
            count_query = "SELECT count(*) FROM brain_expertise WHERE company_id = $1"
        else:
            query = """
                SELECT *
                FROM brain_lessons
                WHERE company_id = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
            """
            count_query = "SELECT count(*) FROM brain_lessons WHERE company_id = $1"

        async with pool.acquire() as conn:
            total = await conn.fetchval(count_query, company_id)
            rows = await conn.fetch(query, company_id, page_size, offset)
        return {"total_count": total, "items": [dict(r) for r in rows]}

    async def update_cognee_status(self, kind: int, item_id: str, status: str) -> None:
        _ = (kind, item_id, status)
        return None

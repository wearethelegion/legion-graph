"""
Base Repository
Common CRUD operations for database repositories.
"""

from typing import Optional, List, Dict, Any, Tuple
import asyncpg
import json
from uuid import UUID
from loguru import logger

from api.utils.text import sanitize_text


class BaseRepository:
    """Base repository with common CRUD operations."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @staticmethod
    def _sanitize_value(arg: Any) -> Any:
        """Recursively sanitize a single parameter value."""
        if isinstance(arg, str):
            return sanitize_text(arg)
        if isinstance(arg, list):
            return [BaseRepository._sanitize_value(item) for item in arg]
        return arg

    @staticmethod
    def _sanitize_params(args: tuple) -> Tuple:
        """
        Apply sanitize_text() to every str argument before it reaches asyncpg.

        Covers both null bytes and invalid UTF-8 / surrogate pairs.
        Recurses into lists so TEXT[] array elements are also sanitized.
        Idempotent on already-clean strings — no performance concern.
        Non-str, non-list args (UUIDs, ints, booleans, None) pass through unchanged.
        """
        return tuple(BaseRepository._sanitize_value(arg) for arg in args)

    async def execute(self, query: str, *args) -> str:
        """Execute a query that doesn't return rows, sanitising all str params."""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *self._sanitize_params(args))

    async def fetch_one(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Fetch a single row, sanitising all str params."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *self._sanitize_params(args))
            if not row:
                return None
            # Convert row to dict and parse JSON fields
            result = dict(row)
            return self._parse_json_fields(result)

    async def fetch_all(self, query: str, *args) -> List[Dict[str, Any]]:
        """Fetch all rows, sanitising all str params."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *self._sanitize_params(args))
            # Convert rows to dicts and parse JSON fields
            return [self._parse_json_fields(dict(row)) for row in rows]

    async def fetch_val(self, query: str, *args) -> Any:
        """Fetch a single value, sanitising all str params."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *self._sanitize_params(args))

    # Known JSONB column names across all Brain tables.
    # Only these columns are candidates for JSON string → dict parsing.
    # This prevents false-positive parsing of text fields (e.g. text_content,
    # content, symptom) that may happen to start with '{' or '['.
    _JSONB_COLUMNS = frozenset(
        {
            "metadata",
            "keywords",
            "tags",
            "files_changed",
        }
    )

    def _parse_json_fields(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse JSONB fields that come back as strings.

        asyncpg sometimes returns JSONB columns as JSON strings instead of dicts.
        This method converts them back to Python dicts/lists.

        Only known JSONB columns are parsed to avoid corrupting text fields
        whose content may coincidentally be valid JSON.
        """
        for key, value in row_dict.items():
            if key not in self._JSONB_COLUMNS:
                continue
            if isinstance(value, str) and value and value[0] in ("{", "["):
                try:
                    row_dict[key] = json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    pass
        return row_dict

    def parse_uuid(self, value: str | UUID) -> UUID:
        """Parse string or UUID to UUID with error handling."""
        if isinstance(value, UUID):
            return value
        try:
            return UUID(value)
        except (ValueError, AttributeError) as e:
            logger.error("Invalid UUID format: {}", value)
            raise ValueError(f"Invalid UUID format: {value}") from e

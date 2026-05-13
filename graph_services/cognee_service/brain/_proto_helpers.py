"""
Brain v2 — Shared proto ↔ Python conversion helpers

Used by all domain handler modules to convert between asyncpg rows
and protobuf messages (Struct, Timestamp, JSONB).
"""

import json
import re
import unicodedata
from typing import Optional
from uuid import UUID

import grpc
from google.protobuf import json_format, struct_pb2, timestamp_pb2


def struct_to_json(s: Optional[struct_pb2.Struct]) -> str:
    """Convert proto Struct to JSON string for JSONB column. Returns '{}' if empty."""
    if s is None or not s.fields:
        return "{}"
    return json.dumps(json_format.MessageToDict(s))


def dict_to_struct(d) -> struct_pb2.Struct:
    """Convert Python dict to proto Struct."""
    s = struct_pb2.Struct()
    if d:
        if isinstance(d, str):
            d = json.loads(d)
        s.update(d)
    return s


def to_timestamp(dt) -> timestamp_pb2.Timestamp:
    """Convert a datetime to proto Timestamp."""
    ts = timestamp_pb2.Timestamp()
    if dt is not None:
        ts.FromDatetime(dt)
    return ts


def parse_jsonb(val) -> dict:
    """Parse a JSONB column value to a Python dict."""
    if not val:
        return {}
    if isinstance(val, str):
        return json.loads(val)
    return val


def parse_jsonb_list(val) -> list:
    """Parse a JSONB column value expected to be a list."""
    if not val:
        return []
    if isinstance(val, str):
        return json.loads(val)
    return list(val)


# ── Security helpers ─────────────────────────────────────────────────────────

_CONTROL_RE = re.compile(r"[\x01-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _sanitize(text: str) -> str:
    """Sanitise a string for safe storage in PostgreSQL UTF-8 text columns.

    Replicates api/utils/text.sanitize_text() without the cross-package import,
    to keep cognee_service.brain self-contained.

    Steps (order is non-negotiable):
      1. Strip null bytes (\\x00) — PostgreSQL hard-rejects chr(0) in text.
      2. Strip dangerous control chars (keep TAB \\x09, LF \\x0a, CR \\x0d).
      3. Fix invalid UTF-8 / surrogates via encode/decode round-trip.
      4. NFC-normalise for storage consistency.
    """
    if not text:
        return text
    # Step 1: Null bytes MUST be removed before encode/decode
    text = text.replace("\x00", "")
    # Step 2: Strip control characters except TAB, LF, CR
    text = _CONTROL_RE.sub("", text)
    # Step 3: Fix invalid UTF-8 / surrogates
    text = text.encode("utf-8", errors="replace").decode("utf-8")
    # Step 4: NFC normalisation
    text = unicodedata.normalize("NFC", text)
    return text


async def _validate_uuid(value: str, field_name: str, context) -> None:
    """Validate UUID format; abort with INVALID_ARGUMENT if malformed.

    Args:
        value: The string to validate.
        field_name: Field name for the error message.
        context: gRPC servicer context (must support await context.abort()).

    Raises:
        Underlying gRPC abort exception if the UUID is invalid.
    """
    try:
        UUID(value)
    except (ValueError, AttributeError):
        await context.abort(
            grpc.StatusCode.INVALID_ARGUMENT,
            f"Invalid UUID format for {field_name}: {value!r}",
        )

"""
Text utility functions for safe database operations.
"""

import re
import unicodedata
from typing import Optional
from uuid import UUID

_CONTROL_RE = re.compile(r"[\x01-\x08\x0b\x0c\x0e-\x1f\x7f]")


def validate_uuid(value: str, field_name: str = "id") -> str:
    """Validate that a string is a valid UUID.

    Args:
        value: The string to validate.
        field_name: The name of the field for error messages.

    Returns:
        The validated string (unchanged).

    Raises:
        ValueError: If the string is not a valid UUID.
    """
    try:
        UUID(value)
    except (ValueError, AttributeError):
        raise ValueError(f"Invalid UUID format for {field_name}: {value}")
    return value


def sanitize_text(value: Optional[str]) -> Optional[str]:
    """
    Sanitise a string for safe storage in PostgreSQL UTF-8 text columns.

    Handles both known corruption vectors:

    1. Null byte (\\x00): PostgreSQL hard-rejects chr(0) in text context.
       Cannot be handled in SQL at all. Stripped FIRST, before encode/decode,
       because \\x00 is a valid Python character and the encode/decode
       round-trip does NOT remove it — PostgreSQL would still reject the result.
    2. Invalid UTF-8 / surrogates (e.g. truncated em-dash 0xe2 0x80):
       encode/decode round-trip with errors='replace' converts to U+FFFD
       and prevents UnicodeEncodeError in asyncpg.

    Order is non-negotiable: \\x00 removal MUST precede encode/decode.

    Idempotent: valid strings (ASCII, UUIDs, enums, emoji) pass through
    unchanged. NFC-normalised for sort/search consistency.

    Args:
        value: Raw string (may contain corrupt bytes or null bytes) or None.

    Returns:
        Sanitised string, or None if *value* was None.
    """
    if value is None:
        return None
    # Step 1: Null bytes FIRST — must be Python-layer only; PG rejects chr(0)
    value = value.replace("\x00", "")
    # Step 2: Strip other dangerous control characters (keep TAB \x09, LF \x0a, CR \x0d)
    value = _CONTROL_RE.sub("", value)
    # Step 3: Fix any remaining invalid UTF-8 / surrogates via round-trip
    value = value.encode("utf-8", errors="replace").decode("utf-8")
    # Step 4: NFC normalisation for storage consistency
    value = unicodedata.normalize("NFC", value)
    return value

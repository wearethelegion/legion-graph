"""Tiny slug helper for machine-friendly identifiers."""

from __future__ import annotations

import re

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_HYPHEN_RE = re.compile(r"-+")


def slugify(text: str, max_len: int = 80) -> str:
    """Convert text to a lowercase hyphenated slug."""
    if not text:
        return ""

    slug = _NON_ALNUM_RE.sub("-", text.strip().lower())
    slug = _HYPHEN_RE.sub("-", slug).strip("-")
    if not slug:
        return ""

    if len(slug) <= max_len:
        return slug

    parts = slug.split("-")
    collected: list[str] = []
    total = 0
    for part in parts:
        next_len = len(part) if not collected else len(part) + 1
        if collected and total + next_len > max_len:
            break
        if not collected and len(part) > max_len:
            return part[:max_len].strip("-")
        collected.append(part)
        total += next_len

    truncated = "-".join(collected).strip("-")
    return truncated or slug[:max_len].strip("-")

"""LLM-based query expansion for natural-language code questions."""

from __future__ import annotations

import asyncio
import os
import re
import time

import structlog
from pydantic import RootModel

from cognee.infrastructure.llm.LLMGateway import LLMGateway

logger = structlog.get_logger(__name__)

_CACHE_TTL_SECONDS = 300
_CACHE: dict[int, tuple[float, list[str]]] = {}
_CACHE_LOCK = asyncio.Lock()
_ACTION_PATTERNS: list[tuple[str, str]] = [
    (
        r"\bcreate\b(?:\s+(?:a|an|the))?\s+(?P<object>.+?)(?:\s+\b(?:in|for|on|with|via|using|to|from)\b|$)",
        "create",
    ),
    (
        r"\badd\b(?:\s+(?:a|an|the))?\s+(?P<object>.+?)(?:\s+\b(?:in|for|on|with|via|using|to|from)\b|$)",
        "add",
    ),
    (
        r"\bupdate\b(?:\s+(?:a|an|the))?\s+(?P<object>.+?)(?:\s+\b(?:in|for|on|with|via|using|to|from)\b|$)",
        "update",
    ),
    (
        r"\bdelete\b(?:\s+(?:a|an|the))?\s+(?P<object>.+?)(?:\s+\b(?:in|for|on|with|via|using|to|from)\b|$)",
        "delete",
    ),
    (
        r"\bsave\b(?:\s+(?:a|an|the))?\s+(?P<object>.+?)(?:\s+\b(?:in|for|on|with|via|using|to|from)\b|$)",
        "save",
    ),
    (
        r"\bfetch\b(?:\s+(?:a|an|the))?\s+(?P<object>.+?)(?:\s+\b(?:in|for|on|with|via|using|to|from)\b|$)",
        "fetch",
    ),
    (
        r"\bget\b(?:\s+(?:a|an|the))?\s+(?P<object>.+?)(?:\s+\b(?:in|for|on|with|via|using|to|from)\b|$)",
        "get",
    ),
    (
        r"\blist\b(?:\s+(?:a|an|the))?\s+(?P<object>.+?)(?:\s+\b(?:in|for|on|with|via|using|to|from)\b|$)",
        "list",
    ),
    (
        r"\bsubmit\b(?:\s+(?:a|an|the))?\s+(?P<object>.+?)(?:\s+\b(?:in|for|on|with|via|using|to|from)\b|$)",
        "submit",
    ),
]
_STOP_WORDS = {
    "a",
    "an",
    "the",
    "and",
    "or",
    "to",
    "for",
    "of",
    "in",
    "on",
    "with",
    "via",
    "using",
    "by",
}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _pascalize(text: str) -> str:
    parts = [part for part in re.findall(r"[A-Za-z0-9]+", text) if part.lower() not in _STOP_WORDS]
    return "".join(part[:1].upper() + part[1:] for part in parts)


def _heuristic_variants(original: str) -> list[str]:
    lower = original.lower()
    variants: list[str] = []
    for pattern, verb in _ACTION_PATTERNS:
        match = re.search(pattern, lower)
        if not match:
            continue
        object_phrase = match.group("object").strip()
        object_phrase = re.sub(r"^(?:a|an|the)\s+", "", object_phrase)
        object_phrase = re.sub(
            r"\s+\b(?:in|for|on|with|via|using|to|from)\b.*$", "", object_phrase
        ).strip()
        if not object_phrase:
            continue
        object_pascal = _pascalize(object_phrase)
        if not object_pascal:
            continue
        variants.append(f"use{verb[:1].upper() + verb[1:]}{object_pascal} hook")
        variants.append(f"{verb} {object_phrase} endpoint")
        break
    return variants


class QueryExpansionResponse(RootModel[list[str]]):
    pass


async def expand_query(query: str) -> list[str]:
    original = query.strip()
    if not original:
        return [query]

    cache_key = hash(original.lower())
    now = time.monotonic()

    async with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached and now - cached[0] <= _CACHE_TTL_SECONDS:
            return list(cached[1])

    max_variants = max(1, _env_int("QUERY_EXPANSION_MAX_VARIANTS", 3))
    timeout_seconds = float(os.getenv("QUERY_EXPANSION_TIMEOUT_SECONDS", "0.35"))
    prompt = (
        "Paraphrase the user's code question into 2-3 short variants in developer terminology "
        "(function names, action verbs, technical jargon). Return ONLY a JSON array of strings. "
        "The original question MUST be the first item."
    )
    heuristic_variants = _heuristic_variants(original)

    try:
        response = await asyncio.wait_for(
            LLMGateway.acreate_structured_output(
                text_input=original,
                system_prompt=prompt,
                response_model=QueryExpansionResponse,
            ),
            timeout=timeout_seconds,
        )
        variants = list(response.root or [])
        if not variants:
            raise ValueError("empty query expansion response")
        if variants[0].strip() != original:
            variants = [original, *variants]
        variants = [str(v).strip() for v in variants if str(v).strip()]
        ordered_candidates = [original, *heuristic_variants, *variants[1:]]
        deduped = []
        for variant in ordered_candidates:
            if variant not in deduped:
                deduped.append(variant)
            if len(deduped) >= max_variants + 1:
                break
        variants = deduped
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.info("query_expansion.fallback", query=original, error=str(exc))
        variants = [original, *heuristic_variants]

    async with _CACHE_LOCK:
        _CACHE[cache_key] = (time.monotonic(), list(variants))

    return variants

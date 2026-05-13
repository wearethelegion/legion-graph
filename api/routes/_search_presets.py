"""Search-quality presets for cognee passthrough endpoints.

Single source of truth for "depth" presets used by `/api/v1/brain/search`
and `/api/v1/code/search`. Each preset bundles a system prompt with
companion top_k / wide_search_top_k values so the answer length, retrieval
breadth, and instruction match.

Callers can:
    - send `depth: "concise" | "standard" | "thorough"` and get the bundle
    - or send raw `system_prompt` / `top_k` / `wide_search_top_k` to override
      individual fields. Raw fields take precedence over the preset.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple


# Common preamble shared by all completion-mode prompts. Tells the LLM what
# the context payload contains and what role it's playing.
_BASE_PREAMBLE = (
    "You are a knowledge expert answering questions using the provided context. "
    "The context contains relevant graph triplets and document excerpts retrieved "
    "from the company's collective brain. Use only the context provided — do not "
    "invent details. Cite specifics: names, file paths, design rationales, "
    "decisions, examples found in the context. If the context is thin or does "
    "not directly address the question, say so plainly rather than padding."
)

# Three presets that span the depth dimension. Tuple is
# (system_prompt, top_k, wide_search_top_k).
_PRESETS: Dict[str, Tuple[str, int, int]] = {
    "concise": (
        f"{_BASE_PREAMBLE} Answer in one or two sentences. Focus on the single "
        "most direct point. Skip background and examples unless essential.",
        10,
        100,
    ),
    "standard": (
        f"{_BASE_PREAMBLE} Provide a focused answer of two to four short paragraphs. "
        "Lead with the direct answer, then add one or two supporting specifics from "
        "the context. Use markdown when helpful.",
        20,
        200,
    ),
    "thorough": (
        f"{_BASE_PREAMBLE} Provide a thorough, structured answer organised with "
        "markdown headings or bullet lists. Cover: (1) the direct answer, "
        "(2) key supporting specifics from the context (names, paths, decisions, "
        "examples), (3) related concepts the context surfaces, "
        "(4) caveats or open questions if the context implies them. "
        "Don't repeat the question. Don't pad. Be as long as the context warrants.",
        30,
        300,
    ),
}

DEFAULT_DEPTH = "thorough"
VALID_DEPTHS = tuple(_PRESETS.keys())


def resolve_depth_preset(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Translate `depth` (and optional raw overrides) from a request payload
    into kwargs ready to pass through to the cognee gRPC client.

    Returns a dict with keys: system_prompt, top_k, wide_search_top_k.
    Empty string / 0 means "let the cognee servicer apply its default".

    Raw fields in the payload take precedence over the preset:
        payload = {"depth": "thorough", "top_k": 50}
        →   system_prompt = thorough_prompt, top_k = 50, wide = 300
    """
    depth = str(payload.get("depth") or DEFAULT_DEPTH).lower().strip()
    if depth not in _PRESETS:
        depth = DEFAULT_DEPTH
    preset_prompt, preset_top_k, preset_wide = _PRESETS[depth]

    raw_prompt = str(payload.get("system_prompt") or "").strip()
    raw_top_k = payload.get("top_k")
    raw_wide = payload.get("wide_search_top_k")

    return {
        "system_prompt": raw_prompt or preset_prompt,
        "top_k": int(raw_top_k) if raw_top_k else preset_top_k,
        "wide_search_top_k": int(raw_wide) if raw_wide else preset_wide,
    }

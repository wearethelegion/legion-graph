"""Content-type classifier for Phase 2 extraction prompt routing.

Maps (file_path, language) → content_type string used to select the correct
extraction prompt from code_processing.extraction_prompt_templates.

Moved from entity_extraction_service/content_type_classifier.py to shared/
so it can be imported by both code_preprocessor and
entity_extraction_service without a cross-service import.

## Routing rules (Phase 2 — Ruby spec only)

| Condition                                                     | content_type  |
|---------------------------------------------------------------|---------------|
| language=Ruby AND path ends in _spec.rb or _test.rb          | ruby_spec     |
| language=Ruby AND path contains /spec/ or starts with spec/  | ruby_spec     |
| language=Ruby (all other Ruby files)                          | ruby_rails    |
| language=TypeScript or JavaScript                             | typescript    |
| any other language                                            | <lang_lower>  |

## Extension points

When future spec-routing phases land (JavaScript/TypeScript jest, Python pytest),
add rules in `_SPEC_RULES` below. The `classify_content_type` signature stays stable.

## Usage

    from shared.content_type_classifier import classify_content_type

    ct = classify_content_type("spec/requests/api/v1/patients_spec.rb", "Ruby")
    # → "ruby_spec"

    ct = classify_content_type("app/models/patient.rb", "Ruby")
    # → "ruby_rails"
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Language → default content_type (for non-spec files)
# ---------------------------------------------------------------------------

_DEFAULT_BY_LANGUAGE: dict[str, str] = {
    "ruby": "ruby_rails",
    "typescript": "typescript",
    "javascript": "typescript",  # JS uses the same TS/React prompt
    "python": "python",
    "go": "go",
    "java": "java",
}


def _default_content_type_for_language(language: str) -> str:
    """Return the default content_type for a given language.

    Falls back to the lowercased language name for unknown languages so the
    caller can attempt a DB lookup by content_type and get a clear miss.
    """
    return _DEFAULT_BY_LANGUAGE.get(language.lower().strip(), language.lower().strip())


# ---------------------------------------------------------------------------
# Spec-file detection helpers
# ---------------------------------------------------------------------------


def _is_ruby_spec(file_path: str) -> bool:
    """Return True when the file path identifies a Ruby test/spec file.

    Checks (in order):
    1. Filename suffix: `_spec.rb` or `_test.rb`
    2. Path segment:   `/spec/` anywhere in path
    3. Path prefix:    `spec/` at the start (relative paths inside a repo)
    """
    path = file_path.replace("\\", "/")  # normalise Windows separators if any

    if path.endswith("_spec.rb") or path.endswith("_test.rb"):
        return True

    if "/spec/" in path:
        return True

    if path.startswith("spec/"):
        return True

    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def classify_content_type(file_path: str, language: str) -> str:
    """Return the content_type string for (file_path, language).

    The content_type is used to select the extraction prompt from
    ``code_processing.extraction_prompt_templates`` via the ``content_type``
    column added in Phase 2.

    Args:
        file_path: File path relative to repository root (or absolute).
        language:  Language string as detected by the preprocessor,
                   e.g. ``"Ruby"``, ``"TypeScript"``, ``"Python"``.

    Returns:
        content_type string — one of the values in ``extraction_prompt_templates``
        or a language-derived fallback if no specific rule matches.
    """
    lang_lower = (language or "").lower().strip()

    # ── Phase 2: Ruby spec routing ────────────────────────────────────
    if lang_lower == "ruby" and _is_ruby_spec(file_path):
        return "ruby_spec"

    # ── Future: add additional spec routing rules here ─────────────────
    # Example (Phase N):
    #   if lang_lower in ("typescript", "javascript") and _is_js_spec(file_path):
    #       return "typescript_spec"

    # ── Default: language-level prompt (existing behaviour) ──────────
    return _default_content_type_for_language(language)

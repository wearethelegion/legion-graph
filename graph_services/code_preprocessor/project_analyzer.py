"""Phase 1.3: LLM Analysis Module — 4-call parallel project analysis.

Runs four parallel Gemini 3.1 Pro calls to generate project-specific
configuration from the file tree + metadata:

  A. analyze_business_domains   — company-level, dedup by normalised_key
  B. analyze_technical_domains  — project-level technical domain map
  C. generate_chunker_config    — AST-aware chunker configuration (JSONB)
  D. generate_extraction_prompt — filled extraction prompt for the KG pipeline

Provider selection: a single lightweight probe call is made at the start of
``analyze_project()`` to determine which provider is reachable. All 4 parallel
analysis calls then use that same provider — no per-call fallback noise.

Provider priority: Google AI (gemini/gemini-2.5-pro) → Vertex AI
(vertex_ai/gemini-2.5-pro). Uses litellm for both.

Usage:
    profile = await analyze_project(
        project_id="uuid",
        repo_path="/path/to/repo",
        company_id="uuid",
        pool=asyncpg_pool,
    )
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Optional

import asyncpg
import litellm

from code_preprocessor.file_tree import build_tree

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_ANALYZER_MODEL_GEMINI = os.environ.get("ANALYZER_MODEL_GEMINI", "gemini/gemini-2.5-pro")
_ANALYZER_MODEL_VERTEX = os.environ.get("ANALYZER_MODEL_VERTEX", "vertex_ai/gemini-2.5-pro")
_MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# Domain normalisation
# ---------------------------------------------------------------------------

# Simple suffix-stripping stemmer as specified in the task
_SUFFIXES = ("tion", "ment", "ing", "s")


def _stem(word: str) -> str:
    """Strip one common English suffix (longest match wins).

    Minimum base length is 4 characters to avoid over-stripping short roots
    (e.g. 'payment' keeps its base 'payment', not 'pay').
    """
    w = word.lower()
    for suffix in _SUFFIXES:
        if w.endswith(suffix) and len(w) - len(suffix) >= 4:
            return w[: -len(suffix)]
    return w


def _split_camel_case(name: str) -> str:
    """Insert spaces at CamelCase boundaries before lowercasing.

    Handles two cases:
    1. lowercase→UPPERCASE transition: "aiIntegration" → "ai Integration"
    2. ALLCAPS→Capword transition:     "HTTPSConnection" → "HTTPS Connection"

    Must be applied BEFORE .lower() so the boundary information is preserved.
    """
    # Insert space between lowercase letter and uppercase letter
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    # Insert space between a run of uppercase letters and an uppercase+lowercase pair
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", name)
    return name


def normalize_domain_key(name: str) -> str:
    """Convert a business domain name to a canonical normalised_key.

    Algorithm:
    1. Pre-split CamelCase boundaries (preserves acronym boundaries)
    2. Extract word tokens (letters only, lowercase)
    3. Stem each word
    4. Sort alphabetically
    5. Join with underscore

    Examples:
        "Payment Processing" → "payment_process"
        "User Authentication" → "authent_user"
        "Order Management"    → "manag_order"
        "AIIntegration"       → same key as "AI Integration"
        "HTTPSConnection"     → "connect_https"
    """
    words = re.findall(r"[a-z]+", _split_camel_case(name).lower())
    stemmed = sorted(_stem(w) for w in words if w)
    return "_".join(stemmed)


# ---------------------------------------------------------------------------
# Provider router — probe-once pattern
# ---------------------------------------------------------------------------


class _AnalyzerRouter:
    """Gemini API primary, Vertex AI fallback for the strong analysis model.

    Exposes ``probe_provider()`` which performs a single lightweight call to
    determine which provider is reachable. The result is a ``(model, vertex_region)``
    tuple — ``vertex_region`` is ``None`` when Gemini is selected.

    Call ``probe_provider()`` once per analysis run; pass the result to every
    downstream call so no per-call fallback noise is generated.

    Thread-safe; shared as a module-level singleton.
    """

    def __init__(self) -> None:
        self._gemini_api_key = os.environ.get("GEMINI_API_KEY")
        self._gemini_model = _ANALYZER_MODEL_GEMINI
        self._vertex_model = _ANALYZER_MODEL_VERTEX

        vertex_regions_str = os.environ.get("VERTEXAI_LLM_REGIONS", "europe-west1")
        self._vertex_regions = [r.strip() for r in vertex_regions_str.split(",") if r.strip()]
        self._region_cycle = itertools.cycle(self._vertex_regions)
        self._lock = threading.Lock()

        logger.info(
            "AnalyzerRouter initialised: gemini=%s vertex=%s regions=%s",
            self._gemini_model,
            self._vertex_model,
            self._vertex_regions,
        )
        if not self._gemini_api_key:
            logger.warning("GEMINI_API_KEY not set — AnalyzerRouter will always use Vertex AI")

    def _next_region(self) -> str:
        with self._lock:
            return next(self._region_cycle)

    async def probe_provider(self) -> tuple[str, str | None]:
        """Probe which LLM provider is reachable with a minimal request.

        Returns:
            ``(model_str, vertex_region)`` where ``vertex_region`` is ``None``
            when Gemini API is selected, or the chosen region string when
            Vertex AI is selected.

        The returned values should be passed directly to ``_llm_json`` (via
        ``model=`` and ``vertex_region=``) for all subsequent calls in the
        same analysis run.
        """
        if self._gemini_api_key:
            try:
                await litellm.acompletion(
                    model=self._gemini_model,
                    api_key=self._gemini_api_key,
                    messages=[{"role": "user", "content": "hi"}],
                    max_tokens=1,
                )
                logger.info("analyzer: probed providers, using gemini (%s)", self._gemini_model)
                return self._gemini_model, None
            except Exception as exc:
                logger.info(
                    "analyzer: Gemini probe failed (%s: %s), falling back to Vertex AI",
                    type(exc).__name__,
                    str(exc)[:120],
                )

        region = self._next_region()
        logger.info(
            "analyzer: probed providers, using vertex_ai (%s, region=%s)",
            self._vertex_model,
            region,
        )
        return self._vertex_model, region

    async def acompletion(self, **kwargs: Any) -> Any:
        """Direct litellm.acompletion with the already-resolved model/region.

        Callers must supply either:
          - ``model`` starting with ``"gemini/"`` + ``api_key`` (Gemini path), or
          - ``model`` starting with ``"vertex_ai/"`` + ``vertex_location`` (Vertex path).

        This method is the single place that calls litellm so retries in
        ``_llm_json`` simply re-call this after adjusting kwargs.
        """
        return await litellm.acompletion(**kwargs)


_ROUTER: _AnalyzerRouter | None = None
_ROUTER_LOCK = threading.Lock()


def _get_router() -> _AnalyzerRouter:
    global _ROUTER
    if _ROUTER is None:
        with _ROUTER_LOCK:
            if _ROUTER is None:
                _ROUTER = _AnalyzerRouter()
    return _ROUTER


# ---------------------------------------------------------------------------
# Low-level LLM call helper
# ---------------------------------------------------------------------------


async def _llm_json(
    system_prompt: str,
    user_prompt: str,
    call_label: str = "llm_call",
    *,
    model: str,
    vertex_region: str | None,
) -> dict:
    """Call litellm with a pre-resolved provider and parse JSON.

    Args:
        system_prompt: System message content.
        user_prompt: User message content.
        call_label: Label used in log messages.
        model: Fully-qualified litellm model string (e.g. ``"vertex_ai/gemini-2.5-pro"``).
        vertex_region: Vertex AI region to pass as ``vertex_location``.
            Pass ``None`` when using the Gemini API.

    Retries on transient failures (not on JSON parse errors).
    """
    router = _get_router()
    last_exc: Exception | None = None

    # Build base kwargs for this provider
    # Temperature from env — Gemini 3 models require 1.0 (Google docs warn
    # temp<1.0 causes degraded reasoning / failure on complex tasks).
    # Default 0.1 preserves old behaviour for non-Gemini-3 providers.
    _temp = float(os.environ.get("LLM_TEMPERATURE", "0.1"))
    base_kw: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
        "temperature": _temp,
    }
    if vertex_region is not None:
        base_kw["vertex_location"] = vertex_region
    else:
        gemini_api_key = os.environ.get("GEMINI_API_KEY")
        if gemini_api_key:
            base_kw["api_key"] = gemini_api_key

    for attempt in range(_MAX_RETRIES):
        try:
            resp = await router.acompletion(**base_kw)
            raw = resp.choices[0].message.content
            return json.loads(raw)
        except (json.JSONDecodeError, KeyError) as exc:
            logger.error("%s: JSON parse failed on attempt %d: %s", call_label, attempt + 1, exc)
            last_exc = exc
            break  # Retrying won't fix a parse error
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES - 1:
                wait = 15 * (attempt + 1)
                logger.warning(
                    "%s: attempt %d failed (%s), retry in %ds",
                    call_label,
                    attempt + 1,
                    exc,
                    wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.error("%s: all %d attempts failed: %s", call_label, _MAX_RETRIES, exc)

    raise RuntimeError(f"{call_label} failed after {_MAX_RETRIES} attempts") from last_exc


# ---------------------------------------------------------------------------
# Call A — Pass 1: identify_project  (language + framework detection only)
# ---------------------------------------------------------------------------

_IDENTIFY_PROJECT_SYSTEM = """\
You are a software architect performing a quick technology stack identification.
Examine the repository file tree and identify the primary programming language and framework.
Return ONLY valid JSON — no prose, no code blocks."""

_IDENTIFY_PROJECT_USER = """\
Repository file tree:
---
{file_tree}
---

Task: Identify ONLY the primary programming language and framework used.

Rules:
- Base the primary language on the MAJORITY of source files (by count), not on isolated scripts.
- A single utility script in a different language must NOT override the majority language.
- For framework, choose the most prominent one — "unknown" if none is clear.

Return exactly this JSON schema and nothing else:
{{
  "language": "Primary programming language (e.g. 'TypeScript', 'Python', 'Ruby', 'Go', 'Java'). Base this on the MAJORITY of source files.",
  "framework": "Primary framework (e.g. 'React', 'Next.js', 'FastAPI', 'Ruby on Rails', 'Django'). Use 'unknown' if no clear framework."
}}
"""

# ---------------------------------------------------------------------------
# Call A — Pass 2: analyze_business_domains  (DB-driven, framework-specific)
# ---------------------------------------------------------------------------

# Tombstone: original _BUSINESS_DOMAINS_SYSTEM / _BUSINESS_DOMAINS_USER removed.
# The prompts now live in code_processing.project_analysis_prompts.
# See _load_analysis_prompt() and analyze_business_domains() below.

# ---------------------------------------------------------------------------
# Framework key routing  (code-side, NOT in the DB)
# ---------------------------------------------------------------------------

# Maps free-text framework names returned by identify_project() → normalised key
# used to look up the prompt in project_analysis_prompts.
_FRAMEWORK_KEY_MAP: dict[str, str] = {
    # Rails
    "ruby on rails": "rails",
    "rails": "rails",
    # React
    "react": "react",
    "react native": "react",
    # Next.js
    "next.js": "nextjs",
    "nextjs": "nextjs",
    "next": "nextjs",
    # Django
    "django": "django",
    # FastAPI
    "fastapi": "fastapi",
    "fast api": "fastapi",
}


def _framework_to_key(framework: str | None) -> str:
    """Map a free-text framework string to a normalised framework_key.

    Falls back to 'default' when the framework is unknown or unmapped.
    """
    if not framework:
        return "default"
    return _FRAMEWORK_KEY_MAP.get(framework.lower().strip(), "default")


# NOTE: The original combined _BUSINESS_DOMAINS_USER prompt was split in Phase 1.
# Pass 1: identify_project()         — tight generic prompt, returns language+framework
# Pass 2: analyze_business_domains() — DB-driven framework-specific prompt


async def identify_project(
    file_tree: str,
    *,
    model: str,
    vertex_region: str | None,
) -> dict:
    """Pass 1 — Identify primary language and framework only.

    This is a lightweight call: no domain extraction, no existing-domain context.
    Returns ``{language, framework, framework_key}``.  The ``framework_key`` is
    the normalised key used to look up the framework-specific analysis prompt.

    Args:
        file_tree: Compact file tree string from build_tree().
        model: Resolved litellm model string from ``probe_provider()``.
        vertex_region: Resolved Vertex AI region, or ``None`` for Gemini.

    Returns:
        Dict with keys:
          - "language":      detected primary language string
          - "framework":     detected primary framework string
          - "framework_key": normalised key for the project_analysis_prompts table
    """
    user_prompt = _IDENTIFY_PROJECT_USER.format(file_tree=file_tree)
    result = await _llm_json(
        _IDENTIFY_PROJECT_SYSTEM,
        user_prompt,
        "identify_project",
        model=model,
        vertex_region=vertex_region,
    )
    language: str | None = result.get("language") or None
    framework: str | None = result.get("framework") or None
    framework_key = _framework_to_key(framework)
    logger.info(
        "identify_project: language=%s framework=%s → framework_key=%s",
        language,
        framework,
        framework_key,
    )
    return {
        "language": language,
        "framework": framework,
        "framework_key": framework_key,
    }


async def _load_analysis_prompt(pool: asyncpg.Pool, framework_key: str) -> tuple[str, str] | None:
    """Load framework-specific analysis prompt from project_analysis_prompts.

    Mirrors the pattern of ``_load_prompt_for_language()`` — same error handling,
    same fallback logging.  When framework_key has no match, falls back to
    framework_key='default'.

    Args:
        pool: asyncpg connection pool.
        framework_key: Normalised framework key (e.g. 'rails', 'react', 'default').

    Returns:
        ``(system_prompt, prompt_text)`` tuple, or ``None`` if even the 'default'
        row is absent (critical configuration gap).
    """
    keys_to_try = [framework_key] if framework_key != "default" else ["default"]
    if framework_key != "default":
        keys_to_try.append("default")

    for key in keys_to_try:
        try:
            row = await pool.fetchrow(
                """
                SELECT system_prompt, prompt_text
                  FROM code_processing.project_analysis_prompts
                 WHERE framework_key = $1
                 ORDER BY version DESC
                 LIMIT 1
                """,
                key,
            )
            if row:
                logger.info(
                    "_load_analysis_prompt: loaded prompt for framework_key=%s "
                    "(requested=%s, system_len=%d, prompt_len=%d)",
                    key,
                    framework_key,
                    len(row["system_prompt"]),
                    len(row["prompt_text"]),
                )
                return row["system_prompt"], row["prompt_text"]
        except Exception as exc:
            logger.error(
                "_load_analysis_prompt: FAILED to query project_analysis_prompts "
                "for framework_key=%s: %s — this is a critical error.",
                key,
                exc,
            )
            return None

    logger.error(
        "_load_analysis_prompt: NO prompt found for framework_key=%s and no 'default' row. "
        "Add rows to code_processing.project_analysis_prompts.",
        framework_key,
    )
    return None


async def analyze_business_domains(
    file_tree: str,
    existing_domains: list[dict],
    framework_key: str,
    pool: asyncpg.Pool,
    *,
    model: str,
    vertex_region: str | None,
) -> list[dict]:
    """Pass 2 — Extract business domains using a framework-specific DB prompt.

    Loads the prompt from ``code_processing.project_analysis_prompts`` by
    ``framework_key``, with automatic fallback to ``framework_key='default'``.
    The JSON contract returned is identical to the old combined Call A:
    ``[{canonical_name, normalised_key, description}]``.

    Args:
        file_tree: Compact file tree string from build_tree().
        existing_domains: List of existing company domain dicts.
        framework_key: Normalised key from ``identify_project()``
            (e.g. 'rails', 'nextjs', 'default').
        pool: asyncpg pool for loading the prompt.
        model: Resolved litellm model string from ``probe_provider()``.
        vertex_region: Resolved Vertex AI region, or ``None`` for Gemini.

    Returns:
        List of domain dicts: ``[{canonical_name, normalised_key, description}]``.
    """
    prompt_row = await _load_analysis_prompt(pool, framework_key)
    if prompt_row is None:
        logger.error(
            "analyze_business_domains: CRITICAL — no prompt available for framework_key=%s "
            "and no 'default' fallback row. "
            "The project_analysis_prompts table is empty or missing. "
            "Run: DATABASE_URL=... python scripts/seed_project_analysis_prompts.py "
            "Returning sentinel domain list to make this failure visible in audit.",
            framework_key,
        )
        return [
            {
                "canonical_name": "ANALYSIS FAILED NO PROMPT",
                "normalised_key": "analysis_failed_no_prompt",
                "description": (
                    f"Business-domain analysis could not run: no prompt found for "
                    f"framework_key='{framework_key}' and no default fallback. "
                    "Seed the project_analysis_prompts table and re-analyse."
                ),
            }
        ]

    system_prompt, prompt_text = prompt_row
    existing_json = json.dumps(existing_domains, indent=2) if existing_domains else "[]"

    # The DB prompt_text uses {file_tree} and {existing_domains} as the only two
    # substitution slots.  We use plain .replace() instead of .format() because
    # the prompt body also contains literal JSON schema examples such as
    # { "canonical_name": "..." } whose curly braces would confuse .format().
    user_prompt = prompt_text.replace("{file_tree}", file_tree).replace(
        "{existing_domains}", existing_json
    )

    result = await _llm_json(
        system_prompt,
        user_prompt,
        "analyze_business_domains",
        model=model,
        vertex_region=vertex_region,
    )
    domains: list[dict] = result.get("business_domains", [])
    logger.info(
        "analyze_business_domains: got %d domains for framework_key=%s",
        len(domains),
        framework_key,
    )
    return domains


# ---------------------------------------------------------------------------
# Call B — Technical Domain Analysis
# ---------------------------------------------------------------------------

_TECHNICAL_DOMAINS_SYSTEM = """\
You are a software architect specialising in codebase structure analysis.
Analyse the file tree and identify the technical domains and subsystems.
Return ONLY valid JSON — no prose, no code blocks."""

_TECHNICAL_DOMAINS_USER = """\
Repository file tree:
---
{file_tree}
---
Detected language: {language}
Detected framework: {framework}

Task: Identify the technical domains/subsystems/layers present in this codebase.

Return a JSON object:
{{
  "technical_domains": [
    {{
      "name": "Domain name (e.g. 'API Layer', 'Authentication', 'Data Access')",
      "description": "2-3 sentences: what this technical domain does, \
its responsibilities, how it interacts with other domains.",
      "patterns": ["list", "of", "file", "path", "patterns", "e.g.", "app/controllers/"]
    }}
  ]
}}
"""


async def analyze_technical_domains(
    file_tree: str,
    language: str,
    framework: str,
    *,
    model: str,
    vertex_region: str | None,
) -> list[dict]:
    """Call B: Identify technical domains from file tree.

    Args:
        file_tree: Compact file tree string.
        language: Detected primary language (e.g. "Python", "Ruby").
        framework: Detected framework (e.g. "FastAPI", "Rails").
        model: Resolved litellm model string from ``probe_provider()``.
        vertex_region: Resolved Vertex AI region, or ``None`` for Gemini.

    Returns:
        List of technical domain dicts: {name, description, patterns}.
    """
    user_prompt = _TECHNICAL_DOMAINS_USER.format(
        file_tree=file_tree,
        language=language or "unknown",
        framework=framework or "unknown",
    )
    result = await _llm_json(
        _TECHNICAL_DOMAINS_SYSTEM,
        user_prompt,
        "analyze_technical_domains",
        model=model,
        vertex_region=vertex_region,
    )
    domains: list[dict] = result.get("technical_domains", [])
    logger.info("analyze_technical_domains: got %d domains", len(domains))
    return domains


# ---------------------------------------------------------------------------
# Call C — Chunker Configuration
# ---------------------------------------------------------------------------

_CHUNKER_CONFIG_SYSTEM = """\
You are an expert in code parsing, AST analysis, and chunking strategies for \
language models. Given a file tree and language, produce an optimal chunking \
configuration for a TreeSitter-based code chunker.
Return ONLY valid JSON — no prose, no code blocks."""

_CHUNKER_CONFIG_USER = """\
Repository file tree:
---
{file_tree}
---
Primary language: {language}
Framework: {framework}

Task: Generate a chunker configuration for this codebase.

Return a JSON object:
{{
  "language": "{language}",
  "framework": "{framework}",
  "ast_chunk_boundaries": [
    {{
      "node_type": "TreeSitter AST node type (e.g. 'function_definition', 'class_definition')",
      "min_size_chars": 50,
      "max_size_chars": 1500,
      "description": "Why this node type is a good chunk boundary"
    }}
  ],
  "fallback_strategy": "recursive_text | line_based",
  "fallback_chunk_size": 1000,
  "fallback_overlap": 100,
  "language_rules": {{
    "rule_description": "any language-specific note (e.g. 'Ruby blocks are significant')"
  }},
  "file_type_overrides": [
    {{
      "extension": ".yml",
      "strategy": "recursive_text",
      "chunk_size": 800
    }}
  ]
}}
"""


async def generate_chunker_config(
    file_tree: str,
    language: str,
    framework: str,
    *,
    model: str,
    vertex_region: str | None,
) -> dict:
    """Call C: Generate language-appropriate chunker configuration.

    Args:
        file_tree: Compact file tree string.
        language: Detected primary language.
        framework: Detected framework.
        model: Resolved litellm model string from ``probe_provider()``.
        vertex_region: Resolved Vertex AI region, or ``None`` for Gemini.

    Returns:
        Chunker config dict (to be stored as JSONB).
    """
    user_prompt = _CHUNKER_CONFIG_USER.format(
        file_tree=file_tree,
        language=language or "unknown",
        framework=framework or "unknown",
    )
    config = await _llm_json(
        _CHUNKER_CONFIG_SYSTEM,
        user_prompt,
        "generate_chunker_config",
        model=model,
        vertex_region=vertex_region,
    )
    logger.info(
        "generate_chunker_config: %d boundary types",
        len(config.get("ast_chunk_boundaries", [])),
    )
    return config


# ---------------------------------------------------------------------------
# Call D — Extraction Prompt Generation
# ---------------------------------------------------------------------------

_EXTRACTION_PROMPT_SYSTEM = """\
You are a knowledge graph architect. Your task is to fill a prompt template \
with project-specific entity types, relationship types, and domain context \
so the extraction LLM can extract a precise, project-appropriate knowledge graph.
Return ONLY valid JSON — no prose, no code blocks."""

_EXTRACTION_PROMPT_USER = """\
Prompt template (fill all {{PLACEHOLDERS}}):
---
{template}
---

Project context:
- Language: {language}
- Framework: {framework}
- Business domains: {business_domains}
- Technical domains: {technical_domains}

Task: Fill the template completely. Derive entity types and relationship types \
from the actual domains above. Be specific and closed (exhaustive list, no "etc.").

Return a JSON object:
{{
  "filled_prompt": "The complete, filled extraction prompt as a single string"
}}
"""

_DEFAULT_EXTRACTION_TEMPLATE = """\
You are a knowledge graph extractor for {language}/{framework} codebases.

Business domains: {{BUSINESS_DOMAINS}}
Technical domains: {{TECHNICAL_DOMAINS}}

Entity types (closed list): {{ENTITY_TYPES}}
Relationship types (closed list): {{RELATIONSHIP_TYPES}}

Language-specific examples:
{{EXAMPLES}}

Extract entities and relationships from the provided code chunk following the \
types above strictly. Return JSON with keys: entities, relationships.
"""


async def generate_extraction_prompt(
    template: str,
    file_tree: str,
    business_domains: list[dict],
    technical_domains: list[dict],
    language: str,
    framework: str,
    *,
    model: str,
    vertex_region: str | None,
) -> str:
    """Call D: Fill extraction prompt template with project-specific context.

    Args:
        template: Raw prompt template from Postgres (may contain {{PLACEHOLDERS}}).
        file_tree: Compact file tree string (context only).
        business_domains: Output of analyze_business_domains().
        technical_domains: Output of analyze_technical_domains().
        language: Detected primary language.
        framework: Detected framework.
        model: Resolved litellm model string from ``probe_provider()``.
        vertex_region: Resolved Vertex AI region, or ``None`` for Gemini.

    Returns:
        Filled extraction prompt string.
    """
    bd_summary = ", ".join(d.get("canonical_name", "") for d in business_domains)
    td_summary = ", ".join(d.get("name", "") for d in technical_domains)

    user_prompt = _EXTRACTION_PROMPT_USER.format(
        template=template,
        language=language or "unknown",
        framework=framework or "unknown",
        business_domains=bd_summary or "not detected",
        technical_domains=td_summary or "not detected",
    )
    result = await _llm_json(
        _EXTRACTION_PROMPT_SYSTEM,
        user_prompt,
        "generate_extraction_prompt",
        model=model,
        vertex_region=vertex_region,
    )
    prompt_text: str = result.get("filled_prompt", "")
    logger.info("generate_extraction_prompt: prompt length %d chars", len(prompt_text))
    return prompt_text


# ---------------------------------------------------------------------------
# Domain deduplication
# ---------------------------------------------------------------------------


def deduplicate_domains(
    existing: list[dict],
    new: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Split new domains into (to_upsert, truly_new).

    Deduplication is by normalised_key. If a new domain's normalised_key
    matches an existing one, we update its description but keep the key.

    Args:
        existing: Domains already in Postgres (have 'id' field).
        new: Domains returned by the LLM.

    Returns:
        (upsert_list, insert_list):
            upsert_list — domains that already exist (may have updated description)
            insert_list — genuinely new domains to INSERT
    """
    existing_by_key = {d["normalised_key"]: d for d in existing}
    upsert: list[dict] = []
    insert: list[dict] = []

    seen_keys: set[str] = set()
    for d in new:
        key = d.get("normalised_key") or normalize_domain_key(d.get("canonical_name", ""))
        if not key:
            continue
        if key in seen_keys:
            continue  # dedup within LLM output
        seen_keys.add(key)

        if key in existing_by_key:
            merged = {**existing_by_key[key], "description": d.get("description", "")}
            upsert.append(merged)
        else:
            insert.append(
                {
                    "canonical_name": d.get("canonical_name", key.replace("_", " ").title()),
                    "normalised_key": key,
                    "description": d.get("description", ""),
                }
            )

    return upsert, insert


# ---------------------------------------------------------------------------
# ProjectProfile dataclass
# ---------------------------------------------------------------------------


@dataclass
class ProjectProfile:
    """Complete analysis result for a project.

    Stored to code_processing.project_profiles (assumed to exist).
    """

    project_id: str
    company_id: str
    language: str
    framework: str
    business_domains: list[dict] = field(default_factory=list)
    technical_domains: list[dict] = field(default_factory=list)
    chunker_config: dict = field(default_factory=dict)
    extraction_prompt: str = ""

    def to_db_dict(self) -> dict:
        """Serialise for Postgres upsert."""
        return {
            "project_id": self.project_id,
            "company_id": self.company_id,
            "language": self.language,
            "framework": self.framework,
            "business_domains": json.dumps(self.business_domains),
            "technical_domains": json.dumps(self.technical_domains),
            "chunker_config": json.dumps(self.chunker_config),
            "extraction_prompt": self.extraction_prompt,
        }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


async def _load_existing_business_domains(pool: asyncpg.Pool, company_id: str) -> list[dict]:
    """Load existing company business domains from Postgres."""
    try:
        rows = await pool.fetch(
            """
            SELECT id, canonical_name, normalised_key, description
              FROM code_processing.company_business_domains
             WHERE company_id = $1
            """,
            company_id,
        )
        return [{k: str(v) if hasattr(v, "hex") else v for k, v in dict(r).items()} for r in rows]
    except Exception as exc:
        logger.warning("Could not load existing business domains: %s", exc)
        return []


# Map detected language to the default content_type used when seeding
# extraction_prompt_templates. Keep in sync with scripts/seed_code_extraction_prompts.py.
# JS shares the TypeScript/React prompt because the prompt body covers both.
_LANG_TO_DEFAULT_CONTENT_TYPE: dict[str, str] = {
    "ruby": "ruby_rails",
    "typescript": "typescript",
    "javascript": "typescript",
    "python": "python",
}


async def _load_prompt_for_language(pool: asyncpg.Pool, language: str) -> str | None:
    """Load the default extraction prompt for a project language.

    Resolves *language* to the canonical *content_type* used by the seed
    pipeline (e.g. Ruby → ``ruby_rails``) and delegates to
    :func:`_load_prompt_for_content_type`. This guarantees a Rails project
    always loads the Rails prompt, even when other content_types
    (``ruby_spec`` etc.) exist in the templates table with higher version
    numbers.

    The previous implementation matched templates by ``template_text LIKE
    '%ruby%' ORDER BY version DESC`` which would return whichever Ruby-
    flavoured prompt happened to have the highest version — typically
    ``ruby_spec`` once Phase 2 spec routing was seeded. That is the bug
    Mark's integration test (test_project_profile_prompt_correctness)
    catches.

    Returns None if no matching template found — caller MUST handle this.
    """
    lang_lower = language.lower().strip()
    content_type = _LANG_TO_DEFAULT_CONTENT_TYPE.get(lang_lower)
    if content_type is None:
        logger.error(
            "_load_prompt_for_language: no default content_type mapping for "
            "language=%s — add it to _LANG_TO_DEFAULT_CONTENT_TYPE.",
            language,
        )
        return None

    prompt = await _load_prompt_for_content_type(pool, content_type)
    if prompt is None:
        logger.error(
            "_load_prompt_for_language: no template for content_type=%s "
            "(language=%s) — seed code_processing.extraction_prompt_templates "
            "via scripts/seed_code_extraction_prompts.py.",
            content_type,
            language,
        )
        return None

    logger.info(
        "_load_prompt_for_language: language=%s → content_type=%s (%d chars)",
        language,
        content_type,
        len(prompt),
    )
    return prompt


async def _load_prompt_for_content_type(pool: asyncpg.Pool, content_type: str) -> str | None:
    """Load an extraction prompt by content_type from extraction_prompt_templates.

    Phase 2 addition: prompts are now keyed by content_type (e.g. 'ruby_spec', 'ruby_rails',
    'typescript'). Falls back to None when no matching template is found — caller handles.

    Args:
        pool: asyncpg connection pool.
        content_type: content_type string (e.g. 'ruby_spec', 'ruby_rails', 'typescript').

    Returns:
        Template text string, or None if not found.
    """
    # Skip DB lookup entirely when content_type is empty (non-code files such
    # as .yml, .erb, .key — language detection returns None for those).
    # Saves a wasted query and removes noise from the warning path.
    if not content_type:
        return None
    try:
        row = await pool.fetchrow(
            """
            SELECT template_text
              FROM code_processing.extraction_prompt_templates
             WHERE content_type = $1
             ORDER BY version DESC
             LIMIT 1
            """,
            content_type,
        )
        if row:
            prompt = row["template_text"]
            logger.debug(
                "_load_prompt_for_content_type: found template for content_type=%s (%d chars)",
                content_type,
                len(prompt),
            )
            return prompt
        logger.warning(
            "_load_prompt_for_content_type: NO template found for content_type=%s "
            "— will fall back to language-level prompt.",
            content_type,
        )
        return None
    except Exception as exc:
        logger.error(
            "_load_prompt_for_content_type: FAILED to query extraction_prompt_templates: %s "
            "— falling back to language-level prompt.",
            exc,
        )
        return None


async def _upsert_business_domains(
    pool: asyncpg.Pool,
    company_id: str,
    upsert_list: list[dict],
    insert_list: list[dict],
) -> None:
    """Persist business domain changes to Postgres."""
    if not upsert_list and not insert_list:
        return

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Update description for existing domains
            for d in upsert_list:
                if d.get("id"):
                    await conn.execute(
                        """
                        UPDATE code_processing.company_business_domains
                           SET description = $2, updated_at = now()
                         WHERE id = $1
                        """,
                        d["id"],
                        d["description"],
                    )

            # Insert new domains
            for d in insert_list:
                await conn.execute(
                    """
                    INSERT INTO code_processing.company_business_domains
                        (company_id, canonical_name, normalised_key, description)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (company_id, normalised_key) DO UPDATE
                       SET description = EXCLUDED.description,
                           updated_at  = now()
                    """,
                    company_id,
                    d["canonical_name"],
                    d["normalised_key"],
                    d["description"],
                )

    logger.info(
        "Business domains: %d updated, %d inserted for company %s",
        len(upsert_list),
        len(insert_list),
        company_id,
    )


async def _upsert_project_profile(pool: asyncpg.Pool, profile: ProjectProfile) -> None:
    """Upsert project profile to code_processing.project_profiles.

    Only writes columns that exist in the table schema:
    project_id, language, framework, technical_domains, chunker_config, extraction_prompt.

    Note: company_id and business_domains are NOT stored in project_profiles —
    business domains live in the separate company_business_domains table.
    """
    import json as _json

    try:
        await pool.execute(
            """
            INSERT INTO code_processing.project_profiles
                (project_id, language, framework,
                 technical_domains, chunker_config, extraction_prompt)
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
            ON CONFLICT ON CONSTRAINT uq_pp_project_id DO UPDATE
               SET language          = EXCLUDED.language,
                   framework         = EXCLUDED.framework,
                   technical_domains = EXCLUDED.technical_domains,
                   chunker_config    = EXCLUDED.chunker_config,
                   extraction_prompt = EXCLUDED.extraction_prompt,
                   updated_at        = now()
            """,
            profile.project_id,
            profile.language,
            profile.framework,
            _json.dumps(profile.technical_domains),
            _json.dumps(profile.chunker_config),
            profile.extraction_prompt,
        )
        logger.info("ProjectProfile upserted for project %s", profile.project_id)
    except Exception as exc:
        logger.error("Failed to upsert ProjectProfile for %s: %s", profile.project_id, exc)
        raise


# ---------------------------------------------------------------------------
# Extension-count fallback for language detection
# ---------------------------------------------------------------------------

# Mapping from file extension → (language, framework_hint)
# Only used when the LLM (Call A) fails to return language/framework.
_EXT_LANGUAGE_MAP: dict[str, tuple[str, str]] = {
    ".ts": ("TypeScript", "unknown"),
    ".tsx": ("TypeScript", "React"),
    ".js": ("JavaScript", "unknown"),
    ".jsx": ("JavaScript", "React"),
    ".py": ("Python", "unknown"),
    ".rb": ("Ruby", "unknown"),
    ".go": ("Go", "unknown"),
    ".rs": ("Rust", "unknown"),
    ".java": ("Java", "unknown"),
    ".kt": ("Kotlin", "unknown"),
    ".swift": ("Swift", "unknown"),
    ".cs": ("C#", "unknown"),
    ".cpp": ("C++", "unknown"),
    ".c": ("C", "unknown"),
    ".php": ("PHP", "unknown"),
    ".scala": ("Scala", "unknown"),
    ".ex": ("Elixir", "unknown"),
    ".exs": ("Elixir", "unknown"),
    ".clj": ("Clojure", "unknown"),
    ".hs": ("Haskell", "unknown"),
    ".dart": ("Dart", "unknown"),
    ".lua": ("Lua", "unknown"),
    ".r": ("R", "unknown"),
}


def _extension_count_fallback(file_tree: str) -> tuple[str, str]:
    """Count file extensions in the file tree and return the dominant language.

    This is a FALLBACK used only when Call A (LLM) fails to return language/framework.
    It counts occurrences of each known extension and picks the language with the
    most files. This avoids the first-match substring bug that caused TypeScript
    projects to be classified as Ruby when a single .rb utility script appeared first.

    Args:
        file_tree: Compact file tree string from build_tree().

    Returns:
        (language, framework) — framework is a best-guess from the tree content.
    """
    counts: dict[str, int] = {}
    for ext, (lang, _) in _EXT_LANGUAGE_MAP.items():
        # Count occurrences of the extension in the tree (case-insensitive)
        counts[lang] = counts.get(lang, 0) + file_tree.lower().count(ext)

    if not any(counts.values()):
        return "unknown", "unknown"

    dominant_lang = max(counts, key=lambda k: counts[k])
    if counts[dominant_lang] == 0:
        return "unknown", "unknown"

    # Infer framework from tree content for the dominant language
    t = file_tree.lower()
    framework = "unknown"
    if dominant_lang == "TypeScript":
        if "next.config" in t or "pages/" in t:
            framework = "Next.js"
        elif "angular.json" in t:
            framework = "Angular"
        elif "vite.config" in t or "capacitor.config" in t:
            framework = "React"
        elif "package.json" in t:
            framework = "React"
    elif dominant_lang == "JavaScript":
        if "next.config" in t:
            framework = "Next.js"
        elif "package.json" in t:
            framework = "Node.js"
    elif dominant_lang == "Python":
        if "django" in t or "settings.py" in t:
            framework = "Django"
        elif "fastapi" in t or "main.py" in t:
            framework = "FastAPI"
        else:
            framework = "Python"
    elif dominant_lang == "Ruby":
        framework = "Ruby on Rails" if "app/controllers" in t else "Ruby"
    elif dominant_lang == "Java":
        framework = "Spring" if "spring" in t else "Java"

    logger.info(
        "_extension_count_fallback: dominant=%s (count=%d) framework=%s",
        dominant_lang,
        counts[dominant_lang],
        framework,
    )
    return dominant_lang, framework


# Alias for backwards compatibility (used in existing tests)
_detect_language_framework = _extension_count_fallback


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def analyze_project(
    project_id: str,
    repo_path: str,
    company_id: str,
    pool: asyncpg.Pool,
    *,
    language: Optional[str] = None,
    framework: Optional[str] = None,
    file_tree: Optional[str] = None,
) -> ProjectProfile:
    """Run full 4-call parallel project analysis and store results.

    Steps:
    1. Build file tree from repo_path (or use pre-built tree if provided)
    2. Detect language/framework (or use provided values)
    3. Load prompt template + existing business domains from Postgres
    4. **Probe provider once** — determine Gemini vs Vertex AI with a single
       lightweight call before any analysis begins
    5. Run all 4 LLM calls in parallel (asyncio.gather) using the locked provider
    6. Deduplicate and persist business domains
    7. Upsert ProjectProfile
    8. Return complete ProjectProfile

    Args:
        project_id: UUID of the project being analysed.
        repo_path: Absolute path to the cloned repository (used only when
            file_tree is not provided).
        company_id: UUID of the owning company.
        pool: asyncpg connection pool.

        language: Override language detection.
        framework: Override framework detection.
        file_tree: Optional pre-built file tree string.  When provided (and
            non-empty), ``build_tree(repo_path)`` is skipped entirely so
            ``repo_path`` does not need to be a real filesystem path.

    Returns:
        Populated ProjectProfile dataclass.
    """
    logger.info("analyze_project: starting for project=%s repo=%s", project_id, repo_path)

    # 1. Build file tree (or use the pre-built one supplied by the caller)
    if not file_tree:
        file_tree = build_tree(repo_path)
        if not file_tree or file_tree.startswith("ERROR"):
            raise ValueError(f"Cannot build file tree for repo_path={repo_path!r}: {file_tree}")

    logger.info("analyze_project: file tree built (%d lines)", file_tree.count("\n") + 1)

    # 2. Load existing business domains from DB
    existing_domains = await _load_existing_business_domains(pool, company_id)
    logger.info("analyze_project: loaded %d existing domains", len(existing_domains))

    # 3. Probe provider once — lock in Gemini or Vertex AI for this run
    router = _get_router()
    resolved_model, resolved_region = await router.probe_provider()

    # 4. Pass 1 (serial): identify_project — returns language + framework + framework_key.
    #    This is a cheap, tight call.  If the caller has already supplied language/framework
    #    overrides we still call identify_project to get framework_key (for the DB prompt
    #    lookup) unless both language AND framework are overridden, in which case we derive
    #    framework_key directly without an LLM call.
    logger.info("analyze_project: Pass 1 — identify_project (language + framework detection)")

    if language and framework:
        # Full caller override — no LLM needed for Pass 1
        lang = language
        fw = framework
        framework_key = _framework_to_key(fw)
        logger.info(
            "analyze_project: caller override lang=%s fw=%s → framework_key=%s",
            lang,
            fw,
            framework_key,
        )
    else:
        identify_result = await identify_project(
            file_tree,
            model=resolved_model,
            vertex_region=resolved_region,
        )
        llm_language: str | None = identify_result.get("language")
        llm_framework: str | None = identify_result.get("framework")
        framework_key: str = identify_result.get("framework_key", "default")

        # Resolve final language/framework:
        #   1. Caller override wins
        #   2. LLM-detected value (holistic, volume-based)
        #   3. Extension-count fallback
        if language:
            lang = language
            fw = framework or llm_framework or _extension_count_fallback(file_tree)[1]
            framework_key = _framework_to_key(fw)
        elif llm_language:
            lang = llm_language
            fw = framework or llm_framework or "unknown"
        else:
            logger.warning(
                "analyze_project: identify_project did not return language/framework — "
                "using extension-count fallback"
            )
            fallback_lang, fallback_fw = _extension_count_fallback(file_tree)
            lang = fallback_lang
            fw = framework or fallback_fw
            framework_key = _framework_to_key(fw)

    logger.info(
        "analyze_project: language=%s framework=%s framework_key=%s",
        lang,
        fw,
        framework_key,
    )

    # 5. Pass 2 (serial): analyze_business_domains — loads framework-specific DB prompt.
    logger.info(
        "analyze_project: Pass 2 — analyze_business_domains (framework_key=%s)",
        framework_key,
    )
    business_domains_raw: list[dict] = await analyze_business_domains(
        file_tree,
        existing_domains,
        framework_key,
        pool,
        model=resolved_model,
        vertex_region=resolved_region,
    )

    # 6. Run Calls B, C in parallel + load extraction prompt from DB (no LLM generation)
    logger.info("analyze_project: launching Calls B, C in parallel + loading extraction prompt")
    (
        technical_domains,
        chunker_config,
        extraction_prompt,
    ) = await asyncio.gather(
        analyze_technical_domains(
            file_tree,
            lang,
            fw,
            model=resolved_model,
            vertex_region=resolved_region,
        ),
        generate_chunker_config(
            file_tree,
            lang,
            fw,
            model=resolved_model,
            vertex_region=resolved_region,
        ),
        _load_prompt_for_language(pool, lang),
    )

    if not extraction_prompt:
        logger.error(
            "analyze_project: CRITICAL — no extraction prompt found for language=%s. "
            "Add a template to code_processing.extraction_prompt_templates. "
            "Extraction will produce garbage without a proper prompt.",
            lang,
        )
        extraction_prompt = ""

    logger.info(
        "analyze_project: LLM calls complete — "
        "business_domains=%d technical_domains=%d chunker_boundaries=%d prompt_len=%d",
        len(business_domains_raw),
        len(technical_domains),
        len(chunker_config.get("ast_chunk_boundaries", [])),
        len(extraction_prompt),
    )

    # 7. Deduplicate and persist business domains
    upsert_list, insert_list = deduplicate_domains(existing_domains, business_domains_raw)
    await _upsert_business_domains(pool, company_id, upsert_list, insert_list)

    # Merged domain list for the profile
    all_business_domains = upsert_list + insert_list

    # 8. Upsert ProjectProfile
    profile = ProjectProfile(
        project_id=project_id,
        company_id=company_id,
        language=lang,
        framework=fw,
        business_domains=all_business_domains,
        technical_domains=technical_domains,
        chunker_config=chunker_config,
        extraction_prompt=extraction_prompt,
    )
    await _upsert_project_profile(pool, profile)

    logger.info(
        "analyze_project: complete for project=%s (lang=%s fw=%s biz_domains=%d tech_domains=%d)",
        project_id,
        lang,
        fw,
        len(all_business_domains),
        len(technical_domains),
    )
    return profile

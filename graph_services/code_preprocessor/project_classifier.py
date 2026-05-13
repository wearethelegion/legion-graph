"""Project-level analysis from folder structure.

Sends the folder-only tree (no filenames) to an LLM in a single call.
Returns a business + technical analysis: domains, design patterns,
architecture decisions. Stored per-repo on ingestion_batches.project_analysis.

This analysis serves as context for downstream processing (e.g. Cognee)
when it analyses individual files.
"""

import json
import logging
import os
from typing import Optional

import asyncpg

from cognee_service.gemini_fallback_router import get_fallback_router

logger = logging.getLogger(__name__)

DEFAULT_MODEL = os.environ.get("LLM_MODEL", "gemini/gemini-3.1-flash-lite-preview")


_PROJECT_PROMPT = """\
You are a senior software architect analysing a codebase from its folder structure.

Folder structure:
---
{folder_tree}
---

Provide a deep business AND technical analysis. Return a JSON object with:

- project_type: "web_api" | "monolith" | "microservice" | "library" | "cli" | "mobile_app"
- framework: detected framework (e.g. "Ruby on Rails", "FastAPI", "Next.js")
- description: 3-5 sentence business description — what problem this software solves, \
who the users are, what value it delivers. NOT a technical summary.

- business_domains: array of objects, each with:
  - name: domain name (e.g. "appointment_scheduling", "billing_and_invoicing", \
"inventory_management", "patient_records")
  - description: 2-3 sentences explaining the business purpose — what real-world \
problem this domain solves, who uses it, how it relates to other domains
  - key_concepts: array of core business entities/concepts in this domain \
(e.g. ["appointment", "time_slot", "provider", "reminder"])

- design_patterns: array of objects, each with:
  - name: pattern name (e.g. "Form Object", "Service Object", "Command", \
"Query Object", "Decorator", "Observer/Event", "Repository")
  - purpose: WHY this pattern is used in this codebase — what problem it solves, \
what OOD principle it enforces (e.g. "Form Objects enforce SRP by separating \
input validation from persistence")
  - how_it_works: one sentence on how files implementing this pattern typically behave

- architecture: object with:
  - style: the actual architecture style derived strictly from the folder structure. \
Do not assume framework defaults — analyse what is present and what is absent. \
(e.g. "API-only layered monolith with service layer", "hexagonal", "clean architecture", "MVC")
  - api_strategy: API design approach (e.g. "versioned REST API with v1/v2 namespaces")
  - async_patterns: how background/async work is handled (e.g. "Sidekiq workers + \
domain event handlers")
  - key_decisions: array of 3-5 notable architectural decisions visible from the structure"""


async def _build_folder_tree(
    pool: asyncpg.Pool,
    repository: str,
    branch: str,
) -> str:
    """Query unique directory paths and render an indented tree string."""
    rows = await pool.fetch(
        """SELECT DISTINCT regexp_replace(file_path, '/[^/]+$', '') AS dir_path
        FROM code_processing.repository_file_versions
        WHERE repository = $1 AND branch = $2 AND file_path LIKE '%%/%%'
        ORDER BY dir_path""",
        repository,
        branch,
    )
    dir_paths = sorted(r["dir_path"] for r in rows)
    if not dir_paths:
        return ""

    # Build tree dict: each node maps to its children names
    tree: dict[str, list[str]] = {}
    for dp in dir_paths:
        parts = dp.split("/")
        for i in range(len(parts)):
            parent = "/".join(parts[:i]) if i > 0 else ""
            child = parts[i]
            tree.setdefault(parent, [])
            if child not in tree[parent]:
                tree[parent].append(child)

    # Render indented tree via DFS
    lines: list[str] = []

    def _render(prefix: str, indent: int) -> None:
        children = tree.get(prefix, [])
        for ch in children:
            lines.append("  " * indent + ch + "/")
            full = f"{prefix}/{ch}" if prefix else ch
            _render(full, indent + 1)

    _render("", 0)
    return "\n".join(lines)


async def _llm_call(prompt: str, api_key: str, model: str) -> dict:
    """Send a single LLM call and parse JSON response.

    Uses GeminiFallbackRouter for automatic Gemini API → Vertex AI failover.
    """
    router = get_fallback_router()
    resp = await router.acompletion(
        messages=[
            {
                "role": "system",
                "content": "You are a code architecture analyst. Return only valid JSON.",
            },
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )
    return json.loads(resp.choices[0].message.content)


async def analyze_project(
    pool: asyncpg.Pool,
    repository: str,
    branch: str,
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
    folder_tree: Optional[str] = None,
) -> dict:
    """Analyse repository folder structure and store project-level analysis.

    Builds a folder-only tree from directory paths in the DB, sends it to
    the LLM in one call, and stores the resulting analysis JSON on the
    latest ingestion_batch row.

    Args:
        pool: Database connection pool.
        repository: Repository slug (e.g. 'oscar-vet/vet_backend').
        branch: Branch name.
        api_key: Gemini API key. Falls back to GEMINI_API_KEY env var.
        model: LLM model identifier for litellm.
        folder_tree: Pre-built folder tree string. If provided, skips DB query.

    Returns:
        The analysis dict (also stored in DB).
    """
    key = api_key or os.environ.get("GEMINI_API_KEY")
    if not key:
        logger.warning("No GEMINI_API_KEY — skipping project analysis")
        return {}

    if not folder_tree:
        folder_tree = await _build_folder_tree(pool, repository, branch)
    if not folder_tree:
        logger.warning("No directories found for %s@%s", repository, branch)
        return {}

    logger.info(
        "Analysing project %s@%s (%d tree lines)",
        repository,
        branch,
        folder_tree.count("\n") + 1,
    )
    prompt = _PROJECT_PROMPT.format(folder_tree=folder_tree)
    analysis = await _llm_call(prompt, key, model)

    # Store on the latest ingestion_batch for this repo/branch
    await pool.execute(
        """UPDATE code_processing.ingestion_batches
        SET project_analysis = $1
        WHERE id = (
            SELECT id FROM code_processing.ingestion_batches
            WHERE repository = $2 AND branch = $3
            ORDER BY created_at DESC LIMIT 1
        )""",
        json.dumps(analysis),
        repository,
        branch,
    )
    logger.info("Project analysis stored for %s@%s", repository, branch)
    return analysis

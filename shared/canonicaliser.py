"""Code-path entity canonicaliser for business-tag entity types.

Pure-function module — no DB, no Kafka, no network.
Operates only on entity dicts; does NOT touch technical entity types
(Class, Method, Module, Function, Service, Enum, Interface, Configuration).

Intended wire-in point: neo4j_storage_service/main.py _process_batch,
just before write_entity_nodes, so that garbage entities are never written
to Neo4j in the first place.

See plan entry a29b52c5-1e64-4927-8903-780e833be79f (engagement 1ed4ce90).
"""

from __future__ import annotations

import re
from typing import Mapping, Optional

# ── Configuration ──────────────────────────────────────────────────────────────

# Entity types subject to canonicalisation.
# Technical types (Class, Method, Module, Function, Service, Enum, Interface,
# Configuration, ProgressionEnum) are explicitly excluded — source identifier
# casing is meaningful there.
BUSINESS_TAG_TYPES: frozenset[str] = frozenset(
    {
        "BusinessDomain",
        "TechnicalTag",
        "Tool",
        "Organization",
    }
)

# Medical / clinical specialties wrongly tagged as BusinessDomain values.
# Normalise to lowercase before comparison (see _normalise_for_reject).
# Do NOT extend this set without Arthur's sign-off.
BUSINESSDOMAIN_MEDICAL_SPECIALTY_REJECTS: frozenset[str] = frozenset(
    {
        "cardiology",
        "oncology",
        "electrocardiography",
        "emergency care",
        "internal medicine",
        "laboratory",
        "pharmacy",
        "boarding",
        "pet owner",  # normalised form: _normalise_for_reject converts underscores to spaces
    }
)

# Acronyms whose casing must be preserved (all-caps) after Title Casing.
ACRONYMS: frozenset[str] = frozenset(
    {
        "AI",
        "API",
        "SOAP",
        "PRRO",
        "HTTP",
        "HTTPS",
        "SQL",
        "URL",
        "UI",
        "SDK",
        "CLI",
        "REST",
        "JSON",
        "XML",
        "CSS",
        "HTML",
        "JS",
        "TS",
        "DB",
    }
)


# ── Public API ─────────────────────────────────────────────────────────────────


def canonicalise_entity(
    entity: dict,
    *,
    business_domain_whitelist: Optional[Mapping[str, str]] = None,
) -> Optional[dict]:
    """Normalise or reject a business-tag entity.  Pass-through for all others.

    Args:
        entity: Dict with at least ``entity_type`` and ``name`` keys.
        business_domain_whitelist: Optional mapping of ``normalised_key`` →
            canonical ``name`` for the BusinessDomain values valid for this
            entity's company. Built upstream from
            ``code_processing.company_business_domains``.  When provided,
            every emitted ``BusinessDomain`` MUST match an entry — values
            outside the whitelist are rejected and casing/spacing drift is
            force-corrected to the whitelist canonical form.  When ``None``
            (legacy callers / unit tests), whitelist enforcement is skipped.

    Returns:
        - Same entity dict (unmodified) if entity_type is not a business-tag type.
        - New entity dict with normalised ``name`` if canonicalisation changed it.
        - Same entity dict (unmodified) if name was already canonical.
        - ``None`` if the entity should be rejected (caller must drop it and its edges).
    """
    entity_type: str = entity.get("entity_type") or ""

    # ── Pass-through: technical types are untouched ────────────────────────────
    if entity_type not in BUSINESS_TAG_TYPES:
        return entity

    name: str = (entity.get("name") or "").strip()

    # ── Reject: empty name ─────────────────────────────────────────────────────
    if not name:
        return None

    # ── Reject: self-referential (name == entity_type) ────────────────────────
    if name == entity_type:
        return None

    # ── Reject: medical specialty on BusinessDomain ───────────────────────────
    if entity_type == "BusinessDomain":
        normalised_for_reject = _normalise_for_reject(name)
        if normalised_for_reject in BUSINESSDOMAIN_MEDICAL_SPECIALTY_REJECTS:
            return None

    # ── Normalise: CamelCase → spaces, collapse whitespace, Title Case ────────
    canonical_name = _canonicalise_name(name)

    # ── Whitelist enforcement (BusinessDomain only, when whitelist provided) ──
    # For BusinessDomain values, the LLM is asked to copy from the project-
    # level whitelist verbatim. In practice it occasionally invents new names
    # ("Clinics.sales") or drifts case ("appointments" vs "Appointments").
    # Reject inventions; force-correct drift to the whitelist's canonical form.
    if entity_type == "BusinessDomain" and business_domain_whitelist is not None:
        match = _match_whitelist(canonical_name, business_domain_whitelist)
        if match is None:
            return None  # invented name not present in whitelist — reject
        canonical_name = match  # force-correct casing/spacing to whitelist value

    if canonical_name == name:
        return entity  # no change — return the same object

    updated = dict(entity)
    updated["name"] = canonical_name
    return updated


def build_business_domain_whitelist(rows: list[dict]) -> dict[str, str]:
    """Build a normalised-key → canonical_name lookup from
    ``company_business_domains`` rows.

    The lookup tolerates LLM drift: whitespace, CamelCase, casing, and
    trailing punctuation differences all collapse to the same key. The
    returned mapping is consumed by :func:`canonicalise_entity` via the
    ``business_domain_whitelist`` keyword argument.

    Args:
        rows: List of dicts with at least a ``canonical_name`` key (the
            DB ``normalised_key`` column is ignored — this function
            recomputes it using the canonicaliser's own normalisation
            pipeline so the keys match exactly what
            :func:`canonicalise_entity` produces).

    Returns:
        Dict ``normalised_key`` → ``canonical_name``. Empty dict if input
        is empty.
    """
    out: dict[str, str] = {}
    for row in rows:
        canonical = (row.get("canonical_name") or "").strip()
        if not canonical:
            continue
        key = _whitelist_key(canonical)
        out[key] = canonical
    return out


# ── Internal helpers ───────────────────────────────────────────────────────────


def _normalise_for_reject(name: str) -> str:
    """Lower-case + collapse underscores/hyphens/whitespace → single spaces.

    Ensures ``Emergency Care``, ``emergency_care``, ``EMERGENCY CARE`` all map
    to the same key for reject-list lookup.
    """
    lowered = name.lower()
    # Replace underscores and hyphens with spaces
    lowered = lowered.replace("_", " ").replace("-", " ")
    # Collapse whitespace
    return " ".join(lowered.split())


def _split_camel_case(name: str) -> str:
    """Insert spaces at CamelCase boundaries.

    Handles two cases:
    1. lowercase→UPPERCASE transition:  ``aiIntegration``  → ``ai Integration``
    2. ALLCAPS→Capword transition:      ``HTTPSConnection`` → ``HTTPS Connection``

    Logic is identical to ``project_analyzer._split_camel_case`` (Phase 1).
    """
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", name)
    return name


def _title_case_with_acronyms(name: str) -> str:
    """Title-case each word, but preserve known acronyms in ALL-CAPS."""
    words = []
    for word in name.split():
        if word.upper() in ACRONYMS:
            words.append(word.upper())
        else:
            words.append(word.capitalize())
    return " ".join(words)


def _canonicalise_name(name: str) -> str:
    """Full normalisation pipeline for a business-tag name.

    1. Split CamelCase boundaries.
    2. Collapse whitespace (handles embedded extra spaces, underscores-as-spaces).
    3. Apply Title Case with acronym preservation.
    """
    # Replace underscores used as word separators with spaces
    name = name.replace("_", " ")
    name = _split_camel_case(name)
    name = " ".join(name.split())  # collapse whitespace
    name = _title_case_with_acronyms(name)
    return name


def _whitelist_key(name: str) -> str:
    """Aggressive normalisation for whitelist matching.

    Strips punctuation that LLMs sometimes inject (parens, dots, ampersands,
    plus signs), collapses CamelCase, lowercases, and trims. The goal is
    that ``"Appointments"``, ``"appointments"``, ``"Appointments."``, and
    ``"Appointments (legacy)"`` all collapse to the same key — so the
    whitelist matcher pulls the canonical form regardless of LLM drift.
    """
    # Drop punctuation characters that aren't part of the canonical name
    cleaned = re.sub(r"[().\[\]{}&+,;:!?\"']", " ", name)
    cleaned = cleaned.replace("_", " ").replace("-", " ")
    cleaned = _split_camel_case(cleaned)
    return " ".join(cleaned.split()).lower()


def _match_whitelist(name: str, whitelist: Mapping[str, str]) -> Optional[str]:
    """Return the canonical whitelist value matching *name*, or None.

    Tries strict key match first; falls back to a startswith match so
    "Clinics.sales" maps to "Clinics" (LLM trying to be hierarchical).
    Returns None for fundamentally non-matching values (e.g.
    "Random Invented Domain").
    """
    key = _whitelist_key(name)
    if not key:
        return None

    # Exact key match
    if key in whitelist:
        return whitelist[key]

    # Startswith match: "clinics.sales" → "clinics" (canonical "Clinics")
    # Only accept when the matched key is a clear prefix word boundary.
    for wl_key, canonical in whitelist.items():
        if not wl_key:
            continue
        if key == wl_key:
            return canonical
        if key.startswith(wl_key + " ") or key.startswith(wl_key + "."):
            return canonical

    return None

"""Document-path entity canonicalisation for Neo4j Storage Service.

This module is intentionally company-scoped only: every operation runs inside
the caller's current Neo4j database, which must be named ``cognee-{company_id}``.
The code never opens any other database session, so cross-company merges are
physically impossible unless a future maintainer removes the assertion below.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import structlog

from .writer import Neo4jBatchWriter

logger = structlog.get_logger(__name__)

_NOISE_SUFFIXES = {
    "corporation",
    "software",
    "technologies",
    "inc",
    "ltd",
    "llc",
    "co",
    "corp",
}
_NOISE_TOKENS = {
    "platform",
    "platforms",
    "product",
    "products",
    "solution",
    "solutions",
    "suite",
    "services",
    "service",
    "system",
    "systems",
    "management",
    "enterprise",
    "cloud",
}
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_PAREN_HINT_RE = re.compile(r"\(([^)]*)\)")
_TRAILING_SUFFIX_RE = re.compile(
    r"(?:\s+)(\d+(?:[./]\d+)*(?:/10)?|Go|Pro|Lite|Mobile|Enterprise|Plus|Basic|Free|Premium|Standard|Advanced)\s*$",
    re.IGNORECASE,
)


@dataclass(slots=True)
class CanonicalisationOutcome:
    merge_count: int = 0
    duplicate_to_canonical: Dict[str, str] = field(default_factory=dict)
    canonical_metadata: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass(slots=True)
class _Candidate:
    node_id: str
    name: str
    entity_type: str
    description: str
    aliases: Tuple[str, ...]
    origin: str

    @property
    def all_names(self) -> Tuple[str, ...]:
        return (self.name, *self.aliases)


def _assert_scoped_database(database: str) -> None:
    if not database.startswith("cognee-"):
        raise AssertionError("canonicalisation requires per-company database scoping")


def _tokens(text: str) -> List[str]:
    return _TOKEN_RE.findall(text.lower())


def _strip_suffixes(tokens: List[str]) -> List[str]:
    while tokens and tokens[-1] in _NOISE_SUFFIXES:
        tokens.pop()
    return tokens


def _normalise_name(name: str) -> str:
    lowered = name.lower().strip()
    lowered = _PAREN_HINT_RE.sub(" ", lowered)
    lowered = lowered.replace(".", " ")
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    tokens = _strip_suffixes(_tokens(lowered))
    return re.sub(r"\s+", " ", " ".join(tokens)).strip()


def _split_trailing_suffix(name: str) -> Tuple[str, Optional[str]]:
    cleaned = re.sub(r"\s+", " ", _PAREN_HINT_RE.sub(" ", name.lower())).strip()
    match = _TRAILING_SUFFIX_RE.search(cleaned)
    if not match:
        return cleaned, None

    suffix = match.group(1)
    stem = re.sub(r"[\s:;,.\-_/]+$", "", cleaned[: match.start()]).strip()
    return stem, suffix.lower() if suffix.isalpha() else suffix


def _should_block_merge(name_a: str, name_b: str) -> bool:
    left_norm = _normalise_name(name_a)
    right_norm = _normalise_name(name_b)
    left_stem, left_suffix = _split_trailing_suffix(name_a)
    right_stem, right_suffix = _split_trailing_suffix(name_b)

    if not left_suffix and not right_suffix:
        return False

    if left_suffix and right_suffix:
        return left_stem == right_stem and left_suffix != right_suffix

    if left_suffix:
        return left_stem == right_norm

    return right_stem == left_norm


def _description_tokens(description: str) -> set[str]:
    return {
        token
        for token in _tokens(description)
        if len(token) > 1 and token not in {"the", "and", "for", "with", "from", "this", "that"}
    }


def _description_overlap(left: str, right: str) -> float:
    left_tokens = _description_tokens(left)
    right_tokens = _description_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    shared = left_tokens & right_tokens
    return len(shared) / min(len(left_tokens), len(right_tokens))


def _levenshtein(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)
    prev = list(range(len(right) + 1))
    for i, left_ch in enumerate(left, start=1):
        curr = [i]
        for j, right_ch in enumerate(right, start=1):
            cost = 0 if left_ch == right_ch else 1
            curr.append(min(curr[-1] + 1, prev[j] + 1, prev[j - 1] + cost))
        prev = curr
    return prev[-1]


def _is_noise_token(token: str) -> bool:
    if token in _NOISE_TOKENS:
        return True
    if token.isdigit():
        return True
    return len(token) <= 4 and token.isalpha()


def _matches_substring_rule(shorter: _Candidate, longer: _Candidate) -> bool:
    shorter_norm = _normalise_name(shorter.name)
    longer_norm = _normalise_name(longer.name)
    if not shorter_norm or not longer_norm:
        return False
    if shorter_norm not in longer_norm:
        return False
    if shorter_norm == longer_norm:
        return True

    shorter_tokens = set(_tokens(shorter_norm))
    longer_tokens = set(_tokens(longer_norm))
    extra_tokens = longer_tokens - shorter_tokens
    if extra_tokens and all(_is_noise_token(token) for token in extra_tokens):
        return True
    return _description_overlap(shorter.description, longer.description) >= 0.6


def _match_reason(left: _Candidate, right: _Candidate) -> Optional[str]:
    left_keys = {_normalise_name(name) for name in left.all_names if name}
    right_keys = {_normalise_name(name) for name in right.all_names if name}
    if left_keys & right_keys:
        return "exact_normalised"

    left_norm = _normalise_name(left.name)
    right_norm = _normalise_name(right.name)
    if not left_norm or not right_norm:
        return None

    if _matches_substring_rule(left, right) or _matches_substring_rule(right, left):
        return "substring_or_description_overlap"

    if _levenshtein(left_norm, right_norm) <= 2:
        return "levenshtein"
    return None


def _canonical_score(candidate: _Candidate) -> Tuple[int, int, int, int, int, str]:
    name = candidate.name.strip()
    normalised = _normalise_name(name)
    tokens = re.findall(r"[A-Za-z0-9]+", normalised)
    abbreviations = sum(
        1
        for token in re.findall(r"[A-Za-z0-9]+", name)
        if token.isalpha() and token.isupper() and len(token) <= 4
    )
    vendor_bonus = 1 if len(tokens) > 1 and tokens[0] not in _NOISE_TOKENS else 0
    parenthetical_penalty = 1 if "(" in name and ")" in name else 0
    return (
        len(tokens),
        len(normalised),
        -parenthetical_penalty,
        vendor_bonus,
        -abbreviations,
        name.lower(),
    )


def _cluster_candidates(candidates: Sequence[_Candidate]) -> List[List[_Candidate]]:
    parent = list(range(len(candidates)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        root_left = find(left)
        root_right = find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    for left_index, left in enumerate(candidates):
        for right_index in range(left_index + 1, len(candidates)):
            right = candidates[right_index]
            if left.entity_type != right.entity_type:
                continue
            if _match_reason(left, right):
                union(left_index, right_index)

    grouped: Dict[int, List[_Candidate]] = {}
    for index, candidate in enumerate(candidates):
        grouped.setdefault(find(index), []).append(candidate)
    return list(grouped.values())


async def _fetch_candidates(
    writer: Neo4jBatchWriter,
    database: str,
    company_id: str,
    entity_types: Sequence[str],
) -> List[_Candidate]:
    _assert_scoped_database(database)
    query = """
    MATCH (n:Entity)
    WHERE n.company_id = $company_id
      AND n.entity_type IN $entity_types
      AND coalesce(n.file_path, '') STARTS WITH 'document://'
    RETURN n.id AS id,
           n.name AS name,
           n.entity_type AS entity_type,
           coalesce(n.description, '') AS description,
           coalesce(n.aliases, []) AS aliases
    """
    async with writer._driver.session(database=database) as session:
        result = await session.run(
            query,
            {"company_id": company_id, "entity_types": list(entity_types)},
        )
        rows = [record async for record in result]

    candidates = [
        _Candidate(
            node_id=str(row["id"]),
            name=str(row["name"] or ""),
            entity_type=str(row["entity_type"] or ""),
            description=str(row["description"] or ""),
            aliases=tuple(str(alias) for alias in (row["aliases"] or [])),
            origin="existing",
        )
        for row in rows
    ]
    return candidates


async def _merge_cluster(
    writer: Neo4jBatchWriter,
    database: str,
    canonical: _Candidate,
    duplicates: Sequence[_Candidate],
) -> None:
    _assert_scoped_database(database)
    duplicate_ids = [
        candidate.node_id for candidate in duplicates if candidate.node_id != canonical.node_id
    ]
    if not duplicate_ids:
        return

    aliases_to_add = sorted(
        {
            alias
            for candidate in duplicates
            for alias in candidate.all_names
            if alias and alias != canonical.name
        }
    )
    query = """
    MATCH (canonical:Entity {id: $canonical_id})
    UNWIND $duplicate_ids AS duplicate_id
    MATCH (duplicate:Entity {id: duplicate_id})
    WITH canonical, collect(duplicate) AS duplicates
    CALL apoc.refactor.mergeNodes([canonical] + duplicates, {
        properties: 'discard',
        mergeRels: true,
        selfRel: false
    }) YIELD node
    WITH node
    SET node.aliases = apoc.coll.toSet(coalesce(node.aliases, []) + $aliases_to_add)
    RETURN node.id AS canonical_id
    """
    async with writer._driver.session(database=database) as session:
        await session.run(
            query,
            {
                "canonical_id": canonical.node_id,
                "duplicate_ids": duplicate_ids,
                "aliases_to_add": aliases_to_add,
            },
        )


async def canonicalise_document_entities(
    writer: Neo4jBatchWriter,
    *,
    company_id: str,
    database: str,
    document_entities: Sequence[Dict[str, Any]],
    content_type: str,
) -> CanonicalisationOutcome:
    if content_type != "document":
        logger.debug(
            "canonicalisation.skipped",
            reason="content_type=code",
            company_id=company_id,
            database=database,
        )
        return CanonicalisationOutcome()

    _assert_scoped_database(database)

    entity_types = sorted(
        {
            str(entity.get("entity_type", ""))
            for entity in document_entities
            if entity.get("entity_type")
        }
    )
    if not entity_types:
        return CanonicalisationOutcome()

    current_batch = [
        _Candidate(
            node_id=str(entity["entity_id"]),
            name=str(entity.get("name", "")),
            entity_type=str(entity.get("entity_type", "")),
            description=str(entity.get("description", "")),
            aliases=tuple(),
            origin="batch",
        )
        for entity in document_entities
        if entity.get("entity_type") and entity.get("name")
    ]
    existing = await _fetch_candidates(writer, database, company_id, entity_types)
    unique_candidates: Dict[str, _Candidate] = {}
    for candidate in [*existing, *current_batch]:
        previous = unique_candidates.get(candidate.node_id)
        if previous is None or (previous.origin != "batch" and candidate.origin == "batch"):
            unique_candidates[candidate.node_id] = candidate

    grouped = _cluster_candidates(list(unique_candidates.values()))

    outcome = CanonicalisationOutcome()
    for cluster in grouped:
        if len(cluster) < 2:
            continue

        canonical = max(cluster, key=_canonical_score)
        duplicates = [candidate for candidate in cluster if candidate.node_id != canonical.node_id]
        if not duplicates:
            continue

        mergeable_duplicates: list[_Candidate] = []
        for duplicate in duplicates:
            if _should_block_merge(canonical.name, duplicate.name):
                logger.info(
                    "canonicalisation.merge_rejected",
                    reason="trailing_suffix_mismatch",
                    candidate_a=canonical.name,
                    candidate_b=duplicate.name,
                    company_id=company_id,
                )
                continue
            mergeable_duplicates.append(duplicate)

        if not mergeable_duplicates:
            continue

        await _merge_cluster(writer, database, canonical, mergeable_duplicates)

        outcome.merge_count += len(mergeable_duplicates)
        outcome.canonical_metadata[canonical.node_id] = {
            "name": canonical.name,
            "description": canonical.description,
            "entity_type": canonical.entity_type,
        }
        for duplicate in mergeable_duplicates:
            if duplicate.origin == "batch":
                outcome.duplicate_to_canonical[duplicate.node_id] = canonical.node_id
            reason = _match_reason(duplicate, canonical) or "clustered_via_transitive_match"
            logger.info(
                "canonicalisation.merged",
                source=duplicate.name,
                target=canonical.name,
                reason=reason,
                company_id=company_id,
            )

    return outcome


def rewrite_document_batch_references(
    entities: Sequence[Dict[str, Any]],
    edges: Sequence[Dict[str, Any]],
    entity_chunk_mappings: Sequence[Dict[str, Any]],
    entity_document_mappings: Sequence[Dict[str, Any]],
    outcome: CanonicalisationOutcome,
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]]]:
    if not outcome.duplicate_to_canonical:
        return (
            list(entities),
            list(edges),
            list(entity_chunk_mappings),
            list(entity_document_mappings),
        )

    canonical_metadata = outcome.canonical_metadata

    def _remap_entity(entity: Dict[str, Any]) -> Dict[str, Any]:
        mapped_id = outcome.duplicate_to_canonical.get(entity["entity_id"], entity["entity_id"])
        canonical_info = canonical_metadata.get(mapped_id)
        remapped = dict(entity)
        remapped["entity_id"] = mapped_id
        if canonical_info:
            remapped["name"] = canonical_info["name"]
            remapped["description"] = canonical_info["description"]
        return remapped

    remapped_entities: list[Dict[str, Any]] = []
    seen_entity_ids: set[str] = set()
    for entity in entities:
        remapped = _remap_entity(entity)
        if remapped["entity_id"] in seen_entity_ids:
            continue
        seen_entity_ids.add(remapped["entity_id"])
        remapped_entities.append(remapped)

    def _remap_edge(edge: Dict[str, Any]) -> Dict[str, Any]:
        remapped = dict(edge)
        source_key = "source_entity_id" if "source_entity_id" in remapped else "source_id"
        target_key = "target_entity_id" if "target_entity_id" in remapped else "target_id"
        remapped[source_key] = outcome.duplicate_to_canonical.get(
            remapped.get(source_key, ""), remapped.get(source_key, "")
        )
        remapped[target_key] = outcome.duplicate_to_canonical.get(
            remapped.get(target_key, ""), remapped.get(target_key, "")
        )
        return remapped

    remapped_edges: list[Dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str]] = set()
    for edge in edges:
        remapped = _remap_edge(edge)
        source = str(remapped.get("source_entity_id") or remapped.get("source_id") or "")
        target = str(remapped.get("target_entity_id") or remapped.get("target_id") or "")
        relation = str(remapped.get("relationship_type", ""))
        edge_key = (source, relation, target)
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)
        remapped_edges.append(remapped)

    def _remap_mapping(mapping: Dict[str, Any]) -> Dict[str, Any]:
        remapped = dict(mapping)
        entity_id = str(remapped.get("entity_id", ""))
        remapped["entity_id"] = outcome.duplicate_to_canonical.get(entity_id, entity_id)
        return remapped

    remapped_entity_chunk_mappings = []
    seen_chunk_pairs: set[tuple[str, str]] = set()
    for mapping in entity_chunk_mappings:
        remapped = _remap_mapping(mapping)
        key = (str(remapped.get("chunk_id", "")), str(remapped.get("entity_id", "")))
        if key in seen_chunk_pairs:
            continue
        seen_chunk_pairs.add(key)
        remapped_entity_chunk_mappings.append(remapped)

    remapped_entity_document_mappings = []
    seen_doc_pairs: set[tuple[str, str]] = set()
    for mapping in entity_document_mappings:
        remapped = _remap_mapping(mapping)
        key = (str(remapped.get("file_path", "")), str(remapped.get("entity_id", "")))
        if key in seen_doc_pairs:
            continue
        seen_doc_pairs.add(key)
        remapped_entity_document_mappings.append(remapped)

    return (
        remapped_entities,
        remapped_edges,
        remapped_entity_chunk_mappings,
        remapped_entity_document_mappings,
    )

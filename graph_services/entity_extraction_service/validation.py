"""Pydantic validation models for entity extraction output.

Philosophy: repair-and-accept, never reject.
- Pydantic enforces structure (correct types, non-empty required strings).
- Quality issues (short descriptions, bad edge endpoints, wrong type strings) are
  REPAIRED automatically, never rejected.
- Chunks are NEVER dropped due to validation — only due to completely unparseable LLM output.

Used by processor.py after each LLM call. No retry loop for validation — call repair_extraction_result()
once and use the result immediately.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)


class ExtractionNode(BaseModel):
    """A single extracted entity node."""

    name: str = Field(..., min_length=1, description="Entity name")
    type: str = Field(..., min_length=1, description="Entity type from allowed set")
    description: str = Field(
        ...,
        min_length=1,
        description="Entity description — any non-empty string is valid",
    )
    business_domain: Optional[str] = Field(
        default=None, description="Business domain this entity belongs to"
    )
    technical_domain: Optional[str] = Field(
        default=None, description="Technical domain classification"
    )


class ExtractionEdge(BaseModel):
    """A directed relationship between two entity nodes."""

    source: str = Field(..., min_length=1, description="Source entity name")
    target: str = Field(..., min_length=1, description="Target entity name")
    type: str = Field(..., min_length=1, description="Relationship type from allowed set")
    description: Optional[str] = Field(default=None, description="Relationship description")

    @model_validator(mode="after")
    def source_not_equal_target(self) -> "ExtractionEdge":
        if self.source == self.target:
            raise ValueError(
                f"Self-loop detected: edge source and target are both '{self.source}'. "
                "An entity cannot have a relationship with itself."
            )
        return self


class ExtractionResult(BaseModel):
    """Full extraction output produced by the LLM — entities + relationships."""

    entities: List[ExtractionNode] = Field(
        default_factory=list, description="Extracted entity nodes"
    )
    relationships: List[ExtractionEdge] = Field(
        default_factory=list, description="Extracted relationship edges"
    )
    file_summary: Optional[str] = Field(
        default=None, description="Optional high-level summary of the file"
    )

    @model_validator(mode="after")
    def repair_graph_integrity(self) -> "ExtractionResult":
        """Repair graph integrity issues instead of raising.

        - Drops edges with unknown source/target (logs WARNING).
        - Deduplicates edges (same source, target, type triple).
        - Never raises — always returns self with cleaned data.
        """
        entity_names: Set[str] = {e.name for e in self.entities}

        # Drop edges with unknown endpoints (repair, not reject)
        valid_relationships = []
        for edge in self.relationships:
            missing = []
            if edge.source not in entity_names:
                missing.append(f"source='{edge.source}'")
            if edge.target not in entity_names:
                missing.append(f"target='{edge.target}'")
            if missing:
                logger.warning(
                    "validation.edge_dropped_unknown_endpoint",
                    extra={
                        "edge_source": edge.source,
                        "edge_target": edge.target,
                        "edge_type": edge.type,
                        "missing": missing,
                        "known_entities": sorted(entity_names),
                    },
                )
            else:
                valid_relationships.append(edge)

        # Deduplicate edges (same source, target, type triple)
        seen_edges: Set[Tuple[str, str, str]] = set()
        unique_relationships = []
        for edge in valid_relationships:
            key = (edge.source, edge.target, edge.type)
            if key not in seen_edges:
                seen_edges.add(key)
                unique_relationships.append(edge)

        self.relationships = unique_relationships
        return self


def repair_extraction_result(result: ExtractionResult) -> ExtractionResult:
    """Repair an ExtractionResult in-place, returning the repaired result.

    Repairs applied:
    - Empty/None descriptions → filled with entity name.
    - Edge endpoint unknown → edge dropped (logged at WARNING).
    - Duplicate edges → deduplicated (kept in model_validator).

    NEVER raises. Returns the repaired result unconditionally.
    """
    # Repair empty descriptions
    for node in result.entities:
        if not node.description or not node.description.strip():
            logger.warning(
                "validation.description_repaired",
                extra={"entity_name": node.name, "original_description": node.description},
            )
            node.description = node.name

    # Re-run model_validator to clean up edges (dedup + unknown endpoint drop)
    # We do this by re-validating the model data
    repaired = ExtractionResult.model_validate(result.model_dump())
    return repaired


# ── Type-constrained subclasses ──────────────────────────────────────


def make_validated_result(
    allowed_entity_types: List[str],
    allowed_relationship_types: List[str],
) -> type:
    """Build a runtime ExtractionResult subclass with type constraints.

    Returns a Pydantic model class that enforces entity/relationship types
    against the provided allowed lists. Used when the allowed sets are known
    at runtime (from extraction prompt or config).

    Args:
        allowed_entity_types: Allowed entity type strings (case-sensitive).
        allowed_relationship_types: Allowed relationship type strings (case-sensitive).

    Returns:
        A Pydantic model class equivalent to ExtractionResult but with
        field-level type validation applied during model_validator.
    """
    allowed_entity_set: Set[str] = set(allowed_entity_types)
    allowed_rel_set: Set[str] = set(allowed_relationship_types)

    class ConstrainedResult(ExtractionResult):
        @model_validator(mode="after")
        def validate_allowed_types(self) -> "ConstrainedResult":
            errors: List[str] = []

            if allowed_entity_set:
                for node in self.entities:
                    if node.type not in allowed_entity_set:
                        errors.append(
                            f"Entity '{node.name}' has invalid type '{node.type}'. "
                            f"Allowed types: {sorted(allowed_entity_set)}"
                        )

            if allowed_rel_set:
                for edge in self.relationships:
                    if edge.type not in allowed_rel_set:
                        errors.append(
                            f"Edge ({edge.source}) --[{edge.type}]--> ({edge.target}) "
                            f"has invalid relationship type. "
                            f"Allowed types: {sorted(allowed_rel_set)}"
                        )

            if errors:
                raise ValueError(
                    "Type constraint violations:\n" + "\n".join(f"  - {e}" for e in errors)
                )

            return self

    return ConstrainedResult


# ── KnowledgeGraph → ExtractionResult conversion ─────────────────────


def knowledge_graph_to_extraction_result(kg: Any) -> ExtractionResult:
    """Convert a Cognee KnowledgeGraph object to an ExtractionResult.

    The KnowledgeGraph uses numeric/UUID node IDs for edges; we map those
    back to entity names for the validation layer which operates on names.

    Empty descriptions are set to the entity name (repair-and-accept policy).
    Self-loop edges are silently dropped at conversion time.

    Args:
        kg: Cognee KnowledgeGraph with .nodes and .edges attributes.

    Returns:
        ExtractionResult (NOT yet type-constrained — call make_validated_result
        separately if type constraints are needed).

    Raises:
        ValueError: If the graph structure cannot be converted.
    """
    node_id_to_name: Dict[str, str] = {node.id: node.name for node in kg.nodes}

    entities: List[ExtractionNode] = []
    for node in kg.nodes:
        description = getattr(node, "description", "").strip()
        # Repair empty descriptions with entity name (repair-and-accept policy)
        if not description:
            logger.warning(
                "validation.description_missing_in_kg",
                extra={"entity_name": node.name},
            )
            description = node.name
        entities.append(
            ExtractionNode(
                name=node.name,
                type=node.type,
                description=description,
                business_domain=getattr(node, "business_domain", None),
                technical_domain=getattr(node, "technical_domain", None),
            )
        )

    relationships: List[ExtractionEdge] = []
    for edge in kg.edges:
        source_name = node_id_to_name.get(edge.source_node_id)
        target_name = node_id_to_name.get(edge.target_node_id)
        # Skip edges with missing node ID mappings
        if not source_name or not target_name:
            continue
        if source_name == target_name:
            # Self-loops are structurally invalid — drop silently
            continue
        relationships.append(
            ExtractionEdge(
                source=source_name,
                target=target_name,
                type=edge.relationship_name,
                description=getattr(edge, "description", None),
            )
        )

    return ExtractionResult(entities=entities, relationships=relationships)


def format_validation_errors(errors: List[str]) -> str:
    """Format validation errors as a prompt appendix for LLM retry.

    Returns a block that can be appended to the extraction prompt so the LLM
    understands what it got wrong and how to fix it.
    """
    lines = [
        "",
        "--- VALIDATION ERRORS FROM PREVIOUS ATTEMPT ---",
        "Your previous response had the following validation errors.",
        "Fix ALL of them in your next response:",
        "",
    ]
    for i, error in enumerate(errors, start=1):
        lines.append(f"{i}. {error}")
    lines.append("")
    lines.append("--- END OF ERRORS ---")
    lines.append("")
    return "\n".join(lines)

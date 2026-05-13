"""Tests for entity_extraction_service/validation.py.

Covers:
- ExtractionNode field constraints (name, type; description accepts any non-empty string)
- ExtractionEdge self-loop detection
- ExtractionResult graph integrity (repair policy: bad edges dropped, not rejected)
- repair_extraction_result() behavior
- make_validated_result type constraints
- knowledge_graph_to_extraction_result conversion
- format_validation_errors prompt appendix
"""

from types import SimpleNamespace
from typing import Any, List

import pytest
from pydantic import ValidationError

from entity_extraction_service.validation import (
    ExtractionEdge,
    ExtractionNode,
    ExtractionResult,
    format_validation_errors,
    knowledge_graph_to_extraction_result,
    make_validated_result,
    repair_extraction_result,
)


# ── Helpers ───────────────────────────────────────────────────────────


def _node(name: str = "MyClass", type_: str = "class", description: str = None) -> dict:
    return {
        "name": name,
        "type": type_,
        "description": description or f"A valid description for {name} that is long enough",
    }


def _edge(source: str, target: str, type_: str = "CALLS") -> dict:
    return {"source": source, "target": target, "type": type_}


def _make_kg(nodes: List[dict], edges: List[dict]) -> Any:
    """Build a minimal KnowledgeGraph-like namespace for conversion tests."""
    kg = SimpleNamespace()
    kg.nodes = [
        SimpleNamespace(
            id=n["id"],
            name=n["name"],
            type=n.get("type", "class"),
            description=n.get("description", ""),
            business_domain=n.get("business_domain"),
            technical_domain=n.get("technical_domain"),
        )
        for n in nodes
    ]
    kg.edges = [
        SimpleNamespace(
            source_node_id=e["source"],
            target_node_id=e["target"],
            relationship_name=e.get("rel", "CALLS"),
            description=e.get("description"),
        )
        for e in edges
    ]
    return kg


# ── ExtractionNode ────────────────────────────────────────────────────


class TestExtractionNode:
    def test_valid_node(self):
        node = ExtractionNode(
            name="Foo",
            type="class",
            description="A class that handles authentication in the system",
        )
        assert node.name == "Foo"
        assert node.type == "class"

    def test_name_cannot_be_empty(self):
        with pytest.raises(ValidationError) as exc_info:
            ExtractionNode(
                name="",
                type="class",
                description="A valid description that is long enough here",
            )
        errors = exc_info.value.errors()
        assert any("name" in str(e["loc"]) for e in errors)

    def test_short_description_accepted(self):
        """Any non-empty description is valid — no minimum length."""
        node = ExtractionNode(name="Foo", type="class", description="Short")
        assert node.description == "Short"

    def test_single_char_description_accepted(self):
        node = ExtractionNode(name="X", type="class", description="x")
        assert node.description == "x"

    def test_empty_description_rejected_by_pydantic(self):
        """Empty string still fails Pydantic min_length=1."""
        with pytest.raises(ValidationError):
            ExtractionNode(name="Foo", type="class", description="")

    def test_type_cannot_be_empty(self):
        with pytest.raises(ValidationError):
            ExtractionNode(
                name="Foo",
                type="",
                description="A valid description that is long enough here",
            )

    def test_optional_domain_fields(self):
        node = ExtractionNode(
            name="Foo",
            type="class",
            description="A valid description that is long enough here",
            business_domain="payments",
            technical_domain="backend",
        )
        assert node.business_domain == "payments"
        assert node.technical_domain == "backend"

    def test_optional_domain_fields_default_none(self):
        node = ExtractionNode(
            name="Foo",
            type="class",
            description="A valid description that is long enough here",
        )
        assert node.business_domain is None
        assert node.technical_domain is None


# ── ExtractionEdge ────────────────────────────────────────────────────


class TestExtractionEdge:
    def test_valid_edge(self):
        edge = ExtractionEdge(source="Foo", target="Bar", type="CALLS")
        assert edge.source == "Foo"
        assert edge.target == "Bar"

    def test_self_loop_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            ExtractionEdge(source="Foo", target="Foo", type="CALLS")
        errors = exc_info.value.errors()
        assert any("self-loop" in e["msg"].lower() or "source" in str(e["loc"]) for e in errors)

    def test_self_loop_error_message_contains_entity_name(self):
        with pytest.raises(ValidationError) as exc_info:
            ExtractionEdge(source="MyService", target="MyService", type="DEPENDS_ON")
        assert "MyService" in str(exc_info.value)

    def test_empty_source_raises(self):
        with pytest.raises(ValidationError):
            ExtractionEdge(source="", target="Bar", type="CALLS")

    def test_empty_target_raises(self):
        with pytest.raises(ValidationError):
            ExtractionEdge(source="Foo", target="", type="CALLS")

    def test_empty_type_raises(self):
        with pytest.raises(ValidationError):
            ExtractionEdge(source="Foo", target="Bar", type="")

    def test_optional_description(self):
        edge = ExtractionEdge(source="Foo", target="Bar", type="CALLS", description="invokes Bar")
        assert edge.description == "invokes Bar"

    def test_description_defaults_none(self):
        edge = ExtractionEdge(source="Foo", target="Bar", type="CALLS")
        assert edge.description is None


# ── ExtractionResult ──────────────────────────────────────────────────


class TestExtractionResult:
    def _valid_entities(self):
        return [
            _node("Foo", "class"),
            _node("Bar", "function"),
        ]

    def test_valid_result(self):
        result = ExtractionResult(
            entities=[ExtractionNode(**_node("Foo")), ExtractionNode(**_node("Bar"))],
            relationships=[ExtractionEdge(**_edge("Foo", "Bar"))],
        )
        assert len(result.entities) == 2
        assert len(result.relationships) == 1

    def test_empty_result_is_valid(self):
        result = ExtractionResult(entities=[], relationships=[])
        assert result.entities == []
        assert result.relationships == []

    def test_edge_with_unknown_source_is_dropped(self):
        """Repair policy: edge with unknown source is dropped, not rejected."""
        result = ExtractionResult(
            entities=[ExtractionNode(**_node("Bar"))],
            relationships=[ExtractionEdge(**_edge("UnknownSource", "Bar"))],
        )
        # Edge dropped, entity preserved
        assert len(result.entities) == 1
        assert len(result.relationships) == 0

    def test_edge_with_unknown_target_is_dropped(self):
        """Repair policy: edge with unknown target is dropped, not rejected."""
        result = ExtractionResult(
            entities=[ExtractionNode(**_node("Foo"))],
            relationships=[ExtractionEdge(**_edge("Foo", "MissingTarget"))],
        )
        assert len(result.entities) == 1
        assert len(result.relationships) == 0

    def test_both_unknown_endpoints_dropped(self):
        """Both edges with unknown endpoints are dropped."""
        result = ExtractionResult(
            entities=[ExtractionNode(**_node("Baz"))],
            relationships=[ExtractionEdge(**_edge("Ghost", "Phantom"))],
        )
        assert len(result.entities) == 1
        assert len(result.relationships) == 0

    def test_valid_edges_retained_alongside_dropped_invalid(self):
        """Valid edges are kept even when invalid ones are dropped."""
        result = ExtractionResult(
            entities=[ExtractionNode(**_node("Foo")), ExtractionNode(**_node("Bar"))],
            relationships=[
                ExtractionEdge(**_edge("Foo", "Bar")),  # valid
                ExtractionEdge(**_edge("Missing", "Bar")),  # invalid — dropped
            ],
        )
        assert len(result.relationships) == 1
        assert result.relationships[0].source == "Foo"

    def test_multiple_unknown_edges_all_dropped(self):
        """All edges with unknown endpoints are dropped."""
        result = ExtractionResult(
            entities=[ExtractionNode(**_node("Foo"))],
            relationships=[
                ExtractionEdge(**_edge("Missing1", "Foo")),
                ExtractionEdge(**_edge("Missing2", "Foo")),
            ],
        )
        assert len(result.relationships) == 0

    def test_duplicate_edge_deduplicated(self):
        """Duplicate edges are silently merged, not rejected."""
        entities = [
            ExtractionNode(**_node("Foo")),
            ExtractionNode(**_node("Bar")),
        ]
        result = ExtractionResult(
            entities=entities,
            relationships=[
                ExtractionEdge(**_edge("Foo", "Bar", "CALLS")),
                ExtractionEdge(**_edge("Foo", "Bar", "CALLS")),  # duplicate — should be merged
            ],
        )
        assert len(result.relationships) == 1
        assert result.relationships[0].source == "Foo"

    def test_same_source_target_different_type_is_allowed(self):
        """Two edges with same endpoints but different type are NOT duplicates."""
        entities = [
            ExtractionNode(**_node("Foo")),
            ExtractionNode(**_node("Bar")),
        ]
        result = ExtractionResult(
            entities=entities,
            relationships=[
                ExtractionEdge(**_edge("Foo", "Bar", "CALLS")),
                ExtractionEdge(**_edge("Foo", "Bar", "EXTENDS")),
            ],
        )
        assert len(result.relationships) == 2

    def test_file_summary_optional(self):
        result = ExtractionResult(
            entities=[ExtractionNode(**_node("Foo"))],
            relationships=[],
            file_summary="This file handles authentication.",
        )
        assert result.file_summary == "This file handles authentication."


# ── repair_extraction_result ──────────────────────────────────────────


class TestRepairExtractionResult:
    def test_empty_description_filled_with_entity_name(self):
        """Empty description is repaired to entity name."""
        # Build a node with a description that passes min_length=1 but is whitespace-only
        # We test repair via the repair function directly
        node = ExtractionNode(name="MyClass", type="class", description="x")
        node.description = ""  # Bypass Pydantic to simulate repaired scenario
        result = ExtractionResult(entities=[node], relationships=[])
        result.entities[0].description = ""  # Force empty to test repair

        repaired = repair_extraction_result(result)
        assert repaired.entities[0].description == "MyClass"

    def test_none_description_filled_with_entity_name(self):
        """None description is repaired to entity name."""
        node = ExtractionNode(name="FooService", type="class", description="placeholder")
        result = ExtractionResult(entities=[node], relationships=[])
        result.entities[0].description = None  # Force None

        repaired = repair_extraction_result(result)
        assert repaired.entities[0].description == "FooService"

    def test_valid_description_unchanged(self):
        """Valid descriptions are never modified."""
        node = ExtractionNode(name="Foo", type="class", description="This is a valid description")
        result = ExtractionResult(entities=[node], relationships=[])

        repaired = repair_extraction_result(result)
        assert repaired.entities[0].description == "This is a valid description"

    def test_short_description_unchanged(self):
        """Short (but non-empty) descriptions pass through unchanged."""
        node = ExtractionNode(name="Foo", type="class", description="hi")
        result = ExtractionResult(entities=[node], relationships=[])

        repaired = repair_extraction_result(result)
        assert repaired.entities[0].description == "hi"

    def test_repair_never_raises(self):
        """repair_extraction_result never raises regardless of input."""
        result = ExtractionResult(entities=[], relationships=[])
        # Should not raise
        repaired = repair_extraction_result(result)
        assert repaired is not None

    def test_repair_returns_extraction_result(self):
        """Returned value is always an ExtractionResult."""
        result = ExtractionResult(entities=[], relationships=[])
        repaired = repair_extraction_result(result)
        assert isinstance(repaired, ExtractionResult)

    def test_repair_deduplicates_edges(self):
        """Duplicate edges are removed during repair."""
        entities = [
            ExtractionNode(**_node("Foo")),
            ExtractionNode(**_node("Bar")),
        ]
        result = ExtractionResult(
            entities=entities,
            relationships=[
                ExtractionEdge(**_edge("Foo", "Bar", "CALLS")),
                ExtractionEdge(**_edge("Foo", "Bar", "CALLS")),
            ],
        )
        repaired = repair_extraction_result(result)
        assert len(repaired.relationships) == 1


# ── make_validated_result ─────────────────────────────────────────────


class TestMakeValidatedResult:
    def test_valid_types_pass(self):
        ResultClass = make_validated_result(
            allowed_entity_types=["class", "function"],
            allowed_relationship_types=["CALLS", "EXTENDS"],
        )
        result = ResultClass(
            entities=[
                ExtractionNode(**_node("Foo", "class")),
                ExtractionNode(**_node("bar", "function")),
            ],
            relationships=[ExtractionEdge(**_edge("Foo", "bar", "CALLS"))],
        )
        assert len(result.entities) == 2

    def test_invalid_entity_type_fails(self):
        ResultClass = make_validated_result(
            allowed_entity_types=["class", "function"],
            allowed_relationship_types=["CALLS"],
        )
        with pytest.raises((ValidationError, ValueError)) as exc_info:
            ResultClass(
                entities=[ExtractionNode(**_node("Foo", "unknown_type"))],
                relationships=[],
            )
        assert "unknown_type" in str(exc_info.value)

    def test_invalid_relationship_type_fails(self):
        ResultClass = make_validated_result(
            allowed_entity_types=["class"],
            allowed_relationship_types=["CALLS"],
        )
        entities = [
            ExtractionNode(**_node("Foo", "class")),
            ExtractionNode(**_node("Bar", "class")),
        ]
        with pytest.raises((ValidationError, ValueError)) as exc_info:
            ResultClass(
                entities=entities,
                relationships=[ExtractionEdge(**_edge("Foo", "Bar", "INVALID_REL"))],
            )
        assert "INVALID_REL" in str(exc_info.value)

    def test_empty_allowed_lists_skip_type_check(self):
        """Empty allowed lists means no type constraints are applied."""
        ResultClass = make_validated_result(
            allowed_entity_types=[],
            allowed_relationship_types=[],
        )
        entities = [
            ExtractionNode(**_node("Foo", "anything")),
            ExtractionNode(**_node("Bar", "whatever")),
        ]
        result = ResultClass(
            entities=entities,
            relationships=[ExtractionEdge(**_edge("Foo", "Bar", "RANDOM_TYPE"))],
        )
        assert len(result.entities) == 2


# ── knowledge_graph_to_extraction_result ─────────────────────────────


class TestKnowledgeGraphToExtractionResult:
    def test_basic_conversion(self):
        kg = _make_kg(
            nodes=[
                {
                    "id": "n1",
                    "name": "Foo",
                    "type": "class",
                    "description": "A class that does important things",
                },
                {
                    "id": "n2",
                    "name": "Bar",
                    "type": "function",
                    "description": "A function that processes data",
                },
            ],
            edges=[{"source": "n1", "target": "n2", "rel": "CALLS"}],
        )
        result = knowledge_graph_to_extraction_result(kg)
        assert len(result.entities) == 2
        assert len(result.relationships) == 1
        assert result.relationships[0].source == "Foo"
        assert result.relationships[0].target == "Bar"
        assert result.relationships[0].type == "CALLS"

    def test_entity_names_preserved(self):
        kg = _make_kg(
            nodes=[
                {
                    "id": "n1",
                    "name": "MyService",
                    "type": "class",
                    "description": "Handles payment processing logic",
                }
            ],
            edges=[],
        )
        result = knowledge_graph_to_extraction_result(kg)
        assert result.entities[0].name == "MyService"

    def test_edge_with_missing_node_skipped(self):
        """Edges referencing missing node IDs are silently skipped."""
        kg = _make_kg(
            nodes=[
                {
                    "id": "n1",
                    "name": "Foo",
                    "type": "class",
                    "description": "A class for foo operations here",
                }
            ],
            edges=[{"source": "n1", "target": "n_MISSING", "rel": "CALLS"}],
        )
        result = knowledge_graph_to_extraction_result(kg)
        assert len(result.relationships) == 0

    def test_self_loop_edges_are_dropped_during_conversion(self):
        """Self-loop edges (source == target) are silently dropped during conversion."""
        kg = _make_kg(
            nodes=[
                {
                    "id": "n1",
                    "name": "Foo",
                    "type": "class",
                    "description": "A class that is self-referential",
                }
            ],
            edges=[{"source": "n1", "target": "n1", "rel": "CALLS"}],  # self-loop
        )
        result = knowledge_graph_to_extraction_result(kg)
        # Self-loop edge is dropped — result has the entity but no relationships
        assert len(result.entities) == 1
        assert len(result.relationships) == 0

    def test_empty_description_repaired_to_entity_name(self):
        """Missing descriptions are repaired to the entity name (not a placeholder)."""
        kg = _make_kg(
            nodes=[{"id": "n1", "name": "Foo", "type": "class", "description": ""}],
            edges=[],
        )
        result = knowledge_graph_to_extraction_result(kg)
        # Repaired to entity name
        assert result.entities[0].description == "Foo"
        assert "_MISSING_DESCRIPTION_FOR_" not in result.entities[0].description

    def test_domain_fields_forwarded(self):
        kg = _make_kg(
            nodes=[
                {
                    "id": "n1",
                    "name": "Foo",
                    "type": "class",
                    "description": "A class that handles payments domain logic",
                    "business_domain": "payments",
                    "technical_domain": "backend",
                }
            ],
            edges=[],
        )
        result = knowledge_graph_to_extraction_result(kg)
        assert result.entities[0].business_domain == "payments"
        assert result.entities[0].technical_domain == "backend"

    def test_empty_kg_gives_empty_result(self):
        kg = _make_kg(nodes=[], edges=[])
        result = knowledge_graph_to_extraction_result(kg)
        assert result.entities == []
        assert result.relationships == []


# ── format_validation_errors ──────────────────────────────────────────


class TestFormatValidationErrors:
    def test_returns_string(self):
        output = format_validation_errors(["Error 1", "Error 2"])
        assert isinstance(output, str)

    def test_contains_all_errors(self):
        errors = ["Self-loop on Foo", "Unknown entity Bar", "Duplicate edge (A, B, CALLS)"]
        output = format_validation_errors(errors)
        for err in errors:
            assert err in output

    def test_contains_header_marker(self):
        output = format_validation_errors(["some error"])
        assert "VALIDATION ERRORS" in output

    def test_contains_end_marker(self):
        output = format_validation_errors(["some error"])
        assert "END OF ERRORS" in output

    def test_empty_errors_list(self):
        output = format_validation_errors([])
        # Should still produce the header/footer without crashing
        assert "VALIDATION ERRORS" in output

    def test_errors_are_numbered(self):
        output = format_validation_errors(["First error", "Second error"])
        assert "1." in output
        assert "2." in output

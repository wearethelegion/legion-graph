"""Unit tests for EnrichedChunkMessage model and build_document_chunk().

Tests the file_skeleton field addition, backward compatibility,
and the build_document_chunk conversion function.

Note: cognee imports are mocked to avoid pulling in the full cognee dependency
tree (which requires mistralai, instructor, etc.).
"""

import json
import sys
import uuid
from types import ModuleType
from unittest.mock import MagicMock

import pytest

# ── Mock cognee dependencies before importing models ──────────────────────
# The models.py file imports DocumentChunk and Document from cognee.
# We mock those modules so the import succeeds without the full cognee stack.

_mock_document_chunk = MagicMock()
_mock_document = MagicMock()


def _ensure_cognee_mocks():
    """Install minimal cognee module mocks if not already present."""
    mods = {
        "cognee": MagicMock(),
        "cognee.modules": MagicMock(),
        "cognee.modules.chunking": MagicMock(),
        "cognee.modules.chunking.models": MagicMock(),
        "cognee.modules.chunking.models.DocumentChunk": MagicMock(
            DocumentChunk=_mock_document_chunk
        ),
        "cognee.modules.data": MagicMock(),
        "cognee.modules.data.processing": MagicMock(),
        "cognee.modules.data.processing.document_types": MagicMock(Document=_mock_document),
    }
    for name, mod in mods.items():
        if name not in sys.modules:
            sys.modules[name] = mod


_ensure_cognee_mocks()

from cognee_service.kafka_consumer.enriched_chunks.models import (
    EnrichedChunkMessage,
    build_document_chunk,
)


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def base_message_data():
    """Minimal valid process-action message data."""
    _fv_id = str(uuid.uuid4())
    return {
        "chunk_id": str(uuid.uuid4()),
        "parent_id": _fv_id,
        "file_version_id": _fv_id,
        "ingestion_id": str(uuid.uuid4()),
        "company_id": str(uuid.uuid4()),
        "project_id": str(uuid.uuid4()),
        "repository": "owner/repo",
        "branch": "main",
        "file_path": "src/app.py",
        "language": "python",
        "chunk_index": 0,
        "total_chunks": 3,
        "content": "def hello(): pass",
        "header": "=== FILE ===\nPath: src/app.py",
        "embedding": [0.1] * 768,
    }


# ── file_skeleton field tests ─────────────────────────────────────────────


class TestFileSkeleton:
    def test_file_skeleton_default_is_empty_string(self, base_message_data):
        """file_skeleton should default to '' when not provided."""
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.file_skeleton == ""

    def test_file_skeleton_set_explicitly(self, base_message_data):
        """file_skeleton should accept a string value."""
        base_message_data["file_skeleton"] = "  def hello()\n  class Foo"
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.file_skeleton == "  def hello()\n  class Foo"

    def test_file_skeleton_serialization_roundtrip(self, base_message_data):
        """file_skeleton should survive JSON serialization/deserialization."""
        skeleton = "  class AppController\n  def index\n  def create"
        base_message_data["file_skeleton"] = skeleton

        msg = EnrichedChunkMessage(**base_message_data)
        json_str = msg.model_dump_json()
        restored = EnrichedChunkMessage.model_validate_json(json_str)

        assert restored.file_skeleton == skeleton

    def test_file_skeleton_in_model_dump(self, base_message_data):
        """file_skeleton should appear in model_dump() output."""
        base_message_data["file_skeleton"] = "skel"
        msg = EnrichedChunkMessage(**base_message_data)
        dump = msg.model_dump()
        assert "file_skeleton" in dump
        assert dump["file_skeleton"] == "skel"


# ── Backward compatibility ────────────────────────────────────────────────


class TestBackwardCompatibility:
    def test_message_without_file_skeleton_parses(self, base_message_data):
        """Messages produced by older preprocessors (no file_skeleton) should still parse."""
        base_message_data.pop("file_skeleton", None)
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.file_skeleton == ""

    def test_json_without_file_skeleton_parses(self, base_message_data):
        """JSON payloads from Kafka without file_skeleton should deserialize."""
        base_message_data.pop("file_skeleton", None)
        json_str = json.dumps(base_message_data)
        msg = EnrichedChunkMessage.model_validate_json(json_str)
        assert msg.file_skeleton == ""

    def test_message_without_header_parses(self, base_message_data):
        """header field should also default to '' for backward compat."""
        base_message_data.pop("header", None)
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.header == ""

    def test_delete_action_minimal_fields(self):
        """Delete action requires only a subset of fields."""
        msg = EnrichedChunkMessage(
            action="delete",
            company_id="c1",
            project_id="p1",
            repository="r",
            branch="main",
            file_path="removed.py",
            ingestion_id="ing-1",
            file_version_id="fv-001",
        )
        assert msg.action == "delete"
        assert msg.chunk_id is None
        assert msg.content is None
        assert msg.file_skeleton == ""

    def test_process_action_is_default(self, base_message_data):
        """Default action should be 'process'."""
        base_message_data.pop("action", None)
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.action == "process"


# ── Model field types and constraints ─────────────────────────────────────


class TestModelFieldTypes:
    def test_embedding_accepts_float_list(self, base_message_data):
        """embedding should accept a list of floats."""
        base_message_data["embedding"] = [0.1, 0.2, 0.3]
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.embedding == [0.1, 0.2, 0.3]

    def test_embedding_none_when_missing(self, base_message_data):
        """embedding should be None when not provided."""
        base_message_data.pop("embedding", None)
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.embedding is None

    def test_language_nullable(self, base_message_data):
        """language should accept None for unsupported files."""
        base_message_data["language"] = None
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.language is None

    def test_all_required_fields_present(self):
        """Should raise when missing required fields (company_id, project_id, etc.)."""
        with pytest.raises(Exception):
            EnrichedChunkMessage()  # Missing all required fields


# ── build_document_chunk ──────────────────────────────────────────────────


class TestBuildDocumentChunk:
    def test_build_document_chunk_raises_on_delete(self, base_message_data):
        """build_document_chunk should raise ValueError for delete actions."""
        base_message_data["action"] = "delete"
        msg = EnrichedChunkMessage(**base_message_data)

        with pytest.raises(ValueError, match="delete action"):
            build_document_chunk(msg)

    def test_build_document_chunk_raises_on_missing_required(self):
        """build_document_chunk should raise if required fields are None."""
        msg = EnrichedChunkMessage(
            action="process",
            company_id="c",
            project_id="p",
            repository="r",
            branch="main",
            file_path="f.py",
            ingestion_id="i",
            file_version_id="fv-001",
            # Missing: chunk_id, parent_id, content, chunk_index, total_chunks
        )

        with pytest.raises(ValueError, match="requires"):
            build_document_chunk(msg)

    def test_build_document_chunk_calls_cognee_constructors(self, base_message_data):
        """build_document_chunk should create Document and DocumentChunk objects."""
        msg = EnrichedChunkMessage(**base_message_data)

        # Since cognee is mocked, this should call the mock constructors
        result = build_document_chunk(msg)

        # The function returns whatever DocumentChunk mock returns
        _mock_document.assert_called_once()
        _mock_document_chunk.assert_called_once()

    def test_build_document_chunk_passes_content_as_text(self, base_message_data):
        """DocumentChunk.text should be the raw content, not header."""
        # Reset mocks
        _mock_document.reset_mock()
        _mock_document_chunk.reset_mock()

        msg = EnrichedChunkMessage(**base_message_data)
        build_document_chunk(msg)

        # Check DocumentChunk was called with text=content
        chunk_call_kwargs = _mock_document_chunk.call_args
        assert chunk_call_kwargs[1]["text"] == base_message_data["content"]

    def test_build_document_chunk_with_file_skeleton(self, base_message_data):
        """build_document_chunk should work when file_skeleton is present."""
        _mock_document.reset_mock()
        _mock_document_chunk.reset_mock()

        base_message_data["file_skeleton"] = "  def hello()\n  class Foo"
        msg = EnrichedChunkMessage(**base_message_data)

        # Should not raise
        build_document_chunk(msg)

        # file_skeleton doesn't go into DocumentChunk — it's on the message
        assert msg.file_skeleton == "  def hello()\n  class Foo"


# ── extraction_prompt field (Phase 2.3) ───────────────────────────────────────


class TestExtractionPrompt:
    def test_extraction_prompt_defaults_to_none(self, base_message_data):
        """extraction_prompt should be None when not provided (legacy messages)."""
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.extraction_prompt is None

    def test_extraction_prompt_set_explicitly(self, base_message_data):
        prompt = "Extract all function signatures and their dependencies."
        base_message_data["extraction_prompt"] = prompt
        msg = EnrichedChunkMessage(**base_message_data)
        assert msg.extraction_prompt == prompt

    def test_extraction_prompt_survives_json_roundtrip(self, base_message_data):
        prompt = "Focus on React component props and state usage."
        base_message_data["extraction_prompt"] = prompt
        msg = EnrichedChunkMessage(**base_message_data)
        json_str = msg.model_dump_json()
        restored = EnrichedChunkMessage.model_validate_json(json_str)
        assert restored.extraction_prompt == prompt

    def test_extraction_prompt_in_model_dump(self, base_message_data):
        prompt = "Identify all async functions and await chains."
        base_message_data["extraction_prompt"] = prompt
        msg = EnrichedChunkMessage(**base_message_data)
        dump = msg.model_dump()
        assert "extraction_prompt" in dump
        assert dump["extraction_prompt"] == prompt

    def test_extraction_prompt_none_in_model_dump(self, base_message_data):
        msg = EnrichedChunkMessage(**base_message_data)
        dump = msg.model_dump()
        assert "extraction_prompt" in dump
        assert dump["extraction_prompt"] is None

    def test_json_without_extraction_prompt_parses(self, base_message_data):
        """Kafka messages from V2 pipeline (no extraction_prompt) should still parse."""
        base_message_data.pop("extraction_prompt", None)
        import json

        json_str = json.dumps(base_message_data)
        msg = EnrichedChunkMessage.model_validate_json(json_str)
        assert msg.extraction_prompt is None

    def test_delete_action_has_no_extraction_prompt(self):
        """Delete messages should also default extraction_prompt to None."""
        msg = EnrichedChunkMessage(
            action="delete",
            company_id="c1",
            project_id="p1",
            repository="r",
            branch="main",
            file_path="removed.py",
            ingestion_id="ing-1",
            file_version_id="fv-001",
        )
        assert msg.extraction_prompt is None

    def test_extraction_prompt_none_indicates_legacy_or_no_profile(self, base_message_data):
        """None extraction_prompt is the documented signal for legacy / no-profile messages."""
        msg = EnrichedChunkMessage(**base_message_data)
        # V3 consumers should branch on this:
        is_v3 = msg.extraction_prompt is not None
        assert is_v3 is False  # default is legacy

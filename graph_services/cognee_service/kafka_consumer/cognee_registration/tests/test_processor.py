"""Unit tests for CogneeRegistrationProcessor.

All cognee and cognee_service imports are mocked so these tests run without
the full cognee stack installed.
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock
import uuid

import pytest


# ── Mock cognee dependencies at module level — MUST happen before any import
# that transitively touches cognee (including EnrichedChunkMessage model which
# imports from cognee.modules.chunking.models.DocumentChunk).

_mock_document_chunk = MagicMock()
_mock_document = MagicMock()


def _install_cognee_mocks():
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
        "cognee_service.cognee_patches": MagicMock(),
        "cognee_service.multi_tenancy": MagicMock(
            ensure_neo4j_database=AsyncMock(),
            set_company_context=MagicMock(),
        ),
    }
    for name, mod in mods.items():
        if name not in sys.modules:
            sys.modules[name] = mod


_install_cognee_mocks()

from cognee_service.kafka_consumer.enriched_chunks.models import EnrichedChunkMessage  # noqa: E402
from cognee_service.kafka_consumer.cognee_registration.processor import (  # noqa: E402
    CogneeRegistrationProcessor,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_msg(**kwargs) -> EnrichedChunkMessage:
    """Build a minimal valid process-action EnrichedChunkMessage."""
    defaults = {
        "action": "process",
        "company_id": str(uuid.uuid4()),
        "project_id": str(uuid.uuid4()),
        "repository": "my-org/my-repo",
        "branch": "main",
        "file_path": "src/app.py",
        "ingestion_id": str(uuid.uuid4()),
        "file_version_id": str(uuid.uuid4()),
        "chunk_id": str(uuid.uuid4()),
        "parent_id": str(uuid.uuid4()),
        "content": "def hello(): pass",
        "chunk_index": 0,
        "total_chunks": 1,
    }
    defaults.update(kwargs)
    if defaults.get("content_type") == "document":
        defaults["project_id"] = None
    return EnrichedChunkMessage(**defaults)


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestCogneeRegistrationProcessor:
    """Tests for CogneeRegistrationProcessor."""

    def test_build_dataset_name_simple_branch(self):
        """Dataset name replaces / and - in branch with _."""
        processor = CogneeRegistrationProcessor()
        msg = _make_msg(project_id="proj-123", branch="main")
        result = processor.build_dataset_name(msg)
        assert result == "proj-123_main_code"

    def test_build_dataset_name_complex_branch(self):
        """Branch with slashes and dashes is normalized."""
        processor = CogneeRegistrationProcessor()
        msg = _make_msg(project_id="proj-abc", branch="feature/my-branch")
        result = processor.build_dataset_name(msg)
        assert result == "proj-abc_feature_my_branch_code"

    def test_build_dataset_name_document_scoped(self):
        """Document content uses company-level knowledge dataset."""
        processor = CogneeRegistrationProcessor()
        msg = _make_msg(project_id=None, content_type="document")
        result = processor.build_dataset_name(msg)
        assert result.endswith("_knowledge")
        assert result.startswith(msg.company_id)

    def test_build_dataset_name_document_uses_company_scope(self):
        """Document chunks should use the company-level knowledge scope."""
        processor = CogneeRegistrationProcessor()
        msg = _make_msg(content_type="document", project_id=None, company_id="comp-123")
        result = processor.build_dataset_name(msg)
        assert result == "comp-123_knowledge"

    def test_code_path_requires_project_id(self):
        """Code chunks must still provide project_id."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            _make_msg(project_id=None)

    @pytest.mark.asyncio
    async def test_register_skips_delete_action(self):
        """Delete action messages are skipped — no _call_cognee_add call."""
        processor = CogneeRegistrationProcessor()
        processor._call_cognee_add = AsyncMock()

        msg = _make_msg(action="delete", content=None)
        await processor.register(msg)

        processor._call_cognee_add.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_skips_empty_content(self):
        """Messages with no content are skipped."""
        processor = CogneeRegistrationProcessor()
        processor._call_cognee_add = AsyncMock()

        msg = _make_msg(content=None)
        await processor.register(msg)

        processor._call_cognee_add.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_calls_cognee_add_for_process_action(self):
        """Process action messages trigger _call_cognee_add with correct args."""
        processor = CogneeRegistrationProcessor()
        processor._call_cognee_add = AsyncMock()

        project_id = "proj-xyz"
        company_id = "comp-abc"
        msg = _make_msg(
            project_id=project_id,
            company_id=company_id,
            branch="main",
            file_path="src/foo.py",
            content="print('hello')",
        )

        await processor.register(msg)

        processor._call_cognee_add.assert_awaited_once()
        call_kwargs = processor._call_cognee_add.await_args.kwargs
        assert call_kwargs["content"] == "print('hello')"
        assert call_kwargs["file_path"] == "src/foo.py"
        assert call_kwargs["company_id"] == company_id
        assert call_kwargs["dataset_name"] == f"{project_id}_main_code"

    @pytest.mark.asyncio
    async def test_call_cognee_add_prepends_source_header(self):
        """cognee.add() receives text with 'Source: {file_path}' prefix."""
        processor = CogneeRegistrationProcessor()

        captured = []

        mock_cognee = MagicMock()
        mock_cognee.add = AsyncMock(side_effect=lambda text, dataset_name: captured.append(text))

        mock_multi = MagicMock()
        mock_multi.ensure_neo4j_database = AsyncMock()
        mock_multi.set_company_context = MagicMock()

        sys.modules["cognee"] = mock_cognee
        sys.modules["cognee_service.multi_tenancy"] = mock_multi

        try:
            await processor._call_cognee_add(
                content="def foo(): pass",
                file_path="/src/foo.py",
                company_id="company-1",
                dataset_name="proj_main_code",
            )
        finally:
            # Restore mocks
            _install_cognee_mocks()

        assert len(captured) == 1
        assert captured[0].startswith("Source: /src/foo.py\n\n")
        assert "def foo(): pass" in captured[0]

    @pytest.mark.asyncio
    async def test_call_cognee_add_swallows_exceptions(self):
        """Exceptions from cognee.add() are caught — method does not raise."""
        processor = CogneeRegistrationProcessor()

        mock_cognee = MagicMock()
        mock_cognee.add = AsyncMock(side_effect=RuntimeError("Neo4j down"))

        mock_multi = MagicMock()
        mock_multi.ensure_neo4j_database = AsyncMock()
        mock_multi.set_company_context = MagicMock()

        sys.modules["cognee"] = mock_cognee
        sys.modules["cognee_service.multi_tenancy"] = mock_multi

        try:
            # Must not raise
            await processor._call_cognee_add(
                content="code",
                file_path="src/foo.py",
                company_id="company-1",
                dataset_name="proj_main_code",
            )
        finally:
            _install_cognee_mocks()

    @pytest.mark.asyncio
    async def test_call_cognee_add_sets_company_context(self):
        """ensure_neo4j_database and set_company_context are called correctly."""
        processor = CogneeRegistrationProcessor()

        mock_cognee = MagicMock()
        mock_cognee.add = AsyncMock()

        ensure_db = AsyncMock()
        set_ctx = MagicMock()

        mock_multi = MagicMock()
        mock_multi.ensure_neo4j_database = ensure_db
        mock_multi.set_company_context = set_ctx

        sys.modules["cognee"] = mock_cognee
        sys.modules["cognee_service.multi_tenancy"] = mock_multi

        try:
            await processor._call_cognee_add(
                content="code",
                file_path="src/foo.py",
                company_id="company-42",
                dataset_name="proj_main_code",
            )
        finally:
            _install_cognee_mocks()

        ensure_db.assert_awaited_once_with("company-42")
        set_ctx.assert_called_once_with("company-42")

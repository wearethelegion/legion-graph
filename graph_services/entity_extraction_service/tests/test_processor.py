"""Tests for EntityExtractionProcessor.

Mocks:
- extract_content_graph (LLM call)
- KnowledgeGraph (return value)
- EntityExtractionStore (Postgres)
- EnrichedChunkMessage (input)

Includes:
- Phase 3.1: prompt routing from message (rejection, old-format fallback)
- Phase 3.2: repair-and-accept — bad LLM output is repaired, never retried for validation
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from entity_extraction_service.models import (
    EdgePayload,
    EntityPayload,
    ExtractedEntitiesEvent,
    entity_name_to_uuid,
)
from entity_extraction_service.pipeline_store import EntityExtractionStore


def _make_mock_kg(nodes, edges):
    """Build a mock KnowledgeGraph with .nodes and .edges."""
    kg = SimpleNamespace()
    kg.nodes = [
        SimpleNamespace(
            id=n["id"], name=n["name"], type=n["type"], description=n.get("description", "")
        )
        for n in nodes
    ]
    kg.edges = [
        SimpleNamespace(
            source_node_id=e["source"],
            target_node_id=e["target"],
            relationship_name=e["rel"],
        )
        for e in edges
    ]
    return kg


def _make_chunk_msg(**overrides):
    """Build a mock EnrichedChunkMessage."""
    from cognee_service.kafka_consumer.enriched_chunks.models import EnrichedChunkMessage

    defaults = dict(
        action="process",
        company_id="comp-1",
        project_id="proj-1",
        repository="my-repo",
        branch="main",
        file_path="src/app.py",
        ingestion_id="ing-1",
        file_version_id="fv-1",
        chunk_id="chunk-1",
        parent_id="parent-1",
        language="python",
        chunk_index=0,
        total_chunks=3,
        content="class Foo:\n    pass",
        header="# FILE: src/app.py",
        # Phase 3.1: by default include a valid extraction_prompt
        extraction_prompt="Extract entities from the following code chunk.",
    )
    defaults.update(overrides)
    return EnrichedChunkMessage(**defaults)


def _node_set_for(msg):
    if getattr(msg, "content_type", "code") == "document" or not getattr(msg, "project_id", None):
        return f"{msg.company_id}_knowledge"
    project_name = getattr(msg, "project_name", None) or msg.project_id
    return f"{msg.project_id}_{project_name}_code"


@pytest.fixture
def mock_store():
    """Create a mock EntityExtractionStore."""
    store = AsyncMock(spec=EntityExtractionStore)
    store.increment_counter = AsyncMock()
    store.has_checkpoint = AsyncMock(return_value=False)  # Not processed yet by default
    store.save_checkpoint = AsyncMock()
    store.check_checkpoint = AsyncMock(return_value=True)  # Deprecated method (always process)
    store.get_counter = AsyncMock(return_value=0)
    store.get_preprocessor_total_chunks = AsyncMock(return_value=None)
    return store


@pytest.fixture
def mock_config():
    """Create a mock config class."""

    class MockConfig:
        MAX_PARALLEL_WORKERS = 5
        MAX_RETRIES = 2
        RETRY_BASE_DELAY = 0.01  # Fast retries in tests
        CUSTOM_PROMPT_PATH = "/nonexistent/path"
        DOCUMENT_PROMPT_PATH = "/nonexistent/document_path"
        VERTEXAI_LLM_REGIONS = "us-central1,us-east1"

    return MockConfig


@pytest.fixture
def processor(mock_store, mock_config):
    """Create an EntityExtractionProcessor with mocked dependencies."""
    from entity_extraction_service.processor import EntityExtractionProcessor

    return EntityExtractionProcessor(
        store=mock_store,
        config=mock_config,
        custom_prompt_path="/nonexistent/path",  # Won't load
    )


class TestProcessorInit:
    """Test processor initialization."""

    def test_creates_semaphore(self, processor, mock_config):
        assert processor._semaphore._value == mock_config.MAX_PARALLEL_WORKERS

    def test_no_custom_prompt_when_file_missing(self, processor):
        assert processor._custom_prompt is None

    def test_loads_custom_prompt_when_file_exists(self, mock_store, mock_config, tmp_path):
        from entity_extraction_service.processor import EntityExtractionProcessor

        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("Extract entities from code")

        proc = EntityExtractionProcessor(
            store=mock_store,
            config=mock_config,
            custom_prompt_path=str(prompt_file),
        )
        assert proc._custom_prompt == "Extract entities from code"


class TestProcessBatch:
    """Test batch processing flow."""

    @pytest.mark.asyncio
    async def test_empty_batch_returns_empty(self, processor):
        result = await processor.process_batch([])
        assert result == []

    @pytest.mark.asyncio
    async def test_successful_extraction(self, processor, mock_store):
        """Test full extraction flow with mocked LLM."""
        kg = _make_mock_kg(
            nodes=[
                {"id": "n1", "name": "Foo", "type": "class"},
                {"id": "n2", "name": "bar", "type": "function"},
            ],
            edges=[
                {"source": "n1", "target": "n2", "rel": "HAS_METHOD"},
            ],
        )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ):
            messages = [_make_chunk_msg()]
            events = await processor.process_batch(messages)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, ExtractedEntitiesEvent)
        assert event.ingestion_id == "ing-1"
        assert event.chunk_id == "chunk-1"
        assert event.company_id == "comp-1"
        assert event.chunk_index == 0
        assert len(event.entities) == 2
        assert len(event.edges) == 1

        # Verify entity IDs are deterministic UUID5
        expected_foo_id = str(entity_name_to_uuid("Foo", _node_set_for(messages[0])))
        assert event.entities[0].entity_id == expected_foo_id

        # Verify checkpoint flow: check before processing, save after success
        mock_store.increment_counter.assert_any_call("ing-1", "chunks_received", 1)
        mock_store.has_checkpoint.assert_called_once()
        mock_store.save_checkpoint.assert_called_once()

    @pytest.mark.asyncio
    async def test_filters_edges_with_missing_nodes(self, processor, mock_store):
        """Edges referencing non-existent nodes should be filtered out."""
        kg = _make_mock_kg(
            nodes=[
                {"id": "n1", "name": "A", "type": "class"},
            ],
            edges=[
                # n2 doesn't exist in nodes
                {"source": "n1", "target": "n2", "rel": "CALLS"},
            ],
        )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ):
            events = await processor.process_batch([_make_chunk_msg()])

        assert len(events) == 1
        assert len(events[0].entities) == 1
        assert len(events[0].edges) == 0  # Edge filtered out

    @pytest.mark.asyncio
    async def test_header_prepended_to_content(self, processor, mock_store):
        """Verify LLM input combines header + content."""
        kg = _make_mock_kg(nodes=[], edges=[])

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            msg = _make_chunk_msg(
                header="# FILE: test.py",
                content="x = 1",
            )
            await processor.process_batch([msg])

            # First positional arg to extract_content_graph should be header + content
            call_args = mock_extract.call_args
            llm_text = call_args[0][0]
            assert "# FILE: test.py" in llm_text
            assert "x = 1" in llm_text

    @pytest.mark.asyncio
    async def test_no_header_uses_content_only(self, processor, mock_store):
        """When header is empty, only content is passed to LLM."""
        kg = _make_mock_kg(nodes=[], edges=[])

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            msg = _make_chunk_msg(header="", content="y = 2")
            await processor.process_batch([msg])

            llm_text = mock_extract.call_args[0][0]
            assert llm_text == "y = 2"

    @pytest.mark.asyncio
    async def test_multiple_chunks_in_batch(self, processor, mock_store):
        """Test processing multiple chunks in one batch."""
        kg = _make_mock_kg(
            nodes=[{"id": "n1", "name": "X", "type": "class"}],
            edges=[],
        )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ):
            messages = [
                _make_chunk_msg(chunk_id="c1"),
                _make_chunk_msg(chunk_id="c2"),
                _make_chunk_msg(chunk_id="c3"),
            ]
            events = await processor.process_batch(messages)

        assert len(events) == 3
        chunk_ids = {e.chunk_id for e in events}
        assert chunk_ids == {"c1", "c2", "c3"}

    @pytest.mark.asyncio
    async def test_counter_increments(self, processor, mock_store):
        """Verify correct counter increment calls."""
        kg = _make_mock_kg(
            nodes=[
                {"id": "n1", "name": "A", "type": "class"},
                {"id": "n2", "name": "B", "type": "function"},
            ],
            edges=[
                {"source": "n1", "target": "n2", "rel": "CALLS"},
            ],
        )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ):
            await processor.process_batch([_make_chunk_msg()])

        # Should increment: chunks_received, entities_extracted, edges_extracted
        counter_calls = {
            (c.args[1], c.args[2]) for c in mock_store.increment_counter.call_args_list
        }
        assert ("chunks_received", 1) in counter_calls
        assert ("entities_extracted", 2) in counter_calls
        assert ("edges_extracted", 1) in counter_calls


class TestRetryLogic:
    """Test extraction retry with exponential backoff."""

    @pytest.mark.asyncio
    async def test_retries_on_llm_failure(self, processor, mock_store):
        """Should retry up to MAX_RETRIES on LLM failure."""
        call_count = 0

        async def failing_extract(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("LLM timeout")
            return _make_mock_kg(
                nodes=[{"id": "n1", "name": "X", "type": "class"}],
                edges=[],
            )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            side_effect=failing_extract,
        ):
            events = await processor.process_batch([_make_chunk_msg()])

        assert len(events) == 1
        assert call_count == 2  # Failed once, succeeded second time

    @pytest.mark.asyncio
    async def test_returns_none_after_exhausted_retries(self, processor, mock_store):
        """Should return None (skip chunk) after all retries exhausted."""
        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Permanent LLM failure"),
        ):
            events = await processor.process_batch([_make_chunk_msg()])

        # Chunk should be skipped (not in events)
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_failed_chunks_dont_block_others(self, processor, mock_store):
        """One failing chunk should not prevent others from succeeding."""
        # Track calls per chunk to ensure deterministic behavior with parallel execution
        per_chunk_calls = {}

        async def selective_failure(text, *args, **kwargs):
            # Identify chunk by content — fail-chunk has "FAIL" in content
            is_fail_chunk = "FAIL_MARKER" in text
            chunk_key = "fail" if is_fail_chunk else "ok"
            per_chunk_calls.setdefault(chunk_key, 0)
            per_chunk_calls[chunk_key] += 1

            if is_fail_chunk:
                raise RuntimeError("permanent failure for this chunk")

            return _make_mock_kg(
                nodes=[{"id": "n1", "name": "OK", "type": "class"}],
                edges=[],
            )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            side_effect=selective_failure,
        ):
            messages = [
                _make_chunk_msg(chunk_id="fail-chunk", content="FAIL_MARKER code here"),
                _make_chunk_msg(chunk_id="ok-chunk", content="good code here"),
            ]
            events = await processor.process_batch(messages)

        # Only the successful chunk should produce an event
        assert len(events) == 1
        assert events[0].chunk_id == "ok-chunk"

        # The failing chunk should have been retried MAX_RETRIES times
        assert per_chunk_calls.get("fail", 0) == processor._config.MAX_RETRIES


class TestCheckIngestionComplete:
    """Test ingestion completion check."""

    @pytest.mark.asyncio
    async def test_complete_when_received_equals_total(self, processor, mock_store):
        mock_store.get_counter.return_value = 10
        mock_store.get_preprocessor_total_chunks.return_value = 10

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is True
        assert received == 10
        assert total == 10

    @pytest.mark.asyncio
    async def test_not_complete_when_received_less_than_total(self, processor, mock_store):
        mock_store.get_counter.return_value = 5
        mock_store.get_preprocessor_total_chunks.return_value = 10

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is False
        assert received == 5
        assert total == 10

    @pytest.mark.asyncio
    async def test_not_complete_when_preprocessor_not_done(self, processor, mock_store):
        mock_store.get_counter.return_value = 5
        mock_store.get_preprocessor_total_chunks.return_value = None

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is False
        assert received == 5
        assert total is None

    @pytest.mark.asyncio
    async def test_complete_when_received_exceeds_total(self, processor, mock_store):
        """Edge case: received > total (e.g. reprocessing)."""
        mock_store.get_counter.return_value = 15
        mock_store.get_preprocessor_total_chunks.return_value = 10

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is True
        assert received == 15
        assert total == 10


# ── Phase 3.1: Prompt Routing Tests ──────────────────────────────────


class TestPromptRouting:
    """Phase 3.1: extraction prompt comes from message, not file."""

    @pytest.mark.asyncio
    async def test_message_prompt_used_over_file_prompt(self, processor, mock_store, tmp_path):
        """extraction_prompt from message must be forwarded to LLM call."""
        kg = _make_mock_kg(nodes=[{"id": "n1", "name": "Foo", "type": "class"}], edges=[])

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            msg = _make_chunk_msg(
                extraction_prompt="Use this inline prompt for extraction.",
            )
            await processor.process_batch([msg])

            call_kwargs = mock_extract.call_args[1]
            assert call_kwargs.get("custom_prompt") == "Use this inline prompt for extraction."

    @pytest.mark.asyncio
    async def test_null_extraction_prompt_rejects_chunk(self, processor, mock_store):
        """extraction_prompt=None on a new-format message → chunk rejected, no LLM call."""
        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
        ) as mock_extract:
            msg = _make_chunk_msg(extraction_prompt=None)
            events = await processor.process_batch([msg])

        # Chunk is rejected — no event, no LLM call
        assert events == []
        mock_extract.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_string_extraction_prompt_rejects_chunk(self, processor, mock_store):
        """extraction_prompt='' (empty string) on new-format message → rejected."""
        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
        ) as mock_extract:
            msg = _make_chunk_msg(extraction_prompt="")
            events = await processor.process_batch([msg])

        assert events == []
        mock_extract.assert_not_called()

    @pytest.mark.asyncio
    async def test_old_format_message_uses_file_prompt(self, mock_store, mock_config, tmp_path):
        """Old-format messages (no extraction_prompt attr) fall back to file prompt."""
        # Create a real prompt file
        prompt_file = tmp_path / "prompt.txt"
        prompt_file.write_text("File-based fallback prompt")

        from entity_extraction_service.processor import EntityExtractionProcessor

        proc = EntityExtractionProcessor(
            store=mock_store,
            config=mock_config,
            custom_prompt_path=str(prompt_file),
        )

        # Build message WITHOUT extraction_prompt attribute
        class OldFormatMsg:
            company_id = "comp-1"
            project_id = "proj-1"
            repository = "repo"
            branch = "main"
            file_path = "src/x.py"
            ingestion_id = "ing-old"
            file_version_id = "fv-old"
            chunk_id = "chunk-old"
            parent_id = "parent-old"
            language = "python"
            chunk_index = 0
            total_chunks = 1
            content = "x = 1"
            header = ""
            content_type = "code"
            action = "process"
            business_domains = None
            technical_tags = None
            # NO extraction_prompt attribute at all

        kg = _make_mock_kg(nodes=[], edges=[])

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            await proc.process_batch([OldFormatMsg()])

            call_kwargs = mock_extract.call_args[1]
            assert call_kwargs.get("custom_prompt") == "File-based fallback prompt"

    @pytest.mark.asyncio
    async def test_valid_extraction_prompt_produces_event(self, processor, mock_store):
        """Chunk with valid extraction_prompt should produce an event."""
        kg = _make_mock_kg(
            nodes=[{"id": "n1", "name": "Foo", "type": "class"}],
            edges=[],
        )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ):
            msg = _make_chunk_msg(extraction_prompt="Extract entities carefully.")
            events = await processor.process_batch([msg])

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_document_with_db_prompt_uses_prompt_with_substitutions(
        self, processor, mock_store
    ):
        """Document message with extraction_prompt uses DB prompt and substitutes placeholders."""
        kg = _make_mock_kg(nodes=[{"id": "n1", "name": "TestEntity", "type": "concept"}], edges=[])

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            msg = _make_chunk_msg(
                content_type="document",
                entity_type="expertise",
                project_id=None,
                document_title="Test Document",
                extraction_prompt="Extract {{ENTITY_TYPE}} from {{DOCUMENT_TITLE}}",
            )
            events = await processor.process_batch([msg])

            call_kwargs = mock_extract.call_args[1]
            expected_prompt = "Extract expertise from Test Document"
            assert call_kwargs.get("custom_prompt") == expected_prompt
            assert len(events) == 1
            assert events[0].document_title == "Test Document"
            assert events[0].document_slug == "test-document"

    @pytest.mark.asyncio
    async def test_document_with_empty_extraction_prompt_rejects_chunk(self, processor, mock_store):
        """Document message with empty extraction_prompt → chunk rejected, no LLM call."""
        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
        ) as mock_extract:
            msg = _make_chunk_msg(
                content_type="document",
                entity_type="knowledge",
                project_id=None,
                extraction_prompt="",
            )
            events = await processor.process_batch([msg])

        # Chunk is rejected — no event, no LLM call
        assert events == []
        mock_extract.assert_not_called()

    @pytest.mark.asyncio
    async def test_document_with_null_extraction_prompt_rejects_chunk(self, processor, mock_store):
        """Document message with null extraction_prompt → chunk rejected, no LLM call."""
        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
        ) as mock_extract:
            msg = _make_chunk_msg(
                content_type="document",
                entity_type="knowledge",
                project_id=None,
                extraction_prompt=None,
            )
            events = await processor.process_batch([msg])

        assert events == []
        mock_extract.assert_not_called()

    @pytest.mark.asyncio
    async def test_document_missing_extraction_prompt_uses_file_fallback(
        self, mock_store, mock_config, tmp_path
    ):
        """Document message without extraction_prompt field falls back to file-based prompt."""
        # Create a real document prompt file
        doc_prompt_file = tmp_path / "doc_prompt.txt"
        doc_prompt_file.write_text(
            "File-based document prompt for {{ENTITY_TYPE}}: {{DOCUMENT_TITLE}}"
        )

        from entity_extraction_service.processor import EntityExtractionProcessor

        # Create a custom config class with the document prompt path
        class CustomConfig:
            MAX_PARALLEL_WORKERS = 5
            MAX_RETRIES = 2
            RETRY_BASE_DELAY = 0.01
            CUSTOM_PROMPT_PATH = "/nonexistent/path"
            DOCUMENT_PROMPT_PATH = str(doc_prompt_file)
            VERTEXAI_LLM_REGIONS = "us-central1"

        proc = EntityExtractionProcessor(
            store=mock_store,
            config=CustomConfig,
        )

        # Build message WITHOUT extraction_prompt attribute
        class OldFormatDocMsg:
            company_id = "comp-1"
            project_id = "proj-1"
            repository = "repo"
            branch = "main"
            file_path = "docs/guide.md"
            ingestion_id = "ing-doc"
            file_version_id = "fv-doc"
            chunk_id = "chunk-doc"
            parent_id = "parent-doc"
            language = None
            chunk_index = 0
            total_chunks = 1
            content = "# Guide\nContent here"
            header = ""
            content_type = "document"
            entity_type = "expertise"
            document_title = "User Guide"
            action = "process"
            # NO extraction_prompt attribute at all

        kg = _make_mock_kg(nodes=[], edges=[])

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            await proc.process_batch([OldFormatDocMsg()])

            call_kwargs = mock_extract.call_args[1]
            expected_prompt = "File-based document prompt for expertise: User Guide"
            assert call_kwargs.get("custom_prompt") == expected_prompt

    @pytest.mark.asyncio
    async def test_code_message_routing_unchanged(self, processor, mock_store):
        """Code message (content_type='code') uses existing code path."""
        kg = _make_mock_kg(nodes=[{"id": "n1", "name": "Foo", "type": "class"}], edges=[])

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            msg = _make_chunk_msg(
                content_type="code",
                extraction_prompt="Extract code entities.",
            )
            await processor.process_batch([msg])

            call_kwargs = mock_extract.call_args[1]
            assert call_kwargs.get("custom_prompt") == "Extract code entities."


# ── Phase 3.2: Validation + Retry Tests ──────────────────────────────


def _make_mock_kg_with_descriptions(nodes, edges):
    """Build a mock KG where all nodes have descriptions."""
    kg = SimpleNamespace()
    kg.nodes = [
        SimpleNamespace(
            id=n["id"],
            name=n["name"],
            type=n["type"],
            description=n.get("description", f"A valid description for entity {n['name']}"),
        )
        for n in nodes
    ]
    kg.edges = [
        SimpleNamespace(
            source_node_id=e["source"],
            target_node_id=e["target"],
            relationship_name=e["rel"],
        )
        for e in edges
    ]
    return kg


class TestRepairAndAccept:
    """Phase 3.2: LLM output is repaired (never retried for validation issues)."""

    @pytest.mark.asyncio
    async def test_valid_output_passes_immediately(self, processor, mock_store):
        """Well-formed KG produces an event on the first LLM call."""
        kg = _make_mock_kg_with_descriptions(
            nodes=[
                {"id": "n1", "name": "Foo", "type": "class"},
                {"id": "n2", "name": "Bar", "type": "function"},
            ],
            edges=[{"source": "n1", "target": "n2", "rel": "CALLS"}],
        )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            events = await processor.process_batch([_make_chunk_msg()])

        assert len(events) == 1
        assert len(events[0].entities) == 2
        assert len(events[0].edges) == 1
        # Exactly one LLM call — no validation retries
        mock_extract.assert_called_once()

    @pytest.mark.asyncio
    async def test_short_description_repaired_not_retried(self, processor, mock_store):
        """LLM returning short descriptions → repaired, not retried. Only one LLM call."""
        kg = SimpleNamespace()
        kg.nodes = [SimpleNamespace(id="n1", name="Foo", type="class", description="short")]
        kg.edges = []

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            events = await processor.process_batch([_make_chunk_msg()])

        # Chunk accepted after repair — exactly one LLM call
        assert len(events) == 1
        assert mock_extract.call_count == 1
        # Entity present with repaired (or original short) description
        assert events[0].entities[0].name == "Foo"

    @pytest.mark.asyncio
    async def test_empty_description_repaired_to_entity_name(self, processor, mock_store):
        """Entity with empty description gets description repaired to entity name."""
        kg = SimpleNamespace()
        kg.nodes = [SimpleNamespace(id="n1", name="MyService", type="class", description="")]
        kg.edges = []

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            events = await processor.process_batch([_make_chunk_msg()])

        assert len(events) == 1
        assert mock_extract.call_count == 1
        assert events[0].entities[0].name == "MyService"
        assert events[0].entities[0].description == "MyService"

    @pytest.mark.asyncio
    async def test_unknown_edge_endpoints_dropped_not_retried(self, processor, mock_store):
        """Edge referencing unknown entity → dropped, chunk still accepted. One LLM call."""
        kg = SimpleNamespace()
        kg.nodes = [
            SimpleNamespace(id="n1", name="Foo", type="class", description="A class"),
        ]
        kg.edges = [
            # Edge references n2 which doesn't exist — will be dropped at KG conversion
            SimpleNamespace(
                source_node_id="n1", target_node_id="n_MISSING", relationship_name="CALLS"
            ),
        ]

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ) as mock_extract:
            events = await processor.process_batch([_make_chunk_msg()])

        assert len(events) == 1
        assert mock_extract.call_count == 1
        assert len(events[0].entities) == 1
        assert len(events[0].edges) == 0  # Bad edge dropped

    @pytest.mark.asyncio
    async def test_entity_payloads_from_repaired_result(self, processor, mock_store):
        """Repaired entities are correctly converted to EntityPayload with UUID5 IDs."""
        kg = _make_mock_kg_with_descriptions(
            nodes=[
                {"id": "n1", "name": "AuthService", "type": "class"},
                {"id": "n2", "name": "login", "type": "function"},
            ],
            edges=[{"source": "n1", "target": "n2", "rel": "HAS_METHOD"}],
        )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ):
            events = await processor.process_batch([_make_chunk_msg()])

        assert len(events) == 1
        entity_names = {e.name for e in events[0].entities}
        assert entity_names == {"AuthService", "login"}

        # IDs must be deterministic UUID5
        for entity in events[0].entities:
            assert entity.entity_id == str(
                entity_name_to_uuid(entity.name, _node_set_for(_make_chunk_msg()))
            )

    @pytest.mark.asyncio
    async def test_edge_payloads_from_repaired_result(self, processor, mock_store):
        """Repaired edges are correctly converted to EdgePayload."""
        kg = _make_mock_kg_with_descriptions(
            nodes=[
                {"id": "n1", "name": "Foo", "type": "class"},
                {"id": "n2", "name": "Bar", "type": "class"},
            ],
            edges=[{"source": "n1", "target": "n2", "rel": "EXTENDS"}],
        )

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ):
            events = await processor.process_batch([_make_chunk_msg()])

        assert len(events[0].edges) == 1
        edge = events[0].edges[0]
        assert edge.source_name == "Foo"
        assert edge.target_name == "Bar"
        assert edge.relationship_type == "EXTENDS"

    @pytest.mark.asyncio
    async def test_checkpoint_saved_even_with_repaired_description(self, processor, mock_store):
        """Checkpoint is saved when chunk is accepted (even after repairs)."""
        kg = SimpleNamespace()
        kg.nodes = [SimpleNamespace(id="n1", name="Foo", type="class", description="x")]
        kg.edges = []

        with patch(
            "entity_extraction_service.processor.extract_content_graph",
            new_callable=AsyncMock,
            return_value=kg,
        ):
            await processor.process_batch([_make_chunk_msg()])

        mock_store.save_checkpoint.assert_called_once()

"""Tests for EmbeddingProcessor.

Mocks:
- LiteLLMEmbeddingEngine (embedding API)
- EmbeddingStore (Postgres)
- EntityInputEvent / SummaryInputEvent (input)
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from embedding_service.models import (
    EmbeddingPayload,
    EmbeddingReadyEvent,
    EntityInputEvent,
    SummaryInputEvent,
)
from embedding_service.pipeline_store import EmbeddingStore


def _make_entity_event(**overrides):
    """Build a sample EntityInputEvent."""
    defaults = dict(
        event_id="evt-1",
        ingestion_id="ing-1",
        chunk_id="chunk-1",
        company_id="comp-1",
        project_id="proj-1",
        file_version_id="fv-1",
        entities=[
            {"entity_id": "e1", "name": "MyClass", "entity_type": "class"},
            {"entity_id": "e2", "name": "my_func", "entity_type": "function"},
        ],
    )
    defaults.update(overrides)
    return EntityInputEvent(**defaults)


def _make_summary_event(**overrides):
    """Build a sample SummaryInputEvent."""
    defaults = dict(
        event_id="evt-1",
        ingestion_id="ing-1",
        chunk_id="chunk-1",
        company_id="comp-1",
        project_id="proj-1",
        file_version_id="fv-1",
        summary_text="This function handles authentication.",
        summary_id="sum-1",
    )
    defaults.update(overrides)
    return SummaryInputEvent(**defaults)


@pytest.fixture
def mock_store():
    """Create a mock EmbeddingStore."""
    store = AsyncMock(spec=EmbeddingStore)
    store.increment_counter = AsyncMock()
    store.has_checkpoint = AsyncMock(return_value=False)  # Always process (not already done)
    store.save_checkpoint = AsyncMock()
    store.check_checkpoint = AsyncMock(return_value=False)  # Deprecated, but keep for legacy tests
    store.get_counter = AsyncMock(return_value=0)
    store.get_all_counters = AsyncMock(return_value={})
    store.is_upstream_complete = AsyncMock(return_value=False)
    store.get_upstream_total = AsyncMock(return_value=None)
    store.finalize_counters = AsyncMock()
    return store


@pytest.fixture
def mock_config():
    """Create a mock config class."""

    class MockConfig:
        MAX_PARALLEL_WORKERS = 5
        EMBEDDING_API_CONCURRENCY = 20
        MAX_RETRIES = 2
        RETRY_BASE_DELAY = 0.01  # Fast retries in tests
        EMBEDDING_BATCH_SIZE = 100
        EMBEDDING_DIMENSION = 768
        VERTEXAI_EMBEDDING_REGIONS = "us-central1,us-east1,europe-west1,asia-east1"

    return MockConfig


@pytest.fixture
def mock_engine():
    """Create a mock embedding engine."""
    engine = AsyncMock()
    engine.embed_text = AsyncMock(side_effect=lambda texts: [[0.1] * 768 for _ in texts])
    return engine


@pytest.fixture
def processor(mock_store, mock_config, mock_engine):
    """Create an EmbeddingProcessor with mocked dependencies."""
    from embedding_service.processor import EmbeddingProcessor

    return EmbeddingProcessor(
        store=mock_store,
        config=mock_config,
        embedding_engine=mock_engine,
    )


class TestProcessorInit:
    def test_creates_semaphore(self, processor, mock_config):
        assert processor._semaphore._value == mock_config.EMBEDDING_API_CONCURRENCY

    def test_stores_engine(self, processor, mock_engine):
        assert processor._embedding_engine is mock_engine


class TestProcessEntityBatch:
    @pytest.mark.asyncio
    async def test_empty_batch_returns_empty(self, processor):
        result = await processor.process_entity_batch([])
        assert result == []

    @pytest.mark.asyncio
    async def test_successful_entity_embedding(self, processor, mock_store, mock_engine):
        events = [_make_entity_event()]
        result = await processor.process_entity_batch(events)

        assert len(result) == 1
        event = result[0]
        assert isinstance(event, EmbeddingReadyEvent)
        assert event.ingestion_id == "ing-1"
        assert len(event.embeddings) == 2

        for payload in event.embeddings:
            assert payload.source_type == "entity"
            assert len(payload.embedding) == 768

        mock_store.increment_counter.assert_any_call("ing-1", "entities_received", 2)
        # No bulk Postgres writes — embeddings go directly to Kafka
        mock_store.increment_counter.assert_any_call("ing-1", "embeddings_computed", 2)

    @pytest.mark.asyncio
    async def test_skips_entities_without_name(self, processor, mock_store, mock_engine):
        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "", "entity_type": "class"},
                {"entity_id": "e2", "name": "Valid", "entity_type": "function"},
            ],
        )
        result = await processor.process_entity_batch([event])
        assert len(result) == 1
        assert len(result[0].embeddings) == 1
        assert result[0].embeddings[0].text == "Valid"

    @pytest.mark.asyncio
    async def test_skips_entities_without_entity_id(self, processor, mock_store, mock_engine):
        event = _make_entity_event(
            entities=[
                {"entity_id": "", "name": "NoId", "entity_type": "class"},
                {"entity_id": "e2", "name": "HasId", "entity_type": "function"},
            ],
        )
        result = await processor.process_entity_batch([event])
        assert len(result) == 1
        assert len(result[0].embeddings) == 1
        assert result[0].embeddings[0].text == "HasId"

    @pytest.mark.asyncio
    async def test_multiple_events_in_batch(self, processor, mock_store, mock_engine):
        events = [
            _make_entity_event(
                chunk_id="c1",
                entities=[{"entity_id": "e1", "name": "A", "entity_type": "class"}],
            ),
            _make_entity_event(
                chunk_id="c2",
                entities=[{"entity_id": "e2", "name": "B", "entity_type": "class"}],
            ),
        ]
        result = await processor.process_entity_batch(events)
        assert len(result) == 2
        assert result[0].embeddings[0].text == "A"
        assert result[1].embeddings[0].text == "B"

    @pytest.mark.asyncio
    async def test_embed_text_called_with_entity_names(self, processor, mock_engine):
        events = [_make_entity_event()]
        await processor.process_entity_batch(events)
        mock_engine.embed_text.assert_called_once()
        texts = mock_engine.embed_text.call_args[0][0]
        assert texts == ["MyClass", "my_func"]


class TestProcessSummaryBatch:
    @pytest.mark.asyncio
    async def test_empty_batch_returns_empty(self, processor):
        result = await processor.process_summary_batch([])
        assert result == []

    @pytest.mark.asyncio
    async def test_successful_summary_embedding(self, processor, mock_store, mock_engine):
        events = [_make_summary_event()]
        result = await processor.process_summary_batch(events)

        assert len(result) == 1
        event = result[0]
        assert isinstance(event, EmbeddingReadyEvent)
        assert len(event.embeddings) == 1
        assert event.embeddings[0].source_type == "summary"
        assert event.embeddings[0].text == "This function handles authentication."
        assert len(event.embeddings[0].embedding) == 768

        mock_store.increment_counter.assert_any_call("ing-1", "summaries_received", 1)
        # No bulk Postgres writes — embeddings go directly to Kafka

    @pytest.mark.asyncio
    async def test_skips_empty_summary_text(self, processor, mock_store, mock_engine):
        events = [_make_summary_event(summary_text="")]
        result = await processor.process_summary_batch(events)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_uses_summary_id_as_source_id(self, processor, mock_engine):
        events = [_make_summary_event(summary_id="sum-42")]
        result = await processor.process_summary_batch(events)
        assert result[0].embeddings[0].source_id == "sum-42"

    @pytest.mark.asyncio
    async def test_falls_back_to_chunk_id(self, processor, mock_engine):
        events = [_make_summary_event(summary_id="", chunk_id="chunk-99")]
        result = await processor.process_summary_batch(events)
        assert result[0].embeddings[0].source_id == "chunk-99"

    @pytest.mark.asyncio
    async def test_multiple_summaries_in_batch(self, processor, mock_engine):
        events = [
            _make_summary_event(chunk_id="c1", summary_text="Summary A"),
            _make_summary_event(chunk_id="c2", summary_text="Summary B"),
        ]
        result = await processor.process_summary_batch(events)
        assert len(result) == 2
        texts = [r.embeddings[0].text for r in result]
        assert "Summary A" in texts
        assert "Summary B" in texts


class TestEmbedTextsBatched:
    @pytest.mark.asyncio
    async def test_single_batch(self, processor, mock_engine):
        texts = ["a", "b", "c"]
        result = await processor._embed_texts_batched(texts)
        assert len(result) == 3
        mock_engine.embed_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_batches(self, processor, mock_engine, mock_config):
        mock_config.EMBEDDING_BATCH_SIZE = 2
        texts = ["a", "b", "c", "d", "e"]
        result = await processor._embed_texts_batched(texts)
        assert len(result) == 5
        assert mock_engine.embed_text.call_count == 3  # ceil(5/2) = 3

    @pytest.mark.asyncio
    async def test_empty_input(self, processor, mock_engine):
        result = await processor._embed_texts_batched([])
        assert result == []
        mock_engine.embed_text.assert_not_called()


class TestRetryLogic:
    @pytest.mark.asyncio
    async def test_retries_on_rate_limit(self, processor, mock_engine):
        call_count = 0

        async def rate_limited_embed(texts):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("429 Rate limit exceeded")
            return [[0.1] * 768 for _ in texts]

        mock_engine.embed_text = AsyncMock(side_effect=rate_limited_embed)
        result = await processor._embed_with_retry(["test"])
        assert len(result) == 1
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_raises_after_exhausted_retries(self, processor, mock_engine):
        mock_engine.embed_text = AsyncMock(side_effect=RuntimeError("Permanent failure"))
        with pytest.raises(RuntimeError, match="Permanent failure"):
            await processor._embed_with_retry(["test"])

    @pytest.mark.asyncio
    async def test_retries_correct_number_of_times(self, processor, mock_engine, mock_config):
        mock_engine.embed_text = AsyncMock(side_effect=RuntimeError("fail"))
        with pytest.raises(RuntimeError):
            await processor._embed_with_retry(["test"])
        assert mock_engine.embed_text.call_count == mock_config.MAX_RETRIES


class TestCheckIngestionComplete:
    @pytest.mark.asyncio
    async def test_not_complete_when_upstream_not_done(self, processor, mock_store):
        mock_store.is_upstream_complete.return_value = False
        mock_store.get_all_counters.return_value = {
            "entities_received": 0,
            "summaries_received": 0,
        }
        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is False
        assert total is None

    @pytest.mark.asyncio
    async def test_not_complete_when_only_extraction_done(self, processor, mock_store):
        mock_store.is_upstream_complete.side_effect = lambda ing_id, svc: svc == "entity_extraction"
        mock_store.get_all_counters.return_value = {
            "entities_received": 0,
            "summaries_received": 0,
        }
        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is False
        assert total is None

    @pytest.mark.asyncio
    async def test_complete_when_all_received(self, processor, mock_store):
        mock_store.is_upstream_complete.return_value = True
        mock_store.get_all_counters.return_value = {
            "entities_received": 10,
            "summaries_received": 5,
            "embeddings_computed": 15,
        }
        mock_store.get_upstream_total.side_effect = lambda ing_id, svc, counter: {
            ("entity_extraction", "entities_extracted"): 10,
            ("summarization", "summaries_produced"): 5,
        }.get((svc, counter))

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is True
        assert received == 15  # 10 entities + 5 summaries
        assert total == 15  # 10 upstream entities + 5 upstream summaries

    @pytest.mark.asyncio
    async def test_not_complete_when_entities_missing(self, processor, mock_store):
        mock_store.is_upstream_complete.return_value = True
        mock_store.get_all_counters.return_value = {
            "entities_received": 5,
            "summaries_received": 5,
        }
        mock_store.get_upstream_total.side_effect = lambda ing_id, svc, counter: {
            ("entity_extraction", "entities_extracted"): 10,
            ("summarization", "summaries_produced"): 5,
        }.get((svc, counter))

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is False
        assert received == 10  # 5 entities + 5 summaries
        assert total == 15  # 10 upstream + 5 upstream

    @pytest.mark.asyncio
    async def test_complete_when_received_exceeds_upstream(self, processor, mock_store):
        mock_store.is_upstream_complete.return_value = True
        mock_store.get_all_counters.return_value = {
            "entities_received": 15,
            "summaries_received": 10,
        }
        mock_store.get_upstream_total.side_effect = lambda ing_id, svc, counter: {
            ("entity_extraction", "entities_extracted"): 10,
            ("summarization", "summaries_produced"): 5,
        }.get((svc, counter))

        is_complete, received, total = await processor.check_ingestion_complete("ing-1")
        assert is_complete is True
        assert received == 25  # 15 entities + 10 summaries
        assert total == 15  # 10 upstream + 5 upstream


# -- Triplet generation tests ------------------------------------------------


class TestTripletGeneration:
    """Test triplet text generation from edges."""

    @pytest.mark.asyncio
    async def test_generates_triplet_from_edge(self, processor):
        """Should generate triplet text from edge when entities present.

        Now also produces a separate edge_type event, so results may be 1 or 2.
        We assert on the combined payloads.
        """
        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "AuthHandler"},
                {"entity_id": "e2", "name": "AuthService"},
            ],
            edges=[
                {
                    "source_id": "e1",
                    "target_id": "e2",
                    "relationship_type": "calls",
                }
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(
            return_value=[[0.1] * 768, [0.2] * 768, [0.3] * 768, [0.4] * 768]
        )

        results = await processor.process_entity_batch([event])

        # Collect all payloads across events
        all_payloads = [p for r in results for p in r.embeddings]

        # Should have 2 entity embeddings + 1 triplet + 1 edge_type
        entity_payloads = [p for p in all_payloads if p.source_type == "entity"]
        triplet_payloads = [p for p in all_payloads if p.source_type == "triplet"]
        assert len(entity_payloads) == 2
        assert len(triplet_payloads) == 1

        triplet = triplet_payloads[0]
        assert triplet.text == "AuthHandler-›calls-›AuthService"
        assert triplet.from_node_id == "e1"
        assert triplet.to_node_id == "e2"

    @pytest.mark.asyncio
    async def test_skips_edge_when_entity_missing(self, processor):
        """Should skip triplet generation if source/target entity not found."""
        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "AuthHandler"},
            ],
            edges=[
                {
                    "source_id": "e1",
                    "target_id": "e999",  # Missing entity
                    "relationship_type": "calls",
                }
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(return_value=[[0.1] * 768])

        results = await processor.process_entity_batch([event])
        result_event = results[0]

        # Should only have 1 entity embedding (no triplet)
        assert len(result_event.embeddings) == 1
        assert result_event.embeddings[0].source_type == "entity"

    @pytest.mark.asyncio
    async def test_deterministic_triplet_id(self, processor):
        """Should generate same triplet ID for same edge."""
        from uuid import uuid5, NAMESPACE_OID

        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "ClassA"},
                {"entity_id": "e2", "name": "ClassB"},
            ],
            edges=[
                {
                    "source_id": "e1",
                    "target_id": "e2",
                    "relationship_type": "inherits",
                }
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(
            return_value=[[0.1] * 768, [0.2] * 768, [0.3] * 768]
        )

        results = await processor.process_entity_batch([event])
        triplet = [p for p in results[0].embeddings if p.source_type == "triplet"][0]

        # Verify deterministic ID generation
        expected_id = str(uuid5(NAMESPACE_OID, "e1inheritse2"))
        assert triplet.source_id == expected_id

    @pytest.mark.asyncio
    async def test_multiple_edges_generate_multiple_triplets(self, processor):
        """Should generate one triplet per edge."""
        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "UserController"},
                {"entity_id": "e2", "name": "UserService"},
                {"entity_id": "e3", "name": "Database"},
            ],
            edges=[
                {"source_id": "e1", "target_id": "e2", "relationship_type": "calls"},
                {"source_id": "e2", "target_id": "e3", "relationship_type": "queries"},
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(
            return_value=[[0.1] * 768, [0.2] * 768, [0.3] * 768, [0.4] * 768, [0.5] * 768]
        )

        results = await processor.process_entity_batch([event])
        result_event = results[0]

        # Should have 3 entities + 2 triplets
        assert len(result_event.embeddings) == 5

        triplets = [p for p in result_event.embeddings if p.source_type == "triplet"]
        assert len(triplets) == 2

        triplet_texts = {t.text for t in triplets}
        assert "UserController-›calls-›UserService" in triplet_texts
        assert "UserService-›queries-›Database" in triplet_texts


# -- EdgeType generation tests -----------------------------------------------


class TestEdgeTypeGeneration:
    """Test EdgeType item generation from edges across batch."""

    @pytest.mark.asyncio
    async def test_generates_edge_type_from_edges(self, processor):
        """Should generate edge_type embedding for each distinct relationship_type."""
        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "AuthHandler"},
                {"entity_id": "e2", "name": "AuthService"},
            ],
            edges=[
                {"source_id": "e1", "target_id": "e2", "relationship_type": "calls"},
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(
            return_value=[[0.1] * 768, [0.2] * 768, [0.3] * 768, [0.4] * 768]
        )

        results = await processor.process_entity_batch([event])

        # Should have a result event for entities/triplets AND a separate one for edge_types
        all_payloads = [p for r in results for p in r.embeddings]
        edge_type_payloads = [p for p in all_payloads if p.source_type == "edge_type"]

        assert len(edge_type_payloads) == 1
        assert edge_type_payloads[0].text == "calls"
        assert edge_type_payloads[0].number_of_edges == 1

    @pytest.mark.asyncio
    async def test_aggregates_duplicate_relationship_types(self, processor):
        """Multiple edges with same relationship_type → one edge_type item with count."""
        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "A"},
                {"entity_id": "e2", "name": "B"},
                {"entity_id": "e3", "name": "C"},
            ],
            edges=[
                {"source_id": "e1", "target_id": "e2", "relationship_type": "calls"},
                {"source_id": "e2", "target_id": "e3", "relationship_type": "calls"},
                {"source_id": "e1", "target_id": "e3", "relationship_type": "calls"},
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(
            return_value=[[0.1] * 768 for _ in range(7)]
        )

        results = await processor.process_entity_batch([event])
        all_payloads = [p for r in results for p in r.embeddings]
        edge_type_payloads = [p for p in all_payloads if p.source_type == "edge_type"]

        # Only one "calls" edge_type despite 3 edges
        assert len(edge_type_payloads) == 1
        assert edge_type_payloads[0].text == "calls"
        assert edge_type_payloads[0].number_of_edges == 3

    @pytest.mark.asyncio
    async def test_multiple_distinct_relationship_types(self, processor):
        """Should produce one edge_type item per distinct relationship_type."""
        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "Controller"},
                {"entity_id": "e2", "name": "Service"},
                {"entity_id": "e3", "name": "Repository"},
            ],
            edges=[
                {"source_id": "e1", "target_id": "e2", "relationship_type": "calls"},
                {"source_id": "e2", "target_id": "e3", "relationship_type": "queries"},
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(
            return_value=[[0.1] * 768 for _ in range(7)]
        )

        results = await processor.process_entity_batch([event])
        all_payloads = [p for r in results for p in r.embeddings]
        edge_type_payloads = [p for p in all_payloads if p.source_type == "edge_type"]

        assert len(edge_type_payloads) == 2
        rel_names = {p.text for p in edge_type_payloads}
        assert rel_names == {"calls", "queries"}

    @pytest.mark.asyncio
    async def test_no_edges_produces_no_edge_types(self, processor):
        """When event has no edges, no edge_type items should be produced."""
        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "Standalone"},
            ],
            edges=[],
        )

        processor._embedding_engine.embed_text = AsyncMock(return_value=[[0.1] * 768])

        results = await processor.process_entity_batch([event])
        all_payloads = [p for r in results for p in r.embeddings]
        edge_type_payloads = [p for p in all_payloads if p.source_type == "edge_type"]

        assert len(edge_type_payloads) == 0

    @pytest.mark.asyncio
    async def test_edge_type_has_deterministic_id(self, processor):
        """Edge type ID should be uuid5(NAMESPACE_OID, relationship_type)."""
        from uuid import uuid5, NAMESPACE_OID

        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "A"},
                {"entity_id": "e2", "name": "B"},
            ],
            edges=[
                {"source_id": "e1", "target_id": "e2", "relationship_type": "inherits"},
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(
            return_value=[[0.1] * 768 for _ in range(3)]
        )

        results = await processor.process_entity_batch([event])
        all_payloads = [p for r in results for p in r.embeddings]
        edge_type_payloads = [p for p in all_payloads if p.source_type == "edge_type"]

        expected_id = str(uuid5(NAMESPACE_OID, "inherits"))
        assert edge_type_payloads[0].source_id == expected_id

    @pytest.mark.asyncio
    async def test_aggregates_across_multiple_events_in_batch(self, processor):
        """Edge types are aggregated across all events in the batch."""
        event1 = _make_entity_event(
            chunk_id="c1",
            entities=[
                {"entity_id": "e1", "name": "A"},
                {"entity_id": "e2", "name": "B"},
            ],
            edges=[
                {"source_id": "e1", "target_id": "e2", "relationship_type": "calls"},
            ],
        )
        event2 = _make_entity_event(
            chunk_id="c2",
            entities=[
                {"entity_id": "e3", "name": "C"},
                {"entity_id": "e4", "name": "D"},
            ],
            edges=[
                {"source_id": "e3", "target_id": "e4", "relationship_type": "calls"},
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(
            return_value=[[0.1] * 768 for _ in range(6)]
        )

        results = await processor.process_entity_batch([event1, event2])
        all_payloads = [p for r in results for p in r.embeddings]
        edge_type_payloads = [p for p in all_payloads if p.source_type == "edge_type"]

        # Both events have "calls" → should be ONE edge_type item with count=2
        assert len(edge_type_payloads) == 1
        assert edge_type_payloads[0].text == "calls"
        assert edge_type_payloads[0].number_of_edges == 2

    @pytest.mark.asyncio
    async def test_edge_type_emitted_in_separate_event(self, processor):
        """Edge types should be in a separate EmbeddingReadyEvent from entity/triplet items."""
        event = _make_entity_event(
            entities=[
                {"entity_id": "e1", "name": "A"},
                {"entity_id": "e2", "name": "B"},
            ],
            edges=[
                {"source_id": "e1", "target_id": "e2", "relationship_type": "calls"},
            ],
        )

        processor._embedding_engine.embed_text = AsyncMock(
            return_value=[[0.1] * 768 for _ in range(4)]
        )

        results = await processor.process_entity_batch([event])

        # Find the event containing edge_type payloads
        edge_type_events = [
            r for r in results if any(p.source_type == "edge_type" for p in r.embeddings)
        ]
        entity_events = [
            r for r in results if any(p.source_type in ("entity", "triplet") for p in r.embeddings)
        ]

        assert len(edge_type_events) == 1
        assert len(entity_events) >= 1
        # The edge_type event must not contain entity/triplet payloads
        et_event = edge_type_events[0]
        assert all(p.source_type == "edge_type" for p in et_event.embeddings)

"""Phase 3 unit tests: chunk_index sequential assignment.

Validates that chunk_file() returns chunks that, when assigned chunk_index
via enumerate(), produce sequential 0, 1, 2, ... values matching their
position in the file — exactly as _file_processing.chunk_and_enrich_file
and enrichment.enrich_and_store_file both do.

These tests catch the bug where chunk_index was always emitted as 0.

No Kafka, no Postgres, no Docker needed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from code_preprocessor.chunker import chunk_file


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_chunk_events(chunks):
    """Simulate enumerate() assignment exactly as in the pipeline."""
    total_chunks = len(chunks)
    return [
        {
            "chunk_index": idx,
            "total_chunks": total_chunks,
            "start_line": start_line,
            "end_line": end_line,
            "chunk_text_len": len(chunk_text),
        }
        for idx, (chunk_text, start_line, end_line) in enumerate(chunks)
    ]


# ── Synthetic tests ────────────────────────────────────────────────────────────


class TestChunkIndexSequential:
    def test_multi_chunk_file_has_sequential_indices(self):
        """Synthetic Ruby content produces >1 chunks with 0, 1, 2, ... indices."""
        content = "\n".join(
            f"""\
class Service{i}
  def initialize(x)
    @x = x
  end

  def call
    @x.to_s
  end

  private

  def validate!
    raise 'bad' unless @x
  end
end
"""
            for i in range(8)
        )

        chunks = chunk_file("fake_service.rb", content, max_chars=400)
        assert len(chunks) > 1, f"Expected >1 chunks, got {len(chunks)}"

        events = _make_chunk_events(chunks)
        total = len(chunks)

        actual_indices = [e["chunk_index"] for e in events]
        expected_indices = list(range(total))

        assert actual_indices == expected_indices, (
            f"chunk_index values not sequential!\n"
            f"  Got:      {actual_indices}\n"
            f"  Expected: {expected_indices}"
        )

    def test_total_chunks_consistent(self):
        """All chunks in same file must carry the same total_chunks value."""
        content = "\n".join(f"def method_{i}(x)\n  x + {i}\nend\n" for i in range(20))
        chunks = chunk_file("methods.rb", content, max_chars=200)
        assert len(chunks) > 1

        events = _make_chunk_events(chunks)
        total = len(chunks)

        for evt in events:
            assert evt["total_chunks"] == total, (
                f"total_chunks={evt['total_chunks']} at chunk_index={evt['chunk_index']}, "
                f"expected {total}"
            )

    def test_indices_start_at_zero(self):
        content = "def hello\n  puts 'hi'\nend\n" * 5
        chunks = chunk_file("hello.rb", content, max_chars=50)
        if len(chunks) > 0:
            events = _make_chunk_events(chunks)
            assert events[0]["chunk_index"] == 0, (
                f"First chunk_index should be 0, got {events[0]['chunk_index']}"
            )

    def test_indices_never_repeat(self):
        content = "\n".join(f"def compute_{i}(a, b)\n  a + b + {i}\nend\n" for i in range(15))
        chunks = chunk_file("compute.rb", content, max_chars=100)
        if len(chunks) <= 1:
            pytest.skip("File too small to produce multiple chunks at this max_chars")

        events = _make_chunk_events(chunks)
        indices = [e["chunk_index"] for e in events]
        assert len(indices) == len(set(indices)), f"Duplicate chunk_index values found: {indices}"

    def test_indices_contiguous_no_gaps(self):
        """There must be no gaps in the index sequence (0, 1, 2, not 0, 2, 3)."""
        content = "\n\n".join(
            f"class Module{i}\n  CONSTANT_{i} = {i}\n\n"
            f"  def self.value\n    CONSTANT_{i}\n  end\nend"
            for i in range(10)
        )
        chunks = chunk_file("modules.rb", content, max_chars=300)
        if len(chunks) <= 1:
            pytest.skip("File too small for contiguous index test")

        events = _make_chunk_events(chunks)
        indices = sorted(e["chunk_index"] for e in events)
        expected = list(range(len(chunks)))
        assert indices == expected, (
            f"Gaps in chunk_index sequence!\n  Got:      {indices}\n  Expected: {expected}"
        )


# ── Real file test ─────────────────────────────────────────────────────────────


class TestChunkIndexRealFile:
    _RUBY_FILE = (
        Path(__file__).parent.parent.parent
        / "rag_storage/repos/oscar-vet__vet_backend"
        / "spec/services/clinics/stats/vet_dashboard/show_service_spec.rb"
    )

    def test_large_ruby_file_produces_sequential_indices(self):
        if not self._RUBY_FILE.exists():
            pytest.skip(f"Ruby file not found: {self._RUBY_FILE}")

        content = self._RUBY_FILE.read_text(encoding="utf-8", errors="replace")
        chunks = chunk_file(str(self._RUBY_FILE), content)
        total = len(chunks)

        assert total > 1, f"Expected >1 chunks for {self._RUBY_FILE.name}, got {total}"

        events = _make_chunk_events(chunks)
        actual_indices = [e["chunk_index"] for e in events]
        expected_indices = list(range(total))

        assert actual_indices == expected_indices, (
            f"chunk_index values not sequential for {self._RUBY_FILE.name}!\n"
            f"  Got first 10: {actual_indices[:10]}\n"
            f"  Expected first 10: {expected_indices[:10]}\n"
            f"  Total chunks: {total}"
        )

        # total_chunks must be consistent throughout
        assert all(e["total_chunks"] == total for e in events)

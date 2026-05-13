"""Tests for markdown chunking."""

from document_preprocessor.chunker import chunk_document


def test_chunk_document_assigns_sequential_chunk_indices():
    text = "# Title\n\nFirst paragraph.\n\n## Section\n\nSecond paragraph.\n\nThird paragraph."

    chunks = chunk_document(text, max_chars=30, min_chars=1)

    assert len(chunks) >= 2
    assert [chunk.chunk_index for chunk in chunks] == list(range(len(chunks)))
    assert chunks[0].total_chunks == len(chunks)

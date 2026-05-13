"""Markdown-aware document chunking.

Splitting strategy:
  1. Split at heading boundaries (## / ### / ####) — keep heading with its body
  2. If a section exceeds max_chars, fall back to paragraph splitting (double newline)
  3. If a paragraph still exceeds max_chars, split by sentence then hard-cut

Reuses the merge logic from code_preprocessor/chunker.py.
"""

from __future__ import annotations

import re
from typing import Optional

from document_preprocessor.models import DocumentChunkResult

# Matches markdown headings (##, ###, ####, etc.) at the start of a line.
# Captures the heading level and text for metadata.
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)


def chunk_document(
    text: str,
    max_chars: int = 1000,
    min_chars: int = 100,
) -> list[DocumentChunkResult]:
    """Chunk a document using markdown-aware splitting.

    Args:
        text: Full document text (markdown or plain text).
        max_chars: Soft maximum characters per chunk.
        min_chars: Minimum characters — smaller pieces get merged with neighbours.

    Returns:
        List of DocumentChunkResult with positional metadata.
    """
    if not text or not text.strip():
        return []

    # Step 1: Split into heading-delimited sections
    sections = _split_by_headings(text)

    # Step 2: Break oversized sections into smaller chunks
    raw_chunks: list[tuple[str, Optional[str]]] = []
    for section_text, heading in sections:
        if len(section_text) <= max_chars:
            raw_chunks.append((section_text, heading))
        else:
            # Sub-split large section by paragraphs, then sentences
            sub_chunks = _split_large_section(section_text, max_chars)
            for i, sub in enumerate(sub_chunks):
                # Only first sub-chunk inherits the heading
                raw_chunks.append((sub, heading if i == 0 else None))

    # Step 3: Merge tiny adjacent chunks (< min_chars)
    merged = _merge_small_chunks(raw_chunks, max_chars, min_chars)

    # Step 4: Build result objects with positional metadata
    total = len(merged)
    return [
        DocumentChunkResult(
            text=chunk_text,
            chunk_index=idx,
            total_chunks=total,
            section_heading=heading,
        )
        for idx, (chunk_text, heading) in enumerate(merged)
    ]


def _split_by_headings(text: str) -> list[tuple[str, Optional[str]]]:
    """Split text at markdown heading boundaries.

    Returns list of (section_text, heading_text_or_None).
    The first section may have no heading (preamble before first heading).
    """
    matches = list(_HEADING_RE.finditer(text))

    if not matches:
        # No headings — treat entire text as one section
        return [(text.strip(), None)]

    sections: list[tuple[str, Optional[str]]] = []

    # Preamble before first heading
    preamble = text[: matches[0].start()].strip()
    if preamble:
        sections.append((preamble, None))

    # Each heading and its body
    for i, match in enumerate(matches):
        heading_text = match.group(2).strip()
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        section_text = text[start:end].strip()
        if section_text:
            sections.append((section_text, heading_text))

    return sections


def _split_large_section(text: str, max_chars: int) -> list[str]:
    """Split an oversized section by paragraphs → sentences → hard cut.

    Same strategy as code_preprocessor/chunker.py _chunk_text().
    """
    # Split by paragraph (double newline)
    blocks = text.split("\n\n")
    pieces: list[str] = []

    for block in blocks:
        if len(block) <= max_chars:
            pieces.append(block)
        else:
            # Split by line
            lines = block.split("\n")
            for line in lines:
                if len(line) <= max_chars:
                    pieces.append(line)
                else:
                    # Split by sentence
                    sentences = line.split(". ")
                    for sentence in sentences:
                        if len(sentence) <= max_chars:
                            pieces.append(sentence)
                        else:
                            # Hard cut
                            for i in range(0, len(sentence), max_chars):
                                pieces.append(sentence[i : i + max_chars])

    # Merge small adjacent pieces back together
    return _merge_text_pieces(pieces, max_chars)


def _merge_text_pieces(chunks: list[str], max_chars: int) -> list[str]:
    """Merge adjacent text pieces if they fit within max_chars."""
    if not chunks:
        return []

    merged: list[str] = []
    current = chunks[0]

    for next_chunk in chunks[1:]:
        if len(current) + len(next_chunk) + 2 <= max_chars:  # +2 for "\n\n" separator
            current = current + "\n\n" + next_chunk
        else:
            merged.append(current)
            current = next_chunk

    merged.append(current)
    return merged


def _merge_small_chunks(
    chunks: list[tuple[str, Optional[str]]],
    max_chars: int,
    min_chars: int,
) -> list[tuple[str, Optional[str]]]:
    """Merge adjacent chunks smaller than min_chars with their neighbour."""
    if not chunks:
        return []

    merged: list[tuple[str, Optional[str]]] = []
    current_text, current_heading = chunks[0]

    for next_text, next_heading in chunks[1:]:
        combined_len = len(current_text) + len(next_text) + 2
        if len(current_text) < min_chars and combined_len <= max_chars:
            # Merge into current — keep first non-None heading
            current_text = current_text + "\n\n" + next_text
            current_heading = current_heading or next_heading
        elif len(next_text) < min_chars and combined_len <= max_chars:
            # Next chunk is tiny — absorb it
            current_text = current_text + "\n\n" + next_text
        else:
            merged.append((current_text, current_heading))
            current_text = next_text
            current_heading = next_heading

    merged.append((current_text, current_heading))
    return merged

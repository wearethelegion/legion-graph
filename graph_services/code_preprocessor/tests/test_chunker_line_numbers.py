"""Unit tests for line number tracking in chunker.py.

Verifies that chunk_file() returns accurate (text, start_line, end_line)
tuples for both TreeSitter code files and plain text files.
"""

import textwrap

import pytest

from code_preprocessor.chunker import (
    MIN_CHUNK_SIZE,
    _chunk_text,
    _merge_small_chunks,
    chunk_file,
)


# ── Helpers ─────────────────────────────────────────────────────────────────


def _lines(content: str) -> list[str]:
    return content.splitlines()


# ── chunk_file return type ───────────────────────────────────────────────────


class TestChunkFileReturnType:
    def test_returns_list_of_tuples(self):
        content = "x = 1\ny = 2\nz = 3\n" * 20  # enough for a chunk
        result = chunk_file("test.py", content)
        assert isinstance(result, list)
        for item in result:
            assert isinstance(item, tuple), f"Expected tuple, got {type(item)}"
            assert len(item) == 3, f"Expected 3-tuple (text, start, end), got {len(item)}-tuple"

    def test_tuple_types(self):
        content = "x = 1\n" * 30
        for text, start, end in chunk_file("test.py", content):
            assert isinstance(text, str)
            assert isinstance(start, int)
            assert isinstance(end, int)

    def test_empty_file_returns_empty_list(self):
        assert chunk_file("test.py", "") == []

    def test_tiny_content_filtered_by_min_chunk_size(self):
        # Content with far less than MIN_CHUNK_SIZE characters
        content = "x = 1\n"
        assert len(content.strip()) < MIN_CHUNK_SIZE
        result = chunk_file("test.py", content)
        assert result == []


# ── Line numbers are 1-indexed ───────────────────────────────────────────────


class TestLineNumbersOneIndexed:
    def test_start_line_minimum_is_1(self):
        content = "# This is a comment\n" * 30
        for _, start, end in chunk_file("test.py", content):
            assert start >= 1, f"start_line {start} should be ≥ 1"

    def test_end_line_minimum_is_1(self):
        content = "x = 1\n" * 30
        for _, start, end in chunk_file("test.py", content):
            assert end >= 1, f"end_line {end} should be ≥ 1"

    def test_end_line_gte_start_line(self):
        content = "x = 1\n" * 30
        for _, start, end in chunk_file("test.py", content):
            assert end >= start, f"end_line {end} should be ≥ start_line {start}"


# ── Plain text chunking line numbers ────────────────────────────────────────


class TestTextChunkingLineNumbers:
    def test_single_chunk_spans_file(self):
        content = "line one\nline two\nline three\n" * 5
        # small max_chars to ensure at least one chunk, but content is short enough
        chunks = _chunk_text(content, max_chars=10000)
        # All content in one chunk → covers line 1 to line count
        assert len(chunks) >= 1
        _, start, end = chunks[0]
        assert start == 1

    def test_first_chunk_starts_at_line_1(self):
        content = "\n".join(f"line {i}" for i in range(1, 101))
        chunks = _chunk_text(content, max_chars=200)
        assert chunks[0][1] == 1

    def test_chunks_cover_all_lines(self):
        """All lines in the source should be covered by some chunk's range."""
        content = "\n".join(
            f"this is line number {i} with some padding content" for i in range(1, 51)
        )
        total_lines = len(_lines(content))
        chunks = _chunk_text(content, max_chars=300)
        assert chunks, "Expected at least one chunk"
        # Last chunk's end_line should reach or exceed the total line count
        last_end = max(end for _, _, end in chunks)
        assert last_end >= total_lines

    def test_sequential_chunks_line_numbers_are_ordered(self):
        """Line numbers of successive chunks should be non-decreasing."""
        content = "\n".join(f"this is test line {i} with filler text" for i in range(1, 80))
        chunks = _chunk_text(content, max_chars=200)
        for i in range(1, len(chunks)):
            prev_end = chunks[i - 1][2]
            curr_start = chunks[i][1]
            assert curr_start >= prev_end, (
                f"Chunk {i} start_line {curr_start} < prev end_line {prev_end}"
            )

    def test_base_line_offset(self):
        """base_line parameter should shift all line numbers accordingly."""
        content = "aaa bbb ccc\nddd eee fff\nggg hhh iii\n" * 5
        chunks_base1 = _chunk_text(content, max_chars=200, base_line=1)
        chunks_base10 = _chunk_text(content, max_chars=200, base_line=10)

        assert len(chunks_base1) == len(chunks_base10), "Same content should yield same chunk count"

        for (_, s1, e1), (_, s10, e10) in zip(chunks_base1, chunks_base10):
            assert s10 == s1 + 9, f"start_line offset wrong: {s10} vs {s1}+9"
            assert e10 == e1 + 9, f"end_line offset wrong: {e10} vs {e1}+9"


# ── TreeSitter code chunking line numbers ────────────────────────────────────


class TestCodeChunkingLineNumbers:
    _PYTHON_SAMPLE = textwrap.dedent("""\
        def alpha():
            x = 1
            return x


        def beta():
            y = 2
            return y


        def gamma():
            z = 3
            return z


        class MyClass:
            def method_one(self):
                pass

            def method_two(self):
                pass
    """)

    def test_python_chunks_have_line_numbers(self):
        chunks = chunk_file("sample.py", self._PYTHON_SAMPLE)
        assert chunks, "Expected at least one chunk for Python sample"
        for text, start, end in chunks:
            assert start >= 1
            assert end >= start

    def test_python_first_chunk_starts_at_line_1_or_2(self):
        """First function starts on line 1; first chunk should begin there."""
        chunks = chunk_file("sample.py", self._PYTHON_SAMPLE)
        assert chunks
        assert chunks[0][1] <= 2  # TreeSitter may include leading blank

    def test_python_chunks_line_numbers_are_monotone(self):
        """Each chunk's start_line must not precede the previous chunk's start_line.

        Gaps between chunks are expected (blank lines between functions are not
        included in any AST node). Overlaps are not expected.
        """
        chunks = chunk_file("sample.py", self._PYTHON_SAMPLE, max_chars=80)
        for i in range(1, len(chunks)):
            prev_start = chunks[i - 1][1]
            curr_start = chunks[i][1]
            assert curr_start >= prev_start, (
                f"Chunk {i} start_line {curr_start} went backward from {prev_start}"
            )

    def test_python_last_chunk_end_line_covers_file(self):
        """Last chunk should end near the last line of the file."""
        total_lines = len(self._PYTHON_SAMPLE.splitlines())
        chunks = chunk_file("sample.py", self._PYTHON_SAMPLE)
        assert chunks
        last_end = max(end for _, _, end in chunks)
        assert last_end >= total_lines - 2  # allow small discrepancy at EOF

    def test_javascript_chunks_have_line_numbers(self):
        js_code = textwrap.dedent("""\
            function foo() {
                return 1;
            }

            function bar() {
                return 2;
            }

            function baz() {
                return 3;
            }
        """)
        chunks = chunk_file("app.js", js_code)
        assert chunks
        for _, start, end in chunks:
            assert start >= 1
            assert end >= start


# ── MIN_CHUNK_SIZE filter with tuples ───────────────────────────────────────


class TestMinChunkSizeWithTuples:
    def test_small_chunks_are_filtered(self):
        """Chunks below MIN_CHUNK_SIZE should be excluded from result."""
        content = "x = 1\n" * 3 + ("# " + "a" * 60 + "\n") * 20
        result = chunk_file("test.py", content)
        for text, _, _ in result:
            assert len(text.strip()) >= MIN_CHUNK_SIZE, (
                f"Chunk shorter than MIN_CHUNK_SIZE slipped through: {repr(text)}"
            )

    def test_merge_small_chunks_preserves_line_range(self):
        """_merge_small_chunks should use first start_line and last end_line."""
        chunks = [
            ("short", 1, 1),  # too small alone
            ("also short", 2, 2),  # too small alone
            ("x" * 60, 3, 5),  # large enough
        ]
        merged = _merge_small_chunks(chunks, max_chars=500)
        assert len(merged) == 1
        _, start, end = merged[0]
        assert start == 1
        assert end == 5

    def test_merge_preserves_independent_large_chunks(self):
        """Large chunks should not be merged across the limit."""
        text_a = "a" * 100
        text_b = "b" * 100
        chunks = [(text_a, 1, 5), (text_b, 6, 10)]
        merged = _merge_small_chunks(chunks, max_chars=150)  # 100+100+1 > 150
        assert len(merged) == 2
        assert merged[0] == (text_a, 1, 5)
        assert merged[1] == (text_b, 6, 10)


# ── Non-code files fall back to text chunking ────────────────────────────────


class TestNonCodeFileFallback:
    def test_markdown_returns_tuples(self):
        md = "# Title\n\nParagraph one with enough words.\n\nParagraph two with enough words.\n" * 5
        result = chunk_file("README.md", md)
        for item in result:
            assert len(item) == 3

    def test_yaml_returns_tuples(self):
        yaml_content = "key: value\nanother: entry\n" * 30
        result = chunk_file("config.yml", yaml_content)
        for item in result:
            assert len(item) == 3

    def test_unknown_extension_returns_tuples(self):
        content = "Some random text content.\n" * 20
        result = chunk_file("data.xyz", content)
        for item in result:
            assert len(item) == 3

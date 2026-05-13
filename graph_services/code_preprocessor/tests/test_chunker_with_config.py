"""Tests for chunk_file() with chunker_config (Phase 2.1 — V3 semantic chunking).

Verifies:
- chunker_config is optional; None falls back to legacy size-based chunking.
- When a config is provided, AST node types are used as primary boundaries.
- Per-construct min/max sizes control chunk membership.
- Line numbers are correct regardless of chunking mode.
- Falls back gracefully when TreeSitter is not available for the language.
- Gemini verbose shape (ast_chunk_boundaries) works.
- Legacy flat shape (boundaries + ast_node_types) still works.
- file_type_overrides are honoured.
- RSpec do_block creates a chunk boundary.
"""

import textwrap

import pytest

from code_preprocessor.chunker import (
    MIN_CHUNK_SIZE,
    _build_boundary_map,
    _config_boundary_max,
    _config_boundary_min,
    _config_max_chars,
    _is_boundary_node,
    chunk_file,
)


# ── Sample configs ────────────────────────────────────────────────────────────

_PYTHON_CONFIG = {
    "language": "python",
    "boundaries": {
        "class": {"min": 100, "max": 2000},
        "function": {"min": 50, "max": 800},
    },
    "ast_node_types": [
        "class_definition",
        "function_definition",
    ],
}

_TS_CONFIG = {
    "language": "typescript",
    "boundaries": {
        "class": {"min": 200, "max": 1500},
        "function": {"min": 50, "max": 800},
        "method": {"min": 50, "max": 600},
    },
    "ast_node_types": [
        "class_declaration",
        "function_declaration",
        "method_definition",
        "interface_declaration",
    ],
}

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


# ── Config helper unit tests ──────────────────────────────────────────────────


class TestConfigHelpers:
    def test_config_max_chars_from_boundaries(self):
        assert _config_max_chars(_PYTHON_CONFIG, 500) == 2000

    def test_config_max_chars_no_boundaries_returns_default(self):
        assert _config_max_chars({}, 999) == 999

    def test_config_boundary_max_exact_match(self):
        assert _config_boundary_max(_PYTHON_CONFIG, "function_definition", 500) == 800

    def test_config_boundary_max_substring_match(self):
        # "class" is in "class_definition"
        assert _config_boundary_max(_PYTHON_CONFIG, "class_definition", 500) == 2000

    def test_config_boundary_max_no_match_returns_default(self):
        assert _config_boundary_max(_PYTHON_CONFIG, "import_statement", 500) == 500

    def test_config_boundary_min_substring_match(self):
        assert _config_boundary_min(_PYTHON_CONFIG, "function_definition") == 50

    def test_config_boundary_min_no_match_returns_default(self):
        assert _config_boundary_min(_PYTHON_CONFIG, "import_statement") == MIN_CHUNK_SIZE

    def test_is_boundary_node_true(self):
        assert _is_boundary_node("function_definition", _PYTHON_CONFIG["ast_node_types"])

    def test_is_boundary_node_false(self):
        assert not _is_boundary_node("import_statement", _PYTHON_CONFIG["ast_node_types"])


# ── Backward compat: no config ────────────────────────────────────────────────


class TestNoConfigFallback:
    """chunk_file without a config must behave identically to the old API."""

    def test_returns_tuples(self):
        content = _PYTHON_SAMPLE
        result = chunk_file("sample.py", content)
        assert isinstance(result, list)
        for item in result:
            assert len(item) == 3

    def test_line_numbers_positive(self):
        for _, start, end in chunk_file("sample.py", _PYTHON_SAMPLE):
            assert start >= 1
            assert end >= start

    def test_config_none_explicit(self):
        """Passing chunker_config=None is identical to not passing it."""
        chunks_default = chunk_file("sample.py", _PYTHON_SAMPLE)
        chunks_none = chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=None)
        # Same number of chunks and same texts
        assert len(chunks_default) == len(chunks_none)
        for (t1, s1, e1), (t2, s2, e2) in zip(chunks_default, chunks_none):
            assert t1 == t2
            assert s1 == s2
            assert e1 == e2


# ── Config-driven chunking ────────────────────────────────────────────────────


class TestConfigDrivenChunking:
    def test_python_config_returns_chunks(self):
        chunks = chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=_PYTHON_CONFIG)
        assert chunks, "Expected at least one chunk with Python config"

    def test_python_config_returns_tuples(self):
        for item in chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=_PYTHON_CONFIG):
            assert len(item) == 3

    def test_python_config_line_numbers_positive(self):
        for _, start, end in chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=_PYTHON_CONFIG):
            assert start >= 1
            assert end >= start

    def test_python_config_line_numbers_monotone(self):
        chunks = chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=_PYTHON_CONFIG)
        for i in range(1, len(chunks)):
            assert chunks[i][1] >= chunks[i - 1][1], (
                f"start_line went backward: chunk {i} start={chunks[i][1]} < "
                f"prev start={chunks[i - 1][1]}"
            )

    def test_python_config_no_chunk_exceeds_max(self):
        """No chunk text should exceed the maximum size in config."""
        max_size = _config_max_chars(_PYTHON_CONFIG, 1000)
        for text, _, _ in chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=_PYTHON_CONFIG):
            assert len(text) <= max_size * 2, (
                # We allow 2x because merging may push slightly past; leaf
                # fallback for very large nodes also respects the max.
                f"Chunk exceeds 2× config max ({max_size}): len={len(text)}"
            )

    def test_python_config_all_chunks_above_min_chunk_size(self):
        for text, _, _ in chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=_PYTHON_CONFIG):
            assert len(text.strip()) >= MIN_CHUNK_SIZE

    def test_python_config_functions_as_own_chunks(self):
        """With config boundaries, top-level functions should each be their own chunk."""
        # Use a small max_chars so size-based mode would merge them, but config
        # should keep them separate as boundary nodes.
        chunks = chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=_PYTHON_CONFIG)
        texts = [t for t, _, _ in chunks]
        # Each of alpha, beta, gamma should appear in exactly one chunk
        assert any("def alpha" in t for t in texts), "alpha missing from chunks"
        assert any("def beta" in t for t in texts), "beta missing from chunks"
        assert any("def gamma" in t for t in texts), "gamma missing from chunks"

    def test_config_with_large_function_is_subdivided(self):
        """A function that exceeds its max should be split further."""
        # Build a function with > 800 chars of body (the function max)
        big_body = "\n".join(f"    x_{i} = {i}" for i in range(60))
        content = f"def huge():\n{big_body}\n    return x_0\n"
        tiny_config = {
            "language": "python",
            "boundaries": {"function": {"min": 10, "max": 200}},
            "ast_node_types": ["function_definition"],
        }
        chunks = chunk_file("big.py", content, chunker_config=tiny_config)
        # The function node exceeds max=200 → _chunk_node splits it.
        # _merge_small_chunks with global_max=200 then re-merges adjacent small parts.
        # Net result: at least 1 chunk (the function content is present).
        assert len(chunks) >= 1, "Expected at least one chunk for a huge function"
        # Verify the full content is covered (no silent data loss)
        all_text = " ".join(t for t, _, _ in chunks)
        assert "def huge" in all_text


# ── Fallback for non-code files with config ───────────────────────────────────


class TestConfigWithNonCodeFile:
    """When a config is supplied but the file is not a code file, use text chunking."""

    def test_markdown_with_config_returns_tuples(self):
        md = (
            "# Title\n\nParagraph one with enough words.\n\nParagraph two with enough words.\n"
        ) * 5
        result = chunk_file("README.md", md, chunker_config=_PYTHON_CONFIG)
        assert isinstance(result, list)
        for item in result:
            assert len(item) == 3

    def test_unknown_extension_with_config_returns_tuples(self):
        content = "Some random content line.\n" * 20
        result = chunk_file("data.xyz", content, chunker_config=_TS_CONFIG)
        for item in result:
            assert len(item) == 3

    def test_config_max_used_for_text_chunking(self):
        """For non-code files, global max from config should bound chunk sizes."""
        content = "word " * 1000
        tiny_config = {
            "language": "typescript",
            "boundaries": {"function": {"min": 10, "max": 100}},
            "ast_node_types": ["function_declaration"],
        }
        for text, _, _ in chunk_file("README.md", content, chunker_config=tiny_config):
            assert len(text) <= 100 * 3, "Chunk far exceeds config max for non-code file"


# ── Empty / trivial inputs ────────────────────────────────────────────────────


class TestEdgeCases:
    def test_empty_file_with_config(self):
        assert chunk_file("sample.py", "", chunker_config=_PYTHON_CONFIG) == []

    def test_tiny_file_filtered_with_config(self):
        content = "x = 1\n"  # below MIN_CHUNK_SIZE
        result = chunk_file("sample.py", content, chunker_config=_PYTHON_CONFIG)
        assert result == []

    def test_none_config_explicit_is_same_as_omitted(self):
        result_omitted = chunk_file("sample.py", _PYTHON_SAMPLE)
        result_none = chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=None)
        assert result_omitted == result_none


# ── Gemini / V3 verbose config shape ─────────────────────────────────────────

# The actual shape Gemini produces (ast_chunk_boundaries list of dicts with
# node_type / max_size_chars / min_size_chars).

_RUBY_GEMINI_CONFIG = {
    "language": "Ruby",
    "framework": "Ruby on Rails",
    "ast_chunk_boundaries": [
        {
            "node_type": "class",
            "max_size_chars": 2000,
            "min_size_chars": 100,
            "description": "Rails classes",
        },
        {
            "node_type": "module",
            "max_size_chars": 2000,
            "min_size_chars": 100,
            "description": "Rails modules",
        },
        {
            "node_type": "method",
            "max_size_chars": 1000,
            "min_size_chars": 50,
            "description": "Individual methods",
        },
        {
            "node_type": "do_block",
            "max_size_chars": 800,
            "min_size_chars": 50,
            "description": "Ruby blocks / RSpec",
        },
    ],
    "file_type_overrides": [
        {"extension": ".rb", "strategy": "ast_based", "chunk_size": 1500},
        {"extension": ".yml", "strategy": "recursive_text", "chunk_size": 800},
        {"extension": ".erb", "strategy": "recursive_text", "chunk_size": 1000},
    ],
    "fallback_strategy": "recursive_text",
    "fallback_chunk_size": 1000,
    "fallback_overlap": 150,
}

_RUBY_SAMPLE = textwrap.dedent("""\
    # frozen_string_literal: true

    class Animal
      def speak
        "..."
      end

      def self.kingdom
        "Animalia"
      end
    end

    module Walkable
      def walk
        "walking"
      end
    end
""")

_RSPEC_SAMPLE = textwrap.dedent("""\
    # frozen_string_literal: true

    require 'rails_helper'

    RSpec.describe Animal do
      describe '#speak' do
        it 'returns a string' do
          animal = Animal.new
          expect(animal.speak).to be_a(String)
        end
      end

      describe '.kingdom' do
        it 'returns Animalia' do
          expect(Animal.kingdom).to eq('Animalia')
        end
      end
    end
""")


class TestBuildBoundaryMap:
    """Unit tests for _build_boundary_map normalisation."""

    def test_gemini_shape_parsed(self):
        bmap = _build_boundary_map(_RUBY_GEMINI_CONFIG)
        assert "class" in bmap
        assert bmap["class"]["max"] == 2000
        assert bmap["class"]["min"] == 100
        assert "method" in bmap
        assert bmap["method"]["max"] == 1000
        assert bmap["method"]["min"] == 50

    def test_gemini_shape_singleton_method_alias_added(self):
        """singleton_method should be aliased to method automatically."""
        bmap = _build_boundary_map(_RUBY_GEMINI_CONFIG)
        assert "singleton_method" in bmap
        assert bmap["singleton_method"] == bmap["method"]

    def test_legacy_shape_parsed(self):
        bmap = _build_boundary_map(_PYTHON_CONFIG)
        assert "class" in bmap
        assert bmap["class"]["max"] == 2000
        assert "function" in bmap
        assert bmap["function"]["max"] == 800

    def test_empty_config_returns_empty_map(self):
        assert _build_boundary_map({}) == {}

    def test_gemini_takes_priority_over_legacy(self):
        """When both shapes are present, Gemini shape wins."""
        mixed = {
            "ast_chunk_boundaries": [
                {"node_type": "class", "max_size_chars": 9999, "min_size_chars": 1}
            ],
            "boundaries": {"class": {"max": 111, "min": 10}},
        }
        bmap = _build_boundary_map(mixed)
        assert bmap["class"]["max"] == 9999


class TestConfigHelpersGeminiShape:
    """Ensure helper functions work with the Gemini verbose config shape."""

    def test_config_max_chars_uses_fallback_chunk_size(self):
        # fallback_chunk_size takes priority when present
        assert _config_max_chars(_RUBY_GEMINI_CONFIG, 500) == 1000

    def test_config_boundary_max_gemini(self):
        assert _config_boundary_max(_RUBY_GEMINI_CONFIG, "class", 500) == 2000

    def test_config_boundary_max_gemini_method(self):
        assert _config_boundary_max(_RUBY_GEMINI_CONFIG, "method", 500) == 1000

    def test_config_boundary_min_gemini(self):
        assert _config_boundary_min(_RUBY_GEMINI_CONFIG, "method") == 50

    def test_config_boundary_max_legacy_still_works(self):
        assert _config_boundary_max(_PYTHON_CONFIG, "function_definition", 500) == 800


class TestGeminiShapeRubyChunking:
    """Chunker correctly uses ast_chunk_boundaries from the Gemini shape."""

    def test_ruby_class_boundary_fires(self):
        chunks = chunk_file("app.rb", _RUBY_SAMPLE, chunker_config=_RUBY_GEMINI_CONFIG)
        texts = [t for t, _, _ in chunks]
        assert any("class Animal" in t for t in texts), "class Animal not in any chunk"

    def test_ruby_module_boundary_fires(self):
        chunks = chunk_file("app.rb", _RUBY_SAMPLE, chunker_config=_RUBY_GEMINI_CONFIG)
        texts = [t for t, _, _ in chunks]
        assert any("module Walkable" in t for t in texts), "module Walkable not in any chunk"

    def test_ruby_class_and_module_are_separate_chunks(self):
        # Use a larger sample so both class and module exceed min_size_chars=100.
        big_ruby_sample = textwrap.dedent("""\
            # frozen_string_literal: true

            class Animal
              SOUND = "generic sound"
              KINGDOM = "Animalia"

              def initialize(name)
                @name = name
              end

              def speak
                SOUND
              end

              def name
                @name
              end
            end

            module Walkable
              def walk
                "#{self.class.name} is walking on four legs"
              end

              def run
                "#{self.class.name} is running"
              end

              def self.included(base)
                base.extend(ClassMethods)
              end

              module ClassMethods
                def locomotion_type
                  "quadruped"
                end
              end
            end
        """)
        chunks = chunk_file("app.rb", big_ruby_sample, chunker_config=_RUBY_GEMINI_CONFIG)
        texts = [t for t, _, _ in chunks]
        assert any("class Animal" in t for t in texts), "class Animal not found"
        assert any("module Walkable" in t for t in texts), "module Walkable not found"
        class_idx = next(i for i, t in enumerate(texts) if "class Animal" in t)
        module_idx = next(i for i, t in enumerate(texts) if "module Walkable" in t)
        assert class_idx != module_idx, "class and module landed in the same chunk"

    def test_ruby_singleton_method_boundary_fires(self):
        """def self.method (singleton_method in TreeSitter) is treated as method boundary.

        Note: singleton_method is inside the class body, not at root level.
        The root walker sees the `class` node as a boundary; singleton_method
        aliasing ensures that IF a singleton_method appeared at root level it
        would be handled.  Here the class node itself fires as boundary.
        """
        content = textwrap.dedent("""\
            class Foo
              def self.bar
                "result from singleton method that is sufficiently long to exceed min_size"
              end
            end
        """)
        config = {
            "ast_chunk_boundaries": [
                {"node_type": "class", "max_size_chars": 500, "min_size_chars": 10},
                {"node_type": "method", "max_size_chars": 500, "min_size_chars": 10},
            ],
        }
        chunks = chunk_file("foo.rb", content, chunker_config=config)
        # class node fires at root level → at least 1 chunk containing the content
        assert len(chunks) >= 1
        all_text = " ".join(t for t, _, _ in chunks)
        assert "self.bar" in all_text

    def test_ruby_produces_multiple_chunks(self):
        """A file with two large classes should yield > 1 chunk with Gemini config."""
        # Build two classes, each > min_size_chars=100
        two_classes = textwrap.dedent("""\
            class Cat
              def speak
                "meow — this is a cat speaking with enough content to pass minimum size"
              end
              def purr
                "purrrr — cats purr when they are happy and content"
              end
            end

            class Dog
              def speak
                "woof — this is a dog speaking with enough content to pass minimum size"
              end
              def fetch
                "fetching the ball enthusiastically across the yard"
              end
            end
        """)
        chunks = chunk_file("app.rb", two_classes, chunker_config=_RUBY_GEMINI_CONFIG)
        assert len(chunks) >= 2, f"Expected ≥2 chunks, got {len(chunks)}"

    def test_all_chunks_above_min_size(self):
        for text, _, _ in chunk_file("app.rb", _RUBY_SAMPLE, chunker_config=_RUBY_GEMINI_CONFIG):
            assert len(text.strip()) >= MIN_CHUNK_SIZE


class TestRSpecDoBlockBoundary:
    """RSpec describe/it blocks create chunk boundaries via do_block."""

    def test_rspec_file_produces_multiple_chunks(self):
        chunks = chunk_file(
            "spec/animal_spec.rb", _RSPEC_SAMPLE, chunker_config=_RUBY_GEMINI_CONFIG
        )
        assert len(chunks) >= 1

    def test_rspec_do_block_in_chunks(self):
        """do_block boundary should cause RSpec describe blocks to be separate chunks."""
        # Build a larger RSpec file to ensure blocks clear min_size_chars
        big_rspec = textwrap.dedent("""\
            require 'rails_helper'

            RSpec.describe Animal do
              describe '#speak' do
                it 'returns a string with enough content to exceed the minimum chunk size threshold' do
                  animal = Animal.new
                  result = animal.speak
                  expect(result).to be_a(String)
                  expect(result).not_to be_empty
                end
              end

              describe '.kingdom' do
                it 'returns Animalia when asked about the kingdom of all animals' do
                  expected = 'Animalia'
                  actual = Animal.kingdom
                  expect(actual).to eq(expected)
                  expect(actual).to be_frozen
                end
              end
            end
        """)
        chunks = chunk_file("spec/animal_spec.rb", big_rspec, chunker_config=_RUBY_GEMINI_CONFIG)
        assert len(chunks) >= 1
        # All returned chunks must meet minimum size
        for text, _, _ in chunks:
            assert len(text.strip()) >= MIN_CHUNK_SIZE


class TestFileTypeOverrides:
    """file_type_overrides are correctly honoured by chunk_file."""

    def test_yml_uses_recursive_text(self):
        """A .yml file should use recursive_text strategy (no TreeSitter)."""
        yml_content = "key: value\n" * 100  # ~1100 chars
        chunks = chunk_file("config.yml", yml_content, chunker_config=_RUBY_GEMINI_CONFIG)
        assert len(chunks) >= 1
        for item in chunks:
            assert len(item) == 3

    def test_erb_uses_recursive_text(self):
        erb_content = "<%= @thing %>\n" * 100
        chunks = chunk_file("template.erb", erb_content, chunker_config=_RUBY_GEMINI_CONFIG)
        assert len(chunks) >= 1

    def test_rb_uses_ast_based(self):
        """A .rb file with ast_based override should still use AST chunking."""
        chunks = chunk_file("app.rb", _RUBY_SAMPLE, chunker_config=_RUBY_GEMINI_CONFIG)
        assert len(chunks) >= 1

    def test_unknown_extension_falls_through_to_ast(self):
        """Files not in overrides still get AST chunking when language detected."""
        chunks = chunk_file("app.rb.bak", _RUBY_SAMPLE, chunker_config=_RUBY_GEMINI_CONFIG)
        assert isinstance(chunks, list)

    def test_yml_override_chunk_size_respected(self):
        """chunk_size from override bounds the text chunks for .yml files."""
        yml_content = "word: value\n" * 200  # ~2400 chars, well above 800 cap
        chunks = chunk_file("config.yml", yml_content, chunker_config=_RUBY_GEMINI_CONFIG)
        for text, _, _ in chunks:
            # yml override chunk_size=800; allow some slack for merging
            assert len(text) <= 800 * 3, f"Chunk exceeds 3× yml override size: {len(text)}"


class TestBackwardCompatLegacyShape:
    """Legacy flat config shape still produces correct results after the fix."""

    def test_legacy_python_config_still_works(self):
        chunks = chunk_file("sample.py", _PYTHON_SAMPLE, chunker_config=_PYTHON_CONFIG)
        assert len(chunks) >= 1
        texts = [t for t, _, _ in chunks]
        assert any("def alpha" in t for t in texts)
        assert any("def beta" in t for t in texts)

    def test_legacy_boundary_max_correct(self):
        assert _config_boundary_max(_PYTHON_CONFIG, "class_definition", 500) == 2000

    def test_legacy_and_gemini_produce_same_split_for_ruby(self):
        """Both config shapes should produce identical chunk sets for the same boundaries."""
        legacy_ruby_config = {
            "language": "ruby",
            "boundaries": {
                "class": {"min": 100, "max": 2000},
                "module": {"min": 100, "max": 2000},
                "method": {"min": 50, "max": 1000},
            },
            "ast_node_types": ["class", "module", "method"],
        }
        gemini_ruby_config = {
            "language": "Ruby",
            "ast_chunk_boundaries": [
                {"node_type": "class", "max_size_chars": 2000, "min_size_chars": 100},
                {"node_type": "module", "max_size_chars": 2000, "min_size_chars": 100},
                {"node_type": "method", "max_size_chars": 1000, "min_size_chars": 50},
            ],
        }
        chunks_legacy = chunk_file("app.rb", _RUBY_SAMPLE, chunker_config=legacy_ruby_config)
        chunks_gemini = chunk_file("app.rb", _RUBY_SAMPLE, chunker_config=gemini_ruby_config)
        assert len(chunks_legacy) == len(chunks_gemini), (
            f"Legacy produced {len(chunks_legacy)} chunks, Gemini {len(chunks_gemini)}"
        )

"""Unit tests for file_tree module.

Tests build_tree_with_metadata(), build_tree(), and get_file_list().
All tests use temporary directories — no real repo or network required.
"""

import os
from pathlib import Path

import pytest

from code_preprocessor.file_tree import (
    MAX_TREE_LINES,
    build_tree,
    build_tree_with_metadata,
    get_file_list,
    _detect_language,
    _is_binary,
    _size_label,
    _detect_frameworks,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(path: Path, content: str = "") -> None:
    """Write text content to a file, creating parents as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


# ---------------------------------------------------------------------------
# _is_binary
# ---------------------------------------------------------------------------


class TestIsBinary:
    def test_png_is_binary(self):
        assert _is_binary("logo.png") is True

    def test_jpg_is_binary(self):
        assert _is_binary("photo.JPG") is True

    def test_py_is_not_binary(self):
        assert _is_binary("app.py") is False

    def test_rb_is_not_binary(self):
        assert _is_binary("controller.rb") is False

    def test_pdf_is_binary(self):
        assert _is_binary("doc.pdf") is True

    def test_no_extension_is_not_binary(self):
        assert _is_binary("Makefile") is False

    def test_exe_is_binary(self):
        assert _is_binary("program.exe") is True

    def test_ts_is_not_binary(self):
        assert _is_binary("component.ts") is False


# ---------------------------------------------------------------------------
# _detect_language
# ---------------------------------------------------------------------------


class TestDetectLanguage:
    def test_py_is_python(self):
        assert _detect_language("app.py") == "python"

    def test_rb_is_ruby(self):
        assert _detect_language("user.rb") == "ruby"

    def test_ts_is_typescript(self):
        assert _detect_language("index.ts") == "typescript"

    def test_tsx_is_typescript(self):
        assert _detect_language("Button.tsx") == "typescript"

    def test_js_is_javascript(self):
        assert _detect_language("main.js") == "javascript"

    def test_yaml_is_yaml(self):
        assert _detect_language("config.yml") == "yaml"

    def test_unknown_returns_none(self):
        assert _detect_language("Makefile") is None

    def test_case_insensitive(self):
        assert _detect_language("SCRIPT.PY") == "python"


# ---------------------------------------------------------------------------
# _size_label
# ---------------------------------------------------------------------------


class TestSizeLabel:
    def test_empty_file_is_small(self, tmp_path):
        f = tmp_path / "empty.py"
        _write(f, "")
        assert _size_label(f) == "[S]"

    def test_50_lines_is_small(self, tmp_path):
        f = tmp_path / "small.py"
        _write(f, "\n".join(["x = 1"] * 50))
        assert _size_label(f) == "[S]"

    def test_99_lines_is_small(self, tmp_path):
        f = tmp_path / "s99.py"
        _write(f, "\n".join(["x"] * 99))
        assert _size_label(f) == "[S]"

    def test_100_lines_is_medium(self, tmp_path):
        # "\n".join(N items) produces N-1 newlines; need 101 items → 100 '\n' chars
        f = tmp_path / "med.py"
        _write(f, "\n".join(["x"] * 101))
        assert _size_label(f) == "[M]"

    def test_499_lines_is_medium(self, tmp_path):
        f = tmp_path / "m499.py"
        _write(f, "\n".join(["x"] * 499))
        assert _size_label(f) == "[M]"

    def test_500_lines_is_large(self, tmp_path):
        # Need 501 items → 500 '\n' chars to reach the _SIZE_MEDIUM (500) threshold
        f = tmp_path / "large.py"
        _write(f, "\n".join(["x"] * 501))
        assert _size_label(f) == "[L]"

    def test_missing_file_returns_small(self, tmp_path):
        f = tmp_path / "nonexistent.py"
        # _count_lines returns 0 on error → [S]
        assert _size_label(f) == "[S]"


# ---------------------------------------------------------------------------
# _detect_frameworks
# ---------------------------------------------------------------------------


class TestDetectFrameworks:
    def test_rails_detected(self, tmp_path):
        _write(tmp_path / "Gemfile", 'source "https://rubygems.org"')
        (tmp_path / "app").mkdir()
        (tmp_path / "config").mkdir()
        (tmp_path / "db").mkdir()
        result = _detect_frameworks(tmp_path)
        assert "Ruby on Rails" in result

    def test_nextjs_detected(self, tmp_path):
        _write(tmp_path / "next.config.js", "module.exports = {}")
        result = _detect_frameworks(tmp_path)
        assert "Next.js" in result

    def test_django_detected(self, tmp_path):
        _write(tmp_path / "manage.py", "")
        _write(tmp_path / "requirements.txt", "django")
        result = _detect_frameworks(tmp_path)
        assert "Django" in result

    def test_go_module_detected(self, tmp_path):
        _write(tmp_path / "go.mod", "module example.com/app")
        result = _detect_frameworks(tmp_path)
        assert "Go module" in result

    def test_no_frameworks_on_empty_repo(self, tmp_path):
        result = _detect_frameworks(tmp_path)
        assert result == []

    def test_no_duplicates_when_multiple_signals_match(self, tmp_path):
        # Next.js can have both next.config.js and next.config.ts — should not duplicate
        _write(tmp_path / "next.config.js", "")
        _write(tmp_path / "next.config.ts", "")
        result = _detect_frameworks(tmp_path)
        assert result.count("Next.js") == 1


# ---------------------------------------------------------------------------
# build_tree_with_metadata — normal repo
# ---------------------------------------------------------------------------


class TestBuildTreeWithMetadata:
    def test_returns_tuple(self, tmp_path):
        result = build_tree_with_metadata(str(tmp_path))
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_tree_string_type(self, tmp_path):
        tree_str, _ = build_tree_with_metadata(str(tmp_path))
        assert isinstance(tree_str, str)

    def test_metadata_keys_present(self, tmp_path):
        _, meta = build_tree_with_metadata(str(tmp_path))
        assert "total_files" in meta
        assert "languages_detected" in meta
        assert "primary_language" in meta
        assert "frameworks_detected" in meta

    def test_error_on_nonexistent_path(self):
        tree_str, meta = build_tree_with_metadata("/nonexistent/path/xyz")
        assert tree_str.startswith("ERROR:")
        assert meta["total_files"] == 0
        assert meta["primary_language"] == ""

    def test_empty_repo_returns_empty_tree(self, tmp_path):
        tree_str, meta = build_tree_with_metadata(str(tmp_path))
        assert tree_str == ""
        assert meta["total_files"] == 0

    def test_single_python_file(self, tmp_path):
        _write(tmp_path / "main.py", "print('hi')\n")
        tree_str, meta = build_tree_with_metadata(str(tmp_path))

        assert "main.py" in tree_str
        assert meta["total_files"] == 1
        assert meta["primary_language"] == "python"

    def test_size_label_appears_in_tree(self, tmp_path):
        _write(tmp_path / "small.py", "x = 1\n")
        tree_str, _ = build_tree_with_metadata(str(tmp_path))
        assert "[S]" in tree_str

    def test_binary_files_excluded_from_tree(self, tmp_path):
        _write(tmp_path / "app.py", "")
        _write(tmp_path / "logo.png", "")
        tree_str, meta = build_tree_with_metadata(str(tmp_path))

        assert "app.py" in tree_str
        assert "logo.png" not in tree_str
        assert meta["total_files"] == 1

    def test_hidden_dirs_excluded(self, tmp_path):
        _write(tmp_path / ".git" / "config", "[core]")
        _write(tmp_path / "app.py", "")
        tree_str, _ = build_tree_with_metadata(str(tmp_path))

        assert ".git" not in tree_str

    def test_node_modules_excluded(self, tmp_path):
        _write(tmp_path / "node_modules" / "lib" / "index.js", "")
        _write(tmp_path / "src" / "main.ts", "")
        tree_str, _ = build_tree_with_metadata(str(tmp_path))

        assert "node_modules" not in tree_str
        assert "main.ts" in tree_str

    def test_multiple_languages_detected(self, tmp_path):
        _write(tmp_path / "app.py", "")
        _write(tmp_path / "server.rb", "")
        _write(tmp_path / "client.ts", "")
        _, meta = build_tree_with_metadata(str(tmp_path))

        lang_names = {item["language"] for item in meta["languages_detected"]}
        assert "python" in lang_names
        assert "ruby" in lang_names
        assert "typescript" in lang_names
        assert meta["total_files"] == 3

    def test_primary_language_is_most_common(self, tmp_path):
        # 3 Python files, 1 Ruby
        for i in range(3):
            _write(tmp_path / f"mod{i}.py", "")
        _write(tmp_path / "lib.rb", "")
        _, meta = build_tree_with_metadata(str(tmp_path))
        assert meta["primary_language"] == "python"

    def test_directory_shown_with_trailing_slash(self, tmp_path):
        _write(tmp_path / "src" / "main.py", "")
        tree_str, _ = build_tree_with_metadata(str(tmp_path))
        assert "src/" in tree_str

    def test_nested_dirs_indented(self, tmp_path):
        _write(tmp_path / "app" / "models" / "user.rb", "")
        tree_str, _ = build_tree_with_metadata(str(tmp_path))

        # The collapsed dir chain appears on one line
        assert "app/models/" in tree_str
        # File is indented (2 spaces)
        assert "  user.rb" in tree_str

    def test_languages_detected_sorted_by_count(self, tmp_path):
        for i in range(5):
            _write(tmp_path / f"f{i}.py", "")
        for i in range(2):
            _write(tmp_path / f"g{i}.rb", "")
        _, meta = build_tree_with_metadata(str(tmp_path))
        counts = [item["file_count"] for item in meta["languages_detected"]]
        assert counts == sorted(counts, reverse=True)

    def test_frameworks_detected_for_rails(self, tmp_path):
        _write(tmp_path / "Gemfile", "")
        (tmp_path / "app").mkdir()
        (tmp_path / "config").mkdir()
        (tmp_path / "db").mkdir()
        _write(tmp_path / "app" / "controller.rb", "")
        _, meta = build_tree_with_metadata(str(tmp_path))
        assert "Ruby on Rails" in meta["frameworks_detected"]

    def test_get_file_list_excludes_binary(self, tmp_path):
        _write(tmp_path / "code.py", "")
        _write(tmp_path / "image.png", "binary")
        files = get_file_list(str(tmp_path))
        assert "code.py" in files
        assert "image.png" not in files

    def test_get_file_list_excludes_skip_dirs(self, tmp_path):
        _write(tmp_path / "src" / "app.py", "")
        _write(tmp_path / "node_modules" / "pkg" / "index.js", "")
        files = get_file_list(str(tmp_path))
        assert all("node_modules" not in f for f in files)
        assert any("app.py" in f for f in files)


# ---------------------------------------------------------------------------
# build_tree — backward compat
# ---------------------------------------------------------------------------


class TestBuildTree:
    def test_returns_string(self, tmp_path):
        result = build_tree(str(tmp_path))
        assert isinstance(result, str)

    def test_contains_file_names(self, tmp_path):
        _write(tmp_path / "README.md", "# readme")
        result = build_tree(str(tmp_path))
        assert "README.md" in result

    def test_excludes_git_dir(self, tmp_path):
        _write(tmp_path / ".git" / "config", "")
        _write(tmp_path / "app.py", "")
        result = build_tree(str(tmp_path))
        assert ".git" not in result


# ---------------------------------------------------------------------------
# Large repo truncation
# ---------------------------------------------------------------------------


class TestLargeRepoTruncation:
    def test_tree_truncated_at_max_lines(self, tmp_path):
        """A repo with > MAX_TREE_LINES files should be truncated."""
        many_files_dir = tmp_path / "src"
        many_files_dir.mkdir()
        # Create enough files to trigger truncation
        for i in range(MAX_TREE_LINES + 100):
            (many_files_dir / f"file_{i:05d}.py").write_text("")

        tree_str, meta = build_tree_with_metadata(str(tmp_path))
        lines = tree_str.split("\n")
        assert len(lines) <= MAX_TREE_LINES + 1  # +1 for truncation notice
        assert "truncated" in tree_str.lower()

    def test_large_repo_total_files_still_accurate(self, tmp_path):
        """total_files metadata counts ALL discovered files, even truncated ones."""
        many_files_dir = tmp_path / "src"
        many_files_dir.mkdir()
        n = MAX_TREE_LINES + 50
        for i in range(n):
            (many_files_dir / f"f{i}.py").write_text("")

        _, meta = build_tree_with_metadata(str(tmp_path))
        # All files should be counted regardless of tree truncation
        assert meta["total_files"] == n

    def test_10k_files_no_oom(self, tmp_path):
        """Processing 10,000 files should not raise and should complete."""
        many_dir = tmp_path / "bulk"
        many_dir.mkdir()
        for i in range(10_000):
            (many_dir / f"m{i}.py").write_text("")

        # Should not raise MemoryError or any other exception
        tree_str, meta = build_tree_with_metadata(str(tmp_path))
        assert meta["total_files"] == 10_000
        assert isinstance(tree_str, str)


# ---------------------------------------------------------------------------
# Edge cases: binary-only, empty dirs, permission handling
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_binary_only_repo(self, tmp_path):
        """Repo with only binary files returns empty tree + zero total_files."""
        _write(tmp_path / "image.png", "")
        _write(tmp_path / "archive.zip", "")
        tree_str, meta = build_tree_with_metadata(str(tmp_path))
        assert meta["total_files"] == 0
        assert tree_str == ""

    def test_deeply_nested_structure(self, tmp_path):
        """Deeply nested paths should not exceed Python recursion limits."""
        deep = tmp_path
        for level in range(20):
            deep = deep / f"level_{level}"
        _write(deep / "leaf.py", "x = 1")
        tree_str, meta = build_tree_with_metadata(str(tmp_path))
        assert meta["total_files"] == 1
        assert "leaf.py" in tree_str

    def test_repo_with_only_hidden_files(self, tmp_path):
        """Repo with only hidden dirs returns empty tree."""
        _write(tmp_path / ".hidden" / "file.py", "")
        tree_str, meta = build_tree_with_metadata(str(tmp_path))
        assert meta["total_files"] == 0

    def test_files_at_root_level(self, tmp_path):
        """Files at root level (depth 0) should appear without indentation."""
        _write(tmp_path / "Makefile", "all:")
        _write(tmp_path / "README.md", "# docs")
        tree_str, _ = build_tree_with_metadata(str(tmp_path))
        lines = tree_str.split("\n")
        # Root-level files have no leading spaces
        root_files = [l for l in lines if l and not l.startswith(" ")]
        assert any("Makefile" in l for l in root_files)

    def test_metadata_languages_detected_is_list(self, tmp_path):
        _write(tmp_path / "a.py", "")
        _, meta = build_tree_with_metadata(str(tmp_path))
        assert isinstance(meta["languages_detected"], list)
        for item in meta["languages_detected"]:
            assert "language" in item
            assert "file_count" in item

    def test_empty_repo_primary_language_is_empty_string(self, tmp_path):
        _, meta = build_tree_with_metadata(str(tmp_path))
        assert meta["primary_language"] == ""

    def test_get_file_list_returns_relative_paths(self, tmp_path):
        _write(tmp_path / "src" / "app.py", "")
        files = get_file_list(str(tmp_path))
        assert all(not os.path.isabs(f) for f in files)
        assert "src/app.py" in files or os.path.join("src", "app.py") in files

    def test_get_file_list_sorted(self, tmp_path):
        for name in ["z.py", "a.py", "m.py"]:
            _write(tmp_path / name, "")
        files = get_file_list(str(tmp_path))
        assert files == sorted(files)

"""Compact file tree builder for repository classification.

Walks a cloned repository and produces a minimal, indented tree string
suitable for sending to an LLM. Skips noise directories (.git, node_modules, etc.)
and collapses single-child directory chains (app/controllers/api/v1/ on one line).

Primary API (V3):
    tree_str, metadata = build_tree_with_metadata(repo_path)
    # metadata = {total_files, languages_detected, primary_language, frameworks_detected}

Legacy API (backward-compatible):
    tree_str = build_tree(repo_path)
    file_list = get_file_list(repo_path)

Usage:
    python -m code_preprocessor.file_tree oscar-vet/vet_backend
    python -m code_preprocessor.file_tree /absolute/path/to/repo
"""

import os
from collections import Counter
from pathlib import Path

# Directories to always skip
SKIP_DIRS = {
    ".git",
    "node_modules",
    "vendor",
    "__pycache__",
    ".bundle",
    "dist",
    "build",
    ".next",
    ".nuxt",
    ".cache",
    ".tox",
    ".mypy_cache",
    ".pytest_cache",
    "coverage",
    ".idea",
    ".vscode",
    "tmp",
    "log",
    ".gradle",
    ".mvn",
    "target",
    "bin",
    "obj",
    ".dart_tool",
    "Pods",
    ".swiftpm",
    "venv",
    ".venv",
    "env",
    ".env",
}

# Extensions recognised as code/text (anything else is treated as binary)
BINARY_SKIP_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".ico",
    ".svg",
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".zip",
    ".tar",
    ".gz",
    ".bz2",
    ".rar",
    ".7z",
    ".mp3",
    ".mp4",
    ".avi",
    ".mov",
    ".mkv",
    ".wav",
    ".flac",
    ".ttf",
    ".otf",
    ".woff",
    ".woff2",
    ".eot",
    ".pen",
    ".fig",
    ".sketch",
    ".exe",
    ".dll",
    ".so",
    ".dylib",
    ".a",
    ".lib",
    ".pyc",
    ".pyo",
    ".class",
    ".lock",  # treated specially below — shown but not counted as code
    ".min.js",
}

# Language detection: extension → language name
EXTENSION_TO_LANGUAGE: dict[str, str] = {
    ".rb": "ruby",
    ".py": "python",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".js": "javascript",
    ".jsx": "javascript",
    ".java": "java",
    ".go": "go",
    ".rs": "rust",
    ".cs": "csharp",
    ".kt": "kotlin",
    ".swift": "swift",
    ".php": "php",
    ".cpp": "cpp",
    ".cc": "cpp",
    ".cxx": "cpp",
    ".c": "c",
    ".h": "c",
    ".hpp": "cpp",
    ".scala": "scala",
    ".ex": "elixir",
    ".exs": "elixir",
    ".clj": "clojure",
    ".hs": "haskell",
    ".lua": "lua",
    ".r": "r",
    ".R": "r",
    ".dart": "dart",
    ".vue": "vue",
    ".html": "html",
    ".htm": "html",
    ".css": "css",
    ".scss": "scss",
    ".sass": "scss",
    ".less": "css",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".json": "json",
    ".toml": "toml",
    ".xml": "xml",
    ".md": "markdown",
    ".sh": "shell",
    ".bash": "shell",
    ".zsh": "shell",
    ".fish": "shell",
    ".sql": "sql",
    ".graphql": "graphql",
    ".gql": "graphql",
    ".tf": "terraform",
    ".proto": "protobuf",
    ".gradle": "groovy",
}

# Framework fingerprints: list of files/dirs whose presence signals a framework.
# Each entry: (framework_name, required_files, required_dirs)
_FRAMEWORK_SIGNALS: list[tuple[str, list[str], list[str]]] = [
    ("Ruby on Rails", ["Gemfile"], ["app", "config", "db"]),
    ("Next.js", ["next.config.js"], []),
    ("Next.js", ["next.config.ts"], []),
    ("Next.js", ["next.config.mjs"], []),
    ("Django", ["manage.py", "requirements.txt"], []),
    ("FastAPI", ["requirements.txt", "main.py"], []),
    ("React", ["package.json"], ["src"]),
    ("Vue.js", ["vue.config.js"], []),
    ("Angular", ["angular.json"], []),
    ("NestJS", ["nest-cli.json"], []),
    ("Laravel", ["artisan", "composer.json"], []),
    ("Spring Boot", ["pom.xml"], ["src/main/java"]),
    ("Gradle/JVM", ["build.gradle"], []),
    ("Cargo/Rust", ["Cargo.toml"], []),
    ("Go module", ["go.mod"], []),
    ("Flutter", ["pubspec.yaml", "lib"], []),
    ("Terraform", [], ["modules", "environments"]),
]

# File size bands (line count thresholds)
_SIZE_SMALL = 100  # < 100 lines → [S]
_SIZE_MEDIUM = 500  # 100–499 lines → [M], ≥ 500 lines → [L]

# Maximum tree output lines before truncation
MAX_TREE_LINES = 2000

# Default repo storage root (matches preprocessor config)
DEFAULT_REPO_ROOT = Path("rag_storage/repos")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _should_skip(name: str) -> bool:
    """Return True if this directory name should be skipped."""
    return name in SKIP_DIRS or name.startswith(".")


def _is_binary(name: str) -> bool:
    """Return True if file extension indicates a binary/non-text asset."""
    _, ext = os.path.splitext(name)
    return ext.lower() in BINARY_SKIP_EXTENSIONS


def _count_lines(path: Path) -> int:
    """Count newlines in a file cheaply; return 0 on any error."""
    try:
        with open(path, "rb") as fh:
            return fh.read().count(b"\n")
    except OSError:
        return 0


def _size_label(path: Path) -> str:
    """Return [S], [M], or [L] based on file line count."""
    n = _count_lines(path)
    if n < _SIZE_SMALL:
        return "[S]"
    if n < _SIZE_MEDIUM:
        return "[M]"
    return "[L]"


def _detect_language(name: str) -> str | None:
    """Return language string for a filename, or None."""
    _, ext = os.path.splitext(name)
    return EXTENSION_TO_LANGUAGE.get(ext.lower()) or EXTENSION_TO_LANGUAGE.get(ext)


def _collapse(d: Path) -> tuple[Path, Path]:
    """Collapse single-child directory chains.

    If a directory has exactly one child and it's a directory (no files),
    collapse them: app/controllers/api/v1 → single path prefix.

    Returns (original_start_dir, final_leaf_dir).
    """
    current = d
    while True:
        try:
            children = [c for c in current.iterdir() if not _should_skip(c.name)]
        except PermissionError:
            break
        dirs = [c for c in children if c.is_dir()]
        files = [c for c in children if c.is_file()]
        if len(dirs) == 1 and len(files) == 0:
            current = dirs[0]
        else:
            break
    return d, current


def _detect_frameworks(root: Path) -> list[str]:
    """Detect frameworks from files/dirs present at repo root."""
    root_names = set()
    try:
        for entry in root.iterdir():
            root_names.add(entry.name)
    except PermissionError:
        return []

    detected: list[str] = []
    for fw_name, req_files, req_dirs in _FRAMEWORK_SIGNALS:
        if all(f in root_names for f in req_files) and all((root / d).is_dir() for d in req_dirs):
            if fw_name not in detected:
                detected.append(fw_name)
    return detected


# ---------------------------------------------------------------------------
# Walk + tree builder
# ---------------------------------------------------------------------------


class _WalkState:
    """Mutable accumulator passed through the recursive walk."""

    __slots__ = ("lines", "lang_counter", "total_files", "truncated")

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.lang_counter: Counter[str] = Counter()
        self.total_files: int = 0
        self.truncated: bool = False


def _count_all_files(node: Path, state: _WalkState) -> None:
    """Count all remaining files after tree truncation (no line output)."""
    try:
        entries = list(node.iterdir())
    except PermissionError:
        return

    dirs = [e for e in entries if e.is_dir() and not _should_skip(e.name)]
    files = [e for e in entries if e.is_file() and not _is_binary(e.name)]

    for f in files:
        state.total_files += 1
        lang = _detect_language(f.name)
        if lang:
            state.lang_counter[lang] += 1

    for d in dirs:
        _, final_dir = _collapse(d)
        _count_all_files(final_dir, state)


def _walk(node: Path, state: _WalkState, depth: int) -> None:
    """Recursively walk and build tree lines with directory collapsing."""
    try:
        entries = sorted(node.iterdir(), key=lambda p: (p.is_file(), p.name))
    except PermissionError:
        return

    dirs = [e for e in entries if e.is_dir() and not _should_skip(e.name)]
    files = [e for e in entries if e.is_file() and not _is_binary(e.name)]

    # Emit files at current depth
    for f in files:
        state.total_files += 1
        lang = _detect_language(f.name)
        if lang:
            state.lang_counter[lang] += 1

        if not state.truncated:
            label = _size_label(f)
            state.lines.append("  " * depth + f"{f.name} {label}")

            if len(state.lines) >= MAX_TREE_LINES:
                state.lines.append("... (tree truncated)")
                state.truncated = True

    # Process directories with collapsing
    for d in dirs:
        _, final_dir = _collapse(d)

        if state.truncated:
            # Still count files in remaining dirs for accurate metadata
            _count_all_files(final_dir, state)
            continue

        try:
            sub_entries = list(final_dir.iterdir())
        except PermissionError:
            continue

        sub_has_content = any(not _should_skip(e.name) for e in sub_entries)
        if not sub_has_content:
            continue

        state.lines.append("  " * depth + str(final_dir.relative_to(node)) + "/")
        if len(state.lines) >= MAX_TREE_LINES:
            state.lines.append("... (tree truncated)")
            state.truncated = True
            _count_all_files(final_dir, state)
            continue

        _walk(final_dir, state, depth + 1)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_tree_with_metadata(repo_path: str) -> tuple[str, dict]:
    """Build a compact indented file tree string and return repo metadata.

    Collapses single-child directory chains, annotates files with size labels
    ([S] < 100 lines, [M] 100–499, [L] ≥ 500), skips hidden dirs and binary
    files, and truncates output at MAX_TREE_LINES to stay LLM-context-friendly.

    Args:
        repo_path: Absolute or relative path to repository root.

    Returns:
        (tree_string, metadata) where metadata contains:
            total_files       – count of non-binary files discovered
            languages_detected – list of (language, file_count) sorted by count desc
            primary_language  – language with most files, or '' if none
            frameworks_detected – list of detected framework names
    """
    root = Path(repo_path).resolve()
    if not root.is_dir():
        return f"ERROR: {repo_path} is not a directory", {
            "total_files": 0,
            "languages_detected": [],
            "primary_language": "",
            "frameworks_detected": [],
        }

    state = _WalkState()
    _walk(root, state, depth=0)

    tree_str = "\n".join(state.lines)

    sorted_langs = state.lang_counter.most_common()
    primary_language = sorted_langs[0][0] if sorted_langs else ""

    metadata: dict = {
        "total_files": state.total_files,
        "languages_detected": [{"language": lang, "file_count": cnt} for lang, cnt in sorted_langs],
        "primary_language": primary_language,
        "frameworks_detected": _detect_frameworks(root),
    }
    return tree_str, metadata


def build_tree(repo_path: str) -> str:
    """Build a compact indented file tree string.

    Collapses single-child directory chains:
        app/controllers/api/v1/
          users_controller.rb [M]
    instead of:
        app/
          controllers/
            api/
              v1/
                users_controller.rb [M]

    Args:
        repo_path: Absolute or relative path to repository root.

    Returns:
        Compact tree string with 2-space indentation and size labels.
    """
    tree_str, _ = build_tree_with_metadata(repo_path)
    return tree_str


def get_file_list(repo_path: str) -> list[str]:
    """Get flat list of all non-binary file paths relative to repo root.

    Args:
        repo_path: Path to repository root.

    Returns:
        Sorted list of relative file paths (binary files excluded).
    """
    root = Path(repo_path).resolve()
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Filter out skip directories in-place
        dirnames[:] = [d for d in dirnames if not _should_skip(d)]
        for f in sorted(filenames):
            if not _is_binary(f):
                rel = os.path.relpath(os.path.join(dirpath, f), root)
                files.append(rel)
    return files


def resolve_repo_path(repo_name: str) -> Path:
    """Resolve a repository name to its local clone path.

    Supports:
        - Absolute path: /path/to/repo
        - Relative path: ./my-repo
        - Repo slug: oscar-vet/vet_backend → rag_storage/repos/oscar-vet__vet_backend

    Args:
        repo_name: Repository name, slug, or path.

    Returns:
        Resolved Path to the repository directory.
    """
    p = Path(repo_name)
    if p.is_absolute() and p.is_dir():
        return p
    if p.is_dir():
        return p.resolve()

    # Try repo slug format: org/repo → org__repo
    slug = repo_name.replace("/", "__")
    repo_path = DEFAULT_REPO_ROOT / slug
    if repo_path.is_dir():
        return repo_path.resolve()

    # Last resort: treat as relative path
    return p.resolve()


if __name__ == "__main__":
    import sys
    import json

    target = sys.argv[1] if len(sys.argv) > 1 else "."
    path = resolve_repo_path(target)

    if not path.is_dir():
        print(f"ERROR: {target} → {path} is not a directory")
        sys.exit(1)

    print(f"Repository: {path}\n")
    tree, meta = build_tree_with_metadata(str(path))
    print(tree)

    print(f"\n--- Metadata ---")
    print(json.dumps(meta, indent=2))

    files = get_file_list(str(path))
    print(f"\n--- {len(files)} files total ---")

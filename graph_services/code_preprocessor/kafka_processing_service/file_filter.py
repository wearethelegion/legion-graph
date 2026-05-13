"""File filtering service for ingestion.

Filters files based on project-specific rules:
- File size limits
- Rejected file extensions
- Rejected directory patterns
"""

from __future__ import annotations

import fnmatch
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional


class FilterReason(str, Enum):
    """Reason why a file was filtered."""

    SIZE = "size"
    EXTENSION = "extension"
    DIRECTORY = "directory"
    NONE = "none"


@dataclass
class FilterResult:
    """Result of file filtering check."""

    filtered: bool
    reason: FilterReason
    detail: str = ""


@dataclass
class IngestionRules:
    """Ingestion rules for filtering files.

    This mirrors the PostgreSQL table structure but is used
    in the preprocessor without direct DB access.
    """

    max_file_size_bytes: int
    rejected_extensions: List[str]
    rejected_directories: List[str]
    rejected_filenames: List[str]

    @classmethod
    def defaults(cls) -> "IngestionRules":
        """Return default ingestion rules."""
        return cls(
            max_file_size_bytes=1048576,  # 1MB
            rejected_extensions=[
                # Documents & spreadsheets
                ".md",
                ".mdx",
                ".rst",
                ".txt",
                ".pdf",
                ".doc",
                ".docx",
                ".xls",
                ".xlsx",
                ".xlsm",
                ".xlsb",
                ".csv",
                ".tsv",
                ".ppt",
                ".pptx",
                ".odt",
                ".ods",
                ".odp",
                ".rtf",
                # Images
                ".png",
                ".jpg",
                ".jpeg",
                ".gif",
                ".bmp",
                ".svg",
                ".ico",
                ".webp",
                ".tiff",
                ".tif",
                ".psd",
                ".ai",
                ".eps",
                # Fonts
                ".woff",
                ".woff2",
                ".ttf",
                ".otf",
                ".eot",
                # Audio & video
                ".mp3",
                ".mp4",
                ".wav",
                ".avi",
                ".mov",
                ".mkv",
                ".flv",
                ".ogg",
                ".webm",
                ".m4a",
                ".m4v",
                # Archives & compressed
                ".zip",
                ".tar",
                ".gz",
                ".bz2",
                ".xz",
                ".7z",
                ".rar",
                ".tgz",
                ".tar.gz",
                ".jar",
                ".war",
                ".ear",
                # Binaries & executables
                ".exe",
                ".dll",
                ".so",
                ".dylib",
                ".bin",
                ".dat",
                ".o",
                ".a",
                ".lib",
                ".class",
                ".pyc",
                ".pyo",
                ".wasm",
                ".deb",
                ".rpm",
                ".dmg",
                ".iso",
                ".img",
                # Database & backups
                ".sql",
                ".sqlite",
                ".db",
                ".bak",
                ".dump",
                ".mdb",
                # Certificates & keys
                ".pem",
                ".crt",
                ".key",
                ".cer",
                ".p12",
                ".pfx",
                # Maps & minified bundles
                ".map",
                ".min.js",
                ".min.css",
                # Design tools (binary/non-code)
                ".pen",
                ".fig",
                ".sketch",
                # Misc non-code
                ".log",
                ".lock",
                ".patch",
                ".diff",
                # Config/template files (non-structural)
                ".yml",
                ".yaml",
                ".toml",
                ".ini",
                ".cfg",
                ".conf",
                ".properties",
                ".erb",
                ".ejs",
                ".hbs",
                ".mustache",
                ".jinja",
                ".jinja2",
            ],
            rejected_directories=[
                # Hidden directories (catches .git, .idea, .vscode, etc.)
                ".*",
                # IDE & editor
                ".idea",
                ".vscode",
                ".cursor",
                ".vs",
                ".fleet",
                ".eclipse",
                ".settings",
                ".project",
                # Dependencies
                "node_modules",
                "vendor",
                "bower_components",
                "jspm_packages",
                ".yarn",
                ".pnp",
                # Python
                "__pycache__",
                ".venv",
                "venv",
                "env",
                ".env",
                ".tox",
                ".nox",
                ".mypy_cache",
                ".pytest_cache",
                ".ruff_cache",
                "*.egg-info",
                # Build output
                "dist",
                "build",
                "out",
                "output",
                "target",
                "_build",
                "public",
                "static",
                ".next",
                ".nuxt",
                ".svelte-kit",
                ".parcel-cache",
                ".turbo",
                # Coverage & test artifacts
                "coverage",
                ".nyc_output",
                "htmlcov",
                # Misc
                "tmp",
                "temp",
                "logs",
                ".cache",
                ".terraform",
                ".serverless",
            ],
            rejected_filenames=[
                # Lock files
                "package-lock.json",
                "yarn.lock",
                "pnpm-lock.yaml",
                "composer.lock",
                "Gemfile.lock",
                "poetry.lock",
                "Pipfile.lock",
                "bun.lockb",
                "shrinkwrap.json",
                "npm-shrinkwrap.json",
                "go.sum",
                "Cargo.lock",
                "flake.lock",
                # Generated / non-code
                ".DS_Store",
                "Thumbs.db",
                ".gitattributes",
                ".editorconfig",
                ".browserslistrc",
                # License & legal (extensionless or non-standard)
                "LICENSE",
                "LICENSE.*",
                "LICENCE",
                "LICENCE.*",
                "COPYING",
                "COPYING.*",
                "NOTICE",
                "NOTICE.*",
                "THIRD_PARTY_NOTICES*",
                "AUTHORS",
                "AUTHORS.*",
                "CONTRIBUTORS",
                "CONTRIBUTORS.*",
                "PATENTS",
                # Readme variants
                "README",
                "README.*",
                "CHANGELOG",
                "CHANGELOG.*",
                "CHANGES",
                "CHANGES.*",
                "HISTORY",
                "HISTORY.*",
                "RELEASE_NOTES*",
                # Docker/CI/config (non-code)
                "Dockerfile",
                "Dockerfile.*",
                "docker-compose*",
                "Makefile",
                "Procfile",
                ".dockerignore",
                ".gitignore",
                ".npmignore",
                ".eslintignore",
                ".prettierignore",
            ],
        )


class FileFilter:
    """Filter files based on ingestion rules.

    Used in the preprocessor BEFORE Kafka emission to reduce
    unnecessary processing of files that should be skipped.
    """

    def __init__(self, rules: Optional[IngestionRules] = None):
        """Initialize filter with rules.

        Args:
            rules: Ingestion rules to use. Defaults to IngestionRules.defaults()
        """
        self.rules = rules or IngestionRules.defaults()

    def check(self, file_path: str, file_size_bytes: int) -> FilterResult:
        """Check if a file should be filtered.

        Args:
            file_path: Relative path of the file (from repo root)
            file_size_bytes: Size of the file in bytes

        Returns:
            FilterResult indicating if file was filtered and why
        """
        # Check file size
        if file_size_bytes > self.rules.max_file_size_bytes:
            return FilterResult(
                filtered=True,
                reason=FilterReason.SIZE,
                detail=f"File size {file_size_bytes} exceeds limit {self.rules.max_file_size_bytes}",
            )

        # Check rejected filenames (lock files, license files, etc.)
        file_name = Path(file_path).name.lower()
        for rejected_name in self.rules.rejected_filenames:
            if self._matches_pattern(file_name, rejected_name.lower()):
                return FilterResult(
                    filtered=True,
                    reason=FilterReason.EXTENSION,
                    detail=f"Filename '{file_name}' matches rejected pattern '{rejected_name}'",
                )

        # Reject files without an extension (LICENSE, COPYING, etc.)
        file_ext = Path(file_path).suffix.lower()
        if not file_ext:
            return FilterResult(
                filtered=True,
                reason=FilterReason.EXTENSION,
                detail=f"File '{file_name}' has no extension",
            )
        for rejected_ext in self.rules.rejected_extensions:
            if file_ext == rejected_ext.lower():
                return FilterResult(
                    filtered=True,
                    reason=FilterReason.EXTENSION,
                    detail=f"Extension {file_ext} is rejected",
                )

        # Check directory patterns
        path_parts = Path(file_path).parts
        for rejected_pattern in self.rules.rejected_directories:
            for part in path_parts:
                if self._matches_pattern(part, rejected_pattern):
                    return FilterResult(
                        filtered=True,
                        reason=FilterReason.DIRECTORY,
                        detail=f"Directory '{part}' matches rejected pattern '{rejected_pattern}'",
                    )

        return FilterResult(filtered=False, reason=FilterReason.NONE)

    def _matches_pattern(self, name: str, pattern: str) -> bool:
        """Check if name matches pattern using fnmatch.

        Supports glob patterns like '.*' for hidden directories.

        Args:
            name: Directory or file name to check
            pattern: Glob pattern to match against

        Returns:
            True if name matches pattern
        """
        return fnmatch.fnmatch(name, pattern)

    def get_file_size(self, repo_path: Path, relative_path: str) -> int:
        """Get file size using os.stat (no content read).

        IMPORTANT: Uses os.stat() instead of reading content
        to avoid memory issues with large files.

        Args:
            repo_path: Absolute path to repository root
            relative_path: Relative path from repo root

        Returns:
            File size in bytes, or 0 if file doesn't exist
        """
        try:
            full_path = repo_path / relative_path
            return os.stat(full_path).st_size
        except (OSError, FileNotFoundError):
            return 0


@dataclass
class FilterStats:
    """Statistics from filtering files."""

    files_filtered_size: int = 0
    files_filtered_extension: int = 0
    files_filtered_directory: int = 0

    @property
    def total_filtered(self) -> int:
        """Total number of filtered files."""
        return (
            self.files_filtered_size + self.files_filtered_extension + self.files_filtered_directory
        )

    def increment(self, reason: FilterReason) -> None:
        """Increment counter for filter reason."""
        if reason == FilterReason.SIZE:
            self.files_filtered_size += 1
        elif reason == FilterReason.EXTENSION:
            self.files_filtered_extension += 1
        elif reason == FilterReason.DIRECTORY:
            self.files_filtered_directory += 1

"""Configuration for Code Service V2.

Centralized configuration for Gemini-based code analysis with caching,
batching, and parallel processing optimizations.
"""

import os
from typing import Final


class CodeServiceConfig:
    """Configuration settings for optimized code analysis."""

    # Gemini API Settings
    GEMINI_MODEL: Final[str] = "gemini-3-flash-preview"
    GEMINI_API_KEY: Final[str] = os.getenv("GEMINI_API_KEY", "")

    # Generation Parameters (deterministic for code parsing)
    TEMPERATURE: Final[float] = 0.1  # Low for consistent parsing
    TOP_P: Final[float] = 0.95
    TOP_K: Final[int] = 40
    MAX_OUTPUT_TOKENS: Final[int] = 64000

    # Timeout Settings
    REQUEST_TIMEOUT: Final[int] = 300  # 5 minutes per request

    # Caching Settings
    CACHE_TTL: Final[int] = 3600  # 1 hour
    USE_CACHING: Final[bool] = True

    # Batching Settings
    MIN_BATCH_SIZE: Final[int] = 1
    MAX_BATCH_SIZE: Final[int] = 5  # Files per batch (for small files)
    MAX_BATCH_TOKENS: Final[int] = 30000  # Max tokens per batch
    CHARS_PER_TOKEN: Final[int] = 4  # Token estimation heuristic

    # Filtering Settings
    MIN_FILE_SIZE: Final[int] = 100  # bytes
    MAX_FILE_SIZE: Final[int] = 100000  # Skip huge files (100KB)
    SKIP_PATTERNS: Final[list[str]] = [
        "**/node_modules/**",
        "**/venv/**",
        "**/__pycache__/**",
        "**/dist/**",
        "**/build/**",
        "**/.git/**",
    ]

    # Parallel Processing Settings
    MAX_CONCURRENT_REQUESTS: Final[int] = 100
    RATE_LIMIT_RPM: Final[int] = 1500  # Requests per minute
    RATE_LIMIT_TPM: Final[int] = 4_000_000  # Tokens per minute

    # Retry Settings
    MAX_RETRIES: Final[int] = 3
    RETRY_DELAY: Final[float] = 2.0  # Seconds (exponential backoff)

    # Known Languages (for better prompts, NOT for filtering)
    KNOWN_LANGUAGES: Final[dict[str, str]] = {
        ".py": "python",
        ".rb": "ruby",
        ".ts": "typescript",
        ".tsx": "typescript",
        ".js": "javascript",
        ".jsx": "javascript",
        ".go": "go",
        ".java": "java",
        ".kt": "kotlin",
        ".rs": "rust",
        ".cpp": "cpp",
        ".c": "c",
        ".cs": "csharp",
        ".php": "php",
        ".swift": "swift",
        ".scala": "scala",
        ".sh": "bash",
        ".bash": "bash",
        ".zsh": "bash",
        ".sql": "sql",
        ".r": "r",
        ".lua": "lua",
        ".pl": "perl",
        ".pm": "perl",
        ".ex": "elixir",
        ".exs": "elixir",
        ".erl": "erlang",
        ".hrl": "erlang",
        ".hs": "haskell",
        ".ml": "ocaml",
        ".fs": "fsharp",
        ".clj": "clojure",
        ".groovy": "groovy",
        ".gradle": "groovy",
        ".tf": "terraform",
        ".tfvars": "terraform",
        ".hcl": "hcl",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".json": "json",
        ".xml": "xml",
        ".html": "html",
        ".htm": "html",
        ".css": "css",
        ".scss": "scss",
        ".sass": "sass",
        ".less": "less",
        ".vue": "vue",
        ".svelte": "svelte",
        ".proto": "protobuf",
        ".graphql": "graphql",
        ".gql": "graphql",
        ".toml": "toml",
        ".ini": "ini",
        ".cfg": "ini",
        ".conf": "config",
        ".env": "dotenv",
        ".dockerfile": "dockerfile",
        ".makefile": "makefile",
        ".cmake": "cmake",
    }

    # Excluded Extensions (blocklist approach - skip these, process everything else)
    EXCLUDED_EXTENSIONS: Final[set[str]] = {
        # Documentation
        ".md", ".rst", ".txt", ".pdf", ".doc", ".docx", ".rtf",
        # Spreadsheets
        ".xlsx", ".xls", ".csv", ".ods",
        # Images
        ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".bmp", ".webp", ".tiff",
        # Audio/Video
        ".mp3", ".mp4", ".wav", ".avi", ".mov", ".mkv", ".webm",
        # Fonts
        ".ttf", ".otf", ".woff", ".woff2", ".eot",
        # Binaries/Executables
        ".exe", ".dll", ".so", ".dylib", ".bin", ".o", ".a", ".lib",
        # Archives
        ".zip", ".tar", ".gz", ".rar", ".7z", ".bz2", ".xz",
        # Lock files
        ".lock",
        # Minified files (detected by pattern, but also by extension)
        ".min.js", ".min.css",
        # Cache/temp
        ".pyc", ".pyo", ".class", ".jar",
        # Data files
        ".db", ".sqlite", ".sqlite3",
        # Certificates/Keys (security - don't process)
        ".pem", ".crt", ".key", ".p12", ".pfx",
        # Logs
        ".log",
    }

    # Keep for backward compatibility (deprecated, use KNOWN_LANGUAGES)
    SUPPORTED_LANGUAGES: Final[dict[str, str]] = KNOWN_LANGUAGES

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration."""
        if not cls.GEMINI_API_KEY:
            raise ValueError(
                "GEMINI_API_KEY environment variable is required"
            )

    @classmethod
    def get_language(cls, file_path: str) -> str | None:
        """Get language from file extension.
        
        Returns known language name or 'unknown' for processable files.
        Returns None only for explicitly excluded extensions.
        """
        file_lower = file_path.lower()
        
        # Check exclusion list first
        for ext in cls.EXCLUDED_EXTENSIONS:
            if file_lower.endswith(ext):
                return None  # Excluded - don't process
        
        # Check known languages
        for ext, lang in cls.KNOWN_LANGUAGES.items():
            if file_lower.endswith(ext):
                return lang
        
        # Not excluded, not known - process as unknown (LLM will figure it out)
        return "unknown"

    @classmethod
    def is_excluded(cls, file_path: str) -> bool:
        """Check if file should be excluded from processing."""
        file_lower = file_path.lower()
        for ext in cls.EXCLUDED_EXTENSIONS:
            if file_lower.endswith(ext):
                return True
        return False
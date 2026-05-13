"""Exception classes for Code Intelligence Preprocessor."""


class PreprocessorError(Exception):
    """Base exception for preprocessor errors."""


class IngestionError(PreprocessorError):
    """Raised when ingestion processing fails."""


class VersionStoreError(PreprocessorError):
    """Raised when version store operations fail."""

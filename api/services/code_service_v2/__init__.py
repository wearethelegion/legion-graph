"""Code Service V2 - Optimized code analysis with Gemini 2.5 Flash.

High-performance code parsing service with:
- Context caching (70% cost savings)
- Parallel processing (50 concurrent requests)
- Rate limiting (1500 RPM)
- Structured JSON output (100% valid)

Public API:
    CodeAnalyzerService: Main service for code analysis
    CodeFile: Input data structure
    AnalysisReport: Output data structure
"""

from .code_analyzer_service import CodeAnalyzerService
from .batch_processor import CodeFile
from .schema import (
    AnalysisReport,
    CodeAnalysisResult,
    Entity,
    FileMetadata,
    Relationship,
    StorageStats,
)
from .config import CodeServiceConfig

__all__ = [
    # Main service
    "CodeAnalyzerService",
    # Input/Output models
    "CodeFile",
    "AnalysisReport",
    "CodeAnalysisResult",
    # Schema models
    "Entity",
    "FileMetadata",
    "Relationship",
    "StorageStats",
    # Configuration
    "CodeServiceConfig",
]

__version__ = "2.0.0"

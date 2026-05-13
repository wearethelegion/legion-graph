"""
Code Search subpackage.

Exports CodeSearchService, which is a thin alias for CodeSearchComposite,
the full implementation of six primitives + full_search composite.

Usage:
    from api.services.code_search import CodeSearchService
    # or equivalently:
    from api.services.code_search_service import CodeSearchService
"""

from api.services.code_search.composite import CodeSearchComposite

# Public alias: CodeSearchComposite IS CodeSearchService for this subpackage
CodeSearchService = CodeSearchComposite

__all__ = ["CodeSearchService"]

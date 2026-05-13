"""
Document Search subpackage.

Exports DocumentSearchService, which is a thin alias for DocumentSearchComposite,
the full implementation of six primitives + full_document_search composite.

Usage:
    from api.services.document_search import DocumentSearchService

Six primitives:
    get_collections(company_id)
    search_documents(query, company_id, collection=None, limit=10)
    get_document_chunk(chunk_id, company_id)
    search_document_summaries(query, company_id, limit=10)
    traverse_document_graph(entity_name, company_id)
    full_document_search(query, company_id, limit=10)  [composite]
"""

from api.services.document_search.composite import DocumentSearchComposite

# Public alias: DocumentSearchComposite IS DocumentSearchService
DocumentSearchService = DocumentSearchComposite

__all__ = ["DocumentSearchService"]

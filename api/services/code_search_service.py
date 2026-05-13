"""
Code Search Service — production entry point.

Ported from scripts/code_search_tools.py (spec) with these critical adaptations:
  1. company_id REQUIRED on every method; drives Neo4j DB = f"cognee-{company_id}".
  2. Vertex AI/litellm replaced with kgrag.embeddings.GeminiEmbedder (sync embed_query).
  3. Injected Neo4jRepository + QdrantRepository singletons (no per-call driver creation).
  4. search_summaries: chunk_id primary path removed (T1 gap — always empty in V2).
     Uses file_version_id directly, saving one futile Neo4j query per result.
  5. get_code_for_entity: prefers has_code → CodeBlock first (richer line metadata),
     falls back to made_from → DocumentChunk.
  6. Qdrant vs Neo4j source_node_set divergence documented in primitives.py.

Qdrant contract (T1 entry 517eac95-366d-46d2-a220-efaf9302dd05):
  - Entity_name        → source_node_set = entities_{project_id}, vector key "text"
  - DocumentChunk_text → source_node_set = code_{project_id}
  - TextSummary_text   → source_node_set = summaries_{project_id}

T1 gaps applied (entry 1bc86073-e08d-48ba-ba03-aa5cd1781f62):
  - TextSummary chunk_id always '' → use file_version_id only
  - CodeBlock preferred over DocumentChunk for richer line metadata

T1 outcome (entry 772c1e29-bb24-4227-9f76-cf3b9b54473e):
  - scripts/code_search_tools.py ported AS-IS with above fixes

Plan (entry 45069f1b-16f8-4c56-8957-b90da4329ab7):
  - T2 deliverable: this file + tests/unit/services/test_code_search_service.py

Implementation split:
  api/services/code_search/
    primitives.py   — six atomic search methods (CodeSearchPrimitives)
    composite.py    — full_search orchestrator (CodeSearchComposite)
    __init__.py     — re-exports CodeSearchService alias
  api/services/code_search_service.py — this file, thin re-export for backward compat
"""

from api.services.code_search.primitives import (
    DEFAULT_BRANCH,
    COLLECTION_CHUNKS,
    COLLECTION_ENTITIES,
    COLLECTION_SUMMARIES,
    DOMAIN_META_TYPES,
    NEO4J_URI,
    NEO4J_USER,
    TEST_ENTITY_PATTERNS,
    TEST_FILE_PATTERNS,
    _neo4j_db_name,
)
from api.services.code_search.composite import CodeSearchComposite

# CodeSearchService is the public API — downstream code imports this
CodeSearchService = CodeSearchComposite

__all__ = [
    "CodeSearchService",
    # Constants (re-exported for tests and servicer layer)
    "DEFAULT_BRANCH",
    "COLLECTION_CHUNKS",
    "COLLECTION_ENTITIES",
    "COLLECTION_SUMMARIES",
    "DOMAIN_META_TYPES",
    "NEO4J_URI",
    "NEO4J_USER",
    "TEST_ENTITY_PATTERNS",
    "TEST_FILE_PATTERNS",
]

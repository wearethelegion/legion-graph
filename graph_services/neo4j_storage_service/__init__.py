"""Service 6: Neo4j Storage — v2 pipeline Phase B.

Batch service triggered after the dedup gate (all Phase A services complete).
Reads deduped entities + edges from Postgres, batch-writes to Neo4j:
- Phase 1: MERGE Entity nodes, EntityType nodes, DocumentChunk nodes (UNWIND)
- Phase 2: MERGE edges — contains, made_from, is_a, LLM relationships (UNWIND)

Listens on pipeline-events Kafka topic for completion signals.
"""

"""Service 5: Qdrant Storage — v2 pipeline Phase B.

Batch service triggered after the dedup gate (all Phase A services complete).
Reads deduped data from Postgres, batch-writes to Qdrant collections:
- DocumentChunk_text: chunk text + pre-computed embeddings
- Entity_name: entity names + embeddings from embedding service
- TextSummary_text: summary text + embeddings from embedding service

Listens on pipeline-events Kafka topic for completion signals.
"""

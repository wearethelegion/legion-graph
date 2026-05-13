"""Document Preprocessor Service.

Consumes BrainEvent messages from the brain_events Kafka topic,
chunks document text (markdown-aware heading splitting + paragraph fallback),
and produces EnrichedChunkMessage messages to the enriched-code-chunks topic
with content_type="document".

This service replaces the Cognee black-box brain pipeline, feeding documents
into the V2 code intelligence pipeline (entity extraction → summarization →
embedding → Neo4j → Qdrant).
"""

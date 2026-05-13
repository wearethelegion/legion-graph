"""Service 2: Entity Extraction Consumer — v2 pipeline.

Standalone service that:
1. Consumes enriched code chunks from Kafka (enriched-code-chunks topic)
2. Runs LLM entity extraction via Cognee's extract_content_graph
3. Stores extracted entities + edges in Postgres staging tables
4. Publishes ExtractedEntitiesEvent to Kafka (extracted-entities topic)
5. Tracks pipeline counters for observability

Consumer group: entity-extraction-processor-v2
"""

"""Service 4: Embedding Service — v2 pipeline.

Standalone service that:
1. Consumes extracted entities from Kafka (extracted-entities topic)
2. Consumes text summaries from Kafka (text-summaries topic)
3. Generates embeddings via embedding API (batched, 100 per call)
4. Stores embeddings in Postgres staging table (pipeline_embeddings)
5. Publishes EmbeddingReadyEvent to Kafka (embeddings-ready topic)
6. Tracks pipeline counters for observability

Consumer group: embedding-processor-v2
"""

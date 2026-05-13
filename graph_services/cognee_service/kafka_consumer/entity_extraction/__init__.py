"""Entity Extraction Kafka Consumer for Cognee.

Independent consumer that reads enriched code chunks from the enriched-code-chunks
Kafka topic and performs ONLY entity extraction + graph storage. Runs with its own
consumer group (entity-extraction-processor) so it receives every message independently
from the summarization consumer.

Pipeline stages:
- Stage 0: Batch store pre-computed chunk embeddings in Qdrant
- Stage 3a: Parallel extract_content_graph per chunk (LLM entity extraction)
- Stage 3b: Integrate chunk graphs (merge, deduplicate, ontology, store to Neo4j + Qdrant)
- Cognee add: Register Data/Dataset in Postgres for access-controlled search
- Metadata: Write searchability metadata

Does NOT include summarization — that is handled by a separate consumer.
"""

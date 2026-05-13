"""Enriched Chunks Kafka Consumer for Cognee.

This consumer reads pre-enriched code chunks from the enriched-code-chunks Kafka topic
and processes them through Cognee stages 3-6 (LLM extraction + storage).

Key differences from the main cogni consumer:
- Skips stages 1-2 (normalization + chunking) - done by preprocessor
- Receives enriched chunks with pre-computed embeddings
- Runs batch-of-50 with sliding window concurrency
- Uses custom code graph extraction prompt
"""

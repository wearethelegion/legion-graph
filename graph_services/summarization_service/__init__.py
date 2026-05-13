"""Service 3: Summarization Consumer — v2 pipeline.

Standalone service that:
1. Consumes enriched code chunks from Kafka (enriched-code-chunks topic)
2. Runs text summarization via Cognee's summarize_text
3. Stores summaries in Postgres staging tables
4. Publishes TextSummaryEvent to Kafka (text-summaries topic)
5. Tracks pipeline counters for observability

Consumer group: summarization-processor-v2
"""

# Neo4j Storage Service — Streaming Architecture

## Overview

The Neo4j Storage Service has been converted from a **gate-based batch processor** to a **streaming micro-batch processor**.

### Before (Gate-Based)
- Waited for 3 completion signals: `extraction_complete`, `summarization_complete`, `embedding_complete`
- Read all data from Postgres pipeline tables AFTER entire pipeline finished
- Batch-wrote everything to Neo4j in one large transaction
- Data sat idle until the entire pipeline completed

### After (Streaming)
- Consumes directly from `extracted-entities` Kafka topic
- Writes entities/edges to Neo4j as they arrive, in micro-batches
- No waiting — data flows to Neo4j immediately after entity extraction
- Two-stage pipeline: Collector → Batch Workers

## Architecture

```
extracted-entities topic (Kafka)
         ↓
   Work Queue (asyncio.Queue)
         ↓
   Collector Loop (batches up to 100 items or 0.5s timeout)
         ↓
   Batch Queue (asyncio.Queue)
         ↓
   10 Batch Workers (parallel)
         ↓
   Neo4j writes (per company database)
         ↓
   Counter updates in Postgres
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_STORAGE_DATA_TOPIC` | `extracted-entities` | Kafka topic to consume from |
| `NEO4J_STORAGE_WORKERS` | `10` | Number of parallel batch workers |
| `NEO4J_STORAGE_BATCH_SIZE` | `100` | Micro-batch size (events per batch) |
| `NEO4J_STORAGE_COLLECT_TIMEOUT` | `0.5` | Seconds to wait for batch to fill |
| `NEO4J_BATCH_SIZE` | `500` | Neo4j UNWIND batch size |
| `NEO4J_STORAGE_CONSUMER_GROUP_ID` | `neo4j-storage-streaming` | Kafka consumer group |

### Kafka Message Schema

The service consumes `ExtractedEntitiesEvent` messages from the `extracted-entities` topic:

```json
{
  "event_id": "uuid",
  "ingestion_id": "uuid",
  "chunk_id": "uuid",
  "company_id": "uuid",
  "project_id": "uuid",
  "entities": [
    {
      "entity_id": "uuid5",
      "name": "MyClass",
      "entity_type": "class",
      "description": "...",
      "properties": {}
    }
  ],
  "edges": [
    {
      "source_id": "uuid5",
      "target_id": "uuid5",
      "relationship_type": "CALLS",
      "properties": {}
    }
  ],
  "extraction_duration_s": 0.123,
  "timestamp": "2026-04-03T02:00:00Z"
}
```

## Processing Flow

### Per Batch

For each micro-batch of extraction events:

1. **Group by company_id** — Multi-tenant database routing
2. **Ensure Neo4j database** — `ensure_neo4j_database(company_id)` (idempotent)
3. **Initialize constraints** — APOC-based `__Node__` constraints (idempotent)
4. **Aggregate data** — Collect all entities, edges, chunks from batch
5. **Write to Neo4j** (6 phases):
   - Phase 1: EntityType nodes
   - Phase 2: Entity nodes
   - Phase 3: DocumentChunk nodes
   - Phase 4: LLM-extracted edges
   - Phase 5: `contains` edges (DocumentChunk → Entity)
   - Phase 6: `is_a` edges (Entity → EntityType)
6. **Update counters** — Increment `chunks_processed`, `nodes_created`, `edges_created` in Postgres

### Writer Methods (Unchanged)

The writer already uses UNWIND for efficient batching:

- `write_entity_type_nodes(entity_types, database)` — Accepts list of `{"name": "class"}`
- `write_entity_nodes(entities, database)` — Accepts list of entity dicts
- `write_chunk_nodes(chunks, database)` — Accepts list of chunk dicts
- `write_llm_edges(edges, database)` — Accepts list of edge dicts
- `write_contains_edges(mappings, database)` — Accepts list of `{"chunk_id": ..., "entity_id": ...}`
- `write_is_a_edges(entities, database)` — Accepts list of entity dicts (extracts entity_type)

All methods use `UNWIND` and batch internally at `NEO4J_BATCH_SIZE` (default 500).

## Multi-Tenancy

Each company gets its own Neo4j database:

- Database name: `cognee-{company_id}`
- Auto-created on first write via `ensure_neo4j_database()`
- All queries route to the correct database via `database=` parameter

## Graceful Shutdown

On SIGINT/SIGTERM:

1. Stop accepting new Kafka messages
2. Drain work queue into final batch
3. Wait for collector to finish
4. Wait for all batch workers to complete
5. Stop Kafka producer and consumer
6. Close Neo4j driver
7. Close Postgres pool

## Testing

Run tests:

```bash
cd neo4j_storage_service
python -m pytest tests/ -v
```

All 37 tests pass (including new tests for `ExtractedEntitiesEvent`, `EntityPayload`, `EdgePayload`).

## Performance

### Throughput
- **Before:** Write entire ingestion in one batch after all 3 gates complete (~10-60s latency)
- **After:** Write micro-batches as soon as entity extraction completes (~0.5-2s latency)

### Scalability
- Horizontal: Increase `NEO4J_STORAGE_WORKERS` for more parallelism
- Batch size: Tune `NEO4J_STORAGE_BATCH_SIZE` and `NEO4J_BATCH_SIZE` for optimal throughput

### Backpressure
- Work queue size: `WORKERS * 2` (default 20)
- Batch queue size: `WORKERS` (default 10)
- Kafka consumer auto-commits after successful batch processing

## Migration from Gate-Based

### Removed
- `REQUIRED_SIGNALS` — No longer needed
- `_received_signals` — Removed signal accumulation
- `_processed_ingestions` — No longer tracking completion per ingestion
- `_handle_event()` — Removed gate logic
- `_execute_batch_write()` — Removed Postgres batch read
- Paginated `_write_*_nodes()` and `_write_*_edges()` methods — Now handled by writer

### Added
- Two-stage pipeline: work_queue → collector → batch_queue → workers
- `_collector_loop()` — Builds maximally-full batches
- `_batch_worker_loop()` — Processes micro-batches in parallel
- `_process_batch()` — Writes batch to Neo4j and updates counters
- `ExtractedEntitiesEvent`, `EntityPayload`, `EdgePayload` models

### Unchanged
- Writer class — Already Cognee-compatible with UNWIND batching
- Multi-tenancy setup — Same database-per-company pattern
- Pipeline store — Still used for counter tracking
- Config structure — Extended with streaming settings

## Troubleshooting

### No data in Neo4j
- Check `extracted-entities` topic has messages: `kafka-console-consumer --topic extracted-entities`
- Check consumer group offset: `kafka-consumer-groups --group neo4j-storage-streaming --describe`
- Check service logs for errors: `docker logs -f neo4j-storage`

### Slow writes
- Increase `NEO4J_STORAGE_WORKERS` (default 10)
- Increase `NEO4J_BATCH_SIZE` (default 500) for larger UNWIND batches
- Check Neo4j query performance: `CALL db.stats.retrieve('QUERIES')`

### OOM errors
- Decrease `NEO4J_STORAGE_BATCH_SIZE` (micro-batch size)
- Decrease `NEO4J_BATCH_SIZE` (UNWIND batch size)
- Increase worker memory limits in docker-compose

### Constraint errors
- Ensure APOC is enabled in Neo4j: `dbms.security.procedures.unrestricted=apoc.*`
- Check Neo4j logs: `docker logs neo4j`
- Verify `__Node__` constraint exists: `SHOW CONSTRAINTS`

## References

- Entity Extraction Service: `entity_extraction_service/`
- Embedding Service (streaming pattern reference): `embedding_service/consumer.py`
- Cognee Migration Doc: `COGNEE_MIGRATION.md`
- Writer Implementation: `writer.py`
- Neo4j Verification Queries: `VERIFICATION_QUERIES.cypher`

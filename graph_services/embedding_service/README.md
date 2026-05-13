# kgrag-embedding

Kafka worker that computes vector embeddings for extracted entities, entity types, triplets, edge types, and text summaries, then emits the vectors downstream for Qdrant storage.

## What it does

The embedding service is Service 4 in the v2 ingestion pipeline. It consumes structured entity and summary events from the entity extraction and summarization services, generates vector embeddings using the configured embedding model (Gemini by default), and publishes embedding-ready events to Qdrant storage.

Unlike document chunks (which are embedded by `document_preprocessor` before reaching Kafka), code entities and summaries pass through this dedicated embedding service. This ensures that all entity names, relationship triplets (e.g., `ClassName-›uses-›FunctionName`), entity types (e.g., `class`, `function`), edge types (e.g., `calls`, `inherits`), and summaries receive embeddings in a consistent format and dimension (3072-d for Gemini).

The service performs content-hash-based deduplication to skip re-embedding identical texts across ingestion runs, batches embedding API calls (default batch size: 100), enforces API concurrency limits (default: 20), and tracks progress in Postgres for pipeline completion detection.

## Where it lives

- Source: `graph_services/embedding_service/`
- Dockerfile: Built from `docker/Dockerfile.ingestion` as part of `kgrag-ingestion:latest` (shared with entity extraction, summarization, and storage services)
- Compose service: `kgrag-embedding` (line 554-608 in `docker/docker-compose.yml`)
- Entrypoint: `python -m embedding_service.main` (line 557 in `docker/docker-compose.yml`)

## Inputs

### Kafka topics consumed

- **`extracted-entities`** (from `entity_extraction_service`) — schema: `ExtractedEntitiesEvent` (config line 18-19)
  - Contains `entities[]` (with `entity_id`, `name`, `entity_type`, `description`) and `edges[]` (with `source_id`, `target_id`, `relationship_type`)
  - Consumer group: `embedding-processor-v2` (config line 21-23)
  
- **`text-summaries`** (from `summarization_service`) — schema: `TextSummaryEvent` (config line 18-19)
  - Contains `summary_text`, `summary_id`, `chunk_id`
  - Consumer group: `embedding-processor-v2` (config line 21-23)

### Environment variables

**Required:**
- `KAFKA_BOOTSTRAP_SERVERS` — Kafka broker address (default: `redpanda:9092`)
- `CODE_PROCESSING_POSTGRES_DSN` — Postgres connection string for checkpoint storage and counter tracking
- `EMBEDDING_PROVIDER` — Embedding model provider (default: `gemini`) — used by Cognee's `LiteLLMEmbeddingEngine`
- `EMBEDDING_MODEL` — Embedding model identifier (default: `gemini/gemini-embedding-001`, compose line 569)
- `GEMINI_API_KEY` — API key for Gemini embedding calls (compose line 565)

**Optional tuning:**
- `EMBEDDING_BATCH_SIZE` — Texts per embedding API call (default: `100`, config line 35)
- `EMBEDDING_WORKERS` — Number of parallel batch workers (default: `50`, config line 36)
- `EMBEDDING_API_CONCURRENCY` — Max concurrent embedding API calls (default: `20`, config line 37)
- `EMBEDDING_MAX_RETRIES` — Retry attempts per batch on API failure (default: `3`, config line 40)
- `EMBEDDING_RETRY_BASE_DELAY` — Initial retry delay in seconds, exponential backoff (default: `2.0`, config line 41)
- `EMBEDDING_COLLECT_TIMEOUT` — Max wait time to fill a batch before processing (default: `0.5`s, config line 38)

**Cognee infrastructure (for embedding engine):**
- `DB_PROVIDER=postgres`, `DB_HOST=postgres`, `DB_PORT=5432`, `DB_NAME=cognee`, `DB_USERNAME`, `DB_PASSWORD` — Cognee's internal database for embedding engine initialization (compose lines 579-584)
- `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` — Required by Cognee configuration (compose lines 575-577)
- `VECTOR_DB_PROVIDER=qdrant`, `VECTOR_DB_URL` — Required by Cognee configuration (compose lines 572-573)

## Outputs

### Kafka topics produced

- **`embeddings-ready`** (config line 31) — schema: `EmbeddingReadyEvent` (`models.py:123-161`)
  - Contains array of `EmbeddingPayload` records (`models.py:100-121`)
  - Each payload has: `source_id`, `source_type` (`"entity"`, `"summary"`, `"triplet"`, `"edge_type"`), `text`, `embedding` (float vector), plus type-specific metadata
  - Consumed by: `qdrant_storage_service` (for writing to Qdrant)

- **`pipeline-events`** (config line 32) — schema: `PipelineEvent` (`models.py:167-185`)
  - Completion signal with `event_type="embedding_complete"`, `ingestion_id`, `entities_received`, `summaries_received`, `embeddings_computed`
  - Emitted once per ingestion when all upstream entities + summaries have been embedded (completion check: `processor.py:537-571`)

### Vector dimensions

- **3072-d** by default (Gemini `text-embedding-004` dimension, compose line 571: `EMBEDDING_DIMENSIONS=3072`)
- **CRITICAL:** Must match the Qdrant collection schema configured in `qdrant_storage_service` — dimension mismatch will cause write failures

### Postgres writes

The service does NOT bulk-write embeddings to Postgres. Embeddings are published directly to Kafka (`embeddings-ready` topic) and stored to Qdrant by `qdrant_storage_service`.

Postgres writes are limited to:
- **`code_processing.pipeline_checkpoints`** — content-hash deduplication (`pipeline_store.py:90-119`)
- **`code_processing.cogni_ingestion_stats`** — counter tracking for `entities_received`, `summaries_received`, `embeddings_computed`, `triplets_received`, `edge_types_received`, `entity_types_received` (`pipeline_store.py:159-232`)
- **`code_processing.pipeline_errors`** — error logging (`pipeline_store.py:234-266`)

## Dependencies

- **Kafka (Redpanda)**: Must be reachable at `KAFKA_BOOTSTRAP_SERVERS`; topics `extracted-entities` and `text-summaries` must exist and be populated by upstream services
- **Postgres**: Must contain schema `code_processing` with tables `pipeline_checkpoints`, `cogni_ingestion_stats`, `pipeline_errors` (created by `pipeline_store.py:25-68`)
- **Cognee `cognee` DB**: Must exist in Postgres (created by `postgres-init` compose service, line 45-70 in docker-compose)
- **Upstream services**:
  - `entity_extraction_service` — publishes to `extracted-entities` (must complete before embedding completion)
  - `summarization_service` — publishes to `text-summaries` (must complete before embedding completion)
- **Embedding API**: Gemini API must be reachable with valid `GEMINI_API_KEY` — rate limits apply (free tier: 1500 RPM for `text-embedding-004`)

## How to run and smoke-test in isolation

### 1. Build the image

```bash
# From /Users/yubozhenko/legion-space/backend-services/
docker build -f docker/Dockerfile.ingestion -t kgrag-ingestion:latest .
```

### 2. Ensure dependencies are running

```bash
docker compose up -d postgres postgres-init redpanda
```

### 3. Seed a test event

Publish a minimal `ExtractedEntitiesEvent` to `extracted-entities`:

```bash
docker exec -i kgrag-redpanda rpk topic produce extracted-entities <<EOF
{
  "ingestion_id": "test-ingestion-001",
  "chunk_id": "test-chunk-001",
  "company_id": "test-company-001",
  "project_id": "test-project-001",
  "content_type": "code",
  "file_version_id": "test-file-version-001",
  "repository": "test-repo",
  "branch": "main",
  "entities": [
    {"entity_id": "entity-001", "name": "TestClass", "entity_type": "class", "description": "A test class"}
  ],
  "edges": [],
  "timestamp": "2026-05-13T12:00:00Z"
}
EOF
```

### 4. Start the embedding service

```bash
docker compose up kgrag-embedding
```

### 5. Check logs for processing

```bash
docker compose logs -f kgrag-embedding | grep -E "entity_batch_complete|embeddings_computed"
```

Expected output:
```
processor.entity_batch_complete events=1 total_entities=1 embedded_entities=1 duration_s=0.234
consumer.embedding_complete ingestion_id=test-ingestion-001 total_embeddings=1
```

### 6. Verify output on Kafka

```bash
docker exec -i kgrag-redpanda rpk topic consume embeddings-ready --num 1
```

Expected JSON with:
- `embeddings[0].source_id` = `"entity-001"`
- `embeddings[0].source_type` = `"entity"`
- `embeddings[0].text` = `"TestClass"`
- `embeddings[0].embedding` = `[0.123, 0.456, ..., 0.789]` (3072 floats)

## Operational notes

### Batch size tuning

- **Default: 100 texts per embedding API call** (`EMBEDDING_BATCH_SIZE`, config line 35)
- Gemini's `text-embedding-004` supports batch embedding (up to 100 texts per request per LiteLLM docs)
- Higher batch size = fewer API calls, but slower recovery on failure
- Lower batch size = faster feedback, higher API call overhead

### Concurrency limits

- **Default: 20 concurrent embedding API calls** (`EMBEDDING_API_CONCURRENCY`, config line 37)
- Enforced via asyncio semaphore (`processor.py:58`)
- Must respect Gemini API rate limits (free tier: **1500 RPM**, paid tier varies)
- If you see `429` rate limit errors in logs, reduce `EMBEDDING_API_CONCURRENCY` or `EMBEDDING_WORKERS`

### Worker count

- **Default: 50 parallel batch workers** (`EMBEDDING_WORKERS`, config line 36)
- Each worker drains batches from the collector and calls `_embed_texts_batched` (`consumer.py:250-282`)
- Worker count > API concurrency is fine — semaphore will gate actual API calls
- High worker count speeds up Postgres writes and Kafka publishing, not just embedding calls

### Collector behavior

- The collector builds maximally-full batches from a work queue (`consumer.py:190-248`)
- Collects up to `EMBEDDING_BATCH_SIZE` items (default: 100)
- Waits up to `EMBEDDING_COLLECT_TIMEOUT` (default: 0.5s) to fill a batch before releasing it
- On shutdown, drains remaining items and pushes a final partial batch

### Vector dimension constraint

- **CRITICAL:** The embedding dimension **MUST match** the Qdrant collection schema
- Gemini `text-embedding-004` outputs 3072-d vectors (compose line 571: `EMBEDDING_DIMENSIONS=3072`)
- If you change `EMBEDDING_MODEL` to a different model (e.g., OpenAI `text-embedding-3-small` = 1536-d), you **MUST**:
  1. Update `EMBEDDING_DIMENSIONS` env var
  2. Recreate or update Qdrant collections in `qdrant_storage_service` to match the new dimension
  3. Consider re-embedding all existing data (dimension mismatch = Qdrant write failure)

### Deduplication and idempotency

- Content-hash checkpointing (`pipeline_store.py:90-119`) prevents re-embedding identical texts across ingestion runs
- If an entity name or summary text has not changed since last ingestion, its embedding is skipped
- Checkpoint key: `(source_id, operation="embedding", content_hash)`
- On `force_full_refresh=true` ingestion, checkpoints are ignored (new content hash → re-embed)

### Triplet and edge_type embedding

- **Triplets**: For each edge in `ExtractedEntitiesEvent`, a triplet text is generated: `"{source_name}-›{relationship_type}-›{target_name}"` (using Unicode `›` character, `processor.py:132`)
- **Triplet ID**: deterministic UUID5 from `"{source_id}{relationship_type}{target_id}"` normalized (lowercase, underscores, no apostrophes) — matches Cognee's `generate_node_id` (`processor.py:135-136`)
- **Edge types**: Aggregated per batch (all edges with the same `relationship_type` are counted), one `edge_type` embedding per distinct relationship type (`processor.py:156-176`)
- **Entity types**: Aggregated per batch (all entities with the same `entity_type` are counted), one `entity_type` embedding per distinct entity type (`consumer.py:376-395`)

### Completion detection

- The service checks `pipeline_events` topic for `entity_extraction_complete` and `summarization_complete` events (stored in `code_processing.cogni_ingestion_stats`)
- Once both upstream services have completed AND `entities_received + summaries_received >= upstream totals`, the service emits `embedding_complete` to `pipeline-events` (`consumer.py:565-611`)
- Qdrant and Neo4j storage services use this signal to determine when to finalize ingestion metadata

### Rate limits and retry

- **Gemini free tier**: 1500 RPM for `text-embedding-004` (as of 2024)
- If you exceed the rate limit, the embedding engine will raise a `RateLimitError`
- The service retries with exponential backoff (3 retries, base delay 2.0s) (`processor.py:492-533`)
- Backoff delays: 2s, 4s, 8s (total ~14s before failure)
- If retries are exhausted, the batch is logged to `pipeline_errors` and skipped (no infinite loop)

### Memory and resource limits

- **Default compose limits**: 2G max memory, 512M reservation (compose lines 597-602)
- Memory usage scales with:
  - Number of workers × batch size × vector dimension × float size (50 workers × 100 texts × 3072 floats × 4 bytes = ~60 MB for in-flight embeddings)
  - Postgres connection pool (default: 2-10 connections, config lines 48-49)
- High worker count + large batch size can cause OOM on machines with <4GB available memory
- Reduce `EMBEDDING_WORKERS` or `EMBEDDING_BATCH_SIZE` if you see OOM kills

### Error handling

- Invalid messages (missing `ingestion_id`, empty `entities`/`summary_text`) are logged and skipped (`consumer.py:164-171`)
- Embedding API failures after retries are logged to `pipeline_errors` table and do not block other batches (`processor.py:531-533`)
- Checkpointing failures are logged but do not block embedding (deduplication degrades gracefully)

## Code map

```
embedding_service/
├── main.py              # Entrypoint: configures Cognee, initializes Postgres pool, starts consumer/processor
│   ├── L54: config.validate() — env var validation
│   ├── L66: configure_cognee() — sets up Cognee embedding engine settings
│   ├── L71-76: asyncpg.create_pool() — Postgres connection pool
│   ├── L79-81: EmbeddingStore.ensure_tables() — create checkpoint/counter tables
│   ├── L84-94: EmbeddingProcessor + EmbeddingConsumer initialization
│   └── L97-99: consumer.start() + consumer.consume() — main loop
│
├── config.py            # Environment variable configuration
│   ├── L18-20: KAFKA_INPUT_TOPICS — consumed topics (extracted-entities, text-summaries)
│   ├── L31: KAFKA_OUTPUT_TOPIC — embeddings-ready
│   ├── L35: EMBEDDING_BATCH_SIZE — texts per API call (default: 100)
│   ├── L36: EMBEDDING_WORKERS — parallel batch workers (default: 50)
│   └── L37: EMBEDDING_API_CONCURRENCY — max concurrent API calls (default: 20)
│
├── consumer.py          # Kafka consumer with two-stage pipeline (collector → workers)
│   ├── L76-92: AIOKafkaConsumer + AIOKafkaProducer initialization
│   ├── L140-188: consume() — main Kafka fetch loop, feeds work_queue
│   ├── L190-248: _collector_loop() — builds maximally-full batches from work_queue
│   ├── L250-281: _batch_worker_loop() — parallel workers that embed batches
│   ├── L283-289: _topic_to_source_type() — maps topic name to "entity" or "summary"
│   ├── L291-415: _extract_items() — parses Kafka message into embeddable items (entities, triplets, edge_types, entity_types, summaries)
│   ├── L417-563: _process_batch() — embed batch → publish → check completion
│   └── L565-611: _check_and_emit_completion() — emit embedding_complete when done
│
├── processor.py         # Embedding logic: batching, deduplication, retry
│   ├── L61-69: _get_engine() — lazy-init LiteLLMEmbeddingEngine from Cognee
│   ├── L73-351: process_entity_batch() — embed entities + triplets + edge_types + entity_types
│   │   ├── L94-110: collect entity names
│   │   ├── L113-151: generate triplet texts ("{source}-›{rel}-›{target}")
│   │   ├── L156-176: aggregate EdgeType items (one per relationship_type)
│   │   ├── L191-218: checkpoint deduplication (content hash check)
│   │   ├── L231-232: _embed_texts_batched() call
│   │   └── L267-350: build EmbeddingReadyEvent per event + batch-level edge_type event
│   ├── L355-467: process_summary_batch() — embed summary texts
│   ├── L471-490: _embed_texts_batched() — split into sub-batches, parallel embed
│   ├── L492-533: _embed_with_retry() — call embedding engine with exponential backoff
│   └── L537-571: check_ingestion_complete() — completion detection logic
│
├── models.py            # Pydantic schemas
│   ├── L21-59: EntityInputEvent — consumed from extracted-entities
│   ├── L62-94: SummaryInputEvent — consumed from text-summaries
│   ├── L100-121: EmbeddingPayload — single embedding record (source_id, text, vector)
│   ├── L123-161: EmbeddingReadyEvent — published to embeddings-ready
│   └── L167-185: PipelineEvent — completion signal (embedding_complete)
│
└── pipeline_store.py    # Postgres storage: checkpoints, counters, errors
    ├── L25-68: ensure_tables() — CREATE TABLE IF NOT EXISTS
    ├── L90-119: save_checkpoint() / has_checkpoint() / check_checkpoint() — deduplication
    ├── L159-232: increment_counter() / get_all_counters() / finalize_counters() — progress tracking
    ├── L234-266: log_error() — error logging
    └── L268-286: is_upstream_complete() / get_upstream_total() — completion detection
```

**Key control flow:**
1. `main.py` starts `consumer.consume()` loop
2. `consumer.consume()` fetches Kafka messages → feeds `work_queue`
3. `_collector_loop()` builds maximally-full batches → pushes to `batch_queue`
4. 50 parallel `_batch_worker_loop()` workers drain `batch_queue`
5. Each worker calls `_process_batch()` → `processor._embed_texts_batched()` → Gemini API
6. Embeddings published to `embeddings-ready` topic
7. Completion check → emit `embedding_complete` to `pipeline-events` when done

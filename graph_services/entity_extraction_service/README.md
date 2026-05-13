# kgrag-entity-extraction

Kafka worker that extracts entities and edges from preprocessed code and document chunks using LLM calls.

## What it does

Consumes enriched chunks from the **`enriched-code-chunks`** Kafka topic (produced by both `code_preprocessor` and `document_preprocessor`), calls an LLM (Gemini 2.0 Flash Lite via LiteLLM) to extract structured entities and relationships, validates and repairs the output, then publishes results to **`extracted-entities`**.

Per chunk:
1. Validates incoming `EnrichedChunkMessage` (requires `company_id`, `chunk_id`, `content`; `project_id` required for code chunks)
2. Sets multi-tenant Neo4j database context (`cognee-{company_id}`)
3. Checks content-hash checkpoint to skip re-processing identical chunks
4. Builds LLM input from chunk metadata + content (file path, language, chunk text)
5. Calls `cognee.infrastructure.llm.extraction.extract_content_graph()` with content-type-aware prompt:
   - Code chunks: `code_graph_extraction_prompt.txt`
   - Document chunks: `document_graph_extraction_prompt.txt`
6. Repairs extraction output (short descriptions, bad edge endpoints, etc.) via `repair_extraction_result()` — repair-and-accept policy, no rejection on quality issues
7. Converts repaired `KnowledgeGraph` to `EntityPayload` / `EdgePayload` with deterministic UUID5 IDs
8. Publishes `ExtractedEntitiesEvent` to **`extracted-entities`** topic
9. Emits `extraction_complete` to **`pipeline-events`** when all chunks for an ingestion are processed

**Input topic**: `enriched-code-chunks` (consumed by consumer group `entity-extraction-processor-v2`)  
**Output topics**: `extracted-entities`, `pipeline-events`

**Kafka topic flow** (from engagement entry `f526b6a1-64c3-4596-8e5c-441b8c1af785`):
- `incoming_requests` → `code_preprocessor` → `enriched-code-chunks` → **entity_extraction_service** → `extracted-entities` → `embedding_service` → `embeddings-ready` → `qdrant_storage_service`
- `brain_events` → `document_preprocessor` → `enriched-code-chunks` → **entity_extraction_service** → `extracted-entities` → `embedding_service`
- **entity_extraction_service** also publishes to `pipeline-events` for completion tracking

## Where it lives

- **Dockerfile**: `Dockerfile.ingestion` at `/Users/yubozhenko/legion-space/backend-services/Dockerfile.ingestion:28` — shared multi-service image (`kgrag-ingestion:latest`) serving all v2 pipeline workers
- **Source folder**: `graph_services/entity_extraction_service/`
- **Image name**: `kgrag-ingestion:latest`
- **Container name**: `kgrag-entity-extraction`
- **Compose service**: `kgrag-entity-extraction` in `docker/docker-compose.yml:434-492`
- **Entrypoint**: `python -m entity_extraction_service.main` (overrides default CMD in Dockerfile)
- **No exposed ports** — background Kafka worker, no REST/gRPC endpoint

## Inputs

### Kafka Topics Consumed

**Topic**: `enriched-code-chunks` (config: `entity_extraction_service/config.py:18-19`)  
**Consumer group**: `entity-extraction-processor-v2` (config: `entity_extraction_service/config.py:22`)

**Message schema** (from `cognee_service/kafka_consumer/enriched_chunks/models.py` — `EnrichedChunkMessage`):
```python
{
  "action": "process",  # Required; skips non-process actions
  "company_id": "uuid",  # Required
  "project_id": "uuid",  # Required for code chunks; absent/None for documents
  "chunk_id": "uuid",  # Required
  "ingestion_id": "uuid",  # Required for completion tracking
  "file_version_id": "uuid",  # Code only
  "file_path": "src/main.py",  # Code only
  "repository": "org/repo",  # Code only
  "branch": "main",  # Code only
  "language": "python",  # Code only
  "content_type": "code",  # or "document"
  "document_title": "Knowledge Title",  # Document only
  "document_slug": "knowledge-title",  # Document only
  "start_line": 42,  # Code only
  "end_line": 58,  # Code only
  "chunk_index": 0,  # 0-based chunk index
  "content": "def extract_entities(...):\n    ...",  # Required
  "embedding": [0.123, ...],  # Optional; not used by this service
  "extraction_prompt": "Extract entities...",  # Optional; message-level prompt override
  "expected_total_chunks": 100  # For completion tracking
}
```

### Environment Variables

**Kafka** (required):
- `KAFKA_BOOTSTRAP_SERVERS` — Kafka broker address (default: `redpanda:9092`)
- `ENTITY_EXTRACTION_INPUT_TOPIC` — Input topic (default: `enriched-code-chunks`)
- `ENTITY_EXTRACTION_OUTPUT_TOPIC` — Output topic (default: `extracted-entities`)
- `ENTITY_EXTRACTION_CONSUMER_GROUP_ID` — Consumer group (default: `entity-extraction-processor-v2`)
- `PIPELINE_EVENTS_TOPIC` — Completion events topic (default: `pipeline-events`)

**Postgres** (required for checkpoint/dedup):
- `CODE_PROCESSING_POSTGRES_DSN` — Connection string to `kgrag_auth` DB (contains `code_processing.entity_extraction_checkpoints` and `cogni_ingestion_stats` tables)

**LLM** (required):
- `LLM_PROVIDER` — LiteLLM provider (default: `gemini`)
- `LLM_MODEL` — Model name (default: `gemini/gemini-2.0-flash-lite`)
- `LLM_API_KEY` or `GEMINI_API_KEY` — API key for LLM provider
- `LLM_API_VERSION` — API version (default: `v1beta` for Gemini)
- `LLM_INSTRUCTOR_MODE` — Structured output mode (default: `json_mode`)
- `LLM_RATE_LIMIT_ENABLED` — Enable rate limiting (default: `true`)
- `LLM_RATE_LIMIT_REQUESTS` — Requests per interval (default: `1500`)
- `LLM_RATE_LIMIT_INTERVAL` — Rate limit window in seconds (default: `60`)

**Embedding** (for Cognee configuration, not directly used):
- `EMBEDDING_PROVIDER` — (default: `gemini`)
- `EMBEDDING_MODEL` — (default: `gemini/gemini-embedding-001`)
- `EMBEDDING_DIMENSIONS` — (default: `3072`)
- `EMBEDDING_BATCH_MODE` — (default: `true`)

**Qdrant / Neo4j** (for Cognee configuration, not directly used by extraction logic):
- `VECTOR_DB_PROVIDER` — (default: `qdrant`)
- `VECTOR_DB_URL` — Qdrant URL (default: `http://qdrant:6333`)
- `QDRANT_API_KEY` — Optional Qdrant API key
- `NEO4J_URI` — Neo4j Bolt URI (default: `bolt://neo4j:7687`)
- `NEO4J_USERNAME` — Neo4j user (default: `neo4j`)
- `NEO4J_PASSWORD` — Neo4j password (required)
- `COGNEE_NEO4J_DATABASE` — Database name for metadata (default: `cognee`)

**Cognee Postgres DB** (for Cognee library internal tables):
- `DB_PROVIDER` — (default: `postgres`)
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD`

**Processing tuning**:
- `ENTITY_EXTRACTION_MAX_WORKERS` — Max parallel LLM calls (default: `50`; compose default: `30`)
- `ENTITY_EXTRACTION_BATCH_SIZE` — (default: `50`; not actively used in streaming architecture)
- `ENTITY_EXTRACTION_MAX_RETRIES` — LLM call retries (default: `3`)
- `ENTITY_EXTRACTION_RETRY_BASE_DELAY` — Exponential backoff base in seconds (default: `2.0`)

**Misc**:
- `LOG_LEVEL` — Logging verbosity (default: `INFO`)
- `TELEMETRY_DISABLED` — Disable Cognee telemetry (default: `true`)
- `PYTHONUNBUFFERED` — (default: `1`)

## Outputs

### Kafka Topics Produced

**Topic 1**: `extracted-entities` (config: `entity_extraction_service/config.py:35-37`)

**Message schema** (`ExtractedEntitiesEvent` from `entity_extraction_service/models.py:81-146`):
```python
{
  "event_id": "uuid",  # Generated per event
  "ingestion_id": "uuid",  # Passed through from input
  "chunk_id": "uuid",  # Source chunk ID
  "company_id": "uuid",  # Multi-tenancy scope
  "project_id": "uuid",  # None for company-level documents
  "file_version_id": "uuid",  # From input
  "file_path": "src/main.py",  # From input
  "repository": "org/repo",  # From input
  "branch": "main",  # From input
  "language": "python",  # From input
  "content_type": "code",  # or "document"
  "document_title": "Knowledge Title",  # Document only
  "document_slug": "knowledge-title",  # Document only
  "start_line": 42,  # From input
  "end_line": 58,  # From input
  "chunk_index": 0,  # From input
  "chunk_text": "def extract_entities(...):\n    ...",  # Raw chunk content
  "entities": [
    {
      "entity_id": "uuid5",  # Deterministic UUID5 from name + node_set
      "name": "extract_entities",
      "entity_type": "function",
      "description": "Extracts entities from code chunks",
      "properties": {}
    }
  ],
  "edges": [
    {
      "source_id": "uuid5",
      "target_id": "uuid5",
      "relationship_type": "calls",
      "source_name": "extract_entities",
      "target_name": "repair_extraction_result",
      "properties": {}
    }
  ],
  "extraction_duration_s": 1.234,
  "timestamp": "2026-05-13T12:34:56.789Z"
}
```

**Topic 2**: `pipeline-events` (config: `entity_extraction_service/config.py:38`)

**Message schema** (`PipelineEvent` from `entity_extraction_service/models.py:151-171`):
```python
{
  "event_type": "extraction_complete",
  "ingestion_id": "uuid",
  "company_id": "uuid",
  "project_id": "uuid",  # None for company-level documents
  "chunks_processed": 123,
  "total_entities": 456,
  "total_edges": 789,
  "timestamp": "2026-05-13T12:34:56.789Z"
}
```

### Database Writes (Postgres)

**Schema**: `code_processing` in `kgrag_auth` database

**Table 1**: `code_processing.entity_extraction_checkpoints`  
Created by: `entity_extraction_service/pipeline_store.py:61-68`  
Purpose: Content-hash deduplication to skip re-processing identical chunks  
Columns: `ingestion_id`, `chunk_id`, `content_hash`, `timestamp`

**Table 2**: `code_processing.cogni_ingestion_stats`  
Created by: `entity_extraction_service/pipeline_store.py:163-180`  
Purpose: Completion tracking + counter storage (chunks received, entities extracted, edges extracted)  
Updated via: `increment_counter()`, `finalize_counters()`, `get_all_counters()` in `pipeline_store.py`

## Dependencies

**Infrastructure** (required before start):
- `redpanda` — Kafka broker for topic consumption/production
- `postgres` — Stores checkpoints and ingestion stats in `kgrag_auth` DB (schema `code_processing`)
- `postgres-init` — Ensures `cognee` DB exists (required by Cognee library for internal tables)

**External APIs** (required at runtime):
- Gemini API (or other LiteLLM-compatible LLM provider) — for `extract_content_graph()` calls

**Consumed by** (downstream):
- `embedding_service` — subscribes to `extracted-entities` + `text-summaries`, embeds entities + summaries, publishes to `embeddings-ready`
- `neo4j_storage_service` — subscribes to `extracted-entities` + `text-summaries` + `enriched-code-chunks`, writes graph nodes/edges to Neo4j `cognee-{company_id}` database

**Metrics/monitoring** (optional):
- `pipeline-events` topic carries completion events for observability

## How to run and smoke-test in isolation

### Start the service

```bash
docker compose up kgrag-entity-extraction -d
```

### Check startup logs

```bash
docker compose logs -f kgrag-entity-extraction
```

**Expected log sequence**:
```
main.starting            input_topic=enriched-code-chunks output_topic=extracted-entities
main.configuring_cognee
main.cognee_configured
main.connecting_postgres dsn=postgresql://kgrag:***...
main.postgres_connected
main.tables_ensured
consumer.starting        group=entity-extraction-processor-v2 max_workers=30
consumer.started         workers=30 queue_size=30
main.consumer_ready
```

**Success indicators**:
- `consumer.started` with `workers=30` (or configured worker count)
- No `fatal_error` or `postgres_connect_error`

### Verify Kafka consumer group is active

```bash
docker exec -it kgrag-redpanda rpk group describe entity-extraction-processor-v2
```

**Expected output**:
- State: `Stable`
- Members: 1 (container name should appear)
- Lag: depends on whether there are pending messages

### Trigger ingestion to produce test chunks

**For code ingestion**:
```bash
curl -X POST http://localhost:8000/api/v1/code_ingestion \
  -H "Authorization: Bearer <jwt_token>" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "project-uuid",
    "repository": "https://github.com/user/repo.git",
    "branch": "main",
    "force_full_refresh": true
  }'
```

**For document ingestion** (via Brain API):
```bash
curl -X POST http://localhost:8000/api/v1/brain \
  -H "Authorization: Bearer <jwt_token>" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": 2,
    "title": "Test Knowledge",
    "content": "This is a test document with some entities: FastAPI, PostgreSQL, Redis."
  }'
```

### Watch for extraction activity

```bash
docker compose logs -f kgrag-entity-extraction | grep 'worker.chunk_published\|extraction_complete'
```

**Expected output per chunk**:
```
worker.chunk_published   chunk_id=<uuid> entities=5 edges=3
worker.chunk_published   chunk_id=<uuid> entities=8 edges=6
...
consumer.extraction_complete ingestion_id=<uuid> chunks_processed=123 total_entities=456 total_edges=789
```

### Check output topic contains messages

```bash
docker exec -it kgrag-redpanda rpk topic consume extracted-entities --num 5
```

**Expected**: JSON events with `"entities": [...]` and `"edges": [...]`

### Common failure modes to check for

**No consumers joining group**:
- Symptom: `rpk group describe` shows `EMPTY` state
- Cause: Service crashed during startup or Kafka broker unreachable
- Fix: Check `docker compose logs kgrag-entity-extraction` for errors; ensure `redpanda` is healthy

**LLM rate limit errors**:
- Symptom: Logs show `worker.chunk_error` with `RateLimitError` or `429`
- Cause: Gemini API rate limits exceeded
- Fix: Reduce `ENTITY_EXTRACTION_MAX_WORKERS` (default: 30) or increase `LLM_RATE_LIMIT_INTERVAL`

**Postgres connection failures**:
- Symptom: `main.postgres_connect_error` or `pipeline_store.upsert_error`
- Cause: `CODE_PROCESSING_POSTGRES_DSN` incorrect or Postgres not ready
- Fix: Verify DSN, ensure `postgres` service healthy

**Cognee configuration errors**:
- Symptom: `cognee.setup_failed` or import errors
- Cause: Missing `DB_PROVIDER`, `NEO4J_URI`, `VECTOR_DB_URL` env vars
- Fix: Ensure all Cognee-required env vars set (see compose file `docker/docker-compose.yml:439-471`)

**No extraction output (entities: [], edges: [])**:
- Symptom: `worker.chunk_published` shows `entities=0 edges=0` for all chunks
- Cause: LLM prompt misconfigured, or content too short/trivial
- Fix: Check `ENTITY_EXTRACTION_PROMPT_PATH` exists and contains valid prompt; verify incoming chunk content is non-trivial

**Chunks skipped due to missing required fields**:
- Symptom: `consumer.missing_required_fields` warnings in logs
- Cause: Upstream `code_preprocessor` or `document_preprocessor` not populating `company_id`, `chunk_id`, `content`, or `project_id` (for code)
- Fix: Check upstream service logs; ensure `EnrichedChunkMessage` schema is fully populated

## Operational notes

### Consumer group and offsets

- **Consumer group**: `entity-extraction-processor-v2` (config: `entity_extraction_service/config.py:22`)
- **Offset reset**: `earliest` (config: `entity_extraction_service/config.py:28`) — on first start, processes all historical chunks
- **Auto-commit**: `true` (config: `entity_extraction_service/config.py:24-26`) — commits offsets automatically after processing

### Batch size vs. streaming architecture

- `BATCH_SIZE=50` (config: `entity_extraction_service/config.py:41`) is defined but **not actively used**
- The service uses a **streaming worker pool** architecture (`consumer.py:33-44`):
  - Kafka consumer feeds chunks into `asyncio.Queue(maxsize=MAX_PARALLEL_WORKERS)`
  - N persistent worker coroutines dequeue → extract → publish continuously
  - No batch boundaries — immediate per-chunk publication

### Idempotency and deduplication

- **Content-hash checkpointing** (processor.py:258-286):
  - On first extraction of a chunk, stores SHA256(content) in `entity_extraction_checkpoints` table
  - On re-ingestion (e.g., `force_full_refresh=true`), skips LLM call if content hash unchanged
  - Uses `(ingestion_id, chunk_id, content_hash)` as composite key
- **Deterministic entity IDs** (models.py:19-37):
  - Entity IDs are UUID5(name + node_set) — same entity extracted multiple times gets same ID
  - Prevents duplicate entity nodes in Neo4j downstream

### Retry and failure handling

- **LLM call retries** (config: `MAX_RETRIES=3`, `RETRY_BASE_DELAY=2.0`):
  - Retries on LLM call failures (network errors, rate limits, unparseable responses)
  - Uses exponential backoff: 2s, 4s, 8s
- **Validation repair policy** (processor.py:10-21):
  - LLM output is **repaired, never rejected** on quality issues (short descriptions, bad edge endpoints)
  - Only skipped if all LLM call attempts fail
- **Error logging** (consumer.py:245-251):
  - Per-chunk errors logged with `chunk_id` and `ingestion_id` but do not block other chunks
  - Failed chunks do not prevent `extraction_complete` emission

### Completion tracking

- `extraction_complete` event emitted to `pipeline-events` when:
  - All expected chunks for an ingestion are received (`chunks_received == expected_total_chunks`)
  - Tracked via `code_processing.cogni_ingestion_stats` table counters
- **Once-only guarantee** (consumer.py:57-58, 319-326):
  - In-memory set `_completed_ingestions` prevents duplicate completion events
  - Lock-protected to avoid race conditions in multi-worker setup

### Rate limiting

- **LLM rate limit** (env vars):
  - `LLM_RATE_LIMIT_ENABLED=true` (default)
  - `LLM_RATE_LIMIT_REQUESTS=1500` (default; compose default: 1500)
  - `LLM_RATE_LIMIT_INTERVAL=60` (default)
- **Semaphore backpressure** (processor.py:77):
  - `asyncio.Semaphore(MAX_PARALLEL_WORKERS)` limits concurrent LLM calls
  - Prevents overwhelming LLM API or Postgres connection pool

### Memory and resource limits (compose)

- **Memory limit**: 2G (compose: `deploy.resources.limits.memory`)
- **Memory reservation**: 512M
- **Max workers**: 30 (compose env `ENTITY_EXTRACTION_MAX_WORKERS=30`; service default: 50)

### Multi-tenancy

- **Per-company Neo4j database context** (consumer.py:265-274):
  - Before extraction, sets company context via `set_company_context(company_id)`
  - Ensures Neo4j database `cognee-{company_id}` exists via `ensure_neo4j_database()`
- **Scope-aware entity IDs** (models.py:19-42):
  - Entity UUIDs include `node_set` in ID generation (e.g., `{project_id}_{project_name}_code` or `{company_id}_knowledge`)
  - Same entity name in different scopes gets different UUIDs

### Healthcheck

**Compose healthcheck** (`docker/docker-compose.yml:487-492`):
```yaml
test: ["CMD-SHELL", "pgrep -f 'entity_extraction_service.main' || exit 1"]
interval: 30s
timeout: 10s
start_period: 60s
retries: 5
```

**Interpretation**:
- Checks if Python process running `entity_extraction_service.main` exists
- Does NOT verify Kafka consumer is active or processing messages
- For deeper health check, use `rpk group describe entity-extraction-processor-v2`

## Code map

### Core files

| File | Lines | Purpose |
|------|-------|---------|
| `main.py` | 124 | Entry point: initializes Cognee, Postgres pool, store, processor, consumer; runs event loop |
| `consumer.py` | 425 | Kafka consumer: getmany loop → validate → enqueue to work queue → N workers dequeue/extract/publish → emit completion |
| `processor.py` | 523 | LLM extraction logic: resolve prompt → dedup check → call `extract_content_graph()` → repair → convert to payloads |
| `config.py` | 79 | Environment variable mapping: Kafka topics, worker count, retry settings, prompt paths, Postgres DSN |
| `models.py` | 171 | Pydantic models: `EntityPayload`, `EdgePayload`, `ExtractedEntitiesEvent`, `PipelineEvent`; UUID5 ID generation |
| `pipeline_store.py` | ~300 | Postgres interface: checkpoint upsert, counter increment/finalize, completion check |
| `validation.py` | ~200 | Extraction result repair: fix short descriptions, canonicalize entity types, validate edge endpoints, assign UUIDs |
| `content_type_classifier.py` | ~50 | Content-type detection (code vs. document) based on message fields |
| `entity_resolver.py` | ~100 | Entity name normalization and canonical type resolution |

### Key dependencies (imported)

- `cognee.infrastructure.llm.extraction.extract_content_graph` — LLM extraction call (returns `KnowledgeGraph`)
- `cognee_service.kafka_consumer.enriched_chunks.models.EnrichedChunkMessage` — Input message schema
- `cognee_service.multi_tenancy.{ensure_neo4j_database, set_company_context}` — Multi-tenant DB setup
- `aiokafka.AIOKafkaConsumer`, `AIOKafkaProducer` — Async Kafka client
- `asyncpg` — Postgres async driver
- `structlog` — Structured logging

### Worker pool architecture (consumer.py:33-44)

```
Kafka → getmany() → validate → asyncio.Queue(maxsize=MAX_WORKERS)
                                      ↓
         ┌──────────────────────────────────────────┐
         │  N persistent worker coroutines          │
         │  (default: 30)                           │
         │                                          │
         │  loop:                                   │
         │    chunk = await queue.get()            │
         │    extract_entities(chunk)              │
         │    publish(extracted-entities)          │
         │    check_completion()                   │
         └──────────────────────────────────────────┘
```

### Extraction flow (processor.py:56-67)

1. Resolve extraction prompt (from message field, fallback to file-based)
2. Check content-hash checkpoint (skip if unchanged)
3. Build LLM input (header + content)
4. Call `extract_content_graph(prompt, text)` → `KnowledgeGraph`
5. Convert to `ExtractionResult` (intermediate repair format)
6. Repair: fix short descriptions, canonicalize types, validate edges
7. Convert repaired result to `EntityPayload` / `EdgePayload` with UUID5 IDs
8. Return `ExtractedEntitiesEvent` for Kafka publishing

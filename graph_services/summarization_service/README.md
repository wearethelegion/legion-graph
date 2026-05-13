# kgrag-summarization

Kafka worker that generates LLM-powered text summaries for enriched code chunks and document chunks.

## What it does

The **summarization service** is a parallel Kafka consumer that:

1. Consumes `EnrichedChunkMessage` from the `enriched-code-chunks` topic (produced by both `code_preprocessor` for code and `document_preprocessor` for documents)
2. For each chunk, extracts the file content (with optional header context) and calls Cognee's `summarize_text` LLM function to generate a natural-language summary
3. Uses content-hash based checkpointing to skip unchanged chunks on re-ingestion
4. Publishes `TextSummaryEvent` per chunk to the `text-summaries` output topic
5. Tracks progress via Postgres counters (`chunks_received`, `summaries_produced`) in the shared `pipeline_counters` table
6. Emits `summarization_complete` event to `pipeline-events` topic when all chunks for an ingestion are processed

**Architecture**: Streaming worker pool. Kafka consumer feeds an asyncio.Queue (bounded by `MAX_PARALLEL_WORKERS`), and N persistent worker coroutines pull chunks continuously, summarize them via LLM, and publish results immediately — no batch boundaries.

**Multi-tenancy**: Per-chunk, the worker calls `ensure_neo4j_database(company_id)` and `set_company_context(company_id)` before processing. Cognee's LLM client inherits the company context.

**Idempotency**: Content-hash based deduplication. If a chunk's content hasn't changed since the last ingestion, summarization is skipped (logged as `chunk_skipped` with reason `unchanged_content`). Checkpoint is saved in `code_processing.processing_checkpoints` AFTER successful LLM call, not before — critical to avoid permanent chunk loss on retry.

## Where it lives

- **Source**: `graph_services/summarization_service/`
- **Docker image**: `kgrag-ingestion:latest` (shared with all v2 pipeline services)
- **Container name**: `kgrag-summarization`
- **Entrypoint**: `python -m summarization_service.main` (`docker/docker-compose.yml:497`)
- **Consumer group**: `summarization-processor-v2` (`config.py:19-21`)

## Inputs

### Kafka topic consumed

- **Topic**: `enriched-code-chunks` (default; override via `SUMMARIZATION_INPUT_TOPIC`)
- **Schema**: `EnrichedChunkMessage` (`cognee_service/kafka_consumer/enriched_chunks/models.py`)
- **Action filter**: Only processes messages with `action == "process"` (`consumer.py:175-181`)
- **Producers**:
  - `code_preprocessor` (code chunks with `content_type="code"`)
  - `document_preprocessor` (document chunks with `content_type="document"`)

### Required env vars

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `redpanda:9092` | Kafka broker address |
| `CODE_PROCESSING_POSTGRES_DSN` | — | Postgres DSN for `kgrag_auth` DB (contains `code_processing` schema) |
| `LLM_PROVIDER` | `gemini` | LLM provider for summarization |
| `LLM_MODEL` | `gemini/gemini-2.0-flash-lite` | Model name for `summarize_text` |
| `LLM_API_KEY` | — | API key for LLM provider (via `GEMINI_API_KEY`) |
| `SUMMARIZATION_MAX_WORKERS` | `30` | Number of parallel summarization workers |

Full env list in `docker/docker-compose.yml:499-531`. Includes LLM rate-limiting, embedding settings (for Cognee init), Neo4j/Qdrant/Cognee DB credentials.

### LLM behavior

- Calls `cognee.tasks.summarization.summarize_text([chunk_obj])` — wraps Cognee's LLM client (`processor.py:243`)
- Input: file content text (optionally prefixed with header as context: "--- CONTEXT (for reference only) ---\nheader\n\n--- CODE CHUNK TO SUMMARIZE ---\ncontent") (`processor.py:186-193`)
- Timeout: 600s (`processor.py:243` — `asyncio.wait_for(..., timeout=600.0)`)
- Retry: Up to 3 attempts with exponential backoff (2s, 4s, 8s delay) on failure (`processor.py:140-154`, `config.py:39-40`)

## Outputs

### Kafka topics produced

| Topic | Schema | Consumer(s) | Purpose |
|---|---|---|---|
| `text-summaries` | `TextSummaryEvent` (`models.py:30-74`) | `embedding_service`, `neo4j_storage_service` | One event per chunk with `summary_text` field |
| `pipeline-events` | `PipelineEvent` (`models.py:79-100`) | Monitoring/coordination | Emitted once per ingestion when all chunks complete: `event_type="summarization_complete"` |

**Key fields in `TextSummaryEvent`**:
- `ingestion_id`, `chunk_id`, `company_id`, `project_id`, `file_version_id`, `file_path`
- `content_type`: `"code"` or `"document"` (from input message)
- `summary_text`: generated LLM summary
- `summary_id`: unique UUID for this summary record
- `summarization_duration_s`: time spent on LLM call

### Postgres writes

**Schema**: `code_processing` (inside `kgrag_auth` DB)

**Tables**:
1. `processing_checkpoints` — Content-hash based deduplication. PK: `(item_id, stage)`. Fields: `content_hash`, `ingestion_id`, `processed_at`. Stage = `"summarization"`. (`pipeline_store.py:43-52`)
2. `pipeline_counters` — Shared progress tracking table. Per ingestion, service tracks: `chunks_received`, `summaries_produced`. Status: `running` → `complete`. (`pipeline_store.py:55-71`)

**Critical ordering**: Checkpoint is saved AFTER successful LLM call + Kafka publish (`processor.py:270-275`). Premature checkpoint causes permanent chunk loss on retry (chunk marked as duplicate before processing completes).

## Dependencies

**Hard dependencies** (must be running):
- `redpanda` — Kafka broker for input/output topics
- `postgres` — stores checkpoints and counters
- **LLM API** — Gemini API (or configured provider) must be reachable; `LLM_API_KEY` must be valid

**Upstream** (produces input):
- `code_preprocessor` — produces enriched code chunks
- `document_preprocessor` — produces enriched document chunks

**Downstream** (consumes output):
- `embedding_service` — embeds summaries (consumes `text-summaries`)
- `neo4j_storage_service` — writes summaries to Neo4j graph (consumes `text-summaries`)

**Cognee library**:
- Imports `cognee.tasks.summarization.summarize_text` (`processor.py:18`)
- Imports `cognee_service.multi_tenancy` for company context (`consumer.py:21`)
- Calls `configure_cognee()` in `main.py:66` to initialize LLM/embedding/vector DB settings

**Postgres init**:
- Tables are created via `CREATE TABLE IF NOT EXISTS` on startup (`pipeline_store.py:33-73`)
- No Alembic migrations; idempotent schema creation

## How to run and smoke-test in isolation

### 1. Prerequisites

Ensure these services are running:
```bash
docker-compose up -d postgres redpanda
```

Set required env vars:
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:19092  # external Kafka port
export CODE_PROCESSING_POSTGRES_DSN="postgresql://kgrag:kgrag_password@localhost:5432/kgrag_auth"
export GEMINI_API_KEY="your-gemini-api-key"
export LLM_PROVIDER=gemini
export LLM_MODEL=gemini/gemini-2.0-flash-lite
export SUMMARIZATION_MAX_WORKERS=5
```

### 2. Run locally

From `backend-services/`:
```bash
python -m graph_services.summarization_service.main
```

Expected startup logs:
```
main.starting input_topic=enriched-code-chunks output_topic=text-summaries batch_size=50 max_workers=5
main.configuring_cognee
main.cognee_configured
main.connecting_postgres dsn=postgresql://kgrag:kgrag_pas...
main.postgres_connected
main.tables_ensured
consumer.started workers=5 queue_size=5
main.consumer_ready
```

### 3. Smoke-test with mock message

Publish a test `EnrichedChunkMessage` to `enriched-code-chunks` topic:
```python
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_message = {
    "action": "process",
    "chunk_id": "test-chunk-001",
    "ingestion_id": "test-ingestion-001",
    "company_id": "test-company-001",
    "project_id": "test-project-001",
    "content_type": "code",
    "file_version_id": "test-file-version-001",
    "file_path": "src/main.py",
    "repository": "test-repo",
    "branch": "main",
    "language": "python",
    "content": "def hello_world():\n    print('Hello, World!')\n",
    "header": "# Module: main",
    "chunk_index": 0
}

producer.send('enriched-code-chunks', test_message)
producer.flush()
```

Expected worker logs:
```
worker.started worker_id=0
processor.chunk_summarized idx=0 total=1 chunk_id=test-chunk-001 summary_length=42 duration_s=2.135
worker.chunk_published chunk_id=test-chunk-001
consumer.summarization_complete ingestion_id=test-ingestion-001 chunks_processed=1 total_chunks=1 total_summaries=1
```

### 4. Verify output

Consume from `text-summaries` topic:
```bash
docker exec -it kgrag-redpanda rpk topic consume text-summaries --brokers localhost:9092
```

Should see `TextSummaryEvent` JSON with `summary_text` field populated.

Check Postgres:
```sql
SELECT * FROM code_processing.processing_checkpoints WHERE item_id = 'test-chunk-001' AND stage = 'summarization';
SELECT * FROM code_processing.pipeline_counters WHERE ingestion_id = 'test-ingestion-001' AND service_name = 'summarization';
```

## Operational notes

### Consumer group

- **Group ID**: `summarization-processor-v2` (`config.py:19-21`)
- **Auto-commit**: `true` (default; override via `SUMMARIZATION_KAFKA_AUTO_COMMIT`)
- **Offset reset**: `earliest` (processes all messages from topic start; override via `SUMMARIZATION_KAFKA_AUTO_OFFSET_RESET`)

### Parallel processing

- **Worker pool size**: `MAX_PARALLEL_WORKERS` (default 30; set via `SUMMARIZATION_MAX_WORKERS`)
- **Backpressure**: Queue is bounded by worker count. If all workers are busy, Kafka consumer blocks on enqueue (`consumer.py:99,204`)
- **No batching**: Each chunk is processed and published immediately upon worker dequeue. The `process_batch` method exists in `processor.py:49-115` but is **not used** by the consumer — legacy from batch-oriented architecture.

### LLM model

- **Default**: `gemini/gemini-2.0-flash-lite` (`docker/docker-compose.yml:504`)
- **Prompt**: Cognee's internal `summarize_text` prompt (not overrideable from this service)
- **Rate limiting**: Controlled via `LLM_RATE_LIMIT_ENABLED`, `LLM_RATE_LIMIT_REQUESTS`, `LLM_RATE_LIMIT_INTERVAL` env vars (default: 1500 req/60s) (`docker/docker-compose.yml:509-511`)
- **Timeout**: 600s per chunk (`processor.py:243`)
- **Retry**: 3 attempts with exponential backoff (2s, 4s, 8s) (`processor.py:140-154`)

### Cognee multi-tenancy

- Per chunk, calls `ensure_neo4j_database(company_id)` — creates Neo4j database `cognee-{company_id}` if not exists (`consumer.py:260`)
- Calls `set_company_context(company_id)` — sets thread-local/context-var used by Cognee's graph/vector clients (`consumer.py:261`)
- This ensures LLM-generated summaries are isolated by company in downstream Neo4j/Qdrant writes

### Completion detection

- Worker checks completion after EVERY chunk: compares `chunks_received` counter (this service) vs `chunks_produced` counter (preprocessor) (`consumer.py:304-311`)
- When `chunks_received >= chunks_produced`, emits `summarization_complete` to `pipeline-events` topic and marks counters as `status='complete'` (`consumer.py:332-344`)
- Completion is checked per-ingestion, not per-batch — relies on preprocessor having set the `chunks_produced` counter first

### Graceful shutdown

- Handles SIGINT/SIGTERM (`consumer.py:399-408`)
- Sends sentinel to all workers to drain queue (`consumer.py:118-120`)
- Waits for all workers to finish (`consumer.py:125`)
- Stops Kafka producer, then consumer (`consumer.py:128-142`)

## Code map

```
summarization_service/
├── main.py              # Entrypoint: setup logging, Cognee config, Postgres pool, start consumer
├── config.py            # Env var configuration (Kafka, Postgres, LLM, worker count, retry settings)
├── consumer.py          # Kafka consumer + streaming worker pool architecture
│                        # - start(): init consumer/producer/queue/workers
│                        # - consume(): Kafka fetch loop → enqueues validated chunks
│                        # - _worker_loop(): persistent worker pulls queue → _process_chunk → loop
│                        # - _process_chunk(): set tenant context → summarize → publish → check completion
│                        # - _check_and_emit_completion(): compare counters → emit summarization_complete if done
├── processor.py         # LLM summarization logic
│                        # - process_batch(): parallel processing with semaphore (UNUSED by consumer)
│                        # - _summarize_with_retries(): exponential backoff retry wrapper
│                        # - _do_summarize(): build LLM input, check checkpoint, call summarize_text, return event
│                        # - check_ingestion_complete(): read counters to determine if all chunks processed
├── models.py            # Pydantic models for Kafka output
│                        # - TextSummaryEvent: per-chunk summary (to text-summaries topic)
│                        # - PipelineEvent: completion signal (to pipeline-events topic)
├── pipeline_store.py    # Postgres storage layer
│                        # - ensure_tables(): create processing_checkpoints + pipeline_counters
│                        # - has_checkpoint() + save_checkpoint(): content-hash deduplication
│                        # - increment_counter(), get_counter(), finalize_counters(): progress tracking
│                        # - get_preprocessor_total_chunks(): read preprocessor's chunks_produced counter
└── tests/               # (empty — no tests yet)
```

**Key execution path**:
1. `main.py` → `consumer.start()` → spawns N workers via `asyncio.create_task(_worker_loop(worker_id))`
2. `consumer.consume()` → Kafka fetch loop → validates chunk → `await work_queue.put(chunk)`
3. Worker: `chunk = await work_queue.get()` → `_process_chunk(chunk)` → `processor._summarize_with_retries()` → LLM call → `_publish_event()` → `_check_and_emit_completion()`
4. On completion: emit `PipelineEvent` to `pipeline-events`, finalize counters

**Critical invariants**:
- Checkpoint saved AFTER LLM call succeeds (`processor.py:270-275`)
- Completion check after EVERY chunk, not at end of batch (`consumer.py:304-311`)
- Workers are persistent — no spawning/destruction per chunk

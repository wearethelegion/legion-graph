# kgrag-document-preprocessor

Entry point for document ingestion into the kgrag knowledge graph — consumes brain events (knowledge/expertise/lesson), chunks documents, embeds chunks, and emits to downstream pipeline.

## What it does

This service is the document-side counterpart to `code_preprocessor`. It consumes `BrainEvent` messages from the REST API's brain endpoints (`/api/v1/brain` POST/PUT/DELETE), loads entity-type-specific extraction prompts from Postgres, chunks document text using markdown-aware splitting, embeds chunks via LiteLLM/Gemini, and publishes `EnrichedChunkMessage` events to the downstream pipeline.

Three entity types are supported:
- **knowledge** — freeform text notes (discoveries, snippets, references)
- **expertise** — structured guides (tutorials, best practices, architectural docs)
- **lesson** — resolved issues (symptom → root cause → solution → prevention)

Each entity type has a dedicated extraction prompt that drives downstream entity/edge extraction. The processor marks chunks with `content_type="document"` to distinguish them from code chunks in the shared `enriched-code-chunks` topic.

Without this service, brain content creation succeeds (persists to `knowledge`/`expertise`/`lessons` Postgres tables) but never reaches the knowledge graph — searches return `NoDataError: No valid chunks loaded`.

## Where it lives

- **Service name**: `kgrag-document-preprocessor`
- **Source**: `graph_services/document_preprocessor/`
- **Image**: `kgrag-ingestion:latest`
- **Entry point**: `python -m document_preprocessor.main` (defined in `docker/docker-compose.yml:774`)
- **Ports**: none exposed (internal Kafka consumer/producer)

## Inputs

### Kafka topic consumed
- **Topic**: `brain_events` (configured via `KAFKA_INPUT_TOPIC=brain_events` in compose, read in `document_preprocessor/config.py:12-13`)
- **Schema**: `shared.kafka_schemas.BrainEvent` (`shared/kafka_schemas.py:211-237`)
  - `entity_type`: `"knowledge"`, `"expertise"`, or `"lesson"`
  - `entity_id`: unique ID of the document
  - `company_id`: tenant identifier
  - `project_id`: optional project scope (usually null for brain content)
  - `text_content`: raw Markdown text
  - `title`: document title
  - `action`: `"create"`, `"update"`, or `"delete"`
  - `cognee_data_id`: Cognee internal ID (optional)

Events are published by `cognee_service/brain_content/servicer.py:205-214` (BrainContentServicer.AddToBrain/UpdateBrain/DeleteFromBrain) after persisting to Postgres.

### Environment variables
Core configuration from `document_preprocessor/config.py`:
- `KAFKA_BOOTSTRAP_SERVERS=redpanda:9092` — Kafka broker address
- `KAFKA_INPUT_TOPIC=brain_events` — source topic
- `KAFKA_OUTPUT_TOPIC=enriched-code-chunks` — destination topic
- `DATABASE_URL` or `POSTGRES_URL` — Postgres connection string for version tracking and prompt loading
- `GEMINI_API_KEY` — required for embedding chunks via LiteLLM (`event_emitter.py:128`)
- `EMBEDDING_MODEL=gemini/gemini-embedding-001` — LiteLLM model identifier (`config.py:51-53`)
- `EMBEDDING_DIMENSIONS=3072` — vector size
- `EMBEDDING_CONCURRENCY=3` — max parallel embedding calls
- `EMBEDDING_BATCH_SIZE=20` — chunks per API call
- `LOG_LEVEL=INFO` — logging verbosity

### Postgres tables read
- **`code_processing.document_extraction_prompts`** — entity-type-specific LLM prompts seeded from `prompts/*.txt` files (`processor.py:550-559`)
  - Auto-bootstrapped on first boot by `db_init.py:61-103` (CREATE TABLE IF NOT EXISTS + INSERT ON CONFLICT DO NOTHING)
  - Three rows expected: entity_types `knowledge`, `expertise`, `lesson`
  - Without this table seeded, every BrainEvent fails with `prompt_load_failed → missing_prompt` and no chunks reach downstream

## Outputs

### Kafka topic produced
- **Topic**: `enriched-code-chunks` (shared with code chunks; routed by `content_type` field)
- **Schema**: `EnrichedChunkMessage` (custom dict, not formal protobuf)
  - `action="process"` — marks this as a new chunk
  - `content_type="document"` — routing hint for downstream consumers (vs. `"code"`)
  - `entity_type`: original entity_type (`knowledge`/`expertise`/`lesson`)
  - `chunk_id`: UUID
  - `parent_id`: entity_id (groups chunks from same document)
  - `chunk_index`, `total_chunks`: position metadata
  - `content`: chunk text
  - `embedding`: populated vector (3072-dim by default) — embedded in this service, not downstream
  - `extraction_prompt`: entity-type-specific prompt from Postgres
  - `company_id`, `project_id`: tenant routing
  - `header`: structured context metadata (title, section heading, chunk position)

Downstream consumers:
- **`entity_extraction_service`** — extracts entities/edges using `document_graph_extraction_prompt.txt`
- **`summarization_service`** — generates text summaries
- **`qdrant_storage_service`** — routes `content_type="document"` to collection `{company_id}_knowledge` (not `DocumentChunk_text`)
- **`neo4j_storage_service`** — writes graph to `cognee-{company_id}` database with `source_node_set = "{company_id}_knowledge"`

### Postgres tables written
Schema `document_processing` (auto-created by `consumer.py:160-204`):
- **`document_versions`** — one row per entity_id, tracks `content_hash` for dedup, chunk count
- **`document_chunks`** — one row per chunk, links to `version_id`, stores `chunk_text`, `chunk_hash`, `section_heading`

Indexes:
- `idx_doc_versions_entity` on `(entity_id, deleted)`
- `idx_doc_versions_company` on `company_id`
- `idx_doc_chunks_version` on `version_id`

## Dependencies

- **redpanda** (Kafka broker) — source of `brain_events`, sink for `enriched-code-chunks`
- **postgres** (kgrag_auth DB) — prompt table (`code_processing.document_extraction_prompts`) and version tracking (`document_processing` schema)
- **LLM provider** (Gemini API) — embedding via LiteLLM (`event_emitter.py:117-169`)

Not directly used by this service (consumed by downstream):
- **qdrant** — chunk storage via `qdrant_storage_service`
- **neo4j** — graph storage via `neo4j_storage_service`

## How to run and smoke-test in isolation

### Start service
```bash
docker compose up kgrag-document-preprocessor
```

### What to look for in logs
Success indicators:
- `Database pool initialised` — Postgres connected (`main.py:34`)
- `document_extraction_prompts.ensured: entity_types=['knowledge', 'expertise', 'lesson']` — prompt table seeded (`db_init.py:100-103`)
- `document_processing schema and tables ready` — version tables created (`consumer.py:205`)
- `Starting document preprocessor consumer: topic=brain_events` — Kafka consumer initialized (`consumer.py:52-56`)
- `Consumer started, processing messages...` — main loop active (`main.py:75`)

Per-message logs:
- `doc_consumer.processed: action=create entity=knowledge/{uuid} result=success` — BrainEvent processed successfully
- `doc_processor.prompt_loaded: entity_type=knowledge` — extraction prompt fetched from DB
- `doc_chunker.split: count=5 avg_size=720` — document chunked (logged by `chunker.py`)
- `doc_emitter.chunks_published: count=5 entity=knowledge/{uuid}` — chunks published to Kafka

Failure indicators:
- `document_extraction_prompts.seed_missing_file` — prompt file missing in `prompts/` directory
- `doc_processor.no_prompt_for_entity_type` — prompt not in DB (bootstrap failed or table empty)
- `GEMINI_API_KEY not configured` — embedding will fail
- `doc_consumer.invalid_message` — malformed Kafka payload
- `doc_consumer.missing_fields: entity_id=None` — required fields missing
- `doc_emitter.embed_failed` — LiteLLM API error (rate limit, auth, network)

### Trigger test event
Requires `kgrag-cognee` running (gRPC service on port 50052, currently missing from compose). Trigger via:
```bash
curl -X POST http://localhost:8000/api/v1/brain \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": "KNOWLEDGE",
    "title": "Test Note",
    "content": "This is a test knowledge entry."
  }'
```

Watch `kgrag-document-preprocessor` logs for the `doc_consumer.processed` log line within 1-2 seconds.

## Operational notes

### Auto-bootstrap of prompts table
The service performs **idempotent prompt table setup** on every boot via `main.py:47-59` → `db_init.py:61-103`:

1. **DDL**: Creates `code_processing` schema and `document_extraction_prompts` table if missing (`db_init.py:37-52`)
2. **Seed**: Inserts three prompt rows from vendored prompt files (`db_init.py:77-98`):
   - `prompts/knowledge.txt` → entity_type `knowledge`, version 1
   - `prompts/expertise.txt` → entity_type `expertise`, version 1
   - `prompts/lesson.txt` → entity_type `lesson`, version 1
3. **Conflict handling**: Uses `ON CONFLICT (entity_type, version) DO NOTHING` — safe to run repeatedly

**Critical**: Without this bootstrap, every BrainEvent fails with `prompt_load_failed` and search returns `NoDataError` forever. The table is not populated by migrations — it is **exclusively seeded by this service**.

Prompt loading at runtime: `processor.py:537-574` queries `SELECT template_text FROM code_processing.document_extraction_prompts WHERE entity_type = $1 ORDER BY version DESC LIMIT 1` — returns the highest version prompt for the given entity_type.

### Three entity types and their prompts
Each entity type has a distinct extraction prompt that guides downstream entity/edge extraction:

- **`knowledge`** (`prompts/knowledge.txt:1-351`) — extracts Concepts, Insights, Tools, References from freeform notes
- **`expertise`** (`prompts/expertise.txt:1-337`) — extracts Concepts, Patterns, Practices, Tools, Decisions from structured guides
- **`lesson`** (`prompts/lesson.txt:1-332`) — extracts Symptom → RootCause → Solution → Prevention causal chains from resolved issues

The prompts define `KnowledgeGraph` JSON schemas, entity type conventions, relationship types, and ID stability rules. They are consumed by `entity_extraction_service` (via the `extraction_prompt` field in `EnrichedChunkMessage`) to produce a deterministic, mergeable knowledge graph.

### Content-hash deduplication
The processor skips re-chunking documents if `content_hash` (SHA-256 of `text_content`) matches the last processed version for the same `entity_id` (`processor.py:238-257`, `_hash_content` at `processor.py:577-579`). On update, if hash unchanged, returns `{"status": "success", "skipped": True, "reason": "content_unchanged"}` without emitting Kafka events.

### Idempotency cache
Uses an in-memory LRU set (default 10,000 entries) to skip duplicate `event_id` values on Kafka replay (`processor.py:32-53`, `_LRUSet`). Mirrors the pattern from `cognee_service/kafka_consumer/brain_event_processor.py`.

### Document chunks are embedded in this service
Unlike code chunks (which pass through `embedding_service` downstream), document chunks are **embedded immediately** by `event_emitter.py:171-251` (batch embedding via LiteLLM Gemini API) before publication. The `EnrichedChunkMessage` carries a populated `embedding` vector. This avoids a round-trip through the `embedding_service` consumer group and ensures `qdrant_storage_service` accepts the message without `chunk.missing_required_fields` rejection.

### Graceful shutdown
Listens for `SIGINT`/`SIGTERM` and stops Kafka consumer/producer cleanly (`main.py:64-72`, `consumer.py:88-96`). Postgres pool closed on exit.

## Code map

- **`main.py`** — entry point, initializes DB pool, calls `init_document_extraction_prompts()`, starts consumer, handles signals (`main.py:1-106`)
- **`db_init.py`** — idempotent bootstrap of `code_processing.document_extraction_prompts` table + seed from `prompts/` directory (`db_init.py:1-103`)
- **`consumer.py`** — Kafka consumer/producer lifecycle, message deserialization, delegates to `DocumentProcessor` (`consumer.py:1-207`)
- **`processor.py`** — routing by action (create/update/delete), content-hash dedup, chunking orchestration, version/chunk storage (`processor.py:1-579`)
  - `_load_prompt_for_entity_type()` — runtime prompt fetch from Postgres (`processor.py:537-574`)
- **`event_emitter.py`** — Kafka production logic, batch embedding via LiteLLM, publishes `EnrichedChunkMessage` (`event_emitter.py:1-348`)
  - `_embed_chunks()` — LiteLLM/Gemini embedding with retry (`event_emitter.py:117-169`)
  - `emit_process_chunks_batch()` — preferred batch emission with embeddings populated (`event_emitter.py:171-251`)
- **`chunker.py`** — markdown-aware text chunking (respects headings, code blocks, lists)
- **`config.py`** — Pydantic settings model for env vars (`config.py:1-86`)
- **`models.py`** — `DocumentChunkResult` dataclass
- **`prompts/`** — vendored extraction prompts:
  - `knowledge.txt` — 351 lines, KGRAG knowledge entry extraction prompt
  - `expertise.txt` — 337 lines, KGRAG expertise document extraction prompt
  - `lesson.txt` — 332 lines, KGRAG lessons learned extraction prompt
- **`tests/`** — pytest suite (not covered here)

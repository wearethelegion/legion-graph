# kgrag-qdrant-storage

Kafka worker that consumes embedded vectors and persists them to Qdrant collections, indexed by project/company for retrieval.

## What it does

The Qdrant Storage Service is a streaming Kafka consumer that writes all embedded data (code chunks, entities, summaries, triplets, edge types, entity types) from the v2 ingestion pipeline to Qdrant vector collections. It consumes pre-embedded data from two Kafka topics, routes each payload to the appropriate Qdrant collection, applies Cognee-compatible schema, and handles tenant scoping via `source_node_set` payloads.

The service operates in pure streaming mode with two-stage micro-batching:
1. **Collector**: builds maximally-full batches from the work queue (up to `QDRANT_BATCH_SIZE` or `COLLECT_TIMEOUT`)
2. **Workers**: process batches in parallel and write to Qdrant (configurable worker count)

All writes use `upsert` semantics (idempotent, safe for retries). Collection creation is lazy and auto-triggered on first write if missing.

## Where it lives

- Source: `graph_services/qdrant_storage_service/`
- Image: `kgrag-ingestion:latest` (shared with all v2 pipeline workers)
- Entry point: `python -m qdrant_storage_service.main`
- Compose service: `kgrag-qdrant-storage` (`docker/docker-compose.yml:610-650`)

## Inputs

### Kafka topics consumed

1. **`enriched-code-chunks`** (`config.py:22`)
   - Pre-embedded code chunks from `code_preprocessor` and document chunks from `document_preprocessor`
   - Schema: dict with `chunk_id`, `embedding` (3072-dim vector), `content`, `company_id`, `project_id` (optional), `content_type` (`"code"` or `"document"`)
   - `action=delete` messages trigger immediate deletion (bypass batching)

2. **`embeddings-ready`** (`config.py:21`)
   - Entity, summary, triplet, edge type, and entity type embeddings from `embedding_service`
   - Schema: `EmbeddingReadyEvent` with `embeddings` array containing `source_id`, `source_type`, `text`, `embedding` per item
   - `source_type` values: `entity`, `summary`, `triplet`, `edge_type`, `entity_type`

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `redpanda:9092` | Kafka broker address |
| `EMBEDDING_OUTPUT_TOPIC` | `embeddings-ready` | Topic for embedded entities/summaries |
| `ENRICHED_CHUNKS_TOPIC` | `enriched-code-chunks` | Topic for pre-embedded chunks |
| `QDRANT_STORAGE_CONSUMER_GROUP_ID` | `qdrant-storage-v3-streaming` | Kafka consumer group |
| `QDRANT_URL` | `http://qdrant:6333` | Qdrant REST API endpoint |
| `QDRANT_API_KEY` | (empty) | Optional Qdrant API key |
| `QDRANT_EMBEDDING_DIMENSION` | `3072` | Vector dimension (Gemini embedding-001) |
| `QDRANT_BATCH_SIZE` | `100` | Max points per upsert batch |
| `QDRANT_STORAGE_WORKERS` | `10` | Number of parallel batch workers |
| `QDRANT_STORAGE_COLLECT_TIMEOUT` | `0.5` | Seconds to wait for batch to fill |
| `CODE_PROCESSING_POSTGRES_DSN` | (required) | Postgres connection for project name resolution and pipeline tracking |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD` | (required) | Cognee Postgres database connection (for migrations) |

## Outputs

### Qdrant collections written

All collections use **named vectors** (`vector={"text": embedding}`) for Cognee compatibility, `Distance.COSINE` metric, and 3072-dimensional vectors.

| Collection name | Payload key field | Content | Source topic | Routing logic |
|-----------------|-------------------|---------|--------------|---------------|
| `DocumentChunk_text` | `text`, `file_path`, `chunk_index` | Code and document chunks | `enriched-code-chunks` | All chunks (both `content_type="code"` and `content_type="document"`) now land here. Legacy per-company collections (`{company_id}_knowledge`) are no longer written but still cleaned up on delete. |
| `Entity_name` | `name`, `entity_type`, `description` | Extracted entities | `embeddings-ready` (source_type=entity) | One point per entity |
| `TextSummary_text` | `text` | File summaries | `embeddings-ready` (source_type=summary) | One point per file |
| `Triplet_text` | `text` (format: `source-›relationship-›target`) | Entity relationships | `embeddings-ready` (source_type=triplet) | One point per triplet |
| `EdgeType_relationship_name` | `relationship_name`, `number_of_edges` | Edge type metadata | `embeddings-ready` (source_type=edge_type) | Aggregated per relationship type |
| `EntityType_name` | `name`, `number_of_entities` | Entity type metadata | `embeddings-ready` (source_type=entity_type) | Aggregated per entity type |

### Collection naming pattern (`writer.py:41-55`)

Tenant scoping is enforced by `source_node_set` and `dataset_id` payload fields, not by collection name:

- **Code chunks/entities/summaries**: `source_node_set = "{project_id}_{project_name}_code"` where `project_name` is the slugified project name resolved from Postgres (`projects` table). Example: `"abc123_my-repo_code"`
- **Document chunks/entities/summaries**: `source_node_set = "{company_id}_knowledge"`. Example: `"550e8400-e29b-41d4-a716-446655440000_knowledge"`
- `dataset_id` is always `company_id` for all points (multi-tenant Cognee compatibility)
- `database_name` is always `"cognee-{company_id}"` (matches Neo4j per-company database name)

### Payload schema

Every Qdrant point includes **Cognee IndexSchema compatibility fields** (`writer.py:490-521` for chunks, similar pattern for all types):

```python
{
    # ── Cognee IndexSchema fields ──
    "id": str,                    # UUID or chunk_id
    "created_at": int,            # Unix timestamp in milliseconds
    "updated_at": int,            # Unix timestamp in milliseconds
    "ontology_valid": False,      # Always false for v2 pipeline
    "version": 1,                 # Schema version
    "topological_rank": 0,        # Graph ranking (unused)
    "metadata": {"index_fields": ["text"]},  # Searchable fields
    "type": "IndexSchema",        # Cognee type marker
    "belongs_to_set": [str],      # List of node sets this point belongs to
    "source_pipeline": "v2_ingestion",
    "source_task": str,           # "chunk_storage", "entity_storage", etc.
    "node_set": str,              # Primary node set
    "source_node_set": str,       # Same as node_set (tenant scoping key)
    "source_user": "default_user@example.com",
    "database_name": str,         # "cognee-{company_id}"
    "dataset_id": str,            # company_id (tenant scoping)
    
    # ── Content-specific fields ──
    "text": str,                  # For chunks/summaries/triplets
    "name": str,                  # For entities/entity types
    "relationship_name": str,     # For edge types
    
    # ── Tracking fields ──
    "company_id": str,            # Tenant ID
    "project_id": str,            # Project UUID (omitted for documents)
    "file_version_id": str,       # For deletion and deduplication
    "branch": str,                # Git branch (default: "main")
    "repository": str,            # Git repo URL
    "file_path": str,             # Source file path (chunks only)
    "language": str,              # Programming language (chunks only)
    "chunk_index": int,           # Position in file (chunks only)
    "start_line": int,            # Start line number (chunks only)
    "end_line": int,              # End line number (chunks only)
}
```

### Deletion behavior (`writer.py:188-258`)

On `action=delete` messages (immediate, bypass batching):

- **Unconditional delete** from: `DocumentChunk_text`, `TextSummary_text`, `Triplet_text`, `EdgeType_relationship_name`
- **Conditional delete (entity survival)** from: `Entity_name`, `EntityType_name` — points are kept if another point with the same `name` exists under a different `file_version_id`
- Returns dict of `{collection_name: points_deleted_count}`

## Dependencies

- **redpanda** (Kafka): message broker for ingestion pipeline
- **qdrant** (Qdrant v1.17.0): vector database for all embedded data
- **postgres** (`kgrag_auth` DB): project name resolution, `code_processing` schema for pipeline tracking
- **postgres** (`cognee` DB): Cognee library migrations (run at startup, `main.py:680-688`)

## How to run and smoke-test in isolation

### Run via docker-compose

```bash
# Start dependencies
docker-compose up -d postgres postgres-init qdrant redpanda

# Build and start the service
docker-compose up kgrag-qdrant-storage

# Watch logs
docker-compose logs -f kgrag-qdrant-storage
```

### Verify in Qdrant UI

1. Open Qdrant UI at http://localhost:6333/dashboard
2. Check **Collections** tab — should see:
   - `DocumentChunk_text`, `Entity_name`, `TextSummary_text`, `Triplet_text`, `EdgeType_relationship_name`, `EntityType_name`
   - Each collection should have `vectors.text` config with size=3072, distance=Cosine
3. Click into a collection → **Points** tab
4. Inspect a point — verify:
   - `vector.text` is a 3072-element array
   - `payload.source_node_set` matches expected pattern (`{project_id}_{project_name}_code` or `{company_id}_knowledge`)
   - `payload.dataset_id` equals `company_id`
   - `payload.file_version_id` is present

### Verify via Qdrant API

```bash
# Count points in a collection
curl http://localhost:6333/collections/DocumentChunk_text

# Scroll first 10 points
curl -X POST http://localhost:6333/collections/DocumentChunk_text/points/scroll \
  -H "Content-Type: application/json" \
  -d '{"limit": 10, "with_payload": true, "with_vector": false}'
```

### Trigger end-to-end test

```bash
# 1. Publish a test chunk to enriched-code-chunks
docker exec -it kgrag-redpanda rpk topic produce enriched-code-chunks --key test_chunk_001

# Paste JSON (Ctrl+D to send):
{
  "chunk_id": "test_chunk_001",
  "embedding": [0.001, ...],  # 3072 floats
  "content": "def test_function():\n    return True",
  "company_id": "test_company_123",
  "project_id": "test_project_456",
  "file_path": "src/test.py",
  "language": "python",
  "repository": "test/repo",
  "branch": "main",
  "content_type": "code"
}

# 2. Check service logs
docker-compose logs -f kgrag-qdrant-storage | grep "worker.batch_processed"

# 3. Verify point landed in Qdrant
curl -X POST http://localhost:6333/collections/DocumentChunk_text/points/scroll \
  -H "Content-Type: application/json" \
  -d '{"filter": {"must": [{"key": "id", "match": {"value": "test_chunk_001"}}]}, "limit": 1, "with_payload": true}'
```

## Operational notes

### Collection auto-creation behaviour

Collections are created **lazily** on first write if missing (`writer.py:134-173`, `writer.py:912-956`). The service:

1. Attempts upsert
2. If `404 collection not found` error is detected (`_is_collection_not_found()` checks `UnexpectedResponse.status_code == 404`)
3. Auto-creates the collection with correct vector config (named vector "text", dimension=3072, distance=Cosine)
4. Retries the upsert immediately (does not count as a retry attempt)

This allows the service to bootstrap new per-company collections without pre-deployment steps. Fixed collections (`DocumentChunk_text`, etc.) are also created at startup via `ensure_collections()` (`main.py:712`).

### Vector dimension

**3072** — matches `gemini/gemini-embedding-001` output dimension (`config.py:50`). This is hardcoded in collection creation and cannot be changed after collection exists. If you switch embedding models, you must:

1. Delete all Qdrant collections (or create new ones with different names)
2. Update `QDRANT_EMBEDDING_DIMENSION` env var
3. Re-ingest all data

### Distance metric

**COSINE** (`qdrant_client.models.Distance.COSINE`) — used for all collections (`writer.py:122`). This is standard for semantic search with normalized embeddings. Cognee also defaults to cosine.

### Streaming architecture

The service uses a two-stage pipeline (`main.py:63-361`):

1. **Main consumer loop** (`consume()`) fetches messages from Kafka in batches (max 100 per poll, 1-second timeout) and pushes `("chunk"|"embedding", data)` tuples to a work queue
2. **Collector task** (`_collector_loop()`) drains the work queue and builds maximally-full batches (up to `QDRANT_BATCH_SIZE` or `COLLECT_TIMEOUT` seconds)
3. **Worker pool** (`_batch_worker_loop()`, configurable count) processes batches in parallel: parse messages, group by type, write to Qdrant, commit Kafka offset

This decouples Kafka polling from Qdrant I/O and allows tuning parallelism independently. Backpressure is managed by queue sizes (work queue = 2×workers, batch queue = workers).

### Retry and error handling

- **Transient errors** (network timeouts, Qdrant overload): exponential backoff retry up to 3 attempts (`config.py:54`, `writer.py:912-972`)
- **Collection not found**: auto-create on first error, retry immediately (does not count as attempt)
- **Permanent errors** (invalid vector dimension, missing required fields): logged and skipped (batch continues)
- **Kafka offset commit**: only after successful batch processing (at-least-once delivery, idempotent upserts prevent duplication)

### Graceful shutdown

On SIGINT/SIGTERM (`main.py:656-665`, `main.py:135-172`):

1. Stop fetching new Kafka messages
2. Send shutdown sentinel to collector
3. Collector drains work queue, pushes final batch
4. Workers process all remaining batches
5. Commit Kafka offsets
6. Close Kafka producer and consumer
7. Close Postgres pool

All in-flight batches are completed before exit (no data loss on restart).

## Code map

```
graph_services/qdrant_storage_service/
├── __init__.py                 # Empty package marker
├── main.py                     # Entry point: streaming service orchestration
│   ├── QdrantStorageService    # Main class (lines 63-666)
│   │   ├── consume()           # Kafka consumer loop (174-222)
│   │   ├── _collector_loop()   # Batch builder (267-328)
│   │   ├── _batch_worker_loop()# Parallel batch processor (330-361)
│   │   ├── _process_batch()    # Parse + route + write (363-462)
│   │   ├── _parse_chunk_message()   # Chunk payload parser (464-518)
│   │   └── _parse_embedding_event() # EmbeddingReadyEvent parser (520-638)
│   └── main()                  # Async entry point (668-743)
│       ├── configure_cognee()  # Cognee env setup (681-688)
│       ├── ensure_tables()     # Postgres schema init (700)
│       └── ensure_collections()# Qdrant collection init (712)
│
├── config.py                   # Environment variable configuration (1-84)
│   └── QdrantStorageConfig     # Static config class with defaults
│       ├── KAFKA_EMBEDDINGS_TOPIC = "embeddings-ready"   (21)
│       ├── KAFKA_CHUNKS_TOPIC = "enriched-code-chunks"   (22)
│       ├── COLLECTION_CHUNKS = "DocumentChunk_text"      (39)
│       ├── COLLECTION_ENTITIES = "Entity_name"           (40)
│       ├── COLLECTION_SUMMARIES = "TextSummary_text"     (41)
│       ├── COLLECTION_TRIPLETS = "Triplet_text"          (42)
│       ├── COLLECTION_EDGE_TYPES = "EdgeType_relationship_name" (43-45)
│       └── COLLECTION_ENTITY_TYPES = "EntityType_name"   (46-48)
│
├── writer.py                   # Qdrant batch write logic (1-972)
│   └── QdrantBatchWriter       # Batch upsert + retry + collection management
│       ├── ensure_collections()     # Create all fixed collections (101-132)
│       ├── _ensure_single_collection() # Lazy collection creation (134-173)
│       ├── delete_by_file_version_id() # Delete all points for a file (189-257)
│       ├── _delete_with_survival()     # Entity survival logic (291-367)
│       ├── upsert_chunks()          # Write chunks (403-451)
│       ├── upsert_entities()        # Write entities (530-594)
│       ├── upsert_summaries()       # Write summaries (596-659)
│       ├── upsert_triplets()        # Write triplets (661-735)
│       ├── upsert_edge_types()      # Write edge types (737-802)
│       ├── upsert_entity_types()    # Write entity types (804-869)
│       ├── _batch_upsert()          # Split into batches + retry (873-910)
│       ├── _upsert_with_retry()     # Exponential backoff (912-972)
│       └── _build_canonical_node_set() # Tenant scoping key (41-55)
│
├── models.py                   # Pydantic message schemas (not shown)
│   ├── ChunkMessage            # enriched-code-chunks payload
│   └── EmbeddingReadyEvent     # embeddings-ready payload
│
├── pipeline_store.py           # Postgres pipeline tracking (not shown)
│   └── QdrantPipelineStore     # CREATE TABLE IF NOT EXISTS for tracking
│
├── dedup.py                    # Deduplication logic (not shown, unused by main.py)
└── tests/                      # Unit tests

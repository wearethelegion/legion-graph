# kgrag-code-preprocessor

Entry point for code ingestion: fetches repository code via GitHub API, chunks it AST-aware, emits enriched chunks for downstream entity extraction and graph storage.

---

## What it does

The **code preprocessor** is the first stage of the code ingestion pipeline. It consumes repository ingestion requests from Kafka, clones or pulls the target repository using a project-specific GitHub token (or env fallback), performs semantic chunking using tree-sitter AST parsing, embeds chunks via Gemini API, and publishes the enriched chunks to Kafka for downstream processing by entity extraction, summarization, and storage services.

**Core responsibilities:**

1. **Consume `incoming_requests` topic** — repository ingestion events from REST API (`POST /api/v1/code_ingestion`) or GitHub webhooks
2. **Fetch project GitHub token** from Postgres `projects` table (`SELECT github_token FROM projects WHERE id = $1`) — `git_repository_manager.py:73`
3. **Clone or `git pull`** the repository using the resolved token — `git_repository_manager.py:59-81`, `git_repository_manager.py:293-321`
4. **Build file tree** for project classification (language detection) — `consumer.py:193-200`
5. **Filter files** using `IngestionRules` (skip binaries, test files, generated code, etc.) — `file_filter.py`
6. **Chunk code** using tree-sitter AST-aware recursive chunking for supported languages (Python, TypeScript, JavaScript, Ruby, Go, Rust, Java, C#, Kotlin, Swift, PHP), fallback to recursive text chunking for non-code files — `chunker.py:17-86`
7. **Extract skeleton** (class/function/method declarations) for structural indexing — `skeleton_extractor.py:1-253`
8. **Embed chunks** via Gemini embedding API — `enrichment.py`, `ingestion_processor.py`
9. **Persist** to Postgres `code_processing.repository_file_versions` and `code_processing.file_chunks` — `storage/`
10. **Publish** enriched chunks to **`enriched-code-chunks`** Kafka topic for parallel consumption by `entity_extraction_service`, `summarization_service`, and `qdrant_storage_service` — `event_emitter.py:61`, `consumer.py:644`, `ingestion_processor.py:285`

---

## Where it lives

**Source:** `/Users/yubozhenko/legion-space/backend-services/graph_services/code_preprocessor/`

**Docker compose service:** `kgrag-code-preprocessor` (`docker/docker-compose.yml:706-770`)

**Container image:** `kgrag-ingestion:latest` (shared with other v2 pipeline services)

**Entry point:** `python -m code_preprocessor.main` (`main.py:37-108`)

---

## Inputs

### Kafka topic consumed

**Topic:** `incoming_requests` (`kafka_processing_service/config.py:14-15`)

**Consumer group:** `code-preprocessor` (`config.py:19`)

**Message schema:** `shared.kafka_schemas.RepositoryIngestionRequest` (from REST API or GitHub webhook)

**Key fields:**
- `repository` (required) — full repository name (e.g., `owner/repo`)
- `branch` (optional) — branch to ingest; defaults to `main`
- `project_id` (required) — project UUID for token lookup and multi-tenant scoping
- `company_id` (required) — company UUID for multi-tenant scoping
- `user_id` (optional) — user UUID for audit
- `framework` (optional) — project framework hint for classification
- `force_full_refresh` (optional, default `false`) — when `true`, all tracked files are re-processed even if no commit changed them (`git_repository_manager.py:220-234`)

### Environment variables

**Critical:**
- `KAFKA_BOOTSTRAP_SERVERS` — Kafka broker (default: `redpanda:9092`)
- `POSTGRES_URL` or `DATABASE_URL` — Postgres connection string for `projects` table GitHub token lookup and chunk storage (database: `kgrag_auth`)
- `GEMINI_API_KEY` — Gemini API key for embeddings
- `GITHUB_TOKEN` — **fallback** GitHub token used when `projects.github_token` is null (`git_repository_manager.py:310`)

**Storage:**
- `DATA_ROOT_DIRECTORY` — root directory for cloned repositories (default: `/data/cognee/data`)
- `ENRICHED_CHUNKS_TOPIC` — output topic name (default: `enriched-code-chunks`)

**Parser tuning:**
- `PARSER_MAX_WORKERS` — max concurrent tree-sitter parser processes (default: `4`)
- `PARSER_TIMEOUT_SECONDS` — timeout for tree-sitter parse per file (default: `60`)

**Embedding:**
- `EMBEDDING_MODEL` — Gemini embedding model (default: `gemini/gemini-embedding-001`)
- `EMBEDDING_DIMENSIONS` — embedding vector size (default: `3072`)
- `EMBEDDING_BATCH_MODE` — batch embed chunks (default: `true`)

### Postgres tables read

**Database:** `kgrag_auth`

**Tables:**
- `projects` — reads `github_token` column for per-project authentication (`git_repository_manager.py:73`)

**Tables written:**
- `code_processing.repository_file_versions` — file version tracking per commit
- `code_processing.file_chunks` — chunk content, embeddings, line ranges
- `code_processing.ingestion_batches` — ingestion batch metadata
- `code_processing.cogni_ingestion_stats` — ingestion metrics
- `code_processing.skipped_files` — filtered file tracking
- `code_processing.pipeline_errors` — error tracking
- `code_processing.ingestions` — ingestion records with status

---

## Outputs

### Kafka topic produced

**Topic:** `enriched-code-chunks` (env: `ENRICHED_CHUNKS_TOPIC`, default: `enriched-code-chunks`)

**Message schema:** Custom dict with embedded chunk metadata

**Key fields per chunk:**
- `chunk_id` — stable chunk UUID
- `company_id` — multi-tenant scope
- `project_id` — multi-tenant scope
- `repository` — repository full name
- `branch` — branch name
- `file_path` — relative file path
- `content` — chunk text
- `embedding` — pre-computed Gemini embedding vector (populated before publishing)
- `content_type` — `"code"` (distinct from `"document"` chunks from `document_preprocessor`)
- `ingestion_id` — end-to-end traceability UUID
- `file_version_id` — stable file version UUID
- `start_line`, `end_line` — 1-indexed line range
- `skeleton` — extracted class/function/method declarations

**Consumed by:**
1. `kgrag-entity-extraction` — LLM entity/edge extraction → publishes to `extracted-entities` topic
2. `kgrag-summarization` — LLM file summaries → publishes to `text-summaries` topic
3. `kgrag-qdrant-storage` — writes embedded chunks to Qdrant `DocumentChunk_text` collection
4. `kgrag-neo4j-storage` — writes nodes/edges to Neo4j database `cognee-{company_id}`

---

## Dependencies

**Infrastructure:**
- **Redpanda (Kafka)** — consumes `incoming_requests`, produces `enriched-code-chunks`
- **Postgres** — `kgrag_auth` database for `projects` table GitHub token lookup and `code_processing` schema chunk storage
- **Gemini API** — embedding generation via LiteLLM

**External:**
- **GitHub API** — repository content fetching via `git clone` / `git pull` using token-authenticated HTTPS URLs

---

## How to run and smoke-test in isolation

### Run via compose

```bash
docker-compose up -d kgrag-code-preprocessor
docker-compose logs -f kgrag-code-preprocessor
```

### Trigger code ingestion

**Via REST API (requires auth):**

```bash
curl -X POST http://localhost:8000/api/v1/code_ingestion \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "repository": "owner/repo",
    "branch": "main",
    "project_id": "PROJECT_UUID",
    "framework": "python"
  }'
```

**Via Kafka (direct message publish to `incoming_requests`):**

Use `rpk` or `kafka-console-producer` to publish a `RepositoryIngestionRequest` JSON message.

### Successful ingestion logs

```
INFO - Consumer started successfully, processing messages...
INFO - Processing repository request: framework=python repo=owner/repo branch=main project=PROJECT_UUID company=COMPANY_UUID force_full_refresh=False
DEBUG - Retrieved GitHub token for project PROJECT_UUID
INFO - Cloning repository owner/repo into rag_storage/repos/owner__repo
INFO - Initial clone completed for owner/repo@main (commit=abc123, files=45)
INFO - Built file tree for owner/repo (123 lines)
INFO - Ingestion INGESTION_UUID: processed 45 files, 234 chunks
INFO - Emitted enriched chunk for owner/repo/src/main.py to enriched-code-chunks
```

**Failure modes:**
- `Failed to fetch github_token for project PROJECT_UUID` → project not found in Postgres or DB pool unavailable
- `Failed to clone https://github.com/owner/repo.git` → bad GitHub token or private repo access denied
- `No database pool configured, cannot fetch project token` → `POSTGRES_URL` not set, will fallback to `GITHUB_TOKEN` env var

---

## Operational notes

### GitHub token resolution (three-tier cascade)

Token resolution follows this priority (`git_repository_manager.py:293-321`):

1. **Per-project token** — `SELECT github_token FROM projects WHERE id = $1` (fetched before clone, `consumer.py:168`)
2. **Env fallback** — `GITHUB_TOKEN` env var (`config.py:30-33`)
3. **Public clone** — no token embedded in URL (works only for public repositories)

**Recommendation:** Store per-project GitHub tokens in `projects.github_token` for private repo access and rate limit isolation.

### Language support (tree-sitter AST-aware chunking)

Supported languages (`skeleton_extractor.py:10-24`, `chunker.py`):
- Python, TypeScript, TSX, JavaScript, JSX
- Ruby, Go, Rust, Java, C#, Kotlin, Swift, PHP

**Fallback:** Files with unsupported extensions use recursive text chunking (paragraphs → lines → sentences).

### Chunking strategy

**Code files (supported by tree-sitter):**
- AST-aware recursive chunking: respects class/function/method boundaries
- Default max chunk size: 1000 characters (configurable via `chunker_config`)
- Min chunk size: 50 characters (smaller chunks filtered out, `chunker.py:9`)
- Node type aliases: `singleton_method` → `method` (`chunker.py:12-14`)

**Non-code files:**
- Recursive text chunking by paragraphs → lines → sentences

### Error handling

**Bad token / private repo access denied:**
- `GitCommandError` raised → logged + skipped, no retry
- Publish empty ingestion result to avoid blocking pipeline

**Parser timeout (tree-sitter):**
- Timeout per file: `PARSER_TIMEOUT_SECONDS` (default: 60s)
- Fallback to text chunking on timeout

**Embed API failures:**
- Retries with exponential backoff (LiteLLM default)
- Rate limit: controlled by `GEMINI_COOLDOWN_SECONDS` (default: 60s)

### Force full refresh mode

When `force_full_refresh=true` in ingestion payload:
- All tracked files are listed via `git ls-files` and marked as "modified" (`git_repository_manager.py:220-234`)
- All files are re-chunked and re-embedded even if no commit changed them
- Use for schema migrations or embedding model upgrades

---

## Code map

```
code_preprocessor/
├── main.py                         # Entry point: consumer lifecycle, DB pool init
├── kafka_processing_service/
│   ├── consumer.py                 # Kafka consumer: consumes incoming_requests, orchestrates pipeline
│   ├── config.py                   # Settings: Kafka topics, GitHub token env fallback
│   ├── git_repository_manager.py  # Git operations: clone/pull, token resolution (L59-81 DB lookup, L293-321 URL builder)
│   ├── ingestion_processor.py     # File processing orchestration: chunking, embedding, Kafka publish
│   ├── event_emitter.py            # Kafka producer: emit enriched chunks + delete messages
│   ├── file_filter.py              # File filtering: skip binaries, tests, generated code
│   └── _file_processing.py         # Per-file processing logic
├── chunker.py                      # Universal chunker: tree-sitter AST-aware for code, recursive text for non-code
├── skeleton_extractor.py           # AST skeleton extractor: class/function/method declarations
├── enrichment.py                   # Embedding + Kafka publish helpers
├── file_tree.py                    # Compact file tree builder for project classification
├── storage/
│   ├── repository_version_store.py # Postgres repository_file_versions CRUD
│   ├── ingestion_store.py          # Postgres ingestion metadata tracking
│   └── db_init.py                  # Postgres code_processing schema init (CREATE IF NOT EXISTS)
├── project_classifier.py           # Project framework classification (Python, Node.js, etc.)
├── project_analyzer.py             # Dependency analysis for framework detection
└── tests/                          # Unit + integration tests
```

**Critical token flow:**
1. REST API → publishes `RepositoryIngestionRequest` with `project_id` to `incoming_requests` (`api/routes/ingestion.py:31-90`)
2. `consumer.py:168` → calls `_get_project_github_token(project_id)` → `git_repository_manager.py:73` → `SELECT github_token FROM projects WHERE id = $1`
3. `git_repository_manager.py:310` → cascades: project token → env `GITHUB_TOKEN` → public clone
4. `git_repository_manager.py:314-319` → embeds token in HTTPS URL: `https://{token}@github.com/owner/repo.git`
5. GitPython clones using authenticated URL → private repo access granted

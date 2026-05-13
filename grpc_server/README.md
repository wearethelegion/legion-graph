# kgrag-search

gRPC search server exposing five gRPC services for code search, document search, code intelligence, ingestion status, and authentication.

## What it does

Historically the gRPC frontend for code search and ingestion metadata queries. After Phase 4 (May 2026), `kgrag-search` serves **five gRPC services** on port 50051:

| Service | Status | Purpose |
|---------|--------|---------|
| **CodeSearchService** | ✅ LIVE | Code entity search, graph traversal, full-text search across code chunks |
| **DocumentSearchService** | ✅ LIVE | Document/knowledge chunk search, graph traversal, summaries |
| **IngestionService** | ✅ LIVE (Phase 4 rewrite) | Ingestion status and progress queries (backed by Postgres `code_processing.ingestion_batches`, not MongoDB) |
| **CodeService** | ✅ LIVE | Code intelligence (similarity search, impact analysis, execution flow tracing) |
| **AuthService** | ✅ LIVE | JWT authentication, project list retrieval |

**Distinction from kgrag-cognee:50052**:
- `kgrag-search:50051` — queries the **v2 pipeline data** (native Neo4j `neo4j` database + standard Qdrant collections written by neo4j_storage_service and qdrant_storage_service). Direct DB queries via `api/repositories/`.
- `kgrag-cognee:50052` — queries via **Cognee library** against per-company Neo4j databases (`cognee-{company_id}`) and Cognee-managed collections. Used by REST endpoints `/api/v1/brain/search` and `/api/v1/code/search`.

Both are fully wired and operational, but REST API routes explicitly use `CogneeGrpcClient` at `:50052`, not the `kgrag-search` grpc server. The `kgrag-search` gRPC server is available for clients that call gRPC directly (external tools, SDKs, or internal services that bypass the REST layer).

Per forensic map entry `f526b6a1-64c3-4596-8e5c-441b8c1af785`, `IngestionServicer` was **dead until Phase 4** (it queried MongoDB which was never in docker-compose). Phase 4 rewrote it to use `IngestionStore` (Postgres-backed; same store the v2 code_preprocessor writes to). `GetIngestionStatus` and `ListIngestions` are now functional.

## Where it lives

- **Dockerfile**: `Dockerfile.search` (line 1-51)
- **Image**: `kgrag-search:latest`
- **Source folder**: `grpc_server/`
- **Compose service**: `kgrag-search` (docker/docker-compose.yml:285-338)
- **Exposed port**: `50051` (internal only — not published to host in compose)
- **Entry point**: `python -m grpc_server.server` (Dockerfile.search:51)

## Inputs

### gRPC Services and Methods

**Proto files**: `grpc_server/protos/*.proto` (source definitions + committed stubs)

| Proto | Service | Methods | Method Signatures (RPC Name → Request/Response) |
|-------|---------|---------|------------------------------------------------|
| `code_search.proto` | `CodeSearchService` | 8 | `GetDomains`, `SearchEntities`, `SearchSummaries`, `GetCodeForEntity`, `TraverseGraph`, `GetEntityGraph`, `FullSearch`, `Health` |
| `document_search.proto` | `DocumentSearchService` | 7 | `GetCollections`, `SearchDocuments`, `GetDocumentChunk`, `SearchDocumentSummaries`, `TraverseDocumentGraph`, `FullDocumentSearch`, `Health` |
| `ingestion.proto` | `IngestionService` | 2 | `GetIngestionStatus(GetIngestionStatusRequest) → GetIngestionStatusResponse`, `ListIngestions(ListIngestionsRequest) → ListIngestionsResponse` |
| `code.proto` | `CodeService` | 4 | `CreateCode`, `FindSimilarCode`, `AnalyzeImpact`, `TraceExecutionFlow` |
| `auth.proto` | `AuthService` | 2 | `Authenticate(AuthRequest) → AuthResponse`, `GetProjects(GetProjectsRequest) → GetProjectsResponse` |

Generated Python stubs: `grpc_server/protos/*_pb2.py`, `grpc_server/protos/*_pb2_grpc.py` (committed; regenerated via `scripts/generate_protos.sh` if .proto files change).

See `grpc_server/protos/README.md` for proto regeneration instructions.

### Environment Variables Consumed

From `docker/docker-compose.yml:285-338` (kgrag-search service):

| Env Var | Purpose | Default / Example |
|---------|---------|-------------------|
| `GRPC_SERVER_HOST` | Bind address | `0.0.0.0` |
| `GRPC_SERVER_PORT` | gRPC port | `50051` |
| `GRPC_MAX_WORKERS` | ThreadPoolExecutor workers | `10` |
| `GRPC_IDEMPOTENCY_CACHE_TTL` | Idempotency cache TTL (seconds) | `3600` |
| `GRPC_ENABLE_HEALTH_CHECK` | Enable health check interceptor | `true` |
| `JWT_SECRET_KEY` | Auth interceptor JWT validation | `CHANGE_THIS_IN_PRODUCTION` |
| `AUTH_SERVICE_URL` | Auth service endpoint (for user validation) | `http://kgrag-auth:8001` |
| `QDRANT_URL` | Qdrant REST API | `http://qdrant:6333` |
| `NEO4J_URI` | Neo4j Bolt URI | `bolt://neo4j:7687` |
| `NEO4J_USER` | Neo4j username | `neo4j` |
| `NEO4J_PASSWORD` | Neo4j password | `kgrag_neo4j_password` |
| `NEO4J_DATABASE` | Neo4j database name (default DB for v2 pipeline) | `neo4j` |
| `DATABASE_URL` | Postgres connection string (for ingestion status queries + project lookup) | `postgresql://kgrag:kgrag_password@postgres:5432/kgrag_auth` |
| `REDIS_URI` / `REDIS_URL` | Redis for JWT blacklist + session cache | `redis://:kgrag_redis_password@redis:6379/0` |
| `GEMINI_API_KEY` | LLM API key (for code intelligence operations) | (required for CodeService) |
| `LLM_ENABLED` | Enable LLM features | `true` |
| `EMBEDDING_DIMENSIONS` | Embedding vector size | `3072` |
| `EMBEDDING_API_KEYS` | Additional embedding API keys | (optional) |
| `PYTHONUNBUFFERED` | Python unbuffered stdout | `1` |
| `LOG_LEVEL` | Logging level | `INFO` |

## Outputs

### Database Queries Issued

**Read-only** — kgrag-search does not write data. It only queries:

1. **Postgres** (`kgrag_auth` DB):
   - `code_processing.ingestion_batches` — read by `IngestionServicer` (via `IngestionStore.get_ingestion()` / `.list_ingestions()`)
   - `projects` table — read by all servicers for project name → ID resolution and company_id derivation (via `ProjectRepository`)

2. **Neo4j** (database `neo4j` — the default v2 pipeline database):
   - Entity nodes (`EntityType`, `Entity`, `DocumentChunk`, `Repository`, etc.)
   - Edges (`contains`, `made_from`, `is_a`, `is_part_of`, `defined_in`, etc.)
   - Queried by `CodeSearchServicer` and `DocumentSearchServicer` via `api/repositories/neo4j_repository.py`

3. **Qdrant** (collections):
   - `DocumentChunk_text` — code chunks (queried by `CodeSearchServicer`)
   - `Entity_name` — extracted entities (queried by `CodeSearchServicer`)
   - `TextSummary_text` — file summaries (queried by `CodeSearchServicer`)
   - `{company_id}_knowledge` — document chunks (queried by `DocumentSearchServicer`)
   - Queried via `api/repositories/qdrant_repository.py`

4. **Redis** (cache + blacklist):
   - JWT blacklist lookup (via `grpc_server/token_blacklist.py:is_blacklisted()`)
   - Session context cache (via `SessionContextInterceptor`)

### Kafka Topics Produced

None. kgrag-search is read-only (no writes, no Kafka producers).

### Downstream Services Called

- `kgrag-auth:8001` — user validation during `AuthenticationInterceptor` token validation (optional fallback; primary validation is JWT decode + Redis blacklist check)

## Dependencies

**Must be running** (per `depends_on` in docker/docker-compose.yml:319-329):

1. **postgres:5432** — for ingestion status queries (`code_processing.ingestion_batches`) and project metadata (`projects` table)
2. **neo4j:7687** (Bolt) — for graph traversal, entity search, code/document search
3. **qdrant:6333** (REST) / `:6334` (gRPC) — for vector similarity search (code chunks, entities, summaries, documents)
4. **redis:6379** — for JWT blacklist and session cache
5. **kgrag-auth:8001** — for user authentication (optional; JWT validation is primary)

**Does NOT depend on**:
- MongoDB (Phase 4 removed MongoDB dependency; `IngestionServicer` now uses Postgres)
- Kafka (read-only service)
- kgrag-cognee:50052 (separate parallel service)

## How to run and smoke-test in isolation

### Start Service

```bash
# Start all dependencies + kgrag-search
docker compose up postgres qdrant neo4j redis kgrag-auth kgrag-search
```

Wait for healthcheck: `timeout 1 bash -c '</dev/tcp/localhost/50051'` (internal healthcheck in compose line 331).

### Smoke Test with grpcurl

**Prerequisite**: Install grpcurl: `brew install grpcurl` (macOS) or see https://github.com/fullstorydev/grpcurl

**Example 1: Health check (CodeSearchService)**

```bash
grpcurl -plaintext \
  -d '{"service_name": "kgrag.code_search.CodeSearchService"}' \
  localhost:50051 \
  kgrag.code_search.CodeSearchService/Health
```

Expected response: `{"status": "ok", "service": "kgrag.code_search.CodeSearchService"}`

**Example 2: Get projects (AuthService)**

```bash
# First, get a JWT token from kgrag-auth:8001 (see auth service docs)
# Then call GetProjects:
grpcurl -plaintext \
  -H "authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{}' \
  localhost:50051 \
  kgrag.auth.AuthService/GetProjects
```

Expected: JSON list of projects the user has access to.

**Example 3: List ingestions (IngestionService)**

```bash
grpcurl -plaintext \
  -H "authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{"limit": 5, "offset": 0}' \
  localhost:50051 \
  kgrag.ingestion.IngestionService/ListIngestions
```

Expected: JSON array of ingestion records from `code_processing.ingestion_batches`.

**List available services**:

```bash
grpcurl -plaintext localhost:50051 list
```

Expected output:
```
grpc.reflection.v1alpha.ServerReflection
kgrag.auth.AuthService
kgrag.code.CodeService
kgrag.code_search.CodeSearchService
kgrag.document_search.DocumentSearchService
kgrag.ingestion.IngestionService
```

## Operational notes

### Which methods are dead?

**None** — all 5 servicers are fully functional as of Phase 4 (May 2026).

- `IngestionServicer` was previously dead (queried MongoDB which was never in compose). Phase 4 rewrote it to use `IngestionStore` (Postgres-backed). Evidence: `grpc_server/servicers/ingestion_servicer.py:1-237` (rewrite note at lines 1-11).
- All other servicers (`AuthServicer`, `CodeServicer`, `CodeSearchServicer`, `DocumentSearchServicer`) have been live since the initial v2 pipeline deploy.

### Env var contracts

**Critical**:
- `JWT_SECRET_KEY` must match the key used by `kgrag-auth` to mint tokens. Mismatch → all auth fails.
- `NEO4J_DATABASE=neo4j` must point to the **default database** where the v2 pipeline (neo4j_storage_service) writes data. Do NOT set to `cognee` (that's for kgrag-cognee:50052).
- `REDIS_URI` must be reachable. If Redis is down, JWT blacklist lookups fail and the auth interceptor **fails closed** (rejects all requests). Set `TOKEN_BLACKLIST_FAIL_OPEN=true` in env to fail open (insecure; dev-only).

**Optional**:
- `GRPC_ENABLE_HEALTH_CHECK=false` to disable health check interceptor.
- `GEMINI_API_KEY` only required if using `CodeService` methods (`AnalyzeImpact`, `TraceExecutionFlow`).

### Startup ordering

1. postgres, qdrant, neo4j, redis must be healthy (via `depends_on` + `condition: service_healthy`)
2. kgrag-auth must be healthy (for auth interceptor fallback user validation)
3. kgrag-search starts and initializes DB pool (`api.database.init_db_pool()` at server.py:58)
4. Servicers are registered (server.py:84-99)
5. Prometheus metrics server starts (CodeSearch observability at server.py:102-108)
6. gRPC server binds to `0.0.0.0:50051` and starts listening (server.py:110-116)

### Interceptors (execution order)

Applied in sequence per `grpc_server/server.py:73-78`:

1. **LoggingInterceptor** — logs all incoming RPCs (method name, peer, latency)
2. **SessionContextInterceptor** — establishes session metadata (request ID, timestamps)
3. **AuthenticationInterceptor** — validates JWT token from `authorization` metadata, injects `CurrentUser` into context. **Fails closed** if token invalid or blacklisted.
4. **IdempotencyInterceptor** — deduplicates requests based on `x-idempotency-key` metadata (TTL = `GRPC_IDEMPOTENCY_CACHE_TTL`)

Auth is enforced on all RPCs except health checks.

### Redis blacklist contract

`AuthenticationInterceptor` calls `grpc_server/token_blacklist.py:is_blacklisted(jti)` which queries Redis key `blacklist:{jti}`. If key exists, token is rejected. Blacklist is populated by `kgrag-auth` on logout or token revocation.

If Redis is unreachable and `TOKEN_BLACKLIST_FAIL_OPEN=false` (default), the interceptor fails closed (rejects all tokens). Set `TOKEN_BLACKLIST_FAIL_OPEN=true` to fail open (dev/test only; insecure).

## Code map

All paths relative to `grpc_server/`:

- **`server.py`** (143 lines) — gRPC server entry point. Registers 5 servicers, applies 4 interceptors, initializes DB pool, starts Prometheus metrics server for CodeSearch.
- **`config.py`** (38 lines) — `GrpcServerConfig` dataclass. Loads `GRPC_SERVER_HOST`, `GRPC_SERVER_PORT`, `GRPC_MAX_WORKERS`, `GRPC_IDEMPOTENCY_CACHE_TTL`, `GRPC_ENABLE_HEALTH_CHECK` from env.
- **`token_blacklist.py`** — JWT blacklist lookup against Redis (`blacklist:{jti}` keys). Used by `AuthenticationInterceptor`.

### Subdirectories

**`interceptors/`** — gRPC interceptors (applied to all RPCs):
- **`logging_interceptor.py`** — Logs method name, peer, latency for every RPC.
- **`session_interceptor.py`** — Injects session metadata (request ID, timestamps) into context.
- **`auth_interceptor.py`** — Validates JWT from `authorization` metadata, checks Redis blacklist, injects `CurrentUser` into context. Fails closed if token invalid.
- **`idempotency_interceptor.py`** — Deduplicates requests using `x-idempotency-key` metadata (in-memory cache, TTL = `GRPC_IDEMPOTENCY_CACHE_TTL`).

**`protos/`** — Proto definitions + generated stubs:
- **`auth.proto`, `code.proto`, `code_search.proto`, `document_search.proto`, `ingestion.proto`, `kgrag_common.proto`** — Proto source files.
- **`*_pb2.py`, `*_pb2_grpc.py`** — Generated Python stubs (committed; regenerated via `scripts/generate_protos.sh` when .proto files change).
- **`loader.py`** — Lazy proto stub loader (prevents duplicate descriptor registration).
- **`README.md`** — Proto regeneration instructions and service/RPC inventory.

**`servicers/`** — gRPC servicer implementations (one per service):
- **`auth_servicer.py`** — `AuthServicer`: JWT validation, project list retrieval. Methods: `Authenticate`, `GetProjects`.
- **`code_servicer.py`** — `CodeServicer`: Code intelligence (similarity search, impact analysis, execution flow tracing). Methods: `CreateCode`, `FindSimilarCode`, `AnalyzeImpact`, `TraceExecutionFlow`.
- **`code_search_servicer.py`** — `CodeSearchServicer`: Code entity search, graph traversal, full-text search. 8 methods including `SearchEntities`, `TraverseGraph`, `FullSearch`. Queries Neo4j + Qdrant directly via `api/repositories/`.
- **`document_search_servicer.py`** — `DocumentSearchServicer`: Document/knowledge chunk search, graph traversal. 7 methods including `SearchDocuments`, `TraverseDocumentGraph`, `FullDocumentSearch`. Queries Neo4j `{company_id}_knowledge` node sets + Qdrant `{company_id}_knowledge` collection.
- **`ingestion_servicer.py`** (237 lines) — `IngestionServicer`: Ingestion status and progress queries. **Phase 4 rewrite** (lines 1-11): replaced MongoDB-backed implementation with Postgres-backed `IngestionStore` (same store the v2 code_preprocessor writes to). Methods: `GetIngestionStatus`, `ListIngestions`. Now fully functional.
- **`code_search_metrics.py`** — Prometheus metrics for CodeSearchService (request counters, latency histograms). Metrics HTTP server on port 8001 (started at server.py:105).
- **`utils.py`** — Shared servicer utilities (response builders, error handlers).

**`utils/`** — gRPC utility modules:
- **`auth.py`** — `get_current_user_from_context()`: extracts `CurrentUser` from grpc context (injected by `AuthenticationInterceptor`).
- **`session.py`** — Session metadata helpers (request ID, timestamps).
- **`access_control.py`** — Multi-tenant access control helpers (company_id / project_id scope validation).
- **`adapters.py`** — Converts between proto messages and internal service models.

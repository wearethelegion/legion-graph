# kgrag-cognee

Wraps the Cognee library and serves the code+document search backend over gRPC on port 50052.

## What it does

This is the actual search backend for KGRAG. It exposes three gRPC services:

1. **CogneeService** — `FlexibleCogneeSearch` (document/knowledge search) and `FlexibleCodeSearch` (code graph search)
2. **BrainService** — 29 brain v2 CRUD RPCs for knowledge/expertise/lessons
3. **BrainContentService** — additive brain content operations (AddToBrain, UpdateBrain, DeleteFromBrain)

The service:
- Stores brain knowledge/expertise/lessons in Postgres `brain_*` tables (auto-created on first boot via `brain/db.py:init_brain_tables`)
- Publishes `BrainEvent` messages to the `brain_events` Kafka topic when brain content changes, triggering downstream document_preprocessor enrichment
- Builds and queries the knowledge graph in Neo4j (per-company database `cognee-{company_id}`)
- Embeds and retrieves vectors in Qdrant (collections: `{company_id}_knowledge` for documents, `{project_id}_{project_name}_code` for code)
- Validates JWTs via Redis-backed blacklist (with fail-closed by default) and `kgrag-auth` service integration
- Applies Cognee library patches at import time (`cognee_patches.py`) for KGRAG-specific behavior

Evidence: `graph_services/cognee_service/server.py:4-29`, `servicer.py:1-577`, `brain/db.py:98-164`, `Dockerfile.cognee:1-53`

## Where it lives

**Source**: `/Users/yubozhenko/legion-space/backend-services/graph_services/cognee_service/`

**Image**: `kgrag-cognee:latest` (built from `Dockerfile.cognee`)

**Compose service**: `kgrag-cognee` in `docker/docker-compose.yml:340-419`

**Entry point**: `python -m cognee_service.server` (see `Dockerfile.cognee:53`)

**Port**: 50052 (gRPC only; no REST API)

## Inputs

### gRPC services on :50052

All three services share the same gRPC port and auth interceptor.

**CogneeService** (`cognee_pb2_grpc.CogneeServiceServicer`):
- `FlexibleCogneeSearch` — search documents/knowledge with scope `knowledge` → queries `{company_id}_knowledge` node set in Neo4j + Qdrant
- `FlexibleCodeSearch` — search code with scope `code:{project_id}:{project_name}` → queries `{project_id}_{project_name}_code` node set
- Supports 14 search types: `GRAPH_COMPLETION` (default), `TRIPLET_COMPLETION`, `CHUNKS`, `RAG_COMPLETION`, `SUMMARIES`, `GRAPH_SUMMARY_COMPLETION`, `CYPHER` (admin only), `NATURAL_LANGUAGE` (admin only), `GRAPH_COMPLETION_COT`, `GRAPH_COMPLETION_CONTEXT_EXTENSION`, `FEELING_LUCKY`, `TEMPORAL`, `CODING_RULES`, `CHUNKS_LEXICAL`

Evidence: `servicer.py:467-531`

**BrainService** (`brain_pb2_grpc.BrainServiceServicer`):
- 29 RPCs for knowledge/expertise/lessons CRUD: `ListKnowledge`, `GetKnowledge`, `CreateKnowledge`, `UpdateKnowledge`, `DeleteKnowledge`, and parallel sets for expertise and lessons
- `UnifiedSearch` — cross-kind search with filters and pagination
- Engagement/entry management RPCs for structured work tracking

Evidence: `brain/servicer.py:1-577`

**BrainContentService** (`brain_pb2_grpc.BrainContentServiceServicer`):
- `AddToBrain` — add knowledge/expertise/lesson (persists to Postgres + publishes to Kafka `brain_events` topic)
- `UpdateBrain` — update existing brain content
- `DeleteFromBrain` — delete brain content

Evidence: `brain_content/servicer.py:183-382`

### Environment variables consumed

**Core connectivity**:
- `GRPC_PORT` — gRPC listen port (default: 50052)
- `NEO4J_URI` — Neo4j Bolt URI (e.g., `bolt://neo4j:7687`)
- `NEO4J_USER`, `NEO4J_PASSWORD` — Neo4j credentials
- `COGNEE_NEO4J_DATABASE` — global Neo4j DB name (default: `cognee`; per-company DBs are `cognee-{company_id}`)
- `VECTOR_DB_PROVIDER` — must be `qdrant`
- `VECTOR_DB_URL` — Qdrant REST API (e.g., `http://qdrant:6333`)
- `QDRANT_API_KEY` — Qdrant auth key (optional)

Evidence: `config.py:34-48`, `docker-compose.yml:348-356`

**Postgres (dual connection)**:
- Cognee internal DB: `DB_PROVIDER`, `DB_HOST`, `DB_PORT`, `DB_NAME` (cognee), `DB_USERNAME`, `DB_PASSWORD` — for Cognee's own relational tables (datasets, data, etc.)
- Brain content DB: `KGRAG_DATABASE_URL` or `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD` — for brain_* tables in `kgrag_auth` database

Evidence: `config.py:50-56`, `brain/db.py:40-63`, `docker-compose.yml:357-365`

**Auth & security**:
- `JWT_SECRET_KEY` — HS256 secret for main JWT validation
- `COGNEE_JWT_SECRET_KEY` — separate secret for Cognee-minted JWTs (defaults to `JWT_SECRET_KEY`)
- `AUTH_SERVICE_URL` — kgrag-auth REST API (e.g., `http://kgrag-auth:8001`) for user validation
- `REDIS_URI` — Redis connection for token blacklist (e.g., `redis://:password@redis:6379/0`)
- `TOKEN_BLACKLIST_FAIL_OPEN` — if `true`, blacklist failures allow request (default: `false`, fail-closed)

Evidence: `auth_interceptor.py:37-148`, `docker-compose.yml:366-375`

**Kafka**:
- `KAFKA_BOOTSTRAP_SERVERS` — Kafka brokers (e.g., `redpanda:9092`) for `brain_events` topic publishing

Evidence: `brain/kafka_producer.py`, `docker-compose.yml:377`

**LLM & embeddings**:
- `LLM_PROVIDER`, `LLM_MODEL`, `LLM_API_KEY` — LLM for query expansion / RAG modes (e.g., `gemini`, `gemini/gemini-2.0-flash-lite`)
- `EMBEDDING_PROVIDER`, `EMBEDDING_MODEL`, `EMBEDDING_DIMENSIONS` — embedding model config (e.g., `gemini`, `gemini/gemini-embedding-001`, `3072`)
- `GEMINI_API_KEY` — Google Gemini API key

Evidence: `config.py:14-29`, `docker-compose.yml:378-385`

**Cognee behavior**:
- `ENABLE_BACKEND_ACCESS_CONTROL` — must be `false` for self-hosted stack (Cognee multi-user mode requires SaaS-only DB handlers)
- `TELEMETRY_DISABLED` — set `true` to disable Cognee telemetry
- `LOG_LEVEL` — logging verbosity (INFO, DEBUG, etc.)

Evidence: `config.py:59-62`, `docker-compose.yml:391-395`

## Outputs

### Postgres tables written (in `kgrag_auth` DB)

Auto-created on first boot via `brain/db.py:init_brain_tables` (idempotent DDL with `CREATE IF NOT EXISTS`):

- **`brain_knowledge`** — unstructured knowledge content (company-scoped)
  - Columns: `id`, `company_id`, `title`, `content`, `metadata`, `content_hash`, `created_by_user_id`, `created_at`, `updated_at`
  - Indexes: `idx_brain_knowledge_company`, `idx_brain_knowledge_hash`

- **`brain_expertise`** — structured expertise/guides (company-scoped)
  - Columns: `id`, `company_id`, `title`, `content`, `when_to_use`, `metadata`, `content_hash`, `created_by_user_id`, `created_at`, `updated_at`
  - Indexes: `idx_brain_expertise_company`, `idx_brain_expertise_hash`

- **`brain_lessons`** — resolved issues and lessons learned (company-scoped)
  - Columns: `id`, `company_id`, `title`, `content`, `metadata`, `content_hash`, `created_by_user_id`, `symptom`, `root_cause`, `solution`, `prevention`, `severity`, `created_at`, `updated_at`
  - Indexes: `idx_brain_lessons_company`, `idx_brain_lessons_hash`

Evidence: `brain/db.py:98-164`

### Neo4j graph database (per-company)

Per-company Neo4j database: `cognee-{company_id}` (created via `CREATE DATABASE cognee-{company_id} IF NOT EXISTS` against `system` database by `multi_tenancy.py:ensure_neo4j_database`).

Stores:
- Graph nodes and edges from ingested code and documents
- Cognee's own ontology nodes
- Entity/relationship structures from entity_extraction_service

Evidence: `multi_tenancy.py:32-55`, engagement entry `f526b6a1-64c3-4596-8e5c-441b8c1af785` (section 2.2, 3.2)

### Qdrant collections

Document/knowledge embeddings → `{company_id}_knowledge`

Code embeddings → `{project_id}_{project_name}_code`

Evidence: `servicer.py:77-88`, engagement entry `f526b6a1-64c3-4596-8e5c-441b8c1af785` (section 2.2, 3.2)

### Kafka messages published

Topic: **`brain_events`** (from `BrainContentService` AddToBrain/UpdateBrain/DeleteFromBrain)

Schema: `shared.kafka_schemas.BrainEvent` — contains `entity_type`, `entity_id`, `company_id`, `project_id`, `text_content`, `title`, `action` (create/update/delete), `cognee_data_id`

Consumed by: `document_preprocessor` (in compose), `cognee_service/kafka_consumer` (source only, not wired in compose)

Evidence: `brain_content/servicer.py:205-214`, `brain/kafka_producer.py`, engagement entry `f526b6a1-64c3-4596-8e5c-441b8c1af785` (section 3.1)

## Dependencies

**Postgres** (`postgres:5432`) — for two separate databases:
- `cognee` DB (Cognee internal relational tables)
- `kgrag_auth` DB (brain_knowledge, brain_expertise, brain_lessons tables)

**Neo4j** (`neo4j:7687`) — requires DozerDB or Enterprise for multi-database support. The `kgrag` database must exist (created by `postgres-init` service via `CREATE DATABASE kgrag IF NOT EXISTS` Cypher against `system` database).

**Qdrant** (`qdrant:6333`) — vector storage for document and code embeddings.

**Redis** (`redis:6379`) — JWT blacklist and revocation timestamp cache (required by `auth_interceptor.py`). Without `REDIS_URI`, the service cannot validate token blacklists and fails closed (all requests → 401).

**Redpanda** (`redpanda:9092`) — Kafka-compatible broker for `brain_events` topic publishing.

**kgrag-auth** (`kgrag-auth:8001`) — for user validation and role checks via REST API.

Evidence: `docker-compose.yml:398-410`, `auth_interceptor.py:60-148`, `brain/kafka_producer.py`

## How to run and smoke-test in isolation

### Start the service

Ensure all dependencies are running first:

```bash
docker compose up -d postgres postgres-init neo4j qdrant redis redpanda kgrag-auth
```

Then start kgrag-cognee:

```bash
docker compose up kgrag-cognee
```

Wait for healthcheck to pass (check logs for `cognee_server.started`).

### Smoke test with grpcurl

**Check gRPC health**:

```bash
grpcurl -plaintext localhost:50052 grpc.health.v1.Health/Check
```

Expected: `{"status": "SERVING"}`

**List services**:

```bash
grpcurl -plaintext localhost:50052 list
```

Expected output includes:
- `kgrag.cognee.CogneeService`
- `kgrag.brain.BrainService`
- `kgrag.brain.BrainContentService`

**Call FlexibleCogneeSearch** (requires valid JWT in `authorization` metadata):

```bash
grpcurl -plaintext \
  -H "authorization: Bearer YOUR_JWT_HERE" \
  -d '{"query": "test search", "search_type": "GRAPH_COMPLETION", "limit": 5, "only_context": false, "scope": "knowledge"}' \
  localhost:50052 \
  kgrag.cognee.CogneeService/FlexibleCogneeSearch
```

Evidence: `Dockerfile.cognee:49-50` (healthcheck uses gRPC channel readiness), `server.py:116-118`

## Operational notes

### Brain tables auto-bootstrap

The `brain_knowledge`, `brain_expertise`, `brain_lessons` tables are auto-created on first boot. The `cognee_service/server.py:serve` function calls `brain_db.init_brain_tables()` at line 102, which executes idempotent DDL (`CREATE TABLE IF NOT EXISTS`). No manual migration required.

Evidence: `server.py:99-103`, `brain/db.py:151-164`

### Neo4j database requirement

The global `kgrag` Neo4j database must exist before kgrag-cognee starts. The `postgres-init` compose service creates it via:

```cypher
CREATE DATABASE kgrag IF NOT EXISTS
```

run against the `system` database. Per-company databases (`cognee-{company_id}`) are created lazily by `multi_tenancy.py:ensure_neo4j_database` on first search/cognify call for each company.

Evidence: `docker-compose.yml:45-77`, `multi_tenancy.py:32-55`

### JVM memory tuning context

The Neo4j container (`graphstack/dozerdb:5.26.3.0`) requires sufficient heap for multi-database operation. The compose config sets `NEO4J_server_memory_heap_max__size=2G` and `NEO4J_server_memory_pagecache_size=1G`. If kgrag-cognee experiences Neo4j connection timeouts or OOM, tune these values higher.

Evidence: `docker-compose.yml:120-122`

### Token blacklist fail-closed setting

By default, `TOKEN_BLACKLIST_FAIL_OPEN=false`. If Redis is unreachable or the blacklist lookup fails, the auth interceptor rejects the request (fail-closed). Set `TOKEN_BLACKLIST_FAIL_OPEN=true` only in development to allow requests when Redis is down (not recommended for production).

Evidence: `auth_interceptor.py:60-148`, `docker-compose.yml:375`

### Cognee patches applied at import time

The `cognee_service/cognee_patches.py` module is imported at the top of `server.py:13` (before any Cognee imports). These patches override Cognee library behaviors for KGRAG-specific needs (e.g., custom Qdrant adapter registration, search contract fixes). The patches are applied to the `cognee` package installed in `kgrag-base:latest`.

Evidence: `server.py:13`, `Dockerfile.cognee:7-16`

## Code map

Key files in `graph_services/cognee_service/`:

- **`server.py:1-141`** — gRPC server entry point; configures Cognee, initializes DB pools, registers all three servicers, starts gRPC server on :50052
- **`servicer.py:1-577`** — CogneeService implementation (FlexibleCogneeSearch, FlexibleCodeSearch, Cognify, Prune, Health)
- **`config.py:1-73`** — Cognee library configuration loader (sets LLM, embedding, Neo4j, Qdrant, Postgres config from env vars)
- **`auth_interceptor.py:1-280`** — CogneeAuthInterceptor validates JWTs, checks Redis blacklist, injects `CurrentUser` into context
- **`cognee_patches.py`** — Cognee library patches (Qdrant adapter, search contract fixes)
- **`multi_tenancy.py:1-55`** — `ensure_neo4j_database` creates per-company Neo4j databases, `set_company_context` switches Cognee's active company
- **`query_expansion.py`** — LLM-based query expansion for search types that benefit from reformulation
- **`lock.py`** — per-dataset asyncio locks to serialize `cognee.add()` calls (prevents concurrent cognify corruption)

**`brain/` subfolder** (Brain v2 CRUD):

- **`db.py:1-164`** — asyncpg connection pool for `kgrag_auth` DB, `init_brain_tables` DDL (creates brain_knowledge/expertise/lessons)
- **`servicer.py:1-577`** — BrainService implementation (29 RPCs for knowledge/expertise/lessons CRUD + UnifiedSearch)
- **`knowledge_handler.py`**, **`expertise_handler.py`**, **`lessons_handler.py`** — per-kind CRUD logic
- **`kafka_producer.py`** — publishes BrainEvent messages to `brain_events` topic
- **`neo4j_client.py`** — Neo4j query helpers for engagement/entry retrieval

**`brain_content/` subfolder** (additive brain content API):

- **`servicer.py:1-382`** — BrainContentService implementation (AddToBrain, UpdateBrain, DeleteFromBrain)
- **`repositories.py`** — Postgres CRUD for brain_knowledge/expertise/lessons tables (shared by both BrainService and BrainContentService)

**`kafka_consumer/` subfolder** (source only, not in compose):

- **`brain_event_processor.py`** — alternative Kafka consumer for `brain_events` that calls `cognee.add() + cognee.cognify()` directly (duplicate of document_preprocessor path)

Evidence: directory listing from initial investigation

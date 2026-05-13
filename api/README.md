# kgrag-rest-api

User-facing HTTP frontend for company/project/user management, document ingestion, code ingestion, and search.

## What it does

`kgrag-rest-api` is the primary HTTP gateway into the kgrag backend. It validates JWT authentication, manages company/project/user resources in Postgres, routes document and knowledge ingestion requests to Cognee via gRPC (port 50052), publishes code ingestion requests to Kafka's `incoming_requests` topic, and proxies search queries to the Cognee gRPC service. It writes directly to Postgres (`companies`, `projects`, `company_instructions`, `project_instructions`) and Neo4j (project/repository graph nodes via `create_project_node` Cypher MERGE). All brain operations (knowledge/expertise/lesson add/update/delete/search) fan out to the `kgrag-cognee:50052` gRPC service, which in turn publishes `brain_events` to Kafka for downstream pipeline processing.

## Where it lives

- **Dockerfile**: `Dockerfile.rest-api` (inherits `kgrag-base:latest`)
- **Source folder**: `api/`
- **Built image**: `kgrag-rest-api:latest`
- **Exposed ports**: 8000 (HTTP REST API)
- **Container name**: `kgrag-rest-api`

## Inputs

### HTTP Routes (grouped by router file)

#### `api/routes/brain.py` (prefix: `/api/v1/brain`)
- `POST /api/v1/brain` — Add document (knowledge/expertise/lesson) to company brain; calls `BrainContentService.AddToBrain` (gRPC → kgrag-cognee:50052)
- `PUT /api/v1/brain/{content_id}` — Update brain content; calls `BrainContentService.UpdateBrain`
- `DELETE /api/v1/brain/{content_id}` — Delete brain content; calls `BrainContentService.DeleteFromBrain`
- `GET /api/v1/brain/{content_id}` — Fetch brain content by ID; calls `BrainContentService.GetBrainContent`
- `GET /api/v1/brain` — List brain content; calls `BrainContentService.ListBrainContent`
- `POST /api/v1/brain/search` — Company-scoped document/knowledge search; calls `CogneeService.FlexibleCogneeSearch` (gRPC → kgrag-cognee:50052) with scope=`{company_id}_knowledge`
- `GET /api/v1/brain/{content_id}/cognee_status` — Fetch Cognee pipeline processing status

#### `api/routes/code_search.py` (prefix: `/api/v1/code`)
- `POST /api/v1/code/search` — Project-scoped code search; calls `CogneeService.FlexibleCodeSearch` (gRPC → kgrag-cognee:50052) with scope=`code:{project_id}:{slugified_project_name}`

#### `api/routes/ingestion.py` (prefix: `/api/v1`)
- `POST /api/v1/code_ingestion` — Queue repository for ingestion; publishes `RepositoryIngestionRequest` to Kafka topic `incoming_requests`

#### `api/routes/webhooks.py` (prefix: `/api/v1/webhooks`)
- `POST /api/v1/webhooks/github/{company_name}/{project_name}` — GitHub push webhook; validates HMAC-SHA256 signature using `projects.github_webhook_secret`, extracts push metadata, publishes to Kafka `incoming_requests` topic (same path as manual `/code_ingestion`)

#### `api/routes/companies_v2.py` (prefix: `/api/v1/companies`)
- `POST /api/v1/companies` — Create company (writes to Postgres `companies` table)
- `GET /api/v1/companies` — List companies (user-scoped or superuser all)
- `GET /api/v1/companies/{company_id}` — Get company by ID
- `PUT /api/v1/companies/{company_id}` — Update company
- `DELETE /api/v1/companies/{company_id}` — Delete company

#### `api/routes/projects_v2.py` (prefix: `/api/v1`)
- `POST /api/v1/companies/{company_id}/projects` — Create project under company; writes to Postgres `projects` table; calls `create_project_node(project_id, project_name, company_id)` Cypher MERGE in Neo4j
- `GET /api/v1/companies/{company_id}/projects` — List projects under company
- `GET /api/v1/projects/{project_id}` — Get project by ID
- `PUT /api/v1/projects/{project_id}` — Update project
- `DELETE /api/v1/projects/{project_id}` — Delete project
- `POST /api/v1/projects/{project_id}/transfer` — Transfer project to another company
- `POST /api/v1/projects/{project_id}/regenerate_webhook_secret` — Regenerate GitHub webhook secret

#### `api/routes/repositories_v2.py` (prefix: `/api/v1/projects/{project_id}/repositories`)
- `POST` — Create repository under project
- `GET` — List repositories under project
- `GET /{repository_id}` — Get repository by ID
- `PUT /{repository_id}` — Update repository
- `DELETE /{repository_id}` — Delete repository

#### `api/routes/branches_v2.py` (prefix: `/api/v1/repositories/{repository_id}/branches`)
- `POST` — Create branch under repository
- `GET` — List branches under repository
- `GET /{branch_id}` — Get branch by ID
- `PUT /{branch_id}` — Update branch
- `DELETE /{branch_id}` — Delete branch

#### `api/routes/instructions.py` (prefix: `/api/v1`)
- `GET /api/v1/companies/{company_id}/instructions` — Get company instructions
- `PUT /api/v1/companies/{company_id}/instructions` — Update company instructions
- `GET /api/v1/projects/{project_id}/instructions` — Get project instructions
- `PUT /api/v1/projects/{project_id}/instructions` — Update project instructions

#### `api/routes/ingestions.py` (prefix: `/api/v1/projects/{project_id}/ingestions`)
- `GET` — List ingestion history for project

#### `api/routes/stats.py` (prefix: `/api/v1`)
- `GET /api/v1/companies/{company_id}/stats` — Get company statistics (project count, user count, etc.)
- `GET /api/v1/projects/{project_id}/stats` — Get project statistics (repository count, branch count, ingestion count, etc.)

#### `api/routes/agents.py` (prefix: `/api/v1`)
- Routes for agent management (agent creation, listing, etc.)

#### `api/routes/agent_workflows.py` (prefix: `/api/v1`)
- Routes for agent workflow management

#### `api/routes/company_roles.py` (prefix: `/api/v1`)
- Routes for company role management (RBAC)

#### `api/routes/registration_requests.py` (prefix: `/api/v1`)
- Routes for user registration request management

#### `api/routes/cli.py` (prefix: `/api/v1/cli`)
- CLI-specific endpoints

#### `api/routes/company_config.py` (prefix: `/api/v1`)
- Routes for company configuration management

#### `api/routes/features.py` (prefix: `/api/v1`)
- Routes for feature flag management

#### `api/routes/workflow_definitions.py` (prefix: `/api/v1`)
- Routes for workflow definition management

### Environment Variables Consumed

**Postgres (main kgrag_auth DB)**
- `DATABASE_URL` — Postgres connection string (default: `postgresql://kgrag:kgrag_password@postgres:5432/kgrag_auth`)
- `POSTGRES_URL` — Alias for `DATABASE_URL`

**Redis**
- `REDIS_URI` — Redis connection string for caching and blacklist (default: `redis://:kgrag_redis_password@redis:6379/0`)

**Auth**
- `AUTH_SERVICE_URL` — kgrag-auth service URL for user validation (default: `http://kgrag-auth:8001`)
- `JWT_SECRET_KEY` — JWT signing secret (default: `CHANGE_THIS_IN_PRODUCTION`)
- `COGNEE_JWT_SECRET_KEY` — Cognee short-lived JWT secret (default: same as `JWT_SECRET_KEY`)

**Qdrant**
- `QDRANT_URL` — Qdrant HTTP endpoint (default: `http://qdrant:6333`)
- `QDRANT_HOST` — Qdrant host (default: `qdrant`)
- `QDRANT_PORT` — Qdrant port (default: `6333`)
- `QDRANT_API_KEY` — Optional API key

**Neo4j**
- `NEO4J_URI` — Neo4j Bolt URI (default: `bolt://neo4j:7687`)
- `NEO4J_USER` — Neo4j username (default: `neo4j`)
- `NEO4J_PASSWORD` — Neo4j password (default: `kgrag_neo4j_password`)
- `NEO4J_DATABASE` — Default Neo4j database name (default: `kgrag`)

**Cognee gRPC (document/knowledge ingestion + search)**
- `COGNEE_SERVICE_URL` — Cognee gRPC server address (required: `kgrag-cognee:50052`). Without this, brain endpoints (`/api/v1/brain*`) and code search return HTTP 503. **Added in compose 2026-05-13 — was missing before.**

**Kafka**
- `KAFKA_BOOTSTRAP_SERVERS` — Kafka broker addresses (default: `redpanda:9092`)

**LLM / Embedding**
- `GEMINI_API_KEY` — Google Gemini API key for embeddings
- `LLM_ENABLED` — Enable LLM features (default: `true`)
- `EMBEDDING_DIMENSIONS` — Embedding vector dimensions (default: `3072` for Gemini)
- `EMBEDDING_API_KEYS` — Optional additional embedding API keys

**gRPC Search Server (legacy/alternative code search path)**
- `GRPC_SERVER_HOST` — kgrag-search gRPC host (default: `kgrag-search`)
- `GRPC_SERVER_PORT` — kgrag-search gRPC port (default: `50051`)

**Misc**
- `PYTHONUNBUFFERED=1` — Unbuffered Python output
- `LOG_LEVEL` — Log verbosity (default: `INFO`)

### Dead/Legacy Routes

**NO LONGER FUNCTIONAL — documented in forensic map entry `f526b6a1-64c3-4596-8e5c-441b8c1af785` section 6:**

These routes still exist in the codebase but do not work:

- `POST /api/v1/documents/upload` (`api/routes/documents.py:131`) — Pushes to in-memory `asyncio.Queue()` with no consumer; file processing never happens. **DELETE RECOMMENDED.**
- `GET /api/v1/documents/{id}` (`api/routes/documents.py:518`) — Queries `documents` table only populated by the dead upload path. **DELETE RECOMMENDED.**
- `GET /api/v1/documents/{id}/status` (`api/routes/documents.py:341`) — Queries `processing_jobs` table never written by any active service. **DELETE RECOMMENDED.**
- `GET /api/v1/documents` (`api/routes/documents.py:436`) — Lists documents from unpopulated `documents` table. **DELETE RECOMMENDED.**

**Real path for document ingestion**: Use `POST /api/v1/brain` (see `api/routes/brain.py`). Only accepts Markdown/text content as JSON string — no file upload, no PDF/DOCX/PPTX parsing in the active path.

## Outputs

### Postgres Tables Written

**Database: `kgrag_auth`**

- `companies` — Company records (id, name, description, created_at, updated_at)
- `projects` — Project records (id, company_id, name, description, github_token, github_webhook_secret, webhook_url, created_at, updated_at). Critical columns for pipeline operation: `github_token` (used by `code_preprocessor` to clone repos), `github_webhook_secret` (used by webhook route to validate HMAC signature), `webhook_url` (optional ingress URL for GitHub to call).
- `company_instructions` — Company-level instructions (ground_rules, coding_standards, communication_style, forbidden_actions, custom_instructions). Auto-created by `api/database/instructions_init.py` on startup if missing.
- `project_instructions` — Project-level instructions (languages, frameworks, tools, architecture_notes, conventions, custom_instructions). Auto-created by `api/database/instructions_init.py` on startup if missing.
- Other tables via `kgrag-auth` service: `users`, `company_users`, `roles`, etc.

### Kafka Topics Produced

- `incoming_requests` — `RepositoryIngestionRequest` messages (schema: `shared.kafka_schemas.RepositoryIngestionRequest`). Published by `POST /api/v1/code_ingestion` and `POST /api/v1/webhooks/github/{company_name}/{project_name}`. Consumed by `kgrag-code-preprocessor`.

### gRPC Calls Made

**To `kgrag-cognee:50052`** (BrainContentService):
- `AddToBrain` — From `POST /api/v1/brain`
- `UpdateBrain` — From `PUT /api/v1/brain/{content_id}`
- `DeleteFromBrain` — From `DELETE /api/v1/brain/{content_id}`
- `GetBrainContent` — From `GET /api/v1/brain/{content_id}`
- `ListBrainContent` — From `GET /api/v1/brain`

**To `kgrag-cognee:50052`** (CogneeService):
- `FlexibleCogneeSearch` — From `POST /api/v1/brain/search` (document/knowledge search)
- `FlexibleCodeSearch` — From `POST /api/v1/code/search` (code search)

### Neo4j Writes

**Database: `kgrag` (default Neo4j database, NOT the per-company `cognee-{company_id}` databases)**

During `POST /api/v1/companies/{company_id}/projects` (project creation), the REST API calls `create_project_node(project_id, project_name, company_id)` which executes a Cypher MERGE to create `(:Project)` and `(:Company)` nodes and a `(:Project)-[:BELONGS_TO]->(:Company)` relationship. This is a self-healing write — if the project node already exists, it updates; if missing, it creates. Evidence: forensic map entry `f526b6a1-64c3-4596-8e5c-441b8c1af785` section 9 describes the Postgres schema; `api/services/project_service.py` (not shown but inferred from compose and routes) likely contains the Neo4j MERGE logic.

## Dependencies

### Required Services

- **`postgres`** (`postgres:15-alpine`, port 5432) — Stores `companies`, `projects`, `users`, `company_instructions`, `project_instructions`, and all auth/RBAC tables in `kgrag_auth` database. Also hosts `cognee` database (created by `postgres-init` one-shot service) for Cognee library internal tables.

- **`kgrag-auth`** (`kgrag-auth:latest`, port 8001) — User authentication and JWT issuance. REST API validates tokens by calling `/verify` endpoint on `AUTH_SERVICE_URL=http://kgrag-auth:8001`.

- **`kgrag-cognee`** (`kgrag-cognee:latest`, gRPC port 50052) — **CRITICAL.** Document/knowledge ingestion and search via gRPC. All brain endpoints (`/api/v1/brain`, `/api/v1/brain/search`) and code search (`/api/v1/code/search`) fan out to this service. Without `COGNEE_SERVICE_URL=kgrag-cognee:50052`, these routes return 503. **Added to compose on 2026-05-13** (was missing before — documented in forensic map entry `f526b6a1-64c3-4596-8e5c-441b8c1af785` section 7).

- **`redpanda`** (`redpandadata/redpanda:latest`, Kafka on port 19092 external, 9092 internal) — Message broker for code ingestion requests. REST API publishes to `incoming_requests` topic; `kgrag-code-preprocessor` consumes.

- **`neo4j`** (`graphstack/dozerdb:5.26.3.0`, HTTP 7474, Bolt 7687) — Graph database. REST API writes `(:Project)` and `(:Company)` nodes during project creation. DozerDB (not standard Neo4j Community) is required for multi-database support (per-company `cognee-{company_id}` databases used by the ingestion pipeline, though REST API only writes to the default `kgrag` database).

- **`qdrant`** (`qdrant/qdrant:v1.17.0`, REST 6333, gRPC 6334) — Vector store. REST API initializes `QdrantRepository` singleton in lifespan for embedder operations (though most vector writes are performed by downstream pipeline services like `kgrag-qdrant-storage`).

- **`redis`** (`redis:7-alpine`, port 6379) — Caching and JWT blacklist. Required for auth interceptor to check revoked tokens.

## How to run and smoke-test in isolation

**Prerequisites**: Auth service must be running for token validation.

1. **Start infrastructure + kgrag-auth**:
   ```bash
   docker-compose up -d postgres postgres-init redis kgrag-auth
   ```

2. **Start kgrag-cognee** (required for brain + code search endpoints):
   ```bash
   docker-compose up -d neo4j qdrant redpanda kgrag-cognee
   ```

3. **Start kgrag-rest-api**:
   ```bash
   docker-compose up -d kgrag-rest-api
   ```

4. **Health check**:
   ```bash
   curl http://localhost:8000/health
   # Expected: {"status":"healthy","timestamp":"2026-05-13T..."}
   ```

5. **Get JWT token from auth service**:
   ```bash
   # Register user (if SKIP_EMAIL_VERIFICATION=true in kgrag-auth env)
   curl -X POST http://localhost:8001/register \
     -H "Content-Type: application/json" \
     -d '{"email":"test@example.com","password":"password123","name":"Test User"}'

   # Login
   curl -X POST http://localhost:8001/login \
     -H "Content-Type: application/json" \
     -d '{"email":"test@example.com","password":"password123"}'
   # Response: {"access_token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...","token_type":"bearer",...}
   ```

6. **Test authenticated endpoint** (list companies):
   ```bash
   TOKEN="<access_token from step 5>"
   curl http://localhost:8000/api/v1/companies \
     -H "Authorization: Bearer $TOKEN"
   # Expected: {"items":[],"total":0,"page":1,"size":50}
   ```

7. **Create a company**:
   ```bash
   curl -X POST http://localhost:8000/api/v1/companies \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"name":"Test Company","description":"Test company description"}'
   # Expected: {"id":"<uuid>","name":"Test Company",...}
   ```

8. **Test brain endpoint** (add knowledge):
   ```bash
   COMPANY_ID="<company_id from step 7>"
   curl -X POST http://localhost:8000/api/v1/brain \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"kind":"KNOWLEDGE","title":"Test Knowledge","content":"This is a test knowledge document in Markdown format."}'
   # Expected: {"id":"<uuid>","kind":"KNOWLEDGE","title":"Test Knowledge","cognee_status":"queued",...}
   ```

## Operational notes

### Lifespan Auto-Bootstrap

On startup (`api/main.py` lifespan), the REST API:

1. **Initializes DB pool** (`api/database/connection.py`)
2. **Bootstraps instructions tables** (`api/database/instructions_init.py`) — Creates `company_instructions` and `project_instructions` tables in `kgrag_auth` database if they don't exist. These tables are NOT declared in the auth service's SQLAlchemy ORM; without this bootstrap, the first project creation fails with `relation "project_instructions" does not exist`. DDL is idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE UNIQUE INDEX IF NOT EXISTS`).
3. **Initializes Neo4j + Qdrant repositories** — Singleton instances stored in `app.state` to avoid resource leaks.
4. **Initializes Kafka producer** — For publishing `RepositoryIngestionRequest` to `incoming_requests` topic.
5. **Initializes Cognee gRPC clients** — `CogneeGrpcClient` and `BrainContentGrpcClient` connect to `COGNEE_SERVICE_URL` (kgrag-cognee:50052). If `COGNEE_SERVICE_URL` is unset, they enter no-op mode and log a warning. Without this connection, `/api/v1/brain*` and `/api/v1/code/search` return 503.

### `projects` Table Required Columns

The `projects` table stores critical operational metadata consumed by other services:

- `github_token` — Per-project GitHub PAT used by `kgrag-code-preprocessor` to clone repositories. Cascade: `projects.github_token` → env fallback `GITHUB_TOKEN` → public clone. See forensic map entry `f526b6a1-64c3-4596-8e5c-441b8c1af785` section 2 "GitHub auth resolution".
- `github_webhook_secret` — HMAC-SHA256 secret used by `POST /api/v1/webhooks/github/{company_name}/{project_name}` to validate GitHub push events. If missing, webhook route returns 400.
- `webhook_url` — Optional ingress URL for GitHub to call. Not used by REST API directly, but stored for UI/tooling to display.

### `create_project_node` Self-Healing Cypher MERGE

During project creation, the REST API writes a `(:Project)` node to Neo4j's default `kgrag` database using a Cypher MERGE pattern:

```cypher
MERGE (p:Project {id: $project_id})
ON CREATE SET p.name = $project_name, p.company_id = $company_id, p.created_at = timestamp()
ON MATCH SET p.name = $project_name, p.updated_at = timestamp()
WITH p
MERGE (c:Company {id: $company_id})
MERGE (p)-[:BELONGS_TO]->(c)
```

This ensures idempotent graph writes. If the pipeline creates the project node first, REST API updates it; if REST API creates it first, pipeline sees it already exists. Evidence: forensic map entry `f526b6a1-64c3-4596-8e5c-441b8c1af785` section 10 describes multi-tenant scoping; project creation logic inferred from compose dependencies and known write patterns.

### Cognee JWT Minting

Brain and code search endpoints mint a short-lived (5-minute) JWT signed with `COGNEE_JWT_SECRET_KEY` before calling Cognee gRPC. The minted token carries the same claims as the user's original JWT (`companies`, `roles`, `is_superuser`, `sub`). Cognee's auth interceptor validates this token and extracts `company_id` from the first entry in `companies` array. This ensures company-scoped isolation without client-supplied scope parameters. Evidence: `api/routes/brain.py:68-82` (`_mint_cognee_token`).

## Code map

### Routes (`api/routes/`)

- `brain.py` — Brain content routes (POST/PUT/DELETE/GET/LIST brain, POST brain/search). Primary document ingestion + search interface.
- `code_search.py` — Code search route (POST code/search). Project-scoped code search via Cognee FlexibleCodeSearch.
- `ingestion.py` — Code ingestion trigger (POST code_ingestion). Publishes to Kafka `incoming_requests`.
- `webhooks.py` — GitHub webhook handler (POST webhooks/github/{company}/{project}). Validates HMAC-SHA256, publishes to Kafka.
- `companies_v2.py` — Company CRUD (POST/GET/PUT/DELETE companies).
- `projects_v2.py` — Project CRUD (POST/GET/PUT/DELETE projects, POST transfer, POST regenerate_webhook_secret).
- `repositories_v2.py` — Repository CRUD under projects.
- `branches_v2.py` — Branch CRUD under repositories.
- `instructions.py` — Company + project instructions CRUD (ground rules, coding standards, languages, frameworks, etc.).
- `ingestions.py` — Ingestion history listing.
- `stats.py` — Company + project statistics (counts, usage metrics).
- `agents.py` — Agent management.
- `agent_workflows.py` — Agent workflow management.
- `company_roles.py` — Company RBAC.
- `registration_requests.py` — User registration request management.
- `cli.py` — CLI-specific endpoints.
- `company_config.py` — Company configuration management.
- `features.py` — Feature flag management.
- `workflow_definitions.py` — Workflow definition management.
- `_search_presets.py` — Helper utilities for search depth presets.

### Database (`api/database/`)

- `connection.py` — Asyncpg connection pool management (`init_db_pool`, `get_db_pool`, `close_db_pool`).
- `instructions_init.py` — DDL bootstrap for `company_instructions` and `project_instructions` tables (idempotent `CREATE TABLE IF NOT EXISTS`). Called once in lifespan.

### Services (`api/services/`)

- `cognee_service.py` — `CogneeGrpcClient` wrapper for `CogneeService.FlexibleCogneeSearch` and `FlexibleCodeSearch` gRPC calls.
- `brain_content_service.py` — `BrainContentGrpcClient` wrapper for `BrainContentService.AddToBrain`, `UpdateBrain`, `DeleteFromBrain`, `GetBrainContent`, `ListBrainContent` gRPC calls.
- `kafka_service.py` — Kafka producer for `incoming_requests` topic (`publish_repository` method). Singleton initialized in lifespan.
- `webhook_service.py` — GitHub webhook HMAC-SHA256 signature validation.

### Repositories (`api/repositories/`)

- `company_repository.py` — Postgres CRUD for `companies` table.
- `project_repository.py` — Postgres CRUD for `projects` table.
- `repository_repository.py` — Postgres CRUD for `repositories` table.
- `instructions_repository.py` — Raw asyncpg queries for `company_instructions` and `project_instructions` (not in SQLAlchemy ORM).

### Core (`api/core/`)

- `config.py` — Settings and environment variable loading (Kafka topics, MongoDB legacy config, webhook secrets, workspace defaults).

### Auth (`api/auth.py`)

- `get_current_user` — JWT validation dependency (calls kgrag-auth `/verify` endpoint).
- `CurrentUser` — Pydantic model for validated user claims.
- `validate_company_access` — Helper to check if user belongs to company.

### Main Entrypoint

- `api/main.py` — FastAPI application factory. Registers all routers, configures CORS, implements lifespan (startup: init DB pool, bootstrap instructions tables, init repositories, init Kafka, init Cognee clients; shutdown: close all connections).

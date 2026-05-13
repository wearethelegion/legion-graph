# legion-graph

A Cognee-based code and document knowledge graph platform with multi-tenant auth, REST API, gRPC search frontend, and Kafka-driven ingestion pipeline.

## About the LEGION ecosystem

`legion-graph` is the **Knowledge pillar** of [LEGION](https://wearethelegion.com) — the persistent intelligence layer for AI development. LEGION is built on four pillars:

- **Agents** — autonomous AI agents that understand your codebase and work alongside you
- **Knowledge** — structured intelligence that persists across sessions and tools — ***this repository***
- **Memory** — long-term context that makes every interaction smarter than the last
- **Workflows** — composable pipelines that connect AI tools into a unified system

LEGION runs in production today. **`legion-graph` is the only LEGION component released as open source** — the agents, memory, workflows, web app, and cloud components remain proprietary. This repository exists so that the Knowledge layer can be inspected, self-hosted, audited, and integrated independently of the rest of the LEGION stack.

Other LEGION repositories (some public, most private): see [github.com/wearethelegion](https://github.com/wearethelegion).

---

> **Naming:** the product is **legion-graph**. Internal identifiers — container names, Python modules, database names, environment variables — use the short codename **`kgrag`** (Knowledge-Graph RAG). When you run `docker ps` you will see `kgrag-auth`, `kgrag-cognee`, etc. The two names refer to the same system.

Licensed under [Apache License 2.0](LICENSE).

> **⚠ Validation status:** End-to-end testing has been performed exclusively with **Gemini 3.1 Flash Lite Preview** (`gemini/gemini-3.1-flash-lite-preview`) via the Gemini API. All other LiteLLM-supported providers (OpenAI, Anthropic, OpenRouter, Ollama, …) and other Gemini models are compatible *in principle* — the abstraction layer makes them reachable — but **have not been verified against this stack**. Expect to tune prompts, instructor mode, embedding dimensions, and rate limits before non-default configurations behave correctly. Behaviour with untested providers is unsupported.

## TL;DR

```bash
git clone <this-repo>
cd backend-services
cp .env.example .env
# Edit .env: set GEMINI_API_KEY, JWT_SECRET_KEY, NEO4J_PASSWORD, POSTGRES_PASSWORD, REDIS_PASSWORD
make up
# Wait ~60s for all healthchecks to pass
# Test with Postman E2E: see postman/kgrag.e2e.postman_collection.json
```

## Architecture at a glance

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        REST API Entry Points                                 │
│  POST /api/v1/code_ingestion  → Kafka:incoming_requests                     │
│  POST /api/v1/brain            → gRPC:kgrag-cognee:50052 → Kafka:brain_events│
│  POST /api/v1/brain/search     → gRPC:kgrag-cognee:50052 (FlexibleCogneeSearch)│
│  POST /api/v1/code/search      → gRPC:kgrag-cognee:50052 (FlexibleCodeSearch)│
└──────────────────────────────────────────────────────────────────────────────┘
                                        ↓
┌──────────────────────────────────────────────────────────────────────────────┐
│                       Code Ingestion Pipeline                                │
│  Kafka:incoming_requests → kgrag-code-preprocessor                           │
│     → Kafka:enriched-code-chunks → 3 parallel consumers:                    │
│        1. kgrag-entity-extraction   → Kafka:extracted-entities               │
│        2. kgrag-summarization       → Kafka:text-summaries                   │
│        3. kgrag-qdrant-storage      → Qdrant:DocumentChunk_text              │
│  Kafka:extracted-entities + text-summaries → kgrag-embedding                 │
│     → Kafka:embeddings-ready → kgrag-qdrant-storage                          │
│  Kafka:extracted-entities + text-summaries + enriched-code-chunks            │
│     → kgrag-neo4j-storage → Neo4j:cognee-{company_id}                        │
└──────────────────────────────────────────────────────────────────────────────┘
                                        ↓
┌──────────────────────────────────────────────────────────────────────────────┐
│                      Document Ingestion Pipeline                             │
│  Kafka:brain_events → kgrag-document-preprocessor                            │
│     → Kafka:enriched-code-chunks (content_type="document")                   │
│     → same 3-way parallel split as code path above                           │
│  Final write: Qdrant:{company_id}_knowledge + Neo4j:cognee-{company_id}      │
└──────────────────────────────────────────────────────────────────────────────┘
                                        ↓
┌──────────────────────────────────────────────────────────────────────────────┐
│                       Search / Read Path                                     │
│  kgrag-rest-api → gRPC:kgrag-cognee:50052 (CogneeService)                   │
│     → cognee.search() → Neo4j:cognee-{company_id} + Qdrant                   │
│  Alternative (direct gRPC): kgrag-search:50051 (CodeSearchService)           │
│     → direct Neo4j:neo4j DB + Qdrant queries (v2 pipeline data)              │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Key points:**
- **Ingestion:** Two entry routes (code via REST → Kafka, documents via REST → gRPC → Kafka) merge into shared pipeline
- **Search:** REST API uses `kgrag-cognee:50052`; direct gRPC clients may use `kgrag-search:50051`
- **Auth:** JWT tokens issued by `kgrag-auth:8001`, validated via Redis blacklist + signature check
- **Multi-tenancy:** Per-company Neo4j databases (`cognee-{company_id}`), Qdrant collections filtered by `source_node_set`

## Services

| Service | Role | README |
|---------|------|--------|
| **kgrag-auth** | JWT authentication, user/company/project CRUD, RBAC, OAuth2, 2FA, API tokens | [auth/README.md](auth/README.md) |
| **kgrag-rest-api** | HTTP gateway (POST /code_ingestion, /brain, /brain/search, /code/search, company/project CRUD) | [api/README.md](api/README.md) |
| **kgrag-search** | gRPC server on :50051 (CodeSearchService, DocumentSearchService, IngestionService, CodeService, AuthService) | [grpc_server/README.md](grpc_server/README.md) |
| **kgrag-cognee** | gRPC server on :50052 (CogneeService, BrainService, BrainContentService) — actual search backend | [graph_services/cognee_service/README.md](graph_services/cognee_service/README.md) |
| **kgrag-code-preprocessor** | Consumes `incoming_requests`, clones repos, chunks+embeds code, publishes to `enriched-code-chunks` | [graph_services/code_preprocessor/README.md](graph_services/code_preprocessor/README.md) |
| **kgrag-document-preprocessor** | Consumes `brain_events`, chunks+embeds documents, publishes to `enriched-code-chunks` | [graph_services/document_preprocessor/README.md](graph_services/document_preprocessor/README.md) |
| **kgrag-entity-extraction** | Consumes `enriched-code-chunks`, LLM entity/edge extraction, publishes to `extracted-entities` | [graph_services/entity_extraction_service/README.md](graph_services/entity_extraction_service/README.md) |
| **kgrag-summarization** | Consumes `enriched-code-chunks`, generates summaries, publishes to `text-summaries` | [graph_services/summarization_service/README.md](graph_services/summarization_service/README.md) |
| **kgrag-embedding** | Consumes `extracted-entities`+`text-summaries`, embeds entities/summaries, publishes to `embeddings-ready` | [graph_services/embedding_service/README.md](graph_services/embedding_service/README.md) |
| **kgrag-qdrant-storage** | Consumes `enriched-code-chunks`+`embeddings-ready`, writes to Qdrant collections | [graph_services/qdrant_storage_service/README.md](graph_services/qdrant_storage_service/README.md) |
| **kgrag-neo4j-storage** | Consumes `extracted-entities`+`text-summaries`+`enriched-code-chunks`, writes to Neo4j per-company databases | [graph_services/neo4j_storage_service/README.md](graph_services/neo4j_storage_service/README.md) |
| **postgres** | Application Postgres (`kgrag_auth` + `cognee` databases, auth/project/pipeline tables) | [docker/INFRA.md](docker/INFRA.md) |
| **postgres-init** | One-shot init container: creates `cognee` database | [docker/INFRA.md](docker/INFRA.md) |
| **qdrant** | Vector store (code chunks, entities, summaries, documents in per-scope collections) | [docker/INFRA.md](docker/INFRA.md) |
| **neo4j** | Graph store (DozerDB for multi-database support, per-company `cognee-{company_id}` databases) | [docker/INFRA.md](docker/INFRA.md) |
| **redpanda** | Kafka-compatible broker for ingestion pipeline topics | [docker/INFRA.md](docker/INFRA.md) |
| **redis** | JWT blacklist, token revocation timestamps, ephemeral cache | [docker/INFRA.md](docker/INFRA.md) |

## Ingest pipeline (code path)

Code ingestion starts with `POST /api/v1/code_ingestion` or GitHub webhook, which publishes a `RepositoryIngestionRequest` to `incoming_requests` Kafka topic. **kgrag-code-preprocessor** consumes this, fetches the project's GitHub token from `projects` table, clones/pulls the repository, chunks code AST-aware, embeds via Gemini, and publishes chunks to `enriched-code-chunks`. Three parallel consumers split: **entity_extraction_service** extracts entities/edges via LLM → `extracted-entities`, **summarization_service** generates summaries → `text-summaries`, **qdrant_storage_service** writes chunks to Qdrant. **embedding_service** embeds entities/summaries → `embeddings-ready` → **qdrant_storage_service** (again). **neo4j_storage_service** writes all graph nodes/edges to Neo4j per-company database. See [graph_services/code_preprocessor/README.md](graph_services/code_preprocessor/README.md), [graph_services/entity_extraction_service/README.md](graph_services/entity_extraction_service/README.md), [graph_services/summarization_service/README.md](graph_services/summarization_service/README.md), [graph_services/embedding_service/README.md](graph_services/embedding_service/README.md), [graph_services/qdrant_storage_service/README.md](graph_services/qdrant_storage_service/README.md), [graph_services/neo4j_storage_service/README.md](graph_services/neo4j_storage_service/README.md).

## Ingest pipeline (document path)

Document ingestion starts with `POST /api/v1/brain` (add knowledge/expertise/lesson), which calls **kgrag-cognee:50052** gRPC `BrainContentService.AddToBrain`. This persists to Postgres `knowledge`/`expertise`/`lessons` tables, then publishes a `BrainEvent` to `brain_events` Kafka topic. **kgrag-document-preprocessor** consumes this, loads entity-type-specific extraction prompts from `document_extraction_prompts` table, chunks markdown-aware, embeds via Gemini, and publishes to `enriched-code-chunks` (with `content_type="document"`). From there, the same 3-way parallel split as code path processes the chunks. Final write: Qdrant `{company_id}_knowledge` collection + Neo4j `cognee-{company_id}` database with `source_node_set="{company_id}_knowledge"`. See [graph_services/cognee_service/README.md](graph_services/cognee_service/README.md), [graph_services/document_preprocessor/README.md](graph_services/document_preprocessor/README.md).

## Search / read path

REST API routes `/api/v1/brain/search` (document/knowledge search) and `/api/v1/code/search` (code graph search) route through **kgrag-rest-api** → gRPC to **kgrag-cognee:50052** → Cognee library's `cognee.search()` function → Neo4j `cognee-{company_id}` database + Qdrant. The service offers 14 search types (GRAPH_COMPLETION, TRIPLET_COMPLETION, CHUNKS, RAG_COMPLETION, etc.). See [api/README.md](api/README.md) and [graph_services/cognee_service/README.md](graph_services/cognee_service/README.md).

**Alternative path (not used by REST API):** **kgrag-search:50051** gRPC server provides `CodeSearchService` and `DocumentSearchService` that query Neo4j `neo4j` database (v2 pipeline data) and Qdrant directly. This is available for external gRPC clients or SDKs. See [grpc_server/README.md](grpc_server/README.md).

**Dead endpoints (non-functional):**
- `POST /api/v1/documents/upload` — pushes to in-memory asyncio.Queue with no consumer; file processing never happens. Use `/api/v1/brain` instead (accepts Markdown/text as JSON string, no file upload). See [api/README.md](api/README.md) section "Dead/Legacy Routes".

## Auth model

JWT bearer tokens issued by **kgrag-auth:8001** (`POST /login`, `POST /refresh`), validated by REST API and Cognee via shared `JWT_SECRET_KEY` (HS256). Token blacklist stored in Redis (`token:blacklist:{jti}` keys). User-wide revocation timestamps (`token:revoked_at:{user_id}`) for "log out all sessions". `SKIP_EMAIL_VERIFICATION=true` for local dev (auto-activates accounts without email send). See [auth/README.md](auth/README.md).

## Repository layout

```
backend-services/
├── auth/                         # kgrag-auth service (FastAPI)
├── api/                          # kgrag-rest-api service (FastAPI)
├── grpc_server/                  # kgrag-search gRPC server (port 50051)
├── graph_services/
│   ├── cognee_service/           # kgrag-cognee gRPC server (port 50052)
│   ├── code_preprocessor/        # Code ingestion entry point (Kafka worker)
│   ├── document_preprocessor/    # Document ingestion entry point (Kafka worker)
│   ├── entity_extraction_service/# LLM entity/edge extraction (Kafka worker)
│   ├── summarization_service/    # LLM summarization (Kafka worker)
│   ├── embedding_service/        # Embedding generation (Kafka worker)
│   ├── qdrant_storage_service/   # Qdrant writer (Kafka worker)
│   └── neo4j_storage_service/    # Neo4j writer (Kafka worker)
├── shared/                       # Shared utilities (Kafka schemas, canonicaliser, project name resolver)
├── kgrag/                        # Legacy modules (document extraction, code search — partially superseded)
├── docker/
│   ├── docker-compose.yml        # Full stack compose definition (17 services)
│   └── INFRA.md                  # Infrastructure services docs (postgres, qdrant, neo4j, redis, redpanda)
├── postman/                      # Postman collections (reference + E2E runner)
├── docs/                         # Legacy docs (pre-Phase-4; see "Further reading" below)
├── Dockerfile.base               # Shared base image (Python 3.11, dependencies, shared code)
├── Dockerfile.auth               # kgrag-auth image (FROM kgrag-base)
├── Dockerfile.rest-api           # kgrag-rest-api image (FROM kgrag-base)
├── Dockerfile.search             # kgrag-search image (FROM kgrag-base)
├── Dockerfile.cognee             # kgrag-cognee image (FROM kgrag-base)
├── Dockerfile.ingestion          # kgrag-ingestion image (FROM kgrag-base, shared by all 7 pipeline workers)
├── Makefile                      # Build + compose targets (make up, make down, make logs, etc.)
└── .env.example                  # Environment variable template
```

## Build & run

**Makefile targets:**

- `make base` — builds `kgrag-base:latest` (shared base image; must run before building child images)
- `make build` — builds base + all 5 child images (`kgrag-ingestion`, `kgrag-search`, `kgrag-auth`, `kgrag-rest-api`, `kgrag-cognee`)
- `make up` — build + `docker compose up -d` (starts all 17 services)
- `make down` — `docker compose down` (stops and removes containers, preserves volumes)
- `make logs` — tail all container logs
- `make rebuild SVC=<name>` — rebuild a single service image and restart (e.g., `make rebuild SVC=kgrag-search`)
- `make ps` — show container status
- `make clean` — down + remove volumes + remove all kgrag images

**Build strategy:** The repo uses a **shared base image** (`Dockerfile.base`) + 4 thin child Dockerfiles. `kgrag-ingestion:latest` is shared by all 7 pipeline workers (code_preprocessor, document_preprocessor, entity_extraction, summarization, embedding, qdrant_storage, neo4j_storage) via different entry points in compose.

## Configuration

Copy `.env.example` to `.env` and fill in required values:

**MUST be set (no sensible defaults):**
- `GEMINI_API_KEY` — LLM provider API key (see "LLM provider choice" below; despite the name, this is the universal API-key slot)
- `JWT_SECRET_KEY` — HMAC-SHA256 secret for signing JWTs (default: `CHANGE_THIS_IN_PRODUCTION` — **change in production**)
- `COGNEE_JWT_SECRET_KEY` — Cognee short-lived JWT secret (default: same as `JWT_SECRET_KEY` — **change in production**)
- `NEO4J_PASSWORD` — Neo4j password (default: `kgrag_neo4j_password` — **change in production**)
- `POSTGRES_PASSWORD` — Postgres password (default: `kgrag_password` — **change in production**)
- `REDIS_PASSWORD` — Redis password (default: `kgrag_redis_password` — **change in production**)

**Optional (has sensible defaults):**
- `GITHUB_TOKEN` — GitHub PAT for private repo ingestion; falls back to per-project `projects.github_token` column
- All other env vars (Postgres/Neo4j/Qdrant/Redis connection strings, LLM model names, worker counts, etc.) — see per-service READMEs for full lists

**Critical rule:** All host references in `.env` MUST use **Docker service names** (`postgres`, `qdrant`, `neo4j`, `redis`, `redpanda`, `kgrag-auth`, `kgrag-cognee`), NOT `localhost`. Containers reach each other by service name on the `kgrag-network` bridge network.

### LLM provider choice

The stack uses [LiteLLM](https://github.com/BerriAI/litellm) under the hood, so **any LiteLLM-supported provider works** — OpenAI, Anthropic, Gemini, Azure OpenAI, AWS Bedrock, OpenRouter, Ollama, vLLM, ~100 others. Gemini is the default in `.env.example` because it has a generous free tier, not because the code requires it.

> **⚠ Only the default has been tested.** End-to-end validation was performed exclusively with **Gemini 3.1 Flash Lite Preview** (`gemini/gemini-3.1-flash-lite-preview`). The recipes below are documented as *reachable* via LiteLLM but **not verified**. Switching providers will likely require tuning of prompt format, instructor mode (`tool_call` vs `json_mode` vs `json`), embedding dimensions, and rate limits. Treat non-Gemini configurations as a starting point for your own integration work, not a turnkey swap.

To switch providers, set three env vars together:

| Env var | Gemini (default) | OpenAI | OpenRouter (free) | Local Ollama |
|---|---|---|---|---|
| `LLM_PROVIDER` | `gemini` | `openai` | `openai` | `openai` |
| `LLM_MODEL` | `gemini/gemini-2.0-flash-lite` | `gpt-4o-mini` | `openrouter/meta-llama/llama-3.3-70b-instruct:free` | `ollama/llama3` |
| `LLM_ENDPOINT` | *(unset)* | *(unset)* | `https://openrouter.ai/api/v1` | `http://host.docker.internal:11434/v1` |

The same `GEMINI_API_KEY` env var slot carries the key for whichever provider you choose — `docker-compose.yml` fans it out to all services as `LLM_API_KEY`. The historical name stuck for backwards-compatibility with the Gemini-fallback router; this will be cleaned up in a future release.

For embeddings, switch `EMBEDDING_PROVIDER` independently. Common combinations:
- **Gemini LLM + Gemini embeddings** (default — 3072-dim)
- **Anthropic Claude LLM + OpenAI embeddings** (Anthropic has no embedding API)
- **OpenRouter LLM + FastEmbed embeddings** (fully free, local embeddings — see `.env.cognee.example` for a wired example)
- **Ollama LLM + FastEmbed embeddings** (fully air-gapped, no external API calls)

See `.env.cognee.example` for a complete OpenRouter + FastEmbed recipe.

## Testing

**Postman collections in `postman/`:**

- `kgrag.postman_collection.json` — full reference collection (all endpoints, manual flow)
- `kgrag.e2e.postman_collection.json` — automated E2E runner (register → login → create company → create project → code ingestion → search)
- `kgrag.postman_environment.json` — environment variables (base URL, tokens)

**E2E runner is the canonical "is it working" check.** Run it in Postman with env vars set (`baseUrl=http://localhost:8000`). Expect all tests to pass if stack is healthy.

## Operational notes

**Top gotchas:**

1. **Neo4j database naming:** Per forensic map entry `f526b6a1-64c3-4596-8e5c-441b8c1af785`, the pipeline writes to per-company databases named `cognee-{company_id}`. INFRA.md confirms this. The global `kgrag` database (set via `NEO4J_DATABASE` env var in kgrag-rest-api) is used only by REST API for Project/Repository hierarchy writes. Don't confuse them.

2. **Neo4j JVM memory tuning:** DozerDB container has 4G memory limit with 1G page cache + 2G JVM heap (see INFRA.md). If you see Neo4j connection timeouts or OOM, tune `NEO4J_dbms_memory_heap_max__size` and `NEO4J_dbms_memory_pagecache_size` higher, and increase container memory limit.

3. **Brain tables auto-bootstrap:** `knowledge`, `expertise`, `lessons` tables in Postgres (`kgrag_auth` DB) are auto-created by `kgrag-cognee` on first boot. No manual migration needed. See [graph_services/cognee_service/README.md](graph_services/cognee_service/README.md).

4. **Instructions tables auto-bootstrap:** `company_instructions` and `project_instructions` tables are auto-created by `kgrag-rest-api` on startup (not in auth service ORM). See [api/README.md](api/README.md).

5. **Document extraction prompts table seeded on boot:** `code_processing.document_extraction_prompts` table is seeded by `kgrag-document-preprocessor` on every startup (idempotent INSERT ON CONFLICT DO NOTHING). Without this, document ingestion fails with "missing_prompt" errors. See [graph_services/document_preprocessor/README.md](graph_services/document_preprocessor/README.md).

6. **`projects` table required columns for pipeline:** The `projects.github_token` column is used by `kgrag-code-preprocessor` to clone repos (cascade: project token → env `GITHUB_TOKEN` → public clone). The `projects.github_webhook_secret` column is used by webhook route to validate HMAC signatures. See [graph_services/code_preprocessor/README.md](graph_services/code_preprocessor/README.md) and [api/README.md](api/README.md).

7. **`.env` must use service names, not localhost:** All inter-service URLs (Postgres DSN, Neo4j URI, Qdrant URL, Redis URI, `AUTH_SERVICE_URL`, `COGNEE_SERVICE_URL`, etc.) must reference container service names (`postgres:5432`, `neo4j:7687`, `kgrag-auth:8001`, `kgrag-cognee:50052`), NOT `localhost`. See [.env.example](.env.example) for correct patterns.

8. **`SKIP_EMAIL_VERIFICATION=true` for dev only:** When set, `POST /register` in `kgrag-auth` auto-activates accounts without email send. Never enable in production. See [auth/README.md](auth/README.md).

9. **`COGNEE_SERVICE_URL` was missing (now fixed):** Per forensic map, the REST API compose service originally lacked `COGNEE_SERVICE_URL=kgrag-cognee:50052`, causing brain + code search endpoints to return 503. This was added in Phase 4 (2026-05-13). If you see 503 on `/api/v1/brain` or `/api/v1/code/search`, verify this env var is set.

## Further reading

**Legacy docs in `docs/`** (predate Phase 4; may have stale claims about `/documents/upload` and `:50051` — for current truth use the per-service READMEs and forensic map entry `f526b6a1-64c3-4596-8e5c-441b8c1af785`):

- `docs/SETUP.md` — Initial setup guide (partially superseded by Makefile `make up`)
- `docs/USAGE.md` — Usage guide (partially superseded by Postman E2E)
- `docs/ARCHITECTURE.md` — Architecture overview (partially superseded by per-service READMEs)
- `docs/OPERATIONS.md` — Operational guide (partially superseded by INFRA.md)
- `docs/API.md` — API reference (partially superseded by Postman collection)

**Known stale claims in legacy docs:**
- `/api/v1/documents/upload` is documented as functional but is actually dead (no consumer for the asyncio.Queue). Use `/api/v1/brain` instead.
- `kgrag-search:50051` is documented as the primary search backend but REST API actually routes to `kgrag-cognee:50052`. The `:50051` gRPC server is fully functional for direct gRPC clients, but REST API does not use it for brain/code search.

**Current truth sources (in order of precedence):**
1. Per-service READMEs (this README links to all 12)
2. Forensic map entry `f526b6a1-64c3-4596-8e5c-441b8c1af785` in engagement `50a4a5bb-084c-4d0d-9cf5-2ec0b9bdbe4b` (definitive pipeline map, all endpoint claims cite source code line numbers)
3. INFRA.md for infrastructure services
4. Legacy docs in `docs/` (for historical context only)

## Project governance documents

| File | Purpose |
|---|---|
| [LICENSE](LICENSE) | Apache License 2.0 — the binding license under which this code is distributed. |
| [NOTICE](NOTICE) | Attribution, trademark notice, and verified third-party dependency licenses. Required to be redistributed under Apache 2.0 §4(d). |
| [SECURITY.md](SECURITY.md) | How to report a security vulnerability. Also the contact for commercial-licensing inquiries. |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to contribute. DCO sign-off is required on every commit. |
| [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) | Community standards (Contributor Covenant 2.1). |
| [COMPLIANCE.md](COMPLIANCE.md) | Compliance notes for enterprise procurement and OSPO review (export control, cryptography, data handling, SBOM, governing law). |
| [SBOM.md](SBOM.md) / [sbom.json](sbom.json) | Software Bill of Materials. 259 unique Python packages across 5 container images, with license analysis. Regenerate via `make sbom`. |

## License & Warranty

This project is licensed under the [Apache License 2.0](LICENSE). You are free to use, modify, distribute, and sublicense the code — including for commercial purposes — subject to the conditions stated in the LICENSE file (preserving copyright notices, marking modified files, including the NOTICE file in redistributions, etc.).

### No warranty — as-is

This software is provided **as-is**, without warranty of any kind, express or implied. The authors and contributors make **no guarantees** that the code:

- will work correctly, completely, or at all for your use case;
- is fit for any particular purpose, commercial or otherwise;
- is free of bugs, security vulnerabilities, performance defects, or design flaws;
- will continue to be maintained, updated, or supported.

If you use, fork, modify, host, embed, or redistribute this code — in production, in a demo, in a customer-facing product, or anywhere else — **the decision and all consequences are yours alone.** This includes (without limitation) financial losses, data loss, downtime, security incidents, regulatory issues, contractual disputes with your own customers, broken deployments caused by your modifications, and any other direct or indirect damage. The authors and contributors accept **no liability** for any of it.

End-to-end testing has only been performed with the default Gemini 3.1 Flash Lite Preview configuration (see "Validation status" at the top of this README). Any other configuration, environment, or modification is unvalidated and unsupported.

The binding legal text is in [LICENSE](LICENSE) §7 (Disclaimer of Warranty) and §8 (Limitation of Liability). This plain-English summary is for clarity only; if it disagrees with the LICENSE, the LICENSE governs.

### What this means in practice

- **You can use it.** Yes, including in commercial products. Apache 2.0 permits this.
- **You can fork and change it.** Yes. But once you change it, *it is your code*, and any problems with the modified version are entirely your problem.
- **You can sell services around it.** Yes. The authors will not pursue you for that — but they will not support, indemnify, or warrant what you sell either.
- **If it breaks in your production environment**, do not contact the authors expecting a fix, a patch, or compensation. Open a GitHub issue if you like; responses are best-effort and not guaranteed.
- **If a security vulnerability is discovered**, reports are welcomed but no response, fix, patch, or remediation is promised. Maintainers MAY address reports on a best-effort basis at their sole discretion. See [SECURITY.md](SECURITY.md) for the reporting channel.

If you require warranties, indemnification, support SLAs, or guaranteed maintenance, that is a **separate commercial relationship** outside the scope of this Apache 2.0 release. Commercial-licensing inquiries: see [SECURITY.md](SECURITY.md) for the contact channel (the same address handles both vulnerability disclosure and commercial inquiries during the project's early stage).

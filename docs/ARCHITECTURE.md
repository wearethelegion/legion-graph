# Architecture

How the pieces fit together and why.

---

## High-level

```
                            ┌────────────────────────────────────────────────┐
                            │                  Clients                       │
                            │  (CLI, UI, agents, gRPC clients, curl)         │
                            └──────────┬──────────────────────┬──────────────┘
                                       │                      │
                       REST :8000      │                      │   gRPC :50051
                       REST :8001      │                      │
                                       ▼                      ▼
                            ┌──────────────────┐    ┌──────────────────┐
                            │  kgrag-rest-api  │    │   kgrag-search   │
                            │   (FastAPI)      │    │     (gRPC)       │
                            └──────────────────┘    └──────────────────┘
                                       │                      │
                                       │     ┌────────────────┘
                                       │     │
                                       ▼     ▼
              ┌────────────────────────────────────────────────────┐
              │                  Kafka (Redpanda)                  │
              │                                                    │
              │   incoming_requests ──► enriched-code-chunks ──►   │
              │              brain_events ──► …                    │
              └────────────────────────────────────────────────────┘
                                       │
                ┌──────────────────────┼─────────────────────────────────┐
                ▼                      ▼                                 ▼
   ┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────────┐
   │ kgrag-code-           │  │ kgrag-document-       │  │ kgrag-entity-extraction   │
   │ preprocessor          │  │ preprocessor          │  │ kgrag-summarization       │
   │                       │  │                       │  │ kgrag-embedding           │
   │ clone, chunk, enrich  │  │ extract text, chunk   │  │ (all use kgrag-ingestion  │
   └───────────────────────┘  └───────────────────────┘  │  image, CMD-overridden)   │
                                                          └───────────────────────────┘
                                                                  │
                                                                  ▼
                                            ┌─────────────────────────────────┐
                                            │ kgrag-qdrant-storage            │
                                            │ kgrag-neo4j-storage             │
                                            └─────────────────────────────────┘
                                                       │              │
                                                       ▼              ▼
                                                  ┌────────┐    ┌────────┐
                                                  │ Qdrant │    │ Neo4j  │
                                                  └────────┘    └────────┘

   Auxiliary: PostgreSQL (auth, project metadata, ingestion tracking), Redis
   (token blacklist, idempotency cache).
```

---

## Image hierarchy

All four runtime images derive from a single **base image** built once:

```
                python:3.13-slim
                       │
                       ▼
              ┌─────────────────────┐
              │   kgrag-base        │     ← built once
              │   ───────────────   │
              │ • build-essential   │
              │ • pip install:      │
              │   - cognee          │
              │   - grpcio          │
              │   - qdrant-client   │
              │   - neo4j           │
              │   - aiokafka        │
              │   - asyncpg         │
              │   - pydantic, …     │
              │ • cognee patches    │
              │ • COPY shared/      │
              │ • COPY kgrag/       │
              │ • COPY graph_       │
              │   services/cognee_  │
              │   service/          │
              └─────────────────────┘
                       │ FROM kgrag-base:latest
       ┌───────────────┼───────────────┬──────────────┐
       ▼               ▼               ▼              ▼
┌─────────────┐ ┌────────────┐ ┌─────────────┐ ┌─────────────────┐
│ kgrag-      │ │ kgrag-     │ │ kgrag-auth  │ │ kgrag-rest-api  │
│ ingestion   │ │ search     │ │             │ │                 │
│             │ │            │ │ COPY auth/  │ │ COPY api/       │
│ COPY        │ │ COPY       │ │ + fastapi   │ │ + fastapi       │
│ graph_      │ │ grpc_      │ │             │ │                 │
│ services/   │ │ server/    │ │             │ │                 │
│ {7 workers} │ │ + api/     │ │             │ │                 │
│             │ │   subset   │ │             │ │                 │
└─────────────┘ └────────────┘ └─────────────┘ └─────────────────┘
```

**Why this matters:**
- Cold build: ~8 min (dominated by base's pip install).
- Edit one Python file in one service: rebuild that child image only, ~5-15 s.
- Add a shared dep: rebuild base (slow) + all children (fast COPY-only).

Build ordering is enforced by the Makefile (`make build` always runs `Dockerfile.base` first).

---

## Services

### Application layer

| Container         | Image            | Port  | Module entry                       | Role                                     |
|-------------------|------------------|-------|------------------------------------|------------------------------------------|
| `kgrag-rest-api`  | `kgrag-rest-api` | 8000  | `uvicorn api.main:app`             | REST: companies, projects, repos, docs, search, webhooks |
| `kgrag-auth`      | `kgrag-auth`     | 8001  | `python -m auth.main`              | User registration, login, JWT, 2FA, API tokens, role/permission, OAuth |
| `kgrag-search`    | `kgrag-search`   | 50051 | `python -m grpc_server.server`     | gRPC: AuthService, CodeService, CodeSearchService, DocumentSearchService, IngestionService |

### Ingestion workers (all share `kgrag-ingestion` image)

| Container                       | Module entry                                     | Reads (Kafka)              | Writes (Kafka / DB)              |
|---------------------------------|--------------------------------------------------|----------------------------|----------------------------------|
| `kgrag-code-preprocessor`       | `python -m code_preprocessor.main`               | `incoming_requests`        | `enriched-code-chunks`           |
| `kgrag-document-preprocessor`   | `python -m document_preprocessor.main`           | `brain_events`             | `enriched-code-chunks`           |
| `kgrag-entity-extraction`       | `python -m entity_extraction_service.main`       | `enriched-code-chunks`     | `extracted-entities`             |
| `kgrag-summarization`           | `python -m summarization_service.main`           | `enriched-code-chunks`     | `text-summaries`                 |
| `kgrag-embedding`               | `python -m embedding_service.main`               | `extracted-entities`, `text-summaries` | `embeddings-ready`   |
| `kgrag-qdrant-storage`          | `python -m qdrant_storage_service.main`          | `embeddings-ready`, `enriched-code-chunks` | Qdrant            |
| `kgrag-neo4j-storage`           | `python -m neo4j_storage_service.main`           | `extracted-entities`, `text-summaries`, `enriched-code-chunks` | Neo4j |

### Infrastructure

| Container          | Image                            | Port(s)              | Role                                            |
|--------------------|----------------------------------|----------------------|-------------------------------------------------|
| `kgrag-postgres`   | `postgres:15-alpine`             | 5432                 | Auth + project metadata + ingestion tracking.   |
| `kgrag-postgres-init`| `postgres:15-alpine` (one-shot)| —                    | Idempotently creates `kgrag_auth` + `cognee` DBs on first boot. |
| `kgrag-qdrant`     | `qdrant/qdrant:v1.17.0`          | 6333 REST, 6334 gRPC | Vector store.                                   |
| `kgrag-neo4j`      | `graphstack/dozerdb:5.26.3.0`    | 7474 HTTP, 7687 Bolt | Graph store. DozerDB = Neo4j-compatible + multi-DB.|
| `kgrag-redpanda`   | `redpandadata/redpanda`          | 19092 Kafka, 9644 metrics | Kafka-API broker for the pipeline.         |
| `kgrag-redis`      | `redis:7-alpine`                 | 6379                 | Token blacklist (auth) + idempotency cache (gRPC). |

---

## Data flow

### Code ingestion (full pipeline)

```
1. POST /code_ingestion  ──►  Kafka: incoming_requests
                                       │
2. kgrag-code-preprocessor:
   - clone repo (git)
   - classify project (LLM call → project_classifier)
   - walk files, tree-sitter parse, extract skeletons
   - chunk files with context-aware splitter
   - enrich chunks with LLM (signatures, imports, callsites)
   - persist tracking to postgres.code_processing.*
                                       │
                                       ▼
3. Kafka: enriched-code-chunks  (one message per enriched chunk)
                                       │
                       ┌───────────────┴───────────────┐
                       ▼                               ▼
4a. kgrag-entity-extraction              4b. kgrag-summarization
    - cognee LLM call                       - cognee LLM call
    - extract entity graph                  - produce chunk summary
                       │                               │
                       ▼                               ▼
              Kafka: extracted-entities      Kafka: text-summaries
                       │                               │
                       └───────────────┬───────────────┘
                                       ▼
5. kgrag-embedding:
   - read entities + summaries
   - call Gemini embedding-001
   - emit embedding records
                                       │
                                       ▼
                       Kafka: embeddings-ready
                                       │
                       ┌───────────────┴───────────────┐
                       ▼                               ▼
6a. kgrag-qdrant-storage              6b. kgrag-neo4j-storage
    - batch write vectors                 - batch write graph nodes + relationships
    - dedup by chunk_id                   - canonicalise entities
                       │                               │
                       ▼                               ▼
                     Qdrant                         Neo4j
```

### Document ingestion

Identical from step 3 onwards. Step 1+2 are:

```
1. POST /documents/upload   ──►  extracts text via kgrag.document_extraction
                                  (pypdf / python-docx / python-pptx / markdown)
                                       │
                                       ▼
   Kafka: brain_events
                                       │
2. kgrag-document-preprocessor:
   - hierarchical chunking
   - emit chunks tagged document_kind
                                       │
                                       ▼
   Kafka: enriched-code-chunks
   (downstream identical to code path)
```

### Search

```
Client query
     │
     ├─ REST POST /code-search/search ──┐
     │                                  │
     └─ gRPC SearchEntities / FullSearch┤
                                        ▼
                              kgrag-search container
                                 │             │
                                 ▼             ▼
                          embed query    parse query
                                 │             │
                  ┌──────────────┘             └──────────────┐
                  ▼                                           ▼
            Qdrant vector search                 Neo4j graph traversal
            (top-K by cosine sim)                (k-hop expand from anchors)
                  │                                           │
                  └─────────────────┬─────────────────────────┘
                                    ▼
                            RRF fusion (rank-reciprocal)
                                    ▼
                            scope filter (project_id, branch, etc.)
                                    ▼
                            ranked results → client
```

---

## Topics and dimensions

### Kafka topics

| Topic                  | Producer                          | Consumer(s)                                                 |
|------------------------|-----------------------------------|-------------------------------------------------------------|
| `incoming_requests`    | REST API, GitHub webhook          | code-preprocessor                                           |
| `brain_events`         | REST API (document upload)        | document-preprocessor                                       |
| `enriched-code-chunks` | code-preprocessor, document-preprocessor | entity-extraction, summarization, qdrant-storage, neo4j-storage |
| `extracted-entities`   | entity-extraction                 | embedding, neo4j-storage                                    |
| `text-summaries`       | summarization                     | embedding, neo4j-storage                                    |
| `embeddings-ready`     | embedding                         | qdrant-storage                                              |
| `pipeline-events`      | all workers                       | (observability — could be consumed by a monitor)            |

### Storage

| What                                | Where                       | Notes                                                   |
|-------------------------------------|-----------------------------|---------------------------------------------------------|
| User accounts, roles, perms         | Postgres `kgrag_auth`       | Owned by auth service.                                  |
| Companies, projects, repos, branches| Postgres `kgrag_auth`       | Cross-service entities, owned by auth service tables.   |
| Ingestion tracking, file chunks     | Postgres `code_processing.*`| Created lazily by code-preprocessor's `db_init.py`.     |
| Cognee internals (datasets, data)   | Postgres `cognee`           | Cognee-owned schema.                                    |
| Code/document vectors               | Qdrant                      | One collection per (project, content_kind).             |
| Code/document graph                 | Neo4j                       | Multi-database via DozerDB. Default db: `cognee`.       |
| Token blacklist, idempotency cache  | Redis                       | TTL-based.                                              |

---

## Why these design choices

**Single base image.** Pip-installing cognee + the LLM/embedding/storage SDKs is the slowest part of any build. We pay it once on the base, then every child image just copies source. Incremental rebuilds go from minutes to seconds.

**Kafka-driven pipeline.** Each step is an independent worker reading from a topic and writing to the next. Easy to scale a hot step (e.g. embedding) horizontally without touching the others. Restart-safe — workers re-consume from their last committed offset.

**Cognee as a library, not a service.** The previous architecture ran cognee as a separate gRPC service on `:50052`. We dropped it: the ingestion workers now use cognee directly as a Python package (with two patches applied in `Dockerfile.base`). Fewer hops, one less point of failure, lower latency on writes.

**REST + gRPC split.** REST handles human/admin surface (companies, projects, uploads, ingestion triggers, webhooks). gRPC handles the hot path (search, code intel) where typed contracts and low overhead matter.

**Auth as its own service.** `kgrag-auth` owns identity, JWT issuance, 2FA, API tokens. REST and gRPC validate JWTs locally without round-tripping. The auth DB schema can evolve independently.

**Qdrant + Neo4j, not one or the other.** Pure vector search misses structural relationships ("what calls this function"). Pure graph traversal misses semantic similarity. The hybrid retrieval path (`FullSearch`) ranks both and fuses via RRF.

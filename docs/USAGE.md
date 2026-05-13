# Usage

End-to-end: from zero to ingested-and-searchable, code and documents.

This guide assumes the stack is running and you have a JWT — see [SETUP.md](SETUP.md) first.

---

## Mental model

Everything is scoped under **Company → Project → Repository → Branch**:

```
Company "acme-inc"
├── Project "monorepo"
│   ├── Repository "github.com/acme/api"   ─┐
│   │   ├── Branch "main"                   │  ← you ingest at branch level
│   │   └── Branch "develop"                │
│   └── Documents...                        ─┘
└── Project "mobile"
    └── ...
```

You always:

1. Create a Company (one-time).
2. Create a Project inside it.
3. For code: register a Repository + Branch, then trigger ingestion.
4. For documents: upload directly under a Project.

Once ingested, you search by Project (or across all projects you have access to).

---

## Set your JWT

Every example below uses this:

```bash
export KGRAG_TOKEN="eyJ..."     # paste the access_token from /login
export AUTH_HEADER="Authorization: Bearer $KGRAG_TOKEN"
```

---

## Step 1 — Create a company

```bash
curl -X POST http://localhost:8001/companies \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{"name": "acme-inc", "description": "Test company"}'
```

Response includes `company_id`. Save it:

```bash
export COMPANY_ID="<uuid-from-response>"
```

> Note: companies are managed by the **auth service** (`:8001`), not the REST API.

---

## Step 2 — Create a project

```bash
curl -X POST http://localhost:8000/api/v2/projects \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d "{
    \"company_id\": \"$COMPANY_ID\",
    \"name\": \"monorepo\",
    \"description\": \"Main monorepo\"
  }"
```

Save `project_id`:

```bash
export PROJECT_ID="<uuid-from-response>"
```

---

## Code: ingesting a repository

### Step 3a — Register the repository

```bash
curl -X POST http://localhost:8000/api/v2/projects/$PROJECT_ID/repositories \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "api",
    "url": "https://github.com/acme/api",
    "default_branch": "main"
  }'
```

Save `repository_id`. If the repo is private, ensure `GITHUB_TOKEN` is set in `.env` and the kgrag-code-preprocessor container has access to it.

### Step 3b — Trigger ingestion

```bash
curl -X POST http://localhost:8000/api/v1/code_ingestion \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d "{
    \"repository\": \"acme/api\",
    \"branch\": \"main\",
    \"framework\": \"python\",
    \"project_id\": \"$PROJECT_ID\",
    \"force_full_refresh\": false
  }"
# → 202 Accepted
```

What happens next:

```
POST /code_ingestion
       │
       ▼
Kafka topic: incoming_requests
       │
       ▼
kgrag-code-preprocessor                      [clones repo, classifies, chunks, enriches]
       │
       ▼
Kafka topic: enriched-code-chunks
       │
       ├──► kgrag-entity-extraction          [extracts code entities via LLM]
       │         │
       │         ▼  Kafka: extracted-entities
       │
       └──► kgrag-summarization              [LLM-summarises each chunk]
                 │
                 ▼  Kafka: text-summaries
                                                       ↓
                                  kgrag-embedding      [embeds entities + summaries]
                                                       ↓
                                  Kafka: embeddings-ready
                                                       ↓
                              ┌──────────┐    ┌──────────┐
                              │ qdrant-  │    │  neo4j-  │
                              │ storage  │    │  storage │
                              └──────────┘    └──────────┘
                                  ↓                ↓
                                Qdrant           Neo4j
```

A medium-sized repo (10–50k LOC) takes 5–20 minutes depending on Gemini rate limits.

### Step 3c — Monitor ingestion status

```bash
curl -H "$AUTH_HEADER" \
  http://localhost:8000/api/v1/projects/$PROJECT_ID/ingestions
```

For a specific ingestion:

```bash
curl -H "$AUTH_HEADER" \
  http://localhost:8000/api/v1/ingestions/<ingestion_id>/progress
```

Or watch worker logs:

```bash
docker compose -f docker/docker-compose.yml logs -f \
  kgrag-code-preprocessor kgrag-entity-extraction kgrag-summarization \
  kgrag-embedding kgrag-qdrant-storage kgrag-neo4j-storage
```

---

## Documents: ingesting files

Supported formats: PDF, DOCX, PPTX, MD, TXT.

```bash
curl -X POST http://localhost:8000/api/v1/documents/upload \
  -H "$AUTH_HEADER" \
  -F "project_id=$PROJECT_ID" \
  -F "files=@/path/to/spec.pdf" \
  -F "files=@/path/to/notes.md"
```

Each uploaded file becomes a `BrainEvent` on Kafka topic `brain_events`. Flow:

```
POST /documents/upload
        │
        ▼
Kafka topic: brain_events
        │
        ▼
kgrag-document-preprocessor          [extracts text, chunks]
        │
        ▼
Kafka topic: enriched-code-chunks    ← yes, same topic as code from here on
        │
        ▼
(downstream: entity-extraction → summarization → embedding → qdrant + neo4j)
```

Documents and code share the same downstream pipeline.

### Check status

```bash
curl -H "$AUTH_HEADER" \
  "http://localhost:8000/api/v1/documents?project_id=$PROJECT_ID"

curl -H "$AUTH_HEADER" \
  "http://localhost:8000/api/v1/documents/<document_id>/status"
```

---

## Searching code

### Option A — REST

```bash
curl -X POST http://localhost:8000/api/v1/code-search/search \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  -d "{
    \"project_id\": \"$PROJECT_ID\",
    \"query\": \"how is authentication handled\",
    \"limit\": 10
  }"
```

### Option B — gRPC (full surface)

```bash
grpcurl -plaintext \
  -H "authorization: Bearer $KGRAG_TOKEN" \
  -d "{
    \"project_id\": \"$PROJECT_ID\",
    \"query\": \"function that validates JWT\",
    \"limit\": 10
  }" \
  localhost:50051 kgrag.code_search.CodeSearchService/SearchEntities
```

Available methods on `kgrag.code_search.CodeSearchService`:

| Method                    | What it does                                                              |
|---------------------------|---------------------------------------------------------------------------|
| `GetDomains`              | List ingested projects/domains visible to the caller.                     |
| `SearchEntities`          | Vector search over code entities (functions, classes, methods).           |
| `SearchSummaries`         | Vector search over LLM-generated code summaries.                          |
| `GetCodeForEntity`        | Fetch source code for an entity ID.                                       |
| `TraverseGraph`           | Walk the code call graph from a starting entity.                          |
| `GetEntityGraph`          | Get the local graph (callers + callees) around an entity.                 |
| `FullSearch`              | Hybrid retrieval: vector + graph traversal + RRF fusion. Recommended.     |
| `Health`                  | Liveness check.                                                           |

And on `kgrag.code.CodeService`:

| Method               | What it does                                              |
|----------------------|-----------------------------------------------------------|
| `FindSimilarCode`    | Vector similarity over code snippets.                     |
| `AnalyzeImpact`      | Blast-radius: who calls this, what does it call.          |
| `TraceExecutionFlow` | DFS from an entry point through callees.                  |

---

## Searching documents

```bash
grpcurl -plaintext \
  -H "authorization: Bearer $KGRAG_TOKEN" \
  -d "{
    \"project_id\": \"$PROJECT_ID\",
    \"query\": \"deployment requirements for staging\",
    \"limit\": 10
  }" \
  localhost:50051 kgrag.document_search.DocumentSearchService/SearchDocuments
```

Methods on `kgrag.document_search.DocumentSearchService`:

| Method                       | What it does                                       |
|------------------------------|----------------------------------------------------|
| `GetCollections`             | List document collections in scope.                |
| `SearchDocuments`            | Vector search over document chunks.                |
| `GetDocumentChunk`           | Fetch a specific chunk by ID.                      |
| `SearchDocumentSummaries`    | Vector search over chunk-level summaries.          |
| `TraverseDocumentGraph`      | Walk the document graph (sections / entities).     |
| `FullDocumentSearch`         | Hybrid: vector + graph + RRF. Recommended.         |
| `Health`                     | Liveness check.                                    |

---

## Authentication for gRPC

The `Authorization: Bearer <token>` metadata header is validated by the gRPC `AuthenticationInterceptor`. The same JWT works for REST and gRPC.

`AuthService` over gRPC has a slim surface — `Authenticate` and `GetProjects`. The **full user/company/project management surface is REST** (`:8000` and `:8001`).

---

## Triggering ingestion from GitHub

You can trigger ingestion automatically on push by pointing a GitHub webhook at:

```
POST  http://<your-host>:8000/api/v1/webhooks/github/{company_name}/{project_name}
```

Use a webhook secret (set in `.env` as `GITHUB_WEBHOOK_SECRET`) and configure the webhook in your repo settings → Webhooks → "push" events, content type `application/json`.

The webhook handler validates the signature, then publishes the same Kafka event as `POST /code_ingestion`.

---

## Common workflows

### Reindex everything in a project after a schema change

```bash
curl -X POST http://localhost:8000/api/v1/code_ingestion \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' \
  -d "{
    \"repository\": \"acme/api\",
    \"branch\": \"main\",
    \"framework\": \"python\",
    \"project_id\": \"$PROJECT_ID\",
    \"force_full_refresh\": true
  }"
```

`force_full_refresh=true` ignores commit-diff and re-ingests every file.

### Delete a project (and its data)

```bash
curl -X DELETE \
  -H "$AUTH_HEADER" \
  http://localhost:8000/api/v2/projects/$PROJECT_ID
```

This removes the project record. Qdrant collections and Neo4j subgraphs for that project are deleted by the storage workers on the next compaction pass.

---

## Limits and quirks

- **Gemini rate limits** dominate ingestion speed. Default config caps at ~1500 req/min per service. If you hit `429`s, lower `LLM_RATE_LIMIT_REQUESTS` in `.env` or wait for cooldown.
- **The IngestionService gRPC is read-only** (`GetIngestionStatus`, `ListIngestions`). To start an ingestion, use REST.
- **One Kafka topic for both** code and document chunks downstream (`enriched-code-chunks`). They're tagged by content-type metadata.
- **Embeddings dimension** defaults to `3072` (Gemini `embedding-001`). Change `EMBEDDING_DIMENSIONS` and `EMBEDDING_MODEL` in `.env` if you switch models — but be aware that mixing dimensions in a Qdrant collection breaks search.

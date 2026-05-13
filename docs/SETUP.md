# Setup

First-time setup of the kgrag stack on a workstation. About 10 minutes once Docker is installed and you have a Gemini API key.

---

## Prerequisites

| Requirement              | Minimum                | Recommended                  |
|--------------------------|------------------------|------------------------------|
| OS                       | Linux, macOS, WSL2     | Linux or macOS               |
| Docker Engine            | 24.x                   | 26.x or newer                |
| Docker Compose           | v2.x                   | v2.27+                       |
| Free disk                | 25 GB                  | 50 GB                        |
| Free RAM                 | 12 GB                  | 16 GB+                       |
| CPU                      | 4 cores                | 8 cores                      |
| GNU Make                 | 3.8+ (default on macOS)|                              |
| `curl` and `grpcurl`     | for smoke tests        |                              |

You also need:

- A **Gemini API key** — get one at https://aistudio.google.com/app/apikey. Used for code/document enrichment, entity extraction, summarisation, and embeddings.
- Optional: a **GitHub personal access token** if you want to ingest private repositories.

The stack does **not** require GPUs and runs entirely on CPU.

---

## 1. Clone and configure

```bash
cd /path/to/your/workspace
git clone <this-repo-url> backend-services
cd backend-services

cp .env.example .env
$EDITOR .env
```

### Minimum env vars to set in `.env`

```ini
# REQUIRED
GEMINI_API_KEY=<your_gemini_key>
JWT_SECRET_KEY=<long_random_string_min_32_chars>

# RECOMMENDED (defaults work but are insecure)
POSTGRES_PASSWORD=<change_me>
NEO4J_PASSWORD=<change_me>
REDIS_PASSWORD=<change_me>

# OPTIONAL: required only for ingesting private GitHub repos
GITHUB_TOKEN=<your_github_pat>
```

Everything else in `.env.example` has sane defaults baked into `docker-compose.yml`.

### Generate a strong JWT secret

```bash
openssl rand -hex 32     # paste into JWT_SECRET_KEY
```

---

## 2. Build the images

```bash
make build
```

What this does:

1. Builds `kgrag-base:latest` — heavy pip install of cognee, grpcio, qdrant-client, neo4j, embeddings stack, etc. **First run: ~5–8 minutes.** Cached after that.
2. Builds the 4 child images (`kgrag-ingestion`, `kgrag-search`, `kgrag-auth`, `kgrag-rest-api`). Each takes ~5–45 s because they just `FROM kgrag-base` and `COPY` source.

After it finishes:

```bash
docker images | grep kgrag
# kgrag-rest-api    latest   ...   2.84GB
# kgrag-auth        latest   ...   2.80GB
# kgrag-search      latest   ...   2.77GB
# kgrag-ingestion   latest   ...   3.13GB
# kgrag-base        latest   ...   2.76GB
```

If `make build` fails, see [OPERATIONS.md](OPERATIONS.md) → Troubleshooting → "Build failures".

---

## 3. Start the stack

```bash
make up
```

This runs `docker compose -f docker/docker-compose.yml up -d`, which starts 16 services:

**Infra (6):** `postgres`, `postgres-init`, `qdrant`, `neo4j`, `redpanda`, `redis`
**App (3):** `kgrag-auth`, `kgrag-rest-api`, `kgrag-search`
**Ingestion workers (7):** `kgrag-code-preprocessor`, `kgrag-document-preprocessor`, `kgrag-entity-extraction`, `kgrag-summarization`, `kgrag-embedding`, `kgrag-qdrant-storage`, `kgrag-neo4j-storage`

Wait ~60 seconds for postgres and neo4j healthchecks to settle.

```bash
make ps
```

Look for `healthy` next to each row. If something is `restarting` or `unhealthy`, jump to [OPERATIONS.md](OPERATIONS.md).

---

## 4. Verify health

```bash
# REST API
curl -s http://localhost:8000/health

# Auth service
curl -s http://localhost:8001/health

# gRPC search (requires grpcurl)
grpcurl -plaintext localhost:50051 list
```

Expected:

- `:8000/health` returns `200` with a JSON body containing `"status": "ok"`.
- `:8001/health` same.
- `grpcurl list` lists: `kgrag.auth.AuthService`, `kgrag.code.CodeService`, `kgrag.code_search.CodeSearchService`, `kgrag.document_search.DocumentSearchService`, `kgrag.ingestion.IngestionService`, `grpc.health.v1.Health`, `grpc.reflection.v1alpha.ServerReflection`.

---

## 5. Bootstrap the database schema

The ingestion services create their own schemas (`code_processing`, `v2_pipeline`) on first startup. If they raced postgres-init they may have failed silently — visible in logs as:

```
ERROR Postgres health check failed: relation "code_processing.ingestion_batches" does not exist
WARNING Ingestion store health check failed - tracking disabled
```

If you see that, restart the affected workers once postgres is healthy:

```bash
docker compose -f docker/docker-compose.yml restart kgrag-code-preprocessor kgrag-document-preprocessor
```

Their `db_init.py` will run `CREATE SCHEMA IF NOT EXISTS` + `CREATE TABLE IF NOT EXISTS` cleanly on the second start.

Optionally verify:

```bash
docker exec -it kgrag-postgres psql -U kgrag -d kgrag_auth -c '\dn'   # list schemas
docker exec -it kgrag-postgres psql -U kgrag -d kgrag_auth -c '\dt code_processing.*'
```

---

## 6. Create your first user

The auth service starts empty. Register a user — this user will be the seed for creating a company, project, and repository.

```bash
curl -X POST http://localhost:8001/register \
  -H 'Content-Type: application/json' \
  -d '{
    "email": "you@example.com",
    "password": "ChooseAStrongPassword123!",
    "full_name": "Your Name"
  }'
```

Then log in to get a JWT:

```bash
curl -X POST http://localhost:8001/login \
  -H 'Content-Type: application/json' \
  -d '{"email": "you@example.com", "password": "ChooseAStrongPassword123!"}'
# → returns { "access_token": "eyJ...", "refresh_token": "...", ... }
```

Save the `access_token`. You'll use it as `Authorization: Bearer <token>` for every subsequent REST call.

See [USAGE.md](USAGE.md) for the full ingestion + search flow.

---

## 7. (Optional) Email verification, 2FA, OAuth

Email verification, password resets, and 2FA setup are available but **off by default** unless you configure SMTP / Infobip credentials. They are not required for local development.

See `auth/main.py` for the full route list, or [API.md](API.md) for endpoint reference.

---

## Stopping and resetting

```bash
make down                 # stop containers, keep volumes (postgres data, qdrant, neo4j)
make clean                # stop + remove volumes + remove kgrag-*:latest images
                          # !! deletes all ingested data !!
```

---

## Next steps

- Read [USAGE.md](USAGE.md) to ingest a repo and run searches.
- Read [ARCHITECTURE.md](ARCHITECTURE.md) to understand the data flow.
- Read [OPERATIONS.md](OPERATIONS.md) for day-to-day operations.

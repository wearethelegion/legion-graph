# Operations

Day-to-day running, debugging, and tuning.

---

## Make targets

```
make base              Build kgrag-base:latest
make build             Build base + all 4 child images
make up                Build everything then `docker compose up -d`
make down              Stop containers (preserves volumes)
make logs              Tail logs from all containers
make ps                Show container status
make config            Validate docker-compose.yml without starting anything
make rebuild SVC=...   Rebuild ONE service image and recreate its container
make clean             Stop + remove volumes + remove all kgrag-*:latest images
make help              List available targets
```

The build ordering matters: `kgrag-base` MUST exist before any child image builds. `make build` enforces this. Running `docker compose build` directly bypasses the dependency and will fail.

---

## Rebuilding a single service after a code change

This is the main day-to-day workflow.

```bash
# Edited graph_services/entity_extraction_service/processor.py
make rebuild SVC=kgrag-ingestion       # ~5–15 s
```

The `SVC` argument maps to the image, not the compose service. Five valid values:

```
kgrag-base
kgrag-ingestion
kgrag-search
kgrag-auth
kgrag-rest-api
```

After `make rebuild`, affected compose services are recreated automatically.

If you only want to restart a container without rebuilding (env var change, etc.):

```bash
docker compose -f docker/docker-compose.yml restart kgrag-entity-extraction
```

---

## Inspecting logs

```bash
# All containers, follow
make logs

# Specific service
docker compose -f docker/docker-compose.yml logs -f kgrag-code-preprocessor

# Just the last 100 lines
docker compose -f docker/docker-compose.yml logs --tail 100 kgrag-search

# Across the whole ingestion pipeline (handy)
docker compose -f docker/docker-compose.yml logs -f \
  kgrag-code-preprocessor kgrag-entity-extraction kgrag-summarization \
  kgrag-embedding kgrag-qdrant-storage kgrag-neo4j-storage
```

---

## Checking pipeline state

### Kafka topic lag

```bash
docker exec kgrag-redpanda rpk topic list
docker exec kgrag-redpanda rpk group describe code-preprocessor
docker exec kgrag-redpanda rpk group describe entity_extraction
```

`LAG` column shows how far behind a consumer is. Persistent lag in one stage = that stage is the bottleneck.

### Qdrant collections

```bash
curl http://localhost:6333/collections
curl http://localhost:6333/collections/<collection-name>
```

### Neo4j

Browser: http://localhost:7474 — user `neo4j`, password from `NEO4J_PASSWORD`.

CLI:

```bash
docker exec -it kgrag-neo4j cypher-shell -u neo4j -p $NEO4J_PASSWORD
```

Useful queries:

```cypher
// node counts by label
MATCH (n) RETURN labels(n) AS lbl, count(*) ORDER BY count(*) DESC;

// relationships
MATCH ()-[r]->() RETURN type(r), count(*) ORDER BY count(*) DESC;

// a specific project
MATCH (n) WHERE n.project_id = '<project-uuid>' RETURN labels(n), count(*);
```

### Postgres

```bash
docker exec -it kgrag-postgres psql -U kgrag -d kgrag_auth

\dn                 # list schemas
\dt code_processing.*   # ingestion tracking tables
SELECT * FROM code_processing.cogni_ingestion_stats ORDER BY started_at DESC LIMIT 5;
```

---

## Tuning

### Embedding throughput

Embeddings are the hot path. Defaults in `.env`:

```ini
EMBEDDING_CONCURRENCY=3
EMBEDDING_BATCH_SIZE=20
EMBEDDING_BATCH_MODE=true
```

If Gemini quota allows, raise concurrency and/or batch size. Watch for 429s — those mean back off.

### LLM rate limiting

```ini
LLM_RATE_LIMIT_ENABLED=true
LLM_RATE_LIMIT_REQUESTS=1500
LLM_RATE_LIMIT_INTERVAL=60   # seconds
```

Per-service rate limit. Each LLM-using worker (entity-extraction, summarization) gets its own bucket.

### Worker concurrency

```ini
ENTITY_EXTRACTION_MAX_WORKERS=30
SUMMARIZATION_MAX_WORKERS=30
PARSER_MAX_WORKERS=4
```

Higher = more in-flight LLM calls per worker. Raise carefully if your Gemini quota allows.

### Neo4j memory

Already pre-configured for a 4 GB container:

```yaml
NEO4J_dbms_memory_pagecache_size=1G
NEO4J_dbms_memory_heap_max__size=2G
```

**Rule:** container `deploy.resources.limits.memory` MUST be at least `pagecache + heap_max + 512 MB JVM overhead`. Neo4j refuses to start otherwise.

---

## Troubleshooting

### Build failures

**`make build` hangs on `pip install`** — usually a slow network or a temporarily broken PyPI mirror. Re-run `make base` (the failing step is almost always inside the base image).

**`ModuleNotFoundError` after rebuild** — the COPY in your Dockerfile is missing a folder. Check the relevant `Dockerfile.<svc>`.

**`gencode N.X runtime M.Y` protobuf mismatch** — the protobuf version patch wasn't applied. The build is supposed to run `scripts/patch_protobuf_version.py` against all `*_pb2*.py`. Inspect the relevant Dockerfile.

### Runtime issues

**`relation "code_processing.X" does not exist`** — the schema bootstrap raced postgres. Restart the affected worker once postgres is `healthy`:

```bash
docker compose -f docker/docker-compose.yml restart kgrag-code-preprocessor
```

It will re-run `CREATE SCHEMA IF NOT EXISTS code_processing` + `CREATE TABLE IF NOT EXISTS` and proceed.

**`ERROR Invalid memory configuration - exceeds physical memory`** (Neo4j) — JVM heap+pagecache exceeds container limit. Fix in `docker/docker-compose.yml` either by raising `deploy.resources.limits.memory` for the `neo4j` service or by lowering `NEO4J_dbms_memory_*`.

**`429 Too Many Requests` from Gemini** — you're hitting the API rate limit. Lower `LLM_RATE_LIMIT_REQUESTS` in `.env` or wait for cooldown (`GEMINI_COOLDOWN_SECONDS=60` by default).

**gRPC client gets `UNAVAILABLE`** — `kgrag-search` not started or unhealthy:

```bash
make ps                                                    # check status
docker compose -f docker/docker-compose.yml logs --tail 50 kgrag-search
```

**JWT validation fails** — token expired, or `JWT_SECRET_KEY` differs between auth and the validator. Same secret must be set in `.env` and visible to both `kgrag-auth` and `kgrag-search`.

**`postgres-init` keeps restarting** — it's idempotent and exits after creating DBs. If it's looping, the `psql` command inside it is failing. Inspect:

```bash
docker compose -f docker/docker-compose.yml logs postgres-init
```

Usually a wrong `POSTGRES_PASSWORD` in `.env`.

**Compose says "version is obsolete"** — cosmetic warning, harmless. Remove `version: "3.8"` from `docker/docker-compose.yml` if you want it silent.

### Search returns nothing

1. Has anything actually finished ingesting? Check Qdrant:
   ```bash
   curl http://localhost:6333/collections
   ```
2. Are you scoping by the right `project_id`?
3. Does Neo4j have nodes for that project?
   ```cypher
   MATCH (n) WHERE n.project_id = '<your-project>' RETURN count(n);
   ```
4. Are workers stuck? Check Kafka lag per consumer group.

---

## Performance reference

Measured on a 2024 MacBook Pro M3 with 16 GB RAM, dev mode:

| Operation                                         | Time                  |
|---------------------------------------------------|-----------------------|
| `make base` (cold, full pip install)              | ~5–8 min              |
| `make build` (4 children, base cached)            | ~30–60 s              |
| `make rebuild SVC=kgrag-ingestion` (one file)     | ~6 s                  |
| `make up` (containers start after build)          | ~60–90 s to healthy   |
| Ingest a 10k-LOC Python repo                      | ~5–10 min             |
| Single `SearchEntities` call (warm Qdrant)        | ~50–200 ms            |
| Single `FullSearch` (vector + graph + fusion)     | ~200–800 ms           |

---

## Known issues / carry-overs

- **`requirements.txt`** at repo root is vestigial (not used by any Dockerfile after Phase 3 split). Safe to delete after a grep for any local script that still reads it.
- **`api/models/*`** contains model files for capabilities that were stripped (knowledge, expertise, lessons, etc.). Dead but harmless — no code imports them.
- **`api/consumers/`** present but unreferenced — kept conservatively.
- **Neo4j 5 deprecation warnings** at startup: `dbms.memory.*` should be `server.memory.*`, `NEO4JLABS_PLUGINS` should be `NEO4J_PLUGINS`. Working as-is; rename next time the compose is touched.

---

## Backup / restore

Volumes that hold real data:

```
postgres_data       /var/lib/postgresql/data
qdrant_data         /qdrant/storage
neo4j_data          /data
redpanda_data       /var/lib/redpanda/data
redis_data          /data
cognee_data         /data/cognee
```

### Quick backup

```bash
docker run --rm \
  -v backend-services_postgres_data:/from:ro \
  -v $(pwd)/backups:/to \
  alpine sh -c "cd /from && tar czf /to/postgres-$(date +%F).tar.gz ."
```

Repeat per volume. For production, use proper backup tooling (pgbackrest for postgres, neo4j-admin for Neo4j, qdrant snapshots).

### Reset everything (destroys data)

```bash
make clean
```

This stops containers, removes volumes, and removes all `kgrag-*:latest` images. You start from scratch with `make up`.

# kgrag infrastructure services

Stock upstream container images used by the kgrag stack: application database, vector store, graph store, message broker, and ephemeral state.

## Overview

| Service | Image | Role | Published port(s) | Volume |
|---------|-------|------|-------------------|--------|
| `postgres` | `postgres:15-alpine` | Application Postgres for auth + projects + brain tables + cognee tables | 5432 | `postgres_data:/var/lib/postgresql/data` |
| `postgres-init` | `postgres:15-alpine` | One-shot init container that creates `cognee` database + extensions | — | — |
| `qdrant` | `qdrant/qdrant:v1.17.0` | Vector store for code chunks, entities, summaries, documents | 6333 (REST), 6334 (gRPC) | `qdrant_data:/qdrant/storage` |
| `neo4j` | `graphstack/dozerdb:5.26.3.0` | Graph store using `cognee-{company_id}` databases per tenant | 7474 (HTTP), 7687 (Bolt) | `neo4j_data:/data`, `neo4j_logs:/logs`, `neo4j_import:/var/lib/neo4j/import`, `neo4j_plugins:/plugins` |
| `redpanda` | `redpandadata/redpanda:latest` | Kafka-compatible message broker for ingestion pipeline | 19092 (Kafka external), 9644 (metrics) | `redpanda_data:/var/lib/redpanda/data` |
| `redis` | `redis:7-alpine` | Token blacklist + auth state + ephemeral pipeline cache | 6379 | `redis_data:/data` |

---

## postgres

### Role

Primary application database for the kgrag stack. Contains three logical databases:

1. **`kgrag_auth`** — default Postgres database created by the container on first boot. Holds:
   - Auth + multi-tenancy tables: `companies`, `projects`, `users`, `company_users`, `roles`
   - Schema `code_processing`: code ingestion pipeline state — `repository_file_versions`, `file_chunks`, `ingestion_batches`, `cogni_ingestion_stats`, `skipped_files`, `pipeline_errors`, `ingestions`, `company_business_domains`, `document_extraction_prompts`
   - Schema `document_processing`: document ingestion state — `document_versions`, `document_chunks`
   - Brain v2 tables: `knowledge`, `expertise`, `lessons`

2. **`cognee`** — created by `postgres-init` on first boot. Holds Cognee library's internal relational tables for graph nodes, edges, datasets, and search indices. Written by the Cognee library (used in `kgrag-cognee`, `kgrag-entity-extraction`, `kgrag-summarization`, `kgrag-embedding`, `kgrag-qdrant-storage`, `kgrag-neo4j-storage`).

3. **`system`** databases (implicit) — standard Postgres metadata.

Evidence: `docker/docker-compose.yml:21-40`, `docker/docker-compose.yml:45-70`

### Image & version

**`postgres:15-alpine`** — Alpine-based Postgres 15. No custom build; official image from Docker Hub.

### Ports

- **Published to host**: `5432` → `5432`
- **Internal**: `5432`

All kgrag services connect via `postgres:5432` from within `kgrag-network`.

### Volumes & persistence

**Named volume**: `postgres_data:/var/lib/postgresql/data`

All three databases (`kgrag_auth`, `cognee`, `postgres`) are stored in this single volume. To wipe all Postgres data, remove this volume.

### Required setup

**On first boot (by the Postgres container itself)**:

- Creates default database `kgrag_auth` (set via `POSTGRES_DB=kgrag_auth` in `docker-compose.yml:32`)
- Creates default role `${POSTGRES_USER:-kgrag}` with password `${POSTGRES_PASSWORD:-kgrag_password}` (set via `docker-compose.yml:30-31`)

**By `postgres-init` service (depends_on postgres healthy)**:

The `postgres-init` one-shot container (`docker-compose.yml:45-70`) creates the `cognee` database idempotently. It:

1. Waits for Postgres readiness via `pg_isready`
2. Checks if `cognee` database exists: `SELECT 1 FROM pg_database WHERE datname='cognee'`
3. If not exists: `CREATE DATABASE cognee OWNER ${POSTGRES_USER:-kgrag}`
4. Exits (container is removed, not restarted)

The `cognee` database is **required** for all Cognee-aware services (`kgrag-cognee`, `kgrag-entity-extraction`, `kgrag-summarization`, `kgrag-embedding`, `kgrag-qdrant-storage`, `kgrag-neo4j-storage`). Those services' `depends_on` blocks specify:

```yaml
depends_on:
  postgres-init:
    condition: service_completed_successfully
```

Evidence: `docker/docker-compose.yml:45-70` (postgres-init inline command), `docker-compose.yml:399-400` (kgrag-cognee depends_on postgres-init)

**Connection tuning**:

- `max_connections=500` (set via `command:` in `docker-compose.yml:28`) — raised from default 100 to accommodate the 15+ kgrag services connecting concurrently

**Schema migrations**:

No Alembic or Flyway migrations found. Tables in `kgrag_auth` (both root tables and `code_processing`/`document_processing` schemas) are created by application services on first run via `CREATE TABLE IF NOT EXISTS` statements. The `cognee` database tables are created by the Cognee library itself via `cognee.infrastructure.databases.relational.create_db_and_tables` called in `kgrag-cognee` and ingestion services.

### How to access from host

**psql command-line**:

```bash
# Connect to kgrag_auth database
psql -h localhost -p 5432 -U kgrag -d kgrag_auth

# Connect to cognee database
psql -h localhost -p 5432 -U kgrag -d cognee

# List all databases
psql -h localhost -p 5432 -U kgrag -c "\l"

# Password: kgrag_password (default; set via POSTGRES_PASSWORD env var)
```

**GUI tools** (DBeaver, pgAdmin, DataGrip, Postico):

- Host: `localhost`
- Port: `5432`
- Database: `kgrag_auth` or `cognee`
- Username: `kgrag`
- Password: `kgrag_password`

### Operational notes

**Healthcheck**: `pg_isready -U kgrag -d kgrag_auth` runs every 10s. Retries 5 times before marking unhealthy. Evidence: `docker-compose.yml:33-37`

**Container name**: `kgrag-postgres`

**Restart policy**: `unless-stopped` — persists across host reboots unless explicitly stopped

**No replication**: single-node Postgres. No streaming replication, no WAL archiving.

**No backup automation**: No `pg_dump` automation. Manual backups recommended before wiping volumes.

**Multi-database**: The stack uses **two** user databases (`kgrag_auth`, `cognee`) in a single Postgres instance. This is intentional — the Cognee library expects its own database namespace separate from application tables.

---

## postgres-init

### Role

One-shot initialization container that creates the `cognee` database in Postgres before any Cognee-aware service starts. Runs once on first stack boot, then exits. Not restarted.

Evidence: `docker/docker-compose.yml:45-70`

### Image & version

**`postgres:15-alpine`** — same as the main `postgres` service. No custom build.

The image is reused solely to provide the `psql` client + `pg_isready` binary for database bootstrapping.

### Ports

None. This is a headless init container that connects to the `postgres` service as a client.

### Volumes & persistence

None. No bind mounts or named volumes. Stateless one-shot command.

### Required setup

This **is** the required setup. The inline shell command (`docker-compose.yml:49-62`):

1. Waits for Postgres readiness: `until pg_isready -h postgres -U kgrag; do sleep 1; done`
2. Checks for `cognee` database: `psql -h postgres -U kgrag -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='cognee'"`
3. If not exists: `psql -h postgres -U kgrag -d postgres -c "CREATE DATABASE cognee OWNER kgrag"`
4. Exits successfully

Idempotent: safe to run multiple times. If the database already exists, it prints `"cognee database already exists — skipping"` and exits 0.

All Cognee-aware services block on `postgres-init` completion via:

```yaml
depends_on:
  postgres-init:
    condition: service_completed_successfully
```

Evidence: `docker-compose.yml:399-400` (kgrag-cognee), `docker-compose.yml:477-478` (kgrag-entity-extraction), `docker-compose.yml:537-538` (kgrag-summarization), `docker-compose.yml:593-594` (kgrag-embedding), `docker-compose.yml:639-640` (kgrag-qdrant-storage), `docker-compose.yml:687-688` (kgrag-neo4j-storage), `docker-compose.yml:757-758` (kgrag-code-preprocessor), `docker-compose.yml:799-800` (kgrag-document-preprocessor)

### How to access from host

Not applicable. This is an ephemeral init container, not a long-running service. Once it exits successfully, the container is removed (`restart: "no"`).

**To manually re-run** (if the database was deleted):

```bash
docker-compose up postgres-init
```

### Operational notes

**Restart policy**: `restart: "no"` — never restarted. Compose lifecycle: run once, exit, remove.

**Container name**: `kgrag-postgres-init`

**Depends on**: `postgres` (with `condition: service_healthy`). Waits for Postgres healthcheck to pass before starting.

**No extensions**: The init script **does not** create Postgres extensions (`pg_trgm`, `pgvector`, `uuid-ossp`, etc.). If extensions are required, they are created lazily by application services or must be added to this init script.

**Env vars**: Inherits `POSTGRES_PASSWORD` from the compose environment. Must match the `postgres` service password.

**No SQL files**: The init logic is fully inline in the `command:` block. No external `.sql` scripts are mounted.

---

## qdrant

### Role

Vector store for the kgrag stack. Stores embedded chunks, entities, summaries, and documents in per-scope collections. Written by `kgrag-qdrant-storage`; queried by `kgrag-search` and `kgrag-cognee`.

**Collection naming scheme** (from forensic entry `f526b6a1-64c3-4596-8e5c-441b8c1af785`):

- **Code**: `DocumentChunk_text` — code chunks from `code_preprocessor`
- **Entities**: `Entity_name` — extracted entities from `entity_extraction_service`
- **Summaries**: `TextSummary_text` — file summaries from `summarization_service`
- **Knowledge/documents**: `{company_id}_knowledge` — document chunks from `document_preprocessor`
- **Graph metadata**: `Triplet_text`, `EdgeType_relationship_name`, `EntityType_name`

Each point carries a `source_node_set` field for multi-tenant filtering (e.g., `{project_id}_{project_name}_code` for code, `{company_id}_knowledge` for documents).

Evidence: `docker/docker-compose.yml:72-104`

### Image & version

**`qdrant/qdrant:v1.17.0`** — official Qdrant image pinned to version 1.17.0. No custom build.

v1.17.0 is a stable release from late 2024. Newer versions exist but are not used (stack was locked to this version for stability).

### Ports

- **REST API** (published to host): `6333` → `6333`
- **gRPC** (published to host): `6334` → `6334`

Internal services use `http://qdrant:6333` (REST) or `qdrant:6334` (gRPC) from within `kgrag-network`.

### Volumes & persistence

**Named volume**: `qdrant_data:/qdrant/storage`

All collections, indices, and metadata are persisted here. To wipe all vectors, remove this volume.

### Required setup

**On first boot (by Qdrant itself)**:

- No init scripts required
- Qdrant auto-creates the `/qdrant/storage` directory structure
- Collections are created lazily by `kgrag-qdrant-storage` via Qdrant REST API when the first document is indexed

**No manual CREATE DATABASE equivalent** — collections are created dynamically by application code.

**Env vars** (`docker-compose.yml:80-82`):

- `QDRANT__SERVICE__HTTP_PORT=6333` — REST API port
- `QDRANT__SERVICE__GRPC_PORT=6334` — gRPC port

### How to access from host

**REST API**:

```bash
# Check cluster health
curl http://localhost:6333/health

# List all collections
curl http://localhost:6333/collections

# Get collection info
curl http://localhost:6333/collections/DocumentChunk_text

# Search (example — requires payload)
curl -X POST http://localhost:6333/collections/DocumentChunk_text/points/search \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.1, 0.2, ...], "limit": 5}'
```

**Qdrant Web UI**:

Open `http://localhost:6333/dashboard` in a browser. Provides a GUI for exploring collections, viewing points, and testing searches.

**Python client** (from host):

```python
from qdrant_client import QdrantClient

client = QdrantClient(url="http://localhost:6333")
collections = client.get_collections()
print(collections)
```

### Operational notes

**Healthcheck**: `timeout 1 bash -c '</dev/tcp/localhost/6333'` — simple TCP check on REST port. Runs every 5s, retries 10 times, 40s start period. Evidence: `docker-compose.yml:83-88`

**Container name**: `kgrag-qdrant`

**Restart policy**: `unless-stopped`

**Resource limits** (`docker-compose.yml:90-97`):
- CPU: 0.5-2.0 cores (reservation-limit)
- Memory: 512M-2G (reservation-limit)

**Logging**: JSON file driver, max 10MB per file, max 3 files (30MB total cap). Evidence: `docker-compose.yml:98-102`

**No authentication**: `QDRANT_API_KEY` is accepted as an env var by kgrag services but is **not** set in the Qdrant service itself. Qdrant runs with no authentication — open to all clients on `kgrag-network`. If `QDRANT_API_KEY` is set in the app services, it will be passed to Qdrant REST/gRPC calls, but Qdrant itself does not enforce it unless configured in a `config.yaml` file (not present in this setup).

**No TLS**: HTTP/gRPC over plaintext. Acceptable for internal bridge network.

---

## neo4j

### Role

Graph store for the kgrag stack. Uses **DozerDB** (Neo4j-compatible fork with multi-database support in Community Edition). Stores code entities, document entities, relationships, project/company hierarchy, and business domain ontologies.

**Database naming scheme** (from forensic entry `f526b6a1-64c3-4596-8e5c-441b8c1af785`):

- **System database**: `system` (Neo4j built-in) — used for `CREATE DATABASE` commands
- **Per-company databases**: `cognee-{company_id}` — one database per company tenant
- **Legacy/default database**: `kgrag` (set via `NEO4J_DATABASE` env var in kgrag-rest-api) — used by kgrag-rest-api for Project/Repository hierarchy writes

Each company database is created idempotently via:

```cypher
CREATE DATABASE `cognee-{company_id}` IF NOT EXISTS
```

run against the `system` database by `kgrag-cognee` and `neo4j_storage_service` before first write.

Evidence: `docker/docker-compose.yml:107-146`, forensic entry insight `f526b6a1-64c3-4596-8e5c-441b8c1af785`

### Image & version

**`graphstack/dozerdb:5.26.3.0`** — DozerDB (Neo4j-compatible, supports multi-database in Community Edition).

DozerDB is a Neo4j-compatible fork maintained by GraphStack. Version 5.26.3.0 corresponds to Neo4j 5.26.x compatibility. **Standard `neo4j:community` does NOT support multi-database** — DozerDB is required for the per-company database architecture.

Evidence: `docker-compose.yml:108`

### Ports

- **HTTP Browser** (published to host): `7474` → `7474`
- **Bolt** (published to host): `7687` → `7687`

Internal services connect via `bolt://neo4j:7687` from within `kgrag-network`.

### Volumes & persistence

**Named volumes**:

- `neo4j_data:/data` — database files, transaction logs, graphs
- `neo4j_logs:/logs` — Neo4j server logs
- `neo4j_import:/var/lib/neo4j/import` — CSV import directory (unused by kgrag)
- `neo4j_plugins:/plugins` — APOC plugin (auto-downloaded on first boot)

To wipe all graph data, remove `neo4j_data`. Logs and plugins are disposable.

Evidence: `docker-compose.yml:113-117`

### Required setup

**On first boot (by Neo4j itself)**:

1. Auto-downloads APOC plugin (set via `NEO4JLABS_PLUGINS=["apoc"]` in `docker-compose.yml:124`)
2. Creates default `system` database
3. Creates default `neo4j` database (standard Neo4j behavior)

**By kgrag services (on first ingestion per company)**:

Each company requires a dedicated Neo4j database. Created idempotently by:

- `kgrag-cognee` (`graph_services/cognee_service/multi_tenancy.py:32-55`)
- `neo4j_storage_service` (`graph_services/neo4j_storage_service/main.py:438-440`)

Both services run this Cypher against the `system` database before any write:

```cypher
CREATE DATABASE `cognee-{company_id}` IF NOT EXISTS
```

**Required Cypher after first boot** (if not auto-created by services):

```cypher
// Connect to system database
:use system;

// Create first company database (example: company_id = 123e4567-e89b-12d3-a456-426614174000)
CREATE DATABASE `cognee-123e4567-e89b-12d3-a456-426614174000` IF NOT EXISTS;

// Verify
SHOW DATABASES;
```

**APOC configuration** (`docker-compose.yml:122-124`):

- `NEO4J_dbms_security_procedures_unrestricted=apoc.*` — allows all APOC procedures to run
- `NEO4J_dbms_security_procedures_allowlist=apoc.*` — explicitly allowlists APOC namespace
- `NEO4JLABS_PLUGINS=["apoc"]` — auto-downloads APOC on first boot

**JVM memory tuning** (`docker-compose.yml:120-121`):

- `NEO4J_dbms_memory_pagecache_size=1G` — OS page cache for graph storage (faster reads)
- `NEO4J_dbms_memory_heap_max__size=2G` — JVM heap (transaction processing, query execution)

Total container memory limit: **4G** (set via `deploy.resources.limits.memory` in `docker-compose.yml:136`). Breakdown:

- 1G page cache
- 2G JVM heap
- 1G headroom for OS, JVM metaspace, off-heap buffers

Evidence: `docker-compose.yml:120-121`, `docker-compose.yml:132-139`

### How to access from host

**Neo4j Browser** (web UI):

Open `http://localhost:7474` in a browser. Login:

- **URL**: `bolt://localhost:7687`
- **Username**: `neo4j`
- **Password**: `kgrag_neo4j_password` (default; set via `NEO4J_PASSWORD` env var)

**cypher-shell** (command-line):

```bash
# Connect to system database
docker exec -it kgrag-neo4j cypher-shell -u neo4j -p kgrag_neo4j_password -d system

# List all databases
SHOW DATABASES;

# Connect to a company database (example)
docker exec -it kgrag-neo4j cypher-shell -u neo4j -p kgrag_neo4j_password -d cognee-123e4567-e89b-12d3-a456-426614174000

# Count nodes
MATCH (n) RETURN count(n);
```

**Python driver** (from host):

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "kgrag_neo4j_password"))

with driver.session(database="system") as session:
    result = session.run("SHOW DATABASES")
    for record in result:
        print(record)

driver.close()
```

### Operational notes

**Healthcheck**: `cypher-shell -u neo4j -p kgrag_neo4j_password "RETURN 1"` — runs every 10s, retries 5 times, 40s start period. Evidence: `docker-compose.yml:125-130`

**Container name**: `kgrag-neo4j`

**Restart policy**: `unless-stopped`

**Resource limits** (`docker-compose.yml:132-139`):
- CPU: 0.5-2.0 cores (reservation-limit)
- Memory: 1G-4G (reservation-limit)

**Logging**: JSON file driver, max 10MB per file, max 3 files (30MB total cap). Evidence: `docker-compose.yml:140-144`

**Multi-database**: DozerDB supports unlimited databases per instance (Community Edition). Standard Neo4j Community Edition **does not** — it is limited to one user database. This is why DozerDB is required.

**No backup automation**: No automated graph dumps. Manual backups via `neo4j-admin database dump` (requires container exec).

**APOC required**: The ingestion pipeline uses APOC procedures for batch writes and graph algorithms. Without APOC, `neo4j_storage_service` and `kgrag-cognee` will fail.

**Authentication**: Single-user mode (`neo4j` user with password `kgrag_neo4j_password`). No RBAC, no LDAP integration.

**No encryption**: Bolt over plaintext. Acceptable for internal bridge network.

---

## redpanda

### Role

Kafka-compatible message broker for the kgrag ingestion pipeline. All inter-service async communication flows through Redpanda topics. Replaces Apache Kafka with a single-binary, lighter-weight implementation (no Zookeeper, no JVM).

**Topic inventory** (from forensic entry `f526b6a1-64c3-4596-8e5c-441b8c1af785`):

| Topic | Producer(s) | Consumer(s) |
|-------|-------------|-------------|
| `incoming_requests` | `kgrag-rest-api` (POST /code_ingestion + webhooks) | `kgrag-code-preprocessor` |
| `brain_events` | `kgrag-cognee` (BrainContentServicer) | `kgrag-document-preprocessor` |
| `enriched-code-chunks` | `kgrag-code-preprocessor`, `kgrag-document-preprocessor` | `kgrag-entity-extraction`, `kgrag-summarization`, `kgrag-qdrant-storage`, `kgrag-neo4j-storage` |
| `extracted-entities` | `kgrag-entity-extraction` | `kgrag-embedding`, `kgrag-neo4j-storage` |
| `text-summaries` | `kgrag-summarization` | `kgrag-embedding`, `kgrag-neo4j-storage` |
| `embeddings-ready` | `kgrag-embedding` | `kgrag-qdrant-storage` |
| `data_enrichment` | `kgrag-code-preprocessor` | (none — archival/diagnostic) |
| `pipeline-events` | Various (progress tracking) | (none — monitoring) |

Topics are **created lazily** by producers. No manual topic creation required.

Evidence: `docker/docker-compose.yml:148-173`

### Image & version

**`redpandadata/redpanda:latest`** — official Redpanda image, using latest stable release (floating tag). No custom build.

Redpanda is a Kafka-compatible event streaming platform written in C++. Single binary, no Zookeeper dependency, lower resource footprint than Kafka.

### Ports

- **Kafka API (external)** (published to host): `19092` → `19092`
- **Prometheus metrics** (published to host): `9644` → `9644`
- **Kafka API (internal)**: `9092` (not published to host)

Internal services connect via `redpanda:9092` from within `kgrag-network`. External clients (e.g., local dev tools like `kafka-console-producer`) connect via `localhost:19092`.

**Advertised listeners** (`docker-compose.yml:157-158`):

- `PLAINTEXT://redpanda:9092` — internal listener for container-to-container
- `EXTERNAL://localhost:19092` — external listener for host-to-container

Evidence: `docker-compose.yml:151-158`

### Volumes & persistence

**Named volume**: `redpanda_data:/var/lib/redpanda/data`

All topic data, logs, and consumer group offsets are persisted here. To wipe all Kafka state, remove this volume.

Evidence: `docker-compose.yml:162-163`

### Required setup

**On first boot (by Redpanda itself)**:

- Auto-creates `/var/lib/redpanda/data` directory structure
- No topics are created — topics are created lazily when the first producer writes to them

**Command-line tuning** (`docker-compose.yml:151-158`):

- `--smp 2` — 2 CPU cores
- `--memory 2G` — 2GB memory limit
- `--overprovisioned` — runs in shared CPU mode (no CPU pinning)
- `--node-id 0` — single-node cluster (node ID 0)
- `--kafka-addr PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:19092` — bind both listeners
- `--advertise-kafka-addr PLAINTEXT://redpanda:9092,EXTERNAL://localhost:19092` — advertise internal + external addresses

**No manual topic creation needed** — all kgrag services use auto-create (Kafka default behavior).

### How to access from host

**rpk CLI** (Redpanda's native CLI — inside container):

```bash
# List topics
docker exec -it kgrag-redpanda rpk topic list

# Describe a topic
docker exec -it kgrag-redpanda rpk topic describe incoming_requests

# Consume messages (tail)
docker exec -it kgrag-redpanda rpk topic consume enriched-code-chunks --num 10

# Produce a test message
docker exec -it kgrag-redpanda rpk topic produce incoming_requests

# Check cluster health
docker exec -it kgrag-redpanda rpk cluster health
```

**Kafka console tools** (from host — requires `kafka-console-consumer` binary):

```bash
# Consume from host (external listener)
kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic enriched-code-chunks --from-beginning

# Produce from host
echo '{"test": "message"}' | kafka-console-producer.sh --bootstrap-server localhost:19092 --topic incoming_requests
```

**Redpanda Console** (web UI — not included in compose):

To add Redpanda Console for a GUI:

```yaml
redpanda-console:
  image: redpandadata/console:latest
  ports:
    - "8080:8080"
  environment:
    - KAFKA_BROKERS=redpanda:9092
  networks:
    - kgrag-network
```

Then open `http://localhost:8080`.

**Prometheus metrics** (from host):

```bash
curl http://localhost:9644/metrics
```

### Operational notes

**Healthcheck**: `rpk cluster health` — runs every 30s, retries 3 times, 10s timeout. Evidence: `docker-compose.yml:169-173`

**Container name**: `kgrag-redpanda`

**Restart policy**: `unless-stopped`

**Single-node cluster**: Not configured for multi-node replication. Acceptable for local dev; production setups should add replica nodes.

**No authentication**: No SASL, no ACLs. Open to all clients on `kgrag-network`. Acceptable for internal bridge network.

**No TLS**: Kafka protocol over plaintext. Acceptable for internal bridge network.

**Network aliases** (`docker-compose.yml:165-167`): The `redpanda` service has an explicit network alias `redpanda` in addition to the default service name. This ensures DNS resolution works for both `redpanda:9092` and `kgrag-redpanda:9092`.

**Consumer group tracking**: Redpanda tracks consumer group offsets internally (same as Kafka). No external Zookeeper.

**Log retention**: Default Redpanda log retention (7 days, 1GB per partition). Not overridden in compose. Large ingestion workloads may fill disk if not monitored.

---

## redis

### Role

Token blacklist + auth state + ephemeral pipeline cache. Used by:

1. **Auth service (`kgrag-auth`)**: stores blacklisted JWT tokens (logout), TOTP secrets, session state, refresh token rotation tracking
2. **gRPC auth interceptors** (`kgrag-cognee`, `kgrag-search`): checks JWT blacklist, fetches per-user password-change/revocation timestamps for token invalidation
3. **Code preprocessor (`kgrag-code-preprocessor`)**: caches git repository state, deduplication hashes (ephemeral)

No persistence semantics — Redis data is treated as cache. If Redis restarts, all cached state is lost (acceptable for token blacklist — worst case: users must re-login).

Evidence: `docker/docker-compose.yml:175-190`

### Image & version

**`redis:7-alpine`** — Alpine-based Redis 7. No custom build; official image from Docker Hub.

Redis 7 is the latest stable major version (released 2022, LTS).

### Ports

- **Published to host**: `6379` → `6379`

Internal services connect via `redis://:<password>@redis:6379/0` from within `kgrag-network`. External tools (e.g., `redis-cli` from host) connect via `localhost:6379`.

### Volumes & persistence

**Named volume**: `redis_data:/data`

Redis persistence is **enabled by default** (RDB snapshots + AOF append-only file). However, the kgrag stack treats Redis as ephemeral — no critical long-term data is stored. Losing this volume on restart means:

- All users must re-login (blacklist cleared)
- TOTP secrets lost (users must re-enroll 2FA)
- Git cache cleared (next ingestion re-clones)

**RDB + AOF enabled** (Redis default behavior — not overridden in compose):

- RDB snapshots: every 60s if ≥1 key changed (default `save 60 1`)
- AOF: append-only file for durability

To disable persistence (pure cache mode), add to `command:`:

```yaml
command: redis-server --requirepass kgrag_redis_password --save "" --appendonly no
```

Evidence: `docker-compose.yml:180-182`

### Required setup

**On first boot (by Redis itself)**:

- No init scripts required
- Redis creates `/data` directory structure for RDB/AOF files

**Password authentication** (`docker-compose.yml:180`):

- `--requirepass ${REDIS_PASSWORD:-kgrag_redis_password}` — requires password for all commands

All kgrag services connect via `REDIS_URI=redis://:<password>@redis:6379/0` (note leading `:` — Redis uses empty username).

**Database 0**: All kgrag services use Redis database `0` (default). No database isolation between services.

### How to access from host

**redis-cli** (from host):

```bash
# Connect with password
redis-cli -h localhost -p 6379 -a kgrag_redis_password

# Ping
redis-cli -h localhost -p 6379 -a kgrag_redis_password PING

# List all keys (WARNING: slow on large datasets)
redis-cli -h localhost -p 6379 -a kgrag_redis_password KEYS "*"

# Get a key
redis-cli -h localhost -p 6379 -a kgrag_redis_password GET "blacklist:jwt:<token>"

# Monitor real-time commands
redis-cli -h localhost -p 6379 -a kgrag_redis_password MONITOR
```

**redis-cli** (from inside container):

```bash
docker exec -it kgrag-redis redis-cli -a kgrag_redis_password
```

**GUI tools** (RedisInsight, Medis):

- Host: `localhost`
- Port: `6379`
- Password: `kgrag_redis_password`

### Operational notes

**Healthcheck**: `redis-cli -a kgrag_redis_password ping` — runs every 10s, retries 5 times. Evidence: `docker-compose.yml:183-187`

**Container name**: `kgrag-redis`

**Restart policy**: `unless-stopped`

**No replication**: Single-node Redis. No replica set, no Sentinel, no Redis Cluster. Acceptable for dev; production should add replicas.

**No TLS**: Redis protocol over plaintext. Acceptable for internal bridge network.

**Memory limit**: No explicit memory limit set in compose. Redis will use host memory until OOM-killed. For production, add:

```yaml
command: redis-server --requirepass kgrag_redis_password --maxmemory 512mb --maxmemory-policy allkeys-lru
```

**Key namespacing** (from kgrag source inspection):

- `blacklist:jwt:<token_hash>` — blacklisted JWT tokens (TTL = original token expiry)
- `user:password_changed:<user_id>` — timestamp of last password change (for token invalidation)
- `user:revocation_timestamp:<user_id>` — timestamp of user revocation (for token invalidation)
- `git:repo:<repo_url>` — git repository clone cache (ephemeral)

**Token blacklist fail-open mode**: The auth interceptor has a `TOKEN_BLACKLIST_FAIL_OPEN` env var (set to `false` by default in `kgrag-cognee:375`). If Redis is unreachable:

- `false` (default): **fail closed** — deny all tokens (503)
- `true`: **fail open** — allow all tokens (security risk)

Current config: **fail closed** — safer default.

---

## Wiping all data

To delete all persisted state and start from a clean slate:

**1. Stop all services**:

```bash
docker-compose down
```

**2. Remove all named volumes**:

```bash
docker volume rm \
  backend-services_postgres_data \
  backend-services_qdrant_data \
  backend-services_neo4j_data \
  backend-services_neo4j_logs \
  backend-services_neo4j_import \
  backend-services_neo4j_plugins \
  backend-services_redpanda_data \
  backend-services_redis_data \
  backend-services_cognee_data
```

(Volume names are prefixed with the compose project name — `backend-services` by default. Verify with `docker volume ls`.)

**3. Restart**:

```bash
docker-compose up -d
```

**Selective wipes**:

- **Postgres only**: `docker volume rm backend-services_postgres_data` — wipes both `kgrag_auth` and `cognee` databases
- **Vectors only**: `docker volume rm backend-services_qdrant_data` — preserves Postgres + Neo4j
- **Graph only**: `docker volume rm backend-services_neo4j_data` — preserves Postgres + Qdrant
- **Kafka only**: `docker volume rm backend-services_redpanda_data` — wipes all topic logs, offsets reset
- **Redis only**: `docker volume rm backend-services_redis_data` — users must re-login, git cache cleared

**Database-level wipes** (without removing volumes):

**Postgres — drop databases**:

```bash
docker exec -it kgrag-postgres psql -U kgrag -d postgres -c "DROP DATABASE kgrag_auth"
docker exec -it kgrag-postgres psql -U kgrag -d postgres -c "DROP DATABASE cognee"
docker-compose restart postgres-init  # recreates cognee
docker-compose restart kgrag-auth     # recreates kgrag_auth tables
```

**Neo4j — drop all company databases**:

```bash
docker exec -it kgrag-neo4j cypher-shell -u neo4j -p kgrag_neo4j_password -d system
# Then run in cypher-shell:
SHOW DATABASES;
DROP DATABASE `cognee-<company-id>`;
```

**Qdrant — delete all collections**:

```bash
curl -X DELETE http://localhost:6333/collections/DocumentChunk_text
curl -X DELETE http://localhost:6333/collections/Entity_name
curl -X DELETE http://localhost:6333/collections/TextSummary_text
# ... repeat for all collections
```

**Redpanda — delete all topics**:

```bash
docker exec -it kgrag-redpanda rpk topic delete incoming_requests
docker exec -it kgrag-redpanda rpk topic delete brain_events
docker exec -it kgrag-redpanda rpk topic delete enriched-code-chunks
# ... repeat for all topics
```

**Redis — flush all keys**:

```bash
docker exec -it kgrag-redis redis-cli -a kgrag_redis_password FLUSHALL
```

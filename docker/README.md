# Kgrag Infrastructure

Docker Compose setup for Kgrag multi-agent memory framework.

**Note:** Kgrag uses non-standard ports (6335/6336 for Qdrant, 7475/7688 for Neo4j) to avoid conflicts with other local Vetlyx services.

## Services

- **Qdrant** (v1.7.4): Vector database for semantic memory search
  - REST API: http://localhost:6335
  - gRPC: localhost:6336

- **Neo4j** (5.15-community): Graph database for agent relationships
  - Browser: http://localhost:7475
  - Bolt: bolt://localhost:7688
  - Username: Configured via `.env` (default: `neo4j`)
  - Password: Configured via `.env` (default: `kgrag_dev_password_CHANGEME`)

## Quick Start

```bash
# 1. Create .env file from template
cp .env.example .env

# 2. Edit .env and set NEO4J_PASSWORD (REQUIRED)
# Change NEO4J_PASSWORD=kgrag_dev_password_CHANGEME to your secure password

# 3. Start services
docker-compose up -d

# 4. Check status
docker-compose ps

# 5. View logs
docker-compose logs -f

# Stop services
docker-compose down

# Stop and remove volumes (⚠️ deletes all data)
docker-compose down -v
```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and customize:

```bash
cp .env.example .env
```

**Required:**
- `NEO4J_USER`: Neo4j username (default: `neo4j`)
- `NEO4J_PASSWORD`: Neo4j password (**MUST CHANGE** from default)

**Optional:**
- `QDRANT_API_KEY`: Leave empty for local dev, set for production

**Security Note:** Never commit `.env` to version control. It's already in `.gitignore`.

### Resource Limits

Each service is configured with:
- **CPU Limit:** 2.0 cores max
- **Memory Limit:** 2GB max
- **CPU Reservation:** 0.5 cores minimum
- **Memory Reservation:** 512MB minimum

Monitor resources:
```bash
docker stats
```

### Log Rotation

Logs are automatically rotated:
- **Max file size:** 10MB
- **Max files kept:** 3 (30MB total per service)

## Health Checks

Both services include proper health checks:

```bash
# Qdrant - checks REST API endpoint
curl http://localhost:6335/health

# Neo4j - checks browser endpoint
curl http://localhost:7475
```

Health check details:
- **Qdrant:** 15s interval, 30s start period, 5 retries (tests raft_state.json file)
- **Neo4j:** 10s interval, 60s start period (APOC plugin loading), 5 retries (tests cypher-shell connection)

## Network

Services communicate via isolated `kgrag-network` bridge network.

## Data Persistence

All data is stored in Docker volumes:
- `qdrant_data`: Qdrant vector storage
- `neo4j_data`: Neo4j graph database
- `neo4j_logs`: Neo4j logs
- `neo4j_plugins`: Neo4j APOC plugin

Data persists across container restarts and `docker-compose down`.

## Validation

Validate configuration before starting:

```bash
# Check compose file syntax and show resolved environment variables
docker-compose config

# Verify services are healthy after startup
docker-compose ps
```

## Troubleshooting

### Services won't start
```bash
# Check logs for errors
docker-compose logs

# Validate compose file
docker-compose config
```

### Neo4j authentication fails
```bash
# Verify .env file exists and has NEO4J_PASSWORD set
cat .env | grep NEO4J_PASSWORD

# Restart services after changing .env
docker-compose down && docker-compose up -d
```

### Resource issues
```bash
# Check resource usage
docker stats

# Services will be throttled at limits, never crash
```

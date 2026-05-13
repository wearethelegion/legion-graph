# Docker Deployment Guide

## 🐳 Quick Start

### Build and Run Consumer

```bash
# Build the image
docker-compose build code-changes-consumer

# Start only the consumer (and dependencies)
docker-compose up code-changes-consumer

# Start entire stack
docker-compose up -d

# View consumer logs
docker-compose logs -f code-changes-consumer

# Stop consumer
docker-compose stop code-changes-consumer
```

## 📋 Configuration

All configuration is via environment variables in `docker-compose.yml`:

### Kafka Settings
- `KAFKA_BOOTSTRAP_SERVERS=redpanda:9092` - Kafka broker (uses internal Docker network)
- `KAFKA_DATA_ENRICHMENT_TOPIC=data_enrichment` - Topic to consume from
- `KAFKA_CONSUMER_GROUP_ID=code-changes-consumer` - Consumer group
- `KAFKA_AUTO_COMMIT=true` - Auto-commit offsets
- `KAFKA_AUTO_OFFSET_RESET=earliest` - Start from beginning if no offset

### Database Connections
- `NEO4J_URI=bolt://neo4j:7687` - Graph database (Docker service name)
- `QDRANT_HOST=qdrant` - Vector database (Docker service name)
- `QDRANT_PORT=6333` - Qdrant HTTP API

### API Keys
- `GEMINI_API_KEY` - From .env file (required for code analysis)

### Processing
- `SKIP_DELETED_FILES=true` - Skip processing deleted files
- `CONSUMER_MAX_RETRIES=3` - Retry attempts per message
- `LOG_LEVEL=INFO` - Logging level (DEBUG, INFO, WARNING, ERROR)

## 🔍 Health Check

The consumer doesn't expose HTTP endpoints, but you can check:

```bash
# Check if process is running
docker-compose exec code-changes-consumer pgrep -f "api.consumers.code_changes_consumer.runner"

# View logs
docker-compose logs --tail=100 code-changes-consumer

# Check Kafka consumer group status
docker-compose exec kgrag-kafka rpk group describe code-changes-consumer
```

## 🚀 Deployment Flow

```
docker-compose up -d
  ↓
Services start in order (depends_on):
  1. redpanda (Kafka)
  2. qdrant (Vector DB)
  3. neo4j (Graph DB)
  4. code-changes-consumer (waits for all healthy)
  ↓
Consumer starts listening to data_enrichment topic
  ↓
Messages consumed → analyzed → stored
```

## 📊 Resource Limits

```yaml
resources:
  limits:
    memory: 2G     # Max memory usage
  reservations:
    memory: 512M   # Minimum guaranteed
```

**Why 2GB limit?**
- Gemini client caching: ~100MB
- Parallel processing (50 concurrent): ~500MB
- Neo4j driver: ~50MB
- Qdrant client: ~50MB
- Python runtime: ~200MB
- Buffer: ~1.1GB

## 🔧 Troubleshooting

### Consumer not starting

```bash
# Check logs
docker-compose logs code-changes-consumer

# Common issues:
# 1. Kafka not ready
docker-compose exec kgrag-kafka rpk cluster health

# 2. Neo4j not ready
docker-compose exec neo4j cypher-shell -u neo4j -p <password> "RETURN 1"

# 3. Qdrant not ready
curl http://localhost:6333/healthz
```

### Consumer crashing

```bash
# Check exit code
docker-compose ps code-changes-consumer

# View crash logs
docker-compose logs --tail=200 code-changes-consumer

# Common crashes:
# - Missing GEMINI_API_KEY
# - Kafka connection failed
# - Out of memory (increase limits)
```

### Messages not being processed

```bash
# Check Kafka topic has messages
docker-compose exec kgrag-kafka rpk topic consume data_enrichment --num 1

# Check consumer group lag
docker-compose exec kgrag-kafka rpk group describe code-changes-consumer

# View consumer logs for errors
docker-compose logs -f code-changes-consumer | grep ERROR
```

## 🔄 Restart Strategies

### Graceful Restart

```bash
# Stop gracefully (handles SIGTERM)
docker-compose stop code-changes-consumer

# Start again
docker-compose up -d code-changes-consumer
```

### Force Restart

```bash
# Kill immediately
docker-compose kill code-changes-consumer

# Remove container
docker-compose rm -f code-changes-consumer

# Start fresh
docker-compose up -d code-changes-consumer
```

### Reset Consumer Offset

```bash
# Stop consumer first
docker-compose stop code-changes-consumer

# Delete consumer group (resets offset)
docker-compose exec kgrag-kafka rpk group delete code-changes-consumer

# Start consumer (will read from beginning)
docker-compose up -d code-changes-consumer
```

## 🛡️ Production Recommendations

### Security
- [ ] Use secrets manager for `GEMINI_API_KEY`
- [ ] Enable Kafka authentication (SASL/SCRAM)
- [ ] Use TLS for Neo4j connection
- [ ] Restrict network access (firewall)

### Monitoring
- [ ] Export logs to centralized system (Loki, ELK)
- [ ] Add Prometheus metrics endpoint
- [ ] Alert on consumer lag
- [ ] Monitor memory usage

### Scaling
- [ ] Multiple consumer instances (same group ID)
- [ ] Increase partition count for parallelism
- [ ] Tune `MAX_CONCURRENT_REQUESTS` in CodeServiceV2

### High Availability
- [ ] Use Kubernetes for orchestration
- [ ] Replicas: 2-3 instances
- [ ] Auto-restart on failure
- [ ] Dead-letter queue for failed messages

## 📦 Build Optimization

### Multi-stage Build (Optional)

```dockerfile
# Stage 1: Build dependencies
FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /root/.local /root/.local
COPY . .
ENV PATH=/root/.local/bin:$PATH
CMD ["python", "-m", "api.consumers.code_changes_consumer.runner"]
```

**Savings**: ~200MB smaller image

## 🔗 Related Documentation

- Consumer README: `README.md`
- CodeServiceV2: `api/services/code_service_v2/README.md`
- Kafka Schemas: `shared/kafka_schemas.py`
- Docker Compose: `docker-compose.yml`

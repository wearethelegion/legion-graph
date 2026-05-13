# Neo4j Storage Service — Cognee Schema

## Overview
The Neo4j storage service now uses **Cognee-compatible schema** with `__Node__` base label, APOC dynamic labels/relationships, and DataPoint properties.

## Quick Start

### 1. Prerequisites
```bash
# Verify APOC is installed
docker exec -it neo4j cypher-shell -u neo4j -p <password>
CALL apoc.help('create') YIELD name RETURN count(name) as apoc_procs;
# Should return > 0
```

### 2. Configuration
Ensure `neo4j.conf` includes:
```properties
dbms.security.procedures.unrestricted=apoc.*
```

### 3. Migration
```bash
# 1. Wipe existing data (DESTRUCTIVE!)
docker exec -it neo4j cypher-shell -u neo4j -p <password>
MATCH (n) DETACH DELETE n;

# 2. Restart service
docker-compose restart neo4j-storage-service

# 3. Re-ingest data
# Service will auto-create __Node__ constraint and write Cognee-compatible nodes/edges
```

### 4. Verification
```cypher
// Check constraint
SHOW CONSTRAINTS;

// Verify node structure
MATCH (n:`__Node__`)
RETURN labels(n), keys(n)
LIMIT 5;

// Verify edge types
MATCH ()-[r]->()
RETURN type(r), count(*) AS count
ORDER BY count DESC;
```

## Schema Reference

### Node Structure
All nodes:
- Base label: `__Node__`
- Secondary label: `Entity`, `DocumentChunk`, or `EntityType` (via APOC)
- Required properties:
  - `id` (unique)
  - `name`, `description`
  - `created_at`, `updated_at` (timestamps in ms)
  - `type` (node type string)
  - `ontology_valid` (boolean, default false)
  - `version` (int, default 1)
  - `topological_rank` (int, default 0)
  - `source_pipeline`, `source_task`, `source_node_set`, `source_user`
  - `company_id`, `project_id` (multi-tenancy)

### Edge Structure
All edges:
- Dynamic relationship types via APOC
- Required properties:
  - `source_node_id`, `target_node_id`
  - `relationship_name` (matches edge type)
  - `ontology_valid` (boolean, default false)
  - `updated_at` (timestamp in ms)

### Structural Edges
- `contains` (lowercase): `DocumentChunk -> Entity`
- `is_a` (lowercase): `Entity -> EntityType`

### Example Queries
```cypher
// Find entities in a file
MATCH (chunk:`__Node__:DocumentChunk`)-[:contains]->(entity:`__Node__:Entity`)
WHERE chunk.file_path = 'src/main.py'
RETURN entity.name, entity.entity_type;

// Find entity type distribution
MATCH (e:`__Node__:Entity`)-[:is_a]->(t:`__Node__:EntityType`)
RETURN t.name, count(e) AS count
ORDER BY count DESC;

// Find function calls
MATCH (a:`__Node__:Entity`)-[r:CALLS]->(b:`__Node__:Entity`)
RETURN a.name, b.name;
```

## Documentation

### Migration Guide
See `COGNEE_MIGRATION.md` for:
- Complete migration workflow
- APOC installation steps
- Troubleshooting guide
- Performance tuning

### Verification Queries
See `VERIFICATION_QUERIES.cypher` for:
- 15 categories of verification queries
- Data integrity checks
- Performance tests
- Cognee compatibility tests

## Testing
```bash
cd neo4j_storage_service
python -m pytest tests/test_writer.py -v
# Expected: 18 passed
```

## Environment Variables
```bash
# Neo4j connection
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<password>
NEO4J_DATABASE=neo4j

# Batch tuning
NEO4J_BATCH_SIZE=500  # Increase for throughput, decrease for memory

# Kafka events
KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
PIPELINE_EVENTS_TOPIC=pipeline-events
NEO4J_STORAGE_CONSUMER_GROUP_ID=neo4j-storage-v2

# Postgres
NEO4J_STORAGE_POSTGRES_DSN=postgresql://user:pass@postgres:5432/db
```

## Architecture

### Write Flow
1. Service listens on `pipeline-events` Kafka topic
2. Waits for `extraction_complete`, `summarization_complete`, `embedding_complete`
3. On completion, executes two-phase batch write:
   - **Phase 0**: Initialize `__Node__` constraint
   - **Phase 1**: Write nodes (EntityType, Entity, DocumentChunk) with APOC dynamic labels
   - **Phase 2**: Write edges (LLM relationships, contains, is_a) with APOC dynamic types
4. Emits `neo4j_complete` event

### Multi-Tenancy
- Each company gets isolated Neo4j database: `cognee-<company_id>`
- All nodes tagged with `company_id` and `project_id`
- Database created automatically via `ensure_neo4j_database()`

## Troubleshooting

### APOC Not Available
```bash
# Install APOC
docker exec -it neo4j bash
cd /var/lib/neo4j/plugins
wget https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/5.x.x/apoc-5.x.x-core.jar
# Restart Neo4j
docker-compose restart neo4j
```

### Nodes Missing Secondary Labels
Check logs for APOC errors:
```bash
docker-compose logs neo4j-storage-service | grep apoc
```

### Lowercase Edges Not Working
Verify migration completed:
```cypher
MATCH ()-[r]->()
WHERE type(r) IN ['CONTAINS', 'IS_A']
RETURN count(r);
# Should be 0
```

## Performance

### Indexing
Unique constraint on `__Node__.id` automatically creates index.

Optional indexes for filtering:
```cypher
CREATE INDEX entity_type_idx FOR (n:`__Node__`) ON (n.entity_type);
CREATE INDEX file_path_idx FOR (n:`__Node__`) ON (n.file_path);
CREATE INDEX company_project_idx FOR (n:`__Node__`) ON (n.company_id, n.project_id);
```

### Monitoring
```bash
# Service logs
docker-compose logs -f neo4j-storage-service

# Neo4j query log
docker exec -it neo4j tail -f /logs/query.log
```

## References
- APOC Documentation: https://neo4j.com/labs/apoc/
- Neo4j Best Practices: https://neo4j.com/developer/guide-performance-tuning/
- Cognee Schema: https://github.com/topoteretes/cognee

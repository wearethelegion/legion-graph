# Neo4j Storage Service — Cognee Schema Migration Guide

## Overview
The Neo4j storage writer has been migrated to use **Cognee-compatible schema**. All nodes now use the `__Node__` base label with APOC dynamic secondary labels, and all edges use APOC dynamic relationship types.

## ⚠️ Breaking Changes

### 1. APOC Plugin Required
**Neo4j APOC plugin is now MANDATORY**. The service will fail without it.

#### Installation Steps:
```bash
# For Docker deployments
docker exec -it neo4j bash
neo4j-admin dbms set-initial-password <password>
# Download APOC from: https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases

# For bare metal deployments
# Download APOC JAR matching your Neo4j version
# Place in: $NEO4J_HOME/plugins/
```

#### Required Configuration:
Add to `neo4j.conf`:
```properties
dbms.security.procedures.unrestricted=apoc.*
dbms.security.procedures.allowlist=apoc.*
```

Restart Neo4j after changes.

#### Verify APOC Installation:
```cypher
CALL apoc.help('create') YIELD name
RETURN name;
```

### 2. Data Migration Required
**All existing data must be wiped and re-ingested**. No backward compatibility.

#### Wipe Existing Data:
```cypher
// WARNING: This deletes ALL data in the database
MATCH (n)
DETACH DELETE n;

// Drop old constraints
DROP CONSTRAINT IF EXISTS entity_id_unique;
DROP CONSTRAINT IF EXISTS chunk_id_unique;
DROP CONSTRAINT IF EXISTS entity_type_name_unique;
```

### 3. Schema Changes

#### Before (Custom Schema):
```cypher
(:Entity {id, name, entity_type, ...})
(:EntityType {name})
(:DocumentChunk {id, file_path, ...})
(a)-[:RELATIONSHIP {type: "CALLS"}]->(b)
(chunk)-[:CONTAINS]->(entity)
(entity)-[:IS_A]->(type)
```

#### After (Cognee Schema):
```cypher
(:__Node__:Entity {
  id,
  name,
  entity_type,
  description,
  company_id,
  project_id,
  // DataPoint base properties
  created_at,
  updated_at,
  ontology_valid,
  version,
  topological_rank,
  type,
  source_pipeline,
  source_task,
  source_node_set,
  source_user
})

(:__Node__:EntityType {
  id: "entity_type_<name>",
  name,
  description,
  // DataPoint base properties
  ...
})

(:__Node__:DocumentChunk {
  id,
  name,
  file_path,
  repository,
  branch,
  language,
  chunk_index,
  company_id,
  project_id,
  description,
  // DataPoint base properties
  ...
})

// Dynamic relationship types via APOC
(a)-[:CALLS {
  source_node_id,
  target_node_id,
  relationship_name: "CALLS",
  ontology_valid,
  updated_at
}]->(b)

// Lowercase structural edges
(chunk)-[:contains]->(entity)
(entity)-[:is_a]->(type)
```

## Migration Workflow

### Step 1: Verify APOC
```bash
# Check APOC is available
docker exec -it neo4j cypher-shell -u neo4j -p <password>
CALL apoc.help('create') YIELD name RETURN count(name) as apoc_procs;
# Should return > 0
```

### Step 2: Backup Existing Data (Optional)
```bash
# Export existing graph (if needed for reference)
docker exec -it neo4j neo4j-admin dump --database=neo4j --to=/backups/pre-migration.dump
```

### Step 3: Wipe Neo4j Data
```cypher
// Connect to each tenant database
:use cognee-<company_id>

// Delete all nodes and relationships
MATCH (n)
DETACH DELETE n;

// Drop old constraints
SHOW CONSTRAINTS;
DROP CONSTRAINT <old_constraint_name> IF EXISTS;
```

### Step 4: Restart Service
```bash
# The service will automatically create the __Node__ constraint
docker-compose restart neo4j-storage-service
```

### Step 5: Re-ingest Data
```bash
# Trigger full pipeline re-ingestion
# The service will:
# 1. Create __Node__ unique constraint
# 2. Write all nodes with __Node__ base label + APOC dynamic labels
# 3. Write all edges with APOC dynamic relationship types
```

### Step 6: Verify Schema
```cypher
// Check __Node__ constraint exists
SHOW CONSTRAINTS;
// Should show: FOR (n:`__Node__`) REQUIRE n.id IS UNIQUE

// Verify node structure
MATCH (n:`__Node__`)
RETURN labels(n), keys(n)
LIMIT 5;
// Should see: ["__Node__", "Entity"] or ["__Node__", "DocumentChunk"]

// Verify DataPoint properties
MATCH (n:`__Node__`)
RETURN n.created_at, n.updated_at, n.type, n.ontology_valid
LIMIT 1;
// Should return non-null values

// Verify dynamic relationship types
MATCH ()-[r]->()
RETURN type(r), keys(r)
LIMIT 5;
// Should see: "CALLS", "contains", "is_a", etc.
```

## Troubleshooting

### APOC Not Available
**Symptom**: `Unknown procedure: apoc.create.addLabels`

**Solution**:
1. Download APOC JAR matching Neo4j version
2. Place in `$NEO4J_HOME/plugins/`
3. Update `neo4j.conf` with unrestricted APOC access
4. Restart Neo4j

### Constraint Already Exists
**Symptom**: Warning log: "Constraints may already exist"

**Solution**: This is normal. The service gracefully handles existing constraints.

### Nodes Missing Secondary Labels
**Symptom**: Only `__Node__` label present, no `Entity` or `DocumentChunk`

**Solution**: APOC not available or query syntax error. Check logs for APOC errors.

### Lowercase Relationship Types Not Working
**Symptom**: Cognee queries fail to find `contains` or `is_a` edges

**Solution**: 
1. Verify re-ingestion completed successfully
2. Check relationship types: `MATCH ()-[r]->() RETURN DISTINCT type(r)`
3. Should see lowercase: `contains`, `is_a`

## Validation Queries

### Count Nodes by Type
```cypher
MATCH (n:`__Node__`)
RETURN labels(n) AS node_labels, count(*) AS count
ORDER BY count DESC;
```

### Count Edges by Type
```cypher
MATCH ()-[r]->()
RETURN type(r) AS rel_type, count(*) AS count
ORDER BY count DESC;
```

### Verify DataPoint Properties
```cypher
MATCH (n:`__Node__`)
WHERE n.ontology_valid IS NULL 
   OR n.created_at IS NULL 
   OR n.type IS NULL
RETURN count(n) AS missing_datapoint_props;
// Should return 0
```

### Test Cognee-Compatible Queries
```cypher
// Find entities in a document chunk
MATCH (chunk:`__Node__:DocumentChunk`)-[:contains]->(entity:`__Node__:Entity`)
WHERE chunk.file_path = 'src/main.py'
RETURN entity.name, entity.entity_type
LIMIT 10;

// Find entity type hierarchy
MATCH (e:`__Node__:Entity`)-[:is_a]->(t:`__Node__:EntityType`)
RETURN t.name AS type, count(e) AS entity_count
ORDER BY entity_count DESC;

// Find LLM-extracted relationships
MATCH (a:`__Node__:Entity`)-[r:CALLS]->(b:`__Node__:Entity`)
RETURN a.name, type(r), b.name
LIMIT 10;
```

## Performance Considerations

### Indexing
The `__Node__.id` constraint automatically creates an index. No additional indexes needed for basic queries.

For advanced filtering, consider:
```cypher
CREATE INDEX entity_type_idx FOR (n:`__Node__`) ON (n.entity_type);
CREATE INDEX file_path_idx FOR (n:`__Node__`) ON (n.file_path);
CREATE INDEX company_project_idx FOR (n:`__Node__`) ON (n.company_id, n.project_id);
```

### Batch Size Tuning
Default batch size: 500 nodes/edges per transaction.

Adjust via environment variable:
```bash
export NEO4J_BATCH_SIZE=1000  # Increase for better throughput
export NEO4J_BATCH_SIZE=100   # Decrease if hitting memory limits
```

## Support

### Log Inspection
```bash
# Service logs
docker-compose logs -f neo4j-storage-service

# Look for:
# - "writer.constraints_ensured" → Constraint creation successful
# - "writer.entity_nodes_written" → Node writes succeeding
# - "apoc" errors → APOC availability issues
```

### Neo4j Query Log
```bash
# Enable query logging in neo4j.conf
dbms.logs.query.enabled=true
dbms.logs.query.threshold=0

# View logs
docker exec -it neo4j tail -f /logs/query.log
```

## References
- Research entry `ff0dcf6e` on engagement `abd6acf7`: Complete Cognee schema analysis
- Plan entry `47c123fe` on engagement `4d6e6673`: Full migration plan
- APOC Documentation: https://neo4j.com/labs/apoc/
- Neo4j APOC Releases: https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases

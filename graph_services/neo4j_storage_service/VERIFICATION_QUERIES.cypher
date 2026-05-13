// ══════════════════════════════════════════════════════════════
// Neo4j Storage Service — Cognee Schema Verification Queries
// ══════════════════════════════════════════════════════════════

// ── 1. Constraint Verification ────────────────────────────────

// Check __Node__ unique constraint exists
SHOW CONSTRAINTS;
// Expected: FOR (n:`__Node__`) REQUIRE n.id IS UNIQUE

// ── 2. Node Structure Verification ───────────────────────────

// Verify all nodes have __Node__ base label
MATCH (n)
WHERE NOT n:`__Node__`
RETURN count(n) AS nodes_without_base_label;
// Expected: 0

// Count nodes by combined labels
MATCH (n:`__Node__`)
RETURN labels(n) AS node_labels, count(*) AS count
ORDER BY count DESC;
// Expected: ["__Node__", "Entity"], ["__Node__", "DocumentChunk"], ["__Node__", "EntityType"]

// Sample node properties
MATCH (n:`__Node__`)
RETURN labels(n), keys(n)
LIMIT 5;
// Expected keys should include: id, name, description, created_at, updated_at, type, ontology_valid, etc.

// ── 3. DataPoint Base Properties Verification ────────────────

// Check all nodes have required DataPoint properties
MATCH (n:`__Node__`)
WHERE n.created_at IS NULL 
   OR n.updated_at IS NULL 
   OR n.type IS NULL
   OR n.ontology_valid IS NULL
   OR n.version IS NULL
RETURN count(n) AS missing_datapoint_props;
// Expected: 0

// Sample DataPoint values
MATCH (n:`__Node__`)
RETURN 
  n.type,
  n.ontology_valid,
  n.version,
  n.topological_rank,
  n.source_pipeline,
  n.source_task
LIMIT 5;
// Expected: type="Entity"|"DocumentChunk"|"EntityType", ontology_valid=false, version=1

// ── 4. Entity Nodes Verification ──────────────────────────────

// Count Entity nodes
MATCH (e:`__Node__:Entity`)
RETURN count(e) AS entity_count;

// Sample Entity properties
MATCH (e:`__Node__:Entity`)
RETURN 
  e.id,
  e.name,
  e.entity_type,
  e.description,
  e.company_id,
  e.project_id
LIMIT 5;

// Count entities by type
MATCH (e:`__Node__:Entity`)
RETURN e.entity_type, count(*) AS count
ORDER BY count DESC;

// ── 5. EntityType Nodes Verification ──────────────────────────

// Count EntityType nodes
MATCH (t:`__Node__:EntityType`)
RETURN count(t) AS entity_type_count;

// List all entity types
MATCH (t:`__Node__:EntityType`)
RETURN t.id, t.name
ORDER BY t.name;
// Expected IDs: "entity_type_class", "entity_type_function", etc.

// Verify synthetic ID format
MATCH (t:`__Node__:EntityType`)
WHERE NOT t.id STARTS WITH 'entity_type_'
RETURN t.id AS invalid_id;
// Expected: 0 rows

// ── 6. DocumentChunk Nodes Verification ───────────────────────

// Count DocumentChunk nodes
MATCH (c:`__Node__:DocumentChunk`)
RETURN count(c) AS chunk_count;

// Sample chunk properties
MATCH (c:`__Node__:DocumentChunk`)
RETURN 
  c.id,
  c.name,
  c.file_path,
  c.repository,
  c.branch,
  c.language,
  c.chunk_index
LIMIT 5;

// Chunks by language
MATCH (c:`__Node__:DocumentChunk`)
RETURN c.language, count(*) AS count
ORDER BY count DESC;

// ── 7. Edge Type Verification ─────────────────────────────────

// Count edges by type
MATCH ()-[r]->()
RETURN type(r) AS rel_type, count(*) AS count
ORDER BY count DESC;
// Expected: lowercase "contains", "is_a", plus LLM types like "CALLS", "IMPORTS"

// Verify no uppercase CONTAINS or IS_A
MATCH ()-[r]->()
WHERE type(r) IN ['CONTAINS', 'IS_A']
RETURN count(r) AS old_uppercase_edges;
// Expected: 0

// ── 8. Edge Properties Verification ───────────────────────────

// Check all edges have required properties
MATCH ()-[r]->()
WHERE r.source_node_id IS NULL 
   OR r.target_node_id IS NULL 
   OR r.relationship_name IS NULL
   OR r.updated_at IS NULL
RETURN count(r) AS missing_edge_props;
// Expected: 0

// Sample edge properties
MATCH (a)-[r]->(b)
RETURN 
  type(r),
  r.source_node_id,
  r.target_node_id,
  r.relationship_name,
  r.ontology_valid,
  r.updated_at
LIMIT 5;
// Expected: ontology_valid=false

// ── 9. Structural Edges Verification ──────────────────────────

// Verify 'contains' edges: DocumentChunk -> Entity
MATCH (c:`__Node__:DocumentChunk`)-[r:contains]->(e:`__Node__:Entity`)
RETURN count(r) AS contains_edges;

// Sample contains edges
MATCH (c:`__Node__:DocumentChunk`)-[r:contains]->(e:`__Node__:Entity`)
RETURN c.file_path, c.chunk_index, e.name, e.entity_type
LIMIT 10;

// Verify 'is_a' edges: Entity -> EntityType
MATCH (e:`__Node__:Entity`)-[r:is_a]->(t:`__Node__:EntityType`)
RETURN count(r) AS is_a_edges;

// Entity type distribution
MATCH (e:`__Node__:Entity`)-[r:is_a]->(t:`__Node__:EntityType`)
RETURN t.name AS type, count(e) AS entity_count
ORDER BY entity_count DESC;

// ── 10. LLM Relationship Edges Verification ───────────────────

// Count LLM-extracted edges
MATCH (a:`__Node__:Entity`)-[r]->(b:`__Node__:Entity`)
WHERE type(r) NOT IN ['contains', 'is_a']
RETURN type(r) AS llm_rel_type, count(*) AS count
ORDER BY count DESC;

// Sample LLM relationships
MATCH (a:`__Node__:Entity`)-[r]->(b:`__Node__:Entity`)
WHERE type(r) IN ['CALLS', 'IMPORTS', 'USES', 'INHERITS']
RETURN a.name, type(r), b.name, r.relationship_name
LIMIT 10;

// ── 11. Multi-Tenancy Verification ────────────────────────────

// Nodes by company
MATCH (n:`__Node__`)
RETURN n.company_id, count(*) AS node_count
ORDER BY node_count DESC;

// Nodes by project
MATCH (n:`__Node__`)
RETURN n.project_id, count(*) AS node_count
ORDER BY node_count DESC;

// Verify company/project isolation
MATCH (n:`__Node__`)
WHERE n.company_id IS NULL OR n.project_id IS NULL
RETURN count(n) AS nodes_without_tenant;
// Expected: Only EntityType nodes may lack company_id/project_id

// ── 12. Cognee Compatibility Test Queries ─────────────────────

// Find entities in a specific file
MATCH (chunk:`__Node__:DocumentChunk`)-[:contains]->(entity:`__Node__:Entity`)
WHERE chunk.file_path = 'src/main.py'
RETURN 
  chunk.chunk_index,
  entity.name,
  entity.entity_type,
  entity.description
ORDER BY chunk.chunk_index, entity.name
LIMIT 20;

// Find all classes and their methods
MATCH (class:`__Node__:Entity`)-[r:CONTAINS]->(method:`__Node__:Entity`)
WHERE class.entity_type = 'class' AND method.entity_type = 'function'
RETURN class.name, collect(method.name) AS methods
LIMIT 10;

// Find function call chains (depth 2)
MATCH path = (a:`__Node__:Entity`)-[:CALLS*1..2]->(b:`__Node__:Entity`)
WHERE a.entity_type = 'function'
RETURN [n in nodes(path) | n.name] AS call_chain
LIMIT 10;

// Find imports by module
MATCH (a:`__Node__:Entity`)-[r:IMPORTS]->(b:`__Node__:Entity`)
RETURN b.name AS imported_module, count(a) AS import_count
ORDER BY import_count DESC
LIMIT 10;

// ── 13. Performance Test Queries ──────────────────────────────

// Index usage test
EXPLAIN
MATCH (n:`__Node__` {id: 'some-id'})
RETURN n;
// Should show index usage on __Node__.id

// Label scan performance
PROFILE
MATCH (n:`__Node__:Entity`)
WHERE n.entity_type = 'class'
RETURN count(n);
// Check db hits and execution plan

// ── 14. Data Integrity Checks ─────────────────────────────────

// Find orphan entities (no incoming 'contains' edges)
MATCH (e:`__Node__:Entity`)
WHERE NOT exists((:`__Node__:DocumentChunk`)-[:contains]->(e))
RETURN count(e) AS orphan_entities;
// May be > 0 if entities span multiple chunks

// Find entities without type classification
MATCH (e:`__Node__:Entity`)
WHERE NOT exists((e)-[:is_a]->(:`__Node__:EntityType`))
RETURN count(e) AS entities_without_type;
// Expected: 0

// Find chunks with no entities
MATCH (c:`__Node__:DocumentChunk`)
WHERE NOT exists((c)-[:contains]->(:`__Node__:Entity`))
RETURN c.file_path, c.chunk_index
LIMIT 10;

// ── 15. Full Graph Statistics ─────────────────────────────────

// Overall summary
MATCH (n:`__Node__`)
WITH labels(n) AS lbls, count(*) AS node_count
OPTIONAL MATCH ()-[r]->()
WITH lbls, node_count, type(r) AS rel_type, count(r) AS rel_count
RETURN 
  lbls AS node_labels,
  node_count,
  rel_type,
  rel_count
ORDER BY node_count DESC, rel_count DESC;

// Database size estimation
CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes
RETURN nodeCount, relCount, labels, relTypes;

// ══════════════════════════════════════════════════════════════
// End of Verification Queries
// ══════════════════════════════════════════════════════════════

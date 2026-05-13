"""
Code Query Service
Provides code intelligence queries for finding similar code, analyzing impact, and tracing execution.
"""

import asyncio
from typing import List, Dict, Any, Optional
from loguru import logger

from kgrag.embeddings import GeminiEmbedder
from kgrag.fusion import RRFusion
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository
from qdrant_client.models import Filter, FieldCondition, MatchValue


class CodeQueryService:
    """Service for code intelligence queries."""

    def __init__(
        self,
        neo4j_repository: Neo4jRepository,
        qdrant_repository: QdrantRepository,
        embedder: Optional[GeminiEmbedder] = None,
    ):
        """
        Initialize CodeQueryService.

        Args:
            neo4j_repository: Neo4j repository for graph queries
            qdrant_repository: Qdrant repository for vector search
            embedder: Gemini embedder (default: creates new instance)
        """
        self.neo4j_repo = neo4j_repository
        self.qdrant_repo = qdrant_repository
        self.embedder = embedder or GeminiEmbedder()
        self._fusion = RRFusion(k=60)

    async def find_similar_code(
        self, query: str, language: str, project_id: str, company_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find similar code using hybrid search (vector + graph) with RRF fusion.

        Combines vector similarity with graph-based fulltext search to provide
        comprehensive results. Results are ranked using Reciprocal Rank Fusion.

        Args:
            query: Natural language or code query
            language: Programming language filter (python, typescript, etc.)
            project_id: Project UUID
            company_id: Company UUID
            limit: Maximum results to return (default: 10)

        Returns:
            List of enriched code results with hybrid ranking
        """
        logger.info(
            f"🔍 find_similar_code HYBRID: query='{query}', language={language}, project={project_id}, company={company_id}, limit={limit}"
        )

        try:
            # Run vector and graph searches in parallel
            vector_task = self._vector_search(query, language, project_id, company_id, limit * 2)
            graph_task = self._graph_search(query, language, project_id, limit * 2)

            vector_results, graph_results = await asyncio.gather(
                vector_task, graph_task, return_exceptions=True
            )

            # Handle exceptions gracefully
            if isinstance(vector_results, Exception):
                logger.warning("Vector search failed: {}, using graph only", vector_results)
                vector_results = []
            if isinstance(graph_results, Exception):
                logger.warning("Graph search failed: {}, using vector only", graph_results)
                graph_results = []

            logger.info(
                f"Vector results: {len(vector_results)}, Graph results: {len(graph_results)}"
            )

            # Fuse results using RRF
            if vector_results and graph_results:
                fused_results = self._fusion.fuse_vector_graph(
                    vector_results,
                    graph_results,
                    vector_weight=1.0,
                    graph_weight=0.8,  # Slightly lower weight for graph
                    limit=limit,
                )
                logger.info(f"Fused {len(fused_results)} hybrid results")
            elif vector_results:
                fused_results = vector_results[:limit]
                for r in fused_results:
                    r["fusion_type"] = "vector_only"
            elif graph_results:
                fused_results = graph_results[:limit]
                for r in fused_results:
                    r["fusion_type"] = "graph_only"
            else:
                fused_results = []

            # Enrich with additional graph context
            enriched_results = await self._enrich_results(fused_results, project_id)

            logger.info(f"Returning {len(enriched_results)} enriched hybrid results")
            return enriched_results

        except Exception as e:
            logger.error("find_similar_code failed: {}", e, exc_info=True)
            raise RuntimeError(f"Failed to find similar code: {e}") from e

    async def _vector_search(
        self, query: str, language: str, project_id: str, company_id: str, limit: int
    ) -> List[Dict[str, Any]]:
        """Execute vector search in Qdrant."""
        logger.debug(f"Starting vector search for: '{query[:50]}...'")

        # Generate embedding
        embedding = self.embedder.embed_query(query)
        collection_name = f"company_{company_id}"

        # Build filter
        filter_conditions = [
            FieldCondition(key="metadata_type", match=MatchValue(value="code")),
            FieldCondition(key="project_id", match=MatchValue(value=project_id)),
        ]
        if language:
            filter_conditions.append(
                FieldCondition(key="language", match=MatchValue(value=language))
            )
        search_filter = Filter(must=filter_conditions)

        # Execute search
        vector_results = self.qdrant_repo.client.query_points(
            collection_name=collection_name,
            query=embedding,
            query_filter=search_filter,
            limit=limit,
            with_payload=True,
            with_vectors=False,
        ).points

        # Format results for fusion (need 'id' field)
        formatted = []
        for r in vector_results:
            payload = r.payload
            entity_id = payload.get("entity_id", "")
            formatted.append(
                {
                    "id": entity_id,
                    "entity_id": entity_id,
                    "score": r.score,
                    "entity_name": payload.get("entity_name"),
                    "entity_type": payload.get("entity_type"),
                    "filename": payload.get("filename"),
                    "file_path": payload.get("file_path"),
                    "language": payload.get("language") or language,
                    "code_snippet": payload.get("code_snippet") or payload.get("content"),
                    "summary": payload.get("summary"),
                    "source": "vector",
                }
            )

        logger.debug(f"Vector search returned {len(formatted)} results")
        return formatted

    async def _graph_search(
        self, query: str, language: str, project_id: str, limit: int
    ) -> List[Dict[str, Any]]:
        """Execute graph-based fulltext search in Neo4j."""
        logger.debug(f"Starting graph search for: '{query[:50]}...'")

        if not self.neo4j_repo or not self.neo4j_repo.driver:
            logger.warning("Neo4j not available for graph search")
            return []

        # Build language filter clause
        language_clause = ""
        if language:
            language_clause = "AND c.language = $language"

        # Fulltext search on entity names and code content
        # Uses Neo4j fulltext index if available, otherwise pattern matching
        cypher = f"""
            MATCH (c:Code {{project_id: $project_id}})-[:CONTAINS]->(e:Entity)
            WHERE (
                toLower(e.name) CONTAINS toLower($query)
                OR toLower(coalesce(e.code_snippet, '')) CONTAINS toLower($query)
                OR toLower(coalesce(c.name, '')) CONTAINS toLower($query)
            )
            {language_clause}
            WITH e, c,
                 CASE 
                     WHEN toLower(e.name) = toLower($query) THEN 1.0
                     WHEN toLower(e.name) CONTAINS toLower($query) THEN 0.8
                     ELSE 0.5
                 END AS relevance_score
            RETURN e.id AS entity_id,
                   e.name AS entity_name,
                   e.type AS entity_type,
                   e.line_start AS line_start,
                   e.line_end AS line_end,
                   e.code_snippet AS code_snippet,
                   c.name AS filename,
                   c.file_path AS file_path,
                   c.language AS language,
                   relevance_score AS score
            ORDER BY relevance_score DESC
            LIMIT $limit
        """

        try:
            params = {"query": query, "project_id": project_id, "limit": limit}
            if language:
                params["language"] = language

            result = await self.neo4j_repo.driver.execute_query(
                cypher, **params, database_=self.neo4j_repo.database
            )

            # Format results for fusion
            formatted = []
            for record in result.records:
                entity_id = record.get("entity_id", "")
                formatted.append(
                    {
                        "id": entity_id,
                        "entity_id": entity_id,
                        "score": record.get("score", 0.5),
                        "entity_name": record.get("entity_name"),
                        "entity_type": record.get("entity_type"),
                        "filename": record.get("filename"),
                        "file_path": record.get("file_path"),
                        "language": record.get("language") or language,
                        "line_start": record.get("line_start"),
                        "line_end": record.get("line_end"),
                        "code_snippet": record.get("code_snippet"),
                        "source": "graph",
                    }
                )

            logger.debug(f"Graph search returned {len(formatted)} results")
            return formatted

        except Exception as e:
            logger.warning("Graph search failed: {}", e)
            return []

    async def _enrich_results(
        self, results: List[Dict[str, Any]], project_id: str
    ) -> List[Dict[str, Any]]:
        """Enrich fused results with additional graph context (dependencies)."""
        if not results or not self.neo4j_repo or not self.neo4j_repo.driver:
            return results

        enriched = []
        for result in results:
            entity_id = result.get("entity_id")
            if not entity_id:
                enriched.append(result)
                continue

            try:
                # Get dependencies for this entity
                dep_query = """
                    MATCH (e:Entity {id: $entity_id})
                    OPTIONAL MATCH (e)-[:calls]->(dep:Entity)
                    OPTIONAL MATCH (c:Code)-[:CONTAINS]->(e)
                    RETURN collect(DISTINCT dep.name) AS dependencies,
                           c.file_path AS file_path,
                           c.name AS filename,
                           e.line_start AS line_start,
                           e.line_end AS line_end
                    LIMIT 1
                """

                dep_result = await self.neo4j_repo.driver.execute_query(
                    dep_query, entity_id=entity_id, database_=self.neo4j_repo.database
                )

                if dep_result.records:
                    record = dep_result.records[0]
                    result["dependencies"] = record.get("dependencies", [])
                    # Fill in missing fields from graph
                    if not result.get("file_path"):
                        result["file_path"] = record.get("file_path")
                    if not result.get("filename"):
                        result["filename"] = record.get("filename")
                    if not result.get("line_start"):
                        result["line_start"] = record.get("line_start")
                    if not result.get("line_end"):
                        result["line_end"] = record.get("line_end")
                else:
                    result["dependencies"] = []

            except Exception as e:
                logger.warning("Enrichment failed for {}: {}", entity_id, e)
                result["dependencies"] = []

            enriched.append(result)

        return enriched

    async def analyze_impact(
        self, entity_name: str, entity_type: str, project_id: str, max_depth: int = 3
    ) -> Dict[str, Any]:
        """
        Analyze impact of changing an entity (function, class, method).

        Performs bidirectional graph traversal to find:
        - Upstream: Who calls this entity? (impact analysis)
        - Downstream: What does this entity call? (dependency analysis)
        - Risk level based on usage frequency

        Args:
            entity_name: Name of the entity (function/class/method)
            entity_type: Type of entity (function, class, method)
            project_id: Project UUID
            max_depth: Maximum graph traversal depth (default: 3)

        Returns:
            Impact analysis with upstream/downstream dependencies and risk level

        Example:
            >>> result = await service.analyze_impact(
            ...     entity_name="process_payment",
            ...     entity_type="function",
            ...     project_id="proj-123",
            ...     max_depth=3
            ... )
            >>> # {
            >>> #   "entity": {...},
            >>> #   "upstream": [[...], [...]],  # Who calls this
            >>> #   "downstream": [[...], [...]],  # What this calls
            >>> #   "risk_level": "high",
            >>> #   "upstream_count": 12,
            >>> #   "downstream_count": 5
            >>> # }
        """
        try:
            # 1. Find entity by name and type
            logger.info(f"Finding entity: {entity_name} ({entity_type}) in project {project_id}")

            entity_result = await self.neo4j_repo.execute_custom_cypher(
                cypher="""
                MATCH (c:Code {project_id: $project_id})-[:CONTAINS]->(e:Entity {name: $name, type: $type})
                RETURN e.id AS entity_id,
                       e.name AS name,
                       e.type AS type,
                       c.filename AS filename,
                       c.file_path AS file_path
                LIMIT 1
                """,
                params={"name": entity_name, "type": entity_type, "project_id": project_id},
            )

            if not entity_result:
                raise ValueError(
                    f"Entity '{entity_name}' of type '{entity_type}' not found in project"
                )

            entity = entity_result[0]
            entity_id = entity["entity_id"]

            # 2. Upstream analysis (who calls this?)
            logger.info(f"Analyzing upstream dependencies for {entity_name} (depth={max_depth})")

            upstream_result = await self.neo4j_repo.execute_custom_cypher(
                cypher=f"""
                MATCH path = (caller:Entity)-[:calls*1..{max_depth}]->(e:Entity {{id: $entity_id}})
                WITH path, [n in nodes(path) | n] AS path_nodes
                RETURN [node in path_nodes | {{
                    name: node.name,
                    type: node.type,
                    file_path: [(c:Code)-[:CONTAINS]->(node) | c.file_path][0],
                    line_start: node.line_start
                }}] AS chain,
                length(path) AS depth
                ORDER BY depth ASC
                LIMIT 50
                """,
                params={"entity_id": entity_id},
            )

            upstream = [record["chain"] for record in upstream_result]

            # 3. Downstream analysis (what does this call?)
            logger.info(f"Analyzing downstream dependencies for {entity_name} (depth={max_depth})")

            downstream_result = await self.neo4j_repo.execute_custom_cypher(
                cypher=f"""
                MATCH path = (e:Entity {{id: $entity_id}})-[:calls*1..{max_depth}]->(dep:Entity)
                WITH path, [n in nodes(path) | n] AS path_nodes
                RETURN [node in path_nodes | {{
                    name: node.name,
                    type: node.type,
                    file_path: [(c:Code)-[:CONTAINS]->(node) | c.file_path][0],
                    line_start: node.line_start
                }}] AS chain,
                length(path) AS depth
                ORDER BY depth ASC
                LIMIT 50
                """,
                params={"entity_id": entity_id},
            )

            downstream = [record["chain"] for record in downstream_result]

            # 4. Calculate risk level based on upstream usage
            upstream_count = len(upstream)
            downstream_count = len(downstream)

            if upstream_count > 10:
                risk_level = "high"
            elif upstream_count > 3:
                risk_level = "medium"
            else:
                risk_level = "low"

            logger.info(
                f"Impact analysis complete: {upstream_count} upstream, "
                f"{downstream_count} downstream, risk={risk_level}"
            )

            return {
                "entity": {
                    "id": entity_id,
                    "name": entity["name"],
                    "type": entity["type"],
                    "filename": entity["filename"],
                    "file_path": entity["file_path"],
                },
                "upstream": upstream,
                "downstream": downstream,
                "risk_level": risk_level,
                "upstream_count": upstream_count,
                "downstream_count": downstream_count,
            }

        except ValueError as e:
            # Entity not found
            logger.warning(str(e))
            raise
        except Exception as e:
            logger.error("analyze_impact failed: {}", e, exc_info=True)
            raise RuntimeError(f"Failed to analyze impact: {e}") from e

    async def trace_execution_flow(
        self, entry_point: str, project_id: str, max_depth: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Trace execution flow from an entry point (DFS via :calls relationships).

        Performs depth-first search to find all possible execution paths from an entry point.
        Useful for understanding code flow, API request handling, and call chains.

        Args:
            entry_point: Name of entry point function/method
            project_id: Project UUID
            max_depth: Maximum traversal depth (default: 5)

        Returns:
            List of execution paths with code details

        Example:
            >>> paths = await service.trace_execution_flow(
            ...     entry_point="handle_api_request",
            ...     project_id="proj-123",
            ...     max_depth=5
            ... )
            >>> # [
            >>> #   {
            >>> #     "path": [
            >>> #       {"name": "handle_api_request", "file": "api.py", "line": 42, "code_snippet": "..."},
            >>> #       {"name": "validate_input", "file": "validators.py", "line": 10, "code_snippet": "..."},
            >>> #       ...
            >>> #     ],
            >>> #     "depth": 3
            >>> #   },
            >>> #   ...
            >>> # ]
        """
        try:
            # 1. Find entry point entity
            logger.info(f"Finding entry point: {entry_point} in project {project_id}")

            entry_result = await self.neo4j_repo.execute_custom_cypher(
                cypher="""
                MATCH (c:Code {project_id: $project_id})-[:CONTAINS]->(e:Entity {name: $name})
                WHERE e.type IN ['function', 'method']
                RETURN e.id AS entity_id,
                       e.name AS name,
                       e.type AS type
                LIMIT 1
                """,
                params={"name": entry_point, "project_id": project_id},
            )

            if not entry_result:
                raise ValueError(f"Entry point '{entry_point}' not found in project")

            entry_entity = entry_result[0]
            entry_id = entry_entity["entity_id"]

            # 2. Trace execution paths (DFS via :calls)
            logger.info(f"Tracing execution paths from {entry_point} (depth={max_depth})")

            paths_result = await self.neo4j_repo.execute_custom_cypher(
                cypher=f"""
                MATCH path = (start:Entity {{id: $entry_id}})-[:calls*1..{max_depth}]->(end:Entity)
                WITH path, [node in nodes(path) | node] AS path_nodes
                RETURN [node in path_nodes | {{
                    name: node.name,
                    type: node.type,
                    file_path: [(c:Code)-[:CONTAINS]->(node) | c.file_path][0],
                    filename: [(c:Code)-[:CONTAINS]->(node) | c.name][0],
                    line_start: node.line_start,
                    line_end: node.line_end,
                    code_snippet: node.code_snippet
                }}] AS execution_path,
                length(path) AS depth
                ORDER BY depth DESC
                LIMIT 10
                """,
                params={"entry_id": entry_id},
            )

            # Format results
            execution_paths = []
            for record in paths_result:
                execution_paths.append({"path": record["execution_path"], "depth": record["depth"]})

            logger.info(f"Found {len(execution_paths)} execution paths from {entry_point}")
            return execution_paths

        except ValueError as e:
            # Entry point not found
            logger.warning(str(e))
            raise
        except Exception as e:
            logger.error("trace_execution_flow failed: {}", e, exc_info=True)
            raise RuntimeError(f"Failed to trace execution flow: {e}") from e

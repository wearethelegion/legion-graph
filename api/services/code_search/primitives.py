"""
Code Search Primitives — six atomic search methods.

Ported from scripts/code_search_tools.py. See module-level docstring in
api/services/code_search_service.py for the full architectural context and
contract references.

T1 Qdrant contract (517eac95):
  - Entity_name       → source_node_set = {project_id}_{project_name}_code, named vector "text"
  - DocumentChunk_text → source_node_set = {project_id}_{project_name}_code
  - TextSummary_text   → source_node_set = {project_id}_{project_name}_code, chunk_id always ''

NOTE: Neo4j now uses the same canonical scheme.
"""

import os
from typing import Any, Dict, List, Optional, Set

from loguru import logger
from qdrant_client.models import FieldCondition, Filter, MatchAny, MatchValue

from kgrag.embeddings import GeminiEmbedder
from api.repositories.neo4j_repository import Neo4jRepository
from api.repositories.qdrant_repository import QdrantRepository

# ── Configuration constants (override via env) ────────────────────────────────

NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")

DEFAULT_BRANCH: str = "develop"

COLLECTION_ENTITIES: str = "Entity_name"
COLLECTION_SUMMARIES: str = "TextSummary_text"
COLLECTION_CHUNKS: str = "DocumentChunk_text"

TEST_FILE_PATTERNS: List[str] = ["spec/", "test/", "__tests__/", "_test."]
TEST_ENTITY_PATTERNS: List[str] = [
    "spec",
    "test",
    "Spec",
    "Test",
    "RSpec",
    "Spec::",
    "Test::",
    "TestCase",
    "SpecHelper",
    "__tests__",
]
DOMAIN_META_TYPES: Set[str] = {"BusinessDomain", "TechnicalTag", "Domain"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


CODE_SEARCH_ENTITY_MIN_SCORE: float = _env_float("CODE_SEARCH_ENTITY_MIN_SCORE", 0.78)
CODE_SEARCH_SUMMARY_MIN_SCORE: float = _env_float("CODE_SEARCH_SUMMARY_MIN_SCORE", 0.72)


def _low_confidence_reason(threshold: float) -> str:
    return f"no high-confidence matches above threshold {threshold:.2f}"


def _neo4j_db_name(company_id: str) -> str:
    """Compute the per-company Neo4j database name."""
    return f"cognee-{company_id}"


def _canonical_code_node_set(project_id: str, project_name: str) -> str:
    """Canonical scope key for code Qdrant payloads."""
    return f"{project_id}_{project_name}_code"


class CodeSearchPrimitives:
    """
    Six atomic search primitives backed by Qdrant (vector) and Neo4j (graph).

    Intended to be subclassed or composed into CodeSearchService.
    """

    def __init__(
        self,
        neo4j_repository: Neo4jRepository,
        qdrant_repository: QdrantRepository,
        embedder: Optional[GeminiEmbedder] = None,
    ) -> None:
        self._neo4j = neo4j_repository
        self._qdrant = qdrant_repository
        self._embedder = embedder or GeminiEmbedder()
        self._last_low_confidence_reason = ""

    def _embed(self, query: str) -> List[float]:
        """Synchronous embedding via GeminiEmbedder.embed_query()."""
        return self._embedder.embed_query(query)

    async def _neo4j_session(self, company_id: str):
        """Return an async Neo4j session scoped to the company's database."""
        await self._neo4j.connect()
        return self._neo4j.driver.session(database=_neo4j_db_name(company_id))

    # ── Level 0: Domain Overview ──────────────────────────────────────────────

    async def get_domains(
        self,
        company_id: str,
        project_id: Optional[str] = None,
        include_technical: bool = True,
    ) -> Dict[str, Any]:
        """
        Level 0: Return domain hierarchy with entity counts.

        Args:
            company_id: Company UUID (REQUIRED — drives Neo4j DB selection).
            project_id: Optionally filter entity counts to a specific project.
            include_technical: Include TechnicalTag domains (default True).

        Returns:
            Dict with 'business_domains' and 'technical_tags' lists.
        """
        params: Dict[str, Any] = {}
        if project_id:
            params["project_id"] = project_id
        if company_id:
            params["company_id"] = company_id

        async with await self._neo4j_session(company_id) as session:
            bd_where = "WHERE d.company_id = $company_id" if company_id else ""
            e_where = "WHERE e.project_id = $project_id" if project_id else ""

            bd_cypher = f"""
            MATCH (d:Entity {{entity_type: 'BusinessDomain'}})
            {bd_where}
            OPTIONAL MATCH (e:Entity)-[:belongs_to_domain]->(d)
            {e_where}
            RETURN d.name AS name, d.description AS description,
                   d.project_id AS project_id, count(e) AS entity_count
            ORDER BY entity_count DESC
            """
            r = await session.run(bd_cypher, params)
            business_domains = [
                {
                    "name": rec["name"],
                    "description": rec["description"] or "",
                    "project_id": rec["project_id"],
                    "entity_count": rec["entity_count"],
                }
                async for rec in r
            ]

            technical_tags: List[Dict[str, Any]] = []
            if include_technical:
                tt_where = "WHERE t.company_id = $company_id" if company_id else ""
                te_where = "WHERE e.project_id = $project_id" if project_id else ""
                tt_cypher = f"""
                MATCH (t:Entity {{entity_type: 'TechnicalTag'}})
                {tt_where}
                OPTIONAL MATCH (e:Entity)-[:tagged_with|belongs_to_domain]->(t)
                {te_where}
                RETURN t.name AS name, t.description AS description,
                       t.project_id AS project_id, count(e) AS entity_count
                ORDER BY entity_count DESC
                LIMIT 50
                """
                r2 = await session.run(tt_cypher, params)
                technical_tags = [
                    {
                        "name": rec["name"],
                        "description": rec["description"] or "",
                        "project_id": rec["project_id"],
                        "entity_count": rec["entity_count"],
                    }
                    async for rec in r2
                ]

        logger.debug(
            "get_domains: company=%s project=%s bd=%d tt=%d",
            company_id,
            project_id,
            len(business_domains),
            len(technical_tags),
        )
        return {"business_domains": business_domains, "technical_tags": technical_tags}

    # ── Level 1: Entity Search ────────────────────────────────────────────────

    async def search_entities(
        self,
        query: str,
        company_id: str,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        branch: str = DEFAULT_BRANCH,
        domain: Optional[str] = None,
        entity_type: Optional[str] = None,
        limit: int = 10,
        exclude_tests: bool = True,
        exclude_domain_meta: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Level 1: Search entities by semantic similarity on Entity_name.

        source_node_set filter = {project_id}_{project_name}_code.
        Branch is stored but not used for filtering (single-branch data currently).

        Returns:
            List of dicts: [{name, entity_type, description, project_id, score, related}]
        """
        query_vector = self._embed(query)

        must: List[Any] = []
        must_not: List[Any] = []

        if project_id and not project_name:
            raise ValueError("project_name required when project_id is set")

        if project_id:
            must.append(
                FieldCondition(
                    key="source_node_set",
                    match=MatchValue(
                        value=_canonical_code_node_set(project_id, project_name or project_id)
                    ),
                )
            )

        if entity_type:
            must.append(FieldCondition(key="entity_type", match=MatchValue(value=entity_type)))

        if exclude_tests:
            for pattern in TEST_ENTITY_PATTERNS:
                must_not.append(FieldCondition(key="entity_type", match=MatchAny(any=[pattern])))

        if exclude_domain_meta:
            for meta_type in DOMAIN_META_TYPES:
                must_not.append(FieldCondition(key="entity_type", match=MatchAny(any=[meta_type])))

        search_filter = (
            Filter(must=must or None, must_not=must_not or None) if (must or must_not) else None
        )

        response = self._qdrant.client.query_points(
            collection_name=COLLECTION_ENTITIES,
            query=query_vector,
            using="text",
            query_filter=search_filter,
            limit=limit * 2 if domain else limit,
            with_payload=True,
            with_vectors=False,
        )

        raw: List[Dict[str, Any]] = [
            {
                "entity_id": str((point.payload or {}).get("id", "")),
                "name": (point.payload or {}).get("name", ""),
                "entity_type": (point.payload or {}).get("entity_type", ""),
                "description": ((point.payload or {}).get("description", "") or "")[:500],
                "project_id": (point.payload or {}).get("project_id", ""),
                "branch": (point.payload or {}).get("branch", ""),
                "score": point.score,
            }
            for point in response.points
        ]

        # Domain post-filter via Neo4j
        if domain:
            async with await self._neo4j_session(company_id) as session:
                r = await session.run(
                    "MATCH (d:Entity {entity_type: 'BusinessDomain', name: $domain})"
                    " MATCH (e:Entity)-[:belongs_to_domain]->(d) RETURN e.id AS eid",
                    {"domain": domain},
                )
                domain_ids = {rec["eid"] async for rec in r}
            raw = [e for e in raw if e["entity_id"] in domain_ids]

        score_filtered = [
            e for e in raw if float(e.get("score") or 0.0) >= CODE_SEARCH_ENTITY_MIN_SCORE
        ]
        self._last_low_confidence_reason = (
            _low_confidence_reason(CODE_SEARCH_ENTITY_MIN_SCORE)
            if raw and not score_filtered
            else ""
        )
        raw = score_filtered

        raw = raw[:limit]

        # Enrich with related entity edges
        results: List[Dict[str, Any]] = []
        if raw:
            async with await self._neo4j_session(company_id) as session:
                for entity in raw:
                    r = await session.run(
                        "MATCH (e:Entity {id: $eid})-[r]-(related:Entity)"
                        " WHERE NOT related.entity_type IN $exclude_types"
                        " RETURN type(r) AS rel, related.name AS name, related.entity_type AS rtype"
                        " LIMIT 10",
                        {"eid": entity["entity_id"], "exclude_types": list(DOMAIN_META_TYPES)},
                    )
                    related = [
                        {"relationship": rec["rel"], "name": rec["name"], "type": rec["rtype"]}
                        async for rec in r
                    ]
                    results.append({**entity, "related": related})

        logger.debug(
            "search_entities: query=%r company=%s → %d", query[:60], company_id, len(results)
        )
        return results

    # ── Level 2: Summary Search ───────────────────────────────────────────────

    async def search_summaries(
        self,
        query: str,
        company_id: str,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        limit: int = 10,
        exclude_tests: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Level 2: Search text summaries on TextSummary_text.

        T1 gap fix: chunk_id is always '' in Qdrant — use file_version_id directly.

        Returns:
            List of dicts: [{text, file_path, language, project_id, score}]
        """
        query_vector = self._embed(query)

        must: List[Any] = []
        if project_id and not project_name:
            raise ValueError("project_name required when project_id is set")

        if project_id:
            must.append(
                FieldCondition(
                    key="source_node_set",
                    match=MatchValue(
                        value=_canonical_code_node_set(project_id, project_name or project_id)
                    ),
                )
            )

        search_filter = Filter(must=must) if must else None

        response = self._qdrant.client.query_points(
            collection_name=COLLECTION_SUMMARIES,
            query=query_vector,
            using="text",
            query_filter=search_filter,
            limit=limit,
            with_payload=True,
            with_vectors=False,
        )

        raw: List[Dict[str, Any]] = [
            {
                "text": (point.payload or {}).get("text", "")
                or (point.payload or {}).get("summary_text", ""),
                "project_id": (point.payload or {}).get("project_id", ""),
                "file_version_id": (point.payload or {}).get("file_version_id", ""),
                "score": point.score,
            }
            for point in response.points
        ]

        score_filtered = [
            s for s in raw if float(s.get("score") or 0.0) >= CODE_SEARCH_SUMMARY_MIN_SCORE
        ]
        self._last_low_confidence_reason = (
            _low_confidence_reason(CODE_SEARCH_SUMMARY_MIN_SCORE)
            if raw and not score_filtered
            else ""
        )
        raw = score_filtered

        results: List[Dict[str, Any]] = []
        if raw:
            async with await self._neo4j_session(company_id) as session:
                for s in raw:
                    file_path = ""
                    language = ""
                    if s["file_version_id"]:
                        r = await session.run(
                            "MATCH (dc:DocumentChunk {file_version_id: $fvid})"
                            " RETURN dc.file_path AS fp, dc.language AS lang LIMIT 1",
                            {"fvid": s["file_version_id"]},
                        )
                        rec = await r.single()
                        if rec:
                            file_path = rec["fp"] or ""
                            language = rec["lang"] or ""
                    if exclude_tests and any(p in file_path for p in TEST_FILE_PATTERNS):
                        continue
                    results.append(
                        {
                            "text": s["text"],
                            "file_path": file_path,
                            "language": language,
                            "project_id": s["project_id"],
                            "score": s["score"],
                        }
                    )

        logger.debug(
            "search_summaries: query=%r company=%s → %d", query[:60], company_id, len(results)
        )
        return results

    # ── Level 3: Code Retrieval ───────────────────────────────────────────────

    async def get_code_for_entity(
        self,
        entity_name: str,
        company_id: str,
        exclude_tests: bool = True,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Level 3: Retrieve source code for a named entity.

        T1 gap fix: Prefers has_code → CodeBlock (richer: start_line, end_line).
        Falls back to made_from → DocumentChunk (start_line=0 always).

        Args:
            entity_name: Entity to retrieve source for.
            company_id: Tenant scope.
            exclude_tests: When True, drop spec/test path matches.
            limit: Maximum number of records (default 10, cap 100).

        Returns:
            List of dicts: [{file_path, language, text, start_line, end_line}]
        """
        # Defensive bounds — servicer also enforces, but keep service-layer safe.
        if limit is None or limit <= 0:
            limit = 10
        if limit > 100:
            limit = 100

        test_cb = (
            " AND NOT cb.file_path CONTAINS 'spec/'"
            " AND NOT cb.file_path CONTAINS 'test/'"
            " AND NOT cb.file_path CONTAINS '__tests__/'"
            if exclude_tests
            else ""
        )
        test_dc = test_cb.replace("cb.", "dc.")

        async with await self._neo4j_session(company_id) as session:
            r = await session.run(
                f"MATCH (e:Entity {{name: $name}})-[:has_code]->(cb:CodeBlock)"
                f" WHERE 1=1 {test_cb}"
                f" RETURN cb.file_path AS file_path, cb.text AS text,"
                f"        cb.language AS language, cb.start_line AS start_line, cb.end_line AS end_line"
                f" LIMIT $lim",
                {"name": entity_name, "lim": limit},
            )
            records = await r.data()

            if not records:
                r2 = await session.run(
                    f"MATCH (e:Entity {{name: $name}})-[:made_from]->(dc:DocumentChunk)"
                    f" WHERE 1=1 {test_dc}"
                    f" RETURN dc.file_path AS file_path, dc.text AS text,"
                    f"        dc.language AS language, 0 AS start_line, 0 AS end_line"
                    f" LIMIT $lim",
                    {"name": entity_name, "lim": limit},
                )
                records = await r2.data()

        results = [
            {
                "file_path": rec.get("file_path", ""),
                "language": rec.get("language", ""),
                "text": rec.get("text", "") or "",
                "start_line": rec.get("start_line", 0),
                "end_line": rec.get("end_line", 0),
            }
            for rec in records
        ]
        logger.debug(
            "get_code_for_entity: name=%r company=%s → %d", entity_name, company_id, len(results)
        )
        return results

    # ── Graph Traversal ───────────────────────────────────────────────────────

    async def traverse_graph(
        self,
        entity_name: str,
        company_id: str,
        exclude_tests: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Traverse 1-hop relationships around a named entity.

        Returns:
            List of dicts: [{source, relationship, target, target_type, file_path}]
        """
        async with await self._neo4j_session(company_id) as session:
            result = await session.run(
                "MATCH (e:Entity {name: $name})-[r]-(related:Entity)"
                " RETURN e.name AS source, type(r) AS relationship,"
                "        related.name AS target, related.entity_type AS target_type,"
                "        related.file_path AS file_path LIMIT 50",
                {"name": entity_name},
            )
            records = await result.data()

        return [
            {
                "source": rec.get("source", ""),
                "relationship": rec.get("relationship", ""),
                "target": rec.get("target", ""),
                "target_type": rec.get("target_type", ""),
                "file_path": rec.get("file_path", ""),
            }
            for rec in records
        ]

    async def get_entity_graph(
        self,
        entity_name: str,
        company_id: str,
        depth: int = 2,
        exclude_tests: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Return a de-duplicated multi-hop entity sub-graph.

        Returns:
            List of unique edge dicts: [{source, relationship, target, target_type, file_path}]
        """
        async with await self._neo4j_session(company_id) as session:
            result = await session.run(
                f"MATCH path = (start:Entity {{name: $name}})-[*1..{depth}]-(end:Entity)"
                f" WITH relationships(path) AS rels, nodes(path) AS nodes"
                f" UNWIND rels AS r"
                f" WITH r, startNode(r) AS src, endNode(r) AS tgt"
                f" RETURN src.name AS source, type(r) AS relationship,"
                f"        tgt.name AS target, tgt.entity_type AS target_type,"
                f"        tgt.file_path AS file_path LIMIT 100",
                {"name": entity_name},
            )
            records = await result.data()

        seen: Set[tuple] = set()
        results: List[Dict[str, Any]] = []
        for rec in records:
            key = (rec.get("source", ""), rec.get("relationship", ""), rec.get("target", ""))
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    "source": rec.get("source", ""),
                    "relationship": rec.get("relationship", ""),
                    "target": rec.get("target", ""),
                    "target_type": rec.get("target_type", ""),
                    "file_path": rec.get("file_path", ""),
                }
            )

        logger.debug(
            "get_entity_graph: name=%r depth=%d → %d edges", entity_name, depth, len(results)
        )
        return results

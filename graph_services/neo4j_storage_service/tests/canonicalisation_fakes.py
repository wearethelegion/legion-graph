"""Fake Neo4j primitives for canonicalisation unit tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace


class _AsyncContextManager:
    def __init__(self, inner):
        self.inner = inner

    async def __aenter__(self):
        return self.inner

    async def __aexit__(self, *args):
        return False


@dataclass
class FakeGraph:
    nodes: dict[str, dict] = field(default_factory=dict)
    relationships: list[dict] = field(default_factory=list)

    def add_node(self, node_id: str, **props):
        self.nodes[node_id] = {"id": node_id, "aliases": [], **props}

    def add_relationship(self, source: str, rel_type: str, target: str, **props):
        self.relationships.append(
            {"source": source, "type": rel_type, "target": target, "props": dict(props)}
        )


class FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def __aiter__(self):
        async def _gen():
            for row in self._rows:
                yield row

        return _gen()

    async def consume(self):
        return SimpleNamespace(
            counters=SimpleNamespace(
                nodes_created=0,
                nodes_deleted=0,
                relationships_created=0,
                relationships_deleted=0,
                properties_set=0,
            )
        )


class FakeSession:
    def __init__(self, graph: FakeGraph, database: str, driver: "FakeDriver"):
        self.graph = graph
        self.database = database
        self.driver = driver

    async def run(self, query, parameters=None):
        params = parameters or {}
        self.driver.runs.append({"database": self.database, "query": query, "parameters": params})
        if "RETURN n.id AS id" in query:
            entity_types = set(params.get("entity_types", []))
            rows = []
            for node in self.graph.nodes.values():
                if node.get("entity_type") not in entity_types:
                    continue
                if not str(node.get("file_path", "")).startswith("document://"):
                    continue
                if node.get("company_id") != params.get("company_id"):
                    continue
                rows.append(
                    {
                        "id": node["id"],
                        "name": node.get("name", ""),
                        "entity_type": node.get("entity_type", ""),
                        "description": node.get("description", ""),
                        "aliases": list(node.get("aliases", [])),
                    }
                )
            return FakeResult(rows)

        if "apoc.refactor.mergeNodes" in query:
            canonical_id = params["canonical_id"]
            duplicate_ids = [dup for dup in params.get("duplicate_ids", []) if dup != canonical_id]
            aliases_to_add = list(params.get("aliases_to_add", []))
            self._merge_nodes(canonical_id, duplicate_ids, aliases_to_add)
            return FakeResult([])

        return FakeResult([])

    def _merge_nodes(self, canonical_id, duplicate_ids, aliases_to_add):
        canonical = self.graph.nodes[canonical_id]
        existing_aliases = list(canonical.get("aliases", []))
        for duplicate_id in duplicate_ids:
            duplicate = self.graph.nodes.get(duplicate_id)
            if not duplicate:
                continue
            existing_aliases.extend([duplicate.get("name", ""), *duplicate.get("aliases", [])])

            new_relationships = []
            for rel in self.graph.relationships:
                if rel["source"] == duplicate_id:
                    new_relationships.append(
                        {
                            **rel,
                            "source": canonical_id,
                        }
                    )
                elif rel["target"] == duplicate_id:
                    if rel["source"] == canonical_id:
                        continue
                    new_relationships.append(
                        {
                            **rel,
                            "target": canonical_id,
                        }
                    )
                else:
                    new_relationships.append(rel)

            deduped = []
            seen = set()
            for rel in new_relationships:
                key = (
                    rel["source"],
                    rel["type"],
                    rel["target"],
                    tuple(sorted(rel["props"].items())),
                )
                if key in seen or rel["source"] == rel["target"]:
                    continue
                seen.add(key)
                deduped.append(rel)
            self.graph.relationships = deduped
            self.graph.nodes.pop(duplicate_id, None)

        canonical["aliases"] = sorted(
            {alias for alias in [*existing_aliases, *aliases_to_add] if alias}
        )


class FakeDriver:
    def __init__(self, graphs_by_database: dict[str, FakeGraph]):
        self.graphs_by_database = graphs_by_database
        self.session_databases: list[str] = []
        self.runs: list[dict] = []

    def session(self, database):
        self.session_databases.append(database)
        graph = self.graphs_by_database[database]
        return _AsyncContextManager(FakeSession(graph, database, self))

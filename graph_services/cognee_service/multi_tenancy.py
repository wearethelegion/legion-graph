"""
Multi-tenancy helpers for Cognee — shared by gRPC servicer and Kafka consumer.

Scopes all Cognee operations to a company-specific Neo4j database and Qdrant
namespace via Cognee's ContextVar mechanism.
"""

import os
import re

import structlog
from neo4j import AsyncGraphDatabase
from cognee.context_global_variables import graph_db_config, vector_db_config

logger = structlog.get_logger(__name__)

# Cache of Neo4j databases already confirmed to exist — avoids repeated
# ``CREATE DATABASE IF NOT EXISTS`` round-trips to the system database.
_created_databases: set[str] = set()


async def ensure_neo4j_database(company_id: str) -> None:
    """Create the company-scoped Neo4j database if it does not already exist.

    Uses DozerDB's ``CREATE DATABASE … IF NOT EXISTS`` executed against the
    ``system`` database.  Results are cached in ``_created_databases`` so that
    each database name is only checked once per process lifetime.
    """
    if not re.match(r"^[a-zA-Z0-9_-]+$", company_id):
        raise ValueError(f"Invalid company_id format: {company_id}")

    db_name = f"cognee-{company_id}"
    if db_name in _created_databases:
        logger.debug(
            "neo4j.database_cache_hit",
            company_id=company_id,
            db_name=db_name,
        )
        return

    logger.debug(
        "neo4j.database_creating",
        company_id=company_id,
        db_name=db_name,
    )
    uri = os.environ["NEO4J_URI"]
    username = os.environ.get("NEO4J_USERNAME", "neo4j")
    password = os.environ["NEO4J_PASSWORD"]

    driver = AsyncGraphDatabase.driver(uri, auth=(username, password))
    try:
        async with driver.session(database="system") as session:
            await session.run(f"CREATE DATABASE `{db_name}` IF NOT EXISTS")
        _created_databases.add(db_name)
        logger.info("neo4j.database_ensured", database=db_name)
    finally:
        await driver.close()


def _assert_tenancy_names(company_id: str, graph_db_name: str, vector_db_name: str) -> None:
    """Assert that both database names contain the company_id substring.

    This is a hard-fail guard — if either name does not embed the company_id,
    we raise RuntimeError before touching any ContextVar.  Silent fallthrough
    would allow a misconfigured name to route queries to the wrong tenant's
    Qdrant partition or Neo4j database.

    Args:
        company_id:     JWT-derived tenant identifier.
        graph_db_name:  Resolved Neo4j database name (must contain company_id).
        vector_db_name: Resolved Qdrant namespace name (must contain company_id).

    Raises:
        RuntimeError: If either name does not contain company_id.
    """
    if not company_id:
        raise RuntimeError(
            "Tenancy assertion failed: company_id is empty. "
            "Refusing to set tenant context without a valid company_id."
        )
    if company_id not in graph_db_name:
        raise RuntimeError(
            f"Tenancy assertion failed: graph_db_name={graph_db_name!r} does not contain "
            f"company_id={company_id!r}. Refusing to set tenant context."
        )
    if company_id not in vector_db_name:
        raise RuntimeError(
            f"Tenancy assertion failed: vector_db_name={vector_db_name!r} does not contain "
            f"company_id={company_id!r}. Refusing to set tenant context."
        )
    # Audit-trail log: emitted on every successful context switch so operators
    # can confirm tenancy is being enforced for each request.
    logger.info(
        "tenancy.assertion_passed",
        company_id=company_id,
        graph_db_name=graph_db_name,
        vector_db_name=vector_db_name,
    )


def set_company_context(company_id: str) -> None:
    """Set Cognee ContextVars to scope all operations to this company.

    Both the graph (Neo4j) and vector (Qdrant) configs are overridden so
    that any downstream ``get_graph_context_config()`` /
    ``get_vectordb_context_config()`` call returns the company-scoped values.

    Raises:
        RuntimeError: If the resolved database names do not embed company_id
            (tenancy assertion guard — prevents silent fallthrough).
    """
    graph_db_name = f"cognee-{company_id}"
    vector_db_name = f"cognee-{company_id}"

    # Hard assertion: fail before setting ContextVars if names are wrong.
    # This turns a convention into a hard architectural invariant.
    _assert_tenancy_names(company_id, graph_db_name, vector_db_name)

    graph_db_config.set(
        {
            "graph_database_provider": "neo4j",
            "graph_database_url": os.environ["NEO4J_URI"],
            "graph_database_name": graph_db_name,
            "graph_database_username": os.environ.get("NEO4J_USERNAME", "neo4j"),
            "graph_database_password": os.environ["NEO4J_PASSWORD"],
            "graph_database_key": "",
            "graph_file_path": "",
            "graph_dataset_database_handler": "",
            "graph_database_port": "",
        }
    )
    vector_db_config.set(
        {
            "vector_db_provider": "qdrant",
            "vector_db_url": os.environ.get("VECTOR_DB_URL", "http://qdrant:6333"),
            "vector_db_key": os.environ.get("QDRANT_API_KEY", ""),
            "vector_db_name": vector_db_name,
        }
    )

"""
Multi-tenancy helpers for Neo4j Storage Service.

Ensures per-company Neo4j databases exist before batch-writing nodes/edges.
Standalone version without Cognee dependencies so the v2 pipeline Docker
image can import it directly.
"""

import asyncio
import os
import re

import structlog
from neo4j import AsyncGraphDatabase

logger = structlog.get_logger(__name__)

# Cache of Neo4j databases already confirmed to exist — avoids repeated
# ``CREATE DATABASE IF NOT EXISTS`` round-trips to the system database.
_created_databases: set[str] = set()

# Lock to prevent concurrent CREATE DATABASE calls for the same db name.
_create_locks: dict[str, asyncio.Lock] = {}
_locks_lock = asyncio.Lock()


def _db_name_for_company(company_id: str) -> str:
    if not re.match(r"^[a-zA-Z0-9_-]+$", company_id):
        raise ValueError(f"Invalid company_id format: {company_id}")
    return f"cognee-{company_id}"


def invalidate_database_cache(db_name: str) -> None:
    """Evict a database from the in-process 'already exists' cache.

    Call this when a downstream query fails with DatabaseNotFound so the
    next ensure_neo4j_database() call actually runs CREATE DATABASE again
    (e.g. after the DB was dropped externally by a cleanup script).
    """
    if db_name in _created_databases:
        _created_databases.discard(db_name)
        logger.warning("neo4j.database_cache_invalidated", database=db_name)


async def ensure_neo4j_database(company_id: str, *, force: bool = False) -> None:
    """Create the company-scoped Neo4j database if it does not already exist.

    Uses ``CREATE DATABASE … IF NOT EXISTS`` executed against the ``system``
    database.  Results are cached in ``_created_databases`` so each database
    name is only checked once per process lifetime.  An asyncio.Lock per
    db_name prevents concurrent workers from racing on the same CREATE.

    Pass ``force=True`` to bypass the cache and re-issue the CREATE — useful
    for recovering from external drops (e.g. after ``clean_cogni_data.sh``).
    """
    db_name = _db_name_for_company(company_id)
    if not force and db_name in _created_databases:
        return

    # Get or create a per-database lock to serialize concurrent callers
    async with _locks_lock:
        if db_name not in _create_locks:
            _create_locks[db_name] = asyncio.Lock()
        lock = _create_locks[db_name]

    async with lock:
        # Double-check after acquiring lock
        if not force and db_name in _created_databases:
            return

        uri = os.environ["NEO4J_URI"]
        username = os.environ.get("NEO4J_USERNAME", "neo4j")
        password = os.environ["NEO4J_PASSWORD"]

        driver = AsyncGraphDatabase.driver(uri, auth=(username, password))
        try:
            async with driver.session(database="system") as session:
                await session.run(f"CREATE DATABASE `{db_name}` IF NOT EXISTS")
            _created_databases.add(db_name)
            logger.info("neo4j.database_ensured", database=db_name, forced=force)
        finally:
            await driver.close()

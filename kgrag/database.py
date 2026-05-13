"""
PostgreSQL Database Connection Pool (Singleton)
Manages connection pool for KGRAG API database operations.
Moved from api.database.connection to avoid circular dependencies.
"""

import os
from typing import Optional
import asyncpg
from loguru import logger


class DatabasePool:
    """Singleton database connection pool."""

    _instance: Optional['DatabasePool'] = None
    _pool: Optional[asyncpg.Pool] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def initialize(self) -> None:
        """Initialize connection pool."""
        if self._pool is not None:
            logger.warning("Database pool already initialized")
            return

        # Get PostgreSQL URL from environment (try DATABASE_URL first, then POSTGRES_URL)
        postgres_url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
        if not postgres_url:
            raise RuntimeError(
                "DATABASE_URL or POSTGRES_URL environment variable not set. "
                "Please configure PostgreSQL connection in .env file."
            )

        # Parse postgres URL to get connection params
        url_parts = postgres_url.replace("postgresql://", "").split("@")
        user_pass = url_parts[0].split(":")
        host_port_db = url_parts[1].split("/")
        host_port = host_port_db[0].split(":")

        try:
            # Pool sizing: match MAX_CONCURRENT_REQUESTS (100) + buffer for internal ops
            # min_size=10: Warm pool for fast response times
            # max_size=120: Supports 100 concurrent requests + 20 internal operations
            self._pool = await asyncpg.create_pool(
                user=user_pass[0],
                password=user_pass[1] if len(user_pass) > 1 else "",
                host=host_port[0],
                port=int(host_port[1]) if len(host_port) > 1 else 5432,
                database=host_port_db[1] if len(host_port_db) > 1 else "kgrag_auth",
                min_size=10,
                max_size=120
            )
            logger.info("✓ Database pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    async def close(self) -> None:
        """Close connection pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            logger.info("✓ Database pool closed")

    def get_pool(self) -> asyncpg.Pool:
        """Get connection pool instance."""
        if self._pool is None:
            raise RuntimeError("Database pool not initialized. Call initialize() first.")
        return self._pool

    async def health_check(self) -> bool:
        """Check database connectivity."""
        if self._pool is None:
            return False

        try:
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False


# Singleton instance
_db_pool = DatabasePool()


async def get_db_pool() -> asyncpg.Pool:
    """Dependency to get database pool."""
    return _db_pool.get_pool()


async def init_db_pool() -> None:
    """Initialize database pool (call on startup)."""
    await _db_pool.initialize()


async def close_db_pool() -> None:
    """Close database pool (call on shutdown)."""
    await _db_pool.close()


async def health_check() -> bool:
    """Check database health."""
    return await _db_pool.health_check()

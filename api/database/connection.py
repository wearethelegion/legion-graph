"""
PostgreSQL Database Connection Pool (Singleton)
Manages connection pool for KGRAG API database operations.

DEPRECATED: Use kgrag.database instead.
This module is kept for backward compatibility.
"""

from kgrag.database import (
    get_db_pool,
    init_db_pool,
    close_db_pool,
    health_check,
    DatabasePool,
    _db_pool  # Re-export singleton for backward compatibility
)

__all__ = [
    "get_db_pool",
    "init_db_pool",
    "close_db_pool",
    "health_check",
    "DatabasePool",
    "_db_pool"
]

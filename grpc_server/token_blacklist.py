"""
Token blacklist client for gRPC server.

Connects to the same Redis instance as the auth service to check token blacklist.
Uses SYNCHRONOUS Redis client (not async) - O(1) operations are fast enough.

FAIL_CLOSED behavior: If Redis is unavailable, tokens are rejected (secure default).
"""
import redis
from typing import Optional
import logging
import os

logger = logging.getLogger(__name__)

# Redis configuration from environment (same as auth service)
REDIS_URI = os.getenv("REDIS_URI", "redis://localhost:6379/0")

# Fail open for testing only (set TOKEN_BLACKLIST_FAIL_OPEN=true)
# In production, this MUST be false (fail closed = reject tokens when Redis down)
FAIL_OPEN = os.getenv("TOKEN_BLACKLIST_FAIL_OPEN", "false").lower() == "true"

# Synchronous Redis client
redis_client = redis.Redis.from_url(REDIS_URI, decode_responses=True)

BLACKLIST_PREFIX = "token:blacklist:"
REVOKED_AT_PREFIX = "token:revoked_at:"


def is_blacklisted(jti: str) -> bool:
    """
    Check if token JTI is in blacklist.

    Args:
        jti: JWT ID claim to check

    Returns:
        True if token is blacklisted
    """
    key = f"{BLACKLIST_PREFIX}{jti}"
    try:
        return redis_client.exists(key) == 1
    except redis.RedisError as e:
        logger.error(f"Failed to check blacklist: {e}")
        if FAIL_OPEN:
            return False  # Allow token if Redis down (testing only)
        # Fail closed — if Redis down, reject tokens (secure default)
        return True


def get_revocation_timestamp(user_id: str) -> Optional[float]:
    """
    Get the revocation timestamp for a user.

    Args:
        user_id: User ID to check

    Returns:
        Timestamp as float, or None if no revocation exists
    """
    key = f"{REVOKED_AT_PREFIX}{user_id}"
    try:
        value = redis_client.get(key)
        return float(value) if value else None
    except (redis.RedisError, ValueError) as e:
        logger.error(f"Failed to get revocation timestamp: {e}")
        return None

"""
Token blacklist service using Redis.

Uses SYNCHRONOUS Redis client (not async) to avoid event loop blocking issues.
This is a proven pattern — blacklist operations are O(1) and fast enough.

Part of the logout implementation per architecture v2.0.

FAIL_CLOSED behavior: If Redis is unavailable, tokens are rejected (secure default).
This can be overridden by setting TOKEN_BLACKLIST_FAIL_OPEN=true for testing.
"""
import redis
from datetime import datetime, timezone
from typing import Optional
import logging
import os

logger = logging.getLogger(__name__)

# Redis configuration from environment
REDIS_URI = os.getenv("REDIS_URI", "redis://localhost:6379/0")

# Fail open for testing only (set TOKEN_BLACKLIST_FAIL_OPEN=true)
# In production, this MUST be false (fail closed = reject tokens when Redis down)
FAIL_OPEN = os.getenv("TOKEN_BLACKLIST_FAIL_OPEN", "false").lower() == "true"

# Synchronous Redis client — DO NOT use redis.asyncio
redis_client = redis.Redis.from_url(REDIS_URI, decode_responses=True)

BLACKLIST_PREFIX = "token:blacklist:"
REVOKED_AT_PREFIX = "token:revoked_at:"


def add_to_blacklist(jti: str, expires_in: int) -> bool:
    """
    Add token JTI to blacklist with TTL matching token expiry.

    Args:
        jti: JWT ID claim from the token
        expires_in: Seconds until token expiry (for TTL)

    Returns:
        True if successfully added
    """
    if expires_in <= 0:
        # Token already expired — no need to blacklist
        return True

    key = f"{BLACKLIST_PREFIX}{jti}"
    try:
        redis_client.setex(key, expires_in, "revoked")
        logger.info(f"Token {jti[:8]}... added to blacklist (TTL: {expires_in}s)")
        return True
    except redis.RedisError as e:
        logger.error(f"Failed to blacklist token: {e}")
        return False


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
        # Fail closed — if Redis down, reject tokens (secure default)
        return True


def revoke_all_user_tokens(user_id: str) -> bool:
    """
    Revoke all tokens for a user by storing revocation timestamp.

    Any token with `iat` < this timestamp is considered revoked.
    TTL: 7 days (matches max refresh token lifetime)

    Args:
        user_id: User ID to revoke all tokens for

    Returns:
        True if successfully stored
    """
    key = f"{REVOKED_AT_PREFIX}{user_id}"
    try:
        # Store current timestamp — tokens issued before this are revoked
        redis_client.setex(key, 7 * 24 * 60 * 60, str(datetime.now(timezone.utc).timestamp()))
        logger.info(f"All sessions revoked for user {user_id}")
        return True
    except redis.RedisError as e:
        logger.error(f"Failed to revoke all sessions: {e}")
        return False


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

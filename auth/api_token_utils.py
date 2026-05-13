"""
API Token Generation and Verification Utilities.

Token format: lgn_{base64_random_32_bytes} (~47 chars)
Storage: SHA-256 hash only (plaintext NEVER stored)

Security:
- SHA-256 for hashing (not bcrypt - tokens have high entropy)
- secrets.compare_digest() for timing-safe comparison
- Tokens shown once at creation, then only hash stored

Usage:
    # Create token
    plaintext, prefix, hash_value, hint = generate_api_token()

    # Verify token
    api_token = verify_api_token(token_string, db)
"""
import secrets
import hashlib
from typing import Optional, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from auth.database import ApiToken


# ============================================================================
# Constants
# ============================================================================

TOKEN_PREFIX = "lgn_"
TOKEN_BYTES = 32  # 256 bits of entropy
MAX_TOKENS_PER_USER = 25  # Ragen's condition: enforce token count limit

# Valid scopes for API tokens
VALID_SCOPES = [
    "read:knowledge",
    "write:knowledge",
    "read:code",
    "write:code",
    "read:engagement",
    "write:engagement",
]


# ============================================================================
# Token Generation
# ============================================================================

def generate_api_token() -> Tuple[str, str, str, str]:
    """
    Generate a new API token.

    Returns:
        Tuple of (plaintext_token, token_prefix, token_hash, token_hint)

    The plaintext_token should be shown to user ONCE and never stored.
    Store token_prefix, token_hash, and token_hint in database.
    """
    # Generate random bytes (256 bits = 32 bytes)
    random_part = secrets.token_urlsafe(TOKEN_BYTES)

    # Create full token with prefix
    plaintext_token = f"{TOKEN_PREFIX}{random_part}"

    # Extract prefix for fast lookup (first 8 chars of full token)
    token_prefix = plaintext_token[:8]

    # Create SHA-256 hash for storage
    token_hash = hash_api_token(plaintext_token)

    # Extract hint for display (last 4 chars)
    token_hint = f"...{plaintext_token[-4:]}"

    return plaintext_token, token_prefix, token_hash, token_hint


def hash_api_token(token: str) -> str:
    """
    Hash a token using SHA-256.

    SHA-256 is appropriate for API tokens because:
    - Tokens have high entropy (256 bits)
    - No salt needed (unlike passwords)
    - Fast hashing is acceptable

    Args:
        token: The plaintext token string

    Returns:
        Hex-encoded SHA-256 hash (64 chars)
    """
    return hashlib.sha256(token.encode('utf-8')).hexdigest()


# ============================================================================
# Token Verification
# ============================================================================

def verify_api_token(token: str, db: Session) -> Optional[ApiToken]:
    """
    Verify an API token and return the ApiToken record if valid.

    Verification steps:
    1. Check token has correct prefix
    2. Hash the token
    3. Lookup by hash (unique index)
    4. Check not revoked
    5. Check not expired

    Note: last_used_at is NOT updated here - that happens asynchronously
    to avoid synchronous DB writes on every request (Ragen's condition).

    Args:
        token: The plaintext token from Authorization header
        db: Database session

    Returns:
        ApiToken record if valid, None if invalid/expired/revoked
    """
    # Check prefix first (O(1) operation)
    if not token.startswith(TOKEN_PREFIX):
        return None

    # Hash the token for lookup
    token_hash = hash_api_token(token)

    # Query by hash (uses unique index)
    api_token = db.query(ApiToken).filter(
        ApiToken.token_hash == token_hash,
        ApiToken.revoked_at.is_(None)  # Not revoked
    ).first()

    if not api_token:
        return None

    # Check expiration
    if api_token.expires_at is not None:
        now = datetime.now(timezone.utc)
        if api_token.expires_at < now:
            return None

    return api_token


def verify_api_token_with_timing_safe(token: str, stored_hash: str) -> bool:
    """
    Timing-safe comparison of token against stored hash.

    Args:
        token: Plaintext token to verify
        stored_hash: SHA-256 hash from database

    Returns:
        True if token matches, False otherwise
    """
    computed_hash = hash_api_token(token)
    return secrets.compare_digest(computed_hash, stored_hash)


# ============================================================================
# Scope Validation
# ============================================================================

def validate_scopes(scopes: list) -> Tuple[bool, list]:
    """
    Validate that all requested scopes are valid.

    Args:
        scopes: List of scope strings to validate

    Returns:
        Tuple of (is_valid, invalid_scopes)
    """
    invalid = [s for s in scopes if s not in VALID_SCOPES]
    return len(invalid) == 0, invalid


def check_scope(api_token: ApiToken, required_scope: str) -> bool:
    """
    Check if an API token has a required scope.

    Args:
        api_token: The ApiToken record
        required_scope: The scope to check for

    Returns:
        True if token has the scope, False otherwise
    """
    return required_scope in (api_token.scopes or [])


# ============================================================================
# User Token Count Validation
# ============================================================================

def get_user_token_count(user_id: str, db: Session) -> int:
    """
    Get count of active (non-revoked) tokens for a user.

    Args:
        user_id: User UUID
        db: Database session

    Returns:
        Number of active tokens
    """
    return db.query(ApiToken).filter(
        ApiToken.user_id == user_id,
        ApiToken.revoked_at.is_(None)
    ).count()


def can_create_token(user_id: str, db: Session) -> Tuple[bool, int]:
    """
    Check if user can create another token (under limit).

    Args:
        user_id: User UUID
        db: Database session

    Returns:
        Tuple of (can_create, current_count)
    """
    count = get_user_token_count(user_id, db)
    return count < MAX_TOKENS_PER_USER, count

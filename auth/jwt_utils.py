"""
KGRAG Auth Service - JWT Token Management
Generate and validate JWT tokens for stateless authentication.

Updated: Added JTI claims for token blacklisting support.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
import uuid
import jwt
from passlib.context import CryptContext
import os
import logging

logger = logging.getLogger(__name__)

# JWT Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "CHANGE_THIS_IN_PRODUCTION_PLEASE")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ============================================================================
# Password Utilities
# ============================================================================

def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)


# ============================================================================
# JWT Token Generation
# ============================================================================

def create_access_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Create JWT access token.

    Args:
        data: Payload to encode in token (user_id, email, roles, etc.)
        expires_delta: Optional custom expiration time

    Returns:
        Encoded JWT token string
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({
        "jti": str(uuid.uuid4()),  # JWT ID for blacklisting
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "access"
    })

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_refresh_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Create JWT refresh token for obtaining new access tokens.

    Args:
        data: Payload to encode (typically just user_id)
        expires_delta: Optional custom expiration time

    Returns:
        Encoded JWT refresh token string
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    to_encode.update({
        "jti": str(uuid.uuid4()),  # JWT ID for blacklisting
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "refresh"
    })

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


# ============================================================================
# JWT Token Validation
# ============================================================================

def decode_token(token: str, check_blacklist: bool = True) -> Optional[Dict[str, Any]]:
    """
    Decode and validate JWT token, checking blacklist and revoke_all.

    Args:
        token: JWT token string
        check_blacklist: Whether to check token against blacklist (default True)

    Returns:
        Decoded payload if valid, None if invalid/expired/revoked
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

        if check_blacklist:
            # Import here to avoid circular imports
            from auth.token_blacklist import is_blacklisted, get_revocation_timestamp

            # Check 1: Individual token blacklist (by JTI)
            jti = payload.get("jti")
            if jti and is_blacklisted(jti):
                logger.info(f"Token {jti[:8]}... is blacklisted")
                return None

            # Check 2: User-wide revocation (revoke_all_sessions)
            user_id = payload.get("sub")
            if user_id:
                revoked_at = get_revocation_timestamp(user_id)
                # Handle iat as datetime or timestamp
                token_iat = payload.get("iat")
                if token_iat:
                    # Convert datetime to timestamp if needed
                    if isinstance(token_iat, datetime):
                        token_iat = token_iat.timestamp()
                    elif not isinstance(token_iat, (int, float)):
                        token_iat = 0

                    if revoked_at and token_iat < revoked_at:
                        logger.info(f"Token issued before revoke_all for user {user_id}")
                        return None

        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.PyJWTError:
        return None


def verify_access_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify access token and return payload.

    Args:
        token: JWT access token

    Returns:
        Decoded payload if valid access token, None otherwise
    """
    payload = decode_token(token)

    if not payload:
        return None

    if payload.get("type") != "access":
        return None

    return payload


def verify_refresh_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify refresh token and return payload.

    Args:
        token: JWT refresh token

    Returns:
        Decoded payload if valid refresh token, None otherwise
    """
    payload = decode_token(token)

    if not payload:
        return None

    if payload.get("type") != "refresh":
        return None

    return payload


# ============================================================================
# Token Payload Helpers
# ============================================================================

def create_token_payload(
    user_id: str,
    email: str,
    roles: list[str],
    companies: list[str] = None,
    is_superuser: bool = False
) -> Dict[str, Any]:
    """
    Create standardized token payload.

    Args:
        user_id: User's unique identifier
        email: User's email address
        roles: List of role names
        companies: List of company IDs user belongs to
        is_superuser: Whether user is superuser

    Returns:
        Token payload dictionary
    """
    return {
        "sub": user_id,  # Standard JWT subject claim
        "email": email,
        "roles": roles,
        "companies": companies or [],
        "is_superuser": is_superuser
    }


def extract_user_id(token: str) -> Optional[str]:
    """
    Extract user ID from token without full validation.
    Useful for logging/debugging.

    Args:
        token: JWT token

    Returns:
        User ID if present, None otherwise
    """
    payload = decode_token(token)
    return payload.get("sub") if payload else None

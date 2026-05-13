"""
Cognee gRPC Authentication Interceptor

Self-contained auth interceptor for the Cognee microservice.
Mirrors the main KGRAG gRPC server's auth logic but is packaged
independently — the Cognee container does NOT have access to
``grpc_server`` or ``api`` packages.

DUAL AUTH SUPPORT:
  - JWT tokens:  Direct decode (fast, no HTTP call)
  - API keys (lgn_*):  Verified via auth service ``/verify`` endpoint

Token blacklist / revoke-all checks use the same Redis instance as
the auth service.

Health-check endpoints are public (no auth required).
All other methods REJECT requests without a valid token
(``UNAUTHENTICATED``).
"""

import os
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import grpc
import httpx
import jwt
import redis
import structlog

logger = structlog.get_logger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://auth-service:8001")
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "CHANGE_THIS_IN_PRODUCTION_PLEASE")
JWT_ALGORITHM = "HS256"

REDIS_URI = os.getenv("REDIS_URI", "redis://localhost:6379/0")
FAIL_OPEN = os.getenv("TOKEN_BLACKLIST_FAIL_OPEN", "false").lower() == "true"

BLACKLIST_PREFIX = "token:blacklist:"
REVOKED_AT_PREFIX = "token:revoked_at:"


# ── CurrentUser ──────────────────────────────────────────────────────────────


class CurrentUser:
    """Authenticated user context injected by the interceptor."""

    def __init__(
        self,
        user_id: str,
        email: str,
        roles: List[str],
        is_superuser: bool = False,
        companies: List[str] | None = None,
    ):
        self.user_id = user_id
        self.email = email
        self.roles = roles
        self.is_superuser = is_superuser
        self.companies = companies or []

    def __repr__(self) -> str:
        return f"<User {self.email} (roles: {', '.join(self.roles)})>"


# Context variable — downstream servicer code can read the current user.
current_user_context: ContextVar[CurrentUser | None] = ContextVar(
    "current_user",
    default=None,
)


# ── Redis client for token blacklist ─────────────────────────────────────────

_redis_client: redis.Redis | None = None


def _get_redis() -> redis.Redis:
    """Lazy-init synchronous Redis client (O(1) lookups are fast enough)."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(REDIS_URI, decode_responses=True)
    return _redis_client


def is_blacklisted(jti: str) -> bool:
    """Return True if the JWT ID is individually blacklisted."""
    try:
        return _get_redis().exists(f"{BLACKLIST_PREFIX}{jti}") == 1
    except redis.RedisError as exc:
        logger.error("blacklist_check_failed", error=str(exc))
        return not FAIL_OPEN  # fail-closed by default


def get_revocation_timestamp(user_id: str) -> Optional[float]:
    """Return the ``revoke_all_sessions`` timestamp for *user_id*, or None."""
    try:
        value = _get_redis().get(f"{REVOKED_AT_PREFIX}{user_id}")
        return float(value) if value else None
    except (redis.RedisError, ValueError) as exc:
        logger.error("revocation_ts_check_failed", error=str(exc))
        return None


# ── Token verification ───────────────────────────────────────────────────────


def verify_jwt_token(token: str) -> Optional[Dict[str, Any]]:
    """Decode and validate a JWT access token.

    Checks expiry, type claim, individual blacklist, and user-wide
    revocation — identical logic to the main gRPC server.
    """
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])

        if payload.get("type") != "access":
            logger.warning("jwt_not_access_token")
            return None

        # Individual blacklist
        jti = payload.get("jti")
        if jti and is_blacklisted(jti):
            logger.warning("jwt_blacklisted", jti=jti[:8])
            return None

        # User-wide revocation
        user_id = payload.get("sub")
        if user_id:
            revoked_at = get_revocation_timestamp(user_id)
            token_iat = payload.get("iat", 0)
            if isinstance(token_iat, datetime):
                token_iat = token_iat.timestamp()
            elif not isinstance(token_iat, (int, float)):
                token_iat = 0
            if revoked_at and token_iat < revoked_at:
                logger.warning("jwt_issued_before_revoke_all", user_id=user_id)
                return None

        return {
            "user_id": payload.get("sub"),
            "email": payload.get("email"),
            "roles": payload.get("roles", []),
            "is_superuser": payload.get("is_superuser", False),
            "companies": payload.get("companies", []),
        }
    except jwt.ExpiredSignatureError:
        logger.warning("jwt_expired")
        return None
    except jwt.InvalidTokenError as exc:
        logger.warning("jwt_invalid", error=str(exc))
        return None
    except Exception as exc:
        logger.error("jwt_verification_error", error=str(exc))
        return None


async def verify_api_token_via_auth_service(
    token: str,
) -> Optional[Dict[str, Any]]:
    """Verify an API key (``lgn_*``) via the auth service ``/verify`` endpoint."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                f"{AUTH_SERVICE_URL}/verify",
                json={"token": token},
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("valid"):
                    return {
                        "user_id": data.get("user_id"),
                        "email": data.get("email"),
                        "roles": data.get("roles", []),
                        "is_superuser": data.get("is_superuser", False),
                        "companies": data.get("companies", []),
                    }
            return None
    except httpx.TimeoutException:
        logger.error("auth_service_timeout")
        return None
    except httpx.RequestError as exc:
        logger.error("auth_service_connection_error", error=str(exc))
        return None
    except Exception as exc:
        logger.error("api_token_verification_error", error=str(exc))
        return None


# ── Abort helper ─────────────────────────────────────────────────────────────


class _AbortHandler(grpc.RpcMethodHandler):
    """Synthetic RPC handler that immediately aborts with UNAUTHENTICATED."""

    def __init__(self, code: grpc.StatusCode, details: str):
        self._code = code
        self._details = details
        # Satisfy the grpc.RpcMethodHandler interface
        self.request_streaming = False
        self.response_streaming = False
        self.request_deserializer = None
        self.response_serializer = None

        async def _abort(_request, context: grpc.aio.ServicerContext):
            await context.abort(self._code, self._details)

        self.unary_unary = _abort
        self.unary_stream = None
        self.stream_unary = None
        self.stream_stream = None


# ── Interceptor ──────────────────────────────────────────────────────────────


class CogneeAuthInterceptor(grpc.aio.ServerInterceptor):
    """Validates JWT / API-key tokens on every Cognee gRPC call.

    Health-check endpoints are exempt.  All other requests without a
    valid ``authorization: Bearer <token>`` header are rejected with
    ``UNAUTHENTICATED``.
    """

    # Methods that skip authentication (health probes)
    PUBLIC_METHODS = {
        "/kgrag.cognee.CogneeService/Health",
        "/grpc.health.v1.Health/Check",
        "/grpc.health.v1.Health/Watch",
    }

    async def intercept_service(
        self,
        continuation: Callable,
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        method = handler_call_details.method

        # ── Public endpoints — no auth required ──────────────────────
        if method in self.PUBLIC_METHODS:
            return await continuation(handler_call_details)

        # ── Extract token ────────────────────────────────────────────
        metadata_dict = dict(handler_call_details.invocation_metadata)
        auth_header = metadata_dict.get("authorization", "")

        if not auth_header:
            logger.warning("auth.missing_header", method=method)
            return _AbortHandler(
                grpc.StatusCode.UNAUTHENTICATED,
                "Missing authorization header",
            )

        parts = auth_header.split(" ")
        if len(parts) != 2 or parts[0].lower() != "bearer":
            logger.warning("auth.invalid_format", method=method)
            return _AbortHandler(
                grpc.StatusCode.UNAUTHENTICATED,
                "Invalid authorization format — expected 'Bearer <token>'",
            )

        token = parts[1]

        # ── Dual auth: route by prefix ───────────────────────────────
        if token.startswith("lgn_"):
            payload = await verify_api_token_via_auth_service(token)
        else:
            payload = verify_jwt_token(token)

        if not payload:
            logger.warning("auth.invalid_token", method=method)
            return _AbortHandler(
                grpc.StatusCode.UNAUTHENTICATED,
                "Invalid or expired token",
            )

        # ── Build CurrentUser and inject into context ────────────────
        user = CurrentUser(
            user_id=payload["user_id"],
            email=payload["email"],
            roles=payload["roles"],
            is_superuser=payload["is_superuser"],
            companies=payload["companies"],
        )

        logger.debug(
            "auth.authenticated",
            method=method,
            user_id=user.user_id,
            email=user.email,
        )

        # Wrap the handler so that ``current_user_context`` is set for
        # the duration of the RPC.
        handler = await continuation(handler_call_details)
        if handler is None:
            return handler

        if handler.unary_unary:
            original = handler.unary_unary

            async def wrapped_unary_unary(request, context):
                current_user_context.set(user)
                try:
                    return await original(request, context)
                finally:
                    current_user_context.set(None)

            return grpc.unary_unary_rpc_method_handler(
                wrapped_unary_unary,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        # Fallback: non-unary handlers pass through authenticated
        # (no context injection — extend if Cognee adds streaming RPCs).
        return handler

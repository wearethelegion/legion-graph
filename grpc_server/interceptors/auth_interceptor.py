"""
gRPC Authentication Interceptor
Validates JWT tokens and API keys, injects CurrentUser into context.

DUAL AUTH SUPPORT:
- JWT tokens: Direct decode (fast, no HTTP call)
- API keys (lgn_*): Verified via auth service /verify endpoint

Updated: Added blacklist and revoke_all checks for logout support.
Updated: Added API key authentication via auth service.
"""

import os
import grpc
import jwt
import httpx
from datetime import datetime
from typing import Callable, Any, Optional, Dict
from contextvars import ContextVar
from loguru import logger

from api.auth import CurrentUser

# Auth service URL for API key verification
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://auth-service:8001")

# JWT Configuration - must match auth service
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "CHANGE_THIS_IN_PRODUCTION_PLEASE")
JWT_ALGORITHM = "HS256"

# Context variable to store current user across async calls
current_user_context: ContextVar[CurrentUser | None] = ContextVar("current_user", default=None)

# Import blacklist functions — local module connects to same Redis as auth service
from grpc_server.token_blacklist import is_blacklisted, get_revocation_timestamp


async def verify_api_token_via_auth_service(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify API token (lgn_*) via auth service /verify endpoint.

    API keys require database lookup, so we delegate to auth service
    which has DB access and handles usage tracking.

    Args:
        token: API token string (must start with lgn_)

    Returns:
        User payload dict if valid, None if invalid/expired
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(f"{AUTH_SERVICE_URL}/verify", json={"token": token})

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
        logger.error("Auth service timeout during API token verification")
        return None
    except httpx.RequestError as e:
        logger.error(f"Auth service connection error: {e}")
        return None
    except Exception as e:
        logger.error(f"API token verification error: {e}")
        return None


def verify_jwt_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify and decode JWT token directly (no HTTP call needed).

    UPDATED: Now includes blacklist and revoke_all checks.

    Args:
        token: JWT access token string

    Returns:
        Decoded payload dict if valid, None if invalid/expired/revoked
    """
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])

        # Verify it's an access token
        if payload.get("type") != "access":
            logger.warning("Token is not an access token")
            return None

        # NEW: Check 1 — Individual token blacklist
        jti = payload.get("jti")
        if jti and is_blacklisted(jti):
            logger.warning(f"Token {jti[:8]}... is blacklisted")
            return None

        # NEW: Check 2 — User-wide revocation (revoke_all_sessions)
        user_id = payload.get("sub")
        if user_id:
            revoked_at = get_revocation_timestamp(user_id)
            token_iat = payload.get("iat", 0)
            # Handle iat as datetime or timestamp
            if token_iat:
                if isinstance(token_iat, datetime):
                    token_iat = token_iat.timestamp()
                elif not isinstance(token_iat, (int, float)):
                    token_iat = 0

                if revoked_at and token_iat < revoked_at:
                    logger.warning(f"Token issued before revoke_all for user {user_id}")
                    return None

        # Return normalized payload (UNCHANGED from original)
        return {
            "user_id": payload.get("sub"),
            "email": payload.get("email"),
            "roles": payload.get("roles", []),
            "is_superuser": payload.get("is_superuser", False),
            "companies": payload.get("companies", []),
        }
    except jwt.ExpiredSignatureError:
        logger.warning("JWT token has expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid JWT token: {e}")
        return None
    except Exception as e:
        logger.error(f"JWT verification error: {e}")
        return None


class AuthenticationInterceptor(grpc.aio.ServerInterceptor):
    """
    Intercepts gRPC calls to validate JWT tokens and API keys.

    DUAL AUTH SUPPORT:
    - API keys (lgn_*): Verified via auth service /verify endpoint
    - JWT tokens: Direct decode (fast, no HTTP call)

    Validates tokens from metadata["authorization"] = "Bearer <token>"
    and stores CurrentUser in context for downstream use.

    Public methods (no auth required):
    - AuthService/Authenticate
    """

    # Methods that don't require authentication
    PUBLIC_METHODS = {
        "/kgrag.auth.AuthService/Authenticate",
    }

    async def intercept_service(
        self, continuation: Callable, handler_call_details: grpc.HandlerCallDetails
    ) -> grpc.RpcMethodHandler:
        """
        Intercept incoming RPC calls.

        Args:
            continuation: Function to invoke the actual RPC handler
            handler_call_details: Details about the RPC call

        Returns:
            RPC method handler
        """
        method = handler_call_details.method

        # Skip auth for public methods
        if method in self.PUBLIC_METHODS:
            return await continuation(handler_call_details)

        # Extract token from metadata
        metadata_dict = dict(handler_call_details.invocation_metadata)
        auth_header = metadata_dict.get("authorization", "")

        if not auth_header:
            logger.warning(f"No authorization header for {method}")
            # Let it through - servicer will handle error
            return await continuation(handler_call_details)

        # Parse "Bearer <token>"
        parts = auth_header.split(" ")
        if len(parts) != 2 or parts[0].lower() != "bearer":
            logger.warning(f"Invalid authorization format for {method}")
            return await continuation(handler_call_details)

        token = parts[1]

        # DUAL AUTH: Route based on token prefix (same pattern as auth/main.py lines 602-627)
        if token.startswith("lgn_"):
            # API key path - verify via auth service
            payload = await verify_api_token_via_auth_service(token)
        else:
            # JWT path - verify directly (no HTTP call)
            payload = verify_jwt_token(token)

        if not payload:
            logger.warning(f"Invalid token for {method}")
            # Let it through - servicer will handle error
            return await continuation(handler_call_details)

        # Create CurrentUser and inject into metadata
        current_user = CurrentUser(
            user_id=payload["user_id"],
            email=payload["email"],
            roles=payload["roles"],
            is_superuser=payload["is_superuser"],
            companies=payload["companies"],
        )

        # Add user to invocation metadata for servicer access
        # Note: We can't modify invocation_metadata directly in aio interceptors,
        # so we'll store it in a way the servicer can access it
        # We'll use a modified continuation that passes the user through context

        async def modified_continuation(modified_details):
            handler = await continuation(modified_details)

            # Wrap the handler to inject user into context
            if handler and handler.unary_unary:
                original_handler = handler.unary_unary

                async def wrapped_handler(request, context):
                    # Store user in context variable for async access
                    current_user_context.set(current_user)
                    try:
                        return await original_handler(request, context)
                    finally:
                        current_user_context.set(None)

                return grpc.unary_unary_rpc_method_handler(
                    wrapped_handler,
                    request_deserializer=handler.request_deserializer,
                    response_serializer=handler.response_serializer,
                )

            if handler and handler.unary_stream:
                original_stream = handler.unary_stream

                async def wrapped_stream(request, context):
                    # Set user context before the first yield — same pattern as unary_unary above.
                    current_user_context.set(current_user)
                    try:
                        async for item in original_stream(request, context):
                            yield item
                    finally:
                        current_user_context.set(None)

                return grpc.unary_stream_rpc_method_handler(
                    wrapped_stream,
                    request_deserializer=handler.request_deserializer,
                    response_serializer=handler.response_serializer,
                )

            # NOTE: stream_unary and stream_stream are not wrapped here because no RPCs
            # of those shapes currently exist in this service (YAGNI).

            return handler

        return await modified_continuation(handler_call_details)

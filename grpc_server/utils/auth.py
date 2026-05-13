"""
gRPC Authentication Utilities
Helper functions for extracting authenticated user from gRPC context.
"""

import grpc
from api.auth import CurrentUser
from loguru import logger

# Import context variable (will be set by auth interceptor)
try:
    from grpc_server.interceptors.auth_interceptor import current_user_context
except ImportError:
    from contextvars import ContextVar
    current_user_context: ContextVar = ContextVar('current_user', default=None)


class AuthenticationError(Exception):
    """Raised when authentication is required but no valid user found."""
    pass


def get_current_user_from_context(context: grpc.aio.ServicerContext) -> CurrentUser:
    """
    Extract CurrentUser from gRPC context.

    This function expects the AuthenticationInterceptor to have already
    validated the JWT token and stored CurrentUser in the context variable.

    Args:
        context: gRPC servicer context

    Returns:
        CurrentUser object

    Raises:
        AuthenticationError: If no valid user in context
    """
    # Get current_user from context variable (set by auth interceptor)
    current_user = current_user_context.get()

    if current_user:
        return current_user

    # No authenticated user - raise exception
    logger.warning("No authenticated user found in context")
    raise AuthenticationError("Authentication required")


def extract_token_from_metadata(context: grpc.ServicerContext) -> str | None:
    """
    Extract JWT token from gRPC metadata.

    Looks for 'authorization' header with format: "Bearer <token>"

    Args:
        context: gRPC servicer context

    Returns:
        Token string if found, None otherwise
    """
    metadata = dict(context.invocation_metadata())

    auth_header = metadata.get("authorization")
    if not auth_header:
        return None

    # Parse "Bearer <token>" format
    parts = auth_header.split(" ")
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None

    return parts[1]

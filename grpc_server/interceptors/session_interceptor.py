"""
Session Context Interceptor for gRPC Server

Extracts x-session-id from gRPC metadata and makes it available
to all downstream handlers via a ContextVar. This allows KGRAG
to correlate which tool invocations happened during which CLI session.

The session ID is set by the opencode CLI client on every gRPC call
via the x-session-id metadata header.
"""

import grpc
from typing import Callable
from contextvars import ContextVar
from loguru import logger


# Context variable to store session ID across async calls
session_id_context: ContextVar[str | None] = ContextVar("session_id", default=None)


def get_session_id() -> str | None:
    """Get the current session ID from context. Use in any servicer."""
    return session_id_context.get()


class SessionContextInterceptor(grpc.aio.ServerInterceptor):
    """
    Intercepts gRPC calls to extract x-session-id from metadata
    and store it in a ContextVar for downstream access.

    This interceptor should be placed AFTER LoggingInterceptor and
    BEFORE AuthenticationInterceptor so the session ID is available
    to all subsequent interceptors and servicers.
    """

    async def intercept_service(
        self,
        continuation: Callable,
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        metadata_dict = dict(handler_call_details.invocation_metadata)
        session_id = metadata_dict.get("x-session-id")

        if session_id:
            logger.debug(f"Session ID: {session_id} for {handler_call_details.method}")

        handler = await continuation(handler_call_details)

        if handler and handler.unary_unary:
            original_handler = handler.unary_unary

            async def wrapped_handler(request, context):
                token = session_id_context.set(session_id)
                try:
                    return await original_handler(request, context)
                finally:
                    session_id_context.reset(token)

            return grpc.unary_unary_rpc_method_handler(
                wrapped_handler,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        return handler

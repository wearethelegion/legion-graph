"""
Logging Interceptor for gRPC Server
Logs all incoming RPC calls with method, duration, and status.
"""

import time
import traceback
import grpc
import uuid
from typing import Callable
from loguru import logger


class LoggingInterceptor(grpc.aio.ServerInterceptor):
    """
    Intercepts gRPC calls to log request/response details.

    Logs:
    - RPC method name
    - Request ID (from metadata or generated)
    - Duration (ms)
    - Status (success/error)
    """

    async def intercept_service(
        self, continuation: Callable, handler_call_details: grpc.HandlerCallDetails
    ) -> grpc.RpcMethodHandler:
        """
        Intercept incoming RPC calls for logging.

        Args:
            continuation: Function to invoke the actual RPC handler
            handler_call_details: Details about the RPC call

        Returns:
            RPC method handler
        """
        method = handler_call_details.method

        # Extract or generate request ID
        metadata_dict = dict(handler_call_details.invocation_metadata)
        request_id = metadata_dict.get("request-id", str(uuid.uuid4()))
        session_id = metadata_dict.get("x-session-id")

        # Log incoming request
        session_tag = f" | session={session_id}" if session_id else ""
        logger.info(f"🔵 gRPC Request: {method} | request_id={request_id}{session_tag}")

        # Get handler from continuation
        handler = await continuation(handler_call_details)

        # Wrap the handler to track duration
        if handler and handler.unary_unary:
            original_handler = handler.unary_unary

            async def wrapped_handler(request, context):
                start_time = time.time()
                status = "success"
                error_msg = None

                try:
                    response = await original_handler(request, context)

                    # Check if response has error status
                    if hasattr(response, "status") and response.status == "error":
                        status = "error"
                        error_msg = getattr(response, "error_message", "Unknown error")

                    return response

                except Exception as e:
                    status = "error"
                    error_msg = str(e)
                    tb = traceback.format_exc()
                    logger.error(
                        f"🔥 gRPC FULL TRACEBACK: {method} | "
                        f"request_id={request_id} | "
                        f"metadata={dict(metadata_dict)}\n{tb}"
                    )
                    raise

                finally:
                    duration_ms = (time.time() - start_time) * 1000

                    if status == "success":
                        logger.info(
                            f"✅ gRPC Success: {method} | "
                            f"request_id={request_id}{session_tag} | "
                            f"duration={duration_ms:.2f}ms"
                        )
                    else:
                        logger.error(
                            f"❌ gRPC Error: {method} | "
                            f"request_id={request_id}{session_tag} | "
                            f"duration={duration_ms:.2f}ms | "
                            f"error={error_msg}"
                        )

            return grpc.unary_unary_rpc_method_handler(
                wrapped_handler,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        return handler

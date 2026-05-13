"""
Idempotency Interceptor for gRPC Server
Prevents duplicate request processing using in-memory cache.
"""

import grpc
import time
from typing import Callable, Dict, Any, Optional
from loguru import logger


class IdempotencyCache:
    """
    In-memory cache for idempotent requests.

    Stores successful responses by request_id with TTL (1 hour default).
    """

    def __init__(self, ttl_seconds: int = 3600):
        """
        Initialize cache.

        Args:
            ttl_seconds: Time-to-live for cached responses (default 1 hour)
        """
        self._cache: Dict[str, tuple[Any, float]] = {}
        self._ttl = ttl_seconds

    def get(self, request_id: str) -> Optional[Any]:
        """
        Get cached response for request_id.

        Args:
            request_id: Unique request identifier

        Returns:
            Cached response if found and not expired, None otherwise
        """
        if request_id not in self._cache:
            return None

        response, timestamp = self._cache[request_id]

        # Check if expired
        if time.time() - timestamp > self._ttl:
            del self._cache[request_id]
            return None

        return response

    def set(self, request_id: str, response: Any):
        """
        Cache successful response.

        Args:
            request_id: Unique request identifier
            response: Response to cache
        """
        self._cache[request_id] = (response, time.time())

    def cleanup_expired(self):
        """Remove expired entries from cache."""
        now = time.time()
        expired_keys = [
            key for key, (_, timestamp) in self._cache.items()
            if now - timestamp > self._ttl
        ]

        for key in expired_keys:
            del self._cache[key]

        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")


class IdempotencyInterceptor(grpc.aio.ServerInterceptor):
    """
    Intercepts gRPC calls to implement idempotency.

    - Extracts request_id from metadata
    - Returns cached response if found
    - Caches successful responses for future requests

    Note: Only caches responses with status="success"
    """

    def __init__(self, cache_ttl_seconds: int = 3600):
        """
        Initialize interceptor.

        Args:
            cache_ttl_seconds: Cache TTL (default 1 hour)
        """
        self.cache = IdempotencyCache(ttl_seconds=cache_ttl_seconds)

    async def intercept_service(
        self,
        continuation: Callable,
        handler_call_details: grpc.HandlerCallDetails
    ) -> grpc.RpcMethodHandler:
        """
        Intercept incoming RPC calls for idempotency check.

        Args:
            continuation: Function to invoke the actual RPC handler
            handler_call_details: Details about the RPC call

        Returns:
            RPC method handler
        """
        method = handler_call_details.method

        # Extract request_id from metadata
        metadata_dict = dict(handler_call_details.invocation_metadata)
        request_id = metadata_dict.get("request-id")

        # If no request_id, skip idempotency check
        if not request_id:
            return await continuation(handler_call_details)

        # Check cache for existing response
        cached_response = self.cache.get(request_id)
        if cached_response:
            logger.info(
                f"🔄 Returning cached response for {method} | request_id={request_id}"
            )

            # Create a handler that returns cached response
            async def cached_handler(request, context):
                return cached_response

            handler = await continuation(handler_call_details)

            return grpc.unary_unary_rpc_method_handler(
                cached_handler,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer
            )

        # No cached response, proceed with request
        handler = await continuation(handler_call_details)

        # Wrap handler to cache successful responses
        if handler and handler.unary_unary:
            original_handler = handler.unary_unary

            async def caching_handler(request, context):
                response = await original_handler(request, context)

                # Cache only successful responses
                if hasattr(response, "status") and response.status == "success":
                    self.cache.set(request_id, response)
                    logger.debug(f"💾 Cached response for request_id={request_id}")

                return response

            return grpc.unary_unary_rpc_method_handler(
                caching_handler,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer
            )

        return handler

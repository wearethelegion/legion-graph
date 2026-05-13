"""
KGRAG gRPC Server
Main server implementation with kept servicers and interceptors.

Phase 1 STRIP: Only 5 servicers remain:
  AuthService, CodeService, CodeSearchService, DocumentSearchService, IngestionService.
"""

import grpc
import asyncio
from concurrent import futures
from loguru import logger

from grpc_server.config import GrpcServerConfig
from grpc_server.interceptors import (
    LoggingInterceptor,
    SessionContextInterceptor,
    AuthenticationInterceptor,
    IdempotencyInterceptor,
)

# Import proto stubs FIRST (prevents duplicate descriptor registration)
from grpc_server.protos.loader import (
    auth_pb2_grpc,
    code_pb2_grpc,
    code_search_pb2_grpc,
    document_search_pb2_grpc,
    ingestion_pb2_grpc,
)

# Import servicers AFTER protos are loaded
from grpc_server.servicers.auth_servicer import AuthServicer
from grpc_server.servicers.code_servicer import CodeServicer
from grpc_server.servicers.code_search_servicer import CodeSearchServicer
from grpc_server.servicers.document_search_servicer import DocumentSearchServicer
from grpc_server.servicers.ingestion_servicer import IngestionServicer


async def serve():
    """
    Start gRPC server with kept servicers and interceptors.

    Servicers:
    - AuthService (authentication, project management)
    - CodeService (code intelligence)
    - CodeSearchService (code search)
    - DocumentSearchService (document search)
    - IngestionService (code/document ingestion)

    Interceptors (order matters):
    1. LoggingInterceptor - logs all requests
    2. AuthenticationInterceptor - validates JWT tokens
    3. IdempotencyInterceptor - prevents duplicate processing
    """
    # Initialize database pool
    from api.database import init_db_pool

    await init_db_pool()
    logger.info("✅ Database pool initialized")

    # Load configuration
    config = GrpcServerConfig.from_env()

    logger.info(f"🚀 Starting KGRAG gRPC Server")
    logger.info(f"   Host: {config.host}")
    logger.info(f"   Port: {config.port}")
    logger.info(f"   Max Workers: {config.max_workers}")
    logger.info(f"   Idempotency Cache TTL: {config.idempotency_cache_ttl}s")

    # Create server with interceptors
    server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=config.max_workers),
        interceptors=[
            LoggingInterceptor(),
            SessionContextInterceptor(),
            AuthenticationInterceptor(),
            IdempotencyInterceptor(cache_ttl_seconds=config.idempotency_cache_ttl),
        ],
    )

    # Register all servicers
    logger.info("📦 Registering servicers...")

    auth_pb2_grpc.add_AuthServiceServicer_to_server(AuthServicer(), server)
    logger.info("   ✅ AuthService registered")

    code_pb2_grpc.add_CodeServiceServicer_to_server(CodeServicer(), server)
    logger.info("   ✅ CodeService registered")

    code_search_pb2_grpc.add_CodeSearchServiceServicer_to_server(CodeSearchServicer(), server)
    logger.info("   ✅ CodeSearchService registered")

    document_search_pb2_grpc.add_DocumentSearchServiceServicer_to_server(
        DocumentSearchServicer(), server
    )
    logger.info("   ✅ DocumentSearchService registered")

    ingestion_pb2_grpc.add_IngestionServiceServicer_to_server(IngestionServicer(), server)
    logger.info("   ✅ IngestionService registered")

    # Start Prometheus metrics HTTP server for CodeSearch observability
    try:
        from grpc_server.servicers.code_search_metrics import start_metrics_server

        start_metrics_server()
        logger.info("   ✅ CodeSearch Prometheus metrics server started")
    except Exception as _metrics_err:
        logger.warning("   ⚠️  CodeSearch metrics server failed to start: %s", _metrics_err)

    server.add_insecure_port(f"{config.host}:{config.port}")

    logger.info(f"🎯 Server listening on {config.host}:{config.port}")

    # Start server
    await server.start()
    logger.info("✅ KGRAG gRPC Server started successfully")

    # Wait for termination
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down server...")
        await server.stop(grace=5)

        # Cleanup database pool
        from api.database import close_db_pool

        await close_db_pool()

        logger.info("✅ Server stopped gracefully")


def main():
    """Entry point for gRPC server."""
    try:
        asyncio.run(serve())
    except Exception as e:
        logger.error(f"❌ Server failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

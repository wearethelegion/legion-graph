"""
Cognee gRPC Server

Starts the async gRPC server, configures cognee, and registers:
  - CogneeServicer (Cognify / Search / Prune / Health)
  - BrainServicer  (Brain v2 CRUD — 29 RPCs)
  - gRPC built-in health servicer (for Docker/k8s readiness probes)

Environment variables (see .env.cognee.example for full list):
  GRPC_PORT     - port to listen on (default: 50052)
"""

import cognee_service.cognee_patches  # noqa: F401  — must be first, before any cognee imports

import asyncio
import logging
import os
import sys

import grpc
import structlog
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from dotenv import load_dotenv

from cognee_service.auth_interceptor import CogneeAuthInterceptor
from cognee_service.config import configure_cognee
from cognee_service.servicer import CogneeServicer
from cognee_service.brain.servicer import BrainServicer
from cognee_service.brain_content.servicer import BrainContentServicer
from cognee_service.generated import cognee_pb2_grpc
from cognee_service.generated import brain_pb2_grpc
from cognee_service.brain import db as brain_db
from cognee_service.brain import kafka_producer as brain_kafka

# Load .env file if present (ignored inside Docker where vars are injected)
load_dotenv()

# ── Logging configuration ────────────────────────────────────────────────────
# Set LOG_LEVEL=DEBUG to see all debug logs from the cognee service.
_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=getattr(logging, _log_level, logging.INFO),
)
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, _log_level, logging.INFO),
    ),
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


async def serve() -> None:
    """Start the cognee gRPC server and block until terminated."""
    port = int(os.environ.get("GRPC_PORT", "50052"))
    listen_addr = f"0.0.0.0:{port}"

    logger.info("cognee_server.configuring")
    configure_cognee()
    logger.info("cognee_server.cognee_configured")

    # Ensure Cognee relational tables exist (idempotent — CREATE IF NOT EXISTS).
    # Must run AFTER configure_cognee() so the DB URL is set.
    from cognee.infrastructure.databases.relational import create_db_and_tables

    await create_db_and_tables()
    logger.info("cognee_server.db_tables_ensured")

    server = grpc.aio.server(interceptors=[CogneeAuthInterceptor()])
    logger.info("cognee_server.auth_interceptor_registered")

    # Register cognee servicer
    cognee_pb2_grpc.add_CogneeServiceServicer_to_server(CogneeServicer(), server)
    logger.info("cognee_server.servicer_registered", servicer="CogneeService")

    # Register brain v2 servicer
    brain_pb2_grpc.add_BrainServiceServicer_to_server(BrainServicer(), server)
    logger.info("cognee_server.servicer_registered", servicer="BrainService")

    # Register additive brain content CRUD servicer
    brain_pb2_grpc.add_BrainContentServiceServicer_to_server(BrainContentServicer(), server)
    logger.info("cognee_server.servicer_registered", servicer="BrainContentService")

    # Initialise brain infrastructure (db pool + tables + kafka producer)
    await brain_db.get_pool()
    logger.info("cognee_server.brain_db_pool_initialised")
    # Create brain content tables idempotently (brain_knowledge / brain_expertise / brain_lessons).
    # Symmetric with create_db_and_tables() above for cognee's own tables; first-boot on a clean
    # Postgres no longer requires manual DDL.
    await brain_db.init_brain_tables()
    logger.info("cognee_server.brain_tables_ensured")

    # Register gRPC built-in health servicer
    health_servicer = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await health_servicer.set("kgrag.cognee.CogneeService", health_pb2.HealthCheckResponse.SERVING)
    await health_servicer.set("kgrag.brain.BrainService", health_pb2.HealthCheckResponse.SERVING)
    await health_servicer.set(
        "kgrag.brain.BrainContentService", health_pb2.HealthCheckResponse.SERVING
    )
    await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    logger.info("cognee_server.health_servicer_registered")

    server.add_insecure_port(listen_addr)
    await server.start()
    logger.info("cognee_server.started", addr=listen_addr)

    try:
        await server.wait_for_termination()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("cognee_server.stopping")
        await server.stop(grace=5)
        # Shutdown brain infrastructure
        await brain_kafka.shutdown_producer()
        await brain_db.shutdown_pool()
        logger.info("cognee_server.stopped")


def main() -> None:
    """Entry point."""
    try:
        asyncio.run(serve())
    except Exception as exc:
        logger.error("cognee_server.fatal", error=str(exc), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

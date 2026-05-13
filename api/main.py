"""
KGRAG REST API Server
FastAPI REST API for company/project/user management and code search.

Phase 1 STRIP: Removed stripped capabilities:
- Knowledge, expertise, lessons, agent_skills routes
- Engagements, tasks, delegation routes
- Memories, sessions routes
- Unified search route
- v2 brain routes (knowledge, expertise, lessons, search, engagements, entries)
- Legacy memory/conversation endpoints
- Stripped kgrag imports (vector_store, memory_graph_store, document_qdrant_store, etc.)
"""

from fastapi import FastAPI, HTTPException, status, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from contextlib import asynccontextmanager

from kgrag.config import config
from kgrag.embeddings import GeminiEmbedder
from loguru import logger
import os

# Import authentication
from api.auth import (
    get_current_user,
    get_current_user_optional,
    CurrentUser,
    validate_company_access,
)

# Import routes (kept capabilities only)
from api.routes.companies_v2 import router as companies_router
from api.routes.projects_v2 import router as projects_router
from api.routes.repositories_v2 import router as repositories_router
from api.routes.branches_v2 import router as branches_router
from api.routes.agents import router as agents_router
from api.routes.features import router as features_router
from api.routes.ingestion import router as ingestion_router
from api.routes.webhooks import router as webhooks_router
from api.routes.instructions import router as instructions_router
from api.routes.ingestions import router as ingestions_router
from api.routes.stats import router as stats_router
from api.routes.agent_workflows import router as agent_workflows_router
from api.routes.company_roles import router as company_roles_router
from api.routes.registration_requests import router as registration_requests_router
from api.routes.cli import router as cli_router
from api.routes.company_config import router as company_config_router
from api.routes.brain import router as brain_router
from api.routes.code_search import router as code_search_router

# Import database connection
from api.database import init_db_pool, close_db_pool

# Import cognee gRPC client
from api.services.cognee_service import CogneeGrpcClient, COGNEE_SERVICE_URL
from api.services.brain_content_service import BrainContentGrpcClient


# ============================================================================
# Pydantic Models (Request/Response schemas)
# ============================================================================


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    timestamp: str


# ============================================================================
# Application Lifecycle
# ============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting KGRAG REST API...")

    # Initialize database pool
    await init_db_pool()
    logger.info("✓ Database pool initialized")

    # Ensure instructions tables exist (company_instructions, project_instructions).
    # These are raw-asyncpg tables not declared in auth's SQLAlchemy ORM; without
    # this bootstrap, the first project create fails with `relation "project_instructions"
    # does not exist` on a fresh Postgres volume.
    try:
        from api.database.instructions_init import init_instructions_tables

        await init_instructions_tables()
    except Exception as e:
        logger.warning(f"Could not bootstrap instructions tables (non-fatal): {e}")

    # Initialize Neo4j and Qdrant repositories (singletons for connection pooling)
    try:
        from api.repositories import Neo4jRepository, QdrantRepository

        neo4j_repository = Neo4jRepository()
        qdrant_repository = QdrantRepository()
        await neo4j_repository.connect()

        app.state.neo4j_repository = neo4j_repository
        app.state.qdrant_repository = qdrant_repository

        # Initialize GeminiEmbedder for document operations
        embedder = GeminiEmbedder()
        app.state.embedder = embedder

        logger.info("✓ Repositories and embedder initialized")

    except Exception as e:
        logger.warning(f"Could not initialize repositories/embedder (non-fatal): {e}")

    # Initialize Kafka producer
    try:
        from api.services.kafka_service import init_kafka_service

        kafka_service = await init_kafka_service()
        app.state.kafka_service = kafka_service
        logger.info("✓ Kafka producer initialized")
    except Exception as e:
        logger.warning(f"Kafka initialization failed (non-fatal): {e}")

    # Cognee gRPC client (no-op when COGNEE_SERVICE_URL is unset)
    cognee_client = CogneeGrpcClient()
    await cognee_client.startup()
    app.state.cognee_service = cognee_client
    if COGNEE_SERVICE_URL:
        logger.info(f"✓ Cognee gRPC client connected to {COGNEE_SERVICE_URL}")
    else:
        logger.info("✓ Cognee gRPC client initialized (no-op mode — COGNEE_SERVICE_URL not set)")

    brain_content_client = BrainContentGrpcClient()
    await brain_content_client.startup()
    app.state.brain_content_service = brain_content_client
    if COGNEE_SERVICE_URL:
        logger.info(f"✓ BrainContent gRPC client connected to {COGNEE_SERVICE_URL}")
    else:
        logger.info(
            "✓ BrainContent gRPC client initialized (no-op mode — COGNEE_SERVICE_URL not set)"
        )

    yield

    # Cleanup
    logger.info("Shutting down KGRAG REST API...")

    # Cognee: drain pending tasks and close gRPC channel
    if hasattr(app.state, "cognee_service"):
        await app.state.cognee_service.shutdown()
        logger.info("✓ Cognee gRPC client shut down")

    if hasattr(app.state, "brain_content_service"):
        await app.state.brain_content_service.shutdown()
        logger.info("✓ BrainContent gRPC client shut down")

    # Close Neo4j connection
    if hasattr(app.state, "neo4j_repository"):
        await app.state.neo4j_repository.close()
        logger.info("✓ Neo4j connection closed")

    # Close database pool
    await close_db_pool()
    logger.info("✓ Database pool closed")

    # Shutdown Kafka producer
    if hasattr(app.state, "kafka_service"):
        try:
            from api.services.kafka_service import shutdown_kafka_service

            await shutdown_kafka_service()
            logger.info("✓ Kafka producer stopped")
        except Exception as e:
            logger.warning(f"Kafka shutdown error (non-fatal): {e}")

    logger.info("✓ Shutdown complete")


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="KGRAG REST API",
    description="Company/project/user management and code search REST API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes (kept capabilities only)
app.include_router(companies_router)
app.include_router(projects_router)
app.include_router(repositories_router)
app.include_router(branches_router)
app.include_router(agents_router)
app.include_router(features_router)
app.include_router(ingestion_router)
app.include_router(webhooks_router)
app.include_router(instructions_router)
app.include_router(ingestions_router)
app.include_router(stats_router)
app.include_router(agent_workflows_router)
app.include_router(company_roles_router)
app.include_router(registration_requests_router)
app.include_router(cli_router)
app.include_router(company_config_router)
app.include_router(brain_router)
app.include_router(code_search_router)


# ============================================================================
# Health & Status Endpoints
# ============================================================================


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Check API health."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
    )


@app.get("/")
async def root():
    """API root endpoint."""
    return {
        "service": "KGRAG REST API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
    }


# ============================================================================
# Run Application
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")

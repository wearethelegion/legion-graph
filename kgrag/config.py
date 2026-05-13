"""
Configuration management for Kgrag.
Loads from environment variables with sensible defaults.
Integrated with secrets management for sensitive values.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Import secrets manager (lazy initialization)
from kgrag.secrets import get_secrets_manager


class Config:
    """Kgrag configuration"""

    # Project paths
    PROJECT_ROOT: Path = PROJECT_ROOT
    DATA_DIR: Path = PROJECT_ROOT / "data"
    SCRIPTS_DIR: Path = PROJECT_ROOT / "scripts"

    def __init__(self):
        """Initialize config with secrets manager integration"""
        self._secrets = get_secrets_manager()

    # Qdrant Configuration
    @property
    def QDRANT_HOST(self) -> str:
        # Parse from QDRANT_URL if set, otherwise use QDRANT_HOST
        qdrant_url = os.getenv("QDRANT_URL")
        if qdrant_url:
            # Extract host from URL like "http://qdrant:6333"
            from urllib.parse import urlparse

            parsed = urlparse(qdrant_url)
            return parsed.hostname or "localhost"
        return os.getenv("QDRANT_HOST", "localhost")

    @property
    def QDRANT_PORT(self) -> int:
        # Parse from QDRANT_URL if set, otherwise use QDRANT_PORT
        qdrant_url = os.getenv("QDRANT_URL")
        if qdrant_url:
            # Extract port from URL like "http://qdrant:6333"
            from urllib.parse import urlparse

            parsed = urlparse(qdrant_url)
            return parsed.port or 6333
        return int(os.getenv("QDRANT_PORT", "6333"))

    @property
    def QDRANT_GRPC_PORT(self) -> int:
        return int(os.getenv("QDRANT_GRPC_PORT", "6336"))

    @property
    def QDRANT_API_KEY(self) -> Optional[str]:
        """Qdrant API key from secrets manager with env fallback"""
        return self._secrets.get_secret("QDRANT_API_KEY", default=os.getenv("QDRANT_API_KEY"))

    # Neo4j Configuration
    @property
    def NEO4J_URI(self) -> str:
        return os.getenv("NEO4J_URI", "bolt://neo4j:7687")

    @property
    def NEO4J_USER(self) -> str:
        return os.getenv("NEO4J_USER")

    @property
    def NEO4J_PASSWORD(self) -> str:
        """Neo4j password from secrets manager with env fallback"""
        return self._secrets.get_secret("NEO4J_PASSWORD", default=os.getenv("NEO4J_PASSWORD"))

    @property
    def NEO4J_DATABASE(self) -> str:
        """Neo4j database name (single database for multi-tenant architecture)"""
        return os.getenv("NEO4J_DATABASE", "kgrag")

    # Gemini Configuration
    @property
    def GEMINI_API_KEY(self) -> Optional[str]:
        """Gemini API key from secrets manager with env fallback"""
        return self._secrets.get_secret("GEMINI_API_KEY", default=os.getenv("GEMINI_API_KEY"))

    @property
    def GEMINI_MODEL(self) -> str:
        return os.getenv("GEMINI_MODEL", "models/gemini-embedding-001")

    @property
    def GEMINI_EMBEDDING_DIM(self) -> int:
        return int(os.getenv("EMBEDDING_DIMENSIONS", "768"))

    # Optional OpenAI for comparison
    @property
    def OPENAI_API_KEY(self) -> Optional[str]:
        """OpenAI API key from secrets manager with env fallback"""
        return self._secrets.get_secret("OPENAI_API_KEY", default=os.getenv("OPENAI_API_KEY"))

    # PostgreSQL Configuration
    @property
    def POSTGRES_HOST(self) -> str:
        """PostgreSQL host from environment"""
        return os.getenv("POSTGRES_HOST", "postgres")

    @property
    def POSTGRES_PORT(self) -> int:
        """PostgreSQL port from environment"""
        return int(os.getenv("POSTGRES_PORT", "5432"))

    @property
    def POSTGRES_USER(self) -> str:
        """PostgreSQL user from environment"""
        return os.getenv("POSTGRES_USER", "kgrag")

    @property
    def POSTGRES_PASSWORD(self) -> str:
        """PostgreSQL password from secrets manager with env fallback"""
        # Try secrets manager first (if configured)
        password = self._secrets.get_secret("POSTGRES_PASSWORD")
        if password:
            return password

        # Fallback to environment variable
        password = os.getenv("POSTGRES_PASSWORD")
        if not password:
            raise RuntimeError("POSTGRES_PASSWORD must be set in environment or secrets manager")
        return password

    @property
    def POSTGRES_DB(self) -> str:
        """PostgreSQL database name from environment"""
        return os.getenv("POSTGRES_DB", "kgrag_auth")

    @property
    def POSTGRES_URL(self) -> str:
        """
        PostgreSQL connection URL.

        Tries in this order:
        1. POSTGRES_URL from secrets manager
        2. POSTGRES_URL from environment
        3. Build from individual components (POSTGRES_HOST, POSTGRES_USER, etc.)
        """
        # Try secrets manager first
        url = self._secrets.get_secret("POSTGRES_URL")
        if url:
            return url

        # Try environment variable
        url = os.getenv("POSTGRES_URL")
        if url:
            return url

        # Build from components
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Chunking Configuration
    @property
    def MAX_CHUNK_TOKENS(self) -> int:
        return int(os.getenv("MAX_CHUNK_TOKENS", "1800"))

    @property
    def CHUNK_OVERLAP(self) -> int:
        return int(os.getenv("CHUNK_OVERLAP", "200"))

    @property
    def TOKENIZER(self) -> str:
        return "cl100k_base"  # OpenAI encoding (tiktoken)

    # Knowledge Processing Configuration
    @property
    def KNOWLEDGE_MAX_CHUNK_TOKENS(self) -> int:
        """Max tokens per knowledge chunk (smaller than documents for better retrieval)"""
        return int(os.getenv("KNOWLEDGE_MAX_CHUNK_TOKENS", "800"))

    @property
    def KNOWLEDGE_CHUNK_OVERLAP(self) -> int:
        """Token overlap between knowledge chunks for continuity"""
        return int(os.getenv("KNOWLEDGE_CHUNK_OVERLAP", "100"))

    @property
    def RESPECT_MARKDOWN_STRUCTURE(self) -> bool:
        """Preserve markdown headers (##, ###) as chunk boundaries"""
        return os.getenv("RESPECT_MARKDOWN_STRUCTURE", "true").lower() == "true"

    @property
    def EXTRACT_ENTITY_DESCRIPTIONS(self) -> bool:
        """Include descriptions in entity extraction"""
        return os.getenv("EXTRACT_ENTITY_DESCRIPTIONS", "true").lower() == "true"

    @property
    def EXTRACT_RELATIONSHIP_CONTEXT(self) -> bool:
        """Include context/explanation in relationships"""
        return os.getenv("EXTRACT_RELATIONSHIP_CONTEXT", "true").lower() == "true"

    @property
    def MIN_ENTITY_CONFIDENCE(self) -> float:
        """Minimum confidence score for entity extraction (0.0-1.0)"""
        return float(os.getenv("MIN_ENTITY_CONFIDENCE", "0.5"))

    # Retrieval Configuration
    @property
    def VECTOR_SEARCH_LIMIT(self) -> int:
        return int(os.getenv("VECTOR_SEARCH_LIMIT", "20"))

    @property
    def GRAPH_TRAVERSAL_HOPS(self) -> int:
        return int(os.getenv("GRAPH_TRAVERSAL_HOPS", "2"))

    @property
    def HYBRID_RESULTS_LIMIT(self) -> int:
        return int(os.getenv("HYBRID_RESULTS_LIMIT", "10"))

    @property
    def RRF_K(self) -> int:
        return int(os.getenv("RRF_K", "60"))  # Reciprocal rank fusion constant

    # HNSW Configuration
    @property
    def HNSW_M(self) -> int:
        return int(os.getenv("HNSW_M", "16"))

    @property
    def HNSW_EF_CONSTRUCT(self) -> int:
        return int(os.getenv("HNSW_EF_CONSTRUCT", "100"))

    @property
    def HNSW_FULL_SCAN_THRESHOLD(self) -> int:
        return int(os.getenv("HNSW_FULL_SCAN_THRESHOLD", "10000"))

    # Memory Lifecycle Configuration
    WORKING_MEMORY_MESSAGES: int = 50  # Last N messages in context
    EPISODIC_MEMORY_DAYS: int = 7  # Recent conversations
    SEMANTIC_MEMORY_DAYS: int = 9999  # Permanent
    ARCHIVE_AFTER_DAYS: int = 30  # Move to cold storage

    # Summarization Configuration
    SUMMARIZE_THRESHOLD: int = 50  # Messages before summarization
    SUMMARIZE_BATCH_SIZE: int = 10  # Messages per summary batch
    TARGET_COMPRESSION_RATIO: float = 0.2  # 80% token reduction

    # Logging Configuration
    @property
    def LOG_LEVEL(self) -> str:
        return os.getenv("LOG_LEVEL", "INFO")

    @property
    def ALLOWED_ORIGINS(self) -> List[str]:
        """List of allowed CORS origins."""
        origins = os.getenv("ALLOWED_ORIGINS", "*")
        return [origin.strip() for origin in origins.split(",")]

    LOG_FILE: Path = PROJECT_ROOT / "kgrag.log"

    @property
    def mem0_config(self) -> Dict[str, Any]:
        """Mem0 configuration dictionary"""
        return {
            "vector_store": {
                "provider": "qdrant",
                "config": {
                    "host": self.QDRANT_HOST,
                    "port": self.QDRANT_PORT,
                    "api_key": self.QDRANT_API_KEY,
                    "collection_name": "kgrag_default",
                },
            },
            "embedder": {
                "provider": "openai",  # Mem0 uses OpenAI by default
                "config": {
                    "api_key": self.OPENAI_API_KEY or self.GEMINI_API_KEY,
                    "model": "text-embedding-3-small",
                },
            },
            "version": "v1.1",
        }

    @property
    def qdrant_url(self) -> str:
        """Qdrant connection URL"""
        return f"http://{self.QDRANT_HOST}:{self.QDRANT_PORT}"

    def validate(self) -> bool:
        """Validate configuration"""
        errors = []

        if not self.GEMINI_API_KEY:
            errors.append("GEMINI_API_KEY not set")

        if errors:
            print("Configuration errors:")
            for error in errors:
                print(f"  - {error}")
            return False
        return True


# Create global config instance
config = Config()

# ============================================================================
# DOCUMENT PROCESSING CONFIG (v4 Unified Chunking Service)
# ============================================================================
# Configuration for document workers - used by kgrag.document_worker
# See: Engagement entry 409feb9b (v4 Plan Section 7 & 8)
#
# Environment Variables:
#   DOC_PROCESSING_CHUNK_SIZE - Max tokens per chunk (default: 512)
#   DOC_PROCESSING_SEMANTIC_THRESHOLD - Semantic similarity threshold (default: 0.7)
#   DOC_PROCESSING_OVERLAP_RATIO - Context overlap ratio (default: 0.15)
#   DOC_PROCESSING_WORKERS - Workers per process (default: 2)
#   REDIS_URL - Redis connection URL
#   GEMINI_API_KEY - Required for embeddings
# ============================================================================

DOCUMENT_PROCESSING_CONFIG = {
    # Embedding model - PRODUCTION (not experimental)
    "embedding_model": os.getenv("DOC_PROCESSING_EMBEDDING_MODEL", "gemini-embedding-001"),
    # Chunking parameters
    "chunk_size": int(os.getenv("DOC_PROCESSING_CHUNK_SIZE", "512")),
    "semantic_threshold": float(os.getenv("DOC_PROCESSING_SEMANTIC_THRESHOLD", "0.7")),
    "overlap_ratio": float(os.getenv("DOC_PROCESSING_OVERLAP_RATIO", "0.15")),
    "tokenizer": os.getenv("DOC_PROCESSING_TOKENIZER", "cl100k_base"),
    # Queue configuration
    # REDIS_URI is set by docker-compose, REDIS_URL is legacy fallback
    "redis_url": os.getenv("REDIS_URI") or os.getenv("REDIS_URL", "redis://localhost:6379"),
    # Worker configuration
    "workers_per_process": int(os.getenv("DOC_PROCESSING_WORKERS", "2")),
}

# Setup logging
from loguru import logger
import sys

logger.remove()
logger.add(
    sys.stdout,
    level=config.LOG_LEVEL,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)

if config.LOG_FILE:
    logger.add(config.LOG_FILE, level=config.LOG_LEVEL, rotation="10 MB", retention="7 days")

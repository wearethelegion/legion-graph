"""Configuration for Qdrant Storage Service (Service 5).

All settings read from environment variables with sensible defaults.
"""

import os
from typing import Final


class QdrantStorageConfig:
    """Configuration for the Qdrant batch storage service.

    Reads from environment variables. Validates required settings on startup.
    """

    # ── Kafka Settings ───────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    KAFKA_EVENTS_TOPIC: Final[str] = os.getenv("PIPELINE_EVENTS_TOPIC", "pipeline-events")

    # Streaming input topics
    KAFKA_EMBEDDINGS_TOPIC: Final[str] = os.getenv("EMBEDDING_OUTPUT_TOPIC", "embeddings-ready")
    KAFKA_CHUNKS_TOPIC: Final[str] = os.getenv("ENRICHED_CHUNKS_TOPIC", "enriched-code-chunks")

    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "QDRANT_STORAGE_CONSUMER_GROUP_ID", "qdrant-storage-v3-streaming"
    )
    KAFKA_AUTO_OFFSET_RESET: Final[str] = os.getenv(
        "QDRANT_STORAGE_KAFKA_AUTO_OFFSET_RESET", "earliest"
    )
    KAFKA_FETCH_TIMEOUT_MS: Final[int] = int(
        os.getenv("QDRANT_STORAGE_KAFKA_FETCH_TIMEOUT_MS", "1000")
    )

    # ── Qdrant Settings ──────────────────────────────────────────
    QDRANT_URL: Final[str] = os.getenv("QDRANT_URL", "http://qdrant:6333")
    QDRANT_API_KEY: Final[str] = os.getenv("QDRANT_API_KEY", "")

    # Collection names
    COLLECTION_CHUNKS: Final[str] = os.getenv("QDRANT_COLLECTION_CHUNKS", "DocumentChunk_text")
    COLLECTION_ENTITIES: Final[str] = os.getenv("QDRANT_COLLECTION_ENTITIES", "Entity_name")
    COLLECTION_SUMMARIES: Final[str] = os.getenv("QDRANT_COLLECTION_SUMMARIES", "TextSummary_text")
    COLLECTION_TRIPLETS: Final[str] = os.getenv("QDRANT_COLLECTION_TRIPLETS", "Triplet_text")
    COLLECTION_EDGE_TYPES: Final[str] = os.getenv(
        "QDRANT_COLLECTION_EDGE_TYPES", "EdgeType_relationship_name"
    )
    COLLECTION_ENTITY_TYPES: Final[str] = os.getenv(
        "QDRANT_COLLECTION_ENTITY_TYPES", "EntityType_name"
    )

    EMBEDDING_DIMENSION: Final[int] = int(os.getenv("QDRANT_EMBEDDING_DIMENSION", "3072"))

    # ── Batch Settings ───────────────────────────────────────────
    QDRANT_BATCH_SIZE: Final[int] = int(os.getenv("QDRANT_BATCH_SIZE", "100"))
    MAX_RETRIES: Final[int] = int(os.getenv("QDRANT_STORAGE_MAX_RETRIES", "3"))
    RETRY_BASE_DELAY: Final[float] = float(os.getenv("QDRANT_STORAGE_RETRY_BASE_DELAY", "2.0"))

    # ── Streaming Settings ────────────────────────────────────────
    STREAMING_WORKERS: Final[int] = int(os.getenv("QDRANT_STORAGE_WORKERS", "10"))
    COLLECT_TIMEOUT: Final[float] = float(os.getenv("QDRANT_STORAGE_COLLECT_TIMEOUT", "0.5"))

    # ── Postgres Settings ────────────────────────────────────────
    POSTGRES_DSN: Final[str] = os.getenv(
        "QDRANT_STORAGE_POSTGRES_DSN",
        os.getenv("CODE_PROCESSING_POSTGRES_DSN", ""),
    )
    POSTGRES_MIN_POOL: Final[int] = int(os.getenv("QDRANT_STORAGE_PG_MIN_POOL", "2"))
    POSTGRES_MAX_POOL: Final[int] = int(os.getenv("QDRANT_STORAGE_PG_MAX_POOL", "10"))

    # ── Logging ──────────────────────────────────────────────────
    LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration values."""
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        if not cls.QDRANT_URL:
            raise ValueError("QDRANT_URL is required")
        if cls.QDRANT_BATCH_SIZE < 1:
            raise ValueError("QDRANT_BATCH_SIZE must be >= 1")
        if not cls.POSTGRES_DSN:
            raise ValueError("POSTGRES_DSN environment variable is required")
        if cls.EMBEDDING_DIMENSION < 1:
            raise ValueError("QDRANT_EMBEDDING_DIMENSION must be >= 1")

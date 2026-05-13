"""Configuration for Neo4j Storage Service (Service 6).

All settings read from environment variables with sensible defaults.

IMPORTANT:
- Neo4j APOC plugin is REQUIRED for Cognee-compatible schema
- All nodes use __Node__ base label + dynamic labels via apoc.create.addLabels
- All edges use dynamic types via apoc.merge.relationship
- User must enable APOC in Neo4j config: dbms.security.procedures.unrestricted=apoc.*
"""

import os
from typing import Final


class Neo4jStorageConfig:
    """Configuration for the Neo4j streaming storage service.

    Reads from environment variables. Validates required settings on startup.
    """

    # ── Kafka Settings ───────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    KAFKA_DATA_TOPIC: Final[str] = os.getenv("NEO4J_STORAGE_DATA_TOPIC", "extracted-entities")
    KAFKA_EVENTS_TOPIC: Final[str] = os.getenv("PIPELINE_EVENTS_TOPIC", "pipeline-events")
    KAFKA_SUMMARIES_TOPIC: Final[str] = os.getenv("NEO4J_STORAGE_SUMMARIES_TOPIC", "text-summaries")
    KAFKA_ENRICHED_CHUNKS_TOPIC: Final[str] = os.getenv(
        "ENRICHED_CHUNKS_TOPIC", "enriched-code-chunks"
    )
    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "NEO4J_STORAGE_CONSUMER_GROUP_ID", "neo4j-storage-streaming"
    )
    KAFKA_AUTO_OFFSET_RESET: Final[str] = os.getenv(
        "NEO4J_STORAGE_KAFKA_AUTO_OFFSET_RESET", "earliest"
    )
    KAFKA_FETCH_TIMEOUT_MS: Final[int] = int(
        os.getenv("NEO4J_STORAGE_KAFKA_FETCH_TIMEOUT_MS", "2000")
    )

    # ── Neo4j Settings ───────────────────────────────────────────
    NEO4J_URI: Final[str] = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    NEO4J_USER: Final[str] = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: Final[str] = os.getenv("NEO4J_PASSWORD", "")
    NEO4J_DATABASE: Final[str] = os.getenv("NEO4J_DATABASE", "neo4j")

    # ── Streaming Settings ───────────────────────────────────────
    STREAMING_WORKERS: Final[int] = int(os.getenv("NEO4J_STORAGE_WORKERS", "10"))
    BATCH_SIZE: Final[int] = int(os.getenv("NEO4J_STORAGE_BATCH_SIZE", "100"))
    COLLECT_TIMEOUT: Final[float] = float(os.getenv("NEO4J_STORAGE_COLLECT_TIMEOUT", "0.5"))

    # ── Neo4j Batch Settings ─────────────────────────────────────
    NEO4J_BATCH_SIZE: Final[int] = int(os.getenv("NEO4J_BATCH_SIZE", "500"))
    MAX_RETRIES: Final[int] = int(os.getenv("NEO4J_STORAGE_MAX_RETRIES", "3"))
    RETRY_BASE_DELAY: Final[float] = float(os.getenv("NEO4J_STORAGE_RETRY_BASE_DELAY", "2.0"))

    # ── Postgres Settings ────────────────────────────────────────
    POSTGRES_DSN: Final[str] = os.getenv(
        "NEO4J_STORAGE_POSTGRES_DSN",
        os.getenv("CODE_PROCESSING_POSTGRES_DSN", ""),
    )
    POSTGRES_MIN_POOL: Final[int] = int(os.getenv("NEO4J_STORAGE_PG_MIN_POOL", "2"))
    POSTGRES_MAX_POOL: Final[int] = int(os.getenv("NEO4J_STORAGE_PG_MAX_POOL", "10"))

    # ── Logging ──────────────────────────────────────────────────
    LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration values."""
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        if not cls.KAFKA_DATA_TOPIC:
            raise ValueError("NEO4J_STORAGE_DATA_TOPIC is required")
        if not cls.NEO4J_URI:
            raise ValueError("NEO4J_URI is required")
        if not cls.NEO4J_PASSWORD:
            raise ValueError("NEO4J_PASSWORD is required")
        if cls.NEO4J_BATCH_SIZE < 1:
            raise ValueError("NEO4J_BATCH_SIZE must be >= 1")
        if cls.STREAMING_WORKERS < 1:
            raise ValueError("NEO4J_STORAGE_WORKERS must be >= 1")
        if cls.BATCH_SIZE < 1:
            raise ValueError("NEO4J_STORAGE_BATCH_SIZE must be >= 1")
        if not cls.POSTGRES_DSN:
            raise ValueError("POSTGRES_DSN environment variable is required")

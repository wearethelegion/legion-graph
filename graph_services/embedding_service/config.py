"""Configuration for Embedding Service (Service 4).

All settings read from environment variables with sensible defaults.
"""

import os
from typing import Final, List


class EmbeddingConfig:
    """Configuration for the embedding Kafka consumer.

    Reads from environment variables. Validates required settings on startup.
    """

    # ── Kafka Consumer Settings ──────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    KAFKA_INPUT_TOPICS: Final[List[str]] = os.getenv(
        "EMBEDDING_INPUT_TOPICS", "extracted-entities,text-summaries"
    ).split(",")
    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "EMBEDDING_CONSUMER_GROUP_ID", "embedding-processor-v2"
    )
    KAFKA_AUTO_COMMIT: Final[bool] = (
        os.getenv("EMBEDDING_KAFKA_AUTO_COMMIT", "true").lower() == "true"
    )
    KAFKA_AUTO_OFFSET_RESET: Final[str] = os.getenv("EMBEDDING_KAFKA_AUTO_OFFSET_RESET", "earliest")
    KAFKA_FETCH_TIMEOUT_MS: Final[int] = int(os.getenv("EMBEDDING_KAFKA_FETCH_TIMEOUT_MS", "1000"))

    # ── Kafka Producer Settings ──────────────────────────────────
    KAFKA_OUTPUT_TOPIC: Final[str] = os.getenv("EMBEDDING_OUTPUT_TOPIC", "embeddings-ready")
    KAFKA_EVENTS_TOPIC: Final[str] = os.getenv("PIPELINE_EVENTS_TOPIC", "pipeline-events")

    # ── Processing Settings ──────────────────────────────────────
    EMBEDDING_BATCH_SIZE: Final[int] = int(os.getenv("EMBEDDING_BATCH_SIZE", "100"))
    EMBEDDING_WORKERS: Final[int] = int(os.getenv("EMBEDDING_WORKERS", "50"))
    EMBEDDING_API_CONCURRENCY: Final[int] = int(os.getenv("EMBEDDING_API_CONCURRENCY", "20"))
    COLLECT_TIMEOUT: Final[float] = float(os.getenv("EMBEDDING_COLLECT_TIMEOUT", "0.5"))
    MAX_PARALLEL_WORKERS: Final[int] = int(os.getenv("EMBEDDING_MAX_WORKERS", "10"))  # Legacy
    MAX_RETRIES: Final[int] = int(os.getenv("EMBEDDING_MAX_RETRIES", "3"))
    RETRY_BASE_DELAY: Final[float] = float(os.getenv("EMBEDDING_RETRY_BASE_DELAY", "2.0"))

    # ── Postgres Settings ────────────────────────────────────────
    POSTGRES_DSN: Final[str] = os.getenv(
        "EMBEDDING_POSTGRES_DSN",
        os.getenv("CODE_PROCESSING_POSTGRES_DSN", ""),
    )
    POSTGRES_MIN_POOL: Final[int] = int(os.getenv("EMBEDDING_PG_MIN_POOL", "2"))
    POSTGRES_MAX_POOL: Final[int] = int(os.getenv("EMBEDDING_PG_MAX_POOL", "10"))

    # ── Logging ──────────────────────────────────────────────────
    LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration values."""
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        if not cls.KAFKA_INPUT_TOPICS:
            raise ValueError("EMBEDDING_INPUT_TOPICS is required")
        if cls.EMBEDDING_BATCH_SIZE < 1:
            raise ValueError("EMBEDDING_BATCH_SIZE must be >= 1")
        if cls.EMBEDDING_WORKERS < 1:
            raise ValueError("EMBEDDING_WORKERS must be >= 1")
        if cls.EMBEDDING_API_CONCURRENCY < 1:
            raise ValueError("EMBEDDING_API_CONCURRENCY must be >= 1")
        if not cls.POSTGRES_DSN:
            raise ValueError("POSTGRES_DSN environment variable is required")

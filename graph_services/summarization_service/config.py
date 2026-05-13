"""Configuration for Summarization Service (Service 3).

All settings read from environment variables with sensible defaults.
"""

import os
from typing import Final


class SummarizationConfig:
    """Configuration for the summarization Kafka consumer.

    Reads from environment variables. Validates required settings on startup.
    """

    # ── Kafka Consumer Settings ──────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    KAFKA_INPUT_TOPIC: Final[str] = os.getenv("SUMMARIZATION_INPUT_TOPIC", "enriched-code-chunks")
    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "SUMMARIZATION_CONSUMER_GROUP_ID", "summarization-processor-v2"
    )
    KAFKA_AUTO_COMMIT: Final[bool] = (
        os.getenv("SUMMARIZATION_KAFKA_AUTO_COMMIT", "true").lower() == "true"
    )
    KAFKA_AUTO_OFFSET_RESET: Final[str] = os.getenv(
        "SUMMARIZATION_KAFKA_AUTO_OFFSET_RESET", "earliest"
    )
    KAFKA_FETCH_TIMEOUT_MS: Final[int] = int(
        os.getenv("SUMMARIZATION_KAFKA_FETCH_TIMEOUT_MS", "1000")
    )

    # ── Kafka Producer Settings ──────────────────────────────────
    KAFKA_OUTPUT_TOPIC: Final[str] = os.getenv("SUMMARIZATION_OUTPUT_TOPIC", "text-summaries")
    KAFKA_EVENTS_TOPIC: Final[str] = os.getenv("PIPELINE_EVENTS_TOPIC", "pipeline-events")

    # ── Processing Settings ──────────────────────────────────────
    BATCH_SIZE: Final[int] = int(os.getenv("SUMMARIZATION_BATCH_SIZE", "50"))
    MAX_PARALLEL_WORKERS: Final[int] = int(os.getenv("SUMMARIZATION_MAX_WORKERS", "50"))
    MAX_RETRIES: Final[int] = int(os.getenv("SUMMARIZATION_MAX_RETRIES", "3"))
    RETRY_BASE_DELAY: Final[float] = float(os.getenv("SUMMARIZATION_RETRY_BASE_DELAY", "2.0"))

    # ── Postgres Settings ────────────────────────────────────────
    POSTGRES_DSN: Final[str] = os.getenv(
        "SUMMARIZATION_POSTGRES_DSN",
        os.getenv("CODE_PROCESSING_POSTGRES_DSN", ""),
    )
    POSTGRES_MIN_POOL: Final[int] = int(os.getenv("SUMMARIZATION_PG_MIN_POOL", "2"))
    POSTGRES_MAX_POOL: Final[int] = int(os.getenv("SUMMARIZATION_PG_MAX_POOL", "10"))

    # ── Logging ──────────────────────────────────────────────────
    LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration values."""
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        if not cls.KAFKA_INPUT_TOPIC:
            raise ValueError("SUMMARIZATION_INPUT_TOPIC is required")
        if cls.BATCH_SIZE < 1:
            raise ValueError("SUMMARIZATION_BATCH_SIZE must be >= 1")
        if cls.MAX_PARALLEL_WORKERS < 1:
            raise ValueError("SUMMARIZATION_MAX_WORKERS must be >= 1")
        if not cls.POSTGRES_DSN:
            raise ValueError("POSTGRES_DSN environment variable is required")

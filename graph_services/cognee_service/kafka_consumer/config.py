"""Configuration for Cogni Kafka Consumer."""

import os
from typing import Final


class CogniConsumerConfig:
    """Configuration for the Cogni Kafka consumer.

    All settings are read from environment variables with sensible defaults.
    """

    # ── Kafka Settings ──────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    KAFKA_TOPIC: Final[str] = os.getenv("KAFKA_DATA_ENRICHMENT_TOPIC", "data_enrichment")
    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "COGNI_KAFKA_CONSUMER_GROUP_ID", "cogni-processor"
    )
    KAFKA_AUTO_COMMIT: Final[bool] = os.getenv("COGNI_KAFKA_AUTO_COMMIT", "true").lower() == "true"
    KAFKA_AUTO_OFFSET_RESET: Final[str] = os.getenv("COGNI_KAFKA_AUTO_OFFSET_RESET", "earliest")
    KAFKA_FETCH_TIMEOUT_MS: Final[int] = int(os.getenv("COGNI_KAFKA_FETCH_TIMEOUT_MS", "1000"))

    # ── Processing Settings ─────────────────────────────────────
    BATCH_SIZE: Final[int] = int(
        os.getenv("COGNI_BATCH_SIZE", "20")
    )  # Number of files to add() before calling cognify() once
    MAX_RETRIES: Final[int] = int(os.getenv("COGNI_MAX_RETRIES", "3"))
    RETRY_DELAY: Final[float] = float(os.getenv("COGNI_RETRY_DELAY", "5.0"))
    COGNIFY_TIMEOUT: Final[int] = int(
        os.getenv("COGNI_COGNIFY_TIMEOUT", "600")
    )  # seconds — cognify can be slow (LLM calls)

    # ── Postgres Settings (code_processing schema) ──────────────
    POSTGRES_DSN: Final[str] = os.getenv(
        "COGNI_POSTGRES_DSN",
        os.getenv(
            "CODE_PROCESSING_POSTGRES_DSN",
            "postgresql://kgrag:kgrag_password@postgres:5432/kgrag_auth",
        ),
    )

    # ── Logging ─────────────────────────────────────────────────
    LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration."""
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        if not cls.KAFKA_TOPIC:
            raise ValueError("KAFKA_DATA_ENRICHMENT_TOPIC is required")
        if cls.BATCH_SIZE < 1:
            raise ValueError("COGNI_BATCH_SIZE must be >= 1")

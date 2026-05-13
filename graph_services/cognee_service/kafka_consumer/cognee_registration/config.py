"""Configuration for Cognee Registration Kafka Consumer."""

import os
from typing import Final


class CogneeRegistrationConsumerConfig:
    """Configuration for the cognee registration Kafka consumer.

    Reads enriched-code-chunks topic and calls cognee.add() for each chunk.
    Uses its own consumer group so it is independent from other consumers
    on the same topic (entity extraction, etc.).
    """

    # ── Kafka Settings ──────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    KAFKA_TOPIC: Final[str] = os.getenv("KAFKA_ENRICHED_CHUNKS_TOPIC", "enriched-code-chunks")
    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "COGNEE_REGISTRATION_CONSUMER_GROUP_ID", "cognee-registration-group"
    )
    KAFKA_AUTO_COMMIT: Final[bool] = (
        os.getenv("COGNEE_REGISTRATION_KAFKA_AUTO_COMMIT", "true").lower() == "true"
    )
    KAFKA_AUTO_OFFSET_RESET: Final[str] = os.getenv(
        "COGNEE_REGISTRATION_KAFKA_AUTO_OFFSET_RESET", "earliest"
    )
    KAFKA_FETCH_TIMEOUT_MS: Final[int] = int(
        os.getenv("COGNEE_REGISTRATION_KAFKA_FETCH_TIMEOUT_MS", "1000")
    )

    # ── Processing Settings ─────────────────────────────────────
    MAX_RETRIES: Final[int] = int(os.getenv("COGNEE_REGISTRATION_MAX_RETRIES", "2"))
    RETRY_DELAY: Final[float] = float(os.getenv("COGNEE_REGISTRATION_RETRY_DELAY", "2.0"))

    # ── Logging ─────────────────────────────────────────────────
    LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration."""
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        if not cls.KAFKA_TOPIC:
            raise ValueError("KAFKA_ENRICHED_CHUNKS_TOPIC is required")
        if not cls.KAFKA_CONSUMER_GROUP_ID:
            raise ValueError("COGNEE_REGISTRATION_CONSUMER_GROUP_ID is required")

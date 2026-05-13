"""Configuration for Entity Extraction Kafka Consumer."""

import os
from typing import Final


class EntityExtractionConsumerConfig:
    """Configuration for the entity extraction Kafka consumer.

    All settings are read from environment variables with sensible defaults.
    This consumer reads from enriched-code-chunks topic (same as enriched_chunks
    consumer) but uses a different consumer group so both consumers receive
    every message independently.
    """

    # ── Kafka Settings ──────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    KAFKA_TOPIC: Final[str] = os.getenv("KAFKA_ENRICHED_CHUNKS_TOPIC", "enriched-code-chunks")
    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "ENTITY_EXTRACTION_CONSUMER_GROUP_ID", "entity-extraction-processor"
    )
    KAFKA_AUTO_COMMIT: Final[bool] = (
        os.getenv("ENTITY_EXTRACTION_KAFKA_AUTO_COMMIT", "true").lower() == "true"
    )
    KAFKA_AUTO_OFFSET_RESET: Final[str] = os.getenv(
        "ENTITY_EXTRACTION_KAFKA_AUTO_OFFSET_RESET", "earliest"
    )
    KAFKA_FETCH_TIMEOUT_MS: Final[int] = int(
        os.getenv("ENTITY_EXTRACTION_KAFKA_FETCH_TIMEOUT_MS", "1000")
    )

    # ── Processing Settings ─────────────────────────────────────
    BATCH_SIZE: Final[int] = int(os.getenv("ENTITY_EXTRACTION_BATCH_SIZE", "50"))
    MAX_RETRIES: Final[int] = int(os.getenv("ENTITY_EXTRACTION_MAX_RETRIES", "3"))
    RETRY_DELAY: Final[float] = float(os.getenv("ENTITY_EXTRACTION_RETRY_DELAY", "5.0"))
    PROCESSING_TIMEOUT: Final[int] = int(
        os.getenv("ENTITY_EXTRACTION_TIMEOUT", "600")
    )  # seconds — LLM extraction calls are slow

    # Custom prompt for code graph extraction
    CUSTOM_PROMPT_PATH: Final[str] = os.getenv(
        "COGNEE_CODE_GRAPH_PROMPT_PATH",
        "/app/cognee_service/prompts/code_graph_extraction_prompt.txt",
    )

    # ── Postgres Settings (code_processing schema) ──────────────
    POSTGRES_DSN: Final[str] = os.getenv(
        "ENTITY_EXTRACTION_POSTGRES_DSN",
        os.getenv(
            "CODE_PROCESSING_POSTGRES_DSN",
            "postgresql://kgrag:kgrag_password@postgres:5432/kgrag_auth",
        ),
    )

    # ── Cognee Postgres (for metadata searchability) ────────────
    COGNEE_POSTGRES_DSN: Final[str] = os.getenv(
        "COGNEE_POSTGRES_DSN",
        "postgresql://kgrag:kgrag_password@postgres:5432/cognee",
    )

    # ── Logging ─────────────────────────────────────────────────
    LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration."""
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        if not cls.KAFKA_TOPIC:
            raise ValueError("KAFKA_ENRICHED_CHUNKS_TOPIC is required")
        if cls.BATCH_SIZE < 1:
            raise ValueError("ENTITY_EXTRACTION_BATCH_SIZE must be >= 1")

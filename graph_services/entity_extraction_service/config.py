"""Configuration for Entity Extraction Service (Service 2).

All settings read from environment variables with sensible defaults.
"""

import os
from typing import Final


class EntityExtractionConfig:
    """Configuration for the entity extraction Kafka consumer.

    Reads from environment variables. Validates required settings on startup.
    """

    # ── Kafka Consumer Settings ──────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    KAFKA_INPUT_TOPIC: Final[str] = os.getenv(
        "ENTITY_EXTRACTION_INPUT_TOPIC", "enriched-code-chunks"
    )
    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "ENTITY_EXTRACTION_CONSUMER_GROUP_ID", "entity-extraction-processor-v2"
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

    # ── Kafka Producer Settings ──────────────────────────────────
    KAFKA_OUTPUT_TOPIC: Final[str] = os.getenv(
        "ENTITY_EXTRACTION_OUTPUT_TOPIC", "extracted-entities"
    )
    KAFKA_EVENTS_TOPIC: Final[str] = os.getenv("PIPELINE_EVENTS_TOPIC", "pipeline-events")

    # ── Processing Settings ──────────────────────────────────────
    BATCH_SIZE: Final[int] = int(os.getenv("ENTITY_EXTRACTION_BATCH_SIZE", "50"))
    MAX_PARALLEL_WORKERS: Final[int] = int(os.getenv("ENTITY_EXTRACTION_MAX_WORKERS", "50"))
    MAX_RETRIES: Final[int] = int(os.getenv("ENTITY_EXTRACTION_MAX_RETRIES", "3"))
    RETRY_BASE_DELAY: Final[float] = float(os.getenv("ENTITY_EXTRACTION_RETRY_BASE_DELAY", "2.0"))

    # ── Custom Prompt ────────────────────────────────────────────
    CUSTOM_PROMPT_PATH: Final[str] = os.getenv(
        "ENTITY_EXTRACTION_PROMPT_PATH",
        "/app/cognee_service/prompts/code_graph_extraction_prompt.txt",
    )
    DOCUMENT_PROMPT_PATH: Final[str] = os.getenv(
        "ENTITY_EXTRACTION_DOCUMENT_PROMPT_PATH",
        "/app/cognee_service/prompts/document_graph_extraction_prompt.txt",
    )

    # ── Postgres Settings ────────────────────────────────────────
    POSTGRES_DSN: Final[str] = os.getenv(
        "ENTITY_EXTRACTION_POSTGRES_DSN",
        os.getenv("CODE_PROCESSING_POSTGRES_DSN", ""),
    )
    POSTGRES_MIN_POOL: Final[int] = int(os.getenv("ENTITY_EXTRACTION_PG_MIN_POOL", "2"))
    POSTGRES_MAX_POOL: Final[int] = int(os.getenv("ENTITY_EXTRACTION_PG_MAX_POOL", "10"))

    # ── Logging ──────────────────────────────────────────────────
    LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration values."""
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        if not cls.KAFKA_INPUT_TOPIC:
            raise ValueError("ENTITY_EXTRACTION_INPUT_TOPIC is required")
        if cls.BATCH_SIZE < 1:
            raise ValueError("ENTITY_EXTRACTION_BATCH_SIZE must be >= 1")
        if cls.MAX_PARALLEL_WORKERS < 1:
            raise ValueError("ENTITY_EXTRACTION_MAX_WORKERS must be >= 1")
        if not cls.POSTGRES_DSN:
            raise ValueError("POSTGRES_DSN environment variable is required")

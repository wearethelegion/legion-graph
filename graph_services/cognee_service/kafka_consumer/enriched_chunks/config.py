"""Configuration for Enriched Chunks Kafka Consumer."""

import os
from typing import Final


class EnrichedChunksConsumerConfig:
    """Configuration for the enriched chunks Kafka consumer.

    All settings are read from environment variables with sensible defaults.
    This consumer reads from enriched-code-chunks topic (batch size 50) and
    uses a custom code graph extraction prompt.
    """

    # ── Kafka Settings ──────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    KAFKA_TOPIC: Final[str] = os.getenv("KAFKA_ENRICHED_CHUNKS_TOPIC", "enriched-code-chunks")
    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "ENRICHED_CHUNKS_CONSUMER_GROUP_ID", "cognee-enriched-chunks-processor"
    )
    KAFKA_AUTO_COMMIT: Final[bool] = (
        os.getenv("ENRICHED_CHUNKS_KAFKA_AUTO_COMMIT", "true").lower() == "true"
    )
    KAFKA_AUTO_OFFSET_RESET: Final[str] = os.getenv(
        "ENRICHED_CHUNKS_KAFKA_AUTO_OFFSET_RESET", "earliest"
    )
    KAFKA_FETCH_TIMEOUT_MS: Final[int] = int(
        os.getenv("ENRICHED_CHUNKS_KAFKA_FETCH_TIMEOUT_MS", "1000")
    )

    # ── Processing Settings ─────────────────────────────────────
    BATCH_SIZE: Final[int] = int(
        os.getenv("ENRICHED_CHUNKS_BATCH_SIZE", "100")
    )  # Sliding window size
    MAX_RETRIES: Final[int] = int(os.getenv("ENRICHED_CHUNKS_MAX_RETRIES", "3"))
    RETRY_DELAY: Final[float] = float(os.getenv("ENRICHED_CHUNKS_RETRY_DELAY", "5.0"))

    # Custom prompt for code graph extraction
    CUSTOM_PROMPT_PATH: Final[str] = os.getenv(
        "COGNEE_CODE_GRAPH_PROMPT_PATH",
        "/app/cognee_service/prompts/code_graph_extraction_prompt.txt",
    )

    # ── Postgres Settings (code_processing schema) ──────────────
    POSTGRES_DSN: Final[str] = os.getenv(
        "ENRICHED_CHUNKS_POSTGRES_DSN",
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
            raise ValueError("ENRICHED_CHUNKS_BATCH_SIZE must be >= 1")

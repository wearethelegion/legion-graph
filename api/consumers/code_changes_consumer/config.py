"""Configuration for Code Changes Kafka Consumer.

Centralized configuration for consuming code change events from
the data_enrichment Kafka topic.
"""

import os
from typing import Final


class CodeChangesConsumerConfig:
    """Configuration settings for code changes consumer."""

    # Kafka Settings
    KAFKA_BOOTSTRAP_SERVERS: Final[str] = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )
    KAFKA_TOPIC: Final[str] = os.getenv(
        "KAFKA_DATA_ENRICHMENT_TOPIC",
        "data_enrichment"
    )
    KAFKA_CONSUMER_GROUP_ID: Final[str] = os.getenv(
        "KAFKA_CONSUMER_GROUP_ID",
        "code-changes-consumer"
    )
    KAFKA_AUTO_COMMIT: Final[bool] = os.getenv(
        "KAFKA_AUTO_COMMIT",
        "true"
    ).lower() == "true"
    KAFKA_AUTO_OFFSET_RESET: Final[str] = os.getenv(
        "KAFKA_AUTO_OFFSET_RESET",
        "earliest"  # earliest or latest
    )

    # Streaming Processing Settings
    MAX_CONCURRENT_WORKERS: Final[int] = int(os.getenv(
        "MAX_CONCURRENT_WORKERS",
        "100"  # Continuous streaming with worker pool
    ))
    FETCH_TIMEOUT_MS: Final[int] = int(os.getenv(
        "FETCH_TIMEOUT_MS",
        "100"  # Short timeout for responsiveness
    ))

    # Processing Settings
    # DEPRECATED: SKIP_DELETED_FILES is no longer used.
    # DELETED files are now properly handled via storage_orchestrator.delete_by_file_path()
    # Keeping for backward compatibility, but has no effect.
    SKIP_DELETED_FILES: Final[bool] = os.getenv(
        "SKIP_DELETED_FILES",
        "false"  # Changed default to false since deletions are now processed
    ).lower() == "true"

    MAX_RETRIES: Final[int] = int(os.getenv("CONSUMER_MAX_RETRIES", "3"))
    RETRY_DELAY: Final[float] = float(os.getenv("CONSUMER_RETRY_DELAY", "2.0"))

    # Default Multi-tenant IDs (will be overridden from workspace/repository)
    DEFAULT_PROJECT_ID: Final[str] = os.getenv(
        "DEFAULT_PROJECT_ID",
        "00000000-0000-0000-0000-000000000000"
    )
    DEFAULT_COMPANY_ID: Final[str] = os.getenv(
        "DEFAULT_COMPANY_ID",
        "00000000-0000-0000-0000-000000000000"
    )
    DEFAULT_USER_ID: Final[str] = os.getenv(
        "DEFAULT_USER_ID",
        "system"
    )

    # Logging
    LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO")

    # MongoDB Settings (for ingestion metrics tracking)
    MONGODB_HOST: Final[str] = os.getenv("MONGODB_HOST", "localhost")
    MONGODB_PORT: Final[str] = os.getenv("MONGODB_PORT", "27017")
    MONGODB_USERNAME: Final[str] = os.getenv("MONGODB_USERNAME", "")
    MONGODB_PASSWORD: Final[str] = os.getenv("MONGODB_PASSWORD", "")
    MONGODB_DATABASE: Final[str] = os.getenv("MONGODB_DATABASE", "code_intel")
    MONGODB_AUTH_DATABASE: Final[str] = os.getenv("MONGODB_AUTH_DATABASE", "admin")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration."""
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")

        if not cls.KAFKA_TOPIC:
            raise ValueError("KAFKA_TOPIC is required")

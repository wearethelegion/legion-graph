"""Configuration helpers for the Kafka processing service."""

from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaProcessingSettings(BaseSettings):
    """Runtime configuration for the Kafka-driven preprocessor."""

    kafka_bootstrap_servers: str = Field("redpanda:9092", description="Kafka bootstrap servers")
    kafka_topic: str = Field(
        "incoming_requests", description="Topic to consume repository requests from"
    )
    # Phase 4: data_enrichment_topic removed — no consumer exists for this topic.
    # The v2 pipeline uses enriched-code-chunks topic exclusively.
    kafka_group_id: str = Field("code-preprocessor", description="Consumer group identifier")
    kafka_producer_client_id: str = Field(
        "code-preprocessor-producer", description="Kafka client id for producer"
    )
    kafka_max_request_size: int = Field(
        8 * 1024 * 1024, description="Maximum Kafka producer request size in bytes"
    )
    repo_storage_root: Path = Field(
        Path("rag_storage/repos"), description="Directory where repositories are cloned"
    )
    github_base_url: str = Field("https://github.com", description="Base URL for Git remotes")
    github_token: str = Field(
        "",
        description="Optional GitHub personal access token for cloning private repositories",
    )
    default_branch: str = Field(
        "main", description="Branch to update if payload does not provide one"
    )

    # Parser worker pool settings
    parser_max_workers: int = Field(20, description="Maximum concurrent parser processes")
    parser_timeout_seconds: int = Field(30, description="Timeout for parser execution per file")

    # Streaming processing settings (OOM prevention)
    max_concurrent_files: int = Field(
        10, description="Maximum concurrent file processing (bounded parallelism)"
    )
    progress_update_interval: int = Field(10, description="Update ingestion progress every N files")

    # Embed worker pool settings (Queue 2: micro-batch embedding pipeline)
    embed_workers: int = Field(5, description="Number of concurrent embed workers")
    embed_batch_size: int = Field(100, description="Chunk batch size for embedding API calls")
    embed_batch_timeout: float = Field(
        2.0, description="Seconds to wait before flushing a partial embed batch"
    )

    @field_validator("repo_storage_root", mode="before")
    @classmethod
    def _expand_repo_root(cls, value: Any) -> Path:
        return Path(value).expanduser().resolve()

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=".env",
    )


settings = KafkaProcessingSettings()


def get_settings() -> KafkaProcessingSettings:
    """Return cached Kafka processing settings."""
    return settings

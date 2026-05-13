"""Configuration for the document preprocessor service."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DocumentPreprocessorSettings(BaseSettings):
    """Runtime configuration for the document preprocessor."""

    # Kafka
    kafka_bootstrap_servers: str = Field("redpanda:9092", description="Kafka bootstrap servers")
    kafka_input_topic: str = Field(
        "brain_events", description="Topic to consume BrainEvent messages from"
    )
    kafka_output_topic: str = Field(
        "enriched-code-chunks",
        description="Topic to publish EnrichedChunkMessage to",
    )
    kafka_group_id: str = Field("document-preprocessor", description="Consumer group identifier")
    kafka_producer_client_id: str = Field(
        "document-preprocessor-producer", description="Kafka producer client id"
    )
    kafka_max_request_size: int = Field(
        8 * 1024 * 1024,
        description="Maximum Kafka producer request size in bytes",
    )

    # Postgres
    database_url: str = Field(
        "",
        description="Postgres connection URL (DATABASE_URL or POSTGRES_URL)",
    )

    # Chunking
    chunk_max_chars: int = Field(
        1000,
        description="Maximum characters per chunk (soft limit, best-effort)",
    )
    chunk_min_chars: int = Field(
        100,
        description="Minimum characters to keep a chunk (smaller chunks are merged)",
    )

    # Idempotency
    idempotency_cache_size: int = Field(
        10_000,
        description="LRU cache capacity for processed event_ids",
    )

    # Embedding — for document chunk indexing (mirrors code_preprocessor)
    embedding_model: str = Field(
        "gemini/gemini-embedding-001",
        description="Embedding model identifier (LiteLLM format, e.g. gemini/gemini-embedding-001)",
    )
    embedding_dimensions: int = Field(
        3072,
        description="Embedding vector dimensions",
    )
    embedding_concurrency: int = Field(
        3,
        description="Max concurrent embedding API calls",
    )
    embedding_batch_size: int = Field(
        20,
        description="Chunks per embedding API call",
    )
    gemini_api_key: str = Field(
        "", description="Gemini API key for embedding (read from GEMINI_API_KEY)"
    )

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=".env",
        env_prefix="",  # No prefix - read KAFKA_BOOTSTRAP_SERVERS directly
    )


_settings: DocumentPreprocessorSettings | None = None


def get_settings() -> DocumentPreprocessorSettings:
    """Return cached service settings (created on first call)."""
    global _settings
    if _settings is None:
        _settings = DocumentPreprocessorSettings()
    return _settings
